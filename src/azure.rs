//! Azure Blob Storage over its REST API (Shared Key or SAS auth), on the blocking
//! [`crate::http`] client; the edge's ranged reads while it serves use the async
//! [`crate::aclient`]. No SDK.
//!
//! Large slab files are uploaded as 16 MiB blocks (8 in parallel) committed atomically
//! with Put Block List, and downloaded as parallel 16 MiB ranged reads.

use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::hmac;

use crate::http::{Client, Url};
use crate::store::{Cond, ObjectInfo, valid_key};
use crate::util::{Context, Error, ErrorKind, Result};

const API_VERSION: &str = "2021-08-06";

/// One Blob service request.
#[derive(Clone, Copy)]
struct Req<'a> {
    method: &'a str,
    path: &'a str,
    query: &'a [(&'a str, String)],
    headers: &'a [(&'a str, String)],
    body: &'a [u8],
}
const SINGLE_PUT_MAX: u64 = 64 << 20;
const BLOCK: u64 = 16 << 20;
const PARALLEL: usize = 8;
const ATTEMPTS: u32 = 4;
const TIMEOUT: Duration = Duration::from_secs(120);

/// The wait before retry `attempt` (1, 2, ...): 400 ms, 800 ms, 1.6 s.
fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(200u64.saturating_mul(1u64.wrapping_shl(attempt)))
}

#[derive(Clone)]
enum Credential {
    Key(Vec<u8>),
    Sas(String),
}

pub struct AzureStore {
    account: String,
    credential: Credential,
    endpoint: Url,
    container: String,
    /// Key prefix inside the container (empty or ending in `/`).
    prefix: String,
    /// The edge's async client ([`AzureStore::read_range_async`]).
    aclient: std::sync::OnceLock<Box<crate::aclient::AClient>>,
}

impl AzureStore {
    /// `spec` is `<container>[/<prefix>]`; `cs` a connection string (account key or SAS).
    pub fn new(cs: &str, spec: &str) -> Result<AzureStore> {
        let (container, prefix) = match spec.split_once('/') {
            Some((c, p)) => (c.to_string(), format!("{}/", p.trim_matches('/'))),
            None => (spec.to_string(), String::new()),
        };
        if container.len() < 3
            || !container
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            crate::bail!("invalid Azure container name {container:?}");
        }
        let mut account = None;
        let mut key = None;
        let mut protocol = "https".to_string();
        let mut suffix = "core.windows.net".to_string();
        let mut endpoint = None;
        let mut sas = None;
        for part in cs.trim().split(';').filter(|p| !p.trim().is_empty()) {
            let (k, v) = part.split_once('=').ctx("malformed connection string")?;
            let v = v.trim();
            match k.trim() {
                "AccountName" => account = Some(v.to_string()),
                "AccountKey" => {
                    key = Some(crate::b64::decode(v).ctx("AccountKey is not valid base64")?)
                }
                "DefaultEndpointsProtocol" => protocol = v.to_string(),
                "EndpointSuffix" => suffix = v.to_string(),
                "BlobEndpoint" => endpoint = Some(v.trim_end_matches('/').to_string()),
                "SharedAccessSignature" => sas = Some(v.trim_start_matches('?').to_string()),
                _ => {}
            }
        }
        let credential = match (key.filter(|k| !k.is_empty()), sas.filter(|s| !s.is_empty())) {
            (Some(k), _) => Credential::Key(k),
            (None, Some(s)) => Credential::Sas(s),
            (None, None) => {
                crate::bail!("connection string has neither AccountKey nor SharedAccessSignature")
            }
        };
        let account = match (account.filter(|a| !a.is_empty()), &endpoint) {
            (Some(a), _) => a,
            (None, Some(e)) => e
                .split("://")
                .nth(1)
                .and_then(|h| h.split(['.', '/', ':']).next())
                .unwrap_or("sas")
                .to_string(),
            (None, None) => crate::bail!("connection string has no AccountName or BlobEndpoint"),
        };
        let endpoint = Url::parse(
            &endpoint.unwrap_or_else(|| format!("{protocol}://{account}.blob.{suffix}")),
        )?;
        Ok(AzureStore {
            account,
            credential,
            endpoint,
            container,
            prefix,
            aclient: std::sync::OnceLock::new(),
        })
    }

    pub fn describe(&self) -> String {
        format!("azure:{}/{}/{}", self.account, self.container, self.prefix)
    }

    fn client(&self) -> Client {
        Client::new(self.endpoint.clone(), TIMEOUT)
    }

    fn blob_path(&self, key: &str) -> Result<String> {
        if !valid_key(key) {
            crate::bail!("invalid store key {key:?}");
        }
        Ok(format!("/{}/{}{key}", self.container, self.prefix))
    }

    /// The headers and query string of one signed request: SharedKey (an `Authorization`
    /// header over the canonical request), or the SAS appended to the query.
    fn signed<'h>(
        &self,
        method: &str,
        path: &str,
        query: &[(&str, String)],
        extra: &[(&'h str, String)],
        body_len: u64,
    ) -> (Vec<(&'h str, String)>, String) {
        let mut headers: Vec<(&str, String)> = vec![
            ("x-ms-date", http_date(SystemTime::now())),
            ("x-ms-version", API_VERSION.to_string()),
        ];
        headers.extend(extra.iter().cloned());
        let mut parts: Vec<String> = query
            .iter()
            .map(|(k, v)| format!("{k}={}", encode_query_value(v)))
            .collect();
        match &self.credential {
            Credential::Key(key) => {
                // The canonicalized resource is the full URI path: a BlobEndpoint with a
                // path (the Azurite emulator: `/devstoreaccount1`) is part of it.
                let full_path = format!("{}{path}", self.endpoint.path);
                let sts =
                    string_to_sign(&self.account, method, &full_path, query, &headers, body_len);
                let sig = crate::b64::encode(
                    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), sts.as_bytes()).as_ref(),
                );
                headers.push(("Authorization", format!("SharedKey {}:{sig}", self.account)));
            }
            // The SAS arrives URL-encoded in the connection string: append it verbatim.
            Credential::Sas(sas) => parts.push(sas.clone()),
        }
        let query_string = if parts.is_empty() {
            String::new()
        } else {
            format!("?{}", parts.join("&"))
        };
        (headers, query_string)
    }

    /// Sends one signed request with retries on transient failures; returns the response
    /// to `handle` (which may stream its body).
    fn call<T>(
        &self,
        client: &mut Client,
        req: Req<'_>,
        mut handle: impl FnMut(crate::http::Response<'_>) -> Result<T>,
    ) -> Result<T> {
        let Req {
            method,
            path,
            query,
            headers: extra,
            body,
        } = req;
        let mut last = None;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                std::thread::sleep(backoff(attempt));
            }
            let (headers, query_string) =
                self.signed(method, path, query, extra, crate::util::u64_of(body.len()));
            let hdrs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            match client.request(method, &format!("{path}{query_string}"), &hdrs, body) {
                Ok(resp) if resp.status >= 500 || resp.status == 408 || resp.status == 429 => {
                    last = Some(crate::err!("{method} {path}: HTTP {}", resp.status));
                    let _ = resp.finish();
                }
                Ok(resp) => return handle(resp),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| crate::err!("{method} {path} failed")))
    }

    /// The edge's async client for this account (connections pooled; made on first use).
    fn aclient(&self) -> &crate::aclient::AClient {
        self.aclient
            .get_or_init(|| Box::new(crate::aclient::AClient::new(self.endpoint.clone(), TIMEOUT)))
    }

    /// [`AzureStore::read_range`] on the async client: one ranged GET (retried on transient
    /// failures before any byte reached `sink`), streamed to `sink`.
    pub async fn read_range_async<S: crate::store::Sink>(
        &self,
        key: &str,
        offset: u64,
        len: u64,
        sink: &mut S,
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let blob = self.blob_path(key)?;
        let last = offset.saturating_add(len).saturating_sub(1);
        let extra = [("x-ms-range", format!("bytes={offset}-{last}"))];
        let mut failed = None;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(backoff(attempt)).await;
            }
            let (headers, query_string) = self.signed("GET", &blob, &[], &extra, 0);
            let hdrs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            let path = format!("{blob}{query_string}");
            let resp = match self
                .aclient()
                .request("GET", &path, &hdrs, bytes::Bytes::new())
                .await
            {
                Ok(r) if r.status >= 500 || r.status == 408 || r.status == 429 => {
                    failed = Some(crate::err!("GET {blob}: HTTP {}", r.status));
                    continue;
                }
                Ok(r) => r,
                Err(e) => {
                    failed = Some(e);
                    continue;
                }
            };
            let mut resp = resp.ensure_ok(&format!("ranged GET {key}")).await?;
            let mut got = 0u64;
            // The sink's work (a block written to the cache) is the worker's: yield as it
            // goes, as the edge's handlers do.
            let mut budget = crate::serve::budget::Budget::new();
            while let Some(chunk) = resp.chunk().await.ctx(&format!("reading {key}"))? {
                got = got.saturating_add(crate::util::u64_of(chunk.len()));
                if got > len {
                    crate::bail!("ranged GET {key}: more bytes than asked for");
                }
                sink.put(&chunk).await?;
                if budget.spend(crate::serve::budget::byte_units(chunk.len())) {
                    budget.pause().await;
                }
            }
            if got != len {
                crate::bail!("ranged GET {key}: {got} of {len} bytes");
            }
            return Ok(());
        }
        Err(failed.unwrap_or_else(|| crate::err!("GET {blob} failed")))
    }

    pub fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let path = self.blob_path(key)?;
        let mut c = self.client();
        self.call(
            &mut c,
            Req {
                method: "GET",
                path: &path,
                query: &[],
                headers: &[],
                body: b"",
            },
            |resp| {
                if resp.status == 404 {
                    let _ = resp.finish();
                    return Ok(None);
                }
                let mut resp = resp.ensure_ok(&format!("GET {key}"))?;
                let etag = resp.header("etag").unwrap_or("").to_string();
                let body = resp.body_vec(1 << 30)?;
                Ok(Some((body, etag)))
            },
        )
    }

    pub fn put(&self, key: &str, data: &[u8], cond: &Cond) -> Result<String> {
        let path = self.blob_path(key)?;
        let mut extra = vec![("x-ms-blob-type", "BlockBlob".to_string())];
        match cond {
            Cond::None => {}
            Cond::IfNoneMatch => extra.push(("If-None-Match", "*".to_string())),
            Cond::IfMatch(e) => extra.push(("If-Match", e.clone())),
        }
        let mut c = self.client();
        self.call(
            &mut c,
            Req {
                method: "PUT",
                path: &path,
                query: &[],
                headers: &extra,
                body: data,
            },
            |resp| {
                if resp.status == 412 || resp.status == 409 {
                    let _ = resp.finish();
                    return Err(Error::with_kind(
                        ErrorKind::Conflict,
                        format!("{key}: precondition failed"),
                    ));
                }
                let resp = resp.ensure_ok(&format!("PUT {key}"))?;
                let etag = resp.header("etag").unwrap_or("").to_string();
                resp.finish()?;
                Ok(etag)
            },
        )
    }

    pub fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let file = std::fs::File::open(path).ctx(&format!("opening {}", path.display()))?;
        let size = file.metadata()?.len();
        if size <= SINGLE_PUT_MAX {
            let mut data = Vec::with_capacity(crate::util::us(size));
            (&file).read_to_end(&mut data)?;
            self.put(key, &data, &Cond::None)?;
            return Ok(());
        }
        let blob = self.blob_path(key)?;
        let blocks = size.div_ceil(BLOCK);
        let next = AtomicU64::new(0);
        std::thread::scope(|s| -> Result<()> {
            let workers: Vec<_> = (0..PARALLEL)
                .map(|_| {
                    s.spawn(|| -> Result<()> {
                        let mut c = self.client();
                        let mut buf = vec![0u8; crate::util::us(BLOCK)];
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= blocks {
                                return Ok(());
                            }
                            let off = i.saturating_mul(BLOCK);
                            let len = crate::util::us(BLOCK.min(size.saturating_sub(off)));
                            let chunk = buf.get_mut(..len).ctx("block buffer")?;
                            file.read_exact_at(chunk, off)?;
                            self.put_block(&mut c, &blob, i, chunk)?;
                        }
                    })
                })
                .collect();
            for w in workers {
                w.join()
                    .map_err(|_| crate::err!("upload worker failed"))??;
            }
            Ok(())
        })?;
        self.put_block_list(&blob, blocks)
    }

    /// Stages block `i` of the blob at `blob` (a [`AzureStore::blob_path`]).
    fn put_block(&self, c: &mut Client, blob: &str, i: u64, chunk: &[u8]) -> Result<()> {
        let q = [("comp", "block".to_string()), ("blockid", block_id(i))];
        self.call(
            c,
            Req {
                method: "PUT",
                path: blob,
                query: &q,
                headers: &[],
                body: chunk,
            },
            |r| r.ensure_ok("Put Block")?.finish(),
        )
    }

    /// Commits blocks `0..blocks`: the blob appears, whole, atomically.
    fn put_block_list(&self, blob: &str, blocks: u64) -> Result<()> {
        let mut list = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for i in 0..blocks {
            list.push_str(&format!("<Latest>{}</Latest>", block_id(i)));
        }
        list.push_str("</BlockList>");
        let mut c = self.client();
        self.call(
            &mut c,
            Req {
                method: "PUT",
                path: blob,
                query: &[("comp", "blocklist".to_string())],
                headers: &[],
                body: list.as_bytes(),
            },
            |r| r.ensure_ok("Put Block List")?.finish(),
        )
    }

    /// A streaming upload of `key`: blocks are staged as they fill, and the blob appears
    /// (atomically) at [`AzureWriter::finish`]. Memory: one block.
    pub fn writer(&self, key: &str) -> Result<AzureWriter<'_>> {
        Ok(AzureWriter {
            key: key.to_string(),
            blob: self.blob_path(key)?,
            client: self.client(),
            buf: Vec::with_capacity(crate::util::us(BLOCK)),
            blocks: 0,
            store: self,
        })
    }

    /// One ranged GET of `len` bytes from `offset`, streamed to `sink`.
    pub fn read_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let blob = self.blob_path(key)?;
        let last = offset.saturating_add(len).saturating_sub(1);
        let h = [("x-ms-range", format!("bytes={offset}-{last}"))];
        let mut c = self.client();
        self.call(
            &mut c,
            Req {
                method: "GET",
                path: &blob,
                query: &[],
                headers: &h,
                body: b"",
            },
            |resp| {
                let mut resp = resp.ensure_ok(&format!("ranged GET {key}"))?;
                let mut buf = vec![0u8; 256 << 10];
                let mut got = 0u64;
                loop {
                    let n = resp.read(&mut buf).ctx(&format!("reading {key}"))?;
                    if n == 0 {
                        break;
                    }
                    got = got.saturating_add(crate::util::u64_of(n));
                    if got > len {
                        crate::bail!("ranged GET {key}: more bytes than asked for");
                    }
                    sink(buf.get(..n).unwrap_or(&[]))?;
                }
                if got != len {
                    crate::bail!("ranged GET {key}: {got} of {len} bytes");
                }
                Ok(())
            },
        )
    }

    pub fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        let blob = self.blob_path(key)?;
        let tmp = crate::fsutil::temp_path(path);
        let result = (|| -> Result<()> {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let file = std::fs::File::create(&tmp)?;
            // First range tells the total size.
            let mut c = self.client();
            let first = |range_end: u64| {
                (
                    "x-ms-range",
                    format!("bytes=0-{}", range_end.saturating_sub(1)),
                )
            };
            let total = self.call(
                &mut c,
                Req {
                    method: "GET",
                    path: &blob,
                    query: &[],
                    headers: &[first(BLOCK)],
                    body: b"",
                },
                |resp| {
                    let mut resp = resp.ensure_ok(&format!("GET {key}"))?;
                    let total = resp
                        .header("content-range")
                        .and_then(|r| r.rsplit('/').next())
                        .and_then(|t| t.parse::<u64>().ok())
                        .or_else(|| resp.header("content-length").and_then(|l| l.parse().ok()))
                        .ctx("no content length")?;
                    let body = resp.body_vec(crate::util::us(BLOCK).saturating_add(1))?;
                    file.write_all_at(&body, 0)?;
                    Ok(total)
                },
            )?;
            let ranges = total.div_ceil(BLOCK);
            let next = AtomicU64::new(1);
            std::thread::scope(|s| -> Result<()> {
                let workers: Vec<_> = (0..PARALLEL)
                    .map(|_| {
                        s.spawn(|| -> Result<()> {
                            let mut c = self.client();
                            loop {
                                let i = next.fetch_add(1, Ordering::Relaxed);
                                if i >= ranges {
                                    return Ok(());
                                }
                                let off = i.saturating_mul(BLOCK);
                                let end = off.saturating_add(BLOCK).min(total);
                                let h = [(
                                    "x-ms-range",
                                    format!("bytes={off}-{}", end.saturating_sub(1)),
                                )];
                                let body = self.call(
                                    &mut c,
                                    Req {
                                        method: "GET",
                                        path: &blob,
                                        query: &[],
                                        headers: &h,
                                        body: b"",
                                    },
                                    |r| {
                                        r.ensure_ok("ranged GET")?
                                            .body_vec(crate::util::us(BLOCK).saturating_add(1))
                                    },
                                )?;
                                if crate::util::u64_of(body.len()) != end.saturating_sub(off) {
                                    crate::bail!("short ranged read of {key}");
                                }
                                file.write_all_at(&body, off)?;
                            }
                        })
                    })
                    .collect();
                for w in workers {
                    w.join()
                        .map_err(|_| crate::err!("download worker failed"))??;
                }
                Ok(())
            })?;
            file.sync_all()?;
            std::fs::rename(&tmp, path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result.ctx(&format!("downloading {key}"))
    }

    pub fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let path = format!("/{}", self.container);
        let full_prefix = format!("{}{prefix}", self.prefix);
        let mut out = Vec::new();
        let mut marker = String::new();
        let mut c = self.client();
        loop {
            let mut q = vec![
                ("restype", "container".to_string()),
                ("comp", "list".to_string()),
                ("prefix", full_prefix.clone()),
            ];
            if !marker.is_empty() {
                q.push(("marker", marker.clone()));
            }
            let body = self.call(
                &mut c,
                Req {
                    method: "GET",
                    path: &path,
                    query: &q,
                    headers: &[],
                    body: b"",
                },
                |r| r.ensure_ok("List Blobs")?.body_vec(64 << 20),
            )?;
            let xml = String::from_utf8_lossy(&body);
            for blob in xml.split("<Blob>").skip(1) {
                let name = tag(blob, "Name").unwrap_or_default();
                let Some(key) = name.strip_prefix(self.prefix.as_str()) else {
                    continue;
                };
                out.push(ObjectInfo {
                    key: key.to_string(),
                    size: tag(blob, "Content-Length")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0),
                    modified: tag(blob, "Last-Modified")
                        .as_deref()
                        .and_then(parse_http_date)
                        .unwrap_or(0),
                });
            }
            marker = tag(&xml, "NextMarker").unwrap_or_default();
            if marker.is_empty() {
                return Ok(out);
            }
        }
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        let path = self.blob_path(key)?;
        let mut c = self.client();
        self.call(
            &mut c,
            Req {
                method: "DELETE",
                path: &path,
                query: &[],
                headers: &[],
                body: b"",
            },
            |resp| {
                if resp.status == 404 {
                    return resp.finish();
                }
                resp.ensure_ok(&format!("DELETE {key}"))?.finish()
            },
        )
    }
}

/// Text of the first `<name>...</name>` element.
fn block_id(i: u64) -> String {
    crate::b64::encode(format!("{i:016}").as_bytes())
}

/// See [`AzureStore::writer`].
pub struct AzureWriter<'a> {
    store: &'a AzureStore,
    key: String,
    blob: String,
    client: Client,
    buf: Vec<u8>,
    blocks: u64,
}

impl AzureWriter<'_> {
    fn stage(&mut self) -> Result<()> {
        let buf = std::mem::take(&mut self.buf);
        let staged = self
            .store
            .put_block(&mut self.client, &self.blob, self.blocks, &buf);
        self.buf = buf;
        self.buf.clear();
        staged?;
        self.blocks = self.blocks.saturating_add(1);
        Ok(())
    }

    /// Stages the rest and commits the block list.
    pub fn finish(mut self) -> Result<()> {
        if self.blocks == 0 {
            self.store.put(&self.key, &self.buf, &Cond::None)?;
            return Ok(());
        }
        if !self.buf.is_empty() {
            self.stage()?;
        }
        self.store.put_block_list(&self.blob, self.blocks)
    }
}

impl std::io::Write for AzureWriter<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let room = crate::util::us(BLOCK).saturating_sub(self.buf.len());
        let n = room.min(data.len());
        self.buf.extend_from_slice(data.get(..n).unwrap_or(&[]));
        if self.buf.len() >= crate::util::us(BLOCK) {
            self.stage()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn tag(xml: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let start = xml.find(&open)?.checked_add(open.len())?;
    let len = xml.get(start..)?.find(&format!("</{name}>"))?;
    Some(xml.get(start..start.checked_add(len)?)?.to_string())
}

/// `(days since 1970-01-01, year, month, day, seconds into the day)` of `t` (UTC).
fn civil(t: SystemTime) -> (i64, i64, i64, i64, u64) {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = i64::try_from(secs.wrapping_div(86_400)).unwrap_or(0);
    let rem = secs.wrapping_rem(86_400);
    // Civil date from days since 1970-01-01 (H. Hinnant's algorithm).
    let z = days.wrapping_add(719_468);
    let era = z.wrapping_div(146_097);
    let doe = z.wrapping_sub(era.wrapping_mul(146_097));
    let yoe = doe
        .wrapping_sub(doe.wrapping_div(1460))
        .wrapping_add(doe.wrapping_div(36_524))
        .wrapping_sub(doe.wrapping_div(146_096))
        .wrapping_div(365);
    let doy = doe.wrapping_sub(
        yoe.wrapping_mul(365)
            .wrapping_add(yoe.wrapping_div(4))
            .wrapping_sub(yoe.wrapping_div(100)),
    );
    let mp = doy.wrapping_mul(5).wrapping_add(2).wrapping_div(153);
    let day = doy
        .wrapping_sub(mp.wrapping_mul(153).wrapping_add(2).wrapping_div(5))
        .wrapping_add(1);
    let month = if mp < 10 {
        mp.wrapping_add(3)
    } else {
        mp.wrapping_sub(9)
    };
    let year = yoe
        .wrapping_add(era.wrapping_mul(400))
        .wrapping_add(i64::from(month <= 2));
    (days, year, month, day, rem)
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// `Sun, 06 Nov 1994 08:49:37 GMT` (RFC 1123, as `x-ms-date` requires).
pub fn http_date(t: SystemTime) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    let (days, year, month, day, rem) = civil(t);
    let wd = DAYS
        .get(usize::try_from(days.rem_euclid(7)).unwrap_or(0))
        .copied()
        .unwrap_or("Thu");
    let mon = MONTHS
        .get(usize::try_from(month.wrapping_sub(1)).unwrap_or(0))
        .copied()
        .unwrap_or("Jan");
    format!(
        "{wd}, {day:02} {mon} {year:04} {:02}:{:02}:{:02} GMT",
        rem.wrapping_div(3600),
        rem.wrapping_rem(3600).wrapping_div(60),
        rem.wrapping_rem(60)
    )
}

/// Unix seconds of an RFC 1123 date.
pub fn parse_http_date(s: &str) -> Option<u64> {
    let mut parts = s.split_whitespace().skip(1);
    let day: i64 = parts.next()?.parse().ok()?;
    let mon = MONTHS
        .iter()
        .position(|m| Some(*m) == parts.clone().next())?;
    parts.next();
    let year: i64 = parts.next()?.parse().ok()?;
    let mut hms = parts.next()?.split(':').map(|v| v.parse::<i64>().ok());
    let (h, m, sec) = (hms.next()??, hms.next()??, hms.next()??);
    // Days from civil (inverse of `civil`).
    let month = i64::try_from(mon).ok()?.wrapping_add(1);
    let y = if month <= 2 {
        year.wrapping_sub(1)
    } else {
        year
    };
    let era = y.div_euclid(400);
    let yoe = y.wrapping_sub(era.wrapping_mul(400));
    let mp = if month > 2 {
        month.wrapping_sub(3)
    } else {
        month.wrapping_add(9)
    };
    let doy = mp
        .wrapping_mul(153)
        .wrapping_add(2)
        .wrapping_div(5)
        .wrapping_add(day)
        .wrapping_sub(1);
    let doe = yoe
        .wrapping_mul(365)
        .wrapping_add(yoe.wrapping_div(4))
        .wrapping_sub(yoe.wrapping_div(100))
        .wrapping_add(doy);
    let days = era
        .wrapping_mul(146_097)
        .wrapping_add(doe)
        .wrapping_sub(719_468);
    let t = days
        .wrapping_mul(86_400)
        .wrapping_add(h.wrapping_mul(3600))
        .wrapping_add(m.wrapping_mul(60))
        .wrapping_add(sec);
    u64::try_from(t).ok()
}

/// Percent-encodes a query value (everything but RFC 3986 unreserved characters).
fn encode_query_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for b in v.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Shared Key string-to-sign for the Blob service (versions 2015-02-21 and later).
pub fn string_to_sign(
    account: &str,
    method: &str,
    path: &str,
    query: &[(&str, String)],
    headers: &[(&str, String)],
    content_length: u64,
) -> String {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    };
    let mut s = String::with_capacity(256);
    s.push_str(method);
    s.push('\n');
    for h in ["Content-Encoding", "Content-Language"] {
        s.push_str(get(h));
        s.push('\n');
    }
    if content_length > 0 {
        s.push_str(&content_length.to_string());
    }
    s.push('\n');
    for h in [
        "Content-MD5",
        "Content-Type",
        "Date",
        "If-Modified-Since",
        "If-Match",
        "If-None-Match",
        "If-Unmodified-Since",
        "Range",
    ] {
        s.push_str(get(h));
        s.push('\n');
    }
    let mut ms: Vec<(String, &str)> = headers
        .iter()
        .filter(|(k, _)| k.to_ascii_lowercase().starts_with("x-ms-"))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim()))
        .collect();
    ms.sort();
    for (k, v) in ms {
        s.push_str(&k);
        s.push(':');
        s.push_str(v);
        s.push('\n');
    }
    s.push('/');
    s.push_str(account);
    s.push_str(path);
    let mut q: Vec<(String, &str)> = query
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.as_str()))
        .collect();
    q.sort();
    for (k, v) in q {
        s.push('\n');
        s.push_str(&k);
        s.push(':');
        s.push_str(v);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_roundtrip() {
        let t = UNIX_EPOCH + Duration::from_secs(1_792_862_110);
        let d = http_date(t);
        assert_eq!(d, "Sat, 24 Oct 2026 17:15:10 GMT");
        assert_eq!(parse_http_date(&d), Some(1_792_862_110));
        assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
    }

    #[test]
    fn shared_key_string_to_sign_matches_reference() {
        let headers = [
            ("x-ms-date", "Sat, 24 Oct 2026 16:32:39 GMT".to_string()),
            ("x-ms-version", API_VERSION.to_string()),
            ("x-ms-blob-type", "BlockBlob".to_string()),
            ("If-None-Match", "*".to_string()),
        ];
        let s = string_to_sign(
            "acct",
            "PUT",
            "/c/log/00000000000000000001.rec",
            &[],
            &headers,
            4096,
        );
        assert_eq!(
            s,
            "PUT\n\n\n4096\n\n\n\n\n\n*\n\n\nx-ms-blob-type:BlockBlob\nx-ms-date:Sat, 24 Oct 2026 16:32:39 GMT\nx-ms-version:2021-08-06\n/acct/c/log/00000000000000000001.rec"
        );
    }

    #[test]
    fn connection_strings_and_xml() {
        let a = AzureStore::new(
            "AccountName=acct;AccountKey=a2V5;EndpointSuffix=core.windows.net",
            "gvfs/repo",
        )
        .unwrap();
        assert_eq!(
            (
                a.account.as_str(),
                a.endpoint.host.as_str(),
                a.prefix.as_str()
            ),
            ("acct", "acct.blob.core.windows.net", "repo/")
        );
        let s = AzureStore::new(
            "BlobEndpoint=https://x.blob.core.windows.net;SharedAccessSignature=sv=1&sig=a%2Bb",
            "gvfs",
        )
        .unwrap();
        assert_eq!(s.account, "x");
        assert!(AzureStore::new("AccountName=a", "gvfs").is_err());
        assert!(AzureStore::new("AccountName=a;AccountKey=a2V5", "Bad_Name").is_err());
        assert_eq!(
            tag("<a><Name>slabs/x.pack</Name></a>", "Name").as_deref(),
            Some("slabs/x.pack")
        );
    }
}
