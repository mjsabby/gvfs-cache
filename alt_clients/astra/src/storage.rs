use crate::{Error, Result};
mod multipart;
#[cfg(test)]
mod tests;
use fs2::FileExt;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const MANIFEST_LIMIT: u64 = 1 << 20;
const MULTIPART_THRESHOLD: u64 = 256 << 20;
const PART_BYTES: usize = 64 << 20;

#[derive(Clone)]
pub enum Storage {
    Fs(PathBuf),
    Http(Arc<HttpStore>),
}

pub struct HttpStore {
    base: ureq::http::Uri,
    query: Option<String>,
    auth: Option<String>,
    s3: Option<S3>,
    azure: bool,
    agent: ureq::Agent,
}

struct S3 {
    access: String,
    secret: String,
    token: Option<String>,
    region: String,
}

fn env(name: &str) -> Result<String> {
    std::env::var(name)
        .map_err(|_| Error::Invalid("required storage credential environment variable is missing"))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = vec![0u8; bytes.len().saturating_mul(2)];
    crate::oid::hex_encode_into(bytes, &mut out);
    // The encoder emits ASCII.
    String::from_utf8_lossy(&out).into_owned()
}

fn hmac(key: &[u8], bytes: &[u8]) -> Result<[u8; 32]> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| Error::Invalid("HMAC key"))?;
    mac.update(bytes);
    Ok(mac.finalize().into_bytes().into())
}

impl HttpStore {
    fn url(&self, key: &str) -> String {
        let mut url = format!("{}/{}", self.base.to_string().trim_end_matches('/'), key);
        if let Some(query) = &self.query {
            url.push('?');
            url.push_str(query.trim_start_matches('?'));
        }
        url
    }

    fn request(&self, method: &str, url: &str) -> Result<ureq::http::request::Builder> {
        let mut request = ureq::http::Request::builder().method(method).uri(url);
        if let Some(auth) = &self.auth {
            request = request.header("Authorization", auth);
        }
        if self.azure {
            request = request.header("x-ms-version", "2023-11-03");
        }
        if let Some(s3) = &self.s3 {
            let now = time::OffsetDateTime::now_utc();
            let date = now
                .format(time::macros::format_description!("[year][month][day]"))
                .map_err(|_| Error::Invalid("signing date"))?;
            let timestamp = now
                .format(time::macros::format_description!(
                    "[year][month][day]T[hour][minute][second]Z"
                ))
                .map_err(|_| Error::Invalid("signing date"))?;
            let uri: ureq::http::Uri = url.parse().map_err(|_| Error::Invalid("storage URL"))?;
            let host = uri
                .authority()
                .ok_or(Error::Invalid("storage host"))?
                .as_str();
            let mut names = "host;x-amz-content-sha256;x-amz-date".to_owned();
            let mut headers = format!(
                "host:{host}\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:{timestamp}\n"
            );
            if let Some(token) = &s3.token {
                names.push_str(";x-amz-security-token");
                headers.push_str(&format!("x-amz-security-token:{token}\n"));
                request = request.header("x-amz-security-token", token);
            }
            let mut query: Vec<&str> = uri
                .query()
                .unwrap_or("")
                .split('&')
                .filter(|p| !p.is_empty())
                .collect();
            query.sort_unstable();
            let canonical = format!(
                "{method}\n{}\n{}\n{headers}\n{names}\nUNSIGNED-PAYLOAD",
                uri.path(),
                query.join("&")
            );
            let scope = format!("{date}/{}/s3/aws4_request", s3.region);
            let to_sign = format!(
                "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
                hex(&Sha256::digest(canonical.as_bytes()))
            );
            let date_key = hmac(format!("AWS4{}", s3.secret).as_bytes(), date.as_bytes())?;
            let region_key = hmac(&date_key, s3.region.as_bytes())?;
            let service_key = hmac(&region_key, b"s3")?;
            let signing_key = hmac(&service_key, b"aws4_request")?;
            let signature = hex(&hmac(&signing_key, to_sign.as_bytes())?);
            request = request.header("x-amz-date", timestamp)
                .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                .header("Authorization", format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={signature}", s3.access));
        }
        Ok(request)
    }
}

impl Storage {
    pub fn open(spec: &str, query: Option<String>, auth: Option<String>) -> Result<Self> {
        let (url, azure, s3) = if let Some(url) = spec.strip_prefix("s3:") {
            (
                url,
                false,
                Some(S3 {
                    access: env("AWS_ACCESS_KEY_ID")?,
                    secret: env("AWS_SECRET_ACCESS_KEY")?,
                    token: std::env::var("AWS_SESSION_TOKEN").ok(),
                    region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
                }),
            )
        } else if let Some(url) = spec.strip_prefix("azure:") {
            (url, true, None)
        } else {
            (spec, false, None)
        };
        if url.starts_with("https://") || url.starts_with("http://") {
            let base: ureq::http::Uri = url.parse().map_err(|_| Error::Invalid("storage URL"))?;
            if base.authority().is_none_or(|a| a.as_str().contains('@'))
                || base.query().is_some()
                || url.contains('#')
            {
                return Err(Error::Invalid(
                    "put storage credentials in environment variables, not the URL",
                ));
            }
            if s3.is_some() && (query.is_some() || base.scheme_str() != Some("https")) {
                return Err(Error::Invalid(
                    "S3 signing requires HTTPS without query parameters",
                ));
            }
            let config = ureq::Agent::config_builder()
                .http_status_as_error(false)
                .max_redirects(0)
                .timeout_global(Some(Duration::from_secs(300)))
                .build();
            return Ok(Self::Http(Arc::new(HttpStore {
                base,
                query,
                auth,
                s3,
                azure,
                agent: config.into(),
            })));
        }
        let root = PathBuf::from(spec.strip_prefix("fs:").unwrap_or(spec));
        fs::create_dir_all(&root)?;
        Ok(Self::Fs(root))
    }

    pub fn manifest(&self) -> Result<Option<(Vec<u8>, String)>> {
        match self {
            Self::Fs(root) => {
                let path = root.join("manifest.bin");
                let file = match File::open(path) {
                    Ok(file) => file,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
                if file.metadata()?.len() > MANIFEST_LIMIT {
                    return Err(Error::Capacity("manifest"));
                }
                let mut bytes = Vec::new();
                file.take(MANIFEST_LIMIT.saturating_add(1))
                    .read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MANIFEST_LIMIT {
                    return Err(Error::Capacity("manifest"));
                }
                let etag = hex(&crate::sha1_parts(&[&bytes]));
                Ok(Some((bytes, etag)))
            }
            Self::Http(store) => {
                let req = store
                    .request("GET", &store.url("manifest.bin"))?
                    .body(())
                    .map_err(|_| Error::Invalid("HTTP request"))?;
                let mut response = store.agent.run(req)?;
                if response.status().as_u16() == 404 {
                    return Ok(None);
                }
                status(response.status().as_u16(), 200)?;
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|h| h.to_str().ok())
                    .ok_or(Error::Invalid("manifest response requires ETag"))?
                    .to_owned();
                let mut bytes = Vec::new();
                response
                    .body_mut()
                    .as_reader()
                    .take(MANIFEST_LIMIT.saturating_add(1))
                    .read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MANIFEST_LIMIT {
                    return Err(Error::Capacity("manifest"));
                }
                Ok(Some((bytes, etag)))
            }
        }
    }

    pub fn publish_manifest(&self, bytes: &[u8], expected: Option<&str>) -> Result<()> {
        match self {
            Self::Fs(root) => {
                let lock = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(root.join("manifest.lock"))?;
                lock.lock_exclusive()?;
                let current = self.manifest()?;
                if current.as_ref().map(|(_, e)| e.as_str()) != expected {
                    return Err(Error::Conflict);
                }
                let mut temp = tempfile::NamedTempFile::new_in(root)?;
                temp.write_all(bytes)?;
                temp.as_file().sync_all()?;
                temp.persist(root.join("manifest.bin"))
                    .map_err(|e| Error::Io(e.error))?;
                File::open(root)?.sync_all()?;
                Ok(())
            }
            Self::Http(store) => {
                let mut request = store.request("PUT", &store.url("manifest.bin"))?;
                request = if let Some(etag) = expected {
                    request.header("If-Match", etag)
                } else {
                    request.header("If-None-Match", "*")
                };
                if store.azure {
                    request = request.header("x-ms-blob-type", "BlockBlob");
                }
                let response = store.agent.run(
                    request
                        .body(bytes)
                        .map_err(|_| Error::Invalid("HTTP request"))?,
                )?;
                match response.status().as_u16() {
                    200 | 201 => Ok(()),
                    409 | 412 => Err(Error::Conflict),
                    s => Err(Error::Http(s)),
                }
            }
        }
    }

    pub fn upload(&self, key: &str, path: &Path) -> Result<()> {
        check_key(key)?;
        match self {
            Self::Fs(root) => {
                let destination = root.join(key);
                if destination.exists() {
                    if digest_file(path)? != digest_file(&destination)? {
                        return Err(Error::Invalid(
                            "immutable artifact already exists with different bytes",
                        ));
                    }
                    return Ok(());
                }
                let mut temp = tempfile::NamedTempFile::new_in(root)?;
                std::io::copy(&mut File::open(path)?, &mut temp)?;
                temp.as_file().sync_all()?;
                match temp.persist_noclobber(&destination) {
                    Ok(_) => {}
                    Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if digest_file(path)? != digest_file(&destination)? {
                            return Err(Error::Invalid("immutable publication collision"));
                        }
                    }
                    Err(e) => return Err(Error::Io(e.error)),
                }
                File::open(root)?.sync_all()?;
                Ok(())
            }
            Self::Http(store) => {
                if fs::metadata(path)?.len() >= MULTIPART_THRESHOLD
                    && (store.azure || store.s3.is_some())
                {
                    match store.multipart(key, path, PART_BYTES) {
                        Err(Error::Conflict) => {
                            let mut temp = tempfile::NamedTempFile::new()?;
                            self.download(key, temp.as_file_mut(), fs::metadata(path)?.len())?;
                            if digest_file(temp.path())? != digest_file(path)? {
                                return Err(Error::Invalid("immutable multipart collision"));
                            }
                            return Ok(());
                        }
                        result => return result,
                    }
                }
                let mut request = store
                    .request("PUT", &store.url(key))?
                    .header("If-None-Match", "*");
                if store.azure {
                    request = request.header("x-ms-blob-type", "BlockBlob");
                }

                let response = store.agent.run(
                    request
                        .body(File::open(path)?)
                        .map_err(|_| Error::Invalid("HTTP request"))?,
                )?;
                match response.status().as_u16() {
                    200 | 201 => Ok(()),
                    // A content-addressed retry is safe, but verify the existing bytes.
                    409 | 412 => {
                        let mut temp = tempfile::NamedTempFile::new()?;
                        self.download(key, temp.as_file_mut(), fs::metadata(path)?.len())?;
                        if digest_file(temp.path())? != digest_file(path)? {
                            return Err(Error::Invalid("immutable remote artifact collision"));
                        }
                        Ok(())
                    }
                    s => Err(Error::Http(s)),
                }
            }
        }
    }

    pub fn download(&self, key: &str, output: &mut File, limit: u64) -> Result<()> {
        check_key(key)?;
        let written = match self {
            Self::Fs(root) => std::io::copy(
                &mut File::open(root.join(key))?.take(limit.saturating_add(1)),
                output,
            )?,
            Self::Http(store) => {
                let request = store
                    .request("GET", &store.url(key))?
                    .body(())
                    .map_err(|_| Error::Invalid("HTTP request"))?;
                let mut response = store.agent.run(request)?;
                status(response.status().as_u16(), 200)?;
                std::io::copy(
                    &mut response
                        .body_mut()
                        .as_reader()
                        .take(limit.saturating_add(1)),
                    output,
                )?
            }
        };
        if written > limit {
            return Err(Error::Capacity("download"));
        }
        output.sync_all()?;
        Ok(())
    }

    pub fn reader(&self, key: &str, len: u64) -> Result<RangeReader> {
        check_key(key)?;
        match self {
            Self::Fs(root) => {
                let file = File::open(root.join(key))?;
                if file.metadata()?.len() != len {
                    return Err(Error::Invalid("artifact length"));
                }
                Ok(RangeReader::File(file))
            }
            Self::Http(store) => Ok(RangeReader::Http {
                store: Arc::clone(store),
                url: store.url(key),
                len,
            }),
        }
    }
}

pub enum RangeReader {
    File(File),
    Memory(crate::arena::Mapping),
    Http {
        store: Arc<HttpStore>,
        url: String,
        len: u64,
    },
}

impl RangeReader {
    pub fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        match self {
            Self::File(file) => {
                #[cfg(unix)]
                {
                    std::os::unix::fs::FileExt::read_exact_at(file, out, offset)?;
                }
                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    let mut pos = 0usize;
                    while pos < out.len() {
                        let n = file.seek_read(
                            out.get_mut(pos..).ok_or(Error::Invalid("range"))?,
                            offset.saturating_add(pos as u64),
                        )?;
                        if n == 0 {
                            return Err(Error::Invalid("short file range"));
                        }
                        pos = pos.saturating_add(n);
                    }
                }
                Ok(())
            }
            Self::Memory(map) => {
                let start = usize::try_from(offset).map_err(|_| Error::Invalid("range offset"))?;
                out.copy_from_slice(
                    map.bytes()
                        .get(start..start.saturating_add(out.len()))
                        .ok_or(Error::Invalid("memory range"))?,
                );
                Ok(())
            }
            Self::Http { store, url, len } => {
                if out.is_empty() {
                    return Ok(());
                }
                let end = offset
                    .checked_add(out.len() as u64)
                    .filter(|e| e <= len)
                    .ok_or(Error::Invalid("HTTP range bounds"))?;
                // The pooled HTTP/TLS implementation has its own connection allocations.
                // The engine's planning, buffers and local I/O remain inside the guard.
                let _transport = alloc_guard::cold_section();
                let range = format!("bytes={offset}-{}", end.saturating_sub(1));
                let request = store
                    .request("GET", url)?
                    .header("Range", range)
                    .body(())
                    .map_err(|_| Error::Invalid("HTTP request"))?;
                let mut response = store.agent.run(request)?;
                status(response.status().as_u16(), 206)?;
                let expected = format!("bytes {offset}-{}/{len}", end.saturating_sub(1));
                if response
                    .headers()
                    .get("content-range")
                    .and_then(|h| h.to_str().ok())
                    != Some(&expected)
                {
                    return Err(Error::Invalid(
                        "storage ignored or changed the requested range",
                    ));
                }
                let mut reader = response.body_mut().as_reader();
                reader.read_exact(out)?;
                if reader.read(&mut [0; 1])? != 0 {
                    return Err(Error::Invalid("long HTTP range"));
                }
                Ok(())
            }
        }
    }
}

fn check_key(key: &str) -> Result<()> {
    if key.is_empty()
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
        || key == "."
        || key == ".."
    {
        return Err(Error::Invalid("storage key"));
    }
    Ok(())
}
fn status(actual: u16, expected: u16) -> Result<()> {
    if actual != expected {
        Err(Error::Http(actual))
    } else {
        Ok(())
    }
}
fn digest_file(path: &Path) -> Result<[u8; 20]> {
    use sha1::Sha1;
    let mut file = File::open(path)?;
    let mut hash = Sha1::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(buffer.get(..n).ok_or(Error::Invalid("read bounds"))?);
    }
    Ok(hash.finalize().into())
}
