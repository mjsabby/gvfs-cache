//! Immutable artifacts + a single conditional manifest publication point.
//! Local publication uses fsync/rename under a process-shared lock. HTTP stores
//! use ETags; pack reads require an exact, validated 206 Content-Range response.

use std::convert::Infallible;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri};
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use ring::{digest, hmac};
use tokio::sync::Mutex;

use crate::codec::{Oid, checksum, encode_hex};
use crate::manifest::{Manifest, SlabRef};
use crate::slab::Slab;
use crate::{Error, Result};

static NONCE: AtomicU64 = AtomicU64::new(0);
const TIMEOUT: Duration = Duration::from_secs(60);
const MANIFEST_LIMIT: usize = 1 << 20;
const PART_BYTES: usize = 64 << 20;

#[derive(Clone)]
pub struct Endpoint {
    origin: String,
    prefix: String,
    query: String,
    host: String,
    https: bool,
}

impl Endpoint {
    pub fn parse(url: &str) -> Result<Self> {
        let uri: Uri = url
            .parse()
            .map_err(|_| Error::Usage("invalid HTTP endpoint".into()))?;
        let scheme = uri.scheme_str().ok_or(Error::Invalid("URL scheme"))?;
        if !matches!(scheme, "http" | "https") {
            return Err(Error::Invalid("HTTP URL scheme"));
        }
        let authority = uri.authority().ok_or(Error::Invalid("URL authority"))?;
        if authority.as_str().contains('@') {
            return Err(Error::Usage(
                "put credentials in environment variables, not URLs".into(),
            ));
        }
        Ok(Self {
            origin: format!("{scheme}://{authority}"),
            prefix: uri.path().trim_end_matches('/').to_owned(),
            query: uri.query().unwrap_or_default().to_owned(),
            host: authority.as_str().to_owned(),
            https: scheme == "https",
        })
    }

    fn uri(&self, key: &str, extra: &str) -> Result<Uri> {
        let separator = if self.query.is_empty() || extra.is_empty() {
            ""
        } else {
            "&"
        };
        let query = format!("{}{}{}", self.query, separator, extra);
        let query_mark = if query.is_empty() { "" } else { "?" };
        format!(
            "{}/{key}{query_mark}{query}",
            format_args!("{}{}", self.origin, self.prefix)
        )
        .parse()
        .map_err(|_| Error::Invalid("object URL"))
    }
}

#[derive(Default)]
pub struct RequestBody(Option<Bytes>);

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|bytes| Ok(Frame::data(bytes))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.as_ref().map_or(0, |bytes| bytes.len() as u64))
    }
}

enum Sender {
    H1(hyper::client::conn::http1::SendRequest<RequestBody>),
    H2(hyper::client::conn::http2::SendRequest<RequestBody>),
}

pub struct HttpClient {
    endpoint: Endpoint,
    tls: Arc<rustls::ClientConfig>,
    sender: Option<Sender>,
}

impl HttpClient {
    pub fn new(endpoint: Endpoint) -> Result<Self> {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Transport(e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            endpoint,
            tls: Arc::new(tls),
            sender: None,
        })
    }

    async fn connect(&mut self) -> Result<()> {
        let uri = self.endpoint.uri("", "")?;
        let host = uri.host().ok_or(Error::Invalid("HTTP host"))?;
        let port = uri
            .port_u16()
            .unwrap_or(if self.endpoint.https { 443 } else { 80 });
        let socket = tokio::time::timeout(TIMEOUT, tokio::net::TcpStream::connect((host, port)))
            .await
            .map_err(|_| Error::Transport("HTTP connect timed out".into()))??;
        socket.set_nodelay(true)?;
        if self.endpoint.https {
            let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                .map_err(|_| Error::Invalid("TLS server name"))?;
            let stream = tokio::time::timeout(
                TIMEOUT,
                tokio_rustls::TlsConnector::from(Arc::clone(&self.tls)).connect(name, socket),
            )
            .await
            .map_err(|_| Error::Transport("TLS handshake timed out".into()))??;
            if stream.get_ref().1.alpn_protocol() == Some(b"h2") {
                self.handshake_h2(TokioIo::new(stream)).await?;
            } else {
                self.handshake_h1(TokioIo::new(stream)).await?;
            }
        } else {
            self.handshake_h1(TokioIo::new(socket)).await?;
        }
        Ok(())
    }

    async fn handshake_h1<T>(&mut self, io: T) -> Result<()>
    where
        T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    {
        let (sender, connection) = hyper::client::conn::http1::handshake(io).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("HTTP/1 connection closed: {error}");
            }
        });
        self.sender = Some(Sender::H1(sender));
        Ok(())
    }

    async fn handshake_h2<T>(&mut self, io: T) -> Result<()>
    where
        T: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    {
        let (sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("HTTP/2 connection closed: {error}");
            }
        });
        self.sender = Some(Sender::H2(sender));
        Ok(())
    }

    pub async fn request(
        &mut self,
        method: Method,
        key: &str,
        query: &str,
        headers: HeaderMap,
        body: Option<Bytes>,
    ) -> Result<Response<Incoming>> {
        if self.sender.is_none() {
            self.connect().await?;
        }
        let mut request = Request::builder()
            .method(method)
            .uri(self.endpoint.uri(key, query)?)
            .header("host", &self.endpoint.host)
            .header(
                "user-agent",
                concat!("gvfs-daemon/", env!("CARGO_PKG_VERSION")),
            )
            .body(RequestBody(body))?;
        request.headers_mut().extend(headers);
        let response = tokio::time::timeout(TIMEOUT, async {
            match self
                .sender
                .as_mut()
                .ok_or(Error::Invalid("HTTP connection"))?
            {
                Sender::H1(sender) => {
                    sender.ready().await?;
                    Ok(sender.send_request(request).await?)
                }
                Sender::H2(sender) => {
                    sender.ready().await?;
                    Ok(sender.send_request(request).await?)
                }
            }
        })
        .await;
        match response {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => {
                self.sender = None;
                Err(error)
            }
            Err(_) => {
                self.sender = None;
                Err(Error::Transport("HTTP request timed out".into()))
            }
        }
    }
}

pub async fn next_data(body: &mut Incoming) -> Result<Option<Bytes>> {
    loop {
        let frame = tokio::time::timeout(
            TIMEOUT,
            std::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)),
        )
        .await
        .map_err(|_| Error::Transport("HTTP body stalled".into()))?;
        match frame {
            Some(Ok(frame)) => {
                if let Ok(bytes) = frame.into_data() {
                    return Ok(Some(bytes));
                }
            }
            Some(Err(error)) => return Err(error.into()),
            None => return Ok(None),
        }
    }
}

fn header(value: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(value).map_err(|_| Error::Invalid("HTTP header value"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreKind {
    Generic,
    Azure,
    S3,
}

pub struct AwsCredentials {
    access: String,
    secret: String,
    token: Option<String>,
    region: String,
}

impl AwsCredentials {
    pub fn from_env(region: String) -> Result<Self> {
        let get = |name| std::env::var(name).map_err(|_| Error::Usage(format!("missing {name}")));
        Ok(Self {
            access: get("AWS_ACCESS_KEY_ID")?,
            secret: get("AWS_SECRET_ACCESS_KEY")?,
            token: std::env::var("AWS_SESSION_TOKEN").ok(),
            region,
        })
    }

    fn sign(&self, method: &Method, uri: &Uri, headers: &mut HeaderMap, body: &[u8]) -> Result<()> {
        let now = time::OffsetDateTime::now_utc();
        let date = now
            .format(time::macros::format_description!("[year][month][day]"))
            .map_err(|_| Error::Invalid("AWS date"))?;
        let timestamp = now
            .format(time::macros::format_description!(
                "[year][month][day]T[hour][minute][second]Z"
            ))
            .map_err(|_| Error::Invalid("AWS timestamp"))?;
        let hash = hex_sha256(body);
        let host = uri.authority().ok_or(Error::Invalid("AWS host"))?.as_str();
        let mut canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{hash}\nx-amz-date:{timestamp}\n");
        let mut signed = String::from("host;x-amz-content-sha256;x-amz-date");
        if let Some(token) = &self.token {
            canonical_headers.push_str(&format!("x-amz-security-token:{token}\n"));
            signed.push_str(";x-amz-security-token");
            headers.insert("x-amz-security-token", header(token)?);
        }
        let mut query: Vec<&str> = uri
            .query()
            .unwrap_or_default()
            .split('&')
            .filter(|part| !part.is_empty())
            .collect();
        query.sort_unstable();
        let canonical = format!(
            "{method}\n{}\n{}\n{canonical_headers}\n{signed}\n{hash}",
            uri.path(),
            query.join("&")
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            hex_sha256(canonical.as_bytes())
        );
        let sign =
            |key: &[u8], data: &[u8]| hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data);
        let date_key = sign(format!("AWS4{}", self.secret).as_bytes(), date.as_bytes());
        let region_key = sign(date_key.as_ref(), self.region.as_bytes());
        let service_key = sign(region_key.as_ref(), b"s3");
        let signing_key = sign(service_key.as_ref(), b"aws4_request");
        let signature = sign(signing_key.as_ref(), string_to_sign.as_bytes());
        let mut encoded = [0; 64];
        encode_hex(signature.as_ref(), &mut encoded);
        let encoded = std::str::from_utf8(&encoded).map_err(|_| Error::Invalid("AWS signature"))?;
        headers.insert("x-amz-date", header(&timestamp)?);
        headers.insert("x-amz-content-sha256", header(&hash)?);
        headers.insert("authorization", header(&format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={encoded}", self.access))?);
        Ok(())
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let hash = digest::digest(&digest::SHA256, bytes);
    let mut out = [0; 64];
    encode_hex(hash.as_ref(), &mut out);
    String::from_utf8_lossy(&out).into_owned()
}

// Constructed once at boot; keeping the variant inline avoids another owner box.
#[allow(clippy::large_enum_variant)]
enum Backend {
    Local(PathBuf),
    Http {
        endpoint: Endpoint,
        clients: Vec<Mutex<HttpClient>>,
        next: AtomicUsize,
        auth: Option<HeaderValue>,
        kind: StoreKind,
        aws: Option<AwsCredentials>,
    },
}

pub struct Storage {
    backend: Backend,
    cache: PathBuf,
    range_bytes: usize,
    maximum_pack: usize,
}

impl Storage {
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        location: &str,
        cache: PathBuf,
        range_bytes: usize,
        maximum_pack: usize,
        kind: StoreKind,
        auth_env: Option<&str>,
        aws_region: Option<String>,
        connections: usize,
    ) -> Result<Self> {
        if range_bytes == 0 || connections == 0 {
            return Err(Error::Usage(
                "range size and storage connections must be positive".into(),
            ));
        }
        std::fs::create_dir_all(&cache)?;
        let backend = if location.starts_with("http://") || location.starts_with("https://") {
            let endpoint = Endpoint::parse(location)?;
            let mut clients = Vec::new();
            for _ in 0..connections {
                clients.push(Mutex::new(HttpClient::new(endpoint.clone())?));
            }
            let auth = auth_env
                .map(|env| {
                    std::env::var(env)
                        .map_err(|_| Error::Usage(format!("missing {env}")))
                        .and_then(|value| header(&value))
                })
                .transpose()?;
            let aws = aws_region.map(AwsCredentials::from_env).transpose()?;
            Backend::Http {
                endpoint,
                clients,
                next: AtomicUsize::new(0),
                auth,
                kind,
                aws,
            }
        } else {
            let root = PathBuf::from(location.strip_prefix("file://").unwrap_or(location));
            std::fs::create_dir_all(root.join("slabs"))?;
            Backend::Local(root)
        };
        Ok(Self {
            backend,
            cache,
            range_bytes,
            maximum_pack,
        })
    }

    pub fn range_bytes(&self) -> usize {
        self.range_bytes
    }
    pub fn cache_dir(&self) -> &Path {
        &self.cache
    }
    pub fn is_local(&self) -> bool {
        matches!(self.backend, Backend::Local(_))
    }

    async fn request(
        &self,
        method: Method,
        key: &str,
        query: &str,
        mut headers: HeaderMap,
        body: Option<Bytes>,
    ) -> Result<Response<Incoming>> {
        let Backend::Http {
            endpoint,
            clients,
            next,
            auth,
            kind,
            aws,
        } = &self.backend
        else {
            return Err(Error::Invalid("HTTP operation on local storage"));
        };
        if let Some(auth) = auth {
            headers.insert("authorization", auth.clone());
        }
        if *kind == StoreKind::Azure {
            headers.insert("x-ms-version", HeaderValue::from_static("2023-11-03"));
        }
        if let Some(aws) = aws {
            aws.sign(
                &method,
                &endpoint.uri(key, query)?,
                &mut headers,
                body.as_deref().unwrap_or_default(),
            )?;
        }
        let index = next
            .fetch_add(1, Ordering::Relaxed)
            .checked_rem(clients.len())
            .ok_or(Error::Invalid("empty storage pool"))?;
        let mut client = clients
            .get(index)
            .ok_or(Error::Invalid("storage connection index"))?
            .lock()
            .await;
        client.request(method, key, query, headers, body).await
    }

    pub async fn manifest(&self) -> Result<Option<(Manifest, String)>> {
        match &self.backend {
            Backend::Local(root) => {
                let bytes = match read_small_file(&root.join("manifest.bin"), MANIFEST_LIMIT) {
                    Ok(bytes) => bytes,
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                };
                if bytes.len() > MANIFEST_LIMIT {
                    return Err(Error::Capacity("manifest file"));
                }
                Ok(Some((
                    Manifest::decode(&bytes)?,
                    Oid(checksum(&bytes)).to_string(),
                )))
            }
            Backend::Http { .. } => {
                let response = self
                    .request(Method::GET, "manifest.bin", "", HeaderMap::new(), None)
                    .await?;
                if response.status() == StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                if response.status() != StatusCode::OK {
                    return Err(Error::Http(response.status().as_u16()));
                }
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|value| value.to_str().ok())
                    .ok_or(Error::Invalid("manifest ETag"))?
                    .to_owned();
                let mut bytes = crate::arena::reserve(MANIFEST_LIMIT)?;
                let mut body = response.into_body();
                while let Some(chunk) = next_data(&mut body).await? {
                    crate::arena::append(&mut bytes, &chunk)?;
                }
                Ok(Some((Manifest::decode(&bytes)?, etag)))
            }
        }
    }

    pub async fn publish_manifest(
        &self,
        manifest: &Manifest,
        expected: Option<&str>,
    ) -> Result<()> {
        let bytes = manifest.encode()?;
        match &self.backend {
            Backend::Local(root) => {
                let lock = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(root.join("manifest.lock"))?;
                fs2::FileExt::lock_exclusive(&lock)?;
                let current = match read_small_file(&root.join("manifest.bin"), MANIFEST_LIMIT) {
                    Ok(bytes) => Some(Oid(checksum(&bytes)).to_string()),
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => return Err(error),
                };
                if current.as_deref() != expected {
                    return Err(Error::Conflict);
                }
                atomic_write(&root.join("manifest.bin"), &bytes, true)?;
                Ok(())
            }
            Backend::Http { .. } => {
                let mut headers = HeaderMap::new();
                if let Some(etag) = expected {
                    headers.insert("if-match", header(etag)?);
                } else {
                    headers.insert("if-none-match", HeaderValue::from_static("*"));
                }
                headers.insert(
                    "content-type",
                    HeaderValue::from_static("application/octet-stream"),
                );
                headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
                let response = self
                    .request(
                        Method::PUT,
                        "manifest.bin",
                        "",
                        headers,
                        Some(Bytes::from(bytes)),
                    )
                    .await?;
                let status = response.status();
                drain(response.into_body()).await?;
                if status == StatusCode::PRECONDITION_FAILED || status == StatusCode::CONFLICT {
                    return Err(Error::Conflict);
                }
                if !status.is_success() {
                    return Err(Error::Http(status.as_u16()));
                }
                Ok(())
            }
        }
    }

    pub async fn upload(&self, slab: &Slab) -> Result<()> {
        for (extension, bytes) in [
            (
                "pack",
                slab.pack
                    .as_ref()
                    .ok_or(Error::Invalid("upload pack not in memory"))?,
            ),
            ("idx", &slab.idx),
            ("meta", &slab.meta),
            ("mphf", &slab.mphf),
            ("links", &slab.links),
            ("hfs", &slab.hfs),
        ] {
            self.put_immutable(&artifact_key(slab.reference.id, extension), bytes.clone())
                .await?;
        }
        if let Some(times) = &slab.times {
            self.put_immutable(&times_key(slab.reference), times.clone())
                .await?;
        }
        Ok(())
    }

    pub async fn put_immutable(&self, key: &str, bytes: Bytes) -> Result<()> {
        match &self.backend {
            Backend::Local(root) => {
                let path = root.join(key);
                match atomic_write(&path, &bytes, false) {
                    Ok(()) => Ok(()),
                    Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !file_equals(&path, &bytes)? {
                            return Err(Error::Invalid(
                                "immutable artifact already exists with different bytes",
                            ));
                        }
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
            Backend::Http { kind, .. } if bytes.len() > PART_BYTES && *kind == StoreKind::Azure => {
                self.put_azure_blocks(key, bytes).await
            }
            Backend::Http { kind, .. } if bytes.len() > PART_BYTES && *kind == StoreKind::S3 => {
                self.put_s3_parts(key, bytes).await
            }
            Backend::Http { .. } => {
                let mut headers = HeaderMap::new();
                headers.insert("if-none-match", HeaderValue::from_static("*"));
                headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
                let response = self
                    .request(Method::PUT, key, "", headers, Some(bytes.clone()))
                    .await?;
                let status = response.status();
                drain(response.into_body()).await?;
                if status == StatusCode::PRECONDITION_FAILED || status == StatusCode::CONFLICT {
                    self.verify_existing(key, &bytes).await?;
                } else if !status.is_success() {
                    return Err(Error::Http(status.as_u16()));
                }
                Ok(())
            }
        }
    }

    async fn verify_existing(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let response = self
            .request(Method::GET, key, "", HeaderMap::new(), None)
            .await?;
        if response.status() != StatusCode::OK {
            return Err(Error::Http(response.status().as_u16()));
        }
        let mut body = response.into_body();
        let mut cursor = 0usize;
        while let Some(chunk) = next_data(&mut body).await? {
            let end = cursor
                .checked_add(chunk.len())
                .ok_or(Error::Invalid("existing artifact length"))?;
            if bytes.get(cursor..end) != Some(chunk.as_ref()) {
                return Err(Error::Invalid("immutable artifact collision"));
            }
            cursor = end;
        }
        if cursor != bytes.len() {
            return Err(Error::Invalid("existing artifact truncated"));
        }
        Ok(())
    }

    async fn put_azure_blocks(&self, key: &str, bytes: Bytes) -> Result<()> {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for (index, range) in (0..bytes.len()).step_by(PART_BYTES).enumerate() {
            let block = base64(format!("{index:08}").as_bytes());
            let end = range.saturating_add(PART_BYTES).min(bytes.len());
            let response = self
                .request(
                    Method::PUT,
                    key,
                    &format!("comp=block&blockid={}", url_encode(&block)),
                    HeaderMap::new(),
                    Some(bytes.slice(range..end)),
                )
                .await?;
            let status = response.status();
            drain(response.into_body()).await?;
            if !status.is_success() {
                return Err(Error::Http(status.as_u16()));
            }
            xml.push_str(&format!("<Latest>{block}</Latest>"));
        }
        xml.push_str("</BlockList>");
        let mut headers = HeaderMap::new();
        headers.insert("if-none-match", HeaderValue::from_static("*"));
        let response = self
            .request(
                Method::PUT,
                key,
                "comp=blocklist",
                headers,
                Some(Bytes::from(xml)),
            )
            .await?;
        let status = response.status();
        drain(response.into_body()).await?;
        if status == StatusCode::PRECONDITION_FAILED {
            return self.verify_existing(key, &bytes).await;
        }
        if !status.is_success() {
            return Err(Error::Http(status.as_u16()));
        }
        Ok(())
    }

    async fn put_s3_parts(&self, key: &str, bytes: Bytes) -> Result<()> {
        let response = self
            .request(Method::POST, key, "uploads=", HeaderMap::new(), None)
            .await?;
        if !response.status().is_success() {
            return Err(Error::Http(response.status().as_u16()));
        }
        let xml = small_body(response.into_body()).await?;
        let upload = xml_field(&xml, "UploadId")?;
        let mut complete = String::from("<CompleteMultipartUpload>");
        let result: Result<()> = async {
            for (index, start) in (0..bytes.len()).step_by(PART_BYTES).enumerate() {
                let number = index.saturating_add(1);
                let response = self
                    .request(
                        Method::PUT,
                        key,
                        &format!("partNumber={number}&uploadId={}", url_encode(upload)),
                        HeaderMap::new(),
                        Some(bytes.slice(start..start.saturating_add(PART_BYTES).min(bytes.len()))),
                    )
                    .await?;
                let status = response.status();
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|value| value.to_str().ok())
                    .ok_or(Error::Invalid("multipart ETag"))?
                    .to_owned();
                drain(response.into_body()).await?;
                if !status.is_success() {
                    return Err(Error::Http(status.as_u16()));
                }
                complete.push_str(&format!(
                    "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
                    xml_escape(&etag)
                ));
            }
            complete.push_str("</CompleteMultipartUpload>");
            let mut headers = HeaderMap::new();
            headers.insert("if-none-match", HeaderValue::from_static("*"));
            let response = self
                .request(
                    Method::POST,
                    key,
                    &format!("uploadId={}", url_encode(upload)),
                    headers,
                    Some(Bytes::from(complete)),
                )
                .await?;
            let status = response.status();
            let response_xml = small_body(response.into_body()).await?;
            if status == StatusCode::PRECONDITION_FAILED || status == StatusCode::CONFLICT {
                return self.verify_existing(key, &bytes).await;
            }
            if !status.is_success() {
                return Err(Error::Http(status.as_u16()));
            }
            if !response_xml.contains("<CompleteMultipartUploadResult")
                || response_xml.contains("<Error>")
            {
                return Err(Error::Invalid("multipart completion response"));
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            match self
                .request(
                    Method::DELETE,
                    key,
                    &format!("uploadId={}", url_encode(upload)),
                    HeaderMap::new(),
                    None,
                )
                .await
            {
                Ok(response) => {
                    if let Err(error) = drain(response.into_body()).await {
                        eprintln!("aborting multipart upload: {error}");
                    }
                }
                Err(error) => eprintln!("aborting multipart upload: {error}"),
            }
        }
        result
    }

    async fn load_artifact(&self, id: Oid, extension: &str, maximum: usize) -> Result<Bytes> {
        let key = artifact_key(id, extension);
        self.load_key(&key, maximum).await
    }

    async fn load_key(&self, key: &str, maximum: usize) -> Result<Bytes> {
        let path = match &self.backend {
            Backend::Local(root) => root.join(key),
            Backend::Http { .. } => {
                let path = self.cache.join(key);
                if !path.exists() {
                    let response = self
                        .request(Method::GET, key, "", HeaderMap::new(), None)
                        .await?;
                    if response.status() != StatusCode::OK {
                        return Err(Error::Http(response.status().as_u16()));
                    }
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let mut temp = TempFile::new(&path)?;
                    let mut body = response.into_body();
                    let mut len = 0usize;
                    while let Some(chunk) = next_data(&mut body).await? {
                        len = len
                            .checked_add(chunk.len())
                            .ok_or(Error::Capacity("artifact length"))?;
                        if len > maximum {
                            return Err(Error::Capacity("artifact download"));
                        }
                        temp.file.write_all(&chunk)?;
                    }
                    temp.commit(&path, false)?;
                }
                path
            }
        };
        Ok(Bytes::from_owner(gvfs_platform::snapshot(&path, maximum)?))
    }

    pub async fn load_slab(&self, reference: SlabRef) -> Result<Slab> {
        self.load_slab_mode(reference, true).await
    }

    pub async fn load_metadata(&self, reference: SlabRef) -> Result<Slab> {
        self.load_slab_mode(reference, false).await
    }

    async fn load_slab_mode(&self, reference: SlabRef, pack: bool) -> Result<Slab> {
        let n = reference.count as usize;
        let metadata_max = n
            .checked_mul(80)
            .and_then(|n| n.checked_add(2048))
            .ok_or(Error::Capacity("slab metadata"))?;
        let slab = Slab {
            reference,
            pack: if pack && self.is_local() {
                Some(
                    self.load_artifact(reference.id, "pack", self.maximum_pack)
                        .await?,
                )
            } else {
                None
            },
            idx: self
                .load_artifact(reference.id, "idx", metadata_max)
                .await?,
            meta: self
                .load_artifact(reference.id, "meta", metadata_max)
                .await?,
            mphf: self
                .load_artifact(reference.id, "mphf", metadata_max)
                .await?,
            links: self
                .load_artifact(reference.id, "links", self.maximum_pack)
                .await?,
            hfs: self
                .load_artifact(reference.id, "hfs", metadata_max)
                .await?,
            times: if reference.times {
                Some(
                    self.load_key(
                        &times_key(reference),
                        n.saturating_mul(8).saturating_add(76),
                    )
                    .await?,
                )
            } else {
                None
            },
        };
        slab.validate()?;
        Ok(slab)
    }

    pub async fn hydrate_pack(&self, reference: SlabRef) -> Result<Bytes> {
        let pack = self
            .load_artifact(reference.id, "pack", self.maximum_pack)
            .await?;
        let body = crate::codec::verify_checksum(&pack)?;
        if pack.len() as u64 != reference.pack_len
            || crate::codec::pack_count(body)? != reference.count
            || crate::codec::Oid::from_bytes(crate::codec::take(&pack, body.len(), 20)?)?
                != reference.id
        {
            return Err(Error::Invalid("hydrated pack identity"));
        }

        Ok(pack)
    }

    /// Local jobs serialize their entire publication/GC cycle. ETag CAS remains
    /// the authority for cloud jobs, where artifacts are never automatically deleted.
    pub async fn writer_lock(&self) -> Result<Option<File>> {
        let Backend::Local(root) = &self.backend else {
            return Ok(None);
        };
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("pipeline.lock"))?;
        tokio::task::spawn_blocking(move || -> Result<Option<File>> {
            fs2::FileExt::lock_exclusive(&file)?;
            Ok(Some(file))
        })
        .await
        .map_err(|error| Error::Transport(error.to_string()))?
    }

    pub async fn read_range(&self, id: Oid, start: u64, out: &mut [u8]) -> Result<()> {
        if out.is_empty() {
            return Ok(());
        }
        let key = artifact_key(id, "pack");
        match &self.backend {
            Backend::Local(root) => {
                let mut file = File::open(root.join(key))?;
                file.seek(SeekFrom::Start(start))?;
                file.read_exact(out)?;
                Ok(())
            }
            Backend::Http { .. } => {
                let end = start
                    .checked_add(out.len() as u64)
                    .and_then(|n| n.checked_sub(1))
                    .ok_or(Error::Invalid("range bounds"))?;
                let mut headers = HeaderMap::new();
                headers.insert("range", header(&format!("bytes={start}-{end}"))?);
                let response = self.request(Method::GET, &key, "", headers, None).await?;
                if response.status() != StatusCode::PARTIAL_CONTENT {
                    return Err(Error::Http(response.status().as_u16()));
                }
                let value = response
                    .headers()
                    .get("content-range")
                    .and_then(|value| value.to_str().ok())
                    .ok_or(Error::Invalid("missing Content-Range"))?;
                let expected = format!("bytes {start}-{end}/");
                let total = value
                    .strip_prefix(&expected)
                    .and_then(|total| total.parse::<u64>().ok())
                    .ok_or(Error::Invalid("Content-Range"))?;
                if total <= end {
                    return Err(Error::Invalid("Content-Range total"));
                }
                let mut body = response.into_body();
                let mut cursor = 0usize;
                while let Some(chunk) = next_data(&mut body).await? {
                    let end = cursor
                        .checked_add(chunk.len())
                        .ok_or(Error::Invalid("range body overflow"))?;
                    out.get_mut(cursor..end)
                        .ok_or(Error::Invalid("oversized range body"))?
                        .copy_from_slice(&chunk);
                    cursor = end;
                }
                if cursor != out.len() {
                    return Err(Error::Invalid("truncated range body"));
                }
                Ok(())
            }
        }
    }

    /// Only unreferenced, old immutable artifacts are eligible. Readers can still
    /// hold older manifests, so the default grace is deliberately hours, not minutes.
    pub fn gc_local(
        &self,
        manifest: &Manifest,
        older_than: std::time::SystemTime,
    ) -> Result<usize> {
        let Backend::Local(root) = &self.backend else {
            return Ok(0);
        };
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("manifest.lock"))?;
        fs2::FileExt::lock_exclusive(&lock)?;
        let current = Manifest::decode(&read_small_file(
            &root.join("manifest.bin"),
            MANIFEST_LIMIT,
        )?)?;
        if current.generation != manifest.generation {
            return Err(Error::Conflict);
        }
        let mut removed = 0usize;
        for entry in std::fs::read_dir(root.join("slabs"))? {
            let entry = entry?;
            if !entry.file_type()?.is_file() || entry.metadata()?.modified()? >= older_than {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some((id, extension)) = name.split_once('.') else {
                continue;
            };
            let times = extension.strip_suffix(".times").and_then(|range| {
                let (start, end) = range.split_once('-')?;
                let (start, end) = (start.parse::<u64>().ok()?, end.parse::<u64>().ok()?);
                (start <= end && extension == format!("{start}-{end}.times"))
                    .then_some((start, end))
            });
            if times.is_none()
                && !matches!(
                    extension,
                    "pack" | "idx" | "meta" | "mphf" | "links" | "hfs"
                )
            {
                continue;
            }
            let Ok(id) = Oid::parse(id.as_bytes()) else {
                continue;
            };
            if std::iter::once(&current.baseline)
                .chain(&current.weekly)
                .chain(&current.pyramid)
                .any(|slab| {
                    slab.id == id
                        && times.is_none_or(|(start, end)| {
                            slab.times && slab.start == start && slab.end == end
                        })
                })
            {
                continue;
            }
            std::fs::remove_file(entry.path())?;
            removed = removed.saturating_add(1);
        }
        Ok(removed)
    }
}

pub fn artifact_key(id: Oid, extension: &str) -> String {
    format!("slabs/{id}.{extension}")
}

pub fn times_key(reference: SlabRef) -> String {
    format!(
        "slabs/{}.{}-{}.times",
        reference.id, reference.start, reference.end
    )
}

pub struct TempFile {
    pub path: PathBuf,
    pub file: File,
}

impl TempFile {
    pub fn new(destination: &Path) -> Result<Self> {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(Error::Invalid("temporary destination"))?;
        let path =
            destination.with_file_name(format!(".{name}.{}.{nonce}.tmp", std::process::id()));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self { path, file })
    }

    pub fn commit(&mut self, destination: &Path, replace: bool) -> Result<()> {
        self.file.sync_all()?;
        if replace {
            std::fs::rename(&self.path, destination)?;
        } else {
            match std::fs::hard_link(&self.path, destination) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(error.into());
                }
                Err(error) => return Err(error.into()),
            }
            std::fs::remove_file(&self.path)?;
        }
        if let Some(parent) = destination.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.path) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => eprintln!("removing temporary file {}: {error}", self.path.display()),
        }
    }
}

fn atomic_write(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    let mut temp = TempFile::new(path)?;
    temp.file.write_all(bytes)?;
    temp.commit(path, replace)
}

fn read_small_file(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > maximum as u64 {
        return Err(Error::Capacity("manifest file"));
    }
    let mut bytes = crate::arena::reserve(maximum.saturating_add(1))?;
    file.take(maximum.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(Error::Capacity("manifest file"));
    }
    Ok(bytes)
}

fn file_equals(path: &Path, expected: &[u8]) -> Result<bool> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != expected.len() as u64 {
        return Ok(false);
    }
    let mut buffer = [0u8; 65536];
    let mut cursor = 0usize;
    loop {
        let len = file.read(&mut buffer)?;
        if len == 0 {
            return Ok(cursor == expected.len());
        }
        let end = cursor
            .checked_add(len)
            .ok_or(Error::Invalid("existing artifact length"))?;
        if expected.get(cursor..end) != Some(crate::codec::take(&buffer, 0, len)?) {
            return Ok(false);
        }
        cursor = end;
    }
}

async fn drain(mut body: Incoming) -> Result<()> {
    while next_data(&mut body).await?.is_some() {}
    Ok(())
}

async fn small_body(mut body: Incoming) -> Result<String> {
    let mut bytes = crate::arena::reserve(1 << 20)?;
    while let Some(chunk) = next_data(&mut body).await? {
        crate::arena::append(&mut bytes, &chunk)?;
    }
    String::from_utf8(bytes).map_err(|_| Error::Invalid("XML response UTF-8"))
}

fn xml_field<'a>(xml: &'a str, tag: &str) -> Result<&'a str> {
    let (_, rest) = xml
        .split_once(&format!("<{tag}>"))
        .ok_or(Error::Invalid("XML field"))?;
    rest.split_once(&format!("</{tag}>"))
        .map(|(field, _)| field)
        .ok_or(Error::Invalid("XML closing tag"))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn url_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            use std::fmt::Write;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let a = chunk.first().copied().unwrap_or(0);
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let value = (u32::from(a) << 16) | (u32::from(b) << 8) | u32::from(c);
        for shift in [18u32, 12, 6, 0] {
            if shift == 6 && chunk.len() < 2 || shift == 0 && chunk.len() < 3 {
                out.push('=');
            } else if let Some(&byte) = ALPHABET.get(((value >> shift) & 63) as usize) {
                out.push(char::from(byte));
            }
        }
    }
    out
}

#[cfg(test)]
mod signing_tests {
    use super::*;

    #[test]
    fn sigv4_matches_an_independent_standard_library_implementation() {
        let credentials = AwsCredentials {
            access: "FIXTUREACCESS".into(),
            secret: "fixture-not-a-real-secret".into(),
            token: Some("fixture-token+/=".into()),
            region: "us-test-1".into(),
        };
        let uri: Uri =
            "https://example.invalid:8443/bucket/a%20b?uploadId=a%2Bb%2F%3D&partNumber=2"
                .parse()
                .unwrap();
        let mut headers = HeaderMap::new();
        credentials
            .sign(&Method::PUT, &uri, &mut headers, b"fixture payload")
            .unwrap();
        let timestamp = headers.get("x-amz-date").unwrap().to_str().unwrap();
        assert_eq!(timestamp.len(), 16);
        let script = r#"
import sys, hashlib, hmac, urllib.parse
stamp, url = sys.argv[1:]
uri = urllib.parse.urlsplit(url)
digest = hashlib.sha256(b'fixture payload').hexdigest()
query = '&'.join(urllib.parse.quote(k, safe='~')+'='+urllib.parse.quote(v, safe='~')
                 for k,v in sorted(urllib.parse.parse_qsl(uri.query, keep_blank_values=True)))
path = urllib.parse.quote(urllib.parse.unquote(uri.path), safe='/~')
fields = {'host':uri.netloc, 'x-amz-content-sha256':digest, 'x-amz-date':stamp,
          'x-amz-security-token':'fixture-token+/='}
names = ';'.join(sorted(fields))
canonical = 'PUT\n'+path+'\n'+query+'\n'+''.join(k+':'+fields[k]+'\n' for k in sorted(fields))+'\n'+names+'\n'+digest
scope = stamp[:8]+'/us-test-1/s3/aws4_request'
message = 'AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()
sign = lambda k,v: hmac.new(k,v.encode(),hashlib.sha256).digest()
key = sign(b'AWS4fixture-not-a-real-secret',stamp[:8])
for part in ['us-test-1','s3','aws4_request']: key = sign(key,part)
signature = sign(key,message).hex()
print('AWS4-HMAC-SHA256 Credential=FIXTUREACCESS/'+scope+', SignedHeaders='+names+', Signature='+signature)
"#;
        let output = std::process::Command::new("python3")
            .args(["-c", script, timestamp, &uri.to_string()])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            headers.get("authorization").unwrap().to_str().unwrap(),
            String::from_utf8(output.stdout).unwrap().trim()
        );
    }
}
