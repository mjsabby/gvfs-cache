//! A small blocking HTTP/1.1 client (plain or TLS via rustls) with streaming bodies.
//!
//! Used for the upstream git server (smart HTTP) and the Azure Blob store. One `Client`
//! owns at most one keep-alive connection to one origin; callers that need parallelism
//! create several clients.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::util::{Context, Error, ErrorKind, Result};

/// `scheme://host[:port]/base/path`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Url {
    pub https: bool,
    pub host: String,
    pub port: u16,
    /// Path prefix without a trailing slash (may be empty).
    pub path: String,
}

impl Url {
    pub fn parse(s: &str) -> Result<Url> {
        let (https, rest) = if let Some(r) = s.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("http://") {
            (false, r)
        } else {
            crate::bail!("not an http(s) URL: {s}");
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (rest.get(..i).unwrap_or(""), rest.get(i..).unwrap_or("")),
            None => (rest, ""),
        };
        let authority = authority.rsplit('@').next().unwrap_or(authority);
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !h.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => (
                h.to_string(),
                p.parse().map_err(|_| crate::err!("bad port in {s}"))?,
            ),
            _ => (authority.to_string(), if https { 443 } else { 80 }),
        };
        if host.is_empty() {
            crate::bail!("no host in URL {s}");
        }
        Ok(Url {
            https,
            host,
            port,
            path: path.trim_end_matches('/').to_string(),
        })
    }

    pub fn origin(&self) -> String {
        format!(
            "{}://{}:{}",
            if self.https { "https" } else { "http" },
            self.host,
            self.port
        )
    }
}

/// The TLS client configuration (webpki roots, ring), shared with [`crate::aclient`].
pub(crate) fn tls_config() -> Result<Arc<rustls::ClientConfig>> {
    static CONFIG: OnceLock<std::result::Result<Arc<rustls::ClientConfig>, String>> =
        OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map(|b| Arc::new(b.with_root_certificates(roots).with_no_client_auth()))
            .map_err(|e| e.to_string())
        })
        .clone()
        .map_err(Error::new)
}

enum Stream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

pub struct Client {
    pub url: Url,
    conn: Option<BufReader<Stream>>,
    timeout: Duration,
    /// Request headers added to every request (e.g. Authorization).
    pub default_headers: Vec<(String, String)>,
}

/// Status, headers, body framing, and whether the connection can be reused.
type Head = (u16, Vec<(String, String)>, Framing, bool);

/// How the response body is delimited.
#[derive(Clone, Copy, Debug)]
enum Framing {
    Length(u64),
    Chunked { left_in_chunk: u64, done: bool },
    UntilClose,
}

/// A response whose body is read incrementally (`Read`).
pub struct Response<'a> {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    framing: Framing,
    conn: &'a mut Option<BufReader<Stream>>,
    reusable: bool,
}

impl Client {
    pub fn new(url: Url, timeout: Duration) -> Client {
        Client {
            url,
            conn: None,
            timeout,
            default_headers: Vec::new(),
        }
    }

    fn connect(&self) -> Result<BufReader<Stream>> {
        let addr = format!("{}:{}", self.url.host, self.url.port);
        let addrs: Vec<_> = std::net::ToSocketAddrs::to_socket_addrs(&addr)
            .ctx(&format!("resolving {addr}"))?
            .collect();
        let mut last = None;
        for a in addrs {
            match TcpStream::connect_timeout(&a, self.timeout) {
                Ok(tcp) => {
                    tcp.set_read_timeout(Some(self.timeout))?;
                    tcp.set_write_timeout(Some(self.timeout))?;
                    tcp.set_nodelay(true)?;
                    let stream = if self.url.https {
                        let name = rustls::pki_types::ServerName::try_from(self.url.host.clone())
                            .map_err(|e| {
                            crate::err!("bad TLS server name {}: {e}", self.url.host)
                        })?;
                        let conn = rustls::ClientConnection::new(tls_config()?, name)
                            .map_err(|e| crate::err!("TLS: {e}"))?;
                        Stream::Tls(Box::new(rustls::StreamOwned::new(conn, tcp)))
                    } else {
                        Stream::Plain(tcp)
                    };
                    return Ok(BufReader::with_capacity(64 << 10, stream));
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last
            .map(Error::from)
            .unwrap_or_else(|| crate::err!("no address for {addr}")))
        .ctx(&format!("connecting to {addr}"))
    }

    /// Sends a request; `path` is appended to the URL's base path. The body is sent with a
    /// Content-Length. A stale keep-alive connection is retried once on a fresh one.
    pub fn request(
        &mut self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<Response<'_>> {
        let mut head = format!(
            "{method} {}{path} HTTP/1.1\r\nHost: {}\r\n",
            self.url.path, self.url.host
        );
        for (k, v) in self.default_headers.iter() {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        if !body.is_empty() || method == "POST" || method == "PUT" {
            head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        head.push_str("\r\n");
        for attempt in 0..2 {
            let fresh = self.conn.is_none();
            if fresh {
                self.conn = Some(self.connect()?);
            }
            let sent = (|| -> std::io::Result<()> {
                let s = self
                    .conn
                    .as_mut()
                    .ok_or(std::io::ErrorKind::NotConnected)?
                    .get_mut();
                s.write_all(head.as_bytes())?;
                s.write_all(body)?;
                s.flush()
            })();
            let parsed = match sent {
                Ok(()) => self.read_head(method),
                Err(e) => Err(Error::from(e)),
            };
            match parsed {
                Ok((status, headers, framing, reusable)) => {
                    return Ok(Response {
                        status,
                        headers,
                        framing,
                        conn: &mut self.conn,
                        reusable,
                    });
                }
                Err(e) => {
                    self.conn = None;
                    if fresh || attempt == 1 {
                        return Err(e).ctx(&format!(
                            "{method} {}{}{path}",
                            self.url.origin(),
                            self.url.path
                        ));
                    }
                }
            }
        }
        crate::bail!("{method} {path}: request failed")
    }

    fn read_head(&mut self, method: &str) -> Result<Head> {
        let conn = self.conn.as_mut().ctx("not connected")?;
        let mut line = String::new();
        loop {
            line.clear();
            if conn.read_line(&mut line)? == 0 {
                crate::bail!("connection closed before the response");
            }
            let status: u16 = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .ctx("bad status line")?;
            let mut headers = Vec::new();
            loop {
                line.clear();
                if conn.read_line(&mut line)? == 0 {
                    crate::bail!("connection closed in the response headers");
                }
                let l = line.trim_end();
                if l.is_empty() {
                    break;
                }
                if let Some((k, v)) = l.split_once(':') {
                    headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
                }
            }
            if status == 100 {
                continue;
            }
            let get = |name: &str| {
                headers
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.as_str())
            };
            let close = get("connection").is_some_and(|v| v.eq_ignore_ascii_case("close"));
            let framing = if method == "HEAD" || status == 204 || status == 304 {
                Framing::Length(0)
            } else if get("transfer-encoding")
                .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"))
            {
                Framing::Chunked {
                    left_in_chunk: 0,
                    done: false,
                }
            } else if let Some(n) = get("content-length").and_then(|v| v.parse().ok()) {
                Framing::Length(n)
            } else {
                Framing::UntilClose
            };
            let reusable = !close && !matches!(framing, Framing::UntilClose);
            return Ok((status, headers, framing, reusable));
        }
    }
}

impl Response<'_> {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Reads the whole body (bounded by `limit`).
    pub fn body_vec(&mut self, limit: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = [0u8; 16 << 10];
        loop {
            let n = self.read(&mut buf)?;
            if n == 0 {
                return Ok(out);
            }
            out.extend_from_slice(buf.get(..n).unwrap_or(&[]));
            if out.len() > limit {
                crate::bail!("response body larger than {limit} bytes");
            }
        }
    }

    /// Fails with the status and a body excerpt unless 2xx.
    pub fn ensure_ok(mut self, what: &str) -> Result<Self> {
        if (200..300).contains(&self.status) {
            return Ok(self);
        }
        let status = self.status;
        let body = self.body_vec(64 << 10).unwrap_or_default();
        let excerpt =
            String::from_utf8_lossy(body.get(..body.len().min(512)).unwrap_or(&[])).into_owned();
        let kind = match status {
            404 => ErrorKind::NotFound,
            409 | 412 => ErrorKind::Conflict,
            _ => ErrorKind::Other,
        };
        Err(Error::with_kind(
            kind,
            format!("{what}: HTTP {status}: {}", excerpt.trim()),
        ))
    }

    /// Drains the rest of the body so the connection can be reused.
    pub fn finish(mut self) -> Result<()> {
        let mut buf = [0u8; 16 << 10];
        while self.read(&mut buf)? > 0 {}
        Ok(())
    }

    fn read_chunk_size(conn: &mut BufReader<Stream>) -> std::io::Result<u64> {
        let mut line = String::new();
        conn.read_line(&mut line)?;
        let hex = line.trim().split(';').next().unwrap_or("");
        u64::from_str_radix(hex, 16)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad chunk size"))
    }
}

impl Read for Response<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let Some(conn) = self.conn.as_mut() else {
            return Ok(0);
        };
        let n = match &mut self.framing {
            Framing::Length(0) => 0,
            Framing::Length(left) => {
                let want = buf.len().min(usize::try_from(*left).unwrap_or(usize::MAX));
                let n = conn.read(buf.get_mut(..want).unwrap_or(&mut []))?;
                if n == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
                *left = left.saturating_sub(crate::util::u64_of(n));
                n
            }
            Framing::Chunked { done: true, .. } => 0,
            Framing::Chunked {
                left_in_chunk,
                done,
            } => {
                if *left_in_chunk == 0 {
                    let size = Self::read_chunk_size(conn)?;
                    if size == 0 {
                        // Trailers end with an empty line.
                        let mut line = String::new();
                        loop {
                            line.clear();
                            if conn.read_line(&mut line)? == 0 || line.trim().is_empty() {
                                break;
                            }
                        }
                        *done = true;
                        return Ok(0);
                    }
                    *left_in_chunk = size;
                }
                let want = buf
                    .len()
                    .min(usize::try_from(*left_in_chunk).unwrap_or(usize::MAX));
                let n = conn.read(buf.get_mut(..want).unwrap_or(&mut []))?;
                if n == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
                *left_in_chunk = left_in_chunk.saturating_sub(crate::util::u64_of(n));
                if *left_in_chunk == 0 {
                    let mut crlf = [0u8; 2];
                    conn.read_exact(&mut crlf)?;
                }
                n
            }
            Framing::UntilClose => conn.read(buf)?,
        };
        if n == 0 && !self.reusable {
            *self.conn = None;
        }
        Ok(n)
    }
}

impl Drop for Response<'_> {
    fn drop(&mut self) {
        let complete = match self.framing {
            Framing::Length(left) => left == 0,
            Framing::Chunked { done, .. } => done,
            Framing::UntilClose => false,
        };
        if !(complete && self.reusable) {
            *self.conn = None;
        }
    }
}

/// `Basic base64(user:password)`.
pub fn basic_auth(user: &str, password: &str) -> String {
    format!(
        "Basic {}",
        crate::b64::encode(format!("{user}:{password}").as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsing() {
        let u = Url::parse("https://dev.azure.com/org/proj/_git/repo/").unwrap();
        assert_eq!(
            (u.https, u.host.as_str(), u.port, u.path.as_str()),
            (true, "dev.azure.com", 443, "/org/proj/_git/repo")
        );
        let u = Url::parse("http://127.0.0.1:9000").unwrap();
        assert_eq!(
            (u.https, u.host.as_str(), u.port, u.path.as_str()),
            (false, "127.0.0.1", 9000, "")
        );
        assert!(Url::parse("ftp://x").is_err());
    }

    #[test]
    fn chunked_and_length_bodies_over_a_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            for reply in [
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
                "HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\n\r\nnope",
            ] {
                let mut line = String::new();
                loop {
                    line.clear();
                    r.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                }
                s.write_all(reply.as_bytes()).unwrap();
            }
        });
        let mut c = Client::new(
            Url::parse(&format!("http://127.0.0.1:{port}/base")).unwrap(),
            Duration::from_secs(5),
        );
        let mut r = c.request("GET", "/a", &[], b"").unwrap();
        assert_eq!(r.body_vec(100).unwrap(), b"hello world");
        drop(r);
        let r = c.request("GET", "/b", &[], b"").unwrap();
        let e = r.ensure_ok("get b").err().unwrap();
        assert!(e.is_not_found(), "{e}");
        server.join().unwrap();
    }
}
