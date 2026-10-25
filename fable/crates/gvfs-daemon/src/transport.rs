//! Upstream Git transports: protocol v2 `ls-refs` and `fetch` over a local
//! `git upload-pack` (any repository on disk — the test and air-gapped path) or smart HTTP(S)
//! (Azure DevOps, GitHub, any `git http-backend`). Both are plain synchronous `Read`
//! streams: the ingest engine is sequential CPU work and runs on its own thread, so there
//! is no async machinery here at all.

use core::fmt;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use crate::arena::try_extend;
use crate::oid::Oid;
use crate::pktline::{DELIM_PKT, FLUSH_PKT, MAX_PKT_LEN, Pkt, chomp, parse_pkt, push_pkt};

#[derive(Debug)]
pub enum UpstreamError {
    Io(std::io::Error),
    Protocol(String),
    Http(u16, String),
    Remote(String),
    Capacity,
    Tls(String),
}

impl fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "upstream i/o: {e}"),
            Self::Protocol(m) => write!(f, "upstream protocol: {m}"),
            Self::Http(code, m) => write!(f, "upstream http {code}: {m}"),
            Self::Remote(m) => write!(f, "upstream error: {m}"),
            Self::Capacity => f.write_str("upstream request too large"),
            Self::Tls(m) => write!(f, "tls: {m}"),
        }
    }
}

impl std::error::Error for UpstreamError {}

impl From<std::io::Error> for UpstreamError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<crate::arena::OutOfCapacity> for UpstreamError {
    fn from(_: crate::arena::OutOfCapacity) -> Self {
        Self::Capacity
    }
}

impl From<crate::pktline::PktError> for UpstreamError {
    fn from(e: crate::pktline::PktError) -> Self {
        Self::Protocol(e.to_string())
    }
}

/// Most `have` lines a fetch sends (the tips of the newest slabs, so a rewound branch is
/// still fetched incrementally).
pub const MAX_HAVES: usize = 32;

/// A Git upstream reachable with protocol v2.
pub trait Upstream {
    /// Current object id of `refname` (`refs/heads/main`), `None` if it does not exist.
    fn ls_ref(&mut self, refname: &str) -> Result<Option<Oid>, UpstreamError>;

    /// Streams the pack of everything reachable from `wants` but not from `haves` into
    /// `sink` (a complete pack: header, entries, SHA-1 trailer; never a thin pack). The
    /// first want is the branch tip; further wants are arbitrary objects a client asked for
    /// that no slab holds.
    fn fetch(
        &mut self,
        wants: &[Oid],
        haves: &[Oid],
        sink: &mut dyn Write,
    ) -> Result<u64, UpstreamError>;

    fn describe(&self) -> String;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Caps {
    object_format: bool,
}

fn parse_caps(mut body: &[u8]) -> Result<Caps, UpstreamError> {
    let mut caps = Caps::default();
    let mut v2 = false;
    while let Some((pkt, n)) = parse_pkt(body)? {
        body = body.get(n..).unwrap_or(&[]);
        match pkt {
            Pkt::Data(d) => {
                let line = chomp(d);
                if line.starts_with(b"# service=") {
                    continue;
                }
                if line == b"version 2" {
                    v2 = true;
                    continue;
                }
                if !v2 {
                    return Err(UpstreamError::Protocol(
                        "upstream does not speak git protocol v2".into(),
                    ));
                }
                if let Some(fmt) = line.strip_prefix(b"object-format=") {
                    if fmt != b"sha1" {
                        return Err(UpstreamError::Protocol(
                            "only sha1 repositories are supported".into(),
                        ));
                    }
                    caps.object_format = true;
                }
            }
            Pkt::Flush if v2 => break,
            _ => {}
        }
    }
    if !v2 {
        return Err(UpstreamError::Protocol(
            "upstream does not speak git protocol v2".into(),
        ));
    }
    Ok(caps)
}

fn ls_refs_request(req: &mut Vec<u8>, caps: Caps, refname: &str) -> Result<(), UpstreamError> {
    req.clear();
    push_pkt(req, &[b"command=ls-refs\n"])?;
    push_pkt(req, &[b"agent=gvfs-daemon/0.1\n"])?;
    if caps.object_format {
        push_pkt(req, &[b"object-format=sha1\n"])?;
    }
    try_extend(req, DELIM_PKT)?;
    push_pkt(req, &[b"ref-prefix ", refname.as_bytes(), b"\n"])?;
    try_extend(req, FLUSH_PKT)?;
    Ok(())
}

fn fetch_request(
    req: &mut Vec<u8>,
    caps: Caps,
    wants: &[Oid],
    haves: &[Oid],
) -> Result<(), UpstreamError> {
    req.clear();
    push_pkt(req, &[b"command=fetch\n"])?;
    push_pkt(req, &[b"agent=gvfs-daemon/0.1\n"])?;
    if caps.object_format {
        push_pkt(req, &[b"object-format=sha1\n"])?;
    }
    try_extend(req, DELIM_PKT)?;
    // No `thin-pack`: every delta base must be inside the pack we store.
    push_pkt(req, &[b"ofs-delta\n"])?;
    push_pkt(req, &[b"no-progress\n"])?;
    for w in wants {
        if !w.is_zero() {
            push_pkt(req, &[b"want ", &w.to_hex(), b"\n"])?;
        }
    }
    for h in haves.iter().take(MAX_HAVES) {
        if !h.is_zero() {
            push_pkt(req, &[b"have ", &h.to_hex(), b"\n"])?;
        }
    }
    push_pkt(req, &[b"done\n"])?;
    try_extend(req, FLUSH_PKT)?;
    Ok(())
}

fn parse_ls_refs(mut body: &[u8], refname: &str) -> Result<Option<Oid>, UpstreamError> {
    while let Some((pkt, n)) = parse_pkt(body)? {
        body = body.get(n..).unwrap_or(&[]);
        let Pkt::Data(d) = pkt else {
            break;
        };
        let line = chomp(d);
        if let Some(msg) = line.strip_prefix(b"ERR ") {
            return Err(UpstreamError::Remote(
                String::from_utf8_lossy(msg).into_owned(),
            ));
        }
        let (Some(hex), Some(rest)) = (line.get(..40), line.get(40..)) else {
            continue;
        };
        let name = rest.strip_prefix(b" ").unwrap_or(rest);
        let name = name.split(|&c| c == b' ').next().unwrap_or(name);
        if name == refname.as_bytes() {
            return Ok(Oid::from_hex(hex));
        }
    }
    Ok(None)
}

/// Reads pkt-lines from a byte stream with one fixed reassembly buffer.
struct PktReader<R: Read> {
    src: R,
    buf: Vec<u8>,
    start: usize,
    end: usize,
    eof: bool,
}

enum PktRef {
    Flush,
    Delim,
    ResponseEnd,
    /// Payload at `buf[start..start + len]`.
    Data(usize, usize),
    Eof,
}

impl<R: Read> PktReader<R> {
    fn new(src: R) -> Self {
        Self {
            src,
            buf: vec![0u8; MAX_PKT_LEN.saturating_mul(2)],
            start: 0,
            end: 0,
            eof: false,
        }
    }

    fn fill(&mut self) -> Result<(), UpstreamError> {
        if self.start > 0 {
            self.buf.copy_within(self.start..self.end, 0);
            self.end = self.end.saturating_sub(self.start);
            self.start = 0;
        }
        let dst = self.buf.get_mut(self.end..).unwrap_or(&mut []);
        if dst.is_empty() {
            return Err(UpstreamError::Protocol("oversized pkt-line".into()));
        }
        let n = loop {
            match self.src.read(dst) {
                Ok(n) => break n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        };
        if n == 0 {
            self.eof = true;
        }
        self.end = self.end.saturating_add(n);
        Ok(())
    }

    fn next(&mut self) -> Result<PktRef, UpstreamError> {
        loop {
            let avail = self.buf.get(self.start..self.end).unwrap_or(&[]);
            match parse_pkt(avail)? {
                Some((pkt, n)) => {
                    let r = match pkt {
                        Pkt::Flush => PktRef::Flush,
                        Pkt::Delim => PktRef::Delim,
                        Pkt::ResponseEnd => PktRef::ResponseEnd,
                        Pkt::Data(d) => PktRef::Data(self.start.saturating_add(4), d.len()),
                    };
                    self.start = self.start.saturating_add(n);
                    return Ok(r);
                }
                None => {
                    if self.eof {
                        return if self.start == self.end {
                            Ok(PktRef::Eof)
                        } else {
                            Err(UpstreamError::Protocol(
                                "response ended inside a pkt-line".into(),
                            ))
                        };
                    }
                    self.fill()?;
                }
            }
        }
    }

    #[inline]
    fn data(&self, start: usize, len: usize) -> &[u8] {
        self.buf
            .get(start..start.saturating_add(len))
            .unwrap_or(&[])
    }
}

/// Demultiplexes a protocol v2 `fetch` response: skips to the `packfile` section and
/// copies side-band channel 1 into `sink`. Returns the number of pack bytes.
fn demux_fetch<R: Read>(src: R, sink: &mut dyn Write) -> Result<u64, UpstreamError> {
    let mut r = PktReader::new(src);
    let mut in_pack = false;
    let mut total = 0u64;
    loop {
        match r.next()? {
            PktRef::Eof => {
                return if in_pack {
                    Ok(total)
                } else {
                    Err(UpstreamError::Protocol(
                        "fetch response ended before the packfile section".into(),
                    ))
                };
            }
            PktRef::Flush => {
                if in_pack {
                    return Ok(total);
                }
            }
            PktRef::Delim | PktRef::ResponseEnd => {}
            PktRef::Data(s, l) => {
                let d = r.data(s, l);
                if !in_pack {
                    let line = chomp(d);
                    if line == b"packfile" {
                        in_pack = true;
                    } else if let Some(msg) = line.strip_prefix(b"ERR ") {
                        return Err(UpstreamError::Remote(
                            String::from_utf8_lossy(msg).into_owned(),
                        ));
                    }
                    continue;
                }
                match d.split_first() {
                    Some((1, payload)) => {
                        sink.write_all(payload)?;
                        total = total.saturating_add(payload.len() as u64);
                    }
                    Some((2, _progress)) => {}
                    Some((3, msg)) => {
                        return Err(UpstreamError::Remote(
                            String::from_utf8_lossy(msg).trim().to_string(),
                        ));
                    }
                    _ => return Err(UpstreamError::Protocol("invalid side-band packet".into())),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Local repository through `git upload-pack`
// ---------------------------------------------------------------------------------------

/// Any repository on disk, driven through `git upload-pack --stateless-rpc` exactly like
/// `git http-backend` would. Used for tests and for mirroring a local clone.
pub struct LocalGit {
    repo: std::path::PathBuf,
    git: String,
    caps: Option<Caps>,
    req: Vec<u8>,
}

impl LocalGit {
    pub fn new(repo: impl Into<std::path::PathBuf>) -> Self {
        Self {
            repo: repo.into(),
            git: "git".into(),
            caps: None,
            req: Vec::with_capacity(16 << 10),
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(&self.git);
        c.env("GIT_PROTOCOL", "version=2");
        c.env_remove("GIT_DIR");
        // Tools and tests want arbitrary commits (`--want <oid>`), not only ref tips.
        c.args(["-c", "uploadpack.allowAnySHA1InWant=true"]);
        c.args(args);
        c.arg(&self.repo);
        c.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }

    fn capabilities(&mut self) -> Result<Caps, UpstreamError> {
        if let Some(c) = self.caps {
            return Ok(c);
        }
        let out = self
            .command(&["upload-pack", "--advertise-refs"])
            .output()?;
        if !out.status.success() {
            return Err(UpstreamError::Remote(
                String::from_utf8_lossy(&out.stderr).trim().to_string(),
            ));
        }
        let caps = parse_caps(&out.stdout)?;
        self.caps = Some(caps);
        Ok(caps)
    }

    /// Runs one stateless-rpc request; `on_stdout` consumes the response stream.
    fn rpc<T>(
        &mut self,
        on_stdout: impl FnOnce(std::process::ChildStdout) -> Result<T, UpstreamError>,
    ) -> Result<T, UpstreamError> {
        let mut child = self.command(&["upload-pack", "--stateless-rpc"]).spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(&self.req)?;
        }
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| UpstreamError::Protocol("no stdout".into()))?;
        let result = on_stdout(stdout);
        let status = child.wait()?;
        match result {
            Ok(v) if status.success() => Ok(v),
            Ok(_) => {
                let mut err = String::new();
                if let Some(mut e) = child.stderr.take() {
                    let _ = e.read_to_string(&mut err);
                }
                Err(UpstreamError::Remote(format!(
                    "git upload-pack exited with {status}: {}",
                    err.trim()
                )))
            }
            Err(e) => Err(e),
        }
    }
}

impl Upstream for LocalGit {
    fn ls_ref(&mut self, refname: &str) -> Result<Option<Oid>, UpstreamError> {
        let caps = self.capabilities()?;
        ls_refs_request(&mut self.req, caps, refname)?;
        let body = self.rpc(|mut out| {
            let mut v = Vec::with_capacity(4096);
            out.read_to_end(&mut v)?;
            Ok(v)
        })?;
        parse_ls_refs(&body, refname)
    }

    fn fetch(
        &mut self,
        wants: &[Oid],
        haves: &[Oid],
        sink: &mut dyn Write,
    ) -> Result<u64, UpstreamError> {
        let caps = self.capabilities()?;
        fetch_request(&mut self.req, caps, wants, haves)?;
        self.rpc(|out| demux_fetch(out, sink))
    }

    fn describe(&self) -> String {
        format!("local:{}", self.repo.display())
    }
}

// ---------------------------------------------------------------------------------------
// Smart HTTP(S)
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Url {
    pub https: bool,
    pub host: String,
    pub port: u16,
    pub path: String,
}

impl Url {
    pub fn parse(s: &str) -> Option<Self> {
        let (https, rest) = match (s.strip_prefix("https://"), s.strip_prefix("http://")) {
            (Some(r), _) => (true, r),
            (None, Some(r)) => (false, r),
            (None, None) => return None,
        };
        let (hostport, path) = match rest.find('/') {
            Some(i) => (rest.get(..i)?, rest.get(i..)?),
            None => (rest, "/"),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse::<u16>().ok()?),
            None => (hostport.to_string(), if https { 443 } else { 80 }),
        };
        if host.is_empty() {
            return None;
        }
        let path = path.trim_end_matches('/').to_string();
        Some(Self {
            https,
            host,
            port,
            path: if path.is_empty() { String::new() } else { path },
        })
    }
}

/// `Authorization` header value.
#[derive(Debug, Clone)]
pub enum Auth {
    None,
    /// `Basic base64(user:token)`; Azure DevOps PATs use an empty user, GitHub tokens
    /// `x-access-token`.
    Basic {
        user: String,
        token: String,
    },
    Bearer(String),
}

impl Auth {
    fn header(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::Basic { user, token } => Some(format!(
                "Basic {}",
                base64(format!("{user}:{token}").as_bytes())
            )),
            Self::Bearer(t) => Some(format!("Bearer {t}")),
        }
    }
}

pub fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len().saturating_mul(4) / 3).saturating_add(4));
    let ch = |i: usize| char::from(T.get(i & 63).copied().unwrap_or(b'A'));
    for chunk in data.chunks(3) {
        let b0 = usize::from(chunk.first().copied().unwrap_or(0));
        let b1 = usize::from(chunk.get(1).copied().unwrap_or(0));
        let b2 = usize::from(chunk.get(2).copied().unwrap_or(0));
        out.push(ch(b0 >> 2));
        out.push(ch(((b0 & 3) << 4) | (b1 >> 4)));
        out.push(if chunk.len() > 1 {
            ch(((b1 & 15) << 2) | (b2 >> 6))
        } else {
            '='
        });
        out.push(if chunk.len() > 2 { ch(b2) } else { '=' });
    }
    out
}

enum Stream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

/// Body framing of an HTTP/1.1 response.
#[derive(Clone, Copy)]
enum Framing {
    Length(u64),
    Chunked,
    UntilClose,
}

/// A response with its head parsed: lower-cased header names, body still on the wire.
pub struct HttpResponse {
    pub status: u16,
    pub status_line: String,
    pub headers: Vec<(String, String)>,
    pub body: HttpBody,
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// A `Read` over an HTTP response body (chunked, content-length or until-close).
pub struct HttpBody {
    stream: Stream,
    framing: Framing,
    /// Bytes already read past the headers.
    pending: Vec<u8>,
    pending_pos: usize,
    remaining_in_chunk: u64,
    chunked_done: bool,
    need_chunk_header: bool,
}

impl HttpBody {
    fn read_raw(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pending_pos < self.pending.len() {
            let src = self.pending.get(self.pending_pos..).unwrap_or(&[]);
            let n = src.len().min(buf.len());
            buf.get_mut(..n)
                .unwrap_or(&mut [])
                .copy_from_slice(src.get(..n).unwrap_or(&[]));
            self.pending_pos = self.pending_pos.saturating_add(n);
            return Ok(n);
        }
        self.stream.read(buf)
    }

    fn read_line(&mut self) -> std::io::Result<Vec<u8>> {
        let mut line = Vec::with_capacity(32);
        let mut b = [0u8; 1];
        loop {
            let n = self.read_raw(&mut b)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof in chunk header",
                ));
            }
            if b == *b"\n" {
                break;
            }
            if line.len() > 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunk header too long",
                ));
            }
            line.extend_from_slice(&b);
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        Ok(line)
    }
}

impl Read for HttpBody {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.framing {
            Framing::UntilClose => self.read_raw(buf),
            Framing::Length(remaining) => {
                if remaining == 0 {
                    return Ok(0);
                }
                let want = buf
                    .len()
                    .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                let n = self.read_raw(buf.get_mut(..want).unwrap_or(&mut []))?;
                self.framing = Framing::Length(remaining.saturating_sub(n as u64));
                Ok(n)
            }
            Framing::Chunked => {
                {
                    if self.chunked_done {
                        return Ok(0);
                    }
                    if self.remaining_in_chunk == 0 {
                        if !self.need_chunk_header {
                            // Consume the CRLF that ends the previous chunk.
                            let _ = self.read_line()?;
                        }
                        self.need_chunk_header = false;
                        let line = self.read_line()?;
                        let hex = line.split(|&c| c == b';').next().unwrap_or(&[]);
                        let size = core::str::from_utf8(hex)
                            .ok()
                            .and_then(|s| u64::from_str_radix(s.trim(), 16).ok())
                            .ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "bad chunk size",
                                )
                            })?;
                        if size == 0 {
                            // Trailers until an empty line.
                            loop {
                                if self.read_line()?.is_empty() {
                                    break;
                                }
                            }
                            self.chunked_done = true;
                            return Ok(0);
                        }
                        self.remaining_in_chunk = size;
                    }
                    let want = buf
                        .len()
                        .min(usize::try_from(self.remaining_in_chunk).unwrap_or(usize::MAX));
                    let n = self.read_raw(buf.get_mut(..want).unwrap_or(&mut []))?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "eof inside chunk",
                        ));
                    }
                    self.remaining_in_chunk = self.remaining_in_chunk.saturating_sub(n as u64);
                    Ok(n)
                }
            }
        }
    }
}

/// Smart-HTTP Git upstream with a from-scratch HTTP/1.1 client (one connection per
/// request; the pipeline polls every few seconds at most).
pub struct SmartHttp {
    url: Url,
    auth: Auth,
    tls: Option<Arc<rustls::ClientConfig>>,
    caps: Option<Caps>,
    req: Vec<u8>,
    timeout: Duration,
}

impl SmartHttp {
    pub fn new(url: Url, auth: Auth) -> Result<Self, UpstreamError> {
        let tls = if url.https {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let cfg = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            Some(Arc::new(cfg))
        } else {
            None
        };
        Ok(Self {
            url,
            auth,
            tls,
            caps: None,
            req: Vec::with_capacity(16 << 10),
            timeout: Duration::from_secs(120),
        })
    }

    fn connect(&self) -> Result<Stream, UpstreamError> {
        let addr = (self.url.host.as_str(), self.url.port);
        let tcp = TcpStream::connect(addr)?;
        tcp.set_read_timeout(Some(self.timeout))?;
        tcp.set_write_timeout(Some(self.timeout))?;
        tcp.set_nodelay(true)?;
        match &self.tls {
            None => Ok(Stream::Plain(tcp)),
            Some(cfg) => {
                let name = rustls_pki_types::ServerName::try_from(self.url.host.clone())
                    .map_err(|e| UpstreamError::Tls(e.to_string()))?;
                let conn = rustls::ClientConnection::new(Arc::clone(cfg), name)
                    .map_err(|e| UpstreamError::Tls(e.to_string()))?;
                Ok(Stream::Tls(Box::new(rustls::StreamOwned::new(conn, tcp))))
            }
        }
    }

    /// Sends one request and returns the response body reader after checking the status.
    fn request(
        &self,
        method: &str,
        path_and_query: &str,
        content_type: Option<&str>,
        body: &[u8],
        ctx: &str,
    ) -> Result<HttpBody, UpstreamError> {
        let r = self.request_full(method, path_and_query, content_type, body)?;
        if !(200..300).contains(&r.status) {
            let mut body = r.body;
            let mut excerpt = vec![0u8; 512];
            let n = body.read(&mut excerpt).unwrap_or(0);
            let text = String::from_utf8_lossy(excerpt.get(..n).unwrap_or(&[]))
                .trim()
                .to_string();
            return Err(UpstreamError::Http(
                r.status,
                format!("{ctx}: {} {text}", r.status_line),
            ));
        }
        Ok(r.body)
    }

    /// `GET path_and_query` under the base URL, any status: the caller reads the headers
    /// and streams the body (the client prefetch hook).
    pub fn get(&self, path_and_query: &str) -> Result<HttpResponse, UpstreamError> {
        let full = format!("{}{path_and_query}", self.url.path);
        self.request_full("GET", &full, None, &[])
    }

    fn request_full(
        &self,
        method: &str,
        path_and_query: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<HttpResponse, UpstreamError> {
        let mut stream = self.connect()?;
        let mut head = format!(
            "{method} {path_and_query} HTTP/1.1\r\nHost: {}\r\nUser-Agent: gvfs-daemon/0.1\r\nGit-Protocol: version=2\r\nAccept: application/x-git-upload-pack-result\r\nConnection: close\r\n",
            self.url.host
        );
        if let Some(a) = self.auth.header() {
            head.push_str(&format!("Authorization: {a}\r\n"));
        }
        if let Some(ct) = content_type {
            head.push_str(&format!(
                "Content-Type: {ct}\r\nContent-Length: {}\r\n",
                body.len()
            ));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes())?;
        if !body.is_empty() {
            stream.write_all(body)?;
        }
        stream.flush()?;
        // Read the response head.
        let mut buf = Vec::with_capacity(8192);
        let mut tmp = [0u8; 4096];
        let header_end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i.saturating_add(4);
            }
            if buf.len() > 256 << 10 {
                return Err(UpstreamError::Protocol("response headers too large".into()));
            }
            let n = stream.read(&mut tmp)?;
            if n == 0 {
                return Err(UpstreamError::Protocol(
                    "connection closed before response headers".into(),
                ));
            }
            buf.extend_from_slice(tmp.get(..n).unwrap_or(&[]));
        };
        let head_bytes = buf.get(..header_end).unwrap_or(&[]).to_vec();
        let head_str = String::from_utf8_lossy(&head_bytes);
        let mut lines = head_str.split("\r\n");
        let status_line = lines.next().unwrap_or("");
        let status: u16 = status_line
            .split(' ')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let mut framing = Framing::UntilClose;
        let mut headers = Vec::new();
        for l in lines {
            let Some((k, v)) = l.split_once(':') else {
                continue;
            };
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            if k == "content-length" {
                framing = Framing::Length(v.parse().unwrap_or(0));
            } else if k == "transfer-encoding" && v.to_ascii_lowercase().contains("chunked") {
                framing = Framing::Chunked;
            }
            headers.push((k, v.to_string()));
        }
        let body = HttpBody {
            stream,
            framing,
            pending: buf,
            pending_pos: header_end,
            remaining_in_chunk: 0,
            chunked_done: false,
            need_chunk_header: true,
        };
        Ok(HttpResponse {
            status,
            status_line: status_line.to_string(),
            headers,
            body,
        })
    }

    fn capabilities(&mut self) -> Result<Caps, UpstreamError> {
        if let Some(c) = self.caps {
            return Ok(c);
        }
        let path = format!("{}/info/refs?service=git-upload-pack", self.url.path);
        let mut body = self.request("GET", &path, None, &[], "info/refs")?;
        let mut v = Vec::with_capacity(16 << 10);
        body.read_to_end(&mut v)?;
        let caps = parse_caps(&v)?;
        self.caps = Some(caps);
        Ok(caps)
    }

    fn post(&self, ctx: &str) -> Result<HttpBody, UpstreamError> {
        let path = format!("{}/git-upload-pack", self.url.path);
        self.request(
            "POST",
            &path,
            Some("application/x-git-upload-pack-request"),
            &self.req,
            ctx,
        )
    }

    /// `POST /gvfs/objects` on the upstream (Azure DevOps, or another gvfs-daemon): a plain
    /// pack of `oids` (commits with their trees). `cause` is why protocol v2 did not do.
    fn gvfs_objects(
        &self,
        oids: &[Oid],
        sink: &mut dyn Write,
        cause: UpstreamError,
    ) -> Result<u64, UpstreamError> {
        let mut json = String::with_capacity(oids.len().saturating_mul(44).saturating_add(64));
        json.push_str("{\"commitDepth\":1,\"objectIds\":[");
        for (i, o) in oids.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            json.push('"');
            json.push_str(&String::from_utf8_lossy(&o.to_hex()));
            json.push('"');
        }
        json.push_str("]}");
        let path = format!("{}/gvfs/objects", self.url.path);
        let mut body = self
            .request(
                "POST",
                &path,
                Some("application/json"),
                json.as_bytes(),
                "gvfs/objects",
            )
            .map_err(|e| {
                UpstreamError::Protocol(format!(
                    "fetch failed ({cause}); gvfs/objects fallback failed too ({e})"
                ))
            })?;
        Ok(std::io::copy(&mut body, sink)?)
    }
}

impl Upstream for SmartHttp {
    fn ls_ref(&mut self, refname: &str) -> Result<Option<Oid>, UpstreamError> {
        let caps = self.capabilities()?;
        ls_refs_request(&mut self.req, caps, refname)?;
        let mut body = self.post("ls-refs")?;
        let mut v = Vec::with_capacity(4096);
        body.read_to_end(&mut v)?;
        parse_ls_refs(&v, refname)
    }

    fn fetch(
        &mut self,
        wants: &[Oid],
        haves: &[Oid],
        sink: &mut dyn Write,
    ) -> Result<u64, UpstreamError> {
        let caps = self.capabilities()?;
        fetch_request(&mut self.req, caps, wants, haves)?;
        let body = match self.post("fetch") {
            Ok(b) => b,
            // A server that refuses arbitrary `want`s (no allowAnySHA1InWant) but speaks the
            // GVFS protocol itself serves the same objects through its own endpoint.
            Err(e) if wants.len() > 1 => {
                return self.gvfs_objects(wants.get(1..).unwrap_or(&[]), sink, e);
            }
            Err(e) => return Err(e),
        };
        match demux_fetch(body, sink) {
            Err(e) if wants.len() > 1 => self.gvfs_objects(wants.get(1..).unwrap_or(&[]), sink, e),
            r => r,
        }
    }

    fn describe(&self) -> String {
        format!(
            "{}://{}:{}{}",
            if self.url.https { "https" } else { "http" },
            self.url.host,
            self.url.port,
            self.url.path
        )
    }
}

/// `--upstream` accepts an http(s) URL or a local path.
pub fn open_upstream(spec: &str, auth: Auth) -> Result<Box<dyn Upstream + Send>, UpstreamError> {
    match Url::parse(spec) {
        Some(url) => Ok(Box::new(SmartHttp::new(url, auth)?)),
        None => {
            let p = std::path::Path::new(spec);
            if !p.exists() {
                return Err(UpstreamError::Protocol(format!(
                    "upstream '{spec}' is neither a URL nor an existing path"
                )));
            }
            Ok(Box::new(LocalGit::new(p)))
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn url_and_base64() {
        let u = Url::parse("https://dev.azure.com/org/proj/_git/repo").unwrap();
        assert_eq!(
            (u.https, u.host.as_str(), u.port, u.path.as_str()),
            (true, "dev.azure.com", 443, "/org/proj/_git/repo")
        );
        let u = Url::parse("http://127.0.0.1:9000/repo.git/").unwrap();
        assert_eq!(
            (u.https, u.port, u.path.as_str()),
            (false, 9000, "/repo.git")
        );
        assert!(Url::parse("/home/x").is_none());
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b":pat"), "OnBhdA==");
    }

    #[test]
    fn demux_reassembles_side_band_at_every_split() {
        let pack: Vec<u8> = (0..3000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let mut resp = Vec::with_capacity(1 << 16);
        push_pkt(&mut resp, &[b"acknowledgments\n"]).unwrap();
        push_pkt(&mut resp, &[b"ready\n"]).unwrap();
        resp.extend_from_slice(DELIM_PKT);
        push_pkt(&mut resp, &[b"packfile\n"]).unwrap();
        let mut at = 0usize;
        for size in [1usize, 2, 7, 100, 999, 1, 1500, 390] {
            let end = (at + size).min(pack.len());
            push_pkt(&mut resp, &[&[1u8], &pack[at..end]]).unwrap();
            push_pkt(&mut resp, &[&[2u8], b"Counting\r"]).unwrap();
            at = end;
        }
        assert_eq!(at, pack.len());
        resp.extend_from_slice(FLUSH_PKT);
        struct Chunky<'a>(&'a [u8], usize, usize);
        impl Read for Chunky<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = self.2.min(buf.len()).min(self.0.len() - self.1);
                buf[..n].copy_from_slice(&self.0[self.1..self.1 + n]);
                self.1 += n;
                Ok(n)
            }
        }
        for w in [1usize, 3, 5, 64, 4096] {
            let mut out = Vec::new();
            let n = demux_fetch(Chunky(&resp, 0, w), &mut out).unwrap();
            assert_eq!(out, pack, "chunks of {w}");
            assert_eq!(n as usize, pack.len());
        }
        let mut out = Vec::new();
        assert!(demux_fetch(Chunky(&resp[..resp.len() - 10], 0, 7), &mut out).is_err());
        let mut err = Vec::with_capacity(1024);
        push_pkt(&mut err, &[b"packfile\n"]).unwrap();
        push_pkt(&mut err, &[&[3u8], b"upload-pack: not our ref"]).unwrap();
        let e = demux_fetch(Chunky(&err, 0, 100), &mut out).unwrap_err();
        assert!(e.to_string().contains("not our ref"), "{e}");
    }

    #[test]
    fn caps_and_ls_refs_parsing() {
        let mut body = Vec::with_capacity(4096);
        push_pkt(&mut body, &[b"# service=git-upload-pack\n"]).unwrap();
        body.extend_from_slice(FLUSH_PKT);
        push_pkt(&mut body, &[b"version 2\n"]).unwrap();
        push_pkt(&mut body, &[b"agent=git/2.x\n"]).unwrap();
        push_pkt(&mut body, &[b"ls-refs=unborn\n"]).unwrap();
        push_pkt(&mut body, &[b"fetch=shallow wait-for-done\n"]).unwrap();
        push_pkt(&mut body, &[b"object-format=sha1\n"]).unwrap();
        body.extend_from_slice(FLUSH_PKT);
        assert_eq!(
            parse_caps(&body).unwrap(),
            Caps {
                object_format: true
            }
        );
        let mut v0 = Vec::with_capacity(4096);
        push_pkt(
            &mut v0,
            &[b"0123456789012345678901234567890123456789 HEAD\0multi_ack\n"],
        )
        .unwrap();
        assert!(parse_caps(&v0).is_err());
        let mut refs = Vec::with_capacity(4096);
        push_pkt(
            &mut refs,
            &[b"0123456789012345678901234567890123456789 refs/heads/main\n"],
        )
        .unwrap();
        push_pkt(
            &mut refs,
            &[b"abcdef0123456789012345678901234567890123 refs/heads/main2\n"],
        )
        .unwrap();
        refs.extend_from_slice(FLUSH_PKT);
        assert_eq!(
            parse_ls_refs(&refs, "refs/heads/main").unwrap(),
            Oid::from_hex(b"0123456789012345678901234567890123456789")
        );
        assert_eq!(parse_ls_refs(&refs, "refs/heads/nope").unwrap(), None);
    }
}
