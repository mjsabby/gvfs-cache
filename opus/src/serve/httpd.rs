//! The server side of HTTP/1.1, in-house: just what the GVFS protocol needs, on tokio
//! sockets.
//!
//! Request heads are parsed in the connection's head buffer into ranges (no allocation);
//! bodies (Content-Length or chunked) are read into the request arena; response heads are
//! formatted on the stack. [`Out`] coalesces small pieces into one buffer and sends large
//! file-backed ranges with `sendfile(2)` straight from the page cache whenever the socket
//! is writable: a slow reader parks its task, never a thread. Every wait has a deadline
//! ([`Deadlines`]).

use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::os::fd::AsFd;
use std::time::Duration;

use tokio::io::Interest;
use tokio::net::TcpStream;

use crate::slab::Slab;

pub const HEAD_MAX: usize = 16 << 10;
/// File ranges at least this long go out with sendfile.
const SENDFILE_MIN: u64 = 16 << 10;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    Get,
    Head,
    Post,
    Other,
}

/// A parsed request head; byte ranges point into the connection's head buffer.
#[derive(Clone, Copy, Debug)]
pub struct Request {
    pub method: Method,
    pub path: (usize, usize),
    pub query: (usize, usize),
    pub content_length: Option<u64>,
    pub chunked: bool,
    pub keep_alive: bool,
    pub expect_continue: bool,
    /// An `Accept` header names `application/x-git-loose-object`.
    pub accepts_loose: bool,
    /// An `Accept` header names `application/x-git-packfile`.
    pub accepts_pack: bool,
    /// `User-Agent: git/...`: git itself (gvfs-helper), which always indexes packs itself.
    pub git_agent: bool,
    /// `X-GVFS-Sizes: 1`: the client wants the blob sizes for a prefetch's trees (a `GSZ1`
    /// section after the packs).
    pub wants_sizes: bool,
    pub head_len: usize,
}

/// How long a connection may wait for its client.
#[derive(Clone, Copy, Debug)]
pub struct Deadlines {
    /// Between requests (`--idle-secs`).
    pub idle: Duration,
    /// For the rest of a request once it has begun (head or body).
    pub patience: Duration,
    /// For the client to take more of a response.
    pub write: Duration,
}

/// A connection's head buffer: a request head plus whatever followed it on the wire.
pub struct Input {
    pub buf: Vec<u8>,
    pub filled: usize,
}

/// What [`read_head`] found on a connection.
pub enum Head {
    Request(Request),
    /// Woken with nothing to read and nothing buffered (the connection is still idle).
    Idle,
    /// The client closed the connection between requests.
    Closed,
}

impl Input {
    pub fn new() -> Input {
        Input {
            buf: vec![0u8; HEAD_MAX],
            filled: 0,
        }
    }

    pub fn bytes(&self, r: (usize, usize)) -> &[u8] {
        self.buf.get(r.0..r.1).unwrap_or(&[])
    }

    /// Drops the first `n` bytes (a consumed request), keeping pipelined bytes.
    pub fn consume(&mut self, n: usize) {
        let n = n.min(self.filled);
        self.buf.copy_within(n..self.filled, 0);
        self.filled = self.filled.saturating_sub(n);
    }
}

impl Default for Input {
    fn default() -> Self {
        Self::new()
    }
}

/// `f`, or `TimedOut` once `t` has passed.
pub async fn within(
    t: Duration,
    f: impl Future<Output = std::io::Result<()>>,
) -> std::io::Result<()> {
    match tokio::time::timeout(t, f).await {
        Ok(r) => r,
        Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
    }
}

/// Reads one request head: [`Head::Idle`] if nothing is readable after all, [`Head::Closed`]
/// on a clean close between requests. Once part of a head is here, the rest must follow
/// within `patience`.
pub async fn read_head(
    stream: &TcpStream,
    input: &mut Input,
    patience: Duration,
) -> std::io::Result<Head> {
    loop {
        if let Some(end) = find_head_end(input.buf.get(..input.filled).unwrap_or(&[])) {
            return parse_head(input.buf.get(..end).unwrap_or(&[]), end).map(Head::Request);
        }
        if input.filled >= input.buf.len() {
            return Err(bad("request head too large"));
        }
        match stream.try_read(input.buf.get_mut(input.filled..).unwrap_or(&mut [])) {
            Ok(0) if input.filled == 0 => return Ok(Head::Closed),
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => input.filled = input.filled.saturating_add(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if input.filled == 0 {
                    return Ok(Head::Idle);
                }
                within(patience, stream.readable()).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

/// The length of the request head at the start of `b` (through its blank line), if whole.
pub fn find_head_end(b: &[u8]) -> Option<usize> {
    b.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .and_then(|p| p.checked_add(4))
}

/// An `InvalidData` error with `msg`, built in a cold section: a malformed request is no
/// reason to fail the allocator's hot-path check (a strict-mode edge would abort).
pub fn bad(msg: &'static str) -> std::io::Error {
    let _cold = gvfs_alloc_guard::cold_section();
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

/// [`std::io::Error::other`] with `msg`, built in a cold section (see [`bad`]).
pub fn other(msg: impl std::fmt::Display) -> std::io::Error {
    let _cold = gvfs_alloc_guard::cold_section();
    std::io::Error::other(msg.to_string())
}

/// Parses a whole request head (`head_len` bytes, as [`find_head_end`] measured).
pub fn parse_head(head: &[u8], head_len: usize) -> std::io::Result<Request> {
    let line_end = head
        .iter()
        .position(|&b| b == b'\r' || b == b'\n')
        .ok_or_else(|| bad("empty request"))?;
    let request_line = head.get(..line_end).unwrap_or(&[]);
    let sp1 = request_line
        .iter()
        .position(|&b| b == b' ')
        .ok_or_else(|| bad("no request target"))?;
    let target_start = sp1.saturating_add(1);
    let target_end = request_line
        .iter()
        .skip(target_start)
        .position(|&b| b == b' ')
        .map_or(line_end, |p| target_start.saturating_add(p));
    let method = match request_line.get(..sp1) {
        Some(b"GET") => Method::Get,
        Some(b"HEAD") => Method::Head,
        Some(b"POST") => Method::Post,
        _ => Method::Other,
    };
    let target = head.get(target_start..target_end).unwrap_or(&[]);
    let version = request_line
        .get(target_end.saturating_add(1)..)
        .unwrap_or(b"HTTP/1.0");
    let lines = head
        .get(line_end..)
        .unwrap_or(&[])
        .split(|&b| b == b'\n')
        .skip(1)
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l));
    let (path, query) = match target.iter().position(|&b| b == b'?') {
        Some(q) => {
            let q_at = target_start.saturating_add(q);
            ((target_start, q_at), (q_at.saturating_add(1), target_end))
        }
        None => ((target_start, target_end), (target_end, target_end)),
    };
    let mut req = Request {
        method,
        path,
        query,
        content_length: None,
        chunked: false,
        keep_alive: version == b"HTTP/1.1",
        expect_continue: false,
        accepts_loose: false,
        accepts_pack: false,
        git_agent: false,
        wants_sizes: false,
        head_len,
    };
    for line in lines {
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            continue;
        };
        let (name, value) = line.split_at(colon);
        let value = value.get(1..).unwrap_or(&[]).trim_ascii();
        if name.eq_ignore_ascii_case(b"content-length") {
            let v = std::str::from_utf8(value)
                .ok()
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| bad("bad content-length"))?;
            req.content_length = Some(v);
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            req.chunked = value.eq_ignore_ascii_case(b"chunked");
        } else if name.eq_ignore_ascii_case(b"connection") {
            if value.eq_ignore_ascii_case(b"close") {
                req.keep_alive = false;
            } else if value.eq_ignore_ascii_case(b"keep-alive") {
                req.keep_alive = true;
            }
        } else if name.eq_ignore_ascii_case(b"expect") {
            req.expect_continue = value.eq_ignore_ascii_case(b"100-continue");
        } else if name.eq_ignore_ascii_case(b"accept") {
            // gvfs-helper sends one `Accept` line per type.
            req.accepts_loose |= value
                .windows(24)
                .any(|w| w.eq_ignore_ascii_case(b"application/x-git-loose-"));
            req.accepts_pack |= value
                .windows(26)
                .any(|w| w.eq_ignore_ascii_case(b"application/x-git-packfile"));
        } else if name.eq_ignore_ascii_case(b"user-agent") {
            req.git_agent = value.starts_with(b"git/");
        } else if name.eq_ignore_ascii_case(b"x-gvfs-sizes") {
            req.wants_sizes = value == b"1";
        }
    }
    Ok(req)
}

/// Reads the request body into `body` (whose capacity bounds it). Returns false if the
/// body does not fit (the caller answers 413 and closes).
pub async fn read_body(
    stream: &TcpStream,
    input: &mut Input,
    req: &Request,
    body: &mut Vec<u8>,
    d: &Deadlines,
) -> std::io::Result<bool> {
    body.clear();
    if req.expect_continue {
        write_all(stream, b"HTTP/1.1 100 Continue\r\n\r\n", d.write).await?;
    }
    input.consume(req.head_len);
    if req.chunked {
        loop {
            let n = line_len(stream, input, d.patience).await?;
            let line = input.buf.get(..n).unwrap_or(&[]);
            let hex = line
                .split(|&b| b == b';')
                .next()
                .unwrap_or(&[])
                .trim_ascii();
            let size = std::str::from_utf8(hex)
                .ok()
                .and_then(|h| usize::from_str_radix(h, 16).ok())
                .ok_or_else(|| bad("bad chunk size"))?;
            input.consume(n.saturating_add(2));
            if size == 0 {
                // Trailers, then the empty line.
                loop {
                    let t = line_len(stream, input, d.patience).await?;
                    input.consume(t.saturating_add(2));
                    if t == 0 {
                        return Ok(true);
                    }
                }
            }
            if body.len().saturating_add(size) > body.capacity() {
                return Ok(false);
            }
            read_exact_into(stream, input, body, size, d.patience).await?;
            if line_len(stream, input, d.patience).await? != 0 {
                return Err(bad("missing chunk terminator"));
            }
            input.consume(2);
        }
    }
    let len = usize::try_from(req.content_length.unwrap_or(0)).unwrap_or(usize::MAX);
    if len > body.capacity() {
        return Ok(false);
    }
    read_exact_into(stream, input, body, len, d.patience).await?;
    Ok(true)
}

/// Length of the CRLF-terminated line at the start of the input (reading more as needed).
async fn line_len(
    stream: &TcpStream,
    input: &mut Input,
    patience: Duration,
) -> std::io::Result<usize> {
    loop {
        if let Some(p) = input
            .buf
            .get(..input.filled)
            .unwrap_or(&[])
            .windows(2)
            .position(|w| w == b"\r\n")
        {
            return Ok(p);
        }
        if input.filled >= input.buf.len() {
            return Err(bad("line too long"));
        }
        let n = read_some(
            stream,
            input.buf.get_mut(input.filled..).unwrap_or(&mut []),
            patience,
        )
        .await?;
        input.filled = input.filled.saturating_add(n);
    }
}

/// At least one byte into `buf` (waiting up to `patience` for it).
async fn read_some(
    stream: &TcpStream,
    buf: &mut [u8],
    patience: Duration,
) -> std::io::Result<usize> {
    loop {
        match stream.try_read(buf) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                within(patience, stream.readable()).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

/// Moves `n` body bytes into `body`: buffered ones first, then straight from the socket.
async fn read_exact_into(
    stream: &TcpStream,
    input: &mut Input,
    body: &mut Vec<u8>,
    n: usize,
    patience: Duration,
) -> std::io::Result<()> {
    let from_buf = n.min(input.filled);
    body.extend_from_slice(input.buf.get(..from_buf).unwrap_or(&[]));
    input.consume(from_buf);
    let mut at = body.len();
    let end = at.saturating_add(n.saturating_sub(from_buf));
    body.resize(end, 0);
    while at < end {
        let got = read_some(stream, body.get_mut(at..end).unwrap_or(&mut []), patience).await?;
        at = at.saturating_add(got);
    }
    Ok(())
}

/// All of `b`, waiting up to `t` each time the socket is full.
pub async fn write_all(stream: &TcpStream, mut b: &[u8], t: Duration) -> std::io::Result<()> {
    while !b.is_empty() {
        match stream.try_write(b) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) => b = b.get(n..).unwrap_or(&[]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                within(t, stream.writable()).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// `a` then `b`, with as few syscalls as possible (one, unless the socket buffer fills).
async fn write_all2(stream: &TcpStream, a: &[u8], b: &[u8], t: Duration) -> std::io::Result<()> {
    let (mut a, mut b) = (a, b);
    while !a.is_empty() {
        match stream.try_write_vectored(&[std::io::IoSlice::new(a), std::io::IoSlice::new(b)]) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) if n < a.len() => a = a.get(n..).unwrap_or(&[]),
            Ok(n) => {
                b = b.get(n.saturating_sub(a.len())..).unwrap_or(&[]);
                a = &[];
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                within(t, stream.writable()).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    write_all(stream, b, t).await
}

/// Writes a whole response, head and body, in one `writev` when the socket takes it.
pub async fn write_response(
    stream: &TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    keep_alive: bool,
    t: Duration,
) -> std::io::Result<()> {
    let mut buf = [0u8; 320];
    let n = format_head(
        &mut buf,
        status,
        content_type,
        crate::util::u64_of(body.len()),
        keep_alive,
    );
    write_all2(stream, buf.get(..n).unwrap_or(&[]), body, t).await
}

/// Formats a response head into `buf`; returns its length.
pub fn format_head(
    buf: &mut [u8],
    status: u16,
    content_type: &str,
    len: u64,
    keep_alive: bool,
) -> usize {
    format_head_with(buf, status, content_type, len, keep_alive, b"")
}

/// [`format_head`] with `extra` header lines (each ending in CRLF) before the blank line.
pub fn format_head_with(
    buf: &mut [u8],
    status: u16,
    content_type: &str,
    len: u64,
    keep_alive: bool,
    extra: &[u8],
) -> usize {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "",
    };
    let mut cur = std::io::Cursor::new(buf);
    let _ = write!(
        cur,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {len}\r\nConnection: {}\r\n",
        if keep_alive { "keep-alive" } else { "close" }
    );
    let _ = cur.write_all(extra);
    let _ = cur.write_all(b"\r\n");
    usize::try_from(cur.position()).unwrap_or(0)
}

/// A small plain-text response.
pub async fn respond_text(
    stream: &TcpStream,
    status: u16,
    text: &str,
    keep_alive: bool,
    t: Duration,
) -> std::io::Result<()> {
    write_response(stream, status, "text/plain", text.as_bytes(), keep_alive, t).await
}

/// Response body writer: coalesces small writes, sendfiles large file ranges.
pub struct Out<'a> {
    stream: &'a TcpStream,
    buf: &'a mut Vec<u8>,
    write: Duration,
    pub sent: u64,
}

impl<'a> Out<'a> {
    /// `buf` is the connection's output buffer (its capacity is the batch size); `write`
    /// bounds each wait for the client to take more.
    pub fn new(stream: &'a TcpStream, buf: &'a mut Vec<u8>, write: Duration) -> Out<'a> {
        buf.clear();
        Out {
            stream,
            buf,
            write,
            sent: 0,
        }
    }

    /// The response head, into the (empty) buffer as the start of the body: small responses
    /// leave in one write. Not counted in `sent` (body bytes only).
    pub fn head(
        &mut self,
        status: u16,
        content_type: &str,
        len: u64,
        keep_alive: bool,
    ) -> std::io::Result<()> {
        self.head_with(status, content_type, len, keep_alive, b"")
    }

    /// [`Out::head`] with `extra` header lines (each ending in CRLF).
    pub fn head_with(
        &mut self,
        status: u16,
        content_type: &str,
        len: u64,
        keep_alive: bool,
        extra: &[u8],
    ) -> std::io::Result<()> {
        let mut h = [0u8; 320];
        let n = format_head_with(&mut h, status, content_type, len, keep_alive, extra);
        if self.put_raw(h.get(..n).unwrap_or(&[])) {
            Ok(())
        } else {
            Err(other("response head does not fit the output buffer"))
        }
    }

    /// `b` into the buffer if it fits there (true); else nothing (then [`Out::bytes`]).
    ///
    /// Synchronous, for per-object loops: an `.await` per small piece stores its arguments
    /// into the future and reads them back at once, a load that must wait for the store
    /// buffer to drain (the previous piece's copy). That made a 4,000-blob batch 17% slower.
    #[inline]
    pub fn put(&mut self, b: &[u8]) -> bool {
        if self.put_raw(b) {
            self.sent = self.sent.saturating_add(crate::util::u64_of(b.len()));
            true
        } else {
            false
        }
    }

    #[inline]
    fn put_raw(&mut self, b: &[u8]) -> bool {
        if self.buf.len().saturating_add(b.len()) > self.buf.capacity() {
            return false;
        }
        self.buf.extend_from_slice(b);
        true
    }

    pub async fn bytes(&mut self, b: &[u8]) -> std::io::Result<()> {
        if self.put(b) {
            return Ok(());
        }
        self.flush().await?;
        if b.len() > self.buf.capacity() {
            write_all(self.stream, b, self.write).await?;
        } else {
            self.buf.extend_from_slice(b);
        }
        self.sent = self.sent.saturating_add(crate::util::u64_of(b.len()));
        Ok(())
    }

    /// `slab.pack()[start .. start + len]`.
    pub async fn slab(&mut self, slab: &Slab, start: u64, len: u64) -> std::io::Result<()> {
        self.mapped(slab.pack_file(), slab.pack(), start, len).await
    }

    /// `len` bytes at `start` of an immutable file mapped as `bytes` (`sendfile(2)` from
    /// `file` when large).
    pub async fn mapped(
        &mut self,
        file: Option<&File>,
        bytes: &[u8],
        start: u64,
        len: u64,
    ) -> std::io::Result<()> {
        match file {
            Some(file) if len >= SENDFILE_MIN => {
                if crate::util::us(start.saturating_add(len)) > bytes.len() {
                    return Err(bad("file range"));
                }
                self.flush().await?;
                sendfile_all(self.stream, file, start, len, self.write).await?;
                self.sent = self.sent.saturating_add(len);
                Ok(())
            }
            _ => {
                let range = crate::util::sub(bytes, crate::util::us(start), crate::util::us(len))
                    .ok_or_else(|| bad("file range"))?;
                self.bytes(range).await
            }
        }
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            write_all(self.stream, self.buf, self.write).await?;
            self.buf.clear();
        }
        Ok(())
    }
}

/// `sendfile(2)` whenever the socket is writable (waiting up to `t` each time it is full)
/// until `len` bytes of `file` from `offset` are in it.
pub async fn sendfile_all(
    stream: &TcpStream,
    file: &File,
    offset: u64,
    len: u64,
    t: Duration,
) -> std::io::Result<()> {
    let mut off = i64::try_from(offset).map_err(|_| bad("offset"))?;
    let mut left = len;
    while left > 0 {
        let chunk = crate::util::us(left.min(1 << 30));
        let sent = stream.try_io(Interest::WRITABLE, || {
            nix::sys::sendfile::sendfile64(stream.as_fd(), file.as_fd(), Some(&mut off), chunk)
                .map_err(std::io::Error::from)
        });
        match sent {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => left = left.saturating_sub(crate::util::u64_of(n)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                within(t, stream.writable()).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_heads() {
        let h = b"POST /org/repo/gvfs/objects?x=1 HTTP/1.1\r\nHost: a\r\nContent-Length: 12\r\nExpect: 100-continue\r\nAccept: application/x-git-loose-object\r\n\r\n";
        let r = parse_head(h, h.len()).unwrap();
        assert_eq!(r.method, Method::Post);
        assert_eq!(&h[r.path.0..r.path.1], b"/org/repo/gvfs/objects");
        assert_eq!(&h[r.query.0..r.query.1], b"x=1");
        assert_eq!(
            (
                r.content_length,
                r.keep_alive,
                r.expect_continue,
                r.accepts_loose
            ),
            (Some(12), true, true, true)
        );
        assert!(!r.git_agent && !r.accepts_pack);
        // gvfs-helper: one Accept line per type.
        let h = b"POST /gvfs/objects HTTP/1.1\r\nAccept: application/x-git-packfile\r\nAccept: application/x-git-loose-object\r\n\r\n";
        let r = parse_head(h, h.len()).unwrap();
        assert!(r.accepts_pack && r.accepts_loose);
        let h = b"POST /gvfs/objects HTTP/1.1\r\nAccept: application/x-git-loose-object\r\nAccept: application/x-git-packfile\r\n\r\n";
        let r = parse_head(h, h.len()).unwrap();
        assert!(r.accepts_pack && r.accepts_loose);
        let h = b"GET /gvfs/config HTTP/1.0\r\nUser-Agent: git/2.45.2.vfs.0.0\r\n\r\n";
        let r = parse_head(h, h.len()).unwrap();
        assert!(!r.keep_alive && r.method == Method::Get && r.git_agent);
        let mut buf = [0u8; 320];
        let n = format_head(&mut buf, 200, "application/json", 5, true);
        assert!(buf[..n].starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert_eq!(
            &buf[..n],
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\n"
        );
        let n = format_head(
            &mut buf,
            413,
            "text/plain",
            18_446_744_073_709_551_615,
            false,
        );
        assert_eq!(
            &buf[..n],
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Type: text/plain\r\nContent-Length: 18446744073709551615\r\nConnection: close\r\n\r\n"
        );
    }
}
