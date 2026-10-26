//! The server side of HTTP/1.1, in-house: just what the GVFS protocol needs.
//!
//! Request heads are parsed in a per-worker buffer into ranges (no allocation); bodies
//! (Content-Length or chunked) are read into the request arena; response heads are
//! formatted on the stack. [`Out`] coalesces small pieces into one buffer and sends large
//! file-backed slab ranges with `sendfile(2)` straight from the page cache.

use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::fd::AsFd;

use crate::slab::Slab;

pub const HEAD_MAX: usize = 16 << 10;
/// Slab ranges at least this long go out with sendfile.
const SENDFILE_MIN: u64 = 16 << 10;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    Get,
    Head,
    Post,
    Other,
}

/// A parsed request head; byte ranges point into the worker's head buffer.
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

/// The worker's input buffer: a request head plus whatever followed it on the wire.
pub struct Input {
    pub buf: Vec<u8>,
    pub filled: usize,
    /// How long a client may pause in the middle of a request (head or body). Between
    /// requests the socket's read timeout is the much shorter linger (see `serve`).
    pub patience: std::time::Duration,
    /// The socket's read timeout is currently `patience` (a request is in progress).
    pub patient: bool,
}

/// What [`read_head`] found on a connection.
pub enum Head {
    Request(Request),
    /// No request arrived within the read timeout and nothing is buffered.
    Idle,
    /// The client closed the connection between requests.
    Closed,
}

impl Input {
    pub fn new() -> Input {
        Input {
            buf: vec![0u8; HEAD_MAX],
            filled: 0,
            patience: std::time::Duration::from_secs(30),
            patient: false,
        }
    }

    /// Switches the socket to the mid-request read timeout (once per request).
    pub fn be_patient(&mut self, stream: &TcpStream) -> std::io::Result<()> {
        if !self.patient {
            stream.set_read_timeout(Some(self.patience))?;
            self.patient = true;
        }
        Ok(())
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

/// Reads one request head: [`Head::Idle`] when no request arrives within the socket's read
/// timeout, [`Head::Closed`] on a clean close between requests.
pub fn read_head(stream: &mut TcpStream, input: &mut Input) -> std::io::Result<Head> {
    loop {
        if let Some(end) = find_head_end(input.buf.get(..input.filled).unwrap_or(&[])) {
            return parse_head(input.buf.get(..end).unwrap_or(&[]), end).map(Head::Request);
        }
        if input.filled >= input.buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request head too large",
            ));
        }
        let n = match stream.read(input.buf.get_mut(input.filled..).unwrap_or(&mut [])) {
            Ok(n) => n,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if input.filled == 0 {
                    return Ok(Head::Idle);
                }
                if input.patient {
                    return Err(e);
                }
                // Part of a head is here: wait for the rest with the mid-request timeout.
                input.be_patient(stream)?;
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if n == 0 {
            return if input.filled == 0 {
                Ok(Head::Closed)
            } else {
                Err(std::io::ErrorKind::UnexpectedEof.into())
            };
        }
        input.filled = input.filled.saturating_add(n);
    }
}

fn find_head_end(b: &[u8]) -> Option<usize> {
    b.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .and_then(|p| p.checked_add(4))
}

fn bad(msg: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

fn parse_head(head: &[u8], head_len: usize) -> std::io::Result<Request> {
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
pub fn read_body(
    stream: &mut TcpStream,
    input: &mut Input,
    req: &Request,
    body: &mut Vec<u8>,
) -> std::io::Result<bool> {
    body.clear();
    if req.expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
    }
    input.consume(req.head_len);
    // A body may take a while to arrive (a large POST over a slow link).
    input.be_patient(stream)?;
    if req.chunked {
        loop {
            let n = line_len(stream, input)?;
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
                    let t = line_len(stream, input)?;
                    input.consume(t.saturating_add(2));
                    if t == 0 {
                        return Ok(true);
                    }
                }
            }
            if body.len().saturating_add(size) > body.capacity() {
                return Ok(false);
            }
            read_exact_into(stream, input, body, size)?;
            if line_len(stream, input)? != 0 {
                return Err(bad("missing chunk terminator"));
            }
            input.consume(2);
        }
    }
    let len = usize::try_from(req.content_length.unwrap_or(0)).unwrap_or(usize::MAX);
    if len > body.capacity() {
        return Ok(false);
    }
    read_exact_into(stream, input, body, len)?;
    Ok(true)
}

/// Length of the CRLF-terminated line at the start of the input (reading more as needed).
fn line_len(stream: &mut TcpStream, input: &mut Input) -> std::io::Result<usize> {
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
        fill(stream, input)?;
    }
}

fn fill(stream: &mut TcpStream, input: &mut Input) -> std::io::Result<()> {
    if input.filled >= input.buf.len() {
        return Err(bad("line too long"));
    }
    let n = stream.read(input.buf.get_mut(input.filled..).unwrap_or(&mut []))?;
    if n == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    input.filled = input.filled.saturating_add(n);
    Ok(())
}

/// Moves `n` body bytes into `body`: buffered ones first, then straight from the socket.
fn read_exact_into(
    stream: &mut TcpStream,
    input: &mut Input,
    body: &mut Vec<u8>,
    n: usize,
) -> std::io::Result<()> {
    let from_buf = n.min(input.filled);
    body.extend_from_slice(input.buf.get(..from_buf).unwrap_or(&[]));
    input.consume(from_buf);
    let rest = n.saturating_sub(from_buf);
    if rest > 0 {
        let start = body.len();
        body.resize(start.saturating_add(rest), 0);
        stream.read_exact(body.get_mut(start..).unwrap_or(&mut []))?;
    }
    Ok(())
}

/// Writes a whole response, head and body, in one `writev` when the socket takes it.
pub fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    keep_alive: bool,
) -> std::io::Result<()> {
    let mut buf = [0u8; 320];
    let n = format_head(
        &mut buf,
        status,
        content_type,
        crate::util::u64_of(body.len()),
        keep_alive,
    );
    write_all2(stream, buf.get(..n).unwrap_or(&[]), body)
}

/// `a` then `b`, with as few syscalls as possible (one, unless the socket buffer fills).
fn write_all2(stream: &mut TcpStream, a: &[u8], b: &[u8]) -> std::io::Result<()> {
    let (mut a, mut b) = (a, b);
    while !a.is_empty() {
        match stream.write_vectored(&[std::io::IoSlice::new(a), std::io::IoSlice::new(b)]) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) if n < a.len() => a = a.get(n..).unwrap_or(&[]),
            Ok(n) => {
                b = b.get(n.saturating_sub(a.len())..).unwrap_or(&[]);
                a = &[];
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    stream.write_all(b)
}

/// Writes a response head (formatted on the stack).
pub fn write_head(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    len: u64,
    keep_alive: bool,
) -> std::io::Result<()> {
    let mut buf = [0u8; 320];
    let n = format_head(&mut buf, status, content_type, len, keep_alive);
    stream.write_all(buf.get(..n).unwrap_or(&[]))
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
pub fn respond_text(
    stream: &mut TcpStream,
    status: u16,
    text: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    write_response(stream, status, "text/plain", text.as_bytes(), keep_alive)
}

/// Response body writer: coalesces small writes, sendfiles large slab ranges.
pub struct Out<'a> {
    stream: &'a mut TcpStream,
    buf: &'a mut Vec<u8>,
    pub sent: u64,
}

impl<'a> Out<'a> {
    /// `buf` is the worker's preallocated output buffer (its capacity is the batch size).
    pub fn new(stream: &'a mut TcpStream, buf: &'a mut Vec<u8>) -> Out<'a> {
        buf.clear();
        Out {
            stream,
            buf,
            sent: 0,
        }
    }

    /// The response head, into the same buffer as the start of the body: small responses
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
        let h = h.get(..n).unwrap_or(&[]);
        if self.buf.len().saturating_add(h.len()) > self.buf.capacity() {
            self.flush()?;
        }
        self.buf.extend_from_slice(h);
        Ok(())
    }

    pub fn bytes(&mut self, b: &[u8]) -> std::io::Result<()> {
        if self.buf.len().saturating_add(b.len()) > self.buf.capacity() {
            self.flush()?;
        }
        if b.len() > self.buf.capacity() {
            self.stream.write_all(b)?;
        } else {
            self.buf.extend_from_slice(b);
        }
        self.sent = self.sent.saturating_add(crate::util::u64_of(b.len()));
        Ok(())
    }

    /// `slab.pack()[start .. start + len]`.
    pub fn slab(&mut self, slab: &Slab, start: u64, len: u64) -> std::io::Result<()> {
        self.mapped(slab.pack_file(), slab.pack(), start, len)
    }

    /// `len` bytes at `start` of an immutable file mapped as `bytes` (`sendfile(2)` from
    /// `file` when large).
    pub fn mapped(
        &mut self,
        file: Option<&std::fs::File>,
        bytes: &[u8],
        start: u64,
        len: u64,
    ) -> std::io::Result<()> {
        match file {
            Some(file) if len >= SENDFILE_MIN => {
                if crate::util::us(start.saturating_add(len)) > bytes.len() {
                    return Err(bad("file range"));
                }
                self.flush()?;
                sendfile_all(self.stream, file, start, len)?;
                self.sent = self.sent.saturating_add(len);
                Ok(())
            }
            _ => {
                let range = crate::util::sub(bytes, crate::util::us(start), crate::util::us(len))
                    .ok_or_else(|| bad("file range"))?;
                self.bytes(range)
            }
        }
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            self.stream.write_all(self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }
}

/// `sendfile(2)` until `len` bytes of `file` from `offset` are in the socket.
pub fn sendfile_all(
    stream: &mut TcpStream,
    file: &File,
    offset: u64,
    len: u64,
) -> std::io::Result<()> {
    let mut off = i64::try_from(offset).map_err(|_| bad("offset"))?;
    let mut left = len;
    while left > 0 {
        let chunk = crate::util::us(left.min(1 << 30));
        match nix::sys::sendfile::sendfile64(stream.as_fd(), file.as_fd(), Some(&mut off), chunk) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => left = left.saturating_sub(crate::util::u64_of(n)),
            Err(nix::errno::Errno::EINTR) => {}
            Err(nix::errno::Errno::EAGAIN) => return Err(std::io::ErrorKind::TimedOut.into()),
            Err(e) => return Err(std::io::Error::from(e)),
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
