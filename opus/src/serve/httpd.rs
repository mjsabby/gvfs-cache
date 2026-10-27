//! The server side of HTTP/1.1, in-house: just what the GVFS protocol needs, on tokio
//! sockets.
//!
//! Request heads are parsed in the connection's head buffer into ranges (no allocation);
//! bodies (Content-Length or chunked) are read into the connection's output buffer before
//! any shared resource is taken ([`Grow`]); response heads are formatted on the stack. [`Out`] coalesces small pieces into one buffer and sends large
//! file-backed ranges with `sendfile(2)` straight from the page cache whenever the socket
//! is writable: a slow reader parks its task, never a thread. Every wait has a deadline
//! ([`Deadlines`]).

use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::os::fd::AsFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::Interest;
use tokio::net::TcpStream;
use tokio::time::Instant;

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
    /// For all of a request, head and body, once its first byte is here: a client that
    /// trickles bytes cannot keep a request open longer.
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

/// `f`, or `TimedOut` once `deadline` has passed.
pub async fn until(
    deadline: Instant,
    f: impl Future<Output = std::io::Result<()>>,
) -> std::io::Result<()> {
    match tokio::time::timeout_at(deadline, f).await {
        Ok(r) => r,
        Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
    }
}

/// Bytes that connection output buffers may hold beyond their pooled size, all together:
/// bodies and answers larger than one buffer grow it from here, as their bytes arrive, so
/// that many large requests at once take a bounded amount of memory.
pub struct Spare {
    left: AtomicUsize,
}

impl Spare {
    pub fn new(bytes: usize) -> Spare {
        Spare {
            left: AtomicUsize::new(bytes),
        }
    }

    /// Takes `n` bytes, if that many are left.
    pub fn take(&self, n: usize) -> bool {
        self.left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |l| l.checked_sub(n))
            .is_ok()
    }

    /// Returns `n` bytes taken before.
    pub fn give(&self, n: usize) {
        self.left.fetch_add(n, Ordering::AcqRel);
    }

    pub fn left(&self) -> usize {
        self.left.load(Ordering::Relaxed)
    }
}

/// A connection's output buffer, allowed to grow past its pooled size (up to `max` bytes,
/// taking the growth from `spare`): a request body, or an answer, larger than it.
pub struct Grow<'a> {
    pub buf: &'a mut Vec<u8>,
    /// What this buffer holds from `spare` (returned when it shrinks back).
    pub extra: &'a mut usize,
    pub max: usize,
    pub spare: &'a Spare,
}

impl Grow<'_> {
    /// Grows the buffer toward `want` bytes of capacity: doubling, so that what it takes stays
    /// within twice what it holds, never past `want` or `max` (a cold section). False if it
    /// cannot grow.
    pub fn more(&mut self, want: usize) -> bool {
        let cap = self.buf.capacity();
        let target = want.min(self.max).min(cap.saturating_mul(2).max(64 << 10));
        if target <= cap {
            return false;
        }
        let add = target.saturating_sub(cap);
        if !self.spare.take(add) {
            return false;
        }
        let _cold = gvfs_alloc_guard::cold_section();
        self.buf
            .reserve_exact(target.saturating_sub(self.buf.len()));
        *self.extra = self.extra.saturating_add(add);
        true
    }

    /// Capacity for at least `total` bytes; false if the buffer cannot grow that far.
    pub fn room(&mut self, total: usize) -> bool {
        while self.buf.capacity() < total {
            if !self.more(total) {
                return false;
            }
        }
        true
    }
}

/// How reading a request body ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Body {
    Read,
    /// Larger than the edge takes (the caller answers 413 and closes).
    TooLarge,
    /// No memory to spare for a body this large right now (503, and close).
    Busy,
}

/// Reads one request head: [`Head::Idle`] if nothing is readable after all, [`Head::Closed`]
/// on a clean close between requests. Once part of a head is here, the rest must follow
/// by `deadline`.
pub async fn read_head(
    stream: &TcpStream,
    input: &mut Input,
    deadline: Instant,
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
                until(deadline, stream.readable()).await?;
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

/// `tchar` (RFC 9110 §5.6.2): what a method or a field name is made of.
fn is_tchar(c: u8) -> bool {
    c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c)
}

/// `v` without leading and trailing spaces and tabs (RFC 9110 `OWS`).
fn trim_ows(v: &[u8]) -> &[u8] {
    let start = v
        .iter()
        .position(|&c| c != b' ' && c != b'\t')
        .unwrap_or(v.len());
    let end = v
        .iter()
        .rposition(|&c| c != b' ' && c != b'\t')
        .map_or(start, |e| e.saturating_add(1));
    v.get(start..end).unwrap_or(&[])
}

/// `1*DIGIT` as a number; `None` for anything else (a sign, spaces, a list) or overflow.
pub fn decimal_u64(v: &[u8]) -> Option<u64> {
    if v.is_empty() {
        return None;
    }
    v.iter().try_fold(0u64, |n, &c| {
        if !c.is_ascii_digit() {
            return None;
        }
        n.checked_mul(10)?
            .checked_add(u64::from(c.wrapping_sub(b'0')))
    })
}

/// The CRLF-terminated lines of a head before its blank line. A bare CR or LF inside a line
/// is an error: some parsers end lines at a bare LF, and two parsers that split a head
/// differently frame the message differently.
fn head_lines(head: &[u8]) -> impl Iterator<Item = std::io::Result<&[u8]>> {
    let mut rest = head;
    std::iter::from_fn(move || {
        let end = rest.windows(2).position(|w| w == b"\r\n")?;
        let line = rest.get(..end)?;
        rest = rest.get(end.saturating_add(2)..)?;
        if line.is_empty() {
            return None;
        }
        Some(if line.iter().any(|&c| c == b'\r' || c == b'\n') {
            Err(bad("bare CR or LF in the request head"))
        } else {
            Ok(line)
        })
    })
}

/// Parses a whole request head (`head_len` bytes through its blank line, as
/// [`find_head_end`] measured).
///
/// Strict wherever a lenient reading could frame the message differently from a proxy in
/// front of the edge (request smuggling, RFC 9112 §6.3 and §11.2), since behind a load
/// balancer one connection carries many clients' requests: lines end in CRLF; the request
/// line is exactly `METHOD SP target SP HTTP/1.x`; a field name is a token right before its
/// colon; no folded lines; no control characters in values; `Content-Length` is digits,
/// given once; `Transfer-Encoding` is `chunked` alone, given once, in HTTP/1.1 and without a
/// `Content-Length`; only POST carries a body. Anything else is an error, which the caller
/// answers with 400 before closing the connection.
pub fn parse_head(head: &[u8], head_len: usize) -> std::io::Result<Request> {
    let mut lines = head_lines(head);
    let request_line = lines.next().ok_or_else(|| bad("empty request"))??;
    let mut parts = request_line.split(|&c| c == b' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("malformed request line"));
    };
    if method.is_empty() || !method.iter().all(|&c| is_tchar(c)) {
        return Err(bad("bad method"));
    }
    if target.is_empty() || target.iter().any(|&c| c <= b' ' || c == 0x7F) {
        return Err(bad("bad request target"));
    }
    let http11 = match version {
        b"HTTP/1.1" => true,
        b"HTTP/1.0" => false,
        _ => return Err(bad("unsupported HTTP version")),
    };
    let method = match method {
        b"GET" => Method::Get,
        b"HEAD" => Method::Head,
        b"POST" => Method::Post,
        _ => Method::Other,
    };
    let target_start = request_line
        .iter()
        .position(|&c| c == b' ')
        .map_or(0, |p| p.saturating_add(1));
    let target_end = target_start.saturating_add(target.len());
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
        keep_alive: http11,
        expect_continue: false,
        accepts_loose: false,
        accepts_pack: false,
        git_agent: false,
        wants_sizes: false,
        head_len,
    };
    let (mut close, mut keep) = (false, false);
    for line in lines {
        let line = line?;
        if line.first().is_some_and(|&c| c == b' ' || c == b'\t') {
            return Err(bad("folded header line"));
        }
        let colon = line
            .iter()
            .position(|&c| c == b':')
            .ok_or_else(|| bad("header line without a colon"))?;
        let (name, value) = line.split_at(colon);
        if name.is_empty() || !name.iter().all(|&c| is_tchar(c)) {
            return Err(bad("bad header name"));
        }
        let value = trim_ows(value.get(1..).unwrap_or(&[]));
        if value.iter().any(|&c| (c < b' ' && c != b'\t') || c == 0x7F) {
            return Err(bad("control character in a header"));
        }
        if name.eq_ignore_ascii_case(b"content-length") {
            if req.content_length.is_some() {
                return Err(bad("more than one Content-Length"));
            }
            req.content_length = Some(decimal_u64(value).ok_or_else(|| bad("bad Content-Length"))?);
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            if req.chunked || !value.eq_ignore_ascii_case(b"chunked") {
                return Err(bad("unsupported Transfer-Encoding"));
            }
            req.chunked = true;
        } else if name.eq_ignore_ascii_case(b"connection") {
            for option in value.split(|&c| c == b',').map(trim_ows) {
                close |= option.eq_ignore_ascii_case(b"close");
                keep |= option.eq_ignore_ascii_case(b"keep-alive");
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
    if req.chunked && (req.content_length.is_some() || !http11) {
        return Err(bad(
            "Transfer-Encoding with a Content-Length, or in HTTP/1.0",
        ));
    }
    if method != Method::Post && (req.chunked || req.content_length.is_some_and(|n| n > 0)) {
        return Err(bad("a body on a method without one"));
    }
    req.keep_alive = !close && (http11 || keep);
    Ok(req)
}

/// Reads the request body into `body`, all of it by `deadline` (the request's), growing the
/// buffer as the bytes arrive if it must (see [`Grow`]).
pub async fn read_body(
    stream: &TcpStream,
    input: &mut Input,
    req: &Request,
    body: &mut Grow<'_>,
    deadline: Instant,
    write: Duration,
) -> std::io::Result<Body> {
    body.buf.clear();
    let declared = usize::try_from(req.content_length.unwrap_or(0)).unwrap_or(usize::MAX);
    if !req.chunked && declared > body.max {
        // Refused before the client is told to send it.
        return Ok(Body::TooLarge);
    }
    if req.expect_continue {
        write_all(stream, b"HTTP/1.1 100 Continue\r\n\r\n", write).await?;
    }
    input.consume(req.head_len);
    if !req.chunked {
        return read_exact_into(stream, input, body, declared, deadline).await;
    }
    loop {
        let n = line_len(stream, input, deadline).await?;
        let size = input
            .buf
            .get(..n)
            .and_then(chunk_size)
            .ok_or_else(|| bad("bad chunk size"))?;
        input.consume(n.saturating_add(2));
        if size == 0 {
            // Trailers (ignored), then the blank line.
            loop {
                let t = line_len(stream, input, deadline).await?;
                let trailer = input.buf.get(..t).unwrap_or(&[]);
                if trailer.iter().any(|&c| c == b'\r' || c == b'\n') {
                    return Err(bad("bare CR or LF in a trailer"));
                }
                input.consume(t.saturating_add(2));
                if t == 0 {
                    return Ok(Body::Read);
                }
            }
        }
        if body.buf.len().saturating_add(size) > body.max {
            return Ok(Body::TooLarge);
        }
        let read = read_exact_into(stream, input, body, size, deadline).await?;
        if read != Body::Read {
            return Ok(read);
        }
        if line_len(stream, input, deadline).await? != 0 {
            return Err(bad("missing chunk terminator"));
        }
        input.consume(2);
    }
}

/// The size on a chunk's line: `1*HEXDIG`, then optionally spaces and `;` extensions
/// (ignored). `None` for anything else (a sign, leading spaces, a bare CR or LF anywhere in
/// the line, more than 16 digits).
fn chunk_size(line: &[u8]) -> Option<usize> {
    if line.iter().any(|&c| c == b'\r' || c == b'\n') {
        return None;
    }
    let size = line.split(|&c| c == b';').next()?;
    let end = size
        .iter()
        .rposition(|&c| c != b' ' && c != b'\t')
        .map_or(0, |e| e.saturating_add(1));
    let size = size.get(..end)?;
    if size.is_empty() || size.len() > 16 {
        return None;
    }
    size.iter().try_fold(0usize, |n, &c| {
        let digit = usize::try_from(char::from(c).to_digit(16)?).ok()?;
        n.checked_mul(16)?.checked_add(digit)
    })
}

/// Length of the CRLF-terminated line at the start of the input (reading more as needed).
async fn line_len(
    stream: &TcpStream,
    input: &mut Input,
    deadline: Instant,
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
            deadline,
        )
        .await?;
        input.filled = input.filled.saturating_add(n);
    }
}

/// At least one byte into `buf` (waiting until `deadline` for it).
async fn read_some(
    stream: &TcpStream,
    buf: &mut [u8],
    deadline: Instant,
) -> std::io::Result<usize> {
    loop {
        match stream.try_read(buf) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                until(deadline, stream.readable()).await?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

/// Appends `n` body bytes to `body`: buffered ones first, then straight from the socket.
/// The buffer grows only as far as the bytes that arrived need ([`Body::Busy`] if it cannot),
/// and each byte of it is zeroed once before the socket fills it.
async fn read_exact_into(
    stream: &TcpStream,
    input: &mut Input,
    body: &mut Grow<'_>,
    n: usize,
    deadline: Instant,
) -> std::io::Result<Body> {
    let start = body.buf.len();
    let end = start.saturating_add(n);
    let from_buf = n.min(input.filled);
    if !body.room(start.saturating_add(from_buf)) {
        return Ok(Body::Busy);
    }
    body.buf
        .extend_from_slice(input.buf.get(..from_buf).unwrap_or(&[]));
    input.consume(from_buf);
    // `body.buf.len()` marks what is zeroed (and read into, up to `filled`).
    let mut filled = body.buf.len();
    while filled < end {
        if filled == body.buf.len() {
            if body.buf.len() == body.buf.capacity() && !body.more(end) {
                body.buf.truncate(filled);
                return Ok(Body::Busy);
            }
            let upto = end.min(body.buf.capacity());
            body.buf.resize(upto, 0);
        }
        let into = body.buf.get_mut(filled..).unwrap_or(&mut []);
        let got = read_some(stream, into, deadline).await?;
        filled = filled.saturating_add(got);
    }
    body.buf.truncate(filled);
    Ok(Body::Read)
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
        408 => "Request Timeout",
        413 => "Payload Too Large",
        416 => "Range Not Satisfiable",
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

/// Only a response head, declaring a body of `len` bytes that does not follow (a HEAD
/// response), with `extra` header lines (each ending in CRLF).
pub async fn write_head(
    stream: &TcpStream,
    status: u16,
    content_type: &str,
    len: u64,
    keep_alive: bool,
    extra: &[u8],
    t: Duration,
) -> std::io::Result<()> {
    let mut buf = [0u8; 320];
    let n = format_head_with(&mut buf, status, content_type, len, keep_alive, extra);
    write_all(stream, buf.get(..n).unwrap_or(&[]), t).await
}

/// What to answer a request whose head or body could not be read (before closing the
/// connection): 400 for a malformed one, 408 for one that took too long; nothing if the
/// client went away.
pub async fn refuse(stream: &TcpStream, e: &std::io::Error, t: Duration) {
    let (status, text) = match e.kind() {
        std::io::ErrorKind::InvalidData => (400, "bad request\n"),
        std::io::ErrorKind::TimedOut => (408, "request timeout\n"),
        _ => return,
    };
    let _ = respond_text(stream, status, text, false, t).await;
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

    fn parse(h: &[u8]) -> std::io::Result<Request> {
        parse_head(h, h.len())
    }

    /// Every way a head could be framed differently by a proxy in front of the edge is
    /// refused; what real clients send is not.
    #[test]
    fn refuses_ambiguous_framing() {
        let refused: &[&[u8]] = &[
            // Content-Length: twice (even equal), signed, spaced, listed, overflowing.
            b"POST /gvfs/sizes HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nContent-Length: +2\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nContent-Length: 2 2\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nContent-Length: 2, 2\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nContent-Length: 99999999999999999999\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nContent-Length: \r\n\r\n",
            // Transfer-Encoding: with a Content-Length, twice, not only chunked, in HTTP/1.0.
            b"POST /gvfs/sizes HTTP/1.1\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nTransfer-Encoding: identity\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n",
            // A name that is not a token right before its colon, a folded line, no colon.
            b"POST /gvfs/sizes HTTP/1.1\r\nTransfer-Encoding : chunked\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\n Transfer-Encoding: chunked\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nX: a\r\n chunked\r\n\r\n",
            b"GET /gvfs/config HTTP/1.1\r\nHost\r\n\r\n",
            b"GET /gvfs/config HTTP/1.1\r\n: x\r\n\r\n",
            // Bare LF or CR, control characters.
            b"POST /gvfs/sizes HTTP/1.1\r\nX: a\nContent-Length: 5\r\n\r\n",
            b"POST /gvfs/sizes HTTP/1.1\r\nX: a\rContent-Length: 5\r\n\r\n",
            b"GET /gvfs/config HTTP/1.1\nHost: a\r\n\r\n",
            b"GET /gvfs/config HTTP/1.1\r\nHost: a\x00b\r\n\r\n",
            // The request line: extra spaces, no version, another version, a bad method.
            b"GET  /gvfs/config HTTP/1.1\r\n\r\n",
            b"GET /gvfs/config\r\n\r\n",
            b"GET /gvfs/config HTTP/1.1 x\r\n\r\n",
            b"GET /gvfs/config HTTP/2.0\r\n\r\n",
            b"G(T /gvfs/config HTTP/1.1\r\n\r\n",
            b"\r\nGET /gvfs/config HTTP/1.1\r\n\r\n",
            // A body on a method without one.
            b"GET /gvfs/config HTTP/1.1\r\nContent-Length: 40\r\n\r\n",
            b"HEAD /gvfs/config HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"PUT /gvfs/config HTTP/1.1\r\nContent-Length: 1\r\n\r\n",
        ];
        for h in refused {
            assert!(parse(h).is_err(), "{:?}", String::from_utf8_lossy(h));
        }
        // gvfs-helper (libcurl), VFS for Git (.NET), chunked POSTs, empty GET bodies.
        let r = parse(b"POST /org/proj/gvfs/objects HTTP/1.1\r\nHost: e\r\nUser-Agent: git/2.53.0.vfs.0.0\r\nAccept: application/x-git-packfile\r\nAccept: application/x-git-loose-object\r\nContent-Type: application/json\r\nContent-Length: 98\r\nExpect: 100-continue\r\n\r\n").unwrap();
        assert!(r.content_length == Some(98) && r.expect_continue && r.keep_alive);
        let r = parse(b"POST /gvfs/sizes HTTP/1.1\r\nTransfer-Encoding: Chunked\r\nConnection: keep-alive, Upgrade\r\n\r\n").unwrap();
        assert!(r.chunked && r.content_length.is_none() && r.keep_alive);
        let r = parse(
            b"GET /gvfs/config HTTP/1.1\r\nContent-Length: 0\r\nConnection: Upgrade, close\r\n\r\n",
        )
        .unwrap();
        assert!(!r.keep_alive);
        let r = parse(b"HEAD /healthz HTTP/1.0\r\nConnection: keep-alive\r\nX-Tab:\tv\t\r\n\r\n")
            .unwrap();
        assert!(r.method == Method::Head && r.keep_alive);
        let h = b"GET /gvfs/prefetch?lastPackTimestamp=5 HTTP/1.1\r\n\r\n";
        let r = parse(h).unwrap();
        assert_eq!(&h[r.path.0..r.path.1], b"/gvfs/prefetch");
        assert_eq!(&h[r.query.0..r.query.1], b"lastPackTimestamp=5");
    }

    #[test]
    fn chunk_sizes_are_hex_digits_only() {
        assert_eq!(chunk_size(b"1a"), Some(26));
        assert_eq!(chunk_size(b"0"), Some(0));
        assert_eq!(chunk_size(b"10 ;name=value"), Some(16));
        assert_eq!(chunk_size(b"ffffffffffffffff"), Some(usize::MAX));
        for bad in [
            &b""[..],
            b"+1",
            b" 1",
            b"0x1",
            b"g",
            b";x",
            b"10000000000000000",
            b"5;a\nb",
            b"5\r",
        ] {
            assert_eq!(chunk_size(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }
}
