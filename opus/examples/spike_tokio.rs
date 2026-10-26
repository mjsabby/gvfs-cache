//! Spike: the edge's connection layer on tokio, measured against today's thread-per-request
//! server (`gvfs-daemon serve`). The same store is served with the same bodies for the
//! endpoints below; only how connections are driven differs.
//!
//! * `--mode own`: tokio sockets with the edge's own HTTP/1 framing (`httpd::parse_head`,
//!   `handlers::route`). A connection holds no buffer while idle: it takes one from a pool
//!   once its next request is readable and returns it when the response is out. Responses
//!   are coalesced into it; file ranges of 16 KiB or more go out with `sendfile(2)` whenever
//!   the socket is writable, so a slow reader parks its task, not a thread.
//! * `--mode hyper`: hyper 1's HTTP/1 server. Bodies are `Bytes`: small pieces copied into
//!   one, file ranges sliced from mmap-backed `Bytes` (made once per slab at startup). hyper
//!   writes them with `writev`, copying them into the socket: there is no `sendfile`.
//!
//! Endpoints: `GET /gvfs/config`; `GET /gvfs/objects/<id>` (a blob of a separate-blob
//! store's packs, or a whole, non-delta slab entry; anything else 404); `GET
//! /gvfs/prefetch` (up to date, or a pack; `.idx` and `GSZ1` as the edge decides them);
//! `POST /gvfs/sizes`; `GET /healthz`; `GET /metrics` (only `gvfs_alloc_post_seal_total`).
//! `--heavy` says where sizes requests (CPU-bound) run: `inline` on the worker that read the
//! request; `blocking` on tokio's blocking pool; `nice` there, at `--nice` (10) priority;
//! `yield` on the worker, yielding to other tasks every 256 ids. `--idle-secs` (30) bounds an idle connection (hyper: until a request head).
//! No upstream, proxy or live tip: the store's manifest as it is at startup.
//!
//! ```sh
//! cargo run --release --example spike_tokio -- --mode own --store fs:/path/store \
//!     --bind 127.0.0.1:8080 --threads 8
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::cell::Cell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs::File;
use std::future::Future;
use std::io;
use std::os::fd::AsFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use gvfs_alloc_guard::SealedAllocator;
use gvfs_daemon::pack::{OBJ_BLOB, parse_entry_header};
use gvfs_daemon::serve::blobs::BlobPack;
use gvfs_daemon::serve::handlers::{Metrics, Route, route, scan_oids};
use gvfs_daemon::serve::httpd::{self, Input, Method, Request};
use gvfs_daemon::serve::view::{Prefetch, View};
use gvfs_daemon::slab::Slab;
use gvfs_daemon::splice::PackPlan;
use gvfs_daemon::store::Store;
use gvfs_daemon::util::{Oid, decimal, sub, try_extend, try_push};
use gvfs_daemon::zlib::LooseSplice;
use http_body::{Frame, SizeHint};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, HeaderName, HeaderValue, USER_AGENT};
use tokio::io::Interest;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

#[global_allocator]
static GLOBAL: SealedAllocator = SealedAllocator;

const CT_PREFETCH: &str = "application/x-gvfs-timestamped-packfiles-indexes";
const CT_LOOSE: &str = "application/x-git-loose-object";
const CT_JSON: &str = "application/json";
const CT_TEXT: &str = "text/plain";
/// As the edge: file ranges at least this long go out with `sendfile(2)`.
const SENDFILE_MIN: u64 = 16 << 10;
/// As the edge's per-worker output buffer.
const OUT_BYTES: usize = 256 << 10;
/// Connection buffers made at startup (the edge has one per worker thread: 256).
const BUFS: usize = 256;
/// Request arenas for sizes (the edge: two per core).
const HEAVY: usize = 16;
/// Lookups started this many ids ahead (as the edge's batch loops).
const AHEAD: usize = 16;

struct Ctx {
    view: RwLock<Arc<View>>,
    config: Vec<u8>,
    config_bytes: Bytes,
    metrics: Metrics,
    bufs: Pool<Bufs>,
    heavy: Pool<Heavy>,
    heavy_permits: Semaphore,
    heavy_mode: HeavyMode,
    idle: Duration,
    patience: Duration,
    write: Duration,
    /// hyper: an mmap-backed `Bytes` per slab and per plan sizes section (by address).
    slab_bytes: HashMap<usize, Bytes>,
    section_bytes: HashMap<usize, Bytes>,
}

impl Ctx {
    /// The current snapshot, as the edge takes it per request (a read lock and an `Arc`).
    fn view(&self) -> Arc<View> {
        Arc::clone(&self.view.read().unwrap())
    }

    fn slab(&self, slab: &Arc<Slab>) -> Bytes {
        match self.slab_bytes.get(&(Arc::as_ptr(slab) as usize)) {
            Some(b) => b.clone(),
            None => Bytes::from_owner(SlabBytes(Arc::clone(slab))),
        }
    }

    fn section(&self, plan: &Arc<PackPlan>) -> Bytes {
        match self.section_bytes.get(&(Arc::as_ptr(plan) as usize)) {
            Some(b) => b.clone(),
            None => Bytes::copy_from_slice(plan.sizes_section()),
        }
    }
}

struct SlabBytes(Arc<Slab>);

impl AsRef<[u8]> for SlabBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.pack()
    }
}

struct PackBytes(Arc<BlobPack>);

impl AsRef<[u8]> for PackBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.bytes().unwrap_or(&[])
    }
}

struct SectionBytes(Arc<PackPlan>);

impl AsRef<[u8]> for SectionBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.sizes_section()
    }
}

/// A connection's buffers while it has a request in progress.
struct Bufs {
    input: Input,
    out: Vec<u8>,
}

fn new_bufs() -> Bufs {
    Bufs {
        input: Input::new(),
        out: Vec::with_capacity(OUT_BYTES),
    }
}

/// A sizes request's body, ids and response.
struct Heavy {
    body: Vec<u8>,
    oids: Vec<Oid>,
    out: Vec<u8>,
}

fn new_heavy() -> Heavy {
    Heavy {
        body: Vec::with_capacity(8 << 20),
        oids: Vec::with_capacity(1 << 18),
        out: Vec::with_capacity(16 << 20),
    }
}

thread_local! {
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

fn shard(n: usize) -> usize {
    SHARD.with(|s| {
        let mut i = s.get();
        if i == usize::MAX {
            i = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
            s.set(i);
        }
        i % n
    })
}

/// Objects made up front, in per-thread shards (a new one when every shard is empty).
struct Pool<T> {
    shards: Vec<Mutex<Vec<T>>>,
    make: fn() -> T,
}

impl<T> Pool<T> {
    fn new(shards: usize, count: usize, make: fn() -> T) -> Pool<T> {
        let shards: Vec<Mutex<Vec<T>>> = (0..shards)
            .map(|_| Mutex::new(Vec::with_capacity(count * 2)))
            .collect();
        for i in 0..count {
            shards[i % shards.len()].lock().unwrap().push(make());
        }
        Pool { shards, make }
    }

    fn take(&self) -> T {
        let n = self.shards.len();
        let first = shard(n);
        for k in 0..n {
            if let Some(t) = self.shards[(first + k) % n].lock().unwrap().pop() {
                return t;
            }
        }
        (self.make)()
    }

    fn put(&self, t: T) {
        self.shards[shard(self.shards.len())]
            .lock()
            .unwrap()
            .push(t);
    }
}

/// Where sizes requests run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HeavyMode {
    /// On the worker that read the request, to the end.
    Inline,
    /// On tokio's blocking pool; `Some(n)`: its threads reniced to `n` (lower priority).
    Blocking(Option<i32>),
    /// On the worker, yielding to other tasks every [`CHUNK`] ids.
    Yield,
}

/// Ids looked up between yields (`--heavy yield`): about 20 µs of work.
const CHUNK: usize = 256;

thread_local! {
    static RENICED: Cell<bool> = const { Cell::new(false) };
}

/// Lowers this thread's priority to `nice` (once per thread). `renice` sets it for one
/// thread when given a thread id; the spike has no `setpriority` without `unsafe`.
fn renice_once(nice: i32) {
    RENICED.with(|r| {
        if r.get() {
            return;
        }
        r.set(true);
        let tid = std::fs::read_link("/proc/thread-self")
            .ok()
            .and_then(|p| p.file_name().map(|t| t.to_string_lossy().into_owned()));
        if let Some(tid) = tid {
            let _ = std::process::Command::new("renice")
                .args(["-n", &nice.to_string(), "-p", &tid])
                .stdout(std::process::Stdio::null())
                .status();
        }
    });
}

fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

fn bad(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn metrics_text() -> String {
    format!(
        "gvfs_alloc_post_seal_total {}\n",
        gvfs_alloc_guard::stats().post_seal
    )
}

/// `f`, or `TimedOut` after `t`.
async fn ready(f: impl Future<Output = io::Result<()>>, t: Duration) -> io::Result<()> {
    tokio::time::timeout(t, f)
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

// ---------------------------------------------------------------------------------------
// `--mode own`

enum Head {
    Request(Request),
    /// Woken with nothing to read (and nothing buffered).
    Idle,
    /// The client closed the connection between requests.
    Closed,
}

enum Next {
    Again,
    Idle,
    Close,
}

async fn conn_own(ctx: Arc<Ctx>, s: TcpStream) {
    let _ = s.set_nodelay(true);
    // Kept across requests only while pipelined bytes are buffered in it.
    let mut held: Option<Bufs> = None;
    loop {
        if held.is_none() {
            match tokio::time::timeout(ctx.idle, s.readable()).await {
                Ok(Ok(())) => {}
                _ => return,
            }
        }
        let mut b = held.take().unwrap_or_else(|| ctx.bufs.take());
        match request_own(&ctx, &s, &mut b).await {
            Ok(Next::Again) if b.input.filled > 0 => held = Some(b),
            Ok(Next::Again | Next::Idle) => ctx.bufs.put(b),
            Ok(Next::Close) | Err(_) => {
                b.input.filled = 0;
                ctx.bufs.put(b);
                return;
            }
        }
    }
}

async fn read_head(s: &TcpStream, inp: &mut Input, patience: Duration) -> io::Result<Head> {
    loop {
        if let Some(end) = httpd::find_head_end(&inp.buf[..inp.filled]) {
            return httpd::parse_head(&inp.buf[..end], end).map(Head::Request);
        }
        if inp.filled >= inp.buf.len() {
            return Err(bad("request head too large"));
        }
        match s.try_read(&mut inp.buf[inp.filled..]) {
            Ok(0) if inp.filled == 0 => return Ok(Head::Closed),
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => inp.filled += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if inp.filled == 0 {
                    return Ok(Head::Idle);
                }
                ready(s.readable(), patience).await?;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

async fn write_all(s: &TcpStream, mut b: &[u8], t: Duration) -> io::Result<()> {
    while !b.is_empty() {
        match s.try_write(b) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => b = &b[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => ready(s.writable(), t).await?,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// `a` then `b` in one `writev` when the socket takes them.
async fn write_all2(s: &TcpStream, a: &[u8], b: &[u8], t: Duration) -> io::Result<()> {
    let (mut a, mut b) = (a, b);
    while !a.is_empty() {
        match s.try_write_vectored(&[io::IoSlice::new(a), io::IoSlice::new(b)]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) if n < a.len() => a = &a[n..],
            Ok(n) => {
                b = &b[n - a.len()..];
                a = &[];
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => ready(s.writable(), t).await?,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    write_all(s, b, t).await
}

/// `sendfile(2)` whenever the socket is writable until `len` bytes of `file` from `offset`
/// are in it.
async fn sendfile_all(
    s: &TcpStream,
    file: &File,
    offset: u64,
    len: u64,
    t: Duration,
) -> io::Result<()> {
    let mut off = i64::try_from(offset).map_err(|_| bad("offset"))?;
    let mut left = len;
    while left > 0 {
        let chunk = left.min(1 << 30) as usize;
        let sent = s.try_io(Interest::WRITABLE, || {
            nix::sys::sendfile::sendfile64(s.as_fd(), file.as_fd(), Some(&mut off), chunk)
                .map_err(io::Error::from)
        });
        match sent {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => left -= n as u64,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => ready(s.writable(), t).await?,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// The edge's `httpd::Out` on a tokio socket.
struct Out<'a> {
    s: &'a TcpStream,
    buf: &'a mut Vec<u8>,
    write: Duration,
    sent: u64,
}

impl Out<'_> {
    /// Into the (empty) buffer, with the start of the body.
    fn head(&mut self, status: u16, ct: &str, len: u64, keep: bool, extra: &[u8]) {
        let mut h = [0u8; 320];
        let n = httpd::format_head_with(&mut h, status, ct, len, keep, extra);
        self.buf.extend_from_slice(&h[..n]);
    }

    async fn bytes(&mut self, b: &[u8]) -> io::Result<()> {
        if self.buf.len() + b.len() > self.buf.capacity() {
            self.flush().await?;
        }
        if b.len() > self.buf.capacity() {
            write_all(self.s, b, self.write).await?;
        } else {
            self.buf.extend_from_slice(b);
        }
        self.sent += b.len() as u64;
        Ok(())
    }

    async fn mapped(
        &mut self,
        file: Option<&File>,
        bytes: &[u8],
        start: u64,
        len: u64,
    ) -> io::Result<()> {
        match file {
            Some(f) if len >= SENDFILE_MIN => {
                if (start + len) as usize > bytes.len() {
                    return Err(bad("file range"));
                }
                self.flush().await?;
                sendfile_all(self.s, f, start, len, self.write).await?;
                self.sent += len;
                Ok(())
            }
            _ => {
                let r =
                    sub(bytes, start as usize, len as usize).ok_or_else(|| bad("file range"))?;
                self.bytes(r).await
            }
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            write_all(self.s, self.buf, self.write).await?;
            self.buf.clear();
        }
        Ok(())
    }

    async fn text(&mut self, status: u16, t: &str, keep: bool) -> io::Result<bool> {
        self.head(status, CT_TEXT, t.len() as u64, keep, b"");
        self.bytes(t.as_bytes()).await?;
        self.flush().await?;
        Ok(keep)
    }
}

async fn request_own(ctx: &Ctx, s: &TcpStream, b: &mut Bufs) -> io::Result<Next> {
    let req = match read_head(s, &mut b.input, ctx.patience).await? {
        Head::Request(r) => r,
        Head::Idle => return Ok(Next::Idle),
        Head::Closed => return Ok(Next::Close),
    };
    let rt = route(
        req.method,
        b.input.bytes(req.path),
        b.input.bytes(req.query),
    );
    let keep = req.keep_alive;
    if req.method != Method::Post {
        b.input.consume(req.head_len);
    }
    b.out.clear();
    let mut out = Out {
        s,
        buf: &mut b.out,
        write: ctx.write,
        sent: 0,
    };
    let keep = match rt {
        Route::Config => {
            inc(&ctx.metrics.config);
            out.head(200, CT_JSON, ctx.config.len() as u64, keep, b"");
            out.bytes(&ctx.config).await?;
            out.flush().await?;
            keep
        }
        Route::Health => out.text(200, "ok\n", keep).await?,
        Route::Metrics => out.text(200, &metrics_text(), keep).await?,
        Route::Prefetch(since) => {
            inc(&ctx.metrics.prefetch);
            let view = ctx.view();
            let (idx, sizes) = (!req.git_agent, req.wants_sizes);
            prefetch_own(ctx, &view, &mut out, since, idx, sizes, keep).await?
        }
        Route::ObjectGet(oid) => {
            inc(&ctx.metrics.object_get);
            let view = ctx.view();
            object_own(ctx, &view, &mut out, &oid, keep).await?
        }
        Route::SizesPost => {
            inc(&ctx.metrics.sizes);
            sizes_own(ctx, s, &mut b.input, &req, keep).await?
        }
        _ => {
            inc(&ctx.metrics.not_found);
            // An unread POST body would be parsed as the next request: close instead.
            let keep = keep && req.method != Method::Post;
            out.text(404, "not found\n", keep).await?
        }
    };
    Ok(if keep { Next::Again } else { Next::Close })
}

async fn object_own(
    ctx: &Ctx,
    view: &View,
    out: &mut Out<'_>,
    oid: &Oid,
    keep: bool,
) -> io::Result<bool> {
    if let Some((pack, rec)) = view.blobs.as_ref().and_then(|s| s.find(oid)) {
        inc(&ctx.metrics.blobs_store);
        let Some(bytes) = pack.bytes() else {
            return out
                .text(500, "remote blob pack: not in the spike\n", false)
                .await;
        };
        let (zat, zlen) = rec.zlib();
        let plan = sub(bytes, zat as usize, zlen as usize)
            .and_then(|z| LooseSplice::plan(OBJ_BLOB, rec.size, z).ok());
        let Some(plan) = plan else {
            return out.text(500, "blob unreadable\n", false).await;
        };
        out.head(200, CT_LOOSE, plan.total_len() as u64, keep, b"");
        out.bytes(plan.prefix()).await?;
        let (at, n) = (
            plan.body_start as u64,
            (plan.body_end - plan.body_start) as u64,
        );
        out.mapped(pack.file(), bytes, zat + at, n).await?;
        out.bytes(&plan.trailer).await?;
        out.flush().await?;
        ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
        inc(&ctx.metrics.objects_out);
        return Ok(keep);
    }
    let Some((slab, rec)) = view
        .find(oid)
        .and_then(|(k, slot)| Some((view.all.get(usize::from(k))?, slot)))
        .and_then(|(s, slot)| Some((s, s.rec(slot)?)))
    else {
        inc(&ctx.metrics.not_found);
        return out.text(404, "not found\n", keep).await;
    };
    let entry = slab.entry(&rec).unwrap_or(&[]);
    let hdr = parse_entry_header(entry).map_or(0, |h| h.header_len);
    let plan = (!rec.is_delta())
        .then(|| LooseSplice::plan(rec.kind, rec.size, &entry[hdr..]).ok())
        .flatten();
    let Some(plan) = plan else {
        return out.text(404, "a delta: not in the spike\n", keep).await;
    };
    let zlib_at = rec.offset + hdr as u64;
    out.head(200, CT_LOOSE, plan.total_len() as u64, keep, b"");
    out.bytes(plan.prefix()).await?;
    let (at, n) = (
        plan.body_start as u64,
        (plan.body_end - plan.body_start) as u64,
    );
    out.mapped(slab.pack_file(), slab.pack(), zlib_at + at, n)
        .await?;
    out.bytes(&plan.trailer).await?;
    out.flush().await?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    inc(&ctx.metrics.objects_out);
    Ok(keep)
}

/// `X-GVFS-Pack-Start/End` (and `X-GVFS-Sizes`) lines into `buf`; returns their length.
fn pack_headers(buf: &mut [u8; 128], start: u64, end: u64, sizes: bool) -> usize {
    use std::io::Write as _;
    let mut cur = io::Cursor::new(buf.as_mut_slice());
    let _ = write!(
        cur,
        "X-GVFS-Pack-Start: {start}\r\nX-GVFS-Pack-End: {end}\r\n{}",
        if sizes { "X-GVFS-Sizes: 1\r\n" } else { "" }
    );
    cur.position() as usize
}

/// The 32 bytes before a prefetch's pack: `GPRE`, version, count, timestamp, pack and
/// `.idx` lengths.
fn gpre(plan: &PackPlan, idx_len: u64) -> [u8; 32] {
    let mut head = [0u8; 32];
    head[..8].copy_from_slice(b"GPRE \x01\x01\x00");
    head[8..16].copy_from_slice(&plan.timestamp.to_le_bytes());
    head[16..24].copy_from_slice(&plan.len.to_le_bytes());
    head[24..].copy_from_slice(&idx_len.to_le_bytes());
    head
}

const UP_TO_DATE: &[u8] = b"GPRE \x01\x00\x00";
const UP_TO_DATE_SIZES: &[u8] = b"GPRE \x01\x00\x00GSZ1\0\0\0\0\0\0\0\0";

#[allow(clippy::too_many_arguments)]
async fn prefetch_own(
    ctx: &Ctx,
    view: &View,
    out: &mut Out<'_>,
    since: u64,
    want_idx: bool,
    want_sizes: bool,
    keep: bool,
) -> io::Result<bool> {
    let sizes = want_sizes && view.blobs.is_some();
    let (plan, start) = match view.prefetch(since) {
        Prefetch::UpToDate => {
            let (body, extra): (&[u8], &[u8]) = if sizes {
                (UP_TO_DATE_SIZES, b"X-GVFS-Sizes: 1\r\n")
            } else {
                (UP_TO_DATE, b"")
            };
            out.head(200, CT_PREFETCH, body.len() as u64, keep, extra);
            out.bytes(body).await?;
            out.flush().await?;
            return Ok(keep);
        }
        Prefetch::Pack(plan, start) => (plan, start),
    };
    let section: &[u8] = if sizes { plan.sizes_section() } else { &[] };
    inc(&ctx.metrics.prefetch_packs);
    let idx = if want_idx { plan.idx() } else { None };
    let idx_len = idx.map_or(u64::MAX, |i| i.len() as u64);
    let body_len = plan.len + 32 + idx.map_or(0, |i| i.len() as u64) + section.len() as u64;
    let mut extra = [0u8; 128];
    let n = pack_headers(&mut extra, start, plan.timestamp, sizes);
    out.head(200, CT_PREFETCH, body_len, keep, &extra[..n]);
    out.bytes(&gpre(&plan, idx_len)).await?;
    out.bytes(&plan.header).await?;
    for (slab, at, len) in plan.bodies() {
        out.mapped(slab.pack_file(), slab.pack(), at, len).await?;
    }
    out.bytes(&plan.trailer()).await?;
    if let Some(idx) = idx {
        out.mapped(plan.idx_file(), idx, 0, idx.len() as u64)
            .await?;
    }
    if sizes {
        out.mapped(plan.sizes_file(), section, 0, section.len() as u64)
            .await?;
    }
    out.flush().await?;
    ctx.metrics.bytes_out.fetch_add(out.sent, Ordering::Relaxed);
    ctx.metrics
        .objects_out
        .fetch_add(u64::from(plan.entries), Ordering::Relaxed);
    Ok(keep)
}

/// Reads a `Content-Length` body into `body` (whose capacity bounds it). Returns false if it
/// does not fit.
async fn read_body(
    ctx: &Ctx,
    s: &TcpStream,
    inp: &mut Input,
    req: &Request,
    body: &mut Vec<u8>,
) -> io::Result<bool> {
    body.clear();
    if req.expect_continue {
        write_all(s, b"HTTP/1.1 100 Continue\r\n\r\n", ctx.write).await?;
    }
    inp.consume(req.head_len);
    if req.chunked {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "chunked bodies: not in the spike",
        ));
    }
    let len = usize::try_from(req.content_length.unwrap_or(0)).unwrap_or(usize::MAX);
    if len > body.capacity() {
        return Ok(false);
    }
    let from = len.min(inp.filled);
    body.extend_from_slice(&inp.buf[..from]);
    inp.consume(from);
    let mut at = body.len();
    body.resize(len, 0);
    while at < len {
        match s.try_read(&mut body[at..]) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => at += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                ready(s.readable(), ctx.patience).await?;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

enum Sizes {
    Ok,
    TooMany,
    NotFound,
    TooLarge,
}

/// The edge's sizes handler: `[{"Id":"<id>","Size":<n>},...]` into `out`.
fn compute_sizes(view: &View, body: &[u8], oids: &mut Vec<Oid>, out: &mut Vec<u8>) -> Sizes {
    let ok = match sizes_start(body, oids, out) {
        Ok(ok) => ok,
        Err(st) => return st,
    };
    match sizes_range(view, oids, 0, oids.len(), out) {
        Some(o) => sizes_end(out, ok && o),
        None => Sizes::NotFound,
    }
}

/// [`compute_sizes`], yielding to other tasks every [`CHUNK`] ids.
async fn compute_sizes_yielding(
    view: &View,
    body: &[u8],
    oids: &mut Vec<Oid>,
    out: &mut Vec<u8>,
) -> Sizes {
    let mut ok = match sizes_start(body, oids, out) {
        Ok(ok) => ok,
        Err(st) => return st,
    };
    let mut at = 0;
    while at < oids.len() {
        let end = (at + CHUNK).min(oids.len());
        match sizes_range(view, oids, at, end, out) {
            Some(o) => ok &= o,
            None => return Sizes::NotFound,
        }
        at = end;
        if at < oids.len() {
            tokio::task::yield_now().await;
        }
    }
    sizes_end(out, ok)
}

/// Parses the ids and opens the array; `Ok(whether it fit)`.
fn sizes_start(body: &[u8], oids: &mut Vec<Oid>, out: &mut Vec<u8>) -> Result<bool, Sizes> {
    oids.clear();
    if !scan_oids(body, oids) {
        return Err(Sizes::TooMany);
    }
    out.clear();
    Ok(try_push(out, b'['))
}

fn sizes_end(out: &mut Vec<u8>, ok: bool) -> Sizes {
    if ok && try_push(out, b']') {
        Sizes::Ok
    } else {
        Sizes::TooLarge
    }
}

/// The entries of `oids[from..to]`; `None` if one is unknown, else whether they fit.
fn sizes_range(
    view: &View,
    oids: &[Oid],
    from: usize,
    to: usize,
    out: &mut Vec<u8>,
) -> Option<bool> {
    let mut ok = true;
    for (i, oid) in oids.iter().enumerate().take(to).skip(from) {
        if let Some(next) = oids.get(i + AHEAD) {
            match view.blobs.as_ref() {
                Some(set) => set.preload(next),
                None => view.preload(next),
            }
        }
        let size = match view.blobs.as_ref().and_then(|s| s.find(oid)) {
            Some((_, r)) => r.size,
            None => match view.find(oid) {
                Some((k, slot)) => view
                    .all
                    .get(usize::from(k))
                    .and_then(|s| s.rec(slot))
                    .map_or(0, |r| r.size),
                None => view.blob_size(oid)?,
            },
        };
        let hex = oid.to_hex();
        let mut digits = [0u8; 20];
        let digits = decimal(size, &mut digits);
        let mut item = [0u8; 80];
        let mut n = 0usize;
        let mut put = |src: &[u8]| {
            item[n..n + src.len()].copy_from_slice(src);
            n += src.len();
        };
        if i != 0 {
            put(b",");
        }
        put(b"{\"Id\":\"");
        put(&hex);
        put(b"\",\"Size\":");
        put(digits);
        put(b"}");
        ok &= try_extend(out, &item[..n]);
    }
    Some(ok)
}

/// Runs the sizes lookups as `--heavy` says.
async fn run_sizes(ctx: &Ctx, h: Heavy, body: Option<Bytes>) -> (Heavy, Sizes) {
    let view = ctx.view();
    match ctx.heavy_mode {
        HeavyMode::Inline => {
            let mut h = h;
            let Heavy {
                body: own,
                oids,
                out,
            } = &mut h;
            let st = compute_sizes(&view, body.as_deref().unwrap_or(own), oids, out);
            (h, st)
        }
        HeavyMode::Yield => {
            let mut h = h;
            let Heavy {
                body: own,
                oids,
                out,
            } = &mut h;
            let st = compute_sizes_yielding(&view, body.as_deref().unwrap_or(own), oids, out).await;
            (h, st)
        }
        HeavyMode::Blocking(nice) => tokio::task::spawn_blocking(move || {
            if let Some(n) = nice {
                renice_once(n);
            }
            let mut h = h;
            let Heavy {
                body: own,
                oids,
                out,
            } = &mut h;
            let st = compute_sizes(&view, body.as_deref().unwrap_or(own), oids, out);
            (h, st)
        })
        .await
        .unwrap(),
    }
}

async fn sizes_own(
    ctx: &Ctx,
    s: &TcpStream,
    inp: &mut Input,
    req: &Request,
    keep: bool,
) -> io::Result<bool> {
    let _permit = ctx
        .heavy_permits
        .acquire()
        .await
        .map_err(io::Error::other)?;
    let mut h = ctx.heavy.take();
    let fits = match read_body(ctx, s, inp, req, &mut h.body).await {
        Ok(f) => f,
        Err(e) => {
            ctx.heavy.put(h);
            return Err(e);
        }
    };
    let (h, st) = if fits {
        run_sizes(ctx, h, None).await
    } else {
        (h, Sizes::TooLarge)
    };
    let (status, ct, body, keep): (u16, &str, &[u8], bool) = match st {
        Sizes::Ok => (200, CT_JSON, &h.out, keep),
        Sizes::TooMany => (413, CT_TEXT, b"too many objects\n", keep),
        Sizes::NotFound => (404, CT_TEXT, b"not found\n", keep),
        Sizes::TooLarge if !fits => (413, CT_TEXT, b"request too large\n", false),
        Sizes::TooLarge => (413, CT_TEXT, b"response too large\n", keep),
    };
    let mut head = [0u8; 320];
    let n = httpd::format_head_with(&mut head, status, ct, body.len() as u64, keep, b"");
    let r = write_all2(s, &head[..n], body, ctx.write).await;
    ctx.heavy.put(h);
    r.map(|()| keep)
}

// ---------------------------------------------------------------------------------------
// `--mode hyper`

/// A response body: one buffer, or several (file ranges sliced from mapped files).
enum Body {
    One(Option<Bytes>),
    Many(std::vec::IntoIter<Bytes>, u64),
}

impl http_body::Body for Body {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let next = match self.get_mut() {
            Body::One(b) => b.take(),
            Body::Many(parts, _) => parts.next(),
        };
        Poll::Ready(next.map(|b| Ok(Frame::data(b))))
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Body::One(b) => b.is_none(),
            Body::Many(parts, _) => parts.len() == 0,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Body::One(b) => SizeHint::with_exact(b.as_ref().map_or(0, |b| b.len() as u64)),
            Body::Many(_, n) => SizeHint::with_exact(*n),
        }
    }
}

type Resp = hyper::Response<Body>;

/// hyper writes `Content-Length` from the body's exact size.
fn respond(status: u16, ct: &'static str, extra: &[(&'static str, u64)], body: Body) -> Resp {
    let mut r = hyper::Response::new(body);
    *r.status_mut() = hyper::StatusCode::from_u16(status).unwrap();
    let h = r.headers_mut();
    h.insert(CONTENT_TYPE, HeaderValue::from_static(ct));
    for (k, v) in extra {
        h.insert(HeaderName::from_static(k), HeaderValue::from(*v));
    }
    r
}

fn one(b: Bytes) -> Body {
    Body::One(Some(b))
}

fn text(status: u16, t: &'static str) -> Resp {
    respond(status, CT_TEXT, &[], one(Bytes::from_static(t.as_bytes())))
}

async fn conn_hyper(ctx: Arc<Ctx>, s: TcpStream) {
    let _ = s.set_nodelay(true);
    let ctx_idle = ctx.idle;
    let svc = hyper::service::service_fn(move |req| {
        let ctx = Arc::clone(&ctx);
        async move { Ok::<_, Infallible>(handle_hyper(&ctx, req).await) }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(ctx_idle)
        .auto_date_header(false)
        .serve_connection(hyper_util::rt::TokioIo::new(s), svc)
        .await;
}

async fn handle_hyper(ctx: &Ctx, req: hyper::Request<Incoming>) -> Resp {
    let method = match *req.method() {
        hyper::Method::GET => Method::Get,
        hyper::Method::HEAD => Method::Head,
        hyper::Method::POST => Method::Post,
        _ => Method::Other,
    };
    let rt = route(
        method,
        req.uri().path().as_bytes(),
        req.uri().query().unwrap_or("").as_bytes(),
    );
    match rt {
        Route::Config => {
            inc(&ctx.metrics.config);
            respond(200, CT_JSON, &[], one(ctx.config_bytes.clone()))
        }
        Route::Health => text(200, "ok\n"),
        Route::Metrics => respond(200, CT_TEXT, &[], one(Bytes::from(metrics_text()))),
        Route::Prefetch(since) => {
            inc(&ctx.metrics.prefetch);
            let h = req.headers();
            let git = h
                .get(USER_AGENT)
                .is_some_and(|v| v.as_bytes().starts_with(b"git/"));
            let sizes = h.get("x-gvfs-sizes").is_some_and(|v| v.as_bytes() == b"1");
            prefetch_hyper(ctx, since, !git, sizes)
        }
        Route::ObjectGet(oid) => {
            inc(&ctx.metrics.object_get);
            object_hyper(ctx, &oid)
        }
        Route::SizesPost => {
            inc(&ctx.metrics.sizes);
            sizes_hyper(ctx, req.into_body()).await
        }
        _ => {
            inc(&ctx.metrics.not_found);
            text(404, "not found\n")
        }
    }
}

/// A loose object: `prefix`, `len` bytes of `file_bytes` at `at`, `trailer`.
fn loose_body(
    prefix: &[u8],
    mapped: &[u8],
    at: usize,
    len: usize,
    trailer: &[u8],
    owner: impl FnOnce() -> Bytes,
) -> (Body, u64) {
    let total = prefix.len() + len + trailer.len();
    let body = if len as u64 >= SENDFILE_MIN {
        let parts = vec![
            Bytes::copy_from_slice(prefix),
            owner().slice(at..at + len),
            Bytes::copy_from_slice(trailer),
        ];
        Body::Many(parts.into_iter(), total as u64)
    } else {
        let mut b = BytesMut::with_capacity(total);
        b.extend_from_slice(prefix);
        b.extend_from_slice(&mapped[at..at + len]);
        b.extend_from_slice(trailer);
        one(b.freeze())
    };
    (body, total as u64)
}

fn object_hyper(ctx: &Ctx, oid: &Oid) -> Resp {
    let view = ctx.view();
    if let Some((pack, rec)) = view.blobs.as_ref().and_then(|s| s.find(oid)) {
        inc(&ctx.metrics.blobs_store);
        let Some(bytes) = pack.bytes() else {
            return text(500, "remote blob pack: not in the spike\n");
        };
        let (zat, zlen) = rec.zlib();
        let plan = sub(bytes, zat as usize, zlen as usize)
            .and_then(|z| LooseSplice::plan(OBJ_BLOB, rec.size, z).ok());
        let Some(plan) = plan else {
            return text(500, "blob unreadable\n");
        };
        let at = zat as usize + plan.body_start;
        let (body, total) = loose_body(
            plan.prefix(),
            bytes,
            at,
            plan.body_end - plan.body_start,
            &plan.trailer,
            || Bytes::from_owner(PackBytes(Arc::clone(pack))),
        );
        ctx.metrics.bytes_out.fetch_add(total, Ordering::Relaxed);
        inc(&ctx.metrics.objects_out);
        return respond(200, CT_LOOSE, &[], body);
    }
    let Some((slab, rec)) = view
        .find(oid)
        .and_then(|(k, slot)| Some((view.all.get(usize::from(k))?, slot)))
        .and_then(|(s, slot)| Some((s, s.rec(slot)?)))
    else {
        inc(&ctx.metrics.not_found);
        return text(404, "not found\n");
    };
    let entry = slab.entry(&rec).unwrap_or(&[]);
    let hdr = parse_entry_header(entry).map_or(0, |h| h.header_len);
    let plan = (!rec.is_delta())
        .then(|| LooseSplice::plan(rec.kind, rec.size, &entry[hdr..]).ok())
        .flatten();
    let Some(plan) = plan else {
        return text(404, "a delta: not in the spike\n");
    };
    let at = rec.offset as usize + hdr + plan.body_start;
    let (body, total) = loose_body(
        plan.prefix(),
        slab.pack(),
        at,
        plan.body_end - plan.body_start,
        &plan.trailer,
        || ctx.slab(slab),
    );
    ctx.metrics.bytes_out.fetch_add(total, Ordering::Relaxed);
    inc(&ctx.metrics.objects_out);
    respond(200, CT_LOOSE, &[], body)
}

fn prefetch_hyper(ctx: &Ctx, since: u64, want_idx: bool, want_sizes: bool) -> Resp {
    let view = ctx.view();
    let sizes = want_sizes && view.blobs.is_some();
    let (plan, start) = match view.prefetch(since) {
        Prefetch::UpToDate if sizes => {
            let body = one(Bytes::from_static(UP_TO_DATE_SIZES));
            return respond(200, CT_PREFETCH, &[("x-gvfs-sizes", 1)], body);
        }
        Prefetch::UpToDate => {
            return respond(200, CT_PREFETCH, &[], one(Bytes::from_static(UP_TO_DATE)));
        }
        Prefetch::Pack(plan, start) => (plan, start),
    };
    inc(&ctx.metrics.prefetch_packs);
    let idx = if want_idx { plan.idx() } else { None };
    let idx_len = idx.map_or(u64::MAX, |i| i.len() as u64);
    let mut first = BytesMut::with_capacity(32 + plan.header.len());
    first.extend_from_slice(&gpre(&plan, idx_len));
    first.extend_from_slice(&plan.header);
    let mut parts = Vec::with_capacity(plan.slabs.len() + 4);
    parts.push(first.freeze());
    for (slab, at, len) in plan.bodies() {
        parts.push(ctx.slab(slab).slice(at as usize..(at + len) as usize));
    }
    parts.push(Bytes::copy_from_slice(&plan.trailer()));
    if let Some(idx) = idx {
        parts.push(Bytes::copy_from_slice(idx));
    }
    let section_len = if sizes {
        let s = ctx.section(&plan);
        let n = s.len() as u64;
        parts.push(s);
        n
    } else {
        0
    };
    let total = plan.len + 32 + idx.map_or(0, |i| i.len() as u64) + section_len;
    ctx.metrics.bytes_out.fetch_add(total, Ordering::Relaxed);
    ctx.metrics
        .objects_out
        .fetch_add(u64::from(plan.entries), Ordering::Relaxed);
    let pack_end = ("x-gvfs-pack-end", plan.timestamp);
    let body = Body::Many(parts.into_iter(), total);
    if sizes {
        let extra = [("x-gvfs-pack-start", start), pack_end, ("x-gvfs-sizes", 1)];
        respond(200, CT_PREFETCH, &extra, body)
    } else {
        respond(
            200,
            CT_PREFETCH,
            &[("x-gvfs-pack-start", start), pack_end],
            body,
        )
    }
}

async fn sizes_hyper(ctx: &Ctx, body: Incoming) -> Resp {
    let Ok(body) = body.collect().await.map(|c| c.to_bytes()) else {
        return text(400, "bad body\n");
    };
    let Ok(_permit) = ctx.heavy_permits.acquire().await else {
        return text(503, "busy\n");
    };
    let h = ctx.heavy.take();
    let (h, st) = run_sizes(ctx, h, Some(body)).await;
    let r = match st {
        Sizes::Ok => respond(200, CT_JSON, &[], one(Bytes::copy_from_slice(&h.out))),
        Sizes::TooMany => text(413, "too many objects\n"),
        Sizes::NotFound => text(404, "not found\n"),
        Sizes::TooLarge => text(413, "response too large\n"),
    };
    ctx.heavy.put(h);
    r
}

// ---------------------------------------------------------------------------------------

fn arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("spike: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mode = arg(&args, "--mode").unwrap_or_else(|| "own".into());
    let hyper = match mode.as_str() {
        "own" => false,
        "hyper" => true,
        m => return Err(format!("--mode is own or hyper, not {m}").into()),
    };
    let store = arg(&args, "--store").ok_or("--store is required")?;
    let bind = arg(&args, "--bind").unwrap_or_else(|| "127.0.0.1:8080".into());
    let threads: usize = arg(&args, "--threads").map_or(Ok(8), |v| v.parse())?;
    let cache_dir = arg(&args, "--cache-dir").map_or_else(
        || std::env::temp_dir().join("gvfs-spike-cache"),
        PathBuf::from,
    );
    let idle: u64 = arg(&args, "--idle-secs").map_or(Ok(30), |v| v.parse())?;
    let nice: i32 = arg(&args, "--nice").map_or(Ok(10), |v| v.parse())?;
    let heavy_mode = match arg(&args, "--heavy").as_deref() {
        None | Some("inline") => HeavyMode::Inline,
        Some("blocking") => HeavyMode::Blocking(None),
        Some("nice") => HeavyMode::Blocking(Some(nice)),
        Some("yield") => HeavyMode::Yield,
        Some(h) => {
            return Err(format!("--heavy is inline, blocking, nice or yield, not {h}").into());
        }
    };

    let view = gvfs_daemon::serve::load_view(
        Store::open(&store, "AZURE_STORAGE_CONNECTION_STRING")?,
        cache_dir,
    )?;
    // As the edge's warmer: the cold plan's trailer and sizes section are ready.
    let cold = view.cold_plan();
    if let Some(plan) = &cold {
        let _ = plan.trailer();
        if view.blobs.is_some() {
            let _ = plan.sizes_section();
        }
    }
    let mut slab_bytes = HashMap::new();
    let mut section_bytes = HashMap::new();
    if hyper {
        for s in view
            .all
            .iter()
            .chain(cold.iter().flat_map(|p| p.slabs.iter()))
        {
            slab_bytes
                .entry(Arc::as_ptr(s) as usize)
                .or_insert_with(|| Bytes::from_owner(SlabBytes(Arc::clone(s))));
        }
        if let (Some(p), true) = (&cold, view.blobs.is_some()) {
            let b = Bytes::from_owner(SectionBytes(Arc::clone(p)));
            section_bytes.insert(Arc::as_ptr(p) as usize, b);
        }
    }
    gvfs_daemon::serve::raise_fd_limit();
    let listener = std::net::TcpListener::bind(&bind)?;
    nix::sys::socket::listen(&listener, nix::sys::socket::Backlog::MAXALLOWABLE)?;
    listener.set_nonblocking(true)?;
    let config = b"{\"AllowedGvfsClientVersions\":null,\"CacheServers\":[]}".to_vec();
    let (slabs, tip) = (view.all.len(), view.tip);
    let ctx = Arc::new(Ctx {
        view: RwLock::new(view),
        config_bytes: Bytes::from(config.clone()),
        config,
        metrics: Metrics::default(),
        bufs: Pool::new(threads, if hyper { 0 } else { BUFS }, new_bufs),
        heavy: Pool::new(threads, HEAVY, new_heavy),
        heavy_permits: Semaphore::new(HEAVY),
        heavy_mode,
        idle: Duration::from_secs(idle),
        patience: Duration::from_secs(30),
        write: Duration::from_secs(120),
        slab_bytes,
        section_bytes,
    });
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let listener = TcpListener::from_std(listener)?;
        gvfs_alloc_guard::seal();
        eprintln!("spike: {mode} on http://{bind}: {slabs} slabs, tip {tip}, {threads} workers");
        loop {
            let s = match listener.accept().await {
                Ok((s, _)) => s,
                Err(e) => {
                    eprintln!("spike: accept: {e}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };
            let ctx = Arc::clone(&ctx);
            if hyper {
                tokio::spawn(conn_hyper(ctx, s));
            } else {
                tokio::spawn(conn_own(ctx, s));
            }
        }
    })
}
