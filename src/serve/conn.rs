//! One task per connection.
//!
//! Between requests a connection holds no buffer. Its task waits up to `--idle-secs` for
//! the next request's bytes, then takes a head buffer and an output buffer from a
//! [`Pool`], serves the request, and gives them back. It keeps them only while pipelined
//! bytes are buffered. While a head arrives in pieces (a slow client), the output buffer
//! (256 KiB) goes back to a pool and the connection holds only its head buffer (16 KiB). An
//! idle client costs its parked task (about 3 KB) and a descriptor. Beyond
//! `--max-idle-conns` idle connections, a connection that goes idle after a response is
//! closed instead, and its client reconnects (a new connection is always served first).
//! At most `--max-conns` connections are open: past that, new ones wait in the listen
//! queue until one closes.
//!
//! A slow client parks only its own task: a response waits for the socket to drain with
//! the worker free for other connections (REPORT.md §14: 400 slow clones beside 1.2M GETs
//! a second).

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::net::TcpStream;

use super::Ctx;
use super::httpd::{self, Head, Input};

/// The buffers of a connection with a request in progress.
pub struct Bufs {
    pub input: Input,
    /// The response buffer; also where a request body is read (before an arena is leased).
    pub out: Vec<u8>,
    /// What `out` took from [`Ctx::spare`] to grow past its pooled size.
    pub extra: usize,
    pooled: usize,
}

impl Bufs {
    /// `out` back to its pooled size, its growth returned to `spare` (a cold section).
    pub fn shrink(&mut self, spare: &httpd::Spare) {
        if self.extra > 0 {
            let _cold = gvfs_alloc_guard::cold_section();
            self.out.clear();
            self.out.shrink_to(self.pooled);
            spare.give(self.extra);
            self.extra = 0;
        }
    }
}

/// What a [`Pool`] holds: made with the pool's size, emptied when it goes back.
pub trait Pooled: Sized {
    fn fresh(size: usize) -> Self;
    fn reset(&mut self);
}

impl Pooled for Bufs {
    fn fresh(out_bytes: usize) -> Bufs {
        Bufs {
            input: Input::new(),
            out: Vec::with_capacity(out_bytes),
            extra: 0,
            pooled: out_bytes,
        }
    }

    fn reset(&mut self) {
        self.input.filled = 0;
    }
}

impl Pooled for Vec<u8> {
    fn fresh(bytes: usize) -> Vec<u8> {
        Vec::with_capacity(bytes)
    }

    fn reset(&mut self) {
        self.clear();
    }
}

/// Connection buffers, made at startup in per-thread shards; more are made (a cold
/// section) while every shard is empty, e.g. for many slow downloads at once. The pool
/// keeps at most `keep` a shard (by default twice its startup count): a burst's extra
/// buffers are freed after it.
pub struct Pool<T> {
    shards: Vec<Mutex<Vec<T>>>,
    size: usize,
}

thread_local! {
    // Const-initialised and without a destructor (the allocator guard's rule for TLS).
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
    /// This thread's idle connections not yet folded into `Ctx::idle`.
    static IDLE_DELTA: Cell<i64> = const { Cell::new(0) };
}

/// Counts `d` (+1, -1) idle connections. Every request goes idle and back, so a shared count
/// updated by every core for each of them costs as much as a small request's own work: each
/// thread folds its count in once it has drifted by `ctx.idle_fold`, and `--max-idle-conns`
/// holds to within that many per thread.
fn idle_add(ctx: &Ctx, d: i64) {
    IDLE_DELTA.with(|c| {
        let v = c.get().saturating_add(d);
        if v.unsigned_abs() >= ctx.idle_fold {
            ctx.idle.fetch_add(v, Ordering::Relaxed);
            c.set(0);
        } else {
            c.set(v);
        }
    });
}
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

/// This thread's shard of `n`.
fn shard(n: usize) -> usize {
    SHARD.with(|s| {
        let mut i = s.get();
        if i == usize::MAX {
            i = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
            s.set(i);
        }
        i.checked_rem(n).unwrap_or(0)
    })
}

impl<T: Pooled> Pool<T> {
    /// `count` items of `size` in `shards` shards, each with room to take back twice its
    /// share of them (buffers go back to the thread that frees them), or `keep` if more.
    pub fn new(shards: usize, count: usize, size: usize, keep: usize) -> Pool<T> {
        let n = shards.max(1);
        let keep = count.div_ceil(n).saturating_mul(2).max(keep).max(2);
        let shards: Vec<Mutex<Vec<T>>> = (0..n)
            .map(|_| Mutex::new(Vec::with_capacity(keep)))
            .collect();
        for i in 0..count {
            if let Some(Ok(mut s)) = shards
                .get(i.checked_rem(shards.len()).unwrap_or(0))
                .map(|s| s.lock())
            {
                s.push(T::fresh(size));
            }
        }
        Pool { shards, size }
    }

    pub fn take(&self) -> T {
        let n = self.shards.len();
        let first = shard(n);
        for k in 0..n {
            let i = first.wrapping_add(k).checked_rem(n).unwrap_or(0);
            if let Some(Ok(mut s)) = self.shards.get(i).map(|s| s.lock())
                && let Some(b) = s.pop()
            {
                return b;
            }
        }
        let _cold = gvfs_alloc_guard::cold_section();
        T::fresh(self.size)
    }

    pub fn put(&self, mut b: T) {
        b.reset();
        let n = self.shards.len();
        let first = shard(n);
        for k in 0..n {
            let i = first.wrapping_add(k).checked_rem(n).unwrap_or(0);
            if let Some(Ok(mut s)) = self.shards.get(i).map(|s| s.lock())
                && s.len() < s.capacity()
            {
                s.push(b);
                return;
            }
        }
        // Every shard is full (buffers made during a burst): free it.
        let _cold = gvfs_alloc_guard::cold_section();
        drop(b);
    }
}

/// Polls `F` inside the allocation guard's hot-path scope. A scope cannot be held across an
/// `.await` (the guard is per thread, and a task may move between threads), so it is
/// entered for each poll instead.
pub struct HotPath<F>(Pin<Box<F>>);

impl<F> HotPath<F> {
    pub fn new(f: F) -> HotPath<F> {
        HotPath(Box::pin(f))
    }
}

impl<F: Future> Future for HotPath<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let _hot = gvfs_alloc_guard::hot_path();
        self.0.as_mut().poll(cx)
    }
}

enum Next {
    Again,
    /// Woken with nothing to read: still idle.
    Idle,
    Close,
}

/// Serves `stream` until its client closes, errs, idles past `--idle-secs`, or goes idle
/// while `--max-idle-conns` others are. `slot` is its place among `--max-conns`.
pub async fn serve(ctx: Arc<Ctx>, stream: TcpStream, slot: tokio::sync::OwnedSemaphorePermit) {
    ctx.metrics.conns_open.fetch_add(1, Ordering::Relaxed);
    let _ = stream.set_nodelay(true);
    connection(&ctx, &stream).await;
    // Closing deregisters the socket from tokio's driver, which allocates (once per
    // connection, like accepting it; never per request).
    let _cold = gvfs_alloc_guard::cold_section();
    drop(stream);
    drop(slot);
    ctx.metrics.conns_open.fetch_sub(1, Ordering::Relaxed);
}

async fn connection(ctx: &Ctx, stream: &TcpStream) {
    let mut held: Option<Bufs> = None;
    // Whether a response has just gone out, so the connection is going idle: the cap applies
    // then, once (a new connection's first request is always read). A wake that finds
    // nothing to read, which tokio allows (right after a response, the socket's readiness
    // is still set), is no new response: checking again then closed idle connections
    // while others merely connected.
    let mut responded = false;
    loop {
        if held.is_none() {
            if responded && ctx.idle.load(Ordering::Relaxed) >= ctx.max_idle {
                ctx.metrics.idle_overflow.fetch_add(1, Ordering::Relaxed);
                return;
            }
            responded = false;
            idle_add(ctx, 1);
            let woke = tokio::time::timeout(ctx.deadlines.idle, stream.readable()).await;
            idle_add(ctx, -1);
            match woke {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return,
                Err(_) => {
                    ctx.metrics.idle_closed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }
        let mut b = match held.take() {
            Some(b) => b,
            None => ctx.bufs.take(),
        };
        let next = request(ctx, stream, &mut b).await;
        b.shrink(&ctx.spare);
        match next {
            Next::Again => {
                responded = true;
                if b.input.filled > 0 {
                    held = Some(b);
                } else {
                    ctx.bufs.put(b);
                }
            }
            Next::Idle => ctx.bufs.put(b),
            Next::Close => {
                ctx.bufs.put(b);
                return;
            }
        }
    }
}

async fn request(ctx: &Ctx, stream: &TcpStream, b: &mut Bufs) -> Next {
    // All of the request, head and body, must arrive by then: a client that trickles bytes
    // cannot hold its buffers (or, for a POST, wait in line for an arena) for longer.
    let mut deadline = httpd::Deadline::after(ctx.deadlines.patience);
    // A head that does not come whole waits without the output buffer, which goes back to
    // a pool meanwhile and comes back (perhaps another one) when the head is in.
    let Bufs { input, out, .. } = b;
    let mut parked = false;
    let head = httpd::read_head(stream, input, &mut deadline, &mut || {
        if !parked {
            ctx.outs.put(std::mem::take(out));
            parked = true;
        }
    })
    .await;
    if parked {
        *out = ctx.outs.take();
    }
    let req = match head {
        Ok(Head::Request(r)) => r,
        Ok(Head::Idle) => return Next::Idle,
        Ok(Head::Closed) => return Next::Close,
        Err(e) => {
            httpd::refuse(stream, &e, ctx.deadlines.write).await;
            return Next::Close;
        }
    };
    match super::handlers::handle(ctx, b, stream, &req, &mut deadline).await {
        Ok(true) => Next::Again,
        Ok(false) | Err(_) => Next::Close,
    }
}
