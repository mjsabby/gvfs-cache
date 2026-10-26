//! One task per connection.
//!
//! Between requests a connection holds no buffer. Its task waits up to `--idle-secs` for
//! the next request's bytes, then takes a head buffer and an output buffer from a
//! [`Pool`], serves the request, and gives them back. It keeps them only while pipelined
//! bytes are buffered. An idle client costs its parked task (about 3 KB) and a descriptor.
//! Beyond `--max-idle-conns` idle connections, a connection that goes idle after a response
//! is closed instead, and its client reconnects (a new connection is always served first).
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
    pub out: Vec<u8>,
}

/// Connection buffers, made at startup in per-thread shards; more are made (a cold
/// section) while every shard is empty, e.g. for many slow downloads at once. The pool
/// keeps at most twice its startup count: a burst's extra buffers are freed after it.
pub struct Pool {
    shards: Vec<Mutex<Vec<Bufs>>>,
    out_bytes: usize,
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

impl Pool {
    /// `count` sets of buffers (a head buffer and `out_bytes` of output each) in `shards`
    /// shards, each with room to take back every one of them.
    pub fn new(shards: usize, count: usize, out_bytes: usize) -> Pool {
        let n = shards.max(1);
        // Room for twice a shard's share: buffers go back to the thread that frees them.
        let keep = count.div_ceil(n).saturating_mul(2).max(2);
        let shards: Vec<Mutex<Vec<Bufs>>> = (0..n)
            .map(|_| Mutex::new(Vec::with_capacity(keep)))
            .collect();
        for i in 0..count {
            if let Some(Ok(mut s)) = shards
                .get(i.checked_rem(shards.len()).unwrap_or(0))
                .map(|s| s.lock())
            {
                s.push(new_bufs(out_bytes));
            }
        }
        Pool { shards, out_bytes }
    }

    pub fn take(&self) -> Bufs {
        let n = self.shards.len();
        let first = shard(n);
        for k in 0..n {
            let i = first.wrapping_add(k).checked_rem(n).unwrap_or(0);
            if let Some(Ok(mut s)) = self.shards.get(i).map(|s| s.lock()) {
                if let Some(b) = s.pop() {
                    return b;
                }
            }
        }
        let _cold = gvfs_alloc_guard::cold_section();
        new_bufs(self.out_bytes)
    }

    pub fn put(&self, mut b: Bufs) {
        b.input.filled = 0;
        let n = self.shards.len();
        let first = shard(n);
        for k in 0..n {
            let i = first.wrapping_add(k).checked_rem(n).unwrap_or(0);
            if let Some(Ok(mut s)) = self.shards.get(i).map(|s| s.lock()) {
                if s.len() < s.capacity() {
                    s.push(b);
                    return;
                }
            }
        }
        // Every shard is full (buffers made during a burst): free it.
        let _cold = gvfs_alloc_guard::cold_section();
        drop(b);
    }
}

fn new_bufs(out_bytes: usize) -> Bufs {
    Bufs {
        input: Input::new(),
        out: Vec::with_capacity(out_bytes),
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
/// while `--max-idle-conns` others are.
pub async fn serve(ctx: Arc<Ctx>, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    connection(&ctx, &stream).await;
    // Closing deregisters the socket from tokio's driver, which allocates (once per
    // connection, like accepting it; never per request).
    let _cold = gvfs_alloc_guard::cold_section();
    drop(stream);
}

async fn connection(ctx: &Ctx, stream: &TcpStream) {
    let mut held: Option<Bufs> = None;
    // Whether a response has gone out: until then the cap does not apply (a wake can find
    // nothing to read, which tokio allows, so the first wake does not tell).
    let mut served = false;
    loop {
        if held.is_none() {
            // The cap applies to a connection going idle after a response (its client
            // reconnects); a new connection's first request is always read.
            if served && ctx.idle.load(Ordering::Relaxed) >= ctx.max_idle {
                ctx.metrics.idle_overflow.fetch_add(1, Ordering::Relaxed);
                return;
            }
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
        match request(ctx, stream, &mut b).await {
            Next::Again => {
                served = true;
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
    let req = match httpd::read_head(stream, &mut b.input, ctx.deadlines.patience).await {
        Ok(Head::Request(r)) => r,
        Ok(Head::Idle) => return Next::Idle,
        Ok(Head::Closed) => return Next::Close,
        Err(_) => {
            let t = ctx.deadlines.write;
            let _ = httpd::respond_text(stream, 400, "bad request\n", false, t).await;
            return Next::Close;
        }
    };
    match super::handlers::handle(ctx, b, stream, &req).await {
        Ok(true) => Next::Again,
        Ok(false) | Err(_) => Next::Close,
    }
}
