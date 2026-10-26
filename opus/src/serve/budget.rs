//! Cooperative scheduling for CPU-bound request work.
//!
//! A tokio worker runs one task until it awaits. A request that computes for a millisecond
//! (a batch's lookups, a commit's trees, a pack's SHA-1) would hold up every request queued
//! behind it on that worker. Such loops count their work in a [`Budget`] and yield once
//! they have run for about [`QUANTUM`] (`--quantum-us`), so a GET waits at most a quantum
//! per busy request ahead of it. REPORT.md §14 measured GETs on 64 connections beside 32
//! connections of 4,000-blob POSTs: without yielding, GET p50 1.3 ms; yielding every 100 µs,
//! p99 293 µs (the thread-per-request edge: 1.3 ms) with the POSTs' throughput kept; every
//! 20 µs, p99 136 µs but 29% fewer POSTs. Moving the work to tokio's blocking pool does not
//! do it, even at a lower priority: its threads share the CPUs with the workers as equals,
//! and a woken worker waits out the running thread's time slice.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Work between yields, by default (`--quantum-us`).
pub const QUANTUM: Duration = Duration::from_micros(100);
static QUANTUM_NS: AtomicU64 = AtomicU64::new(100_000);

/// Sets the work between yields (at startup).
pub fn set_quantum(q: Duration) {
    QUANTUM_NS.store(
        u64::try_from(q.as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}
/// Units of work between clock reads (a read is ~20 ns; a unit up to ~1 µs).
const CHECK_EVERY: u32 = 64;

/// The time a request has computed since it last yielded.
pub struct Budget {
    since: Instant,
    units: u32,
}

impl Budget {
    pub fn new() -> Budget {
        Budget {
            since: Instant::now(),
            units: 0,
        }
    }

    /// Counts one small unit of work (a lookup: ~0.1-1 µs); true once the quantum is spent:
    /// then [`Budget::pause`].
    #[inline]
    pub fn due(&mut self) -> bool {
        self.spend(1)
    }

    /// Counts `units` of work (64 or more for a step that may take the quantum itself, so
    /// that the clock is read after every such step).
    #[inline]
    pub fn spend(&mut self, units: u32) -> bool {
        self.units = self.units.saturating_add(units);
        if self.units < CHECK_EVERY {
            return false;
        }
        self.units = 0;
        let quantum = Duration::from_nanos(QUANTUM_NS.load(Ordering::Relaxed));
        self.since.elapsed() >= quantum
    }

    /// Lets the worker poll for I/O and run its other tasks before this one goes on.
    pub async fn pause(&mut self) {
        yield_now().await;
        self.since = Instant::now();
    }
}

impl Default for Budget {
    fn default() -> Self {
        Self::new()
    }
}

/// Budget units for `n` bytes hashed, copied or written: one per 4 KiB (~1.4 µs).
#[inline]
pub fn byte_units(n: usize) -> u32 {
    u32::try_from(n >> 12).unwrap_or(u32::MAX).saturating_add(1)
}

/// tokio's `yield_now`: the worker polls for I/O before the task resumes. Its list of
/// yielded tasks grows to its high-water mark, rarely: a cold section.
async fn yield_now() {
    let mut y = std::pin::pin!(tokio::task::yield_now());
    std::future::poll_fn(|cx| {
        let _cold = gvfs_alloc_guard::cold_section();
        y.as_mut().poll(cx)
    })
    .await
}

/// Runs `f` as a cold section: allocations while it is polled are not the hot path's (a
/// fetch from the upstream or object storage: its client, TLS and buffers allocate). `f`
/// lives on the heap (allocated here, in the cold section): a rare path's large state (a
/// TLS handshake, a fetch pipeline) would otherwise be part of every connection's task,
/// parked or not, whether that path is ever taken or not.
pub fn cold<F: Future>(f: F) -> Cold<F> {
    let _cold = gvfs_alloc_guard::cold_section();
    Cold(Box::pin(f))
}

/// The future [`cold`] returns.
pub struct Cold<F>(std::pin::Pin<Box<F>>);

impl<F: Future> Future for Cold<F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        let _cold = gvfs_alloc_guard::cold_section();
        self.0.as_mut().poll(cx)
    }
}

/// Runs `f`, which computes for long (a plan built once, a large object's delta chain), on
/// this thread while the runtime moves the worker's other tasks to another thread (a cold
/// section: that may start one).
pub fn blocking<R>(f: impl FnOnce() -> R) -> R {
    let _cold = gvfs_alloc_guard::cold_section();
    tokio::task::block_in_place(f)
}
