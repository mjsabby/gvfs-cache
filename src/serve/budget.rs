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

/// Runs `f`, which computes for long (a large object's delta chain, a commit closure's
/// plan), on this thread while the runtime moves the worker's other tasks to another
/// thread (a cold section: that may start one). For a request's own work only: what many
/// requests wait for is built through [`Builds`].
pub fn blocking<R>(f: impl FnOnce() -> R) -> R {
    let _cold = gvfs_alloc_guard::cold_section();
    tokio::task::block_in_place(f)
}

/// What is being built once for every request that needs it: a prefetch plan, its SHA-1
/// trailer, its `.idx`, its sizes section. The warmer usually has them ready; a request
/// that finds one missing (a new snapshot's, before the warmer got to it) has it built on
/// one thread of the blocking pool, and parks its task until it is there, as does every
/// request after it. Each used to wait on a thread of its own inside `block_in_place`, on
/// the cell being filled: 700 cold clones arriving with a new snapshot took the pool's 512
/// threads, and every other request, a `/gvfs/config` among them, waited behind them for
/// as long as the build took (then 5.6 s on SearchGold).
#[derive(Default)]
pub struct Builds {
    /// Woken when a build ends.
    done: tokio::sync::Notify,
    /// The builds running, each by what it builds: an address and a part of it.
    running: std::sync::Mutex<std::collections::HashSet<(usize, usize)>>,
}

/// A build's end, however it ends: its key is free and those waiting look again.
struct Ended(std::sync::Arc<Builds>, (usize, usize));

impl Drop for Ended {
    fn drop(&mut self) {
        self.0.running().remove(&self.1);
        self.0.done.notify_waiters();
    }
}

impl Builds {
    fn running(&self) -> std::sync::MutexGuard<'_, std::collections::HashSet<(usize, usize)>> {
        match self.running.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// What `ready` returns once it returns something, with `build` (which makes it so)
    /// run on the blocking pool unless a build of `key` is running already. `None` if
    /// builds this request started ended twice with nothing ready: it gives up rather
    /// than start a third. A cold path: await it through [`cold`].
    pub async fn wait<T>(
        self: &std::sync::Arc<Self>,
        key: (usize, usize),
        ready: impl Fn() -> Option<T>,
        build: impl Fn() + Clone + Send + 'static,
    ) -> Option<T> {
        let mut started = 0u32;
        loop {
            // Waiting is registered before `ready` is asked: a build that ends in between
            // still wakes this request.
            let mut done = std::pin::pin!(self.done.notified());
            done.as_mut().enable();
            if let Some(v) = ready() {
                return Some(v);
            }
            if self.running().insert(key) {
                if started == 2 {
                    self.running().remove(&key);
                    return None;
                }
                started = started.saturating_add(1);
                let ended = Ended(std::sync::Arc::clone(self), key);
                let build = build.clone();
                tokio::task::spawn_blocking(move || {
                    let _ended = ended;
                    build();
                });
            }
            done.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    /// A thousand requests waiting for one thing are a thousand parked tasks and one
    /// build: here on a runtime with a single thread and a blocking pool of one, where a
    /// waiter that needed a thread of its own would never be served. Another thing is
    /// built beside it, once too; and a build that makes nothing ready is given up on
    /// after two.
    #[test]
    #[allow(clippy::arithmetic_side_effects)]
    fn waiters_share_one_build_and_hold_no_thread() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let builds = Arc::new(Builds::default());
        let cells: Arc<[std::sync::OnceLock<usize>; 2]> = Arc::default();
        let built = Arc::new(AtomicUsize::new(0));
        let got: Vec<usize> = rt.block_on(async {
            let mut tasks = Vec::new();
            for i in 0..1000usize {
                let (builds, cells, built) = (builds.clone(), cells.clone(), built.clone());
                tasks.push(tokio::spawn(async move {
                    let which = i % 2;
                    let filled = cells.clone();
                    let build = move || {
                        std::thread::sleep(Duration::from_millis(50));
                        built.fetch_add(1, Ordering::SeqCst);
                        let _ = filled[which].set(which + 10);
                    };
                    let ready = || cells[which].get().copied();
                    builds.wait((7, which), ready, build).await.unwrap()
                }));
            }
            let mut got = Vec::new();
            for t in tasks {
                got.push(t.await.unwrap());
            }
            got
        });
        assert_eq!(got.len(), 1000);
        assert!(got.iter().enumerate().all(|(i, &v)| v == i % 2 + 10));
        assert_eq!(built.load(Ordering::SeqCst), 2, "one build of each");
        assert!(builds.running().is_empty());

        let tries = Arc::new(AtomicUsize::new(0));
        let counted = tries.clone();
        let build = move || {
            counted.fetch_add(1, Ordering::SeqCst);
        };
        let nothing = rt.block_on(builds.wait((7, 2), || None::<()>, build));
        assert_eq!((nothing, tries.load(Ordering::SeqCst)), (None, 2));
        assert!(builds.running().is_empty());
    }
}
