//! `gvfs-alloc-guard` — enforcement of invariant 3 ("zero heap allocations after warmup").
//!
//! [`SealedAllocator`] wraps [`std::alloc::System`]. Until [`seal`] is called it is a
//! transparent pass-through. After sealing:
//!
//! * every allocation (`alloc`, `alloc_zeroed`, `realloc`) increments
//!   [`AllocStats::post_seal`];
//! * allocations performed while the current thread is inside a hot-path region
//!   (see [`hot_path`]) additionally increment [`AllocStats::hot_path`], and in
//!   [`Mode::Strict`] they are *rejected* (the allocator returns null, which makes the
//!   Rust runtime abort via `handle_alloc_error`). Strict mode is meant for CI.
//!
//! Hot-path regions are entered by the edge request handlers and by the tailer's
//! per-object ingestion loop. Third-party protocol machinery (hyper connection state,
//! TLS records, socket buffers) lives outside those regions and is only reflected in
//! the `post_seal` counter.
//!
//! This is the only crate in the workspace that contains `unsafe` code: the trait
//! `GlobalAlloc` is `unsafe` to implement. Every block forwards verbatim to `System`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

static SEALED: AtomicBool = AtomicBool::new(false);
static MODE: AtomicU8 = AtomicU8::new(MODE_COUNT);
static TRACE: AtomicBool = AtomicBool::new(false);
static HOT_PATH_ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static POST_SEAL_ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static HOT_PATH_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

const MODE_COUNT: u8 = 0;
const MODE_STRICT: u8 = 1;

thread_local! {
    // Const-initialised and without a destructor, so touching it from inside the
    // allocator never allocates or registers TLS destructors.
    static HOT_DEPTH: Cell<u32> = const { Cell::new(0) };
    static TRACING: Cell<bool> = const { Cell::new(false) };
}

/// What the allocator does with a hot-path allocation after sealing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Count the allocation and let it proceed (production default).
    Count,
    /// Count the allocation and fail it (CI / benchmark mode).
    Strict,
}

/// Snapshot of the allocation counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AllocStats {
    pub sealed: bool,
    /// Allocations made inside a [`hot_path`] region after [`seal`].
    pub hot_path: u64,
    /// Bytes requested by those hot-path allocations.
    pub hot_path_bytes: u64,
    /// All allocations (any thread, any region) after [`seal`].
    pub post_seal: u64,
}

/// The global allocator wrapper. Install it in the final binary with
/// `#[global_allocator] static GLOBAL: SealedAllocator = SealedAllocator;`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SealedAllocator;

#[derive(PartialEq, Eq)]
enum Verdict {
    Allow,
    Reject,
}

#[inline]
fn on_allocate(size: usize) -> Verdict {
    if !SEALED.load(Ordering::Relaxed) {
        return Verdict::Allow;
    }
    if TRACING.try_with(Cell::get).unwrap_or(true) {
        // Allocation made while reporting another one: never counted or traced.
        return Verdict::Allow;
    }
    POST_SEAL_ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    let hot = HOT_DEPTH.try_with(|d| d.get() > 0).unwrap_or(false);
    if !hot {
        return Verdict::Allow;
    }
    HOT_PATH_ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    HOT_PATH_ALLOC_BYTES.fetch_add(u64::try_from(size).unwrap_or(u64::MAX), Ordering::Relaxed);
    if TRACE.load(Ordering::Relaxed) {
        let _ = TRACING.try_with(|t| {
            t.set(true);
            let bt = std::backtrace::Backtrace::force_capture();
            eprintln!("gvfs-alloc-guard: hot-path allocation of {size} bytes at:\n{bt}");
            t.set(false);
        });
    }
    if MODE.load(Ordering::Relaxed) == MODE_STRICT {
        Verdict::Reject
    } else {
        Verdict::Allow
    }
}

#[allow(unsafe_code)]
// SAFETY: every method forwards the caller's pointer/layout unchanged to `System`, which
// upholds the `GlobalAlloc` contract. The bookkeeping in `on_allocate` only touches
// atomics and a const-initialised thread-local, so it never re-enters the allocator.
unsafe impl GlobalAlloc for SealedAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if on_allocate(layout.size()) == Verdict::Reject {
            return std::ptr::null_mut();
        }
        // SAFETY: same contract as our caller's; layout is passed through unchanged.
        unsafe { System.alloc(layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if on_allocate(layout.size()) == Verdict::Reject {
            return std::ptr::null_mut();
        }
        // SAFETY: same contract as our caller's; layout is passed through unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was produced by `System` through this wrapper with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if on_allocate(new_size) == Verdict::Reject {
            return std::ptr::null_mut();
        }
        // SAFETY: `ptr`/`layout` came from `System` through this wrapper; `new_size` is
        // forwarded unchanged, so the caller's obligations carry over directly.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Ends the warmup phase. Must be called once all pools, slabs and arenas exist.
pub fn seal() {
    SEALED.store(true, Ordering::SeqCst);
}

/// Re-opens the warmup phase (tests only; production never unseals).
pub fn unseal() {
    SEALED.store(false, Ordering::SeqCst);
}

pub fn is_sealed() -> bool {
    SEALED.load(Ordering::Relaxed)
}

pub fn set_mode(mode: Mode) {
    let raw = match mode {
        Mode::Count => MODE_COUNT,
        Mode::Strict => MODE_STRICT,
    };
    MODE.store(raw, Ordering::SeqCst);
}

pub fn mode() -> Mode {
    if MODE.load(Ordering::Relaxed) == MODE_STRICT {
        Mode::Strict
    } else {
        Mode::Count
    }
}

/// Diagnostics: print a backtrace for every hot-path allocation (slow; debugging only).
pub fn set_trace(on: bool) {
    TRACE.store(on, Ordering::SeqCst);
}

pub fn stats() -> AllocStats {
    AllocStats {
        sealed: is_sealed(),
        hot_path: HOT_PATH_ALLOC_COUNT.load(Ordering::Relaxed),
        hot_path_bytes: HOT_PATH_ALLOC_BYTES.load(Ordering::Relaxed),
        post_seal: POST_SEAL_ALLOC_COUNT.load(Ordering::Relaxed),
    }
}

/// Zeroes all counters (tests and benchmarks).
pub fn reset_counters() {
    HOT_PATH_ALLOC_COUNT.store(0, Ordering::SeqCst);
    HOT_PATH_ALLOC_BYTES.store(0, Ordering::SeqCst);
    POST_SEAL_ALLOC_COUNT.store(0, Ordering::SeqCst);
}

/// RAII marker for a hot-path region on the current thread.
///
/// The guard is deliberately `!Send`: holding it across an `.await` inside a spawned
/// task is a compile error, so a region can never "leak" onto another worker thread.
#[derive(Debug)]
#[must_use = "the hot-path region ends when the guard is dropped"]
pub struct HotPathGuard {
    _not_send: PhantomData<*const ()>,
}

/// Enters a hot-path region; allocations on this thread are tracked until the guard drops.
#[inline]
pub fn hot_path() -> HotPathGuard {
    // An error here means the thread is being torn down; tracking is then moot.
    let _ = HOT_DEPTH.try_with(|d| d.set(d.get().saturating_add(1)));
    HotPathGuard {
        _not_send: PhantomData,
    }
}

impl Drop for HotPathGuard {
    #[inline]
    fn drop(&mut self) {
        let _ = HOT_DEPTH.try_with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Whether the current thread is inside a hot-path region.
pub fn in_hot_path() -> bool {
    HOT_DEPTH.try_with(|d| d.get() > 0).unwrap_or(false)
}

/// RAII marker for a deliberate cold section nested inside a hot-path region (for example
/// publishing an epoch, which happens at most once per epoch interval). Restores the
/// enclosing hot-path depth on drop.
#[derive(Debug)]
#[must_use = "the cold section ends when the guard is dropped"]
pub struct ColdSectionGuard {
    saved: u32,
    _not_send: PhantomData<*const ()>,
}

/// Suspends hot-path tracking on this thread until the guard drops.
pub fn cold_section() -> ColdSectionGuard {
    let saved = HOT_DEPTH
        .try_with(|d| {
            let v = d.get();
            d.set(0);
            v
        })
        .unwrap_or(0);
    ColdSectionGuard {
        saved,
        _not_send: PhantomData,
    }
}

impl Drop for ColdSectionGuard {
    fn drop(&mut self) {
        let saved = self.saved;
        let _ = HOT_DEPTH.try_with(|d| d.set(saved));
    }
}

/// Touches one byte per 4 KiB page so the kernel backs the whole buffer with physical
/// memory at boot instead of taking minor faults on the hot path.
pub fn prefault(buf: &mut [u8]) {
    for page in buf.chunks_mut(4096) {
        if let Some(b) = page.first_mut() {
            *b = std::hint::black_box(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static GLOBAL: SealedAllocator = SealedAllocator;

    // A single test touches the process-wide counters so it cannot race with siblings.
    #[test]
    fn counts_hot_path_allocations_only_after_seal() {
        reset_counters();
        {
            let _hot = hot_path();
            let v: Vec<u8> = Vec::with_capacity(64);
            std::hint::black_box(&v);
        }
        assert_eq!(stats().hot_path, 0, "unsealed allocations are free");

        seal();
        let cold: Vec<u8> = Vec::with_capacity(64);
        std::hint::black_box(&cold);
        let after_cold = stats();
        assert!(after_cold.post_seal >= 1);
        assert_eq!(after_cold.hot_path, 0);

        {
            let _hot = hot_path();
            assert!(in_hot_path());
            let v: Vec<u8> = Vec::with_capacity(128);
            std::hint::black_box(&v);
        }
        assert!(!in_hot_path());
        let s = stats();
        assert_eq!(s.hot_path, 1);
        assert!(s.hot_path_bytes >= 128);
        unseal();
        reset_counters();
    }
}
