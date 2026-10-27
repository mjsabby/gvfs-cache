//! `gvfs-alloc-guard` — enforcement of the "zero heap allocations after warmup" invariant.
//!
//! [`SealedAllocator`] wraps [`std::alloc::System`]. Until [`seal`] is called it is a
//! transparent pass-through. After sealing every allocation increments
//! [`AllocStats::post_seal`]; allocations made while the current thread is inside a
//! [`hot_path`] region also increment [`AllocStats::hot_path`] and, in [`Mode::Strict`],
//! are rejected (the allocator returns null and the Rust runtime aborts), which is how CI
//! proves that request handling and L0 ingestion never touch the heap.
//!
//! This is one of the two crates in the workspace that contain `unsafe` code: the trait
//! `GlobalAlloc` is `unsafe` to implement. Every block forwards verbatim to `System`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

static SEALED: AtomicBool = AtomicBool::new(false);
static MODE: AtomicU8 = AtomicU8::new(MODE_COUNT);
static HOT_PATH_ALLOCS: AtomicU64 = AtomicU64::new(0);
static HOT_PATH_BYTES: AtomicU64 = AtomicU64::new(0);
static POST_SEAL_ALLOCS: AtomicU64 = AtomicU64::new(0);

const MODE_COUNT: u8 = 0;
const MODE_STRICT: u8 = 1;

thread_local! {
    // Const-initialised and without a destructor, so touching it from inside the
    // allocator never allocates or registers TLS destructors.
    static HOT_DEPTH: Cell<u32> = const { Cell::new(0) };
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

#[inline]
fn account(size: usize) -> bool {
    if !SEALED.load(Ordering::Relaxed) {
        return true;
    }
    POST_SEAL_ALLOCS.fetch_add(1, Ordering::Relaxed);
    let in_hot_path = HOT_DEPTH.try_with(Cell::get).unwrap_or(0) > 0;
    if !in_hot_path {
        return true;
    }
    HOT_PATH_ALLOCS.fetch_add(1, Ordering::Relaxed);
    HOT_PATH_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    MODE.load(Ordering::Relaxed) != MODE_STRICT
}

// SAFETY: every method forwards to `System` with the exact layout it was given; the
// bookkeeping above touches only atomics and a const-initialised thread-local.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for SealedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !account(layout.size()) {
            return core::ptr::null_mut();
        }
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if !account(layout.size()) {
            return core::ptr::null_mut();
        }
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if !account(new_size) {
            return core::ptr::null_mut();
        }
        // SAFETY: same contract as the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Ends the warmup phase: from now on allocations are counted.
pub fn seal() {
    SEALED.store(true, Ordering::SeqCst);
}

pub fn set_mode(mode: Mode) {
    MODE.store(
        match mode {
            Mode::Count => MODE_COUNT,
            Mode::Strict => MODE_STRICT,
        },
        Ordering::SeqCst,
    );
}

pub fn is_sealed() -> bool {
    SEALED.load(Ordering::Relaxed)
}

pub fn stats() -> AllocStats {
    AllocStats {
        sealed: is_sealed(),
        hot_path: HOT_PATH_ALLOCS.load(Ordering::Relaxed),
        hot_path_bytes: HOT_PATH_BYTES.load(Ordering::Relaxed),
        post_seal: POST_SEAL_ALLOCS.load(Ordering::Relaxed),
    }
}

/// Marks the current thread as being on the hot path for the duration of the guard.
#[derive(Debug)]
pub struct HotPathGuard(());

impl HotPathGuard {
    pub fn enter() -> Self {
        let _ = HOT_DEPTH.try_with(|d| d.set(d.get().saturating_add(1)));
        Self(())
    }
}

impl Drop for HotPathGuard {
    fn drop(&mut self) {
        let _ = HOT_DEPTH.try_with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Runs `f` inside a hot-path region.
#[inline]
pub fn hot_path<T>(f: impl FnOnce() -> T) -> T {
    let _g = HotPathGuard::enter();
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static GLOBAL: SealedAllocator = SealedAllocator;

    #[test]
    fn counts_hot_path_allocations_after_seal() {
        let before = stats();
        assert!(!before.sealed);
        let v: Vec<u8> = hot_path(|| vec![1u8; 64]);
        assert_eq!(
            stats().hot_path,
            before.hot_path,
            "nothing is counted before seal"
        );
        seal();
        let w: Vec<u8> = hot_path(|| vec![2u8; 128]);
        let after = stats();
        assert!(after.sealed);
        assert!(after.hot_path >= 1);
        assert!(after.hot_path_bytes >= 128);
        assert!(after.post_seal >= 1);
        let hp = after.hot_path;
        let _x: Vec<u8> = vec![3u8; 16];
        assert_eq!(
            stats().hot_path,
            hp,
            "allocations outside a region are not hot-path"
        );
        drop((v, w));
    }
}
