//! `gvfs-alloc-guard` — enforcement of DESIGN.md invariant 3 ("zero heap allocations after warmup")
//! plus safe read-only OS memory mapping (`mmap_readonly`) returning zero-copy `Bytes`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs::File;
use std::io;
use std::marker::PhantomData;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use bytes::Bytes;

static SEALED: AtomicBool = AtomicBool::new(false);
static MODE: AtomicU8 = AtomicU8::new(MODE_COUNT);
static TRACE: AtomicBool = AtomicBool::new(false);
static HOT_PATH_ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static POST_SEAL_ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static HOT_PATH_ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

const MODE_COUNT: u8 = 0;
const MODE_STRICT: u8 = 1;

thread_local! {
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
    pub hot_path: u64,
    pub hot_path_bytes: u64,
    pub post_seal: u64,
}

/// The global allocator wrapper.
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
// SAFETY: every method forwards the caller's pointer/layout unchanged to `System`.
unsafe impl GlobalAlloc for SealedAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if on_allocate(layout.size()) == Verdict::Reject {
            return std::ptr::null_mut();
        }
        // SAFETY: layout is forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if on_allocate(layout.size()) == Verdict::Reject {
            return std::ptr::null_mut();
        }
        // SAFETY: layout is forwarded unchanged to the system allocator.
        unsafe { System.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` came from `System` through this wrapper.
        unsafe { System.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if on_allocate(new_size) == Verdict::Reject {
            return std::ptr::null_mut();
        }
        // SAFETY: `ptr`/`layout`/`new_size` are forwarded unchanged to `System`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

pub fn seal() {
    SEALED.store(true, Ordering::SeqCst);
}

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

pub fn reset_counters() {
    HOT_PATH_ALLOC_COUNT.store(0, Ordering::SeqCst);
    HOT_PATH_ALLOC_BYTES.store(0, Ordering::SeqCst);
    POST_SEAL_ALLOC_COUNT.store(0, Ordering::SeqCst);
}

#[derive(Debug)]
#[must_use = "the hot-path region ends when the guard is dropped"]
pub struct HotPathGuard {
    _not_send: PhantomData<*const ()>,
}

#[inline]
pub fn hot_path() -> HotPathGuard {
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

pub fn in_hot_path() -> bool {
    HOT_DEPTH.try_with(|d| d.get() > 0).unwrap_or(false)
}

#[derive(Debug)]
#[must_use = "the cold section ends when the guard is dropped"]
pub struct ColdSectionGuard {
    saved: u32,
    _not_send: PhantomData<*const ()>,
}

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

/// Read-only memory-mapped region owned by a `Bytes` handle.
struct MmapOwner {
    ptr: *const u8,
    len: usize,
}

#[allow(unsafe_code)]
// SAFETY: the mapping is `PROT_READ` and immutable for its lifetime; `munmap` is thread-safe.
unsafe impl Send for MmapOwner {}
#[allow(unsafe_code)]
// SAFETY: concurrent readers only read immutable `PROT_READ` pages.
unsafe impl Sync for MmapOwner {}

impl AsRef<[u8]> for MmapOwner {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        #[allow(unsafe_code)]
        // SAFETY: `ptr` was returned by a successful `mmap` of `len` bytes with `PROT_READ`
        // over an immutable slab/pack file and remains mapped until `Drop`.
        unsafe {
            std::slice::from_raw_parts(self.ptr, self.len)
        }
    }
}

impl Drop for MmapOwner {
    fn drop(&mut self) {
        if self.len > 0 {
            #[allow(unsafe_code)]
            // SAFETY: `ptr` and `len` came from a successful `libc::mmap` call.
            unsafe {
                let _ = libc::munmap(self.ptr.cast_mut().cast(), self.len);
            }
        }
    }
}

/// Memory-maps an immutable file (`.pack`, `.idx`, `.rev`, `.mphf`, `.hfs`) read-only
/// and wraps it in a zero-copy `Bytes` handle so sub-slicing (`bytes.slice(a..b)`)
/// is $O(1)$ and allocation-free.
pub fn mmap_readonly(path: &Path) -> io::Result<Bytes> {
    let file = File::open(path)?;
    let meta = file.metadata()?;
    let len = usize::try_from(meta.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "file too large for address space",
        )
    })?;
    if len == 0 {
        return Ok(Bytes::new());
    }
    #[allow(unsafe_code)]
    // SAFETY: `file.as_raw_fd()` is a valid open file descriptor; `PROT_READ` + `MAP_PRIVATE`
    // creates a private read-only mapping of `len` bytes starting at offset 0.
    let raw = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };
    if raw == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(Bytes::from_owner(MmapOwner {
        ptr: raw.cast::<u8>(),
        len,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static GLOBAL: SealedAllocator = SealedAllocator;

    #[test]
    fn counts_hot_path_allocations_only_after_seal() {
        reset_counters();
        {
            let _hot = hot_path();
            let v: Vec<u8> = Vec::with_capacity(64);
            std::hint::black_box(&v);
        }
        assert_eq!(stats().hot_path, 0);

        seal();
        let cold: Vec<u8> = Vec::with_capacity(64);
        std::hint::black_box(&cold);
        assert!(stats().post_seal >= 1);
        assert_eq!(stats().hot_path, 0);

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
