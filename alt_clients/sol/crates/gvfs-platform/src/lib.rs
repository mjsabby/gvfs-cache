//! The two OS boundaries that cannot be expressed in safe Rust: file mappings and
//! the forwarding global allocator. All daemon/format/transport code forbids unsafe.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{Mmap, MmapMut, MmapOptions};

static HOT_ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
thread_local! {
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

pub struct TrackingAllocator;

fn allocation() {
    let hot = DEPTH.try_with(|depth| depth.get() != 0).unwrap_or(false);
    if hot {
        HOT_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        let _ = COUNT.try_with(|count| count.set(count.get().saturating_add(1)));
    }
}

// SAFETY: every allocation/deallocation is forwarded with its original layout.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Synchronous hot region only: never hold this across an await or thread migration.
pub struct HotRegion {
    before: u64,
    _not_send: std::marker::PhantomData<*mut ()>,
}

impl HotRegion {
    pub fn enter() -> Self {
        let before = COUNT.with(Cell::get);
        DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self {
            before,
            _not_send: std::marker::PhantomData,
        }
    }

    pub fn allocations(&self) -> u64 {
        COUNT.with(Cell::get).saturating_sub(self.before)
    }
}

impl Drop for HotRegion {
    fn drop(&mut self) {
        DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

pub fn hot_allocations() -> u64 {
    HOT_ALLOCATIONS.load(Ordering::Relaxed)
}

/// An immutable, read-only file mapping. Unlinking/replacing a name is safe; changing
/// or truncating a mapped inode is not. The daemon's content-addressed store only
/// creates files with create-new + atomic publication, and never overwrites inodes.
/// Load an immutable file without exposing a mapping-safety contract to the daemon.
/// A private anonymous mapping provides the same slice interface, cannot SIGBUS if
/// an external process truncates the source, and becomes read-only before publication.
pub fn snapshot(
    path: &Path,
    maximum: usize,
) -> io::Result<impl AsRef<[u8]> + Send + Sync + 'static> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let len = usize::try_from(file.metadata()?.len())
        .map_err(|_| io::Error::other("file length exceeds address space"))?;
    if len == 0 || len > maximum {
        return Err(io::Error::other("empty file or mapping capacity exceeded"));
    }
    let mut mapping = MmapOptions::new().len(len).map_anon()?;
    file.read_exact(&mut mapping)?;
    if file.read(&mut [0u8; 1])? != 0 {
        return Err(io::Error::other("file changed while reading"));
    }
    mapping.make_read_only()
}

/// A bounded, anonymous temporary-file arena. Its private inode cannot be changed
/// by another daemon. Large experimental full-entry packs can spill through the
/// page cache instead of requiring tens of GiB of anonymous heap.
pub struct FileArena {
    map: MmapMut,
    file: File,
    cursor: usize,
}

impl FileArena {
    pub fn new(directory: &Path, capacity: usize) -> io::Result<Self> {
        let file = tempfile::tempfile_in(directory)?;
        file.set_len(capacity as u64)?;
        // SAFETY: this is a newly created anonymous inode; the fd never escapes.
        // Only this mapping can access it, and its length stays fixed until freeze.
        let map = unsafe { MmapOptions::new().len(capacity).map_mut(&file)? };
        Ok(Self {
            map,
            file,
            cursor: 0,
        })
    }

    pub fn len(&self) -> usize {
        self.cursor
    }
    pub fn is_empty(&self) -> bool {
        self.cursor == 0
    }
    pub fn capacity(&self) -> usize {
        self.map.len()
    }
    pub fn clear(&mut self) {
        self.cursor = 0;
    }
    pub fn truncate(&mut self, len: usize) -> io::Result<()> {
        if len > self.cursor {
            return Err(io::Error::other("file arena truncate exceeds cursor"));
        }
        self.cursor = len;
        Ok(())
    }
    pub fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let end = self
            .cursor
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("file arena overflow"))?;
        self.map
            .get_mut(self.cursor..end)
            .ok_or_else(|| io::Error::other("file arena exhausted"))?
            .copy_from_slice(bytes);
        self.cursor = end;
        Ok(())
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.map.get_mut(..self.cursor).unwrap_or_default()
    }
    pub fn freeze(self) -> io::Result<impl AsRef<[u8]> + Send + Sync + 'static> {
        let Self { map, file, cursor } = self;
        drop(map);
        file.set_len(cursor as u64)?;
        // SAFETY: no writable mapping or exposed fd remains. FrozenFile owns the
        // only fd, never writes/truncates it, and keeps it alive with the mapping.
        let map = unsafe { MmapOptions::new().len(cursor).map(&file)? };
        Ok(FrozenFile { map, _file: file })
    }
}

impl AsRef<[u8]> for FileArena {
    fn as_ref(&self) -> &[u8] {
        self.map.get(..self.cursor).unwrap_or_default()
    }
}

struct FrozenFile {
    map: Mmap,
    _file: File,
}
impl AsRef<[u8]> for FrozenFile {
    fn as_ref(&self) -> &[u8] {
        &self.map
    }
}
