//! `gvfs-mmap` — the workspace's only memory map.
//!
//! Slab files (`.pack` + `.mphf`) are written once under a temporary name, fsynced and
//! renamed into place; they are never modified, truncated or reused afterwards (a new
//! slab always gets a new content-addressed name). That immutability is the invariant
//! that makes a shared read-only mapping sound, and it is the caller's obligation:
//! [`Map::open`] must only ever be given such files.

use std::fs::File;
use std::io;

pub use memmap2::Advice;

/// A read-only, shared mapping of a whole immutable file.
#[derive(Debug)]
pub struct Map {
    inner: Option<memmap2::Mmap>,
}

impl Map {
    /// Maps `file` read-only. `populate` pre-faults every page (MAP_POPULATE) so the first
    /// requests do not take page faults (use it for the hot `.mphf` indexes).
    ///
    /// The file must be immutable for as long as the map lives (see the crate docs).
    pub fn open(file: &File, populate: bool) -> io::Result<Map> {
        if file.metadata()?.len() == 0 {
            // mmap(2) rejects zero-length maps; an empty slice needs no mapping.
            return Ok(Map { inner: None });
        }
        let mut opts = memmap2::MmapOptions::new();
        if populate {
            opts.populate();
        }
        #[allow(unsafe_code)]
        // SAFETY: the mapping is read-only and private to this process' view of an
        // immutable file: slab files are created under a temporary name, fsynced, renamed
        // into place and never written, truncated or replaced in place afterwards (the
        // crate-level contract every caller upholds). No `&mut` to the bytes is ever
        // created, so Rust's aliasing rules hold for the lifetime of the map.
        let map = unsafe { opts.map(file)? };
        Ok(Map { inner: Some(map) })
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        match &self.inner {
            Some(m) => m,
            None => &[],
        }
    }

    /// madvise(2) hint for the whole map (e.g. `Advice::Random` for indexes).
    pub fn advise(&self, advice: Advice) -> io::Result<()> {
        match &self.inner {
            Some(m) => m.advise(advice),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn maps_file_and_empty_file() {
        let dir = std::env::temp_dir().join(format!("gvfs-mmap-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("a");
        File::create(&p).unwrap().write_all(b"hello slab").unwrap();
        let m = Map::open(&File::open(&p).unwrap(), true).unwrap();
        assert_eq!(m.bytes(), b"hello slab");
        m.advise(Advice::Random).unwrap();
        let e = dir.join("empty");
        File::create(&e).unwrap();
        assert_eq!(
            Map::open(&File::open(&e).unwrap(), false).unwrap().bytes(),
            b""
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
