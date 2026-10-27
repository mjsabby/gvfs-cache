//! Read-only memory maps of immutable slab files.
//!
//! Slab files (`.pack`, `.idx`, `.mphf`) are written once, published by renaming into
//! place, and never modified afterwards; the pipeline deletes a file only after the
//! manifest stopped referencing it. That immutability is exactly the precondition
//! `memmap2` cannot prove and therefore marks `Mmap::map` as `unsafe`. This crate owns
//! that single `unsafe` block so that everything else in the workspace can keep
//! `#![forbid(unsafe_code)]`.

use std::fs::File;
use std::io;
use std::ops::Deref;
use std::path::Path;

use memmap2::{Advice, Mmap, MmapOptions};

/// A read-only map of a whole file. Dereferences to `&[u8]`.
#[derive(Debug)]
pub struct Map {
    /// `None` for an empty file (a zero-length mapping is an error for mmap(2)).
    inner: Option<Mmap>,
}

impl Map {
    /// Maps `file` read-only. `populate` asks the kernel to pre-fault every page
    /// (`MAP_POPULATE`), which is what we want for index files that are probed randomly.
    pub fn from_file(file: &File, populate: bool) -> io::Result<Self> {
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(Self { inner: None });
        }
        let mut opts = MmapOptions::new();
        if populate {
            opts.populate();
        }
        // SAFETY: the mapped files are immutable published slab artifacts (see the module
        // docs): they are created under a temporary name, renamed into place once complete,
        // and unlinked only after no manifest references them, which keeps the mapping
        // valid for its whole lifetime. Nothing in this process opens them for writing.
        #[allow(unsafe_code)]
        let map = unsafe { opts.map(file) }?;
        Ok(Self { inner: Some(map) })
    }

    pub fn open(path: &Path, populate: bool) -> io::Result<Self> {
        Self::from_file(&File::open(path)?, populate)
    }

    /// Tells the kernel the map will be read front to back (aggressive read-ahead).
    pub fn advise_sequential(&self) -> io::Result<()> {
        match &self.inner {
            Some(m) => m.advise(Advice::Sequential),
            None => Ok(()),
        }
    }

    /// Tells the kernel the map will be probed at random (no read-ahead).
    pub fn advise_random(&self) -> io::Result<()> {
        match &self.inner {
            Some(m) => m.advise(Advice::Random),
            None => Ok(()),
        }
    }

    /// Asks the kernel to start reading the whole file into the page cache.
    pub fn advise_will_need(&self) -> io::Result<()> {
        match &self.inner {
            Some(m) => m.advise(Advice::WillNeed),
            None => Ok(()),
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            Some(m) => m,
            None => &[],
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }
}

impl Deref for Map {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for Map {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn maps_a_file_and_an_empty_file() {
        let dir = std::env::temp_dir().join(format!("gvfs-mmap-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("a.bin");
        File::create(&p).unwrap().write_all(b"hello slab").unwrap();
        let m = Map::open(&p, true).unwrap();
        assert_eq!(&m[..], b"hello slab");
        m.advise_sequential().unwrap();
        m.advise_random().unwrap();
        m.advise_will_need().unwrap();
        let e = dir.join("empty.bin");
        File::create(&e).unwrap();
        let m = Map::open(&e, false).unwrap();
        assert!(m.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
