//! Slabs: an immutable `.pack` + `.idx` + `.mphf` triple, memory-mapped from the store or
//! held in RAM (the live L0 slab), and the sealing step that builds the two indexes.

use core::fmt;
use std::io::Write;

use bytes::Bytes;

use crate::idx::{IdxEntry, IdxError, IdxView, sort_by_oid, write_idx_v2};
use crate::indexer::{F_DUP, IndexedPack};
use crate::manifest::SlabRef;
use crate::mphf::{Builder, Loc, MphfError, MphfView, file_len_for_keys};
use crate::oid::Oid;
use crate::pack::{PACK_HEADER_LEN, PACK_TRAILER_LEN};
use crate::store::{FsStore, SlabPaths};

#[derive(Debug)]
pub enum SlabError {
    Io(std::io::Error),
    Idx(IdxError),
    Mphf(MphfError),
    /// A file's length does not match the manifest.
    LengthMismatch(&'static str),
    Corrupt(&'static str),
}

impl fmt::Display for SlabError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "slab i/o: {e}"),
            Self::Idx(e) => write!(f, "slab idx: {e}"),
            Self::Mphf(e) => write!(f, "slab mphf: {e}"),
            Self::LengthMismatch(w) => write!(f, "slab {w} length does not match the manifest"),
            Self::Corrupt(w) => write!(f, "corrupt slab: {w}"),
        }
    }
}

impl std::error::Error for SlabError {}

impl From<std::io::Error> for SlabError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<IdxError> for SlabError {
    fn from(e: IdxError) -> Self {
        Self::Idx(e)
    }
}
impl From<MphfError> for SlabError {
    fn from(e: MphfError) -> Self {
        Self::Mphf(e)
    }
}

/// Bytes of one slab file: a read-only map or an owned buffer.
#[derive(Debug)]
pub enum Blob {
    Map(gvfs_mmap::Map),
    Mem(Vec<u8>),
}

impl AsRef<[u8]> for Blob {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Map(m) => m.as_slice(),
            Self::Mem(v) => v,
        }
    }
}

fn blob_bytes(b: Blob) -> Bytes {
    if b.as_ref().is_empty() {
        return Bytes::new();
    }
    // One `Bytes` per file; every response frame is a refcount bump on it, never a copy.
    Bytes::from_owner(b)
}

/// A loaded slab. Response frames are `Bytes::slice`s of `pack`/`idx` (zero-copy).
#[derive(Debug)]
pub struct Slab {
    pub meta: SlabRef,
    pub pack: Bytes,
    pub idx: Bytes,
    /// Empty when the slab is never probed (weekly deltas only feed prefetch).
    pub mphf: Bytes,
}

impl Slab {
    /// Maps a published slab. `probe` loads (and pre-faults) the mphf as well.
    pub fn open(store: &FsStore, meta: SlabRef, probe: bool) -> Result<Self, SlabError> {
        let paths = store.slab_paths(meta.seq);
        Self::open_paths(&paths, meta, probe)
    }

    pub fn open_paths(paths: &SlabPaths, meta: SlabRef, probe: bool) -> Result<Self, SlabError> {
        let pack = gvfs_mmap::Map::open(&paths.pack, false)?;
        if pack.len() as u64 != meta.pack_len {
            return Err(SlabError::LengthMismatch("pack"));
        }
        let _ = pack.advise_will_need();
        let idx = gvfs_mmap::Map::open(&paths.idx, false)?;
        if idx.len() as u64 != meta.idx_len {
            return Err(SlabError::LengthMismatch("idx"));
        }
        let _ = idx.advise_will_need();
        let mphf = if probe {
            let m = gvfs_mmap::Map::open(&paths.mphf, true)?;
            if m.len() as u64 != meta.mphf_len {
                return Err(SlabError::LengthMismatch("mphf"));
            }
            let _ = m.advise_random();
            MphfView::parse(&m)?;
            blob_bytes(Blob::Map(m))
        } else {
            Bytes::new()
        };
        let slab = Self {
            meta,
            pack: blob_bytes(Blob::Map(pack)),
            idx: blob_bytes(Blob::Map(idx)),
            mphf,
        };
        slab.validate()?;
        Ok(slab)
    }

    pub fn from_memory(
        meta: SlabRef,
        pack: Vec<u8>,
        idx: Vec<u8>,
        mphf: Vec<u8>,
    ) -> Result<Self, SlabError> {
        let slab = Self {
            meta,
            pack: blob_bytes(Blob::Mem(pack)),
            idx: blob_bytes(Blob::Mem(idx)),
            mphf: blob_bytes(Blob::Mem(mphf)),
        };
        slab.validate()?;
        Ok(slab)
    }

    fn validate(&self) -> Result<(), SlabError> {
        if self.pack.len() < PACK_HEADER_LEN.saturating_add(PACK_TRAILER_LEN) {
            return Err(SlabError::Corrupt("pack too short"));
        }
        let idx = IdxView::parse(&self.idx)?;
        if idx.n as u64 != u64::from(self.meta.object_count) {
            return Err(SlabError::Corrupt("idx object count"));
        }
        if idx.pack_sha != self.trailer() {
            return Err(SlabError::Corrupt("idx does not belong to pack"));
        }
        Ok(())
    }

    #[inline]
    pub fn mphf(&self) -> Option<MphfView<'_>> {
        if self.mphf.is_empty() {
            None
        } else {
            MphfView::parse(&self.mphf).ok()
        }
    }

    #[inline]
    pub fn idx(&self) -> Option<IdxView<'_>> {
        IdxView::parse(&self.idx).ok()
    }

    /// O(1) membership + location.
    #[inline]
    pub fn lookup(&self, oid: &Oid) -> Option<(usize, Loc)> {
        self.mphf()?.lookup(oid)
    }

    /// The pack minus its 12-byte header and 20-byte trailer: what a splice concatenates.
    #[inline]
    pub fn body(&self) -> Bytes {
        let end = self.pack.len().saturating_sub(PACK_TRAILER_LEN);
        self.pack.slice(PACK_HEADER_LEN.min(end)..end)
    }

    #[inline]
    pub fn body_len(&self) -> u64 {
        (self.pack.len() as u64).saturating_sub((PACK_HEADER_LEN + PACK_TRAILER_LEN) as u64)
    }

    #[inline]
    pub fn trailer(&self) -> [u8; 20] {
        self.pack.last_chunk::<20>().copied().unwrap_or([0u8; 20])
    }

    #[inline]
    pub fn entry(&self, loc: Loc) -> &[u8] {
        let start = loc.offset as usize;
        self.pack
            .get(start..start.saturating_add(loc.len as usize))
            .unwrap_or(&[])
    }
}

/// The published slabs an ingest deduplicates against / a lookup probes, newest first.
#[derive(Debug, Default)]
pub struct SlabSet {
    pub slabs: Vec<std::sync::Arc<Slab>>,
}

impl SlabSet {
    /// Opens `refs` (given oldest first) with their mphf indexes; probes newest first.
    pub fn open<'a>(
        store: &FsStore,
        refs: impl Iterator<Item = &'a SlabRef>,
    ) -> Result<Self, SlabError> {
        let mut slabs: Vec<std::sync::Arc<Slab>> = refs
            .map(|r| Slab::open(store, *r, true).map(std::sync::Arc::new))
            .collect::<Result<_, _>>()?;
        slabs.reverse();
        Ok(Self { slabs })
    }

    #[inline]
    pub fn find(&self, oid: &Oid) -> Option<(&Slab, Loc)> {
        for s in &self.slabs {
            if let Some((_, loc)) = s.lookup(oid) {
                return Some((s, loc));
            }
        }
        None
    }

    #[inline]
    pub fn contains(&self, oid: &Oid) -> bool {
        self.find(oid).is_some()
    }
}

/// Reusable memory for sealing (sizes follow the largest slab sealed so far).
#[derive(Default)]
pub struct SealScratch {
    sort_scratch: Vec<(Oid, u32)>,
    order: Vec<u32>,
    stage: Vec<u8>,
    builder: Builder,
    mphf_out: Vec<u8>,
}

impl SealScratch {
    pub fn new() -> Self {
        Self {
            stage: vec![0u8; 1 << 20],
            ..Self::default()
        }
    }
}

/// Drops duplicate entries from `ip` so that `entries`/`oids` describe the slab pack.
pub fn compact(ip: &mut IndexedPack) {
    let mut keep = ip.entries.iter().map(|e| e.flags & F_DUP == 0);
    ip.oids.retain(|_| keep.next().unwrap_or(false));
    ip.entries.retain(|e| e.flags & F_DUP == 0);
}

/// Writes the `.idx` and `.mphf` of a compacted pack; returns their lengths.
pub fn build_indexes<W1: Write, W2: Write>(
    ip: &IndexedPack,
    pack_len: u64,
    pack_sha: &[u8; 20],
    idx_w: &mut W1,
    mphf_w: &mut W2,
    scratch: &mut SealScratch,
) -> Result<(u64, u64), SlabError> {
    sort_by_oid(&ip.oids, &mut scratch.sort_scratch, &mut scratch.order);
    let entry = |i: usize| -> Option<IdxEntry> {
        let e = ip.entries.get(i)?;
        Some(IdxEntry {
            oid: *ip.oids.get(i)?,
            crc: e.crc,
            offset: e.offset,
        })
    };
    let mut counting = CountingWriter { w: idx_w, n: 0 };
    write_idx_v2(
        &mut counting,
        &mut scratch.stage,
        &scratch.order,
        &entry,
        pack_sha,
    )?;
    let idx_len = counting.n;
    let need = file_len_for_keys(ip.oids.len() as u64);
    if scratch.mphf_out.len() < need {
        scratch.mphf_out.resize(need, 0);
    }
    let loc_of = |i: usize| -> Loc {
        let e = ip.entries.get(i).copied().unwrap_or_default();
        Loc {
            offset: e.offset,
            len: e.len,
        }
    };
    let n = scratch
        .builder
        .build(&ip.oids, &loc_of, pack_len, &mut scratch.mphf_out)?;
    mphf_w.write_all(scratch.mphf_out.get(..n).unwrap_or(&[]))?;
    Ok((idx_len, n as u64))
}

struct CountingWriter<'a, W: Write> {
    w: &'a mut W,
    n: u64,
}

impl<W: Write> Write for CountingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.w.write(buf)?;
        self.n = self.n.saturating_add(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}
