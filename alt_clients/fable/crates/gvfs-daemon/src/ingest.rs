//! Core primitive 1: `ingest_git_delta(have, want) -> Slab`.
//!
//! One `git-upload-pack` fetch (`want <tip>`, `have <previous tips>`), streamed to disk or
//! RAM, indexed, deduplicated against the slabs already published, and sealed as a slab
//! (`.pack` + `.idx` + `.mphf`). The same function builds the weekly baseline (`have` empty),
//! every 15-minute pipeline slab, and the sub-15-minute live slab of a `serve` process.

use core::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};

use gvfs_mmap::Map;

use crate::indexer::{self, IndexError, IndexedPack, ResolveConfig};
use crate::manifest::{SlabRef, Tier};
use crate::oid::Oid;
use crate::pack::PACK_TRAILER_LEN;
use crate::slab::{self, SealScratch, SlabError};
use crate::store::{FsStore, SlabPaths, StoreError};
use crate::transport::{Upstream, UpstreamError};

#[derive(Debug)]
pub enum IngestError {
    Upstream(UpstreamError),
    Index(IndexError),
    Slab(SlabError),
    Store(StoreError),
    Io(std::io::Error),
    /// The in-memory destination is too small for the fetched pack.
    TooLargeForMemory(u64),
}

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Upstream(e) => write!(f, "{e}"),
            Self::Index(e) => write!(f, "indexing: {e}"),
            Self::Slab(e) => write!(f, "{e}"),
            Self::Store(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "i/o: {e}"),
            Self::TooLargeForMemory(n) => write!(
                f,
                "fetched pack ({n} bytes) exceeds the in-memory slab capacity"
            ),
        }
    }
}

impl std::error::Error for IngestError {}

impl From<UpstreamError> for IngestError {
    fn from(e: UpstreamError) -> Self {
        Self::Upstream(e)
    }
}
impl From<IndexError> for IngestError {
    fn from(e: IndexError) -> Self {
        Self::Index(e)
    }
}
impl From<SlabError> for IngestError {
    fn from(e: SlabError) -> Self {
        Self::Slab(e)
    }
}
impl From<StoreError> for IngestError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
impl From<std::io::Error> for IngestError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Debug, Clone, Default)]
pub struct IngestConfig {
    pub resolve: ResolveConfig,
}

/// Where the sealed slab goes.
pub enum Destination<'a> {
    /// Files under the store, published as `slab-<seq>`.
    Store { store: &'a FsStore, seq: u64 },
    /// RAM, bounded by `capacity` bytes for the fetched pack (the live L0 slab).
    Memory { capacity: usize },
}

#[derive(Debug)]
pub enum SlabData {
    Published(SlabPaths),
    Memory {
        pack: Vec<u8>,
        idx: Vec<u8>,
        mphf: Vec<u8>,
    },
}

#[derive(Debug)]
pub struct Ingested {
    pub meta: SlabRef,
    pub data: SlabData,
    pub fetched_bytes: u64,
    pub fetched_objects: usize,
    pub duplicates: u64,
    pub converted: u64,
    pub pointer_blobs: u64,
    pub other_blobs: u64,
}

/// A `Write` into a `Vec` that refuses to grow past its capacity.
struct Bounded<'a>(&'a mut Vec<u8>);

impl Write for Bounded<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(buf.len()) > self.0.capacity() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "in-memory slab capacity exceeded",
            ));
        }
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Fetches `wants` minus `haves` from `upstream` and seals it as a `tier` slab covering
/// `(start_ts, end_ts]` whose tip is the first want. `dedup(oid)` reports objects already
/// published; they are dropped. Returns `Ok(None)` when nothing new arrived.
#[allow(clippy::too_many_arguments)]
pub fn ingest_git_delta(
    upstream: &mut dyn Upstream,
    wants: &[Oid],
    haves: &[Oid],
    tier: Tier,
    start_ts: u64,
    end_ts: u64,
    dedup: Option<&(dyn Fn(&Oid) -> bool + Sync)>,
    dest: Destination<'_>,
    cfg: &IngestConfig,
    scratch: &mut SealScratch,
) -> Result<Option<Ingested>, IngestError> {
    let mut scan_scratch = vec![0u8; 64 << 10];
    let want = wants.first().copied().unwrap_or(Oid::ZERO);
    match dest {
        Destination::Store { store, seq } => {
            let recv_path = store.tmp_path(&format!("recv-{seq:08}.pack"));
            let fetched = {
                let file = File::create(&recv_path)?;
                let mut w = BufWriter::with_capacity(1 << 20, file);
                let n = upstream.fetch(wants, haves, &mut w)?;
                w.flush()?;
                n
            };
            let recv = Map::open(&recv_path, false)?;
            let _ = recv.advise_sequential();
            let mut ip = indexer::scan(&recv, &mut scan_scratch)?;
            indexer::resolve(&mut ip, &recv, &cfg.resolve, dedup)?;
            let fetched_objects = ip.entries.len();
            if ip.kept == 0 {
                drop(recv);
                let _ = std::fs::remove_file(&recv_path);
                return Ok(None);
            }
            let tmp = store.tmp_slab_paths(seq, "seal");
            let (pack_sha, pack_len) = if ip.needs_rewrite {
                let mut w = BufWriter::with_capacity(1 << 20, File::create(&tmp.pack)?);
                let sha = indexer::emit(&mut ip, &recv, &mut w)?;
                w.flush()?;
                drop(recv);
                let _ = std::fs::remove_file(&recv_path);
                (sha, std::fs::metadata(&tmp.pack)?.len())
            } else {
                let sha = recv
                    .last_chunk::<PACK_TRAILER_LEN>()
                    .copied()
                    .unwrap_or([0u8; 20]);
                let len = recv.len() as u64;
                drop(recv);
                std::fs::rename(&recv_path, &tmp.pack)?;
                (sha, len)
            };
            slab::compact(&mut ip);
            let (idx_len, mphf_len) = {
                let mut iw = BufWriter::with_capacity(1 << 20, File::create(&tmp.idx)?);
                let mut mw = BufWriter::with_capacity(1 << 20, File::create(&tmp.mphf)?);
                let r = slab::build_indexes(&ip, pack_len, &pack_sha, &mut iw, &mut mw, scratch)?;
                iw.flush()?;
                mw.flush()?;
                r
            };
            let meta = SlabRef {
                seq,
                tier,
                start_ts,
                end_ts,
                tip: want,
                object_count: ip.oids.len() as u32,
                pack_len,
                idx_len,
                mphf_len,
            };
            let paths = store.publish_slab(seq, &tmp)?;
            Ok(Some(summarize(
                &ip,
                meta,
                SlabData::Published(paths),
                fetched,
                fetched_objects,
            )))
        }
        Destination::Memory { capacity } => {
            let mut recv: Vec<u8> = Vec::with_capacity(capacity);
            let fetched = match upstream.fetch(wants, haves, &mut Bounded(&mut recv)) {
                Ok(n) => n,
                Err(UpstreamError::Io(e)) if e.kind() == std::io::ErrorKind::OutOfMemory => {
                    return Err(IngestError::TooLargeForMemory(capacity as u64));
                }
                Err(e) => return Err(e.into()),
            };
            let mut ip = indexer::scan(&recv, &mut scan_scratch)?;
            indexer::resolve(&mut ip, &recv, &cfg.resolve, dedup)?;
            let fetched_objects = ip.entries.len();
            if ip.kept == 0 {
                return Ok(None);
            }
            let (pack, pack_sha) = if ip.needs_rewrite {
                let mut out: Vec<u8> = Vec::with_capacity(recv.len());
                let sha = indexer::emit(&mut ip, &recv, &mut out)?;
                (out, sha)
            } else {
                let sha = recv
                    .last_chunk::<PACK_TRAILER_LEN>()
                    .copied()
                    .unwrap_or([0u8; 20]);
                (recv, sha)
            };
            slab::compact(&mut ip);
            let mut idx = Vec::with_capacity(crate::idx::idx_len(ip.oids.len() as u64, 0) as usize);
            let mut mphf = Vec::with_capacity(crate::mphf::file_len_for_keys(ip.oids.len() as u64));
            let (idx_len, mphf_len) = slab::build_indexes(
                &ip,
                pack.len() as u64,
                &pack_sha,
                &mut idx,
                &mut mphf,
                scratch,
            )?;
            let meta = SlabRef {
                seq: 0,
                tier,
                start_ts,
                end_ts,
                tip: want,
                object_count: ip.oids.len() as u32,
                pack_len: pack.len() as u64,
                idx_len,
                mphf_len,
            };
            Ok(Some(summarize(
                &ip,
                meta,
                SlabData::Memory { pack, idx, mphf },
                fetched,
                fetched_objects,
            )))
        }
    }
}

fn summarize(
    ip: &IndexedPack,
    meta: SlabRef,
    data: SlabData,
    fetched_bytes: u64,
    fetched_objects: usize,
) -> Ingested {
    use std::sync::atomic::Ordering::Relaxed;
    Ingested {
        meta,
        data,
        fetched_bytes,
        fetched_objects,
        duplicates: ip.stats.duplicates.load(Relaxed),
        converted: ip.stats.converted.load(Relaxed),
        pointer_blobs: ip.stats.pointer_blobs.load(Relaxed),
        other_blobs: ip.stats.other_blobs.load(Relaxed),
    }
}
