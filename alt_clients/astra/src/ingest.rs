use crate::arena::{Arena, push};
use crate::manifest::{SlabRef, Tier};
use crate::mphf::{NO_BASE, Overflow, Record};
use crate::object::{TreeIter, parse_commit};
use crate::pack::{encode_entry_header, pack_header};
use crate::slab::{Artifact, Slab, finish_artifact};
use crate::source::{Decoder, GitUpstream, Source};
use crate::zlib::{Deflater, compress_bound};
use crate::{Error, ObjType, Oid20, Result};
use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PackMode {
    Delta,
    Full,
    Clustered,
}

#[derive(Clone, Copy)]
struct Visit {
    family: u64,
    source: u32,
    ordinal: u32,
    priority: u64,
}
#[derive(Clone, Copy)]
struct Walk {
    oid: Oid20,
    family: u64,
    head: bool,
}

fn visit(source: u32, ordinal: u32, row: &Record) -> Visit {
    Visit {
        source,
        ordinal,
        family: row.family & crate::tree_delta::FAMILY_MASK,
        priority: if row.family & crate::tree_delta::HEAD_TREE != 0 {
            0
        } else {
            (i64::MAX as u64)
                .saturating_sub(row.timestamp)
                .saturating_add(1)
        },
    }
}

pub struct IngestScratch {
    pub source: Source,
    decoder: Decoder,
    compressor: Deflater,
    compressed: Arena,
    visited: Vec<u64>,
    generation: u32,
    order: Vec<Visit>,
    stack: Vec<Walk>,
    records: Vec<Record>,
    sealed: Vec<Record>,
    overflow: Vec<Overflow>,
    cluster_base: Arena,
    instructions: Arena,
    pub pointers: u64,
    pub ordinary_blobs: u64,
}

impl IngestScratch {
    pub fn new(max_objects: usize, max_object: usize, cache_bytes: usize) -> Result<Self> {
        Ok(Self {
            source: Source::new(max_objects)?,
            decoder: Decoder::new(max_object, cache_bytes)?,
            compressor: Deflater::new(1),
            compressed: Arena::new(compress_bound(max_object))?,
            visited: vec![0; max_objects],
            generation: 0,
            order: Vec::with_capacity(max_objects),
            stack: Vec::with_capacity(max_objects),
            records: Vec::with_capacity(max_objects),
            sealed: Vec::with_capacity(max_objects),
            overflow: Vec::with_capacity(max_objects),
            cluster_base: Arena::new(max_object)?,
            instructions: Arena::new(
                max_object
                    .saturating_add(max_object.wrapping_div(64))
                    .saturating_add(128),
            )?,
            pointers: 0,
            ordinary_blobs: 0,
        })
    }
    fn reset(&mut self) {
        if self.generation == u32::MAX {
            self.visited.fill(0);
            self.generation = 0;
        }
        self.generation = self.generation.saturating_add(1);
        self.order.clear();
        self.stack.clear();
        self.records.clear();
        self.sealed.clear();
        self.overflow.clear();
        self.pointers = 0;
        self.ordinary_blobs = 0;
    }
    fn ordinal(&self, source: u32) -> Result<Option<u32>> {
        let value = *self
            .visited
            .get(source as usize)
            .ok_or(Error::Capacity("visited objects"))?;
        Ok(if (value >> 32) as u32 == self.generation {
            Some(value as u32)
        } else {
            None
        })
    }
}

/// The only upstream ingestion path: genesis, scheduled deltas and live catch-up.
#[allow(clippy::too_many_arguments)]
pub fn ingest_git_delta(
    upstream: &GitUpstream,
    have: Oid20,
    want: Oid20,
    known: &[Arc<Slab>],
    tier: Tier,
    start: u64,
    end: u64,
    mode: PackMode,
    scratch: &mut IngestScratch,
    work: &Path,
) -> Result<Artifact> {
    upstream.prepare(&mut scratch.source, want, have)?;
    scratch.reset();
    {
        let _hot = alloc_guard::hot_path();
        push(
            &mut scratch.stack,
            Walk {
                oid: want,
                family: 1,
                head: true,
            },
        )?;
        while let Some(item) = scratch.stack.pop() {
            let oid = item.oid;
            if crate::slab::find(known, &oid)?.is_some() {
                continue;
            }
            let source = scratch.source.find(&oid)?.ok_or(Error::Missing(oid))?;
            if scratch.ordinal(source)?.is_some() {
                continue;
            }
            let ordinal = u32::try_from(scratch.records.len())
                .map_err(|_| Error::Capacity("slab objects"))?;
            *scratch
                .visited
                .get_mut(source as usize)
                .ok_or(Error::Capacity("visited objects"))? =
                (u64::from(scratch.generation) << 32) | u64::from(ordinal);
            let (kind, bytes) = scratch.decoder.decode(&scratch.source, source)?;
            if crate::object_id(kind, bytes) != oid {
                return Err(Error::Invalid("upstream object OID mismatch"));
            }
            let mut row = Record::new(oid, kind, bytes.len(), end, NO_BASE)?;
            if kind == ObjType::Tree {
                row.family = item.family.max(1)
                    | if item.head {
                        crate::tree_delta::HEAD_TREE
                    } else {
                        0
                    };
            }
            match kind {
                ObjType::Commit => {
                    let begin = scratch.stack.len();
                    let info = parse_commit(bytes, |parent| {
                        push(
                            &mut scratch.stack,
                            Walk {
                                oid: parent,
                                family: 1,
                                head: false,
                            },
                        )
                        .is_ok()
                    })
                    .ok_or(Error::Invalid("commit or DFS capacity"))?;
                    scratch
                        .stack
                        .get_mut(begin..)
                        .ok_or(Error::Invalid("DFS stack"))?
                        .reverse();
                    push(
                        &mut scratch.stack,
                        Walk {
                            oid: info.tree,
                            family: 1,
                            head: item.head,
                        },
                    )?;
                }
                ObjType::Tree => {
                    let begin = scratch.stack.len();
                    for tree_item in TreeIter::new(bytes) {
                        let entry = tree_item.map_err(|_| Error::Invalid("tree"))?;
                        if entry.is_tree() || entry.is_blob() {
                            let family = if entry.is_tree() {
                                crate::tree_delta::family(item.family, entry.name)
                            } else {
                                0
                            };
                            push(
                                &mut scratch.stack,
                                Walk {
                                    oid: entry.oid,
                                    family,
                                    head: item.head,
                                },
                            )?;
                        } else if entry.mode != crate::object::MODE_GITLINK {
                            return Err(Error::Invalid("tree mode"));
                        }
                    }
                    scratch
                        .stack
                        .get_mut(begin..)
                        .ok_or(Error::Invalid("DFS stack"))?
                        .reverse();
                }
                ObjType::Blob => {
                    if bytes.starts_with(b"~~Git-hfs ")
                        && crate::hfs::parse_git_hfs_v1(bytes).is_some()
                    {
                        scratch.pointers = scratch.pointers.saturating_add(1);
                    } else {
                        // A blob that merely looks like a pointer (another pointer variant, docs
                        // about the format) is still a Git blob: count it, never abort ingest.
                        // Historical ordinary blobs must not be dropped: that breaks Git reachability.
                        scratch.ordinary_blobs = scratch.ordinary_blobs.saturating_add(1);
                    }
                }
                ObjType::Tag => {
                    let line = bytes
                        .split(|&b| b == b'\n')
                        .next()
                        .ok_or(Error::Invalid("tag"))?;
                    let target = Oid20::from_hex(
                        line.strip_prefix(b"object ").ok_or(Error::Invalid("tag"))?,
                    )
                    .ok_or(Error::Invalid("tag OID"))?;
                    push(
                        &mut scratch.stack,
                        Walk {
                            oid: target,
                            family: 1,
                            head: item.head,
                        },
                    )?;
                }
                _ => return Err(Error::Invalid("resolved delta kind")),
            }
            push(&mut scratch.records, row)?;
            push(&mut scratch.order, visit(source, ordinal, &row))?;
        }
    }
    write_output(scratch, tier, start, end, want, mode, work)
}

fn long_term(tier: Tier) -> bool {
    matches!(tier, Tier::H24 | Tier::WeeklyDelta | Tier::Baseline)
}

fn write_output(
    scratch: &mut IngestScratch,
    tier: Tier,
    start: u64,
    end: u64,
    want: Oid20,
    mode: PackMode,
    work: &Path,
) -> Result<Artifact> {
    let dir = tempfile::tempdir_in(work)?;
    let file = File::create(dir.path().join("slab.pack"))?;
    let mut output = BufWriter::with_capacity(1 << 20, file);
    let clustered = mode == PackMode::Clustered && long_term(tier);
    if clustered {
        scratch
            .order
            .sort_unstable_by_key(|v| (v.family, v.priority, v.ordinal));
    }
    for (i, visit) in scratch.order.iter().enumerate() {
        *scratch
            .visited
            .get_mut(visit.source as usize)
            .ok_or(Error::Capacity("visited objects"))? =
            (u64::from(scratch.generation) << 32) | i as u64;
    }
    let count = scratch.records.len() as u32;
    let mut hash = Sha1::new();
    let header = pack_header(count);
    output.write_all(&header)?;
    hash.update(header);
    let mut offset = 12u64;
    let mut cluster_family = 0u64;
    let mut cluster_ordinal = NO_BASE;
    let mut cluster_oid = Oid20([0; 20]);
    let mut cluster_start = 0u64;
    let mut cluster_members = 0usize;
    {
        let _hot = alloc_guard::hot_path();
        for (i, &visit) in scratch.order.iter().enumerate() {
            let source = visit.source;
            let mut row = *scratch
                .records
                .get(visit.ordinal as usize)
                .ok_or(Error::Invalid("discovery row"))?;
            let source_header = scratch.source.header(source)?;
            let base = scratch.source.base(source, &source_header)?;
            let kept_base = match base {
                Some(b) => scratch.ordinal(b)?,
                None => None,
            };
            let original = scratch.source.raw(source)?;
            if crc32fast::hash(original) != scratch.source.entry(source)?.crc {
                return Err(Error::Invalid("upstream entry CRC"));
            }
            let mut prefix = [0u8; 10];
            let mut crc = crc32fast::Hasher::new();
            let mut length = 0usize;
            let mut emit = |bytes: &[u8]| -> Result<()> {
                output.write_all(bytes)?;
                hash.update(bytes);
                crc.update(bytes);
                length = length
                    .checked_add(bytes.len())
                    .ok_or(Error::Capacity("compressed entry"))?;
                Ok(())
            };
            let target_base;
            if clustered && row.kind()? == ObjType::Tree {
                let (_, bytes) = scratch.decoder.decode(&scratch.source, source)?;
                let mut delta_len = None;
                if row.family & crate::tree_delta::HEAD_TREE == 0
                    && cluster_family == visit.family
                    && cluster_members < crate::tree_delta::MAX_CLUSTER_MEMBERS
                {
                    let n = crate::tree_delta::encode(
                        scratch.cluster_base.bytes(),
                        bytes,
                        scratch.instructions.buffer(),
                    )?;
                    scratch.instructions.set_len(n)?;
                    let compressed = scratch
                        .compressor
                        .deflate(scratch.instructions.bytes(), scratch.compressed.buffer())?;
                    let prefix_len = encode_entry_header(ObjType::RefDelta, n as u64, &mut prefix);
                    let entry_len = prefix_len.saturating_add(20).saturating_add(compressed);
                    let full_bound = if source_header.kind.is_delta() {
                        bytes.len()
                    } else {
                        original.len()
                    };
                    if entry_len < full_bound
                        && offset
                            .saturating_add(entry_len as u64)
                            .saturating_sub(cluster_start)
                            <= crate::tree_delta::MAX_CLUSTER_BYTES
                    {
                        delta_len = Some((prefix_len, compressed));
                    }
                }
                if let Some((n, compressed)) = delta_len {
                    target_base = cluster_ordinal;
                    emit(prefix.get(..n).ok_or(Error::Invalid("tree delta header"))?)?;
                    emit(&cluster_oid.0)?;
                    emit(
                        scratch
                            .compressed
                            .buffer()
                            .get(..compressed)
                            .ok_or(Error::Invalid("tree delta"))?,
                    )?;
                    cluster_members = cluster_members.saturating_add(1);
                } else {
                    target_base = NO_BASE;
                    if source_header.kind.is_delta() {
                        let n = encode_entry_header(ObjType::Tree, bytes.len() as u64, &mut prefix);
                        emit(prefix.get(..n).ok_or(Error::Invalid("tree header"))?)?;
                        let n = scratch
                            .compressor
                            .deflate(bytes, scratch.compressed.buffer())?;
                        emit(
                            scratch
                                .compressed
                                .buffer()
                                .get(..n)
                                .ok_or(Error::Invalid("tree compression"))?,
                        )?;
                    } else {
                        emit(original)?;
                    }
                    scratch
                        .cluster_base
                        .buffer()
                        .get_mut(..bytes.len())
                        .ok_or(Error::Capacity("tree base"))?
                        .copy_from_slice(bytes);
                    scratch.cluster_base.set_len(bytes.len())?;
                    cluster_family = visit.family;
                    cluster_ordinal = i as u32;
                    cluster_oid = row.oid;
                    cluster_start = offset;
                    cluster_members = 1;
                }
            } else if !source_header.kind.is_delta() {
                target_base = NO_BASE;
                emit(original)?;
            } else if mode == PackMode::Delta
                && long_term(tier)
                && row.kind()? == ObjType::Tree
                && kept_base.is_some()
            {
                target_base = kept_base.ok_or(Error::Invalid("retained base"))?;
                let n = encode_entry_header(ObjType::RefDelta, source_header.size, &mut prefix);
                emit(prefix.get(..n).ok_or(Error::Invalid("entry header"))?)?;
                emit(
                    &scratch
                        .source
                        .entry(base.ok_or(Error::Invalid("base source"))?)?
                        .oid
                        .0,
                )?;
                emit(
                    original
                        .get(source_header.header_len..)
                        .ok_or(Error::Invalid("entry zlib"))?,
                )?;
            } else {
                target_base = NO_BASE;
                let (kind, bytes) = scratch.decoder.decode(&scratch.source, source)?;
                let n = encode_entry_header(kind, bytes.len() as u64, &mut prefix);
                emit(prefix.get(..n).ok_or(Error::Invalid("entry header"))?)?;
                let n = scratch
                    .compressor
                    .deflate(bytes, scratch.compressed.buffer())?;
                emit(
                    scratch
                        .compressed
                        .buffer()
                        .get(..n)
                        .ok_or(Error::Invalid("compressed entry"))?,
                )?;
            }
            row.base = target_base;
            row.crc = crc.finalize();
            row.locate(
                offset,
                u32::try_from(length).map_err(|_| Error::Capacity("entry length"))?,
                &mut scratch.overflow,
            )?;
            offset = offset
                .checked_add(length as u64)
                .ok_or(Error::Capacity("pack length"))?;
            push(&mut scratch.sealed, row)?;
        }
    }
    let hash = Oid20(hash.finalize().into());
    output.write_all(&hash.0)?;
    output.flush()?;
    output.get_ref().sync_all()?;
    let meta = SlabRef {
        hash,
        index_hash: Oid20([0; 20]),
        tip: want,
        tier,
        start,
        end,
        count,
        pack_len: offset.saturating_add(20),
        idx_len: 0,
    };
    finish_artifact(dir, meta, &scratch.sealed, &scratch.overflow)
}

/// Daily/weekly repacking reads only immutable storage artifacts, never the upstream.
pub fn cluster_artifact(input: &Artifact, config: &crate::pipeline::Config) -> Result<Artifact> {
    let index = crate::mphf::Index::open(&input.dir.path().join("slab.mphf"))?;
    let mut scratch = IngestScratch::new(
        index.len().max(1),
        config.max_object_bytes,
        config.delta_cache_bytes,
    )?;
    scratch
        .source
        .add_pack(&input.dir.path().join("slab.pack"))?;
    scratch.reset();
    for ordinal in 0..index.len() {
        let mut row = index.record(ordinal as u32)?;
        row.family &= crate::tree_delta::FAMILY_MASK;
        if row.kind()? == ObjType::Tree && row.family == 0 {
            row.family = (crate::arena::mix(crate::arena::hash_oid(&row.oid))
                & crate::tree_delta::FAMILY_MASK)
                .max(1);
        }
        let source = scratch
            .source
            .find(&row.oid)?
            .ok_or(Error::Missing(row.oid))?;
        *scratch
            .visited
            .get_mut(source as usize)
            .ok_or(Error::Invalid("source row"))? =
            (u64::from(scratch.generation) << 32) | ordinal as u64;
        push(&mut scratch.records, row)?;
    }
    push(
        &mut scratch.stack,
        Walk {
            oid: input.meta.tip,
            family: 1,
            head: true,
        },
    )?;
    while let Some(item) = scratch.stack.pop() {
        let Some(source) = scratch.source.find(&item.oid)? else {
            if input.meta.tier == Tier::Baseline {
                return Err(Error::Missing(item.oid));
            }
            continue; // A daily/weekly delta intentionally omits already-published trees.
        };
        let ordinal = scratch
            .ordinal(source)?
            .ok_or(Error::Invalid("source ordinal"))?;
        let row = scratch
            .records
            .get_mut(ordinal as usize)
            .ok_or(Error::Invalid("repack row"))?;
        if row.kind()? == ObjType::Tree && row.family & crate::tree_delta::HEAD_TREE != 0 {
            continue;
        }
        let (kind, bytes) = scratch.decoder.decode(&scratch.source, source)?;
        if kind == ObjType::Commit {
            let info = parse_commit(bytes, |_| true).ok_or(Error::Invalid("HEAD commit"))?;
            push(
                &mut scratch.stack,
                Walk {
                    oid: info.tree,
                    family: 1,
                    head: true,
                },
            )?;
        } else if kind == ObjType::Tree {
            row.family |= crate::tree_delta::HEAD_TREE;
            for entry in TreeIter::new(bytes) {
                let entry = entry.map_err(|_| Error::Invalid("HEAD tree"))?;
                if entry.is_tree() {
                    push(
                        &mut scratch.stack,
                        Walk {
                            oid: entry.oid,
                            family: 1,
                            head: true,
                        },
                    )?;
                }
            }
        }
    }
    for (ordinal, row) in scratch.records.iter().enumerate() {
        let source = scratch
            .source
            .find(&row.oid)?
            .ok_or(Error::Missing(row.oid))?;
        push(&mut scratch.order, visit(source, ordinal as u32, row))?;
    }
    write_output(
        &mut scratch,
        input.meta.tier,
        input.meta.start,
        input.meta.end,
        input.meta.tip,
        PackMode::Clustered,
        &config.work,
    )
}
