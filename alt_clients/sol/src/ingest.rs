//! One decode pass, reusable inflate/delta/deflate scratch, then a Merkle walk over
//! ordinal handles. Original full entries are copied verbatim; only deltas deflate.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::arena::{push, reserve, zeroed};
use crate::codec::{
    Base, EntryHeader, Kind, Oid, TreeEntries, Zlib, apply_delta, entry_header, object_id,
    pack_count, take, verify_checksum, write_entry_header,
};
use crate::hfs::parse_git_hfs_v1;
use crate::manifest::Tier;
use crate::mphf::OrdinalMap;
use crate::slab::{Builder, Limits, NO_BASE, PackArena, Record, Slab};
use crate::splice::locate;
use crate::storage::Storage;
use crate::tree::{TreeLayout, TreePacker};
use crate::upstream::Upstream;
use crate::{Error, Result};

#[derive(Clone, Copy)]
struct Incoming {
    start: u64,
    header: EntryHeader,
    compressed_len: u32,
    record: Record,
    resolved: bool,
    spool: bool,
    depth: u16,
    base_ordinal: u32,
    cache_position: u64,
    visit: u32,
}

#[derive(Clone, Copy)]
struct Walk {
    oid: Oid,
    path: u64,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Diagnostics {
    pub unparsed_hfs: u64,
    pub first: Option<(Oid, &'static str)>,
}

impl Diagnostics {
    pub fn report(self) {
        if let Some((oid, reason)) = self.first {
            eprintln!(
                "warning: preserved {} opaque Git blob(s) with unparsed HFS metadata; first={oid}: {reason}",
                self.unparsed_hfs
            );
        }
    }
}

pub struct IngestArena {
    incoming: Vec<Incoming>,
    oid_map: OrdinalMap,
    spool: PackArena,
    payload: Vec<u8>,
    base: Vec<u8>,
    result: Vec<u8>,
    compressed: Vec<u8>,
    base_delta: Vec<u8>,
    cache: Vec<u8>,
    cache_head: u64,
    zlib: Zlib,
    stack: Vec<Walk>,
    seen: Vec<u8>,
    order: Vec<u32>,
    trees: TreePacker,
    strict_hfs: bool,
    diagnostics: Diagnostics,
}

impl IngestArena {
    pub fn new(limits: Limits) -> Result<Self> {
        Self::with_layout(limits, limits.tree_layout, 4 << 20)
    }

    pub fn with_layout(limits: Limits, layout: TreeLayout, cluster_bytes: usize) -> Result<Self> {
        Self::with_spool(
            limits,
            layout,
            cluster_bytes,
            PackArena::Heap(reserve(limits.pack_bytes)?),
        )
    }

    pub fn mapped(
        limits: Limits,
        layout: TreeLayout,
        cluster_bytes: usize,
        directory: &Path,
    ) -> Result<Self> {
        Self::with_spool(
            limits,
            layout,
            cluster_bytes,
            PackArena::File(gvfs_platform::FileArena::new(directory, limits.pack_bytes)?),
        )
    }

    fn with_spool(
        limits: Limits,
        layout: TreeLayout,
        cluster_bytes: usize,
        spool: PackArena,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let compressed = limits
            .object_bytes
            .checked_add(limits.object_bytes / 100)
            .and_then(|len| len.checked_add(1024))
            .ok_or(Error::Capacity("compression scratch"))?;
        let mut arena = Self {
            incoming: reserve(limits.objects)?,
            oid_map: OrdinalMap::new(limits.objects)?,
            spool,
            payload: zeroed(limits.object_bytes)?,
            base: zeroed(limits.object_bytes)?,
            result: zeroed(limits.object_bytes)?,
            compressed: zeroed(compressed)?,
            zlib: Zlib::new(1),
            base_delta: zeroed(limits.object_bytes)?,
            cache: zeroed(limits.cache_bytes)?,
            cache_head: 0,
            stack: reserve(limits.objects)?,
            seen: reserve(limits.objects)?,
            order: reserve(limits.objects)?,
            trees: TreePacker::new(limits, layout, cluster_bytes)?,
            strict_hfs: limits.strict_hfs,
            diagnostics: Diagnostics::default(),
        };
        let len = arena.zlib.deflate(b"warmup", &mut arena.compressed)?;
        arena
            .zlib
            .inflate(take(&arena.compressed, 0, len)?, &mut arena.payload, 6)?;
        Ok(arena)
    }

    pub fn decode(
        &mut self,
        input: &[u8],
        want: Oid,
        known: &[Arc<Slab>],
        builder: &mut Builder,
    ) -> Result<()> {
        let body = verify_checksum(input)?;
        builder.reset()?;
        let count = pack_count(body)? as usize;
        if count > self.incoming.capacity() {
            return Err(Error::Capacity(
                "incoming object count; increase --max-objects",
            ));
        }
        self.incoming.clear();
        self.spool.clear();
        self.stack.clear();
        self.seen.clear();
        self.order.clear();
        self.oid_map.reset();
        self.cache_head = 0;
        self.trees.reset();
        self.diagnostics = Diagnostics::default();
        let mut cursor = 12usize;
        for ordinal in 0..count {
            let header = entry_header(
                body.get(cursor..).ok_or(Error::Invalid("pack cursor"))?,
                cursor as u64,
            )?;
            let compressed_at = cursor
                .checked_add(header.len)
                .ok_or(Error::Invalid("pack entry overflow"))?;
            let consumed = self.zlib.inflate(
                body.get(compressed_at..)
                    .ok_or(Error::Invalid("pack zlib cursor"))?,
                &mut self.payload,
                header.size,
            )?;
            let len = header
                .len
                .checked_add(consumed)
                .ok_or(Error::Invalid("entry length"))?;
            let placeholder = Record {
                oid: Oid::ZERO,
                offset: cursor as u64,
                len: len as u32,
                size: header.size as u64,
                crc: 0,
                kind: header.kind,
                graph_offset: crate::graph::NO_GRAPH,
                pointer: None,
                base: NO_BASE,
                delta_size: 0,
                path: 0,
                epoch: 0,
            };
            push(
                &mut self.incoming,
                Incoming {
                    start: cursor as u64,
                    header,
                    compressed_len: consumed as u32,
                    record: placeholder,
                    resolved: false,
                    spool: false,
                    depth: 0,
                    base_ordinal: NO_BASE,
                    cache_position: u64::MAX,
                    visit: u32::MAX,
                },
            )?;
            if header.kind.full() {
                self.store_full(
                    ordinal,
                    header.kind,
                    header.size,
                    cursor as u64,
                    len,
                    false,
                    0,
                    builder,
                )?;
            } else {
                self.resolve(ordinal, body, true, builder)?;
            }
            cursor = cursor
                .checked_add(len)
                .ok_or(Error::Invalid("pack entry cursor"))?;
        }
        if cursor != body.len() {
            return Err(Error::Invalid("pack object count or trailing bytes"));
        }
        // Forward REF_DELTA chains are uncommon, but must not depend on input order.
        for _ in 0..129 {
            let mut remaining = 0usize;
            let mut progress = false;
            for ordinal in 0..self.incoming.len() {
                if self
                    .incoming
                    .get(ordinal)
                    .is_some_and(|entry| !entry.resolved)
                {
                    if self.resolve(ordinal, body, false, builder)? {
                        progress = true;
                    } else {
                        remaining = remaining.saturating_add(1);
                    }
                }
            }
            if remaining == 0 {
                break;
            }
            if !progress {
                return Err(Error::Invalid(
                    "unresolved delta bases (thin, cyclic, or missing)",
                ));
            }
        }
        if self.incoming.iter().any(|entry| !entry.resolved) {
            return Err(Error::Invalid("delta depth exceeds 128"));
        }
        self.seen.resize(count, 0);
        push(&mut self.stack, Walk { oid: want, path: 0 })?;
        while let Some(walk) = self.stack.pop() {
            let oid = walk.oid;
            let ordinal = self.oid_map.find(oid, |ordinal| {
                Ok(self
                    .incoming
                    .get(ordinal as usize)
                    .ok_or(Error::Invalid("ingest OID ordinal"))?
                    .record
                    .oid)
            })?;
            let Some(ordinal) = ordinal else {
                if locate(known, oid)?.is_some() {
                    continue;
                }
                return Err(Error::Missing(oid));
            };
            let seen = self
                .seen
                .get_mut(ordinal as usize)
                .ok_or(Error::Invalid("Merkle visited ordinal"))?;
            if *seen != 0 {
                continue;
            }
            *seen = 1;
            // Incremental packs are net-new even when the server repeats a delta base.
            if locate(known, oid)?.is_some() {
                continue;
            }
            let entry = self
                .incoming
                .get_mut(ordinal as usize)
                .ok_or(Error::Invalid("Merkle input ordinal"))?;
            entry.record.path = walk.path;
            entry.visit = self.order.len() as u32;
            let entry = *entry;
            push(&mut self.order, ordinal)?;
            if entry.record.kind == Kind::Tree {
                self.materialize(ordinal, body)?;
                let mut children =
                    TreeEntries::new(take(&self.base, 0, entry.record.size as usize)?);
                let start = self.stack.len();
                while let Some(child) = children.next_entry()? {
                    if child.gitlink {
                        continue;
                    }
                    let path = if child.directory {
                        directory_path(walk.path, child.name)
                    } else {
                        0
                    };
                    push(
                        &mut self.stack,
                        Walk {
                            oid: child.oid,
                            path,
                        },
                    )?;
                }
                self.stack
                    .get_mut(start..)
                    .ok_or(Error::Invalid("Merkle child stack"))?
                    .reverse();
            } else {
                let children = builder.raw_graph.get(entry.record.graph_offset)?;
                for child in children.chunks_exact(20).rev() {
                    push(
                        &mut self.stack,
                        Walk {
                            oid: Oid::from_bytes(child)?,
                            path: 0,
                        },
                    )?;
                }
            }
        }
        if self.trees.layout() == TreeLayout::Clustered {
            let incoming = &self.incoming;
            self.order.sort_unstable_by_key(|ordinal| {
                incoming.get(*ordinal as usize).map(|entry| {
                    (
                        u8::from(entry.record.kind == Kind::Tree),
                        if entry.record.kind == Kind::Tree {
                            entry.record.path
                        } else {
                            0
                        },
                        entry.visit,
                    )
                })
            });
        }
        for i in 0..self.order.len() {
            let ordinal = *self.order.get(i).ok_or(Error::Invalid("layout ordinal"))?;
            let entry = *self
                .incoming
                .get(ordinal as usize)
                .ok_or(Error::Invalid("layout input entry"))?;
            if entry.record.kind == Kind::Tree {
                self.materialize(ordinal, body)?;
                let original = if entry.header.kind.full() {
                    Some(take(body, entry.start as usize, entry.record.len as usize)?)
                } else {
                    None
                };
                self.trees.add(
                    builder,
                    entry.record,
                    take(&self.base, 0, entry.record.size as usize)?,
                    original,
                )?;
            } else {
                let source = if entry.spool {
                    self.spool.as_ref()
                } else {
                    body
                };
                let raw = take(
                    source,
                    entry.record.offset as usize,
                    entry.record.len as usize,
                )?;
                builder.add(entry.record, raw)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn store_full(
        &mut self,
        ordinal: usize,
        kind: Kind,
        size: usize,
        offset: u64,
        len: usize,
        spool: bool,
        depth: u16,
        builder: &mut Builder,
    ) -> Result<()> {
        let content = take(&self.payload, 0, size)?;
        let oid = object_id(kind, content)?;
        let pointer = if kind == Kind::Blob {
            match parse_git_hfs_v1(content) {
                Ok(pointer) => pointer,
                Err(Error::Invalid(reason)) if !self.strict_hfs => {
                    self.diagnostics.unparsed_hfs = self.diagnostics.unparsed_hfs.saturating_add(1);
                    if self.diagnostics.first.is_none() {
                        self.diagnostics.first = Some((oid, reason));
                    }
                    None
                }
                Err(Error::Invalid(reason)) => return Err(Error::MalformedHfs { oid, reason }),
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let graph_offset = builder.raw_graph.object(kind, content)?;
        let record = Record {
            oid,
            offset,
            len: u32::try_from(len).map_err(|_| Error::Capacity("packed object"))?,
            size: size as u64,
            crc: 0,
            kind,
            graph_offset,
            pointer,
            base: NO_BASE,
            delta_size: 0,
            path: 0,
            epoch: 0,
        };
        let entry = self
            .incoming
            .get_mut(ordinal)
            .ok_or(Error::Invalid("decoded entry ordinal"))?;
        entry.record = record;
        entry.resolved = true;
        entry.spool = spool;
        entry.depth = depth;
        let incoming = &self.incoming;
        self.oid_map.insert(oid, ordinal as u32, |ordinal| {
            Ok(incoming
                .get(ordinal as usize)
                .ok_or(Error::Invalid("OID map entry"))?
                .record
                .oid)
        })?;
        let position = cache_put(
            &mut self.cache,
            &mut self.cache_head,
            take(&self.payload, 0, size)?,
        )?;
        self.incoming
            .get_mut(ordinal)
            .ok_or(Error::Invalid("cache ordinal"))?
            .cache_position = position;
        Ok(())
    }

    fn resolve(
        &mut self,
        ordinal: usize,
        input: &[u8],
        payload_ready: bool,
        builder: &mut Builder,
    ) -> Result<bool> {
        let entry = *self
            .incoming
            .get(ordinal)
            .ok_or(Error::Invalid("delta ordinal"))?;
        let base_ordinal = match entry.header.base {
            Base::Offset(offset) => {
                let rank = self
                    .incoming
                    .binary_search_by_key(&offset, |entry| entry.start)
                    .map_err(|_| Error::Invalid("OFS_DELTA does not name an entry boundary"))?;
                self.incoming
                    .get(rank)
                    .filter(|base| base.resolved)
                    .map(|_| rank as u32)
            }
            Base::Oid(oid) => {
                let rank = self.oid_map.find(oid, |rank| {
                    Ok(self
                        .incoming
                        .get(rank as usize)
                        .ok_or(Error::Invalid("delta map ordinal"))?
                        .record
                        .oid)
                })?;
                rank.filter(|rank| {
                    self.incoming
                        .get(*rank as usize)
                        .is_some_and(|base| base.resolved)
                })
            }
            Base::None => return Err(Error::Invalid("delta without base")),
        };
        let Some(base_ordinal) = base_ordinal else {
            return Ok(false);
        };
        let base = *self
            .incoming
            .get(base_ordinal as usize)
            .ok_or(Error::Invalid("delta base ordinal"))?;
        let depth = base
            .depth
            .checked_add(1)
            .filter(|depth| *depth <= 128)
            .ok_or(Error::Invalid("delta depth exceeds 128"))?;
        if !payload_ready {
            let start = (entry.start as usize)
                .checked_add(entry.header.len)
                .ok_or(Error::Invalid("delta zlib offset"))?;
            let consumed = self.zlib.inflate(
                take(input, start, entry.compressed_len as usize)?,
                &mut self.payload,
                entry.header.size,
            )?;
            if consumed != entry.compressed_len as usize {
                return Err(Error::Invalid("delta zlib length"));
            }
        }
        self.materialize(base_ordinal, input)?;
        let size = apply_delta(
            take(&self.base, 0, base.record.size as usize)?,
            take(&self.payload, 0, entry.header.size)?,
            &mut self.result,
        )?;
        // Swap fixed buffers; the next object overwrites the previous scratch bytes.
        std::mem::swap(&mut self.payload, &mut self.result);
        if base.record.kind == Kind::Tree {
            self.store_full(
                ordinal,
                Kind::Tree,
                size,
                entry.start,
                entry
                    .header
                    .len
                    .saturating_add(entry.compressed_len as usize),
                false,
                depth,
                builder,
            )?;
            self.incoming
                .get_mut(ordinal)
                .ok_or(Error::Invalid("tree delta ordinal"))?
                .base_ordinal = base_ordinal;
        } else {
            let compressed_len = self
                .zlib
                .deflate(take(&self.payload, 0, size)?, &mut self.compressed)?;
            let mut header_bytes = [0; 10];
            let header_len = write_entry_header(base.record.kind, size, &mut header_bytes)?;
            let offset = self.spool.len() as u64;
            self.spool.append(take(&header_bytes, 0, header_len)?)?;
            self.spool
                .append(take(&self.compressed, 0, compressed_len)?)?;
            self.store_full(
                ordinal,
                base.record.kind,
                size,
                offset,
                header_len.saturating_add(compressed_len),
                true,
                depth,
                builder,
            )?;
        }
        Ok(true)
    }

    pub fn diagnostics(&self) -> Diagnostics {
        self.diagnostics
    }

    fn materialize(&mut self, ordinal: u32, input: &[u8]) -> Result<()> {
        let mut chain = [0u32; 129];
        let mut count = 0usize;
        let mut current = ordinal;
        loop {
            let entry = *self
                .incoming
                .get(current as usize)
                .ok_or(Error::Invalid("materialize ordinal"))?;
            let size = entry.record.size as usize;
            if let Some(bytes) = cache_get(&self.cache, self.cache_head, entry.cache_position, size)
            {
                self.base
                    .get_mut(..size)
                    .ok_or(Error::Capacity("cached base"))?
                    .copy_from_slice(bytes);
                break;
            }
            if entry.spool || entry.header.kind.full() {
                let source = if entry.spool {
                    self.spool.as_ref()
                } else {
                    input
                };
                let raw = take(
                    source,
                    entry.record.offset as usize,
                    entry.record.len as usize,
                )?;
                let header = entry_header(raw, entry.record.offset)?;
                if !header.kind.full() || header.size != size {
                    return Err(Error::Invalid("materialized full object"));
                }
                self.zlib.inflate(
                    raw.get(header.len..)
                        .ok_or(Error::Invalid("materialize zlib"))?,
                    &mut self.base,
                    size,
                )?;
                break;
            }
            *chain
                .get_mut(count)
                .ok_or(Error::Invalid("materialize delta depth"))? = current;
            count = count.saturating_add(1);
            if entry.base_ordinal == NO_BASE {
                return Err(Error::Invalid("unresolved materialize base"));
            }
            current = entry.base_ordinal;
        }
        for &rank in chain
            .get(..count)
            .ok_or(Error::Invalid("materialize chain"))?
            .iter()
            .rev()
        {
            let entry = *self
                .incoming
                .get(rank as usize)
                .ok_or(Error::Invalid("materialize delta ordinal"))?;
            let parent = self
                .incoming
                .get(entry.base_ordinal as usize)
                .ok_or(Error::Invalid("materialize parent"))?;
            let at = (entry.start as usize).saturating_add(entry.header.len);
            self.zlib.inflate(
                take(input, at, entry.compressed_len as usize)?,
                &mut self.base_delta,
                entry.header.size,
            )?;
            let size = apply_delta(
                take(&self.base, 0, parent.record.size as usize)?,
                take(&self.base_delta, 0, entry.header.size)?,
                &mut self.result,
            )?;
            std::mem::swap(&mut self.base, &mut self.result);
            let position = cache_put(
                &mut self.cache,
                &mut self.cache_head,
                take(&self.base, 0, size)?,
            )?;
            self.incoming
                .get_mut(rank as usize)
                .ok_or(Error::Invalid("materialize cache ordinal"))?
                .cache_position = position;
        }
        Ok(())
    }
}

fn cache_get(bytes: &[u8], head: u64, position: u64, len: usize) -> Option<&[u8]> {
    if position == u64::MAX
        || position < head.saturating_sub(bytes.len() as u64)
        || position.checked_add(len as u64)? > head
    {
        return None;
    }
    let at = usize::try_from(position.checked_rem(bytes.len() as u64)?).ok()?;
    bytes.get(at..at.checked_add(len)?)
}

fn cache_put(bytes: &mut [u8], head: &mut u64, content: &[u8]) -> Result<u64> {
    if content.len() > bytes.len() {
        return Ok(u64::MAX);
    }
    let mut at = head
        .checked_rem(bytes.len() as u64)
        .ok_or(Error::Invalid("empty delta cache"))? as usize;
    if bytes.len().saturating_sub(at) < content.len() {
        *head = head
            .checked_add(bytes.len().saturating_sub(at) as u64)
            .ok_or(Error::Capacity("delta cache sequence"))?;
        at = 0;
    }
    let position = *head;
    bytes
        .get_mut(at..at.saturating_add(content.len()))
        .ok_or(Error::Capacity("delta cache"))?
        .copy_from_slice(content);
    *head = head
        .checked_add(content.len() as u64)
        .ok_or(Error::Capacity("delta cache sequence"))?;
    Ok(position)
}

fn directory_path(parent: u64, name: &[u8]) -> u64 {
    let hash = name.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
    });
    crate::mphf::mix(parent ^ crate::mphf::mix(hash))
}

#[allow(clippy::too_many_arguments)]
pub async fn ingest_git_delta(
    arena: &mut IngestArena,
    upstream: &mut Upstream,
    storage: &Storage,
    known: &[Arc<Slab>],
    have: Oid,
    want: Oid,
    tier: Tier,
    start: u64,
    end: u64,
    limits: Limits,
    input_pack: Option<&PathBuf>,
) -> Result<Slab> {
    let input = if let Some(path) = input_pack {
        if have != Oid::ZERO {
            return Err(Error::Usage(
                "--input-pack is only valid for genesis".into(),
            ));
        }
        crate::upstream::input_pack(path, limits.pack_bytes)?
    } else {
        upstream
            .fetch(have, want, storage.cache_dir(), limits.pack_bytes)
            .await?
    };
    if arena.trees.layout() != limits.tree_layout.for_tier(tier) {
        return Err(Error::Invalid("ingest arena layout does not match tier"));
    }
    let mut builder = Builder::mapped(limits, storage.cache_dir())?;
    {
        let region = gvfs_platform::HotRegion::enter();
        arena.decode(&input, want, known, &mut builder)?;
        if region.allocations() != 0 {
            return Err(Error::Invalid("ingestion allocated after arena warmup"));
        }
    }
    arena.diagnostics().report();
    drop(input);
    builder.finish(tier, start, end, want)
}
