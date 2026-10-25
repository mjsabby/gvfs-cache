use std::sync::Arc;

use crate::arena::{push, reserve};
use crate::codec::{Kind, Oid, TreeEntries, take, write_entry_header, write_ofs_distance};
use crate::mphf::OrdinalMap;
use crate::slab::{Builder, Limits, NO_BASE, Slab};
use crate::storage::Storage;
use crate::{Error, Result};

#[derive(Clone, Copy, Debug)]
pub struct Selection {
    pub slab: u32,
    pub ordinal: u32,
    pub output_offset: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RangeBatch {
    pub slab: u32,
    pub start: u64,
    pub end: u64,
    pub first: u32,
    pub count: u32,
}

pub struct SpliceArena {
    pub entries: Vec<Selection>,
    pub ranges: Vec<RangeBatch>,
    pub bytes: u64,
    seen: OrdinalMap,
    stack: Vec<Oid>,
    depths: Vec<u32>,
    expanded: Vec<u32>,
    dependencies: bool,
}

impl SpliceArena {
    pub fn new(max_objects: usize, max_ranges: usize) -> Result<Self> {
        Ok(Self {
            entries: reserve(max_objects)?,
            ranges: reserve(max_ranges)?,
            bytes: 0,
            seen: OrdinalMap::new(max_objects)?,
            stack: reserve(max_objects)?,
            depths: reserve(max_objects)?,
            expanded: reserve(max_objects)?,
            dependencies: true,
        })
    }

    pub fn reset(&mut self) {
        self.entries.clear();
        self.ranges.clear();
        self.stack.clear();
        self.depths.clear();
        self.expanded.clear();
        self.seen.reset();
        self.bytes = 0;
        self.dependencies = true;
    }

    fn find_seen(&self, slabs: &[Arc<Slab>], oid: Oid) -> Result<Option<u32>> {
        self.seen.find(oid, |rank| {
            let selected = self
                .entries
                .get(rank as usize)
                .ok_or(Error::Invalid("splice seen ordinal"))?;
            slabs
                .get(selected.slab as usize)
                .ok_or(Error::Invalid("splice seen slab"))?
                .oid(selected.ordinal)
        })
    }

    fn add(&mut self, slabs: &[Arc<Slab>], mut selection: Selection) -> Result<bool> {
        let slab = slabs
            .get(selection.slab as usize)
            .ok_or(Error::Invalid("splice slab"))?;
        let record = slab.record(selection.ordinal)?;
        if self.find_seen(slabs, record.oid)?.is_some() {
            return Ok(false);
        }
        if self.dependencies && record.base != NO_BASE {
            self.add(
                slabs,
                Selection {
                    slab: selection.slab,
                    ordinal: record.base,
                    output_offset: 0,
                },
            )?;
        }
        selection.output_offset = self
            .bytes
            .checked_add(12)
            .ok_or(Error::Capacity("splice object offset"))?;
        let output_len = if self.dependencies && record.base != NO_BASE {
            let base = slab.record(record.base)?;
            let prior = self
                .find_seen(slabs, base.oid)?
                .ok_or(Error::Invalid("missing selected delta base"))?;
            let base_offset = self
                .entries
                .get(prior as usize)
                .ok_or(Error::Invalid("selected base ordinal"))?
                .output_offset;
            let mut header = [0; 20];
            let (_, old_len, new_len) = delta_headers(
                record,
                base.offset,
                selection.output_offset,
                base_offset,
                &mut header,
            )?;
            u64::from(record.len)
                .checked_sub(old_len as u64)
                .and_then(|n| n.checked_add(new_len as u64))
                .ok_or(Error::Invalid("delta splice length"))?
        } else {
            u64::from(record.len)
        };
        let entries = &self.entries;
        let prior = self
            .seen
            .insert(record.oid, self.entries.len() as u32, |rank| {
                let selected = entries
                    .get(rank as usize)
                    .ok_or(Error::Invalid("splice seen ordinal"))?;
                slabs
                    .get(selected.slab as usize)
                    .ok_or(Error::Invalid("splice seen slab"))?
                    .oid(selected.ordinal)
            })?;
        if prior.is_some() {
            return Ok(false);
        }
        push(&mut self.entries, selection)?;
        push(&mut self.expanded, 0)?;
        self.bytes = self
            .bytes
            .checked_add(output_len)
            .ok_or(Error::Capacity("splice bytes"))?;
        Ok(true)
    }

    /// Full OID deduplication, no zlib work. Input slab order controls precedence.
    pub fn plan(
        &mut self,
        slabs: &[Arc<Slab>],
        filter: Option<&[Oid]>,
        gap: u64,
        range_bytes: usize,
    ) -> Result<()> {
        self.plan_from(slabs, filter, None, gap, range_bytes)
    }

    pub fn plan_after(
        &mut self,
        slabs: &[Arc<Slab>],
        timestamp: u64,
        gap: u64,
        range_bytes: usize,
    ) -> Result<()> {
        self.plan_from(slabs, None, Some(timestamp), gap, range_bytes)
    }

    fn plan_from(
        &mut self,
        slabs: &[Arc<Slab>],
        filter: Option<&[Oid]>,
        newer_than: Option<u64>,
        gap: u64,
        range_bytes: usize,
    ) -> Result<()> {
        self.reset();
        if let Some(oids) = filter {
            for &oid in oids {
                let selected = locate(slabs, oid)?.ok_or(Error::Missing(oid))?;
                self.add(slabs, selected)?;
            }
        } else {
            for (slab_index, slab) in slabs.iter().enumerate() {
                for ordinal in 0..slab.reference.count {
                    if let Some(timestamp) = newer_than.filter(|timestamp| *timestamp != 0)
                        && slab.record(ordinal)?.epoch <= timestamp
                    {
                        continue;
                    }
                    self.add(
                        slabs,
                        Selection {
                            slab: slab_index as u32,
                            ordinal,
                            output_offset: 0,
                        },
                    )?;
                }
            }
        }
        self.coalesce(slabs, gap, range_bytes)
    }

    /// The weekly baseline is a real Merkle walk, including every historical commit.
    /// Only structural edges are persisted; file edges come from canonical trees.
    pub fn plan_merkle(
        &mut self,
        slabs: &[Arc<Slab>],
        tip: Oid,
        reader: &mut crate::tree::TreeReader,
    ) -> Result<()> {
        self.reset();
        self.dependencies = false;
        push(&mut self.stack, tip)?;
        while let Some(oid) = self.stack.pop() {
            if self.find_seen(slabs, oid)?.is_some() {
                continue;
            }
            let selected = locate(slabs, oid)?.ok_or(Error::Missing(oid))?;
            if !self.add(slabs, selected)? {
                continue;
            }
            let slab = slabs
                .get(selected.slab as usize)
                .ok_or(Error::Invalid("Merkle slab"))?;
            let record = slab.record(selected.ordinal)?;
            if record.kind == Kind::Tree {
                let len = reader.read_local(slab, selected.ordinal)?;
                let mut children = TreeEntries::new(reader.content(len)?);
                let start = self.stack.len();
                while let Some(child) = children.next_entry()? {
                    if !child.gitlink && self.find_seen(slabs, child.oid)?.is_none() {
                        push(&mut self.stack, child.oid)?;
                    }
                }
                self.stack
                    .get_mut(start..)
                    .ok_or(Error::Invalid("Merkle child stack"))?
                    .reverse();
            } else {
                for child in slab.full_children(record)?.chunks_exact(20).rev() {
                    let oid = Oid::from_bytes(child)?;
                    if self.find_seen(slabs, oid)?.is_none() {
                        push(&mut self.stack, oid)?;
                    }
                }
            }
        }
        // Rebuild sources are hydrated. Merkle order is intentionally unrelated
        // to pack offsets; constructing an HTTP range plan here is unbounded.
        Ok(())
    }

    pub fn plan_objects(
        &mut self,
        slabs: &[Arc<Slab>],
        oids: &[Oid],
        depth: u32,
        graph: &mut crate::graph::Scratch,
        gap: u64,
        range_bytes: usize,
    ) -> Result<()> {
        self.reset();
        for &oid in oids.iter().rev() {
            push(&mut self.stack, oid)?;
            push(&mut self.depths, depth)?;
        }
        while let Some(oid) = self.stack.pop() {
            let depth = self
                .depths
                .pop()
                .ok_or(Error::Invalid("closure depth stack"))?;
            let selected = locate(slabs, oid)?.ok_or(Error::Missing(oid))?;
            self.add(slabs, selected)?;
            let rank = self
                .seen
                .find(oid, |rank| {
                    let entry = self
                        .entries
                        .get(rank as usize)
                        .ok_or(Error::Invalid("closure seen entry"))?;
                    slabs
                        .get(entry.slab as usize)
                        .ok_or(Error::Invalid("closure seen slab"))?
                        .oid(entry.ordinal)
                })?
                .ok_or(Error::Invalid("closure seen key"))?;
            let marker = depth.saturating_add(1);
            let visited = self
                .expanded
                .get_mut(rank as usize)
                .ok_or(Error::Invalid("closure visited depth"))?;
            if *visited >= marker {
                continue;
            }
            *visited = marker;
            let slab = slabs
                .get(selected.slab as usize)
                .ok_or(Error::Invalid("closure slab"))?;
            let record = slab.record(selected.ordinal)?;
            let children = graph.read(slab, record)?;
            for (i, child) in children.chunks_exact(20).enumerate().rev() {
                let child_depth = if record.kind == crate::codec::Kind::Commit && i != 0 {
                    if depth <= 1 {
                        continue;
                    }
                    depth.saturating_sub(1)
                } else if record.kind == crate::codec::Kind::Tag {
                    depth
                } else {
                    0
                };
                push(&mut self.stack, Oid::from_bytes(child)?)?;
                push(&mut self.depths, child_depth)?;
            }
        }
        self.coalesce(slabs, gap, range_bytes)
    }

    pub fn coalesce(&mut self, slabs: &[Arc<Slab>], gap: u64, range_bytes: usize) -> Result<()> {
        self.ranges.clear();
        for (i, entry) in self.entries.iter().enumerate() {
            let record = slabs
                .get(entry.slab as usize)
                .ok_or(Error::Invalid("range slab"))?
                .record(entry.ordinal)?;
            let end = record
                .offset
                .checked_add(u64::from(record.len))
                .ok_or(Error::Invalid("range overflow"))?;
            if let Some(last) = self.ranges.last_mut() {
                // Descending ranges must never merge: saturating subtraction would
                // silently turn an earlier object into a zero-based subslice.
                if last.slab == entry.slab
                    && record.offset >= last.end
                    && record.offset.saturating_sub(last.end) <= gap
                    && end.saturating_sub(last.start) <= range_bytes as u64
                {
                    last.end = end;
                    last.count = last.count.saturating_add(1);
                    continue;
                }
            }
            push(
                &mut self.ranges,
                RangeBatch {
                    slab: entry.slab,
                    start: record.offset,
                    end,
                    first: i as u32,
                    count: 1,
                },
            )?;
        }
        Ok(())
    }

    pub fn pack_len(&self) -> Result<u64> {
        self.bytes
            .checked_add(32)
            .ok_or(Error::Capacity("response pack length"))
    }

    /// Return replacement header and source-header bytes to skip. Whole-body
    /// translation leaves distances unchanged and therefore returns (0, 0).
    pub fn rewrite(
        &self,
        slabs: &[Arc<Slab>],
        selected: Selection,
        header: &mut [u8; 20],
    ) -> Result<(usize, usize)> {
        let slab = slabs
            .get(selected.slab as usize)
            .ok_or(Error::Invalid("rewrite slab"))?;
        let record = slab.record(selected.ordinal)?;
        if record.base == NO_BASE {
            return Ok((0, 0));
        }
        let base = slab.record(record.base)?;
        let prior = self
            .find_seen(slabs, base.oid)?
            .ok_or(Error::Invalid("rewrite missing base"))?;
        let output_base = self
            .entries
            .get(prior as usize)
            .ok_or(Error::Invalid("rewrite base ordinal"))?
            .output_offset;
        let (changed, old_len, new_len) = delta_headers(
            record,
            base.offset,
            selected.output_offset,
            output_base,
            header,
        )?;
        if changed {
            Ok((new_len, old_len))
        } else {
            Ok((0, 0))
        }
    }

    fn index_entry(&self, slabs: &[Arc<Slab>], selected: Selection) -> Result<crate::idx::Entry> {
        let slab = slabs
            .get(selected.slab as usize)
            .ok_or(Error::Invalid("index source"))?;
        let record = slab.record(selected.ordinal)?;
        let mut header = [0; 20];
        let (len, skip) = self.rewrite(slabs, selected, &mut header)?;
        let crc = if len == 0 {
            record.crc
        } else {
            let base = slab.record(record.base)?;
            let mut old = [0; 20];
            let (_, _, old_len) =
                delta_headers(record, base.offset, record.offset, base.offset, &mut old)?;
            if old_len != skip {
                return Err(Error::Invalid("index header length"));
            }
            crate::idx::replace_prefix_crc(
                record.crc,
                take(&old, 0, old_len)?,
                take(&header, 0, len)?,
                u64::from(record.len)
                    .checked_sub(skip as u64)
                    .ok_or(Error::Invalid("index payload length"))?,
            )
        };
        Ok(crate::idx::Entry {
            oid: record.oid,
            crc,
            offset: selected.output_offset,
        })
    }

    /// Reuse a complete source's sorted idx and merge only the small new tail.
    /// Cold clones stay O(N), without sorting millions of OIDs per request.
    pub fn prepare_index(
        &self,
        slabs: &[Arc<Slab>],
        out: &mut Vec<crate::idx::Entry>,
    ) -> Result<usize> {
        let mut complete = None;
        let mut largest = 0usize;
        for (start, entry) in self.entries.iter().enumerate() {
            if entry.ordinal != 0 {
                continue;
            }
            let slab = slabs
                .get(entry.slab as usize)
                .ok_or(Error::Invalid("index complete source"))?;
            let n = slab.reference.count as usize;
            if n <= largest || n < 4096 {
                continue;
            }
            if let Some(run) = self.entries.get(start..start.saturating_add(n))
                && run.iter().enumerate().all(|(i, selected)| {
                    selected.slab == entry.slab && selected.ordinal as usize == i
                })
            {
                complete = Some((start, n, entry.slab));
                largest = n;
            }
        }
        out.clear();
        for (i, &selected) in self.entries.iter().enumerate() {
            if complete.is_some_and(|(start, n, _)| i >= start && i < start.saturating_add(n)) {
                continue;
            }
            push(out, self.index_entry(slabs, selected)?)?;
        }
        out.sort_unstable_by_key(|entry| entry.oid);
        if let Some((start, count, source)) = complete {
            let slab = slabs
                .get(source as usize)
                .ok_or(Error::Invalid("index baseline"))?;
            let view = crate::idx::View::parse(&slab.idx, count)?;
            let shift = self
                .entries
                .get(start)
                .ok_or(Error::Invalid("index baseline start"))?
                .output_offset
                .checked_sub(12)
                .ok_or(Error::Invalid("index baseline translation"))?;
            let mut small = out.len();
            let mut base = count;
            if self.entries.len() > out.capacity() {
                return Err(Error::Capacity("prefetch index rows"));
            }
            out.resize(self.entries.len(), crate::idx::Entry::EMPTY);
            let mut destination = out.len();
            while destination != 0 {
                let a = if base != 0 {
                    Some(view.entry(base.saturating_sub(1))?)
                } else {
                    None
                };
                let b = if small != 0 {
                    out.get(small.saturating_sub(1)).copied()
                } else {
                    None
                };
                let value = if a.is_some_and(|a| b.is_none_or(|b| a.oid > b.oid)) {
                    let mut value = a.ok_or(Error::Invalid("index base merge"))?;
                    value.offset = value
                        .offset
                        .checked_add(shift)
                        .ok_or(Error::Capacity("index offset"))?;
                    base = base.saturating_sub(1);
                    value
                } else {
                    small = small
                        .checked_sub(1)
                        .ok_or(Error::Invalid("index tail merge"))?;
                    b.ok_or(Error::Invalid("index tail row"))?
                };
                destination = destination.saturating_sub(1);
                *out.get_mut(destination)
                    .ok_or(Error::Invalid("index merge destination"))? = value;
            }
        }
        crate::idx::encoded_len(
            out.len(),
            out.iter()
                .filter(|entry| entry.offset > 0x7fff_ffff)
                .count(),
        )
    }

    pub fn exclude(
        &mut self,
        slabs: &[Arc<Slab>],
        excluded: &[Arc<Slab>],
        gap: u64,
        range_bytes: usize,
    ) -> Result<()> {
        let mut written = 0usize;
        self.bytes = 0;
        for i in 0..self.entries.len() {
            let selected = *self
                .entries
                .get(i)
                .ok_or(Error::Invalid("exclude ordinal"))?;
            let record = slabs
                .get(selected.slab as usize)
                .ok_or(Error::Invalid("exclude slab"))?
                .record(selected.ordinal)?;
            if locate(excluded, record.oid)?.is_some() {
                continue;
            }
            *self
                .entries
                .get_mut(written)
                .ok_or(Error::Invalid("exclude output"))? = selected;
            written = written.saturating_add(1);
            self.bytes = self
                .bytes
                .checked_add(u64::from(record.len))
                .ok_or(Error::Capacity("excluded splice size"))?;
        }
        self.entries.truncate(written);
        self.dependencies = false;
        self.coalesce(slabs, gap, range_bytes)
    }
}

pub fn locate(slabs: &[Arc<Slab>], oid: Oid) -> Result<Option<Selection>> {
    for (i, slab) in slabs.iter().enumerate() {
        if let Some(ordinal) = slab.lookup(oid)? {
            return Ok(Some(Selection {
                slab: i as u32,
                ordinal,
                output_offset: 0,
            }));
        }
    }
    Ok(None)
}

/// Both compaction and HTTP use this plan and these exact position-independent
/// entry slices. The output owns one pack, never one pack per selected slab.
#[allow(clippy::too_many_arguments)]
pub async fn splice_slab_ranges(
    storage: &Storage,
    slabs: &[Arc<Slab>],
    plan: &SpliceArena,
    limits: Limits,
    tier: crate::manifest::Tier,
    start: u64,
    end: u64,
    tip: Oid,
) -> Result<Slab> {
    let layout = limits.tree_layout.for_tier(tier);
    let hydrated = hydrate_sources(storage, slabs).await?;
    let mut builder = Builder::mapped(limits, storage.cache_dir())?;
    let mut encode_trees = layout == crate::tree::TreeLayout::Clustered;
    if !encode_trees {
        for selected in &plan.entries {
            if hydrated
                .get(selected.slab as usize)
                .ok_or(Error::Invalid("encoding source"))?
                .record(selected.ordinal)?
                .base
                != NO_BASE
            {
                encode_trees = true;
                break;
            }
        }
    }
    let mut reader = if encode_trees {
        Some(crate::tree::TreeReader::new(limits)?)
    } else {
        None
    };
    let mut graph = crate::graph::Scratch::new(limits.object_bytes)?;
    let mut packer = if encode_trees {
        Some(crate::tree::TreePacker::new(
            limits,
            layout,
            storage.range_bytes(),
        )?)
    } else {
        None
    };
    let mut order = reserve(plan.entries.len())?;
    for i in 0..plan.entries.len() {
        push(&mut order, i as u32)?;
    }
    if layout == crate::tree::TreeLayout::Clustered {
        order.sort_unstable_by_key(|index| {
            plan.entries
                .get(*index as usize)
                .and_then(|entry| {
                    hydrated
                        .get(entry.slab as usize)
                        .and_then(|slab| slab.record(entry.ordinal).ok())
                })
                .map(|record| {
                    (
                        u8::from(record.kind == Kind::Tree),
                        if record.kind == Kind::Tree {
                            record.path
                        } else {
                            0
                        },
                        *index,
                    )
                })
        });
    }
    {
        let hot = gvfs_platform::HotRegion::enter();
        for index in order {
            let selected = *plan
                .entries
                .get(index as usize)
                .ok_or(Error::Invalid("seal selection"))?;
            let slab = hydrated
                .get(selected.slab as usize)
                .ok_or(Error::Invalid("seal source"))?;
            let mut record = slab.record(selected.ordinal)?;
            let children = graph.read(slab, record)?;
            builder.set_graph(&mut record, children)?;
            if record.kind != Kind::Tree
                || layout == crate::tree::TreeLayout::Full && record.base == NO_BASE
            {
                record.pointer = slab.pointer(selected.ordinal)?;
                builder.add(record, slab.raw(record)?)?;
            } else {
                let reader = reader
                    .as_mut()
                    .ok_or(Error::Invalid("tree encoding scratch"))?;
                let len = reader.read_local(slab, selected.ordinal)?;
                let original = if record.base == NO_BASE {
                    Some(slab.raw(record)?)
                } else {
                    None
                };
                packer
                    .as_mut()
                    .ok_or(Error::Invalid("tree packing scratch"))?
                    .add(&mut builder, record, reader.content(len)?, original)?;
            }
        }
        if hot.allocations() != 0 {
            return Err(Error::Invalid("slab sealing allocated after warmup"));
        }
    }
    builder.finish(tier, start, end, tip)
}

pub async fn hydrate_sources(storage: &Storage, slabs: &[Arc<Slab>]) -> Result<Vec<Arc<Slab>>> {
    let mut hydrated = reserve(slabs.len())?;
    for slab in slabs {
        if slab.pack.is_some() {
            push(&mut hydrated, Arc::clone(slab))?;
        } else {
            let mut copy = slab.as_ref().clone();
            copy.pack = Some(storage.hydrate_pack(slab.reference).await?);
            push(&mut hydrated, Arc::new(copy))?;
        }
    }
    Ok(hydrated)
}

fn delta_headers(
    record: crate::slab::Record,
    source_base: u64,
    output: u64,
    output_base: u64,
    out: &mut [u8; 20],
) -> Result<(bool, usize, usize)> {
    let source_distance = record
        .offset
        .checked_sub(source_base)
        .ok_or(Error::Invalid("source delta order"))?;
    let output_distance = output
        .checked_sub(output_base)
        .ok_or(Error::Invalid("output delta order"))?;
    let mut kind_header = [0; 10];
    let mut source_ofs = [0; 10];
    let mut output_ofs = [0; 10];
    let kind_len =
        write_entry_header(Kind::OfsDelta, record.delta_size as usize, &mut kind_header)?;
    let old_ofs_len = write_ofs_distance(source_distance, &mut source_ofs)?;
    let new_ofs_len = write_ofs_distance(output_distance, &mut output_ofs)?;
    out.get_mut(..kind_len)
        .ok_or(Error::Capacity("delta header"))?
        .copy_from_slice(take(&kind_header, 0, kind_len)?);
    out.get_mut(kind_len..kind_len.saturating_add(new_ofs_len))
        .ok_or(Error::Capacity("delta offset"))?
        .copy_from_slice(take(&output_ofs, 0, new_ofs_len)?);
    Ok((
        source_distance != output_distance,
        kind_len.saturating_add(old_ofs_len),
        kind_len.saturating_add(new_ofs_len),
    ))
}
