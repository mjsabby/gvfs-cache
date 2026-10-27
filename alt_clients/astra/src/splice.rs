use crate::arena::push;
use crate::manifest::{MAX_SLABS, SlabRef, Tier};
use crate::mphf::NO_BASE;
use crate::slab::{Artifact, Slab, finish_artifact};
use crate::{Error, Oid20, Result};
use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub struct Span {
    pub source: u32,
    pub offset: u64,
    pub len: u64,
    pub base_offset: u64,
}

/// A bitset per selected slab replaces an allocating HashSet of 20-byte OIDs.
/// Reset touches only words used by the previous request.
pub struct Plan {
    pub slabs: Vec<Arc<Slab>>,
    starts: Vec<usize>,
    bits: Vec<u64>,
    touched: Vec<u32>,
    pub selected_rows: Vec<(u32, u32)>,
    pub spans: Vec<Span>,
    pub count: u32,
    pub pack_len: u64,
    pub total_rows: usize,
}

impl Plan {
    pub fn new(max_objects: usize, max_spans: usize) -> Self {
        let words = max_objects.div_ceil(64);
        Self {
            slabs: Vec::with_capacity(MAX_SLABS),
            starts: Vec::with_capacity(MAX_SLABS),
            bits: vec![0; words],
            touched: Vec::with_capacity(words),
            selected_rows: Vec::with_capacity(max_objects),
            spans: Vec::with_capacity(max_spans),
            count: 0,
            pack_len: 32,
            total_rows: 0,
        }
    }
    pub fn reset(&mut self) -> Result<()> {
        for &word in &self.touched {
            *self
                .bits
                .get_mut(word as usize)
                .ok_or(Error::Invalid("plan bitset"))? = 0;
        }
        self.touched.clear();
        self.slabs.clear();
        self.starts.clear();
        self.spans.clear();
        self.selected_rows.clear();
        self.count = 0;
        self.pack_len = 32;
        self.total_rows = 0;
        Ok(())
    }
    pub fn add(&mut self, slab: &Arc<Slab>) -> Result<()> {
        let end = self
            .total_rows
            .checked_add(slab.index.len())
            .ok_or(Error::Capacity("plan rows"))?;
        if end > self.bits.len().saturating_mul(64) {
            return Err(Error::Capacity("plan rows; raise --max-objects"));
        }
        push(&mut self.starts, self.total_rows)?;
        push(&mut self.slabs, Arc::clone(slab))?;
        self.total_rows = end;
        Ok(())
    }
    pub fn row_index(&self, source: usize, row: u32) -> Result<usize> {
        if row as usize
            >= self
                .slabs
                .get(source)
                .ok_or(Error::Invalid("slab"))?
                .index
                .len()
        {
            return Err(Error::Invalid("plan row"));
        }
        self.starts
            .get(source)
            .copied()
            .and_then(|n| n.checked_add(row as usize))
            .ok_or(Error::Invalid("plan row"))
    }
    pub fn selected(&self, source: usize, row: u32) -> Result<bool> {
        let i = self.row_index(source, row)?;
        Ok(self
            .bits
            .get(i >> 6)
            .is_some_and(|word| word & (1u64 << (i & 63)) != 0))
    }
    pub fn select(&mut self, mut source: usize, mut row: u32) -> Result<()> {
        // REF_DELTA payloads are position-independent, but bases must be in the pack.
        // Always prefer an earlier slab, preventing cross-slab representation cycles.
        for _ in 0..129 {
            let oid = self
                .slabs
                .get(source)
                .ok_or(Error::Invalid("slab"))?
                .index
                .record(row)?
                .oid;
            let before = source;
            for earlier in 0..before {
                if let Some(found) = self
                    .slabs
                    .get(earlier)
                    .ok_or(Error::Invalid("slab"))?
                    .index
                    .find(&oid)?
                {
                    source = earlier;
                    row = found;
                    break;
                }
            }
            let i = self.row_index(source, row)?;
            let word = self
                .bits
                .get_mut(i >> 6)
                .ok_or(Error::Invalid("plan bits"))?;
            let mask = 1u64 << (i & 63);
            if *word & mask != 0 {
                return Ok(());
            }
            if *word == 0 {
                push(&mut self.touched, (i >> 6) as u32)?;
            }
            push(&mut self.selected_rows, (source as u32, row))?;
            *word |= mask;
            self.count = self
                .count
                .checked_add(1)
                .ok_or(Error::Capacity("pack objects"))?;
            let base = self
                .slabs
                .get(source)
                .ok_or(Error::Invalid("slab"))?
                .index
                .record(row)?
                .base;
            if base == NO_BASE {
                return Ok(());
            }
            row = base;
        }
        Err(Error::Invalid("excessive delta dependency depth"))
    }
    pub fn select_oid(&mut self, oid: &Oid20) -> Result<(usize, u32)> {
        let (source, row) = crate::slab::find(&self.slabs, oid)?.ok_or(Error::Missing(*oid))?;
        self.select(source, row)?;
        Ok((source, row))
    }
    pub fn select_since(&mut self, timestamp: u64) -> Result<()> {
        for source in 0..self.slabs.len() {
            let len = self
                .slabs
                .get(source)
                .ok_or(Error::Invalid("slab"))?
                .index
                .len();
            for row in 0..len {
                let record = self
                    .slabs
                    .get(source)
                    .ok_or(Error::Invalid("slab"))?
                    .index
                    .record(row as u32)?;
                if timestamp == 0 || record.timestamp > timestamp {
                    self.select(source, row as u32)?;
                }
            }
        }
        self.finish()
    }
    pub fn finish(&mut self) -> Result<()> {
        self.spans.clear();
        self.pack_len = 32;
        self.selected_rows.sort_unstable();
        for &(source, row) in &self.selected_rows {
            let slab = self
                .slabs
                .get(source as usize)
                .ok_or(Error::Invalid("slab"))?;
            let record = slab.index.record(row)?;
            let location = slab.index.location(&record)?;
            let base_offset = if record.base == NO_BASE {
                u64::MAX
            } else {
                slab.index
                    .location(&slab.index.record(record.base)?)?
                    .offset
            };
            self.pack_len = self
                .pack_len
                .checked_add(u64::from(location.len))
                .ok_or(Error::Capacity("wire pack length"))?;
            match self.spans.last_mut() {
                Some(span)
                    if span.source == source
                        && span.offset.checked_add(span.len) == Some(location.offset) =>
                {
                    span.len = span
                        .len
                        .checked_add(u64::from(location.len))
                        .ok_or(Error::Capacity("wire span"))?;
                }
                _ => push(
                    &mut self.spans,
                    Span {
                        source,
                        offset: location.offset,
                        len: u64::from(location.len),
                        base_offset,
                    },
                )?,
            }
        }
        Ok(())
    }
    pub fn whole_slab(&self) -> Option<&Slab> {
        let [span] = self.spans.as_slice() else {
            return None;
        };
        let slab = self.slabs.get(span.source as usize)?;
        if self.count == slab.meta.count && self.pack_len == slab.meta.pack_len && span.offset == 12
        {
            Some(slab)
        } else {
            None
        }
    }
    pub fn freeze(&mut self) {
        self.bits = Vec::new();
        self.touched = Vec::new();
        self.starts = Vec::new();
        self.selected_rows = Vec::new();
    }
    pub(crate) fn compiled_copy(&self) -> Self {
        Self {
            slabs: self.slabs.clone(),
            spans: self.spans.clone(),
            count: self.count,
            pack_len: self.pack_len,
            starts: Vec::new(),
            bits: Vec::new(),
            touched: Vec::new(),
            selected_rows: Vec::new(),
            total_rows: 0,
        }
    }
    pub(crate) fn append_disjoint_live(&mut self, live: &Arc<Slab>) -> Result<()> {
        // ingest_git_delta excludes the complete sealed catalog before creating L0.
        let source = self.slabs.len() as u32;
        self.slabs.push(Arc::clone(live));
        self.count = self
            .count
            .checked_add(live.meta.count)
            .ok_or(Error::Capacity("cold pack count"))?;
        let len = live
            .meta
            .pack_len
            .checked_sub(32)
            .ok_or(Error::Invalid("live pack"))?;
        self.pack_len = self
            .pack_len
            .checked_add(len)
            .ok_or(Error::Capacity("cold pack length"))?;
        if len > 0 {
            self.spans.push(Span {
                source,
                offset: 12,
                len,
                base_offset: u64::MAX,
            });
        }
        Ok(())
    }
}

/// Read adjacent ranges together, but hash and emit only selected bytes.
/// The buffer bounds each Range GET even for a multi-gigabyte baseline.
pub fn splice_slab_ranges(
    plan: &Plan,
    output: &mut impl Write,
    buffer: &mut [u8],
    max_gap: u64,
) -> Result<Oid20> {
    if buffer.is_empty() {
        return Err(Error::Capacity("range buffer"));
    }
    let _hot = alloc_guard::hot_path();
    let mut hash = Sha1::new();
    let header = crate::pack::pack_header(plan.count);
    output.write_all(&header)?;
    hash.update(header);
    let mut i = 0usize;
    while let Some(span) = plan.spans.get(i) {
        let source = plan
            .slabs
            .get(span.source as usize)
            .ok_or(Error::Invalid("span source"))?;
        let mut end = span
            .offset
            .checked_add(span.len)
            .ok_or(Error::Invalid("span length"))?;
        let mut next = i.saturating_add(1);
        while let Some(other) = plan.spans.get(next) {
            let other_end = other
                .offset
                .checked_add(other.len)
                .ok_or(Error::Invalid("span length"))?;
            if other.source != span.source
                || other.offset < end
                || (other.offset.saturating_sub(end) > max_gap
                    && !(other.base_offset >= span.offset && other.base_offset < end))
                || other_end.saturating_sub(span.offset) > buffer.len() as u64
            {
                break;
            }
            end = other_end;
            next = next.saturating_add(1);
        }
        if next > i.saturating_add(1) {
            let len = usize::try_from(end.saturating_sub(span.offset))
                .map_err(|_| Error::Capacity("range"))?;
            let buf = buffer.get_mut(..len).ok_or(Error::Capacity("range"))?;
            source.pack.read(span.offset, buf)?;
            for piece in plan.spans.get(i..next).ok_or(Error::Invalid("spans"))? {
                let from = usize::try_from(piece.offset.saturating_sub(span.offset))
                    .map_err(|_| Error::Invalid("subrange"))?;
                let to = from
                    .checked_add(piece.len as usize)
                    .ok_or(Error::Invalid("subrange"))?;
                let bytes = buf.get(from..to).ok_or(Error::Invalid("subrange"))?;
                output.write_all(bytes)?;
                hash.update(bytes);
            }
        } else {
            let mut at = span.offset;
            while at < end {
                let len = end.saturating_sub(at).min(buffer.len() as u64) as usize;
                let buf = buffer.get_mut(..len).ok_or(Error::Capacity("range"))?;
                source.pack.read(at, buf)?;
                output.write_all(buf)?;
                hash.update(&*buf);
                at = at.saturating_add(len as u64);
            }
        }
        i = next;
    }
    let digest = Oid20(hash.finalize().into());
    output.write_all(&digest.0)?;
    Ok(digest)
}

pub fn seal_plan(
    plan: &Plan,
    tier: Tier,
    start: u64,
    end: u64,
    tip: Oid20,
    work: &Path,
    buffer: &mut [u8],
) -> Result<Artifact> {
    let dir = tempfile::tempdir_in(work)?;
    let mut output = BufWriter::with_capacity(1 << 20, File::create(dir.path().join("slab.pack"))?);
    let hash = splice_slab_ranges(plan, &mut output, buffer, 64 << 10)?;
    output.flush()?;
    output.get_ref().sync_all()?;
    let mut rows = Vec::with_capacity(plan.count as usize);
    let mut overflow = Vec::with_capacity(plan.count as usize);
    let mut ordinals = vec![NO_BASE; plan.total_rows];
    for &(source, row) in &plan.selected_rows {
        let slab = plan
            .slabs
            .get(source as usize)
            .ok_or(Error::Invalid("slab"))?;
        *ordinals
            .get_mut(plan.row_index(source as usize, row)?)
            .ok_or(Error::Invalid("ordinal map"))? = rows.len() as u32;
        rows.push(slab.index.record(row)?);
    }
    let mut offset = 12u64;
    let mut cursor = 0usize;
    for &(source, ordinal) in &plan.selected_rows {
        let slab = plan
            .slabs
            .get(source as usize)
            .ok_or(Error::Invalid("slab"))?;
        let original = slab.index.record(ordinal)?;
        let row = rows.get_mut(cursor).ok_or(Error::Invalid("output row"))?;
        let location = slab.index.location(&original)?;
        row.locate(offset, location.len, &mut overflow)?;
        if original.base != NO_BASE {
            let oid = slab.index.record(original.base)?.oid;
            let (base_source, base_row) =
                crate::slab::find(&plan.slabs, &oid)?.ok_or(Error::Missing(oid))?;
            row.base = *ordinals
                .get(plan.row_index(base_source, base_row)?)
                .ok_or(Error::Invalid("base ordinal"))?;
            if row.base == NO_BASE {
                return Err(Error::Invalid("unselected delta base"));
            }
        }
        offset = offset
            .checked_add(u64::from(location.len))
            .ok_or(Error::Capacity("pack"))?;
        cursor = cursor.saturating_add(1);
    }
    let meta = SlabRef {
        hash,
        index_hash: Oid20([0; 20]),
        tip,
        tier,
        start,
        end,
        count: plan.count,
        pack_len: plan.pack_len,
        idx_len: 0,
    };
    finish_artifact(dir, meta, &rows, &overflow)
}
