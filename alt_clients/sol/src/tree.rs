//! Optional long-tier layout. Full and clustered layouts use the same codecs and
//! are selected at sealing, so their bytes/timing can be compared on identical input.

use crate::arena::{append, reserve, zeroed};
use crate::codec::{
    Base, Kind, TreeEntries, TreeEntry, Zlib, apply_delta, entry_header, take, write_entry_header,
    write_ofs_distance,
};
use crate::manifest::Tier;
use crate::slab::{Builder, Limits, NO_BASE, Record, Slab};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TreeLayout {
    Full,
    #[default]
    Clustered,
}

impl TreeLayout {
    pub fn for_tier(self, tier: Tier) -> Self {
        if matches!(tier, Tier::H24 | Tier::WeeklyDelta | Tier::Baseline) {
            self
        } else {
            Self::Full
        }
    }
}

pub struct TreeReader {
    base: Vec<u8>,
    result: Vec<u8>,
    delta: Vec<u8>,
    zlib: Zlib,
    cached: Option<(crate::codec::Oid, u32)>,
}

impl TreeReader {
    pub fn new(limits: Limits) -> Result<Self> {
        let mut value = Self {
            base: zeroed(limits.object_bytes)?,
            result: zeroed(limits.object_bytes)?,
            delta: zeroed(
                limits
                    .object_bytes
                    .saturating_add(limits.object_bytes / 100)
                    .saturating_add(2048),
            )?,
            zlib: Zlib::new(1),
            cached: None,
        };
        let len = value.zlib.deflate(b"warmup", &mut value.delta)?;
        value
            .zlib
            .inflate(take(&value.delta, 0, len)?, &mut value.result, 6)?;
        Ok(value)
    }

    pub fn span(slab: &Slab, ordinal: u32) -> Result<(u64, usize)> {
        let record = slab.record(ordinal)?;
        let start = if record.base == NO_BASE {
            record.offset
        } else {
            slab.record(record.base)?.offset
        };
        let len = record
            .offset
            .checked_add(u64::from(record.len))
            .and_then(|end| end.checked_sub(start))
            .and_then(|len| usize::try_from(len).ok())
            .ok_or(Error::Invalid("tree span"))?;
        Ok((start, len))
    }

    pub fn read_local(&mut self, slab: &Slab, ordinal: u32) -> Result<usize> {
        let (start, len) = Self::span(slab, ordinal)?;
        let bytes = take(
            slab.pack
                .as_ref()
                .ok_or(Error::Invalid("local tree pack"))?,
            start as usize,
            len,
        )?;
        self.read_span(slab, ordinal, start, bytes)
    }

    pub fn read_span(
        &mut self,
        slab: &Slab,
        ordinal: u32,
        start: u64,
        bytes: &[u8],
    ) -> Result<usize> {
        let record = slab.record(ordinal)?;
        let relative = usize::try_from(
            record
                .offset
                .checked_sub(start)
                .ok_or(Error::Invalid("tree span start"))?,
        )
        .map_err(|_| Error::Invalid("tree span offset"))?;
        let raw = take(bytes, relative, record.len as usize)?;
        if crc32fast::hash(raw) != record.crc {
            return Err(Error::Invalid("tree entry CRC"));
        }
        let header = entry_header(raw, record.offset)?;
        if record.base == NO_BASE {
            if header.kind != record.kind || header.size as u64 != record.size {
                return Err(Error::Invalid("full tree header"));
            }
            let consumed = self.zlib.inflate(
                raw.get(header.len..).ok_or(Error::Invalid("tree zlib"))?,
                &mut self.result,
                header.size,
            )?;
            if consumed.saturating_add(header.len) != raw.len() {
                return Err(Error::Invalid("tree stream length"));
            }
            return Ok(header.size);
        }
        let base = slab.record(record.base)?;
        if header.kind != Kind::OfsDelta
            || header.size != record.delta_size as usize
            || !matches!(header.base, Base::Offset(offset) if offset == base.offset)
            || base.base != NO_BASE
        {
            return Err(Error::Invalid("cluster delta header"));
        }
        if self.cached != Some((slab.reference.id, record.base)) {
            let base_raw = take(
                bytes,
                usize::try_from(
                    base.offset
                        .checked_sub(start)
                        .ok_or(Error::Invalid("cluster span start"))?,
                )
                .map_err(|_| Error::Invalid("cluster base offset"))?,
                base.len as usize,
            )?;
            if crc32fast::hash(base_raw) != base.crc {
                return Err(Error::Invalid("cluster base CRC"));
            }
            let base_header = entry_header(base_raw, base.offset)?;
            if base_header.kind != Kind::Tree || base_header.size as u64 != base.size {
                return Err(Error::Invalid("cluster base header"));
            }
            self.zlib.inflate(
                base_raw
                    .get(base_header.len..)
                    .ok_or(Error::Invalid("cluster base zlib"))?,
                &mut self.base,
                base_header.size,
            )?;
            self.cached = Some((slab.reference.id, record.base));
        }
        self.zlib.inflate(
            raw.get(header.len..)
                .ok_or(Error::Invalid("cluster delta zlib"))?,
            &mut self.delta,
            header.size,
        )?;
        let len = apply_delta(
            take(&self.base, 0, base.size as usize)?,
            take(&self.delta, 0, header.size)?,
            &mut self.result,
        )?;
        if len as u64 != record.size {
            return Err(Error::Invalid("cluster result size"));
        }
        Ok(len)
    }

    pub fn content(&self, len: usize) -> Result<&[u8]> {
        take(&self.result, 0, len)
    }
}

pub fn delta_varint(mut value: u64, out: &mut Vec<u8>) -> Result<()> {
    loop {
        let byte = (value as u8) & 127;
        value >>= 7;
        append(out, &[byte | if value != 0 { 128 } else { 0 }])?;
        if value == 0 {
            return Ok(());
        }
    }
}

pub(crate) fn delta_copy(mut offset: usize, mut len: usize, out: &mut Vec<u8>) -> Result<()> {
    while len != 0 {
        let count = len.min(0xff_ffff);
        let mut op = 0x80u8;
        let mut operands = [0; 7];
        let mut used = 0usize;
        let offset32 = u32::try_from(offset).map_err(|_| Error::Capacity("delta copy offset"))?;
        for (bit, byte) in offset32.to_le_bytes().into_iter().enumerate() {
            if byte != 0 {
                op |= 1u8 << bit;
                *operands
                    .get_mut(used)
                    .ok_or(Error::Invalid("copy operands"))? = byte;
                used = used.saturating_add(1);
            }
        }
        if count != 65536 {
            for (bit, byte) in (count as u32).to_le_bytes().into_iter().take(3).enumerate() {
                if byte != 0 {
                    op |= 0x10u8 << bit;
                    *operands
                        .get_mut(used)
                        .ok_or(Error::Invalid("copy operands"))? = byte;
                    used = used.saturating_add(1);
                }
            }
        }
        append(out, &[op])?;
        append(out, take(&operands, 0, used)?)?;
        len = len.saturating_sub(count);
        offset = offset
            .checked_add(count)
            .ok_or(Error::Capacity("delta copy offset"))?;
    }
    Ok(())
}

/// A valid Git delta instruction stream, not a private prefix/suffix description.
pub fn compute_tree_delta(base: &[u8], target: &[u8], out: &mut Vec<u8>) -> Result<bool> {
    out.clear();
    let prefix = common_prefix(base, target);
    let base_rest = base.get(prefix..).ok_or(Error::Invalid("delta prefix"))?;
    let target_rest = target.get(prefix..).ok_or(Error::Invalid("delta prefix"))?;
    let suffix = common_suffix(base_rest, target_rest);
    if target.len().saturating_sub(prefix).saturating_sub(suffix) > 512 {
        return entry_delta(base, target, out);
    }
    if prefix.saturating_add(suffix) < base.len() / 2 {
        return Ok(false);
    }
    delta_varint(base.len() as u64, out)?;
    delta_varint(target.len() as u64, out)?;
    delta_copy(0, prefix, out)?;
    let literal_end = target
        .len()
        .checked_sub(suffix)
        .ok_or(Error::Invalid("delta suffix"))?;
    for literal in target
        .get(prefix..literal_end)
        .ok_or(Error::Invalid("delta literal"))?
        .chunks(127)
    {
        append(out, &[literal.len() as u8])?;
        append(out, literal)?;
    }
    delta_copy(base.len().saturating_sub(suffix), suffix, out)?;
    Ok(true)
}

fn literal(bytes: &[u8], out: &mut Vec<u8>) -> Result<()> {
    for chunk in bytes.chunks(127) {
        append(out, &[chunk.len() as u8])?;
        append(out, chunk)?;
    }
    Ok(())
}

// Sorted Git tree entries permit a linear merge for scattered edits: copy all
// unchanged islands, rather than encoding the entire middle as one literal.
fn entry_delta(base: &[u8], target: &[u8], out: &mut Vec<u8>) -> Result<bool> {
    out.clear();
    delta_varint(base.len() as u64, out)?;
    delta_varint(target.len() as u64, out)?;
    let mut source = TreeEntries::new(base);
    let mut destination = TreeEntries::new(target);
    let mut left = source.next_entry()?;
    let mut right = destination.next_entry()?;
    let mut literal_start = 0usize;
    let mut copy_start = 0usize;
    let mut copy_len = 0usize;
    let mut shared = 0usize;
    while let Some(target_entry) = right {
        let Some(base_entry) = left else {
            break;
        };
        if base_entry.raw == target_entry.raw {
            if copy_len != 0 && copy_start.saturating_add(copy_len) != base_entry.offset {
                delta_copy(copy_start, copy_len, out)?;
                copy_len = 0;
            }
            if literal_start != target_entry.offset {
                delta_copy(copy_start, copy_len, out)?;
                copy_len = 0;
                literal(
                    target
                        .get(literal_start..target_entry.offset)
                        .ok_or(Error::Invalid("tree delta literal"))?,
                    out,
                )?;
            }
            if copy_len == 0 {
                copy_start = base_entry.offset;
            }
            copy_len = copy_len.saturating_add(base_entry.raw.len());
            shared = shared.saturating_add(base_entry.raw.len());
            literal_start = target_entry.offset.saturating_add(target_entry.raw.len());
            left = source.next_entry()?;
            right = destination.next_entry()?;
        } else if base_entry.name == target_entry.name {
            left = source.next_entry()?;
            right = destination.next_entry()?;
        } else if tree_name_cmp(&base_entry, &target_entry) == std::cmp::Ordering::Less {
            left = source.next_entry()?;
        } else {
            right = destination.next_entry()?;
        }
    }
    delta_copy(copy_start, copy_len, out)?;
    literal(
        target
            .get(literal_start..)
            .ok_or(Error::Invalid("tree delta tail"))?,
        out,
    )?;
    Ok(shared >= base.len() / 2)
}

fn tree_name_cmp(a: &TreeEntry<'_>, b: &TreeEntry<'_>) -> std::cmp::Ordering {
    let common = a.name.len().min(b.name.len());
    let prefix = a.name.get(..common).cmp(&b.name.get(..common));
    if prefix != std::cmp::Ordering::Equal {
        return prefix;
    }
    let next = |entry: &TreeEntry<'_>| {
        entry
            .name
            .get(common)
            .copied()
            .unwrap_or(if entry.directory { b'/' } else { 0 })
    };
    next(a).cmp(&next(b))
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let mut matched = 0usize;
    for (left, right) in a.chunks_exact(32).zip(b.chunks_exact(32)) {
        if left != right {
            return matched
                .saturating_add(left.iter().zip(right).take_while(|(a, b)| a == b).count());
        }
        matched = matched.saturating_add(32);
    }
    matched.saturating_add(
        a.get(matched..)
            .unwrap_or_default()
            .iter()
            .zip(b.get(matched..).unwrap_or_default())
            .take_while(|(a, b)| a == b)
            .count(),
    )
}

fn common_suffix(a: &[u8], b: &[u8]) -> usize {
    let mut matched = 0usize;
    for (left, right) in a.rchunks_exact(32).zip(b.rchunks_exact(32)) {
        if left != right {
            return matched.saturating_add(
                left.iter()
                    .rev()
                    .zip(right.iter().rev())
                    .take_while(|(a, b)| a == b)
                    .count(),
            );
        }
        matched = matched.saturating_add(32);
    }
    matched.saturating_add(
        a.get(..a.len().saturating_sub(matched))
            .unwrap_or_default()
            .iter()
            .rev()
            .zip(
                b.get(..b.len().saturating_sub(matched))
                    .unwrap_or_default()
                    .iter()
                    .rev(),
            )
            .take_while(|(a, b)| a == b)
            .count(),
    )
}

#[derive(Default, Clone, Copy, Debug)]
pub struct TreeStats {
    pub full: u64,
    pub deltas: u64,
    pub bytes_saved: u64,
}

pub struct TreePacker {
    layout: TreeLayout,
    base: Vec<u8>,
    base_len: usize,
    base_full_len: usize,
    path: Option<u64>,
    base_rank: u32,
    members: usize,
    full: Vec<u8>,
    delta: Vec<u8>,
    compressed: Vec<u8>,
    zlib: Zlib,
    max_cluster_bytes: usize,
    pub stats: TreeStats,
}

impl TreePacker {
    pub fn reset(&mut self) {
        self.path = None;
        self.base_rank = NO_BASE;
        self.members = 0;
        self.stats = TreeStats::default();
    }

    pub fn layout(&self) -> TreeLayout {
        self.layout
    }

    pub fn new(limits: Limits, layout: TreeLayout, max_cluster_bytes: usize) -> Result<Self> {
        let compressed = limits
            .object_bytes
            .checked_add(limits.object_bytes / 100)
            .and_then(|n| n.checked_add(2048))
            .ok_or(Error::Capacity("tree scratch"))?;
        let mut value = Self {
            layout,
            base: zeroed(limits.object_bytes)?,
            base_len: 0,
            base_full_len: 0,
            path: None,
            base_rank: NO_BASE,
            members: 0,
            full: zeroed(compressed)?,
            delta: reserve(compressed)?,
            compressed: zeroed(compressed)?,
            zlib: Zlib::new(1),
            max_cluster_bytes,
            stats: TreeStats::default(),
        };
        let len = value.zlib.deflate(b"warmup", &mut value.compressed)?;
        value
            .zlib
            .inflate(take(&value.compressed, 0, len)?, &mut value.base, 6)?;
        Ok(value)
    }

    pub fn add(
        &mut self,
        builder: &mut Builder,
        mut record: Record,
        target: &[u8],
        original_full: Option<&[u8]>,
    ) -> Result<()> {
        if record.kind != Kind::Tree {
            return Err(Error::Invalid("non-tree in tree packer"));
        }
        let mut full_header = [0; 10];
        let full_header_len = write_entry_header(Kind::Tree, target.len(), &mut full_header)?;
        let mut full_len = original_full.map(<[u8]>::len);
        if self.layout == TreeLayout::Clustered
            && self.path == Some(record.path)
            && self.members < 16
            && compute_tree_delta(take(&self.base, 0, self.base_len)?, target, &mut self.delta)?
        {
            let compressed_len = self.zlib.deflate(&self.delta, &mut self.compressed)?;
            let mut header = [0; 10];
            let mut ofs = [0; 10];
            let header_len = write_entry_header(Kind::OfsDelta, self.delta.len(), &mut header)?;
            let base_offset = builder
                .records
                .get(self.base_rank as usize)
                .ok_or(Error::Invalid("tree cluster base"))?
                .offset;
            let distance = (builder.pack.len() as u64)
                .checked_sub(base_offset)
                .ok_or(Error::Invalid("tree cluster distance"))?;
            let ofs_len = write_ofs_distance(distance, &mut ofs)?;
            let packed_len = header_len
                .saturating_add(ofs_len)
                .saturating_add(compressed_len);
            let span = distance
                .checked_add(packed_len as u64)
                .ok_or(Error::Capacity("tree cluster span"))?;
            // Tiny deltas do not justify deflating an entire old tree merely for
            // a cost estimate. Ambiguous candidates compare actual compressed sizes.
            let clearly_smaller =
                packed_len.saturating_mul(4) < self.base_full_len && packed_len < target.len() / 4;
            let useful = if span > self.max_cluster_bytes as u64 {
                false
            } else if let Some(full_len) = full_len {
                packed_len < full_len
            } else if clearly_smaller {
                true
            } else {
                let len = self
                    .zlib
                    .deflate(target, &mut self.full)?
                    .checked_add(full_header_len)
                    .ok_or(Error::Capacity("full tree size"))?;
                full_len = Some(len);
                packed_len < len
            };
            if useful {
                record.base = self.base_rank;
                record.delta_size = u32::try_from(self.delta.len())
                    .map_err(|_| Error::Capacity("delta payload"))?;
                builder.add_parts(
                    record,
                    &[
                        take(&header, 0, header_len)?,
                        take(&ofs, 0, ofs_len)?,
                        take(&self.compressed, 0, compressed_len)?,
                    ],
                )?;
                self.members = self.members.saturating_add(1);
                self.stats.deltas = self.stats.deltas.saturating_add(1);
                if let Some(full_len) = full_len {
                    self.stats.bytes_saved = self
                        .stats
                        .bytes_saved
                        .saturating_add(full_len.saturating_sub(packed_len) as u64);
                }
                return Ok(());
            }
        }
        record.base = NO_BASE;
        record.delta_size = 0;
        self.base_rank = builder.records.len() as u32;
        if let Some(raw) = original_full {
            self.base_full_len = raw.len();
            builder.add_parts(record, &[raw])?;
        } else {
            let full_len = match full_len {
                Some(len) => len,
                None => self
                    .zlib
                    .deflate(target, &mut self.full)?
                    .checked_add(full_header_len)
                    .ok_or(Error::Capacity("full tree size"))?,
            };
            self.base_full_len = full_len;
            builder.add_parts(
                record,
                &[
                    take(&full_header, 0, full_header_len)?,
                    take(&self.full, 0, full_len.saturating_sub(full_header_len))?,
                ],
            )?;
        }
        self.base
            .get_mut(..target.len())
            .ok_or(Error::Capacity("base tree"))?
            .copy_from_slice(target);
        self.base_len = target.len();
        self.path = Some(record.path);
        self.members = 1;
        self.stats.full = self.stats.full.saturating_add(1);
        Ok(())
    }
}
