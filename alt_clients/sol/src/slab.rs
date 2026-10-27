use bytes::Bytes;

use crate::arena::{append, push, reserve};
use crate::codec::{
    Kind, Oid, be32, be64, checksum, pack_count, pack_header, take, verify_checksum,
};
use crate::hfs::HfsPointer;
use crate::manifest::{SlabRef, Tier};
use crate::mphf::{BuildScratch, MphfView};
use crate::{Error, Result};

const META_MAGIC: &[u8; 8] = b"GMET\0\0\0\x02";
const HFS_MAGIC: &[u8; 8] = b"GHFS\0\0\0\x01";
pub const RECORD_LEN: usize = 72;
pub const NO_BASE: u32 = u32::MAX;
const TIMES_MAGIC: &[u8; 8] = b"GTIM\0\0\0\x01";
const TIMES_HEADER: usize = 56;
const HEADER: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub objects: usize,
    pub pack_bytes: usize,
    pub object_bytes: usize,
    pub links: usize,
    pub tree_layout: crate::tree::TreeLayout,
    pub cache_bytes: usize,
    pub strict_hfs: bool,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            objects: 1 << 20,
            pack_bytes: 1 << 30,
            object_bytes: 32 << 20,
            links: 1 << 24,
            tree_layout: crate::tree::TreeLayout::Clustered,
            cache_bytes: 256 << 20,
            strict_hfs: false,
        }
    }
}

impl Limits {
    pub fn validate(self) -> Result<Self> {
        if self.objects == 0
            || self.objects >= (1usize << 31)
            || self.pack_bytes < 32
            || self.object_bytes == 0
            || self.object_bytes > u32::MAX as usize
            || self.cache_bytes == 0
            || self.links > u32::MAX as usize
        {
            return Err(Error::Usage("invalid arena limits".into()));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Record {
    pub oid: Oid,
    pub offset: u64,
    pub len: u32,
    pub size: u64,
    pub crc: u32,
    pub kind: Kind,
    pub graph_offset: u64,
    pub pointer: Option<HfsPointer>,
    pub base: u32,
    pub delta_size: u32,
    pub path: u64,
    /// First publication epoch; separate from immutable Git bytes.
    pub epoch: u64,
}

impl Record {
    fn encode(self, out: &mut Vec<u8>) -> Result<()> {
        append(out, &self.oid.0)?;
        append(out, &self.offset.to_be_bytes())?;
        append(out, &self.len.to_be_bytes())?;
        append(out, &self.size.to_be_bytes())?;
        append(out, &self.crc.to_be_bytes())?;
        append(
            out,
            &[self.kind as u8, u8::from(self.pointer.is_some()), 0, 0],
        )?;
        append(out, &self.graph_offset.to_be_bytes())?;
        append(out, &self.base.to_be_bytes())?;
        append(out, &self.delta_size.to_be_bytes())?;
        append(out, &self.path.to_be_bytes())
    }
}

#[derive(Clone, Copy)]
pub struct MetaView<'a> {
    bytes: &'a [u8],
    pub count: u32,
}

impl<'a> MetaView<'a> {
    pub fn parse(bytes: &'a [u8], reference: SlabRef) -> Result<Self> {
        let body = verify_checksum(bytes)?;
        if take(body, 0, 8)? != META_MAGIC
            || Oid::from_bytes(take(body, 12, 20)?)? != reference.id
            || be32(body, 8)? != reference.count
            || body.len()
                != HEADER.saturating_add((reference.count as usize).saturating_mul(RECORD_LEN))
        {
            return Err(Error::Invalid("slab metadata"));
        }
        Ok(Self {
            bytes: body,
            count: reference.count,
        })
    }

    pub fn oid(self, ordinal: u32) -> Result<Oid> {
        if ordinal >= self.count {
            return Err(Error::Invalid("object ordinal"));
        }
        Oid::from_bytes(take(
            self.bytes,
            HEADER.saturating_add((ordinal as usize).saturating_mul(RECORD_LEN)),
            20,
        )?)
    }

    pub fn record(self, ordinal: u32) -> Result<Record> {
        if ordinal >= self.count {
            return Err(Error::Invalid("object ordinal"));
        }
        let at = HEADER.saturating_add((ordinal as usize).saturating_mul(RECORD_LEN));
        let bytes = take(self.bytes, at, RECORD_LEN)?;
        let kind = Kind::parse(*bytes.get(44).ok_or(Error::Invalid("metadata type"))?)?;
        if !kind.full() {
            return Err(Error::Invalid("delta in sealed slab"));
        }
        Ok(Record {
            oid: Oid::from_bytes(take(bytes, 0, 20)?)?,
            offset: be64(bytes, 20)?,
            len: be32(bytes, 28)?,
            size: be64(bytes, 32)?,
            crc: be32(bytes, 40)?,
            kind,
            graph_offset: be64(bytes, 48)?,
            pointer: (*bytes.get(45).ok_or(Error::Invalid("metadata flags"))? != 0)
                .then_some(HfsPointer::default()),
            base: be32(bytes, 56)?,
            delta_size: be32(bytes, 60)?,
            path: be64(bytes, 64)?,
            epoch: 0,
        })
    }
}

#[derive(Clone)]
pub struct Slab {
    pub reference: SlabRef,
    pub pack: Option<Bytes>,
    pub idx: Bytes,
    pub meta: Bytes,
    pub mphf: Bytes,
    pub links: Bytes,
    pub hfs: Bytes,
    pub times: Option<Bytes>,
}

impl Slab {
    pub fn meta_view(&self) -> Result<MetaView<'_>> {
        // Checksums are verified at publication/load, not on every point lookup.
        Ok(MetaView {
            bytes: &self.meta,
            count: self.reference.count,
        })
    }

    pub fn record(&self, rank: u32) -> Result<Record> {
        let mut record = self.meta_view()?.record(rank)?;
        record.epoch = match &self.times {
            Some(times) => be64(
                times,
                TIMES_HEADER.saturating_add((rank as usize).saturating_mul(8)),
            )?,
            None => self.reference.end,
        };
        Ok(record)
    }

    pub fn oid(&self, rank: u32) -> Result<Oid> {
        self.meta_view()?.oid(rank)
    }

    pub fn minimum_epoch(&self) -> Result<u64> {
        match &self.times {
            Some(times) => be64(times, 48),
            None => Ok(self.reference.end),
        }
    }

    pub fn lookup(&self, oid: Oid) -> Result<Option<u32>> {
        let n = self.reference.count as usize;
        // Header/section sizes are validated once; parsing here is fixed-size only.
        let buckets = n.div_ceil(3).max(1);
        let ranks_at = 48usize.saturating_add(buckets.saturating_mul(4));
        let view = MphfView::validated(&self.mphf, n, buckets, ranks_at)?;
        view.lookup(oid, self.meta_view()?)
    }

    pub fn full_children(&self, record: Record) -> Result<&[u8]> {
        if record.graph_offset == crate::graph::NO_GRAPH {
            return Ok(&[]);
        }
        let node = crate::graph::View::validated(&self.links).node(record.graph_offset)?;
        if node.base != NO_BASE || node.payload.len() != node.size {
            return Err(Error::Invalid("non-full structural list"));
        }
        Ok(node.payload)
    }

    pub fn pointer(&self, rank: u32) -> Result<Option<HfsPointer>> {
        let record = self.record(rank)?;
        if record.pointer.is_none() {
            return Ok(None);
        }
        let count = be32(&self.hfs, 8)? as usize;
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            let at = HEADER.saturating_add(mid.saturating_mul(46));
            let ordinal = be32(&self.hfs, at)?;
            match ordinal.cmp(&rank) {
                std::cmp::Ordering::Less => lo = mid.saturating_add(1),
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let bytes = take(&self.hfs, at.saturating_add(4), 42)?;
                    let len = *bytes.first().ok_or(Error::Invalid("HFS sidecar"))?;
                    if !matches!(len, 0 | 32 | 33) {
                        return Err(Error::Invalid("HFS sidecar hash length"));
                    }
                    return Ok(Some(HfsPointer {
                        blob_id: take(bytes, 1, 33)?
                            .try_into()
                            .map_err(|_| Error::Invalid("HFS hash"))?,
                        blob_id_len: len,
                        real_size: be64(bytes, 34)?,
                    }));
                }
            }
        }
        Err(Error::Invalid("missing HFS sidecar record"))
    }

    pub fn raw(&self, record: Record) -> Result<&[u8]> {
        let pack = self.pack.as_ref().ok_or(Error::Invalid("pack is remote"))?;
        take(
            pack,
            usize::try_from(record.offset).map_err(|_| Error::Capacity("pack offset"))?,
            record.len as usize,
        )
    }

    pub fn validate(&self) -> Result<()> {
        if self.reference.times != self.times.is_some() {
            return Err(Error::Invalid("missing object publication epochs"));
        }
        if let Some(times) = &self.times {
            let body = verify_checksum(times)?;
            if take(body, 0, 8)? != TIMES_MAGIC
                || be32(body, 8)? != self.reference.count
                || Oid::from_bytes(take(body, 12, 20)?)? != self.reference.id
                || be64(body, 32)? != self.reference.start
                || be64(body, 40)? != self.reference.end
                || body.len()
                    != TIMES_HEADER
                        .saturating_add((self.reference.count as usize).saturating_mul(8))
            {
                return Err(Error::Invalid("publication epoch sidecar"));
            }
            let mut minimum = u64::MAX;
            for rank in 0..self.reference.count {
                let epoch = be64(
                    body,
                    TIMES_HEADER.saturating_add((rank as usize).saturating_mul(8)),
                )?;
                if epoch == 0 || epoch > self.reference.end {
                    return Err(Error::Invalid("object publication epoch"));
                }
                minimum = minimum.min(epoch);
            }
            if self.reference.count == 0 {
                minimum = 0;
            }
            if minimum != be64(body, 48)? {
                return Err(Error::Invalid("minimum publication epoch"));
            }
        }
        let meta = MetaView::parse(&self.meta, self.reference)?;
        let mphf = MphfView::parse(&self.mphf, self.reference.id, self.reference.count as usize)?;
        let graph = crate::graph::View::parse(&self.links, self.reference.id)?;
        {
            let (bytes, magic) = (&self.hfs, HFS_MAGIC);
            let body = verify_checksum(bytes)?;
            if take(body, 0, 8)? != magic
                || Oid::from_bytes(take(body, 12, 20)?)? != self.reference.id
            {
                return Err(Error::Invalid("sidecar identity"));
            }
            let stride = 46usize;
            if body.len() != HEADER.saturating_add((be32(body, 8)? as usize).saturating_mul(stride))
            {
                return Err(Error::Invalid("sidecar length"));
            }
        }
        let mut offset = 12u64;
        for rank in 0..self.reference.count {
            let record = meta.record(rank)?;
            if record.offset != offset
                || record.len == 0
                || mphf.lookup(record.oid, meta)? != Some(rank)
            {
                return Err(Error::Invalid(
                    "metadata location, adjacency, or perfect hash",
                ));
            }
            if record.graph_offset != crate::graph::NO_GRAPH {
                let node = graph.node(record.graph_offset)?;
                if node.size as u64 > record.size || record.kind == Kind::Blob {
                    return Err(Error::Invalid("structural list size or type"));
                }
                if node.base != NO_BASE {
                    let base = meta.record(node.base)?;
                    let parent = graph.node(base.graph_offset)?;
                    if record.kind != Kind::Tree
                        || base.kind != Kind::Tree
                        || base.path != record.path
                        || node.base >= rank
                        || parent.base != NO_BASE
                        || parent.size != parent.payload.len()
                    {
                        return Err(Error::Invalid("structural delta dependency"));
                    }
                } else if node.size != node.payload.len() {
                    return Err(Error::Invalid("structural full list"));
                }
            } else if matches!(record.kind, Kind::Commit | Kind::Tag) {
                return Err(Error::Invalid("missing commit/tag adjacency"));
            }
            if record.base != NO_BASE {
                let base = meta.record(record.base)?;
                if record.kind != Kind::Tree
                    || record.base >= rank
                    || base.kind != Kind::Tree
                    || base.base != NO_BASE
                    || rank.saturating_sub(record.base) > 15
                    || base.path != record.path
                    || record.delta_size == 0
                {
                    return Err(Error::Invalid("tree cluster dependency"));
                }
            } else if record.delta_size != 0 {
                return Err(Error::Invalid("full entry with delta size"));
            }
            offset = offset
                .checked_add(u64::from(record.len))
                .ok_or(Error::Invalid("pack offset overflow"))?;
            if record.pointer.is_some() {
                self.pointer(rank)?;
            }
        }
        if offset.checked_add(20) != Some(self.reference.pack_len) {
            return Err(Error::Invalid("metadata pack length"));
        }
        verify_checksum(&self.idx)?;
        if let Some(pack) = &self.pack {
            let body = verify_checksum(pack)?;
            if pack.len() as u64 != self.reference.pack_len
                || pack_count(body)? != self.reference.count
                || Oid::from_bytes(take(pack, body.len(), 20)?)? != self.reference.id
            {
                return Err(Error::Invalid("sealed pack identity"));
            }
        }
        Ok(())
    }
}

pub enum PackArena {
    Heap(Vec<u8>),
    File(gvfs_platform::FileArena),
}

impl PackArena {
    pub fn len(&self) -> usize {
        match self {
            Self::Heap(bytes) => bytes.len(),
            Self::File(bytes) => bytes.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn capacity(&self) -> usize {
        match self {
            Self::Heap(bytes) => bytes.capacity(),
            Self::File(bytes) => bytes.capacity(),
        }
    }
    pub fn clear(&mut self) {
        match self {
            Self::Heap(bytes) => bytes.clear(),
            Self::File(bytes) => bytes.clear(),
        }
    }

    pub fn truncate(&mut self, len: usize) -> Result<()> {
        if len > self.len() {
            return Err(Error::Invalid("arena truncate"));
        }
        match self {
            Self::Heap(bytes) => bytes.truncate(len),
            Self::File(bytes) => bytes.truncate(len)?,
        }
        Ok(())
    }
    pub fn append(&mut self, input: &[u8]) -> Result<()> {
        match self {
            Self::Heap(bytes) => append(bytes, input),
            Self::File(bytes) => {
                bytes.append(input)?;
                Ok(())
            }
        }
    }
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Heap(bytes) => bytes,
            Self::File(bytes) => bytes.as_mut_slice(),
        }
    }
    pub fn freeze(self) -> Result<Bytes> {
        match self {
            Self::Heap(bytes) => Ok(Bytes::from(bytes)),
            Self::File(bytes) => Ok(Bytes::from_owner(bytes.freeze()?)),
        }
    }
}

impl AsRef<[u8]> for PackArena {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Heap(bytes) => bytes,
            Self::File(bytes) => bytes.as_ref(),
        }
    }
}

pub struct Builder {
    pub pack: PackArena,
    pub records: Vec<Record>,
    pub(crate) raw_graph: crate::graph::RawGraph,
    graph_encoder: crate::graph::Encoder,
    index: BuildScratch,
    order: Vec<u32>,
    meta: Vec<u8>,
    mphf: Vec<u8>,
    idx: Vec<u8>,
    hfs: Vec<u8>,
    times: Vec<u8>,
}

fn sized(base: usize, count: usize, stride: usize) -> Result<usize> {
    base.checked_add(
        count
            .checked_mul(stride)
            .ok_or(Error::Capacity("slab index size"))?,
    )
    .ok_or(Error::Capacity("slab index size"))
}

impl Builder {
    pub fn new(limits: Limits) -> Result<Self> {
        Self::with_pack(limits, PackArena::Heap(reserve(limits.pack_bytes)?), None)
    }

    pub fn mapped(limits: Limits, directory: &std::path::Path) -> Result<Self> {
        Self::with_pack(
            limits,
            PackArena::File(gvfs_platform::FileArena::new(directory, limits.pack_bytes)?),
            Some(directory),
        )
    }

    fn with_pack(
        limits: Limits,
        pack: PackArena,
        directory: Option<&std::path::Path>,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let mut value = Self {
            pack,
            records: reserve(limits.objects)?,
            raw_graph: crate::graph::RawGraph::new(limits, directory)?,
            graph_encoder: crate::graph::Encoder::new(limits, directory)?,
            index: BuildScratch::new(limits.objects)?,
            order: reserve(limits.objects)?,
            meta: reserve(sized(52, limits.objects, RECORD_LEN)?)?,
            mphf: reserve(sized(72, limits.objects, 14)?)?,
            idx: reserve(sized(1072, limits.objects, 36)?)?,
            hfs: reserve(sized(52, limits.objects, 46)?)?,
            times: reserve(sized(76, limits.objects, 8)?)?,
        };
        value.reset()?;
        Ok(value)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.pack.clear();
        self.records.clear();
        self.raw_graph.reset();
        self.pack.append(&pack_header(0))
    }

    pub fn add(&mut self, record: Record, raw: &[u8]) -> Result<()> {
        self.add_parts(record, &[raw])
    }

    pub fn add_parts(&mut self, mut record: Record, parts: &[&[u8]]) -> Result<()> {
        let len = parts.iter().try_fold(0usize, |sum, bytes| {
            sum.checked_add(bytes.len())
                .ok_or(Error::Capacity("entry bytes"))
        })?;
        if self.records.len() == self.records.capacity()
            || self
                .pack
                .capacity()
                .saturating_sub(self.pack.len())
                .saturating_sub(20)
                < len
        {
            return Err(Error::Capacity("slab builder"));
        }
        record.offset = self.pack.len() as u64;
        record.len = u32::try_from(len).map_err(|_| Error::Capacity("entry length"))?;
        let mut crc = crc32fast::Hasher::new();
        for bytes in parts {
            crc.update(bytes);
            self.pack.append(bytes)?;
        }
        record.crc = crc.finalize();
        push(&mut self.records, record)
    }

    pub fn set_graph(&mut self, record: &mut Record, children: &[u8]) -> Result<()> {
        record.graph_offset = self.raw_graph.append(children)?;
        Ok(())
    }

    pub fn finish(mut self, tier: Tier, start: u64, end: u64, tip: Oid) -> Result<Slab> {
        let count = u32::try_from(self.records.len()).map_err(|_| Error::Capacity("pack count"))?;
        self.pack
            .as_mut_slice()
            .get_mut(..12)
            .ok_or(Error::Invalid("pack header"))?
            .copy_from_slice(&pack_header(count));
        let id = Oid(checksum(self.pack.as_ref()));
        self.pack.append(&id.0)?;
        let with_times = tier != Tier::Baseline;
        if with_times {
            for record in &mut self.records {
                if record.epoch == 0 {
                    record.epoch = end;
                }
            }
            let minimum = self
                .records
                .iter()
                .map(|record| record.epoch)
                .min()
                .unwrap_or(0);
            append(&mut self.times, TIMES_MAGIC)?;
            append(&mut self.times, &count.to_be_bytes())?;
            append(&mut self.times, &id.0)?;
            append(&mut self.times, &start.to_be_bytes())?;
            append(&mut self.times, &end.to_be_bytes())?;
            append(&mut self.times, &minimum.to_be_bytes())?;
            for record in &self.records {
                append(&mut self.times, &record.epoch.to_be_bytes())?;
            }
            let sum = checksum(&self.times);
            append(&mut self.times, &sum)?;
        }
        let graph_hot = gvfs_platform::HotRegion::enter();
        let links =
            self.graph_encoder
                .encode(&mut self.records, &self.raw_graph, &mut self.order, id)?;
        let graph_allocations = graph_hot.allocations();
        drop(graph_hot);
        if graph_allocations != 0 {
            return Err(Error::Invalid("graph sealing allocated after warmup"));
        }
        let links = links.freeze()?;
        drop(self.raw_graph);
        for (out, magic, n) in [
            (&mut self.meta, META_MAGIC, count),
            (
                &mut self.hfs,
                HFS_MAGIC,
                self.records
                    .iter()
                    .filter(|record| record.pointer.is_some())
                    .count() as u32,
            ),
        ] {
            out.clear();
            append(out, magic)?;
            append(out, &n.to_be_bytes())?;
            append(out, &id.0)?;
        }
        for (rank, record) in self.records.iter().enumerate() {
            record.encode(&mut self.meta)?;
            if let Some(pointer) = record.pointer {
                append(&mut self.hfs, &(rank as u32).to_be_bytes())?;
                append(&mut self.hfs, &[pointer.blob_id_len])?;
                append(&mut self.hfs, &pointer.blob_id)?;
                append(&mut self.hfs, &pointer.real_size.to_be_bytes())?;
            }
        }
        for bytes in [&mut self.meta, &mut self.hfs] {
            let sum = checksum(bytes);
            append(bytes, &sum)?;
        }
        self.index.encode(&self.records, id, &mut self.mphf)?;
        write_idx(&self.records, id, &mut self.order, &mut self.idx)?;
        let reference = SlabRef {
            id,
            tier,
            start,
            end,
            tip,
            count,
            pack_len: self.pack.len() as u64,
            times: with_times,
        };
        Ok(Slab {
            reference,
            pack: Some(self.pack.freeze()?),
            idx: Bytes::from(self.idx),
            meta: Bytes::from(self.meta),
            mphf: Bytes::from(self.mphf),
            links,
            hfs: Bytes::from(self.hfs),
            times: if with_times {
                Some(Bytes::from(self.times))
            } else {
                None
            },
        })
    }
}

pub fn write_idx(
    records: &[Record],
    pack_id: Oid,
    order: &mut Vec<u32>,
    out: &mut Vec<u8>,
) -> Result<()> {
    order.clear();
    for rank in 0..records.len() {
        push(order, rank as u32)?;
    }
    order.sort_unstable_by_key(|rank| records.get(*rank as usize).map(|record| record.oid));
    crate::idx::write(
        order.len(),
        |i| {
            let rank = *order.get(i).ok_or(Error::Invalid("idx order"))?;
            let record = records
                .get(rank as usize)
                .ok_or(Error::Invalid("idx ordinal"))?;
            Ok(crate::idx::Entry {
                oid: record.oid,
                crc: record.crc,
                offset: record.offset,
            })
        },
        pack_id,
        out,
    )
}
