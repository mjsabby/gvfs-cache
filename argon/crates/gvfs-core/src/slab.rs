//! Unified `Slab`, `HttpRangeBatch`, HTTP Range coalescing, and Zero-CPU Pack Splicing
//! (PDF pp. 148–162).
//!
//! Crucially, every splice operation (`plan_deduplicated_range_splice` + `assemble_spliced_pack`)
//! deduplicates `Oid20`s across slabs and emits **1 single valid Git `.pack` (and `.idx` v2)**
//! so clients never accumulate multiple packfiles per fetch.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use sha1::{Digest, Sha1};

use crate::arena::{OidIdxMap, SeenOidSet};
use crate::buf::{ByteWriter, Overflow, be32, be64, prefault_capacity, sub};
use crate::hfs::{HFS_SIDECAR_MAGIC, HFS_SIDECAR_RECORD_LEN, HfsBlobRecord};
use crate::idx::{IdxEntry, IdxView, idx_len, write_idx_v2};
use crate::mphf::MphfSlabIndex;
use crate::oid::{Oid20, ZERO_OID};
use crate::pack::{
    DeltaBase, ObjType, PACK_HEADER_LEN, PACK_TRAILER_LEN, encode_entry_header,
    encode_ofs_distance, pack_header, parse_entry_header,
};
use crate::rev::RevView;
use crate::zlib::{Deflater, Inflater};

/// Compaction and serving tiers (PDF p. 156).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum Tier {
    L0Ram = 0,         // < 15m live delta in RAM
    M15 = 15,          // 15-minute slab
    H1 = 60,           // 1-hour rollup
    H6 = 360,          // 6-hour rollup
    H12 = 720,         // 12-hour rollup
    H24 = 1440,        // 24-hour daily rollup
    H168Delta = 10080, // Weekly "Aha" Delta (~80 MiB, warm workstations)
    H168Base = 10081,  // Weekly Full Baseline (~4 GiB, cold clones)
}

impl Tier {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "l0" | "l0ram" | "ram" => Some(Self::L0Ram),
            "15m" | "m15" => Some(Self::M15),
            "1h" | "h1" => Some(Self::H1),
            "6h" | "h6" => Some(Self::H6),
            "12h" | "h12" => Some(Self::H12),
            "24h" | "h24" | "daily" => Some(Self::H24),
            "168h-delta" | "h168-delta" | "h168delta" | "weekly-delta" => Some(Self::H168Delta),
            "168h" | "h168" | "168h-base" | "h168-base" | "h168base" | "baseline" => {
                Some(Self::H168Base)
            }
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L0Ram => "l0ram",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H6 => "6h",
            Self::H12 => "12h",
            Self::H24 => "24h",
            Self::H168Delta => "168h-delta",
            Self::H168Base => "168h-base",
        }
    }

    pub const fn code(self) -> u16 {
        self as u16
    }

    pub const fn from_code(code: u16) -> Option<Self> {
        match code {
            0 => Some(Self::L0Ram),
            15 => Some(Self::M15),
            60 => Some(Self::H1),
            360 => Some(Self::H6),
            720 => Some(Self::H12),
            1440 => Some(Self::H24),
            10080 => Some(Self::H168Delta),
            10081 => Some(Self::H168Base),
            _ => None,
        }
    }

    pub const fn window_secs(self) -> u64 {
        match self {
            Self::L0Ram | Self::M15 => 900,
            Self::H1 => 3_600,
            Self::H6 => 21_600,
            Self::H12 => 43_200,
            Self::H24 => 86_400,
            Self::H168Delta | Self::H168Base => 604_800,
        }
    }

    /// All persisted rollup slabs (`15m` through `168h`) pack `OBJ_TREE` directories into
    /// contiguous depth-1 reverse-chronological clusters (`[Base Tree + <=15 OFS_DELTAs]`).
    /// Only `< 15m` `L0Ram` micro-deltas skip clustering (or use standalone trees for 2s ticks).
    #[inline]
    pub const fn uses_tree_deltas(self) -> bool {
        !matches!(self, Self::L0Ram)
    }
}

impl core::fmt::Display for Tier {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every object in a slab is recorded in Merkle DFS order with its exact pre-compressed
/// byte slice `[offset .. offset + compressed_len]` inside `.pack` (PDF p. 157).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ObjSliceMeta {
    pub oid: Oid20,
    pub offset: u64,
    pub compressed_len: u32,
    pub crc32: u32,
}

pub const META_RECORD_LEN: usize = 32;
pub const META_MAGIC: &[u8; 8] = b"GMET\x00\x00\x00\x01";

impl ObjSliceMeta {
    pub fn encode(&self, out: &mut [u8; META_RECORD_LEN]) -> Result<(), Overflow> {
        let mut w = ByteWriter::new(out);
        w.put(&self.oid.0)?;
        w.put_u64_be(self.offset)?;
        w.put_u32_be(self.compressed_len)?;
        Ok(())
    }

    pub fn encode_with_crc(&self, out: &mut [u8; 36]) -> Result<(), Overflow> {
        let mut w = ByteWriter::new(out);
        w.put(&self.oid.0)?;
        w.put_u64_be(self.offset)?;
        w.put_u32_be(self.compressed_len)?;
        w.put_u32_be(self.crc32)?;
        Ok(())
    }

    pub fn decode_with_crc(raw: &[u8]) -> Option<Self> {
        if raw.len() < 36 {
            return None;
        }
        let oid = Oid20::from_slice(sub(raw, 0, 20)?)?;
        let offset = be64(raw, 20)?;
        let compressed_len = be32(raw, 28)?;
        let crc32 = be32(raw, 32)?;
        Some(Self {
            oid,
            offset,
            compressed_len,
            crc32,
        })
    }
}

/// Zero-copy memory-mapped bundle for an existing repository packfile (`.pack` + `.idx` + `.rev`),
/// such as `/home/muks/SearchGold/.git/objects/pack/pack-*.pack`.
#[derive(Debug, Clone)]
pub struct MmapPackBundle {
    pub pack: Bytes,
    pub idx: Bytes,
    pub rev: Option<Bytes>,
    pub object_count: u32,
}

/// Unified Slab representation: identical whether it lives in S3/Azure Blob (4 GiB),
/// memory-mapped from local disk, or in RAM for the `< 15m` live window (PDF p. 157).
#[derive(Debug, Clone)]
pub struct Slab {
    pub id: String,
    pub tier: Tier,
    pub start_ts: u64,
    pub end_ts: u64,
    pub have_commit: Oid20,
    pub tip_commit: Oid20,
    /// Ordered list of objects in Merkle DFS order.
    pub objects: Vec<ObjSliceMeta>,
    /// Minimal Perfect Hash Function index for $O(1)$ ~15ns point lookups.
    pub mphf: MphfSlabIndex,
    /// Sorted `(offset -> index in objects)` lookup table so an MPHF `(offset, len)` hit
    /// can verify the full 20-byte `Oid20` in $O(1)$ (because `objects` is in ascending
    /// pack `offset` order!).
    pub hfs_sidecar: Vec<HfsBlobRecord>,
    /// Pre-allocated or memory-mapped `.pack` bytes, if resident locally.
    pub in_memory_pack: Option<Bytes>,
    /// Pre-allocated or memory-mapped `.idx` v2 bytes, if resident locally.
    pub in_memory_idx: Option<Bytes>,
    /// Optional direct mmap bundle for pre-existing multi-GB git packs with `.idx` + `.rev`.
    pub mmap_bundle: Option<MmapPackBundle>,
    /// Total byte length of the `.pack` stream.
    pub pack_byte_len: u64,
}

impl Slab {
    /// Builds a complete `Slab` (including `.mphf` and `.idx` v2) from Merkle-DFS ordered
    /// `objects` and a `.pack` buffer.
    #[allow(clippy::too_many_arguments)]
    pub fn from_pack_and_objects(
        id: String,
        tier: Tier,
        start_ts: u64,
        end_ts: u64,
        have_commit: Oid20,
        tip_commit: Oid20,
        objects: Vec<ObjSliceMeta>,
        hfs_sidecar: Vec<HfsBlobRecord>,
        pack_bytes: Bytes,
    ) -> Option<Self> {
        let mphf_items: Vec<(Oid20, u64, u32)> = objects
            .iter()
            .map(|o| (o.oid, o.offset, o.compressed_len))
            .collect();
        let mphf = MphfSlabIndex::build(&mphf_items)?;

        let pack_checksum: [u8; 20] = if pack_bytes.len() >= PACK_TRAILER_LEN {
            let tail = pack_bytes.get(pack_bytes.len().saturating_sub(PACK_TRAILER_LEN)..)?;
            tail.try_into().ok()?
        } else {
            [0u8; 20]
        };

        let mut idx_entries: Vec<IdxEntry> = objects
            .iter()
            .map(|o| {
                let crc = if o.crc32 != 0 {
                    o.crc32
                } else {
                    let start = usize::try_from(o.offset).unwrap_or(0);
                    let end = start.saturating_add(usize::try_from(o.compressed_len).unwrap_or(0));
                    pack_bytes.get(start..end).map_or(0, crc32fast::hash)
                };
                IdxEntry {
                    oid: o.oid,
                    crc32: crc,
                    offset: o.offset,
                }
            })
            .collect();

        let n_large = idx_entries
            .iter()
            .filter(|e| e.offset > 0x7FFF_FFFF)
            .count();
        let idx_total = idx_len(idx_entries.len(), n_large)?;
        let mut idx_buf = vec![0u8; idx_total];
        write_idx_v2(&mut idx_entries, &pack_checksum, &mut idx_buf).ok()?;
        let pack_byte_len = u64::try_from(pack_bytes.len()).unwrap_or(0);
        let pack_bytes = crate::buf::share_bytes(pack_bytes);
        let idx_bytes = crate::buf::share_bytes(Bytes::from(idx_buf));

        Some(Self {
            id,
            tier,
            start_ts,
            end_ts,
            have_commit,
            tip_commit,
            objects,
            mphf,
            hfs_sidecar,
            in_memory_pack: Some(pack_bytes),
            in_memory_idx: Some(idx_bytes),
            mmap_bundle: None,
            pack_byte_len,
        })
    }

    /// Creates a zero-copy `Slab` backed directly by memory-mapped `.pack`, `.idx`, and `.rev`
    /// files (such as `/home/muks/SearchGold/.git/objects/pack/pack-*.pack`).
    #[allow(clippy::too_many_arguments)]
    pub fn from_mmap_pack_bundle(
        id: String,
        tier: Tier,
        start_ts: u64,
        end_ts: u64,
        tip_commit: Oid20,
        pack: Bytes,
        idx: Bytes,
        rev: Option<Bytes>,
    ) -> Option<Self> {
        let idx_view = IdxView::parse(&idx).ok()?;
        let object_count = u32::try_from(idx_view.len()).ok()?;
        let pack_byte_len = u64::try_from(pack.len()).ok()?;
        Some(Self {
            id,
            tier,
            start_ts,
            end_ts,
            have_commit: ZERO_OID,
            tip_commit,
            objects: Vec::new(),
            mphf: MphfSlabIndex::empty(),
            hfs_sidecar: Vec::new(),
            in_memory_pack: Some(pack.clone()),
            in_memory_idx: Some(idx.clone()),
            mmap_bundle: Some(MmapPackBundle {
                pack,
                idx,
                rev,
                object_count,
            }),
            pack_byte_len,
        })
    }

    #[inline]
    pub fn object_count(&self) -> u32 {
        if let Some(ref b) = self.mmap_bundle {
            b.object_count
        } else {
            u32::try_from(self.objects.len()).unwrap_or(u32::MAX)
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.object_count() == 0
    }

    /// Looks up `oid` in this slab in $O(1)$ via `MphfSlabIndex` (confirming against `objects`
    /// to eliminate the 0.0015% 16-bit fingerprint collision rate), or via `.idx` + `.rev`
    /// when backed by a direct `MmapPackBundle`.
    #[inline]
    pub fn lookup_oid(&self, oid: &Oid20) -> Option<ObjSliceMeta> {
        if !self.objects.is_empty() {
            let (offset, compressed_len) = self.mphf.lookup_resolved(oid)?;
            // `self.objects` is in ascending pack `offset` order: verify the exact OID!
            if let Ok(pos) = self.objects.binary_search_by_key(&offset, |o| o.offset) {
                let meta = *self.objects.get(pos)?;
                if meta.oid == *oid && meta.compressed_len == compressed_len {
                    return Some(meta);
                }
            }
            return None;
        }
        if let Some(ref bundle) = self.mmap_bundle {
            let idx_view = IdxView::parse(&bundle.idx).ok()?;
            if let Some(ref rev_bytes) = bundle.rev {
                let rev_view = RevView::parse(rev_bytes, idx_view.len())?;
                let (offset, compressed_len, crc32) =
                    rev_view.lookup_oid(oid, &idx_view, self.pack_byte_len)?;
                return Some(ObjSliceMeta {
                    oid: *oid,
                    offset,
                    compressed_len,
                    crc32,
                });
            }
            // Fallback if `.rev` is absent: scan entry header + zlib stream from `idx.offset`.
            let idx_pos = idx_view.find(oid)?;
            let offset = idx_view.offset(idx_pos)?;
            let crc32 = idx_view.crc32(idx_pos)?;
            let start = usize::try_from(offset).ok()?;
            let tail = bundle.pack.get(start..)?;
            let hdr = parse_entry_header(tail).ok().flatten()?;
            let mut inf = crate::zlib::Inflater::new();
            let scratch_len = usize::try_from(hdr.size).unwrap_or(0).min(16 << 20);
            let mut scratch = vec![0u8; scratch_len];
            let z_consumed = inf
                .inflate_exact(tail.get(hdr.header_len..)?, &mut scratch, scratch_len)
                .ok()?;
            let compressed_len = u32::try_from(hdr.header_len.checked_add(z_consumed)?).ok()?;
            return Some(ObjSliceMeta {
                oid: *oid,
                offset,
                compressed_len,
                crc32,
            });
        }
        None
    }

    /// Returns the uncompressed Git object size for `/gvfs/sizes` without allocating on the heap.
    pub fn lookup_object_size(&self, oid: &Oid20) -> Option<u64> {
        let meta = self.lookup_oid(oid)?;
        let pack = self.in_memory_pack.as_ref()?;
        let start = usize::try_from(meta.offset).ok()?;
        let end = start.checked_add(usize::try_from(meta.compressed_len).ok()?)?;
        let raw_entry = pack.get(start..end)?;
        let hdr = parse_entry_header(raw_entry).ok().flatten()?;
        if !hdr.kind.is_delta() {
            return Some(hdr.size);
        }
        // If backed by an un-rewritten raw mmap'd git pack where this entry is a delta,
        // the first two varints of the zlib stream are `(base_size, result_size)`.
        let z_slice = raw_entry.get(hdr.header_len..)?;
        let mut inf = crate::zlib::Inflater::new();
        let mut prefix = [0u8; 32];
        let _ = inf.step(z_slice, &mut prefix).ok()?;
        let (_, result_size, _) = crate::delta::delta_sizes(&prefix).ok()?;
        Some(result_size)
    }

    /// Inflates an object from this slab into `out_buf`, transparently resolving a depth-1
    /// `OBJ_OFS_DELTA` against its contiguous cluster `Base Tree` (using `base_buf` and
    /// `delta_buf` without allocating on the heap).
    pub fn inflate_object_into(
        &self,
        meta: &ObjSliceMeta,
        inflater: &mut Inflater,
        base_buf: &mut [u8],
        delta_buf: &mut [u8],
        out_buf: &mut [u8],
    ) -> Option<(ObjType, usize)> {
        let pack = self.in_memory_pack.as_ref()?;
        let start = usize::try_from(meta.offset).ok()?;
        let end = start.checked_add(usize::try_from(meta.compressed_len).ok()?)?;
        let raw_entry = pack.get(start..end)?;
        let hdr = parse_entry_header(raw_entry).ok().flatten()?;
        if !hdr.kind.is_delta() {
            let expected = usize::try_from(hdr.size).ok()?;
            let z = raw_entry.get(hdr.header_len..)?;
            inflater.inflate_exact(z, out_buf, expected).ok()?;
            return Some((hdr.kind, expected));
        }
        let DeltaBase::Ofs(ofs) = hdr.base else {
            return None;
        };
        let base_off = meta.offset.checked_sub(ofs)?;
        let base_pos = self
            .objects
            .binary_search_by_key(&base_off, |o| o.offset)
            .ok()?;
        let base_meta = self.objects.get(base_pos)?;
        let b_start = usize::try_from(base_meta.offset).ok()?;
        let b_end = b_start.checked_add(usize::try_from(base_meta.compressed_len).ok()?)?;
        let b_raw = pack.get(b_start..b_end)?;
        let b_hdr = parse_entry_header(b_raw).ok().flatten()?;
        if b_hdr.kind.is_delta() {
            return None;
        }
        let b_size = usize::try_from(b_hdr.size).ok()?;
        inflater
            .inflate_exact(b_raw.get(b_hdr.header_len..)?, base_buf, b_size)
            .ok()?;

        let d_size = usize::try_from(hdr.size).ok()?;
        inflater
            .inflate_exact(raw_entry.get(hdr.header_len..)?, delta_buf, d_size)
            .ok()?;

        let resolved_len =
            crate::delta::apply_delta(base_buf.get(..b_size)?, delta_buf.get(..d_size)?, out_buf)
                .ok()?;
        Some((b_hdr.kind, resolved_len))
    }

    /// Serializes `objects` metadata (`slab-XXXX.meta`).
    pub fn encode_objects_meta(&self) -> Option<Vec<u8>> {
        let total = 12usize.checked_add(self.objects.len().checked_mul(36)?)?;
        let mut out = vec![0u8; total];
        let mut w = ByteWriter::new(&mut out);
        w.put(META_MAGIC).ok()?;
        w.put_u32_be(u32::try_from(self.objects.len()).ok()?).ok()?;
        let mut rec = [0u8; 36];
        for o in &self.objects {
            o.encode_with_crc(&mut rec).ok()?;
            w.put(&rec).ok()?;
        }
        Some(out)
    }

    pub fn decode_objects_meta(data: &[u8]) -> Option<Vec<ObjSliceMeta>> {
        if data.get(..8) != Some(&META_MAGIC[..]) {
            return None;
        }
        let count = usize::try_from(be32(data, 8)?).ok()?;
        let expected = 12usize.checked_add(count.checked_mul(36)?)?;
        if data.len() != expected {
            return None;
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let at = 12usize.checked_add(i.checked_mul(36)?)?;
            out.push(ObjSliceMeta::decode_with_crc(sub(data, at, 36)?)?);
        }
        Some(out)
    }

    /// Serializes `hfs_sidecar` (`slab-XXXX.hfs`).
    pub fn encode_hfs_sidecar(&self) -> Option<Vec<u8>> {
        let total =
            12usize.checked_add(self.hfs_sidecar.len().checked_mul(HFS_SIDECAR_RECORD_LEN)?)?;
        let mut out = vec![0u8; total];
        let mut w = ByteWriter::new(&mut out);
        w.put(HFS_SIDECAR_MAGIC).ok()?;
        w.put_u32_be(u32::try_from(self.hfs_sidecar.len()).ok()?)
            .ok()?;
        let mut rec = [0u8; HFS_SIDECAR_RECORD_LEN];
        for h in &self.hfs_sidecar {
            h.encode(&mut rec).ok()?;
            w.put(&rec).ok()?;
        }
        Some(out)
    }

    pub fn decode_hfs_sidecar(data: &[u8]) -> Option<Vec<HfsBlobRecord>> {
        if data.get(..8) != Some(&HFS_SIDECAR_MAGIC[..]) {
            return None;
        }
        let count = usize::try_from(be32(data, 8)?).ok()?;
        let expected = 12usize.checked_add(count.checked_mul(HFS_SIDECAR_RECORD_LEN)?)?;
        if data.len() != expected {
            return None;
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let at = 12usize.checked_add(i.checked_mul(HFS_SIDECAR_RECORD_LEN)?)?;
            out.push(HfsBlobRecord::decode(sub(
                data,
                at,
                HFS_SIDECAR_RECORD_LEN,
            )?)?);
        }
        Some(out)
    }
}

/// Coalesced HTTP `Range: bytes=start-end` request against an S3/Azure Blob/disk `.pack` slab
/// (PDF p. 157).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRangeBatch {
    pub slab_id: String,
    pub slab_index: usize,
    pub range_start: u64,
    pub range_end: u64,
    /// Relative sub-slices `(rel_start, rel_end)` within `[range_start .. range_end]`
    /// for each unique object.
    pub sub_slices: Vec<(usize, usize)>,
    /// Corresponding object metadata `(oid, crc32)` for `.idx` / `.mphf` construction.
    pub objects: Vec<(Oid20, u32)>,
}

/// Pre-allocated arena for zero-allocation range coalescing & pack splicing on the hot path.
#[derive(Debug)]
pub struct SpliceArena {
    pub seen_oids: SeenOidSet,
    pub out_offsets: OidIdxMap,
    pub matched_per_slab: Vec<ObjSliceMeta>,
    pub idx_entries: Vec<IdxEntry>,
    pub out_pack: BytesMut,
    pub out_idx: BytesMut,
    pack_cap: usize,
    idx_cap: usize,
    pub inflater: Inflater,
    pub deflater: Deflater,
    pub base_scratch: Vec<u8>,
    pub delta_scratch: Vec<u8>,
    pub resolved_scratch: Vec<u8>,
    pub z_scratch: Vec<u8>,
}

impl SpliceArena {
    pub fn new(oid_log2: u32, max_objects: usize, pack_cap: usize, idx_cap: usize) -> Self {
        let seen_oids = SeenOidSet::with_capacity_log2(oid_log2);
        let mut matched_per_slab = Vec::with_capacity(max_objects);
        prefault_capacity(&mut matched_per_slab, ObjSliceMeta::default());
        let mut idx_entries = Vec::with_capacity(max_objects);
        prefault_capacity(
            &mut idx_entries,
            IdxEntry {
                oid: ZERO_OID,
                crc32: 0,
                offset: 0,
            },
        );

        let mut out_pack = BytesMut::with_capacity(pack_cap);
        out_pack.resize(pack_cap, 0);
        out_pack.clear();
        drop(out_pack.split_to(0));

        let mut out_idx = BytesMut::with_capacity(idx_cap);
        out_idx.resize(idx_cap, 0);
        out_idx.clear();
        drop(out_idx.split_to(0));

        let scratch_sz = 1 << 20;
        let mut base_scratch = vec![0u8; scratch_sz];
        crate::buf::prefault(&mut base_scratch);
        let mut delta_scratch = vec![0u8; scratch_sz];
        crate::buf::prefault(&mut delta_scratch);
        let mut resolved_scratch = vec![0u8; scratch_sz];
        crate::buf::prefault(&mut resolved_scratch);
        let mut z_scratch = vec![0u8; crate::zlib::compress_bound(scratch_sz)];
        crate::buf::prefault(&mut z_scratch);

        Self {
            seen_oids,
            out_offsets: OidIdxMap::with_capacity_log2(oid_log2),
            matched_per_slab,
            idx_entries,
            out_pack,
            out_idx,
            pack_cap,
            idx_cap,
            inflater: Inflater::new(),
            deflater: Deflater::new(1),
            base_scratch,
            delta_scratch,
            resolved_scratch,
            z_scratch,
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.seen_oids.clear();
        self.out_offsets.clear();
        self.matched_per_slab.clear();
        self.idx_entries.clear();
        self.out_pack.clear();
        let _ = self.out_pack.try_reclaim(self.pack_cap);
        self.out_idx.clear();
        let _ = self.out_idx.try_reclaim(self.idx_cap);
    }

    /// Zero-allocation splicer for in-memory / mmap'd slabs when serving `POST /gvfs/objects`
    /// or `GET /gvfs/prefetch` on the hot path!
    ///
    /// Returns `(spliced_pack_bytes, spliced_idx_bytes, unique_object_count)`.
    pub fn splice_local_slabs(
        &mut self,
        slabs: &[Arc<Slab>],
        filter_oids: Option<&[Oid20]>,
    ) -> Option<(Bytes, Bytes, u32)> {
        self.seen_oids.clear();
        self.out_offsets.clear();
        self.idx_entries.clear();
        self.out_pack.clear();
        if self.out_pack.capacity() < self.pack_cap {
            let _ = self.out_pack.try_reclaim(self.pack_cap);
        }
        self.out_idx.clear();
        if self.out_idx.capacity() < self.idx_cap {
            let _ = self.out_idx.try_reclaim(self.idx_cap);
        }

        // Fast-path: if `filter_oids` is `None` and there is exactly 1 slab whose `.pack` and
        // `.idx` are already built in memory or mmap'd, return that 1 pack directly with
        // ZERO copying and ZERO SHA-1 recomputation!
        if filter_oids.is_none() {
            if let [single] = slabs {
                if let (Some(pack), Some(idx)) = (&single.in_memory_pack, &single.in_memory_idx) {
                    return Some((pack.clone(), idx.clone(), single.object_count()));
                }
            }
        }

        // Reserve 12 bytes for the `PACK` v2 header at the start of `out_pack`.
        if self.out_pack.capacity() < PACK_HEADER_LEN.saturating_add(PACK_TRAILER_LEN) {
            return None;
        }
        self.out_pack.extend_from_slice(&[0u8; PACK_HEADER_LEN]);

        let mut unique_count: u32 = 0;

        if let Some(requested) = filter_oids {
            for (slab_idx, slab) in slabs.iter().enumerate() {
                let _ = slab_idx;
                self.matched_per_slab.clear();
                for oid in requested {
                    if self.seen_oids.contains(oid) {
                        continue;
                    }
                    if let Some(meta) = slab.lookup_oid(oid) {
                        if self.seen_oids.insert(*oid) == Some(true)
                            && self.matched_per_slab.len() < self.matched_per_slab.capacity()
                        {
                            self.matched_per_slab.push(meta);
                        }
                    }
                }
                // Sort matched entries by offset inside this slab for sequential memory access.
                self.matched_per_slab.sort_unstable_by_key(|m| m.offset);
                for m_idx in 0..self.matched_per_slab.len() {
                    let meta = *self.matched_per_slab.get(m_idx)?;
                    if !self.append_slab_entry_to_out_pack(slab, &meta) {
                        return None;
                    }
                    unique_count = unique_count.saturating_add(1);
                }
            }
        } else {
            for slab in slabs {
                if !slab.objects.is_empty() {
                    for meta in &slab.objects {
                        if self.seen_oids.insert(meta.oid) != Some(true) {
                            continue;
                        }
                        if !self.append_slab_entry_to_out_pack(slab, meta) {
                            return None;
                        }
                        unique_count = unique_count.saturating_add(1);
                    }
                } else if let Some(ref bundle) = slab.mmap_bundle {
                    let pack_data = slab.in_memory_pack.as_ref()?;
                    let idx_view = IdxView::parse(&bundle.idx).ok()?;
                    let rev_bytes = bundle.rev.as_ref()?;
                    let rev_view = RevView::parse(rev_bytes, idx_view.len())?;
                    for k in 0..rev_view.len() {
                        let (oid, offset, compressed_len, crc) =
                            rev_view.entry_at_pack_order(k, &idx_view, slab.pack_byte_len)?;
                        if self.seen_oids.insert(oid) != Some(true) {
                            continue;
                        }
                        let start = usize::try_from(offset).ok()?;
                        let len = usize::try_from(compressed_len).ok()?;
                        let slice = pack_data.get(start..start.checked_add(len)?)?;
                        let out_offset = u64::try_from(self.out_pack.len()).ok()?;
                        if self.out_pack.capacity().saturating_sub(self.out_pack.len())
                            < slice.len().saturating_add(PACK_TRAILER_LEN)
                        {
                            return None;
                        }
                        self.out_pack.extend_from_slice(slice);
                        if self.idx_entries.len() >= self.idx_entries.capacity() {
                            return None;
                        }
                        self.idx_entries.push(IdxEntry {
                            oid,
                            crc32: crc,
                            offset: out_offset,
                        });
                        unique_count = unique_count.saturating_add(1);
                    }
                }
            }
        }

        // Write 12-byte header and 20-byte SHA-1 trailer.
        let hdr = pack_header(unique_count);
        self.out_pack
            .get_mut(..PACK_HEADER_LEN)?
            .copy_from_slice(&hdr);
        let checksum: [u8; 20] = Sha1::digest(&self.out_pack[..]).into();
        self.out_pack.extend_from_slice(&checksum);

        // Write companion `.idx` v2 in place inside `out_idx`.
        let n_large = self
            .idx_entries
            .iter()
            .filter(|e| e.offset > 0x7FFF_FFFF)
            .count();
        let req_idx_len = idx_len(self.idx_entries.len(), n_large)?;
        if self.out_idx.capacity() < req_idx_len {
            return None;
        }
        self.out_idx.resize(req_idx_len, 0);
        write_idx_v2(&mut self.idx_entries, &checksum, &mut self.out_idx).ok()?;

        let pack_bytes = self.out_pack.split().freeze();
        let idx_bytes = self.out_idx.split().freeze();
        Some((pack_bytes, idx_bytes, unique_count))
    }

    /// Appends `meta` from `slab` into `self.out_pack` with zero heap allocations:
    /// - Standalone entries (`Commit`, `Blob`, `Base Tree`) are copied verbatim.
    /// - `OBJ_OFS_DELTA` entries inside a contiguous `24h`/`168h` tree cluster are copied
    ///   verbatim when their cluster `Base Tree` is at the same relative distance, or have
    ///   their 2-byte `ofs_distance` header patched (zero zlib CPU!) when `Base Tree` is at a
    ///   different offset in `out_pack`, or are reconstructed in-place if `Base Tree` was not
    ///   requested at all (e.g. single historical tree lookup in `POST /gvfs/objects`).
    fn append_slab_entry_to_out_pack(&mut self, slab: &Slab, meta: &ObjSliceMeta) -> bool {
        let Some(ref pack_data) = slab.in_memory_pack else {
            return false;
        };
        let Ok(start) = usize::try_from(meta.offset) else {
            return false;
        };
        let Ok(len) = usize::try_from(meta.compressed_len) else {
            return false;
        };
        let Some(slice) = start.checked_add(len).and_then(|e| pack_data.get(start..e)) else {
            return false;
        };
        let Ok(out_offset) = u64::try_from(self.out_pack.len()) else {
            return false;
        };
        if self.idx_entries.len() >= self.idx_entries.capacity() {
            return false;
        }

        // Fast check: is this entry an `OBJ_OFS_DELTA` (type code 6)?
        let is_ofs_delta = slice
            .first()
            .is_some_and(|&b| (b.wrapping_shr(4) & 0x7) == 6);
        if !is_ofs_delta {
            if self.out_pack.capacity().saturating_sub(self.out_pack.len())
                < slice.len().saturating_add(PACK_TRAILER_LEN)
            {
                return false;
            }
            self.out_pack.extend_from_slice(slice);
            let crc = if meta.crc32 != 0 {
                meta.crc32
            } else {
                crc32fast::hash(slice)
            };
            let _ = self
                .out_offsets
                .insert(meta.oid, u32::try_from(out_offset).unwrap_or(0));
            self.idx_entries.push(IdxEntry {
                oid: meta.oid,
                crc32: crc,
                offset: out_offset,
            });
            return true;
        }

        // Parse the `OFS_DELTA` header to find its cluster `Base Tree` inside `slab`.
        let Some(hdr) = parse_entry_header(slice).ok().flatten() else {
            return false;
        };
        let DeltaBase::Ofs(orig_ofs) = hdr.base else {
            return false;
        };
        let Some(base_slab_off) = meta.offset.checked_sub(orig_ofs) else {
            return false;
        };
        let Ok(base_pos) = slab
            .objects
            .binary_search_by_key(&base_slab_off, |o| o.offset)
        else {
            return false;
        };
        let Some(base_meta) = slab.objects.get(base_pos) else {
            return false;
        };

        // Check in O(1) if `base_meta.oid` is already written to `out_pack`.
        if let Some(existing_base_off32) = self.out_offsets.get(&base_meta.oid) {
            let existing_base_offset = u64::from(existing_base_off32);
            let Some(new_ofs) = out_offset.checked_sub(existing_base_offset) else {
                return false;
            };
            if new_ofs == orig_ofs {
                // Rule 1: Translation invariance! Relative distance is identical -> verbatim copy!
                if self.out_pack.capacity().saturating_sub(self.out_pack.len())
                    < slice.len().saturating_add(PACK_TRAILER_LEN)
                {
                    return false;
                }
                self.out_pack.extend_from_slice(slice);
                let crc = if meta.crc32 != 0 {
                    meta.crc32
                } else {
                    crc32fast::hash(slice)
                };
                let _ = self
                    .out_offsets
                    .insert(meta.oid, u32::try_from(out_offset).unwrap_or(0));
                self.idx_entries.push(IdxEntry {
                    oid: meta.oid,
                    crc32: crc,
                    offset: out_offset,
                });
                return true;
            }
            // Header-only offset adjustment (zero zlib inflate/deflate!):
            let Some(z_delta) = slice.get(hdr.header_len..) else {
                return false;
            };
            let mut type_hdr = [0u8; 10];
            let th_len = encode_entry_header(ObjType::OfsDelta, hdr.size, &mut type_hdr);
            let mut ofs_hdr = [0u8; 10];
            let oh_len = encode_ofs_distance(new_ofs, &mut ofs_hdr);
            let total_len = th_len.saturating_add(oh_len).saturating_add(z_delta.len());
            if self.out_pack.capacity().saturating_sub(self.out_pack.len())
                < total_len.saturating_add(PACK_TRAILER_LEN)
            {
                return false;
            }
            let entry_start = self.out_pack.len();
            self.out_pack
                .extend_from_slice(type_hdr.get(..th_len).unwrap_or(&[]));
            self.out_pack
                .extend_from_slice(ofs_hdr.get(..oh_len).unwrap_or(&[]));
            self.out_pack.extend_from_slice(z_delta);
            let crc = self.out_pack.get(entry_start..).map_or(0, crc32fast::hash);
            let _ = self
                .out_offsets
                .insert(meta.oid, u32::try_from(out_offset).unwrap_or(0));
            self.idx_entries.push(IdxEntry {
                oid: meta.oid,
                crc32: crc,
                offset: out_offset,
            });
            return true;
        }

        // Single historical tree lookup (`POST /gvfs/objects` where `Base Tree` was not requested):
        // Reconstruct the depth-1 delta in-place inside `SpliceArena`'s pre-allocated buffers!
        let Some((kind, resolved_len)) = slab.inflate_object_into(
            meta,
            &mut self.inflater,
            &mut self.base_scratch,
            &mut self.delta_scratch,
            &mut self.resolved_scratch,
        ) else {
            return false;
        };
        let Some(resolved) = self.resolved_scratch.get(..resolved_len) else {
            return false;
        };
        let mut type_hdr = [0u8; 10];
        let th_len = encode_entry_header(
            kind,
            u64::try_from(resolved_len).unwrap_or(0),
            &mut type_hdr,
        );
        let Ok(z_len) = self.deflater.deflate(resolved, &mut self.z_scratch) else {
            return false;
        };
        let total_len = th_len.saturating_add(z_len);
        if self.out_pack.capacity().saturating_sub(self.out_pack.len())
            < total_len.saturating_add(PACK_TRAILER_LEN)
        {
            return false;
        }
        let entry_start = self.out_pack.len();
        self.out_pack
            .extend_from_slice(type_hdr.get(..th_len).unwrap_or(&[]));
        self.out_pack
            .extend_from_slice(self.z_scratch.get(..z_len).unwrap_or(&[]));
        let crc = self.out_pack.get(entry_start..).map_or(0, crc32fast::hash);
        self.idx_entries.push(IdxEntry {
            oid: meta.oid,
            crc32: crc,
            offset: out_offset,
        });
        true
    }
}

// ============================================================================
// CORE PRIMITIVE 2: Coalesce Ranges & Splice Pre-Compressed Pack Slices (PDF pp. 158–160)
// Used by:
//   1. `/gvfs/prefetch` (splices S3/local slabs + `< 15m` L0 RAM slab into 1 wire pack)
//   2. `POST /gvfs/objects` (coalesces HTTP Range reads for requested OIDs into 1 pack)
//   3. Pipeline Rollups (`15m -> 1h -> 6h -> 12h -> 24h -> 168h` & Weekly Aha Delta)
// ============================================================================
pub fn plan_deduplicated_range_splice(
    slabs: &[Arc<Slab>],
    filter_oids: Option<&[Oid20]>,
    max_gap_bytes: u64,
) -> (u32, Vec<HttpRangeBatch>) {
    let mut seen_oids = SeenOidSet::with_capacity_log2(18);
    let mut total_unique_objects: u32 = 0;
    let mut batches = Vec::new();

    if let Some(target_oids) = filter_oids {
        // Point-lookup mode (`POST /gvfs/objects`): use $O(1)$ MPHF lookups per slab,
        // sort matched objects by byte offset, and coalesce adjacent/nearby ranges!
        for (slab_index, slab) in slabs.iter().enumerate() {
            let mut matched: Vec<ObjSliceMeta> = Vec::new();
            for oid in target_oids {
                if seen_oids.contains(oid) {
                    continue;
                }
                if let Some(meta) = slab.lookup_oid(oid) {
                    if seen_oids.insert(*oid) == Some(true) {
                        matched.push(meta);
                    }
                }
            }
            matched.sort_unstable_by_key(|m| m.offset);
            coalesce_slab_entries(
                &slab.id,
                slab_index,
                &matched,
                max_gap_bytes,
                &mut total_unique_objects,
                &mut batches,
            );
        }
    } else {
        // Full slab merge mode (`/gvfs/prefetch` & pipeline tier rollups):
        // iterate objects in Merkle DFS order, deduplicating across slabs.
        for (slab_index, slab) in slabs.iter().enumerate() {
            let mut current_batch: Option<HttpRangeBatch> = None;
            for obj in &slab.objects {
                if seen_oids.insert(obj.oid) != Some(true) {
                    continue;
                }
                total_unique_objects = total_unique_objects.saturating_add(1);
                push_to_coalesced_batch(
                    &slab.id,
                    slab_index,
                    *obj,
                    max_gap_bytes,
                    &mut current_batch,
                    &mut batches,
                );
            }
            if let Some(finished) = current_batch.take() {
                batches.push(finished);
            }
        }
    }

    (total_unique_objects, batches)
}

fn coalesce_slab_entries(
    slab_id: &str,
    slab_index: usize,
    entries: &[ObjSliceMeta],
    max_gap_bytes: u64,
    total_unique_objects: &mut u32,
    batches: &mut Vec<HttpRangeBatch>,
) {
    let mut current_batch: Option<HttpRangeBatch> = None;
    for &obj in entries {
        *total_unique_objects = total_unique_objects.saturating_add(1);
        push_to_coalesced_batch(
            slab_id,
            slab_index,
            obj,
            max_gap_bytes,
            &mut current_batch,
            batches,
        );
    }
    if let Some(finished) = current_batch.take() {
        batches.push(finished);
    }
}

fn push_to_coalesced_batch(
    slab_id: &str,
    slab_index: usize,
    obj: ObjSliceMeta,
    max_gap_bytes: u64,
    current_batch: &mut Option<HttpRangeBatch>,
    batches: &mut Vec<HttpRangeBatch>,
) {
    let obj_start = obj.offset;
    let obj_end = obj.offset.saturating_add(u64::from(obj.compressed_len));

    match current_batch.as_mut() {
        Some(b)
            if obj_start >= b.range_start
                && obj_start <= b.range_end.saturating_add(max_gap_bytes) =>
        {
            let rel_start = usize::try_from(obj_start.saturating_sub(b.range_start)).unwrap_or(0);
            let rel_end = usize::try_from(obj_end.saturating_sub(b.range_start)).unwrap_or(0);
            b.range_end = b.range_end.max(obj_end);
            b.sub_slices.push((rel_start, rel_end));
            b.objects.push((obj.oid, obj.crc32));
        }
        _ => {
            if let Some(finished) = current_batch.take() {
                batches.push(finished);
            }
            let len_usize = usize::try_from(obj.compressed_len).unwrap_or(0);
            *current_batch = Some(HttpRangeBatch {
                slab_id: slab_id.to_string(),
                slab_index,
                range_start: obj_start,
                range_end: obj_end,
                sub_slices: vec![(0, len_usize)],
                objects: vec![(obj.oid, obj.crc32)],
            });
        }
    }
}

/// Executes the splice: writes 1 unified 12-byte `PACK` header, copies the raw
/// pre-compressed zlib slices from the coalesced HTTP Range buffers (zero zlib CPU!),
/// and appends the 20-byte SHA-1 trailer (PDF p. 159–160).
///
/// Also returns the new `ObjSliceMeta` vector in the spliced pack's byte layout so
/// pipeline rollups can immediately build a new `Slab` + `.mphf` + `.idx`.
pub fn assemble_spliced_pack(
    unique_object_count: u32,
    fetched_range_buffers: &[(HttpRangeBatch, Bytes)],
) -> (Bytes, Vec<ObjSliceMeta>) {
    let total_payload: usize = fetched_range_buffers
        .iter()
        .flat_map(|(b, _)| b.sub_slices.iter())
        .fold(0usize, |acc, &(s, e)| {
            acc.saturating_add(e.saturating_sub(s))
        });

    let mut out_pack = Vec::with_capacity(
        PACK_HEADER_LEN
            .saturating_add(total_payload)
            .saturating_add(PACK_TRAILER_LEN),
    );
    let mut out_objects = Vec::with_capacity(usize::try_from(unique_object_count).unwrap_or(0));

    // 1. 12-byte Git Pack v2 Header
    out_pack.extend_from_slice(&pack_header(unique_object_count));

    // 2. Copy each unique object's raw compressed bytes (zero zlib inflation/deflation)
    for (batch, raw_range_bytes) in fetched_range_buffers {
        for (&(rel_start, rel_end), &(oid, orig_crc)) in
            batch.sub_slices.iter().zip(batch.objects.iter())
        {
            if let Some(slice) = raw_range_bytes.get(rel_start..rel_end) {
                let offset = u64::try_from(out_pack.len()).unwrap_or(0);
                let compressed_len = u32::try_from(slice.len()).unwrap_or(0);
                let crc32 = if orig_crc != 0 {
                    orig_crc
                } else {
                    crc32fast::hash(slice)
                };
                out_pack.extend_from_slice(slice);
                out_objects.push(ObjSliceMeta {
                    oid,
                    offset,
                    compressed_len,
                    crc32,
                });
            }
        }
    }

    // 3. Append 20-byte SHA-1 trailer over `out_pack`
    let sha1_trailer: [u8; 20] = Sha1::digest(&out_pack).into();
    out_pack.extend_from_slice(&sha1_trailer);

    (Bytes::from(out_pack), out_objects)
}

/// Merges in-memory slabs (such as merging a 2-second `micro_delta` into `l0_ram_slab`
/// in `ServerRuntimeState::tick_live_sync`, PDF p. 167) using the exact same splice primitive.
pub fn splice_in_memory_slabs(
    slabs: &[Arc<Slab>],
    tip_commit: Oid20,
    tier: Tier,
    now_ts: u64,
) -> Option<Slab> {
    let (unique_count, batches) = plan_deduplicated_range_splice(slabs, None, 4096);
    let mut range_buffers = Vec::with_capacity(batches.len());
    for batch in batches {
        let slab = slabs.get(batch.slab_index)?;
        let pack = slab.in_memory_pack.as_ref()?;
        let start = usize::try_from(batch.range_start).ok()?;
        let end = usize::try_from(batch.range_end).ok()?;
        if end > pack.len() || start > end {
            return None;
        }
        range_buffers.push((batch, pack.slice(start..end)));
    }
    let (spliced_pack, spliced_objects) = assemble_spliced_pack(unique_count, &range_buffers);

    // Deduplicate HFS sidecar entries across the merged slabs.
    let mut seen_hfs = SeenOidSet::with_capacity_log2(14);
    let mut hfs_sidecar = Vec::new();
    for slab in slabs {
        for rec in &slab.hfs_sidecar {
            if seen_hfs.insert(rec.oid) == Some(true) {
                hfs_sidecar.push(*rec);
            }
        }
    }

    let start_ts = slabs
        .iter()
        .map(|s| s.start_ts)
        .min()
        .unwrap_or_else(|| now_ts.saturating_sub(tier.window_secs()));
    let end_ts = slabs
        .iter()
        .map(|s| s.end_ts)
        .max()
        .unwrap_or(now_ts)
        .max(now_ts);
    let have_commit = slabs.last().map_or(ZERO_OID, |s| s.have_commit);

    Slab::from_pack_and_objects(
        format!("slab-{}-{end_ts}", tier.as_str()),
        tier,
        start_ts,
        end_ts,
        have_commit,
        tip_commit,
        spliced_objects,
        hfs_sidecar,
        spliced_pack,
    )
}
