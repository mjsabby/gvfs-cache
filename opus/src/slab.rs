//! A slab: one immutable upstream pack plus its `.mphf` index.
//!
//! The `.pack` is exactly what the upstream sent for one `have`/`want` fetch (deltas and
//! all), so the body of any slab is a self-contained run of entries that can be spliced
//! into another pack verbatim. The `.mphf` index makes every object addressable in O(1):
//!
//! ```text
//! 0     magic "GVDSLAB1"        8     version u32         12    object count u32
//! 16    partitions u32           20    buckets u32         24    seed u64
//! 32    pack length u64          40    pack trailer [20]   60    pack entry count u32
//! 64    offsets_at u64           72    pilots_at u64       80    records_at u64
//! 88    file length u64          96..128 reserved
//! offsets: (partitions + 1) x u32   pilots: partitions x buckets x u16   (8-aligned)
//! records: count x 40 bytes, in MPHF slot order:
//!   [0..20)  object id
//!   [20..28) u64: entry offset (bits 0..40) | object type (40..43) | delta depth (43..51)
//!                 | size high bits (51..59)
//!   [28..32) u32 entry length (header + zlib stream)
//!   [32..36) u32 object size, low bits
//!   [36..40) u32 slot of the delta base, or NO_BASE
//! ```
//!
//! A lookup is `mphf.slot(oid)` then one 40-byte record read and a full-id compare.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::mphf::Mphf;
use crate::util::{Context, Oid, Result, le_u32, le_u64, sub, u64_of, us};

pub const INDEX_MAGIC: &[u8; 8] = b"GVDSLAB1";
pub const INDEX_VERSION: u32 = 1;
pub const INDEX_HEADER_LEN: usize = 128;
pub const REC_LEN: usize = 40;
pub const NO_BASE: u32 = u32::MAX;
const OFFSET_BITS: u32 = 40;
const MAX_OFFSET: u64 = (1u64 << OFFSET_BITS) - 1;

/// The pyramid tier a slab belongs to.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Tier {
    /// The live tip's in-RAM slab (never stored).
    L0 = 0,
    M15 = 1,
    H1 = 2,
    H6 = 3,
    H12 = 4,
    H24 = 5,
    /// The week's net-new objects ("aha" delta) for warm clients.
    Week = 6,
    /// The full baseline for cold clones.
    Base = 7,
}

impl Tier {
    pub const ALL: [Tier; 8] = [
        Tier::L0,
        Tier::M15,
        Tier::H1,
        Tier::H6,
        Tier::H12,
        Tier::H24,
        Tier::Week,
        Tier::Base,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Tier::L0 => "l0",
            Tier::M15 => "m15",
            Tier::H1 => "h1",
            Tier::H6 => "h6",
            Tier::H12 => "h12",
            Tier::H24 => "h24",
            Tier::Week => "week",
            Tier::Base => "base",
        }
    }

    pub fn parse(s: &str) -> Option<Tier> {
        match s {
            "15m" => Some(Tier::M15),
            "1h" => Some(Tier::H1),
            "6h" => Some(Tier::H6),
            "12h" => Some(Tier::H12),
            "24h" => Some(Tier::H24),
            "168h" => Some(Tier::Week),
            _ => Tier::ALL.iter().copied().find(|t| t.name() == s),
        }
    }

    pub fn from_u8(v: u8) -> Option<Tier> {
        Tier::ALL.get(usize::from(v)).copied()
    }
}

/// What the manifest records about a slab.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlabMeta {
    pub id: String,
    pub tier: Tier,
    /// Prefetch timestamps: the slab covers objects that became visible in
    /// `(start_ts, end_ts]`; `end_ts` is the GVFS pack timestamp clients see.
    pub start_ts: u64,
    pub end_ts: u64,
    /// The commit range `(have, want]` it was fetched for (`have` zero for full packs).
    pub have: Oid,
    pub want: Oid,
    pub objects: u32,
    pub pack_len: u64,
    pub index_len: u64,
    pub pack_sha: [u8; 20],
}

impl SlabMeta {
    pub fn make_id(tier: Tier, end_ts: u64, pack_sha: &[u8; 20]) -> String {
        let hex = crate::util::hex_string(pack_sha);
        format!(
            "{}-{}-{}",
            tier.name(),
            end_ts,
            hex.get(..16).unwrap_or(&hex)
        )
    }

    pub fn pack_key(&self) -> String {
        format!("slabs/{}.pack", self.id)
    }

    pub fn index_key(&self) -> String {
        format!("slabs/{}.mphf", self.id)
    }

    /// The slab's blob sizes table (stores with `--blobs separate`).
    pub fn sizes_key(&self) -> String {
        format!("slabs/{}.sizes", self.id)
    }
}

/// Per-object facts the ingester hands to the index builder (indexed by entry number).
#[derive(Clone, Copy, Debug, Default)]
pub struct Facts {
    pub oid: Oid,
    pub offset: u64,
    pub entry_len: u32,
    pub size: u64,
    /// Resolved object type (1..=4).
    pub kind: u8,
    /// Delta chain depth (0 for whole objects).
    pub depth: u8,
    /// Entry number of the delta base, or `NO_BASE`.
    pub base: u32,
}

/// One decoded index record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rec {
    pub oid: Oid,
    pub offset: u64,
    pub kind: u8,
    pub depth: u8,
    pub entry_len: u32,
    pub size: u64,
    pub base: u32,
}

impl Rec {
    pub fn is_delta(&self) -> bool {
        self.base != NO_BASE
    }
}

fn encode_rec(r: &Rec, out: &mut [u8]) {
    let word = (r.offset & MAX_OFFSET)
        | (u64::from(r.kind & 7)).wrapping_shl(40)
        | u64::from(r.depth).wrapping_shl(43)
        | (r.size.wrapping_shr(32) & 0xFF).wrapping_shl(51);
    let parts: [&[u8]; 5] = [
        &r.oid.0,
        &word.to_le_bytes(),
        &r.entry_len.to_le_bytes(),
        &(r.size as u32).to_le_bytes(),
        &r.base.to_le_bytes(),
    ];
    let mut at = 0usize;
    for p in parts {
        if let Some(dst) = out.get_mut(at..at.wrapping_add(p.len())) {
            dst.copy_from_slice(p);
        }
        at = at.wrapping_add(p.len());
    }
}

#[inline]
fn decode_rec(b: &[u8]) -> Option<Rec> {
    let oid = Oid::from_slice(b.get(..20)?)?;
    let word = le_u64(b, 20)?;
    let size_lo = u64::from(le_u32(b, 32)?);
    Some(Rec {
        oid,
        offset: word & MAX_OFFSET,
        kind: (word.wrapping_shr(40) & 7) as u8,
        depth: (word.wrapping_shr(43) & 0xFF) as u8,
        entry_len: le_u32(b, 28)?,
        size: (word.wrapping_shr(51) & 0xFF).wrapping_shl(32) | size_lo,
        base: le_u32(b, 36)?,
    })
}

fn align8(n: usize) -> usize {
    n.saturating_add(7) & !7usize
}

/// Builds the `.mphf` index bytes for a pack whose entries are described by `facts`.
pub fn build_index(
    facts: &[Facts],
    pack_len: u64,
    pack_sha: &[u8; 20],
    threads: usize,
) -> Result<Vec<u8>> {
    build_index_with_entries(
        facts,
        crate::util::u32_of(facts.len()),
        pack_len,
        pack_sha,
        threads,
    )
}

/// Like [`build_index`] for a pack holding `entries` entries of which `facts` are the
/// distinct objects (a concatenated rollup may carry a re-introduced object twice; only
/// its first copy is indexed, and git accepts the duplicate).
pub fn build_index_with_entries(
    facts: &[Facts],
    entries: u32,
    pack_len: u64,
    pack_sha: &[u8; 20],
    threads: usize,
) -> Result<Vec<u8>> {
    let oids: Vec<Oid> = facts.iter().map(|f| f.oid).collect();
    match crate::mphf::build(&oids, threads) {
        Ok(built) => encode_index(facts, &built, entries, pack_len, pack_sha),
        Err(e) if e.is_conflict() => {
            // The pack carries some object twice: index its first copy only.
            let unique = dedupe_facts(facts);
            let oids: Vec<Oid> = unique.iter().map(|f| f.oid).collect();
            let built = crate::mphf::build(&oids, threads)?;
            encode_index(&unique, &built, entries, pack_len, pack_sha)
        }
        Err(e) => Err(e),
    }
}

/// Keeps the first entry of every object id; delta bases are remapped onto kept entries.
pub fn dedupe_facts(facts: &[Facts]) -> Vec<Facts> {
    let mut first: std::collections::HashMap<Oid, u32> =
        std::collections::HashMap::with_capacity(facts.len());
    let mut new_index = vec![NO_BASE; facts.len()];
    let mut out: Vec<Facts> = Vec::with_capacity(facts.len());
    for (i, f) in facts.iter().enumerate() {
        let idx = *first.entry(f.oid).or_insert_with(|| {
            out.push(*f);
            crate::util::u32_of(out.len().saturating_sub(1))
        });
        if let Some(slot) = new_index.get_mut(i) {
            *slot = idx;
        }
    }
    for f in &mut out {
        if f.base != NO_BASE {
            f.base = new_index.get(f.base as usize).copied().unwrap_or(NO_BASE);
        }
    }
    out
}

fn encode_index(
    facts: &[Facts],
    built: &crate::mphf::Built,
    entries: u32,
    pack_len: u64,
    pack_sha: &[u8; 20],
) -> Result<Vec<u8>> {
    let n = facts.len();
    for f in facts {
        if f.offset > MAX_OFFSET || f.size > (1u64 << 40).saturating_sub(1) {
            crate::bail!(
                "object {} too large for the slab index (offset {}, size {})",
                f.oid,
                f.offset,
                f.size
            );
        }
    }

    let offsets_at = INDEX_HEADER_LEN;
    let pilots_at = align8(offsets_at.saturating_add(built.offsets.len().saturating_mul(4)));
    let records_at = align8(pilots_at.saturating_add(built.pilots.len().saturating_mul(2)));
    let total = records_at.saturating_add(n.saturating_mul(REC_LEN));
    let mut out = vec![0u8; total];

    let mut header = [0u8; INDEX_HEADER_LEN];
    let fields: [(usize, &[u8]); 13] = [
        (0, INDEX_MAGIC),
        (8, &INDEX_VERSION.to_le_bytes()),
        (12, &crate::util::u32_of(n).to_le_bytes()),
        (16, &built.parts.to_le_bytes()),
        (20, &built.buckets.to_le_bytes()),
        (24, &built.seed.to_le_bytes()),
        (32, &pack_len.to_le_bytes()),
        (40, pack_sha),
        (60, &entries.to_le_bytes()),
        (64, &u64_of(offsets_at).to_le_bytes()),
        (72, &u64_of(pilots_at).to_le_bytes()),
        (80, &u64_of(records_at).to_le_bytes()),
        (88, &u64_of(total).to_le_bytes()),
    ];
    for (at, bytes) in fields {
        if let Some(dst) = header.get_mut(at..at.saturating_add(bytes.len())) {
            dst.copy_from_slice(bytes);
        }
    }
    if let Some(dst) = out.get_mut(..INDEX_HEADER_LEN) {
        dst.copy_from_slice(&header);
    }
    for (chunk, v) in out
        .get_mut(offsets_at..pilots_at)
        .unwrap_or(&mut [])
        .chunks_exact_mut(4)
        .zip(&built.offsets)
    {
        chunk.copy_from_slice(&v.to_le_bytes());
    }
    for (chunk, v) in out
        .get_mut(pilots_at..records_at)
        .unwrap_or(&mut [])
        .chunks_exact_mut(2)
        .zip(&built.pilots)
    {
        chunk.copy_from_slice(&v.to_le_bytes());
    }
    let records = out.get_mut(records_at..).unwrap_or(&mut []);
    for (f, &slot) in facts.iter().zip(&built.slots) {
        let base = if f.base == NO_BASE {
            NO_BASE
        } else {
            *built
                .slots
                .get(us(u64::from(f.base)))
                .ctx("delta base out of range")?
        };
        let rec = Rec {
            oid: f.oid,
            offset: f.offset,
            kind: f.kind,
            depth: f.depth,
            entry_len: f.entry_len,
            size: f.size,
            base,
        };
        let at = us(u64::from(slot)).saturating_mul(REC_LEN);
        let dst = records
            .get_mut(at..at.saturating_add(REC_LEN))
            .ctx("record slot out of range")?;
        encode_rec(&rec, dst);
    }
    Ok(out)
}

/// Bytes backing a slab file: a read-only map, or a heap buffer (the L0 slab).
pub enum Region {
    Map(gvfs_mmap::Map),
    Heap(Vec<u8>),
}

impl Region {
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        match self {
            Region::Map(m) => m.bytes(),
            Region::Heap(v) => v,
        }
    }
}

/// An open slab.
pub struct Slab {
    pub meta: SlabMeta,
    pack: Region,
    /// The `.pack` file, kept open for `sendfile(2)`; `None` for heap slabs.
    pack_file: Option<File>,
    index: Region,
    /// Distinct objects (records).
    count: u32,
    /// Entries in the pack (>= count).
    entries: u32,
    seed: u64,
    parts: u32,
    buckets: u32,
    offsets_at: usize,
    pilots_at: usize,
    records_at: usize,
    /// Entries in id order with their CRC-32s, for pack indexes (built on first use).
    sorted: std::sync::OnceLock<Option<crate::idx::SortedSlab>>,
    /// The blob sizes its trees introduce (`--blobs separate` stores), once loaded.
    sizes: std::sync::OnceLock<Arc<crate::sizes::Sizes>>,
    /// Its ids, checked before the index once built ([`Slab::prepare_filter`]).
    filter: std::sync::OnceLock<crate::idfilter::IdFilter>,
}

impl Slab {
    /// Maps `<dir>/<id>.pack` and `<id>.mphf` (immutable files) and validates them.
    pub fn open(pack_path: &Path, index_path: &Path, meta: SlabMeta) -> Result<Arc<Slab>> {
        let pack_file = File::open(pack_path).ctx(&format!("opening {}", pack_path.display()))?;
        let pack = gvfs_mmap::Map::open(&pack_file, false).ctx("mapping pack")?;
        let index_file =
            File::open(index_path).ctx(&format!("opening {}", index_path.display()))?;
        let index = gvfs_mmap::Map::open(&index_file, true).ctx("mapping index")?;
        // Lookups touch records at random; packs are mostly streamed.
        let _ = index.advise(gvfs_mmap::Advice::Random);
        Ok(Arc::new(Slab::new(
            meta,
            Region::Map(pack),
            Some(pack_file),
            Region::Map(index),
        )?))
    }

    /// Opens `<name>.mphf` and the `<name>.pack` beside it, outside any manifest (tools).
    pub fn open_file(index: &Path) -> Result<Arc<Slab>> {
        let meta = SlabMeta {
            id: index
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default(),
            tier: Tier::Base,
            start_ts: 0,
            end_ts: 0,
            have: crate::util::Oid::ZERO,
            want: crate::util::Oid::ZERO,
            objects: 0,
            pack_len: 0,
            index_len: 0,
            pack_sha: [0; 20],
        };
        Slab::open(&index.with_extension("pack"), index, meta)
    }

    /// A slab held in memory (the L0 slab, tests).
    pub fn from_heap(meta: SlabMeta, pack: Vec<u8>, index: Vec<u8>) -> Result<Arc<Slab>> {
        Ok(Arc::new(Slab::new(
            meta,
            Region::Heap(pack),
            None,
            Region::Heap(index),
        )?))
    }

    fn new(meta: SlabMeta, pack: Region, pack_file: Option<File>, index: Region) -> Result<Slab> {
        let ix = index.bytes();
        if ix.get(..8) != Some(&INDEX_MAGIC[..]) || le_u32(ix, 8) != Some(INDEX_VERSION) {
            crate::bail!("slab {}: not a slab index", meta.id);
        }
        let count = le_u32(ix, 12).ctx("index header")?;
        let parts = le_u32(ix, 16).ctx("index header")?;
        let buckets = le_u32(ix, 20).ctx("index header")?;
        let seed = le_u64(ix, 24).ctx("index header")?;
        let pack_len = le_u64(ix, 32).ctx("index header")?;
        let pack_sha = sub(ix, 40, 20).ctx("index header")?;
        let entries = le_u32(ix, 60).ctx("index header")?;
        let offsets_at = us(le_u64(ix, 64).ctx("index header")?);
        let pilots_at = us(le_u64(ix, 72).ctx("index header")?);
        let records_at = us(le_u64(ix, 80).ctx("index header")?);
        let total = us(le_u64(ix, 88).ctx("index header")?);
        let offsets_len = us(u64::from(parts)).saturating_add(1).saturating_mul(4);
        let pilots_len = us(u64::from(parts))
            .saturating_mul(us(u64::from(buckets)))
            .saturating_mul(2);
        let records_len = us(u64::from(count)).saturating_mul(REC_LEN);
        let consistent = total == ix.len()
            && offsets_at.saturating_add(offsets_len) <= pilots_at
            && pilots_at.saturating_add(pilots_len) <= records_at
            && records_at.saturating_add(records_len) == total
            && parts > 0
            && count <= entries;
        if !consistent {
            crate::bail!("slab {}: index sections are inconsistent", meta.id);
        }
        let p = pack.bytes();
        if u64_of(p.len()) != pack_len || p.len() < crate::pack::PACK_MIN_LEN {
            crate::bail!(
                "slab {}: pack is {} bytes, index expects {pack_len}",
                meta.id,
                p.len()
            );
        }
        if p.get(p.len().saturating_sub(20)..) != Some(pack_sha) {
            crate::bail!("slab {}: pack trailer does not match its index", meta.id);
        }
        if crate::pack::parse_pack_header(p) != Some(entries) {
            crate::bail!(
                "slab {}: pack header count does not match its index",
                meta.id
            );
        }
        Ok(Slab {
            meta,
            pack,
            pack_file,
            index,
            count,
            entries,
            seed,
            parts,
            buckets,
            offsets_at,
            pilots_at,
            records_at,
            sorted: std::sync::OnceLock::new(),
            sizes: std::sync::OnceLock::new(),
            filter: std::sync::OnceLock::new(),
        })
    }

    /// Attaches the slab's sizes table (the first one set stays).
    pub fn set_sizes(&self, t: Arc<crate::sizes::Sizes>) {
        let _ = self.sizes.set(t);
    }

    /// The slab's sizes table, if one is attached.
    pub fn sizes(&self) -> Option<&Arc<crate::sizes::Sizes>> {
        self.sizes.get()
    }

    #[inline]
    pub fn pack(&self) -> &[u8] {
        self.pack.bytes()
    }

    pub fn pack_file(&self) -> Option<&File> {
        self.pack_file.as_ref()
    }

    pub fn index_bytes(&self) -> &[u8] {
        self.index.bytes()
    }

    /// Distinct objects indexed.
    #[inline]
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Entries in the pack (the pack header's count).
    #[inline]
    pub fn entries(&self) -> u32 {
        self.entries
    }

    /// `[12, len - 20)`: the entries, ready to splice.
    pub fn body(&self) -> (u64, u64) {
        let len = u64_of(self.pack().len());
        (
            crate::pack::PACK_HEADER_LEN as u64,
            len.saturating_sub(crate::pack::PACK_TRAILER_LEN as u64),
        )
    }

    #[inline]
    fn mphf(&self) -> Mphf<'_> {
        let ix = self.index.bytes();
        Mphf {
            seed: self.seed,
            parts: self.parts,
            buckets: self.buckets,
            offsets: ix.get(self.offsets_at..self.pilots_at).unwrap_or(&[]),
            pilots: ix.get(self.pilots_at..self.records_at).unwrap_or(&[]),
        }
    }

    #[inline]
    fn rec_bytes(&self, slot: u32) -> Option<&[u8]> {
        let at = us(u64::from(slot))
            .checked_mul(REC_LEN)?
            .checked_add(self.records_at)?;
        sub(self.index.bytes(), at, REC_LEN)
    }

    /// Starts loading the record [`Slab::find`] would read for `oid`.
    #[inline]
    pub fn preload(&self, oid: &Oid) {
        if let Some(slot) = self.mphf().slot(oid) {
            let at = us(u64::from(slot))
                .saturating_mul(REC_LEN)
                .saturating_add(self.records_at);
            let b = self.index.bytes();
            crate::util::prefetch(b, at);
            crate::util::prefetch(b, at.saturating_add(REC_LEN.saturating_sub(1)));
        }
    }

    /// The slot holding `oid`, if this slab has it.
    #[inline]
    pub fn find(&self, oid: &Oid) -> Option<u32> {
        if self.filter.get().is_some_and(|f| !f.may_contain(oid)) {
            return None;
        }
        let slot = self.mphf().slot(oid)?;
        let rec = self.rec_bytes(slot)?;
        if rec.get(..20)? == oid.0 {
            Some(slot)
        } else {
            None
        }
    }

    /// Builds the id filter [`Slab::find`] checks before the index (once; an edge does it
    /// for every slab its lookups probe before another: see [`crate::idfilter`]).
    pub fn prepare_filter(&self) {
        self.filter
            .get_or_init(|| crate::idfilter::IdFilter::new(us(u64::from(self.count)), self.ids()));
    }

    /// Every id, in slot order.
    pub fn ids(&self) -> impl Iterator<Item = Oid> + '_ {
        (0..self.count).filter_map(|s| {
            self.rec_bytes(s)
                .and_then(|r| r.get(..20))
                .and_then(Oid::from_slice)
        })
    }

    #[inline]
    pub fn rec(&self, slot: u32) -> Option<Rec> {
        decode_rec(self.rec_bytes(slot)?)
    }

    /// The pack bytes of an entry (header + zlib stream).
    #[inline]
    pub fn entry(&self, rec: &Rec) -> Option<&[u8]> {
        sub(self.pack(), us(rec.offset), us(u64::from(rec.entry_len)))
    }

    /// The entries in id order with their CRC-32s (computed once, on first use). `None`
    /// when the pack holds entries the records do not describe.
    pub fn sorted(&self) -> Option<&crate::idx::SortedSlab> {
        self.sorted
            .get_or_init(|| {
                let _cold = gvfs_alloc_guard::cold_section();
                crate::idx::SortedSlab::build(self)
            })
            .as_ref()
    }

    /// Every record, in slot order.
    pub fn records(&self) -> impl Iterator<Item = Rec> + '_ {
        (0..self.count).filter_map(|s| self.rec(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip() {
        let r = Rec {
            oid: Oid([9u8; 20]),
            offset: (1u64 << 39) + 12345,
            kind: 2,
            depth: 51,
            entry_len: 777_777,
            size: (5u64 << 32) + 99,
            base: 42,
        };
        let mut b = [0u8; REC_LEN];
        encode_rec(&r, &mut b);
        assert_eq!(decode_rec(&b), Some(r));
        assert_eq!(Tier::parse("15m"), Some(Tier::M15));
        assert_eq!(Tier::parse("base"), Some(Tier::Base));
        assert_eq!(Tier::from_u8(6), Some(Tier::Week));
    }
}
