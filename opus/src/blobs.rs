//! Blob storage for stores that keep blobs out of the metadata slabs (`--blobs separate`).
//!
//! * **Blob packs** (`blobs/<id>.pack`) are stock git packs holding whole blobs only, never
//!   deltas. Any entry is served by splicing its own bytes, with no inflate and no base.
//!   Entries whose content does not compress are written as deflate *stored* blocks: every
//!   content byte then sits at a computable file offset, so byte ranges are read in place.
//! * **Segments** (`blobs/<id>.bidx`) index one or more blob packs: a minimal perfect hash
//!   and one 48-byte record per blob (64-bit offsets, lengths and sizes). Each pipeline run
//!   or mirror batch adds a segment and folds merge small ones, so a lookup probes a few.
//! * **`blobs.bin`** lists the segments and packs (compare-and-swap, like `manifest.bin`)
//!   and how complete the store is: `tip complete` once every blob of the genesis tip's tree
//!   is in a pack.
//!
//! ```text
//! segment: "GVDBLOBX" | version u32 | count u32 | parts u32 | buckets u32 | seed u64
//!          | packs u32 | reserved u32 | packs_at u64 | offsets_at u64 | pilots_at u64
//!          | records_at u64 | file length u64 | reserved [16]
//!          packs: id_len u16 | id | pack length u64 | pack trailer [20]
//!          offsets (parts + 1) x u32 | pilots parts x buckets x u16 (8-aligned)
//!          records: count x (object id [20] | pack u32 | at u64 | entry length u64 | size u64)
//!          at = entry offset (bits 0..48) | header length (48..56) | stored (bit 56)
//! ```

use std::fs::File;
use std::io::Write;
use std::path::Path;

use sha1::{Digest, Sha1};

use crate::pack::{HeaderBytes, OBJ_BLOB, pack_header};
use crate::slab::Region;
use crate::util::{Context, Oid, Result, le_u16, le_u32, le_u64, sub, u32_of, u64_of, us};

pub const BLOBS_KEY: &str = "blobs.bin";
pub const SEG_MAGIC: &[u8; 8] = b"GVDBLOBX";
pub const SEG_VERSION: u32 = 1;
pub const SEG_HEADER_LEN: usize = 96;
pub const SEG_REC_LEN: usize = 48;
const MAX_OFFSET: u64 = (1u64 << 48) - 1;
/// Content bytes per deflate stored block (the format's maximum).
pub const STORED_BLOCK: usize = 65535;
/// zlib header of a stream of stored blocks: deflate, 32K window, "fastest" level.
const ZLIB_STORED_HEADER: [u8; 2] = [0x78, 0x01];
/// Blobs at least this big whose upstream compression saves under 5% are re-encoded as
/// stored blocks (seekable).
pub const STORED_MIN: u64 = 1 << 20;

pub fn pack_key(id: &str) -> String {
    format!("blobs/{id}.pack")
}

pub fn segment_key(id: &str) -> String {
    format!("blobs/{id}.bidx")
}

/// Where a blob lives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobRec {
    pub oid: Oid,
    /// Index into the segment's pack table.
    pub pack: u32,
    /// Offset of the entry (header + zlib stream) in its pack.
    pub offset: u64,
    pub entry_len: u64,
    /// The blob's size (its content length).
    pub size: u64,
    /// Bytes of the entry header before the zlib stream.
    pub hdr_len: u8,
    /// The zlib stream is stored blocks: content is addressable in place.
    pub stored: bool,
}

impl BlobRec {
    /// The zlib stream's offset and length in the pack.
    pub fn zlib(&self) -> (u64, u64) {
        let h = u64::from(self.hdr_len);
        (
            self.offset.saturating_add(h),
            self.entry_len.saturating_sub(h),
        )
    }

    /// For a stored entry, the pack ranges holding content bytes `[from, from + len)`, in
    /// order (each at most one block).
    pub fn stored_ranges(&self, from: u64, len: u64, out: &mut Vec<(u64, u64)>) {
        out.clear();
        let block = u64_of(STORED_BLOCK);
        let end = from.saturating_add(len).min(self.size);
        let zlib = self.zlib().0.saturating_add(2);
        let mut at = from;
        while at < end {
            let k = at.checked_div(block).unwrap_or(0);
            let within = at.checked_rem(block).unwrap_or(0);
            let take = block.saturating_sub(within).min(end.saturating_sub(at));
            let file = zlib
                .saturating_add(k.saturating_mul(block.saturating_add(5)))
                .saturating_add(5)
                .saturating_add(within);
            out.push((file, take));
            at = at.saturating_add(take);
        }
    }
}

/// A pack a segment indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackRef {
    pub id: String,
    pub len: u64,
    pub sha: [u8; 20],
}

/// How a blob entry's zlib stream is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    /// A zlib stream handed over as is (the upstream's compressed bytes).
    Raw,
    /// The content, written as deflate stored blocks.
    Stored,
}

struct Open {
    start: u64,
    hdr_len: u8,
    size: u64,
    enc: Encoding,
    /// Content bytes taken (stored) or zlib bytes copied (raw).
    taken: u64,
    adler: u32,
    block: Vec<u8>,
}

/// Writes a blob pack: header, whole-blob entries, SHA-1 trailer. The entry count is
/// declared up front, so the pack streams out in one pass (no seek back to the header).
pub struct PackWriter<W: Write> {
    out: W,
    sha: Sha1,
    pos: u64,
    count: u32,
    written: u32,
    open: Option<Open>,
}

impl<W: Write> PackWriter<W> {
    pub fn new(out: W, count: u32) -> Result<PackWriter<W>> {
        let mut w = PackWriter {
            out,
            sha: Sha1::new(),
            pos: 0,
            count,
            written: 0,
            open: None,
        };
        w.put(&pack_header(count))?;
        Ok(w)
    }

    fn put(&mut self, b: &[u8]) -> Result<()> {
        self.out.write_all(b).ctx("writing blob pack")?;
        self.sha.update(b);
        self.pos = self.pos.saturating_add(u64_of(b.len()));
        Ok(())
    }

    /// Bytes written so far.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// The output (to read back entries already written).
    pub fn spool(&mut self) -> &mut W {
        &mut self.out
    }

    /// Starts the entry of a blob of `size` bytes.
    pub fn begin(&mut self, size: u64, enc: Encoding) -> Result<()> {
        if self.open.is_some() {
            crate::bail!("blob pack: an entry is already open");
        }
        if self.written >= self.count {
            crate::bail!("blob pack: more entries than the {} declared", self.count);
        }
        let start = self.pos;
        let hdr = HeaderBytes::whole(OBJ_BLOB, size);
        self.put(hdr.as_slice())?;
        if enc == Encoding::Stored {
            self.put(&ZLIB_STORED_HEADER)?;
        }
        self.open = Some(Open {
            start,
            hdr_len: u8::try_from(hdr.as_slice().len()).unwrap_or(u8::MAX),
            size,
            enc,
            taken: 0,
            adler: 1,
            block: if enc == Encoding::Stored {
                Vec::with_capacity(STORED_BLOCK)
            } else {
                Vec::new()
            },
        });
        Ok(())
    }

    /// Raw entries: the next bytes of the zlib stream.
    pub fn raw(&mut self, zlib: &[u8]) -> Result<()> {
        match self.open.as_mut() {
            Some(o) if o.enc == Encoding::Raw => {
                o.taken = o.taken.saturating_add(u64_of(zlib.len()));
            }
            _ => crate::bail!("blob pack: raw bytes outside a raw entry"),
        }
        self.put(zlib)
    }

    /// Stored entries: the next content bytes.
    pub fn content(&mut self, mut data: &[u8]) -> Result<()> {
        loop {
            let Some(o) = self.open.as_mut().filter(|o| o.enc == Encoding::Stored) else {
                crate::bail!("blob pack: content outside a stored entry");
            };
            if data.is_empty() {
                return Ok(());
            }
            let room = STORED_BLOCK.saturating_sub(o.block.len());
            let (now, rest) = data.split_at(room.min(data.len()));
            o.block.extend_from_slice(now);
            o.adler = zlib_rs::adler32::adler32(o.adler, now);
            o.taken = o.taken.saturating_add(u64_of(now.len()));
            data = rest;
            if o.taken > o.size {
                crate::bail!("blob pack: more content than the declared {} bytes", o.size);
            }
            // A full block is written as soon as more content follows; the last block of
            // the stream is written by `end` with the final bit.
            if o.block.len() == STORED_BLOCK && o.taken < o.size {
                let block = std::mem::take(&mut o.block);
                self.put_block(&block, false)?;
                if let Some(o) = self.open.as_mut() {
                    o.block = block;
                    o.block.clear();
                }
            }
        }
    }

    fn put_block(&mut self, data: &[u8], last: bool) -> Result<()> {
        let len = u16::try_from(data.len())
            .ok()
            .ctx("stored block too long")?;
        let head = [
            u8::from(last),
            (len & 0xFF) as u8,
            len.wrapping_shr(8) as u8,
            (!len & 0xFF) as u8,
            (!len).wrapping_shr(8) as u8,
        ];
        self.put(&head)?;
        self.put(data)
    }

    /// Ends the open entry, which holds blob `oid`; returns its record (pack 0).
    pub fn end(&mut self, oid: Oid) -> Result<BlobRec> {
        let o = self.open.take().ctx("blob pack: no open entry")?;
        let stored = o.enc == Encoding::Stored;
        if stored {
            if o.taken != o.size {
                crate::bail!(
                    "blob pack: {} content bytes for a {}-byte blob {oid}",
                    o.taken,
                    o.size
                );
            }
            self.put_block(&o.block, true)?;
            self.put(&o.adler.to_be_bytes())?;
        }
        self.written = self.written.saturating_add(1);
        Ok(BlobRec {
            oid,
            pack: 0,
            offset: o.start,
            entry_len: self.pos.saturating_sub(o.start),
            size: o.size,
            hdr_len: o.hdr_len,
            stored,
        })
    }

    /// Writes the trailer; returns the output, the pack length and its trailer.
    pub fn finish(mut self) -> Result<(W, u64, [u8; 20])> {
        if self.open.is_some() || self.written != self.count {
            crate::bail!(
                "blob pack: {} of {} entries written",
                self.written,
                self.count
            );
        }
        let sha: [u8; 20] = std::mem::take(&mut self.sha).finalize().into();
        self.out.write_all(&sha).ctx("writing blob pack")?;
        self.out.flush().ctx("writing blob pack")?;
        Ok((self.out, self.pos.saturating_add(20), sha))
    }
}

fn align8(n: usize) -> usize {
    n.saturating_add(7) & !7usize
}

/// Builds a segment index over `packs` for `recs` (each naming its pack by index). A blob
/// listed twice keeps its first record.
pub fn build_segment(packs: &[PackRef], recs: &[BlobRec], threads: usize) -> Result<Vec<u8>> {
    let mut seen = std::collections::HashSet::with_capacity(recs.len());
    let unique: Vec<BlobRec> = recs
        .iter()
        .filter(|r| seen.insert(r.oid))
        .copied()
        .collect();
    for r in &unique {
        if r.offset > MAX_OFFSET || us(u64::from(r.pack)) >= packs.len() {
            crate::bail!("blob {} out of range for a segment", r.oid);
        }
    }
    let oids: Vec<Oid> = unique.iter().map(|r| r.oid).collect();
    let built = crate::mphf::build(&oids, threads)?;
    let mut table = Vec::new();
    for p in packs {
        let id = p.id.as_bytes();
        table.extend_from_slice(
            &u16::try_from(id.len())
                .ok()
                .ctx("pack id too long")?
                .to_le_bytes(),
        );
        table.extend_from_slice(id);
        table.extend_from_slice(&p.len.to_le_bytes());
        table.extend_from_slice(&p.sha);
    }
    let packs_at = SEG_HEADER_LEN;
    let offsets_at = align8(packs_at.saturating_add(table.len()));
    let pilots_at = align8(offsets_at.saturating_add(built.offsets.len().saturating_mul(4)));
    let records_at = align8(pilots_at.saturating_add(built.pilots.len().saturating_mul(2)));
    let total = records_at.saturating_add(unique.len().saturating_mul(SEG_REC_LEN));
    let mut out = vec![0u8; total];
    let fields: [(usize, &[u8]); 13] = [
        (0, SEG_MAGIC),
        (8, &SEG_VERSION.to_le_bytes()),
        (12, &u32_of(unique.len()).to_le_bytes()),
        (16, &built.parts.to_le_bytes()),
        (20, &built.buckets.to_le_bytes()),
        (24, &built.seed.to_le_bytes()),
        (32, &u32_of(packs.len()).to_le_bytes()),
        (40, &u64_of(packs_at).to_le_bytes()),
        (48, &u64_of(offsets_at).to_le_bytes()),
        (56, &u64_of(pilots_at).to_le_bytes()),
        (64, &u64_of(records_at).to_le_bytes()),
        (72, &u64_of(total).to_le_bytes()),
        (packs_at, &table),
    ];
    for (at, bytes) in fields {
        if let Some(dst) = out.get_mut(at..at.saturating_add(bytes.len())) {
            dst.copy_from_slice(bytes);
        }
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
    for (r, &slot) in unique.iter().zip(&built.slots) {
        let at = us(u64::from(slot)).saturating_mul(SEG_REC_LEN);
        let dst = records
            .get_mut(at..at.saturating_add(SEG_REC_LEN))
            .ctx("record slot out of range")?;
        encode_rec(r, dst);
    }
    Ok(out)
}

fn encode_rec(r: &BlobRec, out: &mut [u8]) {
    let at = (r.offset & MAX_OFFSET)
        | u64::from(r.hdr_len).wrapping_shl(48)
        | u64::from(r.stored).wrapping_shl(56);
    let parts: [&[u8]; 5] = [
        &r.oid.0,
        &r.pack.to_le_bytes(),
        &at.to_le_bytes(),
        &r.entry_len.to_le_bytes(),
        &r.size.to_le_bytes(),
    ];
    let mut pos = 0usize;
    for p in parts {
        if let Some(dst) = out.get_mut(pos..pos.wrapping_add(p.len())) {
            dst.copy_from_slice(p);
        }
        pos = pos.wrapping_add(p.len());
    }
}

#[inline]
fn decode_rec(b: &[u8]) -> Option<BlobRec> {
    let at = le_u64(b, 24)?;
    Some(BlobRec {
        oid: Oid::from_slice(b.get(..20)?)?,
        pack: le_u32(b, 20)?,
        offset: at & MAX_OFFSET,
        hdr_len: (at.wrapping_shr(48) & 0xFF) as u8,
        stored: at.wrapping_shr(56) & 1 == 1,
        entry_len: le_u64(b, 32)?,
        size: le_u64(b, 40)?,
    })
}

/// An open segment index.
pub struct Segment {
    pub id: String,
    region: Region,
    count: u32,
    seed: u64,
    parts: u32,
    buckets: u32,
    offsets_at: usize,
    pilots_at: usize,
    records_at: usize,
    packs: Vec<PackRef>,
}

impl Segment {
    pub fn open(path: &Path, id: &str) -> Result<Segment> {
        let file = File::open(path).ctx(&format!("opening {}", path.display()))?;
        let map = gvfs_mmap::Map::open(&file, true).ctx("mapping segment")?;
        let _ = map.advise(gvfs_mmap::Advice::Random);
        Segment::new(id, Region::Map(map))
    }

    pub fn from_heap(bytes: Vec<u8>, id: &str) -> Result<Segment> {
        Segment::new(id, Region::Heap(bytes))
    }

    fn new(id: &str, region: Region) -> Result<Segment> {
        let b = region.bytes();
        if b.get(..8) != Some(&SEG_MAGIC[..]) || le_u32(b, 8) != Some(SEG_VERSION) {
            crate::bail!("blob segment {id}: not a segment index");
        }
        let h = |at| le_u64(b, at).map(us).ctx("segment header");
        let count = le_u32(b, 12).ctx("segment header")?;
        let parts = le_u32(b, 16).ctx("segment header")?;
        let buckets = le_u32(b, 20).ctx("segment header")?;
        let seed = le_u64(b, 24).ctx("segment header")?;
        let npacks = le_u32(b, 32).ctx("segment header")?;
        let (packs_at, offsets_at, pilots_at, records_at, total) =
            (h(40)?, h(48)?, h(56)?, h(64)?, h(72)?);
        let offsets_len = us(u64::from(parts)).saturating_add(1).saturating_mul(4);
        let pilots_len = us(u64::from(parts))
            .saturating_mul(us(u64::from(buckets)))
            .saturating_mul(2);
        let consistent = total == b.len()
            && packs_at <= offsets_at
            && offsets_at.saturating_add(offsets_len) <= pilots_at
            && pilots_at.saturating_add(pilots_len) <= records_at
            && records_at.saturating_add(us(u64::from(count)).saturating_mul(SEG_REC_LEN)) == total
            && parts > 0;
        if !consistent {
            crate::bail!("blob segment {id}: sections are inconsistent");
        }
        let mut packs = Vec::new();
        let mut at = packs_at;
        for _ in 0..npacks {
            let n = us(u64::from(le_u16(b, at).ctx("segment packs")?));
            let pid = sub(b, at.saturating_add(2), n).ctx("segment packs")?;
            let at2 = at.saturating_add(2).saturating_add(n);
            let len = le_u64(b, at2).ctx("segment packs")?;
            let sha: [u8; 20] = sub(b, at2.saturating_add(8), 20)
                .and_then(|s| s.try_into().ok())
                .ctx("segment packs")?;
            packs.push(PackRef {
                id: String::from_utf8_lossy(pid).into_owned(),
                len,
                sha,
            });
            at = at2.saturating_add(28);
        }
        if at > offsets_at {
            crate::bail!("blob segment {id}: pack table overflows");
        }
        Ok(Segment {
            id: id.to_string(),
            region,
            count,
            seed,
            parts,
            buckets,
            offsets_at,
            pilots_at,
            records_at,
            packs,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        self.region.bytes()
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn packs(&self) -> &[PackRef] {
        &self.packs
    }

    #[inline]
    fn mphf(&self) -> crate::mphf::Mphf<'_> {
        let b = self.region.bytes();
        crate::mphf::Mphf {
            seed: self.seed,
            parts: self.parts,
            buckets: self.buckets,
            offsets: b.get(self.offsets_at..self.pilots_at).unwrap_or(&[]),
            pilots: b.get(self.pilots_at..self.records_at).unwrap_or(&[]),
        }
    }

    #[inline]
    fn rec_at(&self, slot: u32) -> Option<BlobRec> {
        let at = us(u64::from(slot))
            .checked_mul(SEG_REC_LEN)?
            .checked_add(self.records_at)?;
        decode_rec(sub(self.region.bytes(), at, SEG_REC_LEN)?)
    }

    /// The record of `oid`, if this segment has it.
    #[inline]
    pub fn find(&self, oid: &Oid) -> Option<BlobRec> {
        let r = self.rec_at(self.mphf().slot(oid)?)?;
        (r.oid == *oid).then_some(r)
    }

    /// Every record, in slot order.
    pub fn records(&self) -> impl Iterator<Item = BlobRec> + '_ {
        (0..self.count).filter_map(|s| self.rec_at(s))
    }
}

/// What `blobs.bin` says about one segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    pub id: String,
    pub blobs: u64,
    pub index_len: u64,
    /// The packs it indexes, by id.
    pub packs: Vec<String>,
}

/// What `blobs.bin` says about one pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackMeta {
    pub id: String,
    pub len: u64,
    pub sha: [u8; 20],
    pub blobs: u32,
}

/// `blobs.bin`: the blob packs and their segments, replaced by compare-and-swap.
///
/// ```text
/// "GVDBLOBM" | version u32 | generation u64 | flags u32 | tip [20] | since_ts u64
/// | genesis [20]
/// | segments u32 | (id_len u16 | id | blobs u64 | index_len u64 | packs u32 | (len u16 | id)*)*
/// | packs u32 | (id_len u16 | id | len u64 | trailer [20] | blobs u32)*
/// | SHA-256 [32]
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlobManifest {
    pub generation: u64,
    /// The commit the store's genesis was taken at (zero until genesis).
    pub genesis: Oid,
    /// Every blob of this commit's tree is in a pack (`mirror --scope tip` finished). With
    /// the pipeline copying every later window's blobs first, `tip_complete == genesis`
    /// means every commit since genesis has all its blobs in the store.
    pub tip_complete: Option<Oid>,
    /// Old versions referenced since this commit stamp are mirrored too (0: none).
    pub since_ts: u64,
    /// Newest last.
    pub segments: Vec<SegmentMeta>,
    pub packs: Vec<PackMeta>,
}

const BM_MAGIC: &[u8; 8] = b"GVDBLOBM";
const BM_VERSION: u32 = 1;
const FLAG_TIP_COMPLETE: u32 = 1;

fn put_str(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    let n = b.len().min(usize::from(u16::MAX));
    out.extend_from_slice(&u16::try_from(n).unwrap_or(u16::MAX).to_le_bytes());
    out.extend_from_slice(b.get(..n).unwrap_or(b));
}

fn get_str(b: &[u8], at: &mut usize) -> Result<String> {
    let n = us(u64::from(le_u16(b, *at).ctx("truncated blobs.bin")?));
    let s = sub(b, at.saturating_add(2), n).ctx("truncated blobs.bin")?;
    *at = at.saturating_add(2).saturating_add(n);
    Ok(String::from_utf8_lossy(s).into_owned())
}

impl BlobManifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(BM_MAGIC);
        out.extend_from_slice(&BM_VERSION.to_le_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        let flags = if self.tip_complete.is_some() {
            FLAG_TIP_COMPLETE
        } else {
            0
        };
        out.extend_from_slice(&flags.to_le_bytes());
        out.extend_from_slice(&self.tip_complete.unwrap_or(Oid::ZERO).0);
        out.extend_from_slice(&self.since_ts.to_le_bytes());
        out.extend_from_slice(&self.genesis.0);
        out.extend_from_slice(&u32_of(self.segments.len()).to_le_bytes());
        for s in &self.segments {
            put_str(&mut out, &s.id);
            out.extend_from_slice(&s.blobs.to_le_bytes());
            out.extend_from_slice(&s.index_len.to_le_bytes());
            out.extend_from_slice(&u32_of(s.packs.len()).to_le_bytes());
            for p in &s.packs {
                put_str(&mut out, p);
            }
        }
        out.extend_from_slice(&u32_of(self.packs.len()).to_le_bytes());
        for p in &self.packs {
            put_str(&mut out, &p.id);
            out.extend_from_slice(&p.len.to_le_bytes());
            out.extend_from_slice(&p.sha);
            out.extend_from_slice(&p.blobs.to_le_bytes());
        }
        let digest = ring::digest::digest(&ring::digest::SHA256, &out);
        out.extend_from_slice(digest.as_ref());
        out
    }

    pub fn decode(b: &[u8]) -> Result<BlobManifest> {
        let body_len = b.len().checked_sub(32).ctx("blobs.bin too short")?;
        let (body, digest) = b.split_at_checked(body_len).ctx("blobs.bin too short")?;
        if ring::digest::digest(&ring::digest::SHA256, body).as_ref() != digest {
            crate::bail!("blobs.bin checksum mismatch");
        }
        if body.get(..8) != Some(&BM_MAGIC[..]) || le_u32(body, 8) != Some(BM_VERSION) {
            crate::bail!("not a blobs.bin (or an unsupported version)");
        }
        let generation = le_u64(body, 12).ctx("blobs.bin header")?;
        let flags = le_u32(body, 20).ctx("blobs.bin header")?;
        let tip =
            Oid::from_slice(sub(body, 24, 20).ctx("blobs.bin header")?).ctx("blobs.bin header")?;
        let since_ts = le_u64(body, 44).ctx("blobs.bin header")?;
        let genesis =
            Oid::from_slice(sub(body, 52, 20).ctx("blobs.bin header")?).ctx("blobs.bin header")?;
        let mut at = 72usize;
        let nseg = le_u32(body, at).ctx("truncated blobs.bin")?;
        at = at.saturating_add(4);
        let mut segments = Vec::new();
        for _ in 0..nseg {
            let id = get_str(body, &mut at)?;
            let blobs = le_u64(body, at).ctx("truncated blobs.bin")?;
            let index_len = le_u64(body, at.saturating_add(8)).ctx("truncated blobs.bin")?;
            let np = le_u32(body, at.saturating_add(16)).ctx("truncated blobs.bin")?;
            at = at.saturating_add(20);
            let mut packs = Vec::new();
            for _ in 0..np {
                packs.push(get_str(body, &mut at)?);
            }
            segments.push(SegmentMeta {
                id,
                blobs,
                index_len,
                packs,
            });
        }
        let npacks = le_u32(body, at).ctx("truncated blobs.bin")?;
        at = at.saturating_add(4);
        let mut packs = Vec::new();
        for _ in 0..npacks {
            let id = get_str(body, &mut at)?;
            let len = le_u64(body, at).ctx("truncated blobs.bin")?;
            let sha: [u8; 20] = sub(body, at.saturating_add(8), 20)
                .and_then(|s| s.try_into().ok())
                .ctx("truncated blobs.bin")?;
            let blobs = le_u32(body, at.saturating_add(28)).ctx("truncated blobs.bin")?;
            at = at.saturating_add(32);
            packs.push(PackMeta {
                id,
                len,
                sha,
                blobs,
            });
        }
        if at != body.len() {
            crate::bail!("blobs.bin has trailing bytes");
        }
        Ok(BlobManifest {
            generation,
            genesis,
            tip_complete: (flags & FLAG_TIP_COMPLETE != 0).then_some(tip),
            since_ts,
            segments,
            packs,
        })
    }

    /// Every commit since genesis has all its blobs in the store (edges serve only then).
    pub fn complete(&self) -> bool {
        !self.genesis.is_zero() && self.tip_complete == Some(self.genesis)
    }

    /// Total blobs indexed (a blob in two segments counts twice).
    pub fn blobs(&self) -> u64 {
        self.segments.iter().map(|s| s.blobs).sum()
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use std::process::Command;

    fn blob_oid(content: &[u8]) -> Oid {
        let mut h = Sha1::new();
        h.update(format!("blob {}\0", content.len()).as_bytes());
        h.update(content);
        Oid(h.finalize().into())
    }

    fn deflate(data: &[u8]) -> Vec<u8> {
        let mut d = crate::zlib::Deflater::new(6);
        let mut out = vec![0u8; crate::zlib::compress_bound(data.len())];
        let n = d.deflate_parts(&[data], &mut out).unwrap();
        out.truncate(n);
        out
    }

    fn noise(n: usize, seed: u64) -> Vec<u8> {
        let mut s = seed | 1;
        (0..n)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
    }

    /// A pack of raw and stored entries is a valid git pack with exactly those blobs, and
    /// stored ranges address content in place.
    #[test]
    fn blob_pack_segment_and_ranges() {
        let small = b"hello, blob\n".to_vec();
        let text: Vec<u8> = (0..200_000u32)
            .flat_map(|i| format!("line {i}\n").into_bytes())
            .collect();
        let big = noise(3 * STORED_BLOCK + 1234, 42);
        let exact = noise(2 * STORED_BLOCK, 7);
        let empty: Vec<u8> = Vec::new();
        let blobs = [&small, &text, &big, &exact, &empty];

        let mut w = PackWriter::new(Vec::new(), u32_of(blobs.len())).unwrap();
        let mut recs = Vec::new();
        for (i, b) in blobs.iter().enumerate() {
            if i % 2 == 0 {
                w.begin(u64_of(b.len()), Encoding::Stored).unwrap();
                for chunk in b.chunks(10_000) {
                    w.content(chunk).unwrap();
                }
            } else {
                w.begin(u64_of(b.len()), Encoding::Raw).unwrap();
                w.raw(&deflate(b)).unwrap();
            }
            recs.push(w.end(blob_oid(b)).unwrap());
        }
        let (pack, len, sha) = w.finish().unwrap();
        assert_eq!(u64_of(pack.len()), len);
        assert_eq!(&pack[pack.len() - 20..], &sha);

        // git agrees: every blob present, content exact.
        let dir = std::env::temp_dir().join(format!("gvfs-blobpack-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let git = |args: &[&str], input: Option<&[u8]>| {
            let mut c = Command::new("git");
            crate::upstream::hermetic_git_env(&mut c);
            c.env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .args(args)
                .current_dir(&dir)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            let mut child = c.spawn().unwrap();
            if let Some(i) = input {
                std::io::Write::write_all(&mut child.stdin.take().unwrap(), i).unwrap();
            }
            drop(child.stdin.take());
            let out = child.wait_with_output().unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            out.stdout
        };
        git(&["init", "-q", "--bare", "r.git"], None);
        git(
            &["--git-dir=r.git", "index-pack", "--strict", "--stdin"],
            Some(&pack),
        );
        for (b, r) in blobs.iter().zip(&recs) {
            let got = git(
                &["--git-dir=r.git", "cat-file", "blob", &r.oid.to_string()],
                None,
            );
            assert_eq!(&got, *b);
            assert_eq!(r.size, u64_of(b.len()));
        }

        // Segment round trip.
        let packs = vec![PackRef {
            id: "p0".into(),
            len,
            sha,
        }];
        let seg = Segment::from_heap(build_segment(&packs, &recs, 1).unwrap(), "s").unwrap();
        assert_eq!(seg.count(), 5);
        assert_eq!(seg.packs(), &packs[..]);
        for r in &recs {
            assert_eq!(seg.find(&r.oid), Some(*r));
        }
        assert_eq!(seg.find(&blob_oid(b"absent")), None);

        // Stored ranges read content in place.
        let r = recs[2];
        assert!(r.stored);
        let mut ranges = Vec::new();
        for (from, n) in [
            (0u64, 10u64),
            (65_530, 20),
            (100_000, 140_000),
            (196_000, 9_999),
        ] {
            r.stored_ranges(from, n, &mut ranges);
            let mut got = Vec::new();
            for &(at, len) in &ranges {
                got.extend_from_slice(&pack[us(at)..us(at + len)]);
            }
            let end = (from + n).min(r.size);
            assert_eq!(got, &big[us(from)..us(end)], "range {from}+{n}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blobs_bin_roundtrip() {
        let m = BlobManifest {
            generation: 7,
            genesis: Oid([4u8; 20]),
            tip_complete: Some(Oid([3u8; 20])),
            since_ts: 123,
            segments: vec![SegmentMeta {
                id: "seg-1".into(),
                blobs: 10,
                index_len: 999,
                packs: vec!["a".into(), "b".into()],
            }],
            packs: vec![
                PackMeta {
                    id: "a".into(),
                    len: 5,
                    sha: [1; 20],
                    blobs: 4,
                },
                PackMeta {
                    id: "b".into(),
                    len: 6,
                    sha: [2; 20],
                    blobs: 6,
                },
            ],
        };
        let bytes = m.encode();
        assert_eq!(BlobManifest::decode(&bytes).unwrap(), m);
        let empty = BlobManifest::default();
        assert_eq!(BlobManifest::decode(&empty.encode()).unwrap(), empty);
        let mut bad = bytes.clone();
        bad[30] ^= 1;
        assert!(BlobManifest::decode(&bad).is_err());
    }
}
