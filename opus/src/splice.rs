//! **Primitive 2: splice.** Turns slabs (or objects inside them) into ONE valid pack on the
//! wire without inflating or deflating anything. (Building a new *slab* from slabs is the
//! optimal splice, [`crate::repack`], which re-deltifies trees.)
//!
//! A slab's body (`pack[12 .. len-20]`) is a self-contained run of entries: every delta's
//! base is inside it (OFS distances are relative, REF bases are named by id). So:
//!
//! * **Prefetch:** concatenate whole bodies between a fresh 12-byte header (the summed
//!   count) and a SHA-1 trailer. The trailer is the only computed byte; a [`PackPlan`]
//!   computes it once and every client of that plan reuses it.
//! * **Objects:** the requested entries plus their delta-base chains, emitted in slab
//!   order. Runs that stay contiguous are copied verbatim; an OFS_DELTA whose base moved
//!   gets a re-encoded header (a few bytes) in front of its untouched zlib stream.

use std::sync::{Arc, OnceLock};

use sha1::{Digest, Sha1};

use crate::pack::{
    HeaderBytes, OBJ_OFS_DELTA, PACK_HEADER_LEN, PACK_TRAILER_LEN, pack_header, parse_entry_header,
};
use crate::slab::{NO_BASE, Rec, Slab};
use crate::util::{Context, Result, u32_of, u64_of, us};

/// One contiguous piece of an output pack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Piece {
    /// `slabs[slab].pack()[start .. start + len]`.
    Slab { slab: u16, start: u64, len: u64 },
    /// `bytes[start .. start + len]` of the plan's (or request arena's) synthesized bytes.
    Bytes { start: u32, len: u32 },
}

impl Piece {
    pub fn len(&self) -> u64 {
        match *self {
            Piece::Slab { len, .. } => len,
            Piece::Bytes { len, .. } => u64::from(len),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Resolves a piece to its bytes.
#[inline]
pub fn piece_bytes<'a>(p: &Piece, slabs: &'a [Arc<Slab>], bytes: &'a [u8]) -> Option<&'a [u8]> {
    match *p {
        Piece::Slab { slab, start, len } => {
            crate::util::sub(slabs.get(usize::from(slab))?.pack(), us(start), us(len))
        }
        Piece::Bytes { start, len } => crate::util::sub(bytes, start as usize, len as usize),
    }
}

/// A whole-slab splice: `header ++ body(slabs[0]) ++ ... ++ trailer`.
pub struct PackPlan {
    pub slabs: Vec<Arc<Slab>>,
    pub header: [u8; 12],
    /// Total pack length, header and trailer included.
    pub len: u64,
    pub entries: u32,
    /// GVFS pack timestamp (the newest slab's `end_ts`).
    pub timestamp: u64,
    /// How many timeline slabs the pack's content came from (several may have been
    /// repacked into one).
    pub sources: usize,
    trailer: OnceLock<[u8; 20]>,
    idx: OnceLock<Option<Shared>>,
    sizes: OnceLock<Shared>,
}

/// Bytes a plan builds once and many responses send (its `.idx`, its `GSZ1` section): moved
/// into a memfd and mapped, so a response sends them with `sendfile(2)` like the pack itself
/// rather than copying them into the socket (where each byte also costs a socket buffer page
/// the kernel zeroes first). Small sections, or no memfd, stay on the heap.
pub struct Shared {
    file: Option<std::fs::File>,
    bytes: crate::slab::Region,
}

/// Sections at least this large go to a memfd.
const SPILL_MIN: usize = 1 << 20;

impl Shared {
    pub fn new(v: Vec<u8>) -> Shared {
        if v.len() < SPILL_MIN {
            return Shared {
                file: None,
                bytes: crate::slab::Region::Heap(v),
            };
        }
        match Shared::spill(&v) {
            Ok((file, map)) => Shared {
                file: Some(file),
                bytes: crate::slab::Region::Map(map),
            },
            Err(_) => Shared {
                file: None,
                bytes: crate::slab::Region::Heap(v),
            },
        }
    }

    fn spill(v: &[u8]) -> std::io::Result<(std::fs::File, gvfs_mmap::Map)> {
        use std::io::Write;
        let fd = nix::sys::memfd::memfd_create(
            c"gvfs-plan",
            nix::sys::memfd::MemFdCreateFlag::MFD_CLOEXEC,
        )
        .map_err(std::io::Error::from)?;
        let mut file = std::fs::File::from(fd);
        file.write_all(v)?;
        let map = gvfs_mmap::Map::open(&file, false)?;
        if map.bytes() != v {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        Ok((file, map))
    }

    pub fn bytes(&self) -> &[u8] {
        self.bytes.bytes()
    }

    /// The memfd holding [`Shared::bytes`], if they were moved to one.
    pub fn file(&self) -> Option<&std::fs::File> {
        self.file.as_ref()
    }
}

impl PackPlan {
    /// `slabs` oldest first.
    pub fn new(slabs: Vec<Arc<Slab>>) -> PackPlan {
        let n = slabs.len();
        PackPlan::with_sources(slabs, n)
    }

    /// `slabs` oldest first, made from `sources` timeline slabs.
    pub fn with_sources(slabs: Vec<Arc<Slab>>, sources: usize) -> PackPlan {
        let mut entries = 0u32;
        let mut len = u64_of(PACK_HEADER_LEN.saturating_add(PACK_TRAILER_LEN));
        let mut timestamp = 0u64;
        for s in &slabs {
            entries = entries.saturating_add(s.entries());
            let (a, b) = s.body();
            len = len.saturating_add(b.saturating_sub(a));
            timestamp = timestamp.max(s.meta.end_ts);
        }
        PackPlan {
            header: pack_header(entries),
            slabs,
            len,
            entries,
            timestamp,
            sources,
            trailer: OnceLock::new(),
            idx: OnceLock::new(),
            sizes: OnceLock::new(),
        }
    }

    /// The body pieces, in order.
    pub fn bodies(&self) -> impl Iterator<Item = (&Arc<Slab>, u64, u64)> {
        self.slabs.iter().map(|s| {
            let (a, b) = s.body();
            (s, a, b.saturating_sub(a))
        })
    }

    /// The SHA-1 trailer: computed by the first caller, shared by everyone after (callers
    /// arriving meanwhile wait for it instead of hashing again).
    pub fn trailer(&self) -> [u8; 20] {
        *self.trailer.get_or_init(|| {
            let mut h = Sha1::new();
            h.update(self.header);
            for (s, start, len) in self.bodies() {
                h.update(crate::util::sub(s.pack(), us(start), us(len)).unwrap_or(&[]));
            }
            h.finalize().into()
        })
    }

    pub fn trailer_ready(&self) -> bool {
        self.trailer.get().is_some()
    }

    /// The pack's git `.idx` ([`crate::idx`]): built by the first caller, shared after.
    /// `None` if a slab's records do not cover its entries.
    pub fn idx(&self) -> Option<&[u8]> {
        self.idx_shared().map(Shared::bytes)
    }

    /// The memfd holding [`PackPlan::idx`], if it is large enough to have one.
    pub fn idx_file(&self) -> Option<&std::fs::File> {
        self.idx_shared().and_then(Shared::file)
    }

    fn idx_shared(&self) -> Option<&Shared> {
        self.idx
            .get_or_init(|| {
                let sha = self.trailer();
                let _cold = gvfs_alloc_guard::cold_section();
                crate::idx::build(self, &sha).map(Shared::new)
            })
            .as_ref()
    }

    pub fn idx_ready(&self) -> bool {
        self.idx.get().is_some()
    }

    /// The `GSZ1` section for this pack: the union of its slabs' sizes tables, built by
    /// the first caller and shared after.
    pub fn sizes_section(&self) -> &[u8] {
        self.sizes_shared().bytes()
    }

    /// The memfd holding [`PackPlan::sizes_section`], if it is large enough to have one.
    pub fn sizes_file(&self) -> Option<&std::fs::File> {
        self.sizes_shared().file()
    }

    fn sizes_shared(&self) -> &Shared {
        self.sizes.get_or_init(|| {
            let _cold = gvfs_alloc_guard::cold_section();
            let tables: Vec<&crate::sizes::Sizes> = self
                .slabs
                .iter()
                .filter_map(|s| s.sizes().map(|t| t.as_ref()))
                .collect();
            Shared::new(crate::sizes::wire_section(&tables))
        })
    }

    pub fn sizes_ready(&self) -> bool {
        self.sizes.get().is_some()
    }
}

/// An object to send: slab number (in the caller's slab list) and slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjRef {
    pub slab: u16,
    pub slot: u32,
}

/// Scratch for object splices, owned by a request arena (cleared, never shrunk).
#[derive(Default)]
pub struct ObjectSplice {
    /// (slab, entry offset, slot) of every entry to emit; sorted before emitting.
    pub refs: Vec<(u16, u64, u32)>,
    pub pieces: Vec<Piece>,
    pub bytes: Vec<u8>,
    /// Output offset of each emitted entry, parallel to `refs` after sorting.
    out_at: Vec<u64>,
    /// A plan in steps ([`ObjectSplice::plan_step`]): the next ref and its output offset.
    next: usize,
    out: u64,
}

impl ObjectSplice {
    pub fn with_capacity(entries: usize, bytes: usize) -> ObjectSplice {
        ObjectSplice {
            refs: Vec::with_capacity(entries),
            pieces: Vec::with_capacity(entries),
            bytes: Vec::with_capacity(bytes),
            out_at: Vec::with_capacity(entries),
            next: 0,
            out: 0,
        }
    }

    pub fn clear(&mut self) {
        self.refs.clear();
        self.pieces.clear();
        self.bytes.clear();
        self.out_at.clear();
    }

    /// Back to `n` entries and `bytes` header bytes of capacity after a request that grew it
    /// (cold path).
    pub fn shrink(&mut self, n: usize, bytes: usize) {
        let _cold = gvfs_alloc_guard::cold_section();
        if self.bytes.capacity() > bytes.saturating_mul(2) {
            self.bytes.clear();
            self.bytes.shrink_to(bytes);
        }
        for v in [&mut self.out_at] {
            if v.capacity() > n.saturating_mul(2) {
                v.clear();
                v.shrink_to(n);
            }
        }
        if self.refs.capacity() > n.saturating_mul(2) {
            self.refs.clear();
            self.refs.shrink_to(n);
        }
        if self.pieces.capacity() > n.saturating_mul(2) {
            self.pieces.clear();
            self.pieces.shrink_to(n);
        }
    }

    /// Adds an object and its delta-base chain (within its slab). `seen` dedupes by id.
    /// `grow` lets the buffers outgrow their preallocated capacity (commit expansion).
    pub fn add(
        &mut self,
        slabs: &[Arc<Slab>],
        obj: ObjRef,
        seen: &mut crate::arena::OidSet,
        grow: bool,
    ) -> Result<()> {
        let slab = slabs.get(usize::from(obj.slab)).ctx("slab out of range")?;
        let mut slot = obj.slot;
        loop {
            let rec = slab.rec(slot).ctx("record out of range")?;
            if !seen.insert_grow(&rec.oid, grow)? {
                return Ok(());
            }
            if !crate::arena::push(&mut self.refs, (obj.slab, rec.offset, slot), grow) {
                crate::bail!("object batch exceeds the request arena");
            }
            if rec.base == NO_BASE {
                return Ok(());
            }
            slot = rec.base;
        }
    }

    /// Lays out the pack: sorted entries, contiguous runs merged, OFS distances re-encoded
    /// where needed. Returns the entry count; `self.pieces` / `self.bytes` hold the body.
    pub fn plan(&mut self, slabs: &[Arc<Slab>], grow: bool) -> Result<u32> {
        self.plan_begin();
        loop {
            if let Some(n) = self.plan_step(slabs, grow, usize::MAX)? {
                return Ok(n);
            }
        }
    }

    /// [`ObjectSplice::plan`] in steps (a request that yields between them): sorts the
    /// entries, then [`ObjectSplice::plan_step`] until it returns the entry count.
    pub fn plan_begin(&mut self) {
        self.refs.sort_unstable();
        self.pieces.clear();
        self.bytes.clear();
        self.out_at.clear();
        self.next = 0;
        self.out = u64_of(PACK_HEADER_LEN);
    }

    /// Lays out up to `max` more entries; the entry count once all are.
    pub fn plan_step(
        &mut self,
        slabs: &[Arc<Slab>],
        grow: bool,
        max: usize,
    ) -> Result<Option<u32>> {
        let end = self.refs.len().min(self.next.saturating_add(max));
        let mut out = self.out;
        for i in self.next..end {
            let (k, offset, slot) = *self.refs.get(i).ctx("ref")?;
            let slab = slabs.get(usize::from(k)).ctx("slab out of range")?;
            let rec: Rec = slab.rec(slot).ctx("record")?;
            let entry = slab.entry(&rec).ctx("entry out of range")?;
            let h = parse_entry_header(entry).ctx("bad entry header")?;
            if !crate::arena::push(&mut self.out_at, out, grow) {
                crate::bail!("object batch exceeds the request arena");
            }
            let mut verbatim = true;
            if h.kind == OBJ_OFS_DELTA {
                // The base precedes the delta in its slab, so it was emitted before it -
                // unless the same object was already planned from another slab, in which
                // case the delta names its base by id instead.
                let base_offset = offset.saturating_sub(h.base_distance);
                let emitted = self
                    .refs
                    .get(..i)
                    .unwrap_or(&[])
                    .binary_search_by(|r| (r.0, r.1).cmp(&(k, base_offset)))
                    .ok();
                let hdr = match emitted {
                    Some(j) => {
                        let dist = out.saturating_sub(*self.out_at.get(j).ctx("base offset")?);
                        (dist != h.base_distance).then(|| HeaderBytes::ofs_delta(h.size, dist))
                    }
                    None => {
                        let base = slab.rec(rec.base).ctx("delta base record")?;
                        Some(HeaderBytes::ref_delta(h.size, &base.oid))
                    }
                };
                if let Some(hdr) = hdr {
                    let start = u32_of(self.bytes.len());
                    if self.bytes.capacity().saturating_sub(self.bytes.len()) < hdr.as_slice().len()
                    {
                        if !grow {
                            crate::bail!("object batch exceeds the request arena");
                        }
                        let _cold = gvfs_alloc_guard::cold_section();
                        self.bytes.reserve(self.bytes.capacity().max(4096));
                    }
                    self.bytes.extend_from_slice(hdr.as_slice());
                    self.push(
                        Piece::Bytes {
                            start,
                            len: u32_of(hdr.as_slice().len()),
                        },
                        grow,
                    )?;
                    let data_len = entry.len().saturating_sub(h.header_len);
                    self.push(
                        Piece::Slab {
                            slab: k,
                            start: rec.offset.saturating_add(u64_of(h.header_len)),
                            len: u64_of(data_len),
                        },
                        grow,
                    )?;
                    out = out
                        .saturating_add(u64_of(hdr.as_slice().len()))
                        .saturating_add(u64_of(data_len));
                    verbatim = false;
                }
            }
            if verbatim {
                self.push(
                    Piece::Slab {
                        slab: k,
                        start: rec.offset,
                        len: u64::from(rec.entry_len),
                    },
                    grow,
                )?;
                out = out.saturating_add(u64::from(rec.entry_len));
            }
        }
        self.next = end;
        self.out = out;
        Ok((end == self.refs.len()).then(|| u32_of(self.refs.len())))
    }

    /// Appends a piece, merging it into the previous one when contiguous.
    fn push(&mut self, p: Piece, grow: bool) -> Result<()> {
        if let (
            Some(Piece::Slab {
                slab: ps,
                start: pstart,
                len: plen,
            }),
            Piece::Slab { slab, start, len },
        ) = (self.pieces.last_mut(), p)
        {
            if *ps == slab && pstart.saturating_add(*plen) == start {
                *plen = plen.saturating_add(len);
                return Ok(());
            }
        }
        if !crate::arena::push(&mut self.pieces, p, grow) {
            crate::bail!("object batch exceeds the request arena");
        }
        Ok(())
    }
}

/// Resolves object contents (whole or deltified) inside one slab; buffers are reused.
#[derive(Default)]
pub struct Resolver {
    inflater: Option<crate::zlib::Inflater>,
    a: Vec<u8>,
    b: Vec<u8>,
    delta: Vec<u8>,
    chain: Vec<u32>,
}

impl Resolver {
    /// Back to buffers of `bytes` (as [`Resolver::with_capacity`] made them) after an object
    /// far larger grew them (cold path).
    pub fn shrink(&mut self, bytes: usize) {
        for buf in [&mut self.a, &mut self.b, &mut self.delta] {
            if buf.capacity() > bytes.saturating_mul(2) {
                let _cold = gvfs_alloc_guard::cold_section();
                buf.clear();
                buf.shrink_to(bytes);
                buf.resize(bytes, 0);
            }
        }
        if self.chain.capacity() > 8192 {
            let _cold = gvfs_alloc_guard::cold_section();
            self.chain.clear();
            self.chain.shrink_to(4096);
        }
    }

    /// Buffers sized for objects up to `bytes` (larger ones grow them once, off the hot
    /// path's books), with the inflate state warmed up.
    pub fn with_capacity(bytes: usize) -> Resolver {
        let mut r = Resolver {
            inflater: Some(crate::zlib::Inflater::new()),
            ..Resolver::default()
        };
        for buf in [&mut r.a, &mut r.b, &mut r.delta] {
            buf.resize(bytes, 0);
        }
        r.chain.reserve(4096);
        // The first inflate allocates zlib's window: do it now, not in a request.
        let mut d = crate::zlib::Deflater::new(1);
        let mut z = [0u8; 64];
        if let (Ok(n), Some(inf)) = (d.deflate_parts(&[b"warm"], &mut z), r.inflater.as_mut()) {
            let mut out = [0u8; 4];
            let _ = inf.inflate_exact(z.get(..n).unwrap_or(&[]), &mut out);
        }
        r
    }

    /// The object's type and content (valid until the next call).
    pub fn resolve(&mut self, slab: &Slab, slot: u32) -> Result<(u8, &[u8])> {
        let inflater = self.inflater.get_or_insert_with(crate::zlib::Inflater::new);
        self.chain.clear();
        let mut cur = slot;
        loop {
            let rec = slab.rec(cur).ctx("record")?;
            self.chain.push(cur);
            if rec.base == NO_BASE {
                break;
            }
            if self.chain.len() > 10_000 {
                crate::bail!("delta chain too long");
            }
            cur = rec.base;
        }
        let root = slab.rec(*self.chain.last().ctx("chain")?).ctx("record")?;
        let kind = root.kind;
        let mut cur_len = inflate_rec(slab, &root, inflater, &mut self.a)?;
        for &d in self.chain.iter().rev().skip(1) {
            let rec = slab.rec(d).ctx("record")?;
            let dlen = inflate_rec(slab, &rec, inflater, &mut self.delta)?;
            let delta = self.delta.get(..dlen).ctx("delta")?;
            let (_, rsz, _) = crate::delta::delta_sizes(delta).ctx("bad delta")?;
            if self.b.len() < us(rsz) {
                let _cold = gvfs_alloc_guard::cold_section();
                self.b.resize(us(rsz), 0);
            }
            let base = self.a.get(..cur_len).ctx("base")?;
            crate::delta::apply_delta(base, delta, self.b.get_mut(..us(rsz)).ctx("target")?)?;
            std::mem::swap(&mut self.a, &mut self.b);
            cur_len = us(rsz);
        }
        Ok((kind, self.a.get(..cur_len).ctx("content")?))
    }
}

/// Inflates a record's entry data (object or delta) into `buf`; returns its length.
fn inflate_rec(
    slab: &Slab,
    rec: &Rec,
    inflater: &mut crate::zlib::Inflater,
    buf: &mut Vec<u8>,
) -> Result<usize> {
    let entry = slab.entry(rec).ctx("entry")?;
    let h = parse_entry_header(entry).ctx("entry header")?;
    let n = us(h.size);
    if buf.len() < n {
        let _cold = gvfs_alloc_guard::cold_section();
        buf.resize(n, 0);
    }
    inflater.inflate_exact(
        entry.get(h.header_len..).ctx("entry")?,
        buf.get_mut(..n).ctx("buffer")?,
    )?;
    Ok(n)
}
