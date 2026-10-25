//! Core primitive 2: splice slabs into one pack (+ one `.idx`).
//!
//! Slabs never share an object and every delta's base is inside its own slab, so a valid
//! pack of several slabs is just `[12-byte header][body₁][body₂]…[SHA-1]`: the bodies are
//! copied by reference (`Bytes::slice` of the mapped file), and the only CPU is the SHA-1.
//! The `.idx` is a k-way merge of the members' sorted tables: the OID and CRC tables come
//! out as runs sliced straight from the source maps, offsets are rebased by each body's
//! position (the first member keeps its offsets verbatim, which is the 3 GiB baseline in a
//! cold clone), and large-offset indices are renumbered. Prefetch responses, pipeline
//! rollups, weekly deltas and spliced baselines all go through this one module.

use core::fmt;
use std::io::Write;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use sha1::{Digest, Sha1};

use crate::idx::{IDX_MAGIC, IDX_VERSION, IdxView, LARGE_OFFSET_THRESHOLD, idx_len};
use crate::mphf::{Builder, Loc, file_len_for_keys};
use crate::oid::Oid;
use crate::pack::{PACK_HEADER_LEN, PACK_TRAILER_LEN, pack_header};
use crate::slab::Slab;

pub const MAX_SPLICE: usize = 64;
/// Largest zero-copy frame handed to the socket.
pub const FRAME_BYTES: usize = 4 << 20;
/// Largest generated (copied) frame: offsets and large offsets are rewritten in chunks of
/// this many bytes so the staging buffer is never outgrown.
pub const GEN_FRAME_BYTES: usize = 64 << 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpliceError {
    TooManySlabs,
    TooManyObjects,
    BadIdx,
    Io,
    Mphf,
}

impl fmt::Display for SpliceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TooManySlabs => "too many slabs in one splice",
            Self::TooManyObjects => "spliced pack would exceed 2^32 objects",
            Self::BadIdx => "member idx is corrupt",
            Self::Io => "write failed",
            Self::Mphf => "mphf build failed",
        })
    }
}

impl std::error::Error for SpliceError {}

/// A fixed-capacity list of slabs (no allocation per request).
#[derive(Debug, Clone)]
pub struct SlabList {
    items: [Option<Arc<Slab>>; MAX_SPLICE],
    len: usize,
}

impl Default for SlabList {
    fn default() -> Self {
        Self::new()
    }
}

impl SlabList {
    pub fn new() -> Self {
        Self {
            items: [const { None }; MAX_SPLICE],
            len: 0,
        }
    }

    pub fn push(&mut self, s: Arc<Slab>) -> Result<(), SpliceError> {
        let slot = self
            .items
            .get_mut(self.len)
            .ok_or(SpliceError::TooManySlabs)?;
        *slot = Some(s);
        self.len = self.len.saturating_add(1);
        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn get(&self, i: usize) -> Option<&Slab> {
        self.items.get(i)?.as_deref()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Slab> {
        self.items
            .iter()
            .take(self.len)
            .filter_map(|s| s.as_deref())
    }
}

/// Everything known up front about a spliced pack (lengths go into the GPRE header).
#[derive(Debug, Clone)]
pub struct Plan {
    pub slabs: SlabList,
    pub object_count: u32,
    pub pack_len: u64,
    pub idx_len: u64,
    pub n_large: u64,
    /// Max `end_ts` of the members: the client's next `lastPackTimestamp`.
    pub timestamp: u64,
    /// Min `start_ts` of the members: with `timestamp`, the interval this pack covers. A
    /// client hook may unlink any local prefetch pack whose interval lies inside it (every
    /// object of such a pack is in this one).
    pub start_ts: u64,
    /// Rebase shift per member: new offset = old offset + shift.
    shift: [u64; MAX_SPLICE],
    /// Large-offset indices of member k start here.
    large_base: [u64; MAX_SPLICE],
}

impl Plan {
    pub fn new(slabs: SlabList) -> Result<Self, SpliceError> {
        let mut count: u64 = 0;
        let mut pack_len: u64 = PACK_HEADER_LEN as u64;
        let mut shift = [0u64; MAX_SPLICE];
        let mut large_base = [0u64; MAX_SPLICE];
        let mut n_large: u64 = 0;
        let mut timestamp = 0u64;
        let mut start_ts = u64::MAX;
        for (k, s) in slabs.iter().enumerate() {
            let idx = s.idx().ok_or(SpliceError::BadIdx)?;
            // Member k's body starts at pack_len; its old offsets started at 12.
            let sh = pack_len.saturating_sub(PACK_HEADER_LEN as u64);
            if let Some(x) = shift.get_mut(k) {
                *x = sh;
            }
            if let Some(x) = large_base.get_mut(k) {
                *x = n_large;
            }
            n_large = n_large.saturating_add(count_large(&idx, sh));
            count = count.saturating_add(idx.n as u64);
            pack_len = pack_len.saturating_add(s.body_len());
            timestamp = timestamp.max(s.meta.end_ts);
            start_ts = start_ts.min(s.meta.start_ts);
        }
        if start_ts == u64::MAX {
            start_ts = 0;
        }
        pack_len = pack_len.saturating_add(PACK_TRAILER_LEN as u64);
        let object_count = u32::try_from(count).map_err(|_| SpliceError::TooManyObjects)?;
        Ok(Self {
            slabs,
            object_count,
            pack_len,
            idx_len: idx_len(count, n_large),
            n_large,
            timestamp,
            start_ts,
            shift,
            large_base,
        })
    }

    #[inline]
    fn shift(&self, k: usize) -> u64 {
        self.shift.get(k).copied().unwrap_or(0)
    }

    #[inline]
    fn large_base(&self, k: usize) -> u64 {
        self.large_base.get(k).copied().unwrap_or(0)
    }

    /// Whether a single member is passed through untouched (its own trailer and idx).
    pub fn is_passthrough(&self) -> bool {
        self.slabs.len() == 1
    }
}

/// How many of `idx`'s offsets land at or above 2^31 after adding `shift`.
fn count_large(idx: &IdxView<'_>, shift: u64) -> u64 {
    if shift == 0 {
        return idx.large_count() as u64;
    }
    let mut n = 0u64;
    for i in 0..idx.n {
        if idx.offset(i).unwrap_or(0).saturating_add(shift) >= LARGE_OFFSET_THRESHOLD {
            n = n.saturating_add(1);
        }
    }
    n
}

/// k-way merge over the members' sorted OID tables, producing runs.
#[derive(Debug, Clone)]
pub struct Merge {
    cursor: [u32; MAX_SPLICE],
}

/// A run of `len` consecutive entries of member `slab` starting at sorted index `start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Run {
    pub slab: usize,
    pub start: usize,
    pub len: usize,
}

impl Default for Merge {
    fn default() -> Self {
        Self::new()
    }
}

impl Merge {
    pub fn new() -> Self {
        Self {
            cursor: [0u32; MAX_SPLICE],
        }
    }

    pub fn reset(&mut self) {
        self.cursor = [0u32; MAX_SPLICE];
    }

    /// The next run in global OID order, or `None` when every member is exhausted.
    pub fn next_run(&mut self, slabs: &SlabList, max_len: usize) -> Option<Run> {
        let mut best: Option<(usize, &[u8])> = None;
        let mut second: Option<&[u8]> = None;
        for (k, s) in slabs.iter().enumerate() {
            let idx = s.idx()?;
            let c = self.cursor.get(k).copied().unwrap_or(0) as usize;
            if c >= idx.n {
                continue;
            }
            let head = idx.oid_bytes(c)?;
            match best {
                None => best = Some((k, head)),
                Some((_, b)) if head < b => {
                    second = Some(b);
                    best = Some((k, head));
                }
                Some(_) => {
                    if second.is_none_or(|s| head < s) {
                        second = Some(head);
                    }
                }
            }
        }
        let (k, _) = best?;
        let s = slabs.get(k)?;
        let idx = s.idx()?;
        let start = self.cursor.get(k).copied().unwrap_or(0) as usize;
        // Extend the run while this member's OIDs stay below every other head.
        let limit = idx.n.min(start.saturating_add(max_len));
        let end = match second {
            None => limit,
            Some(bound) => {
                let (mut lo, mut hi) = (start.saturating_add(1), limit);
                while lo < hi {
                    let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
                    if idx.oid_bytes(mid).is_some_and(|o| o < bound) {
                        lo = mid.saturating_add(1);
                    } else {
                        hi = mid;
                    }
                }
                lo
            }
        };
        if let Some(c) = self.cursor.get_mut(k) {
            *c = end as u32;
        }
        Some(Run {
            slab: k,
            start,
            len: end.saturating_sub(start),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    PackHeader,
    Body {
        slab: usize,
        at: usize,
    },
    PackTrailer,
    IdxHeader,
    IdxOids,
    IdxCrcs,
    IdxOffsets,
    /// Large-offset table, member by member, `at` entries into member `slab`.
    IdxLarge {
        slab: usize,
        at: usize,
    },
    IdxTrailer,
    Done,
}

/// Produces the spliced pack and idx as a sequence of `Bytes` frames.
pub struct SpliceStream {
    plan: Plan,
    phase: Phase,
    merge: Merge,
    pack_hash: Sha1,
    idx_hash: Sha1,
    /// Known in advance (cache) or computed while streaming the pack.
    pack_sha: Option<[u8; 20]>,
    idx_sha: Option<[u8; 20]>,
    large_seen: u64,
    pub with_idx: bool,
}

impl SpliceStream {
    pub fn new(plan: Plan, known: Option<([u8; 20], [u8; 20])>) -> Self {
        let (pack_sha, idx_sha) = match known {
            Some((p, i)) => (Some(p), Some(i)),
            None => (None, None),
        };
        // A single member needs no hashing at all: its own trailer and idx are reused.
        let (pack_sha, idx_sha) = if plan.is_passthrough() {
            let s = plan.slabs.get(0);
            (
                s.map(Slab::trailer),
                s.and_then(|s| s.idx.last_chunk::<20>().copied()),
            )
        } else {
            (pack_sha, idx_sha)
        };
        Self {
            plan,
            phase: Phase::PackHeader,
            merge: Merge::new(),
            pack_hash: Sha1::new(),
            idx_hash: Sha1::new(),
            pack_sha,
            idx_sha,
            large_seen: 0,
            with_idx: true,
        }
    }

    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// True once the last frame has been produced.
    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// Final checksums (available once the stream is done), for a trailer cache.
    pub fn checksums(&self) -> Option<([u8; 20], [u8; 20])> {
        Some((self.pack_sha?, self.idx_sha?))
    }

    fn emit_pack(&mut self, b: Bytes) -> Bytes {
        if self.pack_sha.is_none() {
            self.pack_hash.update(&b);
        }
        b
    }

    fn emit_idx(&mut self, b: Bytes) -> Bytes {
        if self.idx_sha.is_none() {
            self.idx_hash.update(&b);
        }
        b
    }

    /// Next frame, or `None` at the end. `buf` is scratch for generated frames; frames
    /// of slab bytes are zero-copy slices.
    pub fn next_frame(&mut self, buf: &mut BytesMut) -> Option<Bytes> {
        loop {
            match self.phase {
                Phase::PackHeader => {
                    self.phase = Phase::Body { slab: 0, at: 0 };
                    if self.plan.is_passthrough() {
                        // Whole pack verbatim, including its header and trailer.
                        self.phase = Phase::IdxHeader;
                        let s = self.plan.slabs.get(0)?;
                        return Some(s.pack.clone());
                    }
                    let hdr = pack_header(self.plan.object_count);
                    buf.extend_from_slice(&hdr);
                    return Some(self.emit_pack(buf.split().freeze()));
                }
                Phase::Body { slab, at } => {
                    let Some(s) = self.plan.slabs.get(slab) else {
                        self.phase = Phase::PackTrailer;
                        continue;
                    };
                    let body = s.body();
                    if at >= body.len() {
                        self.phase = Phase::Body {
                            slab: slab.saturating_add(1),
                            at: 0,
                        };
                        continue;
                    }
                    let end = body.len().min(at.saturating_add(FRAME_BYTES));
                    self.phase = Phase::Body { slab, at: end };
                    return Some(self.emit_pack(body.slice(at..end)));
                }
                Phase::PackTrailer => {
                    self.phase = if self.with_idx {
                        Phase::IdxHeader
                    } else {
                        Phase::Done
                    };
                    let sha = match self.pack_sha {
                        Some(s) => s,
                        None => {
                            let d: [u8; 20] = self.pack_hash.clone().finalize().into();
                            self.pack_sha = Some(d);
                            d
                        }
                    };
                    buf.extend_from_slice(&sha);
                    return Some(buf.split().freeze());
                }
                Phase::IdxHeader => {
                    if !self.with_idx {
                        self.phase = Phase::Done;
                        continue;
                    }
                    self.merge.reset();
                    self.phase = Phase::IdxOids;
                    if self.plan.is_passthrough() {
                        self.phase = Phase::Done;
                        let s = self.plan.slabs.get(0)?;
                        return Some(s.idx.clone());
                    }
                    buf.extend_from_slice(&IDX_MAGIC);
                    buf.extend_from_slice(&IDX_VERSION.to_be_bytes());
                    let mut fan = [0u32; 256];
                    for s in self.plan.slabs.iter() {
                        if let Some(idx) = s.idx() {
                            for (b, f) in fan.iter_mut().enumerate() {
                                *f = f.saturating_add(idx.fanout(b as u8));
                            }
                        }
                    }
                    for f in fan {
                        buf.extend_from_slice(&f.to_be_bytes());
                    }
                    return Some(self.emit_idx(buf.split().freeze()));
                }
                Phase::IdxOids => match self.merge.next_run(&self.plan.slabs, FRAME_BYTES / 20) {
                    Some(run) => {
                        let s = self.plan.slabs.get(run.slab)?;
                        let idx = s.idx()?;
                        let start = idx_table_offset(&s.idx, idx.oid_table())?
                            .saturating_add(run.start.saturating_mul(20));
                        let frame = s
                            .idx
                            .slice(start..start.saturating_add(run.len.saturating_mul(20)));
                        return Some(self.emit_idx(frame));
                    }
                    None => {
                        self.merge.reset();
                        self.phase = Phase::IdxCrcs;
                    }
                },
                Phase::IdxCrcs => match self.merge.next_run(&self.plan.slabs, FRAME_BYTES / 4) {
                    Some(run) => {
                        let s = self.plan.slabs.get(run.slab)?;
                        let idx = s.idx()?;
                        let start = idx_table_offset(&s.idx, idx.crc_table())?
                            .saturating_add(run.start.saturating_mul(4));
                        let frame = s
                            .idx
                            .slice(start..start.saturating_add(run.len.saturating_mul(4)));
                        return Some(self.emit_idx(frame));
                    }
                    None => {
                        self.merge.reset();
                        self.large_seen = 0;
                        self.phase = Phase::IdxOffsets;
                    }
                },
                Phase::IdxOffsets => {
                    match self.merge.next_run(&self.plan.slabs, GEN_FRAME_BYTES / 4) {
                        Some(run) => {
                            let s = self.plan.slabs.get(run.slab)?;
                            let idx = s.idx()?;
                            let shift = self.plan.shift(run.slab);
                            let large_base = self.plan.large_base(run.slab);
                            // Members that keep their offsets *and* own the first large entries
                            // can pass their table through untouched.
                            if shift == 0 && large_base == 0 {
                                let start = idx_table_offset(&s.idx, offsets_table(&idx))?
                                    .saturating_add(run.start.saturating_mul(4));
                                let frame = s
                                    .idx
                                    .slice(start..start.saturating_add(run.len.saturating_mul(4)));
                                return Some(self.emit_idx(frame));
                            }
                            buf.reserve(run.len.saturating_mul(4));
                            for i in run.start..run.start.saturating_add(run.len) {
                                let off = idx.offset(i).unwrap_or(0).saturating_add(shift);
                                let v = if off >= LARGE_OFFSET_THRESHOLD {
                                    // Large entries of member k are numbered in its OID order.
                                    let j = large_base.saturating_add(large_rank(&idx, i, shift));
                                    0x8000_0000u32 | (j as u32)
                                } else {
                                    off as u32
                                };
                                buf.extend_from_slice(&v.to_be_bytes());
                            }
                            return Some(self.emit_idx(buf.split().freeze()));
                        }
                        None => {
                            self.phase = Phase::IdxLarge { slab: 0, at: 0 };
                        }
                    }
                }
                Phase::IdxLarge { slab, at } => {
                    // Member by member, each in its own OID order (matches `large_rank`),
                    // in bounded chunks. A member that keeps its offsets passes its own
                    // large table through by reference.
                    let Some(s) = self.plan.slabs.get(slab) else {
                        self.phase = Phase::IdxTrailer;
                        continue;
                    };
                    let Some(idx) = s.idx() else {
                        self.phase = Phase::IdxLarge {
                            slab: slab.saturating_add(1),
                            at: 0,
                        };
                        continue;
                    };
                    let shift = self.plan.shift(slab);
                    if shift == 0 {
                        self.phase = Phase::IdxLarge {
                            slab: slab.saturating_add(1),
                            at: 0,
                        };
                        let table = idx.large_table();
                        if table.is_empty() {
                            continue;
                        }
                        let start = idx_table_offset(&s.idx, table)?;
                        return Some(
                            self.emit_idx(s.idx.slice(start..start.saturating_add(table.len()))),
                        );
                    }
                    let end = idx.n.min(at.saturating_add(GEN_FRAME_BYTES / 4));
                    for i in at..end {
                        let off = idx.offset(i).unwrap_or(0).saturating_add(shift);
                        if off >= LARGE_OFFSET_THRESHOLD {
                            buf.extend_from_slice(&off.to_be_bytes());
                        }
                    }
                    self.phase = if end >= idx.n {
                        Phase::IdxLarge {
                            slab: slab.saturating_add(1),
                            at: 0,
                        }
                    } else {
                        Phase::IdxLarge { slab, at: end }
                    };
                    if buf.is_empty() {
                        continue;
                    }
                    return Some(self.emit_idx(buf.split().freeze()));
                }
                Phase::IdxTrailer => {
                    self.phase = Phase::Done;
                    let pack_sha = self.pack_sha?;
                    buf.extend_from_slice(&pack_sha);
                    let head = buf.split().freeze();
                    let head = self.emit_idx(head);
                    let idx_sha = match self.idx_sha {
                        Some(s) => s,
                        None => {
                            let d: [u8; 20] = self.idx_hash.clone().finalize().into();
                            self.idx_sha = Some(d);
                            d
                        }
                    };
                    buf.extend_from_slice(&head);
                    buf.extend_from_slice(&idx_sha);
                    return Some(buf.split().freeze());
                }
                Phase::Done => return None,
            }
        }
    }
}

/// Byte offset of `table` (a sub-slice of `idx`) within `idx`.
#[inline]
fn idx_table_offset(idx: &Bytes, table: &[u8]) -> Option<usize> {
    let base = idx.as_ptr() as usize;
    let at = table.as_ptr() as usize;
    at.checked_sub(base)
        .filter(|o| o.saturating_add(table.len()) <= idx.len())
}

#[inline]
fn offsets_table<'a>(idx: &IdxView<'a>) -> &'a [u8] {
    idx.offsets_table()
}

/// Rank of entry `i` among the member's large (post-shift) offsets, in OID order.
fn large_rank(idx: &IdxView<'_>, i: usize, shift: u64) -> u64 {
    if shift == 0 {
        // Already-large entries keep their original index.
        return idx.large_index(i).map(u64::from).unwrap_or(0);
    }
    let mut r = 0u64;
    for j in 0..i {
        if idx.offset(j).unwrap_or(0).saturating_add(shift) >= LARGE_OFFSET_THRESHOLD {
            r = r.saturating_add(1);
        }
    }
    r
}

/// Writes the spliced pack and idx to files/buffers (pipeline rollups). Returns the plan's
/// checksums.
pub fn write_splice<W1: Write, W2: Write>(
    plan: Plan,
    pack_w: &mut W1,
    idx_w: &mut W2,
) -> Result<([u8; 20], [u8; 20]), SpliceError> {
    let mut stream = SpliceStream::new(plan, None);
    let mut buf = BytesMut::with_capacity(1 << 20);
    let mut in_idx = false;
    let mut pack_written = 0u64;
    let pack_len = stream.plan().pack_len;
    while let Some(frame) = stream.next_frame(&mut buf) {
        if !in_idx {
            pack_w.write_all(&frame).map_err(|_| SpliceError::Io)?;
            pack_written = pack_written.saturating_add(frame.len() as u64);
            if pack_written >= pack_len {
                in_idx = true;
            }
        } else {
            idx_w.write_all(&frame).map_err(|_| SpliceError::Io)?;
        }
    }
    stream.checksums().ok_or(SpliceError::Io)
}

/// Builds the `.mphf` of a spliced pack from the members' idx tables and locations.
pub fn write_merged_mphf<W: Write>(
    plan: &Plan,
    w: &mut W,
    builder: &mut Builder,
) -> Result<u64, SpliceError> {
    let n = plan.object_count as usize;
    let mut oids: Vec<Oid> = Vec::with_capacity(n);
    let mut locs: Vec<Loc> = Vec::with_capacity(n);
    for (k, s) in plan.slabs.iter().enumerate() {
        let idx = s.idx().ok_or(SpliceError::BadIdx)?;
        let shift = plan.shift(k);
        for i in 0..idx.n {
            let oid = idx.oid(i).ok_or(SpliceError::BadIdx)?;
            let (_, loc) = s.lookup(&oid).ok_or(SpliceError::BadIdx)?;
            oids.push(oid);
            locs.push(Loc {
                offset: loc.offset.saturating_add(shift),
                len: loc.len,
            });
        }
    }
    let mut out = vec![0u8; file_len_for_keys(n as u64)];
    let loc_of = |i: usize| locs.get(i).copied().unwrap_or(Loc { offset: 0, len: 0 });
    let written = builder
        .build(&oids, &loc_of, plan.pack_len, &mut out)
        .map_err(|_| SpliceError::Mphf)?;
    w.write_all(out.get(..written).unwrap_or(&[]))
        .map_err(|_| SpliceError::Io)?;
    Ok(written as u64)
}
