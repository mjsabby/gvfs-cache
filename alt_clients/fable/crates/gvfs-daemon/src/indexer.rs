//! Pack indexer: turns the bytes of a received pack into an entry table with every
//! object's id, type, CRC and byte range, deduplicates against existing slabs, and emits the
//! slab pack (verbatim when nothing was dropped).
//!
//! * **Scan** (sequential): walks the entries, measuring each zlib stream to find the next
//!   boundary (zlib does not record compressed lengths), computing CRC-32s and linking
//!   OFS_DELTA bases. Output is discarded through a cache-resident scratch buffer.
//! * **Resolve** (parallel): every non-delta entry is the root of a delta tree. Workers pull
//!   roots from a shared counter and walk each tree depth-first with the objects of the
//!   current path kept in a per-worker bump [`Arena`] (git's `index-pack` strategy), hashing
//!   every object. If a path does not fit, the ancestors are evicted and recomputed from the
//!   root when the walk returns to them. Objects already present in other slabs are flagged
//!   as duplicates; a delta whose base is a duplicate is deflated as a full object into the
//!   worker's side arena right there, while its content is still on the stack.
//! * **Emit**: with no duplicates and no REF_DELTAs the received bytes *are* the slab pack
//!   (the baseline case: zero copies). Otherwise kept entries are copied with OFS_DELTA
//!   distances re-encoded for their new positions and converted objects spliced in.

use core::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use sha1::{Digest, Sha1};

use crate::arena::{Arena, OutOfCapacity, Span};
use crate::delta::{DeltaError, apply_delta, delta_sizes};
use crate::hfs::parse_hfs_pointer;
use crate::oid::Oid;
use crate::pack::{
    DeltaBase, ObjType, PACK_HEADER_LEN, PACK_TRAILER_LEN, PackError, encode_entry_header,
    encode_ofs_delta_header, pack_header, parse_entry_header, parse_pack_header,
};
use crate::zlib::{Deflater, Inflater, ZError, compress_bound};

pub const NONE: u32 = u32::MAX;
pub const F_DUP: u8 = 1;
/// Must be emitted as a full object (its base is dropped or comes after it).
pub const F_FULL: u8 = 2;
/// A full-object encoding of this entry sits in a worker's side arena.
pub const F_CONVERTED: u8 = 4;
const MAX_DEPTH: usize = 4096;
/// Deflate level for converted objects (git's default).
const CONVERT_LEVEL: u32 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexError {
    Pack(PackError),
    Zlib(ZError),
    Delta(DeltaError),
    Trailer,
    BaseNotFound,
    RefDeltaUnresolved,
    DepthExceeded,
    ObjectTooLarge,
    SideArenaFull,
    SizeMismatch,
    Io,
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pack(e) => write!(f, "pack: {e}"),
            Self::Zlib(e) => write!(f, "zlib: {e}"),
            Self::Delta(e) => write!(f, "delta: {e}"),
            Self::Trailer => f.write_str("pack trailer does not match its contents"),
            Self::BaseNotFound => f.write_str("OFS_DELTA base offset is not an entry"),
            Self::RefDeltaUnresolved => f.write_str("REF_DELTA base is not in the pack"),
            Self::DepthExceeded => f.write_str("delta chain deeper than 4096"),
            Self::ObjectTooLarge => {
                f.write_str("object does not fit the resolve arena (raise --arena-mib)")
            }
            Self::SideArenaFull => {
                f.write_str("converted objects exceed the side arena's 4 GiB per worker")
            }
            Self::SizeMismatch => f.write_str("entry size does not match its header"),
            Self::Io => f.write_str("write failed"),
        }
    }
}

impl std::error::Error for IndexError {}

impl From<PackError> for IndexError {
    fn from(e: PackError) -> Self {
        Self::Pack(e)
    }
}
impl From<ZError> for IndexError {
    fn from(e: ZError) -> Self {
        Self::Zlib(e)
    }
}
impl From<DeltaError> for IndexError {
    fn from(e: DeltaError) -> Self {
        Self::Delta(e)
    }
}
impl From<OutOfCapacity> for IndexError {
    fn from(_: OutOfCapacity) -> Self {
        Self::ObjectTooLarge
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Entry {
    pub offset: u64,
    /// Inflated size of the zlib payload (the delta's size for delta entries).
    pub size: u64,
    /// Whole entry length (header + zlib stream).
    pub len: u32,
    /// Base entry index for deltas, else [`NONE`].
    pub base: u32,
    pub crc: u32,
    pub hdr_len: u8,
    /// Stored type code (1–4, 6 = OFS_DELTA, 7 = REF_DELTA).
    pub kind: u8,
    /// Resolved object type (1–4).
    pub real_kind: u8,
    pub flags: u8,
}

/// Counters filled during resolution.
#[derive(Debug, Default)]
pub struct Stats {
    pub pointer_blobs: AtomicU64,
    pub other_blobs: AtomicU64,
    pub other_blob_bytes: AtomicU64,
    pub duplicates: AtomicU64,
    pub converted: AtomicU64,
    pub evictions: AtomicU64,
    pub max_depth: AtomicU64,
}

/// The indexed pack: `entries[i]` and `oids[i]` describe the i-th entry of the received
/// pack; after [`emit`] they describe the written slab pack (dropped entries flagged).
#[derive(Debug, Default)]
pub struct IndexedPack {
    pub entries: Vec<Entry>,
    pub oids: Vec<Oid>,
    pub stats: Stats,
    pub needs_rewrite: bool,
    /// Entries kept (not duplicates).
    pub kept: usize,
    /// Converted (de-deltified) objects, one arena per resolve worker.
    pub side: Vec<Arena>,
    /// `(entry index, side ref)` for every converted entry, sorted by entry index.
    pub side_refs: Vec<(u32, u64)>,
}

/// Sizing of the per-worker memory and the delta policy.
#[derive(Debug, Clone, Copy)]
pub struct ResolveConfig {
    pub workers: usize,
    /// Bump arena for the objects of the current delta path, per worker.
    pub arena_bytes: usize,
    /// Initial side arena for converted (de-deltified) objects, per worker; it grows when a
    /// repository needs more (the indexer is not a hot path).
    pub side_bytes: usize,
    /// Store commits, tags and (pointer) blobs as full objects even when the upstream sent
    /// them as deltas: they are tiny, deltas save nothing on them, and every point lookup
    /// becomes a one-hop read. Trees keep their upstream deltas (15x smaller).
    pub full_non_trees: bool,
    /// Inflated-size ceiling for that policy: larger non-tree objects (real source blobs in
    /// a repository that is not GitHFS) keep their upstream deltas too.
    pub full_max_bytes: usize,
}

/// Default [`ResolveConfig::full_max_bytes`]: covers `~~Git-hfs 1.0` pointers (~100 bytes),
/// commits and tags, not source files.
pub const DEFAULT_FULL_MAX_BYTES: usize = 4096;

/// Largest side arena a worker may grow to (a side ref addresses 32 bits).
const SIDE_ARENA_MAX: usize = 0xFFFF_FFFF;

impl Default for ResolveConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            workers: cpus.clamp(1, 16),
            arena_bytes: 256 << 20,
            side_bytes: 64 << 20,
            full_non_trees: true,
            full_max_bytes: DEFAULT_FULL_MAX_BYTES,
        }
    }
}

/// Scans `pack` (a complete pack with header and trailer), filling the entry table.
pub fn scan(pack: &[u8], scratch: &mut [u8]) -> Result<IndexedPack, IndexError> {
    let n = parse_pack_header(pack)? as usize;
    let body_end = pack
        .len()
        .checked_sub(PACK_TRAILER_LEN)
        .ok_or(PackError::Truncated)?;
    let mut out = IndexedPack {
        entries: Vec::with_capacity(n),
        oids: vec![Oid::ZERO; n],
        ..IndexedPack::default()
    };
    let mut inflater = Inflater::new();
    let mut pos = PACK_HEADER_LEN;
    for _ in 0..n {
        let rest = pack.get(pos..body_end).ok_or(PackError::Truncated)?;
        let hdr = parse_entry_header(rest)?;
        let zsrc = rest.get(hdr.header_len..).ok_or(PackError::Truncated)?;
        let (consumed, produced) = inflater.measure(zsrc, scratch)?;
        if produced as u64 != hdr.size {
            return Err(IndexError::SizeMismatch);
        }
        let len = hdr.header_len.saturating_add(consumed);
        let raw = rest.get(..len).ok_or(PackError::Truncated)?;
        let mut h = crc32fast::Hasher::new();
        h.update(raw);
        let base = match hdr.base {
            DeltaBase::None => NONE,
            DeltaBase::Ofs(dist) => {
                let base_off = (pos as u64)
                    .checked_sub(dist)
                    .ok_or(IndexError::BaseNotFound)?;
                let i = out.entries.partition_point(|e| e.offset < base_off);
                match out.entries.get(i) {
                    Some(e) if e.offset == base_off => i as u32,
                    _ => return Err(IndexError::BaseNotFound),
                }
            }
            // Linked after the scan, once the non-delta object ids are known.
            DeltaBase::Ref(oid) => {
                if let Some(slot) = out.oids.get_mut(out.entries.len()) {
                    *slot = oid;
                }
                NONE
            }
        };
        out.entries.push(Entry {
            offset: pos as u64,
            size: hdr.size,
            len: u32::try_from(len).map_err(|_| PackError::SizeOverflow)?,
            base,
            crc: h.finalize(),
            hdr_len: hdr.header_len as u8,
            kind: hdr.kind.code(),
            real_kind: if hdr.kind.is_delta() {
                0
            } else {
                hdr.kind.code()
            },
            flags: 0,
        });
        pos = pos.saturating_add(len);
    }
    if pos != body_end {
        return Err(PackError::Truncated.into());
    }
    let digest: [u8; 20] = Sha1::digest(pack.get(..body_end).unwrap_or(&[])).into();
    if pack.get(body_end..) != Some(&digest[..]) {
        return Err(IndexError::Trailer);
    }
    Ok(out)
}

#[inline]
fn hash_object(kind: ObjType, content: &[u8]) -> Oid {
    let mut hdr = [0u8; 32];
    let hl = crate::pack::loose_header(kind, content.len() as u64, &mut hdr);
    let mut h = Sha1::new();
    h.update(hdr.get(..hl).unwrap_or(&[]));
    h.update(content);
    Oid(h.finalize().into())
}

/// Per-entry results written by exactly one worker each (plain loads after the join).
struct Resolved {
    w: [AtomicU64; 4],
}

impl Resolved {
    const fn new() -> Self {
        Self {
            w: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    fn store(&self, oid: &Oid, kind: u8, flags: u8, side: u64) {
        let w0 = oid.word(0);
        let w1 = oid.word(8);
        let tail = u64::from(u32::from_le_bytes([
            oid.0[16], oid.0[17], oid.0[18], oid.0[19],
        ]));
        let w2 = tail | (u64::from(kind) << 32) | (u64::from(flags) << 40);
        self.w[0].store(w0, Ordering::Relaxed);
        self.w[1].store(w1, Ordering::Relaxed);
        self.w[2].store(w2, Ordering::Relaxed);
        self.w[3].store(side, Ordering::Relaxed);
    }

    fn load(&self) -> (Oid, u8, u8, u64) {
        let w0 = self.w[0].load(Ordering::Relaxed);
        let w1 = self.w[1].load(Ordering::Relaxed);
        let w2 = self.w[2].load(Ordering::Relaxed);
        let mut o = [0u8; 20];
        o[..8].copy_from_slice(&w0.to_le_bytes());
        o[8..16].copy_from_slice(&w1.to_le_bytes());
        o[16..20].copy_from_slice(&(w2 as u32).to_le_bytes());
        (
            Oid(o),
            (w2 >> 32) as u8,
            (w2 >> 40) as u8,
            self.w[3].load(Ordering::Relaxed),
        )
    }
}

#[inline]
fn side_ref(worker: usize, span: Span) -> u64 {
    ((worker as u64) << 56)
        | ((span.len as u64 & 0xFF_FFFF) << 32)
        | (span.start as u64 & 0xFFFF_FFFF)
}

#[inline]
fn side_unref(r: u64) -> (usize, Span) {
    (
        (r >> 56) as usize,
        Span::new((r & 0xFFFF_FFFF) as usize, ((r >> 32) & 0xFF_FFFF) as usize),
    )
}

struct Frame {
    entry: u32,
    /// Content span in the arena, or `None` once evicted.
    content: Option<Span>,
    next_child: u32,
}

struct Worker<'a> {
    id: usize,
    pack: &'a [u8],
    entries: &'a [Entry],
    children: &'a [u32],
    child_start: &'a [u32],
    resolved: &'a [Resolved],
    dedup: Option<&'a (dyn Fn(&Oid) -> bool + Sync)>,
    stats: &'a Stats,
    full_non_trees: bool,
    full_max_bytes: usize,
    arena: Arena,
    side: Arena,
    inflater: Inflater,
    deflater: Deflater,
    stack: Vec<Frame>,
    chain: Vec<u32>,
}

impl Worker<'_> {
    fn entry(&self, i: u32) -> Result<&Entry, IndexError> {
        self.entries.get(i as usize).ok_or(IndexError::BaseNotFound)
    }

    /// Inflates entry `i`'s payload onto the arena top.
    fn inflate_payload(&mut self, i: u32) -> Result<Span, IndexError> {
        let e = *self.entry(i)?;
        let size = usize::try_from(e.size).map_err(|_| IndexError::ObjectTooLarge)?;
        if size > self.arena.remaining() {
            return Err(IndexError::ObjectTooLarge);
        }
        let src = payload(self.pack, &e)?;
        self.inflater
            .inflate_exact(src, self.arena.free_tail_mut(), size)?;
        Ok(self.arena.commit(size)?)
    }

    /// Content of a delta entry `child` given its base's content span.
    fn apply_child(&mut self, child: u32, base: Span) -> Result<Span, IndexError> {
        let mark = self.arena.mark();
        let delta = self.inflate_payload(child)?;
        let (_, result_size, _) = delta_sizes(self.arena.get(delta))?;
        let result_len = usize::try_from(result_size).map_err(|_| IndexError::ObjectTooLarge)?;
        let out = self.arena.alloc(result_len)?;
        {
            // base < delta < out in the arena, all disjoint.
            let (lo, hi) = self.arena.split_at(delta.start);
            let base_bytes = lo.get(base.start..base.end()).unwrap_or(&[]);
            let (delta_bytes, out_bytes) = hi.split_at_mut(delta.len.min(hi.len()));
            let out_bytes = out_bytes
                .get_mut(..result_len)
                .ok_or(IndexError::ObjectTooLarge)?;
            apply_delta(base_bytes, delta_bytes, out_bytes)?;
        }
        // Slide the result down over the delta bytes so the stack stays tight.
        self.arena.copy_within(out, mark);
        self.arena.truncate(mark.saturating_add(result_len));
        Ok(Span::new(mark, result_len))
    }

    fn record(&mut self, i: u32, kind: ObjType, content: Span) -> Result<Oid, IndexError> {
        let bytes = self.arena.get(content);
        let oid = hash_object(kind, bytes);
        let mut flags = 0u8;
        if kind == ObjType::Blob {
            if parse_hfs_pointer(bytes).is_some() {
                self.stats.pointer_blobs.fetch_add(1, Ordering::Relaxed);
            } else {
                self.stats.other_blobs.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .other_blob_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            }
        }
        if let Some(d) = self.dedup
            && d(&oid)
        {
            flags |= F_DUP;
            self.stats.duplicates.fetch_add(1, Ordering::Relaxed);
        }
        let e = *self.entry(i)?;
        let mut side = 0u64;
        let is_delta = e.kind == ObjType::OfsDelta.code() || e.kind == ObjType::RefDelta.code();
        if flags & F_DUP == 0 && is_delta {
            // The base was recorded before this child, so its flags are final.
            let base = *self.entry(e.base)?;
            let (_, _, base_flags, _) = self
                .resolved
                .get(e.base as usize)
                .map(Resolved::load)
                .unwrap_or((Oid::ZERO, 0, F_DUP, 0));
            let base_dropped = base_flags & F_DUP != 0;
            let base_after = base.offset >= e.offset;
            let policy =
                self.full_non_trees && kind != ObjType::Tree && content.len <= self.full_max_bytes;
            if base_dropped || base_after || policy {
                flags |= F_FULL | F_CONVERTED;
                side = self.convert(kind, content)?;
                self.stats.converted.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(r) = self.resolved.get(i as usize) {
            r.store(&oid, kind.code(), flags, side);
        }
        Ok(oid)
    }

    /// Deflates `content` as a full `kind` entry into the side arena.
    fn convert(&mut self, kind: ObjType, content: Span) -> Result<u64, IndexError> {
        let mut hdr = [0u8; 10];
        let hl = encode_entry_header(kind, content.len as u64, &mut hdr);
        let bound = hl.saturating_add(compress_bound(content.len));
        let span = match self.side.alloc(bound) {
            Ok(span) => span,
            Err(_) => {
                // Sized for pointer blobs; a repository with real blobs (or a burst of
                // orphaned deltas) grows it. The indexer is not a hot path.
                let want = self.side.used().saturating_add(bound);
                if want > SIDE_ARENA_MAX {
                    return Err(IndexError::SideArenaFull);
                }
                self.side.grow_to(
                    self.side
                        .capacity()
                        .saturating_mul(2)
                        .clamp(want, SIDE_ARENA_MAX),
                );
                self.side
                    .alloc(bound)
                    .map_err(|_| IndexError::SideArenaFull)?
            }
        };
        let n = {
            let (arena, side) = (&self.arena, &mut self.side);
            let dst = side.get_mut(span);
            dst.get_mut(..hl)
                .ok_or(IndexError::SideArenaFull)?
                .copy_from_slice(hdr.get(..hl).unwrap_or(&[]));
            self.deflater
                .deflate(arena.get(content), dst.get_mut(hl..).unwrap_or(&mut []))
                .map_err(|_| IndexError::SideArenaFull)?
        };
        let used = Span::new(span.start, hl.saturating_add(n));
        if used.len > 0xFF_FFFF || used.end() > 0xFFFF_FFFF {
            return Err(IndexError::ObjectTooLarge);
        }
        self.side.truncate(used.end());
        Ok(side_ref(self.id, used))
    }

    fn children_of(&self, i: u32) -> (u32, u32) {
        let s = self.child_start.get(i as usize).copied().unwrap_or(0);
        let e = self
            .child_start
            .get((i as usize).saturating_add(1))
            .copied()
            .unwrap_or(s);
        (s, e)
    }

    /// Recomputes an evicted frame's content from the nearest ancestor that still has one
    /// (or from the root), applying the chain of deltas with the free arena top.
    fn recompute(&mut self, depth: usize) -> Result<Span, IndexError> {
        self.chain.clear();
        let mut anc = depth;
        let mut base = loop {
            let f = self.stack.get(anc).ok_or(IndexError::DepthExceeded)?;
            if let Some(c) = f.content {
                break Some(c);
            }
            if self.chain.len() >= MAX_DEPTH {
                return Err(IndexError::DepthExceeded);
            }
            self.chain.push(f.entry);
            if anc == 0 {
                break None;
            }
            anc = anc.saturating_sub(1);
        };
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        // `chain` holds entries from `depth` up to (and including) the first without content.
        let top = self.arena.mark();
        while let Some(e) = self.chain.pop() {
            let next = match base {
                None => self.inflate_payload(e)?,
                Some(b) => self.apply_child(e, b)?,
            };
            // Keep only `next` above `top`.
            self.arena.copy_within(next, top);
            self.arena.truncate(top.saturating_add(next.len));
            base = Some(Span::new(top, next.len));
        }
        base.ok_or(IndexError::DepthExceeded)
    }

    /// Evicts every frame below the top one and slides the top's content to the bottom of
    /// the arena (each tree starts from an empty arena, so the floor is 0).
    fn evict_below_top(&mut self) {
        let Some(top_idx) = self.stack.len().checked_sub(1) else {
            return;
        };
        let Some(content) = self.stack.get(top_idx).and_then(|f| f.content) else {
            return;
        };
        for f in self.stack.iter_mut().take(top_idx) {
            f.content = None;
        }
        self.arena.copy_within(content, 0);
        self.arena.truncate(content.len);
        if let Some(f) = self.stack.get_mut(top_idx) {
            f.content = Some(Span::new(0, content.len));
        }
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);
    }

    fn resolve_tree(&mut self, root: u32) -> Result<(), IndexError> {
        self.arena.reset();
        self.stack.clear();
        let e = *self.entry(root)?;
        let kind = ObjType::from_code(e.kind).ok_or(PackError::BadObjectType(e.kind))?;
        let content = self.inflate_payload(root)?;
        self.record(root, kind, content)?;
        let (cs, _) = self.children_of(root);
        self.stack.push(Frame {
            entry: root,
            content: Some(content),
            next_child: cs,
        });
        let mut depth_seen = 1u64;
        while let Some(top) = self.stack.last() {
            let (_, ce) = self.children_of(top.entry);
            if top.next_child >= ce {
                let popped_start = top.content.map(|c| c.start);
                self.stack.pop();
                if let Some(s) = popped_start {
                    self.arena.truncate(s);
                }
                continue;
            }
            let depth = self.stack.len().saturating_sub(1);
            let child = self
                .children
                .get(top.next_child as usize)
                .copied()
                .ok_or(IndexError::BaseNotFound)?;
            if let Some(f) = self.stack.last_mut() {
                f.next_child = f.next_child.saturating_add(1);
            }
            if self.stack.len() >= MAX_DEPTH {
                return Err(IndexError::DepthExceeded);
            }
            let base = match self.stack.get(depth).and_then(|f| f.content) {
                Some(c) => c,
                None => {
                    let c = self.recompute(depth)?;
                    if let Some(f) = self.stack.get_mut(depth) {
                        f.content = Some(c);
                    }
                    c
                }
            };
            let content = match self.apply_child(child, base) {
                Ok(c) => c,
                Err(IndexError::ObjectTooLarge) => {
                    self.evict_below_top();
                    let base = self
                        .stack
                        .last()
                        .and_then(|f| f.content)
                        .ok_or(IndexError::ObjectTooLarge)?;
                    self.apply_child(child, base)?
                }
                Err(e) => return Err(e),
            };
            self.record(child, kind, content)?;
            let (ccs, _) = self.children_of(child);
            self.stack.push(Frame {
                entry: child,
                content: Some(content),
                next_child: ccs,
            });
            depth_seen = depth_seen.max(self.stack.len() as u64);
        }
        self.stats
            .max_depth
            .fetch_max(depth_seen, Ordering::Relaxed);
        Ok(())
    }
}

/// Resolves every object (parallel), filling `oids`, `real_kind` and `flags`.
/// `dedup(oid)` returns true for objects already stored elsewhere.
pub fn resolve(
    ip: &mut IndexedPack,
    pack: &[u8],
    cfg: &ResolveConfig,
    dedup: Option<&(dyn Fn(&Oid) -> bool + Sync)>,
) -> Result<(), IndexError> {
    let n = ip.entries.len();
    // Link REF_DELTAs to non-delta bases by object id (rare: we ask for ofs-delta).
    let has_ref = ip
        .entries
        .iter()
        .any(|e| e.kind == ObjType::RefDelta.code());
    if has_ref {
        link_ref_deltas(ip, pack)?;
    }
    // Children lists (counting sort by base).
    let mut child_start = vec![0u32; n.saturating_add(1)];
    for e in &ip.entries {
        if e.base != NONE
            && let Some(c) = child_start.get_mut((e.base as usize).saturating_add(1))
        {
            *c = c.saturating_add(1);
        }
    }
    for i in 1..=n {
        let prev = child_start.get(i.saturating_sub(1)).copied().unwrap_or(0);
        if let Some(c) = child_start.get_mut(i) {
            *c = c.saturating_add(prev);
        }
    }
    let n_deltas = child_start.last().copied().unwrap_or(0) as usize;
    let mut children = vec![0u32; n_deltas];
    {
        let mut cursor = child_start.clone();
        for (i, e) in ip.entries.iter().enumerate() {
            if e.base != NONE
                && let Some(c) = cursor.get_mut(e.base as usize)
            {
                if let Some(slot) = children.get_mut(*c as usize) {
                    *slot = i as u32;
                }
                *c = c.saturating_add(1);
            }
        }
    }
    // Roots: non-delta entries, biggest families first.
    let mut roots: Vec<u32> = ip
        .entries
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            e.base == NONE && !ObjType::from_code(e.kind).is_some_and(ObjType::is_delta)
        })
        .map(|(i, _)| i as u32)
        .collect();
    roots.sort_unstable_by_key(|&r| {
        let s = child_start.get(r as usize).copied().unwrap_or(0);
        let e = child_start
            .get((r as usize).saturating_add(1))
            .copied()
            .unwrap_or(0);
        core::cmp::Reverse(e.saturating_sub(s))
    });
    let resolved: Vec<Resolved> = (0..n).map(|_| Resolved::new()).collect();
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let workers = cfg.workers.clamp(1, 256).min(roots.len().max(1));
    let first_error: std::sync::Mutex<Option<IndexError>> = std::sync::Mutex::new(None);
    let side_arenas: Vec<Arena> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(workers);
        for id in 0..workers {
            let entries = &ip.entries;
            let (children, child_start, resolved, roots, next, stats, first_error, failed) = (
                &children,
                &child_start,
                &resolved,
                &roots,
                &next,
                &ip.stats,
                &first_error,
                &failed,
            );
            handles.push(s.spawn(move || {
                let mut w = Worker {
                    id,
                    pack,
                    entries,
                    children,
                    child_start,
                    resolved,
                    dedup,
                    stats,
                    full_non_trees: cfg.full_non_trees,
                    full_max_bytes: cfg.full_max_bytes,
                    arena: Arena::new(cfg.arena_bytes),
                    side: Arena::new(cfg.side_bytes),
                    inflater: Inflater::new(),
                    deflater: Deflater::new(CONVERT_LEVEL),
                    stack: Vec::with_capacity(MAX_DEPTH),
                    chain: Vec::with_capacity(MAX_DEPTH),
                };
                while !failed.load(Ordering::Relaxed) {
                    let r = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&root) = roots.get(r) else {
                        break;
                    };
                    if let Err(e) = w.resolve_tree(root) {
                        if let Ok(mut g) = first_error.lock() {
                            g.get_or_insert(e);
                        }
                        failed.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                w.side
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Arena::new(0)))
            .collect()
    });
    if let Some(e) = first_error.lock().ok().and_then(|g| *g) {
        return Err(e);
    }
    // Read the results back.
    let mut kept = 0usize;
    let mut needs_rewrite = has_ref;
    for (i, r) in resolved.iter().enumerate() {
        let (oid, kind, flags, _) = r.load();
        if kind == 0 {
            return Err(IndexError::RefDeltaUnresolved);
        }
        if let (Some(e), Some(o)) = (ip.entries.get_mut(i), ip.oids.get_mut(i)) {
            e.real_kind = kind;
            e.flags = flags;
            *o = oid;
            if flags & F_DUP == 0 {
                kept = kept.saturating_add(1);
            } else {
                needs_rewrite = true;
            }
            if flags & F_CONVERTED != 0 {
                needs_rewrite = true;
            }
        }
    }
    ip.kept = kept;
    ip.needs_rewrite = needs_rewrite;
    ip.side = side_arenas;
    ip.side_refs.clear();
    for (i, r) in resolved.iter().enumerate() {
        let (_, _, flags, side) = r.load();
        if flags & F_CONVERTED != 0 {
            ip.side_refs.push((i as u32, side));
        }
    }
    Ok(())
}

#[inline]
fn payload<'p>(pack: &'p [u8], e: &Entry) -> Result<&'p [u8], IndexError> {
    let start = (e.offset as usize).saturating_add(usize::from(e.hdr_len));
    let end = (e.offset as usize).saturating_add(e.len as usize);
    pack.get(start..end).ok_or(PackError::Truncated.into())
}

fn link_ref_deltas(ip: &mut IndexedPack, pack: &[u8]) -> Result<(), IndexError> {
    // Hash the non-delta entries first so REF_DELTA bases can be found by id.
    let mut inflater = Inflater::new();
    let max_size = ip
        .entries
        .iter()
        .filter(|e| e.base == NONE && e.kind != ObjType::RefDelta.code())
        .map(|e| e.size)
        .max()
        .unwrap_or(0);
    let mut buf = vec![0u8; usize::try_from(max_size).map_err(|_| IndexError::ObjectTooLarge)?];
    let mut by_oid: Vec<(Oid, u32)> = Vec::with_capacity(ip.entries.len());
    for (i, e) in ip.entries.iter().enumerate() {
        let kind = ObjType::from_code(e.kind).ok_or(PackError::BadObjectType(e.kind))?;
        if kind.is_delta() {
            continue;
        }
        let start = (e.offset as usize).saturating_add(usize::from(e.hdr_len));
        let end = (e.offset as usize).saturating_add(e.len as usize);
        let src = pack.get(start..end).ok_or(PackError::Truncated)?;
        let size = usize::try_from(e.size).map_err(|_| IndexError::ObjectTooLarge)?;
        inflater.inflate_exact(src, &mut buf, size)?;
        by_oid.push((hash_object(kind, buf.get(..size).unwrap_or(&[])), i as u32));
    }
    by_oid.sort_unstable();
    for i in 0..ip.entries.len() {
        let Some(e) = ip.entries.get(i) else { continue };
        if e.kind != ObjType::RefDelta.code() {
            continue;
        }
        let want = ip.oids.get(i).copied().unwrap_or(Oid::ZERO);
        let j = by_oid.partition_point(|(o, _)| *o < want);
        match by_oid.get(j) {
            Some((o, idx)) if *o == want => {
                if let Some(e) = ip.entries.get_mut(i) {
                    e.base = *idx;
                }
            }
            _ => return Err(IndexError::RefDeltaUnresolved),
        }
    }
    Ok(())
}

/// Writes the slab pack for the kept entries to `w`, updating `entries` to the new
/// offsets/lengths/CRCs, and returns the pack's SHA-1 trailer. Call only when
/// `needs_rewrite` (otherwise the received bytes are the slab pack).
pub fn emit<W: Write>(
    ip: &mut IndexedPack,
    pack: &[u8],
    w: &mut W,
) -> Result<[u8; 20], IndexError> {
    let side = core::mem::take(&mut ip.side);
    let side_of = core::mem::take(&mut ip.side_refs);
    let kept = u32::try_from(ip.kept).map_err(|_| PackError::SizeOverflow)?;
    let mut h = Sha1::new();
    let hdr = pack_header(kept);
    h.update(hdr);
    w.write_all(&hdr).map_err(|_| IndexError::Io)?;
    let mut pos = PACK_HEADER_LEN as u64;
    for i in 0..ip.entries.len() {
        let e = *ip.entries.get(i).ok_or(IndexError::BaseNotFound)?;
        if e.flags & F_DUP != 0 {
            continue;
        }
        let start = e.offset as usize;
        let end = start.saturating_add(e.len as usize);
        let new_offset = pos;
        let mut crc = crc32fast::Hasher::new();
        let written: u64;
        if e.flags & F_CONVERTED != 0 {
            let j = side_of.partition_point(|(k, _)| *k < i as u32);
            let (_, r) = side_of
                .get(j)
                .copied()
                .filter(|(k, _)| *k == i as u32)
                .ok_or(IndexError::SideArenaFull)?;
            let (wid, span) = side_unref(r);
            let bytes = side
                .get(wid)
                .map(|a| a.get(span))
                .ok_or(IndexError::SideArenaFull)?;
            h.update(bytes);
            crc.update(bytes);
            w.write_all(bytes).map_err(|_| IndexError::Io)?;
            written = bytes.len() as u64;
        } else if e.kind == ObjType::OfsDelta.code() || e.kind == ObjType::RefDelta.code() {
            let base = ip
                .entries
                .get(e.base as usize)
                .ok_or(IndexError::BaseNotFound)?;
            let distance = new_offset
                .checked_sub(base.offset)
                .filter(|&d| d > 0)
                .ok_or(IndexError::BaseNotFound)?;
            let mut nh = [0u8; 20];
            let nl = encode_ofs_delta_header(e.size, distance, &mut nh);
            let zsrc = pack
                .get(start.saturating_add(usize::from(e.hdr_len))..end)
                .ok_or(PackError::Truncated)?;
            let nhb = nh.get(..nl).unwrap_or(&[]);
            h.update(nhb);
            crc.update(nhb);
            h.update(zsrc);
            crc.update(zsrc);
            w.write_all(nhb).map_err(|_| IndexError::Io)?;
            w.write_all(zsrc).map_err(|_| IndexError::Io)?;
            written = (nl as u64).saturating_add(zsrc.len() as u64);
        } else {
            let raw = pack.get(start..end).ok_or(PackError::Truncated)?;
            h.update(raw);
            w.write_all(raw).map_err(|_| IndexError::Io)?;
            written = raw.len() as u64;
        }
        if let Some(e) = ip.entries.get_mut(i) {
            e.offset = new_offset;
            e.len = u32::try_from(written).map_err(|_| PackError::SizeOverflow)?;
            if e.flags & F_CONVERTED != 0
                || e.kind == ObjType::OfsDelta.code()
                || e.kind == ObjType::RefDelta.code()
            {
                e.crc = crc.finalize();
            }
            if e.flags & F_CONVERTED != 0 {
                e.kind = e.real_kind;
                e.base = NONE;
                e.hdr_len = 0;
            } else if e.kind == ObjType::RefDelta.code() {
                e.kind = ObjType::OfsDelta.code();
            }
        }
        pos = pos.saturating_add(written);
    }
    let digest: [u8; 20] = h.finalize().into();
    w.write_all(&digest).map_err(|_| IndexError::Io)?;
    Ok(digest)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;
    use crate::testutil::{PackBuilder, TestObj};

    fn index(pack: &[u8], dedup: Option<&(dyn Fn(&Oid) -> bool + Sync)>) -> IndexedPack {
        let mut scratch = vec![0u8; 64 << 10];
        let mut ip = scan(pack, &mut scratch).unwrap();
        let cfg = ResolveConfig {
            workers: 3,
            arena_bytes: 1 << 20,
            side_bytes: 1 << 20,
            full_non_trees: false,
            full_max_bytes: usize::MAX,
        };
        resolve(&mut ip, pack, &cfg, dedup).unwrap();
        ip
    }

    #[test]
    fn indexes_full_and_delta_entries() {
        let mut b = PackBuilder::new();
        let base = b.full(
            ObjType::Blob,
            b"hello, world, this is a base object with some length",
        );
        let child = b.ofs_delta(
            base,
            b"hello, world, this is a CHILD object with some length",
        );
        let grand = b.ofs_delta(
            child,
            b"hello, world, this is a GRANDCHILD object with length",
        );
        let tree = b.full(ObjType::Tree, b"100644 a\0....................");
        let pack = b.finish();
        let ip = index(&pack, None);
        assert_eq!(ip.entries.len(), 4);
        assert!(!ip.needs_rewrite);
        assert_eq!(ip.kept, 4);
        for (i, o) in [base, child, grand, tree].iter().enumerate() {
            assert_eq!(ip.oids[i], o.oid, "entry {i}");
            assert_eq!(ip.entries[i].real_kind, o.kind.code());
        }
        assert_eq!(ip.stats.max_depth.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn dedup_drops_duplicates_and_converts_orphaned_deltas() {
        let mut b = PackBuilder::new();
        let base = b.full(
            ObjType::Blob,
            b"the quick brown fox jumps over the lazy dog, again and again",
        );
        let child = b.ofs_delta(
            base,
            b"the quick brown cat jumps over the lazy dog, again and again",
        );
        let other = b.full(
            ObjType::Commit,
            b"tree 0000000000000000000000000000000000000000\n\nmsg\n",
        );
        let pack = b.finish();
        let dup = base.oid;
        let f = move |o: &Oid| *o == dup;
        let mut ip = index(&pack, Some(&f));
        assert!(ip.needs_rewrite);
        assert_eq!(ip.kept, 2);
        assert_eq!(ip.entries[0].flags & F_DUP, F_DUP);
        assert_eq!(ip.entries[1].flags & F_CONVERTED, F_CONVERTED);
        let mut out = Vec::new();
        let sha = emit(&mut ip, &pack, &mut out).unwrap();
        assert_eq!(&out[out.len() - 20..], &sha);
        // The emitted pack indexes cleanly as two full objects with the same ids.
        let ip2 = index(&out, None);
        assert_eq!(ip2.entries.len(), 2);
        assert_eq!(ip2.oids[0], child.oid);
        assert_eq!(ip2.oids[1], other.oid);
        assert_eq!(ip2.entries[0].kind, ObjType::Blob.code());
        // Offsets/lens/crcs of kept entries were updated to the new pack.
        let kept: Vec<&Entry> = ip.entries.iter().filter(|e| e.flags & F_DUP == 0).collect();
        for (k, e2) in kept.iter().zip(&ip2.entries) {
            assert_eq!((k.offset, k.len, k.crc), (e2.offset, e2.len, e2.crc));
        }
    }

    #[test]
    fn rewrites_ofs_distances_when_entries_are_dropped() {
        let mut b = PackBuilder::new();
        let dup = b.full(
            ObjType::Blob,
            b"this object is already known and will be dropped from the pack",
        );
        let base = b.full(
            ObjType::Blob,
            b"a base object that stays, long enough to be worth a delta later on",
        );
        let child = b.ofs_delta(
            base,
            b"a base object that stays, long enough to be worth a delta later ON",
        );
        let pack = b.finish();
        let d = dup.oid;
        let f = move |o: &Oid| *o == d;
        let mut ip = index(&pack, Some(&f));
        assert_eq!(ip.kept, 2);
        assert_eq!(
            ip.entries[2].flags & F_CONVERTED,
            0,
            "base kept: stays a delta"
        );
        let mut out = Vec::new();
        emit(&mut ip, &pack, &mut out).unwrap();
        let ip2 = index(&out, None);
        assert_eq!(ip2.oids, vec![base.oid, child.oid]);
        assert_eq!(ip2.entries[1].kind, ObjType::OfsDelta.code());
        assert_eq!(ip2.entries[1].base, 0);
    }

    #[test]
    fn tiny_arena_forces_evictions_but_stays_correct() {
        let mut b = PackBuilder::new();
        let mut objs: Vec<TestObj> = Vec::new();
        let mut content: Vec<u8> = (0..3000u32).map(|i| (i % 253) as u8).collect();
        objs.push(b.full(ObjType::Blob, &content));
        for d in 1..12usize {
            content[d * 100] ^= 0x55;
            let prev = objs[d - 1];
            objs.push(b.ofs_delta(prev, &content));
        }
        let pack = b.finish();
        let mut scratch = vec![0u8; 4096];
        let mut ip = scan(&pack, &mut scratch).unwrap();
        // Room for roughly three objects plus a delta: forces eviction and recomputation.
        let cfg = ResolveConfig {
            workers: 1,
            arena_bytes: 10_000,
            side_bytes: 1 << 16,
            full_non_trees: false,
            full_max_bytes: usize::MAX,
        };
        resolve(&mut ip, &pack, &cfg, None).unwrap();
        for (i, o) in objs.iter().enumerate() {
            assert_eq!(ip.oids[i], o.oid, "entry {i}");
        }
        assert!(ip.stats.evictions.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn policy_de_deltifies_blobs_but_keeps_tree_deltas() {
        let mut b = PackBuilder::new();
        let tb = b.full(
            ObjType::Tree,
            b"100644 a.txt\0....................100644 b.txt\0....................",
        );
        let tc = b.ofs_delta(
            tb,
            b"100644 a.txt\0....................100644 c.txt\0....................",
        );
        let bb = b.full(ObjType::Blob, b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167");
        let bc = b.ofs_delta(bb, b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9101\nsize:4168");
        let pack = b.finish();
        let mut scratch = vec![0u8; 4096];
        let mut ip = scan(&pack, &mut scratch).unwrap();
        let cfg = ResolveConfig {
            workers: 2,
            arena_bytes: 1 << 20,
            side_bytes: 1 << 20,
            full_non_trees: true,
            full_max_bytes: usize::MAX,
        };
        resolve(&mut ip, &pack, &cfg, None).unwrap();
        assert!(ip.needs_rewrite);
        assert_eq!(ip.entries[1].flags & F_CONVERTED, 0, "tree delta kept");
        assert_eq!(
            ip.entries[3].flags & F_CONVERTED,
            F_CONVERTED,
            "blob delta converted"
        );
        assert_eq!(ip.stats.pointer_blobs.load(Ordering::Relaxed), 2);
        let mut out = Vec::new();
        emit(&mut ip, &pack, &mut out).unwrap();
        let mut ip2 = scan(&out, &mut scratch).unwrap();
        resolve(&mut ip2, &out, &cfg, None).unwrap();
        assert_eq!(ip2.oids, vec![tb.oid, tc.oid, bb.oid, bc.oid]);
        assert_eq!(ip2.entries[1].kind, ObjType::OfsDelta.code());
        assert_eq!(ip2.entries[3].kind, ObjType::Blob.code());
        assert!(!ip2.needs_rewrite);
    }

    #[test]
    fn detects_corruption() {
        let mut b = PackBuilder::new();
        b.full(ObjType::Blob, b"x");
        let mut pack = b.finish();
        let mut scratch = vec![0u8; 4096];
        let last = pack.len() - 1;
        pack[last] ^= 1;
        assert_eq!(scan(&pack, &mut scratch).err(), Some(IndexError::Trailer));
        assert!(scan(&pack[..20], &mut scratch).is_err());
    }
}
