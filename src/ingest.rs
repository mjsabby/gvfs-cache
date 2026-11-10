//! Indexing a pack: what `git index-pack` does, in two parallel passes.
//!
//! **Pass 1 (scan).** Entry boundaries are only discoverable by inflating every zlib
//! stream, which is why `index-pack`'s first pass is sequential. Here the pack is cut into
//! chunks and every worker *speculatively* finds the first plausible entry boundary in its
//! chunk (valid header, valid zlib header, a stream that inflates to exactly the declared
//! size with a matching Adler-32, and a plausible next header) and decodes entries from
//! there, hashing whole objects as it goes. A stitcher then walks the true chain from
//! offset 12 and adopts a chunk's results only from an offset it reached itself, decoding
//! sequentially wherever no result matches. Decoding from a given offset is
//! deterministic, so the outcome is exactly the sequential one; a false start merely
//! wastes that worker's time.
//!
//! **Pass 2 (resolve).** Deltas form families rooted at whole objects. Workers take
//! families from a shared counter and walk each depth-first: inflate the delta, apply it
//! to the parent's content, hash the result. A worker holds an object's content only
//! while the object has deltas left to resolve, so a chain of any depth needs two
//! buffers, and it frees what a family of large objects needed when the family is done.
//!
//! The output is one [`Facts`] per entry, in pack order, for [`crate::slab::build_index`].

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use sha1::{Digest, Sha1};

use crate::delta::{apply_delta, delta_sizes};
use crate::pack::{
    OBJ_OFS_DELTA, OBJ_REF_DELTA, ObjectHeader, PACK_HEADER_LEN, PACK_TRAILER_LEN, is_delta,
    parse_entry_header,
};
use crate::slab::{Facts, NO_BASE};
use crate::util::{Context, Oid, Result, u32_of, u64_of, us};
use crate::zlib::Inflater;

/// Below this many body bytes a pack is scanned on the calling thread.
const PARALLEL_MIN_BYTES: usize = 8 << 20;
/// Smallest speculative chunk.
const MIN_CHUNK: usize = 2 << 20;
const STREAM_CHUNK: usize = 64 << 10;
/// Fewest entries given to one thread when pass 2 lists its roots (about 7 ms of lookups).
const ROOTS_MIN_CHUNK: usize = 1 << 14;
/// Most content a pass-2 worker keeps from one delta family to the next, in bytes and in
/// buffers: what a family of larger objects needed, or one with more objects waiting for
/// their deltas, is freed when the family is done.
const SCRATCH_KEEP: usize = 4 << 20;
const SCRATCH_BUFFERS: usize = 64;

/// Pass-1 facts about one entry, in pack order.
#[derive(Clone, Copy, Default)]
struct Scanned {
    offset: u64,
    /// OFS_DELTA: absolute offset of the base entry.
    base_offset: u64,
    /// Declared inflated size of the entry data (object or delta).
    size: u64,
    entry_len: u32,
    hdr_len: u8,
    /// Stored kind (1..=4, 6, 7).
    kind: u8,
    /// Whole objects: the object id. REF_DELTA: the base object id.
    oid: Oid,
}

/// Per-worker scan state.
struct ScanScratch {
    inflater: Inflater,
    chunk: Vec<u8>,
}

impl ScanScratch {
    fn new() -> ScanScratch {
        ScanScratch {
            inflater: Inflater::new(),
            chunk: vec![0u8; STREAM_CHUNK],
        }
    }
}

/// A plausible zlib stream header (deflate, window <= 32K, no preset dictionary).
#[inline]
fn plausible_zlib(b: &[u8]) -> bool {
    match b {
        [cmf, flg, ..] => {
            cmf & 0x0F == 8
                && cmf.wrapping_shr(4) <= 7
                && flg & 0x20 == 0
                && (u16::from(*cmf).wrapping_shl(8) | u16::from(*flg)).wrapping_rem(31) == 0
        }
        _ => false,
    }
}

/// Decodes the entry starting at `pos` (`< end`, the trailer start). Whole objects are hashed.
fn decode_entry(pack: &[u8], pos: usize, end: usize, s: &mut ScanScratch) -> Option<Scanned> {
    let region = pack.get(pos..end)?;
    let h = parse_entry_header(region)?;
    let zlib = region.get(h.header_len..)?;
    let mut sc = Scanned {
        offset: u64_of(pos),
        size: h.size,
        hdr_len: u8::try_from(h.header_len).ok()?,
        kind: h.kind,
        ..Scanned::default()
    };
    let consumed = match h.kind {
        OBJ_OFS_DELTA | OBJ_REF_DELTA => {
            if h.kind == OBJ_OFS_DELTA {
                sc.base_offset = u64_of(pos).checked_sub(h.base_distance)?;
                if h.base_distance == 0 || sc.base_offset < PACK_HEADER_LEN as u64 {
                    return None;
                }
            } else {
                sc.oid = h.base_oid;
            }
            s.inflater
                .inflate_into(zlib, h.size, &mut s.chunk, |_| {})
                .ok()?
        }
        kind => {
            let mut hasher = Sha1::new();
            hasher.update(ObjectHeader::new(kind, h.size).as_slice());
            let used = s
                .inflater
                .inflate_into(zlib, h.size, &mut s.chunk, |piece| hasher.update(piece))
                .ok()?;
            sc.oid = Oid(hasher.finalize().into());
            used
        }
    };
    sc.entry_len = u32::try_from(h.header_len.checked_add(consumed)?).ok()?;
    Some(sc)
}

/// Cheap test that an entry could start at `pos`.
fn plausible_entry(pack: &[u8], pos: usize, end: usize) -> bool {
    let Some(region) = pack.get(pos..end) else {
        return false;
    };
    match parse_entry_header(region) {
        Some(h) => {
            let ofs_ok = h.kind != OBJ_OFS_DELTA
                || (h.base_distance > 0
                    && h.base_distance <= u64_of(pos).saturating_sub(PACK_HEADER_LEN as u64));
            ofs_ok && plausible_zlib(region.get(h.header_len..).unwrap_or(&[]))
        }
        None => false,
    }
}

/// First offset in `[from, to)` where a valid entry, followed by a plausible one, starts.
fn find_boundary(
    pack: &[u8],
    from: usize,
    to: usize,
    end: usize,
    s: &mut ScanScratch,
) -> Option<usize> {
    (from..to).find(|&o| {
        plausible_entry(pack, o, end)
            && match decode_entry(pack, o, end, s) {
                Some(sc) => {
                    let next = o.saturating_add(sc.entry_len as usize);
                    next == end || plausible_entry(pack, next, end)
                }
                None => false,
            }
    })
}

/// Decodes the chain of entries starting at `start` until one starts at or after `stop`.
/// Returns the entries and the offset just past the last one.
fn decode_run(
    pack: &[u8],
    start: usize,
    stop: usize,
    end: usize,
    s: &mut ScanScratch,
    out: &mut Vec<Scanned>,
) -> usize {
    let mut pos = start;
    while pos < stop && pos < end {
        match decode_entry(pack, pos, end, s) {
            Some(sc) => {
                out.push(sc);
                pos = pos.saturating_add(sc.entry_len as usize);
            }
            None => break,
        }
    }
    pos
}

/// Pass-2 output for one delta.
#[derive(Clone, Copy, Default)]
struct Resolved {
    entry: u32,
    oid: Oid,
    size: u64,
    kind: u8,
    depth: u8,
    /// Entry the delta was resolved against.
    base: u32,
}

/// Per-worker resolve state: content buffers for the objects of the walk that still have
/// deltas to resolve.
struct ResolveScratch {
    inflater: Inflater,
    delta: Vec<u8>,
    /// `levels[k]` is the content of the object of `stack[k]` (`lens[k]` bytes are valid);
    /// the buffer after the last frame's takes the next result.
    levels: Vec<Vec<u8>>,
    lens: Vec<usize>,
    stack: Vec<Frame>,
    out: Vec<Resolved>,
}

impl ResolveScratch {
    fn new() -> ResolveScratch {
        ResolveScratch {
            inflater: Inflater::new(),
            delta: Vec::new(),
            levels: Vec::new(),
            lens: Vec::new(),
            stack: Vec::new(),
            out: Vec::new(),
        }
    }

    /// Frees the content buffers past the first `SCRATCH_KEEP` bytes of them, and past the
    /// first `SCRATCH_BUFFERS`. What one family needed is then not kept for the rest of
    /// the pack, or by an indexer that is used again (an edge's lives as long as the
    /// edge), and there are never more than a few buffers to look at after a family.
    fn release(&mut self) {
        let mut held = self.delta.capacity();
        if held > SCRATCH_KEEP {
            self.delta = Vec::new();
        }
        let mut keep = 0usize;
        for buf in self.levels.iter().take(SCRATCH_BUFFERS) {
            held = held.saturating_add(buf.capacity());
            if held > SCRATCH_KEEP {
                break;
            }
            keep = keep.saturating_add(1);
        }
        self.levels.truncate(keep);
        self.lens.truncate(keep);
    }
}

/// The reusable pack indexer. Buffers keep their capacity between calls, so steady-state
/// indexing of small live-tip packs does not allocate.
pub struct Indexer {
    threads: usize,
    scanned: Vec<Scanned>,
    base: Vec<u32>,
    child_start: Vec<u32>,
    children: Vec<u32>,
    fill: Vec<u32>,
    /// REF_DELTAs as (awaited base id, entry), sorted, with a 64K fanout on the id.
    waiters: Vec<(Oid, u32)>,
    fanout: Vec<u32>,
    /// Pass 2: set at the first waiter of an id once an object with that id has taken
    /// that id's waiters to resolve.
    claimed: Vec<AtomicBool>,
    roots: Vec<u32>,
    facts: Vec<Facts>,
    scan: ScanScratch,
    resolve: Vec<ResolveScratch>,
}

impl Indexer {
    pub fn new(threads: usize) -> Indexer {
        let threads = threads.max(1);
        Indexer {
            threads,
            scanned: Vec::new(),
            base: Vec::new(),
            child_start: Vec::new(),
            children: Vec::new(),
            fill: Vec::new(),
            waiters: Vec::new(),
            fanout: Vec::new(),
            claimed: Vec::new(),
            roots: Vec::new(),
            facts: Vec::new(),
            scan: ScanScratch::new(),
            resolve: (0..threads).map(|_| ResolveScratch::new()).collect(),
        }
    }

    /// Indexes a complete pack (header, entries, trailer). The trailer is verified.
    /// Returns one [`Facts`] per entry, in pack order.
    pub fn index(&mut self, pack: &[u8]) -> Result<&[Facts]> {
        let count = crate::pack::parse_pack_header(pack).ctx("not a pack (bad header)")?;
        let end = pack
            .len()
            .checked_sub(PACK_TRAILER_LEN)
            .filter(|&e| e >= PACK_HEADER_LEN)
            .ctx("truncated pack")?;
        let trailer = pack.get(end..).ctx("truncated pack")?;
        let body = end.saturating_sub(PACK_HEADER_LEN);
        let parallel = self.threads > 1 && body >= PARALLEL_MIN_BYTES;
        let t0 = std::time::Instant::now();

        // The trailer check (one more pass over the bytes) runs alongside a parallel scan.
        let trailer_ok = if parallel {
            std::thread::scope(|s| -> Result<bool> {
                let check = s.spawn(|| trailer_matches(pack, end, trailer));
                self.scan_parallel(pack, end)?;
                Ok(check.join().unwrap_or(false))
            })?
        } else {
            self.scanned.clear();
            let stop = decode_run(
                pack,
                PACK_HEADER_LEN,
                end,
                end,
                &mut self.scan,
                &mut self.scanned,
            );
            if stop != end {
                crate::bail!("corrupt pack entry at offset {stop}");
            }
            trailer_matches(pack, end, trailer)
        };
        if !trailer_ok {
            crate::bail!("pack checksum mismatch");
        }
        if self.scanned.len() != count as usize {
            crate::bail!(
                "pack header says {count} objects, found {}",
                self.scanned.len()
            );
        }
        let t_scan = t0.elapsed();
        if debug_enabled() {
            let mut kinds = [0usize; 8];
            for e in &self.scanned {
                if let Some(k) = kinds.get_mut(usize::from(e.kind)) {
                    *k = k.saturating_add(1);
                }
            }
            crate::log!(
                "entry kinds (commit, tree, blob, tag, -, ofs, ref): {:?}",
                kinds.get(1..).unwrap_or(&[])
            );
        }
        self.link()?;
        let t_link = t0.elapsed();
        self.resolve_all(pack, end)?;
        if debug_enabled() {
            crate::log!(
                "index: scan {:.2}s, link {:.2}s, resolve {:.2}s",
                t_scan.as_secs_f64(),
                t_link.saturating_sub(t_scan).as_secs_f64(),
                t0.elapsed().saturating_sub(t_link).as_secs_f64()
            );
        }
        Ok(&self.facts)
    }

    fn scan_parallel(&mut self, pack: &[u8], end: usize) -> Result<()> {
        let body = end.saturating_sub(PACK_HEADER_LEN);
        let nchunks = self
            .threads
            .saturating_mul(4)
            .max(1)
            .min(body.div_ceil(MIN_CHUNK).max(1));
        let chunk_len = body.div_ceil(nchunks).max(1);
        let starts: Vec<usize> = (0..nchunks)
            .map(|c| {
                PACK_HEADER_LEN
                    .saturating_add(c.saturating_mul(chunk_len))
                    .min(end)
            })
            .collect();
        let next = AtomicUsize::new(0);
        // Each worker claims chunks from a shared counter and returns what it decoded:
        // (chunk, entries, offset just past the last entry).
        let per_worker: Vec<Vec<(usize, Vec<Scanned>, usize)>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..self.threads.min(nchunks))
                .map(|_| {
                    s.spawn(|| {
                        let mut scratch = ScanScratch::new();
                        let mut mine = Vec::new();
                        loop {
                            let c = next.fetch_add(1, Ordering::Relaxed);
                            let Some(&from) = starts.get(c) else { break };
                            let to = starts.get(c.wrapping_add(1)).copied().unwrap_or(end);
                            let start = if c == 0 {
                                Some(PACK_HEADER_LEN)
                            } else {
                                find_boundary(pack, from, to, end, &mut scratch)
                            };
                            let mut entries = Vec::new();
                            let stop = match start {
                                Some(start) => {
                                    decode_run(pack, start, to, end, &mut scratch, &mut entries)
                                }
                                None => 0,
                            };
                            mine.push((c, entries, stop));
                        }
                        mine
                    })
                })
                .collect();
            // A worker that died only costs its chunks a sequential decode below.
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_default())
                .collect()
        });
        let mut results: Vec<(Vec<Scanned>, usize)> =
            (0..nchunks).map(|_| (Vec::new(), 0usize)).collect();
        for (c, entries, stop) in per_worker.into_iter().flatten() {
            if let Some(r) = results.get_mut(c) {
                *r = (entries, stop);
            }
        }

        // Stitch along the true chain.
        let mut sequential = 0usize;
        self.scanned.clear();
        let mut pos = PACK_HEADER_LEN;
        let mut c = 0usize;
        while pos < end {
            while starts.get(c.wrapping_add(1)).is_some_and(|&s| s <= pos) {
                c = c.wrapping_add(1);
            }
            let adopted = results.get(c).and_then(|(entries, stop)| {
                let i = entries
                    .binary_search_by_key(&u64_of(pos), |e| e.offset)
                    .ok()?;
                Some((entries.get(i..)?, *stop))
            });
            match adopted {
                Some((run, stop)) if stop > pos => {
                    self.scanned.extend_from_slice(run);
                    pos = stop;
                }
                _ => {
                    let sc = decode_entry(pack, pos, end, &mut self.scan)
                        .with_ctx(|| format!("corrupt pack entry at offset {pos}"))?;
                    self.scanned.push(sc);
                    pos = pos.saturating_add(sc.entry_len as usize);
                    sequential = sequential.saturating_add(1);
                }
            }
        }
        if debug_enabled() {
            crate::log!(
                "scan: {nchunks} chunks, {sequential} entries decoded sequentially by the stitcher"
            );
        }
        if pos != end {
            crate::bail!("pack entries overrun the trailer");
        }
        Ok(())
    }

    /// Links OFS_DELTAs to their base entries (children lists) and files REF_DELTAs under
    /// the id they wait for (resolved when that object's id becomes known).
    fn link(&mut self) -> Result<()> {
        let n = self.scanned.len();
        self.base.clear();
        self.base.resize(n, NO_BASE);
        self.waiters.clear();
        for i in 0..n {
            let Some(sc) = self.scanned.get(i).copied() else {
                continue;
            };
            match sc.kind {
                OBJ_OFS_DELTA => {
                    let j = self
                        .scanned
                        .binary_search_by_key(&sc.base_offset, |e| e.offset)
                        .ok()
                        .with_ctx(|| {
                            format!(
                                "delta at offset {} has no entry at its base offset {}",
                                sc.offset, sc.base_offset
                            )
                        })?;
                    if let Some(b) = self.base.get_mut(i) {
                        *b = u32_of(j);
                    }
                }
                OBJ_REF_DELTA => self.waiters.push((sc.oid, u32_of(i))),
                _ => {}
            }
        }
        self.waiters.sort_unstable();
        // 64K-entry fanout over the first two bytes of the awaited id.
        self.fanout.clear();
        self.fanout.resize(65537, 0);
        for (oid, _) in &self.waiters {
            let [a, b, ..] = oid.0;
            if let Some(c) = self
                .fanout
                .get_mut(usize::from(u16::from_be_bytes([a, b])).wrapping_add(1))
            {
                *c = c.wrapping_add(1);
            }
        }
        for i in 1..self.fanout.len() {
            let prev = self.fanout.get(i.wrapping_sub(1)).copied().unwrap_or(0);
            if let Some(c) = self.fanout.get_mut(i) {
                *c = c.wrapping_add(prev);
            }
        }
        self.build_children();
        Ok(())
    }

    /// Children lists (CSR) of the OFS_DELTA links in `self.base`.
    fn build_children(&mut self) {
        let n = self.scanned.len();
        self.child_start.clear();
        self.child_start.resize(n.saturating_add(1), 0);
        for &b in &self.base {
            if let Some(c) = self.child_start.get_mut((b as usize).wrapping_add(1)) {
                *c = c.wrapping_add(1);
            }
        }
        for i in 1..self.child_start.len() {
            let prev = self
                .child_start
                .get(i.wrapping_sub(1))
                .copied()
                .unwrap_or(0);
            if let Some(c) = self.child_start.get_mut(i) {
                *c = c.wrapping_add(prev);
            }
        }
        self.children.clear();
        self.children
            .resize(self.child_start.last().copied().unwrap_or(0) as usize, 0);
        self.fill.clear();
        self.fill.extend(self.child_start.iter().take(n).copied());
        for (i, &b) in self.base.iter().enumerate() {
            if let Some(f) = self.fill.get_mut(b as usize) {
                if let Some(slot) = self.children.get_mut(*f as usize) {
                    *slot = u32_of(i);
                }
                *f = f.wrapping_add(1);
            }
        }
    }

    /// Resolves every delta in one parallel pass over the families rooted at whole objects.
    fn resolve_all(&mut self, pack: &[u8], end: usize) -> Result<()> {
        let Indexer {
            threads,
            scanned,
            base,
            child_start,
            children,
            waiters,
            fanout,
            claimed,
            roots,
            facts,
            resolve,
            ..
        } = self;
        claimed.clear();
        claimed.resize_with(waiters.len(), AtomicBool::default);
        let ctx = ResolveCtx {
            pack,
            end,
            scanned,
            child_start,
            children,
            waiters,
            fanout,
            claimed,
        };
        facts.clear();
        facts.extend(scanned.iter().zip(base.iter()).map(|(sc, &b)| Facts {
            oid: if is_delta(sc.kind) { Oid::ZERO } else { sc.oid },
            offset: sc.offset,
            entry_len: sc.entry_len,
            size: sc.size,
            kind: sc.kind,
            depth: 0,
            base: b,
        }));
        // The roots, in pack order whatever the thread count: every thread lists those of
        // one run of entries. On one thread the 6.98M lookups of SearchGold's pack took
        // 3.1 s of the indexer's 8.5 s; on 32 they take 0.3 s.
        roots.clear();
        let per = scanned
            .len()
            .div_ceil((*threads).max(1))
            .max(ROOTS_MIN_CHUNK);
        if scanned.len() <= per {
            ctx.roots_of(0, scanned, roots);
        } else {
            let found: Vec<Option<Vec<u32>>> = std::thread::scope(|s| {
                let ctx = &ctx;
                let handles: Vec<_> = scanned
                    .chunks(per)
                    .enumerate()
                    .map(|(c, part)| {
                        s.spawn(move || {
                            let mut mine = Vec::new();
                            ctx.roots_of(c.saturating_mul(per), part, &mut mine);
                            mine
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().ok()).collect()
            });
            for part in found {
                roots.extend(part.ctx("root finder thread failed")?);
            }
        }

        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let workers = if roots.len() < 64 {
            1
        } else {
            (*threads).min(roots.len())
        };
        // Every scratch's results are merged below, so none may hold an earlier pack's:
        // fewer workers than last time leave some scratches unused.
        for scratch in resolve.iter_mut() {
            scratch.out.clear();
        }
        let roots: &[u32] = roots;
        let run = |scratch: &mut ResolveScratch| -> Result<()> {
            loop {
                let r = next.fetch_add(1, Ordering::Relaxed);
                let Some(&root) = roots.get(r) else { break };
                if failed.load(Ordering::Relaxed) {
                    break;
                }
                let done = resolve_family(&ctx, root, scratch);
                scratch.release();
                if let Err(e) = done {
                    failed.store(true, Ordering::Relaxed);
                    return Err(e);
                }
            }
            Ok(())
        };
        let mut first_err = None;
        if workers <= 1 {
            if let Some(s) = resolve.first_mut() {
                first_err = run(s).err();
            }
        } else {
            std::thread::scope(|s| {
                let handles: Vec<_> = resolve
                    .iter_mut()
                    .take(workers)
                    .map(|scratch| s.spawn(|| run(scratch)))
                    .collect();
                for h in handles {
                    let r = h
                        .join()
                        .unwrap_or_else(|_| Err(crate::err!("delta resolver thread failed")));
                    if let Err(e) = r {
                        first_err = first_err.take().or(Some(e));
                    }
                }
            });
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        for scratch in resolve.iter() {
            for r in &scratch.out {
                if let Some(f) = facts.get_mut(r.entry as usize) {
                    f.oid = r.oid;
                    f.size = r.size;
                    f.kind = r.kind;
                    f.depth = r.depth;
                    f.base = r.base;
                }
            }
        }
        let unresolved = facts.iter().filter(|f| f.oid.is_zero()).count();
        if unresolved > 0 {
            crate::bail!("pack has {unresolved} unresolvable deltas (thin pack, or a delta cycle)");
        }
        Ok(())
    }
}

/// Inputs shared by the resolver threads: read-only, but for the claims.
struct ResolveCtx<'a> {
    pack: &'a [u8],
    end: usize,
    scanned: &'a [Scanned],
    child_start: &'a [u32],
    children: &'a [u32],
    waiters: &'a [(Oid, u32)],
    fanout: &'a [u32],
    claimed: &'a [AtomicBool],
}

impl ResolveCtx<'_> {
    fn zlib(&self, e: u32) -> Option<&[u8]> {
        let sc = self.scanned.get(e as usize)?;
        let start = us(sc.offset).checked_add(usize::from(sc.hdr_len))?;
        let stop = us(sc.offset)
            .checked_add(sc.entry_len as usize)?
            .min(self.end);
        self.pack.get(start..stop)
    }

    /// OFS_DELTA entries based on entry `e`.
    fn ofs_kids(&self, e: u32) -> &[u32] {
        let lo = self.child_start.get(e as usize).copied().unwrap_or(0) as usize;
        let hi = self
            .child_start
            .get((e as usize).wrapping_add(1))
            .copied()
            .unwrap_or(0) as usize;
        self.children.get(lo..hi).unwrap_or(&[])
    }

    /// `waiters[lo..hi]`: the REF_DELTA entries based on the object `oid`.
    fn waiting_range(&self, oid: &Oid) -> (u32, u32) {
        let [a, b, ..] = oid.0;
        let bucket = usize::from(u16::from_be_bytes([a, b]));
        let lo = self.fanout.get(bucket).copied().unwrap_or(0);
        let hi = self
            .fanout
            .get(bucket.wrapping_add(1))
            .copied()
            .unwrap_or(0);
        let run = self.waiters.get(lo as usize..hi as usize).unwrap_or(&[]);
        let first = run.partition_point(|(w, _)| w < oid);
        let len = run
            .get(first..)
            .unwrap_or(&[])
            .partition_point(|(w, _)| w == oid);
        let start = lo.wrapping_add(u32_of(first));
        (start, start.wrapping_add(u32_of(len)))
    }

    /// [`Self::waiting_range`] for the object that will resolve those deltas: the first
    /// with this id to ask gets them and any other gets none, so every REF delta is
    /// resolved once. A pack that git accepts can hold an id more than once: a base
    /// stored twice, or a delta that makes its base's id again (79 bytes are enough: a
    /// blob, and a REF delta on it that copies it). Without the claim such deltas were
    /// resolved once for every copy of their base: a chain stored twice 2^depth times,
    /// and the delta that waits for its own result without end.
    fn claim_waiters(&self, oid: &Oid) -> (u32, u32) {
        let (lo, hi) = self.waiting_range(oid);
        let taken = |first: &AtomicBool| first.swap(true, Ordering::Relaxed);
        if lo < hi && self.claimed.get(lo as usize).is_none_or(taken) {
            return (hi, hi);
        }
        (lo, hi)
    }

    /// Appends the roots among `part`, whose first entry is number `from`: the whole
    /// entries that some delta is based on.
    fn roots_of(&self, from: usize, part: &[Scanned], roots: &mut Vec<u32>) {
        for (i, sc) in part.iter().enumerate() {
            // The cheap tests first: a delta is no root, and OFS children are two adjacent
            // reads. Finding an id's waiters is two binary searches in a table far larger
            // than the cache, 0.3 to 0.4 µs. Made for every entry they took 4.7 s of the
            // indexer's 10.1 s on SearchGold's 12.13M-entry pack; its 5.15M deltas no
            // longer make them.
            if is_delta(sc.kind) {
                continue;
            }
            let e = u32_of(from.saturating_add(i));
            if !self.ofs_kids(e).is_empty() || {
                let (wlo, whi) = self.waiting_range(&sc.oid);
                wlo < whi
            } {
                roots.push(e);
            }
        }
    }
}

/// `buf[..n]`, growing (never shrinking or zero-filling) the buffer as needed.
fn room(buf: &mut Vec<u8>, n: usize) -> &mut [u8] {
    if buf.len() < n {
        buf.resize(n, 0);
    }
    buf.get_mut(..n).unwrap_or(&mut [])
}

/// Inflates entry `e`'s data into `buf[..size]`; returns that size.
fn inflate_entry(
    ctx: &ResolveCtx<'_>,
    e: u32,
    inflater: &mut Inflater,
    buf: &mut Vec<u8>,
) -> Result<usize> {
    let sc = ctx.scanned.get(e as usize).ctx("entry out of range")?;
    let n = us(sc.size);
    let zlib = ctx.zlib(e).ctx("entry out of range")?;
    inflater
        .inflate_exact(zlib, room(buf, n))
        .with_ctx(|| format!("inflating entry at {}", sc.offset))?;
    Ok(n)
}

fn hash_object(kind: u8, content: &[u8]) -> Oid {
    let mut h = Sha1::new();
    h.update(ObjectHeader::new(kind, u64_of(content.len())).as_slice());
    h.update(content);
    Oid(h.finalize().into())
}

/// One frame of the depth-first walk: a resolved object with deltas left to resolve. Its
/// content is the level buffer of the frame's place in the stack.
#[derive(Clone, Copy)]
struct Frame {
    entry: u32,
    depth: u32,
    ofs_next: u32,
    ref_lo: u32,
    ref_hi: u32,
}

/// Depth-first resolution of every delta under the whole object `root`.
fn resolve_family(ctx: &ResolveCtx<'_>, root: u32, s: &mut ResolveScratch) -> Result<()> {
    let ResolveScratch {
        inflater,
        delta,
        levels,
        lens,
        stack,
        out,
    } = s;
    let root_sc = ctx.scanned.get(root as usize).ctx("root out of range")?;
    let root_kind = root_sc.kind;
    if levels.is_empty() {
        levels.push(Vec::new());
        lens.push(0);
    }
    let n = inflate_entry(
        ctx,
        root,
        inflater,
        levels.first_mut().ctx("no level buffer")?,
    )?;
    if let Some(l) = lens.first_mut() {
        *l = n;
    }
    let (ref_lo, ref_hi) = ctx.claim_waiters(&root_sc.oid);
    stack.clear();
    stack.push(Frame {
        entry: root,
        depth: 0,
        ofs_next: 0,
        ref_lo,
        ref_hi,
    });
    // `at` is the top frame's place in the stack: its content is `levels[at]`.
    while let Some(at) = stack.len().checked_sub(1) {
        let Some(top) = stack.get_mut(at) else { break };
        let ofs = ctx.ofs_kids(top.entry);
        let child = if let Some(&c) = ofs.get(top.ofs_next as usize) {
            top.ofs_next = top.ofs_next.wrapping_add(1);
            c
        } else if top.ref_lo < top.ref_hi {
            let c = ctx
                .waiters
                .get(top.ref_lo as usize)
                .map(|w| w.1)
                .ctx("waiter out of range")?;
            top.ref_lo = top.ref_lo.wrapping_add(1);
            c
        } else {
            stack.pop();
            continue;
        };
        // The parent's last delta: nothing reads the parent's content after it.
        let last = top.ofs_next as usize >= ofs.len() && top.ref_lo >= top.ref_hi;
        let (parent, depth) = (top.entry, top.depth);
        let child_sc = ctx.scanned.get(child as usize).ctx("child out of range")?;
        if !is_delta(child_sc.kind) {
            continue;
        }
        let dlen = inflate_entry(ctx, child, inflater, delta)?;
        let delta_bytes = delta.get(..dlen).ctx("delta buffer")?;
        let (_, rsz, _) = delta_sizes(delta_bytes).ctx("bad delta header")?;
        let next_level = at.saturating_add(1);
        while levels.len() <= next_level {
            levels.push(Vec::new());
            lens.push(0);
        }
        let (lower, upper) = levels.split_at_mut(next_level);
        let parent_len = lens.get(at).copied().unwrap_or(0);
        let parent_bytes = lower
            .get(at)
            .and_then(|l| l.get(..parent_len))
            .ctx("missing level")?;
        let target = room(upper.first_mut().ctx("missing level")?, us(rsz));
        apply_delta(parent_bytes, delta_bytes, target)
            .with_ctx(|| format!("applying delta at offset {}", child_sc.offset))?;
        if let Some(l) = lens.get_mut(next_level) {
            *l = us(rsz);
        }
        let oid = hash_object(root_kind, target);
        out.push(Resolved {
            entry: child,
            oid,
            size: rsz,
            kind: root_kind,
            depth: u8::try_from(depth.saturating_add(1)).unwrap_or(u8::MAX),
            base: parent,
        });
        let (ref_lo, ref_hi) = ctx.claim_waiters(&oid);
        if !ctx.ofs_kids(child).is_empty() || ref_lo < ref_hi {
            let frame = Frame {
                entry: child,
                depth: depth.saturating_add(1),
                ofs_next: 0,
                ref_lo,
                ref_hi,
            };
            if last {
                // The child takes its parent's place in the stack, and the parent's
                // buffer takes the next result: a chain of any depth needs two buffers.
                // One per depth cost a worker 0.2 GiB on SearchGold's baseline, whose
                // trees run to 5.3 MB in chains 50 deep: 8.1 GiB at 32 threads, now 2.1.
                if let (Some(p), Some(c)) = (lower.get_mut(at), upper.first_mut()) {
                    std::mem::swap(p, c);
                }
                if let Some(l) = lens.get_mut(at) {
                    *l = us(rsz);
                }
                *top = frame;
            } else {
                stack.push(frame);
            }
        }
    }
    Ok(())
}

/// Manifest facts of a finished pack and its index.
fn slab_meta(
    pack: &[u8],
    index: &[u8],
    tier: crate::slab::Tier,
    range: (u64, u64),
    commits: (Oid, Oid),
) -> Result<crate::slab::SlabMeta> {
    let pack_sha: [u8; 20] = pack
        .get(pack.len().saturating_sub(PACK_TRAILER_LEN)..)
        .and_then(|t| t.try_into().ok())
        .ctx("pack trailer")?;
    Ok(crate::slab::SlabMeta {
        id: crate::slab::SlabMeta::make_id(tier, range.1, &pack_sha),
        tier,
        start_ts: range.0,
        end_ts: range.1,
        have: commits.0,
        want: commits.1,
        objects: crate::util::le_u32(index, 12).ctx("index header")?,
        pack_len: u64_of(pack.len()),
        index_len: u64_of(index.len()),
        pack_sha,
    })
}

/// Writes a slab (`<out>/<id>.pack` + `<id>.mphf`) for the pack bytes in `pack`, whose
/// file is `pack_path` (linked into place, never copied when on the same filesystem).
pub fn write_slab_files(
    indexer: &mut Indexer,
    pack: &[u8],
    pack_path: &std::path::Path,
    out_dir: &std::path::Path,
    tier: crate::slab::Tier,
    range: (u64, u64),
    commits: (Oid, Oid),
) -> Result<crate::slab::SlabMeta> {
    let t0 = std::time::Instant::now();
    let threads = indexer.threads;
    let facts = indexer.index(pack)?;
    let t_index = t0.elapsed();
    let pack_sha: [u8; 20] = pack
        .get(pack.len().saturating_sub(PACK_TRAILER_LEN)..)
        .and_then(|t| t.try_into().ok())
        .ctx("pack trailer")?;
    let index = crate::slab::build_index(facts, u64_of(pack.len()), &pack_sha, threads)?;
    let t_mphf = t0.elapsed().saturating_sub(t_index);
    let meta = slab_meta(pack, &index, tier, range, commits)?;
    crate::fsutil::link_or_copy(pack_path, &out_dir.join(format!("{}.pack", meta.id)))?;
    crate::fsutil::write_atomic(&out_dir.join(format!("{}.mphf", meta.id)), &index)?;
    crate::log!(
        "slab {}: {} objects, pack {}, index {} (indexed in {:.2}s, mphf+records {:.2}s)",
        meta.id,
        meta.objects,
        crate::util::human_bytes(meta.pack_len),
        crate::util::human_bytes(meta.index_len),
        t_index.as_secs_f64(),
        t_mphf.as_secs_f64()
    );
    Ok(meta)
}

/// Where a slab built by [`ingest_git_delta`] or [`crate::repack`] goes.
#[derive(Clone, Copy)]
pub enum Dest<'a> {
    /// `<dir>/<id>.pack` + `<dir>/<id>.mphf` (the pack is streamed to disk, then mapped).
    Dir(&'a std::path::Path),
    /// Heap buffers (the live tip's L0 slab).
    Memory,
}

/// A finished slab: its manifest facts, and the slab itself (mapped from its files for
/// `Dest::Dir`, held in memory for `Dest::Memory`).
pub struct Ingested {
    pub meta: crate::slab::SlabMeta,
    pub slab: std::sync::Arc<crate::slab::Slab>,
}

/// Indexes a complete pack held in memory into a heap slab.
pub fn memory_slab(
    indexer: &mut Indexer,
    pack: Vec<u8>,
    tier: crate::slab::Tier,
    range: (u64, u64),
    commits: (Oid, Oid),
) -> Result<Ingested> {
    let threads = indexer.threads;
    let facts = indexer.index(&pack)?;
    let pack_sha: [u8; 20] = pack
        .get(pack.len().saturating_sub(PACK_TRAILER_LEN)..)
        .and_then(|t| t.try_into().ok())
        .ctx("pack trailer")?;
    let index = crate::slab::build_index(facts, u64_of(pack.len()), &pack_sha, threads)?;
    let meta = slab_meta(&pack, &index, tier, range, commits)?;
    let slab = crate::slab::Slab::from_heap(meta.clone(), pack, index)?;
    Ok(Ingested { meta, slab })
}

/// Indexes the complete pack file `tmp` into the slab `<dir>/<id>.{pack,mphf}` and opens
/// it. `tmp` is removed.
pub fn file_slab(
    indexer: &mut Indexer,
    tmp: &std::path::Path,
    dir: &std::path::Path,
    tier: crate::slab::Tier,
    range: (u64, u64),
    commits: (Oid, Oid),
) -> Result<Ingested> {
    let result = (|| -> Result<Ingested> {
        let file = std::fs::File::open(tmp)?;
        let map = gvfs_mmap::Map::open(&file, false)?;
        let _ = map.advise(gvfs_mmap::Advice::Sequential);
        let meta = write_slab_files(indexer, map.bytes(), tmp, dir, tier, range, commits)?;
        let slab = crate::slab::Slab::open(
            &dir.join(format!("{}.pack", meta.id)),
            &dir.join(format!("{}.mphf", meta.id)),
            meta.clone(),
        )?;
        Ok(Ingested { meta, slab })
    })();
    let _ = std::fs::remove_file(tmp);
    result
}

/// **Primitive 1.** One `have`/`want` fetch from the upstream becomes a slab: the
/// upstream's pack plus its `.mphf` index. No `haves` fetches the full history of `want`
/// (the one-time seed); the first have is recorded as the slab's range start. Returns
/// `None` when there is nothing new. What the upstream sends is raw material:
/// [`crate::repack`] turns it into the slab that is served.
pub fn ingest_git_delta(
    up: &mut crate::upstream::Upstream,
    indexer: &mut Indexer,
    haves: &[Oid],
    want: Oid,
    tier: crate::slab::Tier,
    range: (u64, u64),
    dest: Dest<'_>,
) -> Result<Option<Ingested>> {
    ingest_git_delta_with(up, indexer, haves, want, None, tier, range, dest)
}

/// [`ingest_git_delta`] with a fetch filter (`blob:none`: commits and trees only, for
/// stores that keep blobs in their own packs).
#[allow(clippy::too_many_arguments)]
pub fn ingest_git_delta_with(
    up: &mut crate::upstream::Upstream,
    indexer: &mut Indexer,
    haves: &[Oid],
    want: Oid,
    filter: Option<&str>,
    tier: crate::slab::Tier,
    range: (u64, u64),
    dest: Dest<'_>,
) -> Result<Option<Ingested>> {
    let have = haves.first().copied().unwrap_or(Oid::ZERO);
    if have == want {
        return Ok(None);
    }
    let t0 = std::time::Instant::now();
    match dest {
        Dest::Memory => {
            let mut pack = Vec::new();
            match filter {
                Some(f) => up.fetch_filtered(haves, &want, f, &mut pack)?,
                None => up.fetch(haves, &want, &mut pack)?,
            };
            if crate::pack::parse_pack_header(&pack) == Some(0) {
                return Ok(None);
            }
            memory_slab(indexer, pack, tier, range, (have, want)).map(Some)
        }
        Dest::Dir(dir) => {
            std::fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
            let tmp = crate::fsutil::temp_path(&dir.join("fetch.pack"));
            let fetched = (|| -> Result<bool> {
                let file = std::fs::File::create(&tmp).ctx("creating pack file")?;
                let mut w = std::io::BufWriter::with_capacity(1 << 20, file);
                let n = match filter {
                    Some(f) => up.fetch_filtered(haves, &want, f, &mut w)?,
                    None => up.fetch(haves, &want, &mut w)?,
                };
                let file = w
                    .into_inner()
                    .map_err(|e| crate::err!("writing pack: {e}"))?;
                file.sync_all()?;
                crate::log!(
                    "fetched {} ({}..{}) in {:.2}s",
                    crate::util::human_bytes(n),
                    have,
                    want,
                    t0.elapsed().as_secs_f64()
                );
                let mut head = [0u8; PACK_HEADER_LEN];
                std::io::Read::read_exact(&mut std::fs::File::open(&tmp)?, &mut head)?;
                Ok(crate::pack::parse_pack_header(&head) != Some(0))
            })();
            match fetched {
                Ok(true) => file_slab(indexer, &tmp, dir, tier, range, (have, want)).map(Some),
                other => {
                    let _ = std::fs::remove_file(&tmp);
                    other.map(|_| None)
                }
            }
        }
    }
}

/// `gvfs-cache ingest ...`
pub fn main(args: &[String]) -> Result<()> {
    let f = crate::cli::Flags::parse(
        args,
        &[
            "pack",
            "out",
            "threads",
            "upstream",
            "want",
            "have",
            "branch",
            "auth-command",
            "token-env",
            "token-user",
            "auth-header-env",
            "verify-idx",
        ],
    )?;
    let out = std::path::PathBuf::from(f.require("out")?);
    let threads = f.num("threads", crate::util::default_threads())?;
    let mut indexer = Indexer::new(threads);
    if let Some(pack_path) = f.get("pack") {
        let path = std::path::Path::new(pack_path);
        let file = std::fs::File::open(path).ctx(&format!("opening {pack_path}"))?;
        let map = gvfs_mmap::Map::open(&file, false)?;
        let _ = map.advise(gvfs_mmap::Advice::Sequential);
        let now = crate::util::now_secs();
        let meta = write_slab_files(
            &mut indexer,
            map.bytes(),
            path,
            &out,
            crate::slab::Tier::Base,
            (0, now),
            (Oid::ZERO, Oid::ZERO),
        )?;
        if let Some(idx) = f.get("verify-idx") {
            let slab = crate::slab::Slab::open(
                &out.join(format!("{}.pack", meta.id)),
                &out.join(format!("{}.mphf", meta.id)),
                meta,
            )?;
            let checked = verify_against_git_idx(&slab, std::path::Path::new(idx))?;
            crate::log!("verified {checked} objects against {idx}: ids and offsets match");
        }
        return Ok(());
    }
    let spec = f
        .get("upstream")
        .ctx("ingest needs --pack <FILE> or --upstream <URL|PATH>")?;
    let mut up = crate::upstream::Upstream::open(
        spec,
        f.get("branch").unwrap_or("main"),
        &auth_from_flags(&f)?,
    )?;
    let want = match f.get("want") {
        None | Some("HEAD") => up.head()?,
        Some(w) => Oid::from_hex(w.as_bytes()).ctx("--want must be a 40-hex object id or HEAD")?,
    };
    let have = match f.get("have") {
        None => Oid::ZERO,
        Some(h) => Oid::from_hex(h.as_bytes()).ctx("--have must be a 40-hex object id")?,
    };
    let now = crate::util::now_secs();
    let tier = if have.is_zero() {
        crate::slab::Tier::Base
    } else {
        crate::slab::Tier::M15
    };
    let haves: &[Oid] = if have.is_zero() {
        &[]
    } else {
        std::slice::from_ref(&have)
    };
    match ingest_git_delta(
        &mut up,
        &mut indexer,
        haves,
        want,
        tier,
        (0, now),
        Dest::Dir(&out),
    )? {
        Some(s) => crate::log!("wrote slab {} to {}", s.meta.id, out.display()),
        None => crate::log!("nothing new between {have} and {want}"),
    }
    Ok(())
}

/// Upstream credentials: `--auth-command`, `--token-env` (and `--token-user`) or
/// `--auth-header-env`, at most one of them.
pub fn auth_from_flags(f: &crate::cli::Flags) -> Result<crate::upstream::Auth> {
    use crate::upstream::Auth;
    let sources = ["auth-command", "token-env", "auth-header-env"];
    if sources.iter().filter(|s| f.get(s).is_some()).count() > 1 {
        crate::bail!("give only one of --auth-command, --token-env and --auth-header-env");
    }
    let env = |var: &str| {
        std::env::var(var).map_err(|_| crate::err!("environment variable {var} is not set"))
    };
    if let Some(command) = f.get("auth-command") {
        return Auth::command(command);
    }
    if let Some(var) = f.get("auth-header-env") {
        return Ok(Auth::header(env(var)?));
    }
    if let Some(var) = f.get("token-env") {
        return Ok(Auth::header(crate::http::basic_auth(
            f.get("token-user").unwrap_or(""),
            &env(var)?,
        )));
    }
    Ok(Auth::default())
}

/// `GVFS_DEBUG_INGEST=1`: log indexing phase timings and entry statistics.
fn debug_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var_os("GVFS_DEBUG_INGEST").is_some_and(|v| !v.is_empty() && v != "0")
    })
}

/// Checks that a git `.idx` (v2) for the same pack lists exactly the slab's objects at the
/// same offsets. Returns the number of objects checked.
pub fn verify_against_git_idx(
    slab: &crate::slab::Slab,
    idx_path: &std::path::Path,
) -> Result<usize> {
    let file = std::fs::File::open(idx_path).ctx("opening idx")?;
    let map = gvfs_mmap::Map::open(&file, false)?;
    let ix = map.bytes();
    if ix.get(..8) != Some(&[0xff, b't', b'O', b'c', 0, 0, 0, 2][..]) {
        crate::bail!("not a v2 pack index");
    }
    let n = crate::util::be_u32(ix, 8 + 255 * 4usize).ctx("idx fanout")? as usize;
    if n != slab.count() as usize {
        crate::bail!("idx has {n} objects, slab has {}", slab.count());
    }
    let oids_at: usize = 8 + 256 * 4;
    let offs_at = oids_at.saturating_add(n.saturating_mul(24));
    let large_at = offs_at.saturating_add(n.saturating_mul(4));
    for i in 0..n {
        let oid = Oid::from_slice(
            crate::util::sub(ix, oids_at.saturating_add(i.saturating_mul(20)), 20)
                .ctx("idx oid")?,
        )
        .ctx("idx oid")?;
        let raw = crate::util::be_u32(ix, offs_at.saturating_add(i.saturating_mul(4)))
            .ctx("idx offset")?;
        let offset = if raw & 0x8000_0000 == 0 {
            u64::from(raw)
        } else {
            let at = large_at.saturating_add(((raw & 0x7fff_ffff) as usize).saturating_mul(8));
            u64::from_be_bytes(
                crate::util::sub(ix, at, 8)
                    .ctx("idx large offset")?
                    .try_into()
                    .map_err(|_| crate::err!("idx"))?,
            )
        };
        let slot = slab
            .find(&oid)
            .with_ctx(|| format!("object {oid} missing from the slab"))?;
        let rec = slab.rec(slot).ctx("record")?;
        if rec.offset != offset {
            crate::bail!(
                "object {oid}: slab offset {} != idx offset {offset}",
                rec.offset
            );
        }
    }
    Ok(n)
}

fn trailer_matches(pack: &[u8], end: usize, trailer: &[u8]) -> bool {
    let mut h = Sha1::new();
    h.update(pack.get(..end).unwrap_or(&[]));
    let digest: [u8; 20] = h.finalize().into();
    digest.as_slice() == trailer
}

/// `.with_ctx(|| format!(..))`: lazily built context for hot loops.
trait LazyContext<T> {
    fn with_ctx(self, f: impl FnOnce() -> String) -> Result<T>;
}

impl<T, E: Into<crate::util::Error>> LazyContext<T> for std::result::Result<T, E> {
    fn with_ctx(self, f: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| {
            let e: crate::util::Error = e.into();
            crate::err!("{}: {e}", f())
        })
    }
}

impl<T> LazyContext<T> for Option<T> {
    fn with_ctx(self, f: impl FnOnce() -> String) -> Result<T> {
        self.ok_or_else(|| crate::util::Error::new(f()))
    }
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
pub(crate) mod tests {
    use super::*;
    use crate::delta::{put_copy, put_insert, put_varint};
    use crate::pack::{HeaderBytes, OBJ_BLOB, pack_header};
    use std::process::Command;

    /// A pack written entry by entry, for the shapes git's `pack-objects` does not make.
    struct PackBuilder {
        deflater: crate::zlib::Deflater,
        zbuf: Vec<u8>,
        pack: Vec<u8>,
        entries: u32,
    }

    impl PackBuilder {
        fn new() -> PackBuilder {
            PackBuilder {
                deflater: crate::zlib::Deflater::new(6),
                zbuf: Vec::new(),
                pack: pack_header(0).to_vec(),
                entries: 0,
            }
        }

        /// Appends an entry: `header`, then `data` deflated. Returns its offset.
        fn entry(&mut self, header: HeaderBytes, data: &[u8]) -> u64 {
            let at = self.pack.len() as u64;
            self.zbuf.resize(crate::zlib::compress_bound(data.len()), 0);
            let n = self
                .deflater
                .deflate_parts(&[data], &mut self.zbuf)
                .unwrap();
            self.pack.extend_from_slice(header.as_slice());
            self.pack.extend_from_slice(&self.zbuf[..n]);
            self.entries += 1;
            at
        }

        fn whole(&mut self, kind: u8, content: &[u8]) -> u64 {
            self.entry(HeaderBytes::whole(kind, content.len() as u64), content)
        }

        fn ref_delta(&mut self, base: &Oid, delta: &[u8]) -> u64 {
            self.entry(HeaderBytes::ref_delta(delta.len() as u64, base), delta)
        }

        /// An OFS delta against the entry at offset `base`.
        fn ofs_delta(&mut self, base: u64, delta: &[u8]) -> u64 {
            let dist = self.pack.len() as u64 - base;
            self.entry(HeaderBytes::ofs_delta(delta.len() as u64, dist), delta)
        }

        /// The pack: its header's count set, its trailer appended.
        fn finish(mut self) -> Vec<u8> {
            self.pack[..PACK_HEADER_LEN].copy_from_slice(&pack_header(self.entries));
            let trailer: [u8; 20] = Sha1::digest(&self.pack).into();
            self.pack.extend_from_slice(&trailer);
            self.pack
        }
    }

    /// A delta that makes `result` of `base`: their common prefix copied, the rest inserted.
    fn delta_of(base: &[u8], result: &[u8]) -> Vec<u8> {
        let same = base.iter().zip(result).take_while(|(a, b)| a == b).count();
        let mut d = Vec::new();
        put_varint(&mut d, base.len() as u64);
        put_varint(&mut d, result.len() as u64);
        put_copy(&mut d, 0, same);
        put_insert(&mut d, &result[same..]);
        d
    }

    /// Builds a small repository with deltas and returns its pack (OFS_DELTA entries).
    pub(crate) fn sample_pack(dir: &std::path::Path) -> Vec<u8> {
        sample_pack_with(dir, true)
    }

    /// `ofs`: OFS_DELTA entries (git's default for fetches) or REF_DELTA (what Azure DevOps sends).
    pub(crate) fn sample_pack_with(dir: &std::path::Path, ofs: bool) -> Vec<u8> {
        let git = |args: &[&str]| {
            let out = Command::new("git")
                .args(args)
                .current_dir(dir)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            out.stdout
        };
        std::fs::create_dir_all(dir).unwrap();
        git(&["init", "-q", "-b", "main"]);
        for i in 0..30 {
            let mut big = String::new();
            for l in 0..400 {
                big.push_str(&format!(
                    "line {l} of a file that changes a little {}\n",
                    if l == i { "CHANGED" } else { "" }
                ));
            }
            std::fs::write(dir.join("big.txt"), big).unwrap();
            std::fs::create_dir_all(dir.join(format!("d{}", i % 3))).unwrap();
            std::fs::write(
                dir.join(format!("d{}/f{i}.txt", i % 3)),
                format!("~~Git-hfs 1.0\nblob:{:066X}\nsize:{i}", i),
            )
            .unwrap();
            git(&["add", "-A"]);
            git(&[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "-m",
                &format!("c{i}"),
            ]);
        }
        let mut child = Command::new("git")
            .args(["pack-objects", "--revs", "--stdout", "-q"])
            .args(if ofs {
                &["--delta-base-offset"][..]
            } else {
                &[][..]
            })
            .current_dir(dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child.stdin.take().unwrap().write_all(b"main\n").unwrap();
        child.wait_with_output().unwrap().stdout
    }

    /// (id, type, object size) of every object in `pack`, according to git.
    fn git_ids(dir: &std::path::Path, pack: &[u8]) -> Vec<(String, String, u64)> {
        let p = dir.join("x.pack");
        std::fs::write(&p, pack).unwrap();
        let out = Command::new("git")
            .args(["index-pack", "-o"])
            .arg(dir.join("x.idx"))
            .arg(&p)
            .output()
            .unwrap();
        assert!(out.status.success());
        let v = Command::new("git")
            .arg("verify-pack")
            .arg("-v")
            .arg(dir.join("x.idx"))
            .output()
            .unwrap();
        let ids: Vec<String> = String::from_utf8_lossy(&v.stdout)
            .lines()
            .filter_map(|l| {
                l.split_whitespace()
                    .next()
                    .filter(|f| f.len() == 40)
                    .map(str::to_string)
            })
            .collect();
        // verify-pack reports delta sizes for deltified entries; cat-file has object sizes.
        let mut child = Command::new("git")
            .args([
                "cat-file",
                "--batch-check=%(objectname) %(objecttype) %(objectsize)",
            ])
            .current_dir(dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(ids.join("\n").as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|l| {
                let f: Vec<&str> = l.split_whitespace().collect();
                (f[0].to_string(), f[1].to_string(), f[2].parse().unwrap())
            })
            .collect()
    }

    /// One entry as `git verify-pack -v` lists it: offset, id, type, and for a delta its
    /// chain depth (0 for a whole object) and its base's id.
    type Entry = (u64, String, String, u32, String);

    /// What `git <args>` prints about `pack` once `git index-pack` has indexed it, in a
    /// directory of their own, as `x.pack` and `x.idx` (which is also the standard input).
    fn git_about(name: &str, pack: &[u8], args: &[&str]) -> String {
        let dir = std::env::temp_dir().join(format!("gvfs-ingest-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("x.pack"), pack).unwrap();
        let git = |args: &[&str], stdin: std::process::Stdio| {
            let out = Command::new("git")
                .args(args)
                .current_dir(&dir)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .stdin(stdin)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8(out.stdout).unwrap()
        };
        git(
            &["index-pack", "-o", "x.idx", "x.pack"],
            std::process::Stdio::null(),
        );
        let idx = std::fs::File::open(dir.join("x.idx")).unwrap();
        let said = git(args, idx.into());
        let _ = std::fs::remove_dir_all(&dir);
        said
    }

    /// Every entry of `pack`, by offset, according to `git index-pack` and `verify-pack`.
    fn git_entries(name: &str, pack: &[u8]) -> Vec<Entry> {
        let mut entries: Vec<Entry> = git_about(name, pack, &["verify-pack", "-v", "x.idx"])
            .lines()
            .map(|l| l.split_whitespace().collect::<Vec<&str>>())
            .filter(|f| f.len() >= 5 && f[0].len() == 40)
            .map(|f| {
                (
                    f[4].parse().unwrap(),
                    f[0].to_string(),
                    f[1].to_string(),
                    f.get(5).map_or(0, |d| d.parse().unwrap()),
                    f.get(6).map_or(String::new(), |b| b.to_string()),
                )
            })
            .collect();
        entries.sort();
        entries
    }

    /// (offset, id) of every entry of `pack`, by offset, in the index `git index-pack`
    /// writes: all git says of a pack that holds an id twice (`verify-pack` refuses it).
    fn git_index(name: &str, pack: &[u8]) -> Vec<(u64, String)> {
        let mut entries: Vec<(u64, String)> = git_about(name, pack, &["show-index"])
            .lines()
            .map(|l| l.split_whitespace().collect::<Vec<&str>>())
            .map(|f| (f[0].parse().unwrap(), f[1].to_string()))
            .collect();
        entries.sort();
        entries
    }

    /// The indexer's facts as [`git_index`] gives git's.
    fn our_index(facts: &[Facts]) -> Vec<(u64, String)> {
        let mut entries: Vec<(u64, String)> = facts
            .iter()
            .map(|f| (f.offset, f.oid.to_string()))
            .collect();
        entries.sort();
        entries
    }

    /// The indexer's facts as [`git_entries`] gives git's.
    fn our_entries(facts: &[Facts]) -> Vec<Entry> {
        let mut entries: Vec<Entry> = facts
            .iter()
            .map(|f| {
                (
                    f.offset,
                    f.oid.to_string(),
                    crate::pack::type_name(f.kind).to_string(),
                    u32::from(f.depth),
                    facts
                        .get(f.base as usize)
                        .map_or(String::new(), |b| b.oid.to_string()),
                )
            })
            .collect();
        entries.sort();
        entries
    }

    /// Bytes of content buffer a worker holds.
    fn held(scratch: &ResolveScratch) -> Vec<usize> {
        std::iter::once(&scratch.delta)
            .chain(&scratch.levels)
            .map(Vec::capacity)
            .filter(|&c| c > 0)
            .collect()
    }

    /// A chain of deltas is resolved in two buffers, whatever its depth: an object's
    /// content is given up once its last delta is applied. With a buffer per depth, kept,
    /// SearchGold's baseline (chains 50 deep, trees up to 5.3 MB) took 0.2 GiB per worker.
    #[test]
    fn a_delta_chain_is_resolved_in_two_buffers() {
        // 300 versions of a 64 KiB blob, each a REF delta of the one before it.
        let size = 64 << 10;
        let mut blob: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let mut b = PackBuilder::new();
        b.whole(OBJ_BLOB, &blob);
        for version in 1..=300u32 {
            let base = hash_object(OBJ_BLOB, &blob);
            let mut d = Vec::new();
            put_varint(&mut d, size as u64);
            put_varint(&mut d, size as u64);
            put_insert(&mut d, &version.to_le_bytes());
            put_copy(&mut d, 4, size - 4);
            blob[..4].copy_from_slice(&version.to_le_bytes());
            b.ref_delta(&base, &d);
        }
        let pack = b.finish();
        let mut ix = Indexer::new(1);
        let facts = ix.index(&pack).unwrap().to_vec();
        assert_eq!(facts[300].oid, hash_object(OBJ_BLOB, &blob));
        // The index has eight bits of depth.
        let mut git = git_entries("chain", &pack);
        for e in &mut git {
            e.3 = e.3.min(255);
        }
        assert_eq!(our_entries(&facts), git);
        let kept = held(&ix.resolve[0]);
        assert!(
            kept.len() == 3 && kept.iter().sum::<usize>() < 3 * size,
            "a delta and two versions are enough: {kept:?}"
        );
    }

    /// What a family of large objects needed is freed when the family is done: a worker
    /// keeps `SCRATCH_KEEP` bytes of buffers at most, and works as before with them.
    #[test]
    fn buffers_of_a_large_family_are_freed() {
        // Three versions of a 3 MiB blob: a base and its delta's result are 6 MiB.
        let size = 3 << 20;
        let mut blob: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let mut b = PackBuilder::new();
        let mut at = b.whole(OBJ_BLOB, &blob);
        for version in 1..=2u8 {
            let mut next = blob.clone();
            next[size / 2] = version;
            at = b.ofs_delta(at, &delta_of(&blob, &next));
            blob = next;
        }
        let pack = b.finish();
        let mut ix = Indexer::new(1);
        let ids = |ix: &mut Indexer| -> Vec<Oid> {
            ix.index(&pack).unwrap().iter().map(|f| f.oid).collect()
        };
        let first = ids(&mut ix);
        assert_eq!(first[2], hash_object(OBJ_BLOB, &blob));
        let bytes: usize = held(&ix.resolve[0]).iter().sum();
        assert!(bytes <= SCRATCH_KEEP, "{bytes} bytes kept");
        assert_eq!(ids(&mut ix), first);
    }

    /// Nor does a worker keep a buffer for every object that had deltas waiting at once.
    /// Here 200 versions of a blob each have two deltas, the next version and a dead end,
    /// in that order, so every version waits for its dead end while the rest of the chain
    /// is resolved. A worker looks at its buffers after every family: with one kept for
    /// each of 100,000 such versions, the 200,000 small families after them took 13 s on
    /// one thread, not 0.3 s.
    #[test]
    fn a_family_with_many_objects_waiting_keeps_few_buffers() {
        let mut b = PackBuilder::new();
        let mut blob = b"version 0\n".to_vec();
        b.whole(OBJ_BLOB, &blob);
        for version in 1..=200 {
            let base = hash_object(OBJ_BLOB, &blob);
            let next = [&blob[..], format!("version {version}\n").as_bytes()].concat();
            let end = [&blob[..], format!("dead end {version}\n").as_bytes()].concat();
            b.ref_delta(&base, &delta_of(&blob, &next));
            b.ref_delta(&base, &delta_of(&blob, &end));
            blob = next;
        }
        let pack = b.finish();
        let mut ix = Indexer::new(1);
        let facts = ix.index(&pack).unwrap().to_vec();
        assert_eq!(our_entries(&facts), git_entries("waiting", &pack));
        let buffers = ix.resolve[0].levels.len();
        assert!(buffers <= SCRATCH_BUFFERS, "{buffers} buffers kept");
    }

    /// Families of every shape the walk tells apart, checked against git entry by entry
    /// (id, type, chain depth, base): objects with no delta, one, or several; a last
    /// delta that has deltas of its own, and one that has none; OFS and REF deltas mixed
    /// under one base; REF deltas before their bases; enough families for several workers.
    #[test]
    fn branching_families_are_resolved_as_git_resolves_them() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut rand = move |m: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % m as u64) as usize
        };
        // 80 whole blobs, then 1,120 more, each an edit of an earlier object picked at
        // random and stored as a delta of it.
        let (roots, objects) = (80, 1200);
        let mut content: Vec<Vec<u8>> = Vec::new();
        let mut parent = Vec::new();
        let mut ofs = Vec::new();
        for i in 0..objects {
            let p = if i < roots { i } else { rand(i) };
            let mut c = if i < roots {
                Vec::new()
            } else {
                content[p].clone()
            };
            c.extend_from_slice(format!("line {i} of blob {p}\n").as_bytes());
            content.push(c);
            parent.push(p);
            ofs.push(rand(2) == 0);
        }
        let id = |i: usize| hash_object(OBJ_BLOB, &content[i]);
        let delta = |i: usize| delta_of(&content[parent[i]], &content[i]);
        // Half of the REF deltas first, newest first: each is in the pack before its base.
        let early = |i: usize| i >= roots && !ofs[i] && i % 2 == 1;
        let mut b = PackBuilder::new();
        let mut at = vec![0u64; objects];
        for i in (0..objects).rev().filter(|&i| early(i)) {
            at[i] = b.ref_delta(&id(parent[i]), &delta(i));
        }
        for i in (0..objects).filter(|&i| !early(i)) {
            at[i] = if i < roots {
                b.whole(OBJ_BLOB, &content[i])
            } else if ofs[i] {
                b.ofs_delta(at[parent[i]], &delta(i))
            } else {
                b.ref_delta(&id(parent[i]), &delta(i))
            };
        }
        let pack = b.finish();
        let git = git_entries("families", &pack);
        assert_eq!(git.len(), objects);
        for threads in [1, 4] {
            let mut ix = Indexer::new(threads);
            let facts = ix.index(&pack).unwrap().to_vec();
            assert!(ix.roots.len() >= 64, "{} families", ix.roots.len());
            assert!(our_entries(&facts) == git, "on {threads} threads");
        }
    }

    /// The facts of `pack` and the number of deltas resolved for them, by an indexer on
    /// a thread of its own: a walk that does not end fails the test, it does not hold it.
    fn index_soon(pack: &[u8], threads: usize) -> (Vec<Facts>, usize) {
        let (tx, rx) = std::sync::mpsc::channel();
        let pack = pack.to_vec();
        std::thread::spawn(move || {
            let mut ix = Indexer::new(threads);
            let facts = ix.index(&pack).map(|f| f.to_vec());
            let resolved: usize = ix.resolve.iter().map(|s| s.out.len()).sum();
            let _ = tx.send((facts, resolved));
        });
        let (facts, resolved) = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the indexer is still resolving after 5 s");
        (facts.unwrap(), resolved)
    }

    /// A pack git accepts can hold a delta that makes its base's id again: here a blob
    /// with a REF delta on it that copies it (79 bytes), and a blob with an edit and the
    /// edit's undoing. Each delta is resolved once, to the id git gives it. The copy
    /// used to wait for its own result without end, at 0.5 GB of memory a second.
    #[test]
    fn a_delta_that_makes_its_base_again_is_resolved_once() {
        let (x, y) = (b"hello".to_vec(), b"hello!".to_vec());
        let (xo, yo) = (hash_object(OBJ_BLOB, &x), hash_object(OBJ_BLOB, &y));
        let mut copy = PackBuilder::new();
        copy.whole(OBJ_BLOB, &x);
        copy.ref_delta(&xo, &delta_of(&x, &x));
        let mut undo = PackBuilder::new();
        undo.whole(OBJ_BLOB, &x);
        undo.ref_delta(&xo, &delta_of(&x, &y));
        undo.ref_delta(&yo, &delta_of(&y, &x));
        for (name, pack, ids) in [
            ("copy", copy.finish(), vec![xo, xo]),
            ("undo", undo.finish(), vec![xo, yo, xo]),
        ] {
            let (facts, resolved) = index_soon(&pack, 1);
            assert_eq!(resolved, ids.len() - 1, "{name}");
            for (i, (f, id)) in facts.iter().zip(&ids).enumerate() {
                let base = if i == 0 { NO_BASE } else { i as u32 - 1 };
                assert_eq!((f.oid, f.depth, f.base), (*id, i as u8, base), "{name}");
            }
            assert_eq!(our_index(&facts), git_index(name, &pack));
        }
    }

    /// Deltas on a base the pack holds twice are resolved once, against one copy of it.
    /// Resolved against every copy, the 80 deltas of a chain whose every version is
    /// stored twice took 2^40 resolutions.
    #[test]
    fn deltas_on_a_base_stored_twice_are_resolved_once() {
        let mut chain = PackBuilder::new();
        let mut blob = b"a".to_vec();
        for _ in 0..2 {
            chain.whole(OBJ_BLOB, &blob);
        }
        for _ in 0..40 {
            let next = [&blob[..], b"a"].concat();
            for _ in 0..2 {
                chain.ref_delta(&hash_object(OBJ_BLOB, &blob), &delta_of(&blob, &next));
            }
            blob = next;
        }
        let chain = chain.finish();
        let (facts, resolved) = index_soon(&chain, 1);
        assert_eq!(resolved, 80);
        assert_eq!(our_index(&facts), git_index("chain-twice", &chain));

        // Enough families for several workers: both copies of a base may ask at once.
        let mut pairs = PackBuilder::new();
        for i in 0..100 {
            let blob = format!("blob {i}").into_bytes();
            for _ in 0..2 {
                pairs.whole(OBJ_BLOB, &blob);
            }
            let next = [&blob[..], b", edited"].concat();
            pairs.ref_delta(&hash_object(OBJ_BLOB, &blob), &delta_of(&blob, &next));
        }
        let pairs = pairs.finish();
        let (facts, resolved) = index_soon(&pairs, 4);
        assert_eq!(resolved, 100);
        assert_eq!(our_index(&facts), git_index("pairs", &pairs));
    }

    #[test]
    fn index_matches_git_with_ref_deltas() {
        let dir = std::env::temp_dir().join(format!("gvd-ingest-ref-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let pack = sample_pack_with(&dir, false);
        let expected = git_ids(&dir, &pack);
        let mut ix = Indexer::new(2);
        let facts = ix.index(&pack).unwrap().to_vec();
        let mut got: Vec<(String, String, u64)> = facts
            .iter()
            .map(|f| {
                (
                    f.oid.to_string(),
                    crate::pack::type_name(f.kind).to_string(),
                    f.size,
                )
            })
            .collect();
        let mut want = expected;
        got.sort();
        want.sort();
        assert_eq!(got, want);
        // REF_DELTA chains: some delta's base is itself a delta.
        let deltas: std::collections::HashSet<u32> = facts
            .iter()
            .enumerate()
            .filter(|(_, f)| f.base != NO_BASE)
            .map(|(i, _)| i as u32)
            .collect();
        assert!(
            facts
                .iter()
                .any(|f| f.base != NO_BASE && deltas.contains(&f.base)),
            "expected chained REF deltas"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn index_matches_git() {
        let dir = std::env::temp_dir().join(format!("gvd-ingest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let pack = sample_pack(&dir);
        let expected = git_ids(&dir, &pack);
        for threads in [1, 4] {
            let mut ix = Indexer::new(threads);
            let facts = ix.index(&pack).unwrap().to_vec();
            assert_eq!(facts.len(), expected.len());
            let mut got: Vec<(String, String, u64)> = facts
                .iter()
                .map(|f| {
                    (
                        f.oid.to_string(),
                        crate::pack::type_name(f.kind).to_string(),
                        f.size,
                    )
                })
                .collect();
            let mut want = expected.clone();
            got.sort();
            want.sort();
            assert_eq!(got, want);
            assert!(
                facts.iter().any(|f| f.base != NO_BASE),
                "sample should contain deltas"
            );
        }
        // A corrupted byte is caught.
        let mut bad = pack.clone();
        let mid = bad.len() / 2;
        bad[mid] ^= 0x55;
        assert!(Indexer::new(1).index(&bad).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Pass 2 lists its roots on every thread, each thread those of one run of entries.
    /// The list is the one a single thread makes: the whole entries with a delta on them,
    /// in pack order. Here every seventh blob is followed by an OFS delta on it, and every
    /// fifth has a REF delta on it three blobs earlier.
    #[test]
    fn roots_are_listed_in_pack_order_on_any_number_of_threads() {
        let content = |i: usize| format!("blob {i}").into_bytes();
        let edited = |base: &[u8], how: &str| delta_of(base, &[base, how.as_bytes()].concat());
        let blobs = 40_000;
        let mut b = PackBuilder::new();
        let mut entries = 0u32;
        let mut want = Vec::new();
        for i in 0..blobs {
            let blob = content(i);
            let at = b.whole(OBJ_BLOB, &blob);
            if i % 7 == 3 || i % 5 == 4 {
                want.push(entries);
            }
            entries += 1;
            if i % 7 == 3 {
                b.ofs_delta(at, &edited(&blob, ", edited"));
                entries += 1;
            }
            if i % 5 == 1 && i + 3 < blobs {
                let base = content(i + 3);
                b.ref_delta(&hash_object(OBJ_BLOB, &base), &edited(&base, ", renamed"));
                entries += 1;
            }
        }
        let pack = b.finish();
        assert!(
            entries as usize > 3 * ROOTS_MIN_CHUNK,
            "{entries} entries are no four threads' worth"
        );
        let facts = |ix: &mut Indexer| -> Vec<(Oid, u32, u8)> {
            let facts = ix.index(&pack).unwrap();
            facts.iter().map(|f| (f.oid, f.base, f.depth)).collect()
        };
        let (mut one, mut four) = (Indexer::new(1), Indexer::new(4));
        let (by_one, by_four) = (facts(&mut one), facts(&mut four));
        assert_eq!(by_one.len(), entries as usize);
        assert_eq!(one.roots, want);
        assert_eq!(four.roots, want);
        assert!(by_one == by_four);
    }

    /// The pipeline indexes two packs with one indexer: the upstream's, then its repack's.
    /// A first pack resolved by several workers, then one small enough for one worker,
    /// gives the second pack exactly the facts a fresh indexer gives it (the unused
    /// workers' results from the first pack once overwrote a third of its ids).
    #[test]
    fn a_reused_indexer_keeps_no_earlier_results() {
        let dir = std::env::temp_dir().join(format!("gvfs-reuse-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let pack_of =
            |repo: &str, count: u32, files: &dyn Fn(u32, u32) -> Vec<u8>, versions: u32| {
                let wd = dir.join(repo);
                let git = |args: &[&str], stdin: &[u8]| {
                    let mut child = Command::new("git")
                        .args(args)
                        .current_dir(&wd)
                        .env("GIT_CONFIG_NOSYSTEM", "1")
                        .env("GIT_CONFIG_GLOBAL", "/dev/null")
                        .stdin(std::process::Stdio::piped())
                        .stdout(std::process::Stdio::piped())
                        .spawn()
                        .unwrap();
                    use std::io::Write;
                    child.stdin.take().unwrap().write_all(stdin).unwrap();
                    let out = child.wait_with_output().unwrap();
                    assert!(out.status.success(), "git {args:?}");
                    out.stdout
                };
                std::fs::create_dir_all(&wd).unwrap();
                git(&["init", "-q", "-b", "main"], b"");
                for v in 0..versions {
                    for i in 0..count {
                        std::fs::write(wd.join(format!("f{i}")), files(i, v)).unwrap();
                    }
                    git(&["add", "-A"], b"");
                    let who = ["-c", "user.name=t", "-c", "user.email=t@t"];
                    git(&[&who[..], &["commit", "-q", "-m", "v"]].concat(), b"");
                }
                let pack = [
                    "pack-objects",
                    "--revs",
                    "--stdout",
                    "-q",
                    "--delta-base-offset",
                ];
                git(&pack, b"main\n")
            };
        // 150 files changed once: 150 delta families, resolved by several workers, the
        // deltas (after the newer versions) at entries 150 to 300 or so.
        let families = pack_of(
            "families",
            150,
            &|i, v| {
                (0..100)
                    .map(|l| format!("file {i} line {l}{}\n", if l == 7 { v } else { 0 }))
                    .collect::<String>()
                    .into_bytes()
            },
            2,
        );
        // 600 files of noise: whole objects only, which one worker resolves, at entry
        // numbers the first pack's deltas had too.
        let whole = pack_of(
            "whole",
            600,
            &|i, _| {
                let mut x = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                (0..300)
                    .map(|_| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        x as u8
                    })
                    .collect()
            },
            1,
        );
        let facts = |f: &[Facts]| -> Vec<(Oid, u32, u8, u8, u64)> {
            f.iter()
                .map(|f| (f.oid, f.base, f.kind, f.depth, f.size))
                .collect()
        };
        let want = facts(Indexer::new(4).index(&whole).unwrap());

        let mut ix = Indexer::new(4);
        let mut spread = false;
        for _ in 0..50 {
            ix.index(&families).unwrap();
            assert!(ix.roots.len() >= 64, "{} families", ix.roots.len());
            if ix.resolve.iter().filter(|s| !s.out.is_empty()).count() > 1 {
                spread = true;
                break;
            }
        }
        assert!(
            spread,
            "the first pack was never resolved by more than one worker"
        );
        assert_eq!(facts(ix.index(&whole).unwrap()), want);
        assert!(ix.roots.len() < 64);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
