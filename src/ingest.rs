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
//! families from a shared counter and walk each depth-first, keeping one content buffer
//! per depth: inflate the delta, apply it to the parent's content, hash the result.
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

/// Per-worker resolve state: one content buffer per chain depth.
struct ResolveScratch {
    inflater: Inflater,
    delta: Vec<u8>,
    /// Content of the object at each depth of the current walk (`lens` bytes are valid).
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
            roots,
            facts,
            resolve,
            ..
        } = self;
        let ctx = ResolveCtx {
            pack,
            end,
            scanned,
            child_start,
            children,
            waiters,
            fanout,
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
        roots.clear();
        for (i, sc) in scanned.iter().enumerate() {
            let (wlo, whi) = ctx.waiting_range(&sc.oid);
            if !is_delta(sc.kind) && (!ctx.ofs_kids(u32_of(i)).is_empty() || wlo < whi) {
                roots.push(u32_of(i));
            }
        }

        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let workers = if roots.len() < 64 {
            1
        } else {
            (*threads).min(roots.len())
        };
        let roots: &[u32] = roots;
        let run = |scratch: &mut ResolveScratch| -> Result<()> {
            scratch.out.clear();
            loop {
                let r = next.fetch_add(1, Ordering::Relaxed);
                let Some(&root) = roots.get(r) else { break };
                if failed.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = resolve_family(&ctx, root, scratch) {
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

/// Read-only inputs shared by the resolver threads.
struct ResolveCtx<'a> {
    pack: &'a [u8],
    end: usize,
    scanned: &'a [Scanned],
    child_start: &'a [u32],
    children: &'a [u32],
    waiters: &'a [(Oid, u32)],
    fanout: &'a [u32],
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

/// One frame of the depth-first walk: a resolved object whose content is `levels[depth]`.
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
    let (ref_lo, ref_hi) = ctx.waiting_range(&root_sc.oid);
    stack.clear();
    stack.push(Frame {
        entry: root,
        depth: 0,
        ofs_next: 0,
        ref_lo,
        ref_hi,
    });
    while let Some(top) = stack.last_mut() {
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
        let (parent, depth) = (top.entry, top.depth);
        let child_sc = ctx.scanned.get(child as usize).ctx("child out of range")?;
        if !is_delta(child_sc.kind) {
            continue;
        }
        let dlen = inflate_entry(ctx, child, inflater, delta)?;
        let delta_bytes = delta.get(..dlen).ctx("delta buffer")?;
        let (_, rsz, _) = delta_sizes(delta_bytes).ctx("bad delta header")?;
        let d = depth as usize;
        let next_level = d.checked_add(1).ctx("depth overflow")?;
        while levels.len() <= next_level {
            levels.push(Vec::new());
            lens.push(0);
        }
        let (lower, upper) = levels.split_at_mut(next_level);
        let parent_len = lens.get(d).copied().unwrap_or(0);
        let parent_bytes = lower
            .get(d)
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
        let (ref_lo, ref_hi) = ctx.waiting_range(&oid);
        if !ctx.ofs_kids(child).is_empty() || ref_lo < ref_hi {
            stack.push(Frame {
                entry: child,
                depth: depth.saturating_add(1),
                ofs_next: 0,
                ref_lo,
                ref_hi,
            });
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

/// Upstream credentials from `--token-env` / `--token-user` / `--auth-header-env`.
pub fn auth_from_flags(f: &crate::cli::Flags) -> Result<crate::upstream::Auth> {
    if let Some(var) = f.get("auth-header-env") {
        let v =
            std::env::var(var).map_err(|_| crate::err!("environment variable {var} is not set"))?;
        return Ok(crate::upstream::Auth { header: Some(v) });
    }
    if let Some(var) = f.get("token-env") {
        let v =
            std::env::var(var).map_err(|_| crate::err!("environment variable {var} is not set"))?;
        return Ok(crate::upstream::Auth {
            header: Some(crate::http::basic_auth(
                f.get("token-user").unwrap_or(""),
                &v,
            )),
        });
    }
    Ok(crate::upstream::Auth::default())
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
pub(crate) mod tests {
    use super::*;
    use std::process::Command;

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
}
