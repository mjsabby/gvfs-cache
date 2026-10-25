//! Object access for the edge: type/size without inflating, full resolution of delta chains
//! into a request arena, and the plan for a `POST /gvfs/objects` pack.
//!
//! A planned pack keeps an entry as a delta only when its base is in the same pack (then
//! the OFS distance is re-encoded for the new positions); every other delta is resolved and
//! deflated as a full object while the response streams, so the pack is always
//! self-contained and never drags in the history of a directory.

use core::fmt;

use crate::arena::{Arena, Span};
use crate::delta::{apply_delta, delta_sizes};
use crate::mphf::Loc;
use crate::oid::Oid;
use crate::pack::{
    DeltaBase, ObjType, PACK_HEADER_LEN, PACK_TRAILER_LEN, encode_ofs_delta_header,
    parse_entry_header,
};
use crate::slab::Slab;
use crate::zlib::Inflater;

use super::catalog::Catalog;
use super::json::{ObjectsRequest, oid_at};
use super::pool::{PlanEntry, RequestArena};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjError {
    Corrupt,
    ArenaFull,
    PlanFull,
    ChainTooDeep,
}

impl fmt::Display for ObjError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Corrupt => "corrupt pack entry",
            Self::ArenaFull => "request exceeds the arena",
            Self::PlanFull => "request expands to too many objects",
            Self::ChainTooDeep => "delta chain too deep",
        })
    }
}

impl std::error::Error for ObjError {}

const MAX_CHAIN: usize = 4096;
pub const NO_BASE: u32 = u32::MAX;

/// Offset of the OFS_DELTA base of the entry at `offset`, if it is a delta.
#[inline]
fn base_offset(slab: &Slab, offset: u64) -> Result<Option<u64>, ObjError> {
    let bytes = slab.pack.get(offset as usize..).ok_or(ObjError::Corrupt)?;
    let h = parse_entry_header(bytes).map_err(|_| ObjError::Corrupt)?;
    match h.base {
        DeltaBase::None => Ok(None),
        DeltaBase::Ofs(d) => Ok(Some(offset.checked_sub(d).ok_or(ObjError::Corrupt)?)),
        DeltaBase::Ref(_) => Err(ObjError::Corrupt),
    }
}

/// `(type, inflated size)` of the object at `loc`, inflating at most a delta's size header.
pub fn object_info(
    slab: &Slab,
    loc: Loc,
    inflater: &mut Inflater,
) -> Result<(ObjType, u64), ObjError> {
    let bytes = slab.entry(loc);
    let h = parse_entry_header(bytes).map_err(|_| ObjError::Corrupt)?;
    if !h.kind.is_delta() {
        return Ok((h.kind, h.size));
    }
    let mut prefix = [0u8; 32];
    let z = bytes.get(h.header_len..).ok_or(ObjError::Corrupt)?;
    let n = inflater
        .inflate_prefix(z, &mut prefix)
        .map_err(|_| ObjError::Corrupt)?;
    let (_, result, _) =
        delta_sizes(prefix.get(..n).unwrap_or(&[])).map_err(|_| ObjError::Corrupt)?;
    let mut off = loc.offset;
    for _ in 0..MAX_CHAIN {
        match base_offset(slab, off)? {
            Some(b) => off = b,
            None => {
                let root =
                    parse_entry_header(slab.pack.get(off as usize..).ok_or(ObjError::Corrupt)?)
                        .map_err(|_| ObjError::Corrupt)?;
                return Ok((root.kind, result));
            }
        }
    }
    Err(ObjError::ChainTooDeep)
}

/// Inflates the entry at `offset` onto the arena top; returns its span.
fn inflate_at(
    slab: &Slab,
    offset: u64,
    arena: &mut Arena,
    inflater: &mut Inflater,
) -> Result<Span, ObjError> {
    let bytes = slab.pack.get(offset as usize..).ok_or(ObjError::Corrupt)?;
    let h = parse_entry_header(bytes).map_err(|_| ObjError::Corrupt)?;
    let size = usize::try_from(h.size).map_err(|_| ObjError::ArenaFull)?;
    if size > arena.remaining() {
        return Err(ObjError::ArenaFull);
    }
    let z = bytes.get(h.header_len..).ok_or(ObjError::Corrupt)?;
    inflater
        .inflate_exact(z, arena.free_tail_mut(), size)
        .map_err(|_| ObjError::Corrupt)?;
    arena.commit(size).map_err(|_| ObjError::ArenaFull)
}

/// Resolves the object at `loc` to its full content on the arena top (delta chains are
/// applied root-down with two alternating buffers). Returns `(type, content span)`.
pub fn resolve_full(
    slab: &Slab,
    loc: Loc,
    arena: &mut Arena,
    inflater: &mut Inflater,
    chain: &mut Vec<u64>,
) -> Result<(ObjType, Span), ObjError> {
    chain.clear();
    let mut off = loc.offset;
    loop {
        if chain.len() >= MAX_CHAIN || chain.len() >= chain.capacity() {
            return Err(ObjError::ChainTooDeep);
        }
        chain.push(off);
        match base_offset(slab, off)? {
            Some(b) => off = b,
            None => break,
        }
    }
    let root_off = chain.pop().ok_or(ObjError::Corrupt)?;
    let kind = parse_entry_header(
        slab.pack
            .get(root_off as usize..)
            .ok_or(ObjError::Corrupt)?,
    )
    .map_err(|_| ObjError::Corrupt)?
    .kind;
    let floor = arena.mark();
    let mut content = inflate_at(slab, root_off, arena, inflater)?;
    while let Some(delta_off) = chain.pop() {
        let delta = inflate_at(slab, delta_off, arena, inflater)?;
        let (_, result_size, _) = delta_sizes(arena.get(delta)).map_err(|_| ObjError::Corrupt)?;
        let result_len = usize::try_from(result_size).map_err(|_| ObjError::ArenaFull)?;
        let out = arena.alloc(result_len).map_err(|_| ObjError::ArenaFull)?;
        {
            let (lo, hi) = arena.split_at(delta.start);
            let base = lo
                .get(content.start..content.end())
                .ok_or(ObjError::Corrupt)?;
            let (d, o) = hi.split_at_mut(delta.len.min(hi.len()));
            let o = o.get_mut(..result_len).ok_or(ObjError::ArenaFull)?;
            apply_delta(base, d, o).map_err(|_| ObjError::Corrupt)?;
        }
        arena.copy_within(out, floor);
        arena.truncate(floor.saturating_add(result_len));
        content = Span::new(floor, result_len);
    }
    Ok((kind, content))
}

/// Parses `tree <hex>` and `parent <hex>` header lines of a commit.
fn commit_refs(content: &[u8], mut on_ref: impl FnMut(bool, Oid)) {
    for line in content.split(|&b| b == b'\n') {
        if line.is_empty() {
            break;
        }
        if let Some(h) = line.strip_prefix(b"tree ") {
            if let Some(o) = Oid::from_hex(h.get(..40).unwrap_or(&[])) {
                on_ref(true, o);
            }
        } else if let Some(h) = line.strip_prefix(b"parent ")
            && let Some(o) = Oid::from_hex(h.get(..40).unwrap_or(&[]))
        {
            on_ref(false, o);
        }
    }
}

/// Calls `on_subtree(oid)` for every directory entry of a tree.
fn tree_subtrees(content: &[u8], mut on_subtree: impl FnMut(Oid)) {
    let mut rest = content;
    while !rest.is_empty() {
        let Some(nul) = rest.iter().position(|&b| b == 0) else {
            break;
        };
        let Some(oid_bytes) = rest.get(nul.saturating_add(1)..nul.saturating_add(21)) else {
            break;
        };
        if rest.get(..nul).unwrap_or(&[]).starts_with(b"40000 ")
            && let Some(o) = Oid::from_slice(oid_bytes)
        {
            on_subtree(o);
        }
        rest = rest.get(nul.saturating_add(21)..).unwrap_or(&[]);
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlanStats {
    pub requested: usize,
    pub missing: usize,
    pub entries: usize,
    /// Entries resolved + deflated while streaming (deltas whose base is not in the pack).
    pub converted: usize,
    /// Exact pack length when nothing is converted.
    pub exact_len: Option<u64>,
}

#[inline]
fn add_entry(ra: &mut RequestArena, slab: usize, slot: u32, loc: Loc) -> Result<(), ObjError> {
    if ra.plan.len() >= ra.plan.capacity() {
        return Err(ObjError::PlanFull);
    }
    ra.plan.push(PlanEntry {
        slab: slab as u8,
        convert: false,
        offset: loc.offset,
        len: loc.len,
        slot,
        base: NO_BASE,
    });
    Ok(())
}

#[inline]
fn push(ra: &mut RequestArena, k: usize, slot: u32, depth: u32) -> Result<(), ObjError> {
    if ra.stack.len() >= ra.stack.capacity() {
        return Err(ObjError::PlanFull);
    }
    ra.stack.push((k as u8, slot, depth));
    Ok(())
}

/// Builds the plan for `req` into `ra.plan`: requested objects, commits' tree closures and
/// (for `commit_depth > 1`) parents, sorted by slab and offset with base links resolved.
pub fn plan_objects(
    cat: &Catalog,
    req: &ObjectsRequest,
    ra: &mut RequestArena,
) -> Result<PlanStats, ObjError> {
    let mut inflater = ra.inflater.take().unwrap_or_default();
    let mut chain = core::mem::take(&mut ra.chain);
    let r = plan_inner(cat, req, ra, &mut inflater, &mut chain);
    ra.inflater = Some(inflater);
    ra.chain = chain;
    r
}

fn plan_inner(
    cat: &Catalog,
    req: &ObjectsRequest,
    ra: &mut RequestArena,
    inflater: &mut Inflater,
    chain: &mut Vec<u64>,
) -> Result<PlanStats, ObjError> {
    let mut stats = PlanStats {
        requested: req.count,
        ..PlanStats::default()
    };
    ra.layout_seen(cat.probe_slabs().map(|s| u64::from(s.meta.object_count)));
    for i in 0..req.count {
        let Some(oid) = oid_at(&ra.bytes, req, i) else {
            continue;
        };
        let Some((k, slab, slot, loc)) = cat.lookup(&oid) else {
            stats.missing = stats.missing.saturating_add(1);
            continue;
        };
        if !ra.mark(k, slot as u32) {
            continue;
        }
        add_entry(ra, k, slot as u32, loc)?;
        if req.commit_depth >= 1 {
            let (kind, _) = object_info(slab, loc, inflater)?;
            if kind == ObjType::Commit {
                push(ra, k, slot as u32, req.commit_depth)?;
                walk(cat, ra, inflater, chain)?;
            }
        }
    }
    finalize(cat, ra, &mut stats)?;
    Ok(stats)
}

/// Drains `ra.stack`: a commit item (depth ≥ 1) adds its root tree and, for depth > 1, its
/// parents; a tree item (depth 0) adds itself and queues its subtrees.
fn walk(
    cat: &Catalog,
    ra: &mut RequestArena,
    inflater: &mut Inflater,
    chain: &mut Vec<u64>,
) -> Result<(), ObjError> {
    while let Some((k, slot, depth)) = ra.stack.pop() {
        let Some(slab) = cat.probe(usize::from(k)).cloned() else {
            continue;
        };
        let Some(loc) = slab.mphf().and_then(|m| m.loc_at(slot as usize)) else {
            continue;
        };
        let mark = ra.bytes.mark();
        let (kind, content) = resolve_full(&slab, loc, &mut ra.bytes, inflater, chain)?;
        if depth >= 1 {
            if kind == ObjType::Commit {
                let mut refs: [(bool, Oid); 32] = [(false, Oid::ZERO); 32];
                let mut n = 0usize;
                commit_refs(ra.bytes.get(content), |is_tree, o| {
                    if let Some(r) = refs.get_mut(n) {
                        *r = (is_tree, o);
                        n = n.saturating_add(1);
                    }
                });
                for (is_tree, o) in refs.iter().take(n) {
                    let Some((k2, _, slot2, loc2)) = cat.lookup(o) else {
                        continue;
                    };
                    if !ra.mark(k2, slot2 as u32) {
                        continue;
                    }
                    if *is_tree {
                        push(ra, k2, slot2 as u32, 0)?;
                    } else if depth > 1 {
                        add_entry(ra, k2, slot2 as u32, loc2)?;
                        push(ra, k2, slot2 as u32, depth.saturating_sub(1))?;
                    }
                }
            }
            ra.bytes.truncate(mark);
            continue;
        }
        if kind == ObjType::Tree {
            add_entry(ra, usize::from(k), slot, loc)?;
            // Collect subtree ids above the tree bytes, then look them up.
            let list_start = ra.bytes.mark();
            let mut count = 0usize;
            let mut overflow = false;
            {
                let (lo, free) = ra.bytes.split_at(list_start);
                let tree_bytes = lo.get(content.start..content.end()).unwrap_or(&[]);
                let mut at = 0usize;
                tree_subtrees(tree_bytes, |o| {
                    if let Some(dst) = free.get_mut(at..at.saturating_add(20)) {
                        dst.copy_from_slice(&o.0);
                        at = at.saturating_add(20);
                        count = count.saturating_add(1);
                    } else {
                        overflow = true;
                    }
                });
            }
            if overflow {
                return Err(ObjError::ArenaFull);
            }
            let list = ra
                .bytes
                .commit(count.saturating_mul(20))
                .map_err(|_| ObjError::ArenaFull)?;
            for i in 0..count {
                let at = list.start.saturating_add(i.saturating_mul(20));
                let Some(o) = Oid::from_slice(ra.bytes.get(Span::new(at, 20))) else {
                    continue;
                };
                if let Some((k2, _, slot2, _)) = cat.lookup(&o)
                    && ra.mark(k2, slot2 as u32)
                {
                    push(ra, k2, slot2 as u32, 0)?;
                }
            }
        }
        ra.bytes.truncate(mark);
    }
    Ok(())
}

/// Sorts the plan by (slab, offset), links deltas to in-plan bases or marks them for
/// conversion, and computes the exact pack length when no conversion is needed.
fn finalize(cat: &Catalog, ra: &mut RequestArena, stats: &mut PlanStats) -> Result<(), ObjError> {
    ra.plan.sort_unstable_by_key(|e| (e.slab, e.offset));
    ra.plan.dedup_by_key(|e| (e.slab, e.offset));
    let mut pos = PACK_HEADER_LEN as u64;
    let mut exact = true;
    ra.nums.clear();
    for i in 0..ra.plan.len() {
        let e = ra.plan.get(i).copied().unwrap_or_default();
        let Some(slab) = cat.probe(usize::from(e.slab)) else {
            return Err(ObjError::Corrupt);
        };
        let h = parse_entry_header(slab.entry(Loc {
            offset: e.offset,
            len: e.len,
        }))
        .map_err(|_| ObjError::Corrupt)?;
        if ra.nums.len() >= ra.nums.capacity() {
            return Err(ObjError::PlanFull);
        }
        ra.nums.push(pos);
        let len = match h.base {
            DeltaBase::Ofs(d) => {
                let base_off = e.offset.checked_sub(d).ok_or(ObjError::Corrupt)?;
                let j = ra
                    .plan
                    .partition_point(|x| (x.slab, x.offset) < (e.slab, base_off));
                if ra
                    .plan
                    .get(j)
                    .is_some_and(|x| x.slab == e.slab && x.offset == base_off)
                {
                    if let Some(x) = ra.plan.get_mut(i) {
                        x.base = j as u32;
                    }
                    let base_pos = ra.nums.get(j).copied().ok_or(ObjError::Corrupt)?;
                    let mut hdr = [0u8; 20];
                    let hl =
                        encode_ofs_delta_header(h.size, pos.saturating_sub(base_pos), &mut hdr);
                    (hl as u64).saturating_add((e.len as u64).saturating_sub(h.header_len as u64))
                } else {
                    if let Some(x) = ra.plan.get_mut(i) {
                        x.convert = true;
                    }
                    stats.converted = stats.converted.saturating_add(1);
                    exact = false;
                    0
                }
            }
            DeltaBase::Ref(_) => return Err(ObjError::Corrupt),
            DeltaBase::None => e.len as u64,
        };
        pos = pos.saturating_add(len);
    }
    stats.entries = ra.plan.len();
    stats.exact_len = if exact {
        Some(pos.saturating_add(PACK_TRAILER_LEN as u64))
    } else {
        None
    };
    Ok(())
}
