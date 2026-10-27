//! **The optimal splice.** [`Repacker::repack`] merges slabs into one slab about as small as
//! a single upstream fetch of the whole range, without asking the upstream for anything.
//!
//! Concatenating slabs is not enough. Every slab is fetched for its own commit range and
//! must stand alone (clients index each pack by itself), so it stores whole the first
//! version of every directory its range touched. A day of 15-minute slabs therefore
//! repeats the hot directories (the root tree, ...) once per slab: 3.5 MB for a SearchGold
//! day that one fetch delivers in 622 KB.
//!
//! What `repack` writes:
//!
//! 1. **Objects:** the union of the inputs, first copy wins. Objects of `frozen` (the old
//!    baseline, copied byte for byte as the output's prefix) or of `exclude` are left out.
//! 2. **Paths:** the new commits are walked newest first and every new tree gets the
//!    directory path it is first found at. The walk only descends into new trees: an
//!    unchanged subtree belongs to older history.
//! 3. **Tree clusters:** the versions of one directory are written back to back, newest
//!    first. The newest is whole (appending to `frozen`: a delta against the frozen
//!    version), each older one is a delta against its newer neighbour, and a chain starts
//!    over whole after [`MAX_DEPTH`]. Deltas come from [`crate::delta::tree_delta`]. The
//!    newest version of a directory is one read; an older one is the contiguous byte range
//!    from its cluster's head.
//! 4. **Everything else** (commits, `Git-hfs` pointer blobs, tags) keeps its compressed
//!    bytes. Deltas among them are re-pointed as OFS_DELTA (ADO sends REF_DELTA, which
//!    spends 20 bytes per entry on the base id); one whose base is not in the output, or
//!    which is at most [`WHOLE_MAX`] bytes (pointer blobs, commits), is stored whole: a
//!    request for it is then a splice of its own bytes, not an inflate of its whole chain
//!    (ADO's chains run 50 deep), and a pack of it needs no bases. So is one whose chain
//!    would pass [`MAX_DEPTH`]: a delta against a frozen or earlier object adds a level
//!    to that object's chain, which would otherwise grow with every repack.
//! 5. **Verification:** the output is indexed by the same indexer as an upstream pack,
//!    which recomputes every id from content, and must hold exactly the expected objects.
//!
//! Output order: (frozen prefix), commits and tags, tree clusters, blobs; each group in
//! input order. [`Mode::Copy`] skips steps 2-3 and writes everything in input order: the
//! seed's deltas are only re-pointed (the upstream's tree deltas are already local to each
//! directory, and re-clustering all of history would buy little).

use std::cmp::Ordering;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use sha1::{Digest, Sha1};

use crate::delta::{apply_delta, delta_sizes, tree_delta};
use crate::ingest::{Dest, Indexer, Ingested, file_slab, memory_slab};
use crate::object::{TreeIter, commit_parents, commit_time, commit_tree};
use crate::pack::{HeaderBytes, OBJ_BLOB, OBJ_COMMIT, OBJ_TREE, pack_header, parse_entry_header};
use crate::slab::{NO_BASE, Slab, Tier};
use crate::splice::Resolver;
use crate::util::{Context, Oid, Result, u32_of, u64_of, us};
use crate::zlib::{Deflater, Inflater, compress_bound};

/// Longest delta chain `repack` writes (git's default `--depth`).
pub const MAX_DEPTH: u8 = 50;
/// zlib level of re-encoded entries (git's default).
const LEVEL: i32 = 6;
/// Non-tree objects up to this size are stored whole even when they arrive as deltas
/// (SearchGold: ~330K such deltas, a few MB more as whole objects).
pub const WHOLE_MAX: u64 = 4 << 10;
const NONE: u32 = u32::MAX;
const NOT_WRITTEN: u64 = u64::MAX;
/// A `map` value with this bit set is a slot of the frozen slab.
const FROZEN: u32 = 1 << 31;
const ROOT_PATH: u64 = 0x9E37_79B9_7F4A_7C15;
/// Seed of the made-up paths of trees no commit reaches.
const UNREACHED: u64 = 0xD6E8_FEB8_6659_FD93;

/// How trees are written.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    /// Re-deltified by directory path (every slab but the seed).
    Trees,
    /// Like every other object: compressed bytes kept, deltas re-pointed (the seed).
    Copy,
}

/// What to repack.
pub struct Input<'a> {
    /// The slabs to merge, oldest first.
    pub slabs: &'a [Arc<Slab>],
    /// Copied byte for byte as the output's prefix: its objects are not repeated, and new
    /// tree versions may be deltas against its versions.
    pub frozen: Option<&'a Arc<Slab>>,
    /// Objects found in these slabs are left out.
    pub exclude: &'a [Arc<Slab>],
    pub mode: Mode,
}

/// One object of the output (the frozen prefix aside).
#[derive(Clone, Copy)]
struct Obj {
    /// Its first copy: input slab and slot.
    slab: u16,
    slot: u32,
    kind: u8,
    /// Delta depth as written.
    depth: u8,
    /// Index into `trees` once the path walk has reached it.
    tree: u32,
    out_at: u64,
}

/// A new tree's place in history.
#[derive(Clone, Copy)]
struct TreeFacts {
    obj: u32,
    path: u64,
    /// Committer time of the newest commit it was found in.
    time: i64,
    /// Discovery rank: newest commits first, depth first.
    rank: u32,
}

/// The versions of one directory: `members[start..end]` (indexes into `trees`), newest
/// first.
#[derive(Clone, Copy)]
struct Cluster {
    start: u32,
    end: u32,
    /// Rank of its newest version (clusters are written in this order).
    rank: u32,
    path: u64,
    /// The frozen slab's version of this directory: (index into `contents`, slot).
    frozen: Option<(u32, u32)>,
}

#[derive(Clone, Copy, Default)]
struct Stats {
    whole_trees: u64,
    delta_trees: u64,
    copied: u64,
    rewritten: u64,
}

/// Open-addressed map from object id to object index (ids are uniform: no hashing).
#[derive(Default)]
struct IdMap {
    slots: Vec<(Oid, u32)>,
    mask: usize,
}

impl IdMap {
    fn reset(&mut self, n: usize) {
        let cap = n.saturating_mul(2).max(16).next_power_of_two();
        self.slots.clear();
        self.slots.resize(cap, (Oid::ZERO, NONE));
        self.mask = cap.saturating_sub(1);
    }

    fn home(&self, oid: &Oid) -> usize {
        let [a, b, c, d, e, f, g, h, ..] = oid.0;
        (u64::from_le_bytes([a, b, c, d, e, f, g, h]) as usize) & self.mask
    }

    fn insert(&mut self, oid: &Oid, v: u32) {
        let mut i = self.home(oid);
        while let Some(s) = self.slots.get_mut(i) {
            if s.1 == NONE || s.0 == *oid {
                *s = (*oid, v);
                return;
            }
            i = i.wrapping_add(1) & self.mask;
        }
    }

    fn get(&self, oid: &Oid) -> Option<u32> {
        let mut i = self.home(oid);
        loop {
            let s = self.slots.get(i)?;
            if s.1 == NONE {
                return None;
            }
            if s.0 == *oid {
                return Some(s.1);
            }
            i = i.wrapping_add(1) & self.mask;
        }
    }
}

/// The hash of directory `name` inside the directory hashed `parent` (FNV-1a, mixed).
/// A collision only costs delta quality: copies are made of byte-identical entries only.
fn path_hash(parent: u64, name: &[u8]) -> u64 {
    let mut h = parent ^ 0xCBF2_9CE4_8422_2325;
    for &b in name {
        h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01B3);
    }
    h ^= h.wrapping_shr(30);
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= h.wrapping_shr(27);
    h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ h.wrapping_shr(31)
}

/// A delta that the output stores as a whole object (see [`WHOLE_MAX`]).
fn stored_whole(rec: &crate::slab::Rec) -> bool {
    rec.base != NO_BASE && rec.kind != OBJ_TREE && rec.size <= WHOLE_MAX
}

/// Orders `(time, index)` pairs newest first, ties by index.
fn newest_first(a: &(i64, u32), b: &(i64, u32)) -> Ordering {
    b.0.cmp(&a.0).then(a.1.cmp(&b.1))
}

/// Deflates `data` into `buf` (grown as needed) and returns the zlib stream.
fn deflate<'a>(d: &mut Deflater, buf: &'a mut Vec<u8>, data: &[u8]) -> Result<&'a [u8]> {
    let bound = compress_bound(data.len());
    if buf.len() < bound {
        buf.resize(bound, 0);
    }
    let n = d.deflate_parts(&[data], buf)?;
    buf.get(..n).ctx("deflate output")
}

/// The output pack: bytes go to a file or to memory, and through SHA-1.
struct Sink {
    file: Option<std::io::BufWriter<std::fs::File>>,
    mem: Vec<u8>,
    sha: Sha1,
    pos: u64,
}

impl Sink {
    fn put(&mut self, b: &[u8]) -> Result<()> {
        match self.file.as_mut() {
            Some(w) => w.write_all(b)?,
            None => self.mem.extend_from_slice(b),
        }
        self.sha.update(b);
        self.pos = self.pos.saturating_add(u64_of(b.len()));
        Ok(())
    }

    /// Appends the SHA-1 trailer; returns the bytes when writing to memory.
    fn finish(self) -> Result<Vec<u8>> {
        let sha: [u8; 20] = self.sha.finalize().into();
        match self.file {
            Some(mut w) => {
                w.write_all(&sha)?;
                let f = w
                    .into_inner()
                    .map_err(|e| crate::err!("writing pack: {e}"))?;
                f.sync_all()?;
                Ok(Vec::new())
            }
            None => {
                let mut mem = self.mem;
                mem.extend_from_slice(&sha);
                Ok(mem)
            }
        }
    }
}

/// Reusable state of [`Repacker::repack`]. Buffers keep their capacity between runs: the
/// server repacks its live-tip slab on every upstream change.
pub struct Repacker {
    objs: Vec<Obj>,
    /// `map[first[k] + slot]` for input slab `k`: an object index, `FROZEN | slot`, or
    /// `NONE` (excluded).
    map: Vec<u32>,
    first: Vec<usize>,
    /// `memo[first[k] + slot]`: index into `contents` once resolved.
    memo: Vec<u32>,
    ids: IdMap,
    /// Resolved commits and trees: `raw[at .. at + len]` for each `(at, len)` of `contents`.
    raw: Vec<u8>,
    contents: Vec<(usize, usize)>,
    /// (committer time, object) of the new commits, newest first.
    commits: Vec<(i64, u32)>,
    trees: Vec<TreeFacts>,
    members: Vec<u32>,
    clusters: Vec<Cluster>,
    /// (path, cluster), sorted.
    paths: Vec<(u64, u32)>,
    /// (child's committer time, frozen slot) of frozen commits the new history builds on.
    boundary: Vec<(i64, u32)>,
    stack: Vec<(u32, u64)>,
    chain: Vec<u32>,
    order: Vec<(u64, u32)>,
    delta: Vec<u8>,
    zbuf: Vec<u8>,
    inflater: Inflater,
    deflater: Deflater,
    resolver: Resolver,
    stats: Stats,
    /// Do not log each repack (the server's per-plan repacks).
    pub quiet: bool,
}

impl Default for Repacker {
    fn default() -> Self {
        Self::new()
    }
}

impl Repacker {
    pub fn new() -> Repacker {
        Repacker {
            objs: Vec::new(),
            map: Vec::new(),
            first: Vec::new(),
            memo: Vec::new(),
            ids: IdMap::default(),
            raw: Vec::new(),
            contents: Vec::new(),
            commits: Vec::new(),
            trees: Vec::new(),
            members: Vec::new(),
            clusters: Vec::new(),
            paths: Vec::new(),
            boundary: Vec::new(),
            stack: Vec::new(),
            chain: Vec::new(),
            order: Vec::new(),
            delta: Vec::new(),
            zbuf: Vec::new(),
            inflater: Inflater::new(),
            deflater: Deflater::new(LEVEL),
            resolver: Resolver::default(),
            stats: Stats::default(),
            quiet: false,
        }
    }

    /// Repacks `input` into one slab at `dest`. `None` when nothing is new (every object is
    /// in `frozen` or `exclude`).
    pub fn repack(
        &mut self,
        indexer: &mut Indexer,
        input: &Input<'_>,
        tier: Tier,
        range: (u64, u64),
        commits: (Oid, Oid),
        dest: Dest<'_>,
    ) -> Result<Option<Ingested>> {
        let t0 = Instant::now();
        self.stats = Stats::default();
        self.raw.clear();
        self.contents.clear();
        self.collect(input)?;
        if self.objs.is_empty() {
            return Ok(None);
        }
        if input.mode == Mode::Trees {
            self.walk(input)?;
            self.cluster(input)?;
        }
        let t_plan = t0.elapsed();

        let frozen_entries = input.frozen.map_or(0, |f| f.entries());
        let entries = frozen_entries
            .checked_add(u32_of(self.objs.len()))
            .ctx("too many objects for one pack")?;
        let tmp = match dest {
            Dest::Dir(dir) => {
                std::fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
                Some(crate::fsutil::temp_path(&dir.join("repack.pack")))
            }
            Dest::Memory => None,
        };
        let input_bytes: usize = input.slabs.iter().map(|s| s.pack().len()).sum();
        let written = (|| -> Result<Vec<u8>> {
            let file = match &tmp {
                Some(p) => Some(std::io::BufWriter::with_capacity(
                    4 << 20,
                    std::fs::File::create(p).ctx("creating repacked pack")?,
                )),
                None => None,
            };
            let mem = if file.is_none() {
                Vec::with_capacity(input_bytes)
            } else {
                Vec::new()
            };
            let mut sink = Sink {
                file,
                mem,
                sha: Sha1::new(),
                pos: 0,
            };
            self.write(input, &mut sink, entries)?;
            sink.finish()
        })();
        let t_write = t0.elapsed();
        let out = match (dest, written, &tmp) {
            (Dest::Dir(dir), Ok(_), Some(p)) => file_slab(indexer, p, dir, tier, range, commits)?,
            (Dest::Memory, Ok(bytes), _) => memory_slab(indexer, bytes, tier, range, commits)?,
            (_, Err(e), _) => {
                if let Some(p) = &tmp {
                    let _ = std::fs::remove_file(p);
                }
                return Err(e);
            }
            _ => crate::bail!("repack: no output"),
        };
        self.verify(input, &out.slab)?;
        if self.quiet {
            return Ok(Some(out));
        }
        let s = self.stats;
        crate::log!(
            "repack: {} slabs ({}) + {} -> {} {} ({} new objects; trees {} whole + {} deltas; {} entries kept, {} stored whole) in {:.2}s (plan {:.2}s, write {:.2}s, index {:.2}s)",
            input.slabs.len(),
            crate::util::human_bytes(u64_of(input_bytes)),
            input.frozen.map_or_else(
                || "nothing".to_string(),
                |f| format!(
                    "frozen {}",
                    crate::util::human_bytes(u64_of(f.pack().len()))
                )
            ),
            out.meta.id,
            crate::util::human_bytes(out.meta.pack_len),
            self.objs.len(),
            s.whole_trees,
            s.delta_trees,
            s.copied,
            s.rewritten,
            t0.elapsed().as_secs_f64(),
            t_plan.as_secs_f64(),
            t_write.saturating_sub(t_plan).as_secs_f64(),
            t0.elapsed().saturating_sub(t_write).as_secs_f64()
        );
        Ok(Some(out))
    }

    /// Step 1: the new objects, each slab's in entry order, first copy wins.
    fn collect(&mut self, input: &Input<'_>) -> Result<()> {
        self.objs.clear();
        self.map.clear();
        self.first.clear();
        if input.frozen.is_some_and(|f| f.count() >= FROZEN)
            || input.slabs.len() > usize::from(u16::MAX)
        {
            crate::bail!("repack input too large");
        }
        let mut order = std::mem::take(&mut self.order);
        for (k, slab) in input.slabs.iter().enumerate() {
            let base = self.map.len();
            self.first.push(base);
            self.map
                .resize(base.saturating_add(slab.count() as usize), NONE);
            order.clear();
            for slot in 0..slab.count() {
                order.push((slab.rec(slot).ctx("record")?.offset, slot));
            }
            order.sort_unstable();
            for &(_, slot) in &order {
                let rec = slab.rec(slot).ctx("record")?;
                let v = if let Some(fs) = input.frozen.and_then(|f| f.find(&rec.oid)) {
                    FROZEN | fs
                } else if input.exclude.iter().any(|e| e.find(&rec.oid).is_some()) {
                    NONE
                } else if let Some(v) = self.earlier(input, k, &rec.oid) {
                    v
                } else {
                    self.objs.push(Obj {
                        slab: k as u16,
                        slot,
                        kind: rec.kind,
                        depth: 0,
                        tree: NONE,
                        out_at: NOT_WRITTEN,
                    });
                    u32_of(self.objs.len().saturating_sub(1))
                };
                if let Some(m) = self.map.get_mut(base.saturating_add(slot as usize)) {
                    *m = v;
                }
            }
        }
        self.order = order;
        self.memo.clear();
        self.memo.resize(self.map.len(), NONE);
        Ok(())
    }

    /// The map value of `oid`'s copy in an input slab before `k`.
    fn earlier(&self, input: &Input<'_>, k: usize, oid: &Oid) -> Option<u32> {
        input.slabs.get(..k)?.iter().enumerate().find_map(|(j, s)| {
            let slot = s.find(oid)?;
            self.map
                .get(self.first.get(j)?.saturating_add(slot as usize))
                .copied()
        })
    }

    fn content(&self, ci: u32) -> &[u8] {
        self.contents
            .get(ci as usize)
            .and_then(|&(at, len)| self.raw.get(at..at.saturating_add(len)))
            .unwrap_or(&[])
    }

    fn resolve(&mut self, input: &Input<'_>, obj: u32) -> Result<u32> {
        let o = *self.objs.get(obj as usize).ctx("object")?;
        self.resolve_at(input, usize::from(o.slab), o.slot)
    }

    /// Resolves input slab `k`'s object at `slot` (and the unresolved part of its delta
    /// chain) into `raw`, memoized. Returns its index into `contents`.
    fn resolve_at(&mut self, input: &Input<'_>, k: usize, slot: u32) -> Result<u32> {
        let slab = input.slabs.get(k).ctx("slab")?;
        let first = *self.first.get(k).ctx("slab")?;
        self.chain.clear();
        let mut known = NONE;
        let mut cur = slot;
        loop {
            let m = *self
                .memo
                .get(first.saturating_add(cur as usize))
                .ctx("slot")?;
            if m != NONE {
                known = m;
                break;
            }
            self.chain.push(cur);
            let rec = slab.rec(cur).ctx("record")?;
            if rec.base == NO_BASE {
                break;
            }
            if self.chain.len() > 100_000 {
                crate::bail!("delta chain too long");
            }
            cur = rec.base;
        }
        while let Some(s) = self.chain.pop() {
            let rec = slab.rec(s).ctx("record")?;
            let entry = slab.entry(&rec).ctx("entry")?;
            let h = parse_entry_header(entry).ctx("entry header")?;
            let zdata = entry.get(h.header_len..).ctx("entry")?;
            let at = self.raw.len();
            let len = if rec.base == NO_BASE {
                let n = us(h.size);
                self.raw.resize(at.saturating_add(n), 0);
                self.inflater
                    .inflate_exact(zdata, self.raw.get_mut(at..).ctx("content")?)?;
                n
            } else {
                let dn = us(h.size);
                if self.delta.len() < dn {
                    self.delta.resize(dn, 0);
                }
                let delta = self.delta.get_mut(..dn).ctx("delta")?;
                self.inflater.inflate_exact(zdata, delta)?;
                let (_, result, _) = delta_sizes(delta).ctx("bad delta")?;
                let n = us(result);
                let &(bat, blen) = self.contents.get(known as usize).ctx("delta base")?;
                self.raw.resize(at.saturating_add(n), 0);
                let (done, tail) = self.raw.split_at_mut_checked(at).ctx("content")?;
                apply_delta(
                    done.get(bat..bat.saturating_add(blen)).ctx("delta base")?,
                    self.delta.get(..dn).ctx("delta")?,
                    tail,
                )?;
                n
            };
            self.contents.push((at, len));
            known = u32_of(self.contents.len().saturating_sub(1));
            if let Some(m) = self.memo.get_mut(first.saturating_add(s as usize)) {
                *m = known;
            }
        }
        Ok(known)
    }

    /// Step 2: the directory path of every new tree.
    fn walk(&mut self, input: &Input<'_>) -> Result<()> {
        self.ids.reset(self.objs.len());
        for (i, o) in self.objs.iter().enumerate() {
            let rec = input
                .slabs
                .get(usize::from(o.slab))
                .and_then(|s| s.rec(o.slot))
                .ctx("record")?;
            self.ids.insert(&rec.oid, u32_of(i));
        }
        let mut commits = std::mem::take(&mut self.commits);
        commits.clear();
        for i in 0..u32_of(self.objs.len()) {
            if self
                .objs
                .get(i as usize)
                .is_some_and(|o| o.kind == OBJ_COMMIT)
            {
                let ci = self.resolve(input, i)?;
                commits.push((commit_time(self.content(ci)).unwrap_or(0), i));
            }
        }
        commits.sort_unstable_by(newest_first);
        self.trees.clear();
        for &(time, c) in &commits {
            let ci = self.resolve(input, c)?;
            let Some(root) = commit_tree(self.content(ci)).and_then(|r| self.ids.get(&r)) else {
                continue;
            };
            self.stack.clear();
            self.stack.push((root, ROOT_PATH));
            while let Some((t, path)) = self.stack.pop() {
                let Some(o) = self.objs.get(t as usize).copied() else {
                    continue;
                };
                if o.kind != OBJ_TREE || o.tree != NONE {
                    continue;
                }
                let rank = u32_of(self.trees.len());
                self.trees.push(TreeFacts {
                    obj: t,
                    path,
                    time,
                    rank,
                });
                if let Some(ob) = self.objs.get_mut(t as usize) {
                    ob.tree = rank;
                }
                let ti = self.resolve(input, t)?;
                let &(at, len) = self.contents.get(ti as usize).ctx("tree")?;
                let mark = self.stack.len();
                for e in TreeIter::new(self.raw.get(at..at.saturating_add(len)).ctx("tree")?) {
                    if let Some(child) = e.is_tree().then(|| self.ids.get(&e.oid)).flatten() {
                        self.stack.push((child, path_hash(path, e.name)));
                    }
                }
                // Depth first in tree order.
                if let Some(pushed) = self.stack.get_mut(mark..) {
                    pushed.reverse();
                }
            }
        }
        self.commits = commits;
        // Trees no new commit reaches (an upstream may send extras) stand alone.
        for i in 0..u32_of(self.objs.len()) {
            if self
                .objs
                .get(i as usize)
                .is_some_and(|o| o.kind == OBJ_TREE && o.tree == NONE)
            {
                let rank = u32_of(self.trees.len());
                self.trees.push(TreeFacts {
                    obj: i,
                    path: path_hash(UNREACHED, &i.to_le_bytes()),
                    time: i64::MIN,
                    rank,
                });
                if let Some(ob) = self.objs.get_mut(i as usize) {
                    ob.tree = rank;
                }
            }
        }
        Ok(())
    }

    /// Step 3a: group the versions of each directory, newest first.
    fn cluster(&mut self, input: &Input<'_>) -> Result<()> {
        self.members.clear();
        self.members.extend(0..u32_of(self.trees.len()));
        let trees = &self.trees;
        let key = |i: u32| {
            trees
                .get(i as usize)
                .map(|t| (t.path, std::cmp::Reverse(t.time), t.rank))
        };
        self.members.sort_unstable_by_key(|&i| key(i));
        self.clusters.clear();
        let mut start = 0usize;
        while let Some(head) = self
            .members
            .get(start)
            .and_then(|&m| self.trees.get(m as usize))
        {
            let mut end = start.saturating_add(1);
            let mut rank = head.rank;
            while let Some(t) = self
                .members
                .get(end)
                .and_then(|&m| self.trees.get(m as usize))
                .filter(|t| t.path == head.path)
            {
                rank = rank.min(t.rank);
                end = end.saturating_add(1);
            }
            self.clusters.push(Cluster {
                start: u32_of(start),
                end: u32_of(end),
                rank,
                path: head.path,
                frozen: None,
            });
            start = end;
        }
        if input.frozen.is_some() {
            self.frozen_versions(input)?;
        }
        self.clusters.sort_unstable_by_key(|c| (c.rank, c.path));
        Ok(())
    }

    /// Step 3b (appending to `frozen`): each directory's version in the frozen slab, read
    /// from the frozen commits the new history builds on, along changed paths only.
    fn frozen_versions(&mut self, input: &Input<'_>) -> Result<()> {
        let Some(frozen) = input.frozen else {
            return Ok(());
        };
        self.paths.clear();
        self.paths.extend(
            self.clusters
                .iter()
                .enumerate()
                .map(|(i, c)| (c.path, u32_of(i))),
        );
        self.paths.sort_unstable();
        self.boundary.clear();
        let commits = std::mem::take(&mut self.commits);
        for &(time, c) in &commits {
            let ci = self.resolve(input, c)?;
            let &(at, len) = self.contents.get(ci as usize).ctx("commit")?;
            for p in commit_parents(self.raw.get(at..at.saturating_add(len)).unwrap_or(&[])) {
                if self.ids.get(&p).is_none()
                    && let Some(fs) = frozen.find(&p)
                {
                    self.boundary.push((time, fs));
                }
            }
        }
        self.commits = commits;
        // The newest new commit's parent first: its trees are the closest versions.
        self.boundary.sort_unstable_by(newest_first);
        for bi in 0..self.boundary.len() {
            let Some(&(_, fs)) = self.boundary.get(bi) else {
                break;
            };
            if self
                .boundary
                .get(..bi)
                .is_some_and(|done| done.iter().any(|d| d.1 == fs))
            {
                continue;
            }
            let (kind, commit) = self.resolver.resolve(frozen, fs)?;
            let Some(root) = commit_tree(commit)
                .filter(|_| kind == OBJ_COMMIT)
                .and_then(|r| frozen.find(&r))
            else {
                continue;
            };
            self.stack.clear();
            self.stack.push((root, ROOT_PATH));
            while let Some((slot, path)) = self.stack.pop() {
                let Ok(pi) = self.paths.binary_search_by_key(&path, |p| p.0) else {
                    continue;
                };
                let Some(&(_, c)) = self.paths.get(pi) else {
                    continue;
                };
                if self
                    .clusters
                    .get(c as usize)
                    .is_none_or(|cl| cl.frozen.is_some())
                {
                    continue;
                }
                let (kind, tree) = self.resolver.resolve(frozen, slot)?;
                if kind != OBJ_TREE {
                    continue;
                }
                let at = self.raw.len();
                self.raw.extend_from_slice(tree);
                self.contents.push((at, tree.len()));
                let ci = u32_of(self.contents.len().saturating_sub(1));
                if let Some(cl) = self.clusters.get_mut(c as usize) {
                    cl.frozen = Some((ci, slot));
                }
                for e in TreeIter::new(self.raw.get(at..).unwrap_or(&[])) {
                    if !e.is_tree() {
                        continue;
                    }
                    let child = path_hash(path, e.name);
                    if self.paths.binary_search_by_key(&child, |p| p.0).is_ok()
                        && let Some(cs) = frozen.find(&e.oid)
                    {
                        self.stack.push((cs, child));
                    }
                }
            }
        }
        Ok(())
    }

    fn write(&mut self, input: &Input<'_>, sink: &mut Sink, entries: u32) -> Result<()> {
        sink.put(&pack_header(entries))?;
        if let Some(f) = input.frozen {
            let (a, b) = f.body();
            let body =
                crate::util::sub(f.pack(), us(a), us(b.saturating_sub(a))).ctx("frozen body")?;
            for chunk in body.chunks(8 << 20) {
                sink.put(chunk)?;
            }
        }
        let n = u32_of(self.objs.len());
        match input.mode {
            Mode::Copy => {
                for i in 0..n {
                    self.emit_copied(input, sink, i)?;
                }
            }
            Mode::Trees => {
                for i in 0..n {
                    if self
                        .objs
                        .get(i as usize)
                        .is_some_and(|o| o.kind != OBJ_TREE && o.kind != OBJ_BLOB)
                    {
                        self.emit_copied(input, sink, i)?;
                    }
                }
                self.write_clusters(input, sink)?;
                for i in 0..n {
                    self.emit_copied(input, sink, i)?;
                }
            }
        }
        Ok(())
    }

    /// Step 3c: every cluster, newest version first, older ones as chained deltas.
    fn write_clusters(&mut self, input: &Input<'_>, sink: &mut Sink) -> Result<()> {
        for c in 0..self.clusters.len() {
            let cl = *self.clusters.get(c).ctx("cluster")?;
            // (content, output offset, depth) of the version the next one is a delta against.
            let mut prev = match (cl.frozen, input.frozen) {
                (Some((ci, slot)), Some(f)) => f.rec(slot).map(|r| (ci, r.offset, r.depth)),
                _ => None,
            };
            for m in cl.start..cl.end {
                let t = *self.members.get(m as usize).ctx("cluster member")?;
                let obj = self.trees.get(t as usize).ctx("tree")?.obj;
                let ci = self.resolve(input, obj)?;
                let &(at, len) = self.contents.get(ci as usize).ctx("tree")?;
                let target = self.raw.get(at..at.saturating_add(len)).ctx("tree")?;
                let out_at = sink.pos;
                let mut depth = 0u8;
                let mut as_delta = false;
                if let Some((bci, bat, bdepth)) = prev.filter(|p| p.2 < MAX_DEPTH) {
                    let base = self
                        .contents
                        .get(bci as usize)
                        .and_then(|&(a, l)| self.raw.get(a..a.saturating_add(l)))
                        .ctx("delta base")?;
                    if tree_delta(base, target, len / 2, &mut self.delta) {
                        let z = deflate(&mut self.deflater, &mut self.zbuf, &self.delta)?;
                        sink.put(
                            HeaderBytes::ofs_delta(
                                u64_of(self.delta.len()),
                                out_at.saturating_sub(bat),
                            )
                            .as_slice(),
                        )?;
                        sink.put(z)?;
                        depth = bdepth.saturating_add(1);
                        as_delta = true;
                        self.stats.delta_trees = self.stats.delta_trees.saturating_add(1);
                    }
                }
                if !as_delta {
                    let z = deflate(&mut self.deflater, &mut self.zbuf, target)?;
                    sink.put(HeaderBytes::whole(OBJ_TREE, u64_of(len)).as_slice())?;
                    sink.put(z)?;
                    self.stats.whole_trees = self.stats.whole_trees.saturating_add(1);
                }
                if let Some(o) = self.objs.get_mut(obj as usize) {
                    o.out_at = out_at;
                    o.depth = depth;
                }
                prev = Some((ci, out_at, depth));
            }
        }
        Ok(())
    }

    /// Writes object `obj` with its own compressed bytes, after those of its delta bases
    /// that are not written yet.
    fn emit_copied(&mut self, input: &Input<'_>, sink: &mut Sink, obj: u32) -> Result<()> {
        let retree = input.mode == Mode::Trees;
        self.chain.clear();
        let mut cur = obj;
        loop {
            let o = *self.objs.get(cur as usize).ctx("object")?;
            if o.out_at != NOT_WRITTEN {
                break;
            }
            self.chain.push(cur);
            if self.chain.len() > 100_000 {
                crate::bail!("delta chain too long");
            }
            let rec = input
                .slabs
                .get(usize::from(o.slab))
                .and_then(|s| s.rec(o.slot))
                .ctx("record")?;
            if rec.base == NO_BASE || stored_whole(&rec) {
                break;
            }
            let m = self.base_of(&o, rec.base);
            if m == NONE
                || m & FROZEN != 0
                || (retree
                    && self
                        .objs
                        .get(m as usize)
                        .is_some_and(|b| b.kind == OBJ_TREE))
            {
                break;
            }
            cur = m;
        }
        while let Some(o) = self.chain.pop() {
            self.write_copied(input, sink, o)?;
        }
        Ok(())
    }

    /// The map value of the delta base (slot `base` of the same input slab) of `o`.
    fn base_of(&self, o: &Obj, base: u32) -> u32 {
        self.first
            .get(usize::from(o.slab))
            .and_then(|&f| self.map.get(f.saturating_add(base as usize)))
            .copied()
            .unwrap_or(NONE)
    }

    fn write_copied(&mut self, input: &Input<'_>, sink: &mut Sink, obj: u32) -> Result<()> {
        let o = *self.objs.get(obj as usize).ctx("object")?;
        let slab = input.slabs.get(usize::from(o.slab)).ctx("slab")?;
        let rec = slab.rec(o.slot).ctx("record")?;
        let entry = slab.entry(&rec).ctx("entry")?;
        let out_at = sink.pos;
        let mut depth = 0u8;
        let base = if rec.base == NO_BASE {
            None
        } else {
            let m = self.base_of(&o, rec.base);
            if m == NONE {
                None
            } else if m & FROZEN != 0 {
                input
                    .frozen
                    .and_then(|f| f.rec(m & !FROZEN))
                    .map(|r| (r.offset, r.depth))
            } else {
                self.objs
                    .get(m as usize)
                    .filter(|b| b.out_at != NOT_WRITTEN)
                    .map(|b| (b.out_at, b.depth))
            }
        };
        match base {
            _ if rec.base == NO_BASE => {
                sink.put(entry)?;
                self.stats.copied = self.stats.copied.saturating_add(1);
            }
            Some((at, d)) if at < out_at && d < MAX_DEPTH && !stored_whole(&rec) => {
                let h = parse_entry_header(entry).ctx("entry header")?;
                sink.put(HeaderBytes::ofs_delta(h.size, out_at.saturating_sub(at)).as_slice())?;
                sink.put(entry.get(h.header_len..).ctx("entry")?)?;
                depth = d.saturating_add(1);
                self.stats.copied = self.stats.copied.saturating_add(1);
            }
            _ => {
                // The base is not in the output (or its chain is as deep as allowed):
                // store the object whole.
                let (kind, content) = self.resolver.resolve(slab, o.slot)?;
                let z = deflate(&mut self.deflater, &mut self.zbuf, content)?;
                sink.put(HeaderBytes::whole(kind, u64_of(content.len())).as_slice())?;
                sink.put(z)?;
                self.stats.rewritten = self.stats.rewritten.saturating_add(1);
            }
        }
        if let Some(ob) = self.objs.get_mut(obj as usize) {
            ob.out_at = out_at;
            ob.depth = depth;
        }
        Ok(())
    }

    /// Step 5: the indexer re-derived every id from the written bytes; the output must
    /// hold exactly the frozen objects plus the new ones.
    fn verify(&self, input: &Input<'_>, out: &Slab) -> Result<()> {
        let expected = input
            .frozen
            .map_or(0, |f| f.count())
            .saturating_add(u32_of(self.objs.len()));
        if out.count() != expected {
            crate::bail!("repack wrote {} objects, expected {expected}", out.count());
        }
        for o in &self.objs {
            let rec = input
                .slabs
                .get(usize::from(o.slab))
                .and_then(|s| s.rec(o.slot))
                .ctx("record")?;
            if out.find(&rec.oid).is_none() {
                crate::bail!("repack lost object {}", rec.oid);
            }
        }
        Ok(())
    }
}

/// `gvfs-daemon repack [--frozen <FILE.mphf>] [--mode trees|copy] --out <DIR> <FILE.mphf>...`:
/// the optimal splice by hand (inspection, measurements).
pub fn main(args: &[String]) -> Result<()> {
    let f = crate::cli::Flags::parse(args, &["out", "frozen", "mode", "tier", "threads"])?;
    let out = std::path::PathBuf::from(f.require("out")?);
    let mode = match f.get("mode").unwrap_or("trees") {
        "trees" => Mode::Trees,
        "copy" => Mode::Copy,
        other => crate::bail!("--mode must be trees or copy, not {other:?}"),
    };
    let tier = Tier::parse(f.get("tier").unwrap_or("1h"))
        .ctx("--tier must be 15m, 1h, 6h, 12h, 24h, 168h or base")?;
    let slabs = f
        .positional
        .iter()
        .map(|p| Slab::open_file(std::path::Path::new(p)))
        .collect::<Result<Vec<_>>>()?;
    if slabs.is_empty() {
        crate::bail!("repack needs at least one <FILE.mphf>");
    }
    let frozen = f
        .get("frozen")
        .map(|p| Slab::open_file(std::path::Path::new(p)))
        .transpose()?;
    let mut indexer = Indexer::new(f.num("threads", crate::util::default_threads())?);
    let input = Input {
        slabs: &slabs,
        frozen: frozen.as_ref(),
        exclude: &[],
        mode,
    };
    let now = crate::util::now_secs();
    match Repacker::new().repack(
        &mut indexer,
        &input,
        tier,
        (0, now),
        (Oid::ZERO, Oid::ZERO),
        Dest::Dir(&out),
    )? {
        Some(s) => crate::log!(
            "wrote {} ({} objects, {})",
            out.join(format!("{}.pack", s.meta.id)).display(),
            s.meta.objects,
            crate::util::human_bytes(s.meta.pack_len)
        ),
        None => crate::log!("nothing new: every object is already in the frozen slab"),
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use crate::ingest::memory_slab;
    use crate::pack::{OBJ_OFS_DELTA, OBJ_REF_DELTA};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};

    fn git(dir: &Path, args: &[&str], stdin: Option<&[u8]>) -> Vec<u8> {
        let mut cmd = Command::new("git");
        cmd.args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        crate::upstream::hermetic_git_env(&mut cmd);
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.unwrap_or(b""))
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    }

    /// A repository with a directory hierarchy and `n` commits, each editing one to three
    /// `Git-hfs` pointer files. Returns the commit ids, oldest first.
    fn history(dir: &Path, n: usize) -> Vec<String> {
        std::fs::create_dir_all(dir).unwrap();
        git(dir, &["init", "-q", "-b", "main"], None);
        let dirs: Vec<String> = (0..24)
            .map(|i| format!("src/m{}/p{}/q{}", i % 3, i % 6, i))
            .collect();
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut rand = move |m: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % m
        };
        let mut ids = Vec::new();
        for c in 0..n {
            let edits = if c == 0 { 200 } else { 1 + rand(3) as usize };
            for e in 0..edits {
                let d = &dirs[if c == 0 {
                    e % dirs.len()
                } else {
                    rand(dirs.len() as u64) as usize
                }];
                std::fs::create_dir_all(dir.join(d)).unwrap();
                let f = dir.join(d).join(format!(
                    "f{}.bin",
                    if c == 0 { e } else { rand(200) as usize }
                ));
                std::fs::write(
                    f,
                    format!(
                        "~~Git-hfs 1.0\nblob:{:064X}\nsize:{}\n",
                        rand(u64::MAX),
                        rand(1 << 30)
                    ),
                )
                .unwrap();
            }
            git(dir, &["add", "-A"], None);
            let date = format!("{} +0000", 1_700_000_000 + c * 60);
            let mut cmd = Command::new("git");
            cmd.args([
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "-m",
                &format!("c{c}"),
            ])
            .current_dir(dir)
            .env("GIT_COMMITTER_DATE", &date)
            .env("GIT_AUTHOR_DATE", &date);
            crate::upstream::hermetic_git_env(&mut cmd);
            assert!(cmd.status().unwrap().success());
            ids.push(
                String::from_utf8(git(dir, &["rev-parse", "HEAD"], None))
                    .unwrap()
                    .trim()
                    .to_string(),
            );
        }
        ids
    }

    /// What `git upload-pack` would send for `have..want` (non-thin; OFS or REF deltas).
    fn fetch(dir: &Path, have: Option<&str>, want: &str, ofs: bool) -> Vec<u8> {
        let revs = match have {
            Some(h) => format!("{want}\n^{h}\n"),
            None => format!("{want}\n"),
        };
        let mut args = vec!["pack-objects", "--revs", "--stdout", "-q"];
        if ofs {
            args.push("--delta-base-offset");
        }
        git(dir, &args, Some(revs.as_bytes()))
    }

    fn slab(ix: &mut Indexer, pack: Vec<u8>) -> Arc<Slab> {
        memory_slab(ix, pack, Tier::M15, (0, 1), (Oid::ZERO, Oid::ZERO))
            .unwrap()
            .slab
    }

    fn ids(s: &Slab) -> Vec<Oid> {
        let mut v: Vec<Oid> = s.records().map(|r| r.oid).collect();
        v.sort();
        v
    }

    /// git accepts the pack (every object hashed, every delta resolved).
    fn git_accepts(dir: &Path, pack: &[u8]) {
        let p = dir.join("check.pack");
        std::fs::write(&p, pack).unwrap();
        git(
            dir,
            &[
                "index-pack",
                "--strict",
                "-o",
                dir.join("check.idx").to_str().unwrap(),
                p.to_str().unwrap(),
            ],
            None,
        );
    }

    fn repack_mem(ix: &mut Indexer, rp: &mut Repacker, input: &Input<'_>) -> Arc<Slab> {
        rp.repack(
            ix,
            input,
            Tier::H1,
            (0, 2),
            (Oid::ZERO, Oid::ZERO),
            Dest::Memory,
        )
        .unwrap()
        .unwrap()
        .slab
    }

    fn entry_kinds(s: &Slab) -> Vec<u8> {
        s.records()
            .map(|r| parse_entry_header(s.entry(&r).unwrap()).unwrap().kind)
            .collect()
    }

    fn tempdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("gvd-repack-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn rollup_is_as_small_as_one_fetch() {
        let dir = tempdir("rollup");
        let c = history(&dir, 41);
        let mut ix = Indexer::new(2);
        let mut rp = Repacker::new();
        // Ten 4-commit slabs, as the 15m tier would fetch them, against one fetch of all.
        let slabs: Vec<Arc<Slab>> = (0..10)
            .map(|i| slab(&mut ix, fetch(&dir, Some(&c[i * 4]), &c[i * 4 + 4], true)))
            .collect();
        let single = slab(&mut ix, fetch(&dir, Some(&c[0]), &c[40], true));
        let out = repack_mem(
            &mut ix,
            &mut rp,
            &Input {
                slabs: &slabs,
                frozen: None,
                exclude: &[],
                mode: Mode::Trees,
            },
        );
        assert_eq!(ids(&out), ids(&single));
        git_accepts(&dir, out.pack());
        let concat: usize = slabs.iter().map(|s| s.pack().len()).sum();
        let (got, one) = (out.pack().len(), single.pack().len());
        eprintln!("concat {concat} B, repacked {got} B, one fetch {one} B");
        assert!(got * 4 < concat * 3, "repack {got} vs concat {concat}");
        assert!(got * 10 < one * 11, "repack {got} vs one fetch {one}");
        // Every directory as of the newest commit is stored whole; older versions are deltas.
        let listing = String::from_utf8(git(&dir, &["ls-tree", "-r", "-t", &c[40]], None)).unwrap();
        let mut newest: Vec<Oid> = listing
            .lines()
            .filter(|l| l.contains(" tree "))
            .map(|l| Oid::from_hex(l.split_whitespace().nth(2).unwrap().as_bytes()).unwrap())
            .collect();
        newest.push(
            Oid::from_hex(
                String::from_utf8(git(
                    &dir,
                    &["rev-parse", &format!("{}^{{tree}}", c[40])],
                    None,
                ))
                .unwrap()
                .trim()
                .as_bytes(),
            )
            .unwrap(),
        );
        let mut seen = 0;
        for t in &newest {
            if let Some(slot) = out.find(t) {
                assert_eq!(
                    out.rec(slot).unwrap().depth,
                    0,
                    "newest tree {t} is a delta"
                );
                seen += 1;
            }
        }
        assert!(seen > 3);
        assert!(
            out.records().any(|r| r.kind == OBJ_TREE && r.depth >= 2),
            "expected chained tree deltas"
        );
        // Repacking a repacked slab changes nothing but stays valid; one slab works too.
        let again = repack_mem(
            &mut ix,
            &mut rp,
            &Input {
                slabs: std::slice::from_ref(&out),
                frozen: None,
                exclude: &[],
                mode: Mode::Trees,
            },
        );
        assert_eq!(again.pack(), out.pack());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_to_frozen_baseline() {
        let dir = tempdir("frozen");
        let c = history(&dir, 31);
        let mut ix = Indexer::new(2);
        let mut rp = Repacker::new();
        let base_pack = slab(&mut ix, fetch(&dir, None, &c[20], false));
        // The seed: REF deltas re-pointed as OFS, nothing else changes.
        let base = repack_mem(
            &mut ix,
            &mut rp,
            &Input {
                slabs: std::slice::from_ref(&base_pack),
                frozen: None,
                exclude: &[],
                mode: Mode::Copy,
            },
        );
        assert_eq!(ids(&base), ids(&base_pack));
        assert!(entry_kinds(&base_pack).contains(&OBJ_REF_DELTA));
        assert!(
            !entry_kinds(&base).contains(&OBJ_REF_DELTA)
                && entry_kinds(&base).contains(&OBJ_OFS_DELTA)
        );
        assert!(base.pack().len() < base_pack.pack().len());
        git_accepts(&dir, base.pack());

        let week: Vec<Arc<Slab>> = (0..5)
            .map(|i| {
                slab(
                    &mut ix,
                    fetch(&dir, Some(&c[20 + i * 2]), &c[22 + i * 2], true),
                )
            })
            .collect();
        let week = repack_mem(
            &mut ix,
            &mut rp,
            &Input {
                slabs: &week,
                frozen: None,
                exclude: &[],
                mode: Mode::Trees,
            },
        );
        let next = repack_mem(
            &mut ix,
            &mut rp,
            &Input {
                slabs: std::slice::from_ref(&week),
                frozen: Some(&base),
                exclude: &[],
                mode: Mode::Trees,
            },
        );
        let full = slab(&mut ix, fetch(&dir, None, &c[30], true));
        assert_eq!(ids(&next), ids(&full));
        git_accepts(&dir, next.pack());
        // The old baseline is the new one's prefix, byte for byte.
        let (a, b) = base.body();
        assert_eq!(
            &next.pack()[a as usize..b as usize],
            &base.pack()[a as usize..b as usize]
        );
        // The week's newest directories became deltas against the baseline's versions.
        let grew = next.pack().len() - base.pack().len();
        eprintln!(
            "baseline {} B + week {} B -> {} B (grew {grew} B)",
            base.pack().len(),
            week.pack().len(),
            next.pack().len()
        );
        assert!(
            grew * 10 < week.pack().len() * 8,
            "grew {grew} vs week {}",
            week.pack().len()
        );
        // Nothing new: nothing to write.
        assert!(
            rp.repack(
                &mut ix,
                &Input {
                    slabs: std::slice::from_ref(&base),
                    frozen: Some(&next),
                    exclude: &[],
                    mode: Mode::Trees
                },
                Tier::Base,
                (0, 1),
                (Oid::ZERO, Oid::ZERO),
                Dest::Memory
            )
            .unwrap()
            .is_none()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn exclude_drops_objects_and_rebases_the_rest() {
        let dir = tempdir("exclude");
        let c = history(&dir, 21);
        let mut ix = Indexer::new(2);
        let mut rp = Repacker::new();
        // REF deltas inside: some objects lose their base to the exclusion.
        let all = slab(&mut ix, fetch(&dir, Some(&c[0]), &c[20], false));
        let sealed = slab(&mut ix, fetch(&dir, Some(&c[0]), &c[10], true));
        let rest = repack_mem(
            &mut ix,
            &mut rp,
            &Input {
                slabs: std::slice::from_ref(&all),
                frozen: None,
                exclude: std::slice::from_ref(&sealed),
                mode: Mode::Trees,
            },
        );
        let want = slab(&mut ix, fetch(&dir, Some(&c[10]), &c[20], true));
        assert_eq!(ids(&rest), ids(&want));
        git_accepts(&dir, rest.pack());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Deltas copied as they are (a seed in `Mode::Copy`, whose upstream packed a file's
    /// versions in a chain deeper than git's default) stop at `MAX_DEPTH`: past it, an
    /// object is stored whole and a new chain starts from it.
    #[test]
    fn copied_delta_chains_stop_at_max_depth() {
        let dir = tempdir("depth");
        std::fs::create_dir_all(&dir).unwrap();
        git(&dir, &["init", "-q", "-b", "main"], None);
        // Each version edits one more line: its nearest version is the best base.
        let mut lines: Vec<String> = (0..300)
            .map(|i| format!("line {i} of a file edited a line at a time"))
            .collect();
        for c in 0..70 {
            lines[c] = format!("line {c} edited in commit {c}");
            std::fs::write(dir.join("f.txt"), lines.join("\n")).unwrap();
            git(&dir, &["add", "-A"], None);
            let msg = format!("c{c}");
            let who = ["-c", "user.name=t", "-c", "user.email=t@t"];
            git(
                &dir,
                &[&who[..], &["commit", "-q", "-m", &msg]].concat(),
                None,
            );
        }
        let args = [
            "pack-objects",
            "--revs",
            "--stdout",
            "-q",
            "--delta-base-offset",
        ];
        let deep = ["--depth=100", "--window=100"];
        let pack = git(&dir, &[&args[..], &deep].concat(), Some(b"main\n"));
        let mut ix = Indexer::new(2);
        let seed = slab(&mut ix, pack);
        let deepest = seed.records().map(|r| r.depth).max().unwrap();
        assert!(
            deepest > MAX_DEPTH,
            "the upstream's chains run {deepest} deep"
        );
        let mut rp = Repacker::new();
        let input = Input {
            slabs: std::slice::from_ref(&seed),
            frozen: None,
            exclude: &[],
            mode: Mode::Copy,
        };
        let out = repack_mem(&mut ix, &mut rp, &input);
        assert_eq!(ids(&out), ids(&seed));
        let depth = out.records().map(|r| r.depth).max().unwrap();
        assert_eq!(depth, MAX_DEPTH, "chains as deep as allowed, no deeper");
        git_accepts(&dir, out.pack());
        let mut r = Resolver::with_capacity(4096);
        for rec in out.records() {
            let want = r
                .resolve(&seed, seed.find(&rec.oid).unwrap())
                .unwrap()
                .1
                .to_vec();
            assert!(r.resolve(&out, out.find(&rec.oid).unwrap()).unwrap().1 == want);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What an entry declares is checked before a buffer is sized for it: a header that
    /// claims more than its zlib stream can hold, and a delta that disagrees with the
    /// index's size, are errors (they were allocations of whatever they said).
    #[test]
    fn the_resolver_refuses_corrupt_sizes() {
        let dir = tempdir("corrupt");
        std::fs::create_dir_all(&dir).unwrap();
        git(&dir, &["init", "-q", "-b", "main"], None);
        let text: String = (0..200).map(|i| format!("line {i} of a text\n")).collect();
        let edited = text.replace("line 7 of", "line 7 (edited) of");
        let mut blobs = Vec::new();
        for t in [&text, &edited] {
            let id = git(&dir, &["hash-object", "-w", "--stdin"], Some(t.as_bytes()));
            blobs.push(String::from_utf8(id).unwrap().trim().to_string());
        }
        let list = format!("{}\n{}\n", blobs[0], blobs[1]);
        let args = ["pack-objects", "--stdout", "-q", "--delta-base-offset"];
        let pack = git(&dir, &args, Some(list.as_bytes()));
        let mut ix = Indexer::new(1);
        let s = slab(&mut ix, pack);
        let delta = s.records().position(|r| r.is_delta()).expect("a delta") as u32;
        let whole = 1 - delta;
        let mut r = Resolver::with_capacity(4096);
        assert!(r.resolve(&s, whole).is_ok() && r.resolve(&s, delta).is_ok());
        let le = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
        let records_at = le(s.index_bytes(), 80) as usize;
        let rec_at = |slot: u32| records_at + slot as usize * crate::slab::REC_LEN;

        // The whole object's header says 2^56 bytes: same stream, longer header.
        let w = s.rec(whole).unwrap();
        let entry = s.entry(&w).unwrap();
        let h = parse_entry_header(entry).unwrap();
        let huge = HeaderBytes::whole(OBJ_BLOB, 1 << 56);
        let grow = huge.as_slice().len() - h.header_len;
        let mut pack = s.pack()[..w.offset as usize].to_vec();
        pack.extend_from_slice(huge.as_slice());
        pack.extend_from_slice(&entry[h.header_len..]);
        let rest = (w.offset + u64::from(w.entry_len)) as usize;
        pack.extend_from_slice(&s.pack()[rest..]);
        let mut index = s.index_bytes().to_vec();
        index[32..40].copy_from_slice(&(pack.len() as u64).to_le_bytes());
        index[40..60].copy_from_slice(&pack[pack.len() - 20..]);
        for slot in 0..s.count() {
            let rec = s.rec(slot).unwrap();
            let at = rec_at(slot);
            if slot == whole {
                let len = rec.entry_len + grow as u32;
                index[at + 28..at + 32].copy_from_slice(&len.to_le_bytes());
            } else if rec.offset > w.offset {
                // Its offset (the low 40 bits of the record's second word) moves too.
                let word = le(&index, at + 20) + grow as u64;
                index[at + 20..at + 28].copy_from_slice(&word.to_le_bytes());
            }
        }
        let bad = Slab::from_heap(s.meta.clone(), pack, index).unwrap();
        let e = r.resolve(&bad, whole).expect_err("refused");
        assert!(e.to_string().contains("declares"), "{e}");

        // The delta's record says one byte more than the delta makes.
        let mut index = s.index_bytes().to_vec();
        let at = rec_at(delta) + 32;
        let size = u32::from_le_bytes(index[at..at + 4].try_into().unwrap()) + 1;
        index[at..at + 4].copy_from_slice(&size.to_le_bytes());
        let bad = Slab::from_heap(s.meta.clone(), s.pack().to_vec(), index).unwrap();
        let e = r.resolve(&bad, delta).expect_err("refused");
        assert!(e.to_string().contains("the index says"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
