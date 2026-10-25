//! Stores that keep blobs in their own packs (`--blobs separate`): what the pipeline,
//! `mirror` and the edge share.
//!
//! **The consistency rule.** Once `blobs.bin` is complete, every blob of every tree in the
//! store is in a blob pack it lists (a manifest's trees' at the `blobs.bin` generation the
//! manifest records, `blobs_generation`). Independently, every blob any tree references has
//! its size in the sizes table of the slab whose trees introduced it, so sizes are complete
//! for every tree a client can see, before and after the mirror.
//!
//! `gvfs-daemon mirror` makes it true for the history up to genesis (every blob of every
//! tree in the slabs). A pipeline run keeps it by working blobs first: it copies the blobs
//! that the trees in its window's pack reference and the store lacks, publishes them in
//! `blobs.bin`, and only then publishes the trees. That is enough because an upstream
//! leaves out of a pack only objects its haves reach, and the haves are the store's own
//! earlier tips: a tree it leaves out is in the store, and so are its blobs (by induction
//! from the mirrored genesis). Whatever the upstream (plain git, bitmaps), a reused old
//! version (a revert, an old branch merged) is either in the pack's trees or in the store.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::blobcopy::{Copied, copy_item, mode_kind, plan, publish};
use crate::blobs::{BlobManifest, BlobRec, Segment};
use crate::manifest::Manifest;
use crate::object::{TreeIter, commit_tree};
use crate::pack::{OBJ_BLOB, OBJ_COMMIT, OBJ_TREE};
use crate::sizes::Sizes;
use crate::slab::{Slab, SlabMeta};
use crate::splice::Resolver;
use crate::store::Store;
use crate::upstream::Upstream;
use crate::util::{Context, Oid, Result, u32_of, u64_of, us};

/// Default bytes of small blobs per blob pack (`--blob-pack-mib`).
pub const ITEM_BYTES: u64 = 2 << 30;

/// Opens an object of the store in place (directory stores) or from a download to
/// `work` (kept: these objects are immutable, so a later run may reuse it).
fn local_copy(store: &Store, work: &Path, key: &str) -> Result<PathBuf> {
    if let Some(p) = store.local_path(key) {
        return Ok(p);
    }
    let name = key.replace('/', "_");
    let p = work.join(name);
    if !p.exists() {
        store.download_file(key, &p)?;
    }
    Ok(p)
}

/// Sizes known from a set of tables.
pub struct KnownSizes {
    tables: Vec<Sizes>,
}

impl KnownSizes {
    pub fn get(&self, oid: &Oid) -> Option<u64> {
        self.tables.iter().find_map(|t| t.get(oid))
    }

    pub fn tables(&self) -> &[Sizes] {
        &self.tables
    }
}

/// The sizes tables of `metas`.
pub fn load_sizes(store: &Store, work: &Path, metas: &[&SlabMeta]) -> Result<KnownSizes> {
    let mut tables = Vec::with_capacity(metas.len());
    for m in metas {
        let path = local_copy(store, work, &m.sizes_key())?;
        tables.push(Sizes::open(&path).ctx(&format!("sizes of slab {}", m.id))?);
    }
    Ok(KnownSizes { tables })
}

/// Every size a manifest's trees need: the baseline's table and the pyramid's (a
/// rollup's table is the union of its parts', a new baseline's the old one's plus the
/// week's, so these cover every slab ever published).
pub fn known_sizes(store: &Store, work: &Path, m: &Manifest) -> Result<KnownSizes> {
    let metas: Vec<&SlabMeta> = std::iter::once(&m.baseline).chain(&m.pyramid).collect();
    load_sizes(store, work, &metas)
}

/// Uploads `table` as slab `meta`'s sizes table.
pub fn put_sizes(store: &Store, meta: &SlabMeta, table: &[u8]) -> Result<()> {
    let mut w = store.writer(&meta.sizes_key())?;
    w.write_all(table).ctx("uploading a sizes table")?;
    w.finish()
}

/// Uploads the union of `parts`' sizes tables as `out`'s.
pub fn union_sizes(store: &Store, work: &Path, parts: &[&SlabMeta], out: &SlabMeta) -> Result<()> {
    let known = load_sizes(store, work, parts)?;
    let tables: Vec<&Sizes> = known.tables.iter().collect();
    put_sizes(store, out, &crate::sizes::merge(&tables))
}

/// Lookups across the segments `blobs.bin` lists.
pub struct BlobIndex {
    segments: Vec<Segment>,
}

impl BlobIndex {
    /// The segments `bm` lists. One the store no longer holds is left out (its blobs count
    /// as not copied, so a pipeline run copies again those its window needs), with a
    /// warning: `gvfs-daemon mirror` repairs `blobs.bin`.
    pub fn load(store: &Store, work: &Path, bm: &BlobManifest) -> Result<BlobIndex> {
        let mut segments = Vec::with_capacity(bm.segments.len());
        for s in &bm.segments {
            match crate::blobcopy::open_segment(store, work, s) {
                Ok(seg) => segments.push(seg),
                Err(e) if e.is_not_found() => crate::log!(
                    "warning: blobs.bin generation {} lists segment {}, which the store does \
                     not hold ({e}); its blobs count as missing until gvfs-daemon mirror \
                     repairs blobs.bin",
                    bm.generation,
                    s.id
                ),
                Err(e) => return Err(e),
            }
        }
        // Biggest first: most lookups end there.
        segments.sort_by_key(|s| std::cmp::Reverse(s.count()));
        Ok(BlobIndex { segments })
    }

    /// The blob's record and the segment that has it.
    pub fn find(&self, oid: &Oid) -> Option<(&Segment, BlobRec)> {
        self.segments
            .iter()
            .find_map(|s| s.find(oid).map(|r| (s, r)))
    }
}

/// Copies the blobs `need` into new blob packs (sizes asked of the upstream first, so
/// large blobs stream alone) and publishes them in `blobs.bin` as one segment. Returns the
/// published `blobs.bin` (`None` if nothing was needed) and every copied blob's size, as
/// verified by hashing its content.
pub fn copy_needed(
    up: &mut Upstream,
    store: &Store,
    work: &Path,
    need: &[Oid],
    item_bytes: u64,
    threads: usize,
) -> Result<(Option<BlobManifest>, HashMap<Oid, u64>)> {
    let mut got = HashMap::with_capacity(need.len());
    if need.is_empty() {
        return Ok((None, got));
    }
    let reported = up.object_sizes(need)?;
    let mut wants = Vec::with_capacity(need.len());
    for (oid, size) in need.iter().zip(reported) {
        let Some(size) = size else {
            crate::bail!("the upstream has no blob {oid}, which a tree references");
        };
        wants.push((*oid, size));
    }
    let items = plan(&wants, item_bytes);
    let spool = work.join("blob-spool");
    let mut copied: Vec<Copied> = Vec::with_capacity(items.len());
    for item in &items {
        copied.push(copy_item(up, store, &spool, item)?);
    }
    for c in &copied {
        for r in &c.recs {
            got.insert(r.oid, r.size);
        }
    }
    for (oid, size) in &wants {
        match got.get(oid) {
            Some(s) if s == size => {}
            Some(s) => {
                crate::bail!("blob {oid}: the upstream reported {size} bytes, its content has {s}")
            }
            None => crate::bail!("blob {oid} was not copied"),
        }
    }
    let bytes: u64 = wants.iter().map(|w| w.1).sum();
    let bm = publish(store, work, &copied, threads, |_| Ok(()))?;
    crate::log!(
        "copied {} blobs ({}) into {} blob packs; blobs.bin generation {}",
        wants.len(),
        crate::util::human_bytes(bytes),
        copied.len(),
        bm.generation
    );
    Ok((Some(bm), got))
}

/// Keeps in a pipeline runner's store cache (`--cache-dir`: downloaded copies of immutable
/// store objects, reused across runs) only what manifest `m` and `blobs.bin` `bm` list.
pub fn prune_cache(dir: &Path, m: &Manifest, bm: &BlobManifest) -> Result<usize> {
    let mut keep: HashSet<String> = HashSet::new();
    for s in m.slabs() {
        keep.insert(format!("{}.pack", s.id));
        keep.insert(format!("{}.mphf", s.id));
        keep.insert(s.sizes_key().replace('/', "_"));
    }
    for seg in &bm.segments {
        keep.insert(format!("{}.bidx", seg.id));
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(crate::util::Error::from(e)).ctx(&format!("listing {}", dir.display()));
        }
    };
    let mut removed = 0usize;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // Dot files are downloads in progress.
        if name.starts_with('.') || keep.contains(&name) {
            continue;
        }
        if std::fs::remove_file(entry.path()).is_ok() {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

/// Every blob the trees of `slabs` reference, distinct, in the order they are best copied:
/// those of commit `first`'s tree depth first in name order (a checkout reads them back to
/// back), then the others by where the first tree that names them is stored (slab, then
/// offset: an upstream writes trees newest commit first, each commit's in path order).
/// Every tree is read once, in parallel.
pub fn history_blobs(slabs: &[Arc<Slab>], first: Option<&Oid>, threads: usize) -> Result<Vec<Oid>> {
    let mut out = match first {
        Some(c) => tree_blobs(slabs, c)?,
        None => Vec::new(),
    };
    let seen: HashSet<Oid> = out.iter().copied().collect();
    let ranked = ranked_blobs(slabs, threads)?;
    out.reserve(ranked.len().saturating_sub(seen.len()));
    out.extend(
        ranked
            .into_iter()
            .map(|(_, oid)| oid)
            .filter(|oid| !seen.contains(oid)),
    );
    Ok(out)
}

/// An object's content from the newest slab that has it.
fn object<'r>(
    slabs: &[Arc<Slab>],
    resolver: &'r mut Resolver,
    oid: &Oid,
    kind: u8,
) -> Result<&'r [u8]> {
    let Some((slab, slot)) = slabs
        .iter()
        .rev()
        .find_map(|s| s.find(oid).map(|slot| (s, slot)))
    else {
        crate::bail!("object {oid} is not in the store's slabs");
    };
    let (k, content) = resolver.resolve(slab, slot)?;
    if k != kind {
        crate::bail!("object {oid} is a {}", crate::pack::type_name(k));
    }
    Ok(content)
}

/// The blobs of commit `commit`'s tree, distinct, depth first in name order.
fn tree_blobs(slabs: &[Arc<Slab>], commit: &Oid) -> Result<Vec<Oid>> {
    let mut resolver = Resolver::default();
    let c = object(slabs, &mut resolver, commit, OBJ_COMMIT)?;
    let Some(root) = commit_tree(c) else {
        crate::bail!("commit {commit} has no tree");
    };
    let (mut seen_trees, mut seen_blobs) = (HashSet::new(), HashSet::new());
    let mut out = Vec::new();
    let mut stack = vec![root];
    let mut subtrees = Vec::new();
    while let Some(t) = stack.pop() {
        if !seen_trees.insert(t) {
            continue;
        }
        let content = object(slabs, &mut resolver, &t, OBJ_TREE)?;
        let mut it = TreeIter::new(content);
        subtrees.clear();
        let mut bad = false;
        for e in &mut it {
            match mode_kind(e.mode) {
                Some(OBJ_BLOB) => {
                    if seen_blobs.insert(e.oid) {
                        out.push(e.oid);
                    }
                }
                Some(OBJ_TREE) => subtrees.push(e.oid),
                Some(_) => {}
                None => bad = true,
            }
        }
        if bad || it.malformed {
            crate::bail!("malformed tree {t}");
        }
        stack.extend(subtrees.iter().rev());
    }
    Ok(out)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Every blob the trees of `slabs` reference, distinct, with the rank of the first tree
/// naming it (`slab index << 40 | offset`; offsets fit 40 bits), sorted by rank. Workers
/// take runs of trees in storage order and merge what they find into maps sharded by id.
fn ranked_blobs(slabs: &[Arc<Slab>], threads: usize) -> Result<Vec<(u64, Oid)>> {
    const STEP: usize = 1024;
    const SHARDS: usize = 256;
    let mut trees: Vec<(u64, u32, u32)> = Vec::new();
    for (si, slab) in slabs.iter().enumerate() {
        let base = u64_of(si).wrapping_shl(40);
        for (slot, r) in slab.records().enumerate() {
            if r.kind == OBJ_TREE {
                trees.push((base | r.offset, u32_of(si), u32_of(slot)));
            }
        }
    }
    trees.sort_unstable_by_key(|t| t.0);
    let shards: Vec<Mutex<HashMap<Oid, u64>>> =
        (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| -> Result<()> {
        let workers: Vec<_> = (0..threads.clamp(1, 64))
            .map(|_| {
                sc.spawn(|| -> Result<()> {
                    let mut resolver = Resolver::default();
                    let mut first: HashMap<Oid, u64> = HashMap::new();
                    let mut parts: Vec<Vec<(Oid, u64)>> = vec![Vec::new(); SHARDS];
                    loop {
                        let start = next.fetch_add(STEP, Ordering::Relaxed);
                        let Some(run) =
                            trees.get(start..start.saturating_add(STEP).min(trees.len()))
                        else {
                            break;
                        };
                        if run.is_empty() {
                            break;
                        }
                        // In rank order: a blob's first sighting in the run is its lowest.
                        for &(rank, si, slot) in run {
                            let slab = slabs.get(us(u64::from(si))).ctx("tree scan: slab")?;
                            let (kind, content) = resolver.resolve(slab, slot)?;
                            let mut it = TreeIter::new(content);
                            let mut bad = kind != OBJ_TREE;
                            for e in &mut it {
                                match mode_kind(e.mode) {
                                    Some(OBJ_BLOB) => {
                                        first.entry(e.oid).or_insert(rank);
                                    }
                                    Some(_) => {}
                                    None => bad = true,
                                }
                            }
                            if bad || it.malformed {
                                let oid = slab.rec(slot).map(|r| r.oid).unwrap_or(Oid::ZERO);
                                crate::bail!("malformed tree {oid}");
                            }
                        }
                        for (oid, rank) in first.drain() {
                            if let Some(p) = parts.get_mut(usize::from(oid.0[0])) {
                                p.push((oid, rank));
                            }
                        }
                        for (part, shard) in parts.iter_mut().zip(&shards) {
                            if part.is_empty() {
                                continue;
                            }
                            let mut map = lock(shard);
                            for (oid, rank) in part.drain(..) {
                                map.entry(oid)
                                    .and_modify(|r| *r = (*r).min(rank))
                                    .or_insert(rank);
                            }
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        for w in workers {
            w.join()
                .map_err(|_| crate::err!("tree scan worker failed"))??;
        }
        Ok(())
    })?;
    let mut out: Vec<(u64, Oid)> = Vec::new();
    for shard in shards {
        let map = match shard.into_inner() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        out.extend(map.into_iter().map(|(oid, rank)| (rank, oid)));
    }
    out.sort_unstable();
    Ok(out)
}

/// Deletes the segment indexes in `dir` (downloads, `<id>.bidx`) that `bm` no longer lists
/// (a fold replaced them). Best effort.
pub fn prune_segments(dir: &Path, bm: &BlobManifest) {
    let keep: HashSet<String> = bm
        .segments
        .iter()
        .map(|s| format!("{}.bidx", s.id))
        .collect();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".bidx") && !keep.contains(&name) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Packs and segments `blobs.bin` lists that the store does not hold (gone, or another
/// length): storage lost them.
#[derive(Debug, Default)]
pub struct Lost {
    pub packs: HashSet<String>,
    pub segments: HashSet<String>,
}

impl Lost {
    pub fn is_empty(&self) -> bool {
        self.packs.is_empty() && self.segments.is_empty()
    }
}

/// What `bm` lists that `listing` (of `blobs/`) lacks. The listing must be taken after `bm`
/// was read: everything `bm` lists was written before it was published.
pub fn lost(bm: &BlobManifest, listing: &[crate::store::ObjectInfo]) -> Lost {
    let sizes: HashMap<&str, u64> = listing.iter().map(|o| (o.key.as_str(), o.size)).collect();
    let gone = |key: String, len: u64| sizes.get(key.as_str()) != Some(&len);
    Lost {
        packs: bm
            .packs
            .iter()
            .filter(|p| gone(crate::blobs::pack_key(&p.id), p.len))
            .map(|p| p.id.clone())
            .collect(),
        segments: bm
            .segments
            .iter()
            .filter(|s| gone(crate::blobs::segment_key(&s.id), s.index_len))
            .map(|s| s.id.clone())
            .collect(),
    }
}

/// Takes what storage lost out of `blobs.bin`: lost segments are dropped, segments that
/// index lost packs are rewritten without those records, and packs no segment indexes
/// leave the list. The blobs involved are then unlisted: edges proxy them (counting them
/// as missing) until `mirror` copies them again.
pub fn drop_lost(store: &Store, work: &Path, threads: usize, lost: &Lost) -> Result<()> {
    if lost.is_empty() {
        return Ok(());
    }
    crate::log!(
        "blobs.bin lists {} packs and {} segments the store does not hold; dropping them",
        lost.packs.len(),
        lost.segments.len()
    );
    let indexed = |bm: &BlobManifest| -> HashSet<String> {
        bm.segments
            .iter()
            .flat_map(|s| s.packs.iter().cloned())
            .collect()
    };
    if !lost.segments.is_empty() {
        crate::blobcopy::update(store, |bm| {
            bm.segments.retain(|s| !lost.segments.contains(&s.id));
            let keep = indexed(bm);
            bm.packs.retain(|p| keep.contains(&p.id));
            Ok(())
        })?;
    }
    if !lost.packs.is_empty() {
        let pick = |bm: &BlobManifest| -> Vec<String> {
            bm.segments
                .iter()
                .filter(|s| s.packs.iter().any(|p| lost.packs.contains(p)))
                .map(|s| s.id.clone())
                .collect()
        };
        for _ in 0..3 {
            let (bm, _) = crate::blobcopy::load(store)?;
            if pick(&bm).is_empty() {
                break;
            }
            crate::blobcopy::fold(store, work, threads, pick, &lost.packs)?;
        }
        let (bm, _) = crate::blobcopy::load(store)?;
        if !pick(&bm).is_empty() {
            crate::bail!("blobs.bin kept changing; lost packs are still indexed");
        }
        if bm.packs.iter().any(|p| lost.packs.contains(&p.id)) {
            crate::blobcopy::update(store, |bm| {
                let keep = indexed(bm);
                bm.packs.retain(|p| keep.contains(&p.id));
                Ok(())
            })?;
        }
    }
    Ok(())
}
