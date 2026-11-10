//! Stores that keep blobs in their own packs (`--blobs separate`): what the pipeline,
//! `mirror` and the edge share.
//!
//! **The consistency rule.** Once `blobs.bin` is complete, every blob of every tree in the
//! store is in a blob pack it lists (a manifest's trees' at the `blobs.bin` generation the
//! manifest records, `blobs_generation`). Independently, every blob any tree references has
//! its size in the sizes table of the slab whose trees introduced it, so sizes are complete
//! for every tree a client can see, before and after the mirror.
//!
//! `gvfs-cache mirror` makes it true for the history up to genesis (every blob of every
//! tree in the slabs). A pipeline run keeps it by working blobs first: it copies the blobs
//! that the trees in its window's pack reference and the store lacks, publishes them in
//! `blobs.bin`, and only then publishes the trees. Both copy through [`copy_all`]: several
//! blobs at a time, published as the copy goes, so `blobs.bin` may list blobs no tree
//! references yet (a run that stopped early left them: harmless, and the next run's to
//! keep). (Until the mirror has finished, a run copies only those no earlier tree
//! references, which no sizes table has: the rest are in the plan of the mirror, whose
//! last run checks every blob of the trees it planned from. Every blob in a sizes table
//! is then either the mirror's or new in the window that introduced it, and that window
//! copied it.) That is enough because an upstream leaves out of a pack only objects its
//! haves reach, and the haves are the store's own earlier tips: a tree it leaves out is
//! in the store, and so are its blobs (by induction from the mirrored genesis). Whatever
//! the upstream (plain git, bitmaps), a reused old version (a revert, an old branch
//! merged) is either in the pack's trees or in the store.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::time::{Duration, Instant};

use crate::blobcopy::{Copied, Item, copy_item, mode_kind, plan, publish};
use crate::blobs::{BlobManifest, BlobRec, Segment};
use crate::blobstream::Throttle;
use crate::manifest::Manifest;
use crate::object::{TreeIter, commit_tree};
use crate::pack::{OBJ_BLOB, OBJ_COMMIT, OBJ_TREE};
use crate::sizes::Sizes;
use crate::slab::{Slab, SlabMeta};
use crate::splice::Resolver;
use crate::store::Store;
use crate::upstream::Upstream;
use crate::util::{Context, Oid, Result, human_bytes, u32_of, u64_of, us};

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
    /// warning: `gvfs-cache mirror` repairs `blobs.bin`.
    pub fn load(store: &Store, work: &Path, bm: &BlobManifest) -> Result<BlobIndex> {
        let mut segments = Vec::with_capacity(bm.segments.len());
        for s in &bm.segments {
            match crate::blobcopy::open_segment(store, work, s) {
                Ok(seg) => segments.push(seg),
                Err(e) if e.is_not_found() => crate::log!(
                    "warning: blobs.bin generation {} lists segment {}, which the store does \
                     not hold ({e}); its blobs count as missing until gvfs-cache mirror \
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

/// A batch is published once it holds this many packs, whatever the time.
const PUBLISH_ITEMS: usize = 256;

/// How blobs are copied into the store, by `mirror` (the history's) and by a pipeline run
/// (its window's): several at a time, each on an upstream connection of its own, and
/// what has been copied published as the copy goes ([`copy_all`]).
pub struct Copies<'a> {
    /// Whose copy it is: begins its log lines (`mirror`, `window`).
    pub who: &'a str,
    pub store: &'a Store,
    /// Scratch space: a spool per job.
    pub work: &'a Path,
    /// Where the segment indexes read are kept (downloads, pruned to what `blobs.bin`
    /// lists).
    pub cache: &'a Path,
    pub threads: usize,
    /// Copies in flight at once (`--jobs`).
    pub jobs: usize,
    /// Finished packs are published at least this often (`--publish-secs`).
    pub publish_every: Duration,
    /// A cap on the download rate of all copies together, in bytes per second.
    pub rate: Option<u64>,
}

/// The size each blob of a copy is to have: what clients were told, or will be. A blob
/// whose content has another size fails its copy, before its pack is published.
pub struct SizeOf<'a> {
    /// Who gave the sizes, for that error: `the sizes table says`.
    pub said: &'a str,
    pub get: &'a (dyn Fn(&Oid) -> Option<u64> + Sync),
}

/// `item` without the blobs `index` lists now; `None` if it lists them all.
fn unlisted<'a>(
    item: &'a Item,
    index: &BlobIndex,
    sizes: &SizeOf,
) -> Result<Option<Cow<'a, Item>>> {
    let oids = item.oids();
    let listed = oids.iter().filter(|o| index.find(o).is_some()).count();
    if listed == 0 {
        return Ok(Some(Cow::Borrowed(item)));
    }
    if listed == oids.len() {
        return Ok(None);
    }
    let left: Vec<Oid> = oids
        .iter()
        .filter(|o| index.find(o).is_none())
        .copied()
        .collect();
    let mut bytes = 0u64;
    for o in &left {
        let Some(size) = (sizes.get)(o) else {
            crate::bail!("no size for blob {o}");
        };
        bytes = bytes.saturating_add(size);
    }
    Ok(Some(Cow::Owned(Item::Batch { oids: left, bytes })))
}

/// Every copied blob has the size it is to have.
fn check_sizes(c: &Copied, sizes: &SizeOf) -> Result<()> {
    for r in &c.recs {
        match (sizes.get)(&r.oid) {
            Some(s) if s == r.size => {}
            Some(s) => crate::bail!(
                "blob {}: {} {s} bytes, its content has {}",
                r.oid,
                sizes.said,
                r.size
            ),
            None => crate::bail!("no size for blob {}", r.oid),
        }
    }
    Ok(())
}

/// What a job did with an item.
enum Done {
    Copied(Copied),
    /// Every blob of it was listed meanwhile.
    Skipped,
}

fn read<T: Clone>(l: &RwLock<T>) -> T {
    match l.read() {
        Ok(g) => g.clone(),
        Err(p) => p.into_inner().clone(),
    }
}

fn write<T>(l: &RwLock<T>, v: T) {
    match l.write() {
        Ok(mut g) => *g = v,
        Err(p) => *p.into_inner() = v,
    }
}

/// Copies `items`, in the order given, with [`Copies::jobs`] jobs, each on a connection
/// `connect` opens, and publishes finished packs in `blobs.bin` as it goes: at least every
/// [`Copies::publish_every`], and whatever has finished when a copy fails or the last one
/// ends. A run that stops early (killed at its runner's time limit, an item that failed
/// after its retries) therefore keeps what it published, and the next one copies the rest.
///
/// Each item is first checked against the newest index this run has seen (`index` at
/// first, then `blobs.bin` as read again after each publish): blobs another writer has
/// listed meanwhile are left out, an item that has none left is skipped. A rate limit is
/// waited out by the job that met it ([`Upstream`]). The first failure stops the jobs from
/// taking more items, and is returned once the packs that finished are published.
pub fn copy_all(
    c: &Copies,
    connect: &dyn Fn() -> Result<Upstream>,
    items: &[Item],
    sizes: &SizeOf,
    index: Arc<BlobIndex>,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let mut ups = Vec::new();
    for _ in 0..c.jobs.clamp(1, items.len()) {
        ups.push(connect()?);
    }
    let throttle = c.rate.map(|r| Arc::new(Throttle::new(r)));
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let listed = RwLock::new(index);
    let total_bytes: u64 = items.iter().map(Item::bytes).sum();
    let (tx, rx) = mpsc::channel::<Result<Done>>();
    std::thread::scope(|sc| -> Result<()> {
        for (w, mut up) in ups.into_iter().enumerate() {
            let tx = tx.clone();
            let (next, stop, throttle, listed) = (&next, &stop, &throttle, &listed);
            let spool = c.work.join(format!("spool-{w}"));
            sc.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) else {
                        break;
                    };
                    let r = (|| -> Result<Done> {
                        let index: Arc<BlobIndex> = read(listed);
                        let Some(item) = unlisted(item, &index, sizes)? else {
                            return Ok(Done::Skipped);
                        };
                        let copied = copy_item(&mut up, c.store, &spool, &item, throttle.as_ref())?;
                        check_sizes(&copied, sizes)?;
                        Ok(Done::Copied(copied))
                    })();
                    let failed = r.is_err();
                    if tx.send(r).is_err() || failed {
                        break;
                    }
                }
            });
        }
        drop(tx);
        let t0 = Instant::now();
        let mut pending: Vec<Copied> = Vec::new();
        let mut last = Instant::now();
        let mut failure = None;
        let (mut done_items, mut done_bytes, mut skipped) = (0usize, 0u64, 0usize);
        let mut flush = |pending: &mut Vec<Copied>, skipped: usize| -> Result<()> {
            let bm = publish(c.store, c.work, pending, c.threads, |_| Ok(()))?;
            done_items = done_items.saturating_add(pending.len());
            done_bytes = done_bytes.saturating_add(
                pending
                    .iter()
                    .flat_map(|c| c.recs.iter().map(|r| r.size))
                    .sum::<u64>(),
            );
            let secs = t0.elapsed().as_secs_f64().max(0.001);
            crate::log!(
                "{}: {done_items}/{} items, {} of {} ({:.1} MB/s){}; blobs.bin generation {}",
                c.who,
                items.len(),
                human_bytes(done_bytes),
                human_bytes(total_bytes),
                done_bytes as f64 / secs / 1e6,
                if skipped > 0 {
                    format!("; {skipped} items listed meanwhile, skipped")
                } else {
                    String::new()
                },
                bm.generation
            );
            pending.clear();
            // What other writers published meanwhile is left out of the next items.
            match BlobIndex::load(c.store, c.cache, &bm) {
                Ok(i) => {
                    write(&listed, Arc::new(i));
                    prune_segments(c.cache, &bm);
                }
                Err(e) => crate::log!("{}: reloading the blob index failed ({e})", c.who),
            }
            Ok(())
        };
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(Ok(Done::Copied(c))) => pending.push(c),
                Ok(Ok(Done::Skipped)) => skipped = skipped.saturating_add(1),
                Ok(Err(e)) => {
                    stop.store(true, Ordering::Relaxed);
                    failure.get_or_insert(e);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            if !pending.is_empty()
                && (last.elapsed() >= c.publish_every || pending.len() >= PUBLISH_ITEMS)
            {
                if let Err(e) = flush(&mut pending, skipped) {
                    stop.store(true, Ordering::Relaxed);
                    return Err(e);
                }
                last = Instant::now();
            }
        }
        // What finished is published even when a copy failed: a rerun skips it.
        if !pending.is_empty() {
            flush(&mut pending, skipped)?;
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })
}

/// Copies the blobs `need`, which a pipeline run's window references and `index` (the
/// `blobs.bin` it read) lacks, through [`copy_all`]: their sizes are asked of the upstream
/// first (so that large blobs stream alone), and each job's connection is another of
/// `up`'s. Returns the `blobs.bin` that lists every one of them (`None` if nothing was
/// needed) and their sizes, as verified by hashing their content.
///
/// The items are taken in an order of this run's own. Runs that overlap (the next
/// quarter's, started while this one still copies) plan the same items, and in one order
/// each would take an item while the other was still copying it, to the last one:
/// everything twice, however often they published. In orders of their own, a run mostly
/// comes to items the other has published, and skips them. What is still copied twice is
/// what both took before either had published it and the other had read that: about what
/// the runs copy in two publish intervals, a few times over a long copy (and all of a
/// window that lasts no longer). A blob listed twice is found once; its other copy is
/// wasted bytes.
pub fn copy_needed(
    up: &mut Upstream,
    c: &Copies,
    need: &[Oid],
    item_bytes: u64,
    index: Arc<BlobIndex>,
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
    let mut items = plan(&wants, item_bytes);
    let order = std::collections::hash_map::RandomState::new();
    items.sort_by_cached_key(|item| order.hash_one(item.oids().first()));
    let reported: HashMap<Oid, u64> = wants.iter().copied().collect();
    crate::log!(
        "{}: {} blobs ({}) to copy in {} items ({} large), {} at a time",
        c.who,
        wants.len(),
        human_bytes(wants.iter().map(|w| w.1).sum()),
        items.len(),
        items
            .iter()
            .filter(|i| matches!(i, Item::Large { .. }))
            .count(),
        c.jobs.clamp(1, items.len())
    );
    let sizes = SizeOf {
        said: "the upstream reported",
        get: &|oid| reported.get(oid).copied(),
    };
    copy_all(c, &|| up.another(), &items, &sizes, index)?;

    // Every blob must now be listed, whoever copied it: only then may the trees that
    // reference them be published, naming this `blobs.bin`.
    let (bm, _) = crate::blobcopy::load(c.store)?;
    let index = BlobIndex::load(c.store, c.cache, &bm)?;
    let mut missing = 0usize;
    for (oid, size) in &wants {
        match index.find(oid) {
            Some((_, rec)) if rec.size == *size => {
                got.insert(*oid, rec.size);
            }
            Some((_, rec)) => crate::bail!(
                "blob {oid}: the upstream reported {size} bytes, its content has {}",
                rec.size
            ),
            None => missing = missing.saturating_add(1),
        }
    }
    if missing > 0 {
        crate::bail!("{missing} blobs of the window are still missing after the copy");
    }
    Ok((Some(bm), got))
}

/// Keeps in a pipeline runner's store cache (`--cache-dir`: downloaded copies of immutable
/// store objects, reused across runs) only what manifest `m` and `blobs.bin` `bm` list.
/// What it removes are downloads, known by their names ([`is_download`]): the directory is
/// the operator's choice and may hold other files, which are not this program's to delete.
/// (It removed every file the store did not list.)
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
        if name.starts_with('.') || keep.contains(&name) || !is_download(&name) {
            continue;
        }
        if std::fs::remove_file(entry.path()).is_ok() {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

/// Whether `name` is one a runner gives a store object it downloads into its cache: a
/// slab's pack or index (`<slab id>.pack`, `.mphf`: [`crate::pipeline::open_slabs`]), its
/// sizes table (`slabs_<slab id>.sizes`: [`local_copy`]) or a segment index (`<id>.bidx`,
/// the 40 hex digits of its SHA-1: [`crate::blobcopy::open_segment`]). A slab id is
/// `<tier>-<end_ts>-<16 hex digits>` ([`SlabMeta::make_id`]).
fn is_download(name: &str) -> bool {
    let hex = |s: &str, n: usize| {
        s.len() == n && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    };
    match name.rsplit_once('.') {
        Some((id, "pack" | "mphf")) => SlabMeta::is_id(id),
        Some((id, "sizes")) => id.strip_prefix("slabs_").is_some_and(SlabMeta::is_id),
        Some((id, "bidx")) => hex(id, 40),
        _ => false,
    }
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
pub fn tree_blobs(slabs: &[Arc<Slab>], commit: &Oid) -> Result<Vec<Oid>> {
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use crate::blobs::SegmentMeta;
    use crate::slab::Tier;

    fn slab(tier: Tier, end: u64) -> SlabMeta {
        SlabMeta {
            id: SlabMeta::make_id(tier, end, &[end as u8; 20]),
            tier,
            start_ts: 0,
            end_ts: end,
            have: Oid::ZERO,
            want: Oid([end as u8; 20]),
            objects: 1,
            pack_len: 1,
            index_len: 1,
            pack_sha: [end as u8; 20],
        }
    }

    /// Pruning the runner's cache removes the downloads the store no longer lists (a slab's
    /// pack, index and sizes table, a segment index), which it knows by their names, and
    /// nothing else: whatever else `--cache-dir` holds stays. (It removed every file the
    /// store did not list: in a directory shared with anything else, all of that.)
    #[test]
    fn pruning_the_cache_removes_only_its_own_downloads() {
        let dir = std::env::temp_dir().join(format!("gvd-cache-prune-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        let (base, hour, week) = (
            slab(Tier::Base, 10),
            slab(Tier::H1, 20),
            slab(Tier::Week, 15),
        );
        let (dropped, older) = (slab(Tier::M15, 12), slab(Tier::Base, 5));
        let segment = |c: char| SegmentMeta {
            id: c.to_string().repeat(40),
            blobs: 1,
            index_len: 1,
            packs: Vec::new(),
        };
        let m = Manifest {
            generation: 3,
            tip: Oid([1; 20]),
            max_ts: 20,
            baseline: base.clone(),
            weekly: vec![week.clone()],
            pyramid: vec![hour.clone()],
            retained: Vec::new(),
            flags: crate::manifest::FLAG_SEPARATE_BLOBS,
            blobs_generation: 2,
        };
        let bm = BlobManifest {
            segments: vec![segment('a')],
            ..BlobManifest::default()
        };
        let downloads = |s: &SlabMeta| {
            [
                format!("{}.pack", s.id),
                format!("{}.mphf", s.id),
                format!("slabs_{}.sizes", s.id),
            ]
        };
        let mut listed: Vec<String> = [&base, &hour, &week]
            .into_iter()
            .flat_map(downloads)
            .collect();
        listed.push(format!("{}.bidx", segment('a').id));
        let mut stale: Vec<String> = [&dropped, &older].into_iter().flat_map(downloads).collect();
        stale.push(format!("{}.bidx", segment('b').id));
        let others = [
            "notes.txt".to_string(),
            format!("pack-{}.pack", "c".repeat(40)),
            "data.pack".to_string(),
            "m15-12.mphf".to_string(),
            "report.sizes".to_string(),
            "index.bidx".to_string(),
            format!(".{}.bidx.tmp-1-0", "d".repeat(40)),
        ];
        for name in listed.iter().chain(&stale).chain(&others) {
            std::fs::write(dir.join(name), b"x").unwrap();
        }
        std::fs::write(dir.join("sub").join(&stale[0]), b"x").unwrap();

        assert_eq!(prune_cache(&dir, &m, &bm).unwrap(), stale.len());
        let mut left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        let mut want: Vec<String> = listed.into_iter().chain(others).collect();
        want.push("sub".to_string());
        want.sort();
        assert_eq!(left, want);
        assert!(dir.join("sub").join(&stale[0]).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
