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
//!
//! Runs that copy at once (two runs of one window, two mirrors, a mirror and a window)
//! claim the blobs they copy ([`crate::claims`]): each blob is copied by one of them.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, mpsc};
use std::time::{Duration, Instant};

use crate::blobcopy::{Copied, Item, copy_item, mode_kind, plan, publish};
use crate::blobs::{BlobManifest, BlobRec, Segment};
use crate::blobstream::Throttle;
use crate::claims::Ledger;
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

/// A copy claims items ahead of its jobs ([`crate::claims`]). When fewer are claimed and
/// not begun than it has jobs, it claims the next ones: up to [`CLAIM_ITEMS`] claimed
/// and not begun per job, [`CLAIM_BYTES`] in one claim, and [`CLAIM_BLOBS`] blobs
/// claimed and not begun (but an item, if none is). So a job that ends an item finds
/// the next one claimed, and a claim, which is a write to the store, is made once for
/// several items where they are small (blobs of 16 MiB: 13 at a time with four jobs).
/// What a run has claimed and not begun no other run takes meanwhile, and if the run
/// dies the others wait `--claim-secs` for it. And every write of `claims.bin` has the
/// id of every blob claimed, 20 bytes each: one item ahead, where an item is a pack of
/// 100,000 small blobs.
const CLAIM_ITEMS: usize = 4;
const CLAIM_BYTES: u64 = 256 << 20;
const CLAIM_BLOBS: usize = 4096;

/// How many of the items a copy comes to next (`coming`, in their order) it claims now,
/// for `jobs` jobs, with `ready` items of `ahead` blobs claimed and not begun
/// ([`CLAIM_ITEMS`]).
fn claim_ahead<'i>(
    coming: impl Iterator<Item = &'i Item>,
    jobs: usize,
    ready: usize,
    ahead: usize,
) -> usize {
    if ready >= jobs {
        return 0;
    }
    let room = jobs.saturating_mul(CLAIM_ITEMS).saturating_sub(ready);
    let (mut n, mut bytes, mut blobs) = (0usize, 0u64, ahead);
    for item in coming.take(room) {
        bytes = bytes.saturating_add(item.bytes());
        blobs = blobs.saturating_add(item.oids().len());
        if (bytes > CLAIM_BYTES && n > 0) || (blobs > CLAIM_BLOBS && (n > 0 || ready > 0)) {
            break;
        }
        n = n.saturating_add(1);
    }
    n
}

/// A copy that is through its items goes back to the blobs that other runs' claims hold:
/// at once, [`LOOK_FIRST`] later, and then after twice as long each time it finds them
/// as they were, up to [`LOOK_EVERY`]. What it waits for changes when another run
/// publishes (every `--publish-secs`, and when it has no more to copy): a run about to
/// end is found within a quarter of a second, and one that copies on for an hour is
/// asked for two objects it need not send, every two seconds.
const LOOK_FIRST: Duration = Duration::from_millis(250);
const LOOK_EVERY: Duration = Duration::from_secs(2);

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
    /// How long the run's claim on the blobs it is copying holds without being written
    /// again (`--claim-secs`): what other runs wait for those blobs if this one dies.
    pub claim: Duration,
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

/// The items a copy has claimed and no job has begun, each with its claim's number
/// ([`Ledger::release`]).
struct Queue<'a> {
    ready: VecDeque<(u64, Cow<'a, Item>)>,
    /// No more will come.
    closed: bool,
}

/// What the threads of a copy share: its jobs, the one that claims, and the one that
/// publishes.
struct Shared<'a> {
    queue: Mutex<Queue<'a>>,
    /// Told when the queue changes, when a copy fails and when the copy ends.
    wake: Condvar,
    /// A copy failed: no job takes another item, and no more are claimed.
    stop: AtomicBool,
    /// The last pack is published, or will not be: the claims are given up.
    ended: AtomicBool,
    /// The numbers of the claims to give up: their packs are listed, or their copies
    /// failed.
    released: Mutex<Vec<u64>>,
    /// What `blobs.bin` listed when the run last read it.
    listed: RwLock<Arc<BlobIndex>>,
    /// Blobs of the items that `blobs.bin` listed when the run came to them: other
    /// runs' copies.
    elsewhere: AtomicUsize,
    /// How many items the jobs have taken.
    taken: AtomicUsize,
    /// The run has no item left to claim but those other runs' claims hold: what its
    /// jobs have finished is published without waiting for `--publish-secs`, since
    /// those runs may be waiting for it likewise.
    waiting: AtomicBool,
}

/// Waits on `cv`, for `most` at most.
fn wait<'g, T>(
    cv: &Condvar,
    held: std::sync::MutexGuard<'g, T>,
    most: Duration,
) -> std::sync::MutexGuard<'g, T> {
    match cv.wait_timeout(held, most) {
        Ok((held, _)) => held,
        Err(p) => p.into_inner().0,
    }
}

/// How long a thread of a copy waits for its cue before it looks for itself.
const TICK: Duration = Duration::from_millis(200);

impl<'a> Shared<'a> {
    /// The next item for a job, waited for: `None` once none is to come, or a copy has
    /// failed.
    fn next(&self) -> Option<(u64, Cow<'a, Item>)> {
        let mut queue = lock(&self.queue);
        loop {
            if self.stop.load(Ordering::Relaxed) {
                return None;
            }
            if let Some(next) = queue.ready.pop_front() {
                self.taken.fetch_add(1, Ordering::Relaxed);
                // One fewer is ready: the claimer's cue.
                self.wake.notify_all();
                return Some(next);
            }
            if queue.closed {
                return None;
            }
            queue = wait(&self.wake, queue, TICK);
        }
    }

    /// Wakes the threads that wait: none of them is between looking and waiting.
    fn tell(&self) {
        let _queue = lock(&self.queue);
        self.wake.notify_all();
    }
}

/// Ends the thread that claims when the copy is over, however it ends.
struct Ended<'s, 'a>(&'s Shared<'a>);

impl Drop for Ended<'_, '_> {
    fn drop(&mut self) {
        self.0.ended.store(true, Ordering::Relaxed);
        self.0.tell();
    }
}

/// The thread of a copy that claims ([`crate::claims`]), until the copy has ended. It
/// keeps the jobs in items: it goes through `items` in their order, claims of each what
/// is free ([`Ledger::claim`]) and queues it for the jobs, a few items ahead of them
/// ([`CLAIM_ITEMS`]). What other runs' claims hold it comes back to once it is through
/// the items ([`LOOK_FIRST`]): listed by then, it is done; still held, it waits on;
/// neither (the run that held it failed, or died and its claim has lapsed), this run
/// claims and copies it. It gives up what the publisher has listed and what failed,
/// with a write of its own if no claim is due to carry it, and writes the run's claim
/// again before it can lapse ([`Ledger::due`]).
fn claim_items<'a>(
    c: &Copies,
    items: &'a [Item],
    sizes: &SizeOf,
    jobs: usize,
    ledger: &mut Ledger<'a>,
    s: &Shared<'a>,
) -> Result<()> {
    let mut next = 0usize;
    // What other runs' claims held of the items when this run came to them, and the
    // part of that it is going through again.
    let mut later: Vec<Item> = Vec::new();
    let mut again: VecDeque<Item> = VecDeque::new();
    // When it last went back to `later`, and how long it waits before the next time.
    let (mut looked, mut pause) = (Instant::now(), Duration::ZERO);
    let (mut said, mut gave_back) = (false, false);
    while !s.ended.load(Ordering::Relaxed) {
        ledger.release(&std::mem::take(&mut *lock(&s.released)));
        let stopped = s.stop.load(Ordering::Relaxed);
        if stopped && !gave_back {
            // A copy failed, and the run ends with the items its jobs are in: what none
            // has begun is given up now, for the other runs.
            let unbegun: Vec<u64> = lock(&s.queue).ready.drain(..).map(|(n, _)| n).collect();
            ledger.release(&unbegun);
            gave_back = true;
        }
        // The items claimed and not begun, and their blobs.
        let (ready, ahead) = {
            let queue = lock(&s.queue);
            let blobs = queue.ready.iter().map(|(_, item)| item.oids().len());
            (queue.ready.len(), blobs.sum::<usize>())
        };
        let mut through = next >= items.len() && again.is_empty();
        if !stopped && ready < jobs {
            if through && !later.is_empty() && looked.elapsed() >= pause {
                ledger.look()?;
                again.extend(later.drain(..));
                looked = Instant::now();
                // The next time after twice as long: sooner, if any of it is settled
                // this time.
                pause = pause.saturating_mul(2).clamp(LOOK_FIRST, LOOK_EVERY);
            }
            let back = next >= items.len();
            let coming = items.get(next..).unwrap_or(&[]).iter().chain(&again);
            let mut chunk: Vec<Cow<'a, Item>> = Vec::new();
            for _ in 0..claim_ahead(coming, jobs, ready, ahead) {
                match items.get(next) {
                    Some(item) => {
                        chunk.push(Cow::Borrowed(item));
                        next = next.saturating_add(1);
                    }
                    None => chunk.extend(again.pop_front().map(Cow::Owned)),
                }
            }
            if !chunk.is_empty() {
                let taken = ledger.claim(&chunk, sizes)?;
                if !Arc::ptr_eq(&read(&s.listed), ledger.index()) {
                    write(&s.listed, Arc::clone(ledger.index()));
                }
                s.elsewhere.fetch_add(taken.listed, Ordering::Relaxed);
                if back && (taken.listed > 0 || !taken.ready.is_empty()) {
                    pause = LOOK_FIRST;
                }
                later.extend(taken.later);
                if !taken.ready.is_empty() {
                    s.waiting.store(false, Ordering::Relaxed);
                    lock(&s.queue).ready.extend(taken.ready);
                    s.wake.notify_all();
                }
                continue;
            }
            through = next >= items.len() && again.is_empty();
            if through {
                s.waiting.store(!later.is_empty(), Ordering::Relaxed);
            }
            if through && later.is_empty() {
                let mut queue = lock(&s.queue);
                if !queue.closed {
                    queue.closed = true;
                    s.wake.notify_all();
                }
            } else if through
                && !said
                && let (blobs @ 1.., until) = ledger.waits(&later)
            {
                crate::log!(
                    "{}: {blobs} blobs are other runs', claimed until {until} for now: \
                     waiting for them to be listed, or for the claims to lapse",
                    c.who
                );
                said = true;
            }
        }
        if ledger.due() {
            ledger.sync()?;
            continue;
        }
        // Its cue: a job took an item, a claim is to be given up, the copy has failed
        // or ended. Or the time to go back to what other runs hold.
        let queue = lock(&s.queue);
        if s.ended.load(Ordering::Relaxed)
            || s.stop.load(Ordering::Relaxed) != stopped
            || queue.ready.len() != ready
            || !lock(&s.released).is_empty()
        {
            continue;
        }
        let most = match through && !later.is_empty() && !stopped && ready < jobs {
            true => pause.saturating_sub(looked.elapsed()).min(TICK),
            false => TICK,
        };
        drop(wait(&s.wake, queue, most));
    }
    Ok(())
}

/// Copies `items`, in the order given, with [`Copies::jobs`] jobs, each on a connection
/// `connect` opens, and publishes finished packs in `blobs.bin` as it goes: at least every
/// [`Copies::publish_every`], and whatever has finished when a copy fails, when the run
/// has only other runs' blobs left to wait for, or when the last one ends. A run that
/// stops early (killed at its runner's time limit, an item that failed after its
/// retries) therefore keeps what it published, and the next one copies the rest.
///
/// Other runs may be copying into the store at once (another run of the window, a
/// mirror), and may have planned the same blobs. Before a job gets an item the run
/// claims its blobs in `claims.bin` ([`crate::claims`], [`claim_items`]), leaving out
/// those that `blobs.bin` lists by now (`index` was what it listed when the run planned)
/// and those that another run's claim holds: these it waits for, and copies itself if
/// that run does not. So a blob is copied by one run, and the runs share the items. A
/// pack's claim is given up once the pack is published, in that order, and every claim
/// when the copy ends.
///
/// A rate limit is waited out by the job that met it ([`Upstream`]). The first failure
/// stops the jobs from taking more items, gives up the claims of the item that failed
/// and of those no job has begun (no other run waits for them to lapse), and is returned
/// once the packs that finished are published.
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
    let jobs = c.jobs.clamp(1, items.len());
    let mut ups = Vec::new();
    for _ in 0..jobs {
        ups.push(connect()?);
    }
    let throttle = c.rate.map(|r| Arc::new(Throttle::new(r)));
    let mut ledger = Ledger::new(
        c.store,
        c.cache,
        c.who,
        c.claim.as_secs(),
        Arc::clone(&index),
    );
    let shared = Shared {
        queue: Mutex::new(Queue {
            ready: VecDeque::new(),
            closed: false,
        }),
        wake: Condvar::new(),
        stop: AtomicBool::new(false),
        ended: AtomicBool::new(false),
        released: Mutex::new(Vec::new()),
        listed: RwLock::new(index),
        elsewhere: AtomicUsize::new(0),
        taken: AtomicUsize::new(0),
        waiting: AtomicBool::new(false),
    };
    let total_bytes: u64 = items.iter().map(Item::bytes).sum();
    let (tx, rx) = mpsc::channel::<(u64, Result<Done>)>();
    std::thread::scope(|sc| -> Result<()> {
        for (w, mut up) in ups.into_iter().enumerate() {
            let tx = tx.clone();
            let (shared, throttle) = (&shared, &throttle);
            let spool = c.work.join(format!("spool-{w}"));
            sc.spawn(move || {
                while let Some((claim, item)) = shared.next() {
                    let r = (|| -> Result<Done> {
                        // Listed since it was claimed, if the claim had lapsed (a run
                        // that stood still for longer than `--claim-secs`) and another
                        // run took its blobs.
                        let index: Arc<BlobIndex> = read(&shared.listed);
                        let Some(item) = unlisted(&item, &index, sizes)? else {
                            let blobs = item.oids().len();
                            shared.elsewhere.fetch_add(blobs, Ordering::Relaxed);
                            return Ok(Done::Skipped);
                        };
                        let copied = copy_item(&mut up, c.store, &spool, &item, throttle.as_ref())?;
                        check_sizes(&copied, sizes)?;
                        Ok(Done::Copied(copied))
                    })();
                    let failed = r.is_err();
                    if tx.send((claim, r)).is_err() || failed {
                        break;
                    }
                }
            });
        }
        drop(tx);
        let (shared, ledger) = (&shared, &mut ledger);
        let claimer = sc.spawn(move || {
            let claimed = claim_items(c, items, sizes, jobs, ledger, shared);
            if claimed.is_err() {
                shared.stop.store(true, Ordering::Relaxed);
            }
            lock(&shared.queue).closed = true;
            shared.tell();
            if claimed.is_ok() {
                // The copy has ended: what it published is listed, and the rest will
                // not be copied by this run.
                ledger.release_all();
                if ledger.due()
                    && let Err(e) = ledger.sync()
                {
                    crate::log!(
                        "{}: its claims were not given up ({e}): they lapse in {}s",
                        c.who,
                        c.claim.as_secs()
                    );
                }
            }
            claimed
        });
        let ended = Ended(shared);
        let copied = (|| -> Result<()> {
            let t0 = Instant::now();
            // The packs to publish, and their claims' numbers.
            let mut pending: Vec<Copied> = Vec::new();
            let mut claims: Vec<u64> = Vec::new();
            let mut last = Instant::now();
            let mut failure = None;
            // How many of the items the jobs have taken they are done with.
            let mut finished = 0usize;
            let (mut done_items, mut done_bytes) = (0usize, 0u64);
            let mut flush = |pending: &mut Vec<Copied>, claims: &mut Vec<u64>| -> Result<()> {
                let bm = publish(c.store, c.work, pending, c.threads, |_| Ok(()))?;
                // Listed: only now are their claims given up, never before
                // (`crate::claims`).
                lock(&shared.released).append(claims);
                done_items = done_items.saturating_add(pending.len());
                done_bytes = done_bytes.saturating_add(
                    pending
                        .iter()
                        .flat_map(|c| c.recs.iter().map(|r| r.size))
                        .sum::<u64>(),
                );
                let secs = t0.elapsed().as_secs_f64().max(0.001);
                let elsewhere = shared.elsewhere.load(Ordering::Relaxed);
                crate::log!(
                    "{}: {done_items}/{} items, {} of {} ({:.1} MB/s){}; blobs.bin generation {}",
                    c.who,
                    items.len(),
                    human_bytes(done_bytes),
                    human_bytes(total_bytes),
                    done_bytes as f64 / secs / 1e6,
                    if elsewhere > 0 {
                        format!("; {elsewhere} blobs listed by other runs")
                    } else {
                        String::new()
                    },
                    bm.generation
                );
                pending.clear();
                Ok(())
            };
            loop {
                let got = rx.recv_timeout(TICK);
                finished = finished.saturating_add(usize::from(got.is_ok()));
                match got {
                    Ok((claim, Ok(Done::Copied(c)))) => {
                        pending.push(c);
                        claims.push(claim);
                    }
                    Ok((claim, Ok(Done::Skipped))) => lock(&shared.released).push(claim),
                    Ok((claim, Err(e))) => {
                        shared.stop.store(true, Ordering::Relaxed);
                        // Given up at once: no other run waits for the claim of a
                        // copy that failed to lapse.
                        lock(&shared.released).push(claim);
                        shared.tell();
                        failure.get_or_insert(e);
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                // The run has copied what it could claim, and waits for other runs'
                // blobs: they may be waiting for these.
                let idle = shared.waiting.load(Ordering::Relaxed)
                    && shared.taken.load(Ordering::Relaxed) == finished;
                if !pending.is_empty()
                    && (idle || last.elapsed() >= c.publish_every || pending.len() >= PUBLISH_ITEMS)
                {
                    if let Err(e) = flush(&mut pending, &mut claims) {
                        shared.stop.store(true, Ordering::Relaxed);
                        return Err(e);
                    }
                    last = Instant::now();
                }
            }
            // What finished is published even when a copy failed: a rerun skips it.
            if !pending.is_empty() {
                flush(&mut pending, &mut claims)?;
            }
            match failure {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })();
        drop(ended);
        let claimed = claimer
            .join()
            .unwrap_or_else(|_| Err(crate::err!("the thread that claims failed")));
        copied.and(claimed)
    })
}

/// Copies the blobs `need`, which a pipeline run's window references and `index` (the
/// `blobs.bin` it read) lacks, through [`copy_all`]: their sizes are asked of the upstream
/// first (so that large blobs stream alone), and each job's connection is another of
/// `up`'s. Returns the `blobs.bin` that lists every one of them (`None` if nothing was
/// needed) and their sizes, as verified by hashing their content.
///
/// The items are taken in an order of this run's own. Runs that overlap (the next
/// quarter's, started while this one still copies) plan the same items; each claims what
/// it copies and leaves to the others what they have claimed ([`copy_all`]), in whatever
/// order. The order is for runs that do not see each other's claims (a build from before
/// `claims.bin`, a clock too far off: [`crate::claims`]): in one order each takes an
/// item while the other is still copying it, to the last one, and in orders of their own
/// a run mostly comes to items the other has published by then.
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

    /// What a copy claims ahead of its jobs: nothing while each job has an item claimed
    /// and not begun; else up to four items a job, 256 MiB in a claim, and 4,096 blobs
    /// claimed and not begun, but an item if none is.
    #[test]
    fn a_copy_claims_a_few_items_ahead_of_its_jobs() {
        let large = |mib: u64| Item::Large {
            oid: Oid::ZERO,
            size: mib << 20,
        };
        let pack = |blobs: usize| Item::Batch {
            oids: vec![Oid::ZERO; blobs],
            bytes: 64 << 20,
        };
        let ahead =
            |items: &[Item], jobs, ready, blobs| claim_ahead(items.iter(), jobs, ready, blobs);
        // Blobs of 16 MiB.
        let items = vec![large(16); 100];
        assert_eq!(ahead(&items, 1, 0, 0), 4);
        assert_eq!(ahead(&items, 4, 0, 0), 16);
        assert_eq!(ahead(&items, 4, 3, 3), 13);
        assert_eq!(ahead(&items, 4, 4, 4), 0);
        assert_eq!(ahead(&items, 16, 0, 0), 16);
        assert_eq!(ahead(&items[..5], 4, 0, 0), 5);
        assert_eq!(ahead(&[], 4, 0, 0), 0);
        // Blobs of 1 GiB: one a claim, as long as a job would find none.
        let items = vec![large(1024); 10];
        assert_eq!(ahead(&items, 4, 0, 0), 1);
        assert_eq!(ahead(&items, 4, 3, 3), 1);
        assert_eq!(ahead(&items, 4, 4, 4), 0);
        // Packs of 5,000 small blobs: one ahead. Of 1,000: four.
        let items = vec![pack(5000); 10];
        assert_eq!(ahead(&items, 4, 0, 0), 1);
        assert_eq!(ahead(&items, 4, 1, 5000), 0);
        let items = vec![pack(1000); 10];
        assert_eq!(ahead(&items, 4, 0, 0), 4);
        assert_eq!(ahead(&items, 4, 3, 3000), 1);
        assert_eq!(ahead(&items, 4, 3, 4000), 0);
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
