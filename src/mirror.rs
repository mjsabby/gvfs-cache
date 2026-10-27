//! `gvfs-daemon mirror`: copies every blob of the history into a `--blobs separate` store,
//! resumably. Edges serve the store once this has finished.
//!
//! The work is planned from the store's own metadata, never from the upstream: blob ids
//! come from every tree in the slabs ([`crate::separate::history_blobs`]: the current tip's
//! tree first, in path order, so a checkout reads its blobs back to back; then the rest of
//! the history as its trees are stored), sizes from the slabs' sizes tables. Blobs
//! `blobs.bin` already lists (the pipeline's, an earlier run's) are skipped, so a restart
//! resumes where the published packs end; before each pack, blobs that pipeline runs copied
//! meanwhile (an old version a new commit reuses) are left out, so nothing is copied twice.
//!
//! Every blob is verified by its id as it streams in, and its size against the sizes table
//! clients were given. `--jobs` copies run in parallel, one upstream connection each.
//! Finished packs are published every `--publish-secs` (one segment per batch,
//! compare-and-swap of `blobs.bin`), so a crash loses at most that much work.
//! `--rate-mbps` caps the combined download rate; `--max-items` stops after that many items
//! (seeding in slices). The run that copies the last item checks that every blob of the
//! store's trees is listed, then records `blobs.bin` as mirrored.
//!
//! It is also the repair, run on demand (nothing runs it automatically): it first takes out
//! of `blobs.bin` the packs and segments the store no longer holds
//! ([`crate::separate::drop_lost`]), so their blobs are planned again, and on a store
//! missing nothing it copies nothing. Edges count what they had to proxy for a complete
//! store (`gvfs_blobs_missing_total`): the signal to run it.
//!
//! The pipeline copies each later window's new blobs before publishing its trees, so from
//! then on every blob of every tree in the store is in it: a window's pack leaves out only
//! what its haves reach, and the blobs of those trees are in the store already.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, mpsc};
use std::time::{Duration, Instant};

use crate::blobcopy::{Copied, Item, copy_item_with, plan, publish, update};
use crate::blobs::BlobManifest;
use crate::blobstream::Throttle;
use crate::manifest::{MANIFEST_KEY, Manifest};
use crate::separate::{BlobIndex, KnownSizes, history_blobs, known_sizes};
use crate::slab::SlabMeta;
use crate::store::Store;
use crate::upstream::{Auth, Upstream};
use crate::util::{Context, Oid, Result, human_bytes};

/// A batch is published once it holds this many packs, whatever the time.
const PUBLISH_ITEMS: usize = 256;

pub struct Options {
    pub store: Store,
    pub upstream: String,
    pub branch: String,
    pub auth: Auth,
    pub jobs: usize,
    pub item_bytes: u64,
    pub work_dir: PathBuf,
    pub threads: usize,
    /// Combined download cap (bytes per second).
    pub rate: Option<u64>,
    pub max_items: Option<usize>,
    pub publish_every: Duration,
}

/// `gvfs-daemon mirror ...`
pub fn main(args: &[String]) -> Result<()> {
    let f = crate::cli::Flags::parse(
        args,
        &[
            "store",
            "store-auth-env",
            "upstream",
            "branch",
            "token-env",
            "token-user",
            "auth-header-env",
            "jobs",
            "blob-pack-mib",
            "work-dir",
            "threads",
            "rate-mbps",
            "max-items",
            "publish-secs",
        ],
    )?;
    let o = Options {
        store: Store::open(
            f.require("store")?,
            f.get("store-auth-env")
                .unwrap_or("AZURE_STORAGE_CONNECTION_STRING"),
        )?,
        upstream: f.require("upstream")?.to_string(),
        branch: f.get("branch").unwrap_or("main").to_string(),
        auth: crate::ingest::auth_from_flags(&f)?,
        jobs: f.num("jobs", 4usize)?.max(1),
        item_bytes: f
            .num("blob-pack-mib", crate::separate::ITEM_BYTES >> 20)?
            .max(1u64)
            .saturating_mul(1 << 20),
        work_dir: f
            .get("work-dir")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir),
        threads: f.num("threads", crate::util::default_threads())?,
        rate: match f.get("rate-mbps") {
            None => None,
            Some(_) => Some(
                f.num("rate-mbps", 0u64)?
                    .max(1)
                    .saturating_mul(1_000_000 / 8),
            ),
        },
        max_items: match f.get("max-items") {
            None => None,
            Some(_) => Some(f.num("max-items", 0usize)?),
        },
        publish_every: Duration::from_secs(f.num("publish-secs", 60u64)?),
    };
    let bm = run(&o)?;
    crate::log!(
        "blobs.bin generation {}: {} segments, {} packs, {} blobs; {}",
        bm.generation,
        bm.segments.len(),
        bm.packs.len(),
        bm.blobs(),
        if bm.complete() {
            "complete"
        } else {
            "not complete (run again)"
        }
    );
    crate::log!(
        "peak RSS {}",
        crate::util::human_bytes(crate::util::peak_rss())
    );
    Ok(())
}

/// Runs the copy; returns the final `blobs.bin`.
pub fn run(o: &Options) -> Result<BlobManifest> {
    let (bytes, _) = o
        .store
        .get(MANIFEST_KEY)?
        .ctx("the store has no manifest: run the pipeline's genesis (--blobs separate) first")?;
    let m = Manifest::decode(&bytes).ctx("reading manifest.bin")?;
    if !m.separate_blobs() {
        crate::bail!("this store keeps blobs inline (chosen at genesis): nothing to mirror");
    }
    let work = crate::fsutil::TempDir::new(&o.work_dir, "mirror")?;
    let cache = work.path.join("store-cache");
    std::fs::create_dir_all(&cache)?;
    // What storage lost leaves blobs.bin first (listed after blobs.bin is read: everything
    // it lists had been written by then), so its blobs are planned again.
    let (bm, _) = crate::blobcopy::load(&o.store)?;
    let lost = crate::separate::lost(&bm, &o.store.list("blobs/")?);
    crate::separate::drop_lost(&o.store, &cache, o.threads, &lost)?;
    let (bm, _) = crate::blobcopy::load(&o.store)?;
    let genesis = bm.genesis;
    if genesis.is_zero() {
        crate::bail!("blobs.bin records no genesis");
    }
    let t0 = Instant::now();
    let metas: Vec<SlabMeta> = std::iter::once(&m.baseline)
        .chain(&m.pyramid)
        .cloned()
        .collect();
    let slabs = crate::pipeline::open_slabs(&o.store, &cache, &metas)?;
    let targets = history_blobs(&slabs, Some(&m.tip), o.threads)?;
    drop(slabs);
    let known = known_sizes(&o.store, &cache, &m)?;
    let index = BlobIndex::load(&o.store, &cache, &bm)?;
    let mut wants = Vec::new();
    let (mut present, mut present_bytes, mut want_bytes) = (0usize, 0u64, 0u64);
    for oid in &targets {
        let size = known
            .get(oid)
            .with_ctx(|| format!("no sizes table has blob {oid}"))?;
        if index.find(oid).is_some() {
            present = present.saturating_add(1);
            present_bytes = present_bytes.saturating_add(size);
        } else {
            wants.push((*oid, size));
            want_bytes = want_bytes.saturating_add(size);
        }
    }
    let mut items = plan(&wants, o.item_bytes);
    drop(wants);
    let planned = items.len();
    if let Some(n) = o.max_items {
        items.truncate(n);
    }
    crate::log!(
        "mirror: {} blobs in the history ({}); {} ({}) already in the store; {} ({}) to copy \
         in {} items ({} large), {} this run; planned in {:.1}s",
        targets.len(),
        human_bytes(present_bytes.saturating_add(want_bytes)),
        present,
        human_bytes(present_bytes),
        targets.len().saturating_sub(present),
        human_bytes(want_bytes),
        planned,
        items
            .iter()
            .filter(|i| matches!(i, Item::Large { .. }))
            .count(),
        items.len(),
        t0.elapsed().as_secs_f64()
    );
    copy_all(o, &work.path, &cache, &items, &known, index)?;
    if items.len() < planned {
        crate::log!(
            "stopped after {} of {planned} items (--max-items); run again to continue",
            items.len()
        );
        return Ok(crate::blobcopy::load(&o.store)?.0);
    }

    // Every blob of the store's trees must now be listed before the history counts as
    // mirrored (trees published since the plan are their pipeline runs' to cover).
    let (now_bm, _) = crate::blobcopy::load(&o.store)?;
    let index = BlobIndex::load(&o.store, &cache, &now_bm)?;
    let missing = targets.iter().filter(|b| index.find(b).is_none()).count();
    if missing > 0 {
        crate::bail!("{missing} blobs of the history are still missing after the copy");
    }
    if now_bm.complete() {
        return Ok(now_bm);
    }
    update(&o.store, |bm| {
        if bm.genesis != genesis {
            crate::bail!("the store was seeded again (new genesis) during the mirror");
        }
        bm.mirrored = true;
        Ok(())
    })
}

trait WithCtx<T> {
    fn with_ctx(self, f: impl FnOnce() -> String) -> Result<T>;
}

impl<T> WithCtx<T> for Option<T> {
    fn with_ctx(self, f: impl FnOnce() -> String) -> Result<T> {
        self.ok_or_else(|| crate::util::Error::new(f()))
    }
}

/// `item` without the blobs `index` lists now; `None` if it lists them all.
fn unlisted<'a>(
    item: &'a Item,
    index: &BlobIndex,
    known: &KnownSizes,
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
        bytes = bytes.saturating_add(known.get(o).with_ctx(|| format!("no size for blob {o}"))?);
    }
    Ok(Some(Cow::Owned(Item::Batch { oids: left, bytes })))
}

/// Every copied blob has the size the sizes tables (what clients were told) give it.
fn check_sizes(c: &Copied, known: &KnownSizes) -> Result<()> {
    for r in &c.recs {
        match known.get(&r.oid) {
            Some(s) if s == r.size => {}
            Some(s) => crate::bail!(
                "blob {}: the sizes table says {s} bytes, its content has {}",
                r.oid,
                r.size
            ),
            None => crate::bail!("blob {} has no size in any sizes table", r.oid),
        }
    }
    Ok(())
}

/// What a worker did with an item.
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

/// Copies `items` with `--jobs` workers, publishing finished packs as it goes. Each item is
/// first checked against the newest index this run has seen (`cache` holds its segments).
fn copy_all(
    o: &Options,
    work: &Path,
    cache: &Path,
    items: &[Item],
    known: &KnownSizes,
    index: BlobIndex,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let throttle = o.rate.map(|r| Arc::new(Throttle::new(r)));
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let listed = RwLock::new(Arc::new(index));
    let total_bytes: u64 = items.iter().map(Item::bytes).sum();
    let (tx, rx) = mpsc::channel::<Result<Done>>();
    std::thread::scope(|sc| -> Result<()> {
        for w in 0..o.jobs.min(items.len()) {
            let tx = tx.clone();
            let (next, stop, throttle, listed) = (&next, &stop, &throttle, &listed);
            let spool = work.join(format!("spool-{w}"));
            sc.spawn(move || {
                let mut up = match Upstream::open(&o.upstream, &o.branch, &o.auth) {
                    Ok(u) => u,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                };
                while !stop.load(Ordering::Relaxed) {
                    let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) else {
                        break;
                    };
                    let r = (|| -> Result<Done> {
                        let index: Arc<BlobIndex> = read(listed);
                        let Some(item) = unlisted(item, &index, known)? else {
                            return Ok(Done::Skipped);
                        };
                        let c =
                            copy_item_with(&mut up, &o.store, &spool, &item, throttle.as_ref())?;
                        check_sizes(&c, known)?;
                        Ok(Done::Copied(c))
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
            let bm = publish(&o.store, work, pending, o.threads, |_| Ok(()))?;
            done_items = done_items.saturating_add(pending.len());
            done_bytes = done_bytes.saturating_add(
                pending
                    .iter()
                    .flat_map(|c| c.recs.iter().map(|r| r.size))
                    .sum::<u64>(),
            );
            let secs = t0.elapsed().as_secs_f64().max(0.001);
            crate::log!(
                "mirror: {done_items}/{} items, {} of {} ({:.1} MB/s){}; blobs.bin generation {}",
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
            // What pipeline runs published meanwhile is left out of the next items.
            match BlobIndex::load(&o.store, cache, &bm) {
                Ok(i) => {
                    write(&listed, Arc::new(i));
                    crate::separate::prune_segments(cache, &bm);
                }
                Err(e) => crate::log!("mirror: reloading the blob index failed ({e})"),
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
                && (last.elapsed() >= o.publish_every || pending.len() >= PUBLISH_ITEMS)
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
