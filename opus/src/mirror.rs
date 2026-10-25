//! `gvfs-daemon mirror`: copies blobs into a `--blobs separate` store, resumably.
//!
//! The work is planned from the store's own metadata, never from the upstream: blob ids
//! come from walking trees already in the slabs (depth first, so in path order: a sparse
//! cone maps to few packs) and sizes from the slabs' sizes tables. Blobs `blobs.bin`
//! already lists (the pipeline's, an earlier run's) are skipped, so a restart resumes
//! where the published packs end, and nothing is copied twice.
//!
//! Scopes (`--scope`, comma-separated):
//! * `tip`: the genesis commit's tree. The pipeline copies every later window's blobs
//!   before publishing its trees, so this completes every commit since genesis: the run
//!   then records `tip complete` in `blobs.bin`, and edges start serving.
//! * `since:<when>`: every tree of commits since `<when>` (`30d`, `YYYY-MM-DD` or Unix
//!   seconds): old versions that checking out recent history needs.
//! * `history`: every tree.
//!
//! `--jobs` copies run in parallel, one upstream connection each. Finished packs are
//! published every `--publish-secs` (one segment per batch, compare-and-swap of
//! `blobs.bin`), so a crash loses at most that much work. `--rate-mbps` caps the combined
//! download rate; `--max-items` stops after that many items (seeding in slices).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::blobcopy::{Copied, Item, copy_item_with, mode_kind, plan, publish, update};
use crate::blobs::BlobManifest;
use crate::blobstream::Throttle;
use crate::manifest::{MANIFEST_KEY, Manifest};
use crate::object::{TreeIter, commit_time, commit_tree};
use crate::pack::{OBJ_BLOB, OBJ_COMMIT, OBJ_TREE};
use crate::separate::{BlobIndex, known_sizes};
use crate::slab::{Slab, SlabMeta};
use crate::splice::Resolver;
use crate::store::Store;
use crate::upstream::{Auth, Upstream};
use crate::util::{Context, Oid, Result, human_bytes};

/// A batch is published once it holds this many packs, whatever the time.
const PUBLISH_ITEMS: usize = 256;

/// What to copy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Scope {
    pub tip: bool,
    /// Commit time (Unix seconds) from which every tree is copied.
    pub since: Option<u64>,
    pub history: bool,
}

impl Scope {
    /// `tip`, `since:<30d|YYYY-MM-DD|seconds>`, `history`, comma-separated.
    pub fn parse(spec: &str, now: u64) -> Result<Scope> {
        let mut s = Scope::default();
        for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            match part {
                "tip" => s.tip = true,
                "history" => s.history = true,
                _ => {
                    let when = part
                        .strip_prefix("since:")
                        .with_ctx(|| format!("--scope: unknown part {part:?}"))?;
                    s.since = Some(parse_when(when, now)?);
                }
            }
        }
        if s == Scope::default() {
            crate::bail!("--scope: nothing to copy (tip, since:<when>, history)");
        }
        Ok(s)
    }
}

trait WithCtx<T> {
    fn with_ctx(self, f: impl FnOnce() -> String) -> Result<T>;
}

impl<T> WithCtx<T> for Option<T> {
    fn with_ctx(self, f: impl FnOnce() -> String) -> Result<T> {
        self.ok_or_else(|| crate::util::Error::new(f()))
    }
}

/// `30d` (before `now`), `YYYY-MM-DD` (UTC midnight) or Unix seconds.
fn parse_when(s: &str, now: u64) -> Result<u64> {
    if let Some(days) = s.strip_suffix('d') {
        let d: u64 = days
            .parse()
            .map_err(|_| crate::err!("since:{s}: bad day count"))?;
        return Ok(now.saturating_sub(d.saturating_mul(86_400)));
    }
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(secs);
    }
    let bad = || crate::err!("since:{s}: expected 30d, YYYY-MM-DD or Unix seconds");
    let mut f = s.split('-');
    let (Some(y), Some(m), Some(d), None) = (f.next(), f.next(), f.next(), f.next()) else {
        return Err(bad());
    };
    let (y, m, d): (i64, i64, i64) = (
        y.parse().map_err(|_| bad())?,
        m.parse().map_err(|_| bad())?,
        d.parse().map_err(|_| bad())?,
    );
    if !(1970..=9999).contains(&y) || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    // Days from the civil date (H. Hinnant's algorithm); the ranges above bound every term.
    let y = if m <= 2 { y.wrapping_sub(1) } else { y };
    let era = y.wrapping_div(400);
    let yoe = y.wrapping_sub(era.wrapping_mul(400));
    let mp = m.wrapping_add(9).wrapping_rem(12);
    let doy = mp
        .wrapping_mul(153)
        .wrapping_add(2)
        .wrapping_div(5)
        .wrapping_add(d)
        .wrapping_sub(1);
    let doe = yoe
        .wrapping_mul(365)
        .wrapping_add(yoe.wrapping_div(4))
        .wrapping_sub(yoe.wrapping_div(100))
        .wrapping_add(doy);
    let days = era
        .wrapping_mul(146_097)
        .wrapping_add(doe)
        .wrapping_sub(719_468);
    u64::try_from(days.wrapping_mul(86_400)).map_err(|_| bad())
}

pub struct Options {
    pub store: Store,
    pub upstream: String,
    pub branch: String,
    pub auth: Auth,
    pub scope: Scope,
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
            "scope",
            "jobs",
            "blob-pack-mib",
            "work-dir",
            "threads",
            "rate-mbps",
            "max-items",
            "publish-secs",
            "now",
        ],
    )?;
    let now = f.num("now", crate::util::now_secs())?;
    let o = Options {
        store: Store::open(
            f.require("store")?,
            f.get("store-auth-env")
                .unwrap_or("AZURE_STORAGE_CONNECTION_STRING"),
        )?,
        upstream: f.require("upstream")?.to_string(),
        branch: f.get("branch").unwrap_or("main").to_string(),
        auth: crate::ingest::auth_from_flags(&f)?,
        scope: Scope::parse(f.get("scope").unwrap_or("tip"), now)?,
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
        "blobs.bin generation {}: {} segments, {} packs, {} blobs{}",
        bm.generation,
        bm.segments.len(),
        bm.packs.len(),
        bm.blobs(),
        if bm.complete() { "; tip complete" } else { "" }
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
    let (bm, _) = crate::blobcopy::load(&o.store)?;
    let genesis = bm.genesis;
    if genesis.is_zero() {
        crate::bail!("blobs.bin records no genesis");
    }
    let work = crate::fsutil::TempDir::new(&o.work_dir, "mirror")?;
    let cache = work.path.join("store-cache");
    std::fs::create_dir_all(&cache)?;
    let t0 = Instant::now();
    let metas: Vec<SlabMeta> = std::iter::once(&m.baseline)
        .chain(&m.pyramid)
        .cloned()
        .collect();
    let slabs = crate::pipeline::open_slabs(&o.store, &cache, &metas)?;
    let targets = targets(&slabs, &o.scope, genesis)?;
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
    let planned = items.len();
    if let Some(n) = o.max_items {
        items.truncate(n);
    }
    crate::log!(
        "mirror {:?}: {} blobs ({}); {} ({}) already in the store; {} ({}) to copy in {} items \
         ({} large), {} this run; planned in {:.1}s",
        o.scope,
        targets.len(),
        human_bytes(present_bytes.saturating_add(want_bytes)),
        present,
        human_bytes(present_bytes),
        wants.len(),
        human_bytes(want_bytes),
        planned,
        items
            .iter()
            .filter(|i| matches!(i, Item::Large { .. }))
            .count(),
        items.len(),
        t0.elapsed().as_secs_f64()
    );
    copy_all(o, &work.path, &items)?;
    if items.len() < planned {
        crate::log!(
            "stopped after {} of {planned} items (--max-items); run again to continue",
            items.len()
        );
        return Ok(crate::blobcopy::load(&o.store)?.0);
    }

    // Every target must now be listed before the scope is recorded as complete.
    let (now_bm, _) = crate::blobcopy::load(&o.store)?;
    let index = BlobIndex::load(&o.store, &cache, &now_bm)?;
    let missing = targets.iter().filter(|b| index.find(b).is_none()).count();
    if missing > 0 {
        crate::bail!("{missing} blobs of the scope are still missing after the copy");
    }
    let scope = o.scope;
    update(&o.store, |bm| {
        if bm.genesis != genesis {
            crate::bail!("the store was seeded again (new genesis) during the mirror");
        }
        if scope.tip {
            bm.tip_complete = Some(genesis);
        }
        let since = if scope.history { Some(1) } else { scope.since };
        if let Some(s) = since {
            bm.since_ts = if bm.since_ts == 0 {
                s
            } else {
                bm.since_ts.min(s)
            };
        }
        Ok(())
    })
}

/// Copies `items` with `--jobs` workers, publishing finished packs as it goes.
fn copy_all(o: &Options, work: &Path, items: &[Item]) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let throttle = o.rate.map(|r| Arc::new(Throttle::new(r)));
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let total_bytes: u64 = items.iter().map(Item::bytes).sum();
    let (tx, rx) = mpsc::channel::<Result<Copied>>();
    std::thread::scope(|sc| -> Result<()> {
        for w in 0..o.jobs.min(items.len()) {
            let tx = tx.clone();
            let (next, stop, throttle) = (&next, &stop, &throttle);
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
                    let r = copy_item_with(&mut up, &o.store, &spool, item, throttle.as_ref());
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
        let (mut done_items, mut done_bytes) = (0usize, 0u64);
        let mut flush = |pending: &mut Vec<Copied>| -> Result<()> {
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
                "mirror: {done_items}/{} items, {} of {} ({:.1} MB/s); blobs.bin generation {}",
                items.len(),
                human_bytes(done_bytes),
                human_bytes(total_bytes),
                done_bytes as f64 / secs / 1e6,
                bm.generation
            );
            pending.clear();
            Ok(())
        };
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(Ok(c)) => pending.push(c),
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
                if let Err(e) = flush(&mut pending) {
                    stop.store(true, Ordering::Relaxed);
                    return Err(e);
                }
                last = Instant::now();
            }
        }
        // What finished is published even when a copy failed: a rerun skips it.
        if !pending.is_empty() {
            flush(&mut pending)?;
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })
}

/// The scope's blobs, distinct, in walk order.
fn targets(slabs: &[Arc<Slab>], scope: &Scope, genesis: Oid) -> Result<Vec<Oid>> {
    let mut w = Walker {
        slabs,
        resolver: Resolver::default(),
        seen_trees: HashSet::new(),
        seen_blobs: HashSet::new(),
        out: Vec::new(),
    };
    if scope.tip {
        let tree = w.commit(&genesis)?.1;
        w.walk(tree)?;
    }
    if scope.history || scope.since.is_some() {
        let mut commits = w.commits()?;
        commits.sort_by_key(|c| std::cmp::Reverse(c.0));
        let since = i64::try_from(scope.since.unwrap_or(0)).unwrap_or(i64::MAX);
        for (time, tree) in commits {
            if scope.history || time >= since {
                w.walk(tree)?;
            }
        }
    }
    Ok(w.out)
}

struct Walker<'a> {
    slabs: &'a [Arc<Slab>],
    resolver: Resolver,
    seen_trees: HashSet<Oid>,
    seen_blobs: HashSet<Oid>,
    out: Vec<Oid>,
}

/// An object's content from the newest slab that has it.
fn object<'r>(
    slabs: &[Arc<Slab>],
    resolver: &'r mut Resolver,
    oid: &Oid,
    kind: u8,
) -> Result<&'r [u8]> {
    let (slab, slot) = slabs
        .iter()
        .rev()
        .find_map(|s| s.find(oid).map(|slot| (s, slot)))
        .with_ctx(|| format!("object {oid} is not in the store's slabs"))?;
    let (k, content) = resolver.resolve(slab, slot)?;
    if k != kind {
        crate::bail!("object {oid} is a {}", crate::pack::type_name(k));
    }
    Ok(content)
}

impl Walker<'_> {
    /// `(committer time, tree)` of a commit.
    fn commit(&mut self, oid: &Oid) -> Result<(i64, Oid)> {
        let c = object(self.slabs, &mut self.resolver, oid, OBJ_COMMIT)?;
        Ok((
            commit_time(c).unwrap_or(0),
            commit_tree(c).with_ctx(|| format!("commit {oid} has no tree"))?,
        ))
    }

    /// Every commit in the slabs.
    fn commits(&mut self) -> Result<Vec<(i64, Oid)>> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for slab in self.slabs {
            for r in slab.records() {
                if r.kind == OBJ_COMMIT && seen.insert(r.oid) {
                    out.push(self.commit(&r.oid)?);
                }
            }
        }
        Ok(out)
    }

    /// Depth first from `root`: each tree's blobs, then its subtrees in name order.
    fn walk(&mut self, root: Oid) -> Result<()> {
        let mut stack = vec![root];
        let mut subtrees = Vec::new();
        while let Some(t) = stack.pop() {
            if !self.seen_trees.insert(t) {
                continue;
            }
            let content = object(self.slabs, &mut self.resolver, &t, OBJ_TREE)?;
            let mut it = TreeIter::new(content);
            subtrees.clear();
            let mut bad = false;
            for e in &mut it {
                match mode_kind(e.mode) {
                    Some(OBJ_BLOB) => {
                        if self.seen_blobs.insert(e.oid) {
                            self.out.push(e.oid);
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
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn scopes_parse() {
        let now = 1_700_000_000;
        assert_eq!(
            Scope::parse("tip", now).unwrap(),
            Scope {
                tip: true,
                ..Scope::default()
            }
        );
        let s = Scope::parse("tip,since:30d", now).unwrap();
        assert!(s.tip && s.since == Some(now - 30 * 86_400) && !s.history);
        assert_eq!(
            Scope::parse("since:2024-03-01", now).unwrap().since,
            Some(1_709_251_200)
        );
        assert_eq!(
            Scope::parse("since:1970-01-01", now).unwrap().since,
            Some(0)
        );
        assert_eq!(
            Scope::parse("since:2000-02-29", now).unwrap().since,
            Some(951_782_400)
        );
        assert_eq!(
            Scope::parse("since:12345", now).unwrap().since,
            Some(12_345)
        );
        assert!(Scope::parse("history", now).unwrap().history);
        for bad in ["", "tips", "since:", "since:2024-13-01", "since:x"] {
            assert!(Scope::parse(bad, now).is_err(), "{bad}");
        }
    }
}
