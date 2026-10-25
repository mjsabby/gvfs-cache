//! `gvfs-daemon pipeline`: the scheduled job (ADO pipeline / GitHub Action, every 15 min).
//! Runners are stateless: everything a run needs comes from the store.
//!
//! The upstream is asked for every commit exactly once: by the one-time seed, then by the
//! 15-minute increment that first sees it. What it sends is raw material. Every slab that
//! is published comes out of [`crate::repack`], and every rollup is built from slabs
//! already in the store.
//!
//! One run is one compare-and-swap of `manifest.bin`:
//!
//! 1. **Genesis** (no manifest): `ingest_git_delta(none, HEAD)`, repacked with its deltas
//!    re-pointed as OFS, becomes the first baseline, published with `If-None-Match: *`
//!    (two racing genesis jobs cannot both win).
//! 2. **15m**: `ingest_git_delta(manifest tips, HEAD)`, repacked with its trees
//!    re-deltified by directory, becomes a 15m slab. A crashed or skipped run just makes
//!    the next one's range wider.
//! 3. **Rollup** (`--tier 1h|6h|12h|24h`): for each tier up to the requested one, the run
//!    of lower-tier slabs at the end of the pyramid is repacked into one slab of that tier
//!    (so `24h` first makes the last hour's 1h slab, then the 6h, 12h and daily ones). The
//!    slabs a rollup replaces stay listed as `retained` (15m for a day, 1h/6h/12h for a
//!    week, dailies for the warm window), so a warm client is answered from exactly where
//!    its last pack ended.
//! 4. **Weekly** (`--tier 168h`): the pyramid is repacked into the week's delta (kept for
//!    warm clients; the pyramid's slabs are retained). The new baseline is the old one byte
//!    for byte plus the delta's objects, with each directory's newest version stored as a
//!    delta against the old baseline's version.
//! 5. Publish with `If-Match` (lost race: start over), then delete unreferenced slab files
//!    older than the grace period (a crashed run's uploads; slabs no longer listed).
//!
//! Timestamps are derived from commits ([`crate::timeline`]): a 15m slab ends at the
//! stamp of the tip it was fetched up to, which is exactly the stamp an edge gave the same
//! tip in its L0 slab.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::ingest::{Dest, Indexer, ingest_git_delta};
use crate::manifest::{MANIFEST_KEY, Manifest};
use crate::repack::{Input, Mode, Repacker};
use crate::slab::{Slab, SlabMeta, Tier};
use crate::splice::Resolver;
use crate::store::{Cond, Store};
use crate::upstream::Upstream;
use crate::util::{Context, Oid, Result};

const DAY: u64 = 86_400;

pub struct Options {
    pub store: Store,
    pub upstream: Upstream,
    pub tier: Tier,
    pub work_dir: PathBuf,
    /// The clock (seconds), for garbage collection.
    pub now: u64,
    pub keep_weeks: usize,
    pub gc_grace: u64,
    pub threads: usize,
}

/// How long a slab a rollup replaced stays listed for warm clients, by its tier.
fn retention(tier: Tier, keep_weeks: usize) -> u64 {
    match tier {
        Tier::L0 | Tier::M15 => DAY,
        Tier::H1 | Tier::H6 | Tier::H12 => 7 * DAY,
        _ => (keep_weeks.max(1) as u64).saturating_mul(7 * DAY),
    }
}

/// `gvfs-daemon pipeline ...`
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
            "tier",
            "work-dir",
            "now",
            "keep-weeks",
            "gc-grace-secs",
            "threads",
        ],
    )?;
    let tier = Tier::parse(f.require("tier")?)
        .filter(|t| (Tier::M15..=Tier::Week).contains(t))
        .ctx("--tier must be 15m, 1h, 6h, 12h, 24h or 168h")?;
    let mut opts = Options {
        store: Store::open(
            f.require("store")?,
            f.get("store-auth-env")
                .unwrap_or("AZURE_STORAGE_CONNECTION_STRING"),
        )?,
        upstream: Upstream::open(
            f.require("upstream")?,
            f.get("branch").unwrap_or("main"),
            &crate::ingest::auth_from_flags(&f)?,
        )?,
        tier,
        work_dir: f
            .get("work-dir")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir),
        now: f.num("now", crate::util::now_secs())?,
        keep_weeks: f.num("keep-weeks", 8usize)?,
        gc_grace: f.num("gc-grace-secs", 7200u64)?,
        threads: f.num("threads", crate::util::default_threads())?,
    };
    let m = run(&mut opts)?;
    crate::log!("published manifest:\n{}", m.describe());
    Ok(())
}

/// The per-run engines: the pack indexer, the repacker, and a resolver for commits.
struct Engines {
    indexer: Indexer,
    repacker: Repacker,
    resolver: Resolver,
}

/// Runs one pipeline step, retrying from scratch when another writer wins the CAS.
pub fn run(o: &mut Options) -> Result<Manifest> {
    let mut e = Engines {
        indexer: Indexer::new(o.threads),
        repacker: Repacker::new(),
        resolver: Resolver::default(),
    };
    let work = crate::fsutil::TempDir::new(&o.work_dir, "pipeline")?;
    let mut attempt = 0u32;
    loop {
        let head = o.upstream.head().ctx("reading the upstream branch")?;
        match step(o, &mut e, &work.path, head) {
            Err(err) if err.is_conflict() && attempt < 3 => {
                crate::log!("manifest changed concurrently ({err}); starting over");
                attempt = attempt.saturating_add(1);
            }
            other => return other,
        }
    }
}

fn step(o: &mut Options, e: &mut Engines, work: &Path, head: Oid) -> Result<Manifest> {
    let Some((bytes, etag)) = o.store.get(MANIFEST_KEY)? else {
        return genesis(o, e, work, head);
    };
    let mut m = Manifest::decode(&bytes).ctx("reading manifest.bin")?;

    if head != m.tip {
        let haves = m.known_tips();
        match ingest_git_delta(
            &mut o.upstream,
            &mut e.indexer,
            &haves,
            head,
            Tier::M15,
            (m.max_ts, m.max_ts),
            Dest::Dir(&work.join("upstream")),
        )? {
            Some(raw) => {
                let mut known = crate::timeline::boundaries(&m);
                let ts =
                    crate::timeline::stamp(&raw.slab, &mut e.resolver, head, &mut known, m.max_ts)?;
                let range = (m.max_ts, ts);
                let s = repack(
                    e,
                    &[raw.slab],
                    None,
                    Mode::Trees,
                    Tier::M15,
                    range,
                    (raw.meta.have, raw.meta.want),
                    work,
                )?
                .ctx("empty 15m slab")?;
                upload(&o.store, work, &s)?;
                crate::log!(
                    "sealed 15m slab {} ({} objects, {}..{})",
                    s.id,
                    s.objects,
                    m.tip,
                    head
                );
                m.pyramid.push(s);
                m.max_ts = ts;
            }
            None => crate::log!("upstream moved to {head} without new objects"),
        }
        m.tip = head;
    } else {
        crate::log!("no new commits since {}", m.tip);
    }

    // Rollups cascade (1h, then 6h, ...): every hour gets a 1h slab, every six hours a 6h
    // slab, and so on, even when a higher tier is due, so warm clients find a slab that
    // starts at their boundary at every level.
    for tier in [Tier::H1, Tier::H6, Tier::H12, Tier::H24] {
        if tier <= o.tier {
            rollup(o, e, work, &mut m, tier)?;
        }
    }
    if o.tier == Tier::Week {
        weekly(o, e, work, &mut m)?;
    }
    let (max_ts, keep_weeks) = (m.max_ts, o.keep_weeks);
    m.retained
        .retain(|s| s.end_ts.saturating_add(retention(s.tier, keep_weeks)) >= max_ts);

    m.generation = m.generation.saturating_add(1);
    o.store
        .put(MANIFEST_KEY, &m.encode(), Cond::IfMatch(etag))?;
    gc(o, &m)?;
    Ok(m)
}

/// The one-time seed: the upstream's full pack, deltas re-pointed as OFS.
fn genesis(o: &mut Options, e: &mut Engines, work: &Path, head: Oid) -> Result<Manifest> {
    crate::log!("no manifest in {}: genesis from {head}", o.store.describe());
    let raw_dir = work.join("upstream");
    let raw = ingest_git_delta(
        &mut o.upstream,
        &mut e.indexer,
        &[],
        head,
        Tier::Base,
        (0, 0),
        Dest::Dir(&raw_dir),
    )?
    .ctx("the upstream branch has no objects")?;
    let ts = crate::timeline::seed_ts(&raw.slab, &mut e.resolver, head)?;
    let s = repack(
        e,
        &[raw.slab],
        None,
        Mode::Copy,
        Tier::Base,
        (0, ts),
        (Oid::ZERO, head),
        work,
    )?
    .ctx("the upstream branch has no objects")?;
    let _ = std::fs::remove_dir_all(&raw_dir);
    upload(&o.store, work, &s)?;
    let m = Manifest {
        generation: 1,
        tip: head,
        max_ts: ts,
        baseline: s,
        weekly: Vec::new(),
        pyramid: Vec::new(),
        retained: Vec::new(),
    };
    o.store.put(MANIFEST_KEY, &m.encode(), Cond::IfNoneMatch)?;
    Ok(m)
}

/// Repacks `slabs` (onto `frozen`) into a slab in `work`.
#[allow(clippy::too_many_arguments)]
fn repack(
    e: &mut Engines,
    slabs: &[Arc<Slab>],
    frozen: Option<&Arc<Slab>>,
    mode: Mode,
    tier: Tier,
    range: (u64, u64),
    commits: (Oid, Oid),
    work: &Path,
) -> Result<Option<SlabMeta>> {
    let input = Input {
        slabs,
        frozen,
        exclude: &[],
        mode,
    };
    Ok(e.repacker
        .repack(
            &mut e.indexer,
            &input,
            tier,
            range,
            commits,
            Dest::Dir(work),
        )?
        .map(|s| s.meta))
}

/// Uploads a slab built in `work` (pack first: an index implies a complete pack).
fn upload(store: &Store, work: &Path, meta: &SlabMeta) -> Result<()> {
    store.upload_file(&meta.pack_key(), &work.join(format!("{}.pack", meta.id)))?;
    store.upload_file(&meta.index_key(), &work.join(format!("{}.mphf", meta.id)))
}

/// Folds the run of slabs below `tier` at the end of the pyramid into one `tier` slab.
fn rollup(
    o: &mut Options,
    e: &mut Engines,
    work: &Path,
    m: &mut Manifest,
    tier: Tier,
) -> Result<()> {
    let start = m
        .pyramid
        .iter()
        .rposition(|s| s.tier >= tier)
        .map_or(0, |i| i.saturating_add(1));
    let run: Vec<SlabMeta> = m.pyramid.get(start..).unwrap_or(&[]).to_vec();
    let merged = match run.as_slice() {
        [] => return Ok(()),
        // One slab already is the rollup: retag it.
        [only] => SlabMeta {
            tier,
            ..only.clone()
        },
        _ => {
            let merged = merge(o, e, work, &run, tier)?;
            m.retained.extend(run.iter().cloned());
            merged
        }
    };
    crate::log!(
        "rolled {} slabs into {} slab {}",
        run.len(),
        tier.name(),
        merged.id
    );
    m.pyramid.truncate(start);
    m.pyramid.push(merged);
    Ok(())
}

/// The weekly rebuild: the pyramid becomes the week's delta, and the baseline grows by it.
fn weekly(o: &mut Options, e: &mut Engines, work: &Path, m: &mut Manifest) -> Result<()> {
    let run = m.pyramid.clone();
    let delta = match run.as_slice() {
        [] => {
            crate::log!("weekly: nothing new since the baseline");
            return Ok(());
        }
        [only] => SlabMeta {
            tier: Tier::Week,
            ..only.clone()
        },
        _ => {
            let delta = merge(o, e, work, &run, Tier::Week)?;
            m.retained.extend(run.iter().cloned());
            delta
        }
    };
    let slabs = open_slabs(&o.store, work, &[m.baseline.clone(), delta.clone()])?;
    let (Some(old), Some(week)) = (slabs.first(), slabs.get(1)) else {
        crate::bail!("weekly: slabs missing");
    };
    match repack(
        e,
        std::slice::from_ref(week),
        Some(old),
        Mode::Trees,
        Tier::Base,
        (0, m.max_ts),
        (Oid::ZERO, m.tip),
        work,
    )? {
        Some(baseline) => {
            upload(&o.store, work, &baseline)?;
            crate::log!(
                "weekly: baseline {} ({} objects), delta {} ({} objects)",
                baseline.id,
                baseline.objects,
                delta.id,
                delta.objects
            );
            m.baseline = baseline;
        }
        None => crate::log!(
            "weekly: the baseline already has every object of delta {}",
            delta.id
        ),
    }
    m.weekly.push(delta);
    let excess = m.weekly.len().saturating_sub(o.keep_weeks.max(1));
    m.weekly.drain(..excess);
    m.pyramid.clear();
    Ok(())
}

/// One slab covering `run` (consecutive, oldest first), repacked from the store.
fn merge(
    o: &mut Options,
    e: &mut Engines,
    work: &Path,
    run: &[SlabMeta],
    tier: Tier,
) -> Result<SlabMeta> {
    let (Some(first), Some(last)) = (run.first(), run.last()) else {
        crate::bail!("empty rollup");
    };
    let slabs = open_slabs(&o.store, work, run)?;
    let s = repack(
        e,
        &slabs,
        None,
        Mode::Trees,
        tier,
        (first.start_ts, last.end_ts),
        (first.have, last.want),
        work,
    )?
    .ctx("rollup of empty slabs")?;
    upload(&o.store, work, &s)?;
    Ok(s)
}

/// Opens slabs from the store (in place for directory stores, else downloaded to `work`).
pub fn open_slabs(store: &Store, work: &Path, metas: &[SlabMeta]) -> Result<Vec<Arc<Slab>>> {
    let mut out = Vec::with_capacity(metas.len());
    for meta in metas {
        let (pack, index) = match (
            store.local_path(&meta.pack_key()),
            store.local_path(&meta.index_key()),
        ) {
            (Some(p), Some(i)) => (p, i),
            _ => {
                let p = work.join(format!("{}.pack", meta.id));
                let i = work.join(format!("{}.mphf", meta.id));
                if !p.exists() {
                    store.download_file(&meta.pack_key(), &p)?;
                }
                if !i.exists() {
                    store.download_file(&meta.index_key(), &i)?;
                }
                (p, i)
            }
        };
        out.push(Slab::open(&pack, &index, meta.clone())?);
    }
    Ok(out)
}

/// Deletes slab files no manifest references any more, once older than the grace period.
fn gc(o: &Options, m: &Manifest) -> Result<()> {
    let referenced: std::collections::HashSet<String> = m
        .slabs()
        .flat_map(|s| [s.pack_key(), s.index_key()])
        .collect();
    let cutoff = o.now.saturating_sub(o.gc_grace);
    let mut deleted = 0usize;
    for obj in o.store.list("slabs/")? {
        if !referenced.contains(&obj.key) && obj.modified < cutoff {
            o.store.delete(&obj.key)?;
            deleted = deleted.saturating_add(1);
        }
    }
    if deleted > 0 {
        crate::log!("gc: deleted {deleted} unreferenced slab files");
    }
    Ok(())
}
