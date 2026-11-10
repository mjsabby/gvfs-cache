//! `gvfs-cache pipeline`: the scheduled job (ADO pipeline / GitHub Action, every 15 min).
//! Runners are stateless: everything a run needs comes from the store.
//!
//! The upstream is asked for every commit exactly once: by the one-time seed, then by the
//! 15-minute increment that first sees it. What it sends is raw material. Every slab that
//! is published comes out of [`crate::repack`], and every rollup is built from slabs
//! already in the store.
//!
//! One run is at most one compare-and-swap of `manifest.bin`:
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
//!    delta against the old baseline's version. It waits for its week ([`weekly_wait`]),
//!    and publishes the new baseline's tail on the old one beside it ([`crate::tail`]).
//! 5. Publish with `If-Match` (lost race: start over), then delete unreferenced slab files
//!    older than the grace period (a crashed run's uploads; slabs no longer listed). A run
//!    that changed nothing (no new commits, no rollup due) publishes nothing: the manifest
//!    keeps its generation, and every edge its view. It still collects garbage. A failure
//!    of that collection is logged, not returned: the run has published by then, and a
//!    scheduler that runs a failed run again would repeat it.
//!
//! Timestamps are derived from commits ([`crate::timeline`]): a 15m slab ends at the
//! stamp of the tip it was fetched up to, which is exactly the stamp an edge gave the same
//! tip in its L0 slab.
//!
//! **Stores with `--blobs separate`** (chosen at genesis, recorded in the manifest) keep
//! commits and trees in the slabs and every blob, real content, in blob packs listed in
//! `blobs.bin` ([`crate::separate`]). Fetches are `blob:none`, and every slab gets a sizes
//! table (`slabs/<id>.sizes`) for the blobs its trees introduce:
//!
//! * Genesis takes every size from the upstream and publishes the history;
//!   `gvfs-cache mirror` then copies every blob of it (edges serve once it has).
//! * A 15m run works **blobs first**: it lists the blobs its new trees reference, copies
//!   the ones `blobs.bin` lacks (verified by id as they stream through), publishes them in
//!   `blobs.bin`, and only then publishes its slab, recording the `blobs.bin` generation
//!   it needs. A run that dies in between leaves unreferenced blobs, never trees without
//!   their blobs. Until the mirror has finished, old versions are the mirror's to copy,
//!   except those the new tip has, within one blob pack's bytes, so that a tip the mirror
//!   has copied stays whole.
//! * Rollups and weekly rebuilds union their parts' sizes tables.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::ingest::{Dest, Indexer, ingest_git_delta, ingest_git_delta_with};
use crate::manifest::{FLAG_SEPARATE_BLOBS, MANIFEST_KEY, Manifest};
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
    /// The clock (seconds): for garbage collection, for retention when stamps are ahead of
    /// it, and for whether a weekly rebuild is due.
    pub now: u64,
    pub keep_weeks: usize,
    /// `--weekly always`: a `--tier 168h` run rebuilds the baseline whenever it is asked
    /// to, not only when one is due ([`weekly_due`]).
    pub weekly_always: bool,
    pub gc_grace: u64,
    pub threads: usize,
    /// `--blobs separate` / `inline`: the store mode, chosen at genesis (later runs follow
    /// the manifest and only check that this agrees).
    pub blobs: Option<bool>,
    /// Bytes of small blobs per blob pack (`--blob-pack-mib`).
    pub blob_pack_bytes: u64,
    /// Downloaded copies of immutable store objects (`--blobs separate`: the sizes tables
    /// and segment indexes a window run reads), kept across runs and pruned to what the
    /// store lists. Other files in the directory are left alone.
    pub cache_dir: PathBuf,
}

/// How long after a week's end the next weekly rebuild is due: a day less than a week, so
/// that a run a little early, or after a quiet day, is not put off by a week.
const WEEKLY_DUE: u64 = 6 * DAY;

/// How long a `--tier 168h` run has to wait before it may rebuild the baseline (`None`:
/// it is due). A week is the pyramid since the last week's end, whatever it holds, and
/// each rebuild rewrites the baseline (which every edge then downloads) and drops the
/// oldest week beyond `--keep-weeks`. Asked for twice (a run retried after an error, a
/// trigger that fired twice, a job run by hand), the second rebuild made a "week" of a
/// few minutes: with `--keep-weeks 2`, two such runs left two weeks of three objects
/// each, and every client older than that was sent the cold pack. So a rebuild waits
/// until the last week's end is six days old by the run's clock. Nothing waits for the
/// first week, for an empty pyramid (the rebuild then says so and does nothing), or
/// when the stamps are ahead of the clock (a commit dated in the future), which then
/// says nothing of when a week ended.
fn weekly_wait(m: &Manifest, now: u64) -> Option<u64> {
    let last = m.weekly.last()?;
    if m.pyramid.is_empty() || last.end_ts > now {
        return None;
    }
    let wait = WEEKLY_DUE.saturating_sub(now.saturating_sub(last.end_ts));
    (wait > 0).then_some(wait)
}

/// How long a slab a rollup replaced stays listed for warm clients, by its tier.
fn retention(tier: Tier, keep_weeks: usize) -> u64 {
    match tier {
        Tier::L0 | Tier::M15 => DAY,
        Tier::H1 | Tier::H6 | Tier::H12 => 7 * DAY,
        _ => (keep_weeks.max(1) as u64).saturating_mul(7 * DAY),
    }
}

/// The most slabs of a tier that stay listed as retained: four times what [`retention`]
/// holds of them when one is made every period the tier is named for (384 15m slabs, 672
/// of an hour).
fn retained_max(tier: Tier, keep_weeks: usize) -> usize {
    let period = match tier {
        Tier::L0 | Tier::M15 => 900,
        Tier::H1 => 3600,
        Tier::H6 => 6 * 3600,
        Tier::H12 => 12 * 3600,
        Tier::H24 => DAY,
        Tier::Week | Tier::Base => 7 * DAY,
    };
    let held = retention(tier, keep_weeks).checked_div(period).unwrap_or(0);
    crate::util::us(held).max(4).saturating_mul(4)
}

/// Drops the retained slabs that have aged out; returns how many more went because their
/// tier held too many.
///
/// A slab ages by stamps: it goes once the newest stamp, `max_ts`, is more than its
/// tier's [`retention`] past its end. Stamps are commit times ([`crate::timeline`]), and
/// a commit dated in the future puts every stamp after it ahead of the clock until the
/// clock passes that date, each rising by one per commit meanwhile. With stamps alone,
/// such a commit dropped every retained slab at once (two days ahead was enough: every
/// warm client was then answered from an older boundary, with a rollup it mostly had),
/// and from then on none: "a day" was 86,400 commits, so after a commit dated ten years
/// ahead the manifest grew by 96 slabs a day, each one two more files every edge maps.
/// So an age is taken against the run's clock, `now`, when the newest stamp is ahead of
/// it, and no tier keeps more than [`retained_max`] slabs, those that end first going
/// first.
fn retain(retained: &mut Vec<SlabMeta>, max_ts: u64, now: u64, keep_weeks: usize) -> usize {
    let at = max_ts.min(now);
    retained.retain(|s| s.end_ts.saturating_add(retention(s.tier, keep_weeks)) >= at);
    let mut dropped = 0usize;
    for tier in Tier::ALL {
        let mut ends: Vec<u64> = retained
            .iter()
            .filter(|s| s.tier == tier)
            .map(|s| s.end_ts)
            .collect();
        let excess = ends.len().saturating_sub(retained_max(tier, keep_weeks));
        if excess == 0 {
            continue;
        }
        ends.sort_unstable();
        let Some(&last) = ends.get(excess.saturating_sub(1)) else {
            continue;
        };
        let mut left = excess;
        retained.retain(|s| {
            let goes = s.tier == tier && s.end_ts <= last && left > 0;
            left = left.saturating_sub(usize::from(goes));
            !goes
        });
        dropped = dropped.saturating_add(excess.saturating_sub(left));
    }
    dropped
}

/// `gvfs-cache pipeline ...`
pub fn main(args: &[String]) -> Result<()> {
    let f = crate::cli::Flags::parse(
        args,
        &[
            "store",
            "store-auth-env",
            "upstream",
            "branch",
            "auth-command",
            "token-env",
            "token-user",
            "auth-header-env",
            "tier",
            "work-dir",
            "now",
            "keep-weeks",
            "weekly",
            "gc-grace-secs",
            "threads",
            "blobs",
            "blob-pack-mib",
            "cache-dir",
        ],
    )?;
    let work_dir = f
        .get("work-dir")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
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
        work_dir: work_dir.clone(),
        now: f.num("now", crate::util::now_secs())?,
        keep_weeks: f.num("keep-weeks", 8usize)?,
        weekly_always: match f.get("weekly").unwrap_or("due") {
            "due" => false,
            "always" => true,
            other => crate::bail!("--weekly must be due or always, not {other:?}"),
        },
        gc_grace: f.num("gc-grace-secs", 7200u64)?,
        threads: f.num("threads", crate::util::default_threads())?,
        blobs: match f.get("blobs") {
            None => None,
            Some("separate") => Some(true),
            Some("inline") => Some(false),
            Some(other) => crate::bail!("--blobs {other}: expected inline or separate"),
        },
        blob_pack_bytes: f
            .num("blob-pack-mib", crate::separate::ITEM_BYTES >> 20)?
            .max(1u64)
            .saturating_mul(1 << 20),
        cache_dir: f
            .get("cache-dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| work_dir.join("gvfs-cache-store-cache")),
    };
    let (m, published) = run(&mut opts)?;
    if published {
        crate::log!("published manifest:\n{}", m.describe());
    } else {
        crate::log!(
            "nothing to publish: the manifest stays at generation {}",
            m.generation
        );
    }
    crate::log!(
        "peak RSS {}",
        crate::util::human_bytes(crate::util::peak_rss())
    );
    Ok(())
}

/// The per-run engines: the pack indexer, the repacker, and a resolver for commits.
struct Engines {
    indexer: Indexer,
    repacker: Repacker,
    resolver: Resolver,
}

/// Runs one pipeline step, retrying from scratch when another writer wins the CAS. Returns
/// the store's manifest and whether this run published it: a run that changed nothing
/// leaves the one it read.
pub fn run(o: &mut Options) -> Result<(Manifest, bool)> {
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

fn step(o: &mut Options, e: &mut Engines, work: &Path, head: Oid) -> Result<(Manifest, bool)> {
    let Some((bytes, etag)) = o.store.get(MANIFEST_KEY)? else {
        return Ok((genesis(o, e, work, head)?, true));
    };
    let mut m = Manifest::decode(&bytes).ctx("reading manifest.bin")?;
    let read = m.clone();
    if let Some(want) = o.blobs
        && want != m.separate_blobs()
    {
        crate::bail!(
            "this store keeps blobs {} (chosen at genesis); --blobs {} does not apply",
            if m.separate_blobs() {
                "separate"
            } else {
                "inline"
            },
            if want { "separate" } else { "inline" }
        );
    }

    if head != m.tip && m.separate_blobs() {
        window_separate(o, e, work, &mut m, head)?;
    } else if head != m.tip {
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
        match weekly_wait(&m, o.now).filter(|_| !o.weekly_always) {
            None => weekly(o, e, work, &mut m)?,
            Some(wait) => crate::log!(
                "weekly: the last week ended {:.1} days ago: no rebuild for {:.1} days more \
                 (--weekly always rebuilds whenever asked)",
                (WEEKLY_DUE.saturating_sub(wait)) as f64 / DAY as f64,
                wait as f64 / DAY as f64
            ),
        }
    }
    if m.separate_blobs() {
        // A rollup of slabs whose blobs a run copied needs no more than they did.
        let (bm, _) = crate::blobcopy::load(&o.store)?;
        if bm.generation < m.blobs_generation {
            crate::bail!(
                "blobs.bin is at generation {}, behind the manifest's {}",
                bm.generation,
                m.blobs_generation
            );
        }
    }
    if m.max_ts > o.now.saturating_add(DAY) {
        crate::log!(
            "warning: the branch's newest stamp is {} days ahead of the clock (a commit dated \
             in the future): until the clock passes it, slabs kept for warm clients age by \
             the clock and are bounded in number",
            m.max_ts.saturating_sub(o.now) / DAY
        );
    }
    let capped = retain(&mut m.retained, m.max_ts, o.now, o.keep_weeks);
    if capped > 0 {
        crate::log!("retention: {capped} more slabs dropped, their tiers holding too many");
    }

    // A run that changed nothing publishes nothing: a new generation has every edge build a
    // view and hash its cold pack again (1.3 s on SearchGold, its `.idx` beside it),
    // up to 96 times a day while the branch is quiet. Everything but the generation counts
    // as a change, the `blobs.bin` generation the trees need included.
    let published = m != read;
    if published {
        m.generation = m.generation.saturating_add(1);
        o.store
            .put(MANIFEST_KEY, &m.encode(), Cond::IfMatch(etag))?;
    }
    // The manifest is the store's now, so the run has succeeded, and nothing after this
    // fails it: an error exit has a scheduler run it again, and a weekly run again is a
    // second weekly. What garbage collection did not get to waits for a later run.
    if let Err(e) = crate::gc::collect(&o.store, &m, o.now, o.gc_grace) {
        crate::log!("gc: incomplete ({e}); what it did not delete stays until a later run");
    }
    if m.separate_blobs() {
        // A failure here only costs downloads next run.
        match crate::blobcopy::load(&o.store)
            .and_then(|(bm, _)| crate::separate::prune_cache(&o.cache_dir, &m, &bm))
        {
            Ok(0) => {}
            Ok(n) => crate::log!("cache: removed {n} files the store no longer lists"),
            Err(e) => crate::log!("cache: pruning {} failed: {e}", o.cache_dir.display()),
        }
    }
    Ok((m, published))
}

/// The one-time seed: the upstream's full pack, deltas re-pointed as OFS.
fn genesis(o: &mut Options, e: &mut Engines, work: &Path, head: Oid) -> Result<Manifest> {
    if o.blobs == Some(true) {
        return genesis_separate(o, e, work, head);
    }
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
        flags: 0,
        blobs_generation: 0,
    };
    o.store.put(MANIFEST_KEY, &m.encode(), Cond::IfNoneMatch)?;
    Ok(m)
}

/// Genesis of a `--blobs separate` store: the history's commits and trees (`blob:none`),
/// every size their trees need from the upstream, and `blobs.bin` naming the genesis
/// commit, not mirrored: `gvfs-cache mirror` copies the history's blobs.
fn genesis_separate(o: &mut Options, e: &mut Engines, work: &Path, head: Oid) -> Result<Manifest> {
    crate::log!(
        "no manifest in {}: genesis (blobs separate) from {head}",
        o.store.describe()
    );
    let raw_dir = work.join("upstream");
    let raw = ingest_git_delta_with(
        &mut o.upstream,
        &mut e.indexer,
        &[],
        head,
        Some("blob:none"),
        Tier::Base,
        (0, 0),
        Dest::Dir(&raw_dir),
    )?
    .ctx("the upstream branch has no objects")?;
    let ts = crate::timeline::seed_ts(&raw.slab, &mut e.resolver, head)?;
    let blobs = crate::blobcopy::referenced_blobs(&raw.slab, o.threads)?;
    let t0 = std::time::Instant::now();
    let reported = o.upstream.object_sizes(&blobs)?;
    let mut pairs = Vec::with_capacity(blobs.len());
    for (oid, size) in blobs.iter().zip(reported) {
        let Some(size) = size else {
            crate::bail!("the upstream has no blob {oid}, which a tree references");
        };
        pairs.push((*oid, size));
    }
    crate::log!(
        "sizes of {} blobs from the upstream in {:.2}s",
        pairs.len(),
        t0.elapsed().as_secs_f64()
    );
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
    crate::separate::put_sizes(&o.store, &s, &crate::sizes::build(&mut pairs))?;
    let bm = crate::blobcopy::update(&o.store, |bm| {
        if bm.genesis != head {
            // A new genesis (the store was reset): no earlier mirror covers its history.
            bm.genesis = head;
            bm.mirrored = false;
        }
        Ok(())
    })?;
    let m = Manifest {
        generation: 1,
        tip: head,
        max_ts: ts,
        baseline: s,
        weekly: Vec::new(),
        pyramid: Vec::new(),
        retained: Vec::new(),
        flags: FLAG_SEPARATE_BLOBS,
        blobs_generation: bm.generation,
    };
    o.store.put(MANIFEST_KEY, &m.encode(), Cond::IfNoneMatch)?;
    Ok(m)
}

/// A 15m run of a `--blobs separate` store, blobs first: the window's commits and trees
/// (`blob:none`); the blobs their trees reference that `blobs.bin` lacks (and, until the
/// mirror has finished, those the new tip lacks: [`tip_gaps`]), copied and published; then
/// the slab with its sizes table.
fn window_separate(
    o: &mut Options,
    e: &mut Engines,
    work: &Path,
    m: &mut Manifest,
    head: Oid,
) -> Result<()> {
    let haves = m.known_tips();
    let Some(raw) = ingest_git_delta_with(
        &mut o.upstream,
        &mut e.indexer,
        &haves,
        head,
        Some("blob:none"),
        Tier::M15,
        (m.max_ts, m.max_ts),
        Dest::Dir(&work.join("upstream")),
    )?
    else {
        crate::log!("upstream moved to {head} without new objects");
        m.tip = head;
        return Ok(());
    };
    let mut known_ts = crate::timeline::boundaries(m);
    let ts = crate::timeline::stamp(&raw.slab, &mut e.resolver, head, &mut known_ts, m.max_ts)?;

    // The trees in the window's pack: their blobs are copied (if the store lacks them) and
    // their sizes go in the slab's table. Subtrees the pack leaves out are trees the haves
    // reach, so they and their blobs are in the store already (`crate::separate`). Until the
    // mirror has finished, a blob an earlier tree names (an unchanged file of a changed
    // directory, an old version) is the mirror's to copy: copying it here too would race the
    // mirror, a whole directory of large files at a time. The new tip's are the exception,
    // within one blob pack's bytes (`tip_gaps`).
    let referenced = crate::blobcopy::referenced_blobs(&raw.slab, o.threads)?;
    let cache = o.cache_dir.clone();
    std::fs::create_dir_all(&cache).ctx(&format!("creating {}", cache.display()))?;
    let known = crate::separate::known_sizes(&o.store, &cache, m)?;
    let (bm, _) = crate::blobcopy::load(&o.store)?;
    let index = crate::separate::BlobIndex::load(&o.store, &cache, &bm)?;
    let mirrored = bm.complete();
    let mut need: Vec<Oid> = referenced
        .iter()
        .filter(|b| index.find(b).is_none() && (mirrored || known.get(b).is_none()))
        .copied()
        .collect();
    if !mirrored {
        let gaps = tip_gaps(o, m, &raw.slab, head, &index, &known, &need)?;
        need.extend(gaps);
    }
    let (published, copied) = crate::separate::copy_needed(
        &mut o.upstream,
        &o.store,
        work,
        &need,
        o.blob_pack_bytes,
        o.threads,
    )?;
    // The sizes the window introduces: every referenced blob no earlier table has.
    let mut pairs = Vec::new();
    for b in &referenced {
        if known.get(b).is_some() {
            continue;
        }
        let Some(size) = copied
            .get(b)
            .copied()
            .or_else(|| index.find(b).map(|(_, r)| r.size))
        else {
            crate::bail!("no size for blob {b}");
        };
        pairs.push((*b, size));
    }
    let s = repack(
        e,
        &[raw.slab],
        None,
        Mode::Trees,
        Tier::M15,
        (m.max_ts, ts),
        (raw.meta.have, raw.meta.want),
        work,
    )?
    .ctx("empty 15m slab")?;
    upload(&o.store, work, &s)?;
    crate::separate::put_sizes(&o.store, &s, &crate::sizes::build(&mut pairs))?;
    crate::log!(
        "sealed 15m slab {} ({} objects, {} new sizes, {} blobs copied, {}..{})",
        s.id,
        s.objects,
        pairs.len(),
        need.len(),
        m.tip,
        head
    );
    m.pyramid.push(s);
    m.max_ts = ts;
    m.tip = head;
    // The trees need every blob of the generation `need` was computed against, whether or
    // not this run copied any (a run that died after publishing `blobs.bin` left them).
    m.blobs_generation = m.blobs_generation.max(bm.generation);
    if let Some(p) = published {
        m.blobs_generation = m.blobs_generation.max(p.generation);
    }
    Ok(())
}

/// While the mirror has not finished: the blobs of the new tip's tree that the store lacks
/// and `need` does not have yet (a file back at an old version, which an earlier tree
/// names), so that a checkout of the tip is served from the store. Only if they fit in one
/// blob pack's bytes: more is a tip the mirror has not copied yet, which stays the mirror's.
fn tip_gaps(
    o: &Options,
    m: &Manifest,
    window: &Arc<Slab>,
    head: Oid,
    index: &crate::separate::BlobIndex,
    known: &crate::separate::KnownSizes,
    need: &[Oid],
) -> Result<Vec<Oid>> {
    let t0 = std::time::Instant::now();
    let metas: Vec<SlabMeta> = std::iter::once(&m.baseline)
        .chain(&m.pyramid)
        .cloned()
        .collect();
    // Downloads stay in the cache directory for the next run (it keeps listed slabs).
    let mut slabs = open_slabs(&o.store, &o.cache_dir, &metas)?;
    slabs.push(Arc::clone(window));
    let queued: std::collections::HashSet<&Oid> = need.iter().collect();
    let gaps: Vec<Oid> = crate::separate::tree_blobs(&slabs, &head)?
        .into_iter()
        .filter(|b| !queued.contains(b) && index.find(b).is_none())
        .collect();
    if gaps.is_empty() {
        return Ok(gaps);
    }
    let bytes = gaps
        .iter()
        .filter_map(|b| known.get(b))
        .fold(0u64, u64::saturating_add);
    let fits = bytes <= o.blob_pack_bytes;
    crate::log!(
        "the tip lacks {} blobs ({}) that earlier trees name: {} ({:.2}s)",
        gaps.len(),
        crate::util::human_bytes(bytes),
        if fits {
            "copying them too"
        } else {
            "more than --blob-pack-mib, left to the mirror"
        },
        t0.elapsed().as_secs_f64()
    );
    Ok(if fits { gaps } else { Vec::new() })
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
            let merged = merge(o, e, work, &run, tier, m.separate_blobs())?;
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
            let delta = merge(o, e, work, &run, Tier::Week, m.separate_blobs())?;
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
            publish_tail(&o.store, work, old, &baseline);
            if m.separate_blobs() {
                crate::separate::union_sizes(&o.store, work, &[&m.baseline, &delta], &baseline)?;
            }
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

/// Publishes, beside the new baseline `new`, its tail on the old one ([`crate::tail`]):
/// an edge that has `old` makes the new pack of the two and downloads megabytes, not the
/// baseline. A tail that cannot be published is said and is no error of the run: edges
/// then download the pack whole, as they did.
fn publish_tail(store: &Store, work: &Path, old: &Slab, new: &SlabMeta) {
    let pack = work.join(format!("{}.pack", new.id));
    let tail = work.join(format!("{}.tail", new.id));
    let published = crate::tail::write(&old.meta, old.pack(), new, &pack, &tail).and_then(|made| {
        if made {
            store.upload_file(&new.tail_key(), &tail)?;
        }
        Ok(made)
    });
    let size = std::fs::metadata(&tail).map_or(0, |m| m.len());
    match published {
        Ok(true) => crate::log!(
            "weekly: baseline {} is {} and a tail of {}",
            new.id,
            old.meta.id,
            crate::util::human_bytes(size)
        ),
        Ok(false) => crate::log!(
            "weekly: warning: baseline {} is not {} and more: no tail, edges download it whole",
            new.id,
            old.meta.id
        ),
        Err(e) => crate::log!(
            "weekly: warning: the tail of baseline {} was not published ({e}): edges \
             download it whole",
            new.id
        ),
    }
}

/// One slab covering `run` (consecutive, oldest first), repacked from the store.
fn merge(
    o: &mut Options,
    e: &mut Engines,
    work: &Path,
    run: &[SlabMeta],
    tier: Tier,
    separate: bool,
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
    if separate {
        let parts: Vec<&SlabMeta> = run.iter().collect();
        crate::separate::union_sizes(&o.store, work, &parts, &s)?;
    }
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

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    fn meta(tier: Tier, end_ts: u64) -> SlabMeta {
        SlabMeta {
            id: SlabMeta::make_id(tier, end_ts, &[0; 20]),
            tier,
            start_ts: end_ts.saturating_sub(1),
            end_ts,
            have: Oid::ZERO,
            want: Oid::ZERO,
            objects: 1,
            pack_len: 32,
            index_len: 32,
            pack_sha: [0; 20],
        }
    }

    fn ends(retained: &[SlabMeta], tier: Tier) -> Vec<u64> {
        retained
            .iter()
            .filter(|s| s.tier == tier)
            .map(|s| s.end_ts)
            .collect()
    }

    /// A weekly rebuild waits until the last week's end is six days old; not the first
    /// week's, not for an empty pyramid, not when the stamps are ahead of the clock.
    #[test]
    fn a_weekly_rebuild_waits_for_the_week() {
        let now = 1_800_000_000;
        let manifest = |weeks: &[u64], pyramid: &[u64]| Manifest {
            generation: 1,
            tip: Oid::ZERO,
            max_ts: pyramid.last().or(weeks.last()).copied().unwrap_or(100),
            baseline: meta(Tier::Base, 100),
            weekly: weeks.iter().map(|&end| meta(Tier::Week, end)).collect(),
            pyramid: pyramid.iter().map(|&end| meta(Tier::M15, end)).collect(),
            retained: Vec::new(),
            flags: 0,
            blobs_generation: 0,
        };
        assert_eq!(weekly_wait(&manifest(&[], &[now - 60]), now), None);
        let day_old = manifest(&[now - 30 * DAY, now - DAY], &[now - 60]);
        assert_eq!(weekly_wait(&day_old, now), Some(5 * DAY));
        assert_eq!(weekly_wait(&day_old, now + 5 * DAY - 1), Some(1));
        assert_eq!(weekly_wait(&day_old, now + 5 * DAY), None);
        assert_eq!(weekly_wait(&manifest(&[now - DAY], &[]), now), None);
        let ahead = now + 3650 * DAY;
        assert_eq!(weekly_wait(&manifest(&[ahead], &[ahead + 5]), now), None);
    }

    /// Retained slabs age by stamps while the stamps are not ahead of the clock.
    #[test]
    fn retained_slabs_age_by_their_stamps() {
        let now = 1_800_000_000;
        let mut retained = vec![
            meta(Tier::M15, now - DAY - 1),
            meta(Tier::M15, now - DAY),
            meta(Tier::H1, now - 7 * DAY - 1),
            meta(Tier::H1, now - 2 * DAY),
            meta(Tier::H24, now - 14 * DAY - 1),
            meta(Tier::H24, now - 8 * DAY),
        ];
        assert_eq!(retain(&mut retained, now, now + 5, 2), 0);
        assert_eq!(ends(&retained, Tier::M15), [now - DAY]);
        assert_eq!(ends(&retained, Tier::H1), [now - 2 * DAY]);
        assert_eq!(ends(&retained, Tier::H24), [now - 8 * DAY]);
        // A branch that has not moved for a month: its stamps have not, so nothing ages.
        assert_eq!(retain(&mut retained, now, now + 30 * DAY, 2), 0);
        assert_eq!(retained.len(), 3);
    }

    /// A commit dated ten years ahead puts every later stamp ahead of the clock. The slabs
    /// from before it then age by the clock (they all went at once), and the ones after
    /// it, which the stamps would keep for ten years, are bounded in number (they were
    /// not: 96 more a day).
    #[test]
    fn a_commit_dated_in_the_future_neither_empties_nor_floods_retention() {
        let now = 1_800_000_000;
        let ahead = now + 3650 * DAY;
        let mut retained: Vec<SlabMeta> = (1..=95)
            .map(|i| meta(Tier::M15, now - i * 900))
            .chain((1..=20).map(|i| meta(Tier::H1, now - i * 3600)))
            .collect();
        // The run that seals the commit: nothing goes.
        assert_eq!(retain(&mut retained, ahead, now, 2), 0);
        assert_eq!(retained.len(), 115);
        // Half a day later, half of the day's 15m slabs have aged out, by the clock.
        assert_eq!(retain(&mut retained, ahead + 50, now + DAY / 2, 2), 0);
        assert_eq!(ends(&retained, Tier::M15).len(), 48);
        assert_eq!(ends(&retained, Tier::H1).len(), 20);
        // Runs for a month: a 15m slab each, stamped a little after the date.
        let cap = retained_max(Tier::M15, 2);
        assert_eq!(cap, 384);
        let mut capped = 0;
        for run in 1..=2880u64 {
            retained.push(meta(Tier::M15, ahead + 50 + run));
            capped += retain(&mut retained, ahead + 50 + run, now + DAY + run * 900, 2);
            assert!(ends(&retained, Tier::M15).len() <= cap);
        }
        assert_eq!(capped, 2880 - cap);
        // What stays is the newest of them; the hour slabs from before went by the clock.
        let kept = ends(&retained, Tier::M15);
        assert_eq!(kept.len(), cap);
        assert_eq!(kept.first().copied(), Some(ahead + 50 + 2880 - 383));
        assert!(ends(&retained, Tier::H1).is_empty());
    }
}
