//! `gvfs-daemon pipeline --tier <m15|h1|h6|h12|h24|h168>`: the scheduled job.
//!
//! One run, in order:
//!
//! 1. **Genesis** (no manifest yet): `ingest_git_delta(∅, HEAD)` → the baseline slab;
//!    publish with create-only semantics.
//! 2. **m15**: if the branch moved, `ingest_git_delta(manifest.tip, HEAD)` deduplicated
//!    against every published slab → one new pyramid slab.
//! 3. **Rollups**: for every rollup tier ≤ the requested one, roll the pyramid's lower-tier
//!    slabs (always a suffix) into one slab of that tier — spliced (no upstream traffic)
//!    below `--rollup-fetch-from`, or re-fetched from the upstream as one window
//!    (`have` = tip before the window, `want` = tip at its end) at and above it, which
//!    gives git-optimal deltas (3–5× smaller than a concatenation of 15-minute slabs).
//!    Below `--leaf-tier` a rollup keeps its leaves so clients inside its window receive
//!    only what they lack.
//! 4. **h168**: roll the whole pyramid into a weekly "aha" delta (warm clients), prune
//!    weeklies past the retention window, and rebuild the baseline (fresh fetch for a
//!    defragmented, garbage-free pack, or a splice of the old baseline + the new weekly).
//! 5. Publish the manifest with compare-and-swap; on conflict the files just written stay
//!    unreferenced and the garbage sweep deletes them later.
//!
//! Every step is idempotent: a crash before step 5 leaves the manifest untouched and the
//! next run spans the whole gap.

use core::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use crate::ingest::{Destination, IngestConfig, IngestError, Ingested, SlabData, ingest_git_delta};
use crate::manifest::{Manifest, SlabRef, Tier};
use crate::mphf::Builder;
use crate::oid::Oid;
use crate::slab::{SealScratch, Slab, SlabError, SlabSet};
use crate::splice::{Plan, SlabList, SpliceError, write_merged_mphf, write_splice};
use crate::store::{FsStore, StoreError};
use crate::transport::{MAX_HAVES, Upstream};

#[derive(Debug)]
pub enum PipelineError {
    Ingest(IngestError),
    Store(StoreError),
    Slab(SlabError),
    Splice(SpliceError),
    Io(std::io::Error),
    BranchNotFound(String),
    Upstream(crate::transport::UpstreamError),
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ingest(e) => write!(f, "ingest: {e}"),
            Self::Store(e) => write!(f, "{e}"),
            Self::Slab(e) => write!(f, "{e}"),
            Self::Splice(e) => write!(f, "splice: {e}"),
            Self::Io(e) => write!(f, "i/o: {e}"),
            Self::BranchNotFound(b) => write!(f, "branch {b} not found upstream"),
            Self::Upstream(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<IngestError> for PipelineError {
    fn from(e: IngestError) -> Self {
        Self::Ingest(e)
    }
}
impl From<StoreError> for PipelineError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
impl From<SlabError> for PipelineError {
    fn from(e: SlabError) -> Self {
        Self::Slab(e)
    }
}
impl From<SpliceError> for PipelineError {
    fn from(e: SpliceError) -> Self {
        Self::Splice(e)
    }
}
impl From<std::io::Error> for PipelineError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<crate::transport::UpstreamError> for PipelineError {
    fn from(e: crate::transport::UpstreamError) -> Self {
        Self::Upstream(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaselineMode {
    /// Fetch the whole branch again: optimal layout, unreachable objects dropped.
    Fetch,
    /// Splice the previous baseline with the new weekly delta: no upstream traffic.
    Splice,
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub branch: String,
    pub baseline: BaselineMode,
    pub retain_weeks: u64,
    /// Unreferenced slab files older than this are deleted at the end of a run.
    pub gc_after_secs: u64,
    /// Rollups of this tier and above are re-fetched from the upstream as one window
    /// (splice fallback); below it they are spliced. `None` = always splice.
    pub rollup_fetch_from: Option<Tier>,
    /// Rollups below this tier keep their leaves for clients inside their window.
    /// `None` = never (pure superset rollups).
    pub leaf_tier: Option<Tier>,
    pub ingest: IngestConfig,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            branch: "refs/heads/main".into(),
            baseline: BaselineMode::Fetch,
            retain_weeks: 4,
            gc_after_secs: 7200,
            rollup_fetch_from: Some(Tier::H24),
            leaf_tier: None,
            ingest: IngestConfig::default(),
        }
    }
}

/// What a run did (for logs and tests).
#[derive(Debug, Default, Clone)]
pub struct Report {
    pub genesis: bool,
    pub head: Oid,
    pub ingested: Option<SlabRef>,
    pub ingested_stats: Option<(u64, usize, u64, u64)>,
    pub rollups: Vec<SlabRef>,
    pub weekly: Option<SlabRef>,
    pub baseline: Option<SlabRef>,
    pub gc_removed: usize,
    pub generation: u64,
}

/// Unix seconds. `GVFS_FAKE_NOW=<secs>` overrides the clock (replays and tests).
pub fn now_ts() -> u64 {
    if let Some(fake) = fake_now() {
        return fake;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `GVFS_FAKE_NOW=<unix seconds>` drives the pipeline clock in replays.
pub fn fake_now() -> Option<u64> {
    std::env::var_os("GVFS_FAKE_NOW").and_then(|v| v.to_str().and_then(|s| s.parse::<u64>().ok()))
}

/// The grace period before unreferenced files are swept. File mtimes are wall-clock, so
/// under a fake clock (a replay: no server maps the store) the sweep is immediate.
fn gc_grace(cfg: &PipelineConfig) -> u64 {
    if fake_now().is_some() {
        0
    } else {
        cfg.gc_after_secs
    }
}

/// The `have` list for an incremental fetch: the manifest tip, then the newest slab tips.
pub fn haves_for(m: &Manifest) -> Vec<Oid> {
    let mut haves = Vec::with_capacity(MAX_HAVES);
    haves.push(m.tip);
    for s in m
        .pyramid
        .iter()
        .rev()
        .chain(m.weekly.iter().rev())
        .chain(m.baseline.iter())
    {
        if haves.len() >= MAX_HAVES {
            break;
        }
        if !haves.contains(&s.tip) && !s.tip.is_zero() {
            haves.push(s.tip);
        }
    }
    haves
}

/// Runs one pipeline step for `tier`.
pub fn run_pipeline(
    store: &FsStore,
    upstream: &mut dyn Upstream,
    tier: Tier,
    cfg: &PipelineConfig,
    log: &mut dyn Write,
) -> Result<Report, PipelineError> {
    let mut report = Report::default();
    let mut scratch = SealScratch::new();
    let head = upstream
        .ls_ref(&cfg.branch)?
        .ok_or_else(|| PipelineError::BranchNotFound(cfg.branch.clone()))?;
    report.head = head;
    let now = now_ts();
    let Some(mut m) = store.load_manifest()? else {
        // Genesis: the whole branch as one baseline slab.
        let _ = writeln!(
            log,
            "genesis: no manifest; fetching the full branch at {head}"
        );
        let mut m = Manifest {
            tip: head,
            next_seq: 1,
            ..Manifest::default()
        };
        let seq = 0;
        let ing = ingest_git_delta(
            upstream,
            &[head],
            &[],
            Tier::Baseline,
            0,
            now,
            None,
            Destination::Store { store, seq },
            &cfg.ingest,
            &mut scratch,
        )?;
        if let Some(ing) = ing {
            log_ingest(log, &ing);
            report.baseline = Some(ing.meta);
            report.ingested_stats = Some((
                ing.fetched_bytes,
                ing.fetched_objects,
                ing.duplicates,
                ing.converted,
            ));
            m.baseline = Some(ing.meta);
        }
        store.store_manifest(&mut m, None)?;
        report.genesis = true;
        report.generation = m.generation;
        report.gc_removed = store.gc_unreferenced(&m, gc_grace(cfg))?;
        return Ok(report);
    };
    let expected = Some(m.generation);

    // 1. Incremental ingest.
    if head != m.tip {
        let haves = haves_for(&m);
        let dedup_set = SlabSet::open(store, m.lookup_slabs())?;
        let dedup = |oid: &Oid| dedup_set.contains(oid);
        let seq = m.allocate_seq();
        let start_ts = m.latest_ts();
        let end_ts = m.next_end_ts(now);
        let _ = writeln!(
            log,
            "ingest: {} -> {head} as slab {seq} (m15), dedup against {} slabs",
            m.tip,
            dedup_set.slabs.len()
        );
        let ing = ingest_git_delta(
            upstream,
            &[head],
            &haves,
            Tier::M15,
            start_ts,
            end_ts,
            Some(&dedup),
            Destination::Store { store, seq },
            &cfg.ingest,
            &mut scratch,
        )?;
        drop(dedup_set);
        match ing {
            Some(ing) => {
                log_ingest(log, &ing);
                report.ingested = Some(ing.meta);
                report.ingested_stats = Some((
                    ing.fetched_bytes,
                    ing.fetched_objects,
                    ing.duplicates,
                    ing.converted,
                ));
                m.push_pyramid(ing.meta, Vec::new());
            }
            None => {
                let _ = writeln!(
                    log,
                    "ingest: branch moved but every object was already published"
                );
            }
        }
        m.tip = head;
    } else {
        let _ = writeln!(log, "ingest: branch unchanged at {head}");
    }

    // 2. Rollups (each consumes the lower-tier suffix of the pyramid). The weekly run
    //    splices the whole pyramid at once instead.
    for t in Tier::ROLLUPS {
        if t > tier || tier == Tier::Weekly {
            break;
        }
        let first = m.pyramid.iter().position(|s| s.tier < t);
        let Some(first) = first else { continue };
        let members: Vec<SlabRef> = m
            .pyramid
            .get(first..)
            .map(<[SlabRef]>::to_vec)
            .unwrap_or_default();
        if members.len() < 2 || members.iter().any(|s| s.tier >= t) {
            continue;
        }
        let seq = m.allocate_seq();
        let leaves = if cfg.leaf_tier.is_some_and(|lt| t < lt) {
            m.flatten_leaves(first)
        } else {
            Vec::new()
        };
        // A rollup sealed now covers up to now: a fresh end_ts (strictly after its last
        // member) makes it reach clients that are already current, exactly once, so the
        // superset can replace their small packs.
        let end_ts = m.next_end_ts(now);
        let merged = rollup(
            store,
            upstream,
            &m,
            first,
            &members,
            seq,
            t,
            end_ts,
            cfg,
            &mut scratch,
            log,
        )?;
        m.truncate_pyramid(first);
        m.push_pyramid(merged, leaves);
        report.rollups.push(merged);
    }

    // 3. Weekly delta + baseline.
    if tier == Tier::Weekly {
        if !m.pyramid.is_empty() {
            let members = m.pyramid.clone();
            let weekly = match members.as_slice() {
                [only] => {
                    let mut only = *only;
                    only.tier = Tier::Weekly;
                    let _ = writeln!(log, "weekly: promoting slab {} to h168", only.seq);
                    only
                }
                _ => {
                    let seq = m.allocate_seq();
                    let end_ts = m.next_end_ts(now);
                    rollup(
                        store,
                        upstream,
                        &m,
                        0,
                        &members,
                        seq,
                        Tier::Weekly,
                        end_ts,
                        cfg,
                        &mut scratch,
                        log,
                    )?
                }
            };
            m.clear_pyramid();
            m.weekly.push(weekly);
            report.weekly = Some(weekly);
            let cutoff = now.saturating_sub(cfg.retain_weeks.saturating_mul(7 * 86400));
            m.weekly.retain(|w| w.end_ts >= cutoff);
            // Baseline.
            let seq = m.allocate_seq();
            let new_baseline = match (cfg.baseline, m.baseline) {
                (BaselineMode::Splice, Some(old)) => {
                    let _ = writeln!(
                        log,
                        "baseline: splicing slab {} + weekly -> slab {seq}",
                        old.seq
                    );
                    let mut b =
                        splice_slabs(store, &[old, weekly], seq, Tier::Baseline, weekly.end_ts)?;
                    b.start_ts = 0;
                    Some(b)
                }
                _ => {
                    let _ = writeln!(
                        log,
                        "baseline: fetching the full branch at {} -> slab {seq}",
                        m.tip
                    );
                    let end_ts = m.next_end_ts(now);
                    let ing = ingest_git_delta(
                        upstream,
                        &[m.tip],
                        &[],
                        Tier::Baseline,
                        0,
                        end_ts,
                        None,
                        Destination::Store { store, seq },
                        &cfg.ingest,
                        &mut scratch,
                    )?;
                    ing.map(|i| {
                        log_ingest(log, &i);
                        i.meta
                    })
                }
            };
            if let Some(b) = new_baseline {
                m.baseline = Some(b);
                report.baseline = Some(b);
            }
        } else {
            let _ = writeln!(log, "weekly: nothing new this week; baseline kept");
        }
    }

    // 4. Publish + sweep.
    store.store_manifest(&mut m, expected)?;
    report.generation = m.generation;
    report.gc_removed = store.gc_unreferenced(&m, gc_grace(cfg))?;
    let _ = writeln!(
        log,
        "published manifest generation {} (tip {}, {} pyramid, {} weekly, baseline {})",
        m.generation,
        m.tip,
        m.pyramid.len(),
        m.weekly.len(),
        m.baseline.map_or("none".to_string(), |b| b.seq.to_string())
    );
    Ok(report)
}

fn log_ingest(log: &mut dyn Write, ing: &Ingested) {
    let where_ = match &ing.data {
        SlabData::Published(p) => p.pack.display().to_string(),
        SlabData::Memory { .. } => "memory".to_string(),
    };
    let _ = writeln!(
        log,
        "  fetched {} bytes / {} objects; kept {} objects ({} duplicates dropped, {} de-deltified); {} pointer blobs, {} other blobs; pack {} bytes, idx {} bytes, mphf {} bytes -> {}",
        ing.fetched_bytes,
        ing.fetched_objects,
        ing.meta.object_count,
        ing.duplicates,
        ing.converted,
        ing.pointer_blobs,
        ing.other_blobs,
        ing.meta.pack_len,
        ing.meta.idx_len,
        ing.meta.mphf_len,
        where_
    );
}

/// Rolls `members` (the pyramid suffix from `first`) into one slab of `tier` covering
/// `[members' start, end_ts]`: re-fetched as one window when `tier >= rollup_fetch_from`,
/// spliced otherwise or when the upstream refuses.
#[allow(clippy::too_many_arguments)]
fn rollup(
    store: &FsStore,
    upstream: &mut dyn Upstream,
    m: &Manifest,
    first: usize,
    members: &[SlabRef],
    seq: u64,
    tier: Tier,
    end_ts: u64,
    cfg: &PipelineConfig,
    scratch: &mut SealScratch,
    log: &mut dyn Write,
) -> Result<SlabRef, PipelineError> {
    let (Some(last), Some(head_member)) = (members.last(), members.first()) else {
        return Err(PipelineError::Splice(SpliceError::BadIdx));
    };
    let fetch = cfg.rollup_fetch_from.is_some_and(|from| tier >= from);
    if fetch {
        // The window starts at the tip of the slab just before the members.
        let have_start = m
            .pyramid
            .get(first.wrapping_sub(1))
            .map(|s| s.tip)
            .or_else(|| m.weekly.last().map(|w| w.tip))
            .or_else(|| m.baseline.map(|b| b.tip));
        if let Some(have_start) = have_start {
            let mut haves = vec![have_start];
            for s in m
                .pyramid
                .iter()
                .take(first)
                .rev()
                .chain(m.weekly.iter().rev())
                .chain(m.baseline.iter())
            {
                if haves.len() >= MAX_HAVES {
                    break;
                }
                if !haves.contains(&s.tip) && !s.tip.is_zero() {
                    haves.push(s.tip);
                }
            }
            let outside: Vec<SlabRef> = m
                .baseline
                .iter()
                .chain(m.pyramid.iter().take(first))
                .copied()
                .collect();
            let dedup_set = SlabSet::open(store, outside.iter())?;
            let dedup = |oid: &Oid| dedup_set.contains(oid);
            let _ = writeln!(
                log,
                "rollup: re-fetching {} -> {} as slab {seq} ({tier}), replacing {} slabs",
                have_start,
                last.tip,
                members.len()
            );
            match ingest_git_delta(
                upstream,
                &[last.tip],
                &haves,
                tier,
                head_member.start_ts,
                end_ts,
                Some(&dedup),
                Destination::Store { store, seq },
                &cfg.ingest,
                scratch,
            ) {
                Ok(Some(ing)) => {
                    let before: u64 = members.iter().map(|s| s.pack_len).sum();
                    let _ = writeln!(
                        log,
                        "  {} objects, {} bytes (members: {} bytes)",
                        ing.meta.object_count, ing.meta.pack_len, before
                    );
                    return Ok(ing.meta);
                }
                Ok(None) => {
                    let _ = writeln!(
                        log,
                        "  window re-fetch returned nothing new; splicing instead"
                    );
                }
                Err(e) => {
                    let _ = writeln!(log, "  window re-fetch failed ({e}); splicing instead");
                }
            }
        }
    }
    let _ = writeln!(
        log,
        "rollup: splicing {} slabs -> slab {seq} ({tier})",
        members.len()
    );
    splice_slabs(store, members, seq, tier, end_ts)
}

/// Splices published slabs (oldest first) into a new published slab of `tier` covering
/// `[oldest member start, end_ts]`.
pub fn splice_slabs(
    store: &FsStore,
    members: &[SlabRef],
    seq: u64,
    tier: Tier,
    end_ts: u64,
) -> Result<SlabRef, PipelineError> {
    let mut list = SlabList::new();
    for r in members {
        list.push(Arc::new(Slab::open(store, *r, true)?))?;
    }
    let plan = Plan::new(list)?;
    let tmp = store.tmp_slab_paths(seq, "splice");
    let (object_count, pack_len, idx_len) = (plan.object_count, plan.pack_len, plan.idx_len);
    let mphf_len = {
        let mut pw = BufWriter::with_capacity(1 << 20, File::create(&tmp.pack)?);
        let mut iw = BufWriter::with_capacity(1 << 20, File::create(&tmp.idx)?);
        let mut mw = BufWriter::with_capacity(1 << 20, File::create(&tmp.mphf)?);
        let mut builder = Builder::new();
        let mphf_len = write_merged_mphf(&plan, &mut mw, &mut builder)?;
        write_splice(plan, &mut pw, &mut iw)?;
        pw.flush()?;
        iw.flush()?;
        mw.flush()?;
        mphf_len
    };
    let meta = SlabRef {
        seq,
        tier,
        start_ts: members.iter().map(|s| s.start_ts).min().unwrap_or(0),
        end_ts: members
            .iter()
            .map(|s| s.end_ts)
            .max()
            .unwrap_or(0)
            .max(end_ts),
        tip: members.last().map(|s| s.tip).unwrap_or(Oid::ZERO),
        object_count,
        pack_len,
        idx_len,
        mphf_len,
    };
    store.publish_slab(seq, &tmp)?;
    Ok(meta)
}
