//! The background thread of a `serve` process: re-reads the manifest, keeps the live
//! (<15 min) slab current by running `ingest_git_delta(tip, HEAD)` against the upstream
//! every few seconds and splicing the result into the previous live slab, and resolves
//! misses — objects clients asked for that no slab holds (another branch, a commit the
//! pipeline has not seen yet) — by fetching them into the same live slab.

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::ingest::{Destination, IngestConfig, Ingested, SlabData, ingest_git_delta};
use crate::manifest::{SlabRef, Tier};
use crate::mphf::Builder;
use crate::oid::Oid;
use crate::pipeline::{haves_for, now_ts};
use crate::slab::{SealScratch, Slab};
use crate::splice::{Plan, SlabList, write_merged_mphf, write_splice};
use crate::store::FsStore;
use crate::transport::Upstream;

use super::State;
use super::catalog::Catalog;

pub struct LiveConfig {
    pub poll: Duration,
    pub manifest_poll: Duration,
    pub l0_bytes: usize,
    pub ingest: IngestConfig,
}

/// Runs forever (the process exits with the runtime).
pub fn run(
    state: Arc<State>,
    store: FsStore,
    mut upstream: Option<(Box<dyn Upstream + Send>, String)>,
    cfg: LiveConfig,
    log: &mut dyn Write,
) {
    let mut scratch = SealScratch::new();
    let mut last_manifest = Instant::now();
    let mut last_poll = Instant::now()
        .checked_sub(cfg.poll)
        .unwrap_or_else(Instant::now);
    let mut misses: Vec<Oid> = Vec::with_capacity(super::miss::MAX_PENDING);
    loop {
        let now = Instant::now();
        if now.duration_since(last_manifest) >= cfg.manifest_poll {
            last_manifest = now;
            if let Err(e) = reload_manifest(&state, &store, log) {
                let _ = writeln!(log, "manifest reload failed: {e}");
            }
        }
        if let Some((up, branch)) = upstream.as_mut()
            && !cfg.poll.is_zero()
            && now.duration_since(last_poll) >= cfg.poll
        {
            last_poll = now;
            if let Err(e) = live_tick(&state, up.as_mut(), branch, &cfg, &mut scratch, log) {
                let _ = writeln!(log, "live sync: {e}");
            }
        }
        // Sleep — or wake at once for a miss and resolve the whole batch in one round.
        if state.misses.take(&mut misses, Duration::from_millis(200)) {
            if let Some((up, _)) = upstream.as_mut()
                && let Err(e) = miss_round(&state, up.as_mut(), &misses, &cfg, &mut scratch, log)
            {
                let _ = writeln!(log, "miss fetch of {} object(s) failed: {e}", misses.len());
            }
            state.misses.finish();
        }
    }
}

/// Swaps in a new catalog when `manifest.bin` changed (the live slab is rebuilt from the
/// new tip by the next tick).
fn reload_manifest(
    state: &Arc<State>,
    store: &FsStore,
    log: &mut dyn Write,
) -> Result<(), Box<dyn std::error::Error>> {
    let current = state.catalog().generation;
    let Some(generation) = store.manifest_generation()? else {
        return Ok(());
    };
    if generation == current {
        return Ok(());
    }
    let Some(m) = store.load_manifest()? else {
        return Ok(());
    };
    let t0 = Instant::now();
    let cat = Catalog::load(store, m)?;
    let _ = writeln!(
        log,
        "manifest generation {} loaded in {:.2}s: tip {}, baseline {}, {} weekly, {} pyramid; live slab reset",
        cat.generation,
        t0.elapsed().as_secs_f64(),
        cat.manifest.tip,
        cat.baseline
            .as_ref()
            .map_or("none".to_string(), |b| b.meta.seq.to_string()),
        cat.weekly.len(),
        cat.pyramid.len()
    );
    state.swap_catalog(Arc::new(cat));
    Ok(())
}

/// One live tick: if the branch moved past our tip, fetch the delta and merge it into L0.
fn live_tick(
    state: &Arc<State>,
    up: &mut dyn Upstream,
    branch: &str,
    cfg: &LiveConfig,
    scratch: &mut SealScratch,
    log: &mut dyn Write,
) -> Result<(), Box<dyn std::error::Error>> {
    let cat = state.catalog();
    let Some(head) = up.ls_ref(branch)? else {
        return Ok(());
    };
    let tip = cat.tip();
    if head == tip {
        return Ok(());
    }
    let t0 = Instant::now();
    let mut haves = haves_for(&cat.manifest);
    if let Some(l0) = &cat.l0 {
        haves.insert(0, l0.meta.tip);
    }
    let dedup = |oid: &Oid| cat.lookup(oid).is_some();
    // Strictly after everything published and after the previous live slab.
    let now = now_ts()
        .max(cat.manifest.latest_ts().saturating_add(1))
        .max(
            cat.l0
                .as_ref()
                .map_or(0, |l| l.meta.end_ts.saturating_add(1)),
        );
    let start_ts = cat
        .l0
        .as_ref()
        .map_or(cat.manifest.latest_ts(), |l| l.meta.start_ts);
    let ing = ingest_git_delta(
        up,
        &[head],
        &haves,
        Tier::L0,
        start_ts,
        now,
        Some(&dedup),
        Destination::Memory {
            capacity: cfg.l0_bytes,
        },
        &cfg.ingest,
        scratch,
    )?;
    let Some(ing) = ing else {
        // Branch moved to objects we already have (e.g. a reset): just advance the tip.
        let next = cat.with_l0(
            cat.l0
                .as_ref()
                .map(|l| Arc::new(retip(l, head, now)))
                .or_else(|| empty_l0(head, now).ok().map(Arc::new)),
        );
        state.swap_catalog(Arc::new(next));
        let _ = writeln!(log, "live: tip {tip} -> {head}, no new objects");
        return Ok(());
    };
    let merged = absorb(state, &cat, ing, head, now)?;
    let _ = writeln!(
        log,
        "live: {tip} -> {head} in {:.2}s; live slab now {} objects / {} bytes",
        t0.elapsed().as_secs_f64(),
        merged.object_count,
        merged.pack_len
    );
    Ok(())
}

/// One miss round: fetches what is still absent from the catalog (`want`s next to the tip,
/// `have`s = every tip, so only the missing closure comes back) and merges it into L0. One
/// id the upstream does not know fails a batch, so a failed batch is retried id by id.
fn miss_round(
    state: &Arc<State>,
    up: &mut dyn Upstream,
    oids: &[Oid],
    cfg: &LiveConfig,
    scratch: &mut SealScratch,
    log: &mut dyn Write,
) -> Result<(), Box<dyn std::error::Error>> {
    match fetch_missing(state, up, oids, cfg, scratch, log) {
        Err(e) if oids.len() > 1 => {
            let _ = writeln!(
                log,
                "miss fetch of {} object(s) failed ({e}); retrying one by one",
                oids.len()
            );
            for o in oids {
                if let Err(e) = fetch_missing(state, up, std::slice::from_ref(o), cfg, scratch, log)
                {
                    let _ = writeln!(log, "miss fetch of {o} failed: {e}");
                }
            }
            Ok(())
        }
        r => r,
    }
}

fn fetch_missing(
    state: &Arc<State>,
    up: &mut dyn Upstream,
    oids: &[Oid],
    cfg: &LiveConfig,
    scratch: &mut SealScratch,
    log: &mut dyn Write,
) -> Result<(), Box<dyn std::error::Error>> {
    let cat = state.catalog();
    let tip = cat.tip();
    let mut wants: Vec<Oid> = Vec::with_capacity(oids.len().saturating_add(1));
    wants.push(tip);
    wants.extend(oids.iter().copied().filter(|o| cat.lookup(o).is_none()));
    if wants.len() == 1 {
        return Ok(());
    }
    let t0 = Instant::now();
    let mut haves = haves_for(&cat.manifest);
    if let Some(l0) = &cat.l0 {
        haves.insert(0, l0.meta.tip);
    }
    let dedup = |oid: &Oid| cat.lookup(oid).is_some();
    let now = now_ts()
        .max(cat.manifest.latest_ts().saturating_add(1))
        .max(
            cat.l0
                .as_ref()
                .map_or(0, |l| l.meta.end_ts.saturating_add(1)),
        );
    let start_ts = cat
        .l0
        .as_ref()
        .map_or(cat.manifest.latest_ts(), |l| l.meta.start_ts);
    let requested = wants.len().saturating_sub(1);
    let ing = ingest_git_delta(
        up,
        &wants,
        &haves,
        Tier::L0,
        start_ts,
        now,
        Some(&dedup),
        Destination::Memory {
            capacity: cfg.l0_bytes,
        },
        &cfg.ingest,
        scratch,
    )?;
    state
        .misses
        .rounds
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let Some(ing) = ing else {
        let _ = writeln!(
            log,
            "miss fetch: {requested} object(s) requested, upstream had nothing new"
        );
        return Ok(());
    };
    state.misses.fetched_objects.fetch_add(
        u64::from(ing.meta.object_count),
        std::sync::atomic::Ordering::Relaxed,
    );
    let merged = absorb(state, &cat, ing, tip, now)?;
    let _ = writeln!(
        log,
        "miss fetch: {requested} object(s) requested, +{} objects in {:.2}s; live slab now {} objects",
        merged.object_count_delta,
        t0.elapsed().as_secs_f64(),
        merged.object_count
    );
    Ok(())
}

struct Absorbed {
    object_count: u32,
    object_count_delta: u32,
    pack_len: u64,
}

/// Merges a freshly ingested in-memory slab into the live slab and publishes the catalog.
fn absorb(
    state: &Arc<State>,
    cat: &Catalog,
    ing: Ingested,
    tip: Oid,
    now: u64,
) -> Result<Absorbed, Box<dyn std::error::Error>> {
    let SlabData::Memory { pack, idx, mphf } = ing.data else {
        return Err("ingest did not return an in-memory slab".into());
    };
    let micro = Slab::from_memory(ing.meta, pack, idx, mphf)?;
    let merged = match &cat.l0 {
        None => micro,
        Some(prev) if prev.meta.object_count == 0 => micro,
        Some(prev) => merge_l0(prev, &micro, tip, now)?,
    };
    let out = Absorbed {
        object_count: merged.meta.object_count,
        object_count_delta: ing.meta.object_count,
        pack_len: merged.meta.pack_len,
    };
    let next = cat.with_l0(Some(Arc::new(merged)));
    state.swap_catalog(Arc::new(next));
    Ok(out)
}

/// The same live slab with a new tip (nothing new to store).
fn retip(l0: &Slab, tip: Oid, now: u64) -> Slab {
    let mut meta = l0.meta;
    meta.tip = tip;
    meta.end_ts = now;
    Slab {
        meta,
        pack: l0.pack.clone(),
        idx: l0.idx.clone(),
        mphf: l0.mphf.clone(),
    }
}

/// An empty live slab carrying only the tip.
fn empty_l0(tip: Oid, now: u64) -> Result<Slab, crate::slab::SlabError> {
    let pack = {
        let mut p = crate::pack::pack_header(0).to_vec();
        let d: [u8; 20] = <sha1::Sha1 as sha1::Digest>::digest(&p).into();
        p.extend_from_slice(&d);
        p
    };
    let mut idx = Vec::new();
    let order: [u32; 0] = [];
    let entry = |_: usize| None;
    let sha: [u8; 20] = pack.last_chunk::<20>().copied().unwrap_or([0; 20]);
    let mut stage = vec![0u8; 64];
    crate::idx::write_idx_v2(&mut idx, &mut stage, &order, &entry, &sha)?;
    let mphf = crate::mphf::build_vec(&[], &[], pack.len() as u64).unwrap_or_default();
    let meta = SlabRef {
        seq: 0,
        tier: Tier::L0,
        start_ts: now,
        end_ts: now,
        tip,
        object_count: 0,
        pack_len: pack.len() as u64,
        idx_len: idx.len() as u64,
        mphf_len: mphf.len() as u64,
    };
    Slab::from_memory(meta, pack, idx, mphf)
}

/// Concatenates the previous live slab and the new micro slab into one in-memory slab.
fn merge_l0(
    prev: &Slab,
    micro: &Slab,
    tip: Oid,
    now: u64,
) -> Result<Slab, Box<dyn std::error::Error>> {
    let prev_arc = Arc::new(Slab {
        meta: prev.meta,
        pack: prev.pack.clone(),
        idx: prev.idx.clone(),
        mphf: prev.mphf.clone(),
    });
    let micro_arc = Arc::new(Slab {
        meta: micro.meta,
        pack: micro.pack.clone(),
        idx: micro.idx.clone(),
        mphf: micro.mphf.clone(),
    });
    let mut list = SlabList::new();
    list.push(prev_arc)?;
    list.push(micro_arc)?;
    let plan = Plan::new(list)?;
    let mut pack = Vec::with_capacity(usize::try_from(plan.pack_len).unwrap_or(0));
    let mut idx = Vec::with_capacity(usize::try_from(plan.idx_len).unwrap_or(0));
    let mut mphf = Vec::new();
    let mut builder = Builder::new();
    let mphf_len = write_merged_mphf(&plan, &mut mphf, &mut builder)?;
    let (object_count, pack_len, idx_len) = (plan.object_count, plan.pack_len, plan.idx_len);
    write_splice(plan, &mut pack, &mut idx)?;
    let meta = SlabRef {
        seq: 0,
        tier: Tier::L0,
        start_ts: prev.meta.start_ts,
        end_ts: now,
        tip,
        object_count,
        pack_len,
        idx_len,
        mphf_len,
    };
    Ok(Slab::from_memory(meta, pack, idx, mphf)?)
}
