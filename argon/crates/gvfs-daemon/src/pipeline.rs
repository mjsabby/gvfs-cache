//! Mode 1: `gvfs-daemon pipeline` — Idempotent, self-healing compaction & sealing pipeline
//! (`run_resilient_pipeline_step` & `rollup_pyramid_tier`, PDF pp. 161–169).
//!
//! Uses the exact same two primitives as `gvfs-daemon serve`:
//! 1. [`ingest_git_delta_with_arena`] (for the 15m delta and Day-0 genesis bootstrap)
//! 2. [`plan_deduplicated_range_splice`] + [`assemble_spliced_pack`] (for 1h, 6h, 12h, 24h,
//!    and Sunday 168h Weekly "Aha" Delta + Weekly Full Baseline rollups — without ever
//!    touching upstream Git/ADO!).

use std::sync::Arc;

use gvfs_core::arena::SeenOidSet;
use gvfs_core::{
    Slab, Tier, UnifiedManifest, ZERO_OID, assemble_spliced_pack, plan_deduplicated_range_splice,
};
use gvfs_storage::{ETag, StorageClient, StorageError};

use crate::ingest::{IngestArena, IngestError, ingest_git_delta_with_arena};
use crate::upstream::{GitUpstream, UpstreamError};

#[derive(Debug)]
pub enum DaemonError {
    Storage(StorageError),
    Ingest(IngestError),
    Upstream(UpstreamError),
    ManifestNotBootstrappedYet,
    SlabBuildFailed,
    Http(String),
    Config(String),
}

impl core::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "storage: {e}"),
            Self::Ingest(e) => write!(f, "ingest: {e}"),
            Self::Upstream(e) => write!(f, "upstream: {e}"),
            Self::ManifestNotBootstrappedYet => f.write_str(
                "manifest.bin does not exist in storage yet (run `gvfs-daemon pipeline` first)",
            ),
            Self::SlabBuildFailed => f.write_str("failed to build rollup slab"),
            Self::Http(m) => write!(f, "http: {m}"),
            Self::Config(m) => write!(f, "config: {m}"),
        }
    }
}

impl std::error::Error for DaemonError {}

impl From<StorageError> for DaemonError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}
impl From<IngestError> for DaemonError {
    fn from(e: IngestError) -> Self {
        Self::Ingest(e)
    }
}
impl From<UpstreamError> for DaemonError {
    fn from(e: UpstreamError) -> Self {
        Self::Upstream(e)
    }
}
impl From<std::io::Error> for DaemonError {
    fn from(e: std::io::Error) -> Self {
        Self::Storage(StorageError::Io(e))
    }
}

/// IDEMPOTENT PIPELINE EXECUTION WITH CRASH SELF-HEALING & GENESIS BOOTSTRAP (PDF pp. 167–169).
pub fn run_resilient_pipeline_step(
    storage: &StorageClient,
    ado: &dyn GitUpstream,
    requested_tier: Tier,
    now_ts: u64,
) -> Result<ETag, DaemonError> {
    let mut arena = IngestArena::default();
    run_resilient_pipeline_step_with_arena(storage, ado, requested_tier, now_ts, &mut arena)
}

/// Arena-reusing version of [`run_resilient_pipeline_step`].
pub fn run_resilient_pipeline_step_with_arena(
    storage: &StorageClient,
    ado: &dyn GitUpstream,
    requested_tier: Tier,
    now_ts: u64,
    arena: &mut IngestArena,
) -> Result<ETag, DaemonError> {
    let current_ado_head = ado.get_main_head_oid()?;

    match storage.get_manifest_with_etag()? {
        // ====================================================================
        // CASE 1: DAY-0 GENESIS COLD START (`manifest.bin` does not exist yet)
        // ====================================================================
        None => {
            // Calling `ingest_git_delta` with `ZERO_OID` pulls the full reachable
            // repository graph from `current_ado_head` into 1 Merkle-DFS baseline slab!
            let genesis_baseline = Arc::new(ingest_git_delta_with_arena(
                ado,
                ZERO_OID,
                current_ado_head,
                &[],
                Tier::H168Base,
                now_ts,
                arena,
            )?);
            storage.upload_slab(&genesis_baseline)?;

            let genesis_manifest = UnifiedManifest {
                version_seq: 1,
                last_15m_tip_oid: current_ado_head,
                weekly_baseline: genesis_baseline,
                weekly_deltas: Vec::new(),
                sub_week_pyramid: Vec::new(),
            };

            // Atomic creation (`If-None-Match: *`) prevents split-brain genesis
            let etag = storage.put_manifest_if_none_match(&genesis_manifest)?;
            Ok(etag)
        }

        // ====================================================================
        // CASE 2: NORMAL / SELF-HEALING INCREMENTAL RUN
        // If the previous cron crashed or was skipped for hours, `last_15m_tip_oid`
        // automatically spans the entire gap up to `current_ado_head`.
        // ====================================================================
        Some((mut manifest, etag)) => {
            if current_ado_head != manifest.last_15m_tip_oid {
                let fallback_haves = manifest.fallback_have_commits();
                let new_m15_slab = Arc::new(ingest_git_delta_with_arena(
                    ado,
                    manifest.last_15m_tip_oid,
                    current_ado_head,
                    &fallback_haves,
                    Tier::M15,
                    now_ts,
                    arena,
                )?);
                storage.upload_slab(&new_m15_slab)?;
                manifest.sub_week_pyramid.push(new_m15_slab);
                manifest.last_15m_tip_oid = current_ado_head;
            }

            // If a higher tier (1h, 6h, 12h, 24h, 168h) was triggered,
            // roll up existing S3/storage slabs via `plan_deduplicated_range_splice()`
            // without touching upstream ADO/Git!
            if requested_tier > Tier::M15 {
                rollup_pyramid_tier(storage, &mut manifest, requested_tier, now_ts)?;
            }

            // Atomic CAS update (`If-Match: <etag>`). If another runner raced us,
            // storage returns HTTP 412 (`PreconditionFailed`) and our newly uploaded
            // slab remains harmlessly unreferenced.
            let new_etag = storage.put_manifest_if_match(&manifest, &etag)?;

            // Sweep any orphaned/crashed `.pack` or `.mphf` files older than 2 hours
            let _ = storage.gc_unreferenced_slabs(&manifest, now_ts.saturating_sub(7200))?;
            Ok(new_etag)
        }
    }
}

/// Rolls up lower-tier slabs in `manifest.sub_week_pyramid` into `requested_tier`
/// (`1h`, `6h`, `12h`, `24h`, or Sunday `168h` Weekly "Aha" Delta + Weekly Full Baseline)
/// using Core Primitive 2 (`plan_deduplicated_range_splice` + `assemble_spliced_pack`)
/// with zero zlib decompression or re-compression (PDF pp. 161–162, 168).
pub fn rollup_pyramid_tier(
    storage: &StorageClient,
    manifest: &mut UnifiedManifest,
    requested_tier: Tier,
    now_ts: u64,
) -> Result<(), DaemonError> {
    match requested_tier {
        Tier::L0Ram | Tier::M15 => Ok(()),

        Tier::H1 | Tier::H6 | Tier::H12 | Tier::H24 => {
            // Collect all slabs in `sub_week_pyramid` with `tier < requested_tier`
            // (ordered newest to oldest during deduplication).
            let mut to_merge: Vec<Arc<Slab>> = manifest
                .sub_week_pyramid
                .iter()
                .filter(|s| s.tier < requested_tier)
                .cloned()
                .collect();
            if to_merge.is_empty() {
                return Ok(());
            }
            to_merge.reverse(); // newest first

            let merged_slab = Arc::new(splice_storage_slabs(
                storage,
                &to_merge,
                requested_tier,
                manifest.last_15m_tip_oid,
                now_ts,
            )?);
            storage.upload_slab(&merged_slab)?;

            manifest
                .sub_week_pyramid
                .retain(|s| s.tier >= requested_tier);
            manifest.sub_week_pyramid.push(merged_slab);
            Ok(())
        }

        Tier::H168Delta | Tier::H168Base => {
            // Sunday 168h Rollup (PDF p. 148–149, 162):
            // 1. Emit the Weekly "Aha" Delta (`weekly-delta`) merging all sub-week slabs
            //    for warm workstations (Track B).
            if !manifest.sub_week_pyramid.is_empty() {
                let mut week_slabs: Vec<Arc<Slab>> = manifest.sub_week_pyramid.clone();
                week_slabs.reverse(); // newest first

                let weekly_delta = Arc::new(splice_storage_slabs(
                    storage,
                    &week_slabs,
                    Tier::H168Delta,
                    manifest.last_15m_tip_oid,
                    now_ts,
                )?);
                storage.upload_slab(&weekly_delta)?;
                manifest.weekly_deltas.push(Arc::clone(&weekly_delta));

                // 2. If `H168Base` was requested, also splice `[weekly_delta, old_baseline]`
                //    into the new Weekly Full Baseline (`baseline`) for cold clones (Track A)
                //    and clear `sub_week_pyramid`!
                if requested_tier == Tier::H168Base {
                    let base_inputs = [weekly_delta, Arc::clone(&manifest.weekly_baseline)];
                    let new_baseline = Arc::new(splice_storage_slabs(
                        storage,
                        &base_inputs,
                        Tier::H168Base,
                        manifest.last_15m_tip_oid,
                        now_ts,
                    )?);
                    storage.upload_slab(&new_baseline)?;
                    manifest.weekly_baseline = new_baseline;
                    manifest.sub_week_pyramid.clear();
                }
            }
            Ok(())
        }
    }
}

fn splice_storage_slabs(
    storage: &StorageClient,
    slabs: &[Arc<Slab>],
    target_tier: Tier,
    tip_commit: gvfs_core::Oid20,
    now_ts: u64,
) -> Result<Slab, DaemonError> {
    let (unique_count, batches) = plan_deduplicated_range_splice(slabs, None, 4096);
    let range_buffers = storage.fetch_range_batches(&batches, slabs)?;
    let (spliced_pack, spliced_objects) = assemble_spliced_pack(unique_count, &range_buffers);

    // When rolling up slabs in the pipeline (`15m -> 1h -> 6h -> 12h -> 24h -> 168h`),
    // re-cluster `OBJ_TREE` directories into fresh depth-1 contiguous reverse-chronological
    // clusters so delta chain depth never exceeds 1 in any sealed slab!
    if target_tier.uses_tree_deltas() {
        let mut arena = IngestArena::default();
        if let Ok(mut clustered) = crate::ingest::ingest_pack_bytes_into_slab(
            &spliced_pack,
            None,
            slabs.last().map_or(ZERO_OID, |s| s.have_commit),
            tip_commit,
            target_tier,
            now_ts,
            &mut arena,
        ) {
            clustered.start_ts = slabs
                .iter()
                .map(|s| s.start_ts)
                .min()
                .unwrap_or_else(|| now_ts.saturating_sub(target_tier.window_secs()));
            clustered.end_ts = slabs
                .iter()
                .map(|s| s.end_ts)
                .max()
                .unwrap_or(now_ts)
                .max(now_ts);
            clustered.id = match target_tier {
                Tier::H168Delta => format!("weekly-delta-{}", clustered.end_ts),
                Tier::H168Base => format!("baseline-{}", clustered.end_ts),
                other => format!("slab-{}-{}", other.as_str(), clustered.end_ts),
            };
            return Ok(clustered);
        }
    }

    let mut seen_hfs = SeenOidSet::with_capacity_log2(14);
    let mut hfs_sidecar = Vec::new();
    for slab in slabs {
        for rec in &slab.hfs_sidecar {
            if seen_hfs.insert(rec.oid) == Some(true) {
                hfs_sidecar.push(*rec);
            }
        }
    }

    let start_ts = slabs
        .iter()
        .map(|s| s.start_ts)
        .min()
        .unwrap_or_else(|| now_ts.saturating_sub(target_tier.window_secs()));
    let end_ts = slabs
        .iter()
        .map(|s| s.end_ts)
        .max()
        .unwrap_or(now_ts)
        .max(now_ts);
    let have_commit = slabs.last().map_or(ZERO_OID, |s| s.have_commit);

    let id = match target_tier {
        Tier::H168Delta => format!("weekly-delta-{end_ts}"),
        Tier::H168Base => format!("baseline-{end_ts}"),
        other => format!("slab-{}-{end_ts}", other.as_str()),
    };

    Slab::from_pack_and_objects(
        id,
        target_tier,
        start_ts,
        end_ts,
        have_commit,
        tip_commit,
        spliced_objects,
        hfs_sidecar,
        spliced_pack,
    )
    .ok_or(DaemonError::SlabBuildFailed)
}
