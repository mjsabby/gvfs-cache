use std::sync::Arc;

use crate::arena::{push, reserve};
use crate::codec::Oid;
use crate::ingest::{IngestArena, ingest_git_delta};
use crate::manifest::{MAX_SLABS, Manifest, SlabRef, Tier};
use crate::slab::{Limits, Slab};
use crate::splice::{SpliceArena, splice_slab_ranges};
use crate::storage::Storage;
use crate::upstream::Upstream;
use crate::{Error, Result};

pub struct PipelineOptions {
    /// None runs the scheduled 15-minute ingest and all due higher rollups.
    pub tier: Option<Tier>,
    pub now: u64,
    pub limits: Limits,
    pub max_ranges: usize,
    pub gap: u64,
    pub retention_secs: u64,
    pub input_pack: Option<std::path::PathBuf>,
}

#[derive(Debug)]
pub struct PipelineReport {
    pub generation: u64,
    pub tip: Oid,
    pub objects: u64,
    pub artifacts: usize,
    pub changed: bool,
}

/// Reused by long-lived pipeline runners and deterministic workload replays.
pub struct PipelineCache {
    pub loaded: Vec<Arc<Slab>>,
    ingest: Option<IngestArena>,
    limits: Option<Limits>,
}

impl PipelineCache {
    pub fn new() -> Result<Self> {
        Ok(Self {
            loaded: reserve(MAX_SLABS.saturating_add(1))?,
            ingest: None,
            limits: None,
        })
    }
}

pub async fn load_slabs(
    storage: &Storage,
    manifest: &Manifest,
    previous: &[Arc<Slab>],
    weekly: bool,
    packs: bool,
) -> Result<Vec<Arc<Slab>>> {
    let mut out = reserve(MAX_SLABS.saturating_add(1))?;
    for reference in manifest
        .pyramid
        .iter()
        .rev()
        .chain(std::iter::once(&manifest.baseline))
        .chain(manifest.weekly.iter().filter(|_| weekly).rev())
    {
        if out
            .iter()
            .any(|slab: &Arc<Slab>| slab.reference == *reference)
        {
            continue;
        }
        let slab = if let Some(slab) = previous.iter().find(|slab| slab.reference == *reference) {
            Arc::clone(slab)
        } else {
            Arc::new(if packs {
                storage.load_slab(*reference).await?
            } else {
                storage.load_metadata(*reference).await?
            })
        };
        push(&mut out, slab)?;
    }
    Ok(out)
}

pub async fn run_pipeline(
    storage: &Storage,
    upstream: Option<&mut Upstream>,
    options: &PipelineOptions,
) -> Result<PipelineReport> {
    run_pipeline_cached(storage, upstream, options, &mut PipelineCache::new()?).await
}

pub async fn run_pipeline_cached(
    storage: &Storage,
    mut upstream: Option<&mut Upstream>,
    options: &PipelineOptions,
    cache: &mut PipelineCache,
) -> Result<PipelineReport> {
    if options.now > i64::MAX as u64 {
        return Err(Error::Usage(
            "pipeline timestamp exceeds the GVFS i64 range".into(),
        ));
    }
    let _writer = storage.writer_lock().await?;
    let current = storage.manifest().await?;
    let Some((mut manifest, etag)) = current else {
        let upstream =
            upstream.ok_or_else(|| Error::Usage("genesis requires --upstream".into()))?;
        let tip = upstream.head().await?;
        let mut arena = IngestArena::mapped(
            options.limits,
            options.limits.tree_layout.for_tier(Tier::Baseline),
            storage.range_bytes(),
            storage.cache_dir(),
        )?;
        let slab = ingest_git_delta(
            &mut arena,
            upstream,
            storage,
            &[],
            Oid::ZERO,
            tip,
            Tier::Baseline,
            0,
            options.now.max(1),
            options.limits,
            options.input_pack.as_ref(),
        )
        .await?;
        drop(arena);
        storage.upload(&slab).await?;
        let manifest = Manifest::genesis(slab.reference)?;
        storage.publish_manifest(&manifest, None).await?;
        let count = slab.reference.count;
        cache.loaded.clear();
        push(&mut cache.loaded, Arc::new(slab))?;
        return Ok(PipelineReport {
            generation: 1,
            tip,
            objects: u64::from(count),
            artifacts: 1,
            changed: true,
        });
    };
    if options.input_pack.is_some() {
        return Err(Error::Usage(
            "--input-pack is only valid when the store has no manifest".into(),
        ));
    }
    let polled_tip = if options.tier.is_none() || options.tier == Some(Tier::M15) {
        Some(
            upstream
                .as_deref_mut()
                .ok_or_else(|| Error::Usage("15-minute ingestion requires --upstream".into()))?
                .head()
                .await?,
        )
    } else {
        None
    };
    if polled_tip == Some(manifest.tip)
        && (options.tier == Some(Tier::M15) || manifest.pyramid.is_empty())
    {
        return Ok(PipelineReport {
            generation: manifest.generation,
            tip: manifest.tip,
            objects: 0,
            artifacts: 0,
            changed: false,
        });
    }
    let mut loaded = load_slabs(storage, &manifest, &cache.loaded, false, false).await?;
    let mut artifacts = 0usize;
    let mut objects = 0u64;
    let previous_generation = manifest.generation;
    if let Some(tip) = polled_tip {
        let upstream = upstream.as_mut().ok_or(Error::Invalid("polled upstream"))?;
        if tip != manifest.tip {
            let end = options.now.max(
                manifest
                    .cursor()
                    .checked_add(1)
                    .ok_or(Error::Capacity("prefetch timestamp"))?,
            );
            if cache.limits != Some(options.limits) {
                cache.ingest = Some(IngestArena::mapped(
                    options.limits,
                    crate::tree::TreeLayout::Full,
                    storage.range_bytes(),
                    storage.cache_dir(),
                )?);
                cache.limits = Some(options.limits);
            }
            let slab = ingest_git_delta(
                cache
                    .ingest
                    .as_mut()
                    .ok_or(Error::Invalid("cached ingestion scratch"))?,
                upstream,
                storage,
                &loaded,
                manifest.tip,
                tip,
                Tier::M15,
                manifest.cursor(),
                end,
                options.limits,
                None,
            )
            .await?;
            storage.upload(&slab).await?;
            push(&mut manifest.pyramid, slab.reference)?;
            manifest.tip = tip;
            objects = objects.saturating_add(u64::from(slab.reference.count));
            artifacts = artifacts.saturating_add(1);
            loaded.insert(0, Arc::new(slab));
        }
    }
    if options.tier != Some(Tier::M15) {
        let mut plan = SpliceArena::new(options.limits.objects, options.max_ranges)?;
        for tier in [Tier::H1, Tier::H6, Tier::H12, Tier::H24] {
            if options.tier.is_none() || options.tier == Some(tier) {
                let seconds = u64::from(tier as u32).saturating_mul(60);
                let cutoff = if options.tier.is_none() {
                    options.now.saturating_sub(
                        options
                            .now
                            .checked_rem(seconds)
                            .ok_or(Error::Invalid("rollup interval"))?,
                    )
                } else {
                    options.now
                };
                let count = rollup_tier(
                    storage,
                    &mut manifest,
                    &mut loaded,
                    &mut plan,
                    tier,
                    cutoff,
                    options,
                )
                .await?;
                artifacts = artifacts.saturating_add(count);
            }
        }
        let due_week = options.tier == Some(Tier::Baseline)
            || options.tier.is_none()
                && options.now.saturating_add(345600) / 604800
                    > manifest.baseline.end.saturating_add(345600) / 604800;
        if due_week && manifest.cursor() > manifest.baseline.end {
            let mut weekly_sources = reserve(MAX_SLABS)?;
            for reference in manifest.pyramid.iter().rev() {
                push(&mut weekly_sources, find_slab(&loaded, *reference)?)?;
            }
            plan.plan(&weekly_sources, None, options.gap, storage.range_bytes())?;
            let baseline = find_slab(&loaded, manifest.baseline)?;
            plan.exclude(
                &weekly_sources,
                std::slice::from_ref(&baseline),
                options.gap,
                storage.range_bytes(),
            )?;
            let delta = splice_slab_ranges(
                storage,
                &weekly_sources,
                &plan,
                options.limits,
                Tier::WeeklyDelta,
                manifest.baseline.end,
                manifest.cursor(),
                manifest.tip,
            )
            .await?;
            storage.upload(&delta).await?;
            let delta = Arc::new(delta);
            let baseline_sources =
                crate::splice::hydrate_sources(storage, &[Arc::clone(&delta), baseline]).await?;
            let mut tree_reader = crate::tree::TreeReader::new(options.limits)?;
            plan.plan_merkle(&baseline_sources, manifest.tip, &mut tree_reader)?;
            let new_baseline = splice_slab_ranges(
                storage,
                &baseline_sources,
                &plan,
                options.limits,
                Tier::Baseline,
                0,
                manifest.cursor(),
                manifest.tip,
            )
            .await?;
            storage.upload(&new_baseline).await?;
            push(&mut manifest.weekly, delta.reference)?;
            manifest.baseline = new_baseline.reference;
            loaded.push(Arc::new(new_baseline));
            manifest.pyramid.clear();
            let retain_after = manifest.baseline.end.saturating_sub(options.retention_secs);
            manifest.weekly.retain(|slab| slab.end > retain_after);
            artifacts = artifacts.saturating_add(2);
        }
    }
    if artifacts != 0 {
        manifest.generation = previous_generation
            .checked_add(1)
            .ok_or(Error::Capacity("manifest generation"))?;
        manifest.validate()?;
        storage.publish_manifest(&manifest, Some(&etag)).await?;
        if storage.is_local() {
            let older_than = std::time::SystemTime::now()
                .checked_sub(std::time::Duration::from_secs(7200))
                .ok_or(Error::Invalid("GC clock"))?;
            storage.gc_local(&manifest, older_than)?;
        }
    }
    cache.loaded = load_slabs(storage, &manifest, &loaded, false, false).await?;
    Ok(PipelineReport {
        generation: manifest.generation,
        tip: manifest.tip,
        objects,
        artifacts,
        changed: artifacts != 0,
    })
}

fn find_slab(slabs: &[Arc<Slab>], reference: SlabRef) -> Result<Arc<Slab>> {
    slabs
        .iter()
        .find(|slab| slab.reference == reference)
        .map(Arc::clone)
        .ok_or(Error::Invalid("missing loaded slab"))
}

#[allow(clippy::too_many_arguments)]
async fn rollup_tier(
    storage: &Storage,
    manifest: &mut Manifest,
    loaded: &mut Vec<Arc<Slab>>,
    plan: &mut SpliceArena,
    tier: Tier,
    cutoff: u64,
    options: &PipelineOptions,
) -> Result<usize> {
    let mut cursor = 0usize;
    let mut outputs = 0usize;
    while cursor < manifest.pyramid.len() {
        let first = *manifest
            .pyramid
            .get(cursor)
            .ok_or(Error::Invalid("rollup cursor"))?;
        if first.tier >= tier || first.end > cutoff {
            cursor = cursor.saturating_add(1);
            continue;
        }
        let mut finish = cursor;
        while let Some(reference) = manifest.pyramid.get(finish) {
            if reference.tier >= tier || reference.end > cutoff {
                break;
            }
            finish = finish.saturating_add(1);
        }
        let references = manifest
            .pyramid
            .get(cursor..finish)
            .ok_or(Error::Invalid("rollup range"))?;
        let last = *references.last().ok_or(Error::Invalid("empty rollup"))?;
        let mut sources = reserve(references.len())?;
        for &reference in references.iter().rev() {
            push(&mut sources, find_slab(loaded, reference)?)?;
        }
        plan.plan(&sources, None, options.gap, storage.range_bytes())?;
        let merged = splice_slab_ranges(
            storage,
            &sources,
            plan,
            options.limits,
            tier,
            first.start,
            last.end,
            last.tip,
        )
        .await?;
        storage.upload(&merged).await?;
        let reference = merged.reference;
        // Replace in place without a temporary collect/Splice allocation.
        *manifest
            .pyramid
            .get_mut(cursor)
            .ok_or(Error::Invalid("rollup replacement"))? = reference;
        let tail = finish;
        let destination = cursor.saturating_add(1);
        manifest.pyramid.copy_within(tail.., destination);
        manifest.pyramid.truncate(
            manifest
                .pyramid
                .len()
                .saturating_sub(finish.saturating_sub(cursor).saturating_sub(1)),
        );
        loaded.push(Arc::new(merged));
        outputs = outputs.saturating_add(1);
        cursor = cursor.saturating_add(1);
    }
    Ok(outputs)
}
