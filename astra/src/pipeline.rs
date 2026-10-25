use crate::arena::Arena;
use crate::ingest::{IngestScratch, PackMode, ingest_git_delta};
use crate::manifest::{Manifest, SlabRef, Tier, WEEK};
use crate::slab::Slab;
use crate::source::GitUpstream;
use crate::splice::{Plan, seal_plan};
use crate::storage::Storage;
use crate::{Error, Oid20, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct Config {
    pub max_objects: usize,
    pub max_object_bytes: usize,
    pub delta_cache_bytes: usize,
    pub live_bytes: usize,
    pub mode: PackMode,
    pub work: PathBuf,
}

#[derive(Clone)]
pub struct Catalog {
    pub manifest: Manifest,
    pub baseline: Arc<Slab>,
    pub weekly: Vec<Arc<Slab>>,
    pub pyramid: Vec<Arc<Slab>>,
    pub live: Option<Arc<Slab>>,
}

impl Catalog {
    pub fn revision(&self) -> Oid20 {
        use sha1::{Digest, Sha1};
        let mut hash = Sha1::new();
        hash.update(b"gvfs-catalog-v1");
        hash.update(self.manifest.generation.to_le_bytes());
        hash.update(self.manifest.timestamp.to_le_bytes());
        hash.update(self.manifest.tip.0);
        hash.update(self.baseline.meta.index_hash.0);
        hash.update((self.weekly.len() as u64).to_le_bytes());
        for slab in &self.weekly {
            hash.update(slab.meta.index_hash.0);
        }
        hash.update((self.pyramid.len() as u64).to_le_bytes());
        for slab in &self.pyramid {
            hash.update(slab.meta.index_hash.0);
        }
        if let Some(live) = &self.live {
            hash.update([1]);
            hash.update(live.meta.index_hash.0);
            hash.update(live.meta.tip.0);
        } else {
            hash.update([0]);
        }
        Oid20(hash.finalize().into())
    }

    pub fn tip(&self) -> Oid20 {
        self.live.as_ref().map_or(self.manifest.tip, |l| l.meta.tip)
    }
    pub fn active(&self) -> Vec<Arc<Slab>> {
        std::iter::once(Arc::clone(&self.baseline))
            .chain(self.pyramid.iter().cloned())
            .chain(self.live.iter().cloned())
            .collect()
    }
    pub fn prefetch(&self, timestamp: u64, plan: &mut Plan) -> Result<()> {
        plan.reset()?;
        self.validate_cursor(timestamp)?;
        if timestamp == 0 {
            plan.add(&self.baseline)?;
        } else {
            for slab in &self.weekly {
                if slab.meta.end > timestamp {
                    plan.add(slab)?;
                }
            }
        }
        for slab in &self.pyramid {
            if slab.meta.end > timestamp {
                plan.add(slab)?;
            }
        }
        if let Some(live) = &self.live {
            plan.add(live)?;
        }
        plan.select_since(timestamp)
    }

    pub fn validate_cursor(&self, timestamp: u64) -> Result<()> {
        if timestamp > self.manifest.timestamp {
            return Err(Error::Invalid("cache is behind client cursor"));
        }
        if timestamp != 0 && timestamp < self.manifest.genesis {
            return Err(Error::Invalid(
                "cursor predates this repository's genesis; cold prefetch required",
            ));
        }
        Ok(())
    }
}

pub struct Engine {
    pub config: Config,
    pub store: Storage,
    upstream: Option<GitUpstream>,
    scratch: Option<IngestScratch>,
    plan: Plan,
    buffer: Arena,
    pub catalog: Option<Catalog>,
    etag: Option<String>,
}

impl Engine {
    pub fn new(config: Config, store: Storage, upstream: Option<GitUpstream>) -> Result<Self> {
        fs::create_dir_all(&config.work)?;
        Ok(Self {
            plan: Plan::new(config.max_objects, config.max_objects),
            buffer: Arena::new(1 << 20)?,
            config,
            store,
            upstream,
            scratch: None,
            catalog: None,
            etag: None,
        })
    }
    fn scratch(&mut self) -> Result<()> {
        if self.scratch.is_none() {
            self.scratch = Some(IngestScratch::new(
                self.config.max_objects,
                self.config.max_object_bytes,
                self.config.delta_cache_bytes,
            )?);
        }
        Ok(())
    }
    pub fn reload(&mut self) -> Result<bool> {
        let Some((bytes, etag)) = self.store.manifest()? else {
            if self.catalog.is_some() {
                return Err(Error::Invalid("published manifest disappeared"));
            }
            return Ok(false);
        };
        if self.etag.as_ref() == Some(&etag) {
            return Ok(false);
        }
        let manifest = Manifest::decode(&bytes)?;
        if self
            .catalog
            .as_ref()
            .is_some_and(|c| manifest.generation <= c.manifest.generation)
        {
            return Err(Error::Invalid(
                "manifest generation rollback or reused generation",
            ));
        }
        let cache = self.config.work.join("indexes");
        let load = |meta: SlabRef| -> Result<Arc<Slab>> {
            if let Some(old) = &self.catalog {
                for slab in std::iter::once(&old.baseline)
                    .chain(&old.weekly)
                    .chain(&old.pyramid)
                {
                    if slab.meta == meta {
                        return Ok(Arc::clone(slab));
                    }
                }
            }
            Slab::load(&self.store, meta, &cache)
        };
        let baseline = load(manifest.baseline)?;
        let weekly = manifest
            .weekly
            .iter()
            .copied()
            .map(&load)
            .collect::<Result<Vec<_>>>()?;
        let pyramid = manifest
            .pyramid
            .iter()
            .copied()
            .map(load)
            .collect::<Result<Vec<_>>>()?;
        // The caller rebuilds the remainder from the NEW sealed tip before publication.
        self.catalog = Some(Catalog {
            manifest,
            baseline,
            weekly,
            pyramid,
            live: None,
        });
        self.etag = Some(etag);
        Ok(true)
    }

    pub fn pipeline(&mut self, tier: Option<Tier>, now: u64) -> Result<bool> {
        for attempt in 0..3 {
            self.reload()?;
            match self.pipeline_once(tier, now) {
                Err(Error::Conflict) if attempt < 2 => {
                    eprintln!("manifest CAS conflict; reloading and retrying");
                    self.catalog = None;
                    self.etag = None;
                }
                outcome => return outcome,
            }
        }
        Err(Error::Conflict)
    }

    fn pipeline_once(&mut self, tier: Option<Tier>, now: u64) -> Result<bool> {
        if now == 0 || now >= i64::MAX as u64 {
            return Err(Error::Invalid("publication timestamp"));
        }
        if self.catalog.is_none() {
            self.scratch()?;
            let upstream = self
                .upstream
                .as_ref()
                .ok_or(Error::Invalid("genesis requires --upstream"))?;
            let want = upstream.tip()?;
            let artifact = ingest_git_delta(
                upstream,
                Oid20([0; 20]),
                want,
                &[],
                Tier::Baseline,
                now,
                now,
                self.config.mode,
                self.scratch
                    .as_mut()
                    .ok_or(Error::Invalid("ingest scratch"))?,
                &self.config.work,
            )?;
            artifact.publish(&self.store)?;
            let manifest = Manifest {
                generation: 1,
                timestamp: now,
                genesis: now,
                tip: want,
                baseline: artifact.meta,
                weekly: Vec::new(),
                pyramid: Vec::new(),
            };
            self.store.publish_manifest(&manifest.encode()?, None)?;
            self.reload()?;
            return Ok(true);
        }
        let mut changed = false;
        let mut manifest = self
            .catalog
            .as_ref()
            .ok_or(Error::Invalid("catalog"))?
            .manifest
            .clone();
        let expected = self.etag.clone().ok_or(Error::Invalid("manifest ETag"))?;
        if tier.is_none() || tier == Some(Tier::M15) {
            let want = self
                .upstream
                .as_ref()
                .ok_or(Error::Invalid("ingest requires --upstream"))?
                .tip()?;
            if want != manifest.tip {
                self.scratch()?;
                let known = self
                    .catalog
                    .as_ref()
                    .ok_or(Error::Invalid("catalog"))?
                    .active();
                let end = now.max(
                    manifest
                        .timestamp
                        .checked_add(1)
                        .ok_or(Error::Capacity("timestamp"))?,
                );
                let artifact = ingest_git_delta(
                    self.upstream.as_ref().ok_or(Error::Invalid("upstream"))?,
                    manifest.tip,
                    want,
                    &known,
                    Tier::M15,
                    manifest.timestamp,
                    end,
                    self.config.mode,
                    self.scratch
                        .as_mut()
                        .ok_or(Error::Invalid("ingest scratch"))?,
                    &self.config.work,
                )?;
                artifact.publish(&self.store)?;
                manifest.pyramid.push(artifact.meta);
                manifest.timestamp = end;
                manifest.tip = want;
                changed = true;
            }
        }
        if tier != Some(Tier::M15) {
            for target in [Tier::H1, Tier::H6, Tier::H12, Tier::H24, Tier::WeeklyDelta] {
                if tier.is_none_or(|t| t as u32 >= target as u32) {
                    changed |= self.rollup(&mut manifest, target, now)?;
                }
            }
        }
        if !changed {
            return Ok(false);
        }
        manifest.generation = manifest
            .generation
            .checked_add(1)
            .ok_or(Error::Capacity("manifest generation"))?;
        let bytes = manifest.encode()?;
        Manifest::decode(&bytes)?;
        self.store.publish_manifest(&bytes, Some(&expected))?;
        self.reload()?;
        Ok(true)
    }

    fn merge(
        &mut self,
        refs: &[SlabRef],
        tier: Tier,
        start: u64,
        end: u64,
        tip: Oid20,
    ) -> Result<SlabRef> {
        self.plan.reset()?;
        for &meta in refs {
            let slab = Slab::load(&self.store, meta, &self.config.work.join("indexes"))?;
            self.plan.add(&slab)?;
        }
        self.plan.select_since(0)?;
        let artifact = seal_plan(
            &self.plan,
            tier,
            start,
            end,
            tip,
            &self.config.work,
            self.buffer.buffer(),
        )?;
        let artifact = if self.config.mode == PackMode::Clustered
            && matches!(tier, Tier::H24 | Tier::WeeklyDelta | Tier::Baseline)
        {
            crate::ingest::cluster_artifact(&artifact, &self.config)?
        } else {
            artifact
        };
        artifact.publish(&self.store)?;
        Ok(artifact.meta)
    }

    fn rollup(&mut self, manifest: &mut Manifest, tier: Tier, now: u64) -> Result<bool> {
        let duration = u64::from(tier as u32);
        let boundary = if tier == Tier::WeeklyDelta {
            now.saturating_add(259200)
                .wrapping_div(WEEK)
                .saturating_mul(WEEK)
                .saturating_sub(259200)
        } else {
            now.checked_div(duration)
                .ok_or(Error::Invalid("zero tier duration"))?
                .saturating_mul(duration)
        };
        if tier == Tier::WeeklyDelta {
            let count = manifest
                .pyramid
                .iter()
                .take_while(|s| s.end <= boundary)
                .count();
            if count == 0 {
                return Ok(false);
            }
            let group = manifest
                .pyramid
                .get(..count)
                .ok_or(Error::Invalid("weekly group"))?;
            let last = *group.last().ok_or(Error::Invalid("weekly group"))?;
            let delta = self.merge(
                group,
                Tier::WeeklyDelta,
                manifest.baseline.end,
                last.end,
                last.tip,
            )?;
            let baseline = self.merge(
                &[manifest.baseline, delta],
                Tier::Baseline,
                manifest.genesis,
                last.end,
                last.tip,
            )?;
            manifest.weekly.push(delta);
            manifest.baseline = baseline;
            manifest.pyramid.drain(..count);
            return Ok(true);
        }
        let mut changed = false;
        let mut i = 0usize;
        while let Some(first) = manifest.pyramid.get(i).copied() {
            if first.end > boundary || first.tier as u32 >= tier as u32 {
                i = i.saturating_add(1);
                continue;
            }
            let bucket = first
                .end
                .saturating_sub(1)
                .checked_div(duration)
                .ok_or(Error::Invalid("zero tier duration"))?;
            let mut end = i.saturating_add(1);
            while manifest.pyramid.get(end).is_some_and(|s| {
                s.end <= boundary
                    && (s.tier as u32) < tier as u32
                    && s.end.saturating_sub(1).checked_div(duration) == Some(bucket)
            }) {
                end = end.saturating_add(1);
            }
            let last = *manifest
                .pyramid
                .get(end.saturating_sub(1))
                .ok_or(Error::Invalid("rollup"))?;
            let merged = if end == i.saturating_add(1)
                && !(tier == Tier::H24 && self.config.mode == PackMode::Clustered)
            {
                SlabRef { tier, ..first }
            } else {
                self.merge(
                    manifest
                        .pyramid
                        .get(i..end)
                        .ok_or(Error::Invalid("rollup"))?,
                    tier,
                    first.start,
                    last.end,
                    last.tip,
                )?
            };
            manifest.pyramid.splice(i..end, [merged]);
            changed = true;
            i = i.saturating_add(1);
        }
        Ok(changed)
    }

    pub fn live_sync(&mut self, now: u64) -> Result<bool> {
        let upstream = self
            .upstream
            .as_ref()
            .ok_or(Error::Invalid("live sync requires --upstream"))?;
        let want = upstream.tip()?;
        let catalog = self
            .catalog
            .as_ref()
            .ok_or(Error::Invalid("run pipeline once before serve"))?;
        if want == catalog.tip() {
            return Ok(false);
        }
        let have = catalog.tip();
        let known = catalog.active();
        let start = catalog.manifest.timestamp;
        let end = now.max(start.saturating_add(1));
        let previous = catalog.live.clone();
        self.scratch()?;
        let artifact = ingest_git_delta(
            self.upstream.as_ref().ok_or(Error::Invalid("upstream"))?,
            have,
            want,
            &known,
            Tier::Live,
            start,
            end,
            self.config.mode,
            self.scratch
                .as_mut()
                .ok_or(Error::Invalid("ingest scratch"))?,
            &self.config.work,
        )?;
        let next = Slab::from_artifact(&artifact, self.config.live_bytes)?;
        let next = if let Some(previous) = previous {
            self.plan.reset()?;
            self.plan.add(&previous)?;
            self.plan.add(&next)?;
            self.plan.select_since(0)?;
            if self.plan.pack_len > self.config.live_bytes as u64 {
                return Err(Error::Capacity(
                    "live slab; run pipeline or raise --live-mib",
                ));
            }
            let merged = seal_plan(
                &self.plan,
                Tier::Live,
                start,
                end,
                want,
                &self.config.work,
                self.buffer.buffer(),
            )?;
            Slab::from_artifact(&merged, self.config.live_bytes)?
        } else {
            next
        };
        self.catalog.as_mut().ok_or(Error::Invalid("catalog"))?.live = Some(next);
        Ok(true)
    }
}

pub fn work_lock(path: &Path) -> Result<fs::File> {
    fs::create_dir_all(path)?;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join("process.lock"))?;
    fs2::FileExt::try_lock_exclusive(&file)?;
    Ok(file)
}
