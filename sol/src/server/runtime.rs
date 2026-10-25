use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::arena::{push, reserve};
use crate::ingest::{IngestArena, ingest_git_delta};
use crate::manifest::{MAX_SLABS, Manifest, Tier};
use crate::pipeline::load_slabs;
use crate::slab::Slab;
use crate::splice::{SpliceArena, splice_slab_ranges};
use crate::storage::Storage;
use crate::upstream::Upstream;
use crate::{Error, Result};

use super::body::BufferPool;
use super::{ServerOptions, WorkerPool};

pub struct Catalog {
    pub manifest: Manifest,
    pub etag: String,
    pub persistent: Vec<Arc<Slab>>,
    pub live: Option<Arc<Slab>>,
    pub _permit: OwnedSemaphorePermit,
}

impl Catalog {
    pub fn tip(&self) -> crate::Oid {
        self.live
            .as_ref()
            .map_or(self.manifest.tip, |slab| slab.reference.tip)
    }
    pub fn cursor(&self) -> u64 {
        self.live
            .as_ref()
            .map_or(self.manifest.cursor(), |slab| slab.reference.end)
    }
    pub fn find(&self, reference: crate::manifest::SlabRef) -> Result<Arc<Slab>> {
        self.persistent
            .iter()
            .find(|slab| slab.reference == reference)
            .map(Arc::clone)
            .ok_or(Error::Invalid("catalog slab"))
    }
    pub fn points(&self, out: &mut Vec<Arc<Slab>>) -> Result<()> {
        out.clear();
        if let Some(live) = &self.live {
            push(out, Arc::clone(live))?;
        }
        for reference in self
            .manifest
            .pyramid
            .iter()
            .rev()
            .chain(std::iter::once(&self.manifest.baseline))
        {
            push(out, self.find(*reference)?)?;
        }
        Ok(())
    }
}

pub struct State {
    pub current: RwLock<Arc<Catalog>>,
    pub storage: Arc<Storage>,
    pub options: ServerOptions,
    pub workers: Arc<WorkerPool>,
    pub buffers: Arc<BufferPool>,
    pub config: Bytes,
    pub ready: AtomicBool,
    pub requests: AtomicU64,
    pub errors: AtomicU64,
    pub ranges: AtomicU64,
    pub generations: Arc<Semaphore>,
    ingest: tokio::sync::Mutex<Option<IngestArena>>,
}

impl State {
    pub fn snapshot(&self) -> Result<Arc<Catalog>> {
        let current = self
            .current
            .read()
            .map_err(|_| Error::Invalid("catalog lock poisoned"))?;
        Ok(Arc::clone(&current))
    }
}

pub async fn boot(
    storage: Arc<Storage>,
    upstream: Option<Upstream>,
    options: ServerOptions,
) -> Result<(Arc<State>, Option<Upstream>)> {
    boot_with_slabs(storage, upstream, options, &[]).await
}

pub async fn boot_with_slabs(
    storage: Arc<Storage>,
    mut upstream: Option<Upstream>,
    options: ServerOptions,
    previous: &[Arc<Slab>],
) -> Result<(Arc<State>, Option<Upstream>)> {
    options.validate()?;
    if options.live && upstream.is_none() {
        return Err(Error::Usage(
            "serve requires --upstream, or explicitly --no-live".into(),
        ));
    }
    let (manifest, etag) = storage.manifest().await?.ok_or_else(|| {
        Error::Usage("store is not bootstrapped; run gvfs-daemon pipeline first".into())
    })?;
    for reference in manifest
        .pyramid
        .iter()
        .chain(&manifest.weekly)
        .chain(std::iter::once(&manifest.baseline))
    {
        if reference.count as usize > options.limits.objects {
            return Err(Error::Capacity("stored slab exceeds --max-objects"));
        }
    }
    let persistent = load_slabs(&storage, &manifest, previous, true, true).await?;
    let generations = Arc::new(Semaphore::new(options.snapshot_slots));
    let permit = Arc::clone(&generations)
        .try_acquire_owned()
        .map_err(|_| Error::Capacity("catalog generations"))?;
    let mut catalog = Catalog {
        manifest,
        etag,
        persistent,
        live: None,
        _permit: permit,
    };
    let mut ingest = if options.live {
        Some(IngestArena::mapped(
            options.live_limits,
            crate::tree::TreeLayout::Full,
            storage.range_bytes(),
            storage.cache_dir(),
        )?)
    } else {
        None
    };
    if options.live {
        let upstream = upstream.as_mut().ok_or(Error::Invalid("live upstream"))?;
        let head = upstream.head().await?;
        if head != catalog.manifest.tip {
            let end = crate::cli::now()?.max(catalog.manifest.cursor().saturating_add(1));
            let mut known = reserve(MAX_SLABS.saturating_add(2))?;
            catalog.points(&mut known)?;
            let live = ingest_git_delta(
                ingest
                    .as_mut()
                    .ok_or(Error::Invalid("live ingestion scratch"))?,
                upstream,
                &storage,
                &known,
                catalog.manifest.tip,
                head,
                Tier::L0,
                catalog.manifest.cursor(),
                end,
                options.live_limits,
                None,
            )
            .await?;
            catalog.live = Some(Arc::new(live));
        }
    }
    let config = super::config_json(&options)?;
    let buffer_bytes = options.range_bytes.max(4096);
    let buffers = BufferPool::new(options.arenas.saturating_mul(2), buffer_bytes, 4)?;
    let workers = WorkerPool::new(&options)?;
    let state = Arc::new(State {
        current: RwLock::new(Arc::new(catalog)),
        storage,
        options,
        workers,
        buffers,
        config,
        ready: AtomicBool::new(true),
        requests: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        ranges: AtomicU64::new(0),
        generations,
        ingest: tokio::sync::Mutex::new(ingest),
    });
    Ok((state, upstream))
}

/// Manifest installation and L0 rebuilding happen before one atomic catalog swap.
/// A failed refresh leaves the complete old snapshot available, never a half-handoff.
pub async fn refresh(
    state: &State,
    upstream: Option<&mut Upstream>,
    check_manifest: bool,
) -> Result<()> {
    refresh_at(state, upstream, check_manifest, crate::cli::now()?).await
}

pub async fn refresh_at(
    state: &State,
    upstream: Option<&mut Upstream>,
    check_manifest: bool,
    now: u64,
) -> Result<()> {
    let current = state.snapshot()?;
    let candidate = if check_manifest {
        let value = state
            .storage
            .manifest()
            .await?
            .ok_or(Error::Invalid("published manifest disappeared"))?;
        if value.1 == current.etag {
            None
        } else {
            if value.0.generation <= current.manifest.generation {
                return Err(Error::Invalid("manifest rollback or generation reuse"));
            }
            Some(value)
        }
    } else {
        None
    };
    let mut upstream = upstream;
    let head = if state.options.live {
        Some(
            upstream
                .as_mut()
                .ok_or(Error::Invalid("live upstream"))?
                .head()
                .await?,
        )
    } else {
        None
    };
    if candidate.is_none() && head.is_none_or(|head| head == current.tip()) {
        return Ok(());
    }
    let permit = Arc::clone(&state.generations)
        .try_acquire_owned()
        .map_err(|_| Error::Capacity("all snapshot slots are pinned by clients"))?;
    let changed = candidate.is_some();
    let (manifest, etag, persistent) = if let Some((manifest, etag)) = candidate {
        let persistent =
            load_slabs(&state.storage, &manifest, &current.persistent, true, true).await?;
        (manifest, etag, persistent)
    } else {
        (
            current.manifest.clone(),
            current.etag.clone(),
            current.persistent.clone(),
        )
    };
    let mut next = Catalog {
        manifest,
        etag,
        persistent,
        live: None,
        _permit: permit,
    };
    if let Some(head) = head {
        let have = if changed {
            next.manifest.tip
        } else {
            current.tip()
        };
        if head != next.manifest.tip {
            let mut known = reserve(MAX_SLABS.saturating_add(2))?;
            if changed {
                next.points(&mut known)?;
            } else {
                current.points(&mut known)?;
            }
            let end = now
                .max(next.manifest.cursor().saturating_add(1))
                .max(current.cursor().saturating_add(1));
            let mut ingest = state.ingest.lock().await;
            let micro = Arc::new(
                ingest_git_delta(
                    ingest
                        .as_mut()
                        .ok_or(Error::Invalid("live ingestion scratch"))?,
                    upstream.as_mut().ok_or(Error::Invalid("live upstream"))?,
                    &state.storage,
                    &known,
                    have,
                    head,
                    Tier::L0,
                    next.manifest.cursor(),
                    end,
                    state.options.live_limits,
                    None,
                )
                .await?,
            );
            drop(ingest);
            let live = if !changed {
                if let Some(previous) = &current.live {
                    let sources = [micro, Arc::clone(previous)];
                    let mut plan = SpliceArena::new(
                        state.options.live_limits.objects,
                        state.options.max_ranges,
                    )?;
                    plan.plan(&sources, None, state.options.gap, state.options.range_bytes)?;
                    Arc::new(
                        splice_slab_ranges(
                            &state.storage,
                            &sources,
                            &plan,
                            state.options.live_limits,
                            Tier::L0,
                            next.manifest.cursor(),
                            end,
                            head,
                        )
                        .await?,
                    )
                } else {
                    micro
                }
            } else {
                micro
            };
            next.live = Some(live);
        }
    }
    *state
        .current
        .write()
        .map_err(|_| Error::Invalid("catalog lock poisoned"))? = Arc::new(next);
    Ok(())
}

pub async fn synchronize(
    state: Arc<State>,
    mut upstream: Option<Upstream>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(state.options.poll_seconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_manifest = Instant::now();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let check = last_manifest.elapsed() >= Duration::from_secs(state.options.manifest_seconds);
                if check { last_manifest = Instant::now(); }
                let result = refresh(&state, upstream.as_mut(), check).await;
                state.ready.store(result.is_ok(), Ordering::Release);
                if let Err(error) = result { state.errors.fetch_add(1, Ordering::Relaxed); eprintln!("live synchronization: {error}"); }
            }
            result = stop.changed() => if result.is_err() || *stop.borrow() { return; },
        }
    }
}
