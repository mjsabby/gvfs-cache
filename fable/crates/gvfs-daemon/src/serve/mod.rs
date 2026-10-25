//! `gvfs-daemon serve`: the stateless GVFS edge, or the same binary as a localhost proxy.
//!
//! Boot: map every slab of the manifest, pre-allocate the request arenas, seal the
//! allocator, start the live-sync thread, bind. From then on request handling touches
//! only memory-mapped slabs and pre-allocated arenas.

pub mod body;
pub mod catalog;
pub mod handlers;
pub mod json;
pub mod live;
pub mod miss;
pub mod objects;
pub mod pool;

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

use crate::ingest::IngestConfig;
use crate::store::FsStore;
use crate::transport::Upstream;

use self::body::TrailerCache;
use self::catalog::Catalog;
use self::pool::ArenaPool;

pub use crate::pipeline::now_ts;

#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub bind: String,
    pub store: PathBuf,
    pub arenas: usize,
    pub arena_bytes: usize,
    pub l0_bytes: usize,
    pub poll_secs: u64,
    pub manifest_poll_secs: u64,
    pub cache_servers: Vec<String>,
    pub guard: Option<gvfs_alloc_guard::Mode>,
}

#[derive(Debug)]
pub struct ServeError(pub String);

impl core::fmt::Display for ServeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ServeError {}

impl From<std::io::Error> for ServeError {
    fn from(e: std::io::Error) -> Self {
        Self(e.to_string())
    }
}
impl From<crate::store::StoreError> for ServeError {
    fn from(e: crate::store::StoreError) -> Self {
        Self(e.to_string())
    }
}
impl From<crate::slab::SlabError> for ServeError {
    fn from(e: crate::slab::SlabError) -> Self {
        Self(e.to_string())
    }
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub requests: AtomicU64,
    pub prefetch_bytes: AtomicU64,
    pub trailer_hits: AtomicU64,
    pub objects_served: AtomicU64,
    pub misses: AtomicU64,
}

/// Shared server state.
pub struct State {
    catalog: RwLock<Arc<Catalog>>,
    pub pool: Arc<ArenaPool>,
    pub trailers: Arc<TrailerCache>,
    pub config_json: Bytes,
    pub metrics: Metrics,
    pub misses: miss::MissQueue,
    /// Raised on every catalog swap; the warm thread re-hashes the cold selection.
    warm: (std::sync::Mutex<bool>, std::sync::Condvar),
}

impl State {
    /// The current snapshot (a refcount bump; the lock is held for nanoseconds).
    pub fn catalog(&self) -> Arc<Catalog> {
        self.catalog
            .read()
            .map(|g| Arc::clone(&g))
            .unwrap_or_default()
    }

    pub fn swap_catalog(&self, cat: Arc<Catalog>) {
        if let Ok(mut g) = self.catalog.write() {
            *g = cat;
        }
        if let Ok(mut flag) = self.warm.0.lock() {
            *flag = true;
            self.warm.1.notify_one();
        }
    }

    /// Blocks the warm thread until the catalog changed.
    fn wait_for_catalog_change(&self) {
        let Ok(mut flag) = self.warm.0.lock() else {
            return;
        };
        while !*flag {
            match self.warm.1.wait(flag) {
                Ok(g) => flag = g,
                Err(_) => return,
            }
        }
        *flag = false;
    }

    pub fn metrics_text(&self) -> Bytes {
        let a = gvfs_alloc_guard::stats();
        let cat = self.catalog();
        let s = format!(
            "gvfs_requests_total {}\ngvfs_prefetch_bytes_total {}\ngvfs_prefetch_trailer_cache_hits_total {}\ngvfs_objects_served_total {}\ngvfs_object_misses_total {}\ngvfs_miss_fetch_rounds_total {}\ngvfs_miss_fetched_objects_total {}\ngvfs_manifest_generation {}\ngvfs_slabs {}\ngvfs_live_objects {}\ngvfs_alloc_sealed {}\ngvfs_alloc_post_seal_total {}\ngvfs_alloc_hot_path_total {}\ngvfs_alloc_hot_path_bytes_total {}\ngvfs_frame_ring_busy_total {}\n",
            self.metrics.requests.load(Ordering::Relaxed),
            self.metrics.prefetch_bytes.load(Ordering::Relaxed),
            self.metrics.trailer_hits.load(Ordering::Relaxed),
            self.metrics.objects_served.load(Ordering::Relaxed),
            self.metrics.misses.load(Ordering::Relaxed),
            self.misses.rounds.load(Ordering::Relaxed),
            self.misses.fetched_objects.load(Ordering::Relaxed),
            cat.generation,
            cat.probe_count().saturating_add(cat.weekly.len()),
            cat.l0.as_ref().map_or(0, |l| l.meta.object_count),
            u8::from(a.sealed),
            a.post_seal,
            a.hot_path,
            a.hot_path_bytes,
            crate::frames::BUSY_TOTAL.load(Ordering::Relaxed),
        );
        Bytes::from(s)
    }
}

fn config_json(cache_servers: &[String]) -> Bytes {
    let mut s = String::from("{\"AllowedGvfsClientVersions\":[],\"CacheServers\":[");
    for (i, cs) in cache_servers.iter().enumerate() {
        let (name, url) = cs.split_once('=').unwrap_or(("default", cs.as_str()));
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            "{{\"Url\":\"{url}\",\"Name\":\"{name}\",\"GlobalDefault\":{}}}",
            i == 0
        ));
    }
    s.push_str("]}");
    Bytes::from(s)
}

/// Boots and serves until the process is terminated.
pub fn run(
    cfg: ServeConfig,
    upstream: Option<(Box<dyn Upstream + Send>, String)>,
) -> Result<(), ServeError> {
    let mut log = std::io::stderr();
    let store = FsStore::open(&cfg.store)?;
    let t0 = std::time::Instant::now();
    let catalog = match store.load_manifest()? {
        Some(m) => Catalog::load(&store, m)?,
        None => {
            let _ = writeln!(
                log,
                "store {} has no manifest yet: serving an empty catalog until the pipeline publishes one",
                cfg.store.display()
            );
            Catalog::default()
        }
    };
    let _ = writeln!(
        log,
        "catalog generation {} mapped in {:.2}s: {} slabs, {} objects (tip {})",
        catalog.generation,
        t0.elapsed().as_secs_f64(),
        catalog.probe_count().saturating_add(catalog.weekly.len()),
        catalog.probe_keys(),
        catalog.manifest.tip
    );
    // Arenas sized for the current catalog (grown lazily after a bigger manifest lands).
    let max_entries = 1usize << 20;
    let pool = ArenaPool::new(
        cfg.arenas,
        cfg.arena_bytes,
        max_entries,
        catalog.probe_keys().saturating_add(1 << 20),
    );
    let state = Arc::new(State {
        catalog: RwLock::new(Arc::new(catalog)),
        pool,
        trailers: Arc::new(TrailerCache::new()),
        config_json: config_json(&cfg.cache_servers),
        metrics: Metrics::default(),
        misses: miss::MissQueue::new(upstream.is_some()),
        warm: (std::sync::Mutex::new(true), std::sync::Condvar::new()),
    });

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let listener = runtime.block_on(TcpListener::bind(&cfg.bind))?;
    let _ = writeln!(
        log,
        "listening on http://{} ({} arenas × {} MiB)",
        listener.local_addr()?,
        cfg.arenas,
        cfg.arena_bytes >> 20
    );

    // Live sync + manifest polling on a plain thread (synchronous upstream I/O).
    {
        let state = Arc::clone(&state);
        let store = store.clone();
        let live_cfg = live::LiveConfig {
            poll: Duration::from_secs(cfg.poll_secs),
            manifest_poll: Duration::from_secs(cfg.manifest_poll_secs),
            l0_bytes: cfg.l0_bytes,
            ingest: IngestConfig::default(),
        };
        std::thread::Builder::new()
            .name("gvfs-live".into())
            .spawn(move || {
                let mut log = std::io::stderr();
                live::run(state, store, upstream, live_cfg, &mut log);
            })?;
    }

    // Cold-selection checksums are computed ahead of the first cold clone, and again after
    // every catalog change (publish, live tick, miss round), off the request path.
    {
        let state = Arc::clone(&state);
        std::thread::Builder::new()
            .name("gvfs-warm".into())
            .spawn(move || {
                let mut log = std::io::stderr();
                loop {
                    state.wait_for_catalog_change();
                    if let Some((took, bytes)) =
                        body::warm_cold_selection(&state.catalog(), &state.trailers)
                    {
                        let _ = writeln!(
                            log,
                            "cold selection checksums warmed in {:.2}s ({} bytes)",
                            took.as_secs_f64(),
                            bytes
                        );
                    }
                }
            })?;
    }

    if let Some(mode) = cfg.guard {
        gvfs_alloc_guard::set_mode(mode);
        gvfs_alloc_guard::seal();
    }

    runtime.block_on(async move {
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => continue,
            };
            let _ = stream.set_nodelay(true);
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| handlers::handle(Arc::clone(&state), req));
                let _ = ConnBuilder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    })
}
