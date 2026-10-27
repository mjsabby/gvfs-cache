//! `gvfs-daemon serve`: the GVFS server. The same code is the remote edge
//! (`--bind 0.0.0.0:8080`) and the localhost proxy (`--bind 127.0.0.1:8080`).
//!
//! Boot (also crash recovery — there is no other state):
//! 1. read `manifest.bin` (waiting for genesis if the store is empty);
//! 2. hydrate its slabs: open them in place (directory store) or download them into
//!    `--cache-dir`, then map them;
//! 3. with `--upstream`, rebuild the live tip: `ingest_git_delta(manifest.tip, HEAD)`,
//!    repacked into the in-RAM L0 slab — before the port opens;
//! 4. allocate the connection buffers and request arenas, start the runtime, then seal the
//!    allocator and serve.
//!
//! A live thread then polls the manifest and the upstream every `--poll-ms`. The L0 slabs
//! bridge the minutes since the last sealed 15m slab, one per move of the branch, and every
//! commit in them is fetched from the upstream once:
//!
//! * a moved branch: `ingest_git_delta(newest boundary, HEAD)` fetches only the new
//!   commits, repacked into one more L0 slab, stamped from its commits
//!   ([`crate::timeline`]) exactly as the pipeline will stamp the same tip;
//! * a new manifest swaps in its slabs. L0 slabs it covers stay in the timeline for warm
//!   clients for a few hours ([`L0_RETAIN`]); the ones after its tip stay live.
//!
//! Every change publishes a new immutable [`view::View`].
//!
//! **Stores with `--blobs separate`** ([`crate::separate`]): the slabs hold commits and
//! trees, each with its sizes table; blobs come from the blob packs `blobs.bin` lists
//! ([`blobs::BlobSet`], polled like the manifest), or, for the L0 overlay's (not copied
//! until the pipeline seals them), from the upstream through [`blobs::Proxy`]. A manifest is
//! applied only once the `blobs.bin` generation it requires is loaded, and `/healthz`
//! reports ready only once `blobs.bin` is complete (every blob of the history mirrored).
//! A sealed tree's blob missing from a complete store is a fault: still served through the
//! proxy, but counted (`gvfs_blobs_missing_total`) and logged. L0 fetches are `blob:none`,
//! with the sizes their trees introduce asked of the upstream.
//!
//! Requests are served on a tokio runtime (`--workers` threads, one per CPU by default), one
//! task per connection ([`conn`]): an idle client costs a parked task, not a thread, and a
//! slow one holds no thread while its response drains. HTTP/1.1 is the edge's own
//! ([`httpd`]); large slab ranges leave through `sendfile(2)`. POSTs lease a request arena.
//! Handlers yield in their long loops ([`budget`]), so a heavy request does not hold up the
//! small ones queued behind it.

pub mod blobcache;
pub mod blobs;
pub mod budget;
pub mod conn;
pub mod handlers;
pub mod httpd;
pub mod view;

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::arena::ArenaPool;
use crate::ingest::{Dest, Indexer, ingest_git_delta_with};
use crate::manifest::{MANIFEST_KEY, Manifest};
use crate::repack::{Mode, Repacker};
use crate::slab::{Slab, Tier};
use crate::splice::Resolver;
use crate::store::Store;
use crate::upstream::Upstream;
use crate::util::{Context, Oid, Result};
use handlers::Metrics;
use view::{Prefetch, View};

/// Which prefetch clients get the pack's `.idx`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IdxPolicy {
    /// Everyone but git itself (gvfs-helper re-indexes every pack and would discard it).
    Auto,
    Always,
    Never,
}

/// Shared by every connection.
pub struct Ctx {
    view: RwLock<Arc<View>>,
    /// Buffers of the connections with a request in progress.
    pub bufs: conn::Pool,
    pub deadlines: httpd::Deadlines,
    /// What connection buffers may grow by, together, for request bodies and answers larger
    /// than one buffer.
    pub spare: httpd::Spare,
    /// The largest request body, and the largest answer built in a connection's buffer (a
    /// sizes answer).
    pub body_max: usize,
    pub out_max: usize,
    /// Connections waiting for their next request (to within `idle_fold` per thread: see
    /// [`conn`]), and how many may.
    pub idle: AtomicI64,
    pub idle_fold: u64,
    pub max_idle: i64,
    pub arenas: ArenaPool,
    pub arena_wait: Duration,
    pub config_json: Vec<u8>,
    pub metrics: Metrics,
    pub ready: AtomicBool,
    pub prefetch_idx: IdxPolicy,
    /// A client was sent an `.idx`: the warmer builds the next cold plan's index too.
    pub idx_wanted: AtomicBool,
    /// A client asked for sizes: the warmer builds the next cold plan's `GSZ1` section too.
    pub sizes_wanted: AtomicBool,
    /// Read-through of blobs the store lacks (`--blobs separate` stores with an upstream).
    pub proxy: Option<blobs::Proxy>,
    /// Local blocks of blob packs in object storage.
    pub blob_cache: Option<Arc<blobcache::BlockCache>>,
}

impl Ctx {
    /// The current snapshot (an `Arc` clone: no allocation).
    pub fn view(&self) -> Arc<View> {
        match self.view.read() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    fn publish(&self, v: Arc<View>) {
        match self.view.write() {
            Ok(mut g) => *g = v,
            Err(p) => *p.into_inner() = v,
        }
    }
}

pub struct Options {
    pub store: Store,
    pub upstream: Option<Upstream>,
    /// The upstream's spec, branch and credentials (for the blob proxy's connections).
    pub upstream_spec: Option<(String, String, crate::upstream::Auth)>,
    /// Bytes of proxied blobs kept on local disk (`--proxy-cache-mib`).
    pub proxy_cache_bytes: u64,
    /// Bytes of remote blob-pack blocks kept on local disk (`--blob-cache-mib`).
    pub blob_cache_bytes: u64,
    pub bind: String,
    pub cache_dir: PathBuf,
    pub poll: Duration,
    /// Runtime worker threads.
    pub workers: usize,
    pub arenas: usize,
    pub arena_bytes: usize,
    pub arena_objects: usize,
    /// Objects commit expansions may plan beyond their arenas' size, together
    /// (`--grow-objects`), and the most one may plan (`--max-request-objects`).
    pub grow_objects: usize,
    pub max_request_objects: usize,
    pub cache_servers: Vec<(String, String)>,
    pub idle_timeout: Duration,
    pub write_timeout: Duration,
    pub prefetch_idx: IdxPolicy,
    /// Idle keep-alive connections that can wait at once (beyond that, a connection that
    /// goes idle is closed: its client reconnects).
    pub max_idle_conns: usize,
    /// Sealed rollups of this tier and above (24h, or the weekly delta) are delivered whole
    /// to the warm clients inside them, once ([`view`]); `None`: exact covers only.
    pub supersets: Option<Tier>,
}

/// `gvfs-daemon serve ...`
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
            "bind",
            "cache-dir",
            "poll-ms",
            "workers",
            "arenas",
            "arena-mib",
            "arena-objects",
            "grow-objects",
            "max-request-objects",
            "cache-server",
            "alloc-guard",
            "idle-secs",
            "max-idle-conns",
            "quantum-us",
            "prefetch-idx",
            "superset-tier",
            "proxy-cache-mib",
            "blob-cache-mib",
        ],
    )?;
    budget::set_quantum(Duration::from_micros(f.num(
        "quantum-us",
        u64::try_from(budget::QUANTUM.as_micros()).unwrap_or(20),
    )?));
    match f.get("alloc-guard").unwrap_or("count") {
        "count" => gvfs_alloc_guard::set_mode(gvfs_alloc_guard::Mode::Count),
        "strict" => gvfs_alloc_guard::set_mode(gvfs_alloc_guard::Mode::Strict),
        "trace" => gvfs_alloc_guard::set_trace(true),
        other => crate::bail!("--alloc-guard must be count, strict or trace, not {other:?}"),
    }
    let upstream_spec = match f.get("upstream") {
        Some(u) => Some((
            u.to_string(),
            f.get("branch").unwrap_or("main").to_string(),
            crate::ingest::auth_from_flags(&f)?,
        )),
        None => None,
    };
    let upstream = match &upstream_spec {
        Some((u, branch, auth)) => Some(Upstream::open(u, branch, auth)?),
        None => None,
    };
    let mut cache_servers = Vec::new();
    for cs in f.all("cache-server") {
        let (name, url) = cs.split_once('=').ctx("--cache-server takes NAME=URL")?;
        cache_servers.push((name.to_string(), url.to_string()));
    }
    let prefetch_idx = match f.get("prefetch-idx").unwrap_or("auto") {
        "auto" => IdxPolicy::Auto,
        "always" => IdxPolicy::Always,
        "never" => IdxPolicy::Never,
        other => crate::bail!("--prefetch-idx must be auto, always or never, not {other:?}"),
    };
    let supersets = match f.get("superset-tier").unwrap_or("24h") {
        "24h" => Some(Tier::H24),
        "168h" => Some(Tier::Week),
        "never" => None,
        other => crate::bail!("--superset-tier must be 24h, 168h or never, not {other:?}"),
    };
    let cores = crate::util::default_threads();
    let arenas = f.num("arenas", cores.saturating_mul(2))?;
    let arena_objects = f.num("arena-objects", 1usize << 18)?;
    let opts = Options {
        store: Store::open(
            f.require("store")?,
            f.get("store-auth-env")
                .unwrap_or("AZURE_STORAGE_CONNECTION_STRING"),
        )?,
        upstream,
        upstream_spec,
        proxy_cache_bytes: f.num("proxy-cache-mib", 10240u64)?.saturating_mul(1 << 20),
        blob_cache_bytes: f.num("blob-cache-mib", 32768u64)?.saturating_mul(1 << 20),
        bind: f.get("bind").unwrap_or("127.0.0.1:8080").to_string(),
        cache_dir: f
            .get("cache-dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("gvfs-daemon-cache")),
        poll: Duration::from_millis(f.num("poll-ms", 2000u64)?),
        workers: f.num("workers", cores)?,
        arenas,
        arena_bytes: f.num("arena-mib", 8usize)?.saturating_mul(1 << 20),
        arena_objects,
        // As much again as the arenas hold.
        grow_objects: f.num("grow-objects", arenas.saturating_mul(arena_objects))?,
        max_request_objects: f.num("max-request-objects", 1usize << 22)?,
        cache_servers,
        // At most a year: deadlines are instants, which must not overflow.
        idle_timeout: Duration::from_secs(f.num("idle-secs", 30u64)?.min(365 * 86400)),
        write_timeout: Duration::from_secs(120),
        prefetch_idx,
        max_idle_conns: f.num("max-idle-conns", 65536usize)?,
        supersets,
    };
    run(opts)
}

/// `{"AllowedGvfsClientVersions":null,"CacheServers":[...]}` (compact: gvfs-helper
/// looks for `"Url":"<url>"`).
fn config_json(servers: &[(String, String)]) -> Vec<u8> {
    let mut s = String::from("{\"AllowedGvfsClientVersions\":null,\"CacheServers\":[");
    for (i, (name, url)) in servers.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let esc = |v: &str| v.replace('\\', "\\\\").replace('"', "\\\"");
        s.push_str(&format!(
            "{{\"Url\":\"{}\",\"Name\":\"{}\",\"GlobalDefault\":{}}}",
            esc(url),
            esc(name),
            i == 0
        ));
    }
    s.push_str("]}");
    s.into_bytes()
}

pub fn run(o: Options) -> Result<()> {
    let Options {
        store,
        upstream,
        upstream_spec,
        proxy_cache_bytes,
        blob_cache_bytes,
        bind,
        cache_dir,
        poll,
        workers,
        arenas,
        arena_bytes,
        arena_objects,
        grow_objects,
        max_request_objects,
        cache_servers,
        idle_timeout,
        write_timeout,
        prefetch_idx,
        max_idle_conns,
        supersets,
    } = o;
    let store = Arc::new(store);
    let mut live = Live {
        store,
        blob_cache: None,
        upstream,
        tip: LiveTip::default(),
        cache_dir,
        opened: HashMap::new(),
        sealed: Vec::new(),
        l0: Vec::new(),
        last_head: Oid::ZERO,
        generation: 0,
        supersets,
        blobs: None,
        waiting_for: 0,
    };

    // 1-3. Manifest, slabs, live tip.
    let (manifest, etag) = live.wait_for_manifest()?;
    let separate = manifest.separate_blobs();
    if separate {
        // Blob packs in object storage are read in blocks kept on local disk.
        if live.store.local_path(crate::blobs::BLOBS_KEY).is_none() && blob_cache_bytes > 0 {
            live.blob_cache = Some(Arc::new(blobcache::BlockCache::new(
                Arc::clone(&live.store),
                live.cache_dir.join("blob-blocks"),
                blob_cache_bytes,
            )?));
        }
        live.tip.filter = Some("blob:none".to_string());
        // The manifest's trees need the blobs.bin generation it names.
        loop {
            live.load_blobs()?;
            if live.blobs_generation() >= manifest.blobs_generation {
                break;
            }
            crate::log!(
                "serve: waiting for blobs.bin generation {} (have {})",
                manifest.blobs_generation,
                live.blobs_generation()
            );
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    live.open_manifest(&manifest)?;
    let first = live.snapshot(manifest, etag);
    let proxy = match (&upstream_spec, separate) {
        (Some((u, _, auth)), true) => Some(blobs::Proxy::new(
            u,
            auth,
            &live.cache_dir.join("proxy"),
            proxy_cache_bytes,
        )?),
        _ => None,
    };
    let workers = workers.max(1);
    let ctx = Arc::new(Ctx {
        view: RwLock::new(Arc::new(first)),
        bufs: conn::Pool::new(workers, CONN_BUFS, OUT_BYTES),
        // Bodies and answers may take, together, what one body and one answer per arena
        // would (as when bodies and answers lived in the arenas).
        spare: httpd::Spare::new(
            arenas.max(1).saturating_mul(
                arena_bytes
                    .saturating_div(4)
                    .saturating_add(arena_bytes / 2),
            ),
        ),
        body_max: arena_bytes / 4,
        out_max: arena_bytes / 2,
        deadlines: httpd::Deadlines {
            idle: idle_timeout,
            patience: idle_timeout,
            write: write_timeout,
        },
        idle: AtomicI64::new(0),
        // Exact for small caps; for the default, within 64 per thread.
        idle_fold: u64::try_from(
            max_idle_conns
                .checked_div(workers.saturating_mul(64))
                .unwrap_or(1),
        )
        .unwrap_or(1)
        .clamp(1, 64),
        max_idle: i64::try_from(max_idle_conns).unwrap_or(i64::MAX),
        arenas: ArenaPool::new(
            arenas.max(1),
            arena_bytes,
            arena_objects,
            grow_objects,
            max_request_objects,
        ),
        arena_wait: Duration::from_secs(10),
        config_json: config_json(&cache_servers),
        metrics: Metrics::default(),
        ready: AtomicBool::new(false),
        prefetch_idx,
        idx_wanted: AtomicBool::new(prefetch_idx == IdxPolicy::Always),
        sizes_wanted: AtomicBool::new(false),
        proxy,
        blob_cache: live.blob_cache.clone(),
    });
    if let Err(e) = live.sync_upstream(&ctx) {
        crate::log!("serve: live tip not caught up at boot ({e}); serving sealed slabs");
    }
    let warmer = Arc::new(Warmer::default());
    warmer.offer(ctx.view());

    // 4. The runtime and its buffers, then seal.
    raise_fd_limit();
    let listener = TcpListener::bind(&bind).ctx(&format!("binding {bind}"))?;
    // std listens with a backlog of 128: a burst of new clients (a fleet starting at 09:00,
    // every client reconnecting after a restart) overflows it, and the rest wait for SYN
    // retries (1 s, 3 s, ...). Listening again raises the queue to the system's limit
    // (`net.core.somaxconn`).
    if let Err(e) = nix::sys::socket::listen(&listener, nix::sys::socket::Backlog::MAXALLOWABLE) {
        crate::log!("serve: could not raise the listen backlog: {e}");
    }
    listener.set_nonblocking(true)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("gvfs-w")
        .enable_all()
        .build()
        .ctx("starting the runtime")?;
    let listener = {
        let _guard = rt.enter();
        tokio::net::TcpListener::from_std(listener).ctx("registering the listener")?
    };
    rt.spawn(accept_loop(Arc::clone(&ctx), listener));
    {
        let warmer = warmer.clone();
        let ctx = ctx.clone();
        std::thread::Builder::new()
            .name("gvfs-warmer".into())
            .spawn(move || warmer.run(&ctx))?;
    }
    // Every worker has started and run a task before the allocator is sealed.
    rt.block_on(async {
        let mut started = Vec::with_capacity(workers);
        for _ in 0..workers {
            started.push(tokio::spawn(async { tokio::task::yield_now().await }));
        }
        for t in started {
            let _ = t.await;
        }
    });
    gvfs_alloc_guard::seal();
    ctx.ready.store(true, Ordering::Relaxed);
    let v = ctx.view();
    crate::log!(
        "serving on http://{bind}: {} slabs ({} in L0), tip {}, {}, {workers} workers",
        v.all.len(),
        v.l0_len,
        v.tip,
        live.store.describe()
    );

    // The live loop runs on this thread for the life of the process.
    loop {
        std::thread::sleep(poll);
        if let Err(e) = live.tick(&ctx, &warmer) {
            crate::log!("serve: live sync failed: {e}");
        }
    }
}

/// The store's current manifest as a [`View`], as `serve` boots it without an upstream
/// (experiments that drive the handlers' building blocks themselves).
#[doc(hidden)]
pub fn load_view(store: Store, cache_dir: PathBuf) -> Result<Arc<View>> {
    let mut live = Live {
        store: Arc::new(store),
        blob_cache: None,
        upstream: None,
        tip: LiveTip::default(),
        cache_dir,
        opened: HashMap::new(),
        sealed: Vec::new(),
        l0: Vec::new(),
        last_head: Oid::ZERO,
        generation: 0,
        supersets: Some(Tier::H24),
        blobs: None,
        waiting_for: 0,
    };
    let (manifest, etag) = live.wait_for_manifest()?;
    if manifest.separate_blobs() {
        live.load_blobs()?;
        if live.blobs_generation() < manifest.blobs_generation {
            crate::bail!(
                "blobs.bin is at generation {}, the manifest needs {}",
                live.blobs_generation(),
                manifest.blobs_generation
            );
        }
    }
    live.open_manifest(&manifest)?;
    Ok(Arc::new(live.snapshot(manifest, etag)))
}

/// Raises the soft open-file limit to the hard limit: every idle client holds a descriptor.
pub fn raise_fd_limit() {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};
    if let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_NOFILE) {
        if soft < hard {
            let _ = setrlimit(Resource::RLIMIT_NOFILE, hard, hard);
        }
    }
}

/// Connection buffers made at startup (more are made while they run out).
const CONN_BUFS: usize = 256;
/// Bytes of each connection's output buffer (the batch a response coalesces).
const OUT_BYTES: usize = 256 << 10;

/// Accepts connections for the life of the process: one task each.
async fn accept_loop(ctx: Arc<Ctx>, listener: tokio::net::TcpListener) {
    let mut logged: Option<Instant> = None;
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(conn::HotPath::new(conn::serve(Arc::clone(&ctx), stream)));
            }
            Err(e) => {
                // Out of descriptors, or a connection reset before it was accepted: back off
                // briefly instead of spinning, and say so at most once a second.
                if logged.is_none_or(|t| t.elapsed() >= Duration::from_secs(1)) {
                    crate::log!("serve: accept failed: {e}");
                    logged = Some(Instant::now());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

/// Prepares plans in the background, so clients do not wait for them: the newest
/// snapshot's cold-clone plan (its SHA-1 trailer, and its `.idx` once clients have asked
/// for one), and the plan clients up to date with the previous snapshot ask for next.
#[derive(Default)]
struct Warmer {
    latest: Mutex<Option<Arc<View>>>,
    wake: Condvar,
}

impl Warmer {
    fn offer(&self, v: Arc<View>) {
        if let Ok(mut g) = self.latest.lock() {
            *g = Some(v);
            self.wake.notify_one();
        }
    }

    fn run(&self, ctx: &Ctx) {
        let mut prev_tip_ts = 0u64;
        loop {
            let view = {
                let Ok(mut g) = self.latest.lock() else {
                    return;
                };
                while g.is_none() {
                    g = match self.wake.wait(g) {
                        Ok(g) => g,
                        Err(_) => return,
                    };
                }
                g.take()
            };
            let Some(view) = view else { continue };
            let idx =
                ctx.prefetch_idx != IdxPolicy::Never && ctx.idx_wanted.load(Ordering::Relaxed);
            if let Some(plan) = view.cold_plan() {
                let t0 = std::time::Instant::now();
                plan.trailer();
                if ctx.sizes_wanted.load(Ordering::Relaxed) {
                    let _ = plan.sizes_section();
                }
                let idx_len = idx.then(|| plan.idx().map_or(0, <[u8]>::len));
                if plan.len > (64 << 20) {
                    let idx_len = idx_len.map_or_else(String::new, |n| {
                        format!(
                            " + .idx ({})",
                            crate::util::human_bytes(crate::util::u64_of(n))
                        )
                    });
                    crate::log!(
                        "serve: cold-clone pack ({}){idx_len} ready in {:.2}s",
                        crate::util::human_bytes(plan.len),
                        t0.elapsed().as_secs_f64()
                    );
                }
            }
            if prev_tip_ts != 0 && prev_tip_ts < view.tip_ts {
                if let Prefetch::Pack(plan, _) = view.prefetch(prev_tip_ts) {
                    plan.trailer();
                    if idx {
                        let _ = plan.idx();
                    }
                }
            }
            prev_tip_ts = view.tip_ts;
        }
    }
}

/// How long an L0 slab stays in the timeline after the manifest covers it (in timestamp
/// seconds): a client whose cursor is one of its boundaries is answered exactly until then.
pub const L0_RETAIN: u64 = 6 * 3600;

/// The live tip's L0 slabs, one per move of the upstream branch, oldest first: how they
/// follow the upstream and how a new manifest prunes them. The server's live thread runs
/// this on the wall clock; `examples/clients.rs` runs it on a simulated one.
pub struct LiveTip {
    pub indexer: Indexer,
    pub repacker: Repacker,
    resolver: Resolver,
    /// Stamps of the commits in unsealed L0 slabs: parents later pushes may name.
    stamps: HashMap<Oid, u64>,
    /// Fetch filter (`blob:none` for `--blobs separate` stores).
    pub filter: Option<String>,
    /// Boundaries this edge handed out that a forced push (or a manifest that stamped the
    /// branch otherwise) took away: `(from, upto)`, a cursor in `(from, upto]` that is no
    /// longer a boundary stands for `from` ([`View::prefetch`]). Oldest first, at most
    /// [`RETRACTED_MAX`].
    retracted: Vec<(u64, u64)>,
}

/// Retracted boundaries an edge remembers (a few per forced push under its live L0).
pub const RETRACTED_MAX: usize = 1024;

impl Default for LiveTip {
    fn default() -> Self {
        LiveTip {
            indexer: Indexer::new(1),
            repacker: Repacker::new(),
            resolver: Resolver::default(),
            stamps: HashMap::new(),
            filter: None,
            retracted: Vec::new(),
        }
    }
}

/// How the L0 slabs follow a move of the upstream branch ([`LiveTip::advance`]).
pub struct Moved {
    /// How many of the L0 slabs stay: all of them for a push that descends from the newest
    /// one, fewer when a forced push left the branch without the newest ones.
    pub keep: usize,
    /// The slab for what the new head adds after them (`None`: nothing, e.g. a rewind).
    pub slab: Option<Arc<Slab>>,
}

impl LiveTip {
    /// How the L0 slabs follow the upstream branch's move to `head`. A push that descends
    /// from the newest boundary (the newest live L0 slab's tip, or the manifest's) adds a
    /// slab: the commits since, fetched once, repacked, and stamped from its commits. A
    /// forced push drops the live L0 slabs the branch no longer reaches and adds what it
    /// has after the newest one it does, stamped from that boundary, as the next pipeline
    /// run will (it never saw what was dropped); their boundaries are retracted. `None`
    /// when nothing changes.
    pub fn advance(
        &mut self,
        up: &mut Upstream,
        m: &Manifest,
        l0: &[Arc<Slab>],
        head: Oid,
    ) -> Result<Option<Moved>> {
        // The live slabs (after the manifest's tip): a chain from it, each slab from the
        // previous one's tip.
        let first_live = l0
            .iter()
            .position(|s| s.meta.start_ts >= m.max_ts)
            .unwrap_or(l0.len());
        let live = l0.get(first_live..).unwrap_or(&[]);
        let (from, from_ts) = live
            .last()
            .map_or((m.tip, m.max_ts), |s| (s.meta.want, s.meta.end_ts));
        if from == head {
            return Ok(None);
        }
        let raw = self.fetch(up, m, (from, from_ts), head)?;
        // Every push but a forced one descends from the newest boundary. Without live
        // slabs that is the manifest's tip, from which the pipeline fetches too, forced or
        // not: nothing to drop.
        let forward = live.is_empty()
            || match &raw {
                Some(r) => self
                    .reached(head, Some(&r.slab), live, from)?
                    .contains(&from),
                None => false,
            };
        if forward {
            let Some(raw) = raw else {
                return Ok(None);
            };
            let slab = self.slab(raw, m, (from, from_ts), head)?;
            return Ok(slab.map(|s| Moved {
                keep: l0.len(),
                slab: Some(s),
            }));
        }
        // A forced push: keep the live slabs up to the newest tip the branch still reaches.
        let reached = self.reached(head, raw.as_ref().map(|r| &*r.slab), live, Oid::ZERO)?;
        let kept = live
            .iter()
            .rposition(|s| reached.contains(&s.meta.want))
            .map_or(0, |j| j.saturating_add(1));
        let (fork, fork_ts) = live
            .get(..kept)
            .and_then(<[_]>::last)
            .map_or((m.tip, m.max_ts), |s| (s.meta.want, s.meta.end_ts));
        let dropped = live.get(kept..).unwrap_or(&[]);
        if let Some(upto) = dropped.iter().map(|s| s.meta.end_ts).max() {
            self.retract(fork_ts, upto);
        }
        let kept_slabs = live.get(..kept).unwrap_or(&[]);
        self.stamps
            .retain(|c, _| kept_slabs.iter().any(|s| s.find(c).is_some()));
        let keep = first_live.saturating_add(kept);
        if fork == head {
            return Ok(Some(Moved { keep, slab: None }));
        }
        // What the branch has after that boundary, fetched again from it (the first fetch
        // left out what the dropped commits reach, which the branch may share).
        let slab = match self.fetch(up, m, (fork, fork_ts), head)? {
            Some(raw) => self.slab(raw, m, (fork, fork_ts), head)?,
            None => None,
        };
        Ok(Some(Moved { keep, slab }))
    }

    /// The commits `head` adds after boundary `from` (stamped `from_ts`), fetched once with
    /// the manifest's boundaries as the other haves; `None` if it adds none.
    fn fetch(
        &mut self,
        up: &mut Upstream,
        m: &Manifest,
        (from, from_ts): (Oid, u64),
        head: Oid,
    ) -> Result<Option<crate::ingest::Ingested>> {
        let mut haves = vec![from];
        haves.extend(m.known_tips().into_iter().filter(|t| *t != from));
        ingest_git_delta_with(
            up,
            &mut self.indexer,
            &haves,
            head,
            self.filter.as_deref(),
            Tier::L0,
            (from_ts, from_ts),
            Dest::Memory,
        )
    }

    /// The commits of the `live` L0 slabs that `head` reaches, through `raw` (the commits
    /// a fetch of it added) and those slabs; the walk ends early at `stop`. Sealed history
    /// is not walked: no sealed commit descends from a live one.
    fn reached(
        &mut self,
        head: Oid,
        raw: Option<&Slab>,
        live: &[Arc<Slab>],
        stop: Oid,
    ) -> Result<std::collections::HashSet<Oid>> {
        let mut reached = std::collections::HashSet::new();
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![head];
        while let Some(c) = stack.pop() {
            if !seen.insert(c) {
                continue;
            }
            let found = match raw.and_then(|r| Some((r, r.find(&c)?))) {
                Some(hit) => Some(hit),
                None => live
                    .iter()
                    .find_map(|s| Some((&**s, s.find(&c)?)))
                    .inspect(|_| {
                        reached.insert(c);
                    }),
            };
            if c == stop && reached.contains(&c) {
                break;
            }
            let Some((slab, slot)) = found else {
                continue;
            };
            let (kind, commit) = self.resolver.resolve(slab, slot)?;
            if kind == crate::pack::OBJ_COMMIT {
                stack.extend(crate::object::commit_parents(commit));
            }
        }
        Ok(reached)
    }

    /// Clients holding a boundary in `(from, upto]` are answered from `from` while that
    /// boundary is gone (see [`LiveTip::retracted`]).
    fn retract(&mut self, from: u64, upto: u64) {
        if upto <= from {
            return;
        }
        if self.retracted.len() >= RETRACTED_MAX {
            self.retracted.remove(0);
        }
        self.retracted.push((from, upto));
    }

    /// Boundaries this edge handed out that are gone: `(from, upto]` stands for `from`.
    pub fn retracted(&self) -> &[(u64, u64)] {
        &self.retracted
    }

    /// The L0 slab of `raw`, the commits `head` adds after boundary `from`: stamped from
    /// what the pipeline will know too, then repacked.
    fn slab(
        &mut self,
        raw: crate::ingest::Ingested,
        m: &Manifest,
        (from, from_ts): (Oid, u64),
        head: Oid,
    ) -> Result<Option<Arc<Slab>>> {
        // Stamped from what the pipeline will know too: the manifest's boundaries and the
        // unsealed L0 commits (which the pipeline will fetch and stamp alike).
        let mut known = crate::timeline::boundaries(m);
        known.extend(self.stamps.iter().map(|(c, t)| (*c, *t)));
        let ts = crate::timeline::stamp(&raw.slab, &mut self.resolver, head, &mut known, from_ts)?;
        for rec in raw
            .slab
            .records()
            .filter(|r| r.kind == crate::pack::OBJ_COMMIT)
        {
            if let Some(&t) = known.get(&rec.oid) {
                self.stamps.insert(rec.oid, t);
            }
        }
        let input = crate::repack::Input {
            slabs: std::slice::from_ref(&raw.slab),
            frozen: None,
            exclude: &[],
            mode: Mode::Trees,
        };
        Ok(self
            .repacker
            .repack(
                &mut self.indexer,
                &input,
                Tier::L0,
                (from_ts, ts),
                (from, head),
                Dest::Memory,
            )?
            .map(|x| x.slab))
    }

    /// The L0 slabs once manifest `m` (whose slabs are `sealed`) replaces `old`. Those the
    /// manifest covers stay until they age out, the ones after its tip stay live. If the tip
    /// falls inside an L0 slab, what follows it is repacked out of that slab; if the branch
    /// was rewritten, L0 starts over from the manifest.
    pub fn prune(
        &mut self,
        l0: &[Arc<Slab>],
        old: &Manifest,
        m: &Manifest,
        sealed: &[Arc<Slab>],
    ) -> Result<Vec<Arc<Slab>>> {
        let out = match self.pruned(l0, old, m, sealed)? {
            Some(out) => out,
            None => {
                // L0 starts over from the manifest (the branch was rewritten, or the
                // pipeline stamped it otherwise): the live boundaries it handed out are
                // retracted to the old tip, which every client holding one has.
                let live = l0.iter().filter(|s| s.meta.start_ts >= old.max_ts);
                if let Some(upto) = live.map(|s| s.meta.end_ts).max() {
                    self.retract(old.max_ts, upto);
                }
                Vec::new()
            }
        };
        // Sealed commits are stamped from the manifest from now on, as the pipeline does.
        let live: Vec<&Arc<Slab>> = out.iter().filter(|s| s.meta.start_ts >= m.max_ts).collect();
        self.stamps
            .retain(|c, _| live.iter().any(|s| s.find(c).is_some()));
        Ok(out)
    }

    /// [`LiveTip::prune`]'s slabs; `None` when L0 starts over.
    fn pruned(
        &mut self,
        l0: &[Arc<Slab>],
        old: &Manifest,
        m: &Manifest,
        sealed: &[Arc<Slab>],
    ) -> Result<Option<Vec<Arc<Slab>>>> {
        let mut out = l0.to_vec();
        if !l0.is_empty() && m.tip != old.tip {
            if let Some(s) = l0.iter().find(|s| s.meta.want == m.tip) {
                if s.meta.end_ts != m.max_ts {
                    return Ok(None);
                }
            } else if l0
                .last()
                .is_some_and(|s| sealed.iter().any(|x| x.find(&s.meta.want).is_some()))
            {
                // The pipeline got ahead of L0: all of it is sealed.
            } else if let Some(j) = l0.iter().position(|s| s.find(&m.tip).is_some()) {
                let Some(s) = l0.get(j).filter(|s| s.meta.end_ts > m.max_ts) else {
                    return Ok(None);
                };
                let input = crate::repack::Input {
                    slabs: std::slice::from_ref(s),
                    frozen: None,
                    exclude: sealed,
                    mode: Mode::Trees,
                };
                if let Some(rest) = self.repacker.repack(
                    &mut self.indexer,
                    &input,
                    Tier::L0,
                    (m.max_ts, s.meta.end_ts),
                    (m.tip, s.meta.want),
                    Dest::Memory,
                )? {
                    // What follows the tip introduces no sizes its source slab did not.
                    if let Some(t) = s.sizes() {
                        rest.slab.set_sizes(Arc::clone(t));
                    }
                    out.insert(j.saturating_add(1), rest.slab);
                }
            } else {
                return Ok(None);
            }
        }
        retain_l0(&mut out, m.max_ts);
        Ok(Some(out))
    }
}

/// Drops the sealed L0 slabs (ending at or before `max_ts`, the manifest's tip) that ended
/// more than [`L0_RETAIN`] before the newest boundary. Unsealed ones hold the live tip and
/// always stay, however long the pipeline is away.
pub fn retain_l0(l0: &mut Vec<Arc<Slab>>, max_ts: u64) {
    let newest = l0.last().map_or(max_ts, |s| s.meta.end_ts.max(max_ts));
    l0.retain(|s| keep_l0(s.meta.end_ts, max_ts, newest));
}

fn keep_l0(end_ts: u64, max_ts: u64, newest: u64) -> bool {
    end_ts > max_ts || end_ts.saturating_add(L0_RETAIN) >= newest
}

/// The live thread's state.
struct Live {
    store: Arc<Store>,
    blob_cache: Option<Arc<blobcache::BlockCache>>,
    upstream: Option<Upstream>,
    tip: LiveTip,
    cache_dir: PathBuf,
    /// Sealed slabs already open, by id (reused across manifests).
    opened: HashMap<String, Arc<Slab>>,
    /// The current manifest's slabs, in manifest order.
    sealed: Vec<Arc<Slab>>,
    /// The L0 slabs, oldest first.
    l0: Vec<Arc<Slab>>,
    /// Upstream tip last ingested (or found to bring nothing).
    last_head: Oid,
    generation: u64,
    supersets: Option<Tier>,
    /// The loaded `blobs.bin` (`--blobs separate` stores).
    blobs: Option<Arc<blobs::BlobSet>>,
    /// A manifest generation held back until its `blobs.bin` generation loads (logged once).
    waiting_for: u64,
}

impl Live {
    fn blobs_generation(&self) -> u64 {
        self.blobs.as_ref().map_or(0, |b| b.generation)
    }

    /// Loads `blobs.bin` if it changed. Returns whether it did.
    fn load_blobs(&mut self) -> Result<bool> {
        let Some((bytes, etag)) = self.store.get(crate::blobs::BLOBS_KEY)? else {
            return Ok(false);
        };
        if self.blobs.as_ref().is_some_and(|b| b.etag == etag) {
            return Ok(false);
        }
        let bm = crate::blobs::BlobManifest::decode(&bytes).ctx("reading blobs.bin")?;
        let set = blobs::BlobSet::load(
            &self.store,
            &self.cache_dir.join("blobs"),
            self.blob_cache.as_ref(),
            &bm,
            etag,
            self.blobs.as_deref(),
        )?;
        crate::log!(
            "serve: blobs.bin generation {}: {} segments, {} packs{}",
            set.generation,
            set.segments(),
            set.packs(),
            if set.complete {
                ", complete"
            } else {
                " (not complete: gvfs-daemon mirror running)"
            }
        );
        self.blobs = Some(Arc::new(set));
        Ok(true)
    }

    /// The sizes the trees of a new L0 slab introduce (those `known` does not have), asked
    /// of the upstream.
    fn l0_sizes(&mut self, slab: &Arc<Slab>, known: &dyn Fn(&Oid) -> bool) -> Result<()> {
        let Some(up) = self.upstream.as_mut() else {
            return Ok(());
        };
        let unknown: Vec<Oid> = crate::blobcopy::referenced_blobs(slab, 1)?
            .into_iter()
            .filter(|b| !known(b))
            .collect();
        let sizes = up.object_sizes(&unknown)?;
        let mut pairs = Vec::with_capacity(unknown.len());
        for (oid, size) in unknown.iter().zip(sizes) {
            let Some(size) = size else {
                crate::bail!("the upstream has no blob {oid}, which a tree references");
            };
            pairs.push((*oid, size));
        }
        slab.set_sizes(Arc::new(crate::sizes::Sizes::from_heap(
            crate::sizes::build(&mut pairs),
        )?));
        Ok(())
    }
    fn wait_for_manifest(&mut self) -> Result<(Manifest, String)> {
        let mut logged = false;
        loop {
            if let Some((bytes, etag)) = self.store.get(MANIFEST_KEY)? {
                return Ok((Manifest::decode(&bytes)?, etag));
            }
            if !logged {
                crate::log!(
                    "serve: {} has no manifest yet; waiting for the pipeline's genesis run",
                    self.store.describe()
                );
                logged = true;
            }
            std::thread::sleep(Duration::from_secs(5));
        }
    }

    /// Opens (hydrating if needed) every slab of `m`; they become the sealed slabs.
    fn open_manifest(&mut self, m: &Manifest) -> Result<()> {
        let mut sealed = Vec::with_capacity(m.slabs().count());
        let mut keep = HashMap::new();
        for meta in m.slabs() {
            let slab = match self.opened.get(&meta.id) {
                Some(s) => s.clone(),
                None => {
                    let s = self.open(meta)?;
                    if m.separate_blobs() {
                        let path = match self.store.local_path(&meta.sizes_key()) {
                            Some(p) => p,
                            None => {
                                let p = self.cache_dir.join(format!("{}.sizes", meta.id));
                                hydrate(&self.store, &meta.sizes_key(), &p)?;
                                p
                            }
                        };
                        s.set_sizes(Arc::new(
                            crate::sizes::Sizes::open(&path)
                                .ctx(&format!("sizes of slab {}", meta.id))?,
                        ));
                    }
                    s
                }
            };
            keep.insert(meta.id.clone(), slab.clone());
            sealed.push(slab);
        }
        self.opened = keep;
        self.sealed = sealed;
        Ok(())
    }

    /// A snapshot of the sealed slabs plus the L0 slabs.
    fn snapshot(&mut self, m: Manifest, etag: String) -> View {
        self.generation = self.generation.saturating_add(1);
        View::new(
            self.generation,
            m,
            etag,
            self.sealed.clone(),
            self.l0.clone(),
            self.supersets,
            self.blobs.clone(),
            self.tip.retracted(),
        )
    }

    fn open(&self, meta: &crate::slab::SlabMeta) -> Result<Arc<Slab>> {
        let (pack, index) = match (
            self.store.local_path(&meta.pack_key()),
            self.store.local_path(&meta.index_key()),
        ) {
            (Some(p), Some(i)) => (p, i),
            _ => {
                let p = self.cache_dir.join(format!("{}.pack", meta.id));
                let i = self.cache_dir.join(format!("{}.mphf", meta.id));
                hydrate(&self.store, &meta.pack_key(), &p)?;
                hydrate(&self.store, &meta.index_key(), &i)?;
                (p, i)
            }
        };
        Slab::open(&pack, &index, meta.clone()).ctx(&format!("opening slab {}", meta.id))
    }

    /// One poll: a new manifest, then a moved upstream branch.
    fn tick(&mut self, ctx: &Ctx, warmer: &Warmer) -> Result<()> {
        let view = ctx.view();
        if view.manifest.separate_blobs() && self.load_blobs()? {
            let next = Arc::new(self.snapshot(view.manifest.clone(), view.etag.clone()));
            ctx.publish(next);
        }
        let view = ctx.view();
        if let Some((bytes, etag)) = self.store.get(MANIFEST_KEY)? {
            if etag != view.etag {
                let m = Manifest::decode(&bytes)?;
                let blobs_behind =
                    m.separate_blobs() && self.blobs_generation() < m.blobs_generation;
                if blobs_behind && self.waiting_for != m.generation {
                    crate::log!(
                        "serve: manifest generation {} waits for blobs.bin generation {} (have {})",
                        m.generation,
                        m.blobs_generation,
                        self.blobs_generation()
                    );
                    self.waiting_for = m.generation;
                }
                if m.generation > view.manifest.generation && !blobs_behind {
                    self.open_manifest(&m)?;
                    self.l0 = self.tip.prune(&self.l0, &view.manifest, &m, &self.sealed)?;
                    let next = self.snapshot(m, etag);
                    crate::log!(
                        "serve: manifest generation {} ({} slabs, {} L0)",
                        next.manifest.generation,
                        next.all.len(),
                        next.l0_len
                    );
                    ctx.metrics.manifest_loads.fetch_add(1, Ordering::Relaxed);
                    let next = Arc::new(next);
                    ctx.publish(next.clone());
                    warmer.offer(next);
                    // The tip may now be behind the upstream: catch up below.
                    self.last_head = Oid::ZERO;
                }
            }
        }
        if self.sync_upstream(ctx)? {
            warmer.offer(ctx.view());
        }
        Ok(())
    }

    /// Brings L0 up to the upstream branch. Returns whether a new snapshot was published.
    fn sync_upstream(&mut self, ctx: &Ctx) -> Result<bool> {
        let Some(up) = self.upstream.as_mut() else {
            return Ok(false);
        };
        let head = up.head()?;
        let view = ctx.view();
        if head == view.tip || head == self.last_head {
            return Ok(false);
        }
        let Some(moved) = self.tip.advance(up, &view.manifest, &self.l0, head)? else {
            self.last_head = head;
            return Ok(false);
        };
        let dropped: Vec<Arc<Slab>> = self.l0.get(moved.keep..).unwrap_or(&[]).to_vec();
        if let (Some(slab), true) = (&moved.slab, view.manifest.separate_blobs()) {
            // Sizes the dropped slabs' tables gave are not known once they go.
            let known = |b: &Oid| {
                view.all
                    .iter()
                    .filter(|s| !dropped.iter().any(|d| Arc::ptr_eq(d, s)))
                    .any(|s| s.sizes().is_some_and(|t| t.get(b).is_some()))
            };
            self.l0_sizes(slab, &known)?;
        }
        self.last_head = head;
        if !dropped.is_empty() {
            crate::log!(
                "serve: forced push to {head}: dropped {} L0 slabs the branch no longer reaches",
                dropped.len()
            );
        }
        self.l0.truncate(moved.keep);
        if let Some(slab) = moved.slab {
            crate::log!(
                "serve: L0 {} ({} objects, {}) {}..{head}",
                slab.meta.id,
                slab.meta.objects,
                crate::util::human_bytes(slab.meta.pack_len),
                slab.meta.have
            );
            self.l0.push(slab);
        }
        retain_l0(&mut self.l0, view.manifest.max_ts);
        let next = self.snapshot(view.manifest.clone(), view.etag.clone());
        ctx.metrics.l0_ingests.fetch_add(1, Ordering::Relaxed);
        ctx.publish(Arc::new(next));
        Ok(true)
    }
}

/// Downloads `key` to `path` unless a complete copy is already there.
fn hydrate(store: &Store, key: &str, path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    let t0 = std::time::Instant::now();
    store.download_file(key, path)?;
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    crate::log!(
        "serve: hydrated {key} ({}) in {:.2}s",
        crate::util::human_bytes(size),
        t0.elapsed().as_secs_f64()
    );
    Ok(())
}

/// The `/metrics` text (cold path).
pub fn metrics_text(ctx: &Ctx) -> String {
    let m = &ctx.metrics;
    let a = gvfs_alloc_guard::stats();
    let v = ctx.view();
    let get = |c: &std::sync::atomic::AtomicU64| c.load(Ordering::Relaxed);
    format!(
        "gvfs_requests_total{{endpoint=\"prefetch\"}} {}\n\
         gvfs_requests_total{{endpoint=\"objects_get\"}} {}\n\
         gvfs_requests_total{{endpoint=\"objects_post\"}} {}\n\
         gvfs_requests_total{{endpoint=\"sizes\"}} {}\n\
         gvfs_requests_total{{endpoint=\"config\"}} {}\n\
         gvfs_prefetch_packs_total {}\n\
         gvfs_not_found_total {}\n\
         gvfs_busy_total {}\n\
         gvfs_errors_total {}\n\
         gvfs_bytes_out_total {}\n\
         gvfs_objects_out_total {}\n\
         gvfs_l0_ingests_total {}\n\
         gvfs_manifest_loads_total {}\n\
         gvfs_slabs {}\n\
         gvfs_l0_slabs {}\n\
         gvfs_manifest_generation {}\n\
         gvfs_alloc_hot_path_total {}\n\
         gvfs_alloc_hot_path_bytes_total {}\n\
         gvfs_alloc_post_seal_total {}\n\
         gvfs_blobs_from_store_total {}\n\
         gvfs_blobs_proxied_total {}\n\
         gvfs_blobs_missing_total {}\n\
         gvfs_blobs_unavailable_total {}\n\
         gvfs_blobs_generation {}\n\
         gvfs_blobs_complete {}\n\
         gvfs_blob_cache_hits_total {}\n\
         gvfs_blob_cache_misses_total {}\n\
         gvfs_blob_cache_fetches_total {}\n\
         gvfs_blob_cache_bypass_total {}\n\
         gvfs_blob_cache_bytes {}\n\
         gvfs_connections_idle {}\n\
         gvfs_connections_idle_closed_total {}\n\
         gvfs_connections_idle_overflow_total {}\n\
         gvfs_buffer_spare_bytes {}\n\
         gvfs_grown_objects {}\n",
        get(&m.prefetch),
        get(&m.object_get),
        get(&m.object_post),
        get(&m.sizes),
        get(&m.config),
        get(&m.prefetch_packs),
        get(&m.not_found),
        get(&m.busy),
        get(&m.errors),
        get(&m.bytes_out),
        get(&m.objects_out),
        get(&m.l0_ingests),
        get(&m.manifest_loads),
        v.all.len(),
        v.l0_len,
        v.manifest.generation,
        a.hot_path,
        a.hot_path_bytes,
        a.post_seal,
        get(&m.blobs_store),
        get(&m.blobs_proxied),
        get(&m.blobs_missing),
        get(&m.unavailable),
        v.blobs.as_ref().map_or(0, |b| b.generation),
        u8::from(v.blobs.as_ref().is_some_and(|b| b.complete)),
        ctx.blob_cache.as_ref().map_or(0, |c| get(&c.hits)),
        ctx.blob_cache.as_ref().map_or(0, |c| get(&c.misses)),
        ctx.blob_cache.as_ref().map_or(0, |c| get(&c.fetches)),
        ctx.blob_cache.as_ref().map_or(0, |c| get(&c.bypassed)),
        ctx.blob_cache.as_ref().map_or(0, |c| c.bytes()),
        ctx.idle.load(Ordering::Relaxed).max(0),
        get(&m.idle_closed),
        get(&m.idle_overflow),
        ctx.spare.left(),
        ctx.arenas.grown()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_sealed_l0_slabs_age_out() {
        let (max_ts, newest) = (1_000_000, 1_000_000 + 10 * L0_RETAIN);
        // Unsealed: kept however old, e.g. while the pipeline is away.
        assert!(keep_l0(max_ts + 1, max_ts, newest));
        // Sealed: kept for L0_RETAIN after the newest boundary, then dropped.
        assert!(keep_l0(max_ts, max_ts, max_ts + L0_RETAIN));
        assert!(!keep_l0(max_ts, max_ts, max_ts + L0_RETAIN + 1));
    }
}
