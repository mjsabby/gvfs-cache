//! `gvfs-cache serve`: the GVFS server. The same code is the remote edge
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
//! Two threads then poll every `--poll-ms`: one the store (`blobs.bin` and the manifest,
//! downloading and opening what they list), one the upstream, which also makes the views.
//! Neither waits for the other. The L0 slabs
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
pub mod pages;
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

/// How an object the store keeps as a delta is sent loose, once rebuilt
/// (`--loose-deltas`). Deflating it again is the larger part of the request: on
/// SearchGold a deltified tree costs 102 µs of CPU deflated (5.8 KB) and 55 µs in stored
/// blocks (9.8 KB), since a tree is mostly ids, which do not compress. A deltified blob
/// is text: 118 µs and 3.6 KB deflated, 95 µs and 56 KB stored.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LooseDeltas {
    /// Deflated at level 1.
    Deflate,
    /// Trees in stored blocks, the rest deflated: for clients on a fast link.
    StoredTrees,
    /// Everything in stored blocks: for clients on the same host (the localhost proxy),
    /// which inflate at once what was deflated for them.
    Stored,
}

/// `--profile`: the defaults of the flags that differ between an edge, which serves many
/// clients over a network, and the localhost proxy, which serves the one machine it runs
/// on. A flag given says otherwise.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Profile {
    /// `--arenas` (`None`: two per core). An arena is about 31 MB resident from the
    /// start, and one client needs two.
    arenas: Option<usize>,
    /// `--poll-ms`. A fleet of proxies polls the upstream and the store once each: a
    /// thousand at an edge's two seconds are 500 requests a second for a branch that
    /// moves a few times a minute.
    poll_ms: u64,
    /// `--loose-deltas`. A proxy's clients inflate at once what it deflated for them.
    loose_deltas: &'static str,
}

impl Profile {
    const EDGE: Profile = Profile {
        arenas: None,
        poll_ms: 2000,
        loose_deltas: "deflate",
    };
    const PROXY: Profile = Profile {
        arenas: Some(2),
        poll_ms: 15_000,
        loose_deltas: "stored",
    };
}

/// `--index-pages huge`: indexes at least this long are read into memory that asks for
/// huge pages (one 2 MiB page).
const HUGE_INDEX_MIN: u64 = 2 << 20;

/// Baselines this edge made of the one before and a tail, instead of downloading them
/// ([`crate::tail`]; `/metrics`), and those of them whose index it made as well.
static GROWN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static GROWN_INDEXES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Shared by every connection.
pub struct Ctx {
    view: RwLock<Arc<View>>,
    /// Buffers of the connections with a request in progress, and output buffers returned
    /// while a head arrives in pieces.
    pub bufs: conn::Pool<conn::Bufs>,
    pub outs: conn::Pool<Vec<u8>>,
    /// One permit per connection that may be open (`--max-conns`).
    pub conns: Arc<tokio::sync::Semaphore>,
    pub max_conns: usize,
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
    /// The most levels of parents a commit's expansion follows (`--max-commit-depth`).
    pub max_commit_depth: u64,
    /// How an object rebuilt from a delta is sent loose (`--loose-deltas`).
    pub loose_deltas: LooseDeltas,
    pub config_json: Vec<u8>,
    pub metrics: Metrics,
    pub ready: AtomicBool,
    pub prefetch_idx: IdxPolicy,
    /// A client was sent an `.idx`: the warmer builds the next cold plan's index too.
    pub idx_wanted: AtomicBool,
    /// A client asked for sizes: the warmer builds the next cold plan's `GSZ1` section too.
    pub sizes_wanted: AtomicBool,
    /// Plans and their sections being built for the requests waiting for them.
    pub builds: Arc<budget::Builds>,
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
    /// Proxied packs kept as files at once (`--proxy-cache-packs`; `None`: as many as the
    /// descriptor limit suggests, [`default_proxy_packs`]).
    pub proxy_cache_packs: Option<usize>,
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
    /// Levels of parents a `commitDepth` may ask for (`--max-commit-depth`).
    pub max_commit_depth: u64,
    pub cache_servers: Vec<(String, String)>,
    pub idle_timeout: Duration,
    pub write_timeout: Duration,
    pub prefetch_idx: IdxPolicy,
    /// Idle keep-alive connections that can wait at once (beyond that, a connection that
    /// goes idle is closed: its client reconnects).
    pub max_idle_conns: usize,
    /// `--max-conns` (default: [`default_max_conns`]).
    pub max_conns: Option<usize>,
    /// Sealed rollups of this tier and above (24h, or the weekly delta) are delivered whole
    /// to the warm clients inside them, once ([`view`]); `None`: exact covers only.
    pub supersets: Option<Tier>,
    /// `--loose-deltas`.
    pub loose_deltas: LooseDeltas,
}

/// `gvfs-cache serve ...`
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
            "bind",
            "cache-dir",
            "poll-ms",
            "workers",
            "arenas",
            "arena-mib",
            "arena-objects",
            "grow-objects",
            "max-request-objects",
            "max-commit-depth",
            "cache-server",
            "alloc-guard",
            "idle-secs",
            "max-idle-conns",
            "max-conns",
            "quantum-us",
            "prefetch-idx",
            "superset-tier",
            "loose-deltas",
            "index-pages",
            "profile",
            "proxy-cache-mib",
            "proxy-cache-packs",
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
    let profile = match f.get("profile").unwrap_or("edge") {
        "edge" => Profile::EDGE,
        "proxy" => Profile::PROXY,
        other => crate::bail!("--profile must be edge or proxy, not {other:?}"),
    };
    let loose_deltas = match f.get("loose-deltas").unwrap_or(profile.loose_deltas) {
        "deflate" => LooseDeltas::Deflate,
        "stored-trees" => LooseDeltas::StoredTrees,
        "stored" => LooseDeltas::Stored,
        other => {
            crate::bail!("--loose-deltas must be deflate, stored-trees or stored, not {other:?}")
        }
    };
    match f.get("index-pages").unwrap_or("file") {
        "file" => {}
        // From the first index opened: an index of less than a huge page gains nothing.
        "huge" => crate::slab::huge_indexes(HUGE_INDEX_MIN),
        other => crate::bail!("--index-pages must be file or huge, not {other:?}"),
    }
    let cores = crate::util::default_threads();
    let arenas = f.num("arenas", profile.arenas.unwrap_or(cores.saturating_mul(2)))?;
    let arena_objects = f.num("arena-objects", 1usize << 18)?;
    let store = Store::open(
        f.require("store")?,
        f.get("store-auth-env")
            .unwrap_or("AZURE_STORAGE_CONNECTION_STRING"),
    )?;
    let cache_dir = match f.get("cache-dir") {
        Some(d) => PathBuf::from(d),
        // The system's temp directory is memory on many hosts.
        None if store.local_path(MANIFEST_KEY).is_none() => crate::bail!(
            "an edge of an object store keeps its slabs (gigabytes) and its caches of blob \
             packs in --cache-dir: give it a directory on local disk"
        ),
        None => std::env::temp_dir().join("gvfs-cache"),
    };
    let opts = Options {
        store,
        upstream,
        upstream_spec,
        proxy_cache_bytes: f.num("proxy-cache-mib", 10240u64)?.saturating_mul(1 << 20),
        proxy_cache_packs: match f.get("proxy-cache-packs") {
            Some(_) => Some(f.num("proxy-cache-packs", 0usize)?),
            None => None,
        },
        blob_cache_bytes: f.num("blob-cache-mib", 32768u64)?.saturating_mul(1 << 20),
        bind: f.get("bind").unwrap_or("127.0.0.1:8080").to_string(),
        cache_dir,
        poll: Duration::from_millis(f.num("poll-ms", profile.poll_ms)?),
        workers: f.num("workers", cores)?,
        arenas,
        arena_bytes: f.num("arena-mib", 8usize)?.saturating_mul(1 << 20),
        arena_objects,
        // As much again as the arenas hold.
        grow_objects: f.num("grow-objects", arenas.saturating_mul(arena_objects))?,
        max_request_objects: f.num("max-request-objects", 1usize << 22)?,
        max_commit_depth: f.num("max-commit-depth", 16u64)?.max(1),
        cache_servers,
        // At most a year: deadlines are instants, which must not overflow.
        idle_timeout: Duration::from_secs(f.num("idle-secs", 30u64)?.min(365 * 86400)),
        write_timeout: Duration::from_secs(120),
        prefetch_idx,
        max_idle_conns: f.num("max-idle-conns", 65536usize)?,
        max_conns: f
            .get("max-conns")
            .map(|_| f.num("max-conns", 0usize))
            .transpose()?,
        supersets,
        loose_deltas,
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
        proxy_cache_packs,
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
        max_commit_depth,
        cache_servers,
        idle_timeout,
        write_timeout,
        prefetch_idx,
        max_idle_conns,
        max_conns,
        supersets,
        loose_deltas,
    } = o;
    let store = Arc::new(store);
    let mut sync = StoreSync {
        store,
        blob_cache: None,
        cache_dir,
        opened: HashMap::new(),
        etag: String::new(),
        generation: 0,
        separate: false,
        manifest_retry: Retry::default(),
        blobs_retry: Retry::default(),
        poll,
        blobs: None,
        waiting_for: 0,
        behind: None,
        polls: crate::store::Poll::default(),
        manifest: Followed::default(),
        blobs_bin: Followed::default(),
    };
    let mut tip = LiveTip::default();

    // 1-3. Manifest, slabs, live tip.
    let (manifest, etag) = sync.wait_for_manifest()?;
    let separate = manifest.separate_blobs();
    let remote = sync.store.local_path(MANIFEST_KEY).is_none();
    // Connections, the proxy's files and the block cache's may not take the descriptors
    // the edge needs for its other files and sockets.
    let fds = raise_fd_limit();
    if remote || (separate && upstream_spec.is_some()) {
        std::fs::create_dir_all(&sync.cache_dir)
            .ctx(&format!("creating {}", sync.cache_dir.display()))?;
        if let Some(kind) = in_memory(&sync.cache_dir) {
            crate::log!(
                "serve: warning: --cache-dir {} is in memory ({kind}); it holds {}up to \
                 --blob-cache-mib and --proxy-cache-mib of blobs: give it a directory on disk",
                sync.cache_dir.display(),
                if remote { "the store's slabs and " } else { "" }
            );
        }
    }
    if separate {
        // Blob packs in object storage are read in blocks kept on local disk, each open
        // and mapped: as many as the byte budget holds, within what the limits leave.
        if sync.store.local_path(crate::blobs::BLOBS_KEY).is_none() && blob_cache_bytes > 0 {
            let maps = max_map_count();
            let blocks = cache_blocks(fds, maps);
            let wanted = blob_cache_bytes.div_ceil(blobcache::BLOCK);
            if wanted > crate::util::u64_of(blocks) {
                crate::log!(
                    "serve: --blob-cache-mib {} is {wanted} blocks, each a file with a \
                     descriptor and a mapping of its own; within the limits here ({fds} \
                     descriptors, {maps} mappings) the cache keeps {blocks} ({})",
                    blob_cache_bytes.wrapping_shr(20),
                    crate::util::human_bytes(
                        crate::util::u64_of(blocks).saturating_mul(blobcache::BLOCK)
                    )
                );
            }
            sync.blob_cache = Some(Arc::new(blobcache::BlockCache::new(
                Arc::clone(&sync.store),
                sync.cache_dir.join("blob-blocks"),
                blob_cache_bytes,
                blocks,
            )?));
        }
        tip.filter = Some("blob:none".to_string());
        // The manifest's trees need the blobs.bin generation it names.
        loop {
            if let Some((etag, bytes)) = sync.changed_blobs()? {
                sync.read_blobs(&bytes, etag)?;
            }
            if sync.blobs_generation() >= manifest.blobs_generation {
                break;
            }
            crate::log!(
                "serve: waiting for blobs.bin generation {} (have {})",
                manifest.blobs_generation,
                sync.blobs_generation()
            );
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    let sealed = sync.adopt(&manifest, &etag)?;
    let (changed, changes) = std::sync::mpsc::channel();
    let mut live = Live {
        upstream,
        tip,
        sealed,
        l0: Vec::new(),
        last_head: Oid::ZERO,
        retry: Retry::default(),
        apply_retry: Retry::default(),
        poll,
        generation: 0,
        supersets,
        blobs: sync.blobs.clone(),
        changes,
        pending: None,
    };
    let first = live.snapshot(manifest, etag);
    let proxy = match (&upstream_spec, separate) {
        (Some((u, _, auth)), true) => Some(blobs::Proxy::new(
            u,
            auth,
            &sync.cache_dir.join("proxy"),
            proxy_cache_bytes,
            proxy_cache_packs.unwrap_or_else(|| default_proxy_packs(fds)),
        )?),
        _ => None,
    };
    let workers = workers.max(1);
    let max_conns = match max_conns {
        Some(n) => {
            if n > fds.saturating_sub(fd_reserve(fds)) {
                crate::log!(
                    "serve: warning: --max-conns {n} leaves the edge too few of its {fds} file \
                     descriptors for its own files and connections"
                );
            }
            n.max(1)
        }
        None => default_max_conns(fds, max_idle_conns),
    };
    let ctx = Arc::new(Ctx {
        view: RwLock::new(Arc::new(first)),
        bufs: conn::Pool::new(workers, CONN_BUFS, OUT_BYTES, 0),
        outs: conn::Pool::new(workers, 0, OUT_BYTES, 4),
        conns: Arc::new(tokio::sync::Semaphore::new(max_conns)),
        max_conns,
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
        max_commit_depth,
        loose_deltas,
        config_json: config_json(&cache_servers),
        metrics: Metrics::default(),
        ready: AtomicBool::new(false),
        prefetch_idx,
        idx_wanted: AtomicBool::new(prefetch_idx == IdxPolicy::Always),
        sizes_wanted: AtomicBool::new(false),
        builds: Arc::default(),
        proxy,
        blob_cache: sync.blob_cache.clone(),
    });
    if let Err(e) = live.sync_upstream(&ctx) {
        crate::log!("serve: live tip not caught up at boot ({e}); serving sealed slabs");
    }
    note_indexes(&ctx, &ctx.view(), true);
    let warmer = Arc::new(Warmer::default());
    warmer.offer(ctx.view());

    // 4. The runtime and its buffers, then seal.
    let listener = TcpListener::bind(&bind).ctx(&format!("binding {bind}"))?;
    // As bound (`--bind` with port 0 takes any free port, and the log says which).
    let addr = listener.local_addr().ctx("reading the bound address")?;
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
        "serving on http://{addr}: {} slabs ({} in L0), tip {}, {}, {workers} workers, \
         {arenas} arenas, a poll every {poll:?}",
        v.all.len(),
        v.l0_len,
        v.tip,
        sync.store.describe()
    );

    // The store is followed on a thread of its own, the upstream branch on this one, for
    // the life of the process.
    {
        let ctx = ctx.clone();
        std::thread::Builder::new()
            .name("gvfs-store".into())
            .spawn(move || sync.run(&ctx, &changed))?;
    }
    loop {
        std::thread::sleep(poll);
        live.tick(&ctx, &warmer);
    }
}

/// Raises the soft open-file limit to the hard limit: every idle client holds a descriptor.
/// Raises the soft limit on open files to the hard one; returns the limit in force.
fn raise_fd_limit() -> usize {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};
    let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_NOFILE) else {
        return 1024;
    };
    let limit = if soft < hard && setrlimit(Resource::RLIMIT_NOFILE, hard, hard).is_ok() {
        hard
    } else {
        soft
    };
    usize::try_from(limit).unwrap_or(usize::MAX)
}

/// Descriptors kept for the edge's own use: open slabs, the memfds of L0 slabs and of
/// the packs plans repack (one per boundary a warm client asks from), block cache and
/// proxy files, the store's and the upstream's connections, pipes of local `git` processes.
fn fd_reserve(fds: usize) -> usize {
    (fds / 16).max(4096)
}

/// `--max-conns` by default: twice `--max-idle-conns` (as many working as idle), within the
/// descriptors left after [`fd_reserve`].
pub fn default_max_conns(fds: usize, max_idle: usize) -> usize {
    max_idle
        .saturating_mul(2)
        .min(fds.saturating_sub(fd_reserve(fds)))
        .max(64)
}

/// Connection buffers made at startup (more are made while they run out).
const CONN_BUFS: usize = 256;
/// Bytes of each connection's output buffer (the batch a response coalesces).
const OUT_BYTES: usize = 256 << 10;
/// `--proxy-cache-packs` by default: a quarter of the descriptors the edge keeps for its
/// own use ([`fd_reserve`]), within its limit and well within the mappings a process may
/// have (`vm.max_map_count`, 65,530 by default). A proxied pack kept as a file holds one
/// of each, and the byte budget alone let them take every descriptor the edge has. 256
/// under a limit of 1,024 descriptors, which leaves three quarters to the 64 connections
/// served under it ([`default_max_conns`]), the slabs and a directory store's blob packs;
/// 8,192 under 524,288.
pub fn default_proxy_packs(fds: usize) -> usize {
    (fd_reserve(fds).min(fds) / 4).clamp(256, 16384)
}

/// The blocks the block cache keeps at most, whatever `--blob-cache-mib` would hold: a
/// cached block is a file with a descriptor and a mapping of its own. A quarter of the
/// descriptors the edge keeps for its own use ([`fd_reserve`]), as the proxy's packs
/// take ([`default_proxy_packs`]), and a quarter of the mappings a process may have
/// (`maps`, `vm.max_map_count`: 65,530 by default, of which the proxy's packs take
/// 16,384 at most). 8,192 blocks under a limit of 524,288 descriptors, which is the
/// default budget of 32 GiB; 1,024 (4 GiB) under 65,536; 256 (1 GiB) under 1,024.
pub fn cache_blocks(fds: usize, maps: usize) -> usize {
    (fd_reserve(fds).min(fds) / 4).min(maps / 4)
}

/// The mappings a process may have (`vm.max_map_count`); the kernel's default where it
/// cannot be read.
fn max_map_count() -> usize {
    std::fs::read_to_string("/proc/sys/vm/max_map_count")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(65_530)
}

/// Accepts connections for the life of the process: one task each, at most `--max-conns`
/// at once (past that, new ones wait in the listen queue until one closes).
async fn accept_loop(ctx: Arc<Ctx>, listener: tokio::net::TcpListener) {
    let mut logged: Option<Instant> = None;
    let mut full_logged: Option<Instant> = None;
    loop {
        let slot = match Arc::clone(&ctx.conns).try_acquire_owned() {
            Ok(s) => s,
            Err(_) => {
                ctx.metrics.conns_full.fetch_add(1, Ordering::Relaxed);
                if full_logged.is_none_or(|t| t.elapsed() >= Duration::from_secs(60)) {
                    crate::log!(
                        "serve: {} connections open (--max-conns): new ones wait",
                        ctx.max_conns
                    );
                    full_logged = Some(Instant::now());
                }
                match Arc::clone(&ctx.conns).acquire_owned().await {
                    Ok(s) => s,
                    Err(_) => return,
                }
            }
        };
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(conn::HotPath::new(conn::serve(
                    Arc::clone(&ctx),
                    stream,
                    slot,
                )));
            }
            Err(e) => {
                drop(slot);
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
                // With an index wanted, its tables are merged while the trailer is hashed.
                let idx_len = idx.then(|| plan.idx().map_or(0, <[u8]>::len));
                plan.trailer();
                if ctx.sizes_wanted.load(Ordering::Relaxed) {
                    let _ = plan.sizes_section();
                }
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
            if prev_tip_ts != 0
                && prev_tip_ts < view.tip_ts
                && let Prefetch::Pack(plan, _) = view.prefetch(prev_tip_ts)
            {
                if idx {
                    let _ = plan.idx();
                }
                plan.trailer();
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

    /// What the tip knows of the L0 slabs, to put back with [`LiveTip::restore`] when a
    /// move of theirs fails after [`LiveTip::advance`] has noted it.
    fn state(&self) -> (HashMap<Oid, u64>, Vec<(u64, u64)>) {
        (self.stamps.clone(), self.retracted.clone())
    }

    fn restore(&mut self, (stamps, retracted): (HashMap<Oid, u64>, Vec<(u64, u64)>)) {
        self.stamps = stamps;
        self.retracted = retracted;
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

    /// The L0 slabs once manifest `m` (whose slabs are `sealed`) replaces `old`, at `now`
    /// (the clock, in seconds). Those the manifest covers stay until they age out, the
    /// ones after its tip stay live. If the tip falls inside an L0 slab, what follows it
    /// is repacked out of that slab; if the branch was rewritten, L0 starts over from the
    /// manifest.
    pub fn prune(
        &mut self,
        l0: &[Arc<Slab>],
        old: &Manifest,
        m: &Manifest,
        sealed: &[Arc<Slab>],
        now: u64,
    ) -> Result<Vec<Arc<Slab>>> {
        let out = match self.pruned(l0, old, m, sealed, now)? {
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
        now: u64,
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
        retain_l0(&mut out, m.max_ts, now);
        Ok(Some(out))
    }
}

/// Drops the sealed L0 slabs (ending at or before `max_ts`, the manifest's tip) that ended
/// more than [`L0_RETAIN`] before the newest boundary. Unsealed ones hold the live tip and
/// always stay, however long the pipeline is away.
///
/// The boundaries are stamps, which are commit times: a commit dated in the future puts
/// every later one ahead of the clock, a second apart per commit, and six hours of them
/// are then 21,600 commits' L0 slabs, in memory. So the newest boundary counts as no
/// later than `now` (the clock, in seconds), and no more than [`L0_SEALED_MAX`] sealed
/// slabs stay, the oldest going first.
pub fn retain_l0(l0: &mut Vec<Arc<Slab>>, max_ts: u64, now: u64) {
    retain_ends(l0, max_ts, now, |s| s.meta.end_ts);
}

/// The most sealed L0 slabs kept for warm clients: more than six hours hold at a move of
/// the branch every six seconds.
pub const L0_SEALED_MAX: usize = 4096;

/// [`retain_l0`], of slabs (oldest first) that end at `end`.
fn retain_ends<T>(l0: &mut Vec<T>, max_ts: u64, now: u64, end: impl Fn(&T) -> u64) {
    let newest = l0.last().map_or(max_ts, |s| end(s).max(max_ts)).min(now);
    l0.retain(|s| keep_l0(end(s), max_ts, newest));
    let sealed = l0.iter().filter(|s| end(s) <= max_ts).count();
    let mut excess = sealed.saturating_sub(L0_SEALED_MAX);
    if excess > 0 {
        l0.retain(|s| {
            let goes = end(s) <= max_ts && excess > 0;
            excess = excess.saturating_sub(usize::from(goes));
            !goes
        });
    }
}

fn keep_l0(end_ts: u64, max_ts: u64, newest: u64) -> bool {
    end_ts > max_ts || end_ts.saturating_add(L0_RETAIN) >= newest
}

/// The longest wait before something that failed is tried again.
const RETRY_MAX: Duration = Duration::from_secs(60);

/// When something that failed is tried again: an upstream head that could not be
/// ingested, a manifest that could not be applied. An attempt costs a fetch of what the
/// branch added, or a download of what the manifest lists, so a failure that repeats (a
/// pack this edge cannot index, a slab the store lacks, a full disk) must not cost one per
/// poll: the wait starts at the poll interval and doubles with each failure, up to
/// [`RETRY_MAX`]. Something else (another head, another manifest) is tried at once.
struct Retry<K> {
    key: Option<K>,
    failures: u32,
    wait: Duration,
    at: Option<Instant>,
}

impl<K> Default for Retry<K> {
    fn default() -> Self {
        Retry {
            key: None,
            failures: 0,
            wait: Duration::ZERO,
            at: None,
        }
    }
}

impl<K: PartialEq> Retry<K> {
    fn due(&self, key: &K, now: Instant) -> bool {
        self.key.as_ref() != Some(key) || self.at.is_none_or(|at| now >= at)
    }

    /// Notes that the attempt for `key` failed at `now`; returns the wait before its next.
    fn failed(&mut self, key: K, poll: Duration, now: Instant) -> Duration {
        if self.key.as_ref() == Some(&key) {
            self.failures = self.failures.saturating_add(1);
            self.wait = self.wait.saturating_mul(2).min(RETRY_MAX.max(poll));
        } else {
            self.key = Some(key);
            self.failures = 1;
            self.wait = poll.max(Duration::from_millis(1));
        }
        self.at = now.checked_add(self.wait);
        self.wait
    }

    /// The outcome of the attempt for `key`: a success forgets the failures before it, a
    /// failure is told which attempt it was and when the next is.
    fn note<T>(&mut self, key: K, poll: Duration, what: &str, outcome: Result<T>) -> Result<T> {
        match outcome {
            Ok(v) => {
                *self = Retry::default();
                Ok(v)
            }
            Err(e) => {
                let wait = self.failed(key, poll, Instant::now());
                Err(e).ctx(&format!(
                    "{what} (attempt {}, the next in {wait:?})",
                    self.failures
                ))
            }
        }
    }
}

/// What the store's thread follows: `blobs.bin` and the manifest. It downloads and opens
/// what they list and hands it to the live thread ([`StoreChange`]), which makes the views.
/// The two do not wait for each other: a store that is slow (a weekly baseline is
/// gigabytes), unreachable or wrong does not keep the edge from following the branch, nor
/// a long fetch from the upstream the store's next manifest from being downloaded.
struct StoreSync {
    store: Arc<Store>,
    blob_cache: Option<Arc<blobcache::BlockCache>>,
    cache_dir: PathBuf,
    /// Sealed slabs already open, by id (reused across manifests).
    opened: HashMap<String, Arc<Slab>>,
    /// The manifest last opened: its etag, its generation, whether its blobs are separate.
    etag: String,
    generation: u64,
    separate: bool,
    /// When a manifest that could not be opened and a `blobs.bin` that could not be loaded
    /// are tried again (by etag), and the poll interval the waits start from.
    manifest_retry: Retry<String>,
    blobs_retry: Retry<String>,
    poll: Duration,
    /// The loaded `blobs.bin` (`--blobs separate` stores).
    blobs: Option<Arc<blobs::BlobSet>>,
    /// A manifest generation held back until its `blobs.bin` generation loads (logged once).
    waiting_for: u64,
    /// A manifest not newer than the last one opened: its etag, and the polls it has been
    /// the store's ([`StoreSync::made_anew`]).
    behind: Option<(String, u32)>,
    /// What the polls keep between them, and the manifest and the `blobs.bin` last read.
    polls: crate::store::Poll,
    manifest: Followed,
    blobs_bin: Followed,
}

/// An object the store's thread follows, as it last read it. A poll asks the store for it
/// only if it is no longer this one ([`Store::get_unless`]).
#[derive(Default)]
struct Followed {
    etag: Option<String>,
    /// Its bytes, until the edge has opened them: an object that could not be opened is
    /// tried again from these, and a manifest that waits (for its `blobs.bin`, or to be
    /// believed) is looked at again, without another read.
    bytes: Arc<Vec<u8>>,
}

impl Followed {
    /// The store's object `key`, its etag and its bytes: read if it is not the one read
    /// last. `None` if the store has no such object.
    fn poll(
        &mut self,
        store: &Store,
        polls: &mut crate::store::Poll,
        key: &str,
    ) -> Result<Option<(String, Arc<Vec<u8>>)>> {
        match store.get_unless(polls, key, self.etag.as_deref())? {
            crate::store::Fresh::Gone => return Ok(None),
            crate::store::Fresh::Same => {}
            crate::store::Fresh::New(bytes, etag) => {
                self.etag = Some(etag);
                self.bytes = Arc::new(bytes);
            }
        }
        Ok(self
            .etag
            .clone()
            .map(|etag| (etag, Arc::clone(&self.bytes))))
    }

    /// The edge has opened the object: its bytes are not needed again.
    fn opened(&mut self) {
        if !self.bytes.is_empty() {
            self.bytes = Arc::default();
        }
    }
}

/// Polls a manifest that is not newer than the one served stays the store's before the
/// edge follows it: a minute at the default `--poll-ms`.
const BEHIND_POLLS: u32 = 30;

/// What the store's thread hands the live thread, in the order it happened.
enum StoreChange {
    Blobs(Arc<blobs::BlobSet>),
    Manifest(Box<Opened>),
}

/// A newer manifest, with its etag and its slabs open in its order.
struct Opened {
    manifest: Manifest,
    etag: String,
    sealed: Vec<Arc<Slab>>,
}

/// The live thread's state.
struct Live {
    upstream: Option<Upstream>,
    tip: LiveTip,
    /// The current manifest's slabs, in manifest order.
    sealed: Vec<Arc<Slab>>,
    /// The L0 slabs, oldest first.
    l0: Vec<Arc<Slab>>,
    /// Upstream tip last ingested (or found to bring nothing).
    last_head: Oid,
    /// When a head whose ingest failed is tried again, and a manifest whose L0 slabs could
    /// not be pruned against it; and the poll interval the waits start from.
    retry: Retry<Oid>,
    apply_retry: Retry<String>,
    poll: Duration,
    generation: u64,
    supersets: Option<Tier>,
    /// The loaded `blobs.bin` (`--blobs separate` stores).
    blobs: Option<Arc<blobs::BlobSet>>,
    /// What the store's thread has opened, and the manifest among it not applied yet.
    changes: std::sync::mpsc::Receiver<StoreChange>,
    pending: Option<Box<Opened>>,
}

impl StoreSync {
    fn blobs_generation(&self) -> u64 {
        self.blobs.as_ref().map_or(0, |b| b.generation)
    }

    /// The store's `blobs.bin`, its etag and its bytes, if it is not the one loaded (and,
    /// after a failure to load it, it is time to try again).
    fn changed_blobs(&mut self) -> Result<Option<(String, Arc<Vec<u8>>)>> {
        let found = self
            .blobs_bin
            .poll(&self.store, &mut self.polls, crate::blobs::BLOBS_KEY)
            .ctx("reading blobs.bin")?;
        let Some((etag, bytes)) = found else {
            return Ok(None);
        };
        if self.blobs.as_ref().is_some_and(|b| b.etag == etag) {
            self.blobs_bin.opened();
            return Ok(None);
        }
        Ok(self
            .blobs_retry
            .due(&etag, Instant::now())
            .then_some((etag, bytes)))
    }

    /// The `blobs.bin` in `bytes` becomes the loaded one, or nothing changes.
    fn read_blobs(&mut self, bytes: &[u8], etag: String) -> Result<()> {
        let bm = crate::blobs::BlobManifest::decode(bytes).ctx("reading blobs.bin")?;
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
                " (not complete: gvfs-cache mirror running)"
            }
        );
        self.blobs = Some(Arc::new(set));
        // Views on the previous set keep the segments they mapped.
        let keep: std::collections::HashSet<&str> =
            bm.segments.iter().map(|s| s.id.as_str()).collect();
        let removed = prune_dir(&self.cache_dir.join("blobs"), |id, ext| {
            stale_segment_index(id, ext, &keep)
        });
        if removed > 0 {
            crate::log!("serve: pruned {removed} segment indexes blobs.bin no longer lists");
        }
        Ok(())
    }

    /// Removes the cached files of slabs `m` does not list (a request on an older view keeps
    /// what it mapped). Without this, an edge of an object store kept every slab it ever
    /// downloaded: each 15m slab, each rollup, a new baseline every week.
    fn prune_slabs(&self, m: &Manifest) {
        let keep: std::collections::HashSet<&str> = m.slabs().map(|s| s.id.as_str()).collect();
        let removed = prune_dir(&self.cache_dir, |id, ext| stale_slab_file(id, ext, &keep));
        if removed > 0 {
            crate::log!(
                "serve: pruned {removed} files of slabs manifest generation {} does not list",
                m.generation
            );
        }
    }

    fn wait_for_manifest(&mut self) -> Result<(Manifest, String)> {
        let mut logged = false;
        loop {
            let found = self
                .manifest
                .poll(&self.store, &mut self.polls, MANIFEST_KEY)?;
            if let Some((etag, bytes)) = found {
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

    /// Opens (hydrating if needed) every slab of `m`: its slabs in manifest order, and by
    /// id.
    #[allow(clippy::type_complexity)]
    fn open_manifest(&self, m: &Manifest) -> Result<(Vec<Arc<Slab>>, HashMap<String, Arc<Slab>>)> {
        let mut sealed = Vec::with_capacity(m.slabs().count());
        let mut keep = HashMap::new();
        for meta in m.slabs() {
            let slab = match self.opened.get(&meta.id) {
                Some(s) => s.clone(),
                None => {
                    let s = self.open(meta)?;
                    if m.separate_blobs() {
                        s.set_sizes(Arc::new(self.sizes(meta)?));
                    }
                    s
                }
            };
            keep.insert(meta.id.clone(), slab.clone());
            sealed.push(slab);
        }
        Ok((sealed, keep))
    }

    /// The sizes table of slab `meta` (a table carries its own checksum). A cached one
    /// that does not open is downloaded again, once, as a slab's files are.
    fn sizes(&self, meta: &crate::slab::SlabMeta) -> Result<crate::sizes::Sizes> {
        use crate::sizes::Sizes;
        let (what, key) = (format!("sizes of slab {}", meta.id), meta.sizes_key());
        if let Some(path) = self.store.local_path(&key) {
            return Sizes::open(&path).ctx(&what);
        }
        let path = self.cache_dir.join(format!("{}.sizes", meta.id));
        let new = hydrate(&self.store, &key, &path).ctx(&what)?;
        match Sizes::open(&path) {
            Err(e) if !new && std::fs::File::open(&path).is_ok() => {
                crate::log!("serve: {what} from the cache failed ({e}): downloading them again");
                let _ = std::fs::remove_file(&path);
                hydrate(&self.store, &key, &path).ctx(&what)?;
                Sizes::open(&path)
            }
            sizes => sizes,
        }
        .ctx(&what)
    }

    fn open(&self, meta: &crate::slab::SlabMeta) -> Result<Arc<Slab>> {
        let what = format!("opening slab {}", meta.id);
        if let (Some(pack), Some(index)) = (
            self.store.local_path(&meta.pack_key()),
            self.store.local_path(&meta.index_key()),
        ) {
            return Slab::open(&pack, &index, meta.clone()).ctx(&what);
        }
        let pack = self.cache_dir.join(format!("{}.pack", meta.id));
        let index = self.cache_dir.join(format!("{}.mphf", meta.id));
        // Cached files that can be read and are not the slab (a disk that lost part of
        // one) are downloaded again, once: an edge must not need its cache wiped by hand
        // to start, or to take the next manifest.
        let cached = std::fs::File::open(&pack).is_ok() && std::fs::File::open(&index).is_ok();
        match self.download(meta, &pack, &index) {
            Err(e) if cached => {
                crate::log!("serve: {what} from the cache failed ({e}): downloading it again");
                let _ = std::fs::remove_file(&pack);
                let _ = std::fs::remove_file(&index);
                self.download(meta, &pack, &index)
            }
            slab => slab,
        }
        .ctx(&what)
    }

    /// Slab `meta` from the cache directory, once the files of it that are not there are
    /// downloaded. A pack that was is hashed: it must be the pack whose SHA-1 the manifest
    /// lists, not only end with it. A baseline's pack is made of the one this edge has
    /// and its tail, if the store holds one ([`StoreSync::grown`]).
    fn download(
        &self,
        meta: &crate::slab::SlabMeta,
        pack: &Path,
        index: &Path,
    ) -> Result<Arc<Slab>> {
        if meta.tier == Tier::Base && !pack.exists() {
            let mut with_index = false;
            let grown = self.grown(meta, pack, index).and_then(|made| {
                let Some(index_too) = made else {
                    return Ok(None);
                };
                with_index = index_too;
                // Unless it was made of the old one, as the pack was.
                hydrate(&self.store, &meta.index_key(), index)?;
                listed(meta, pack, index, true).map(Some)
            });
            match grown {
                Ok(Some(slab)) => {
                    GROWN.fetch_add(1, Ordering::Relaxed);
                    if with_index {
                        GROWN_INDEXES.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(slab);
                }
                Ok(None) => {}
                // What was made of a tail and is not the baseline is not kept, the
                // index made of the same tail with it: the store's files are the
                // baseline.
                Err(e) => {
                    if with_index {
                        let _ = std::fs::remove_file(index);
                    }
                    crate::log!(
                        "serve: baseline {} was not made of the one before it ({e}): \
                         downloading it",
                        meta.id
                    );
                }
            }
        }
        let new = hydrate(&self.store, &meta.pack_key(), pack)?;
        hydrate(&self.store, &meta.index_key(), index)?;
        listed(meta, pack, index, new)
    }

    /// Makes the pack of baseline `meta` of the baseline this edge has and the tail the
    /// weekly run published beside the new one ([`crate::tail`]): megabytes to download
    /// where the pack is gigabytes. Its index too, at `index`, of the old baseline's and
    /// the records in the tail, where there is none yet and the tail holds them: on this
    /// thread, in a few seconds for millions of objects (2.4 s for SearchGold's 9.25M,
    /// with 12 bytes of memory for each meanwhile), in place of a download of 40 bytes an
    /// object. `None` if the store has no tail for the baseline, or the tail continues a
    /// pack that is not in the cache; else whether the index was made as well.
    fn grown(
        &self,
        meta: &crate::slab::SlabMeta,
        pack: &Path,
        index: &Path,
    ) -> Result<Option<bool>> {
        let t0 = Instant::now();
        let tail = self.cache_dir.join(format!("{}.tail", meta.id));
        match self.store.download_file(&meta.tail_key(), &tail) {
            Ok(()) => {}
            Err(e) if e.is_not_found() => return Ok(None),
            Err(e) => return Err(e),
        }
        let size = std::fs::metadata(&tail).map_or(0, |m| m.len());
        let old = |id: &str| Some(self.cache_dir.join(format!("{id}.pack")));
        let head = match crate::tail::grow(&tail, old, meta, pack) {
            Ok(Some(head)) => head,
            other => {
                let _ = std::fs::remove_file(&tail);
                return other.map(|_| None);
            }
        };
        let t1 = Instant::now();
        let old_index = self.cache_dir.join(format!("{}.mphf", head.old_id));
        let with_index = !index.exists()
            && match crate::tail::grow_index(&tail, &head, &old_index, meta, 1, index) {
                Ok(made) => made,
                Err(e) => {
                    crate::log!(
                        "serve: the index of baseline {} was not made of {}'s ({e}): \
                         downloading it",
                        meta.id,
                        head.old_id
                    );
                    false
                }
            };
        let _ = std::fs::remove_file(&tail);
        crate::log!(
            "serve: baseline {} ({}) made of {} and a tail of {} in {:.2}s{}",
            meta.id,
            crate::util::human_bytes(meta.pack_len),
            head.old_id,
            crate::util::human_bytes(size),
            t1.duration_since(t0).as_secs_f64(),
            if with_index {
                format!(
                    "; its index ({}) of the old one and the tail's records in {:.2}s",
                    crate::util::human_bytes(meta.index_len),
                    t1.elapsed().as_secs_f64()
                )
            } else {
                String::new()
            }
        );
        Ok(Some(with_index))
    }

    /// Follows the store for the life of the process, a poll every `poll`.
    fn run(mut self, ctx: &Ctx, changes: &std::sync::mpsc::Sender<StoreChange>) {
        loop {
            std::thread::sleep(self.poll);
            if let Err(e) = self.step(changes) {
                live_failed(ctx, &e);
            }
        }
    }

    /// One poll of the store: a changed `blobs.bin`, then a newer manifest.
    fn step(&mut self, changes: &std::sync::mpsc::Sender<StoreChange>) -> Result<()> {
        if self.separate
            && let Some((etag, bytes)) = self.changed_blobs()?
        {
            let loaded = self.read_blobs(&bytes, etag.clone());
            self.blobs_retry
                .note(etag, self.poll, "loading blobs.bin", loaded)?;
            if let Some(set) = &self.blobs {
                let _ = changes.send(StoreChange::Blobs(Arc::clone(set)));
            }
        }
        let found = self
            .manifest
            .poll(&self.store, &mut self.polls, MANIFEST_KEY)
            .ctx("reading the manifest")?;
        let Some((etag, bytes)) = found else {
            return Ok(());
        };
        if etag == self.etag {
            self.manifest.opened();
            return Ok(());
        }
        if !self.manifest_retry.due(&etag, Instant::now()) {
            return Ok(());
        }
        let newer = self.open_newer(&bytes, &etag);
        let what = "opening the store's manifest";
        if let Some((manifest, sealed)) =
            self.manifest_retry
                .note(etag.clone(), self.poll, what, newer)?
        {
            let opened = Opened {
                manifest,
                etag,
                sealed,
            };
            let _ = changes.send(StoreChange::Manifest(Box::new(opened)));
        }
        Ok(())
    }

    /// The manifest in `bytes` and its slabs, open, if it is newer than the last one opened
    /// (or the store was made anew) and its `blobs.bin` is loaded.
    fn open_newer(
        &mut self,
        bytes: &[u8],
        etag: &str,
    ) -> Result<Option<(Manifest, Vec<Arc<Slab>>)>> {
        let m = Manifest::decode(bytes)?;
        if m.generation <= self.generation && !self.made_anew(&m, etag) {
            return Ok(None);
        }
        let blobs_behind = m.separate_blobs() && self.blobs_generation() < m.blobs_generation;
        if blobs_behind && self.waiting_for != m.generation {
            crate::log!(
                "serve: manifest generation {} waits for blobs.bin generation {} (have {})",
                m.generation,
                m.blobs_generation,
                self.blobs_generation()
            );
            self.waiting_for = m.generation;
        }
        if blobs_behind {
            return Ok(None);
        }
        let sealed = self.adopt(&m, etag)?;
        self.behind = None;
        Ok(Some((m, sealed)))
    }

    /// Whether manifest `m`, which is not newer than the last one opened, is the store's
    /// all the same. A store's generations only grow, so a manifest behind the one served
    /// is a store made anew (or put back from a copy), not an older state of the same one,
    /// and an edge that ignored it would serve the old store for good, its L0 slabs
    /// growing. It is followed, as a restarted edge would follow it, once it has stayed
    /// the store's for [`BEHIND_POLLS`] polls: a read that returned an older manifest for
    /// a moment (a store that is not consistent) does not last that long.
    fn made_anew(&mut self, m: &Manifest, etag: &str) -> bool {
        let polls = match &self.behind {
            Some((seen, polls)) if seen == etag => polls.saturating_add(1),
            _ => 1,
        };
        self.behind = Some((etag.to_string(), polls));
        if polls == 1 {
            crate::log!(
                "serve: the store's manifest is generation {}, not after generation {} which \
                 this edge serves: followed if it is still the store's in {BEHIND_POLLS} polls",
                m.generation,
                self.generation
            );
        }
        if polls < BEHIND_POLLS {
            return false;
        }
        crate::log!(
            "serve: the store was made anew: following its manifest, generation {}",
            m.generation
        );
        true
    }

    /// Opens manifest `m`, which becomes the last one opened: its slabs, in its order. The
    /// cached files of slabs it does not list go (views on older manifests keep what they
    /// mapped).
    fn adopt(&mut self, m: &Manifest, etag: &str) -> Result<Vec<Arc<Slab>>> {
        let (sealed, opened) = self.open_manifest(m)?;
        self.opened = opened;
        self.prune_slabs(m);
        self.etag = etag.to_string();
        self.generation = m.generation;
        self.separate = m.separate_blobs();
        Ok(sealed)
    }
}

/// A failure to follow the store or the upstream: logged and counted.
fn live_failed(ctx: &Ctx, e: &crate::util::Error) {
    ctx.metrics.live_failures.fetch_add(1, Ordering::Relaxed);
    crate::log!("serve: live sync failed: {e}");
}

/// Measures where the indexes of `view` lie ([`pages`]) for the metrics, and says it if
/// `say`: at start, and when the baseline or the biggest blob segment is another.
fn note_indexes(ctx: &Ctx, view: &View, say: bool) {
    let p = pages::measure(view);
    let m = &ctx.metrics;
    m.index_bytes.store(p.bytes, Ordering::Relaxed);
    m.index_copied.store(p.copied, Ordering::Relaxed);
    m.index_huge.store(p.huge, Ordering::Relaxed);
    if !say || p.bytes == 0 {
        return;
    }
    let percent = p.huge.saturating_mul(100).checked_div(p.bytes).unwrap_or(0);
    let how = if p.copied > 0 {
        format!(
            "; {} read into memory (--index-pages huge)",
            crate::util::human_bytes(p.copied)
        )
    } else if percent < 90 {
        "; --index-pages huge asks for them".to_string()
    } else {
        String::new()
    };
    crate::log!(
        "serve: indexes: {}, {percent}% on 2 MiB pages{how}",
        crate::util::human_bytes(p.bytes)
    );
}

impl Live {
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

    /// One poll: what the store's thread has opened since the last, then the upstream
    /// branch. Neither needs the other to work: an edge that cannot read its store, or
    /// cannot apply its manifest, still follows the branch, and the reverse.
    fn tick(&mut self, ctx: &Ctx, warmer: &Warmer) {
        if let Err(e) = self.take_changes(ctx, warmer) {
            live_failed(ctx, &e);
        }
        match self.sync_upstream(ctx) {
            Ok(true) => warmer.offer(ctx.view()),
            Ok(false) => {}
            Err(e) => live_failed(ctx, &e),
        }
    }

    /// Makes views of what the store's thread has opened: a `blobs.bin`, then the newest
    /// manifest.
    fn take_changes(&mut self, ctx: &Ctx, warmer: &Warmer) -> Result<()> {
        while let Ok(change) = self.changes.try_recv() {
            match change {
                StoreChange::Blobs(set) => {
                    self.blobs = Some(Arc::clone(&set));
                    // The view's slabs are this thread's own: the view is the same but for
                    // its blobs, plans included.
                    self.generation = self.generation.saturating_add(1);
                    let view = ctx.view();
                    let next = Arc::new(view.with_blobs(self.generation, Some(Arc::clone(&set))));
                    ctx.publish(Arc::clone(&next));
                    // Most lookups end in the biggest segment: where a new one lies.
                    let biggest = |v: &View| {
                        let set = v.blobs.as_ref()?;
                        set.indexes().next().map(|s| s.id.clone())
                    };
                    if biggest(&view) != biggest(&next) {
                        note_indexes(ctx, &next, true);
                    }
                    // Requests find these blobs in the store from here on.
                    let released = ctx.proxy.as_ref().map_or(0, |p| p.release_stored(&set));
                    if released > 0 {
                        crate::log!(
                            "serve: dropped {released} proxied packs whose blobs blobs.bin \
                             generation {} lists",
                            set.generation
                        );
                    }
                }
                StoreChange::Manifest(opened) => self.pending = Some(opened),
            }
        }
        let Some(opened) = self.pending.take() else {
            return Ok(());
        };
        if !self.apply_retry.due(&opened.etag, Instant::now()) {
            self.pending = Some(opened);
            return Ok(());
        }
        // The slabs, the L0 slabs pruned against them and the view change together, once
        // the pruning (which may repack an L0 slab) has not failed: until then this edge's
        // slabs are those of the view it serves, which a later view is made of.
        let view = ctx.view();
        let pruned = self.tip.prune(
            &self.l0,
            &view.manifest,
            &opened.manifest,
            &opened.sealed,
            crate::util::now_secs(),
        );
        let what = "applying the store's manifest";
        let l0 = match self
            .apply_retry
            .note(opened.etag.clone(), self.poll, what, pruned)
        {
            Ok(l0) => l0,
            Err(e) => {
                self.pending = Some(opened);
                return Err(e);
            }
        };
        let Opened {
            manifest,
            etag,
            sealed,
        } = *opened;
        (self.sealed, self.l0) = (sealed, l0);
        let next = self.snapshot(manifest, etag);
        crate::log!(
            "serve: manifest generation {} ({} slabs, {} L0)",
            next.manifest.generation,
            next.all.len(),
            next.l0_len
        );
        ctx.metrics.manifest_loads.fetch_add(1, Ordering::Relaxed);
        let next = Arc::new(next);
        ctx.publish(next.clone());
        if view.manifest.baseline.id != next.manifest.baseline.id {
            note_indexes(ctx, &next, true);
        }
        warmer.offer(next);
        // The tip may now be behind the upstream: the upstream's turn catches up.
        self.last_head = Oid::ZERO;
        self.retry = Retry::default();
        Ok(())
    }

    /// Brings L0 up to the upstream branch. Returns whether a new snapshot was published.
    fn sync_upstream(&mut self, ctx: &Ctx) -> Result<bool> {
        let Some(up) = self.upstream.as_mut() else {
            return Ok(false);
        };
        let head = up.head().ctx("reading the upstream's head")?;
        let view = ctx.view();
        if head == view.tip || head == self.last_head || !self.retry.due(&head, Instant::now()) {
            return Ok(false);
        }
        let ingested = self.ingest(ctx, &view, head);
        self.retry
            .note(head, self.poll, &format!("ingesting {head}"), ingested)
    }

    /// The L0 slabs follow the upstream branch to `head`. Returns whether a new snapshot
    /// was published.
    fn ingest(&mut self, ctx: &Ctx, view: &View, head: Oid) -> Result<bool> {
        // What the tip knows of the L0 slabs (their commits' stamps, the boundaries a forced
        // push took away) changes with them or not at all: a move that fails halfway, its
        // slabs dropped on paper but still served, is tried again from where it began.
        let before = self.tip.state();
        let (moved, dropped) = match self.advance(view, head) {
            Ok(Some(moved)) => moved,
            Ok(None) => {
                self.last_head = head;
                return Ok(false);
            }
            Err(e) => {
                self.tip.restore(before);
                return Err(e);
            }
        };
        self.last_head = head;
        if dropped > 0 {
            crate::log!(
                "serve: forced push to {head}: dropped {dropped} L0 slabs the branch no longer \
                 reaches"
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
        retain_l0(&mut self.l0, view.manifest.max_ts, crate::util::now_secs());
        let next = self.snapshot(view.manifest.clone(), view.etag.clone());
        ctx.metrics.l0_ingests.fetch_add(1, Ordering::Relaxed);
        ctx.publish(Arc::new(next));
        Ok(true)
    }

    /// How the L0 slabs follow the branch to `head`, the new slab with the sizes it
    /// introduces, and how many slabs go. Everything about the move that can fail.
    fn advance(&mut self, view: &View, head: Oid) -> Result<Option<(Moved, usize)>> {
        let Some(up) = self.upstream.as_mut() else {
            return Ok(None);
        };
        let Some(moved) = self.tip.advance(up, &view.manifest, &self.l0, head)? else {
            return Ok(None);
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
        Ok(Some((moved, dropped.len())))
    }
}

/// Whether `<id>.<ext>` in the edge's cache is a slab's file that the manifest, whose slab
/// ids are `keep`, no longer lists. Only names the edge gives its downloads count
/// (`<slab id>.pack`, `.mphf`, `.sizes`, `.tail`): `--cache-dir` is the operator's
/// choice, and what else it holds is not the edge's to delete. (Any `*.pack` there went.)
fn stale_slab_file(id: &str, ext: &str, keep: &std::collections::HashSet<&str>) -> bool {
    matches!(ext, "pack" | "mphf" | "sizes" | "tail")
        && crate::slab::SlabMeta::is_id(id)
        && !keep.contains(id)
}

/// The same for a segment index (`<40 hex digits>.bidx`) that `blobs.bin`, whose segment
/// ids are `keep`, no longer lists.
fn stale_segment_index(id: &str, ext: &str, keep: &std::collections::HashSet<&str>) -> bool {
    ext == "bidx"
        && id.len() == 40
        && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        && !keep.contains(id)
}

/// Removes the files of `dir` (not its directories) named `<id>.<ext>` that `doomed` picks,
/// and downloads in progress (`.<name>.tmp-<pid>-<n>`) of processes no longer running.
/// Returns how many went.
fn prune_dir(dir: &Path, doomed: impl Fn(&str, &str) -> bool) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut removed = 0usize;
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|t| t.is_file()) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let gone = match name.strip_prefix('.') {
            Some(temp) => temp
                .rsplit_once(".tmp-")
                .and_then(|(_, tail)| tail.split('-').next()?.parse::<u32>().ok())
                .is_some_and(|pid| {
                    pid != std::process::id() && !Path::new(&format!("/proc/{pid}")).exists()
                }),
            None => name
                .rsplit_once('.')
                .is_some_and(|(id, ext)| doomed(id, ext)),
        };
        if gone && std::fs::remove_file(entry.path()).is_ok() {
            removed = removed.saturating_add(1);
        }
    }
    removed
}

/// The kind of memory filesystem `path` is on (`tmpfs`, `ramfs`), if it is on one.
fn in_memory(path: &Path) -> Option<&'static str> {
    const RAMFS_MAGIC: i64 = 0x8584_58f6;
    let kind = nix::sys::statfs::statfs(path).ok()?.filesystem_type();
    if kind == nix::sys::statfs::TMPFS_MAGIC {
        Some("tmpfs")
    } else if kind.0 == RAMFS_MAGIC {
        Some("ramfs")
    } else {
        None
    }
}

/// Slab `meta` of the files `pack` and `index` in the cache. A pack that is `new` there
/// (downloaded, or made of a tail) is hashed: it must be the pack whose SHA-1 the manifest
/// lists, not only have its length and end with it. One that is not is not kept.
fn listed(meta: &crate::slab::SlabMeta, pack: &Path, index: &Path, new: bool) -> Result<Arc<Slab>> {
    let checked = Slab::open(pack, index, meta.clone()).and_then(|slab| {
        if new {
            use sha1::Digest;
            let bytes = slab.pack();
            let body = bytes.get(..bytes.len().saturating_sub(20)).unwrap_or(&[]);
            let sha: [u8; 20] = sha1::Sha1::digest(body).into();
            if sha != meta.pack_sha {
                crate::bail!(
                    "the pack is not the one listed: its SHA-1 is {}",
                    crate::util::hex_string(&sha)
                );
            }
        }
        Ok(slab)
    });
    if checked.is_err() && new {
        let _ = std::fs::remove_file(pack);
    }
    checked
}

/// Downloads `key` to `path` unless a complete copy is already there. Returns whether it
/// did.
fn hydrate(store: &Store, key: &str, path: &Path) -> Result<bool> {
    if path.exists() {
        return Ok(false);
    }
    let t0 = std::time::Instant::now();
    store.download_file(key, path)?;
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    crate::log!(
        "serve: hydrated {key} ({}) in {:.2}s",
        crate::util::human_bytes(size),
        t0.elapsed().as_secs_f64()
    );
    Ok(true)
}

/// The `/metrics` text (cold path).
pub fn metrics_text(ctx: &Ctx) -> String {
    let m = &ctx.metrics;
    let a = gvfs_alloc_guard::stats();
    let v = ctx.view();
    let get = |c: &std::sync::atomic::AtomicU64| c.load(Ordering::Relaxed);
    let proxied = ctx.proxy.as_ref().map_or((0, 0, 0), blobs::Proxy::kept);
    let blocks = ctx.blob_cache.as_ref().map_or((0, 0), |c| c.kept());
    format!(
        "gvfs_requests_total{{endpoint=\"prefetch\"}} {}\n\
         gvfs_requests_total{{endpoint=\"objects_get\"}} {}\n\
         gvfs_requests_total{{endpoint=\"objects_post\"}} {}\n\
         gvfs_requests_total{{endpoint=\"sizes\"}} {}\n\
         gvfs_requests_total{{endpoint=\"config\"}} {}\n\
         gvfs_prefetch_packs_total {}\n\
         gvfs_prefetch_plans_total {}\n\
         gvfs_not_found_total {}\n\
         gvfs_busy_total {}\n\
         gvfs_errors_total {}\n\
         gvfs_bytes_out_total {}\n\
         gvfs_objects_out_total {}\n\
         gvfs_l0_ingests_total {}\n\
         gvfs_manifest_loads_total {}\n\
         gvfs_live_failures_total {}\n\
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
         gvfs_blob_cache_blocks {}\n\
         gvfs_blob_cache_fetching {}\n\
         gvfs_proxy_cache_packs {}\n\
         gvfs_proxy_cache_files {}\n\
         gvfs_proxy_cache_bytes {}\n\
         gvfs_connections_idle {}\n\
         gvfs_connections_idle_closed_total {}\n\
         gvfs_connections_idle_overflow_total {}\n\
         gvfs_connections_open {}\n\
         gvfs_connections_full_total {}\n\
         gvfs_buffer_spare_bytes {}\n\
         gvfs_grown_objects {}\n\
         gvfs_index_bytes {}\n\
         gvfs_index_copied_bytes {}\n\
         gvfs_index_huge_bytes {}\n\
         gvfs_baselines_grown_total {}\n\
         gvfs_baseline_indexes_grown_total {}\n",
        get(&m.prefetch),
        get(&m.object_get),
        get(&m.object_post),
        get(&m.sizes),
        get(&m.config),
        get(&m.prefetch_packs),
        get(&crate::splice::TRAILERS),
        get(&m.not_found),
        get(&m.busy),
        get(&m.errors),
        get(&m.bytes_out),
        get(&m.objects_out),
        get(&m.l0_ingests),
        get(&m.manifest_loads),
        get(&m.live_failures),
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
        blocks.1,
        blocks.0,
        ctx.blob_cache.as_ref().map_or(0, |c| c.fetching()),
        proxied.0,
        proxied.1,
        proxied.2,
        ctx.idle.load(Ordering::Relaxed).max(0),
        get(&m.idle_closed),
        get(&m.idle_overflow),
        get(&m.conns_open),
        get(&m.conns_full),
        ctx.spare.left(),
        ctx.arenas.grown(),
        get(&m.index_bytes),
        get(&m.index_copied),
        get(&m.index_huge),
        get(&GROWN),
        get(&GROWN_INDEXES)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A head whose ingest fails is tried again after one poll interval, then after twice
    /// as long each time, up to a minute; another head is tried at once and starts over.
    #[test]
    #[allow(clippy::arithmetic_side_effects)]
    fn a_failing_head_is_retried_less_and_less_often() {
        let poll = Duration::from_secs(2);
        let (a, b) = (Oid([1; 20]), Oid([2; 20]));
        let mut now = Instant::now();
        let mut retry = Retry::default();
        assert!(retry.due(&a, now));
        let mut waits = Vec::new();
        for _ in 0..8 {
            let wait = retry.failed(a, poll, now);
            assert!(!retry.due(&a, now + wait - Duration::from_millis(1)));
            assert!(retry.due(&a, now + wait));
            assert!(retry.due(&b, now));
            waits.push(wait.as_secs());
            now += wait;
        }
        assert_eq!(waits, [2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(retry.failed(b, poll, now), poll);
        assert_eq!(retry.failed(a, poll, now), poll);
        // A success forgets them.
        assert!(retry.note(a, poll, "trying", Ok(())).is_ok());
        assert!(retry.due(&a, now));
        let e = retry.note(a, poll, "trying", Err::<(), _>(crate::err!("no")));
        assert_eq!(
            e.unwrap_err().to_string(),
            "trying (attempt 1, the next in 2s): no"
        );
    }

    /// Memory filesystems are told apart from the rest (`/dev/shm` is tmpfs on Linux).
    #[test]
    fn memory_filesystems_are_recognised() {
        if Path::new("/dev/shm").is_dir() {
            assert_eq!(in_memory(Path::new("/dev/shm")), Some("tmpfs"));
        }
        assert_eq!(in_memory(Path::new("/proc")), None);
    }

    /// Pruning removes the files `doomed` picks and downloads of dead processes, and leaves
    /// directories, other names, and downloads still running (this process's).
    #[test]
    fn pruning_leaves_what_is_still_used() {
        let dir = std::env::temp_dir().join(format!("gvd-prune-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("blobs")).unwrap();
        let me = std::process::id();
        let files = [
            "kept.pack".to_string(),
            "gone.pack".to_string(),
            "gone.sizes".to_string(),
            "notes.txt".to_string(),
            ".dead.pack.tmp-999999999-3".to_string(),
            format!(".live.pack.tmp-{me}-4"),
        ];
        for f in &files {
            std::fs::write(dir.join(f), b"x").unwrap();
        }
        let removed = prune_dir(&dir, |id, ext| id == "gone" && ext != "txt");
        assert_eq!(removed, 3);
        let mut left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        let mut want = vec![
            "blobs".to_string(),
            "kept.pack".to_string(),
            "notes.txt".to_string(),
            format!(".live.pack.tmp-{me}-4"),
        ];
        want.sort();
        assert_eq!(left, want);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What an edge prunes from its cache are its own downloads no longer listed, known by
    /// their names; a file that only shares an extension with them is another program's.
    #[test]
    fn pruning_goes_by_the_names_of_the_edges_downloads() {
        let listed = "base-10-0123456789abcdef";
        let keep: std::collections::HashSet<&str> = [listed].into();
        let git_pack = format!("pack-{}", "c".repeat(40));
        for (id, ext, stale) in [
            ("m15-12-0123456789abcdef", "pack", true),
            ("m15-12-0123456789abcdef", "mphf", true),
            ("week-7-fedcba9876543210", "sizes", true),
            ("base-9-fedcba9876543210", "tail", true),
            (listed, "pack", false),
            (listed, "tail", false),
            (git_pack.as_str(), "pack", false),
            ("data", "pack", false),
            ("m15-12", "mphf", false),
            ("m15-12-0123456789ABCDEF", "pack", false),
            ("report", "sizes", false),
            ("m15-12-0123456789abcdef", "txt", false),
        ] {
            assert_eq!(stale_slab_file(id, ext, &keep), stale, "{id}.{ext}");
        }
        let (kept, gone) = ("a".repeat(40), "b".repeat(40));
        let keep: std::collections::HashSet<&str> = [kept.as_str()].into();
        assert!(stale_segment_index(&gone, "bidx", &keep));
        assert!(!stale_segment_index(&kept, "bidx", &keep));
        assert!(!stale_segment_index("index", "bidx", &keep));
        assert!(!stale_segment_index(&gone, "pack", &keep));
    }

    #[test]
    fn only_sealed_l0_slabs_age_out() {
        let (max_ts, newest) = (1_000_000, 1_000_000 + 10 * L0_RETAIN);
        // Unsealed: kept however old, e.g. while the pipeline is away.
        assert!(keep_l0(max_ts + 1, max_ts, newest));
        // Sealed: kept for L0_RETAIN after the newest boundary, then dropped.
        assert!(keep_l0(max_ts, max_ts, max_ts + L0_RETAIN));
        assert!(!keep_l0(max_ts, max_ts, max_ts + L0_RETAIN + 1));
    }

    /// A commit dated in the future puts the boundaries after it ahead of the clock, one
    /// second apart per commit. The sealed slabs from before it age by the clock (they
    /// all went at once), and those after it, which six hours of stamps would keep for
    /// 21,600 commits, are bounded in number.
    #[test]
    fn sealed_l0_slabs_age_by_the_clock_once_stamps_are_ahead_of_it() {
        let now = 1_800_000_000u64;
        let ahead = now + 3650 * 86_400;
        // Slabs sealed over the last six hours, then one that ends at the date.
        let mut l0: Vec<u64> = (0..=6).rev().map(|h| now - h * 3600).collect();
        l0.push(ahead);
        let before = l0.clone();
        // The manifest has sealed them all: none goes, the newest counted as now.
        retain_ends(&mut l0, ahead, now, |end| *end);
        assert_eq!(l0, before);
        // An hour later, the oldest has aged out.
        retain_ends(&mut l0, ahead, now + 3600, |end| *end);
        assert_eq!(l0, before[1..]);
        // Without the clock, the stamps alone drop every one of them but the last.
        let mut by_stamps = before.clone();
        retain_ends(&mut by_stamps, ahead, u64::MAX, |end| *end);
        assert_eq!(by_stamps, [ahead]);
        // Ten thousand moves of the branch, sealed: the newest L0_SEALED_MAX stay.
        let mut l0: Vec<u64> = (0..10_000).map(|i| ahead + i).collect();
        let max_ts = ahead + 9_990;
        retain_ends(&mut l0, max_ts, now, |end| *end);
        let sealed: Vec<u64> = l0.iter().copied().filter(|end| *end <= max_ts).collect();
        assert_eq!(sealed.len(), L0_SEALED_MAX);
        assert_eq!(sealed.last().copied(), Some(max_ts));
        // The unsealed ones all stay.
        assert_eq!(l0.len(), L0_SEALED_MAX + 9);
    }
}
