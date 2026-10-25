//! `gvfs-daemon serve`: the GVFS server. The same code is the remote edge
//! (`--bind 0.0.0.0:8080`) and the localhost proxy (`--bind 127.0.0.1:8080`).
//!
//! Boot (also crash recovery — there is no other state):
//! 1. read `manifest.bin` (waiting for genesis if the store is empty);
//! 2. hydrate its slabs: open them in place (directory store) or download them into
//!    `--cache-dir`, then map them;
//! 3. with `--upstream`, rebuild the live tip: `ingest_git_delta(manifest.tip, HEAD)`,
//!    repacked into the in-RAM L0 slab — before the port opens;
//! 4. allocate every worker buffer and request arena, then seal the allocator and serve.
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
//! Requests are served by a fixed set of threads, each with its own buffers, all waiting on
//! one epoll set that holds the listening socket and every idle keep-alive connection
//! ([`conns`]): an idle client costs a table slot, not a thread. POSTs lease a request arena.
//! Large slab ranges leave through `sendfile(2)`.

pub mod conns;
pub mod handlers;
pub mod httpd;
pub mod view;

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::arena::ArenaPool;
use crate::ingest::{Dest, Indexer, ingest_git_delta};
use crate::manifest::{MANIFEST_KEY, Manifest};
use crate::repack::{Mode, Repacker};
use crate::slab::{Slab, Tier};
use crate::splice::Resolver;
use crate::store::Store;
use crate::upstream::Upstream;
use crate::util::{Context, Oid, Result};
use handlers::Metrics;
use httpd::Input;
use view::{Prefetch, View};

/// Which prefetch clients get the pack's `.idx`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IdxPolicy {
    /// Everyone but git itself (gvfs-helper re-indexes every pack and would discard it).
    Auto,
    Always,
    Never,
}

/// Shared by every worker.
pub struct Ctx {
    view: RwLock<Arc<View>>,
    pub arenas: ArenaPool,
    pub arena_wait: Duration,
    pub config_json: Vec<u8>,
    pub metrics: Metrics,
    pub ready: AtomicBool,
    pub prefetch_idx: IdxPolicy,
    /// A client was sent an `.idx`: the warmer builds the next cold plan's index too.
    pub idx_wanted: AtomicBool,
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

/// Per-worker buffers, allocated once.
pub struct Worker {
    pub input: Input,
    pub out: Vec<u8>,
}

pub struct Options {
    pub store: Store,
    pub upstream: Option<Upstream>,
    pub bind: String,
    pub cache_dir: PathBuf,
    pub poll: Duration,
    pub workers: usize,
    pub arenas: usize,
    pub arena_bytes: usize,
    pub arena_objects: usize,
    pub cache_servers: Vec<(String, String)>,
    pub idle_timeout: Duration,
    pub write_timeout: Duration,
    pub prefetch_idx: IdxPolicy,
    /// Idle keep-alive connections that can wait at once (beyond that, the oldest idle
    /// clients must reconnect).
    pub max_idle_conns: usize,
    /// How long a worker waits on a keep-alive connection for its next request before
    /// parking it.
    pub linger: Duration,
    /// While no worker is free, how long a worker keeps serving one connection before
    /// parking it between requests, so busy connections beyond `workers` take turns.
    pub hold: Duration,
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
            "cache-server",
            "alloc-guard",
            "idle-secs",
            "max-idle-conns",
            "linger-ms",
            "hold-ms",
            "prefetch-idx",
        ],
    )?;
    match f.get("alloc-guard").unwrap_or("count") {
        "count" => gvfs_alloc_guard::set_mode(gvfs_alloc_guard::Mode::Count),
        "strict" => gvfs_alloc_guard::set_mode(gvfs_alloc_guard::Mode::Strict),
        "trace" => gvfs_alloc_guard::set_trace(true),
        other => crate::bail!("--alloc-guard must be count, strict or trace, not {other:?}"),
    }
    let upstream = match f.get("upstream") {
        Some(u) => Some(Upstream::open(
            u,
            f.get("branch").unwrap_or("main"),
            &crate::ingest::auth_from_flags(&f)?,
        )?),
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
    let cores = crate::util::default_threads();
    let opts = Options {
        store: Store::open(
            f.require("store")?,
            f.get("store-auth-env")
                .unwrap_or("AZURE_STORAGE_CONNECTION_STRING"),
        )?,
        upstream,
        bind: f.get("bind").unwrap_or("127.0.0.1:8080").to_string(),
        cache_dir: f
            .get("cache-dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("gvfs-daemon-cache")),
        poll: Duration::from_millis(f.num("poll-ms", 2000u64)?),
        workers: f.num("workers", 256usize)?,
        arenas: f.num("arenas", cores.saturating_mul(2))?,
        arena_bytes: f.num("arena-mib", 8usize)?.saturating_mul(1 << 20),
        arena_objects: f.num("arena-objects", 1usize << 18)?,
        cache_servers,
        idle_timeout: Duration::from_secs(f.num("idle-secs", 30u64)?),
        write_timeout: Duration::from_secs(120),
        prefetch_idx,
        max_idle_conns: f.num("max-idle-conns", 65536usize)?,
        linger: Duration::from_millis(f.num("linger-ms", 5u64)?),
        hold: Duration::from_millis(f.num("hold-ms", 5u64)?),
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
        bind,
        cache_dir,
        poll,
        workers,
        arenas,
        arena_bytes,
        arena_objects,
        cache_servers,
        idle_timeout,
        write_timeout,
        prefetch_idx,
        max_idle_conns,
        linger,
        hold,
    } = o;
    let mut live = Live {
        store,
        upstream,
        tip: LiveTip::default(),
        cache_dir,
        opened: HashMap::new(),
        sealed: Vec::new(),
        l0: Vec::new(),
        last_head: Oid::ZERO,
        generation: 0,
    };

    // 1-3. Manifest, slabs, live tip.
    let (manifest, etag) = live.wait_for_manifest()?;
    live.open_manifest(&manifest)?;
    let first = live.snapshot(manifest, etag);
    let ctx = Arc::new(Ctx {
        view: RwLock::new(Arc::new(first)),
        arenas: ArenaPool::new(arenas.max(1), arena_bytes, arena_objects),
        arena_wait: Duration::from_secs(10),
        config_json: config_json(&cache_servers),
        metrics: Metrics::default(),
        ready: AtomicBool::new(false),
        prefetch_idx,
        idx_wanted: AtomicBool::new(prefetch_idx == IdxPolicy::Always),
    });
    if let Err(e) = live.sync_upstream(&ctx) {
        crate::log!("serve: live tip not caught up at boot ({e}); serving sealed slabs");
    }
    let warmer = Arc::new(Warmer::default());
    warmer.offer(ctx.view());

    // 4. Workers, then seal.
    raise_fd_limit();
    let listener = TcpListener::bind(&bind).ctx(&format!("binding {bind}"))?;
    // std listens with a backlog of 128: a burst of new clients (a fleet starting at 09:00,
    // every client reconnecting after a restart) overflows it, and the rest wait for SYN
    // retries (1 s, 3 s, ...). Listening again raises the queue to the system's limit
    // (`net.core.somaxconn`).
    if let Err(e) = nix::sys::socket::listen(&listener, nix::sys::socket::Backlog::MAXALLOWABLE) {
        crate::log!("serve: could not raise the listen backlog: {e}");
    }
    let conns = Arc::new(conns::Conns::new(&listener, max_idle_conns)?);
    let started = Arc::new(Barrier::new(workers.saturating_add(1)));
    for i in 0..workers {
        let listener = listener.try_clone()?;
        let ctx = ctx.clone();
        let conns = conns.clone();
        let started = started.clone();
        std::thread::Builder::new()
            .name(format!("gvfs-w{i}"))
            .stack_size(512 << 10)
            .spawn(move || {
                let mut w = Worker {
                    input: Input::new(),
                    out: Vec::with_capacity(256 << 10),
                };
                w.input.patience = idle_timeout;
                started.wait();
                let pace = Pace {
                    linger,
                    hold,
                    write: write_timeout,
                };
                worker_loop(&listener, &conns, &ctx, &mut w, i, pace);
            })?;
    }
    {
        let conns = conns.clone();
        std::thread::Builder::new()
            .name("gvfs-reaper".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    conns.reap(idle_timeout);
                }
            })?;
    }
    {
        let warmer = warmer.clone();
        let ctx = ctx.clone();
        std::thread::Builder::new()
            .name("gvfs-warmer".into())
            .spawn(move || warmer.run(&ctx))?;
    }
    started.wait();
    gvfs_alloc_guard::seal();
    ctx.ready.store(true, Ordering::Relaxed);
    let v = ctx.view();
    crate::log!(
        "serving on http://{bind}: {} slabs ({} in L0), tip {}, {}",
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

/// Raises the soft open-file limit to the hard limit: every idle client holds a descriptor.
fn raise_fd_limit() {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};
    if let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_NOFILE) {
        if soft < hard {
            let _ = setrlimit(Resource::RLIMIT_NOFILE, hard, hard);
        }
    }
}

/// A worker's per-connection timeouts (see [`Options`]).
#[derive(Clone, Copy)]
struct Pace {
    linger: Duration,
    hold: Duration,
    write: Duration,
}

fn worker_loop(
    listener: &TcpListener,
    conns: &conns::Conns,
    ctx: &Ctx,
    w: &mut Worker,
    index: usize,
    pace: Pace,
) {
    loop {
        let Some((mut stream, fresh)) = conns.next(listener) else {
            continue;
        };
        if fresh {
            let _ = stream.set_nodelay(true);
            let _ = stream.set_read_timeout(Some(pace.linger));
            let _ = stream.set_write_timeout(Some(pace.write));
        }
        w.input.filled = 0;
        w.input.patient = false;
        if serve_connection(ctx, conns, w, &mut stream, pace) {
            conns.park(stream, !fresh, index);
        }
    }
}

/// Serves requests until the client closes, errs, or pauses for longer than `pace.linger`
/// between requests, or — while no worker is free — until this worker has held the
/// connection for `pace.hold`. Then `true`: the connection is parked between requests (it no
/// longer holds this worker; the epoll set hands it to whichever worker is free when it
/// speaks again, after the connections that were already waiting).
fn serve_connection(
    ctx: &Ctx,
    conns: &conns::Conns,
    w: &mut Worker,
    stream: &mut TcpStream,
    pace: Pace,
) -> bool {
    let picked = Instant::now();
    loop {
        if w.input.patient {
            if stream.set_read_timeout(Some(pace.linger)).is_err() {
                return false;
            }
            w.input.patient = false;
        }
        if w.input.filled == 0 && conns.all_busy() && picked.elapsed() >= pace.hold {
            return true;
        }
        let req = match httpd::read_head(stream, &mut w.input) {
            Ok(httpd::Head::Request(r)) => r,
            Ok(httpd::Head::Idle) => return w.input.filled == 0,
            Ok(httpd::Head::Closed) => return false,
            Err(_) => {
                let _ = httpd::respond_text(stream, 400, "bad request\n", false);
                return false;
            }
        };
        let _hot = gvfs_alloc_guard::hot_path();
        match handlers::handle(ctx, w, stream, &req) {
            Ok(true) => {}
            Ok(false) | Err(_) => return false,
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
                if let Prefetch::Pack(plan) = view.prefetch(prev_tip_ts) {
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
}

impl Default for LiveTip {
    fn default() -> Self {
        LiveTip {
            indexer: Indexer::new(1),
            repacker: Repacker::new(),
            resolver: Resolver::default(),
            stamps: HashMap::new(),
        }
    }
}

impl LiveTip {
    /// The L0 slab for the upstream branch's move to `head`: the commits since the newest
    /// boundary (the newest live L0 slab's tip, or the manifest's), fetched once, repacked,
    /// and stamped from its commits. `None` when the upstream had nothing new.
    pub fn advance(
        &mut self,
        up: &mut Upstream,
        m: &Manifest,
        l0: &[Arc<Slab>],
        head: Oid,
    ) -> Result<Option<Arc<Slab>>> {
        let (from, from_ts) = match l0.last() {
            Some(s) if s.meta.end_ts > m.max_ts => (s.meta.want, s.meta.end_ts),
            _ => (m.tip, m.max_ts),
        };
        if from == head {
            return Ok(None);
        }
        let mut haves = vec![from];
        haves.extend(m.known_tips().into_iter().filter(|t| *t != from));
        let Some(raw) = ingest_git_delta(
            up,
            &mut self.indexer,
            &haves,
            head,
            Tier::L0,
            (from_ts, from_ts),
            Dest::Memory,
        )?
        else {
            return Ok(None);
        };
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
        let out = self.pruned(l0, old, m, sealed)?;
        // Sealed commits are stamped from the manifest from now on, as the pipeline does.
        let live: Vec<&Arc<Slab>> = out.iter().filter(|s| s.meta.start_ts >= m.max_ts).collect();
        self.stamps
            .retain(|c, _| live.iter().any(|s| s.find(c).is_some()));
        Ok(out)
    }

    fn pruned(
        &mut self,
        l0: &[Arc<Slab>],
        old: &Manifest,
        m: &Manifest,
        sealed: &[Arc<Slab>],
    ) -> Result<Vec<Arc<Slab>>> {
        let mut out = l0.to_vec();
        if !l0.is_empty() && m.tip != old.tip {
            if let Some(s) = l0.iter().find(|s| s.meta.want == m.tip) {
                if s.meta.end_ts != m.max_ts {
                    return Ok(Vec::new());
                }
            } else if l0
                .last()
                .is_some_and(|s| sealed.iter().any(|x| x.find(&s.meta.want).is_some()))
            {
                // The pipeline got ahead of L0: all of it is sealed.
            } else if let Some(j) = l0.iter().position(|s| s.find(&m.tip).is_some()) {
                let Some(s) = l0.get(j).filter(|s| s.meta.end_ts > m.max_ts) else {
                    return Ok(Vec::new());
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
                    out.insert(j.saturating_add(1), rest.slab);
                }
            } else {
                return Ok(Vec::new());
            }
        }
        retain_l0(&mut out, m.max_ts);
        Ok(out)
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
    store: Store,
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
}

impl Live {
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
                None => self.open(meta)?,
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
        if let Some((bytes, etag)) = self.store.get(MANIFEST_KEY)? {
            if etag != view.etag {
                let m = Manifest::decode(&bytes)?;
                if m.generation > view.manifest.generation {
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
        let advanced = self.tip.advance(up, &view.manifest, &self.l0, head)?;
        self.last_head = head;
        let Some(slab) = advanced else {
            return Ok(false);
        };
        crate::log!(
            "serve: L0 {} ({} objects, {}) {}..{head}",
            slab.meta.id,
            slab.meta.objects,
            crate::util::human_bytes(slab.meta.pack_len),
            slab.meta.have
        );
        self.l0.push(slab);
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
         gvfs_alloc_post_seal_total {}\n",
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
        a.post_seal
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
