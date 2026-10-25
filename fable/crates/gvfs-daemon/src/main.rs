//! `gvfs-daemon`: one binary, several modes, two primitives.
//!
//! * `serve`    — the stateless edge / localhost proxy (GVFS protocol over HTTP).
//! * `pipeline` — the scheduled slab builder (`--tier m15|h1|h6|h12|h24|h168`).
//! * `ingest`, `splice`, `inspect`, `lookup` — the primitives and store tools by hand.
#![forbid(unsafe_code)]

use std::io::Write;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use gvfs_daemon::cli;
use gvfs_daemon::indexer::ResolveConfig;
use gvfs_daemon::ingest::IngestConfig;
use gvfs_daemon::manifest::Tier;
use gvfs_daemon::oid::Oid;
use gvfs_daemon::pipeline::{BaselineMode, PipelineConfig, run_pipeline};
use gvfs_daemon::store::FsStore;

#[global_allocator]
static GLOBAL: gvfs_alloc_guard::SealedAllocator = gvfs_alloc_guard::SealedAllocator;

#[derive(Parser)]
#[command(
    name = "gvfs-daemon",
    version,
    about = "Unified GVFS monorepo acceleration layer: edge/localhost proxy and slab pipeline in one binary"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TierArg {
    M15,
    H1,
    H6,
    H12,
    H24,
    H168,
    Baseline,
}

impl From<TierArg> for Tier {
    fn from(t: TierArg) -> Self {
        match t {
            TierArg::M15 => Tier::M15,
            TierArg::H1 => Tier::H1,
            TierArg::H6 => Tier::H6,
            TierArg::H12 => Tier::H12,
            TierArg::H24 => Tier::H24,
            TierArg::H168 => Tier::Weekly,
            TierArg::Baseline => Tier::Baseline,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BaselineArg {
    Fetch,
    Splice,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RollupTierArg {
    Never,
    H1,
    H6,
    H12,
    H24,
    H168,
}

impl RollupTierArg {
    fn tier(self) -> Option<Tier> {
        match self {
            Self::Never => None,
            Self::H1 => Some(Tier::H1),
            Self::H6 => Some(Tier::H6),
            Self::H12 => Some(Tier::H12),
            Self::H24 => Some(Tier::H24),
            Self::H168 => Some(Tier::Weekly),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum GuardArg {
    Off,
    Count,
    Strict,
}

#[derive(clap::Args, Clone)]
struct UpstreamArgs {
    /// Upstream repository: an http(s) smart-HTTP URL (Azure DevOps, GitHub) or a local path.
    #[arg(long, env = "GVFS_UPSTREAM")]
    upstream: String,
    /// Branch to track.
    #[arg(long, default_value = "main")]
    branch: String,
    /// Environment variable holding the PAT / token (sent as HTTP Basic; never on the command line).
    #[arg(long)]
    token_env: Option<String>,
    /// Basic-auth user for the token (empty for Azure DevOps PATs, `x-access-token` for GitHub).
    #[arg(long, default_value = "")]
    token_user: String,
}

/// `serve` can run without an upstream (a pure cache of the store), so its upstream is optional.
#[derive(clap::Args, Clone)]
struct UpstreamArgsOpt {
    /// Upstream repository for the live (<15 min) slab: URL or local path.
    #[arg(long, env = "GVFS_UPSTREAM")]
    upstream: Option<String>,
    #[arg(long, default_value = "main")]
    branch: String,
    #[arg(long)]
    token_env: Option<String>,
    #[arg(long, default_value = "")]
    token_user: String,
}

#[derive(clap::Args, Clone)]
struct ResolveArgs {
    /// Delta-resolution worker threads (default: cores, at most 16).
    #[arg(long)]
    workers: Option<usize>,
    /// Per-worker arena for the objects of a delta chain, in MiB.
    #[arg(long, default_value_t = 256)]
    arena_mib: usize,
    /// Keep upstream deltas on commits/blobs/tags too (default: only trees stay deltified).
    #[arg(long)]
    keep_upstream_deltas: bool,
    /// De-deltify non-tree objects only up to this inflated size in bytes (pointer blobs
    /// and commits qualify; real source blobs keep their upstream deltas).
    #[arg(long, default_value_t = gvfs_daemon::indexer::DEFAULT_FULL_MAX_BYTES)]
    full_max_bytes: usize,
}

impl ResolveArgs {
    fn config(&self) -> IngestConfig {
        let mut r = ResolveConfig::default();
        if let Some(w) = self.workers {
            r.workers = w.max(1);
        }
        r.arena_bytes = self.arena_mib.max(1).saturating_mul(1 << 20);
        r.side_bytes = (self.arena_mib.max(1) / 4).max(16).saturating_mul(1 << 20);
        r.full_non_trees = !self.keep_upstream_deltas;
        r.full_max_bytes = self.full_max_bytes;
        IngestConfig { resolve: r }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Run the GVFS edge (or localhost proxy): /gvfs/config, /gvfs/prefetch, /gvfs/objects, /gvfs/sizes.
    Serve {
        /// Listen address (127.0.0.1:8080 for a workstation proxy, 0.0.0.0:8080 for a shared edge).
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        /// Slab store directory (filled by `pipeline`).
        #[arg(long, env = "GVFS_STORE")]
        store: PathBuf,
        #[command(flatten)]
        upstream: UpstreamArgsOpt,
        /// Request arenas (bounds concurrent POST /gvfs/objects requests).
        #[arg(long, default_value_t = 16)]
        arenas: usize,
        /// Size of each request arena, in MiB.
        #[arg(long, default_value_t = 64)]
        arena_mib: usize,
        /// Capacity of the in-memory live (<15 min) slab, in MiB.
        #[arg(long, default_value_t = 64)]
        l0_mib: usize,
        /// How often to poll the upstream branch for the live slab, in seconds (0 = never).
        #[arg(long, default_value_t = 2)]
        poll_secs: u64,
        /// How often to re-read manifest.bin, in seconds.
        #[arg(long, default_value_t = 30)]
        manifest_poll_secs: u64,
        /// Cache server advertised by GET /gvfs/config ("name=url", repeatable).
        #[arg(long)]
        cache_server: Vec<String>,
        /// Allocation guard after warmup: count (metrics), strict (abort on a hot-path allocation), off.
        #[arg(long, value_enum, default_value_t = GuardArg::Count)]
        alloc_guard: GuardArg,
    },
    /// Run one scheduled pipeline step: ingest since the last tip, roll up, publish.
    Pipeline {
        #[arg(long, value_enum)]
        tier: TierArg,
        #[arg(long, env = "GVFS_STORE")]
        store: PathBuf,
        #[command(flatten)]
        upstream: UpstreamArgs,
        /// How the weekly baseline is rebuilt.
        #[arg(long, value_enum, default_value_t = BaselineArg::Fetch)]
        baseline: BaselineArg,
        /// Weeks of weekly deltas kept for warm clients.
        #[arg(long, default_value_t = 4)]
        retain_weeks: u64,
        /// Rollups of this tier and above are re-fetched from the upstream as one window
        /// (git-optimal deltas, 3–5x smaller); below it they are spliced. `never` = always splice.
        #[arg(long, value_enum, default_value_t = RollupTierArg::H24)]
        rollup_fetch_from: RollupTierArg,
        /// Rollups below this tier keep their leaves, so a warm client inside the window gets
        /// only the slabs it lacks (bandwidth) instead of the whole rollup (bounded pack count).
        /// `never` = pure superset rollups.
        #[arg(long, value_enum, default_value_t = RollupTierArg::Never)]
        leaf_tier: RollupTierArg,
        #[command(flatten)]
        resolve: ResolveArgs,
    },
    /// Build one slab by hand: ingest_git_delta(have, want) into the store.
    Ingest {
        #[arg(long, env = "GVFS_STORE")]
        store: PathBuf,
        #[command(flatten)]
        upstream: UpstreamArgs,
        /// Object to fetch (default: the branch tip).
        #[arg(long)]
        want: Option<Oid>,
        /// Objects the fetch may assume present (default: the manifest's tips).
        #[arg(long = "have")]
        haves: Vec<Oid>,
        #[arg(long, value_enum, default_value_t = TierArg::M15)]
        tier: TierArg,
        /// Do not drop objects already present in published slabs.
        #[arg(long)]
        no_dedup: bool,
        /// Also record the slab in the manifest (genesis when there is none).
        #[arg(long)]
        publish: bool,
        #[command(flatten)]
        resolve: ResolveArgs,
    },
    /// Write the single pack (+idx) a client would receive, to <out>.pack / <out>.idx.
    Splice {
        #[arg(long, env = "GVFS_STORE")]
        store: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// The client's lastPackTimestamp (default 0: cold clone).
        #[arg(long)]
        since: Option<u64>,
        /// Explicit slab sequence numbers instead of a timestamp selection.
        #[arg(long = "slab")]
        slabs: Vec<u64>,
    },
    /// Print, as JSON, what a client with the given lastPackTimestamp would receive.
    Plan {
        #[arg(long, env = "GVFS_STORE")]
        store: PathBuf,
        #[arg(long, default_value_t = 0)]
        since: u64,
        /// Superset rollups only (what `&compact=1` returns).
        #[arg(long)]
        compact: bool,
    },
    /// Show the manifest and slabs; --verify checks every object of every slab.
    /// Client prefetch hook: fetch into a repository's pack directory, name packs by the
    /// covered interval, unlink subsumed packs. Never `git gc`, never `index-pack`.
    ClientPrefetch {
        /// Edge or localhost proxy base URL (e.g. http://127.0.0.1:8080 or https://edge/org/proj/_git/repo).
        #[arg(long, env = "GVFS_SERVER")]
        server: String,
        /// Working tree, `.git` directory or bare repository.
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        /// Pack directory override (default: the first alternate's `pack/` — the shared object cache — else `objects/pack`).
        #[arg(long)]
        pack_dir: Option<PathBuf>,
        /// Environment variable holding the token (sent as HTTP Basic).
        #[arg(long)]
        token_env: Option<String>,
        #[arg(long, default_value = "")]
        token_user: String,
        /// Collapse small packs with `compact=1` when more prefetch packs than this remain (0 = never).
        #[arg(long, default_value_t = 32)]
        budget: usize,
        /// Ask for whole rollups instead of leaves this time.
        #[arg(long)]
        compact: bool,
        /// Run `git multi-pack-index write` afterwards.
        #[arg(long)]
        midx: bool,
    },
    Inspect {
        #[arg(long, env = "GVFS_STORE")]
        store: PathBuf,
        #[arg(long)]
        verify: bool,
    },
    /// Locate objects across the published slabs.
    Lookup {
        #[arg(long, env = "GVFS_STORE")]
        store: PathBuf,
        oids: Vec<Oid>,
    },
}

fn main() {
    let cli = Cli::parse();
    let mut out = std::io::stdout().lock();
    let result: Result<(), cli::CliError> = match cli.command {
        Commands::Serve {
            bind,
            store,
            upstream,
            arenas,
            arena_mib,
            l0_mib,
            poll_secs,
            manifest_poll_secs,
            cache_server,
            alloc_guard,
        } => (|| -> Result<(), cli::CliError> {
            let up = match upstream.upstream {
                Some(spec) => Some((
                    cli::upstream_from_args(
                        &spec,
                        upstream.token_env.as_deref(),
                        &upstream.token_user,
                    )?,
                    cli::branch_ref(&upstream.branch),
                )),
                None => None,
            };
            let cfg = gvfs_daemon::serve::ServeConfig {
                bind,
                store,
                arenas: arenas.max(1),
                arena_bytes: arena_mib.max(1).saturating_mul(1 << 20),
                l0_bytes: l0_mib.max(1).saturating_mul(1 << 20),
                poll_secs,
                manifest_poll_secs: manifest_poll_secs.max(1),
                cache_servers: cache_server,
                guard: match alloc_guard {
                    GuardArg::Off => None,
                    GuardArg::Count => Some(gvfs_alloc_guard::Mode::Count),
                    GuardArg::Strict => Some(gvfs_alloc_guard::Mode::Strict),
                },
            };
            gvfs_daemon::serve::run(cfg, up)?;
            Ok(())
        })(),
        Commands::Pipeline {
            tier,
            store,
            upstream,
            baseline,
            retain_weeks,
            rollup_fetch_from,
            leaf_tier,
            resolve,
        } => (|| -> Result<(), cli::CliError> {
            let store = FsStore::open(&store)?;
            let mut up = cli::upstream_from_args(
                &upstream.upstream,
                upstream.token_env.as_deref(),
                &upstream.token_user,
            )?;
            let cfg = PipelineConfig {
                branch: cli::branch_ref(&upstream.branch),
                baseline: match baseline {
                    BaselineArg::Fetch => BaselineMode::Fetch,
                    BaselineArg::Splice => BaselineMode::Splice,
                },
                retain_weeks,
                rollup_fetch_from: rollup_fetch_from.tier(),
                leaf_tier: leaf_tier.tier(),
                ingest: resolve.config(),
                ..PipelineConfig::default()
            };
            let t0 = std::time::Instant::now();
            let report = run_pipeline(&store, up.as_mut(), tier.into(), &cfg, &mut out)?;
            let _ = writeln!(
                out,
                "pipeline --tier {} done in {:.1}s (generation {})",
                Tier::from(tier),
                t0.elapsed().as_secs_f64(),
                report.generation
            );
            Ok(())
        })(),
        Commands::Ingest {
            store,
            upstream,
            want,
            haves,
            tier,
            no_dedup,
            publish,
            resolve,
        } => (|| -> Result<(), cli::CliError> {
            let mut up = cli::upstream_from_args(
                &upstream.upstream,
                upstream.token_env.as_deref(),
                &upstream.token_user,
            )?;
            cli::cmd_ingest(
                cli::IngestArgs {
                    store: &store,
                    upstream: up.as_mut(),
                    branch: cli::branch_ref(&upstream.branch),
                    want,
                    haves,
                    tier: tier.into(),
                    dedup: !no_dedup,
                    publish,
                    ingest: resolve.config(),
                },
                &mut out,
            )
        })(),
        Commands::Splice {
            store,
            out: prefix,
            since,
            slabs,
        } => cli::cmd_splice(&store, &prefix, since, &slabs, &mut out),
        Commands::Plan {
            store,
            since,
            compact,
        } => cli::cmd_plan(&store, since, compact, &mut out),
        Commands::ClientPrefetch {
            server,
            repo,
            pack_dir,
            token_env,
            token_user,
            budget,
            compact,
            midx,
        } => (|| -> Result<(), cli::CliError> {
            let pack_dir = match pack_dir {
                Some(p) => p,
                None => gvfs_daemon::client::pack_dir_for(&repo)?,
            };
            let opts = gvfs_daemon::client::Options {
                server: &server,
                auth: cli::auth_from_args(token_env.as_deref(), &token_user)?,
                pack_dir,
                budget,
                compact,
                midx,
            };
            cli::cmd_client_prefetch(&opts, &mut out)
        })(),
        Commands::Inspect { store, verify } => cli::cmd_inspect(&store, verify, &mut out),
        Commands::Lookup { store, oids } => cli::cmd_lookup(&store, &oids, &mut out),
    };
    if let Err(e) = result {
        eprintln!("gvfs-daemon: {e}");
        std::process::exit(1);
    }
}
