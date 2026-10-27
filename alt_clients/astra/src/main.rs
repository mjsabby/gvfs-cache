#![forbid(unsafe_code)]

use clap::{Args, Parser, Subcommand};
use gvfs_daemon::ingest::PackMode;
use gvfs_daemon::manifest::Tier;
use gvfs_daemon::pipeline::{Config, Engine, work_lock};
use gvfs_daemon::source::GitUpstream;
use gvfs_daemon::storage::Storage;
use gvfs_daemon::{Error, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOCATOR: alloc_guard::SealedAllocator = alloc_guard::SealedAllocator;

#[derive(Parser)]
#[command(
    version,
    about = "One Git pack. Shared ingest, range-splice, pipeline and edge engine."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Ingest or compact once, then conditionally publish manifest.bin.
    Pipeline {
        #[command(flatten)]
        common: Common,
        #[arg(long, default_value = "auto")]
        tier: String,
        /// Explicit Unix publication time (for reproducible fixtures).
        #[arg(long)]
        timestamp: Option<u64>,
    },
    /// Run the same server locally or remotely. No client-side pack rewriting.
    #[command(alias = "edge", alias = "proxy")]
    Serve {
        #[command(flatten)]
        common: Common,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: std::net::SocketAddr,
        #[arg(long, default_value_t = 8)]
        workers: usize,
        #[arg(long, default_value_t = 2)]
        poll_seconds: u64,
        #[arg(long, default_value_t = 30)]
        manifest_poll_seconds: u64,
        #[arg(long, default_value_t = 300)]
        request_timeout_seconds: u64,
        #[arg(long)]
        read_only: bool,
        #[arg(long)]
        public_url: Option<String>,
        /// Existing HFS payload service to advertise (the daemon does not hydrate payloads).
        #[arg(long)]
        blob_store_url: Option<String>,
        /// Include prebuilt indexes on the wire (native gvfs-helper rebuilds them anyway).
        #[arg(long)]
        prefetch_index: bool,
    },
    /// Build each representation in an independent store and print measured sizes/times.
    Benchmark {
        #[command(flatten)]
        common: Common,
    },
    /// Populate a shared Git cache; keep its baseline and locally compact only deltas.
    Sync {
        #[arg(long)]
        server: String,
        /// Empty directory or an existing Git directory; unmanaged packs are never pruned.
        #[arg(long)]
        cache_dir: PathBuf,
        /// Poll continuously when set; otherwise sync once.
        #[arg(long)]
        poll_seconds: Option<u64>,
        #[arg(long)]
        auth_env: Option<String>,
        #[arg(long, default_value_t = 16777216)]
        max_objects: usize,
        #[arg(long, default_value_t = 32)]
        max_object_mib: usize,
        #[arg(long, default_value_t = 64)]
        max_download_gib: u64,
        #[arg(long)]
        strict_alloc: bool,
    },
}

#[derive(Args, Clone)]
struct Common {
    /// Local Git repository, SSH URL, ADO URL or GitHub URL.
    #[arg(long)]
    upstream: Option<String>,
    #[arg(long, default_value = "HEAD")]
    branch: String,
    /// Directory/fs:path, azure:https://host/container, or s3:https://host/bucket.
    #[arg(long, default_value = "data")]
    store: String,
    #[arg(long, default_value = "work")]
    work_dir: PathBuf,
    #[arg(long, value_enum, default_value = "clustered")]
    pack_mode: PackMode,
    #[arg(long, default_value_t = 16777216)]
    max_objects: usize,
    #[arg(long, default_value_t = 32)]
    max_object_mib: usize,
    #[arg(long, default_value_t = 256)]
    delta_cache_mib: usize,
    #[arg(long, default_value_t = 128)]
    live_mib: usize,
    /// Name of an environment variable containing a complete Git HTTP header (Authorization: ...).
    #[arg(long)]
    git_auth_env: Option<String>,
    /// Name of an environment variable containing a storage Authorization header value.
    #[arg(long)]
    storage_auth_env: Option<String>,
    /// Name of an environment variable containing the Azure container SAS query.
    #[arg(long)]
    storage_query_env: Option<String>,
    /// Abort on any engine hot-path allocation.
    #[arg(long)]
    strict_alloc: bool,
}

fn secret(name: &Option<String>) -> Result<Option<String>> {
    name.as_ref()
        .map(|name| {
            std::env::var(name)
                .map_err(|_| Error::Invalid("credential environment variable not set"))
        })
        .transpose()
}

fn mib(n: usize) -> Result<usize> {
    n.checked_mul(1 << 20)
        .filter(|&n| n > 0)
        .ok_or(Error::Capacity("memory setting"))
}

fn engine(common: &Common) -> Result<Engine> {
    if common.strict_alloc {
        alloc_guard::set_mode(alloc_guard::Mode::Strict);
    }
    let config = Config {
        max_objects: common.max_objects,
        max_object_bytes: mib(common.max_object_mib)?,
        delta_cache_bytes: mib(common.delta_cache_mib)?,
        live_bytes: mib(common.live_mib)?,
        mode: common.pack_mode,
        work: common.work_dir.clone(),
    };
    let store = Storage::open(
        &common.store,
        secret(&common.storage_query_env)?,
        secret(&common.storage_auth_env)?,
    )?;
    let upstream = common
        .upstream
        .as_ref()
        .map(|upstream| {
            GitUpstream::open(
                upstream,
                &common.branch,
                &common.work_dir,
                secret(&common.git_auth_env)?,
            )
        })
        .transpose()?;
    Engine::new(config, store, upstream)
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Pipeline {
            common,
            tier,
            timestamp,
        } => {
            let _lock = work_lock(&common.work_dir)?;
            let mut engine = engine(&common)?;
            let tier = if tier == "auto" {
                None
            } else {
                Some(Tier::parse(&tier)?)
            };
            alloc_guard::seal();
            let start = Instant::now();
            let changed =
                engine.pipeline(tier, timestamp.map_or_else(gvfs_daemon::serve::now, Ok)?)?;
            let catalog = engine.catalog.as_ref().ok_or(Error::Invalid("catalog"))?;
            println!(
                "changed={changed} generation={} tip={} baseline_objects={} baseline_bytes={} seconds={:.3} hot_allocations={}",
                catalog.manifest.generation,
                catalog.tip(),
                catalog.baseline.meta.count,
                catalog.baseline.meta.pack_len,
                start.elapsed().as_secs_f64(),
                alloc_guard::stats().hot_path
            );
            Ok(())
        }
        Command::Serve {
            common,
            bind,
            workers,
            poll_seconds,
            manifest_poll_seconds,
            request_timeout_seconds,
            read_only,
            public_url,
            blob_store_url,
            prefetch_index,
        } => {
            let _lock = work_lock(&common.work_dir)?;
            if !read_only && common.upstream.is_none() {
                return Err(Error::Invalid(
                    "serve requires --upstream or explicit --read-only",
                ));
            }
            let branch = if common.branch == "HEAD" {
                "refs/heads/main".to_owned()
            } else if common.branch.starts_with("refs/heads/") {
                common.branch.clone()
            } else {
                format!("refs/heads/{}", common.branch)
            };
            gvfs_daemon::serve::run(
                engine(&common)?,
                gvfs_daemon::serve::ServerOptions {
                    bind,
                    workers,
                    poll: Duration::from_secs(poll_seconds),
                    manifest_poll: Duration::from_secs(manifest_poll_seconds),
                    request_timeout: Duration::from_secs(request_timeout_seconds),
                    read_only,
                    public_url,
                    blob_store_url,
                    prefetch_index,
                    branch,
                },
            )
        }
        Command::Benchmark { common } => {
            if common.upstream.is_none() {
                return Err(Error::Invalid("benchmark requires --upstream"));
            }
            if common.store.contains("://") {
                return Err(Error::Invalid(
                    "benchmark requires a local output directory",
                ));
            }
            let root = PathBuf::from(common.store.strip_prefix("fs:").unwrap_or(&common.store));
            println!("mode,objects,pack_bytes,build_seconds,hot_allocations");
            let timestamp = gvfs_daemon::serve::now()?;
            for mode in [PackMode::Delta, PackMode::Clustered, PackMode::Full] {
                let name = match mode {
                    PackMode::Delta => "delta",
                    PackMode::Full => "full",
                    PackMode::Clustered => "clustered",
                };
                let mut options = common.clone();
                options.pack_mode = mode;
                options.store = root.join(name).to_string_lossy().into_owned();
                options.work_dir = common.work_dir.join(name);
                if PathBuf::from(&options.store).join("manifest.bin").exists() {
                    return Err(Error::Invalid(
                        "benchmark output already contains a manifest; choose a fresh --store",
                    ));
                }
                let _lock = work_lock(&options.work_dir)?;
                let mut engine = engine(&options)?;
                alloc_guard::reset_counters();
                alloc_guard::seal();
                let start = Instant::now();
                engine.pipeline(Some(Tier::M15), timestamp)?;
                let meta = engine
                    .catalog
                    .as_ref()
                    .ok_or(Error::Invalid("catalog"))?
                    .baseline
                    .meta;
                println!(
                    "{name},{},{},{:.3},{}",
                    meta.count,
                    meta.pack_len,
                    start.elapsed().as_secs_f64(),
                    alloc_guard::stats().hot_path
                );
            }
            Ok(())
        }
        Command::Sync {
            server,
            cache_dir,
            poll_seconds,
            auth_env,
            max_objects,
            max_object_mib,
            max_download_gib,
            strict_alloc,
        } => {
            if poll_seconds == Some(0) {
                return Err(Error::Invalid("poll interval must be positive"));
            }
            if strict_alloc {
                alloc_guard::set_mode(alloc_guard::Mode::Strict);
            }
            let mut config = gvfs_daemon::client::Config::new(server, cache_dir);
            config.authorization = secret(&auth_env)?;
            config.max_objects = max_objects;
            config.max_object_bytes = mib(max_object_mib)?;
            config.max_download_bytes = max_download_gib
                .checked_mul(1 << 30)
                .ok_or(Error::Capacity("download limit"))?;
            let mut cache = gvfs_daemon::client::Cache::open(config)?;
            alloc_guard::seal();
            loop {
                match cache.sync_once() {
                    Ok(report) => {
                        if poll_seconds.is_none() || !report.not_modified {
                            println!(
                                "downloaded_bytes={} local_pack_bytes={} managed_packs={} cursor={} not_modified={}",
                                report.downloaded_bytes,
                                report.local_pack_bytes_written,
                                report.managed_packs,
                                report.cursor,
                                report.not_modified
                            );
                        }
                    }
                    Err(error) if poll_seconds.is_some() => eprintln!("cache sync failed: {error}"),
                    Err(error) => return Err(error),
                }
                let Some(seconds) = poll_seconds else {
                    return Ok(());
                };
                std::thread::sleep(Duration::from_secs(seconds));
            }
        }
    }
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gvfs-daemon: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
