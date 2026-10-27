use std::net::SocketAddr;
use std::path::PathBuf;

use crate::manifest::Tier;
use crate::pipeline::PipelineOptions;
use crate::slab::Limits;
use crate::storage::{Storage, StoreKind};
use crate::upstream::Upstream;
use crate::{Error, Result};

pub const HELP: &str = "\
gvfs-daemon - one binary, one pack per prefetch

Usage:
  gvfs-daemon pipeline --storage DIR|URL --upstream PATH|URL [--tier auto|15m|1h|6h|12h|24h|168h]
  gvfs-daemon serve    --storage DIR|URL --upstream PATH|URL [--bind 127.0.0.1:8080]
  gvfs-daemon edge|proxy ...    aliases for serve (identical engine)
  gvfs-daemon bench    --storage OUTPUT_DIR --upstream PATH|URL [--input-pack FILE]
      Benchmarks both layouts on the same input, verifies with native Git, writes CSV.

Storage:
  --cache-dir DIR              local immutable metadata/download cache [.gvfs-cache]
  --storage-kind generic|azure|s3 [generic; Azure SAS URLs are accepted]
  --storage-auth-env NAME      complete Authorization header, read at boot
  --s3-region REGION           sign S3 requests using standard AWS_* environment variables
  --storage-connections N      persistent HTTP connection pool [4]

Git / pipeline:
  --branch NAME               upstream branch [main]; HEAD is also accepted
  --upstream-auth-env NAME     complete upstream Authorization header
  --token-env NAME             ADO PAT / GitHub token (environment only)
  --strict-hfs                 reject malformed HFS-looking blobs instead of diagnosing/preserving
  --tier auto                  ingest and run due rollups [default]
  --tier 1h|6h|12h|24h|168h     compact existing storage; never fetch upstream
  --input-pack FILE            genesis only: seed from an existing SHA-1 Git pack
  --retention-days N           warm-client incremental coverage [14]
  --now SECONDS                deterministic pipeline timestamp [system clock]

Memory / capacity (fixed before processing; exhaustion is an explicit error):
  --max-objects N              objects per slab / response plan [1048576]
  --pack-mib N                 pack arena capacity [1024; disk-backed, bounded]
  --object-mib N               maximum inflated object / delta [32]
  --max-links N                graph adjacency arena [16777216]
  --tree-layout full|clustered  comparable long-tier layouts [clustered]
  --delta-cache-mib N          reusable upstream delta-base ring [256]
  --max-ranges N               coalesced ranges per plan [262144]
  --range-mib N                target HTTP range size [4]
  --range-gap N                maximum overfetch between selected entries [4096]
  --arenas N                   concurrent planned HTTP responses [4]
  --max-request-objects N      OIDs per POST [16384]
  --request-mib N              POST body limit [1]
  --live-objects N             live-window objects [65536]
  --live-mib N                 live-window pack arena [256]
  --live-links N               live-window adjacency [1048576]

Serving:
  --bind IP:PORT               localhost and remote run the same handler
  --advertise-url URL          advertised GVFS cache URL [http://BIND]
  --poll-seconds N             live upstream polling [2]
  --manifest-seconds N         manifest polling [30]
  --snapshot-slots N           bound generations pinned by slow clients [8]
  --max-connections N          concurrent TCP connections [1024]
  --connection-seconds N       maximum connection lifetime [300]
  --no-live                    explicitly disable upstream live synchronization
  --coarse-prefetch            compatibility baseline: send complete intersecting rollups
  --no-prefetch-index          compatibility baseline: omit synthesized prefetch indexes
  --tls-cert FILE --tls-key FILE   PEM server identity; ALPN h2/http/1.1
  --help | --version
";

pub enum Mode {
    Help,
    Version,
    Pipeline,
    Serve,
    Benchmark,
}

pub struct Cli {
    pub mode: Mode,
    pub storage: String,
    pub cache: PathBuf,
    pub kind: StoreKind,
    pub storage_auth: Option<String>,
    pub region: Option<String>,
    pub storage_connections: usize,
    pub upstream: Option<String>,
    pub branch: String,
    pub upstream_auth: Option<String>,
    pub token: Option<String>,
    pub pipeline: PipelineOptions,
    pub server: crate::server::ServerOptions,
}

impl Cli {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let command = args.next().unwrap_or_else(|| "--help".into());
        let mode = match command.as_str() {
            "--help" | "-h" | "help" => Mode::Help,
            "--version" | "-V" => Mode::Version,
            "pipeline" => Mode::Pipeline,
            "serve" | "edge" | "proxy" => Mode::Serve,
            "bench" | "benchmark" => Mode::Benchmark,
            _ => return Err(Error::Usage(format!("unknown command: {command}"))),
        };
        let mut cli = Self {
            mode,
            storage: std::env::var("STORAGE_BUCKET").unwrap_or_default(),
            cache: PathBuf::from(".gvfs-cache"),
            kind: StoreKind::Generic,
            storage_auth: None,
            region: None,
            storage_connections: 4,
            upstream: None,
            branch: "main".into(),
            upstream_auth: None,
            token: None,
            pipeline: PipelineOptions {
                tier: None,
                now: now()?,
                limits: Limits::default(),
                max_ranges: 1 << 18,
                gap: 4096,
                retention_secs: 14 * 86400,
                input_pack: None,
            },
            server: crate::server::ServerOptions::default(),
        };
        while let Some(flag) = args.next() {
            if matches!(flag.as_str(), "--help" | "-h") {
                cli.mode = Mode::Help;
                continue;
            }
            if flag == "--no-live" {
                cli.server.live = false;
                continue;
            }
            if flag == "--coarse-prefetch" {
                cli.server.coarse_prefetch = true;
                continue;
            }
            if flag == "--no-prefetch-index" {
                cli.server.prefetch_indexes = false;
                continue;
            }
            if flag == "--strict-hfs" {
                cli.pipeline.limits.strict_hfs = true;
                continue;
            }
            let value = args
                .next()
                .ok_or_else(|| Error::Usage(format!("missing value for {flag}")))?;
            let integer = || {
                value
                    .parse::<usize>()
                    .map_err(|_| Error::Usage(format!("invalid integer for {flag}")))
            };
            let mib = || {
                integer()?
                    .checked_mul(1 << 20)
                    .ok_or_else(|| Error::Usage(format!("{flag} overflows")))
            };
            match flag.as_str() {
                "--storage" => cli.storage = value,
                "--cache-dir" => cli.cache = value.into(),
                "--storage-kind" => {
                    cli.kind = match value.as_str() {
                        "generic" => StoreKind::Generic,
                        "azure" => StoreKind::Azure,
                        "s3" => StoreKind::S3,
                        _ => return Err(Error::Usage("invalid storage kind".into())),
                    }
                }
                "--storage-auth-env" => cli.storage_auth = Some(value),
                "--s3-region" => cli.region = Some(value),
                "--storage-connections" => cli.storage_connections = integer()?,
                "--upstream" => cli.upstream = Some(value),
                "--branch" => cli.branch = value,
                "--upstream-auth-env" => cli.upstream_auth = Some(value),
                "--token-env" => cli.token = Some(value),
                "--tier" => {
                    cli.pipeline.tier = if value == "auto" {
                        None
                    } else {
                        Some(Tier::parse(&value)?)
                    }
                }
                "--input-pack" => cli.pipeline.input_pack = Some(value.into()),
                "--retention-days" => {
                    cli.pipeline.retention_secs = (integer()? as u64)
                        .checked_mul(86400)
                        .ok_or(Error::Capacity("retention"))?
                }
                "--now" => {
                    cli.pipeline.now = value
                        .parse()
                        .map_err(|_| Error::Usage("invalid --now timestamp".into()))?
                }
                "--max-objects" => cli.pipeline.limits.objects = integer()?,
                "--pack-mib" => cli.pipeline.limits.pack_bytes = mib()?,
                "--object-mib" => cli.pipeline.limits.object_bytes = mib()?,
                "--max-links" => cli.pipeline.limits.links = integer()?,
                "--tree-layout" => {
                    cli.pipeline.limits.tree_layout = match value.as_str() {
                        "full" => crate::tree::TreeLayout::Full,
                        "clustered" => crate::tree::TreeLayout::Clustered,
                        _ => {
                            return Err(Error::Usage(
                                "--tree-layout must be full or clustered".into(),
                            ));
                        }
                    }
                }
                "--delta-cache-mib" => cli.pipeline.limits.cache_bytes = mib()?,
                "--max-ranges" => cli.pipeline.max_ranges = integer()?,
                "--range-mib" => cli.server.range_bytes = mib()?,
                "--range-gap" => cli.pipeline.gap = integer()? as u64,
                "--arenas" => cli.server.arenas = integer()?,
                "--max-request-objects" => cli.server.request_objects = integer()?,
                "--request-mib" => cli.server.request_bytes = mib()?,
                "--live-objects" => cli.server.live_limits.objects = integer()?,
                "--live-mib" => cli.server.live_limits.pack_bytes = mib()?,
                "--live-links" => cli.server.live_limits.links = integer()?,
                "--bind" => {
                    cli.server.bind = value
                        .parse::<SocketAddr>()
                        .map_err(|_| Error::Usage("invalid --bind socket address".into()))?
                }
                "--advertise-url" => cli.server.advertise_url = Some(value),
                "--poll-seconds" => cli.server.poll_seconds = integer()? as u64,
                "--manifest-seconds" => cli.server.manifest_seconds = integer()? as u64,
                "--snapshot-slots" => cli.server.snapshot_slots = integer()?,
                "--max-connections" => cli.server.connections = integer()?,
                "--connection-seconds" => cli.server.connection_seconds = integer()? as u64,
                "--tls-cert" => cli.server.tls_cert = Some(value.into()),
                "--tls-key" => cli.server.tls_key = Some(value.into()),
                _ => return Err(Error::Usage(format!("unknown option: {flag}"))),
            }
        }
        if !matches!(cli.mode, Mode::Help | Mode::Version) {
            if cli.storage.is_empty() {
                return Err(Error::Usage("--storage is required".into()));
            }
            cli.pipeline.limits.validate()?;
            cli.server.limits = cli.pipeline.limits;
            cli.server.live_limits.object_bytes = cli.pipeline.limits.object_bytes;
            cli.server.live_limits.tree_layout = crate::tree::TreeLayout::Full;
            cli.server.live_limits.cache_bytes = cli.pipeline.limits.cache_bytes;
            cli.server.live_limits.strict_hfs = cli.pipeline.limits.strict_hfs;
            cli.server.max_ranges = cli.pipeline.max_ranges;
            cli.server.gap = cli.pipeline.gap;
            if matches!(cli.mode, Mode::Serve) {
                cli.server.validate()?;
            }
            if cli.region.is_some() && cli.kind != StoreKind::S3 {
                return Err(Error::Usage(
                    "--s3-region requires --storage-kind s3".into(),
                ));
            }
        }
        Ok(cli)
    }

    pub fn storage_client(&self) -> Result<Storage> {
        Storage::open(
            &self.storage,
            self.cache.clone(),
            self.server.range_bytes,
            self.pipeline.limits.pack_bytes,
            self.kind,
            self.storage_auth.as_deref(),
            self.region.clone(),
            self.storage_connections,
        )
    }

    pub fn git_upstream(&self) -> Result<Option<Upstream>> {
        self.upstream
            .as_ref()
            .map(|source| {
                Upstream::new(
                    source.clone(),
                    self.branch.clone(),
                    self.upstream_auth.as_deref(),
                    self.token.as_deref(),
                )
            })
            .transpose()
    }
}

pub fn now() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::Invalid("system clock"))?
        .as_secs())
}
