#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use clap::Parser;
use gvfs_daemon::buf::ByteWriter;
use gvfs_daemon::client::{Cache, Config as ClientConfig, ResponseMeta};
use gvfs_daemon::ingest::PackMode;
use gvfs_daemon::manifest::{Manifest, SlabRef, Tier, WEEK};
use gvfs_daemon::object::{TreeIter, tree_entry_cmp};
use gvfs_daemon::pipeline::{Catalog, Config, Engine};
use gvfs_daemon::source::GitUpstream;
use gvfs_daemon::splice::{Plan, splice_slab_ranges};
use gvfs_daemon::storage::Storage;
use gvfs_daemon::zlib::{Deflater, compress_bound};
use gvfs_daemon::{Error, ObjType, Oid20, Result};
use sha2::{Digest, Sha256};

#[global_allocator]
static ALLOC: alloc_guard::SealedAllocator = alloc_guard::SealedAllocator;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "target/searchgold-benchmark/clustered")]
    seed: PathBuf,
    #[arg(long, default_value = "target/workload-replay-optimized")]
    work: PathBuf,
    #[arg(long, default_value = "benchmarks/workloads-optimized.csv")]
    output: PathBuf,
    /// Reproduce the pre-optimization protocol and modeled unpruned client.
    #[arg(long)]
    legacy: bool,
    #[arg(long, default_value_t = 8)]
    days: u32,
    #[arg(long, default_value_t = 5)]
    change_minutes: u32,
    /// Restrict source changes to 09:00-17:00 UTC on weekdays.
    #[arg(long)]
    diurnal_writes: bool,
    #[arg(long, default_value_t = 256)]
    files: u32,
    #[arg(long, default_value_t = 16777216)]
    max_objects: usize,
}

fn git(repo: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_CONFIG_") {
            command.env_remove(key);
        }
    }
    let output = command
        .arg("-C")
        .arg(repo)
        .args(args)
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        return Err(Error::Git(output.status.code().unwrap_or(-1)));
    }
    Ok(output.stdout)
}

struct Entry {
    name: Vec<u8>,
    mode: u32,
    oid: Oid20,
}

struct ObjectWriter {
    odb: PathBuf,
    deflate: Deflater,
    compressed: Vec<u8>,
}

impl ObjectWriter {
    fn write(&mut self, kind: ObjType, bytes: &[u8]) -> Result<Oid20> {
        let oid = gvfs_daemon::object_id(kind, bytes);
        let mut header = [0; 32];
        let n = gvfs_daemon::pack::loose_header(kind, bytes.len() as u64, &mut header);
        let n = self.deflate.deflate_parts(
            &[
                header.get(..n).ok_or(Error::Invalid("loose header"))?,
                bytes,
            ],
            &mut self.compressed,
        )?;
        let hex = oid.to_string();
        let (directory, name) = hex.split_at(2);
        let path = self.odb.join(directory);
        fs::create_dir_all(&path)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.join(name))?;
        file.write_all(
            self.compressed
                .get(..n)
                .ok_or(Error::Invalid("loose object"))?,
        )?;
        Ok(oid)
    }
}

struct Fixture {
    repo: PathBuf,
    writer: ObjectWriter,
    root: Vec<Entry>,
    pointers: Vec<Oid20>,
    bytes: Vec<u8>,
    tip: Oid20,
    revision: u32,
}

impl Fixture {
    fn open(seed: &Path, baseline: SlabRef, repo: &Path, files: u32) -> Result<Self> {
        fs::create_dir_all(repo)?;
        git(repo, &["init", "--quiet", "-b", "main"])?;
        let pack_dir = repo.join(".git/objects/pack");
        fs::create_dir_all(&pack_dir)?;
        for extension in ["pack", "idx"] {
            fs::hard_link(
                seed.join(baseline.key(extension)),
                pack_dir.join(format!("pack-{}.{}", baseline.hash, extension)),
            )?;
        }
        let tip = baseline.tip;
        let tree = git(repo, &["rev-parse", &format!("{tip}^{{tree}}")])?;
        let tree = Oid20::from_hex(tree.trim_ascii()).ok_or(Error::Invalid("source tree"))?;
        let bytes = git(repo, &["cat-file", "tree", &tree.to_string()])?;
        let mut root = Vec::new();
        for entry in TreeIter::new(&bytes) {
            let entry = entry.map_err(|_| Error::Invalid("source tree"))?;
            if entry.name == b".gvfs-workload" {
                return Err(Error::Invalid("fixture directory already exists"));
            }
            root.push(Entry {
                name: entry.name.to_vec(),
                mode: entry.mode,
                oid: entry.oid,
            });
        }
        root.push(Entry {
            name: b".gvfs-workload".to_vec(),
            mode: 0o40000,
            oid: Oid20([0; 20]),
        });
        root.sort_unstable_by(|a, b| {
            tree_entry_cmp(&a.name, a.mode == 0o40000, &b.name, b.mode == 0o40000)
        });
        let mut fixture = Self {
            repo: repo.to_owned(),
            writer: ObjectWriter {
                odb: repo.join(".git/objects"),
                deflate: Deflater::new(1),
                compressed: vec![0; compress_bound(4 << 20)],
            },
            root,
            pointers: Vec::with_capacity(files as usize),
            bytes: Vec::with_capacity(4 << 20),
            tip,
            revision: 0,
        };
        for file in 0..files {
            let pointer = fixture.pointer(file, 0)?;
            fixture.pointers.push(pointer);
        }
        fixture.update_ref()?;
        Ok(fixture)
    }

    fn pointer(&mut self, file: u32, revision: u32) -> Result<Oid20> {
        let mut sha = Sha256::new();
        sha.update(file.to_le_bytes());
        sha.update(revision.to_le_bytes());
        let hash: [u8; 32] = sha.finalize().into();
        let mut hex = [0; 64];
        gvfs_daemon::oid::hex_encode_into(&hash, &mut hex);
        let mut bytes = [0; 128];
        let mut w = ByteWriter::new(&mut bytes);
        w.put(b"~~Git-hfs 1.0\nblob:")?;
        w.put(&hex)?;
        w.put(b"00\nsize:4096\n")?;
        gvfs_daemon::hfs::parse_git_hfs_v1(w.written()).ok_or(Error::Invalid("fixture pointer"))?;
        self.writer.write(ObjType::Blob, w.written())
    }

    fn update_ref(&self) -> Result<()> {
        let path = self.repo.join(".git/refs/heads");
        fs::create_dir_all(&path)?;
        fs::write(path.join("main.next"), format!("{}\n", self.tip))?;
        fs::rename(path.join("main.next"), path.join("main"))?;
        Ok(())
    }

    fn commit(&mut self, timestamp: u64) -> Result<()> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(Error::Capacity("fixture revision"))?;
        let file = (self.revision as usize)
            .checked_rem(self.pointers.len())
            .ok_or(Error::Invalid("fixture files"))?;
        let pointer = self.pointer(file as u32, self.revision)?;
        *self
            .pointers
            .get_mut(file)
            .ok_or(Error::Invalid("fixture file"))? = pointer;
        self.bytes.clear();
        for (i, oid) in self.pointers.iter().enumerate() {
            write!(&mut self.bytes, "100644 file-{i:08}\0")?;
            self.bytes.extend_from_slice(&oid.0);
        }
        let tree = self.writer.write(ObjType::Tree, &self.bytes)?;
        self.bytes.clear();
        for entry in &mut self.root {
            if entry.name == b".gvfs-workload" {
                entry.oid = tree;
            }
            write!(&mut self.bytes, "{:o} ", entry.mode)?;
            self.bytes.extend_from_slice(&entry.name);
            self.bytes.push(0);
            self.bytes.extend_from_slice(&entry.oid.0);
        }
        let tree = self.writer.write(ObjType::Tree, &self.bytes)?;
        self.bytes.clear();
        write!(
            &mut self.bytes,
            "tree {tree}\nparent {}\nauthor Benchmark <bench@example.invalid> {timestamp} +0000\ncommitter Benchmark <bench@example.invalid> {timestamp} +0000\n\nworkload {}\n",
            self.tip, self.revision
        )?;
        self.tip = self.writer.write(ObjType::Commit, &self.bytes)?;
        self.update_ref()
    }
}

struct CountWriter<'a, W: Write> {
    output: &'a mut W,
    bytes: u64,
}
impl<W: Write> Write for CountWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let n = self.output.write(bytes)?;
        self.bytes = self
            .bytes
            .checked_add(n as u64)
            .ok_or_else(|| std::io::Error::other("byte count overflow"))?;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

struct Response {
    bytes: u64,
    hash: Option<Oid20>,
    cursor: u64,
}

fn warm_response(
    catalog: &Catalog,
    cursor: u64,
    plan: &mut Plan,
    buffer: &mut [u8],
    output: &mut impl Write,
    include_index: bool,
) -> Result<Response> {
    catalog.prefetch(cursor, plan)?;
    if plan.slabs.iter().any(|s| s.meta.tier == Tier::Baseline) {
        return Err(Error::Invalid("warm client was sent a baseline"));
    }
    let mut output = CountWriter { output, bytes: 0 };
    if plan.count == 0 {
        output.write_all(&gvfs_daemon::gpre::gpre_file_header(0))?;
        return Ok(Response {
            bytes: output.bytes,
            hash: None,
            cursor,
        });
    }
    let whole = plan.whole_slab();
    let idx_len = if include_index {
        whole.map_or(0, |s| s.meta.idx_len)
    } else {
        0
    };
    output.write_all(&gvfs_daemon::gpre::gpre_file_header(1))?;
    output.write_all(&gvfs_daemon::gpre::gpre_pack_header(
        catalog.manifest.timestamp as i64,
        i64::try_from(plan.pack_len).map_err(|_| Error::Capacity("pack length"))?,
        if idx_len == 0 {
            -1
        } else {
            i64::try_from(idx_len).map_err(|_| Error::Capacity("index length"))?
        },
    ))?;
    let hash = splice_slab_ranges(plan, &mut output, buffer, 64 << 10)?;
    if let Some(slab) = whole {
        if hash != slab.meta.hash {
            return Err(Error::Invalid("whole-pack splice changed bytes"));
        }
        let mut offset = 0u64;
        while offset < idx_len {
            let n = idx_len.saturating_sub(offset).min(buffer.len() as u64) as usize;
            let bytes = buffer.get_mut(..n).ok_or(Error::Capacity("index buffer"))?;
            slab.idx.read(offset, bytes)?;
            output.write_all(bytes)?;
            offset = offset.saturating_add(n as u64);
        }
    }
    if output.bytes != 32u64.saturating_add(plan.pack_len).saturating_add(idx_len) {
        return Err(Error::Invalid("response byte accounting"));
    }
    Ok(Response {
        bytes: output.bytes,
        hash: Some(hash),
        cursor: catalog.manifest.timestamp,
    })
}

// Cold responses would total TB. Count their exact encoded lengths instead of
// moving those bytes; verified disjointness makes whole-body concatenation exact.
fn cold_bytes(catalog: &Catalog, include_index: bool) -> Result<u64> {
    let mut pack = 32u64;
    let mut nonempty = 0usize;
    let mut idx = 0u64;
    for slab in std::iter::once(&catalog.baseline)
        .chain(&catalog.pyramid)
        .chain(catalog.live.iter())
    {
        if slab.meta.count == 0 {
            continue;
        }
        nonempty = nonempty.saturating_add(1);
        idx = slab.meta.idx_len;
        pack = pack
            .checked_add(
                slab.meta
                    .pack_len
                    .checked_sub(32)
                    .ok_or(Error::Invalid("pack length"))?,
            )
            .ok_or(Error::Capacity("cold pack"))?;
    }
    if nonempty == 0 {
        return Ok(8);
    }
    Ok(32u64
        .saturating_add(pack)
        .saturating_add(if nonempty == 1 && include_index {
            idx
        } else {
            0
        }))
}

fn check_disjoint(catalog: &Catalog, verified: &mut BTreeSet<Oid20>) -> Result<()> {
    let active = catalog.active();
    for slab in &active {
        if slab.meta.tier == Tier::Baseline || !verified.insert(slab.meta.index_hash) {
            continue;
        }
        for i in 0..slab.index.len() {
            let oid = slab.index.record(i as u32)?.oid;
            for other in &active {
                if other.meta.index_hash != slab.meta.index_hash
                    && other.index.find(&oid)?.is_some()
                {
                    return Err(Error::Invalid(
                        "cold accounting requires disjoint admitted slabs",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Pattern {
    Fresh,
    Minute,
    QuarterHour,
    Hour,
    Developer,
    Daily,
    Weekend,
    Idle,
}

fn diurnal_activity(minute: u32) -> bool {
    let within_day = minute.wrapping_rem(1440);
    minute.wrapping_div(1440).wrapping_rem(7) < 5 && (540..1020).contains(&within_day)
}

impl Pattern {
    fn name(self) -> &'static str {
        match self {
            Self::Fresh => "fresh_isolated_cache_every_5m",
            Self::Minute => "warm_cache_poll_every_1m",
            Self::QuarterHour => "warm_cache_poll_every_15m",
            Self::Hour => "warm_cache_poll_every_1h",
            Self::Developer => "developer_four_fetches_per_workday",
            Self::Daily => "developer_one_fetch_per_workday",
            Self::Weekend => "friday_evening_then_monday_morning",
            Self::Idle => "idle_repo_poll_every_1m",
        }
    }
    fn due(self, minute: u32) -> bool {
        let day = minute.wrapping_div(1440);
        let within = minute.wrapping_rem(1440);
        let weekday = day.wrapping_rem(7) < 5;
        match self {
            Self::Fresh => minute.wrapping_rem(5) == 2,
            Self::Minute | Self::Idle => true,
            Self::QuarterHour => minute.is_multiple_of(15),
            Self::Hour => minute.is_multiple_of(60),
            Self::Developer => weekday && [540, 720, 900, 1080].contains(&within),
            Self::Daily => weekday && within == 540,
            Self::Weekend => (day == 4 && within == 1080) || (day == 7 && within == 540),
        }
    }
}

struct Client {
    pattern: Pattern,
    cache: Option<Cache>,
    include_index: bool,
    cursor: Option<u64>,
    identities: BTreeSet<(u64, Oid20)>,
    requests: u64,
    cold_clones: u64,
    pack_responses: u64,
    bytes: u64,
    incremental_bytes: u64,
    duplicates: u64,
    max_incremental: u64,
    last_incremental: u64,
    not_modified: u64,
    local_pack_bytes: u64,
}
impl Client {
    fn new(pattern: Pattern, cache: Option<Cache>, legacy: bool) -> Self {
        Self {
            pattern,
            cache,
            include_index: legacy,
            cursor: None,
            identities: BTreeSet::new(),
            requests: 0,
            cold_clones: 0,
            pack_responses: 0,
            bytes: 0,
            incremental_bytes: 0,
            duplicates: 0,
            max_incremental: 0,
            last_incremental: 0,
            not_modified: 0,
            local_pack_bytes: 0,
        }
    }
    fn request(
        &mut self,
        catalog: &Catalog,
        plan: &mut Plan,
        buffer: &mut [u8],
        download: &mut [u8],
    ) -> Result<()> {
        self.requests = self.requests.saturating_add(1);
        if self.cursor.is_none() || matches!(self.pattern, Pattern::Fresh) {
            self.bytes = self
                .bytes
                .checked_add(cold_bytes(catalog, self.include_index)?)
                .ok_or(Error::Capacity("traffic bytes"))?;
            self.cursor = Some(catalog.manifest.timestamp);
            self.pack_responses = self.pack_responses.saturating_add(1);
            self.cold_clones = self.cold_clones.saturating_add(1);
            return Ok(());
        }
        let cursor = self
            .cache
            .as_ref()
            .map_or(self.cursor, |cache| Some(cache.cursor()))
            .ok_or(Error::Invalid("client cursor"))?;
        let etag = gvfs_daemon::gpre::etag(catalog.revision(), cursor, self.include_index);
        if let Some(cache) = &mut self.cache
            && cache.etag() == Some(etag)
        {
            cache.apply_response(
                304,
                ResponseMeta {
                    etag,
                    start: None,
                    end: None,
                    baseline: false,
                    tip: Some(catalog.tip()),
                },
                &mut b"".as_slice(),
            )?;
            self.not_modified = self.not_modified.saturating_add(1);
            self.last_incremental = 0;
            return Ok(());
        }
        let mut capture = std::io::Cursor::new(download);
        let response = warm_response(
            catalog,
            cursor,
            plan,
            buffer,
            &mut capture,
            self.include_index,
        )?;
        if let Some(cache) = &mut self.cache {
            let bytes = capture
                .into_inner()
                .get(..response.bytes as usize)
                .ok_or(Error::Capacity("replay response"))?;
            let mut input = bytes;
            let report = cache.apply_response(
                200,
                ResponseMeta {
                    etag,
                    start: response.hash.map(|_| cursor),
                    end: response.hash.map(|_| response.cursor),
                    baseline: false,
                    tip: Some(catalog.tip()),
                },
                &mut input,
            )?;
            if report.downloaded_bytes != response.bytes || report.managed_packs > 2 {
                return Err(Error::Invalid(
                    "actual client disagrees with replay accounting/pack bound",
                ));
            }
            self.local_pack_bytes = self
                .local_pack_bytes
                .saturating_add(report.local_pack_bytes_written);
        }
        self.cursor = Some(response.cursor);
        self.bytes = self
            .bytes
            .checked_add(response.bytes)
            .ok_or(Error::Capacity("traffic bytes"))?;
        self.incremental_bytes = self
            .incremental_bytes
            .checked_add(response.bytes)
            .ok_or(Error::Capacity("traffic bytes"))?;
        self.max_incremental = self.max_incremental.max(response.bytes);
        self.last_incremental = response.bytes;
        if let Some(hash) = response.hash {
            self.pack_responses = self.pack_responses.saturating_add(1);
            if !self.identities.insert((response.cursor, hash)) {
                self.duplicates = self.duplicates.saturating_add(1);
            }
        }
        Ok(())
    }
    fn row(&self, out: &mut impl Write) -> Result<()> {
        let unique = self
            .cold_clones
            .saturating_add(self.identities.len() as u64);
        let per_cache = if let Some(cache) = &self.cache {
            let physical = fs::read_dir(cache.cache_dir().join("objects/pack"))?
                .collect::<std::io::Result<Vec<_>>>()?
                .into_iter()
                .filter(|e| e.path().extension().is_some_and(|x| x == "pack"))
                .count();
            if physical != cache.managed_pack_count() {
                return Err(Error::Invalid("physical cache pack count"));
            }
            physical as u64
        } else if matches!(self.pattern, Pattern::Fresh) {
            1
        } else {
            unique
        };
        writeln!(
            out,
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.pattern.name(),
            self.requests,
            self.cold_clones,
            self.pack_responses,
            unique,
            per_cache,
            self.bytes,
            self.incremental_bytes,
            self.duplicates,
            self.max_incremental,
            self.last_incremental,
            self.not_modified,
            self.local_pack_bytes
        )?;
        Ok(())
    }
}

const REPLAY_SERVER: &str = "http://replay.invalid";

fn client_config(path: PathBuf, max_objects: usize) -> ClientConfig {
    let mut config = ClientConfig::new(REPLAY_SERVER.to_owned(), path);
    config.max_objects = max_objects;
    config
}

fn client_template(args: &Args, catalog: &Catalog) -> Result<PathBuf> {
    let path = args.work.join("client-template");
    let mut cache = Cache::open(client_config(path.clone(), args.max_objects))?;
    let mut header = gvfs_daemon::gpre::gpre_file_header(1).to_vec();
    header.extend_from_slice(&gvfs_daemon::gpre::gpre_pack_header(
        catalog.manifest.timestamp as i64,
        catalog.baseline.meta.pack_len as i64,
        -1,
    ));
    let mut body = std::io::Cursor::new(header).chain(File::open(
        args.seed.join(catalog.baseline.meta.key("pack")),
    )?);
    let report = cache.apply_response(
        200,
        ResponseMeta {
            etag: gvfs_daemon::gpre::etag(catalog.revision(), 0, false),
            start: Some(0),
            end: Some(catalog.manifest.timestamp),
            baseline: true,
            tip: Some(catalog.tip()),
        },
        &mut body,
    )?;
    if report.downloaded_bytes != cold_bytes(catalog, false)? || report.managed_packs != 1 {
        return Err(Error::Invalid("client bootstrap accounting"));
    }
    Ok(path)
}

fn fork_client(template: &Path, target: PathBuf, max_objects: usize) -> Result<Cache> {
    for directory in ["objects/pack", "refs/heads", "gvfs-daemon"] {
        fs::create_dir_all(target.join(directory))?;
    }
    for file in [
        "HEAD",
        "config",
        "refs/heads/gvfs-cache",
        "gvfs-daemon/state.bin",
    ] {
        fs::copy(template.join(file), target.join(file))?;
    }
    for file in fs::read_dir(template.join("objects/pack"))? {
        let file = file?;
        fs::hard_link(
            file.path(),
            target.join("objects/pack").join(file.file_name()),
        )?;
    }
    Cache::open(client_config(target, max_objects))
}

fn create_engine(config: &Config, store: &Storage, repo: &Path, role: &str) -> Result<Engine> {
    let mut config = config.clone();
    config.work = config.work.join(role);
    let upstream = GitUpstream::open(
        repo.to_str().ok_or(Error::Invalid("fixture path"))?,
        "main",
        &config.work,
        None,
    )?;
    Engine::new(config, store.clone(), Some(upstream))
}

fn run(args: Args) -> Result<()> {
    if args.days == 0
        || args.days > 31
        || args.change_minutes == 0
        || args.files == 0
        || args.files > 8192
    {
        return Err(Error::Invalid("workload dimensions"));
    }
    if args.work.exists() {
        return Err(Error::Invalid("use a fresh --work directory"));
    }
    let seed_store = Storage::open(
        args.seed.to_str().ok_or(Error::Invalid("seed path"))?,
        None,
        None,
    )?;
    let (bytes, _) = seed_store
        .manifest()?
        .ok_or(Error::Invalid("missing seed manifest"))?;
    let manifest = Manifest::decode(&bytes)?;
    if manifest.baseline.count == 0 || !manifest.weekly.is_empty() || !manifest.pyramid.is_empty() {
        return Err(Error::Invalid("workload seed must contain one baseline"));
    }
    fs::create_dir_all(&args.work)?;
    let store_path = args.work.join("store");
    fs::create_dir(&store_path)?;
    for extension in ["pack", "idx", "mphf"] {
        let key = manifest.baseline.key(extension);
        fs::hard_link(args.seed.join(&key), store_path.join(key))?;
    }
    let store = Storage::open(
        store_path.to_str().ok_or(Error::Invalid("store path"))?,
        None,
        None,
    )?;
    store.publish_manifest(&bytes, None)?;
    let repo = args.work.join("upstream");
    let mut fixture = Fixture::open(&args.seed, manifest.baseline, &repo, args.files)?;
    let config = Config {
        max_objects: args.max_objects,
        max_object_bytes: 32 << 20,
        delta_cache_bytes: 128 << 20,
        live_bytes: 128 << 20,
        mode: PackMode::Clustered,
        work: args.work.clone(),
    };
    let mut pipeline = create_engine(&config, &store, &repo, "pipeline")?;
    let mut edge = create_engine(&config, &store, &repo, "edge")?;
    pipeline.reload()?;
    edge.reload()?;
    let idle = edge
        .catalog
        .as_ref()
        .ok_or(Error::Invalid("catalog"))?
        .clone();
    let mut plan = Plan::new(args.max_objects, args.max_objects);
    let mut buffer = vec![0; 1 << 20];
    let mut download = vec![0; 128 << 20];
    let template = if args.legacy {
        None
    } else {
        Some(client_template(&args, &idle)?)
    };
    let mut clients = Vec::new();
    for pattern in [
        Pattern::Fresh,
        Pattern::Minute,
        Pattern::QuarterHour,
        Pattern::Hour,
        Pattern::Developer,
        Pattern::Daily,
        Pattern::Weekend,
        Pattern::Idle,
    ] {
        let cache = if let Some(template) = &template
            && !matches!(pattern, Pattern::Fresh)
        {
            Some(fork_client(
                template,
                args.work.join("clients").join(pattern.name()),
                args.max_objects,
            )?)
        } else {
            None
        };
        clients.push(Client::new(pattern, cache, args.legacy));
    }
    for client in &mut clients {
        if !matches!(client.pattern, Pattern::Fresh) {
            client.request(&idle, &mut plan, &mut buffer, &mut download)?;
        }
    }
    let start = manifest
        .timestamp
        .saturating_add(259200)
        .wrapping_div(WEEK)
        .saturating_add(1)
        .saturating_mul(WEEK)
        .saturating_sub(259200);
    let mut verified = BTreeSet::new();
    let mut baseline = manifest.baseline.index_hash;
    let mut max_active = 1usize;
    let mut max_sealed = 1usize;
    let clock = Instant::now();
    alloc_guard::set_mode(alloc_guard::Mode::Strict);
    alloc_guard::seal();
    eprintln!(
        "replay: seed_objects={} initial_body_bytes={} days={} change_minutes={} files={} diurnal_writes={} legacy={} start={start}",
        manifest.baseline.count,
        cold_bytes(&idle, args.legacy)?,
        args.days,
        args.change_minutes,
        args.files,
        args.diurnal_writes,
        args.legacy
    );
    for minute in 1..=args.days.saturating_mul(1440) {
        let timestamp = start.saturating_add(u64::from(minute).saturating_mul(60));
        let changed = minute.is_multiple_of(args.change_minutes)
            && (!args.diurnal_writes || diurnal_activity(minute));
        if changed {
            fixture.commit(timestamp)?;
        }
        let published = minute.is_multiple_of(15);
        if published {
            pipeline.pipeline(None, timestamp)?;
            edge.reload()?;
        }
        if changed || published {
            edge.live_sync(timestamp)?;
            let catalog = edge.catalog.as_ref().ok_or(Error::Invalid("catalog"))?;
            if catalog.manifest.baseline.index_hash != baseline {
                baseline = catalog.manifest.baseline.index_hash;
                verified.clear();
            }
            check_disjoint(catalog, &mut verified)?;
            max_sealed = max_sealed.max(catalog.pyramid.len().saturating_add(1));
            max_active = max_active.max(
                catalog
                    .pyramid
                    .len()
                    .saturating_add(1)
                    .saturating_add(usize::from(catalog.live.is_some())),
            );
            if minute.is_multiple_of(1440) {
                // Cross-check the cheap cold accounting against the exact production planner.
                catalog.prefetch(0, &mut plan)?;
                let actual = 32u64
                    .saturating_add(plan.pack_len)
                    .saturating_add(if args.legacy {
                        plan.whole_slab().map_or(0, |s| s.meta.idx_len)
                    } else {
                        0
                    });
                if actual != cold_bytes(catalog, args.legacy)? {
                    return Err(Error::Invalid(
                        "cold response accounting disagrees with production planner",
                    ));
                }
                eprintln!(
                    "day={} commits={} active={} elapsed_seconds={:.1}",
                    minute.wrapping_div(1440),
                    fixture.revision,
                    catalog.pyramid.len().saturating_add(1),
                    clock.elapsed().as_secs_f64()
                );
            }
        }
        let catalog = edge.catalog.as_ref().ok_or(Error::Invalid("catalog"))?;
        for client in &mut clients {
            if client.pattern.due(minute) {
                client.request(
                    if matches!(client.pattern, Pattern::Idle) {
                        &idle
                    } else {
                        catalog
                    },
                    &mut plan,
                    &mut buffer,
                    &mut download,
                )?;
            }
        }
    }
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut output = File::create(&args.output)?;
    let header = "scenario,requests,cold_clones,pack_responses,unique_pack_files,retained_packs_per_cache,gvfs_body_bytes,after_initial_clone_bytes,duplicate_pack_responses,largest_incremental_bytes,last_incremental_bytes,not_modified,local_replacement_pack_bytes";
    writeln!(output, "{header}")?;
    println!("{header}");
    for client in clients {
        client.row(&mut output)?;
        client.row(&mut std::io::stdout())?;
    }
    eprintln!(
        "complete: commits={} max_sealed_slabs={max_sealed} max_active_with_live={max_active} hot_allocations={} elapsed_seconds={:.1}",
        fixture.revision,
        alloc_guard::stats().hot_path,
        clock.elapsed().as_secs_f64()
    );
    Ok(())
}

fn main() -> std::process::ExitCode {
    match run(Args::parse()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("workload benchmark: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eight_day_schedules_include_the_weekend_and_next_monday() {
        for (pattern, expected) in [
            (Pattern::Fresh, 2304),
            (Pattern::Minute, 11520),
            (Pattern::QuarterHour, 768),
            (Pattern::Hour, 192),
            (Pattern::Developer, 24),
            (Pattern::Daily, 6),
            (Pattern::Weekend, 2),
            (Pattern::Idle, 11520),
        ] {
            let actual = (1..=11520).filter(|&minute| pattern.due(minute)).count();
            assert_eq!(actual, expected, "{}", pattern.name());
        }
        let changes = (1..=11520u32)
            .filter(|&minute| minute.is_multiple_of(5) && diurnal_activity(minute))
            .count();
        assert_eq!(changes, 576);
    }
}
