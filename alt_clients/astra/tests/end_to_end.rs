#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use gvfs_daemon::ingest::PackMode;
use gvfs_daemon::manifest::{Manifest, SlabRef, Tier};
use gvfs_daemon::mphf::NO_BASE;
use gvfs_daemon::pipeline::{Config, Engine};
use gvfs_daemon::slab::Slab;
use gvfs_daemon::source::GitUpstream;
use gvfs_daemon::splice::{Plan, splice_slab_ranges};
use gvfs_daemon::storage::Storage;
use gvfs_daemon::{Error, ObjType, Oid20};
use std::fs;
use std::io::{BufRead, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

#[global_allocator]
static ALLOC: alloc_guard::SealedAllocator = alloc_guard::SealedAllocator;

fn git(repo: &Path, args: &[&str]) -> Vec<u8> {
    let mut c = Command::new("git");
    c.arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_CONFIG_") {
            c.env_remove(key);
        }
    }
    let out = c.output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}
fn tip(repo: &Path) -> Oid20 {
    Oid20::from_hex(git(repo, &["rev-parse", "HEAD"]).trim_ascii()).unwrap()
}
fn config(work: PathBuf, mode: PackMode) -> Config {
    Config {
        max_objects: 32768,
        max_object_bytes: 1 << 20,
        delta_cache_bytes: 4 << 20,
        live_bytes: 8 << 20,
        mode,
        work,
    }
}
fn pointer(file: u32, version: u32) -> Vec<u8> {
    let hash = gvfs_daemon::sha1_parts(&[&file.to_le_bytes(), &version.to_le_bytes()]);
    let mut id = [0; 33];
    id[..20].copy_from_slice(&hash);
    let mut hex = [0; 66];
    gvfs_daemon::oid::hex_encode_into(&id, &mut hex);
    format!(
        "~~Git-hfs 1.0\nblob:{}\nsize:1234\n",
        std::str::from_utf8(&hex).unwrap()
    )
    .into_bytes()
}
fn commit(repo: &Path, version: u32) {
    fs::write(
        repo.join(format!("file-{:04}", version % 400)),
        pointer(version % 400, version),
    )
    .unwrap();
    git(repo, &["add", "."]);
    git(
        repo,
        &["commit", "--quiet", "-m", &format!("change {version}")],
    );
}
fn fixture(path: &Path) {
    fs::create_dir_all(path).unwrap();
    git(path, &["init", "--quiet", "-b", "main"]);
    git(path, &["config", "user.name", "Test"]);
    git(path, &["config", "user.email", "test@example.invalid"]);
    git(path, &["config", "gc.auto", "0"]);
    for i in 0..400 {
        fs::write(path.join(format!("file-{i:04}")), pointer(i, 0)).unwrap();
    }
    fs::write(
        path.join("ordinary-history"),
        b"historical ordinary blob\0retained",
    )
    .unwrap();
    for i in 1..35 {
        commit(path, i);
    }
    git(path, &["repack", "-a", "-d", "--window=50", "--depth=30"]);
}
fn engine(repo: Option<&Path>, store: &Path, work: &Path, mode: PackMode) -> Engine {
    fs::create_dir_all(work).unwrap();
    let upstream =
        repo.map(|p| GitUpstream::open(p.to_str().unwrap(), "HEAD", work, None).unwrap());
    Engine::new(
        config(work.to_owned(), mode),
        Storage::open(store.to_str().unwrap(), None, None).unwrap(),
        upstream,
    )
    .unwrap()
}
fn verify_pack(path: &Path, idx: Option<&Path>, scratch: &Path) {
    let bare = scratch.join("native.git");
    if !bare.exists() {
        assert!(
            Command::new("git")
                .args(["init", "--bare", "--quiet"])
                .arg(&bare)
                .status()
                .unwrap()
                .success()
        );
    }
    let out_idx = scratch.join("git-verified.idx");
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(&bare)
        .args(["index-pack", "--strict", "--index-version=2", "-o"])
        .arg(&out_idx)
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git index-pack {:?}: stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if let Some(idx) = idx {
        assert_eq!(fs::read(idx).unwrap(), fs::read(&out_idx).unwrap());
    }
    let bytes = fs::read(path).unwrap();
    let hash = Oid20::from_slice(&bytes[bytes.len() - 20..]).unwrap();
    let pack_path = bare.join(format!("objects/pack/pack-{hash}.pack"));
    let index_path = bare.join(format!("objects/pack/pack-{hash}.idx"));
    if !pack_path.exists() {
        fs::copy(path, pack_path).unwrap();
    }
    if !index_path.exists() {
        fs::copy(out_idx, index_path).unwrap();
    }
}
fn verify_stored(store: &Path, meta: SlabRef, scratch: &Path) {
    verify_pack(
        &store.join(meta.key("pack")),
        Some(&store.join(meta.key("idx"))),
        scratch,
    );
}
fn pack_oids(pack: &[u8], root: &Path) -> Vec<Oid20> {
    let path = root.join("response.pack");
    fs::write(&path, pack).unwrap();
    verify_pack(&path, None, root);
    let index = fs::read(root.join("git-verified.idx")).unwrap();
    let view = gvfs_daemon::idx::IdxView::parse(&index).unwrap();
    (0..view.len()).map(|i| view.oid(i).unwrap()).collect()
}
fn execute(plan: &Plan) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(plan.pack_len as usize);
    let mut range = vec![0; 1 << 20];
    let before = alloc_guard::stats().hot_path;
    splice_slab_ranges(plan, &mut bytes, &mut range, 64 << 10).unwrap();
    assert_eq!(alloc_guard::stats().hot_path, before, "splicing allocated");
    assert_eq!(bytes.len() as u64, plan.pack_len);
    bytes
}

struct Server {
    child: Child,
    url: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
fn server(repo: Option<&Path>, store: &Path, work: &Path) -> Server {
    server_options(repo, store, work, &[])
}

fn server_options(repo: Option<&Path>, store: &Path, work: &Path, options: &[&str]) -> Server {
    let mut c = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"));
    c.args([
        "serve",
        "--bind",
        "127.0.0.1:0",
        "--workers",
        "2",
        "--max-objects",
        "32768",
        "--max-object-mib",
        "1",
        "--delta-cache-mib",
        "4",
        "--live-mib",
        "8",
        "--strict-alloc",
        "--poll-seconds",
        "1",
        "--manifest-poll-seconds",
        "1",
    ]);
    c.arg("--store").arg(store).arg("--work-dir").arg(work);
    c.args(options);
    if let Some(repo) = repo {
        c.arg("--upstream").arg(repo);
    } else {
        c.arg("--read-only");
    }
    let mut child = c
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let url = line
        .trim()
        .strip_prefix("listening ")
        .expect("server did not become ready")
        .to_owned();
    Server { child, url }
}
fn http(server: &Server, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let (status, _, bytes) = http_response(server, method, path, body);
    (status, bytes)
}

fn http_response(
    server: &Server,
    method: &str,
    path: &str,
    body: &[u8],
) -> (u16, ureq::http::HeaderMap, Vec<u8>) {
    http_headers(server, method, path, body, &[])
}

fn http_headers(
    server: &Server,
    method: &str,
    path: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> (u16, ureq::http::HeaderMap, Vec<u8>) {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .into();
    let mut request = ureq::http::Request::builder()
        .method(method)
        .uri(format!("{}{path}", server.url));
    for &(name, value) in headers {
        request = request.header(name, value);
    }
    let mut response = agent.run(request.body(body).unwrap()).unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let mut bytes = Vec::new();
    response
        .body_mut()
        .as_reader()
        .read_to_end(&mut bytes)
        .unwrap();
    (status, headers, bytes)
}

fn assert_coverage(headers: &ureq::http::HeaderMap, start: u64, end: u64, mode: &str, live: bool) {
    for (name, expected) in [
        ("x-gvfs-pack-start", start.to_string()),
        ("x-gvfs-pack-end", end.to_string()),
        ("x-gvfs-pack-mode", mode.to_owned()),
        ("x-gvfs-pack-contains-live", live.to_string()),
    ] {
        assert_eq!(
            headers.get(name).and_then(|h| h.to_str().ok()),
            Some(expected.as_str()),
            "{name}"
        );
    }
}

fn assert_no_coverage(headers: &ureq::http::HeaderMap) {
    for name in [
        "x-gvfs-pack-start",
        "x-gvfs-pack-end",
        "x-gvfs-pack-mode",
        "x-gvfs-pack-contains-live",
    ] {
        assert!(!headers.contains_key(name), "unexpected {name}");
    }
}
fn prefetch_pack(bytes: &[u8]) -> (&[u8], u64) {
    assert_eq!(gvfs_daemon::gpre::parse_gpre_file_header(bytes), Some(1));
    let header = gvfs_daemon::gpre::parse_gpre_pack_header(&bytes[8..]).unwrap();
    assert_eq!(
        bytes.len(),
        32 + header.pack_len as usize + header.idx_len.unwrap_or(0) as usize
    );
    (
        &bytes[32..32 + header.pack_len as usize],
        header.timestamp as u64,
    )
}

#[test]
fn unified_pipeline_protocol_recovery_and_allocation_contract() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("source");
    fixture(&repo);
    let original_tip = tip(&repo);
    let scratch_dir = root.path().join("verification");
    fs::create_dir(&scratch_dir).unwrap();
    let t0 = 1_699_920_000u64; // Monday UTC.
    let mut expected = None;
    for (name, mode) in [
        ("full", PackMode::Full),
        ("delta", PackMode::Delta),
        ("clustered", PackMode::Clustered),
    ] {
        let store = root.path().join(name);
        let work = root.path().join(format!("work-{name}"));
        let mut e = engine(Some(&repo), &store, &work, mode);
        alloc_guard::seal();
        let before = alloc_guard::stats().hot_path;
        assert!(e.pipeline(Some(Tier::M15), t0).unwrap());
        assert_eq!(alloc_guard::stats().hot_path, before, "ingest allocated");
        let catalog = e.catalog.as_ref().unwrap();
        verify_stored(&store, catalog.baseline.meta, &scratch_dir);
        let bytes = fs::read(store.join(catalog.baseline.meta.key("pack"))).unwrap();
        let ids = pack_oids(&bytes, &scratch_dir);
        if let Some(expected) = &expected {
            assert_eq!(&ids, expected);
        } else {
            expected = Some(ids.clone());
        }
        let mut deltas = 0;
        for i in 0..catalog.baseline.index.len() {
            let row = catalog.baseline.index.record(i as u32).unwrap();
            if row.base != NO_BASE {
                assert_eq!(row.kind().unwrap(), ObjType::Tree);
                deltas += 1;
                if mode == PackMode::Clustered {
                    let base = catalog.baseline.index.record(row.base).unwrap();
                    assert_eq!(base.base, NO_BASE);
                    let start = catalog.baseline.index.location(&base).unwrap().offset;
                    let loc = catalog.baseline.index.location(&row).unwrap();
                    assert!(loc.offset + u64::from(loc.len) - start <= 1 << 20);
                }
            }
        }
        if mode == PackMode::Full {
            assert_eq!(deltas, 0);
        } else {
            assert!(deltas > 0);
        }
        if mode == PackMode::Clustered {
            let head_tree =
                Oid20::from_hex(git(&repo, &["rev-parse", "HEAD^{tree}"]).trim_ascii()).unwrap();
            let ordinal = catalog.baseline.index.find(&head_tree).unwrap().unwrap();
            assert_eq!(
                catalog.baseline.index.record(ordinal).unwrap().base,
                NO_BASE
            );
        }
        let mut plan = Plan::new(32768, 32768);
        plan.add(&catalog.baseline).unwrap();
        plan.add(&catalog.baseline).unwrap();
        plan.select_since(0).unwrap();
        assert_eq!(plan.count, catalog.baseline.meta.count);
        assert_eq!(
            execute(&plan),
            bytes,
            "whole-pack dedup should preserve original bytes"
        );

        // Exact MPHF misses cannot silently select an arbitrary fingerprint match.
        let mut absent = Oid20([0; 20]);
        absent.0[19] = 9;
        assert!(catalog.baseline.index.find(&absent).unwrap().is_none());
        assert!(matches!(plan.select_oid(&absent), Err(Error::Missing(_))));
        assert!(!e.pipeline(Some(Tier::M15), t0 + 900).unwrap());
    }

    let store = root.path().join("clustered");
    {
        let s = server(None, &store, &root.path().join("unchanged-server"));
        let (status, headers, body) = http_response(&s, "GET", "/gvfs/prefetch", b"");
        assert_eq!(status, 200);
        assert_coverage(&headers, 0, t0, "baseline", false);
        assert_eq!(prefetch_pack(&body).1, t0);
        let (status, headers, body) = http_response(
            &s,
            "GET",
            &format!("/gvfs/prefetch?lastPackTimestamp={t0}"),
            b"",
        );
        assert_eq!(status, 200);
        assert_no_coverage(&headers);
        assert_eq!(
            body,
            gvfs_daemon::gpre::gpre_file_header(0),
            "an up-to-date client must not accumulate empty packs"
        );
    }
    let mut e = engine(
        Some(&repo),
        &store,
        &root.path().join("writer"),
        PackMode::Clustered,
    );
    e.reload().unwrap();
    commit(&repo, 100);
    assert!(e.pipeline(Some(Tier::M15), t0 + 900).unwrap());
    let first_tip = tip(&repo);
    let first = *e
        .catalog
        .as_ref()
        .unwrap()
        .manifest
        .pyramid
        .first()
        .unwrap();
    let slab = Slab::load(&e.store, first, &root.path().join("cache")).unwrap();
    for i in 0..slab.index.len() {
        assert_eq!(slab.index.record(i as u32).unwrap().base, NO_BASE);
    }
    commit(&repo, 101);
    e.pipeline(Some(Tier::M15), t0 + 3600).unwrap();
    // Higher tiers have no upstream configured: they must use storage alone.
    let mut compactor = engine(
        None,
        &store,
        &root.path().join("compactor"),
        PackMode::Clustered,
    );
    compactor.pipeline(Some(Tier::H24), t0 + 86400).unwrap();
    assert!(
        compactor
            .catalog
            .as_ref()
            .unwrap()
            .manifest
            .pyramid
            .iter()
            .all(|s| s.tier == Tier::H24)
    );
    compactor
        .pipeline(Some(Tier::WeeklyDelta), t0 + 604800)
        .unwrap();
    let catalog = compactor.catalog.as_ref().unwrap();
    assert_eq!(catalog.manifest.weekly.len(), 1);
    assert!(catalog.manifest.pyramid.is_empty());
    verify_stored(&store, catalog.baseline.meta, &scratch_dir);
    verify_stored(&store, catalog.manifest.weekly[0], &scratch_dir);
    let mut warm = Plan::new(32768, 32768);
    catalog.prefetch(t0, &mut warm).unwrap();
    assert!(warm.pack_len < catalog.baseline.meta.pack_len);
    assert!(!warm.slabs.iter().any(|s| s.meta.tier == Tier::Baseline));
    let warm_ids = pack_oids(&execute(&warm), &scratch_dir);
    assert!(warm_ids.contains(&first_tip));
    assert!(!warm_ids.contains(&original_tip));
    {
        let s = server(None, &store, &root.path().join("partial-server"));
        let since = t0 + 900;
        let (status, headers, body) = http_response(
            &s,
            "GET",
            &format!("/gvfs/prefetch?lastPackTimestamp={since}"),
            b"",
        );
        assert_eq!(status, 200);
        assert_coverage(
            &headers,
            since,
            catalog.manifest.timestamp,
            "incremental",
            false,
        );
        let (pack, cursor) = prefetch_pack(&body);
        assert_eq!(cursor, catalog.manifest.timestamp);
        let ids = pack_oids(pack, &scratch_dir);
        assert!(
            !ids.contains(&first_tip),
            "filtered weekly response is not a full-week replacement"
        );
        assert!(ids.contains(&catalog.tip()));
    }

    // CAS failure, restart, and orphaned uploads leave the published root unchanged.
    let saved = e.store.manifest().unwrap().unwrap();
    assert!(matches!(
        e.store.publish_manifest(&saved.0, Some("stale-etag")),
        Err(Error::Conflict)
    ));
    fs::write(store.join("unreferenced-crash.pack"), b"truncated").unwrap();
    assert_eq!(e.store.manifest().unwrap().unwrap(), saved);
    let mut corrupt = saved.0.clone();
    corrupt[10] ^= 1;
    assert!(Manifest::decode(&corrupt).is_err());

    // Recover the unsealed live gap before opening the listener.
    commit(&repo, 102);
    let server_work = root.path().join("server");
    let s = server(Some(&repo), &store, &server_work);
    assert_eq!(http(&s, "GET", "/healthz", b"").0, 200);
    let (status, config_bytes) = http(&s, "GET", "/gvfs/config", b"");
    assert_eq!(status, 200);
    let config_json = std::str::from_utf8(&config_bytes).unwrap();
    assert!(config_json.contains("\"Min\":{\"Major\":0,\"Minor\":0,\"Build\":0,\"Revision\":0}"));
    assert!(config_json.contains("\"Max\":null"));
    let (status, headers, body) = http_response(&s, "GET", "/gvfs/prefetch", b"");
    assert_eq!(status, 200);
    let (pack, cursor) = prefetch_pack(&body);
    assert_coverage(&headers, 0, cursor, "baseline", true);
    let cold_ids = pack_oids(pack, &scratch_dir);
    assert!(cold_ids.contains(&tip(&repo)));
    // L0 must not advance a durable cursor ahead of an independently running pipeline.
    assert_eq!(
        cursor,
        compactor.catalog.as_ref().unwrap().manifest.timestamp
    );
    let (status, headers, body) = http_response(
        &s,
        "GET",
        &format!("/gvfs/prefetch?lastPackTimestamp={cursor}"),
        b"",
    );
    assert_eq!(status, 200);
    assert_coverage(&headers, cursor, cursor, "incremental", true);
    assert_eq!(
        prefetch_pack(&body).1,
        cursor,
        "live-only data must not advance sealed coverage"
    );
    let object_oid =
        Oid20::from_hex(git(&repo, &["rev-parse", "HEAD:file-0102"]).trim_ascii()).unwrap();
    let (status, headers, loose) =
        http_response(&s, "GET", &format!("/gvfs/objects/{object_oid}"), b"");
    assert_eq!(status, 200);
    assert_no_coverage(&headers);
    let mut decoded = Vec::new();
    flate2::read::ZlibDecoder::new(loose.as_slice())
        .read_to_end(&mut decoded)
        .unwrap();
    assert!(decoded.ends_with(&pointer(102, 102)));
    let request = format!("{{\"commitDepth\":1,\"objectIds\":[\"{}\"]}}", tip(&repo));
    let (status, bytes) = http(&s, "POST", "/gvfs/objects", request.as_bytes());
    assert_eq!(status, 200);
    let ids = pack_oids(&bytes, &scratch_dir);
    assert!(ids.contains(&object_oid));
    let (status, sizes) = http(
        &s,
        "POST",
        "/gvfs/sizes",
        format!("[\"{object_oid}\"]").as_bytes(),
    );
    assert_eq!(status, 200);
    assert!(
        String::from_utf8(sizes)
            .unwrap()
            .contains(&format!("\"Size\":{}", pointer(102, 102).len()))
    );
    assert_eq!(
        http(
            &s,
            "GET",
            "/gvfs/objects/0000000000000000000000000000000000000000",
            b""
        )
        .0,
        404
    );
    let metrics = String::from_utf8(http(&s, "GET", "/metrics", b"").1).unwrap();
    assert!(
        metrics.contains("gvfs_alloc_hot_path_total 0\n"),
        "{metrics}"
    );

    let head_tree =
        Oid20::from_hex(git(&repo, &["rev-parse", "HEAD^{tree}"]).trim_ascii()).unwrap();
    let (status, bytes) = http(
        &s,
        "POST",
        "/gvfs/objects",
        format!("{{\"objectIds\":[\"{head_tree}\"]}}").as_bytes(),
    );
    assert_eq!(status, 200);
    assert_eq!(
        gvfs_daemon::pack::parse_pack_header(&bytes).unwrap(),
        1,
        "direct tree requests are point lookups"
    );

    // A pipeline publication behind the live edge must not lose the remainder.
    let partial_tip = tip(&repo);
    commit(&repo, 103);
    let ahead = tip(&repo);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !String::from_utf8(http(&s, "GET", "/info/refs?service=git-upload-pack", b"").1)
        .unwrap()
        .contains(&ahead.to_string())
    {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(100));
    }
    git(
        &repo,
        &[
            "update-ref",
            "refs/heads/partial-pipeline",
            &partial_tip.to_string(),
        ],
    );
    let partial_work = root.path().join("partial-pipeline");
    let upstream = GitUpstream::open(
        repo.to_str().unwrap(),
        "refs/heads/partial-pipeline",
        &partial_work,
        None,
    )
    .unwrap();
    let mut partial = Engine::new(
        config(partial_work, PackMode::Clustered),
        e.store.clone(),
        Some(upstream),
    )
    .unwrap();
    partial.pipeline(Some(Tier::M15), t0 + 604810).unwrap();
    std::thread::sleep(Duration::from_millis(2200));
    assert_eq!(
        http(&s, "GET", &format!("/gvfs/objects/{ahead}"), b"").0,
        200
    );

    // Stock Git smart HTTP clone also receives exactly one pack and the exact original OIDs.
    let clone = root.path().join("client");
    let output = Command::new("git")
        .args(["clone", "--quiet", "--no-checkout"])
        .arg(&s.url)
        .arg(&clone)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "clone: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(tip(&clone), tip(&repo));
    git(&clone, &["fsck", "--full", "--no-dangling"]);
    assert_eq!(
        fs::read_dir(clone.join(".git/objects/pack"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|e| e == "pack"))
            .count(),
        1
    );
    drop(s);
    let s = server(Some(&repo), &store, &server_work);
    assert_eq!(
        http(&s, "GET", &format!("/gvfs/objects/{}", tip(&repo)), b"").0,
        200
    );
    drop(s);

    // Force rewind: update refs without dropping historical objects or inventing data.
    git(
        &repo,
        &["update-ref", "refs/heads/main", &original_tip.to_string()],
    );
    let mut rewound = engine(
        Some(&repo),
        &store,
        &root.path().join("rewound"),
        PackMode::Clustered,
    );
    rewound.pipeline(Some(Tier::M15), t0 + 604900).unwrap();
    assert_eq!(rewound.catalog.as_ref().unwrap().tip(), original_tip);
    assert_eq!(
        rewound
            .catalog
            .as_ref()
            .unwrap()
            .manifest
            .pyramid
            .last()
            .unwrap()
            .count,
        0
    );
}

#[test]
fn remote_git_transport_reuses_a_real_negotiation_ref() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("source");
    fixture(&repo);
    let work = root.path().join("transport");
    let upstream =
        GitUpstream::open(&format!("file://{}", repo.display()), "HEAD", &work, None).unwrap();
    let initial = upstream.tip().unwrap();
    let mut source = gvfs_daemon::source::Source::new(32768).unwrap();
    upstream
        .prepare(&mut source, initial, Oid20([0; 20]))
        .unwrap();
    let before = source.entries.len();
    assert_eq!(upstream.tip().unwrap(), initial);
    upstream.prepare(&mut source, initial, initial).unwrap();
    assert_eq!(source.entries.len(), before);
    commit(&repo, 199);
    let next = upstream.tip().unwrap();
    assert_ne!(next, initial);
    upstream.prepare(&mut source, next, initial).unwrap();
    assert!(
        source.entries.len() - before < 20,
        "incremental transport redownloaded history"
    );
    let reference = Command::new("git")
        .arg("--git-dir")
        .arg(work.join("upstream.git"))
        .args(["rev-parse", "refs/gvfs/upstream"])
        .output()
        .unwrap();
    assert!(reference.status.success());
    assert_eq!(
        Oid20::from_hex(reference.stdout.trim_ascii()).unwrap(),
        next
    );
}

#[test]
fn newly_published_have_need_not_be_in_the_edges_decoder_cache() {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("source");
    fixture(&repo);
    let upstream = GitUpstream::open(
        repo.to_str().unwrap(),
        "HEAD",
        &root.path().join("edge"),
        None,
    )
    .unwrap();
    let mut source = gvfs_daemon::source::Source::new(32768).unwrap();
    let initial = tip(&repo);
    upstream
        .prepare(&mut source, initial, Oid20([0; 20]))
        .unwrap();
    commit(&repo, 200);
    let published = tip(&repo);
    assert!(source.find(&published).unwrap().is_none());
    commit(&repo, 201);
    let next = tip(&repo);
    let before = source.entries.len();
    upstream.prepare(&mut source, next, published).unwrap();
    assert!(
        source.entries.len().saturating_sub(before) < 20,
        "handoff fetched full history"
    );
    assert!(
        source.find(&published).unwrap().is_none(),
        "the published commit should be excluded"
    );
    assert!(source.find(&next).unwrap().is_some());
}

#[test]
fn conditional_http_and_real_client_compaction_keep_two_managed_packs() {
    use gvfs_daemon::client::{Cache, Config as ClientConfig};
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("source");
    fixture(&repo);
    let original = tip(&repo);
    let store = root.path().join("store");
    let mut publisher = engine(
        Some(&repo),
        &store,
        &root.path().join("publisher"),
        PackMode::Clustered,
    );
    let t0 = 1_699_920_000;
    publisher.pipeline(Some(Tier::M15), t0).unwrap();
    let s = server(Some(&repo), &store, &root.path().join("server"));
    let (status, headers, body) = http_response(&s, "GET", "/gvfs/prefetch", b"");
    assert_eq!(status, 200);
    assert_eq!(
        gvfs_daemon::gpre::parse_gpre_pack_header(&body[8..])
            .unwrap()
            .idx_len,
        None
    );
    let tag = headers.get("etag").unwrap().to_str().unwrap();
    let (status, headers, body) =
        http_headers(&s, "GET", "/gvfs/prefetch", b"", &[("If-None-Match", tag)]);
    assert_eq!(status, 304);
    assert!(body.is_empty());
    assert!(!headers.contains_key("content-length"));
    assert!(!headers.contains_key("transfer-encoding"));
    assert_no_coverage(&headers);
    let weak = format!("\"not-a-tag\", W/{tag}");
    assert_eq!(
        http_headers(
            &s,
            "GET",
            "/gvfs/prefetch",
            b"",
            &[("If-None-Match", &weak)]
        )
        .0,
        304
    );
    assert_eq!(
        http_headers(
            &s,
            "GET",
            &format!("/gvfs/prefetch?lastPackTimestamp={t0}"),
            b"",
            &[("If-None-Match", tag)]
        )
        .0,
        200
    );
    assert_eq!(
        http_headers(
            &s,
            "GET",
            &format!("/gvfs/prefetch?lastPackTimestamp={}", t0 + 1),
            b"",
            &[("If-None-Match", "*")]
        )
        .0,
        503
    );

    let cache_path = root.path().join("cache");
    let mut config = ClientConfig::new(s.url.clone(), cache_path.clone());
    config.max_objects = 32768;
    config.max_object_bytes = 1 << 20;
    config.max_download_bytes = 64 << 20;
    let mut cache = Cache::open(config.clone()).unwrap();
    assert_eq!(cache.sync_once().unwrap().managed_packs, 1);
    assert_eq!(cache.tip(), Some(original));
    let baseline = fs::read_dir(cache_path.join("objects/pack"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("gvfs-base-")
                && p.extension().is_some_and(|e| e == "pack")
        })
        .unwrap();
    let baseline_bytes = fs::read(&baseline).unwrap();
    cache.sync_once().unwrap(); // Establish the validator for the advanced cursor.
    assert!(cache.sync_once().unwrap().not_modified);

    let wait_tip = |wanted: Oid20| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !String::from_utf8(http(&s, "GET", "/info/refs?service=git-upload-pack", b"").1)
            .unwrap()
            .contains(&wanted.to_string())
        {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    commit(&repo, 100);
    let orphan_a = tip(&repo);
    wait_tip(orphan_a);
    assert_eq!(cache.sync_once().unwrap().managed_packs, 2);
    assert!(cache.sync_once().unwrap().not_modified);
    let status = Command::new("git")
        .arg("--git-dir")
        .arg(&cache_path)
        .args(["multi-pack-index", "write"])
        .status()
        .unwrap();
    assert!(status.success());
    commit(&repo, 101);
    let orphan_b = tip(&repo);
    wait_tip(orphan_b);
    assert_eq!(cache.sync_once().unwrap().managed_packs, 2);

    // Neither unsealed commit reaches the pipeline after this rewind.
    git(
        &repo,
        &["update-ref", "refs/heads/main", &original.to_string()],
    );
    wait_tip(original);
    cache.sync_once().unwrap();
    assert_eq!(cache.tip(), Some(original));
    commit(&repo, 200);
    let current = tip(&repo);
    publisher.pipeline(Some(Tier::M15), t0 + 900).unwrap();
    wait_tip(current);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (_, headers, _) = http_response(
            &s,
            "GET",
            &format!("/gvfs/prefetch?lastPackTimestamp={t0}"),
            b"",
        );
        if headers.get("x-gvfs-pack-end").and_then(|h| h.to_str().ok())
            == Some((t0 + 900).to_string().as_str())
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(100));
    }
    for oid in [orphan_a, orphan_b] {
        assert!(
            publisher.catalog.as_ref().unwrap().pyramid.iter().all(|s| s
                .index
                .find(&oid)
                .unwrap()
                .is_none())
        );
    }
    cache.sync_once().unwrap();
    assert_eq!(cache.tip(), Some(current));
    assert_eq!(cache.managed_pack_count(), 2);
    assert_eq!(fs::read(&baseline).unwrap(), baseline_bytes);
    for oid in [original, orphan_a, orphan_b, current] {
        assert!(
            Command::new("git")
                .arg("--git-dir")
                .arg(&cache_path)
                .args(["cat-file", "-e"])
                .arg(format!("{oid}^{{commit}}"))
                .status()
                .unwrap()
                .success()
        );
    }
    assert_eq!(
        fs::read_dir(cache_path.join("objects/pack"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|e| e == "pack"))
            .count(),
        2
    );
    assert!(
        Command::new("git")
            .arg("--git-dir")
            .arg(&cache_path)
            .args(["multi-pack-index", "verify"])
            .status()
            .unwrap()
            .success()
    );

    // New workspaces share the cache without downloading or creating another pack.
    let workspace = root.path().join("workspace");
    assert!(
        Command::new("git")
            .args(["clone", "--shared", "--quiet", "--no-checkout"])
            .arg(&cache_path)
            .arg(&workspace)
            .status()
            .unwrap()
            .success()
    );
    assert_eq!(tip(&workspace), current);
    assert_eq!(
        fs::read_dir(workspace.join(".git/objects/pack"))
            .unwrap()
            .count(),
        0
    );
    drop(cache);
    let output = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"))
        .args(["sync", "--server"])
        .arg(&s.url)
        .arg("--cache-dir")
        .arg(&cache_path)
        .args([
            "--max-objects",
            "32768",
            "--max-object-mib",
            "1",
            "--strict-alloc",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("managed_packs=2")
    );
    let cache = Cache::open(config).unwrap();
    assert_eq!(cache.tip(), Some(current));
    assert_eq!(cache.managed_pack_count(), 2);

    let legacy = server_options(
        None,
        &store,
        &root.path().join("legacy"),
        &["--prefetch-index"],
    );
    let (_, _, body) = http_response(
        &legacy,
        "GET",
        &format!("/gvfs/prefetch?lastPackTimestamp={t0}"),
        b"",
    );
    assert!(
        gvfs_daemon::gpre::parse_gpre_pack_header(&body[8..])
            .unwrap()
            .idx_len
            .is_some()
    );
}
