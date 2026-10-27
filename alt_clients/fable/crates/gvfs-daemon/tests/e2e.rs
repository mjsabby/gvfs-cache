//! End-to-end: a pointer-file repository → baseline → incremental slabs (with a revert, so
//! duplicates must be dropped) → rollup → one spliced pack, checked with `git index-pack`
//! and `git fsck`; then the server: prefetch, objects, sizes, loose objects, live sync, and
//! zero hot-path allocations.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic
)]

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use gvfs_daemon::manifest::Tier;
use gvfs_daemon::oid::Oid;
use gvfs_daemon::pipeline::{BaselineMode, PipelineConfig, run_pipeline};
use gvfs_daemon::store::FsStore;
use gvfs_daemon::transport::LocalGit;
use gvfs_daemon::zlib::Inflater;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env_remove("GIT_DIR")
        .output()
        .expect("git runs");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn pointer(seed: u64) -> String {
    let mut h = [0u8; 33];
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x1234_5678_9ABC_DEF0;
    for b in h.iter_mut().take(32) {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *b = (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8;
    }
    let mut hex = String::new();
    for b in h {
        hex.push_str(&format!("{b:02X}"));
    }
    format!("~~Git-hfs 1.0\nblob:{hex}\nsize:{}", 10 + seed % 100_000)
}

fn write_pointer(repo: &Path, rel: &str, seed: u64) {
    let p = repo.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, pointer(seed)).unwrap();
}

fn make_repo(repo: &Path) {
    fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-q", "-b", "main"]);
    git(repo, &["config", "user.email", "t@t"]);
    git(repo, &["config", "user.name", "t"]);
    for d in 0..12u64 {
        for f in 0..25u64 {
            write_pointer(repo, &format!("src/dir{d}/file{f}.txt"), d * 1000 + f);
        }
    }
    fs::write(repo.join("README.md"), "a real file, not a pointer\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-qm", "initial"]);
    for c in 0..15u64 {
        for k in 0..4u64 {
            let d = (c * 7 + k) % 12;
            let f = (c * 13 + k * 5) % 25;
            write_pointer(
                repo,
                &format!("src/dir{d}/file{f}.txt"),
                100_000 + c * 10 + k,
            );
        }
        git(repo, &["add", "."]);
        git(repo, &["commit", "-qm", &format!("change {c}")]);
    }
    git(repo, &["repack", "-adq"]);
}

fn add_commits(repo: &Path, n: u64, salt: u64) {
    for c in 0..n {
        for k in 0..3u64 {
            let d = (salt + c * 5 + k) % 12;
            let f = (salt * 3 + c * 11 + k) % 25;
            write_pointer(
                repo,
                &format!("src/dir{d}/file{f}.txt"),
                500_000 + salt * 1000 + c * 10 + k,
            );
        }
        git(repo, &["add", "."]);
        git(repo, &["commit", "-qm", &format!("inc {salt}/{c}")]);
    }
}

/// `git index-pack --verify` on a pack/idx pair, and the idx must equal git's own.
fn verify_pack_with_git(scratch: &Path, pack: &[u8], idx: &[u8], label: &str) {
    let dir = scratch.join(format!("verify-{label}"));
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("p.pack"), pack).unwrap();
    fs::write(dir.join("p.idx"), idx).unwrap();
    git(&dir, &["index-pack", "--verify", "p.pack"]);
    fs::write(dir.join("g.pack"), pack).unwrap();
    git(&dir, &["index-pack", "g.pack"]);
    assert_eq!(
        fs::read(dir.join("g.idx")).unwrap(),
        idx,
        "{label}: idx must be byte-identical to git's"
    );
}

fn pipeline(
    store: &FsStore,
    repo: &Path,
    tier: Tier,
    baseline: BaselineMode,
) -> gvfs_daemon::pipeline::Report {
    let mut up = LocalGit::new(repo);
    // Fetch-built rollups from h1 up; leaves kept below h24.
    let mut cfg = PipelineConfig {
        baseline,
        gc_after_secs: 0,
        rollup_fetch_from: Some(Tier::H1),
        leaf_tier: Some(Tier::H24),
        ..PipelineConfig::default()
    };
    cfg.ingest.resolve.workers = 4;
    cfg.ingest.resolve.arena_bytes = 8 << 20;
    cfg.ingest.resolve.side_bytes = 4 << 20;
    let mut log = Vec::new();
    run_pipeline(store, &mut up, tier, &cfg, &mut log)
        .unwrap_or_else(|e| panic!("pipeline {tier}: {e}\n{}", String::from_utf8_lossy(&log)))
}

struct Http {
    addr: String,
}

impl Http {
    fn request(&self, method: &str, path: &str, body: Option<&[u8]>) -> (u16, Vec<u8>) {
        let (status, _, body) = self.request_full(method, path, body);
        (status, body)
    }

    fn request_full(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> (u16, String, Vec<u8>) {
        let mut s = TcpStream::connect(&self.addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        let mut head = format!("{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n");
        if let Some(b) = body {
            head.push_str(&format!("Content-Length: {}\r\n", b.len()));
        }
        head.push_str("\r\n");
        s.write_all(head.as_bytes()).unwrap();
        if let Some(b) = body {
            s.write_all(b).unwrap();
        }
        let mut resp = Vec::new();
        s.read_to_end(&mut resp).unwrap();
        let sep = resp
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("headers");
        let head = String::from_utf8_lossy(&resp[..sep]).to_string();
        let status: u16 = head.split(' ').nth(1).unwrap().parse().unwrap();
        let mut body = resp[sep + 4..].to_vec();
        if let Some(cl) = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length: "))
        {
            assert_eq!(
                body.len(),
                cl.trim().parse::<usize>().unwrap(),
                "content-length must match the body"
            );
        } else if head
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            body = dechunk(&body);
        }
        (status, head, body)
    }

    fn metric(&self, name: &str) -> u64 {
        let (_, b) = self.request("GET", "/metrics", None);
        String::from_utf8_lossy(&b)
            .lines()
            .find_map(|l| {
                l.strip_prefix(name)
                    .map(|v| v.trim().parse::<u64>().unwrap())
            })
            .unwrap_or_else(|| panic!("metric {name}"))
    }
}

fn dechunk(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = 0usize;
    loop {
        let eol = raw[at..].windows(2).position(|w| w == b"\r\n").unwrap() + at;
        let size = usize::from_str_radix(
            std::str::from_utf8(&raw[at..eol])
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .trim(),
            16,
        )
        .unwrap();
        at = eol + 2;
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&raw[at..at + size]);
        at += size + 2;
    }
}

fn gpre_packs(stream: &[u8]) -> Vec<(i64, Vec<u8>, Vec<u8>)> {
    assert_eq!(&stream[..6], b"GPRE \x01");
    let count = u16::from_le_bytes([stream[6], stream[7]]) as usize;
    let mut at = 8;
    let mut out = Vec::new();
    for _ in 0..count {
        let ts = i64::from_le_bytes(stream[at..at + 8].try_into().unwrap());
        let pl = i64::from_le_bytes(stream[at + 8..at + 16].try_into().unwrap()) as usize;
        let il = i64::from_le_bytes(stream[at + 16..at + 24].try_into().unwrap()) as usize;
        at += 24;
        let pack = stream[at..at + pl].to_vec();
        at += pl;
        let idx = stream[at..at + il].to_vec();
        at += il;
        out.push((ts, pack, idx));
    }
    assert_eq!(at, stream.len(), "no trailing bytes");
    out
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_server(store: &Path, repo: &Path) -> (Server, Http) {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"))
        .args(["serve", "--bind", &addr, "--store"])
        .arg(store)
        .arg("--upstream")
        .arg(repo)
        .args([
            "--arenas",
            "2",
            "--arena-mib",
            "8",
            "--poll-secs",
            "1",
            "--manifest-poll-secs",
            "1",
            "--alloc-guard",
            "count",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let http = Http { addr };
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if TcpStream::connect(&http.addr).is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "server did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
    (Server(child), http)
}

#[test]
fn pipeline_splice_and_serve_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let root: PathBuf = tmp.path().to_path_buf();
    if std::env::var_os("GVFS_E2E_KEEP").is_some() {
        eprintln!("keeping {}", root.display());
        std::mem::forget(tmp);
    }
    let repo = root.join("repo");
    make_repo(&repo);
    let store_dir = root.join("store");
    let store = FsStore::open(&store_dir).unwrap();

    // Genesis: the whole branch becomes the baseline slab.
    let r = pipeline(&store, &repo, Tier::M15, BaselineMode::Fetch);
    assert!(r.genesis);
    let m = store.load_manifest().unwrap().unwrap();
    let base = m.baseline.unwrap();
    assert_eq!(
        m.tip,
        Oid::from_hex(git(&repo, &["rev-parse", "HEAD"]).as_bytes()).unwrap()
    );
    let all_objects: usize = git(&repo, &["rev-list", "--objects", "--all"])
        .lines()
        .count();
    assert_eq!(base.object_count as usize, all_objects);
    let paths = store.slab_paths(base.seq);
    verify_pack_with_git(
        &root,
        &fs::read(&paths.pack).unwrap(),
        &fs::read(&paths.idx).unwrap(),
        "baseline",
    );

    // Incremental slabs; the revert re-sends objects the baseline already has.
    add_commits(&repo, 3, 1);
    let r = pipeline(&store, &repo, Tier::M15, BaselineMode::Fetch);
    assert!(r.ingested.is_some());
    add_commits(&repo, 2, 2);
    pipeline(&store, &repo, Tier::M15, BaselineMode::Fetch);
    git(&repo, &["revert", "--no-edit", "HEAD"]);
    let r = pipeline(&store, &repo, Tier::M15, BaselineMode::Fetch);
    let (_, fetched, dups, _) = r.ingested_stats.unwrap();
    assert!(
        dups > 0 && dups < fetched as u64,
        "revert must drop duplicates ({dups} of {fetched})"
    );
    assert_eq!(
        r.ingested.unwrap().object_count,
        1,
        "only the revert commit itself is new"
    );

    // Rollup into one h1 slab, re-fetched from the upstream as one window; the three m15
    // slabs stay on disk as its leaves (everything else unreferenced is swept: gc = 0).
    let members_before = store.load_manifest().unwrap().unwrap().pyramid.clone();
    assert_eq!(members_before.len(), 3);
    let r = pipeline(&store, &repo, Tier::H1, BaselineMode::Fetch);
    assert_eq!(r.rollups.len(), 1);
    let m = store.load_manifest().unwrap().unwrap();
    assert_eq!(m.pyramid.len(), 1);
    let h1 = m.pyramid[0];
    assert_eq!(h1.tier, Tier::H1);
    assert_eq!(
        (h1.start_ts, h1.tip),
        (members_before[0].start_ts, members_before[2].tip)
    );
    assert!(
        h1.end_ts > members_before[2].end_ts,
        "a rollup gets a fresh end_ts so current clients receive the superset once"
    );
    let member_objects: u32 = members_before.iter().map(|s| s.object_count).sum();
    assert!(
        h1.object_count <= member_objects && h1.object_count >= member_objects - 1,
        "a fetched window holds the members' objects ({} vs {member_objects})",
        h1.object_count
    );
    assert_eq!(
        m.leaves_of(0).iter().map(|s| s.seq).collect::<Vec<_>>(),
        members_before.iter().map(|s| s.seq).collect::<Vec<_>>()
    );
    assert_eq!(
        store.list_slab_seqs().unwrap().len(),
        5,
        "baseline + h1 + its 3 leaves remain on disk"
    );
    let p = store.slab_paths(h1.seq);
    verify_pack_with_git(
        &root,
        &fs::read(&p.pack).unwrap(),
        &fs::read(&p.idx).unwrap(),
        "h1-fetched",
    );

    // The server: one spliced pack for a cold clone, verified by git and fully connected.
    let (_server, http) = start_server(&store_dir, &repo);
    let (code, cfg) = http.request("GET", "/gvfs/config", None);
    assert_eq!(code, 200);
    assert!(String::from_utf8_lossy(&cfg).contains("CacheServers"));
    let (code, head, body) = http.request_full(
        "GET",
        "/org/proj/_git/repo/gvfs/prefetch?lastPackTimestamp=0",
        None,
    );
    assert_eq!(code, 200);
    // The covered interval, for client-side superset tombstoning.
    assert!(head.contains("x-gvfs-pack-start: 0\r\n"), "{head}");
    assert!(
        head.contains(&format!("x-gvfs-pack-end: {}\r\n", m.latest_ts())),
        "{head}"
    );
    let packs = gpre_packs(&body);
    assert_eq!(packs.len(), 1, "a cold clone gets exactly one pack");
    let (ts, pack, idx) = &packs[0];
    assert_eq!(*ts as u64, m.latest_ts());
    verify_pack_with_git(&root, pack, idx, "cold");
    let bare = root.join("clone.git");
    git(&root, &["init", "-q", "--bare", "clone.git"]);
    fs::write(bare.join("objects/pack/pack-c.pack"), pack).unwrap();
    fs::write(bare.join("objects/pack/pack-c.idx"), idx).unwrap();
    let tip = git(&repo, &["rev-parse", "HEAD"]);
    let gd = format!("--git-dir={}", bare.display());
    git(&root, &[&gd, "update-ref", "refs/heads/main", &tip]);
    git(&root, &[&gd, "fsck", "--connectivity-only"]);
    let reachable = git(&root, &[&gd, "rev-list", "--objects", "--all"])
        .lines()
        .count();
    assert_eq!(
        reachable,
        git(&repo, &["rev-list", "--objects", "--all"])
            .lines()
            .count()
    );

    // Warm client at the window start: the h1 slab whole (passthrough).
    let (_, body) = http.request(
        "GET",
        &format!("/gvfs/prefetch?lastPackTimestamp={}", base.end_ts),
        None,
    );
    let packs = gpre_packs(&body);
    assert_eq!(packs.len(), 1);
    assert_eq!(
        packs[0].1,
        fs::read(store.slab_paths(m.pyramid[0].seq).pack).unwrap()
    );
    // Warm client inside the window: only the two leaves it lacks, spliced into one pack.
    let (_, head, body) = http.request_full(
        "GET",
        &format!(
            "/gvfs/prefetch?lastPackTimestamp={}",
            members_before[0].end_ts
        ),
        None,
    );
    let packs = gpre_packs(&body);
    assert_eq!(packs.len(), 1);
    let expected: u32 = members_before[1].object_count + members_before[2].object_count;
    assert_eq!(
        u32::from_be_bytes(packs[0].1[8..12].try_into().unwrap()),
        expected,
        "leaves 2+3 only"
    );
    assert!(
        head.contains(&format!(
            "x-gvfs-pack-start: {}\r\n",
            members_before[1].start_ts
        )),
        "{head}"
    );
    verify_pack_with_git(&root, &packs[0].1, &packs[0].2, "leaves");
    let (_, body) = http.request(
        "GET",
        &format!("/gvfs/prefetch?lastPackTimestamp={}", m.latest_ts()),
        None,
    );
    assert!(gpre_packs(&body).is_empty());
    // A client holding every leaf (cursor at the last leaf's end) is current: nothing by
    // default, the whole h1 rollup with compact=1 (so its leaf packs can be unlinked).
    let (_, body) = http.request(
        "GET",
        &format!(
            "/gvfs/prefetch?lastPackTimestamp={}",
            members_before[2].end_ts
        ),
        None,
    );
    assert!(gpre_packs(&body).is_empty(), "all leaves held: no pack");
    let (_, head, body) = http.request_full(
        "GET",
        &format!(
            "/gvfs/prefetch?lastPackTimestamp={}&compact=1",
            members_before[2].end_ts
        ),
        None,
    );
    let packs = gpre_packs(&body);
    assert_eq!(packs.len(), 1);
    assert_eq!(
        packs[0].1,
        fs::read(store.slab_paths(h1.seq).pack).unwrap(),
        "compact=1 serves the rollup itself"
    );
    assert!(
        head.contains(&format!(
            "x-gvfs-pack-start: {}\r\nx-gvfs-pack-end: {}\r\n",
            h1.start_ts, h1.end_ts
        )),
        "{head}"
    );

    // Objects: a blob, a subtree, a commit closure; sizes; loose objects.
    let blob = git(&repo, &["rev-parse", "HEAD:src/dir3/file7.txt"]);
    let sub = git(&repo, &["rev-parse", "HEAD:src/dir3"]);
    let req = format!(
        "{{\"commitDepth\":1,\"objectIds\":[\"{blob}\",\"{sub}\",\"{}\"]}}",
        "0".repeat(40)
    );
    let (code, pack) = http.request("POST", "/gvfs/objects", Some(req.as_bytes()));
    assert_eq!(code, 200);
    let dir = root.join("objs");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("o.pack"), &pack).unwrap();
    git(&dir, &["index-pack", "o.pack"]);
    let listed = git(&dir, &["verify-pack", "-v", "o.idx"]);
    assert!(listed.contains(&blob) && listed.contains(&sub));
    assert_eq!(
        listed
            .lines()
            .filter(|l| l.len() > 40 && l.as_bytes()[40] == b' ')
            .count(),
        2
    );
    let req = format!("{{\"commitDepth\":1,\"objectIds\":[\"{tip}\"]}}");
    let (code, pack) = http.request("POST", "/gvfs/objects", Some(req.as_bytes()));
    assert_eq!(code, 200);
    fs::write(dir.join("c.pack"), &pack).unwrap();
    git(&dir, &["index-pack", "c.pack"]);
    let trees = git(&repo, &["ls-tree", "-r", "-d", "HEAD"]).lines().count();
    let listed = git(&dir, &["verify-pack", "-v", "c.idx"]);
    assert_eq!(
        listed
            .lines()
            .filter(|l| l.len() > 40 && l.as_bytes()[40] == b' ')
            .count(),
        trees + 2,
        "commit + root tree + subtrees"
    );
    let (code, sizes) = http.request(
        "POST",
        "/gvfs/sizes",
        Some(format!("[\"{blob}\",\"{tip}\"]").as_bytes()),
    );
    assert_eq!(code, 200);
    let sizes = String::from_utf8_lossy(&sizes).to_string();
    assert!(
        sizes.contains(&format!(
            "\"Id\":\"{blob}\",\"Size\":{}",
            git(&repo, &["cat-file", "-s", &blob])
        )),
        "{sizes}"
    );
    assert!(
        sizes.contains(&format!(
            "\"Id\":\"{tip}\",\"Size\":{}",
            git(&repo, &["cat-file", "-s", &tip])
        )),
        "{sizes}"
    );
    for oid in [&blob, &sub, &tip] {
        let (code, loose) = http.request("GET", &format!("/gvfs/objects/{oid}"), None);
        assert_eq!(code, 200);
        let mut inflater = Inflater::new();
        let mut out = vec![0u8; 1 << 20];
        let (_, n) = inflater.inflate(&loose, &mut out).unwrap();
        let digest: [u8; 20] = <sha1::Sha1 as sha1::Digest>::digest(&out[..n]).into();
        assert_eq!(
            Oid(digest).to_string(),
            *oid,
            "loose object hashes to its id"
        );
    }
    let (code, _) = http.request("GET", &format!("/gvfs/objects/{}", "f".repeat(40)), None);
    assert_eq!(
        code, 404,
        "an id the upstream does not have either stays a 404"
    );
    assert_eq!(http.metric("gvfs_miss_fetched_objects_total "), 0);

    // Objects outside the tracked branch (a feature branch the pipeline never saw): the
    // live thread fetches them from the upstream on demand, so the client never sees 404.
    git(&repo, &["checkout", "-q", "-b", "feature"]);
    add_commits(&repo, 1, 21);
    let feat = git(&repo, &["rev-parse", "HEAD"]);
    let feat_tree = git(&repo, &["rev-parse", "HEAD^{tree}"]);
    git(&repo, &["checkout", "-q", "main"]);
    let (code, loose) = http.request("GET", &format!("/gvfs/objects/{feat}"), None);
    assert_eq!(code, 200, "a miss is fetched from the upstream");
    let mut inflater = Inflater::new();
    let mut out = vec![0u8; 1 << 20];
    let (_, n) = inflater.inflate(&loose, &mut out).unwrap();
    assert!(out[..n].starts_with(b"commit "));
    let feature_only = git(&repo, &["rev-list", "--objects", "feature", "^main"])
        .lines()
        .count() as u64;
    assert_eq!(
        http.metric("gvfs_miss_fetched_objects_total "),
        feature_only,
        "exactly the branch's own objects came back (have = every tip)"
    );
    let rounds = http.metric("gvfs_miss_fetch_rounds_total ");
    // Its tree came along in that round; the unknown id costs a round that fetches nothing.
    let req = format!(
        "{{\"commitDepth\":1,\"objectIds\":[\"{feat_tree}\",\"{}\"]}}",
        "e".repeat(40)
    );
    let (code, pack) = http.request("POST", "/gvfs/objects", Some(req.as_bytes()));
    assert_eq!(code, 200);
    fs::write(dir.join("f.pack"), &pack).unwrap();
    git(&dir, &["index-pack", "f.pack"]);
    assert!(git(&dir, &["verify-pack", "-v", "f.idx"]).contains(&feat_tree));
    assert!(http.metric("gvfs_miss_fetch_rounds_total ") <= rounds + 1);
    assert_eq!(
        http.metric("gvfs_miss_fetched_objects_total "),
        feature_only
    );
    let (code, _) = http.request("GET", &format!("/gvfs/objects/{feat_tree}"), None);
    assert_eq!(code, 200, "served from the live slab now");

    // Live sync: a new commit shows up without any pipeline run, then the pipeline
    // publishes it and the server drops the live slab.
    add_commits(&repo, 1, 9);
    let new_tip = git(&repo, &["rev-parse", "HEAD"]);
    let deadline = Instant::now() + Duration::from_secs(15);
    while http.metric("gvfs_live_objects ") == 0 {
        assert!(Instant::now() < deadline, "live slab never appeared");
        std::thread::sleep(Duration::from_millis(100));
    }
    let (code, _) = http.request("GET", &format!("/gvfs/objects/{new_tip}"), None);
    assert_eq!(code, 200);
    let (_, body) = http.request(
        "GET",
        &format!("/gvfs/prefetch?lastPackTimestamp={}", m.latest_ts()),
        None,
    );
    let packs = gpre_packs(&body);
    assert_eq!(packs.len(), 1, "the live slab is served as a pack");
    verify_pack_with_git(&root, &packs[0].1, &packs[0].2, "live");
    let gen_before = http.metric("gvfs_manifest_generation ");
    pipeline(&store, &repo, Tier::M15, BaselineMode::Fetch);
    let deadline = Instant::now() + Duration::from_secs(15);
    while http.metric("gvfs_manifest_generation ") == gen_before {
        assert!(Instant::now() < deadline, "manifest reload never happened");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(http.metric("gvfs_live_objects "), 0);
    let (_, body) = http.request("GET", "/gvfs/prefetch?lastPackTimestamp=0", None);
    let packs = gpre_packs(&body);
    assert_eq!(packs.len(), 1);
    verify_pack_with_git(&root, &packs[0].1, &packs[0].2, "cold-after-publish");

    // The client hook: a cold clone into a bare repository (one pack named by its interval,
    // a multi-pack-index), incremental packs, then the budget collapses them with compact=1.
    let hook = root.join("hook.git");
    git(&root, &["init", "-q", "--bare", "hook.git"]);
    let hook_gd = format!("--git-dir={}", hook.display());
    let hook_run = |extra: &[&str]| -> String {
        let out = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"))
            .args([
                "client-prefetch",
                "--server",
                &format!("http://{}", http.addr),
                "--repo",
            ])
            .arg(&hook)
            .args(extra)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let hook_packs = || -> Vec<String> {
        let mut v: Vec<String> = fs::read_dir(hook.join("objects/pack"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".pack"))
            .collect();
        v.sort();
        v
    };
    let reload = |http: &Http, gen_before: u64| {
        let deadline = Instant::now() + Duration::from_secs(15);
        while http.metric("gvfs_manifest_generation ") == gen_before {
            assert!(Instant::now() < deadline, "manifest reload never happened");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let m = store.load_manifest().unwrap().unwrap();
    let out = hook_run(&["--midx"]);
    assert!(out.contains("1 pack(s)"), "{out}");
    assert_eq!(
        hook_packs(),
        vec![format!("prefetch-0-{}.pack", m.latest_ts())]
    );
    assert!(hook.join("objects/pack/multi-pack-index").is_file());
    git(
        &root,
        &[
            &hook_gd,
            "update-ref",
            "refs/heads/main",
            &git(&repo, &["rev-parse", "HEAD"]),
        ],
    );
    git(&root, &[&hook_gd, "fsck", "--connectivity-only"]);
    assert_eq!(
        git(&root, &[&hook_gd, "rev-list", "--objects", "--all"])
            .lines()
            .count(),
        git(&repo, &["rev-list", "--objects", "main"])
            .lines()
            .count()
    );
    for salt in [11, 12] {
        add_commits(&repo, 1, salt);
        let g = http.metric("gvfs_manifest_generation ");
        pipeline(&store, &repo, Tier::M15, BaselineMode::Fetch);
        reload(&http, g);
        hook_run(&[]);
    }
    assert_eq!(hook_packs().len(), 3, "{:?}", hook_packs());
    assert!(hook_run(&[]).contains("nothing new"));
    // The h1 rollup keeps its leaves (leaf tier h24), so a client holding them all gets
    // nothing — until it is over budget: the compact=1 superset replaces the two m15 packs.
    let g = http.metric("gvfs_manifest_generation ");
    let r = pipeline(&store, &repo, Tier::H1, BaselineMode::Fetch);
    assert_eq!(r.rollups.len(), 1);
    reload(&http, g);
    let out = hook_run(&["--budget", "2"]);
    assert!(
        out.contains("compaction:") && out.contains("2 subsumed pack(s) unlinked"),
        "{out}"
    );
    let names = hook_packs();
    let h1b = r.rollups[0];
    assert_eq!(
        names,
        vec![
            format!("prefetch-0-{}.pack", m.latest_ts()),
            format!("prefetch-{}-{}.pack", h1b.start_ts, h1b.end_ts)
        ],
        "{names:?}"
    );
    git(
        &root,
        &[
            &hook_gd,
            "update-ref",
            "refs/heads/main",
            &git(&repo, &["rev-parse", "HEAD"]),
        ],
    );
    git(&root, &[&hook_gd, "fsck", "--connectivity-only"]);
    assert_eq!(
        git(&root, &[&hook_gd, "rev-list", "--objects", "--all"])
            .lines()
            .count(),
        git(&repo, &["rev-list", "--objects", "main"])
            .lines()
            .count()
    );

    // Every endpoint ran: no heap allocation inside a hot-path region.
    assert_eq!(
        http.metric("gvfs_alloc_hot_path_total "),
        0,
        "hot paths must not allocate"
    );

    // Weekly tier: the pyramid becomes a weekly delta, the baseline is spliced.
    let r = pipeline(&store, &repo, Tier::Weekly, BaselineMode::Splice);
    assert!(r.weekly.is_some() && r.baseline.is_some());
    let m = store.load_manifest().unwrap().unwrap();
    assert!(m.pyramid.is_empty());
    assert_eq!(m.weekly.len(), 1);
    let b = m.baseline.unwrap();
    let p = store.slab_paths(b.seq);
    verify_pack_with_git(
        &root,
        &fs::read(&p.pack).unwrap(),
        &fs::read(&p.idx).unwrap(),
        "spliced-baseline",
    );
    assert_eq!(
        b.object_count as usize,
        git(&repo, &["rev-list", "--objects", "main"])
            .lines()
            .count()
    );
}
