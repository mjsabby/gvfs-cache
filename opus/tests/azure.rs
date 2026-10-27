//! The Azure Blob store: conditional writes, block uploads, ranged parallel downloads,
//! listing, and the whole pipeline + serve flow on it.
//!
//! Runs against the Azurite emulator (Shared Key auth, path-style endpoint) when
//! `azurite-blob` is on PATH (`npm install -g azurite`) or `GVFS_TEST_AZURITE` names it, or
//! against a real account when `GVFS_TEST_AZURE_CS_FILE` (a file holding a connection
//! string) and `GVFS_TEST_AZURE_CONTAINER` (an existing container) are set; each test then
//! works under its own unique prefix and deletes it afterwards. Skipped otherwise.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

mod common;

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime};

use common::*;
use gvfs_daemon::azure::AzureStore;
use gvfs_daemon::http::{Client, Url};
use gvfs_daemon::store::{Cond, Store};

const ACCOUNT: &str = "devstoreaccount1";
/// Azurite's published development key.
const KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

struct Azurite {
    child: Child,
    endpoint: String,
}

impl Azurite {
    fn start() -> Option<Azurite> {
        let on_path = || {
            std::env::split_paths(&std::env::var_os("PATH")?)
                .map(|d| d.join("azurite-blob"))
                .find(|p| p.exists())
        };
        let Some(bin) = std::env::var_os("GVFS_TEST_AZURITE")
            .map(PathBuf::from)
            .or_else(on_path)
            .filter(|p| p.exists())
        else {
            eprintln!(
                "skipping: Azurite not found (npm install -g azurite, or set GVFS_TEST_AZURITE to azurite-blob)"
            );
            return None;
        };
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = Command::new(bin)
            .args([
                "--blobHost",
                "127.0.0.1",
                "--blobPort",
                &port.to_string(),
                "--silent",
                "--skipApiVersionCheck",
                "--inMemoryPersistence",
            ])
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut a = Azurite {
            child,
            endpoint: format!("http://127.0.0.1:{port}/{ACCOUNT}"),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "Azurite did not start");
            if let Ok(Some(status)) = a.child.try_wait() {
                panic!("Azurite exited: {status}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Some(a)
    }

    fn connection_string(&self) -> String {
        format!(
            "DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};BlobEndpoint={};",
            self.endpoint
        )
    }

    /// Creates a container (provisioning is not the daemon's job).
    fn create_container(&self, name: &str) {
        let key = gvfs_daemon::b64::decode(KEY).unwrap();
        let headers = [
            (
                "x-ms-date",
                gvfs_daemon::azure::http_date(SystemTime::now()),
            ),
            ("x-ms-version", "2021-08-06".to_string()),
        ];
        let query = [("restype", "container".to_string())];
        let sts = gvfs_daemon::azure::string_to_sign(
            ACCOUNT,
            "PUT",
            &format!("/{ACCOUNT}/{name}"),
            &query,
            &headers,
            0,
        );
        let sig = gvfs_daemon::b64::encode(
            ring::hmac::sign(
                &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &key),
                sts.as_bytes(),
            )
            .as_ref(),
        );
        let auth = format!("SharedKey {ACCOUNT}:{sig}");
        let mut hdrs: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
        hdrs.push(("Authorization", &auth));
        let mut c = Client::new(Url::parse(&self.endpoint).unwrap(), Duration::from_secs(30));
        let r = c
            .request("PUT", &format!("/{name}?restype=container"), &hdrs, b"")
            .unwrap();
        assert!(
            r.status == 201 || r.status == 409,
            "create container: HTTP {}",
            r.status
        );
    }
}

/// Where the tests run: Azurite, or a real account.
struct Backend {
    _azurite: Option<Azurite>,
    cs: String,
    /// `<container>/<unique prefix>`.
    spec: String,
}

impl Backend {
    fn new(test: &str) -> Option<Backend> {
        let unique = format!(
            "gvd-{test}-{}-{}",
            std::process::id(),
            gvfs_daemon::util::now_secs()
        );
        if let (Some(file), Ok(container)) = (
            std::env::var_os("GVFS_TEST_AZURE_CS_FILE"),
            std::env::var("GVFS_TEST_AZURE_CONTAINER"),
        ) {
            let cs = std::fs::read_to_string(file).unwrap().trim().to_string();
            return Some(Backend {
                _azurite: None,
                cs,
                spec: format!("{container}/{unique}"),
            });
        }
        let az = Azurite::start()?;
        az.create_container("gvfs");
        let cs = az.connection_string();
        Some(Backend {
            _azurite: Some(az),
            cs,
            spec: format!("gvfs/{unique}"),
        })
    }

    fn store(&self, sub: &str) -> Store {
        Store::Azure(AzureStore::new(&self.cs, &format!("{}{sub}", self.spec)).unwrap())
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        let store = self.store("");
        for o in store.list("").unwrap_or_default() {
            let _ = store.delete(&o.key);
        }
    }
}

impl Drop for Azurite {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn azure_store_operations() {
    let Some(backend) = Backend::new("ops") else {
        return;
    };
    let root = tempdir("azure-ops");
    let store = backend.store("/repo1");

    // The manifest's compare-and-swap.
    assert!(store.get("manifest.bin").unwrap().is_none());
    let e1 = store
        .put("manifest.bin", b"one", Cond::IfNoneMatch)
        .unwrap();
    assert!(
        store
            .put("manifest.bin", b"lost race", Cond::IfNoneMatch)
            .unwrap_err()
            .is_conflict()
    );
    let e2 = store
        .put("manifest.bin", b"two", Cond::IfMatch(e1.clone()))
        .unwrap();
    assert!(
        store
            .put("manifest.bin", b"stale", Cond::IfMatch(e1))
            .unwrap_err()
            .is_conflict()
    );
    assert_eq!(
        store.get("manifest.bin").unwrap().unwrap(),
        (b"two".to_vec(), e2)
    );

    // A slab-sized file: 16 MiB blocks uploaded in parallel, downloaded as parallel ranges.
    let big: Vec<u8> = (0..(70u32 << 20) + 12345)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let src = root.join("big.pack");
    std::fs::write(&src, &big).unwrap();
    store.upload_file("slabs/big.pack", &src).unwrap();
    std::fs::write(root.join("small.mphf"), b"small").unwrap();
    store
        .upload_file("slabs/small.mphf", &root.join("small.mphf"))
        .unwrap();
    let dst = root.join("down/big.pack");
    store.download_file("slabs/big.pack", &dst).unwrap();
    assert!(
        std::fs::read(&dst).unwrap() == big,
        "downloaded bytes differ"
    );
    store
        .download_file("slabs/small.mphf", &root.join("down/small.mphf"))
        .unwrap();
    assert_eq!(
        std::fs::read(root.join("down/small.mphf")).unwrap(),
        b"small"
    );

    let now = gvfs_daemon::util::now_secs();
    let mut listed = store.list("slabs/").unwrap();
    listed.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(
        listed
            .iter()
            .map(|o| (o.key.as_str(), o.size))
            .collect::<Vec<_>>(),
        vec![
            ("slabs/big.pack", big.len() as u64),
            ("slabs/small.mphf", 5)
        ]
    );
    assert!(
        listed.iter().all(|o| o.modified.abs_diff(now) < 300),
        "modification times"
    );
    // The prefix isolates repositories in one container.
    assert!(backend.store("/repo2").list("slabs/").unwrap().is_empty());

    // Streaming uploads: blocks staged as they fill, the object appears only when finished.
    for (key, len) in [
        ("blobs/stream.pack", (40usize << 20) + 777),
        ("blobs/exact.pack", 32 << 20),
        ("blobs/small.pack", 1000),
        ("blobs/empty.pack", 0),
    ] {
        let mut w = store.writer(key).unwrap();
        for piece in big[..len].chunks(3 << 20) {
            std::io::Write::write_all(&mut w, piece).unwrap();
        }
        assert!(
            store.get(key).unwrap().is_none(),
            "{key} visible before finish"
        );
        w.finish().unwrap();
        if len > 100 {
            let mut got = Vec::new();
            store
                .read_range(key, 17, (len - 40) as u64, &mut |b| {
                    got.extend_from_slice(b);
                    Ok(())
                })
                .unwrap();
            assert!(got == big[17..len - 23], "{key}: ranged read differs");
        }
        if len == 0 {
            assert_eq!(store.get(key).unwrap().unwrap().0, b"");
        } else {
            let down = root.join(format!("down/{}", key.replace('/', "_")));
            store.download_file(key, &down).unwrap();
            assert!(std::fs::read(&down).unwrap() == big[..len], "{key} differs");
        }
        store.delete(key).unwrap();
    }
    let mut w = store.writer("blobs/abandoned.pack").unwrap();
    std::io::Write::write_all(&mut w, &big[..20 << 20]).unwrap();
    drop(w);
    assert!(store.get("blobs/abandoned.pack").unwrap().is_none());

    store.delete("slabs/big.pack").unwrap();
    store.delete("slabs/big.pack").unwrap();
    assert_eq!(store.list("slabs/").unwrap().len(), 1);
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
}

/// Pipeline (genesis, 15m, repacked 1h, weekly with GC) and an edge, all on Azure.
#[test]
fn azure_pipeline_and_serve() {
    let Some(backend) = Backend::new("e2e") else {
        return;
    };
    let root = tempdir("azure-e2e");
    let env = [("GVD_AZ_CS", backend.cs.as_str())];
    let store = format!("azure:{}", backend.spec);
    let store = store.as_str();
    let mut up = Upstream::new(&root);
    up.commit(5);
    let upstream = up.bare.to_str().unwrap().to_string();
    let run = |tier: &str, now: u64, extra: &[&str]| {
        let mut args = vec!["--store-auth-env", "GVD_AZ_CS"];
        let now = now.to_string();
        args.extend(["--now", now.as_str()]);
        args.extend(extra);
        pipeline_at(&root, store, &upstream, tier, &args, &env)
            .unwrap_or_else(|log| panic!("pipeline {tier}: {log}"))
    };
    let t0 = gvfs_daemon::util::now_secs();
    run("15m", t0, &[]);
    up.commit(5);
    run("15m", t0 + 900, &[]);
    up.commit(5);
    let log = run("1h", t0 + 1800, &[]);
    assert!(log.contains("repack: 2 slabs"), "{log}");

    // What an earlier run left: a slab the store has since dropped, and a download that
    // died with its process (no process has that id).
    let cache = root.join("cache");
    std::fs::create_dir_all(&cache).unwrap();
    for stale in ["gone.pack", "gone.mphf", ".old.pack.tmp-999999999-0"] {
        std::fs::write(cache.join(stale), b"stale").unwrap();
    }
    let server = Server::start_with_env(
        &[
            "--store",
            store,
            "--store-auth-env",
            "GVD_AZ_CS",
            "--poll-ms",
            "100",
            "--cache-dir",
            cache.to_str().unwrap(),
        ],
        &env,
    );
    // The edge's cache holds the files of the slabs its manifest lists, and nothing else.
    let cached = || {
        let mut v: Vec<String> = std::fs::read_dir(&cache)
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.file_type().unwrap().is_file())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    };
    let listed = || {
        let (bytes, _) = backend.store("").get("manifest.bin").unwrap().unwrap();
        let m = gvfs_daemon::manifest::Manifest::decode(&bytes).unwrap();
        let mut v: Vec<String> = m
            .slabs()
            .flat_map(|x| [format!("{}.pack", x.id), format!("{}.mphf", x.id)])
            .collect();
        v.sort();
        v
    };
    assert_eq!(cached(), listed());
    let packs = server.prefetch(0);
    assert_eq!(packs.len(), 1);
    let client = fresh_repo(&root, "client");
    install_pack(&client, &packs[0].1);
    assert_complete(&client, &up.tip());

    up.commit(5);
    run("168h", t0 + 2700, &["--gc-grace-secs", "0"]);
    server.wait_for("the weekly manifest", |s| {
        s.metric("gvfs_manifest_generation") == 4
    });
    let packs = server.prefetch(packs[0].0);
    assert_eq!(packs.len(), 1);
    install_pack(&client, &packs[0].1);
    assert_complete(&client, &up.tip());
    let packs = server.prefetch(0);
    assert_eq!(packs.len(), 1);
    let cold = fresh_repo(&root, "cold");
    install_pack(&cold, &packs[0].1);
    assert_complete(&cold, &up.tip());
    server.assert_clean();
    assert!(
        !cache.join("blob-blocks").exists(),
        "an inline store's edge keeps no blob blocks"
    );
    // The weekly run replaced every slab: the old ones' files went with them.
    assert_eq!(cached(), listed());

    // GC removed the slabs the rollups and the weekly rebuild replaced.
    let s = backend.store("");
    let (bytes, _) = s.get("manifest.bin").unwrap().unwrap();
    let m = gvfs_daemon::manifest::Manifest::decode(&bytes).unwrap();
    let mut keys: Vec<String> = s
        .list("slabs/")
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    keys.sort();
    let mut referenced: Vec<String> = m
        .slabs()
        .flat_map(|x| [x.pack_key(), x.index_key()])
        .collect();
    referenced.sort();
    assert_eq!(keys, referenced);
    drop(server);
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
}

/// A `--blobs separate` store on Azure: the large blob streamed into a block blob by the
/// mirror, and an edge that reads blob packs by range where they are (never downloading
/// one), every byte checked by git.
#[test]
fn azure_separate_store() {
    let Some(backend) = Backend::new("sep") else {
        return;
    };
    let root = tempdir("azure-sep");
    let env = [("GVD_AZ_CS", backend.cs.as_str())];
    let store = format!("azure:{}", backend.spec);
    let up = Upstream::new(&root);
    let upstream = up.bare.to_str().unwrap().to_string();
    let noise_bytes = noise(2 << 20, 11);
    // Stored (incompressible) and in a pack of its own, six blocks of the edge's cache.
    let huge = noise(20 << 20, 5);
    commit_files(
        &up,
        "c1",
        &[
            ("a/text.txt", Some(text(1))),
            ("b/noise.bin", Some(noise_bytes.clone())),
            ("big/large.bin", Some(large((16 << 20) + 9))),
            ("big/noise.bin", Some(huge.clone())),
        ],
    );
    let g = commit_files(&up, "c2", &[("a/text.txt", Some(text(2)))]);
    let run = |extra: &[&str]| {
        let mut args = vec!["--store-auth-env", "GVD_AZ_CS"];
        args.extend(extra);
        pipeline_at(&root, &store, &upstream, "15m", &args, &env)
            .unwrap_or_else(|log| panic!("pipeline: {log}"))
    };
    run(&["--blobs", "separate"]);
    let work = root.join("mirror-work");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"));
    cmd.args(["mirror", "--store", &store, "--store-auth-env", "GVD_AZ_CS"])
        .args([
            "--upstream",
            &upstream,
            "--work-dir",
            work.to_str().unwrap(),
        ])
        .args(["--publish-secs", "0", "--blob-pack-mib", "1", "--jobs", "2"])
        .envs(env.iter().copied());
    gvfs_daemon::upstream::hermetic_git_env(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let t = commit_files(
        &up,
        "c3",
        &[
            ("n/new.txt", Some(b"new\n".to_vec())),
            ("a/text.txt", Some(text(3))),
        ],
    );
    run(&[]);
    // The runner's cache holds only what the store lists.
    let s = backend.store("");
    let (bytes, _) = s.get("manifest.bin").unwrap().unwrap();
    let m = gvfs_daemon::manifest::Manifest::decode(&bytes).unwrap();
    let (bm, _) = gvfs_daemon::blobcopy::load(&s).unwrap();
    let listed: std::collections::HashSet<String> = m
        .slabs()
        .flat_map(|x| {
            [
                format!("{}.pack", x.id),
                format!("{}.mphf", x.id),
                x.sizes_key().replace('/', "_"),
            ]
        })
        .chain(bm.segments.iter().map(|g| format!("{}.bidx", g.id)))
        .collect();
    let cached: Vec<String> = std::fs::read_dir(root.join("pipeline-work/gvfs-daemon-store-cache"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !cached.is_empty() && !cached.iter().any(|f| f.ends_with(".pack")),
        "a window reads sizes tables and segment indexes, never a slab: {cached:?}"
    );
    assert!(cached.iter().all(|f| listed.contains(f)), "{cached:?}");

    // A segment index blobs.bin no longer lists, and a dead download of one.
    let cache = root.join("cache");
    std::fs::create_dir_all(cache.join("blobs")).unwrap();
    for stale in ["gone.bidx", ".x.bidx.tmp-999999999-0"] {
        std::fs::write(cache.join("blobs").join(stale), b"stale").unwrap();
    }
    let server = Server::start_with_env(
        &[
            "--store",
            &store,
            "--store-auth-env",
            "GVD_AZ_CS",
            "--upstream",
            &upstream,
            "--poll-ms",
            "100",
            "--cache-dir",
            cache.to_str().unwrap(),
        ],
        &env,
    );
    // Every blob of history as a loose object, old versions too: read by range from the
    // store, none through the proxy.
    let history = git_blobs(&up.bare, &["--all"]);
    let ids: Vec<String> = history.keys().cloned().collect();
    let loose = fresh_repo(&root, "loose");
    for id in &ids {
        let (status, ct, body) = server.get(&format!("/gvfs/objects/{id}"));
        assert_eq!((status, ct.as_str()), (200, CT_LOOSE), "{id}");
        let dir = loose.join("objects").join(&id[..2]);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(&id[2..]), body).unwrap();
    }
    git(&loose, &["fsck", "--strict", "--no-dangling"]);
    assert_eq!(cat_batch(&loose, &ids), cat_batch(&up.bare, &ids));
    assert_eq!(
        server.metric("gvfs_blobs_from_store_total"),
        ids.len() as u64
    );
    assert_eq!(server.metric("gvfs_blobs_proxied_total"), 0);
    assert_eq!(server.metric("gvfs_blobs_missing_total"), 0);
    // Read again: from the edge's block cache, without asking Azure.
    let fetches = server.metric("gvfs_blob_cache_fetches_total");
    assert!(fetches > 0);
    for id in &ids {
        assert_eq!(server.get(&format!("/gvfs/objects/{id}")).0, 200);
    }
    assert_eq!(server.metric("gvfs_blob_cache_fetches_total"), fetches);
    // One pack of every blob.
    let (status, _, pack) = server.post("/gvfs/objects", json_ids(&ids, None).as_bytes(), CT_PACK);
    assert_eq!(status, 200);
    let posted = fresh_repo(&root, "post");
    git_stdin(
        &posted,
        &["index-pack", "--stdin", "--strict", "--keep=t"],
        &pack,
    );
    assert_eq!(cat_batch(&posted, &ids), cat_batch(&up.bare, &ids));
    assert_eq!(server.metric("gvfs_blob_cache_fetches_total"), fetches);
    // Ranges of a stored and of a compressed blob.
    let noise_id = rev(&up.bare, &format!("{g}:b/noise.bin"));
    let text_id = rev(&up.bare, &format!("{t}:a/text.txt"));
    for (id, content) in [(noise_id, noise_bytes), (text_id, text(3))] {
        let n = content.len();
        for (off, len) in [(0usize, 7usize), (65_533, 9), (n - 2, 2), (3, n - 6)] {
            let (status, _, body) =
                server.get(&format!("/gvfs/blobs/{id}?offset={off}&length={len}"));
            assert_eq!(status, 200);
            assert!(body == content[off..off + len], "{id} {off}+{len}");
        }
    }
    // Blob packs stay in the store: the edge holds their indexes only, those blobs.bin
    // lists.
    let mut local: Vec<String> = std::fs::read_dir(cache.join("blobs"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    local.sort();
    let mut segments: Vec<String> = bm
        .segments
        .iter()
        .map(|g| format!("{}.bidx", g.id))
        .collect();
    segments.sort();
    assert_eq!(local, segments);
    server.assert_clean();
    drop(server);

    // A long read of a remote pack goes through the block cache a window (8 blocks) per
    // fetch, or past the cache when longer than a quarter of it (they went a MiB at a time:
    // a fetch per block, and never past the cache).
    let huge_id = rev(&up.bare, &format!("{g}:big/noise.bin"));
    let fresh_edge = |tag: &str, extra: &[&str]| -> Server {
        let dir = root.join(format!("cache-{tag}"));
        let dir = dir.to_str().unwrap();
        let mut args = vec!["--store", &store, "--store-auth-env", "GVD_AZ_CS"];
        args.extend(["--upstream", &upstream, "--cache-dir", dir]);
        args.extend(extra);
        Server::start_with_env(&args, &env)
    };
    let counts = |s: &Server| {
        (
            s.metric("gvfs_blob_cache_fetches_total"),
            s.metric("gvfs_blob_cache_bypass_total"),
        )
    };
    let huge_ids = std::slice::from_ref(&huge_id);
    let edge = fresh_edge("a", &[]);
    let (status, ct, _) = edge.get(&format!("/gvfs/objects/{huge_id}"));
    assert_eq!((status, ct.as_str()), (200, CT_LOOSE));
    assert_eq!(
        counts(&edge),
        (2, 0),
        "the zlib header's block, then the other five in one fetch"
    );
    // As a pack of its own (the pack file whole): from the blocks the loose read cached.
    let (status, _, pack) = edge.post(
        "/gvfs/objects",
        json_ids(huge_ids, None).as_bytes(),
        CT_PACK,
    );
    assert_eq!(status, 200);
    let one = fresh_repo(&root, "huge");
    git_stdin(
        &one,
        &["index-pack", "--stdin", "--strict", "--keep=t"],
        &pack,
    );
    assert_eq!(cat_batch(&one, huge_ids), cat_batch(&up.bare, huge_ids));
    assert_eq!(counts(&edge), (2, 0));
    edge.assert_clean();
    drop(edge);
    let edge = fresh_edge("b", &["--blob-cache-mib", "64"]);
    assert_eq!(edge.get(&format!("/gvfs/objects/{huge_id}")).0, 200);
    assert_eq!(counts(&edge), (2, 1), "20 MiB is more than a quarter of 64");
    assert_eq!(
        edge.metric("gvfs_blob_cache_bytes"),
        4 << 20,
        "the zlib header's block only"
    );
    edge.assert_clean();
    drop(edge);
    // A range of it: one read of the raw range, the stored blocks' headers dropped.
    let edge = fresh_edge("c", &[]);
    let (off, len) = (1000usize, 12usize << 20);
    let (status, _, body) = edge.get(&format!("/gvfs/blobs/{huge_id}?offset={off}&length={len}"));
    assert_eq!(status, 200);
    assert!(body == huge[off..off + len]);
    assert_eq!(counts(&edge), (1, 0), "one fetch of four blocks");
    edge.assert_clean();
    drop(edge);
    let _ = std::fs::remove_dir_all(&root);
}

/// An edge of an object store keeps gigabytes in its cache directory: one is required (the
/// system's temp directory, the default for a directory store, is memory on many hosts).
#[test]
fn an_object_store_edge_needs_a_cache_dir() {
    let cs = "DefaultEndpointsProtocol=http;AccountName=a;AccountKey=AAAA;\
              BlobEndpoint=http://127.0.0.1:1/a";
    let out = Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"))
        .args([
            "serve",
            "--store",
            "azure:gvfs/x",
            "--store-auth-env",
            "GVD_NO_CS",
        ])
        .env("GVD_NO_CS", cs)
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && err.contains("--cache-dir"),
        "{err}"
    );
}
