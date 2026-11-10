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

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime};

use common::*;
use gvfs_cache::azure::AzureStore;
use gvfs_cache::http::{Client, Url};
use gvfs_cache::store::{Cond, Store};

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
        // Any free port, as Azurite reports it: a port picked here and released could be
        // taken by another test's server before Azurite binds it.
        let mut child = Command::new(bin)
            .args([
                "--blobHost",
                "127.0.0.1",
                "--blobPort",
                "0",
                "--silent",
                "--skipApiVersionCheck",
                "--inMemoryPersistence",
            ])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufRead::lines(std::io::BufReader::new(out)) {
                let Ok(line) = line else { break };
                let port = line
                    .split_once("listens on http://127.0.0.1:")
                    .and_then(|(_, p)| p.trim().parse::<u16>().ok());
                if let Some(port) = port {
                    let _ = tx.send(port);
                }
            }
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        let port = loop {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(port) => break port,
                // Its output closed: it is exiting.
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("Azurite exited: {status}");
            }
            assert!(Instant::now() < deadline, "Azurite did not start");
        };
        Some(Azurite {
            child,
            endpoint: format!("http://127.0.0.1:{port}/{ACCOUNT}"),
        })
    }

    fn connection_string(&self) -> String {
        format!(
            "DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};BlobEndpoint={};",
            self.endpoint
        )
    }

    /// Creates a container (provisioning is not the daemon's job).
    fn create_container(&self, name: &str) {
        let key = gvfs_cache::b64::decode(KEY).unwrap();
        let headers = [
            ("x-ms-date", gvfs_cache::azure::http_date(SystemTime::now())),
            ("x-ms-version", "2021-08-06".to_string()),
        ];
        let query = [("restype", "container".to_string())];
        let sts = gvfs_cache::azure::string_to_sign(
            ACCOUNT,
            "PUT",
            &format!("/{ACCOUNT}/{name}"),
            &query,
            &headers,
            0,
        );
        let sig = gvfs_cache::b64::encode(
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
            gvfs_cache::util::now_secs()
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

    let now = gvfs_cache::util::now_secs();
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

    // An object uploaded again is another object: the listing gives it another version,
    // and a delete that names the version listed before leaves it (garbage collection
    // lists, then deletes; a run may upload the same slab in between).
    let version = |key: &str| {
        let listed = store.list(key).unwrap();
        assert_eq!(listed.len(), 1, "{key}");
        assert!(!listed[0].version.is_empty());
        listed[0].version.clone()
    };
    let twice = "slabs/twice.mphf";
    store.upload_file(twice, &root.join("small.mphf")).unwrap();
    let first = version(twice);
    store.upload_file(twice, &root.join("small.mphf")).unwrap();
    let second = version(twice);
    assert_ne!(first, second);
    assert!(!store.delete_if(twice, &first).unwrap());
    assert_eq!(store.get(twice).unwrap().unwrap().0, b"small");
    assert!(store.delete_if(twice, &second).unwrap());
    assert!(store.get(twice).unwrap().is_none());
    // Gone: no error (Azure answers 404, Azurite that the condition failed).
    store.delete_if(twice, &second).unwrap();
    // So is a blob whose block list is committed again.
    let blocks = "blobs/twice.pack";
    let mut versions = Vec::new();
    for _ in 0..2 {
        let mut w = store.writer(blocks).unwrap();
        std::io::Write::write_all(&mut w, &big[..(16 << 20) + 1]).unwrap();
        w.finish().unwrap();
        versions.push(version(blocks));
    }
    assert_ne!(versions[0], versions[1]);
    assert!(!store.delete_if(blocks, &versions[0]).unwrap());
    assert!(store.delete_if(blocks, &versions[1]).unwrap());
    assert!(store.list("blobs/").unwrap().is_empty());

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
    let t0 = gvfs_cache::util::now_secs();
    run("15m", t0, &[]);
    up.commit(5);
    run("15m", t0 + 900, &[]);
    up.commit(5);
    let log = run("1h", t0 + 1800, &[]);
    assert!(log.contains("repack: 2 slabs"), "{log}");

    // What an earlier run left: a slab the store has since dropped, and a download that
    // died with its process (no process has that id). And a file that is not the edge's.
    let cache = root.join("cache");
    std::fs::create_dir_all(&cache).unwrap();
    for stale in [
        "m15-1-0123456789abcdef.pack",
        "m15-1-0123456789abcdef.mphf",
        ".old.pack.tmp-999999999-0",
    ] {
        std::fs::write(cache.join(stale), b"stale").unwrap();
    }
    let other = cache.join("another-program.pack");
    std::fs::write(&other, b"not the edge's").unwrap();
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
    // The edge's cache holds the files of the slabs its manifest lists and none of an
    // earlier run's; what was not the edge's is still there.
    let cached = || {
        let mut v: Vec<String> = std::fs::read_dir(&cache)
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| e.file_type().unwrap().is_file())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name != "another-program.pack")
            .collect();
        v.sort();
        v
    };
    assert!(other.exists());
    let manifest_of = |backend: &Backend| {
        let (bytes, _) = backend.store("").get("manifest.bin").unwrap().unwrap();
        gvfs_cache::manifest::Manifest::decode(&bytes).unwrap()
    };
    let listed = || {
        let m = manifest_of(&backend);
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

    let s = backend.store("");
    let published = || {
        let (bytes, _) = s.get("manifest.bin").unwrap().unwrap();
        gvfs_cache::manifest::Manifest::decode(&bytes).unwrap()
    };
    let slab_files = |m: &gvfs_cache::manifest::Manifest| {
        let mut files: Vec<String> = m
            .slabs()
            .flat_map(|x| [x.pack_key(), x.index_key()])
            .collect();
        files.sort();
        files
    };
    let in_store = || {
        let mut keys: Vec<String> = s
            .list("slabs/")
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        keys.sort();
        keys
    };
    up.commit(5);
    let before = published();
    // A day later by the runs' clock: what the weekly run drops was written long before.
    let weekly_at = t0 + 86_400;
    run("168h", weekly_at, &[]);
    server.wait_for("the weekly manifest", |s| {
        s.metric("gvfs_manifest_generation") == 4
    });
    // The weekly run published the new baseline's tail on the old one beside it, and the
    // edge, which had the old one, made the new pack of the two: it did not download it
    // (every edge downloaded every weekly baseline whole).
    let after = manifest_of(&backend);
    assert_ne!(after.baseline.id, before.baseline.id);
    let tail = backend
        .store("")
        .get(&after.baseline.tail_key())
        .unwrap()
        .unwrap()
        .0;
    assert!(
        (tail.len() as u64) < after.baseline.pack_len - before.baseline.pack_len + 200,
        "a tail of {} bytes",
        tail.len()
    );
    assert_eq!(server.metric("gvfs_baselines_grown_total"), 1);
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
    // The weekly run replaced every slab: the old ones' files went with them, and only
    // they.
    assert_eq!(cached(), listed());
    assert!(other.exists());

    // The baseline the weekly run replaced is in the store still, noted in `gc.bin`: an
    // edge that reads the manifest before finds every file it lists. (It went with the
    // run that replaced it.)
    let m = published();
    let dropped = [before.baseline.index_key(), before.baseline.pack_key()];
    for file in slab_files(&before) {
        assert!(in_store().contains(&file), "{file} went with its manifest");
    }
    let noted = |s: &Store| -> Vec<(String, u64)> {
        let entries = gvfs_cache::gc::load(s).unwrap();
        entries.into_iter().map(|e| (e.key, e.since)).collect()
    };
    assert_eq!(noted(&s), dropped.clone().map(|key| (key, weekly_at)));
    // What the manifest lists: its slabs' files, and the new baseline's tail.
    let mut listed_files = slab_files(&m);
    listed_files.push(m.baseline.tail_key());
    listed_files.sort();
    // A grace period later a collection finds both files due. The index is uploaded
    // again between its listing and its deletes (a run that makes the same slab): the
    // service refuses that delete, and the index stays.
    let pass = gvfs_cache::gc::Pass::survey(&s, &m, weekly_at + 7200, 7200).unwrap();
    assert_eq!(pass.due().collect::<Vec<_>>(), dropped);
    let again = root.join("again.mphf");
    s.download_file(&dropped[0], &again).unwrap();
    s.upload_file(&dropped[0], &again).unwrap();
    let swept = pass.sweep(&s).unwrap();
    assert_eq!((swept.noted, swept.deleted, swept.replaced), (0, 1, 1));
    let mut left = listed_files.clone();
    left.push(dropped[0].clone());
    left.sort();
    assert_eq!(in_store(), left);
    // The next run notes the index anew, and a run a grace period after that one
    // deletes it: the store holds what the manifest lists.
    run("15m", weekly_at + 7200, &[]);
    assert_eq!(noted(&s), [(dropped[0].clone(), weekly_at + 7200)]);
    run("15m", weekly_at + 2 * 7200 - 1, &[]);
    assert_eq!(in_store(), left);
    run("15m", weekly_at + 2 * 7200, &[]);
    assert_eq!(in_store(), listed_files);
    assert_eq!(noted(&s), []);
    assert_eq!(published(), m);
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
            ("big/noise.bin", Some(huge)),
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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_gvfs-cache"));
    cmd.args(["mirror", "--store", &store, "--store-auth-env", "GVD_AZ_CS"])
        .args([
            "--upstream",
            &upstream,
            "--work-dir",
            work.to_str().unwrap(),
        ])
        .args(["--publish-secs", "0", "--blob-pack-mib", "1", "--jobs", "2"])
        .envs(env.iter().copied());
    gvfs_cache::upstream::hermetic_git_env(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    commit_files(
        &up,
        "c3",
        &[
            ("n/new.txt", Some(b"new\n".to_vec())),
            ("a/text.txt", Some(text(3))),
        ],
    );
    // In the runner's cache from before: a download of a table the store has dropped
    // since, and a file that is not a download.
    let runner_cache = root.join("pipeline-work/gvfs-cache-store-cache");
    std::fs::create_dir_all(&runner_cache).unwrap();
    for old in ["slabs_m15-1-0123456789abcdef.sizes", "notes.txt"] {
        std::fs::write(runner_cache.join(old), b"old").unwrap();
    }
    run(&[]);
    // The runner's cache holds only what the store lists, and what is not its own.
    let s = backend.store("");
    let (bytes, _) = s.get("manifest.bin").unwrap().unwrap();
    let m = gvfs_cache::manifest::Manifest::decode(&bytes).unwrap();
    let (bm, _) = gvfs_cache::blobcopy::load(&s).unwrap();
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
    assert!(runner_cache.join("notes.txt").exists());
    let cached: Vec<String> = std::fs::read_dir(&runner_cache)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|f| f != "notes.txt")
        .collect();
    assert!(
        !cached.is_empty() && !cached.iter().any(|f| f.ends_with(".pack")),
        "a window reads sizes tables and segment indexes, never a slab: {cached:?}"
    );
    assert!(cached.iter().all(|f| listed.contains(f)), "{cached:?}");

    // A segment index blobs.bin no longer lists, and a dead download of one.
    let cache = root.join("cache");
    std::fs::create_dir_all(cache.join("blobs")).unwrap();
    let gone = format!("{}.bidx", "e".repeat(40));
    for stale in [gone.as_str(), ".x.bidx.tmp-999999999-0"] {
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
    let hits = server.metric("gvfs_blob_cache_hits_total");
    for id in &ids {
        assert_eq!(server.get(&format!("/gvfs/objects/{id}")).0, 200);
    }
    assert_eq!(server.metric("gvfs_blob_cache_fetches_total"), fetches);
    let again = server.metric("gvfs_blob_cache_hits_total");
    assert!(again >= hits + ids.len() as u64, "{hits} then {again}");
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
    let hits = server.metric("gvfs_blob_cache_hits_total");
    assert!(hits >= again + ids.len() as u64, "{again} then {hits}");
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
        (1, 0),
        "the entry's six blocks in one fetch, its zlib header with them"
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
    assert_eq!(counts(&edge), (1, 0));
    edge.assert_clean();
    drop(edge);
    let edge = fresh_edge("b", &["--blob-cache-mib", "64"]);
    assert_eq!(edge.get(&format!("/gvfs/objects/{huge_id}")).0, 200);
    assert_eq!(counts(&edge), (1, 1), "20 MiB is more than a quarter of 64");
    assert_eq!(
        edge.metric("gvfs_blob_cache_bytes"),
        0,
        "none of it is kept, nor a block for its zlib header"
    );
    edge.assert_clean();
    drop(edge);
    let _ = std::fs::remove_dir_all(&root);
}

/// Runs `gvfs-cache mirror` to its end: every blob of the upstream's history in one pack.
fn mirror(root: &std::path::Path, store: &str, upstream: &str, env: &[(&str, &str)]) {
    let work = root.join("mirror-work");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_gvfs-cache"));
    cmd.args(["mirror", "--store", store, "--store-auth-env", "GVD_AZ_CS"])
        .args(["--upstream", upstream, "--work-dir", work.to_str().unwrap()])
        .args(["--publish-secs", "0", "--jobs", "1"])
        .envs(env.iter().copied());
    gvfs_cache::upstream::hermetic_git_env(&mut cmd);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The files of the edge's block cache that process `pid` holds open, and those it has
/// mapped.
fn open_and_mapped_blocks(pid: u32) -> (usize, usize) {
    let descriptors = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|target| target.to_string_lossy().contains("/blob-blocks/"))
        .count();
    let mappings = std::fs::read_to_string(format!("/proc/{pid}/maps"))
        .unwrap()
        .lines()
        .filter(|l| l.contains("/blob-blocks/"))
        .count();
    (descriptors, mappings)
}

/// Blobs of a pack in object storage that the edge has read before come from the blocks
/// its cache keeps open and mapped, as a local pack's do: the store is not asked, and
/// nothing is allocated. (A read of cached blocks was a cold section of its own, for
/// every blob: a boxed future, a zeroed buffer, and the block's file opened by a name
/// formatted for it, read and closed. Four POSTs of these 623 blobs made 17,481
/// allocations, seven a blob; they make 21, for their connections.) A cached block is a
/// file, a descriptor and a mapping, and the cache keeps as many as it has room for.
#[test]
fn cached_blocks_are_read_as_a_local_pack_is() {
    let Some(backend) = Backend::new("hits") else {
        return;
    };
    let root = tempdir("azure-hits");
    let env = [("GVD_AZ_CS", backend.cs.as_str())];
    let store = format!("azure:{}", backend.spec);
    let up = Upstream::new(&root);
    let upstream = up.bare.to_str().unwrap().to_string();
    // Blobs that are copied into a response (600 of some hundred bytes, half of them
    // before the others in path order, which is the pack's, and half after) and blobs
    // that leave with sendfile (20 of 40 KiB, 3 of 1.5 MiB): one pack of 6 MiB, two
    // blocks of the edge's cache, with small blobs in both and a large one across them.
    let mut files: Vec<(String, Vec<u8>)> = (0..600usize)
        .map(|i| {
            let text = format!("small blob {i}\n").repeat(1 + i % 40);
            let dir = if i % 2 == 0 { "a" } else { "s" };
            (format!("{dir}/{}/f{i}.txt", i % 16), text.into_bytes())
        })
        .collect();
    files.extend((0..20).map(|i| (format!("m/n{i}.bin"), noise(40 << 10, 101 + 2 * i))));
    files.extend((0..3).map(|i| (format!("l/n{i}.bin"), noise(3 << 19, 201 + 2 * i))));
    let listed: Vec<(&str, Option<Vec<u8>>)> = files
        .iter()
        .map(|(path, content)| (path.as_str(), Some(content.clone())))
        .collect();
    commit_files(&up, "c1", &listed);
    let auth = ["--store-auth-env", "GVD_AZ_CS", "--blobs", "separate"];
    pipeline_at(&root, &store, &upstream, "15m", &auth, &env)
        .unwrap_or_else(|log| panic!("pipeline: {log}"));
    mirror(&root, &store, &upstream, &env);
    let packs: Vec<u64> = backend
        .store("")
        .list("blobs/")
        .unwrap()
        .iter()
        .filter(|o| o.key.ends_with(".pack"))
        .map(|o| o.size)
        .collect();
    assert!(
        packs.len() == 1 && packs[0] > 4 << 20 && packs[0] < 8 << 20,
        "{packs:?}"
    );
    let ids: Vec<String> = git_blobs(&up.bare, &["--all"]).keys().cloned().collect();
    assert_eq!(ids.len(), 623);
    let blobs = ids.len() as u64;
    // Nothing follows the store or an upstream meanwhile: what is allocated is the
    // requests'.
    let edge = |tag: &str, extra: &[&str]| -> Server {
        let dir = root.join(format!("cache-{tag}"));
        let mut args = vec!["--store", &store, "--store-auth-env", "GVD_AZ_CS"];
        args.extend(["--poll-ms", "600000", "--cache-dir", dir.to_str().unwrap()]);
        args.extend(extra);
        Server::start_with_env(&args, &env)
    };
    let post = |server: &Server, ids: &[String]| {
        let body = json_ids(ids, None);
        let (status, _, pack) = server.post("/gvfs/objects", body.as_bytes(), CT_PACK);
        assert_eq!(status, 200);
        pack
    };
    let server = edge("a", &[]);
    // The first read fetches the pack's two blocks.
    let pack = post(&server, &ids);
    let posted = fresh_repo(&root, "post");
    git_stdin(
        &posted,
        &["index-pack", "--stdin", "--strict", "--keep=t"],
        &pack,
    );
    assert_eq!(cat_batch(&posted, &ids), cat_batch(&up.bare, &ids));
    let fetches = server.metric("gvfs_blob_cache_fetches_total");
    assert!((1..=2).contains(&fetches), "{fetches} fetches");
    // Read again, four times: the same bytes, from the blocks.
    let hits = server.metric("gvfs_blob_cache_hits_total");
    let allocated = server.metric("gvfs_alloc_post_seal_total");
    for _ in 0..4 {
        assert!(post(&server, &ids) == pack);
    }
    let allocated = server.metric("gvfs_alloc_post_seal_total") - allocated;
    assert!(
        allocated < blobs / 2,
        "{allocated} allocations for four POSTs of {blobs} cached blobs"
    );
    let again = server.metric("gvfs_blob_cache_hits_total");
    assert!(again >= hits + 4 * blobs, "{hits} then {again}");
    // Each as a loose object.
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
    let hits = server.metric("gvfs_blob_cache_hits_total");
    assert!(hits >= again + blobs, "{again} then {hits}");
    // The blobs in path order, as a checkout asks for them: the pack's order.
    let by_path: Vec<String> = git(&up.bare, &["ls-tree", "-r", "main"])
        .lines()
        .map(|l| l.split_whitespace().nth(2).unwrap().to_string())
        .collect();
    assert_eq!(by_path.len(), ids.len());
    let in_order = post(&server, &by_path);
    assert_eq!(server.metric("gvfs_blob_cache_fetches_total"), fetches);
    assert_eq!(server.metric("gvfs_blob_cache_bypass_total"), 0);
    // Both blocks are kept: a file, a descriptor and a mapping each.
    assert_eq!(server.metric("gvfs_blob_cache_blocks"), 2);
    assert_eq!(server.metric("gvfs_blob_cache_bytes"), packs[0]);
    assert_eq!(open_and_mapped_blocks(server.pid()), (2, 2));
    let files = std::fs::read_dir(root.join("cache-a/blob-blocks")).unwrap();
    assert_eq!(files.count(), 2);
    server.assert_clean();
    drop(server);

    // With room for one block the edge keeps one: a batch in path order reads the small
    // blobs of the first block, then those of the second, which takes the first one's
    // place while the request still holds it. Reads of more than a quarter of a block
    // (the three largest blobs) come straight from the store.
    let server = edge("b", &["--blob-cache-mib", "4"]);
    for _ in 0..2 {
        assert!(post(&server, &by_path) == in_order);
        assert_eq!(server.metric("gvfs_blob_cache_blocks"), 1);
        assert_eq!(open_and_mapped_blocks(server.pid()), (1, 1));
    }
    assert_eq!(server.metric("gvfs_blob_cache_bypass_total"), 6);
    assert_eq!(
        server.metric("gvfs_blob_cache_fetches_total"),
        10,
        "the large blobs, and each block once a request"
    );
    assert!(server.metric("gvfs_blob_cache_bytes") <= 4 << 20);
    server.assert_clean();
    drop(server);
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
}

/// An edge of an object store keeps gigabytes in its cache directory: one is required (the
/// system's temp directory, the default for a directory store, is memory on many hosts).
#[test]
fn an_object_store_edge_needs_a_cache_dir() {
    let cs = "DefaultEndpointsProtocol=http;AccountName=a;AccountKey=AAAA;\
              BlobEndpoint=http://127.0.0.1:1/a";
    let out = Command::new(env!("CARGO_BIN_EXE_gvfs-cache"))
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

/// An edge of an object store serves the slabs its manifest lists: cached files that are
/// not one (here a pack cut short, then an index) are downloaded again rather than keep
/// the edge from starting, and a pack the store returns with the listed length and trailer
/// but other bytes is refused, and not kept.
#[test]
fn an_edge_checks_its_slabs_against_the_manifest() {
    let Some(backend) = Backend::new("listed") else {
        return;
    };
    let root = tempdir("azure-listed");
    let env = [("GVD_AZ_CS", backend.cs.as_str())];
    let store = format!("azure:{}", backend.spec);
    let mut up = Upstream::new(&root);
    up.commit(5);
    let upstream = up.bare.to_str().unwrap();
    let auth = ["--store-auth-env", "GVD_AZ_CS"];
    pipeline_at(&root, &store, upstream, "15m", &auth, &env).unwrap_or_else(|log| panic!("{log}"));
    let cache = root.join("cache");
    let args = [
        "--store",
        store.as_str(),
        "--store-auth-env",
        "GVD_AZ_CS",
        "--cache-dir",
        cache.to_str().unwrap(),
    ];
    let server = Server::start_with_env(&args, &env);
    let cold = server.prefetch(0);
    drop(server);

    let s = backend.store("");
    let (bytes, _) = s.get("manifest.bin").unwrap().unwrap();
    let base = gvfs_cache::manifest::Manifest::decode(&bytes)
        .unwrap()
        .baseline;
    for ext in ["pack", "mphf"] {
        let path = cache.join(format!("{}.{ext}", base.id));
        let len = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len / 2).unwrap();
        let server = Server::start_with_env(&args, &env);
        assert!(server.prefetch(0) == cold);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
        server.assert_clean();
    }

    let pack = cache.join(format!("{}.pack", base.id));
    let mut bytes = std::fs::read(&pack).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 1;
    let other = root.join("other.pack");
    std::fs::write(&other, &bytes).unwrap();
    s.delete(&base.pack_key()).unwrap();
    s.upload_file(&base.pack_key(), &other).unwrap();
    std::fs::remove_dir_all(&cache).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_gvfs-cache"))
        .args(["serve", "--bind", "127.0.0.1:0"])
        .args(args)
        .envs(env)
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && err.contains("is not the one listed"),
        "{err}"
    );
    assert!(!pack.exists(), "the pack refused is not kept");
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
}

/// An edge that has the baseline before it makes a weekly rebuild's baseline of that one
/// and the tail the run published, also when it was not running at the time. What it
/// makes of a tail that is not the baseline's is not served: it downloads the pack. An
/// edge with nothing in its cache downloads it too.
#[test]
fn an_edge_makes_a_new_baseline_of_the_one_it_has() {
    let Some(backend) = Backend::new("tail") else {
        return;
    };
    let root = tempdir("azure-tail");
    let env = [("GVD_AZ_CS", backend.cs.as_str())];
    let store = format!("azure:{}", backend.spec);
    let mut up = Upstream::new(&root);
    up.commit(5);
    let upstream = up.bare.to_str().unwrap().to_string();
    let t0 = gvfs_cache::util::now_secs();
    let run = |tier: &str, now: u64| {
        let now = now.to_string();
        let args = ["--store-auth-env", "GVD_AZ_CS", "--now", now.as_str()];
        pipeline_at(&root, &store, &upstream, tier, &args, &env)
            .unwrap_or_else(|log| panic!("pipeline {tier}: {log}"))
    };
    run("15m", t0);
    let edge = |cache: &std::path::Path| {
        Server::start_with_env(
            &[
                "--store",
                store.as_str(),
                "--store-auth-env",
                "GVD_AZ_CS",
                "--cache-dir",
                cache.to_str().unwrap(),
            ],
            &env,
        )
    };
    let baseline = || {
        let (bytes, _) = backend.store("").get("manifest.bin").unwrap().unwrap();
        gvfs_cache::manifest::Manifest::decode(&bytes)
            .unwrap()
            .baseline
    };
    let served = |server: &Server, name: &str, tip: &str| {
        let packs = server.prefetch(0);
        assert_eq!(packs.len(), 1);
        let repo = fresh_repo(&root, name);
        install_pack(&repo, &packs[0].1);
        assert_complete(&repo, tip);
        server.assert_clean();
    };

    // Two edges with the first baseline in their caches, not running during the rebuild.
    let (cache_a, cache_b) = (root.join("cache-a"), root.join("cache-b"));
    drop((edge(&cache_a), edge(&cache_b)));
    let first = baseline();
    let tip = up.commit(5);
    let log = run("168h", t0 + 900);
    assert!(log.contains("and a tail of"), "{log}");
    let second = baseline();
    assert_ne!(second.id, first.id);

    // The first makes the new pack of the old one and the tail.
    let a = edge(&cache_a);
    assert_eq!(a.metric("gvfs_baselines_grown_total"), 1);
    served(&a, "from-a", &tip);
    let made = std::fs::read(cache_a.join(format!("{}.pack", second.id))).unwrap();
    assert_eq!(made.len() as u64, second.pack_len);
    assert!(!cache_a.join(format!("{}.tail", second.id)).exists());
    drop(a);

    // The second finds a tail that makes another pack (a byte of it changed): it serves
    // the store's.
    let s = backend.store("");
    let mut tail = s.get(&second.tail_key()).unwrap().unwrap().0;
    let last = tail.len() - 30;
    tail[last] ^= 1;
    let changed = root.join("changed.tail");
    std::fs::write(&changed, &tail).unwrap();
    s.delete(&second.tail_key()).unwrap();
    s.upload_file(&second.tail_key(), &changed).unwrap();
    let b = edge(&cache_b);
    assert_eq!(b.metric("gvfs_baselines_grown_total"), 0);
    served(&b, "from-b", &tip);
    assert!(std::fs::read(cache_b.join(format!("{}.pack", second.id))).unwrap() == made);
    drop(b);

    // An edge that never had the first baseline downloads the second.
    let c = edge(&root.join("cache-c"));
    assert_eq!(c.metric("gvfs_baselines_grown_total"), 0);
    served(&c, "from-c", &tip);
    drop(c);
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
}
