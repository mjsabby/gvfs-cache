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

    let cache = root.join("cache");
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
