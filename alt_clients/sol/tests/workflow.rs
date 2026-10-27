#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod common;

use std::sync::Arc;

use bytes::Bytes;
use gvfs_daemon::codec::{Kind, Oid, checksum, pack_count};
use gvfs_daemon::manifest::{Manifest, Tier};
use gvfs_daemon::pipeline::{PipelineOptions, run_pipeline};
use gvfs_daemon::server::{ServerOptions, runtime};
use gvfs_daemon::slab::NO_BASE;
use gvfs_daemon::storage::{Endpoint, HttpClient, Storage, next_data};
use gvfs_daemon::tree::TreeLayout;
use gvfs_daemon::upstream::Upstream;
use http::{HeaderMap, Method, StatusCode};

fn options(tier: Option<Tier>, now: u64) -> PipelineOptions {
    PipelineOptions {
        tier,
        now,
        limits: common::limits(TreeLayout::Clustered),
        max_ranges: 8192,
        gap: 4096,
        retention_secs: 14 * 86400,
        input_pack: None,
    }
}

fn upstream(fixture: &common::Fixture) -> Upstream {
    Upstream::new(
        fixture.git.to_str().unwrap().to_owned(),
        "main".into(),
        None,
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn genesis_incremental_daily_weekly_and_cas_recovery() {
    let fixture = common::Fixture::new(24);
    let dir = tempfile::tempdir().unwrap();
    let storage = common::storage(dir.path());
    fixture.set_tip(5);
    let mut git = upstream(&fixture);
    let first = run_pipeline(&storage, Some(&mut git), &options(Some(Tier::M15), 100))
        .await
        .unwrap();
    assert!(first.changed);
    assert_eq!(first.generation, 1);
    let (genesis, stale_etag) = storage.manifest().await.unwrap().unwrap();
    fixture.set_tip(11);
    run_pipeline(&storage, Some(&mut git), &options(Some(Tier::M15), 1000))
        .await
        .unwrap();
    fixture.set_tip(17);
    run_pipeline(&storage, Some(&mut git), &options(Some(Tier::M15), 2000))
        .await
        .unwrap();
    let (before, _) = storage.manifest().await.unwrap().unwrap();
    assert_eq!(before.pyramid.len(), 2);
    let mut epochs = std::collections::BTreeMap::new();
    for reference in before.pyramid.iter().rev() {
        let slab = storage.load_slab(*reference).await.unwrap();
        for rank in 0..reference.count {
            let record = slab.record(rank).unwrap();
            epochs.entry(record.oid).or_insert(record.epoch);
        }
    }
    assert!(matches!(
        storage.publish_manifest(&genesis, Some(&stale_etag)).await,
        Err(gvfs_daemon::Error::Conflict)
    ));
    // Higher tiers require no upstream and preserve complete coverage.
    let rolled = run_pipeline(&storage, None, &options(Some(Tier::H24), 86400))
        .await
        .unwrap();
    assert!(rolled.changed);
    let (daily, _) = storage.manifest().await.unwrap().unwrap();
    assert_eq!(daily.pyramid.len(), 1);
    assert_eq!(daily.pyramid[0].tier, Tier::H24);
    let daily_slab = storage.load_slab(daily.pyramid[0]).await.unwrap();
    assert_eq!(daily_slab.minimum_epoch().unwrap(), 1000);
    for rank in 0..daily_slab.reference.count {
        let record = daily_slab.record(rank).unwrap();
        assert_eq!(record.epoch, epochs[&record.oid]);
    }
    let daily_sources = [Arc::new(daily_slab.clone())];
    let mut after = gvfs_daemon::splice::SpliceArena::new(4096, 4096).unwrap();
    after.plan_after(&daily_sources, 1000, 4096, 65536).unwrap();
    assert!(after.entries.len() < daily_slab.reference.count as usize);
    for (&oid, &epoch) in &epochs {
        if epoch > 1000 {
            assert!(
                after.entries.iter().any(|selected| daily_slab
                    .record(selected.ordinal)
                    .unwrap()
                    .oid
                    == oid)
            );
        }
    }
    after.plan_after(&daily_sources, 2000, 4096, 65536).unwrap();
    assert!(after.entries.is_empty());
    assert!(
        (0..daily_slab.reference.count)
            .any(|rank| daily_slab.record(rank).unwrap().base != NO_BASE)
    );
    run_pipeline(&storage, None, &options(Some(Tier::Baseline), 604800))
        .await
        .unwrap();
    let (weekly, etag) = storage.manifest().await.unwrap().unwrap();
    assert!(weekly.pyramid.is_empty());
    assert_eq!(weekly.weekly.len(), 1);
    assert_eq!(weekly.tip, fixture.tips[17]);
    let baseline = storage.load_slab(weekly.baseline).await.unwrap();
    common::verify_native(&baseline, Some(fixture.tips[17]));
    let delta = storage.load_slab(weekly.weekly[0]).await.unwrap();
    let old = storage.load_slab(genesis.baseline).await.unwrap();
    for rank in 0..delta.reference.count {
        assert!(
            old.lookup(delta.record(rank).unwrap().oid)
                .unwrap()
                .is_none()
        );
    }
    let mut selected = Vec::with_capacity(4098);
    weekly.select(100, &mut selected).unwrap();
    assert_eq!(selected, weekly.weekly);
    weekly.select(1, &mut selected).unwrap();
    assert_eq!(selected, vec![weekly.baseline]);
    weekly.select(0, &mut selected).unwrap();
    assert_eq!(selected.len(), 1);
    weekly.select(weekly.cursor(), &mut selected).unwrap();
    assert!(selected.is_empty());
    // Unpublished immutable files are harmless after a simulated crash.
    fixture.set_tip(23);
    let orphan = common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Full,
        Tier::M15,
        weekly.cursor(),
        3000,
        &[Arc::new(baseline)],
    );
    storage.upload(&orphan).await.unwrap();
    assert_eq!(storage.manifest().await.unwrap().unwrap().1, etag);
    let recovered = run_pipeline(&storage, Some(&mut git), &options(Some(Tier::M15), 15000))
        .await
        .unwrap();
    assert_eq!(recovered.tip, fixture.tip());
    let (recovered, _) = storage.manifest().await.unwrap().unwrap();
    assert_eq!(recovered.pyramid[0].start, weekly.cursor());
    assert_eq!(recovered.pyramid[0].end, 15000);
    // A rewind may need no new objects, but still advances the cursor and tip.
    fixture.set_tip(3);
    run_pipeline(&storage, Some(&mut git), &options(Some(Tier::M15), 16000))
        .await
        .unwrap();
    let (rewound, _) = storage.manifest().await.unwrap().unwrap();
    assert_eq!(rewound.tip, fixture.tips[3]);
    assert_eq!(rewound.pyramid.last().unwrap().count, 0);
    let unchanged = run_pipeline(&storage, Some(&mut git), &options(Some(Tier::M15), 17000))
        .await
        .unwrap();
    assert!(!unchanged.changed);
}

#[tokio::test]
async fn publication_sidecars_are_required_and_gc_is_range_specific() {
    let dir = tempfile::tempdir().unwrap();
    let storage = common::storage(dir.path());
    let limits = common::limits(TreeLayout::Full);
    let mut builder = gvfs_daemon::slab::Builder::new(limits).unwrap();
    let record = common::record(Kind::Blob, b"baseline");
    builder
        .add(record, &common::full_entry(Kind::Blob, b"baseline"))
        .unwrap();
    let baseline = builder.finish(Tier::Baseline, 0, 100, record.oid).unwrap();
    storage.upload(&baseline).await.unwrap();
    let mut builder = gvfs_daemon::slab::Builder::new(limits).unwrap();
    let mut record = common::record(Kind::Blob, b"increment");
    record.epoch = 150;
    builder
        .add(record, &common::full_entry(Kind::Blob, b"increment"))
        .unwrap();
    let active = builder.finish(Tier::M15, 100, 200, record.oid).unwrap();
    let mut builder = gvfs_daemon::slab::Builder::new(limits).unwrap();
    builder
        .add(record, &common::full_entry(Kind::Blob, b"increment"))
        .unwrap();
    let orphan = builder.finish(Tier::M15, 200, 300, record.oid).unwrap();
    assert_eq!(active.reference.id, orphan.reference.id);
    storage.upload(&active).await.unwrap();
    storage.upload(&orphan).await.unwrap();
    let mut manifest = Manifest::genesis(baseline.reference).unwrap();
    manifest.pyramid.push(active.reference);
    manifest.tip = active.reference.tip;
    storage.publish_manifest(&manifest, None).await.unwrap();
    let active_path = dir
        .path()
        .join("store")
        .join(gvfs_daemon::storage::times_key(active.reference));
    let orphan_path = dir
        .path()
        .join("store")
        .join(gvfs_daemon::storage::times_key(orphan.reference));
    let unknown_path = dir
        .path()
        .join("store/slabs")
        .join(format!("{}.not-a-range.times", active.reference.id));
    std::fs::write(&unknown_path, b"not daemon-owned").unwrap();
    assert_eq!(
        storage
            .gc_local(
                &manifest,
                std::time::SystemTime::now() + std::time::Duration::from_secs(1)
            )
            .unwrap(),
        1
    );
    assert!(active_path.exists());
    assert!(!orphan_path.exists());
    assert!(unknown_path.exists());
    let loaded = storage.load_slab(active.reference).await.unwrap();
    assert_eq!(loaded.record(0).unwrap().epoch, 150);
    let mut missing = loaded.clone();
    missing.times = None;
    assert!(missing.validate().is_err());
    std::fs::remove_file(active_path).unwrap();
    assert!(storage.load_metadata(active.reference).await.is_err());
}

#[tokio::test]
async fn restart_and_manifest_handoff_rebuild_only_the_unsealed_live_window() {
    let fixture = common::Fixture::new(20);
    let dir = tempfile::tempdir().unwrap();
    let storage = common::storage(dir.path());
    fixture.set_tip(5);
    run_pipeline(
        &storage,
        Some(&mut upstream(&fixture)),
        &options(Some(Tier::M15), 100),
    )
    .await
    .unwrap();
    fixture.set_tip(19);
    let opts = server_options(true);
    let (state, mut git) = runtime::boot(Arc::clone(&storage), Some(upstream(&fixture)), opts)
        .await
        .unwrap();
    let old = state.snapshot().unwrap();
    assert_eq!(old.tip(), fixture.tip());
    assert!(old.live.is_some());
    fixture.set_tip(11);
    run_pipeline(
        &storage,
        Some(&mut upstream(&fixture)),
        &options(Some(Tier::M15), 200),
    )
    .await
    .unwrap();
    fixture.set_tip(19);
    runtime::refresh(&state, git.as_mut(), true).await.unwrap();
    let current = state.snapshot().unwrap();
    assert_eq!(current.tip(), fixture.tip());
    assert_eq!(current.manifest.tip, fixture.tips[11]);
    let live = current.live.as_ref().unwrap();
    let mut points = Vec::with_capacity(4098);
    current.points(&mut points).unwrap();
    for &oid in fixture.tips.iter().skip(12) {
        assert!(live.lookup(oid).unwrap().is_some());
        assert!(gvfs_daemon::splice::locate(&points, oid).unwrap().is_some());
    }
    assert_eq!(
        old.tip(),
        fixture.tip(),
        "clients holding the previous snapshot stay valid"
    );
    drop(current);
    drop(old);
    drop(state);
    let (restarted, _) = runtime::boot(storage, Some(upstream(&fixture)), server_options(true))
        .await
        .unwrap();
    assert_eq!(restarted.snapshot().unwrap().tip(), fixture.tip());
}

#[tokio::test]
async fn conditional_genesis_and_corrupt_manifest_are_not_success_shaped_fallbacks() {
    let fixture = common::Fixture::new(2);
    let dir = tempfile::tempdir().unwrap();
    let storage = common::storage(dir.path());
    let slab = common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Full,
        Tier::Baseline,
        0,
        100,
        &[],
    );
    let manifest = Manifest::genesis(slab.reference).unwrap();
    storage.upload(&slab).await.unwrap();
    let (a, b) = tokio::join!(
        storage.publish_manifest(&manifest, None),
        storage.publish_manifest(&manifest, None)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let mut bytes = manifest.encode().unwrap();
    bytes[10] ^= 1;
    std::fs::write(dir.path().join("store/manifest.bin"), bytes).unwrap();
    assert!(storage.manifest().await.is_err());
}

fn server_options(live: bool) -> ServerOptions {
    let limits = common::limits(TreeLayout::Clustered);
    ServerOptions {
        bind: ([127, 0, 0, 1], 0).into(),
        limits,
        live_limits: limits,
        range_bytes: 65536,
        gap: 4096,
        max_ranges: 8192,
        arenas: 2,
        request_bytes: 65536,
        request_objects: 512,
        snapshot_slots: 4,
        poll_seconds: 2,
        manifest_seconds: 30,
        live,
        ..ServerOptions::default()
    }
}

struct Server {
    address: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<gvfs_daemon::Result<()>>,
}

impl Server {
    async fn start(storage: Arc<Storage>, options: ServerOptions) -> Self {
        let (state, _) = runtime::boot(storage, None, options).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(gvfs_daemon::server::run_listener(
            listener,
            state,
            None,
            async move {
                shutdown.await.unwrap();
                Ok(())
            },
        ));
        Self {
            address,
            stop,
            task,
        }
    }
    fn client(&self) -> HttpClient {
        HttpClient::new(Endpoint::parse(&format!("http://{}", self.address)).unwrap()).unwrap()
    }
    async fn stop(self) {
        self.stop.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

async fn request(
    client: &mut HttpClient,
    method: Method,
    path: &str,
    query: &str,
    body: Option<Bytes>,
) -> (StatusCode, Vec<u8>) {
    let response = client
        .request(method, path, query, HeaderMap::new(), body)
        .await
        .unwrap();
    let status = response.status();
    let mut body = response.into_body();
    let mut out = Vec::new();
    while let Some(bytes) = next_data(&mut body).await.unwrap() {
        out.extend_from_slice(&bytes);
    }
    (status, out)
}

fn unpack_prefetch(bytes: &[u8]) -> &[u8] {
    assert_eq!(&bytes[..6], b"GPRE \x01");
    assert_eq!(
        u16::from_le_bytes(bytes[6..8].try_into().unwrap()),
        1,
        "a fetch must produce exactly one pack"
    );
    let pack_len = i64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
    let idx_len = i64::from_le_bytes(bytes[24..32].try_into().unwrap());
    assert_eq!(
        bytes.len(),
        32 + pack_len + if idx_len < 0 { 0 } else { idx_len as usize }
    );
    &bytes[32..32 + pack_len]
}

fn native_index_wire(bytes: &[u8], wanted: &[Oid]) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    common::run(
        gvfs_daemon::upstream::git_command()
            .args(["init", "--quiet", "--bare"])
            .arg(dir.path()),
        b"",
    );
    common::run(
        common::command(dir.path()).args(["index-pack", "--stdin"]),
        bytes,
    );
    for oid in wanted {
        common::run(
            common::command(dir.path()).args(["cat-file", "-t", &oid.to_string()]),
            b"",
        );
    }
    assert_eq!(
        std::fs::read_dir(dir.path().join("objects/pack"))
            .unwrap()
            .filter(|path| path
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "pack"))
            .count(),
        1
    );
    let id = Oid::from_bytes(&bytes[bytes.len() - 20..]).unwrap();
    std::fs::read(dir.path().join(format!("objects/pack/pack-{id}.idx"))).unwrap()
}

async fn populated_store(fixture: &common::Fixture, storage: &Storage) {
    fixture.set_tip(5);
    let mut git = upstream(fixture);
    run_pipeline(storage, Some(&mut git), &options(Some(Tier::M15), 100))
        .await
        .unwrap();
    fixture.set_tip(11);
    run_pipeline(storage, Some(&mut git), &options(Some(Tier::M15), 200))
        .await
        .unwrap();
    fixture.set_tip(fixture.tips.len() - 1);
    run_pipeline(storage, Some(&mut git), &options(Some(Tier::M15), 300))
        .await
        .unwrap();
}

#[tokio::test]
async fn http_single_pack_prefetch_objects_sizes_loose_and_reused_arenas() {
    let fixture = common::Fixture::new(20);
    let dir = tempfile::tempdir().unwrap();
    let storage = common::storage(dir.path());
    populated_store(&fixture, &storage).await;
    let server = Server::start(storage, server_options(false)).await;
    let mut client = server.client();
    assert_eq!(
        request(&mut client, Method::GET, "healthz", "", None)
            .await
            .0,
        StatusCode::OK
    );
    let (status, config) = request(&mut client, Method::GET, "repo/gvfs/config", "", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        String::from_utf8(config)
            .unwrap()
            .contains("\"CacheServers\"")
    );
    let (status, cold) = request(
        &mut client,
        Method::GET,
        "gvfs/prefetch",
        "lastPackTimestamp=0",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let pack = unpack_prefetch(&cold);
    assert_eq!(checksum(&pack[..pack.len() - 20]), &pack[pack.len() - 20..]);
    let index = native_index_wire(pack, &[fixture.tip()]);
    assert_eq!(&cold[32 + pack.len()..], index);
    for _ in 0..20 {
        let body = Bytes::from(format!(
            "{{\"commitDepth\":1,\"objectIds\":[\"{}\"]}}",
            fixture.tip()
        ));
        let (status, bytes) =
            request(&mut client, Method::POST, "gvfs/objects", "", Some(body)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(pack_count(&bytes).unwrap() > 1);
        native_index_wire(&bytes, &[fixture.tip()]);
    }
    let body = Bytes::from(format!("[\"{}\"]", fixture.blobs[3]));
    let (status, bytes) = request(&mut client, Method::POST, "gvfs/sizes", "", Some(body)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8(bytes).unwrap().contains(&format!(
        "\"Size\":{}",
        fixture.bytes(fixture.blobs[3]).len()
    )));
    for oid in [fixture.blobs[3], fixture.trees[3], fixture.tip()] {
        let (status, compressed) = request(
            &mut client,
            Method::GET,
            &format!("gvfs/objects/{oid}"),
            "",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        use std::io::Read;
        let mut loose = Vec::new();
        flate2::read::ZlibDecoder::new(compressed.as_slice())
            .read_to_end(&mut loose)
            .unwrap();
        let nul = loose.iter().position(|byte| *byte == 0).unwrap();
        assert_eq!(&loose[nul + 1..], fixture.bytes(oid));
        let kind = if oid == fixture.tip() {
            Kind::Commit
        } else if oid == fixture.trees[3] {
            Kind::Tree
        } else {
            Kind::Blob
        };
        assert_eq!(
            gvfs_daemon::codec::object_id(kind, &loose[nul + 1..]).unwrap(),
            oid
        );
    }
    let (status, empty) = request(
        &mut client,
        Method::GET,
        "gvfs/prefetch",
        "lastPackTimestamp=300",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty, b"GPRE \x01\0\0");
    assert_eq!(
        request(
            &mut client,
            Method::GET,
            "gvfs/prefetch",
            "lastPackTimestamp=-1",
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &mut client,
            Method::POST,
            "gvfs/objects",
            "",
            Some(Bytes::from_static(b"{bad"))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &mut client,
            Method::POST,
            "gvfs/objects",
            "",
            Some(Bytes::from(format!(
                "{{\"objectIds\":[\"{}\"]}}",
                Oid([99; 20])
            )))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    server.stop().await;
}

#[tokio::test]
async fn http2_prior_knowledge_uses_the_same_endpoint_engine() {
    let fixture = common::Fixture::new(2);
    let dir = tempfile::tempdir().unwrap();
    let storage = common::storage(dir.path());
    let slab = common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Full,
        Tier::Baseline,
        0,
        100,
        &[],
    );
    common::publish(&storage, &slab).await;
    let server = Server::start(storage, server_options(false)).await;
    let socket = tokio::net::TcpStream::connect(server.address)
        .await
        .unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake::<_, _, gvfs_daemon::storage::RequestBody>(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(socket),
        )
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let response = sender
        .send_request(
            http::Request::builder()
                .uri(format!("http://{}/gvfs/prefetch", server.address))
                .body(gvfs_daemon::storage::RequestBody::default())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.version(), http::Version::HTTP_2);
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = next_data(&mut body).await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    native_index_wire(unpack_prefetch(&bytes), &[fixture.tip()]);
    driver.abort();
    server.stop().await;
}

#[test]
fn json_protocol_arrays_escapes_unknown_fields_and_malformed_values() {
    let oid = Oid([0x01; 20]);
    let mut out = Vec::with_capacity(16);
    let raw = format!(
        "{{\"object\\u0049ds\":[\"{}\"],\"commitDepth\":2,\"future\":{{\"x\":[null,true,-1.5e+2,\"\\uD834\\uDD1E\"]}}}}",
        oid
    );
    let hot = gvfs_platform::HotRegion::enter();
    assert_eq!(
        gvfs_daemon::server::json::object_request(raw.as_bytes(), &mut out).unwrap(),
        2
    );
    let count = hot.allocations();
    drop(hot);
    assert_eq!(count, 0);
    assert_eq!(out, [oid]);
    for value in [
        b"{\"objectIds\":[],\"future\":- 1}".as_slice(),
        b"{\"objectIds\":[],\"future\":1 .2}",
        b"{\"objectIds\":[],\"future\":\"\\uD800\"}",
        b"{\"objectIds\":[],\"commitDepth\":01}",
        b"{\"objectIds\":[],\"objectIds\":[]}",
        b"{\"objectIds\":[],\"future\":truex}",
    ] {
        assert!(gvfs_daemon::server::json::object_request(value, &mut out).is_err());
    }
    gvfs_daemon::server::json::sizes_request(format!("[\"{oid}\"]").as_bytes(), &mut out).unwrap();
    assert_eq!(out, [oid]);
    assert!(gvfs_daemon::server::json::sizes_request(b"[] junk", &mut out).is_err());
}

#[tokio::test]
async fn tls_alpn_http2_serves_one_native_git_pack() {
    use rustls::pki_types::pem::PemObject;
    let fixture = common::Fixture::new(2);
    let dir = tempfile::tempdir().unwrap();
    let certificate = dir.path().join("certificate.pem");
    let key = dir.path().join("key.pem");
    let result = std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            certificate.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let storage = common::storage(dir.path());
    let slab = common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Clustered,
        Tier::Baseline,
        0,
        100,
        &[],
    );
    common::publish(&storage, &slab).await;
    let mut options = server_options(false);
    options.tls_cert = Some(certificate.clone());
    options.tls_key = Some(key);
    let server = Server::start(storage, options).await;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls::pki_types::CertificateDer::pem_file_iter(certificate).unwrap() {
        roots.add(certificate.unwrap()).unwrap();
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let socket = tokio::net::TcpStream::connect(server.address)
        .await
        .unwrap();
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake::<_, _, gvfs_daemon::storage::RequestBody>(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(tls),
        )
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let response = sender
        .send_request(
            http::Request::builder()
                .uri(format!(
                    "https://localhost:{}/gvfs/prefetch",
                    server.address.port()
                ))
                .body(gvfs_daemon::storage::RequestBody::default())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.version(), http::Version::HTTP_2);
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = next_data(&mut body).await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    native_index_wire(unpack_prefetch(&bytes), &[fixture.tip()]);
    driver.abort();
    server.stop().await;
}
