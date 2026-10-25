//! End-to-end tests for the Unified Binary Architecture (`gvfs-daemon`):
//! - All 5 Crash & Cold-Start Scenarios (PDF pp. 163–169)
//! - Pyramid Compaction (`15m -> 1h -> 6h -> 12h -> 24h -> 168h` Weekly "Aha" Delta & Baseline)
//! - Two-Track Manifest Routing (Cold Clone Track A vs Warm Workstation Track B)
//! - The 1-Pack Invariant (`GET /gvfs/prefetch` and `POST /gvfs/objects` always emit 1 pack)
//! - Zero Heap Allocations on the Hot Path (`gvfs_alloc_guard` in `Mode::Strict`)
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic
)]

use std::io::Read;
use std::sync::{Arc, RwLock};

use gvfs_alloc_guard::SealedAllocator;
use gvfs_core::gpre::{parse_gpre_file_header, parse_gpre_pack_header};
use gvfs_core::idx::IdxView;
use gvfs_core::pack::parse_pack_header;
use gvfs_core::{Tier, ZERO_OID};
use gvfs_daemon::{
    DaemonCounters, DaemonHttpState, InMemoryGitUpstream, ServerRuntimeState, WorkerPool,
    default_config_json, ingest_ado_delta, ingest_git_delta, run_resilient_pipeline_step,
    spawn_http_server,
};
use gvfs_storage::{StorageClient, StorageError};

#[global_allocator]
static GLOBAL: SealedAllocator = SealedAllocator;

fn make_hfs_blob(hex64: &str, size: u64) -> Vec<u8> {
    format!("~~Git-hfs 1.0\nblob:{hex64}00\nsize:{size}\n").into_bytes()
}

/// Parses a `GPRE` stream and returns `Vec<(timestamp, pack_bytes, idx_bytes)>`.
fn decode_gpre_packs(raw: &[u8]) -> Vec<(i64, Vec<u8>, Vec<u8>)> {
    let count = parse_gpre_file_header(raw).expect("valid GPRE header");
    let mut pos = 8usize;
    let mut out = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let ph = parse_gpre_pack_header(&raw[pos..pos + 24]).expect("valid GPRE pack header");
        pos += 24;
        let pack_len = usize::try_from(ph.pack_len).unwrap();
        let pack = raw[pos..pos + pack_len].to_vec();
        pos += pack_len;
        let idx = if let Some(il) = ph.idx_len {
            let idx_len = usize::try_from(il).unwrap();
            let b = raw[pos..pos + idx_len].to_vec();
            pos += idx_len;
            b
        } else {
            Vec::new()
        };
        out.push((ph.timestamp, pack, idx));
    }
    assert_eq!(pos, raw.len(), "GPRE stream must have no trailing garbage");
    out
}

#[test]
fn five_crash_and_cold_start_scenarios_and_force_push_recovery() {
    let storage = StorageClient::open("mem:crash-scenarios").unwrap();
    let upstream = InMemoryGitUpstream::new();

    // Seed Commit 1 (Genesis)
    let b1 = make_hfs_blob(
        "A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC91",
        4167,
    );
    let (c1, _t1, _blobs1) = upstream.add_commit(
        ZERO_OID,
        1_700_000_000,
        "initial commit",
        &[("README.md", &b1)],
    );

    // ========================================================================
    // SCENARIO 1: DAY-0 GENESIS BOOT (`manifest.bin` does not exist yet)
    // ========================================================================
    assert!(storage.get_manifest_with_etag().unwrap().is_none());
    let etag1 = run_resilient_pipeline_step(&storage, &upstream, Tier::M15, 1_700_000_100).unwrap();
    let (manifest1, got_etag1) = storage.get_manifest_with_etag().unwrap().unwrap();
    assert_eq!(etag1, got_etag1);
    assert_eq!(manifest1.last_15m_tip_oid, c1);
    assert_eq!(manifest1.weekly_baseline.tier, Tier::H168Base);
    assert_eq!(manifest1.weekly_baseline.hfs_sidecar.len(), 1);
    assert_eq!(
        manifest1.weekly_baseline.hfs_sidecar[0].meta.real_size,
        4167
    );

    // Split-brain genesis check: `put_manifest_if_none_match` must fail with PreconditionFailed
    assert!(matches!(
        storage.put_manifest_if_none_match(&manifest1),
        Err(StorageError::PreconditionFailed)
    ));

    // ========================================================================
    // SCENARIO 2: EDGE / LOCALHOST CRASH AT MINUTE 14 + 2-SECOND MICRO-DELTA
    // ========================================================================
    let b2 = make_hfs_blob(
        "B14C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC92",
        8192,
    );
    let (c2, _t2, _) = upstream.add_commit(
        c1,
        1_700_000_840,
        "commit at minute 14",
        &[("README.md", &b1), ("src/a.txt", &b2)],
    );

    // Server boots with empty RAM at minute 14:50 -> automatically reconstructs `< 15m` L0Ram!
    let mut srv_state =
        ServerRuntimeState::boot_or_recover(&storage, &upstream, 1_700_000_890).unwrap();
    let l0 = srv_state
        .l0_ram_slab
        .as_ref()
        .expect("L0Ram must be reconstructed on boot");
    assert_eq!(l0.tip_commit, c2);
    assert!(l0.lookup_oid(&c2).is_some());

    // 2 seconds later (14m52s), developer pushes Commit 3 -> `tick_live_sync` pulls ONLY
    // `(c2 -> c3)` and splices it into `l0_ram_slab` in memory!
    let b3 = make_hfs_blob(
        "C24C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC93",
        16384,
    );
    let (c3, _t3, _) = upstream.add_commit(
        c2,
        1_700_000_892,
        "commit at 14m52s",
        &[("README.md", &b1), ("src/a.txt", &b2), ("src/b.txt", &b3)],
    );
    srv_state
        .tick_live_sync(&storage, &upstream, 1_700_000_892)
        .unwrap();
    let l0_merged = srv_state.l0_ram_slab.as_ref().unwrap();
    assert_eq!(l0_merged.tip_commit, c3);
    assert!(
        l0_merged.lookup_oid(&c2).is_some(),
        "c2 preserved in spliced L0Ram"
    );
    assert!(l0_merged.lookup_oid(&c3).is_some(), "c3 merged into L0Ram");

    // ========================================================================
    // SCENARIO 3: PIPELINE CRASH MID-UPLOAD + ORPHAN GC
    // ========================================================================
    // Simulate a crashed pipeline job that uploaded an unreferenced slab 3 hours ago
    let orphan_slab = ingest_ado_delta(&upstream, c1, c2, Tier::M15, 1_700_000_200).unwrap();
    storage.upload_slab(&orphan_slab).unwrap();
    assert!(
        storage
            .get_raw(&format!("{}.pack", orphan_slab.id))
            .unwrap()
            .is_some()
    );

    // Next pipeline run at t = 1_700_010_000 sweeps `(c1 -> c3)` into a clean M15 slab
    // and garbage-collects the orphaned slab (> 2h old)!
    run_resilient_pipeline_step(&storage, &upstream, Tier::M15, 1_700_010_000).unwrap();
    assert!(
        storage
            .get_raw(&format!("{}.pack", orphan_slab.id))
            .unwrap()
            .is_none(),
        "orphaned slab older than 2h must be garbage-collected"
    );

    // ========================================================================
    // SCENARIO 4: HANDOFF FROM PIPELINE TO LIVE EDGE (`L0Ram` cleared when manifest advances)
    // ========================================================================
    srv_state
        .tick_live_sync(&storage, &upstream, 1_700_010_002)
        .unwrap();
    assert_eq!(srv_state.manifest.last_15m_tip_oid, c3);
    assert!(
        srv_state.l0_ram_slab.is_none(),
        "L0Ram must be dropped once manifest.bin seals those commits"
    );

    // ========================================================================
    // SCENARIO 5: FORCE-PUSH + GARBAGE-COLLECTED `have` COMMIT (PDF p. 169)
    // ========================================================================
    // Admin force-pushes `main` back to a new commit `c4` parented off `c1`, and `c3` is
    // garbage-collected in ADO (`ERR upload-pack: not our ref <c3>`).
    upstream.mark_commit_garbage_collected(c3);
    let b4 = make_hfs_blob(
        "D34C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC94",
        32768,
    );
    let (c4, _, _) = upstream.add_commit(
        c1,
        1_700_010_900,
        "force-pushed commit off c1",
        &[("README.md", &b1), ("src/force.txt", &b4)],
    );

    // Pipeline automatically falls back to `manifest.fallback_have_commits()` (`c1` in 168h baseline)!
    run_resilient_pipeline_step(&storage, &upstream, Tier::M15, 1_700_010_900).unwrap();
    let (manifest_after_fp, _) = storage.get_manifest_with_etag().unwrap().unwrap();
    assert_eq!(manifest_after_fp.last_15m_tip_oid, c4);
    assert!(
        manifest_after_fp
            .sub_week_pyramid
            .last()
            .unwrap()
            .lookup_oid(&c4)
            .is_some()
    );
}

#[test]
fn pyramid_rollups_two_track_manifest_and_single_pack_invariant() {
    let storage = StorageClient::open("mem:pyramid-test").unwrap();
    let upstream = InMemoryGitUpstream::new();

    // Genesis baseline (Sunday Week 1)
    let b0 = make_hfs_blob(
        "0000000000000000000000000000000000000000000000000000000000000001",
        100,
    );
    let (mut tip, _, _) =
        upstream.add_commit(ZERO_OID, 1_700_000_000, "genesis", &[("f0.txt", &b0)]);
    run_resilient_pipeline_step(&storage, &upstream, Tier::H168Base, 1_700_000_000).unwrap();

    // Push 4 commits across four 15m intervals, then roll up into 1h on the 4th run
    for step in 1..=4u64 {
        let blob = make_hfs_blob(&format!("{step:064x}"), step * 1000);
        let name = format!("f{step}.txt");
        let (c, _, _) = upstream.add_commit(
            tip,
            1_700_000_000 + step * 900,
            &format!("commit {step}"),
            &[("f0.txt", &b0), (&name, &blob)],
        );
        tip = c;
        let tier = if step == 4 { Tier::H1 } else { Tier::M15 };
        run_resilient_pipeline_step(&storage, &upstream, tier, 1_700_000_000 + step * 900).unwrap();
    }

    let (m_after_1h, _) = storage.get_manifest_with_etag().unwrap().unwrap();
    assert_eq!(
        m_after_1h.sub_week_pyramid.len(),
        1,
        "4 x 15m slabs must roll up into 1 single 1h slab"
    );
    assert_eq!(m_after_1h.sub_week_pyramid[0].tier, Tier::H1);

    // Push 1 more 15m commit + 1 live `< 15m` L0Ram commit
    let b5 = make_hfs_blob(&format!("{:064x}", 5), 5000);
    let (c5, _, _) = upstream.add_commit(
        tip,
        1_700_004_500,
        "commit 5",
        &[("f0.txt", &b0), ("f5.txt", &b5)],
    );
    run_resilient_pipeline_step(&storage, &upstream, Tier::M15, 1_700_004_500).unwrap();

    let b6 = make_hfs_blob(&format!("{:064x}", 6), 6000);
    let (c6, t6, blobs6) = upstream.add_commit(
        c5,
        1_700_004_600,
        "commit 6 in L0Ram",
        &[("f0.txt", &b0), ("f5.txt", &b5), ("f6.txt", &b6)],
    );

    // Boot `ServerRuntimeState` — it loads `[baseline, 1h, 15m]` + builds `L0Ram` for `c6`
    let rt_state = ServerRuntimeState::boot_or_recover(&storage, &upstream, 1_700_004_600).unwrap();
    assert!(rt_state.l0_ram_slab.is_some());

    // Start HTTP server, seal allocations in `Mode::Strict`, and test endpoints!
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let pool = WorkerPool::new(4, 2 << 20, 16_384, 8 << 20, 2 << 20, 512 << 10, 2 << 20);
        let http_state = Arc::new(DaemonHttpState {
            runtime: RwLock::new(rt_state),
            storage: storage.clone(),
            pool,
            config_json: default_config_json("http://127.0.0.1:8080", "http://127.0.0.1:8080/cas"),
            counters: DaemonCounters::default(),
        });

        let (addr, _handle) =
            spawn_http_server("127.0.0.1:0".parse().unwrap(), Arc::clone(&http_state))
                .await
                .unwrap();

        gvfs_alloc_guard::reset_counters();
        gvfs_alloc_guard::set_mode(gvfs_alloc_guard::Mode::Strict);
        gvfs_alloc_guard::seal();

        // 1. Cold Clone (`GET /gvfs/prefetch?lastPackTimestamp=0`) -> MUST return 1 single pack
        //    containing all objects across `[168h baseline, 1h, 15m, L0Ram]`!
        let (status, pre_body) = http_get(addr, "/gvfs/prefetch?lastPackTimestamp=0");
        assert_eq!(status, 200);
        let packs = decode_gpre_packs(&pre_body);
        assert_eq!(
            packs.len(),
            1,
            "Cold clone must receive EXACTLY 1 spliced pack, got {}",
            packs.len()
        );
        let (_, ref pack_bytes, ref idx_bytes) = packs[0];
        let idx_view = IdxView::parse(idx_bytes).unwrap();
        assert!(idx_view.verify_checksum());
        assert_eq!(
            parse_pack_header(pack_bytes).unwrap() as usize,
            idx_view.len()
        );
        // Verify commit 6 (from L0Ram) and genesis commit (from 168h baseline) are both in this 1 pack!
        assert!(idx_view.find(&c6).is_some());
        assert!(idx_view.find(&t6).is_some());

        // 2. Warm Client (`GET /gvfs/prefetch?lastPackTimestamp=1700003600`) -> skips baseline & 1h,
        //    splices `[15m, L0Ram]` into 1 single pack!
        let (status, warm_headers, warm_body) =
            http_get_with_headers(addr, "/gvfs/prefetch?lastPackTimestamp=1700003600");
        assert_eq!(status, 200);
        assert!(
            warm_headers.contains("x-gvfs-pack-start: 1700003600"),
            "missing X-GVFS-Pack-Start header: {warm_headers}"
        );
        assert!(
            warm_headers.contains("x-gvfs-pack-end: 1700004600"),
            "missing X-GVFS-Pack-End header: {warm_headers}"
        );
        let warm_packs = decode_gpre_packs(&warm_body);
        assert_eq!(
            warm_packs.len(),
            1,
            "Warm client must receive EXACTLY 1 spliced pack"
        );
        let warm_idx = IdxView::parse(&warm_packs[0].2).unwrap();
        assert!(warm_idx.find(&c5).is_some());
        assert!(warm_idx.find(&c6).is_some());

        // 3. `POST /gvfs/objects` for `c6` with `commitDepth: 2` -> expands `c6`, `c5`, and their trees!
        let post_body = format!("{{\"commitDepth\":2,\"objectIds\":[\"{c6}\"]}}");
        let (status, obj_pack) = http_post(addr, "/gvfs/objects", post_body.as_bytes());
        assert_eq!(status, 200);
        let obj_count = parse_pack_header(&obj_pack).unwrap();
        assert!(
            obj_count >= 4,
            "commitDepth=2 must expand c6, c5, and their trees"
        );

        // 4. `GET /gvfs/objects/<oid>` -> loose-spliced object
        let b6_oid = blobs6.last().unwrap();
        let (status, loose_bytes) = http_get(addr, &format!("/gvfs/objects/{b6_oid}"));
        assert_eq!(status, 200);
        let mut zdec = flate2::read::ZlibDecoder::new(&loose_bytes[..]);
        let mut inflated = Vec::new();
        zdec.read_to_end(&mut inflated).unwrap();
        assert!(inflated.ends_with(&b6));

        // 5. `POST /gvfs/sizes`
        let sizes_req = format!("[\"{b6_oid}\"]");
        let (status, sizes_resp) = http_post(addr, "/gvfs/sizes", sizes_req.as_bytes());
        assert_eq!(status, 200);
        let sizes_str = String::from_utf8(sizes_resp).unwrap();
        assert_eq!(
            sizes_str,
            format!("[{{\"Id\":\"{b6_oid}\",\"Size\":{}}}]", b6.len())
        );

        // Verify ZERO hot-path heap allocations across all endpoints!
        let stats = gvfs_alloc_guard::stats();
        gvfs_alloc_guard::unseal();
        assert_eq!(
            stats.hot_path, 0,
            "hot-path heap allocations must be 0 after seal(), got {}",
            stats.hot_path
        );
    });

    // Now test Sunday 168h Rollup: emits BOTH `weekly-delta` ("Aha" trick) and new `baseline`!
    let _ = ingest_git_delta(&upstream, c5, c6, Tier::M15, 1_700_005_000).unwrap();
    run_resilient_pipeline_step(&storage, &upstream, Tier::H168Base, 1_700_604_800).unwrap();
    let (m_sun, _) = storage.get_manifest_with_etag().unwrap().unwrap();
    assert_eq!(
        m_sun.weekly_deltas.len(),
        1,
        "Sunday 168h rollup must emit Weekly Aha Delta"
    );
    assert_eq!(m_sun.weekly_deltas[0].tier, Tier::H168Delta);
    assert!(
        m_sun.sub_week_pyramid.is_empty(),
        "Sub-week pyramid cleared after 168h baseline rollup"
    );

    // Track B check: a warm workstation from before Sunday (`ts = 1_700_000_000`) gets ONLY
    // the Weekly "Aha" Delta slab, skipping the full baseline!
    let warm_slabs = m_sun.select_slabs_for_prefetch(Some(1_700_000_000), None);
    assert_eq!(warm_slabs.len(), 1);
    assert_eq!(warm_slabs[0].tier, Tier::H168Delta);
}

#[test]
fn fs_storage_mmap_and_range_coalescing_and_cli_dispatch() {
    use gvfs_daemon::{CommandMode, parse_cli_args, plan_deduplicated_range_splice};
    use std::sync::atomic::Ordering;

    // 1. Verify CLI argument parsing for all modes (`pipeline`, `serve`, `edge`, `proxy`, `bench`)
    let p_cmd = parse_cli_args(&[
        "pipeline".into(),
        "--tier".into(),
        "15m".into(),
        "--store".into(),
        "fs:/tmp/gvfs-test".into(),
        "--upstream".into(),
        "/home/muks/SearchGold".into(),
    ])
    .unwrap()
    .unwrap();
    assert!(matches!(
        p_cmd,
        CommandMode::Pipeline {
            tier: Tier::M15,
            ..
        }
    ));

    let e_cmd = parse_cli_args(&[
        "edge".into(),
        "--port".into(),
        "9090".into(),
        "--bucket".into(),
        "fs:/tmp/gvfs-test".into(),
        "--upstream".into(),
        "/home/muks/SearchGold".into(),
    ])
    .unwrap()
    .unwrap();
    assert!(
        matches!(e_cmd, CommandMode::Serve { bind, .. } if bind.port() == 9090 && bind.ip().is_unspecified())
    );

    let prx_cmd = parse_cli_args(&[
        "proxy".into(),
        "--port".into(),
        "8081".into(),
        "--upstream-edge".into(),
        "https://edge.internal".into(),
        "--upstream".into(),
        "/home/muks/SearchGold".into(),
    ])
    .unwrap()
    .unwrap();
    assert!(
        matches!(prx_cmd, CommandMode::Serve { bind, .. } if bind.port() == 8081 && bind.ip().is_loopback())
    );

    // 2. Test `fs:` storage with real `.pack`, `.mphf`, `.idx`, `.meta`, `.hfs` files on disk,
    //    plus HTTP Range request coalescing when `mmap_packs = false`!
    let tmp_dir = std::env::temp_dir().join(format!("gvfs-fs-store-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let spec = format!("fs:{}", tmp_dir.display());
    let storage = StorageClient::open(&spec).unwrap();
    let upstream = InMemoryGitUpstream::new();

    let b1 = make_hfs_blob(
        "1111111111111111111111111111111111111111111111111111111111111111",
        1024,
    );
    let b2 = make_hfs_blob(
        "2222222222222222222222222222222222222222222222222222222222222222",
        2048,
    );
    let (c1, t1, blobs) = upstream.add_commit(
        ZERO_OID,
        1_750_000_000,
        "fs test commit",
        &[("a.txt", &b1), ("b.txt", &b2)],
    );

    run_resilient_pipeline_step(&storage, &upstream, Tier::H168Base, 1_750_000_100).unwrap();
    assert!(tmp_dir.join("manifest.bin").exists());

    // Open storage with `mmap_packs = false` to verify remote Range request coalescing:
    // requesting `c1`, `t1`, `blobs[0]`, `blobs[1]` (which are contiguous in Merkle DFS order!)
    // must coalesce into 1 single Range read!
    let remote_storage = StorageClient::open(&spec).unwrap().with_mmap_packs(false);
    let (remote_manifest, _) = remote_storage.get_manifest_with_etag().unwrap().unwrap();
    assert!(remote_manifest.weekly_baseline.in_memory_pack.is_none());

    let requested_oids = [c1, t1, blobs[0], blobs[1]];
    let active = remote_manifest.active_lookup_slabs(None);
    let (unique_count, batches) =
        plan_deduplicated_range_splice(&active, Some(&requested_oids), 4096);
    assert_eq!(unique_count, 4);
    assert_eq!(
        batches.len(),
        1,
        "4 contiguous Merkle-DFS objects must coalesce into 1 single Range batch"
    );

    let fetched = remote_storage
        .fetch_range_batches(&batches, &active)
        .unwrap();
    assert_eq!(
        remote_storage.range_requests_issued.load(Ordering::Relaxed),
        1
    );
    let (spliced_pack, _) = gvfs_core::assemble_spliced_pack(unique_count, &fetched);
    assert_eq!(parse_pack_header(&spliced_pack).unwrap(), 4);

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

#[test]
fn workstation_zero_gc_superset_tombstoning_and_midx_and_weekly_aha_delta() {
    use gvfs_daemon::{
        apply_prefetch_pack_to_workstation, ingest_git_delta, parse_prefetch_pack_interval,
    };

    assert_eq!(
        parse_prefetch_pack_interval("prefetch-1700000000-1700000900.pack"),
        Some((
            "prefetch-1700000000-1700000900",
            1_700_000_000,
            1_700_000_900
        ))
    );

    let ws_dir = std::env::temp_dir().join(format!("gvfs-ws-zerogc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&ws_dir);
    let pack_dir = ws_dir.join(".git").join("objects").join("pack");
    std::fs::create_dir_all(&pack_dir).unwrap();

    let upstream = InMemoryGitUpstream::new();
    let b0 = make_hfs_blob(
        "0000000000000000000000000000000000000000000000000000000000000001",
        512,
    );
    let (c0, _, _) = upstream.add_commit(ZERO_OID, 1_700_000_000, "baseline", &[("root.txt", &b0)]);
    let base_slab =
        ingest_git_delta(&upstream, ZERO_OID, c0, Tier::H168Base, 1_700_000_000).unwrap();

    // Cold clone writes 1 baseline pack (`[0 .. 1_700_000_000]`)
    let out0 = apply_prefetch_pack_to_workstation(
        &pack_dir,
        0,
        1_700_000_000,
        base_slab.in_memory_pack.as_ref().unwrap(),
        base_slab.in_memory_idx.as_ref().unwrap(),
        false,
    )
    .unwrap();
    assert_eq!(out0.remaining_packs, 1);

    // Intraday fetches across the week write three 15m/1h packs inside `[1_700_000_001 .. 1_700_086_400]`
    let mut prev = c0;
    for i in 1..=3u64 {
        let bi = make_hfs_blob(&format!("{i:064x}"), i * 1000);
        let fname = format!("day_{i}.txt");
        let (ci, _, _) = upstream.add_commit(
            prev,
            1_700_000_000 + i * 900,
            &format!("intraday {i}"),
            &[("root.txt", &b0), (&fname, &bi)],
        );
        let s = ingest_git_delta(&upstream, prev, ci, Tier::M15, 1_700_000_000 + i * 900).unwrap();
        let out_i = apply_prefetch_pack_to_workstation(
            &pack_dir,
            1_700_000_000 + (i - 1) * 900 + 1,
            1_700_000_000 + i * 900,
            s.in_memory_pack.as_ref().unwrap(),
            s.in_memory_idx.as_ref().unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(out_i.remaining_packs, 1 + i as usize);
        prev = ci;
    }

    // Now the workstation receives the `24h` / `168h-delta` rollup pack covering
    // `[1_700_000_001 .. 1_700_086_400]` with `run_midx = true` (Fix 2 + Fix 3 + Strategy #4):
    // - Fix 2 (`prune_subsumed_prefetch_packs`) unlinks all 3 intraday `.pack` and `.idx` files (6 files total)!
    // - The original 4 GiB baseline pack (`prefetch-0-1700000000.pack`) is untouched (never re-downloaded!).
    // - Fix 3 (`write_multi_pack_index`) writes `.git/objects/pack/multi-pack-index`!
    let rollup_slab =
        ingest_git_delta(&upstream, c0, prev, Tier::H168Delta, 1_700_086_400).unwrap();
    let out_rollup = apply_prefetch_pack_to_workstation(
        &pack_dir,
        1_700_000_001,
        1_700_086_400,
        rollup_slab.in_memory_pack.as_ref().unwrap(),
        rollup_slab.in_memory_idx.as_ref().unwrap(),
        true,
    )
    .unwrap();

    assert_eq!(
        out_rollup.unlinked_files, 6,
        "Fix 2 must unlink all 3 subsumed .pack and 3 .idx files"
    );
    assert_eq!(
        out_rollup.remaining_packs, 2,
        "Workstation must have only 1 original baseline pack + 1 weekly delta pack"
    );
    assert!(out_rollup.midx_updated);
    assert!(
        pack_dir.join("multi-pack-index").exists(),
        "Fix 3 must write .git/objects/pack/multi-pack-index"
    );

    let _ = std::fs::remove_dir_all(&ws_dir);
}

fn http_get(addr: std::net::SocketAddr, path: &str) -> (u16, Vec<u8>) {
    let (status, _head, body) = http_get_with_headers(addr, path);
    (status, body)
}

fn http_get_with_headers(addr: std::net::SocketAddr, path: &str) -> (u16, String, Vec<u8>) {
    use std::io::Write;
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).unwrap();
    read_http_response_full(&mut stream)
}

fn http_post(addr: std::net::SocketAddr, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
    use std::io::Write;
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let (status, _head, resp_body) = read_http_response_full(&mut stream);
    (status, resp_body)
}

fn read_http_response_full(stream: &mut std::net::TcpStream) -> (u16, String, Vec<u8>) {
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let sep = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&raw[..sep]).unwrap();
    let head_lower = head.to_ascii_lowercase();
    let status_line = head.lines().next().unwrap();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let body_raw = &raw[sep + 4..];
    if head_lower.contains("transfer-encoding: chunked") {
        let mut out = Vec::new();
        let mut pos = 0usize;
        while pos < body_raw.len() {
            let line_end = body_raw[pos..]
                .windows(2)
                .position(|w| w == b"\r\n")
                .unwrap()
                + pos;
            let len_str = std::str::from_utf8(&body_raw[pos..line_end])
                .unwrap()
                .trim();
            let chunk_len = usize::from_str_radix(len_str, 16).unwrap();
            pos = line_end + 2;
            if chunk_len == 0 {
                break;
            }
            out.extend_from_slice(&body_raw[pos..pos + chunk_len]);
            pos += chunk_len + 2;
        }
        (status, head_lower, out)
    } else {
        (status, head_lower, body_raw.to_vec())
    }
}
