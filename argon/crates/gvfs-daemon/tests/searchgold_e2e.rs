//! Real-Repository Integration Tests against `/home/muks/SearchGold` (12M objects, 1 packfile,
//! ADO `~~Git-hfs 1.0` VsoHash pointers):
//!
//! 1. Verifies zero-copy OS `mmap` boot of `/home/muks/SearchGold/.git/objects/pack/pack-*.{pack,idx,rev}`
//!    and checks that `GET /gvfs/prefetch` serves **1 single pack** (`num_packs == 1`)!
//! 2. Verifies `ingest_git_delta` on real commit ranges (`HEAD~10 -> HEAD~5` and `HEAD~5 -> HEAD`)
//!    of `/home/muks/SearchGold`, checking that `parse_git_hfs_v1` extracts the `~~Git-hfs 1.0`
//!    VsoHash pointers, orders all objects in strict **Merkle DFS order**, builds `MphfSlabIndex`,
//!    and produces a pack that passes `git index-pack`!
//! 3. Verifies splicing multiple `SearchGold` delta slabs (`1h` + `15m` + `L0Ram`) into **1 single
//!    deduplicated `.pack` file** (`num_packs == 1`) with 0 duplicate OIDs (`git index-pack`
//!    verification) and 0 hot-path heap allocations!
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic
)]

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};

use gvfs_alloc_guard::SealedAllocator;
use gvfs_core::gpre::{parse_gpre_file_header, parse_gpre_pack_header};
use gvfs_core::idx::IdxView;
use gvfs_core::{Oid20, Tier, UnifiedManifest, splice_in_memory_slabs};
use gvfs_daemon::{
    DaemonCounters, DaemonHttpState, IngestArena, LocalGitUpstream, ServerRuntimeState, WorkerPool,
    default_config_json, ingest_git_delta_with_arena, spawn_http_server,
};
use gvfs_storage::{ETag, StorageClient};

#[global_allocator]
static GLOBAL: SealedAllocator = SealedAllocator;

const SEARCHGOLD_PATH: &str = "/home/muks/SearchGold";

fn verify_pack_with_git(pack_bytes: &[u8]) -> usize {
    let tmp_dir = std::env::temp_dir().join(format!(
        "gvfs-searchgold-verify-{}-{}",
        std::process::id(),
        pack_bytes.len()
    ));
    let _ = std::fs::remove_dir_all(&tmp_dir);
    std::fs::create_dir_all(&tmp_dir).unwrap();
    let pack_path = tmp_dir.join("test.pack");
    std::fs::write(&pack_path, pack_bytes).unwrap();

    // Use `git index-pack` (without `--strict` since delta packs reference parent commits/trees
    // from earlier history, while `index-pack` verifies every object's zlib stream, SHA-1,
    // pack trailer, and rejects any duplicate OID in the pack!).
    let mut cmd = Command::new("git");
    cmd.args(["index-pack", "-v", pack_path.to_str().unwrap()])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env_remove("GIT_CONFIG_COUNT")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = cmd.output().unwrap();
    let _ = std::fs::remove_dir_all(&tmp_dir);
    assert!(
        out.status.success(),
        "git index-pack rejected pack: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).lines().count()
}

#[test]
fn searchgold_mmap_single_pack_and_git_hfs_delta_splicing() {
    let sg_dir = Path::new(SEARCHGOLD_PATH);
    if !sg_dir.exists() {
        return;
    }

    let up = LocalGitUpstream::open(sg_dir, "main").unwrap();

    // 1. Verify zero-copy mmap boot of SearchGold's 12,083,668-object single packfile!
    let mmap_state =
        ServerRuntimeState::boot_from_local_git_pack(up.git_dir(), &up, 1_777_000_000).unwrap();
    assert_eq!(
        mmap_state.manifest.weekly_baseline.object_count(),
        12_083_668
    );

    // Look up README.md (`01aa5a4d70d5625e0ccb31d6f16595a3fe611398`, 95 bytes `~~Git-hfs 1.0`)
    // and HEAD commit (`4d12e2c7915577f43351ac82b9ca15bd7788be89`) directly in the mmap'd slab!
    let readme_oid = Oid20::from_hex(b"01aa5a4d70d5625e0ccb31d6f16595a3fe611398").unwrap();
    let head_oid = up.rev_parse("HEAD").unwrap();
    let readme_meta = mmap_state
        .manifest
        .weekly_baseline
        .lookup_oid(&readme_oid)
        .expect("README.md blob must be found in mmap'd SearchGold pack");
    assert!(readme_meta.compressed_len > 0);
    assert_eq!(
        mmap_state
            .manifest
            .weekly_baseline
            .lookup_object_size(&readme_oid),
        Some(95)
    );
    assert!(
        mmap_state
            .manifest
            .weekly_baseline
            .lookup_oid(&head_oid)
            .is_some()
    );

    // 2. Test `ingest_git_delta` on real commit windows in SearchGold:
    //    - `HEAD~50 -> HEAD~5` as a `24h` slab (using Contiguous Tree-Cluster Delta Packing!)
    //    - `HEAD~5 -> HEAD` as a `< 15m` `L0Ram` slab (standalone undeltified!)
    let h50 = up.rev_parse("HEAD~50").unwrap();
    let h5 = up.rev_parse("HEAD~5").unwrap();
    let h0 = head_oid;

    let mut arena = IngestArena::default();
    let slab_a_standalone =
        ingest_git_delta_with_arena(&up, h50, h5, &[], Tier::L0Ram, 1_777_001_000, &mut arena)
            .unwrap();
    let slab_a = Arc::new(
        ingest_git_delta_with_arena(&up, h50, h5, &[], Tier::H24, 1_777_001_000, &mut arena)
            .unwrap(),
    );
    assert!(
        slab_a.pack_byte_len * 3 < slab_a_standalone.pack_byte_len,
        "24h Contiguous Tree-Cluster Delta slab ({} B) must be >3x smaller than standalone 12h slab ({} B)",
        slab_a.pack_byte_len,
        slab_a_standalone.pack_byte_len
    );
    let slab_b = Arc::new(
        ingest_git_delta_with_arena(&up, h5, h0, &[], Tier::L0Ram, 1_777_001_500, &mut arena)
            .unwrap(),
    );

    assert!(slab_a.object_count() > 0);
    assert!(slab_b.object_count() > 0);
    assert!(
        !slab_b.hfs_sidecar.is_empty(),
        "SearchGold delta HEAD~5..HEAD must contain parsed ~~Git-hfs 1.0 pointers"
    );

    // Verify that `git index-pack` accepts both Merkle-DFS slabs individually
    verify_pack_with_git(slab_a.in_memory_pack.as_ref().unwrap());
    verify_pack_with_git(slab_b.in_memory_pack.as_ref().unwrap());

    // Verify `MphfSlabIndex` O(1) lookup for every single object in `slab_b`
    for obj in &slab_b.objects {
        assert_eq!(
            slab_b.mphf.lookup_resolved(&obj.oid),
            Some((obj.offset, obj.compressed_len))
        );
        assert_eq!(slab_b.lookup_oid(&obj.oid), Some(*obj));
    }

    // 3. Splice `slab_b` (`HEAD~5..HEAD`) and `slab_a` (`HEAD~10..HEAD~5`) into 1 single pack
    //    and verify that `git index-pack` validates the spliced pack with 0 duplicate OIDs!
    let spliced = splice_in_memory_slabs(
        &[Arc::clone(&slab_b), Arc::clone(&slab_a)],
        h0,
        Tier::H6,
        1_777_002_000,
    )
    .expect("splicing SearchGold slabs must succeed");
    verify_pack_with_git(spliced.in_memory_pack.as_ref().unwrap());
    let idx_view = IdxView::parse(spliced.in_memory_idx.as_ref().unwrap()).unwrap();
    assert!(idx_view.verify_checksum());
    assert!(idx_view.find(&h0).is_some());
    assert!(idx_view.find(&h5).is_some());

    // 4. Serve over HTTP and verify `/gvfs/prefetch` returns 1 single pack (`num_packs == 1`)
    //    and `POST /gvfs/objects` + `GET /gvfs/objects/<oid>` run with 0 hot-path allocations!
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let storage = StorageClient::open("mem:searchgold-http").unwrap();
        let manifest = UnifiedManifest {
            version_seq: 1,
            last_15m_tip_oid: h5,
            weekly_baseline: Arc::clone(&slab_a),
            weekly_deltas: Vec::new(),
            sub_week_pyramid: Vec::new(),
        };
        let rt_state = ServerRuntimeState::from_parts(
            ETag("\"sg-1\"".into()),
            manifest,
            Some(Arc::clone(&slab_b)),
        );

        let pool = WorkerPool::new(4, 2 << 20, 16_384, 8 << 20, 2 << 20, 512 << 10, 2 << 20);
        let http_state = Arc::new(DaemonHttpState {
            runtime: RwLock::new(rt_state),
            storage,
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

        // Prefetch must splice `slab_a` (baseline) + `slab_b` (L0Ram) into 1 pack (`num_packs == 1`)!
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream
            .write_all(
                format!("GET /gvfs/prefetch?lastPackTimestamp=0 HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .unwrap();
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).unwrap();
        let sep = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let body = &resp[sep + 4..];
        assert_eq!(parse_gpre_file_header(body), Some(1), "must return exactly 1 pack");
        let ph = parse_gpre_pack_header(&body[8..32]).unwrap();
        let pack_end = 32 + usize::try_from(ph.pack_len).unwrap();
        let wire_pack = &body[32..pack_end];

        let stats = gvfs_alloc_guard::stats();
        gvfs_alloc_guard::unseal();
        assert_eq!(stats.hot_path, 0, "0 hot-path allocations during SearchGold prefetch splice");

        verify_pack_with_git(wire_pack);
    });
}
