//! End-to-end tests: the `pipeline` and `serve` commands of the real binary against a
//! scratch upstream repository, with every byte the server sends checked by git itself.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

mod common;

use common::*;
use gvfs_daemon::slab::Tier;

fn store_spec(dir: &std::path::Path) -> String {
    format!("fs:{}", dir.display())
}

/// Genesis, then `serve` with a live upstream: config, cold/warm/up-to-date prefetch (one
/// pack each, with git's exact .idx unless the client is git), L0 pick-up of pushes (one
/// slab each, stamped as the pipeline later stamps the same tips), loose GETs, POST
/// batches and commit closures, sizes, sealing of L0 by the next pipeline run, and zero
/// hot-path allocations throughout.
#[test]
fn serve_end_to_end_with_live_l0() {
    let root = tempdir("serve");
    let mut up = Upstream::new(&root);
    for _ in 0..3 {
        up.commit(6);
    }
    let store_dir = root.join("store");
    let store = store_spec(&store_dir);
    let log = pipeline(&root, &store, &up, "15m", &[]);
    assert!(log.contains("genesis"), "{log}");
    let m = manifest(&store_dir);
    assert_eq!(
        (m.generation, m.tip.to_string(), m.pyramid.len()),
        (1, up.tip(), 0)
    );
    assert_eq!(m.baseline.objects as usize, up.objects(&up.tip()).len());

    let cache = root.join("cache");
    let server = Server::start(&[
        "--store",
        &store,
        "--upstream",
        up.bare.to_str().unwrap(),
        "--poll-ms",
        "100",
        "--cache-dir",
        cache.to_str().unwrap(),
    ]);

    let (status, ct, body) = server.get("/org/repo/gvfs/config");
    assert_eq!((status, ct.as_str()), (200, "application/json"));
    assert!(body.starts_with(b"{"));

    // Cold clone: exactly one pack with everything.
    let tip1 = up.tip();
    let packs = server.prefetch(0);
    assert_eq!(packs.len(), 1);
    let (ts1, pack) = &packs[0];
    assert_eq!(*ts1, m.baseline.end_ts);
    let client = fresh_repo(&root, "client");
    install_pack(&client, pack);
    assert_complete(&client, &tip1);
    assert!(server.prefetch(*ts1).is_empty(), "up to date");
    // gvfs-helper (`User-Agent: git/...`) gets the same pack, without the .idx it would discard.
    let as_git = server.prefetch_git(0);
    assert_eq!((as_git.len(), as_git[0].0, &as_git[0].1), (1, *ts1, pack));

    // Pushes reach the edge's in-RAM L0 slab through the same ingest.
    up.commit(5);
    let tip2 = up.commit(5);
    server.wait_for("the new tip in L0", |s| {
        s.get(&format!("/gvfs/objects/{tip2}")).0 == 200
    });
    assert!(server.metric("gvfs_l0_slabs") >= 1);

    // Warm fetch: one pack with the new objects.
    let packs = server.prefetch(*ts1);
    assert_eq!(packs.len(), 1);
    let (ts2, pack) = &packs[0];
    assert!(*ts2 > *ts1);
    install_pack(&client, pack);
    assert_complete(&client, &tip2);
    assert!(server.prefetch(*ts2).is_empty());

    // Cold clone now: baseline + L0 spliced into one pack.
    let packs = server.prefetch(0);
    assert_eq!(packs.len(), 1);
    assert_eq!(packs[0].0, *ts2);
    let cold = fresh_repo(&root, "cold");
    install_pack(&cold, &packs[0].1);
    assert_complete(&cold, &tip2);

    // Every object as a loose object: git re-hashes each one (fsck) and the contents match.
    let all = up.objects(&tip2);
    let loose = fresh_repo(&root, "loose");
    for oid in &all {
        let (status, ct, body) = server.get(&format!("/org/repo/gvfs/objects/{oid}"));
        assert_eq!((status, ct.as_str()), (200, CT_LOOSE), "{oid}");
        let dir = loose.join("objects").join(&oid[..2]);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(&oid[2..]), body).unwrap();
    }
    git(&loose, &["fsck", "--strict", "--no-dangling"]);
    assert_eq!(cat_batch(&loose, &all), cat_batch(&up.bare, &all));
    assert_eq!(
        server.get(&format!("/gvfs/objects/{}", "0".repeat(40))).0,
        404
    );

    // POST a batch of trees and blobs: one self-contained pack (index-pack would reject
    // missing delta bases).
    let kinds = types(&up.bare, &all);
    let batch: Vec<String> = kinds
        .iter()
        .filter(|(_, t)| t != "commit")
        .map(|(o, _)| o.clone())
        .collect();
    let (status, ct, pack) =
        server.post("/gvfs/objects", json_ids(&batch, None).as_bytes(), CT_PACK);
    assert_eq!((status, ct.as_str()), (200, CT_PACK));
    let posted = fresh_repo(&root, "post");
    assert_eq!(install_pack(&posted, &pack), batch.len());
    assert_eq!(cat_batch(&posted, &batch), cat_batch(&up.bare, &batch));

    // A single blob with a loose-object Accept comes back loose.
    let blob = kinds
        .iter()
        .find(|(_, t)| t == "blob")
        .map(|(o, _)| o.clone())
        .unwrap();
    let (status, ct, _) = server.post(
        "/gvfs/objects",
        json_ids(std::slice::from_ref(&blob), None).as_bytes(),
        "application/x-git-loose-object",
    );
    assert_eq!((status, ct.as_str()), (200, CT_LOOSE));

    // A commit with commitDepth 2: both commits and every tree of both (the GVFS expansion).
    let (status, _, pack) = server.post(
        "/gvfs/objects",
        json_ids(std::slice::from_ref(&tip2), Some(2)).as_bytes(),
        CT_PACK,
    );
    assert_eq!(status, 200);
    let closure = fresh_repo(&root, "closure");
    install_pack(&closure, &pack);
    let parent = git(&up.bare, &["rev-parse", &format!("{tip2}^")])
        .trim()
        .to_string();
    let mut expect = vec![tip2.clone(), parent.clone()];
    for c in [&tip2, &parent] {
        expect.push(
            git(&up.bare, &["rev-parse", &format!("{c}^{{tree}}")])
                .trim()
                .to_string(),
        );
        expect.extend(
            git(
                &up.bare,
                &["ls-tree", "-r", "-d", "--format=%(objectname)", c],
            )
            .lines()
            .map(str::to_string),
        );
    }
    assert_eq!(cat_batch(&closure, &expect), cat_batch(&up.bare, &expect));
    assert_eq!(
        server
            .post(
                "/gvfs/objects",
                json_ids(&["1".repeat(40)], None).as_bytes(),
                CT_PACK
            )
            .0,
        404
    );

    // Sizes come from the slab records (no inflation).
    let (status, _, body) = server.post(
        "/gvfs/sizes",
        format!(
            "[{}]",
            all.iter()
                .map(|o| format!("\"{o}\""))
                .collect::<Vec<_>>()
                .join(",")
        )
        .as_bytes(),
        "application/json",
    );
    assert_eq!(status, 200);
    let got = String::from_utf8(body).unwrap();
    let want = git_stdin(
        &up.bare,
        &[
            "cat-file",
            "--batch-check={\"Id\":\"%(objectname)\",\"Size\":%(objectsize)}",
        ],
        all.join("\n").as_bytes(),
    );
    let want = format!(
        "[{}]",
        String::from_utf8(want)
            .unwrap()
            .trim_end()
            .replace('\n', ",")
    );
    assert_eq!(got, want);

    // The next pipeline run seals what L0 held, stamped exactly as the edge stamped the
    // same tip. L0 leaves the live set, and a client whose cursor came from L0 is up to date.
    pipeline(&root, &store, &up, "15m", &[]);
    let m = manifest(&store_dir);
    assert_eq!(
        (m.generation, m.pyramid.len(), m.tip.to_string(), m.max_ts),
        (2, 1, tip2.clone(), *ts2)
    );
    server.wait_for("the new manifest", |s| {
        s.metric("gvfs_manifest_generation") == 2
    });
    server.wait_for("L0 to leave the live set", |s| {
        s.metric("gvfs_l0_slabs") == 0
    });
    assert!(
        server.prefetch(*ts2).is_empty(),
        "nothing new for a client that took L0"
    );
    // A client still on the genesis pack gets the sealed range, as one pack.
    let packs = server.prefetch(*ts1);
    assert_eq!((packs.len(), packs[0].0), (1, m.max_ts));
    let late = fresh_repo(&root, "late");
    install_pack(&late, &server.prefetch(0)[0].1);
    assert_complete(&late, &tip2);
    let packs = server.prefetch(0);
    assert_eq!(packs.len(), 1);
    let cold2 = fresh_repo(&root, "cold2");
    install_pack(&cold2, &packs[0].1);
    assert_complete(&cold2, &tip2);

    // Two more pushes: one L0 slab each, every commit fetched once. A client at the sealed
    // slab gets both, merged into one pack.
    let sealed_ts = m.pyramid[0].end_ts;
    let tip3 = up.commit(3);
    server.wait_for("tip3 in L0", |s| {
        s.get(&format!("/gvfs/objects/{tip3}")).0 == 200
    });
    let ts3 = server.prefetch_git(sealed_ts)[0].0;
    let tip4 = up.commit(3);
    server.wait_for("tip4 in L0", |s| {
        s.get(&format!("/gvfs/objects/{tip4}")).0 == 200
    });
    assert_eq!(server.metric("gvfs_l0_slabs"), 2);
    let packs = server.prefetch(sealed_ts);
    assert_eq!(packs.len(), 1);
    let ts4 = packs[0].0;
    assert!(ts4 > ts3 && ts3 > sealed_ts);
    install_pack(&client, &packs[0].1);
    assert_complete(&client, &tip4);

    // The pipeline seals only tip3 (its branch lags the edge's), at the edge's stamp for
    // tip3. L0 keeps tip4's slab live: a client at the new sealed slab gets exactly tip4's
    // objects, and one whose cursor came from L0 has nothing to fetch.
    git(&up.bare, &["branch", "-f", "sealed", &tip3]);
    pipeline(&root, &store, &up, "15m", &["--branch", "sealed"]);
    let m = manifest(&store_dir);
    assert_eq!(
        (m.generation, m.tip.to_string(), m.max_ts),
        (3, tip3.clone(), ts3)
    );
    server.wait_for("the third manifest", |s| {
        s.metric("gvfs_manifest_generation") == 3
    });
    assert_eq!(server.metric("gvfs_l0_slabs"), 1);
    let only_tip4 = git(
        &up.bare,
        &["rev-list", "--objects", &tip4, &format!("^{tip3}")],
    )
    .lines()
    .count();
    let packs = server.prefetch(m.max_ts);
    assert_eq!((packs.len(), packs[0].0), (1, ts4));
    assert_eq!(
        install_pack(&fresh_repo(&root, "l0-only"), &packs[0].1),
        only_tip4
    );
    let at_tip3 = fresh_repo(&root, "at-tip3");
    install_pack(&at_tip3, &server.prefetch(0)[0].1);
    assert_complete(&at_tip3, &tip4);
    assert!(server.prefetch(ts4).is_empty());

    // `git pull`, then push: a merge whose first parent is a side commit made with a much
    // older clock, its second the tip. Edge and pipeline stamp it alike, above the tip, so
    // the client at the tip's cursor gets the side commit and the merge, before and after
    // the pipeline seals them.
    let old_clock = "1000000000 +0000";
    let run = |args: &[&str]| {
        let out = git_cmd(&up.work)
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(args)
            .env("GIT_COMMITTER_DATE", old_clock)
            .env("GIT_AUTHOR_DATE", old_clock)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["checkout", "-q", "-b", "side", &tip3]);
    std::fs::write(up.work.join("side.txt"), "side\n").unwrap();
    run(&["add", "side.txt"]);
    run(&["commit", "-q", "-m", "side"]);
    run(&["merge", "-q", "--no-ff", "-m", "merge", &tip4]);
    run(&["push", "-q", "origin", "side:main"]);
    let merge = up.tip();
    server.wait_for("the merge in L0", |s| {
        s.get(&format!("/gvfs/objects/{merge}")).0 == 200
    });
    let packs = server.prefetch(ts4);
    assert_eq!(packs.len(), 1);
    let ts_merge = packs[0].0;
    assert!(ts_merge > ts4);
    install_pack(&client, &packs[0].1);
    assert_complete(&client, &merge);
    pipeline(&root, &store, &up, "15m", &[]);
    let m = manifest(&store_dir);
    assert_eq!((m.tip.to_string(), m.max_ts), (merge.clone(), ts_merge));
    server.wait_for("the fourth manifest", |s| {
        s.metric("gvfs_manifest_generation") == 4
    });
    assert!(server.prefetch(ts_merge).is_empty());
    let late2 = fresh_repo(&root, "late2");
    install_pack(&late2, &server.prefetch(0)[0].1);
    assert_complete(&late2, &merge);

    server.assert_clean();
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// Advances the clock by one 15-minute period and runs a pipeline step.
fn step(
    root: &std::path::Path,
    store: &str,
    up: &Upstream,
    now: &mut u64,
    tier: &str,
    extra: &[&str],
) -> String {
    *now += 900;
    let mut args = vec!["--now".to_string(), now.to_string()];
    args.extend(extra.iter().map(|s| s.to_string()));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    pipeline(root, store, up, tier, &args)
}

/// Checks a running server against the published manifest: it serves that generation, a
/// cold clone is one pack with everything, and a client at `since` (holding `have`) gets
/// one pack that completes it. Returns the new cursor.
fn check_prefetch(
    root: &std::path::Path,
    server: &Server,
    store_dir: &std::path::Path,
    up: &Upstream,
    name: &str,
    since: u64,
    have: &std::path::Path,
) -> u64 {
    let m = manifest(store_dir);
    server.wait_for("the new manifest", |s| {
        s.metric("gvfs_manifest_generation") == m.generation
    });
    let tip = up.tip();
    assert_eq!(m.tip.to_string(), tip);

    let packs = server.prefetch(0);
    assert_eq!(packs.len(), 1, "{name}: cold prefetch");
    assert_eq!(packs[0].0, m.max_ts);
    let cold = fresh_repo(root, &format!("cold-{name}"));
    install_pack(&cold, &packs[0].1);
    assert_complete(&cold, &tip);

    let packs = server.prefetch(since);
    if since == m.max_ts {
        // A rollup without new commits keeps the timestamps: nothing new for this client.
        assert!(packs.is_empty(), "{name}: up to date");
    } else {
        assert_eq!(packs.len(), 1, "{name}: warm prefetch");
        assert_eq!(packs[0].0, m.max_ts);
        install_pack(have, &packs[0].1);
    }
    assert_complete(have, &tip);
    assert!(server.prefetch(m.max_ts).is_empty());
    m.max_ts
}

/// The pyramid: 15m slabs, 1h/6h/24h rollups (re-fetched and spliced), weekly rebuilds
/// (baseline re-fetched and spliced), weekly retention, the cold fallback for clients
/// older than the warm window, and garbage collection. A server without an upstream
/// follows every publication.
#[test]
fn pyramid_rollups_and_weekly_rebuilds() {
    let root = tempdir("pyramid");
    let mut up = Upstream::new(&root);
    up.commit(4);
    up.commit(4);
    let store_dir = root.join("store");
    let store = store_spec(&store_dir);
    let mut now = gvfs_daemon::util::now_secs();
    step(&root, &store, &up, &mut now, "15m", &[]);
    let genesis = manifest(&store_dir);

    let cache = root.join("cache");
    let server = Server::start(&[
        "--store",
        &store,
        "--poll-ms",
        "100",
        "--cache-dir",
        cache.to_str().unwrap(),
    ]);
    let client = fresh_repo(&root, "client");
    let packs = server.prefetch(0);
    assert_eq!(packs.len(), 1);
    install_pack(&client, &packs[0].1);
    let ts0 = packs[0].0;
    assert_eq!(ts0, genesis.baseline.end_ts);

    // Three 15m runs.
    for _ in 0..3 {
        up.commit(4);
        step(&root, &store, &up, &mut now, "15m", &[]);
    }
    let m = manifest(&store_dir);
    assert_eq!(
        m.pyramid.iter().map(|s| s.tier).collect::<Vec<_>>(),
        vec![Tier::M15; 3]
    );
    let ts = check_prefetch(&root, &server, &store_dir, &up, "m15", ts0, &client);

    // 1h re-fetched from the upstream: the run's new 15m slab and the three before it.
    up.commit(4);
    step(&root, &store, &up, &mut now, "1h", &[]);
    let m = manifest(&store_dir);
    assert_eq!(m.pyramid.len(), 1);
    let h1 = &m.pyramid[0];
    assert_eq!(
        (h1.tier, h1.have, h1.want, h1.start_ts),
        (Tier::H1, genesis.tip, m.tip, genesis.max_ts)
    );
    let ts = check_prefetch(&root, &server, &store_dir, &up, "h1", ts, &client);

    // 1h repacked from two 15m slabs. The 15m slabs stay listed (retained): a client whose
    // cursor is the first one's end gets just the second one's objects, not the hour.
    let tip_a = up.commit(4);
    step(&root, &store, &up, &mut now, "15m", &[]);
    let ts_a = manifest(&store_dir).max_ts;
    let tip_b = up.commit(4);
    let log = step(&root, &store, &up, &mut now, "1h", &[]);
    assert!(log.contains("repack: 2 slabs"), "{log}");
    let m = manifest(&store_dir);
    assert_eq!(
        m.pyramid.iter().map(|s| s.tier).collect::<Vec<_>>(),
        vec![Tier::H1, Tier::H1]
    );
    let retained: Vec<(Tier, u64)> = m
        .retained
        .iter()
        .rev()
        .take(2)
        .map(|s| (s.tier, s.start_ts))
        .collect();
    assert_eq!(
        retained,
        vec![(Tier::M15, ts_a), (Tier::M15, m.pyramid[1].start_ts)]
    );
    server.wait_for("the rollup", |s| {
        s.metric("gvfs_manifest_generation") == m.generation
    });
    let only_b = git(
        &up.bare,
        &["rev-list", "--objects", &tip_b, &format!("^{tip_a}")],
    )
    .lines()
    .count();
    let packs = server.prefetch(ts_a);
    assert_eq!((packs.len(), packs[0].0), (1, m.max_ts));
    assert_eq!(
        install_pack(&fresh_repo(&root, "at-a"), &packs[0].1),
        only_b
    );
    let ts = check_prefetch(&root, &server, &store_dir, &up, "h1-splice", ts, &client);

    // 6h from the two 1h slabs (no new commits): same range, same timestamps.
    step(&root, &store, &up, &mut now, "6h", &[]);
    let m = manifest(&store_dir);
    assert_eq!(
        m.pyramid
            .iter()
            .map(|s| (s.tier, s.have))
            .collect::<Vec<_>>(),
        vec![(Tier::H6, genesis.tip)]
    );
    assert_eq!(
        check_prefetch(&root, &server, &store_dir, &up, "h6", ts, &client),
        ts
    );

    // 24h repacked from the 6h slab and a new 15m slab.
    up.commit(4);
    step(&root, &store, &up, &mut now, "24h", &[]);
    let m = manifest(&store_dir);
    assert_eq!(
        m.pyramid.iter().map(|s| s.tier).collect::<Vec<_>>(),
        vec![Tier::H24]
    );
    let ts = check_prefetch(&root, &server, &store_dir, &up, "h24", ts, &client);

    // Weekly: the pyramid becomes the week's delta, and the baseline grows by it.
    up.commit(4);
    step(&root, &store, &up, &mut now, "168h", &[]);
    let m = manifest(&store_dir);
    assert!(m.pyramid.is_empty());
    assert_eq!(m.weekly.len(), 1);
    assert_eq!(
        (m.weekly[0].tier, m.weekly[0].have, m.weekly[0].start_ts),
        (Tier::Week, genesis.tip, genesis.max_ts)
    );
    assert_eq!(
        (m.baseline.want, m.baseline.objects as usize),
        (m.tip, up.objects(&up.tip()).len())
    );
    let ts_week1 = check_prefetch(&root, &server, &store_dir, &up, "week1", ts, &client);
    // A client still on the genesis pack catches up from the weekly delta alone.
    let old = fresh_repo(&root, "old-client");
    install_pack(&old, &server.prefetch(0).remove(0).1);
    let packs = server.prefetch(ts0);
    assert_eq!(packs.len(), 1);
    assert_eq!(
        install_pack(&old, &packs[0].1),
        m.weekly[0].objects as usize
    );
    assert_complete(&old, &up.tip());

    // The second week, keeping one week, collecting garbage immediately.
    up.commit(4);
    step(&root, &store, &up, &mut now, "15m", &[]);
    up.commit(4);
    step(
        &root,
        &store,
        &up,
        &mut now,
        "168h",
        &["--keep-weeks", "1", "--gc-grace-secs", "0"],
    );
    let m = manifest(&store_dir);
    assert_eq!(m.weekly.len(), 1);
    assert_eq!(m.weekly[0].start_ts, ts_week1);
    assert_eq!(m.baseline.objects as usize, up.objects(&up.tip()).len());
    check_prefetch(&root, &server, &store_dir, &up, "week2", ts_week1, &client);
    // Older than the warm window: the cold pack.
    let packs = server.prefetch(ts0);
    assert_eq!(packs.len(), 1);
    assert_eq!(
        install_pack(&fresh_repo(&root, "too-old"), &packs[0].1),
        m.baseline.objects as usize
    );

    // GC left exactly the referenced slab files.
    let mut files: Vec<String> = std::fs::read_dir(store_dir.join("slabs"))
        .unwrap()
        .map(|e| format!("slabs/{}", e.unwrap().file_name().to_string_lossy()))
        .collect();
    files.sort();
    let mut referenced: Vec<String> = m
        .slabs()
        .flat_map(|s| [s.pack_key(), s.index_key()])
        .collect();
    referenced.sort();
    assert_eq!(files, referenced);

    server.assert_clean();
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// `ingest --pack` on a git-made pack reproduces git's own index.
#[test]
fn ingest_cli_matches_git_index() {
    let root = tempdir("ingest");
    let mut up = Upstream::new(&root);
    for _ in 0..4 {
        up.commit(10);
    }
    let pack = git_stdin(
        &up.bare,
        &["pack-objects", "--revs", "--stdout", "-q"],
        b"main\n",
    );
    let pack_file = root.join("in.pack");
    std::fs::write(&pack_file, &pack).unwrap();
    git(
        &root,
        &[
            "index-pack",
            "-o",
            root.join("in.idx").to_str().unwrap(),
            pack_file.to_str().unwrap(),
        ],
    );
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"))
        .args([
            "ingest",
            "--pack",
            pack_file.to_str().unwrap(),
            "--out",
            root.join("out").to_str().unwrap(),
            "--threads",
            "3",
        ])
        .args(["--verify-idx", root.join("in.idx").to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // `inspect slab` finds the 40 pointer files and the 4 versions of the big text file.
    let index = std::fs::read_dir(root.join("out"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "mphf"))
        .unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"))
        .args(["inspect", "slab", index.to_str().unwrap()])
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        out.status.success()
            && text.contains("4 commits")
            && text.contains("40 GitHFS pointers")
            && text.contains("4 other blobs"),
        "{text}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// Microsoft git's gvfs-helper against the daemon: a cold prefetch installs exactly one
/// pack, a warm prefetch adds one more, and `get`/`post` fault in objects. Skipped when
/// gvfs-helper is not available.
#[test]
fn gvfs_helper_against_the_daemon() {
    let Some(msgit) = msgit() else {
        eprintln!("skipping: Microsoft git with gvfs-helper not found (set GVFS_TEST_MSGIT)");
        return;
    };
    let root = tempdir("helper");
    let mut up = Upstream::new(&root);
    up.commit(8);
    up.commit(8);
    let store_dir = root.join("store");
    let store = store_spec(&store_dir);
    pipeline(&root, &store, &up, "15m", &[]);
    let cache = root.join("cache");
    let server = Server::start(&[
        "--store",
        &store,
        "--upstream",
        up.bare.to_str().unwrap(),
        "--poll-ms",
        "100",
        "--cache-dir",
        cache.to_str().unwrap(),
    ]);

    let client = root.join("client");
    git(&root, &["init", "-q", client.to_str().unwrap()]);
    git(
        &client,
        &[
            "config",
            "remote.origin.url",
            &format!("{}/org/repo", server.url()),
        ],
    );
    let helper = |cmd: &str, stdin: &[String]| {
        let mut c = std::process::Command::new(&msgit);
        c.current_dir(&client)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        gvfs_daemon::upstream::hermetic_git_env(&mut c);
        let mut child = c
            .args(["gvfs-helper", "--cache-server=disable", cmd])
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        let mut input = child.stdin.take().unwrap();
        input.write_all(stdin.join("\n").as_bytes()).unwrap();
        drop(input);
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "gvfs-helper {cmd}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    let packs = || {
        std::fs::read_dir(client.join(".git/objects/pack"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "pack")
            })
            .count()
    };

    helper("prefetch", &[]);
    assert_eq!(packs(), 1, "a cold clone is one pack");
    assert_complete(&client, &up.tip());

    let tip2 = up.commit(8);
    server.wait_for("the new tip in L0", |s| {
        s.get(&format!("/gvfs/objects/{tip2}")).0 == 200
    });
    helper("prefetch", &[]);
    assert_eq!(packs(), 2, "a warm fetch adds one pack");
    assert_complete(&client, &tip2);

    // Objects not prefetched yet: faulted in one by one (GET) and in batches (POST).
    let tip3 = up.commit(8);
    server.wait_for("the next tip in L0", |s| {
        s.get(&format!("/gvfs/objects/{tip3}")).0 == 200
    });
    let new: Vec<String> = up
        .objects(&tip3)
        .into_iter()
        .filter(|o| {
            !git_cmd(&client)
                .args(["cat-file", "-e", o])
                .status()
                .unwrap()
                .success()
        })
        .collect();
    assert!(new.len() > 8);
    let (one, rest) = new.split_at(new.len() / 2);
    helper("get", one);
    helper("post", rest);
    assert_complete(&client, &tip3);

    server.assert_clean();
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// The upstream over smart HTTP (Basic auth, keep-alive, chunked responses), once with
/// protocol v2 and once against a server that only speaks v0: genesis, a 15m slab, an
/// upstream re-fetched rollup, and an edge whose L0 follows pushes over the same transport.
#[test]
fn smart_http_upstream_v2_and_v0() {
    for v2 in [true, false] {
        let root = tempdir(if v2 { "http-v2" } else { "http-v0" });
        let mut up = Upstream::new(&root);
        up.commit(5);
        let http = SmartHttp::start(&up.bare, "secret", v2);
        let store_dir = root.join("store");
        let store = store_spec(&store_dir);
        let token = [("GVD_TEST_TOKEN", "secret")];
        let auth = ["--token-env", "GVD_TEST_TOKEN"];

        let err = pipeline_at(&root, &store, &http.url, "15m", &[], &[]).unwrap_err();
        assert!(err.contains("401"), "{err}");
        pipeline_at(&root, &store, &http.url, "15m", &auth, &token).unwrap();
        let genesis_tip = up.tip();
        assert_eq!(
            manifest(&store_dir).baseline.objects as usize,
            up.objects(&genesis_tip).len()
        );

        up.commit(5);
        up.commit(5);
        let mut now = gvfs_daemon::util::now_secs() + 900;
        pipeline_at(
            &root,
            &store,
            &http.url,
            "15m",
            &[&auth[..], &["--now", &now.to_string()]].concat(),
            &token,
        )
        .unwrap();
        up.commit(5);
        now += 900;
        let log = pipeline_at(
            &root,
            &store,
            &http.url,
            "1h",
            &[&auth[..], &["--now", &now.to_string()]].concat(),
            &token,
        )
        .unwrap();
        assert!(!log.contains("splicing"), "the rollup is re-fetched: {log}");
        let m = manifest(&store_dir);
        assert_eq!(
            (m.pyramid.len(), m.pyramid[0].tier, m.tip.to_string()),
            (1, Tier::H1, up.tip())
        );
        let new = git(
            &up.bare,
            &["rev-list", "--objects", &format!("{genesis_tip}..main")],
        )
        .lines()
        .count();
        assert_eq!(m.pyramid[0].objects as usize, new);

        let cache = root.join("cache");
        let server = Server::start_with_env(
            &[
                "--store",
                &store,
                "--upstream",
                &http.url,
                "--token-env",
                "GVD_TEST_TOKEN",
                "--poll-ms",
                "100",
                "--cache-dir",
                cache.to_str().unwrap(),
            ],
            &token,
        );
        let tip = up.commit(5);
        server.wait_for("the new tip in L0", |s| {
            s.get(&format!("/gvfs/objects/{tip}")).0 == 200
        });
        let packs = server.prefetch(0);
        assert_eq!(packs.len(), 1);
        let cold = fresh_repo(&root, "cold");
        install_pack(&cold, &packs[0].1);
        assert_complete(&cold, &tip);
        server.assert_clean();
        drop(server);

        assert!(http.posts() >= 4, "{} upload-pack requests", http.posts());
        assert_eq!(http.v2_posts() == http.posts(), v2);
        assert_eq!(http.v2_posts() == 0, !v2);
        let _ = std::fs::remove_dir_all(&root);
    }
}
