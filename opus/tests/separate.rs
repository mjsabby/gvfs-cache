//! Stores with `--blobs separate`: real blobs in their own packs, commits and trees in the
//! slabs with sizes tables. Stock git is the oracle for every id, size and byte.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

mod common;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use common::*;
use gvfs_daemon::blobcopy;
use gvfs_daemon::manifest::Manifest;
use gvfs_daemon::pack::{OBJ_BLOB, OBJ_COMMIT, OBJ_TREE, parse_entry_header};
use gvfs_daemon::sizes::Sizes;
use gvfs_daemon::slab::{Slab, SlabMeta};
use gvfs_daemon::store::Store;
use gvfs_daemon::util::Oid;

fn sizes_of(store_dir: &Path, meta: &SlabMeta) -> BTreeMap<String, u64> {
    Sizes::open(&store_dir.join(meta.sizes_key()))
        .unwrap()
        .iter()
        .map(|(o, s)| (o.to_string(), s))
        .collect()
}

/// The union of the tables a manifest's trees need (baseline + pyramid).
fn all_sizes(store_dir: &Path, m: &Manifest) -> BTreeMap<String, u64> {
    let mut all = BTreeMap::new();
    for meta in std::iter::once(&m.baseline).chain(&m.pyramid) {
        all.extend(sizes_of(store_dir, meta));
    }
    all
}

fn slab_kinds(store_dir: &Path, meta: &SlabMeta) -> Vec<u8> {
    let slab = Slab::open(
        &store_dir.join(meta.pack_key()),
        &store_dir.join(meta.index_key()),
        meta.clone(),
    )
    .unwrap();
    let mut k: Vec<u8> = slab.records().map(|r| r.kind).collect();
    k.sort_unstable();
    k.dedup();
    k
}

/// Every blob the store's blob packs hold, read back through `blobs.bin` and its
/// segments; every pack must also pass `git index-pack --strict`.
fn store_blobs(root: &Path, store_dir: &Path) -> HashMap<String, Vec<u8>> {
    let store = Store::open(&format!("fs:{}", store_dir.display()), "").unwrap();
    let (bm, _) = blobcopy::load(&store).unwrap();
    let check = fresh_repo(root, &format!("check-{}", bm.generation));
    for p in &bm.packs {
        let bytes = std::fs::read(store_dir.join(format!("blobs/{}.pack", p.id))).unwrap();
        assert_eq!(bytes.len() as u64, p.len);
        git_stdin(&check, &["index-pack", "--stdin", "--strict"], &bytes);
    }
    let mut out = HashMap::new();
    let mut inf = gvfs_daemon::zlib::Inflater::new();
    for meta in &bm.segments {
        let seg = blobcopy::open_segment(&store, root, meta).unwrap();
        for rec in seg.records() {
            let pack = &seg.packs()[rec.pack as usize];
            let bytes = std::fs::read(store_dir.join(format!("blobs/{}.pack", pack.id))).unwrap();
            let entry = &bytes[rec.offset as usize..(rec.offset + rec.entry_len) as usize];
            let h = parse_entry_header(entry).unwrap();
            assert_eq!((h.kind, h.size), (OBJ_BLOB, rec.size));
            let mut content = vec![0u8; rec.size as usize];
            inf.inflate_exact(&entry[h.header_len..], &mut content)
                .unwrap();
            out.insert(rec.oid.to_string(), content);
        }
    }
    out
}

#[test]
fn separate_store_pipeline_copies_blobs_before_trees() {
    let root = tempdir("sep-pipeline");
    let up = Upstream::new(&root);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());

    // History before genesis.
    commit_files(
        &up,
        "c1",
        &[
            ("a/text.txt", Some(text(1))),
            ("a/gone.txt", Some(b"bye\n".to_vec())),
            ("b/bin.dat", Some(noise(300_000, 1))),
        ],
    );
    commit_files(
        &up,
        "c2",
        &[("a/text.txt", Some(text(2))), ("a/gone.txt", None)],
    );
    let g = commit_files(
        &up,
        "c3",
        &[("a/text.txt", Some(text(3))), ("c/empty", Some(Vec::new()))],
    );
    let log = pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);
    assert!(log.contains("blobs separate"), "{log}");
    let m = manifest(&store_dir);
    assert!(m.separate_blobs());
    assert_eq!(
        slab_kinds(&store_dir, &m.baseline),
        vec![OBJ_COMMIT, OBJ_TREE]
    );
    assert_eq!(all_sizes(&store_dir, &m), git_blobs(&up.bare, &["--all"]));
    let s = Store::open(&store, "").unwrap();
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(bm.segments.is_empty() && !bm.complete());
    assert!(m.blobs_generation >= 1 && bm.generation >= m.blobs_generation);
    // The mode is the store's: an inline run on it is refused.
    let refused = pipeline_at(
        &root,
        &store,
        up.bare.to_str().unwrap(),
        "15m",
        &["--blobs", "inline"],
        &[],
    )
    .unwrap_err();
    assert!(refused.contains("chosen at genesis"), "{refused}");

    // A window: a revert to a version never copied, a large blob, an incompressible one,
    // a copy of an old blob under a new path, and a new version.
    commit_files(
        &up,
        "c4",
        &[
            ("a/text.txt", Some(text(1))),
            ("big/large.bin", Some(large((16 << 20) + 4097))),
            ("big/noise.bin", Some(noise(2 << 20, 9))),
        ],
    );
    let t = commit_files(
        &up,
        "c5",
        &[
            ("a/text.txt", Some(text(4))),
            ("d/copy.dat", Some(noise(300_000, 1))),
        ],
    );
    let log = pipeline(&root, &store, &up, "15m", &["--blob-pack-mib", "1"]);
    let m2 = manifest(&store_dir);
    assert_eq!(m2.pyramid.len(), 1);
    let window = &m2.pyramid[0];
    assert_eq!(slab_kinds(&store_dir, window), vec![OBJ_COMMIT, OBJ_TREE]);
    let range = format!("{g}..{t}");
    let introduced = git_blobs(&up.bare, &[&range]);
    let stored = store_blobs(&root, &store_dir);
    for (id, size) in &introduced {
        let content = stored
            .get(id)
            .unwrap_or_else(|| panic!("blob {id} of the window is not in the store\n{log}"));
        assert_eq!(content.len() as u64, *size);
        assert!(*content == cat_blob(&up.bare, id), "blob {id} differs");
    }
    // The window's table: exactly the sizes no earlier table had.
    let baseline_sizes = sizes_of(&store_dir, &m2.baseline);
    let expect: BTreeMap<String, u64> = introduced
        .iter()
        .filter(|(id, _)| !baseline_sizes.contains_key(*id))
        .map(|(id, s)| (id.clone(), *s))
        .collect();
    assert_eq!(sizes_of(&store_dir, window), expect);
    assert_eq!(expect.len(), 3, "text v4, large.bin, noise.bin");
    assert_eq!(all_sizes(&store_dir, &m2), git_blobs(&up.bare, &["--all"]));
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(m2.blobs_generation > m.blobs_generation && bm.generation >= m2.blobs_generation);
    // The large blob went alone (streamed); the incompressible one is stored blocks.
    let large_id = git(&up.bare, &["rev-parse", &format!("{t}:big/large.bin")]);
    let noise_id = git(&up.bare, &["rev-parse", &format!("{t}:big/noise.bin")]);
    let mut lone = false;
    let mut stored_noise = false;
    for meta in &bm.segments {
        let seg = blobcopy::open_segment(&s, &root, meta).unwrap();
        for rec in seg.records() {
            if rec.oid == Oid::from_hex(large_id.trim().as_bytes()).unwrap() {
                lone = bm
                    .packs
                    .iter()
                    .any(|p| p.id == seg.packs()[rec.pack as usize].id && p.blobs == 1);
            }
            if rec.oid == Oid::from_hex(noise_id.trim().as_bytes()).unwrap() {
                stored_noise = rec.stored;
            }
        }
    }
    assert!(lone && stored_noise);

    // Nothing new: no copy, no new slab.
    pipeline(&root, &store, &up, "15m", &[]);
    assert_eq!(manifest(&store_dir).pyramid.len(), 1);

    // Another window, then rollups: their tables are unions.
    let t2 = commit_files(&up, "c6", &[("a/text.txt", Some(text(5)))]);
    pipeline(&root, &store, &up, "1h", &[]);
    let m3 = manifest(&store_dir);
    assert_eq!(m3.pyramid.len(), 1, "rolled into one 1h slab");
    assert_eq!(all_sizes(&store_dir, &m3), git_blobs(&up.bare, &["--all"]));
    let range = format!("{g}..{t2}");
    let stored = store_blobs(&root, &store_dir);
    for id in git_blobs(&up.bare, &[&range]).keys() {
        assert!(
            stored.contains_key(id),
            "blob {id} missing after the rollup"
        );
    }
    pipeline(&root, &store, &up, "168h", &[]);
    let m4 = manifest(&store_dir);
    assert!(m4.pyramid.is_empty());
    assert_eq!(
        sizes_of(&store_dir, &m4.baseline),
        git_blobs(&up.bare, &["--all"])
    );
    assert_eq!(
        slab_kinds(&store_dir, &m4.baseline),
        vec![OBJ_COMMIT, OBJ_TREE]
    );

    // GC keeps everything listed (sizes tables, blob packs, segments) and drops the rest.
    std::fs::write(store_dir.join("blobs/orphan.pack"), b"x").unwrap();
    let far = (gvfs_daemon::util::now_secs() + 86_400 * 30).to_string();
    pipeline(
        &root,
        &store,
        &up,
        "15m",
        &["--now", &far, "--gc-grace-secs", "60"],
    );
    let m5 = manifest(&store_dir);
    for meta in m5.slabs() {
        assert!(
            store_dir.join(meta.sizes_key()).exists(),
            "{} gone",
            meta.id
        );
    }
    assert!(!store_dir.join("blobs/orphan.pack").exists());
    assert_eq!(store_blobs(&root, &store_dir).len(), stored.len());
    let _ = std::fs::remove_dir_all(&root);
}

/// Runs `gvfs-daemon mirror`; `Err` holds the log of a failure.
fn mirror(root: &Path, store: &str, up: &Upstream, extra: &[&str]) -> Result<String, String> {
    let work = root.join("mirror-work");
    std::fs::create_dir_all(&work).unwrap();
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"));
    cmd.args(["mirror", "--store", store, "--upstream"])
        .arg(&up.bare)
        .args(["--work-dir", work.to_str().unwrap(), "--threads", "2"])
        .args(["--jobs", "3", "--publish-secs", "0"])
        .args(extra);
    gvfs_daemon::upstream::hermetic_git_env(&mut cmd);
    let out = cmd.output().unwrap();
    let log = String::from_utf8_lossy(&out.stderr).into_owned();
    if out.status.success() {
        Ok(log)
    } else {
        Err(log)
    }
}

/// The mirror copies every blob of the history (the tip's tree first), in slices, never
/// twice, and is then complete; with the pipeline's windows, every blob of every commit.
#[test]
fn mirror_copies_the_whole_history_and_resumes() {
    let root = tempdir("sep-mirror");
    let up = Upstream::new(&root);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());
    let file = |dir: &str, i: u32, v: u32| -> (String, Option<Vec<u8>>) {
        let mut t = text(v * 1000 + i);
        t.extend_from_slice(format!("{dir} file {i}\n").as_bytes());
        (format!("{dir}/f{i}.txt"), Some(t))
    };
    let mut first: Vec<(String, Option<Vec<u8>>)> = Vec::new();
    for dir in ["x", "y/deep", "z"] {
        for i in 0..12 {
            first.push(file(dir, i, 1));
        }
    }
    first.push(("big/large.bin".into(), Some(large((16 << 20) + 1))));
    first.push(("bin/noise.bin".into(), Some(noise(1_500_000, 3))));
    let as_refs =
        |v: &[(String, Option<Vec<u8>>)]| -> Vec<(String, Option<Vec<u8>>)> { v.to_vec() };
    let commit = |msg: &str, files: Vec<(String, Option<Vec<u8>>)>| {
        let refs: Vec<(&str, Option<Vec<u8>>)> =
            files.iter().map(|(p, c)| (p.as_str(), c.clone())).collect();
        commit_files(&up, msg, &refs)
    };
    let c1 = commit("c1", as_refs(&first));
    commit("c2", (0..6).map(|i| file("x", i, 2)).collect());
    let g = commit("c3", (0..4).map(|i| file("z", i, 3)).collect());
    pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);

    // After genesis: x/f0.txt is deleted, y/deep/f1.txt changes, a new file appears.
    let t = commit(
        "c4",
        vec![
            ("x/f0.txt".into(), None),
            file("y/deep", 1, 4),
            ("n/new.txt".into(), Some(b"new\n".to_vec())),
        ],
    );
    pipeline(&root, &store, &up, "15m", &[]);
    let s = Store::open(&store, "").unwrap();
    assert!(!blobcopy::load(&s).unwrap().0.complete());

    // In slices: two packs (the tip's blobs come first), then the rest.
    let log = mirror(
        &root,
        &store,
        &up,
        &["--blob-pack-mib", "1", "--max-items", "2"],
    )
    .unwrap();
    assert!(log.contains("stopped after 2"), "{log}");
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(!bm.complete() && !bm.segments.is_empty());
    let tip = commit_blobs(&up.bare, &["-1", &t]);
    let first = store_blobs(&root, &store_dir);
    assert!(!first.is_empty());
    for id in first.keys() {
        assert!(
            tip.contains_key(id),
            "{id}: not a tip blob in the first slice"
        );
    }
    let log = mirror(&root, &store, &up, &["--blob-pack-mib", "1"]).unwrap();
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(bm.complete(), "{log}");
    assert_eq!(bm.genesis.to_string(), g);

    // Every blob of the history, byte for byte: old versions (c1's), the genesis tree's
    // (even the file deleted since) and the window's.
    let stored = store_blobs(&root, &store_dir);
    let history = git_blobs(&up.bare, &["--all"]);
    assert!(
        commit_blobs(&up.bare, &["-1", &c1])
            .keys()
            .any(|id| !tip.contains_key(id))
    );
    for id in history.keys() {
        let content = stored
            .get(id)
            .unwrap_or_else(|| panic!("{id} missing\n{log}"));
        assert!(*content == cat_blob(&up.bare, id), "{id} differs");
    }
    assert_eq!(stored.len(), history.len());
    // Nothing was copied twice: one record per blob across all segments.
    let records: usize = bm.segments.iter().map(|m| m.blobs as usize).sum();
    assert_eq!(records, stored.len(), "a blob listed twice");

    // Again: nothing to copy.
    let log = mirror(&root, &store, &up, &[]).unwrap();
    assert!(log.contains("to copy in 0 items"), "{log}");
    // There is no partial mirror any more.
    let refused = mirror(&root, &store, &up, &["--scope", "tip"]).unwrap_err();
    assert!(refused.contains("scope"), "{refused}");
    let _ = std::fs::remove_dir_all(&root);
}

/// A prefetch body with a `GSZ1` section: the packs, then `(id, size)` of each record.
#[allow(clippy::type_complexity)]
fn parse_gpre_sizes(body: &[u8]) -> (Vec<(u64, Vec<u8>, Option<Vec<u8>>)>, BTreeMap<String, u64>) {
    let at = body
        .windows(4)
        .rposition(|w| w == b"GSZ1")
        .expect("a GSZ1 section");
    let packs = parse_gpre(&body[..at]);
    let n = u64::from_le_bytes(body[at + 4..at + 12].try_into().unwrap()) as usize;
    let recs = &body[at + 12..];
    assert_eq!(recs.len(), n * 28, "GSZ1 length");
    let sizes = recs
        .chunks(28)
        .map(|r| {
            (
                Oid::from_slice(&r[..20]).unwrap().to_string(),
                u64::from_le_bytes(r[20..].try_into().unwrap()),
            )
        })
        .collect();
    (packs, sizes)
}

/// `[{"Id":"..","Size":N},...]`.
fn parse_sizes_json(body: &[u8]) -> BTreeMap<String, u64> {
    let text = String::from_utf8(body.to_vec()).unwrap();
    text.split("{\"Id\":\"")
        .skip(1)
        .map(|item| {
            let id = item[..40].to_string();
            let size = item.split("\"Size\":").nth(1).unwrap();
            let digits: String = size.chars().take_while(char::is_ascii_digit).collect();
            (id, digits.parse().unwrap())
        })
        .collect()
}

/// An edge of a separate-blob store: unready until the mirror is done (blobs it is asked
/// for meanwhile come through the proxy), then every blob of the history from the store
/// as loose objects, packs, sizes and ranges; a push's blobs through the proxy until the
/// pipeline seals them.
#[test]
fn edge_serves_blobs_from_the_store_and_l0_through_the_proxy() {
    let root = tempdir("sep-edge");
    let up = Upstream::new(&root);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());
    let noise_bytes = noise(2 << 20, 5);
    commit_files(
        &up,
        "c1",
        &[
            ("a/text.txt", Some(text(1))),
            ("a/gone.txt", Some(b"gone\n".to_vec())),
            ("b/noise.bin", Some(noise_bytes.clone())),
            ("big/large.bin", Some(large((16 << 20) + 3))),
        ],
    );
    commit_files(
        &up,
        "c2",
        &[("a/text.txt", Some(text(2))), ("a/gone.txt", None)],
    );
    let g = commit_files(&up, "c3", &[("c/small.txt", Some(b"small\n".to_vec()))]);
    pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);
    let cache = root.join("cache");
    let server = Server::start_unready(&[
        "--store",
        &store,
        "--upstream",
        up.bare.to_str().unwrap(),
        "--poll-ms",
        "100",
        "--cache-dir",
        cache.to_str().unwrap(),
    ]);
    let (status, _, body) = server.get("/healthz");
    assert_eq!(status, 503);
    assert!(String::from_utf8_lossy(&body).contains("blobs incomplete"));

    // Before the mirror, blobs come through the proxy (not a fault: not complete yet).
    let text2 = rev(&up.bare, &format!("{g}:a/text.txt"));
    let (status, ct, _) = server.get(&format!("/gvfs/objects/{text2}"));
    assert_eq!((status, ct.as_str()), (200, CT_LOOSE));
    assert_eq!(server.metric("gvfs_blobs_proxied_total"), 1);
    assert_eq!(server.metric("gvfs_blobs_missing_total"), 0);

    mirror(&root, &store, &up, &[]).unwrap();
    server.wait_for("complete", |s| s.get("/healthz").0 == 200);

    // Every blob of history as a loose object, from the store. git re-hashes each one.
    let history = git_blobs(&up.bare, &["--all"]);
    let all_blobs: Vec<String> = history.keys().cloned().collect();
    let tip_blobs = commit_blobs(&up.bare, &["-1", &g]);
    let loose = fresh_repo(&root, "loose");
    for id in &all_blobs {
        let (status, ct, body) = server.get(&format!("/org/repo/gvfs/objects/{id}"));
        assert_eq!((status, ct.as_str()), (200, CT_LOOSE), "{id}");
        let dir = loose.join("objects").join(&id[..2]);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(&id[2..]), body).unwrap();
    }
    git(&loose, &["fsck", "--strict", "--no-dangling"]);
    assert_eq!(
        cat_batch(&loose, &all_blobs),
        cat_batch(&up.bare, &all_blobs)
    );
    assert!(server.metric("gvfs_blobs_from_store_total") >= all_blobs.len() as u64);
    assert_eq!(server.metric("gvfs_blobs_proxied_total"), 1);
    assert!(all_blobs.len() > tip_blobs.len(), "old versions too");
    // Ids no tree of the store names are not proxied.
    assert_eq!(
        server.get(&format!("/gvfs/objects/{}", "0".repeat(40))).0,
        404
    );

    // A POST of trees and blobs (the tip's and old versions): one pack git accepts.
    let kinds = types(&up.bare, &up.objects(&g));
    let mut batch: Vec<String> = kinds
        .iter()
        .filter(|(_, t)| t != "commit")
        .map(|(o, _)| o.clone())
        .collect();
    batch.extend(
        all_blobs
            .iter()
            .filter(|b| !tip_blobs.contains_key(*b))
            .cloned(),
    );
    let (status, ct, pack) =
        server.post("/gvfs/objects", json_ids(&batch, None).as_bytes(), CT_PACK);
    assert_eq!((status, ct.as_str()), (200, CT_PACK));
    let posted = fresh_repo(&root, "post");
    git_stdin(
        &posted,
        &["index-pack", "--stdin", "--strict", "--keep=t"],
        &pack,
    );
    assert_eq!(cat_batch(&posted, &batch), cat_batch(&up.bare, &batch));
    let one = std::slice::from_ref(&text2);
    let (status, ct, _) = server.post("/gvfs/objects", json_ids(one, None).as_bytes(), CT_LOOSE);
    assert_eq!((status, ct.as_str()), (200, CT_LOOSE));

    // Sizes of blobs (stored, old), trees and commits.
    let mut ids: Vec<String> = all_blobs.clone();
    ids.push(g.clone());
    ids.push(rev(&up.bare, &format!("{g}^{{tree}}")));
    let (status, _, body) = server.post(
        "/gvfs/sizes",
        json_ids(&ids, None).as_bytes(),
        "application/json",
    );
    assert_eq!(status, 200);
    let sizes = parse_sizes_json(&body);
    assert_eq!(sizes.len(), ids.len());
    for (id, size) in &sizes {
        let s: u64 = git(&up.bare, &["cat-file", "-s", id])
            .trim()
            .parse()
            .unwrap();
        assert_eq!(*size, s, "{id}");
    }

    // Prefetch: unchanged without the header; with it, the history's sizes follow.
    let agent = ("User-Agent", "git/2.45.2.vfs.0.0");
    let (status, h, plain) = server.get_with("/gvfs/prefetch", &[agent], &["x-gvfs-sizes"]);
    assert_eq!((status, h[0].clone()), (200, None));
    let plain_packs = parse_gpre(&plain);
    let (status, h, with) = server.get_with(
        "/gvfs/prefetch",
        &[agent, ("X-GVFS-Sizes", "1")],
        &["x-gvfs-sizes"],
    );
    assert_eq!((status, h[0].as_deref()), (200, Some("1")));
    let (packs, cold_sizes) = parse_gpre_sizes(&with);
    assert_eq!(packs, plain_packs);
    assert_eq!(cold_sizes, history);
    let cold = fresh_repo(&root, "cold");
    install_pack(&cold, &packs[0].1);
    let in_pack = git(
        &cold,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objecttype)",
        ],
    );
    assert!(!in_pack.contains("blob"), "prefetch packs carry no blobs");
    let cold_ts = packs[0].0;

    // Byte ranges: in place for stored (incompressible) blobs, inflated for compressed ones.
    let noise_id = rev(&up.bare, &format!("{g}:b/noise.bin"));
    for (id, content) in [
        (noise_id.clone(), noise_bytes.clone()),
        (text2.clone(), text(2)),
    ] {
        let n = content.len() as u64;
        for (off, len) in [(0u64, 10u64), (65_530, 20), (n - 3, 3), (1, n - 2), (n, 5)] {
            let (status, h, body) = server.get_with(
                &format!("/gvfs/blobs/{id}?offset={off}&length={len}"),
                &[],
                &["x-gvfs-blob-size"],
            );
            assert_eq!(
                (status, h[0].clone()),
                (200, Some(n.to_string())),
                "{id} {off}+{len}"
            );
            let end = (off + len).min(n) as usize;
            assert!(body == content[off as usize..end], "{id} {off}+{len}");
        }
        let (_, _, rest) = server.get_with(&format!("/gvfs/blobs/{id}?offset=7"), &[], &[]);
        assert!(rest == content[7..]);
        assert_eq!(
            server.get(&format!("/gvfs/blobs/{id}?offset={}", n + 1)).0,
            416
        );
    }

    // Pipelined requests (two in one write): each is answered from its own query.
    let bodies = pipelined(
        server.port,
        &[
            format!("/gvfs/blobs/{noise_id}?offset=0&length=100"),
            format!("/gvfs/blobs/{noise_id}?offset=4096&length=100"),
        ],
    );
    assert!(bodies[0] == noise_bytes[..100], "first pipelined range");
    assert!(
        bodies[1] == noise_bytes[4096..4196],
        "second pipelined range"
    );

    // A push is served at once (L0): its blobs through the proxy, its sizes with the warm
    // prefetch.
    let t = commit_files(
        &up,
        "c4",
        &[
            ("n/new.txt", Some(b"brand new\n".to_vec())),
            ("a/text.txt", Some(text(4))),
        ],
    );
    server.wait_for("the push in L0", |s| {
        s.get(&format!("/gvfs/objects/{t}")).0 == 200
    });
    let new_id = rev(&up.bare, &format!("{t}:n/new.txt"));
    let proxied = server.metric("gvfs_blobs_proxied_total");
    assert_eq!(server.get(&format!("/gvfs/objects/{new_id}")).0, 200);
    assert_eq!(server.metric("gvfs_blobs_proxied_total"), proxied + 1);
    let (_, _, warm) = server.get_with(
        &format!("/gvfs/prefetch?lastPackTimestamp={cold_ts}"),
        &[agent, ("X-GVFS-Sizes", "1")],
        &[],
    );
    let (warm_packs, warm_sizes) = parse_gpre_sizes(&warm);
    assert_eq!(warm_packs.len(), 1);
    let introduced: BTreeMap<String, u64> = git_blobs(&up.bare, &[&format!("{g}..{t}")])
        .into_iter()
        .filter(|(id, _)| !history.contains_key(id))
        .collect();
    assert_eq!(warm_sizes, introduced);

    // The pipeline seals it, blobs first; the edge then serves those blobs from the store.
    pipeline(&root, &store, &up, "15m", &[]);
    server.wait_for("the sealed window", |s| {
        s.metric("gvfs_manifest_generation") >= 2
    });
    let text4 = rev(&up.bare, &format!("{t}:a/text.txt"));
    let (stored, proxied) = (
        server.metric("gvfs_blobs_from_store_total"),
        server.metric("gvfs_blobs_proxied_total"),
    );
    assert_eq!(server.get(&format!("/gvfs/objects/{text4}")).0, 200);
    assert_eq!(server.metric("gvfs_blobs_from_store_total"), stored + 1);
    assert_eq!(server.metric("gvfs_blobs_proxied_total"), proxied);
    assert_eq!(server.metric("gvfs_blobs_missing_total"), 0);
    server.assert_clean();
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// Stock microsoft/git against an edge of a separate-blob store: gvfs-helper prefetches
/// commits and trees, and a checkout faults the blobs in (GET singles, POST batches),
/// byte for byte git's, old versions too, all from the store; a sparse cone fetches only
/// its own blobs.
#[test]
fn stock_gvfs_helper_checks_out_a_separate_store() {
    let Some(msgit) = msgit() else {
        eprintln!("skipping: Microsoft git with gvfs-helper not found (set GVFS_TEST_MSGIT)");
        return;
    };
    let root = tempdir("sep-helper");
    let up = Upstream::new(&root);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());
    let c1 = commit_files(
        &up,
        "c1",
        &[
            ("a/text.txt", Some(text(1))),
            ("a/deep/more.txt", Some(text(7))),
            ("b/noise.bin", Some(noise(2 << 20, 6))),
            ("big/large.bin", Some(large((16 << 20) + 5))),
        ],
    );
    let g = commit_files(
        &up,
        "c2",
        &[
            ("a/text.txt", Some(text(2))),
            ("c/x.txt", Some(b"x\n".to_vec())),
        ],
    );
    pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);
    mirror(&root, &store, &up, &[]).unwrap();
    let t = commit_files(
        &up,
        "c3",
        &[
            ("a/text.txt", Some(text(3))),
            ("n/new.txt", Some(b"new\n".to_vec())),
        ],
    );
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

    let msg = |dir: &Path, args: &[&str]| -> Vec<u8> {
        let mut c = std::process::Command::new(&msgit);
        c.current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .args(args);
        gvfs_daemon::upstream::hermetic_git_env(&mut c);
        let out = c.output().unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        out.stdout
    };
    let client = |name: &str| {
        let dir = root.join(name);
        msg(&root, &["init", "-q", dir.to_str().unwrap()]);
        msg(
            &dir,
            &[
                "config",
                "remote.origin.url",
                &format!("{}/org/repo", server.url()),
            ],
        );
        msg(&dir, &["config", "gc.auto", "0"]);
        msg(&dir, &["gvfs-helper", "--cache-server=disable", "prefetch"]);
        dir
    };
    let missing = |dir: &Path, rev: &str| -> Vec<String> {
        String::from_utf8(msg(
            dir,
            &[
                "-c",
                "core.useGVFSHelper=false",
                "rev-list",
                "--objects",
                "--missing=print",
                rev,
            ],
        ))
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix('?').map(str::to_string))
        .collect()
    };

    // Full checkout: every file is git's.
    let full = client("full");
    let tip_blobs = commit_blobs(&up.bare, &["-1", &t]);
    let reachable = git_blobs(&up.bare, &[&t]);
    let mut gone = missing(&full, &t);
    gone.sort();
    assert_eq!(
        gone,
        reachable.keys().cloned().collect::<Vec<_>>(),
        "prefetch brings no blobs"
    );
    msg(
        &full,
        &[
            "-c",
            "core.useGVFSHelper=true",
            "checkout",
            "-q",
            "--detach",
            &t,
        ],
    );
    for line in git(&up.bare, &["ls-tree", "-r", &t]).lines() {
        let (meta, path) = line.split_once('\t').unwrap();
        let id = meta.split(' ').nth(2).unwrap();
        assert!(
            std::fs::read(full.join(path)).unwrap() == cat_blob(&up.bare, id),
            "{path}"
        );
    }
    let gone = missing(&full, &t);
    assert!(
        tip_blobs.keys().all(|b| !gone.contains(b)),
        "the tip's blobs were faulted in"
    );
    // An old version: mirrored with the history.
    let shown = msg(
        &full,
        &[
            "-c",
            "core.useGVFSHelper=true",
            "show",
            &format!("{c1}:a/text.txt"),
        ],
    );
    assert!(shown == text(1));

    // A sparse cone fetches its own blobs only.
    let sparse = client("sparse");
    msg(&sparse, &["sparse-checkout", "set", "--cone", "a"]);
    msg(
        &sparse,
        &[
            "-c",
            "core.useGVFSHelper=true",
            "checkout",
            "-q",
            "--detach",
            &g,
        ],
    );
    assert!(std::fs::read(sparse.join("a/text.txt")).unwrap() == text(2));
    assert!(!sparse.join("big/large.bin").exists());
    let large_id = rev(&up.bare, &format!("{g}:big/large.bin"));
    assert!(
        missing(&sparse, &g).contains(&large_id),
        "outside the cone: not fetched"
    );
    assert_eq!(server.metric("gvfs_blobs_proxied_total"), 0);
    assert_eq!(server.metric("gvfs_blobs_missing_total"), 0);
    server.assert_clean();
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// Writes `n` incompressible bytes to `path` without holding them in memory; returns
/// the blob id git gives them.
fn write_noise_file(path: &Path, n: u64, seed: u64) -> Oid {
    use sha1::{Digest, Sha1};
    use std::io::Write;
    let mut f = std::io::BufWriter::with_capacity(8 << 20, std::fs::File::create(path).unwrap());
    let mut h = Sha1::new();
    h.update(format!("blob {n}\0").as_bytes());
    let mut x = seed | 1;
    let mut buf = vec![0u8; 8 << 20];
    let mut left = n;
    while left > 0 {
        for w in buf.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            w.copy_from_slice(&x.to_le_bytes()[..w.len()]);
        }
        let k = left.min(buf.len() as u64) as usize;
        f.write_all(&buf[..k]).unwrap();
        h.update(&buf[..k]);
        left -= k as u64;
    }
    f.flush().unwrap();
    Oid(h.finalize().into())
}

/// Reads `len` bytes of `path` at `at`.
fn file_range(path: &Path, at: u64, len: usize) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    let mut b = vec![0u8; len];
    std::fs::File::open(path)
        .unwrap()
        .read_exact_at(&mut b, at)
        .unwrap();
    b
}

/// A blob over 4 GiB, end to end: the pipeline streams it into a blob pack with memory
/// bounded by its buffers; the edge sends it loose and in a pack (git verifies both) and
/// serves ranges beyond 4 GiB. Writes ~13 GB under the target directory:
/// `cargo test --release --test separate -- --ignored blob_over_4gib`.
#[test]
#[ignore = "writes ~13 GB; run explicitly"]
fn blob_over_4gib_end_to_end() {
    use sha1::{Digest, Sha1};
    use std::io::Read;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("sep-huge");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let up = Upstream::new(&root);
    for (k, v) in [
        ("core.compression", "0"),
        ("core.looseCompression", "0"),
        ("pack.compression", "0"),
    ] {
        git(&up.bare, &["config", k, v]);
    }
    commit_files(&up, "c1", &[("a/small.txt", Some(b"small\n".to_vec()))]);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());
    pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);
    mirror(&root, &store, &up, &[]).unwrap();

    // The huge commit, made with plumbing in the upstream (no 4 GiB push).
    let n: u64 = (4 << 30) + 12_345;
    let src = root.join("huge.bin");
    let want = write_noise_file(&src, n, 99);
    let blob = git(&up.bare, &["hash-object", "-w", src.to_str().unwrap()])
        .trim()
        .to_string();
    assert_eq!(blob, want.to_string());
    let index = root.join("tmp-index");
    let with_index = |args: &[&str]| -> String {
        let mut c = git_cmd(&up.bare);
        c.env("GIT_INDEX_FILE", &index).args(args);
        let out = c.output().unwrap();
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    with_index(&["read-tree", "main"]);
    with_index(&[
        "update-index",
        "--add",
        "--cacheinfo",
        &format!("100644,{blob},big/huge.bin"),
    ]);
    let tree = with_index(&["write-tree"]);
    let commit = with_index(&[
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@t",
        "commit-tree",
        &tree,
        "-p",
        "main",
        "-m",
        "huge",
    ]);
    git(&up.bare, &["update-ref", "refs/heads/main", &commit]);

    // The window copies it, streamed, in bounded memory.
    // (The daemon's own peak: the upstream's git processes are not ours to bound.)
    let t0 = std::time::Instant::now();
    let log = pipeline(&root, &store, &up, "15m", &[]);
    let rss = log
        .lines()
        .find_map(|l| l.split("peak RSS ").nth(1))
        .unwrap()
        .to_string();
    eprintln!(
        "pipeline window with a {n}-byte blob: {:.1}s, peak RSS {rss}",
        t0.elapsed().as_secs_f64()
    );
    assert!(
        rss.ends_with("KiB")
            || (rss.ends_with("MiB")
                && rss.trim_end_matches(" MiB").parse::<f64>().unwrap() < 256.0),
        "RSS bounded by buffers, not blob size: {rss}"
    );

    let cache = root.join("cache");
    let server = Server::start(&[
        "--store",
        &store,
        "--upstream",
        up.bare.to_str().unwrap(),
        "--cache-dir",
        cache.to_str().unwrap(),
    ]);
    // Loose: inflated and hashed as it streams in.
    let t0 = std::time::Instant::now();
    let mut c = server.client();
    let mut r = c
        .request("GET", &format!("/gvfs/objects/{blob}"), &[], b"")
        .unwrap();
    assert_eq!(r.status, 200);
    let mut z = zlib_rs::Inflate::new(true, 15);
    let mut h = Sha1::new();
    let (mut inbuf, mut outbuf) = (vec![0u8; 1 << 20], vec![0u8; 4 << 20]);
    let mut total_out = 0u64;
    loop {
        let k = r.read(&mut inbuf).unwrap();
        if k == 0 {
            break;
        }
        let mut input = &inbuf[..k];
        while !input.is_empty() {
            let (i0, o0) = (z.total_in(), z.total_out());
            z.decompress(input, &mut outbuf, zlib_rs::InflateFlush::NoFlush)
                .unwrap();
            let used = (z.total_in() - i0) as usize;
            let made = (z.total_out() - o0) as usize;
            h.update(&outbuf[..made]);
            total_out += made as u64;
            input = &input[used..];
            if used == 0 && made == 0 {
                break;
            }
        }
    }
    drop(r);
    let header = format!("blob {n}\0");
    assert_eq!(total_out, n + header.len() as u64);
    assert_eq!(
        Oid(h.finalize().into()).to_string(),
        blob,
        "loose object hashes to the blob"
    );
    eprintln!("loose GET of {n} bytes: {:.1}s", t0.elapsed().as_secs_f64());

    // In a pack, piped into git index-pack (which re-hashes it).
    let t0 = std::time::Instant::now();
    let check = fresh_repo(&root, "check");
    let mut ip = git_cmd(&check)
        .args(["index-pack", "--stdin", "--strict", "--keep=t"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    {
        let mut sink = ip.stdin.take().unwrap();
        let mut r = c
            .request(
                "POST",
                "/gvfs/objects",
                &[("Content-Type", "application/json"), ("Accept", CT_PACK)],
                json_ids(std::slice::from_ref(&blob), None).as_bytes(),
            )
            .unwrap();
        assert_eq!(r.status, 200);
        std::io::copy(&mut r, &mut sink).unwrap();
    }
    assert!(ip.wait().unwrap().success(), "index-pack accepted the pack");
    let s: u64 = git(&check, &["cat-file", "-s", &blob])
        .trim()
        .parse()
        .unwrap();
    assert_eq!(s, n);
    eprintln!(
        "POST pack of {n} bytes into index-pack: {:.1}s",
        t0.elapsed().as_secs_f64()
    );

    // Ranges beyond 4 GiB, in place.
    for (off, len) in [
        (0u64, 100usize),
        ((4 << 30) - 50, 100),
        (n - 777, 777),
        (3 << 30, 65_536 * 3),
    ] {
        let (status, _, body) =
            server.get(&format!("/gvfs/blobs/{blob}?offset={off}&length={len}"));
        assert_eq!(status, 200);
        assert!(body == file_range(&src, off, len), "range {off}+{len}");
    }
    server.assert_clean();
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// `mirror` killed (SIGKILL) at arbitrary points and run again: it resumes from what it
/// published, the result is complete and correct with every blob listed once, and the
/// packs a killed run uploaded but never published are invisible until GC removes them.
#[test]
fn mirror_survives_being_killed() {
    let root = tempdir("sep-kill");
    let up = Upstream::new(&root);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());
    let mut files: Vec<(String, Option<Vec<u8>>)> = (0..60u32)
        .map(|i| {
            (
                format!("d{}/f{i}.bin", i % 6),
                Some(noise(60_000 + i as usize * 997, u64::from(i) + 1)),
            )
        })
        .collect();
    files.push(("big/one.bin".into(), Some(large((16 << 20) + 11))));
    files.push(("big/two.bin".into(), Some(noise((16 << 20) + 13, 77))));
    let refs: Vec<(&str, Option<Vec<u8>>)> =
        files.iter().map(|(p, c)| (p.as_str(), c.clone())).collect();
    let g = commit_files(&up, "c1", &refs);
    pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);

    let work = root.join("mirror-work");
    std::fs::create_dir_all(&work).unwrap();
    // Two runs that publish only at the end (killed: their finished packs are uploaded
    // but never listed), then one that publishes as it goes.
    for (delay_ms, publish) in [(300u64, "3600"), (500, "3600"), (400, "0")] {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_gvfs-daemon"));
        cmd.args(["mirror", "--store", &store, "--upstream"])
            .arg(&up.bare)
            .args(["--work-dir", work.to_str().unwrap(), "--jobs", "2"])
            .args([
                "--publish-secs",
                publish,
                "--blob-pack-mib",
                "1",
                "--rate-mbps",
                "240",
            ])
            .stderr(std::process::Stdio::null());
        gvfs_daemon::upstream::hermetic_git_env(&mut cmd);
        let mut child = cmd.spawn().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        let _ = child.kill();
        let _ = child.wait();
    }
    let s = Store::open(&store, "").unwrap();
    let partial = blobcopy::load(&s).unwrap().0;
    assert!(!partial.complete(), "killed before the end");
    assert!(
        partial.blobs() > 0,
        "the last killed run published as it went"
    );
    let listed_then = partial.packs.len() + partial.segments.len();
    let orphans = s.list("blobs/").unwrap().len() - listed_then;
    assert!(orphans > 0, "killed runs left unpublished packs");
    let log = mirror(&root, &store, &up, &["--blob-pack-mib", "1"]).unwrap();
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(bm.complete(), "{log}");
    let stored = store_blobs(&root, &store_dir);
    for (id, at) in commit_blobs(&up.bare, &["-1", &g]) {
        let content = stored.get(&id).unwrap_or_else(|| panic!("{at} missing"));
        assert!(*content == cat_blob(&up.bare, &id), "{at} differs");
    }
    let records: usize = bm.segments.iter().map(|m| m.blobs as usize).sum();
    assert_eq!(records, stored.len(), "a blob listed twice");

    // Unpublished uploads are not listed; the pipeline's GC removes them.
    let listed: std::collections::HashSet<String> = bm
        .packs
        .iter()
        .map(|p| gvfs_daemon::blobs::pack_key(&p.id))
        .chain(
            bm.segments
                .iter()
                .map(|m| gvfs_daemon::blobs::segment_key(&m.id)),
        )
        .collect();
    let on_disk = || -> Vec<String> {
        s.list("blobs/")
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect()
    };
    let far = (gvfs_daemon::util::now_secs() + 86_400).to_string();
    pipeline(
        &root,
        &store,
        &up,
        "15m",
        &["--now", &far, "--gc-grace-secs", "60"],
    );
    let mut left = on_disk();
    left.sort();
    let mut want: Vec<String> = listed.into_iter().collect();
    want.sort();
    assert_eq!(left, want, "only what blobs.bin lists remains");
    assert_eq!(store_blobs(&root, &store_dir).len(), stored.len());
    let _ = std::fs::remove_dir_all(&root);
}

fn work_git(up: &Upstream, args: &[&str]) -> String {
    let mut all = vec!["-c", "user.name=t", "-c", "user.email=t@t"];
    all.extend_from_slice(args);
    git(&up.work, &all)
}

/// Commits after genesis that reuse versions from before genesis: an old side branch
/// merged (git leaves its base's subtrees out of the window's pack) and a revert against an
/// upstream with reachability bitmaps (which leaves out every subtree the haves reach).
/// The mirror copies the whole history, so the versions a window's pack leaves out are in
/// the store already: whether the mirror runs before the windows (store A) or after them
/// (store B), every blob of every commit is in the store once, and an edge serves them all
/// from it, none through the proxy.
#[test]
fn reused_old_versions_are_in_the_store() {
    let root = tempdir("sep-reused");
    let up = Upstream::new(&root);
    let stores: Vec<(std::path::PathBuf, String)> = ["store-a", "store-b"]
        .iter()
        .map(|n| {
            let d = root.join(n);
            let spec = format!("fs:{}", d.display());
            (d, spec)
        })
        .collect();
    let (store_a, store_b) = (&stores[0].1, &stores[1].1);
    commit_files(
        &up,
        "c1",
        &[
            ("lib/foo/a.c", Some(b"a v1\n".to_vec())),
            ("lib/bar/c.c", Some(b"c v1\n".to_vec())),
            ("other/x.txt", Some(b"x\n".to_vec())),
        ],
    );
    let x = commit_files(&up, "X", &[("lib/foo/a.c", Some(b"a v2\n".to_vec()))]);
    work_git(&up, &["branch", "topic", &x]);
    commit_files(&up, "Y", &[("lib/bar/c.c", Some(b"c v2\n".to_vec()))]);
    for (_, spec) in &stores {
        pipeline(&root, spec, &up, "15m", &["--blobs", "separate"]);
    }
    mirror(&root, store_a, &up, &[]).unwrap();

    // The side branch's commit keeps lib/bar from before genesis (c v1).
    work_git(&up, &["checkout", "-q", "topic"]);
    std::fs::write(up.work.join("other/y.txt"), b"y\n").unwrap();
    work_git(&up, &["add", "-A"]);
    work_git(&up, &["commit", "-q", "-m", "s1"]);
    work_git(&up, &["checkout", "-q", "main"]);
    work_git(
        &up,
        &["merge", "-q", "--no-ff", "-m", "merge topic", "topic"],
    );
    work_git(&up, &["push", "-q", "origin", "HEAD:main"]);
    for (_, spec) in &stores {
        pipeline(&root, spec, &up, "15m", &[]);
    }
    // A revert of X restores lib/foo from before genesis (a v1).
    work_git(&up, &["revert", "--no-edit", &x]);
    work_git(&up, &["push", "-q", "origin", "HEAD:main"]);
    git(&up.bare, &["repack", "-adbq"]);
    for (_, spec) in &stores {
        pipeline(&root, spec, &up, "15m", &[]);
    }
    let log = mirror(&root, store_b, &up, &[]).unwrap();

    let history = git_blobs(&up.bare, &["--all"]);
    let reused: Vec<String> = [format!("{x}^:lib/foo/a.c"), format!("{x}:lib/bar/c.c")]
        .iter()
        .map(|spec| rev(&up.bare, spec))
        .collect();
    for (dir, spec) in &stores {
        let s = Store::open(spec, "").unwrap();
        let (bm, _) = blobcopy::load(&s).unwrap();
        assert!(bm.complete(), "{spec}\n{log}");
        let stored = store_blobs(&root, dir);
        for id in history.keys().chain(&reused) {
            assert!(
                stored.contains_key(id),
                "{spec}: {id} not in the store\n{log}"
            );
        }
        let records: usize = bm.segments.iter().map(|m| m.blobs as usize).sum();
        assert_eq!(records, stored.len(), "{spec}: a blob listed twice");
    }

    // Every blob of every commit, through an edge of each store, from the store: git
    // re-hashes each one.
    let ids: Vec<String> = history.keys().cloned().collect();
    for (i, (_, spec)) in stores.iter().enumerate() {
        let cache = root.join(format!("cache-{i}"));
        let server = Server::start(&[
            "--store",
            spec,
            "--upstream",
            up.bare.to_str().unwrap(),
            "--poll-ms",
            "100",
            "--cache-dir",
            cache.to_str().unwrap(),
        ]);
        let loose = fresh_repo(&root, &format!("loose-{i}"));
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
        server.assert_clean();
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// A blob a sealed tree names that a complete store has lost track of (here: its segment
/// dropped from `blobs.bin`) is a fault, not a 404: an edge stays ready, serves it through
/// the proxy, and counts it in `gvfs_blobs_missing_total` once per upstream fetch.
#[test]
fn a_blob_missing_from_a_complete_store_is_served_and_counted() {
    let root = tempdir("sep-missing");
    let up = Upstream::new(&root);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());
    let files: Vec<(String, Option<Vec<u8>>)> = (0..6u32)
        .map(|i| (format!("d/f{i}.txt"), Some(text(100 + i))))
        .collect();
    let refs: Vec<(&str, Option<Vec<u8>>)> =
        files.iter().map(|(p, c)| (p.as_str(), c.clone())).collect();
    let g = commit_files(&up, "c1", &refs);
    pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);
    mirror(&root, &store, &up, &[]).unwrap();
    let t = commit_files(&up, "c2", &[("n/new.txt", Some(b"new\n".to_vec()))]);
    pipeline(&root, &store, &up, "15m", &[]);
    let lost: Vec<String> = (0..6)
        .map(|i| rev(&up.bare, &format!("{g}:d/f{i}.txt")))
        .collect();

    // blobs.bin forgets the segment that lists c1's blobs.
    let s = Store::open(&store, "").unwrap();
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(bm.complete());
    let first = Oid::from_hex(lost[0].as_bytes()).unwrap();
    let dropped = bm
        .segments
        .iter()
        .find(|m| {
            blobcopy::open_segment(&s, &root, m)
                .unwrap()
                .find(&first)
                .is_some()
        })
        .unwrap()
        .clone();
    blobcopy::update(&s, |bm| {
        bm.segments.retain(|x| x.id != dropped.id);
        Ok(())
    })
    .unwrap();

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
    let (status, ct, body) = server.get(&format!("/gvfs/objects/{}", lost[0]));
    assert_eq!((status, ct.as_str()), (200, CT_LOOSE));
    let loose = fresh_repo(&root, "loose");
    let dir = loose.join("objects").join(&lost[0][..2]);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(&lost[0][2..]), body).unwrap();
    git(&loose, &["fsck", "--strict", "--no-dangling"]);
    assert_eq!(server.metric("gvfs_blobs_missing_total"), 1);
    // A POST of that one (now in the proxy's cache) and two more: two more fetched.
    let batch: Vec<String> = lost[..3].to_vec();
    let (status, _, pack) =
        server.post("/gvfs/objects", json_ids(&batch, None).as_bytes(), CT_PACK);
    assert_eq!(status, 200);
    let posted = fresh_repo(&root, "post");
    git_stdin(
        &posted,
        &["index-pack", "--stdin", "--strict", "--keep=t"],
        &pack,
    );
    assert_eq!(cat_batch(&posted, &batch), cat_batch(&up.bare, &batch));
    assert_eq!(server.metric("gvfs_blobs_missing_total"), 3);
    // The window's blob is in the store, and the edge stays ready.
    let new_id = rev(&up.bare, &format!("{t}:n/new.txt"));
    let before = server.metric("gvfs_blobs_from_store_total");
    assert_eq!(server.get(&format!("/gvfs/objects/{new_id}")).0, 200);
    assert_eq!(server.metric("gvfs_blobs_from_store_total"), before + 1);
    assert_eq!(server.get("/healthz").0, 200);
    server.assert_clean();
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// A run that copied a window's blobs and published `blobs.bin`, then died before the
/// manifest: the next run copies nothing, and its manifest must still name the
/// `blobs.bin` generation its trees need.
#[test]
fn a_manifest_names_the_blobs_generation_its_trees_need() {
    let root = tempdir("sep-gen");
    let up = Upstream::new(&root);
    let store_dir = root.join("store");
    let store = format!("fs:{}", store_dir.display());
    let g = commit_files(&up, "c1", &[("a.txt", Some(b"one\n".to_vec()))]);
    pipeline(&root, &store, &up, "15m", &["--blobs", "separate"]);
    mirror(&root, &store, &up, &[]).unwrap();
    let t = commit_files(&up, "c2", &[("b.txt", Some(b"two\n".to_vec()))]);

    // What the dead run left: the window's blobs, published.
    let s = Store::open(&store, "").unwrap();
    let mut u = gvfs_daemon::upstream::Upstream::open(
        up.bare.to_str().unwrap(),
        "main",
        &gvfs_daemon::upstream::Auth::default(),
    )
    .unwrap();
    let new: Vec<Oid> = git_blobs(&up.bare, &[&format!("{g}..{t}")])
        .keys()
        .map(|k| Oid::from_hex(k.as_bytes()).unwrap())
        .collect();
    let work = root.join("dead-run");
    let (published, _) = gvfs_daemon::separate::copy_needed(
        &mut u,
        &s,
        &work,
        &new,
        gvfs_daemon::separate::ITEM_BYTES,
        2,
    )
    .unwrap();
    let needed = published.unwrap().generation;

    let log = pipeline(&root, &store, &up, "15m", &[]);
    let m = manifest(&store_dir);
    assert_eq!(m.tip.to_string(), t);
    assert!(
        m.blobs_generation >= needed,
        "manifest names blobs.bin generation {}, its trees need {needed}\n{log}",
        m.blobs_generation
    );
    let _ = std::fs::remove_dir_all(&root);
}
