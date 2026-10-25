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

/// Commits `files` (`None` deletes) and pushes; returns the new tip.
fn commit_files(up: &Upstream, msg: &str, files: &[(&str, Option<Vec<u8>>)]) -> String {
    for (path, content) in files {
        let p = up.work.join(path);
        match content {
            Some(c) => {
                std::fs::create_dir_all(p.parent().unwrap()).unwrap();
                std::fs::write(&p, c).unwrap();
            }
            None => std::fs::remove_file(&p).unwrap(),
        }
    }
    git(&up.work, &["add", "-A"]);
    git(
        &up.work,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-q",
            "-m",
            msg,
        ],
    );
    git(&up.work, &["push", "-q", "origin", "HEAD:main"]);
    up.tip()
}

fn text(version: u32) -> Vec<u8> {
    (0..3000u32)
        .map(|l| {
            if l % 211 == version % 211 {
                format!("line {l} edited in version {version}\n")
            } else {
                format!("line {l} of a text file that changes a little\n")
            }
        })
        .collect::<String>()
        .into_bytes()
}

fn noise(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// Compressible, but large enough to be copied alone.
fn large(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i / 4096) as u8 ^ (i % 7) as u8).collect()
}

/// `(id, size)` of every blob `rev-list --objects <revs>` lists, from git.
fn git_blobs(bare: &Path, revs: &[&str]) -> BTreeMap<String, u64> {
    let mut args = vec!["rev-list", "--objects"];
    args.extend_from_slice(revs);
    let ids: Vec<String> = git(bare, &args)
        .lines()
        .map(|l| l[..40].to_string())
        .collect();
    let input: String = ids.iter().map(|i| format!("{i}\n")).collect();
    let out = git_stdin(
        bare,
        &[
            "cat-file",
            "--batch-check=%(objectname) %(objecttype) %(objectsize)",
        ],
        input.as_bytes(),
    );
    String::from_utf8(out)
        .unwrap()
        .lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.split(' ').collect();
            (f[1] == "blob").then(|| (f[0].to_string(), f[2].parse().unwrap()))
        })
        .collect()
}

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

fn cat_blob(bare: &Path, id: &str) -> Vec<u8> {
    let mut cmd = git_cmd(bare);
    let out = cmd.args(["cat-file", "blob", id]).output().unwrap();
    assert!(out.status.success());
    out.stdout
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
    assert!(m.separate_blobs() && m.blobs_partial());
    assert_eq!(
        slab_kinds(&store_dir, &m.baseline),
        vec![OBJ_COMMIT, OBJ_TREE]
    );
    assert_eq!(all_sizes(&store_dir, &m), git_blobs(&up.bare, &["--all"]));
    let s = Store::open(&store, "").unwrap();
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(bm.segments.is_empty() && bm.tip_complete.is_none());
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

/// Blob ids (with their paths) of every commit in `revs`, from git.
fn commit_blobs(bare: &Path, revs: &[&str]) -> BTreeMap<String, String> {
    let mut args = vec!["rev-list"];
    args.extend_from_slice(revs);
    let mut out = BTreeMap::new();
    for c in git(bare, &args).lines() {
        for l in git(bare, &["ls-tree", "-r", c]).lines() {
            let (meta, path) = l.split_once('\t').unwrap();
            let f: Vec<&str> = meta.split(' ').collect();
            if f[1] == "blob" {
                out.insert(f[2].to_string(), format!("{c}:{path}"));
            }
        }
    }
    out
}

#[test]
fn mirror_completes_every_commit_since_genesis_and_resumes() {
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

    // In slices: two packs, then the rest.
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
    let log = mirror(&root, &store, &up, &["--blob-pack-mib", "1"]).unwrap();
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(bm.complete(), "{log}");
    assert_eq!(bm.genesis.to_string(), g);

    // Every blob of every commit since genesis, byte for byte: the genesis tree's (even
    // the file deleted since) and the window's.
    let stored = store_blobs(&root, &store_dir);
    let since_genesis = commit_blobs(&up.bare, &["--ancestry-path", &format!("{g}..{t}")]);
    let at_genesis = commit_blobs(&up.bare, &["-1", &g]);
    for (id, at) in since_genesis.iter().chain(&at_genesis) {
        let content = stored
            .get(id)
            .unwrap_or_else(|| panic!("{at} ({id}) missing\n{log}"));
        assert!(*content == cat_blob(&up.bare, id), "{at} differs");
    }
    // Nothing was copied twice: one record per blob across all segments.
    let records: usize = bm.segments.iter().map(|m| m.blobs as usize).sum();
    assert_eq!(records, stored.len(), "a blob listed twice");
    // Versions only commits before genesis reference were not copied.
    let old_only: Vec<String> = commit_blobs(&up.bare, &["-1", &c1])
        .into_keys()
        .filter(|id| !at_genesis.contains_key(id))
        .collect();
    assert!(!old_only.is_empty() && old_only.iter().all(|id| !stored.contains_key(id)));

    // Again: nothing to copy.
    let log = mirror(&root, &store, &up, &[]).unwrap();
    assert!(log.contains("to copy in 0 items"), "{log}");

    // The whole history: every blob ever referenced.
    let log = mirror(
        &root,
        &store,
        &up,
        &["--scope", "history", "--blob-pack-mib", "1"],
    )
    .unwrap();
    let stored = store_blobs(&root, &store_dir);
    for id in git_blobs(&up.bare, &["--all"]).keys() {
        assert!(
            stored.contains_key(id),
            "{id} missing after --scope history\n{log}"
        );
    }
    let (bm, _) = blobcopy::load(&s).unwrap();
    assert!(bm.complete() && bm.since_ts == 1);
    let records: usize = bm.segments.iter().map(|m| m.blobs as usize).sum();
    assert_eq!(records, stored.len(), "a blob listed twice");
    let _ = std::fs::remove_dir_all(&root);
}
