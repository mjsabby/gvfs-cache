#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;

use bytes::Bytes;
use gvfs_daemon::Oid;
use gvfs_daemon::codec::{Kind, Zlib, object_id, pack_header, write_entry_header};
use gvfs_daemon::ingest::IngestArena;
use gvfs_daemon::manifest::{Manifest, Tier};
use gvfs_daemon::slab::{Builder, Limits, NO_BASE, Record, Slab};
use gvfs_daemon::storage::{Storage, StoreKind};
use gvfs_daemon::tree::TreeLayout;
use gvfs_daemon::upstream::git_command;
use sha1::{Digest, Sha1};

pub fn limits(layout: TreeLayout) -> Limits {
    Limits {
        objects: 4096,
        pack_bytes: 16 << 20,
        object_bytes: 1 << 20,
        links: 1 << 17,
        cache_bytes: 4 << 20,
        tree_layout: layout,
        strict_hfs: false,
    }
}

pub fn command(path: &Path) -> Command {
    let mut command = git_command();
    command.arg("--git-dir").arg(path);
    command
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000");
    command
}

pub fn run(command: &mut Command, input: &[u8]) -> Output {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "{}: {}",
        result.status,
        String::from_utf8_lossy(&result.stderr)
    );
    result
}

pub fn oid_command(path: &Path, args: &[&str], input: &[u8]) -> Oid {
    let output = run(command(path).args(args), input);
    Oid::parse(
        output
            .stdout
            .split(|byte| byte.is_ascii_whitespace())
            .next()
            .unwrap(),
    )
    .unwrap()
}

pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub git: PathBuf,
    pub tips: Vec<Oid>,
    pub trees: Vec<Oid>,
    pub blobs: Vec<Oid>,
    pub pack: Bytes,
}

impl Fixture {
    pub fn new(versions: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let git = dir.path().join("source.git");
        run(
            git_command().args(["init", "--quiet", "--bare"]).arg(&git),
            b"",
        );
        let blob = |bytes: &[u8]| oid_command(&git, &["hash-object", "-w", "--stdin"], bytes);
        let mut files: Vec<Oid> = (0..96)
            .map(|i| {
                blob(format!("~~Git-hfs 1.0\nblob:{i:064X}00\nsize:{}\n", i * 1024).as_bytes())
            })
            .collect();
        let attributes = blob(b"*.txt filter=hfs\n");
        let symlink = blob(b"src/file0040.txt");
        let empty_tree = oid_command(&git, &["mktree"], b"");
        let mut tips = Vec::new();
        let mut trees = Vec::new();
        let mut blobs = Vec::new();
        for version in 0..versions {
            let position = version.wrapping_mul(37) % files.len();
            files[position] = blob(
                format!(
                    "~~Git-hfs 1.0\r\nblob:{:064X}00\r\nsize:{}\r\n",
                    10000 + version,
                    1024 + version
                )
                .as_bytes(),
            );
            blobs.push(files[position]);
            let mut listing = String::new();
            for (i, oid) in files.iter().enumerate() {
                listing.push_str(&format!("100644 blob {oid}\tfile{i:04}.txt\n"));
            }
            let tree = oid_command(&git, &["mktree"], listing.as_bytes());
            trees.push(tree);
            let mut directories = String::new();
            for i in 0..32 {
                directories.push_str(&format!("040000 tree {empty_tree}\tdir{i:03}\n"));
            }
            let root = oid_command(&git, &["mktree", "--missing"], format!(
                "{directories}100644 blob {attributes}\t.gitattributes\n120000 blob {symlink}\tlink\n040000 tree {tree}\tsrc\n160000 commit {}\tmodule\n", Oid([42; 20])).as_bytes());
            let mut commit = command(&git);
            commit.args(["commit-tree", &root.to_string()]);
            if let Some(parent) = tips.last() {
                commit.args(["-p", &format!("{parent}")]);
            }
            let output = run(
                &mut commit,
                format!("fixture version {version}\n").as_bytes(),
            );
            tips.push(
                Oid::parse(
                    output
                        .stdout
                        .split(|byte| byte.is_ascii_whitespace())
                        .next()
                        .unwrap(),
                )
                .unwrap(),
            );
        }
        let tip = *tips.last().unwrap();
        run(
            command(&git).args(["update-ref", "refs/heads/main", &tip.to_string()]),
            b"",
        );
        let pack = run(
            command(&git).args([
                "pack-objects",
                "--all",
                "--stdout",
                "--delta-base-offset",
                "--window=64",
                "--depth=50",
            ]),
            b"",
        );
        Self {
            dir,
            git,
            tips,
            trees,
            blobs,
            pack: Bytes::from(pack.stdout),
        }
    }

    pub fn tip(&self) -> Oid {
        *self.tips.last().unwrap()
    }
    pub fn set_tip(&self, index: usize) {
        run(
            command(&self.git).args([
                "update-ref",
                "refs/heads/main",
                &self.tips[index].to_string(),
            ]),
            b"",
        );
    }
    pub fn bytes(&self, oid: Oid) -> Vec<u8> {
        let kind = run(
            command(&self.git).args(["cat-file", "-t", &oid.to_string()]),
            b"",
        );
        let kind = std::str::from_utf8(&kind.stdout).unwrap().trim();
        run(
            command(&self.git).args(["cat-file", kind, &oid.to_string()]),
            b"",
        )
        .stdout
    }
}

pub fn decode(
    pack: &[u8],
    tip: Oid,
    layout: TreeLayout,
    tier: Tier,
    start: u64,
    end: u64,
    known: &[Arc<Slab>],
) -> Slab {
    let limits = limits(layout);
    let mut arena = IngestArena::with_layout(limits, layout.for_tier(tier), 65536).unwrap();
    let mut builder = Builder::new(limits).unwrap();
    let hot = gvfs_platform::HotRegion::enter();
    let result = arena.decode(pack, tip, known, &mut builder);
    let allocations = hot.allocations();
    drop(hot);
    result.unwrap();
    assert_eq!(allocations, 0, "ingestion must not allocate after startup");
    let slab = builder.finish(tier, start, end, tip).unwrap();
    slab.validate().unwrap();
    slab
}

pub fn verify_native(slab: &Slab, tip: Option<Oid>) {
    let dir = tempfile::tempdir().unwrap();
    let git = dir.path().join("verify.git");
    run(
        git_command().args(["init", "--bare", "--quiet"]).arg(&git),
        b"",
    );
    let prefix = git
        .join("objects/pack")
        .join(format!("pack-{}", slab.reference.id));
    std::fs::write(prefix.with_extension("pack"), slab.pack.as_ref().unwrap()).unwrap();
    std::fs::write(prefix.with_extension("idx"), &slab.idx).unwrap();
    run(
        git_command()
            .args(["verify-pack", "-v"])
            .arg(prefix.with_extension("idx")),
        b"",
    );
    if let Some(tip) = tip {
        run(
            command(&git).args(["update-ref", "refs/heads/main", &tip.to_string()]),
            b"",
        );
        run(
            command(&git).args(["fsck", "--strict", "--full", "--no-reflogs"]),
            b"",
        );
    }
    assert_eq!(
        std::fs::read_dir(git.join("objects/pack"))
            .unwrap()
            .filter(|entry| entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|extension| extension == "pack"))
            .count(),
        1
    );
}

pub fn record(kind: Kind, bytes: &[u8]) -> Record {
    Record {
        oid: object_id(kind, bytes).unwrap(),
        offset: 0,
        len: 0,
        size: bytes.len() as u64,
        crc: 0,
        kind,
        graph_offset: gvfs_daemon::graph::NO_GRAPH,
        pointer: None,
        base: NO_BASE,
        delta_size: 0,
        path: 0,
        epoch: 0,
    }
}

pub fn full_entry(kind: Kind, bytes: &[u8]) -> Vec<u8> {
    let mut zlib = Zlib::new(1);
    let mut compressed = vec![0; bytes.len() + bytes.len() / 100 + 1024];
    let len = zlib.deflate(bytes, &mut compressed).unwrap();
    compressed.truncate(len);
    let mut header = [0; 10];
    let len = write_entry_header(kind, bytes.len(), &mut header).unwrap();
    let mut out = header[..len].to_vec();
    out.extend_from_slice(&compressed);
    out
}

pub fn pack(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut out = pack_header(entries.len() as u32).to_vec();
    for entry in entries {
        out.extend_from_slice(entry);
    }
    let digest: [u8; 20] = Sha1::digest(&out).into();
    out.extend_from_slice(&digest);
    out
}

pub fn storage(dir: &Path) -> Arc<Storage> {
    Arc::new(
        Storage::open(
            dir.join("store").to_str().unwrap(),
            dir.join("cache"),
            65536,
            16 << 20,
            StoreKind::Generic,
            None,
            None,
            2,
        )
        .unwrap(),
    )
}

pub async fn publish(storage: &Storage, slab: &Slab) {
    storage.upload(slab).await.unwrap();
    storage
        .publish_manifest(&Manifest::genesis(slab.reference).unwrap(), None)
        .await
        .unwrap();
}
