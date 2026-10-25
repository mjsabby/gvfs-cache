//! Upstream Git drivers (`GitUpstream`) for `gvfs-daemon`:
//!
//! 1. [`LocalGitUpstream`] — reads refs and streams delta packs (`have_tip -> want_tip`)
//!    from a local Git repository (such as `/home/muks/SearchGold`). Strips injected
//!    `GIT_CONFIG_*` environment variables so bare/cloned repos work hermetically.
//! 2. [`InMemoryGitUpstream`] — deterministic in-memory upstream for unit and crash-recovery
//!    tests (supports simulating force-pushes and garbage-collected `have` commits).

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

use gvfs_core::pack::{ObjType, encode_entry_header, pack_header};
use gvfs_core::zlib::{Deflater, compress_bound};
use gvfs_core::{Oid20, ZERO_OID, object_id, sha1_parts};

#[derive(Debug)]
pub enum UpstreamError {
    /// Upstream reported `ERR upload-pack: not our ref <oid>` (e.g., force-push + GC).
    NotOurRef(Oid20),
    Io(std::io::Error),
    Git(String),
    MissingObject(Oid20),
}

impl core::fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotOurRef(oid) => write!(f, "ERR upload-pack: not our ref {oid}"),
            Self::Io(e) => write!(f, "upstream I/O error: {e}"),
            Self::Git(m) => write!(f, "upstream git error: {m}"),
            Self::MissingObject(oid) => write!(f, "missing upstream object: {oid}"),
        }
    }
}

impl std::error::Error for UpstreamError {}

impl From<std::io::Error> for UpstreamError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Unified interface for querying upstream `HEAD` and fetching a delta `.pack` stream.
pub trait GitUpstream: Send + Sync + core::fmt::Debug {
    /// Current `refs/heads/<branch>` tip commit SHA (`get_main_head_oid` in PDF p. 166).
    fn get_main_head_oid(&self) -> Result<Oid20, UpstreamError>;

    /// Streams a Git Pack v2 covering `(have_tip -> want_tip)`.
    /// If `have_tip == ZERO_OID`, streams all reachable objects from `want_tip`.
    /// If `have_tip` was garbage-collected after a force-push, returns `UpstreamError::NotOurRef`.
    fn fetch_delta_pack(
        &self,
        have_tip: Oid20,
        want_tip: Oid20,
        extra_haves: &[Oid20],
    ) -> Result<Vec<u8>, UpstreamError>;

    /// Reads a raw uncompressed `(ObjType, Vec<u8>)` object by OID, used only when an upstream
    /// sends a thin-pack `REF_DELTA` whose base was in an earlier commit outside the delta pack.
    fn read_base_object(&self, oid: &Oid20) -> Result<Option<(ObjType, Vec<u8>)>, UpstreamError> {
        let _ = oid;
        Ok(None)
    }

    /// If backed by a local `.git` directory, returns its path.
    fn local_git_dir(&self) -> Option<&Path> {
        None
    }
}

fn clean_git_cmd(git_dir: &Path) -> Command {
    let mut cmd = Command::new("git");
    cmd.arg(format!("--git-dir={}", git_dir.display()))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_CONFIG_COUNT");
    for i in 0..16 {
        cmd.env_remove(format!("GIT_CONFIG_KEY_{i}"));
        cmd.env_remove(format!("GIT_CONFIG_VALUE_{i}"));
    }
    cmd
}

/// Upstream backed by a local Git repository (either `<repo>/.git` or a bare `<repo>.git`),
/// such as `/home/muks/SearchGold`.
#[derive(Debug, Clone)]
pub struct LocalGitUpstream {
    git_dir: PathBuf,
    branch_ref: String,
    /// Optional override for testing incremental advances along an existing commit history.
    forced_head: ArcMutexOptionOid,
}

#[derive(Debug, Clone, Default)]
struct ArcMutexOptionOid(std::sync::Arc<Mutex<Option<Oid20>>>);

impl LocalGitUpstream {
    pub fn open(repo_or_git_dir: impl AsRef<Path>, branch: &str) -> Result<Self, UpstreamError> {
        let p = repo_or_git_dir.as_ref();
        let dot_git = p.join(".git");
        let git_dir = if dot_git.is_dir() {
            dot_git
        } else {
            p.to_path_buf()
        };
        if !git_dir.exists() {
            return Err(UpstreamError::Git(format!(
                "git directory does not exist: {}",
                git_dir.display()
            )));
        }
        let branch_ref = if branch.starts_with("refs/") {
            branch.to_string()
        } else {
            format!("refs/heads/{branch}")
        };
        Ok(Self {
            git_dir,
            branch_ref,
            forced_head: ArcMutexOptionOid::default(),
        })
    }

    pub fn set_simulated_head(&self, oid: Option<Oid20>) {
        let mut g = self
            .forced_head
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *g = oid;
    }

    pub fn git_dir(&self) -> &Path {
        &self.git_dir
    }

    /// Resolves any revision (e.g. `HEAD~5`, `HEAD~10`, `refs/heads/main`) to an `Oid20`.
    pub fn rev_parse(&self, rev: &str) -> Result<Oid20, UpstreamError> {
        let out = clean_git_cmd(&self.git_dir)
            .args(["rev-parse", "--verify", rev])
            .output()?;
        if !out.status.success() {
            return Err(UpstreamError::Git(format!(
                "git rev-parse {rev} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let hex = out.stdout.as_slice().trim_ascii();
        Oid20::from_hex(hex)
            .ok_or_else(|| UpstreamError::Git(format!("invalid oid from rev-parse {rev}")))
    }

    fn verify_commit_exists(&self, oid: &Oid20) -> bool {
        let hex = oid.to_string();
        clean_git_cmd(&self.git_dir)
            .args(["cat-file", "-e", &format!("{hex}^{{commit}}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }
}

impl GitUpstream for LocalGitUpstream {
    fn get_main_head_oid(&self) -> Result<Oid20, UpstreamError> {
        if let Some(forced) = *self
            .forced_head
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            return Ok(forced);
        }
        // Fast path: read `.git/refs/heads/<branch>` directly from disk without spawning a process.
        let ref_file = self.git_dir.join(&self.branch_ref);
        if let Ok(raw) = fs::read(&ref_file) {
            if let Some(oid) = Oid20::from_hex(raw.as_slice().trim_ascii()) {
                return Ok(oid);
            }
        }
        self.rev_parse(&self.branch_ref)
            .or_else(|_| self.rev_parse("HEAD"))
    }

    fn fetch_delta_pack(
        &self,
        have_tip: Oid20,
        want_tip: Oid20,
        extra_haves: &[Oid20],
    ) -> Result<Vec<u8>, UpstreamError> {
        if want_tip == ZERO_OID || have_tip == want_tip {
            let mut empty = Vec::with_capacity(32);
            empty.extend_from_slice(&pack_header(0));
            let sum = sha1_parts(&[&empty]);
            empty.extend_from_slice(&sum);
            return Ok(empty);
        }
        if !have_tip.is_zero() && extra_haves.is_empty() && !self.verify_commit_exists(&have_tip) {
            return Err(UpstreamError::NotOurRef(have_tip));
        }

        let mut rev_stdin = String::with_capacity(256);
        rev_stdin.push_str(&want_tip.to_string());
        rev_stdin.push('\n');
        if !have_tip.is_zero() && self.verify_commit_exists(&have_tip) {
            rev_stdin.push('^');
            rev_stdin.push_str(&have_tip.to_string());
            rev_stdin.push('\n');
        }
        for h in extra_haves {
            if !h.is_zero() && self.verify_commit_exists(h) {
                rev_stdin.push('^');
                rev_stdin.push_str(&h.to_string());
                rev_stdin.push('\n');
            }
        }

        let mut child = clean_git_cmd(&self.git_dir)
            .args(["pack-objects", "--stdout", "--revs"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(rev_stdin.as_bytes())?;
        }
        let out = child.wait_with_output()?;
        if !out.status.success() {
            return Err(UpstreamError::Git(format!(
                "git pack-objects failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(out.stdout)
    }

    fn read_base_object(&self, oid: &Oid20) -> Result<Option<(ObjType, Vec<u8>)>, UpstreamError> {
        let hex = oid.to_string();
        let t_out = clean_git_cmd(&self.git_dir)
            .args(["cat-file", "-t", &hex])
            .output()?;
        if !t_out.status.success() {
            return Ok(None);
        }
        let Some(kind) = ObjType::from_name(t_out.stdout.as_slice().trim_ascii()) else {
            return Ok(None);
        };
        let c_out = clean_git_cmd(&self.git_dir)
            .args(["cat-file", "-p", &hex])
            .output()?;
        if !c_out.status.success() {
            let raw_out = clean_git_cmd(&self.git_dir)
                .args(["cat-file", kind_str(kind), &hex])
                .output()?;
            if !raw_out.status.success() {
                return Ok(None);
            }
            return Ok(Some((kind, raw_out.stdout)));
        }
        let raw_out = clean_git_cmd(&self.git_dir)
            .args(["cat-file", kind_str(kind), &hex])
            .output()?;
        if !raw_out.status.success() {
            return Ok(None);
        }
        Ok(Some((kind, raw_out.stdout)))
    }

    fn local_git_dir(&self) -> Option<&Path> {
        Some(&self.git_dir)
    }
}

const fn kind_str(kind: ObjType) -> &'static str {
    match kind {
        ObjType::Commit => "commit",
        ObjType::Tree => "tree",
        ObjType::Blob => "blob",
        ObjType::Tag => "tag",
        ObjType::OfsDelta | ObjType::RefDelta => "blob",
    }
}

/// Deterministic in-memory Git repository upstream for unit tests, pipeline rollup tests,
/// and crash-recovery / force-push tests.
#[derive(Debug, Default)]
pub struct InMemoryGitUpstream {
    state: Mutex<InMemState>,
}

#[derive(Debug, Default)]
struct InMemState {
    head: Oid20,
    objects: HashMap<Oid20, (ObjType, Vec<u8>)>,
    commit_parents: HashMap<Oid20, Vec<Oid20>>,
    commit_objects: HashMap<Oid20, Vec<Oid20>>,
    gc_deleted_commits: Vec<Oid20>,
}

impl InMemoryGitUpstream {
    pub fn new() -> Self {
        Self::default()
    }

    /// Commits a set of files `(filename, raw_blob_bytes)` on top of `parent` (or `ZERO_OID`
    /// for root commit) and advances `HEAD` to the new commit, returning `(commit_oid, tree_oid, blob_oids)`.
    pub fn add_commit(
        &self,
        parent: Oid20,
        timestamp: u64,
        message: &str,
        files: &[(&str, &[u8])],
    ) -> (Oid20, Oid20, Vec<Oid20>) {
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut sorted_files: Vec<(&str, &[u8])> = files.to_vec();
        sorted_files.sort_unstable_by_key(|&(name, _)| name);

        let mut tree_buf = Vec::new();
        let mut blob_oids = Vec::with_capacity(sorted_files.len());
        let mut reachable_in_commit = Vec::new();

        for (name, content) in sorted_files {
            let b_oid = object_id(ObjType::Blob, content);
            s.objects.insert(b_oid, (ObjType::Blob, content.to_vec()));
            blob_oids.push(b_oid);
            reachable_in_commit.push(b_oid);
            tree_buf.extend_from_slice(b"100644 ");
            tree_buf.extend_from_slice(name.as_bytes());
            tree_buf.push(0);
            tree_buf.extend_from_slice(&b_oid.0);
        }

        let tree_oid = object_id(ObjType::Tree, &tree_buf);
        s.objects.insert(tree_oid, (ObjType::Tree, tree_buf));
        reachable_in_commit.push(tree_oid);

        let mut commit_buf = Vec::new();
        commit_buf.extend_from_slice(format!("tree {tree_oid}\n").as_bytes());
        let mut parents = Vec::new();
        if !parent.is_zero() {
            commit_buf.extend_from_slice(format!("parent {parent}\n").as_bytes());
            parents.push(parent);
        }
        commit_buf.extend_from_slice(
            format!(
                "author Test <test@example.com> {timestamp} +0000\ncommitter Test <test@example.com> {timestamp} +0000\n\n{message}\n"
            )
            .as_bytes(),
        );
        let commit_oid = object_id(ObjType::Commit, &commit_buf);
        s.objects.insert(commit_oid, (ObjType::Commit, commit_buf));
        reachable_in_commit.push(commit_oid);

        s.commit_parents.insert(commit_oid, parents);
        s.commit_objects.insert(commit_oid, reachable_in_commit);
        s.head = commit_oid;
        (commit_oid, tree_oid, blob_oids)
    }

    /// Simulates a commit being garbage-collected on upstream ADO after a force-push (PDF p. 169).
    pub fn mark_commit_garbage_collected(&self, commit_oid: Oid20) {
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.gc_deleted_commits.push(commit_oid);
    }

    pub fn set_head(&self, head: Oid20) {
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.head = head;
    }

    fn collect_reachable(state: &InMemState, start: Oid20, out_commits: &mut Vec<Oid20>) {
        if start.is_zero() || out_commits.contains(&start) {
            return;
        }
        out_commits.push(start);
        if let Some(parents) = state.commit_parents.get(&start) {
            for &p in parents {
                Self::collect_reachable(state, p, out_commits);
            }
        }
    }
}

impl GitUpstream for InMemoryGitUpstream {
    fn get_main_head_oid(&self) -> Result<Oid20, UpstreamError> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(s.head)
    }

    fn fetch_delta_pack(
        &self,
        have_tip: Oid20,
        want_tip: Oid20,
        extra_haves: &[Oid20],
    ) -> Result<Vec<u8>, UpstreamError> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !have_tip.is_zero() && extra_haves.is_empty() && s.gc_deleted_commits.contains(&have_tip)
        {
            return Err(UpstreamError::NotOurRef(have_tip));
        }

        let mut have_commits = Vec::new();
        if !have_tip.is_zero() && !s.gc_deleted_commits.contains(&have_tip) {
            Self::collect_reachable(&s, have_tip, &mut have_commits);
        }
        for &h in extra_haves {
            if !h.is_zero() && !s.gc_deleted_commits.contains(&h) {
                Self::collect_reachable(&s, h, &mut have_commits);
            }
        }

        let mut want_commits = Vec::new();
        Self::collect_reachable(&s, want_tip, &mut want_commits);

        let mut excluded_oids = Vec::new();
        for c in &have_commits {
            if let Some(objs) = s.commit_objects.get(c) {
                for &o in objs {
                    if !excluded_oids.contains(&o) {
                        excluded_oids.push(o);
                    }
                }
            }
        }

        let mut to_pack = Vec::new();
        for c in &want_commits {
            if have_commits.contains(c) {
                continue;
            }
            if let Some(objs) = s.commit_objects.get(c) {
                for &o in objs {
                    if !excluded_oids.contains(&o) && !to_pack.contains(&o) {
                        to_pack.push(o);
                    }
                }
            }
        }

        let mut deflater = Deflater::new(1);
        let mut pack_bytes = Vec::with_capacity(4096);
        let count = u32::try_from(to_pack.len()).unwrap_or(0);
        pack_bytes.extend_from_slice(&pack_header(count));

        for oid in to_pack {
            let Some((kind, raw)) = s.objects.get(&oid) else {
                continue;
            };
            let mut hdr = [0u8; 10];
            let hdr_len =
                encode_entry_header(*kind, u64::try_from(raw.len()).unwrap_or(0), &mut hdr);
            pack_bytes.extend_from_slice(hdr.get(..hdr_len).unwrap_or(&[]));
            let mut zbuf = vec![0u8; compress_bound(raw.len())];
            let zlen = deflater
                .deflate(raw, &mut zbuf)
                .map_err(|e| UpstreamError::Git(e.to_string()))?;
            pack_bytes.extend_from_slice(zbuf.get(..zlen).unwrap_or(&[]));
        }

        let trailer = sha1_parts(&[&pack_bytes]);
        pack_bytes.extend_from_slice(&trailer);
        Ok(pack_bytes)
    }

    fn read_base_object(&self, oid: &Oid20) -> Result<Option<(ObjType, Vec<u8>)>, UpstreamError> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(s.objects.get(oid).cloned())
    }
}
