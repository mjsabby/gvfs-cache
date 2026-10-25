//! The slab store: a directory (local disk or a shared filesystem) holding immutable slab
//! files and the compare-and-swap `manifest.bin`.
//!
//! ```text
//! <root>/manifest.bin           replaced atomically; CAS on its generation under flock
//! <root>/manifest.lock
//! <root>/slabs/slab-<seq:08>.pack|.idx|.mphf
//! <root>/tmp/                   in-progress files, renamed into slabs/ when complete
//! ```
//!
//! Cloud object stores (Azure Blob with `If-Match`, S3 with conditional writes) fit the
//! same six operations; an edge in front of one would mirror the referenced slab files
//! into a local directory like this one before mapping them.

use core::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::manifest::{Manifest, ManifestError};

#[derive(Debug)]
pub enum StoreError {
    Io(std::io::Error),
    Manifest(ManifestError),
    /// The manifest changed underneath us (another pipeline run won the race).
    Conflict {
        expected: Option<u64>,
        found: Option<u64>,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "store i/o: {e}"),
            Self::Manifest(e) => write!(f, "manifest: {e}"),
            Self::Conflict { expected, found } => write!(
                f,
                "manifest generation conflict (expected {expected:?}, found {found:?})"
            ),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ManifestError> for StoreError {
    fn from(e: ManifestError) -> Self {
        Self::Manifest(e)
    }
}

#[derive(Debug, Clone)]
pub struct SlabPaths {
    pub pack: PathBuf,
    pub idx: PathBuf,
    pub mphf: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let root: PathBuf = root.into();
        fs::create_dir_all(root.join("slabs"))?;
        fs::create_dir_all(root.join("tmp"))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn manifest_path(&self) -> PathBuf {
        self.root.join("manifest.bin")
    }

    pub fn slab_paths(&self, seq: u64) -> SlabPaths {
        let base = self.root.join("slabs").join(format!("slab-{seq:08}"));
        SlabPaths {
            pack: base.with_extension("pack"),
            idx: base.with_extension("idx"),
            mphf: base.with_extension("mphf"),
        }
    }

    /// Where an in-progress slab's files live before publication.
    pub fn tmp_slab_paths(&self, seq: u64, tag: &str) -> SlabPaths {
        let base = self.root.join("tmp").join(format!("slab-{seq:08}-{tag}"));
        SlabPaths {
            pack: base.with_extension("pack"),
            idx: base.with_extension("idx"),
            mphf: base.with_extension("mphf"),
        }
    }

    pub fn tmp_path(&self, name: &str) -> PathBuf {
        self.root.join("tmp").join(name)
    }

    /// Moves a completed slab's files into `slabs/` (fsync'd, then renamed).
    pub fn publish_slab(&self, seq: u64, from: &SlabPaths) -> Result<SlabPaths, StoreError> {
        let to = self.slab_paths(seq);
        for (src, dst) in [
            (&from.pack, &to.pack),
            (&from.idx, &to.idx),
            (&from.mphf, &to.mphf),
        ] {
            File::open(src)?.sync_all()?;
            fs::rename(src, dst)?;
        }
        if let Ok(dir) = File::open(self.root.join("slabs")) {
            let _ = dir.sync_all();
        }
        Ok(to)
    }

    pub fn delete_slab(&self, seq: u64) {
        let p = self.slab_paths(seq);
        for f in [&p.pack, &p.idx, &p.mphf] {
            let _ = fs::remove_file(f);
        }
    }

    /// Sequence numbers of every slab with a pack file in `slabs/`.
    pub fn list_slab_seqs(&self) -> Result<Vec<u64>, StoreError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(self.root.join("slabs"))? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(seq) = name
                .strip_prefix("slab-")
                .and_then(|s| s.strip_suffix(".pack"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                out.push(seq);
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// Deletes slab files not referenced by `m` whose pack is older than `older_than_secs`
    /// (a crashed pipeline's leftovers), plus stale `tmp/` files. Returns how many slabs.
    pub fn gc_unreferenced(&self, m: &Manifest, older_than_secs: u64) -> Result<usize, StoreError> {
        let now = std::time::SystemTime::now();
        let mut removed = 0usize;
        for seq in self.list_slab_seqs()? {
            if m.all_slabs().any(|s| s.seq == seq) {
                continue;
            }
            let p = self.slab_paths(seq);
            let age_ok = fs::metadata(&p.pack)
                .and_then(|md| md.modified())
                .ok()
                .and_then(|t| now.duration_since(t).ok())
                .is_some_and(|d| d.as_secs() >= older_than_secs);
            if age_ok {
                self.delete_slab(seq);
                removed = removed.saturating_add(1);
            }
        }
        for entry in fs::read_dir(self.root.join("tmp"))?.flatten() {
            let old = entry
                .metadata()
                .and_then(|md| md.modified())
                .ok()
                .and_then(|t| now.duration_since(t).ok())
                .is_some_and(|d| d.as_secs() >= older_than_secs);
            if old {
                let _ = fs::remove_file(entry.path());
            }
        }
        Ok(removed)
    }

    fn read_manifest_file(&self) -> Result<Option<Manifest>, StoreError> {
        let mut f = match File::open(self.manifest_path()) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut v = Vec::with_capacity(4096);
        f.read_to_end(&mut v)?;
        Ok(Some(Manifest::decode(&v)?))
    }

    /// The current manifest, if the store was bootstrapped.
    pub fn load_manifest(&self) -> Result<Option<Manifest>, StoreError> {
        self.read_manifest_file()
    }

    /// Cheap change check for pollers: the current generation (None = no manifest).
    pub fn manifest_generation(&self) -> Result<Option<u64>, StoreError> {
        Ok(self.read_manifest_file()?.map(|m| m.generation))
    }

    /// Publishes `m` (whose `generation` is set here to `expected + 1`) if and only if the
    /// stored manifest still has generation `expected` (`None` = must not exist yet, the
    /// `If-None-Match: *` genesis case). Atomic: lock, compare, write temp, fsync, rename.
    pub fn store_manifest(
        &self,
        m: &mut Manifest,
        expected: Option<u64>,
    ) -> Result<(), StoreError> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join("manifest.lock"))?;
        lock.lock()?;
        let result = (|| {
            let found = self.read_manifest_file()?.map(|cur| cur.generation);
            if found != expected {
                return Err(StoreError::Conflict { expected, found });
            }
            m.generation = expected.map_or(1, |g| g.saturating_add(1));
            m.updated_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let bytes = m.encode();
            let tmp = self.root.join("tmp").join("manifest.bin.tmp");
            {
                let mut f = File::create(&tmp)?;
                f.write_all(&bytes)?;
                f.sync_all()?;
            }
            fs::rename(&tmp, self.manifest_path())?;
            if let Ok(dir) = File::open(&self.root) {
                let _ = dir.sync_all();
            }
            Ok(())
        })();
        let _ = lock.unlock();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{SlabRef, Tier};
    use crate::oid::Oid;

    #[test]
    fn manifest_cas_and_slab_publication() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::open(dir.path()).unwrap();
        assert!(store.load_manifest().unwrap().is_none());
        let mut m = Manifest::default();
        store.store_manifest(&mut m, None).unwrap();
        assert_eq!(m.generation, 1);
        assert_eq!(store.manifest_generation().unwrap(), Some(1));
        // Genesis twice fails; CAS with a stale generation fails.
        assert!(matches!(
            store.store_manifest(&mut Manifest::default(), None),
            Err(StoreError::Conflict { .. })
        ));
        assert!(matches!(
            store.store_manifest(&mut Manifest::default(), Some(5)),
            Err(StoreError::Conflict { .. })
        ));
        m.tip = Oid([1; 20]);
        store.store_manifest(&mut m, Some(1)).unwrap();
        assert_eq!(store.load_manifest().unwrap().unwrap().tip, Oid([1; 20]));
        // Slab files move from tmp into slabs; unreferenced ones get collected.
        let tmp = store.tmp_slab_paths(3, "recv");
        for p in [&tmp.pack, &tmp.idx, &tmp.mphf] {
            File::create(p).unwrap().write_all(b"x").unwrap();
        }
        let published = store.publish_slab(3, &tmp).unwrap();
        assert!(published.pack.exists() && !tmp.pack.exists());
        assert_eq!(store.list_slab_seqs().unwrap(), vec![3]);
        assert_eq!(store.gc_unreferenced(&m, 0).unwrap(), 1);
        assert!(store.list_slab_seqs().unwrap().is_empty());
        m.push_pyramid(
            SlabRef {
                seq: 4,
                tier: Tier::M15,
                start_ts: 0,
                end_ts: 1,
                tip: Oid::ZERO,
                object_count: 0,
                pack_len: 0,
                idx_len: 0,
                mphf_len: 0,
            },
            Vec::new(),
        );
        let tmp = store.tmp_slab_paths(4, "recv");
        for p in [&tmp.pack, &tmp.idx, &tmp.mphf] {
            File::create(p).unwrap().write_all(b"x").unwrap();
        }
        store.publish_slab(4, &tmp).unwrap();
        assert_eq!(
            store.gc_unreferenced(&m, 0).unwrap(),
            0,
            "referenced slabs stay"
        );
    }
}
