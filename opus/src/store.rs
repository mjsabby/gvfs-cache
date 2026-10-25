//! Object storage for slabs and the manifest.
//!
//! The pipeline needs only what every object store offers: whole and ranged reads, listing
//! by prefix, deletes, and two conditional writes for the manifest — *create only if
//! absent* (`If-None-Match: *`, genesis) and *replace only if unchanged* (`If-Match`,
//! every later publication). Slab files are immutable and content-addressed, so they are
//! plain uploads.
//!
//! * `fs:<dir>` — a directory; conditional writes are serialized with `flock(2)`.
//! * `azure:<container>[/<prefix>]` — Azure Blob Storage over its REST API.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use crate::util::{Context, Error, ErrorKind, Result};

/// Condition for a write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cond {
    None,
    /// Create only if the key does not exist.
    IfNoneMatch,
    /// Replace only if the current etag is this one.
    IfMatch(String),
}

#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Unix seconds of the last write.
    pub modified: u64,
}

pub enum Store {
    Fs(FsStore),
    Azure(crate::azure::AzureStore),
}

impl Store {
    /// `fs:<dir>`, a plain directory path, or `azure:<container>[/<prefix>]` (credentials
    /// from the environment variable `auth_env`).
    pub fn open(spec: &str, auth_env: &str) -> Result<Store> {
        if let Some(rest) = spec.strip_prefix("azure:") {
            let cs = std::env::var(auth_env).map_err(|_| {
                crate::err!("--store {spec} needs ${auth_env} (a connection string)")
            })?;
            return Ok(Store::Azure(crate::azure::AzureStore::new(&cs, rest)?));
        }
        let dir = spec.strip_prefix("fs:").unwrap_or(spec);
        Ok(Store::Fs(FsStore::new(Path::new(dir))?))
    }

    pub fn describe(&self) -> String {
        match self {
            Store::Fs(s) => format!("fs:{}", s.root.display()),
            Store::Azure(a) => a.describe(),
        }
    }

    /// Whole object and its etag; `None` if absent.
    pub fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match self {
            Store::Fs(s) => s.get(key),
            Store::Azure(a) => a.get(key),
        }
    }

    /// Small writes (the manifest). Returns the new etag; `Conflict` if `cond` fails.
    pub fn put(&self, key: &str, data: &[u8], cond: Cond) -> Result<String> {
        match self {
            Store::Fs(s) => s.put(key, data, cond),
            Store::Azure(a) => a.put(key, data, &cond),
        }
    }

    /// Uploads an immutable file (a slab).
    pub fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        match self {
            Store::Fs(s) => crate::fsutil::link_or_copy(path, &s.path(key)?),
            Store::Azure(a) => a.upload_file(key, path),
        }
    }

    /// Downloads an object to `path` (atomically).
    pub fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        match self {
            Store::Fs(s) => crate::fsutil::link_or_copy(&s.path(key)?, path),
            Store::Azure(a) => a.download_file(key, path),
        }
    }

    /// For stores that are directories: the object's file, usable in place.
    pub fn local_path(&self, key: &str) -> Option<PathBuf> {
        match self {
            Store::Fs(s) => s.path(key).ok(),
            Store::Azure(_) => None,
        }
    }

    pub fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        match self {
            Store::Fs(s) => s.list(prefix),
            Store::Azure(a) => a.list(prefix),
        }
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        match self {
            Store::Fs(s) => s.delete(key),
            Store::Azure(a) => a.delete(key),
        }
    }
}

/// Keys are relative paths of `[A-Za-z0-9._-]` components.
pub fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 512
        && key.split('/').all(|c| {
            !c.is_empty()
                && c != "."
                && c != ".."
                && c.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        })
}

pub struct FsStore {
    pub root: PathBuf,
}

impl FsStore {
    pub fn new(root: &Path) -> Result<FsStore> {
        fs::create_dir_all(root).ctx(&format!("creating store directory {}", root.display()))?;
        Ok(FsStore {
            root: root.to_path_buf(),
        })
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        if !valid_key(key) {
            crate::bail!("invalid store key {key:?}");
        }
        Ok(self.root.join(key))
    }

    fn etag(data: &[u8]) -> String {
        let d = ring::digest::digest(&ring::digest::SHA256, data);
        crate::util::hex_string(d.as_ref().get(..16).unwrap_or(&[]))
    }

    fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match fs::read(self.path(key)?) {
            Ok(data) => {
                let etag = Self::etag(&data);
                Ok(Some((data, etag)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::from(e)).ctx(&format!("reading {key}")),
        }
    }

    fn put(&self, key: &str, data: &[u8], cond: Cond) -> Result<String> {
        let path = self.path(key)?;
        // Conditional writes are serialized by an exclusive lock on the store.
        let lock_file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join(".lock"))
            .ctx("opening store lock")?;
        let _lock = nix::fcntl::Flock::lock(lock_file, nix::fcntl::FlockArg::LockExclusive)
            .map_err(|(_, e)| crate::err!("locking store: {e}"))?;
        let current = self.get(key)?;
        match (&cond, &current) {
            (Cond::IfNoneMatch, Some(_)) => {
                return Err(Error::with_kind(
                    ErrorKind::Conflict,
                    format!("{key} already exists"),
                ));
            }
            (Cond::IfMatch(want), Some((_, have))) if want != have => {
                return Err(Error::with_kind(
                    ErrorKind::Conflict,
                    format!("{key} changed (etag {have}, expected {want})"),
                ));
            }
            (Cond::IfMatch(_), None) => {
                return Err(Error::with_kind(
                    ErrorKind::Conflict,
                    format!("{key} does not exist"),
                ));
            }
            _ => {}
        }
        crate::fsutil::write_atomic(&path, data)?;
        Ok(Self::etag(data))
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let dir_part = prefix.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
        let dir = if dir_part.is_empty() {
            self.root.clone()
        } else {
            self.path(dir_part)?
        };
        let mut out = Vec::new();
        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(Error::from(e)).ctx(&format!("listing {}", dir.display())),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let key = if dir_part.is_empty() {
                name
            } else {
                format!("{dir_part}/{name}")
            };
            let meta = entry.metadata()?;
            if !meta.is_file() || !key.starts_with(prefix) {
                continue;
            }
            let modified = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            out.push(ObjectInfo {
                key,
                size: meta.len(),
                modified,
            });
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    fn delete(&self, key: &str) -> Result<()> {
        match fs::remove_file(self.path(key)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::from(e)).ctx(&format!("deleting {key}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_store_conditional_writes() {
        let dir = std::env::temp_dir().join(format!("gvd-store-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let s = Store::open(&format!("fs:{}", dir.display()), "UNUSED").unwrap();
        assert!(s.get("manifest.bin").unwrap().is_none());
        let e1 = s.put("manifest.bin", b"one", Cond::IfNoneMatch).unwrap();
        assert!(
            s.put("manifest.bin", b"two", Cond::IfNoneMatch)
                .unwrap_err()
                .is_conflict()
        );
        assert!(
            s.put("manifest.bin", b"two", Cond::IfMatch("nope".into()))
                .unwrap_err()
                .is_conflict()
        );
        let e2 = s
            .put("manifest.bin", b"two", Cond::IfMatch(e1.clone()))
            .unwrap();
        assert_ne!(e1, e2);
        assert_eq!(
            s.get("manifest.bin").unwrap().unwrap(),
            (b"two".to_vec(), e2)
        );
        let src = dir.join("src.bin");
        fs::write(&src, b"slab").unwrap();
        s.upload_file("slabs/a.pack", &src).unwrap();
        let listed = s.list("slabs/").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            (listed[0].key.as_str(), listed[0].size),
            ("slabs/a.pack", 4)
        );
        s.delete("slabs/a.pack").unwrap();
        assert!(s.list("slabs/").unwrap().is_empty());
        assert!(!valid_key("../x") && !valid_key("a//b") && valid_key("slabs/m15-1-ab.pack"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
