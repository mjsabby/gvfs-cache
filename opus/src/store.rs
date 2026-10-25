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

    /// Reads `len` bytes of the immutable object `key` from `offset`, handing them to
    /// `sink` in pieces as they arrive (blob packs, read where they are).
    pub fn read_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        match self {
            Store::Fs(s) => {
                use std::os::unix::fs::FileExt;
                let f = File::open(s.path(key)?).ctx(&format!("opening {key}"))?;
                let mut buf = vec![0u8; crate::util::us(len.min(1 << 20))];
                let mut at = 0u64;
                while at < len {
                    let n = crate::util::us(len.saturating_sub(at)).min(buf.len());
                    let piece = buf.get_mut(..n).ctx("read buffer")?;
                    f.read_exact_at(piece, offset.saturating_add(at))
                        .ctx(&format!("reading {key}"))?;
                    sink(piece)?;
                    at = at.saturating_add(crate::util::u64_of(n));
                }
                Ok(())
            }
            Store::Azure(a) => a.read_range(key, offset, len, sink),
        }
    }

    /// A streaming upload of an immutable object: it appears, whole, at
    /// [`StoreWriter::finish`] (never partially); dropped unfinished, it never appears.
    pub fn writer(&self, key: &str) -> Result<StoreWriter<'_>> {
        match self {
            Store::Fs(s) => {
                let path = s.path(key)?;
                if let Some(dir) = path.parent() {
                    fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
                }
                let tmp = crate::fsutil::temp_path(&path);
                let file = File::create(&tmp).ctx(&format!("creating {}", tmp.display()))?;
                Ok(StoreWriter::Fs(FsWriter {
                    w: Some(std::io::BufWriter::with_capacity(1 << 20, file)),
                    tmp,
                    path,
                }))
            }
            Store::Azure(a) => Ok(StoreWriter::Azure(a.writer(key)?)),
        }
    }
}

/// See [`Store::writer`].
pub enum StoreWriter<'a> {
    Fs(FsWriter),
    Azure(crate::azure::AzureWriter<'a>),
}

/// A local upload: a temp file renamed into place when finished, removed if dropped.
pub struct FsWriter {
    w: Option<std::io::BufWriter<File>>,
    tmp: PathBuf,
    path: PathBuf,
}

impl FsWriter {
    fn finish(mut self) -> Result<()> {
        let w = self.w.take().ctx("upload already finished")?;
        let file = w.into_inner().map_err(|e| crate::err!("writing: {e}"))?;
        file.sync_all()?;
        fs::rename(&self.tmp, &self.path).ctx(&format!("writing {}", self.path.display()))?;
        crate::fsutil::sync_dir(&self.path);
        Ok(())
    }

    fn out(&mut self) -> std::io::Result<&mut std::io::BufWriter<File>> {
        self.w
            .as_mut()
            .ok_or_else(|| std::io::Error::other("upload already finished"))
    }
}

impl Drop for FsWriter {
    fn drop(&mut self) {
        // Unfinished (or failed): no temp file is left behind. Finished: already renamed.
        let _ = fs::remove_file(&self.tmp);
    }
}

impl StoreWriter<'_> {
    pub fn finish(self) -> Result<()> {
        match self {
            StoreWriter::Fs(f) => f.finish(),
            StoreWriter::Azure(a) => a.finish(),
        }
    }
}

impl std::io::Write for StoreWriter<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        match self {
            StoreWriter::Fs(f) => f.out()?.write(data),
            StoreWriter::Azure(a) => a.write(data),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            StoreWriter::Fs(f) => f.out()?.flush(),
            StoreWriter::Azure(a) => a.flush(),
        }
    }
}

impl crate::blobstream::Spool for StoreWriter<'_> {
    fn read_back(&mut self, _: u64, _: &mut [u8]) -> Result<()> {
        crate::bail!("a pack streamed to the store cannot be read back (deltas need a local spool)")
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
        // Streaming uploads appear only when finished.
        let mut w = s.writer("blobs/x.pack").unwrap();
        std::io::Write::write_all(&mut w, b"partial").unwrap();
        drop(w);
        assert!(s.list("blobs/").unwrap().is_empty());
        assert_eq!(fs::read_dir(dir.join("blobs")).unwrap().count(), 0);
        let mut w = s.writer("blobs/x.pack").unwrap();
        std::io::Write::write_all(&mut w, b"whole").unwrap();
        assert!(s.list("blobs/").unwrap().is_empty());
        w.finish().unwrap();
        assert_eq!(s.get("blobs/x.pack").unwrap().unwrap().0, b"whole");
        let mut got = Vec::new();
        s.read_range("blobs/x.pack", 1, 3, &mut |b| {
            got.extend_from_slice(b);
            Ok(())
        })
        .unwrap();
        assert_eq!(got, b"hol");
        assert!(s.list("slabs/").unwrap().is_empty());
        assert!(!valid_key("../x") && !valid_key("a//b") && valid_key("slabs/m15-1-ab.pack"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
