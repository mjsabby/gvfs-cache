//! Object storage for slabs and the manifest.
//!
//! The pipeline needs only what every object store offers: whole and ranged reads, listing
//! by prefix, deletes, and three conditional operations. Two are writes, for the manifest:
//! *create only if absent* (`If-None-Match: *`, genesis) and *replace only if unchanged*
//! (`If-Match`, every later publication). The third is garbage collection's: *delete only
//! if it is still the object that was listed* ([`Store::delete_if`]). Slab files are
//! immutable and content-addressed, so they are plain uploads; one made again (the same id,
//! by another run) replaces the object, which is then another to a conditional delete.
//!
//! * `fs:<dir>` — a directory; conditional writes and deletes, and the renames that put
//!   uploads in place, are serialized with `flock(2)`.
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
    /// What tells this object from one written under its key later, for
    /// [`Store::delete_if`]: an object store's ETag; of a file, its inode, its length and
    /// its time of last write.
    pub version: String,
}

/// What [`Store::get_unless`] finds of an object its reader may already have.
#[derive(Debug, PartialEq, Eq)]
pub enum Fresh {
    /// Still the object the reader has.
    Same,
    /// No such object.
    Gone,
    /// Another object: its bytes and its etag.
    New(Vec<u8>, String),
}

/// What a reader that asks for the same objects again and again keeps between its
/// requests (an edge following `manifest.bin` and `blobs.bin`): its connection to an
/// object store, and for a directory store what each file looked like when it last read
/// it. A poll of an object that has not changed is then one conditional request on that
/// connection, or one `open` and `fstat`, and none of the object's bytes. (Each poll was
/// a GET of the whole object on a new TLS connection, compared with what the edge had
/// afterwards, and `blobs.bin` is about 120 bytes per blob pack: megabytes, every two
/// seconds, for a store of large blobs.)
#[derive(Default)]
pub struct Poll {
    client: Option<crate::http::Client>,
    /// When `client` last had an answer.
    used: Option<std::time::Instant>,
    seen: std::collections::HashMap<String, (FileMark, String)>,
}

/// How long a reader's connection is kept without a request. One that has been quiet for
/// longer may be gone without a word (a NAT's or a load balancer's idle timeout), and a
/// request on it would wait for the client's whole timeout to learn that: the next request
/// makes a new one.
const POLL_KEEP: std::time::Duration = std::time::Duration::from_secs(30);

/// A file's device, inode, length and times of last write and last change: another file,
/// or the same one written again, has another.
type FileMark = (u64, u64, u64, i64, i64, i64, i64);

/// How long ago a file must have been written for its [`FileMark`] to be believed. The
/// times a file system gives a file are a clock's that ticks every few milliseconds, so a
/// file written again in the tick it was read in (in place and to the same length: not
/// how a store is written, but a `cp` over its manifest is that) may look as it did. A
/// file younger than this is read at every poll.
const SETTLED_SECS: i64 = 2;

/// Where an async read's bytes go, a piece at a time as they arrive. A piece may be awaited
/// (sent to a client), so a ranged GET streams through to a socket without a buffer; a
/// closure is a sink that does not wait.
pub trait Sink: Send {
    fn put(&mut self, piece: &[u8]) -> impl Future<Output = Result<()>> + Send;
}

impl<F: FnMut(&[u8]) -> Result<()> + Send> Sink for F {
    fn put(&mut self, piece: &[u8]) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(self(piece))
    }
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

    /// The object `key`, unless it is still the one `have` is the etag of. `poll` is the
    /// reader's own, kept from one call to the next.
    pub fn get_unless(&self, poll: &mut Poll, key: &str, have: Option<&str>) -> Result<Fresh> {
        match self {
            Store::Fs(s) => s.get_unless(&mut poll.seen, key, have, std::time::SystemTime::now()),
            Store::Azure(a) => {
                if poll.used.is_some_and(|at| at.elapsed() > POLL_KEEP) {
                    poll.client = None;
                }
                let fresh = a.get_unless(&mut poll.client, key, have);
                poll.used = Some(std::time::Instant::now());
                fresh
            }
        }
    }

    /// Small writes (the manifest). Returns the new etag; `Conflict` if `cond` fails.
    pub fn put(&self, key: &str, data: &[u8], cond: Cond) -> Result<String> {
        match self {
            Store::Fs(s) => s.put(key, data, cond),
            Store::Azure(a) => a.put(key, data, &cond),
        }
    }

    /// Uploads an immutable file (a slab): a new object, in place of any the key has. (To
    /// a directory it left a file of that name as it was, the bytes being the same: the
    /// file a garbage collection had listed before, and deleted after the run that made
    /// this upload had published it.) A directory store renames the new file over the old
    /// one, which readers that have it open or mapped keep.
    pub fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        match self {
            Store::Fs(s) => s.upload(key, path),
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

    /// Deletes `key` if it still is the object a listing gave as `version`
    /// ([`ObjectInfo::version`]). `false`: the object listed is not there, another having
    /// been written under the key since; that one stays. An object that is gone counts as
    /// deleted (an object store may answer `false` for it instead).
    pub fn delete_if(&self, key: &str, version: &str) -> Result<bool> {
        match self {
            Store::Fs(s) => s.delete_if(key, version),
            Store::Azure(a) => a.delete_if(key, version),
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

    /// [`Store::read_range`] for the edge while it serves: object storage through the async
    /// client (the task waits, not its thread); a directory store reads its file here.
    pub async fn read_range_async<S: Sink>(
        &self,
        key: &str,
        offset: u64,
        len: u64,
        sink: &mut S,
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
                    sink.put(piece).await?;
                    at = at.saturating_add(crate::util::u64_of(n));
                }
                Ok(())
            }
            Store::Azure(a) => a.read_range_async(key, offset, len, sink).await,
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
                    root: s.root.clone(),
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
    /// The store's directory.
    root: PathBuf,
}

impl FsWriter {
    fn finish(mut self) -> Result<()> {
        let w = self.w.take().ctx("upload already finished")?;
        let file = w.into_inner().map_err(|e| crate::err!("writing: {e}"))?;
        file.sync_all()?;
        install(&self.root, &self.tmp, &self.path)
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

/// The lock of the store in `root`, held until it is dropped. What it serializes:
/// conditional writes; and a conditional delete against the renames that put uploads in
/// place.
fn lock(root: &Path) -> Result<nix::fcntl::Flock<File>> {
    let lock_file = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(".lock"))
        .ctx("opening store lock")?;
    nix::fcntl::Flock::lock(lock_file, nix::fcntl::FlockArg::LockExclusive)
        .map_err(|(_, e)| crate::err!("locking store: {e}"))
}

/// Puts an upload's file `tmp` in place as `path`, by a rename under the store's lock:
/// [`FsStore::delete_if`] looks at a file and unlinks it under the same lock, so the file
/// it unlinks is the one it looked at, never one put there in between.
fn install(root: &Path, tmp: &Path, path: &Path) -> Result<()> {
    let renamed = lock(root)
        .and_then(|_held| fs::rename(tmp, path).ctx(&format!("writing {}", path.display())));
    if renamed.is_err() {
        let _ = fs::remove_file(tmp);
    }
    crate::fsutil::sync_dir(path);
    renamed
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

    /// [`Store::upload_file`]: a link to `src` (a copy across file systems), renamed over
    /// whatever file the key has.
    fn upload(&self, key: &str, src: &Path) -> Result<()> {
        let path = self.path(key)?;
        let tmp = crate::fsutil::stage(src, &path)?;
        install(&self.root, &tmp, &path)
    }

    /// [`ObjectInfo::version`] of a file: its inode, its length and its time of last
    /// write. A file renamed over it is another inode; an inode's number is used again
    /// once it is free, by a file written at another time. Not the device, which on a
    /// shared file system is each mount's own number, nor the time of last change, which
    /// moves when a link to the file goes (the one in the work directory of the run that
    /// uploaded it).
    fn version(m: &fs::Metadata) -> String {
        use std::os::unix::fs::MetadataExt;
        format!(
            "{:x}-{:x}-{:x}.{:09}",
            m.ino(),
            m.len(),
            m.mtime(),
            m.mtime_nsec()
        )
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

    /// [`Store::get_unless`]: the file is opened, and read and hashed for its etag only if
    /// it does not look as it did when `seen` last read it, at `now`. The file is opened
    /// and not only looked at by its path, because on a network file system that is what
    /// asks the server what the file is now.
    fn get_unless(
        &self,
        seen: &mut std::collections::HashMap<String, (FileMark, String)>,
        key: &str,
        have: Option<&str>,
        now: std::time::SystemTime,
    ) -> Result<Fresh> {
        use std::io::Read;
        use std::os::unix::fs::MetadataExt;
        let what = || format!("reading {key}");
        let mut file = match File::open(self.path(key)?) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                seen.remove(key);
                return Ok(Fresh::Gone);
            }
            Err(e) => return Err(Error::from(e)).ctx(&what()),
        };
        let m = file.metadata().ctx(&what())?;
        let mark: FileMark = (
            m.dev(),
            m.ino(),
            m.len(),
            m.mtime(),
            m.mtime_nsec(),
            m.ctime(),
            m.ctime_nsec(),
        );
        if have.is_some()
            && seen
                .get(key)
                .is_some_and(|(was, etag)| *was == mark && Some(etag.as_str()) == have)
        {
            return Ok(Fresh::Same);
        }
        let mut data = Vec::with_capacity(crate::util::us(m.len()).min(1 << 24));
        file.read_to_end(&mut data).ctx(&what())?;
        let etag = Self::etag(&data);
        let now = now
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        if now.saturating_sub(m.mtime().max(m.ctime())) >= SETTLED_SECS {
            seen.insert(key.to_string(), (mark, etag.clone()));
        } else {
            seen.remove(key);
        }
        Ok(if have == Some(etag.as_str()) {
            Fresh::Same
        } else {
            Fresh::New(data, etag)
        })
    }

    fn put(&self, key: &str, data: &[u8], cond: Cond) -> Result<String> {
        let path = self.path(key)?;
        // Conditional writes are serialized by an exclusive lock on the store.
        let _lock = lock(&self.root)?;
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
            // An entry deleted since the directory was read (another run's garbage
            // collection) is not listed: it failed the listing, and the run with it.
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::from(e)).ctx(&format!("listing {key}")),
            };
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
                version: Self::version(&meta),
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

    /// [`Store::delete_if`]: the file is looked at again, and unlinked if it is the one
    /// the listing saw, under the lock that uploads are renamed into place under
    /// ([`install`]). It is opened to be looked at, as a poll opens the manifest: on a
    /// network file system that asks the server which file has the name now. A file put
    /// there by anything else between the look and the unlink (a `cp` into the store) is
    /// not protected, nor is one where the file system's locks do not reach the other
    /// machines.
    fn delete_if(&self, key: &str, version: &str) -> Result<bool> {
        let path = self.path(key)?;
        let what = || format!("deleting {key}");
        let _lock = lock(&self.root)?;
        let seen = match File::open(&path).and_then(|file| file.metadata()) {
            Ok(m) => Self::version(&m),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(e) => return Err(Error::from(e)).ctx(&what()),
        };
        if seen != version {
            return Ok(false);
        }
        match fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(Error::from(e)).ctx(&what()),
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

    /// An upload to a directory store leaves a new file where the key had one, and a
    /// delete that names what a listing saw takes only that file: a collection that
    /// listed a slab's file before a run uploaded the slab again leaves the run's. A
    /// reader of the old file keeps it. (The upload left the old file, which the
    /// collection then deleted, by its name.)
    #[test]
    fn an_upload_is_a_new_file_and_a_delete_takes_only_the_one_listed() {
        use std::io::{Read, Write};
        use std::os::unix::fs::MetadataExt;
        let dir = std::env::temp_dir().join(format!("gvd-store-replace-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let s = Store::open(&format!("fs:{}", dir.display()), "UNUSED").unwrap();
        let key = "slabs/a.pack";
        let path = dir.join(key);
        let (one, two) = (dir.join("one.bin"), dir.join("two.bin"));
        fs::write(&one, b"first").unwrap();
        fs::write(&two, b"again").unwrap();
        let listed = || s.list("slabs/").unwrap().remove(0).version;

        s.upload_file(key, &one).unwrap();
        let first = listed();
        let mut reader = File::open(&path).unwrap();
        s.upload_file(key, &two).unwrap();
        let second = listed();
        assert_ne!(first, second);
        assert_eq!(fs::read(&path).unwrap(), b"again");
        let mut held = String::new();
        reader.read_to_string(&mut held).unwrap();
        assert_eq!(held, "first");
        // What was listed first is not there: the second stays. Listed, it goes; and a
        // file that is gone counts as deleted.
        assert!(!s.delete_if(key, &first).unwrap());
        assert_eq!(fs::read(&path).unwrap(), b"again");
        assert!(s.delete_if(key, &second).unwrap());
        assert!(!path.exists());
        assert!(s.delete_if(key, &second).unwrap());

        // The same file uploaded twice (a run that starts over builds the same slab in
        // the same work directory): the store's file is a link to it, and the second
        // upload makes another file all the same, leaving no temp file behind.
        s.upload_file(key, &one).unwrap();
        let (linked, inode) = (listed(), fs::metadata(&path).unwrap().ino());
        assert_eq!(inode, fs::metadata(&one).unwrap().ino());
        s.upload_file(key, &one).unwrap();
        assert_ne!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(fs::read(&path).unwrap(), b"first");
        assert!(!s.delete_if(key, &linked).unwrap());
        let files = |sub: &str| fs::read_dir(dir.join(sub)).unwrap().count();
        assert_eq!(files("slabs"), 1);

        // So is a streamed upload another file.
        let before = listed();
        let mut w = s.writer(key).unwrap();
        w.write_all(b"stream").unwrap();
        w.finish().unwrap();
        assert!(!s.delete_if(key, &before).unwrap());
        assert_eq!(fs::read(&path).unwrap(), b"stream");
        assert_eq!(files("slabs"), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// What a reader that polls a directory store is told: the object once, then that it
    /// is the same, until the file is another. A file that has not been written for a
    /// while is not read to learn that; one just written is.
    #[test]
    fn a_poll_of_a_directory_store_reads_what_changed() {
        let dir = std::env::temp_dir().join(format!("gvd-store-poll-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let s = FsStore::new(&dir).unwrap();
        let key = "manifest.bin";
        let mut seen = std::collections::HashMap::new();
        let now = std::time::SystemTime::now();
        let later = now + std::time::Duration::from_secs(60);
        let new = |bytes: &[u8]| Fresh::New(bytes.to_vec(), FsStore::etag(bytes));

        assert_eq!(
            s.get_unless(&mut seen, key, None, now).unwrap(),
            Fresh::Gone
        );
        let one = s.put(key, b"one", Cond::None).unwrap();
        assert_eq!(
            s.get_unless(&mut seen, key, None, now).unwrap(),
            new(b"one")
        );
        // Just written: read again, and found the same by its bytes.
        assert!(seen.is_empty());
        assert_eq!(
            s.get_unless(&mut seen, key, Some(&one), now).unwrap(),
            Fresh::Same
        );
        assert!(seen.is_empty());
        // Written a while ago: read once more, then known by its look alone. (An etag
        // that is not its bytes' shows that they were not read.)
        assert_eq!(
            s.get_unless(&mut seen, key, Some(&one), later).unwrap(),
            Fresh::Same
        );
        seen.get_mut(key).unwrap().1 = "not read".to_string();
        assert_eq!(
            s.get_unless(&mut seen, key, Some("not read"), later)
                .unwrap(),
            Fresh::Same
        );
        // What the reader has is not what was seen: read.
        assert_eq!(
            s.get_unless(&mut seen, key, Some("other"), later).unwrap(),
            new(b"one")
        );
        // Another file of the same length, written in the same instant.
        let two = s.put(key, b"two", Cond::None).unwrap();
        assert_eq!(
            s.get_unless(&mut seen, key, Some(&one), later).unwrap(),
            new(b"two")
        );
        // The same file written in place.
        fs::write(dir.join(key), b"six").unwrap();
        assert_eq!(
            s.get_unless(&mut seen, key, Some(&two), later).unwrap(),
            new(b"six")
        );
        // The same bytes in another file are the same object.
        let six = s.put(key, b"six", Cond::None).unwrap();
        assert_eq!(
            s.get_unless(&mut seen, key, Some(&six), later).unwrap(),
            Fresh::Same
        );
        s.delete(key).unwrap();
        assert_eq!(
            s.get_unless(&mut seen, key, Some(&six), later).unwrap(),
            Fresh::Gone
        );
        assert!(seen.is_empty());
        fs::remove_dir_all(&dir).unwrap();
    }

    /// What a reader that polls an object store sends: a conditional request on the
    /// connection it keeps, answered without the object while the object is the same. A
    /// connection that has been quiet for long is not used again.
    #[test]
    #[allow(clippy::arithmetic_side_effects)]
    fn a_poll_of_an_object_store_is_a_conditional_request_on_one_connection() {
        use crate::testhttp::{Reply, Request};
        use std::sync::{Arc, Mutex};
        let version = Arc::new(Mutex::new(1u32));
        let asked = Arc::new(Mutex::new(Vec::new()));
        let (v, a) = (Arc::clone(&version), Arc::clone(&asked));
        let port = crate::testhttp::serve(move |r: &Request| {
            let version = *v.lock().unwrap();
            let etag = format!("\"v{version}\"");
            let unless = r.header("if-none-match").map(str::to_string);
            let same = unless.as_deref() == Some(etag.as_str());
            a.lock().unwrap().push((unless, r.earlier));
            match (version, same) {
                (0, _) => Reply::status("404 Not Found"),
                (_, true) => Reply::status(&format!("304 Not Modified\r\nETag: {etag}")),
                _ => Reply::Whole(
                    format!("200 OK\r\nETag: {etag}"),
                    format!("object {version}").into_bytes(),
                ),
            }
        });
        let account =
            format!("BlobEndpoint=http://127.0.0.1:{port}/acct;AccountName=acct;AccountKey=a2V5");
        let store = Store::Azure(crate::azure::AzureStore::new(&account, "packs").unwrap());
        let key = "manifest.bin";
        let mut poll = Poll::default();
        let v1 = "\"v1\"".to_string();
        let v2 = "\"v2\"".to_string();
        assert_eq!(
            store.get_unless(&mut poll, key, None).unwrap(),
            Fresh::New(b"object 1".to_vec(), v1.clone())
        );
        for _ in 0..3 {
            assert_eq!(
                store.get_unless(&mut poll, key, Some(&v1)).unwrap(),
                Fresh::Same
            );
        }
        *version.lock().unwrap() = 2;
        assert_eq!(
            store.get_unless(&mut poll, key, Some(&v1)).unwrap(),
            Fresh::New(b"object 2".to_vec(), v2.clone())
        );
        *version.lock().unwrap() = 0;
        assert_eq!(
            store.get_unless(&mut poll, key, Some(&v2)).unwrap(),
            Fresh::Gone
        );
        // One connection carried them all, each but the first with the reader's etag.
        let unless = |etag: &String| Some(etag.clone());
        assert_eq!(
            *asked.lock().unwrap(),
            [
                (None, 0),
                (unless(&v1), 1),
                (unless(&v1), 2),
                (unless(&v1), 3),
                (unless(&v1), 4),
                (unless(&v2), 5)
            ]
        );
        // After a long silence, another connection.
        poll.used = std::time::Instant::now().checked_sub(POLL_KEEP * 2);
        assert!(poll.used.is_some());
        assert_eq!(
            store.get_unless(&mut poll, key, Some(&v2)).unwrap(),
            Fresh::Gone
        );
        assert_eq!(asked.lock().unwrap().last().unwrap().1, 0);
    }

    /// A file deleted between the moment a listing reads its name and the moment it
    /// looks at it (another run collecting garbage) is left out of the listing; it does
    /// not fail it.
    #[test]
    fn a_listing_survives_files_deleted_meanwhile() {
        let dir = std::env::temp_dir().join(format!("gvd-store-list-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let s = Store::open(&format!("fs:{}", dir.display()), "UNUSED").unwrap();
        fs::create_dir_all(dir.join("slabs")).unwrap();
        for round in 0..10 {
            let files: Vec<PathBuf> = (0..2000)
                .map(|i| dir.join(format!("slabs/r{round}-{i}.pack")))
                .collect();
            for f in &files {
                fs::write(f, b"x").unwrap();
            }
            let deleter = std::thread::spawn(move || {
                for f in &files {
                    fs::remove_file(f).unwrap();
                }
            });
            let listed = s.list("slabs/");
            deleter.join().unwrap();
            assert!(listed.is_ok(), "{}", listed.err().unwrap());
        }
        assert!(s.list("slabs/").unwrap().is_empty());
        fs::remove_dir_all(&dir).unwrap();
    }
}
