//! Filesystem helpers: atomic, durable writes of immutable files.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::util::{Context, Result};

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// A unique temporary name next to `path` (same directory, so `rename` is atomic).
pub fn temp_path(path: &Path) -> PathBuf {
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!(".{name}.tmp-{}-{seq}", std::process::id()))
}

pub fn sync_dir(path: &Path) {
    if let Some(dir) = path.parent()
        && let Ok(d) = File::open(dir)
    {
        let _ = d.sync_all();
    }
}

/// Writes `bytes` to `path` via temp file + fsync + rename (+ directory fsync).
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
    }
    let tmp = temp_path(path);
    let result = (|| -> Result<()> {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    sync_dir(path);
    result.ctx(&format!("writing {}", path.display()))
}

/// Makes `dst` a copy of the immutable file `src`: a hard link when possible. A `dst` that
/// exists is left as it is (a download, a slab file in a work directory: the same bytes
/// whoever made them). An upload to a store replaces what is there: [`stage`].
pub fn link_or_copy(src: &Path, dst: &Path) -> Result<()> {
    if let Some(dir) = dst.parent() {
        fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
    }
    if dst.exists() {
        return Ok(());
    }
    if fs::hard_link(src, dst).is_ok() {
        sync_dir(dst);
        return Ok(());
    }
    let tmp = temp_path(dst);
    let result = (|| -> Result<()> {
        fs::copy(src, &tmp)?;
        File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, dst)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    sync_dir(dst);
    result.ctx(&format!("copying {} to {}", src.display(), dst.display()))
}

/// A file under a temporary name beside `dst` that holds what the immutable file `src`
/// holds: a hard link to `src` when possible, else a copy. The caller renames it over
/// `dst`, which then is a file that no reader of the old `dst` has (they keep the old
/// one) and no listing has seen. Where `dst` is a link to `src`'s file already it is a
/// copy too: renaming one link of a file over another does nothing, and `dst` would stay
/// the file it was.
pub fn stage(src: &Path, dst: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::MetadataExt;
    if let Some(dir) = dst.parent() {
        fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
    }
    let tmp = temp_path(dst);
    let linked = match (fs::metadata(src), fs::metadata(dst)) {
        (Ok(s), Ok(d)) => s.dev() == d.dev() && s.ino() == d.ino(),
        _ => false,
    };
    if !linked && fs::hard_link(src, &tmp).is_ok() {
        return Ok(tmp);
    }
    let result = (|| -> Result<()> {
        fs::copy(src, &tmp)?;
        File::open(&tmp)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result.ctx(&format!("copying {} to {}", src.display(), dst.display()))?;
    Ok(tmp)
}

/// A scratch directory removed on drop.
pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    pub fn new(parent: &Path, label: &str) -> Result<TempDir> {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("gvfs-cache-{label}-{}-{seq}", std::process::id()));
        fs::create_dir_all(&path).ctx(&format!("creating {}", path.display()))?;
        Ok(TempDir { path })
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
