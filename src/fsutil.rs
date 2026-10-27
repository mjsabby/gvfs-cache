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

/// Makes `dst` a copy of the immutable file `src`: a hard link when possible.
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

/// A scratch directory removed on drop.
pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    pub fn new(parent: &Path, label: &str) -> Result<TempDir> {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("gvfs-daemon-{label}-{}-{seq}", std::process::id()));
        fs::create_dir_all(&path).ctx(&format!("creating {}", path.display()))?;
        Ok(TempDir { path })
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
