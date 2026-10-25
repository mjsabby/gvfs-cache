//! Zero-GC Workstation Maintenance (Fix 2: Superset Tombstoning & Fix 3: `multi-pack-index write`,
//! PDF pp. 141–144).
//!
//! Together with Fix 1 (Server-Side On-the-Fly Pack Splicing on `/gvfs/prefetch`) and
//! Strategy #4 (Weekly "Aha" Delta `weekly-delta-YYYY-WW.pack`), these primitives keep a
//! developer workstation or persistent agent VM fast for years with `gc.auto=0`:
//!
//! - **Fix 2 (`prune_subsumed_prefetch_packs`)**: Encodes `[start_ts .. end_ts]` in the
//!   prefetch pack filename (`prefetch-<start_ts>-<end_ts>.pack`). Whenever a rollup pack
//!   covering `[T_start .. T_end]` lands, any existing `prefetch-<s_i>-<e_i>.{pack,idx}`
//!   where `s_i >= T_start && e_i <= T_end` is deleted in `< 1 ms` via `unlink()`.
//! - **Fix 3 (`write_multi_pack_index`)**: Runs `git multi-pack-index write` (`midx`) over
//!   the pack directory so Git performs 1 unified $O(\log N)$ binary search instead of
//!   linearly scanning $P$ `.idx` files.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pipeline::DaemonError;

static TMP_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkstationPrefetchOutcome {
    pub written_pack_stem: String,
    pub start_ts: u64,
    pub end_ts: u64,
    /// Number of subsumed `.pack`/`.idx` files removed by Superset Tombstoning (`unlink`).
    pub unlinked_files: usize,
    /// Remaining `.pack` files in the directory after tombstoning.
    pub remaining_packs: usize,
    pub midx_updated: bool,
}

/// Extracts `(start_ts, end_ts)` from `X-GVFS-Pack-Start` and `X-GVFS-Pack-End` HTTP headers.
pub fn parse_pack_start_end_headers(headers: &http::HeaderMap) -> Option<(u64, u64)> {
    let start_val = headers.get("x-gvfs-pack-start")?.as_bytes();
    let end_val = headers.get("x-gvfs-pack-end")?.as_bytes();
    let start_ts = gvfs_core::buf::parse_dec_u64(start_val)?;
    let end_ts = gvfs_core::buf::parse_dec_u64(end_val)?;
    if start_ts <= end_ts {
        Some((start_ts, end_ts))
    } else {
        None
    }
}

/// Parses `(start_ts, end_ts)` and the file stem from a prefetch pack/idx filename:
/// - `prefetch-<start_ts>-<end_ts>.pack` / `.idx`
/// - `prefetch-<start_ts>-<end_ts>-<guid>.pack` / `.idx`
#[inline]
pub fn parse_prefetch_pack_interval(filename: &str) -> Option<(&str, u64, u64)> {
    let stem = filename
        .strip_suffix(".pack")
        .or_else(|| filename.strip_suffix(".idx"))
        .or_else(|| filename.strip_suffix(".rev"))?;
    let after_prefix = stem.strip_prefix("prefetch-")?;
    let mut parts = after_prefix.split('-');
    let start_str = parts.next()?;
    let end_str = parts.next()?;
    let start_ts = start_str.parse::<u64>().ok()?;
    let end_ts = end_str.parse::<u64>().ok()?;
    if start_ts > end_ts {
        return None;
    }
    Some((stem, start_ts, end_ts))
}

/// Fix 2: Zero-GC "Superset Tombstoning" (`unlink` via math, not `git gc`, PDF pp. 142–143).
///
/// Deletes any existing `prefetch-<s_i>-<e_i>.{pack,idx,rev}` in `pack_dir` whose time interval
/// `[s_i .. e_i]` is a subset of `[new_start_ts .. new_end_ts]` (`s_i >= new_start_ts && e_i <= new_end_ts`),
/// excluding `keep_stem`. Returns the number of unlinked files.
pub fn prune_subsumed_prefetch_packs(
    pack_dir: &Path,
    new_start_ts: u64,
    new_end_ts: u64,
    keep_stem: &str,
) -> Result<usize, std::io::Error> {
    if !pack_dir.exists() {
        return Ok(0);
    }
    let mut unlinked = 0usize;
    for entry_res in fs::read_dir(pack_dir)? {
        let entry = entry_res?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Some((stem, s_i, e_i)) = parse_prefetch_pack_interval(name) else {
            continue;
        };
        if stem == keep_stem {
            continue;
        }
        if s_i >= new_start_ts && e_i <= new_end_ts && fs::remove_file(entry.path()).is_ok() {
            unlinked = unlinked.saturating_add(1);
        }
    }
    Ok(unlinked)
}

/// Fix 3: Runs `git multi-pack-index write` (`midx`) on `objects_dir` (`.git/objects`),
/// coalescing all `.idx` headers into a single `.git/objects/pack/multi-pack-index` file
/// without touching or rewriting a single `.pack` file (PDF p. 143).
pub fn write_multi_pack_index(objects_dir: &Path) -> Result<(), DaemonError> {
    let mut cmd = Command::new("git");
    cmd.args([
        "multi-pack-index",
        "write",
        &format!("--object-dir={}", objects_dir.display()),
    ])
    .env("GIT_CONFIG_NOSYSTEM", "1")
    .env("GIT_CONFIG_GLOBAL", "/dev/null")
    .env_remove("GIT_CONFIG_COUNT")
    .stdout(Stdio::null())
    .stderr(Stdio::piped());
    for i in 0..16 {
        cmd.env_remove(format!("GIT_CONFIG_KEY_{i}"));
        cmd.env_remove(format!("GIT_CONFIG_VALUE_{i}"));
    }
    let out = cmd.output().map_err(DaemonError::from)?;
    if !out.status.success() {
        return Err(DaemonError::Config(format!(
            "git multi-pack-index write failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

/// Writes a spliced prefetch pack (`prefetch-<start_ts>-<end_ts>.{pack,idx}`) into `pack_dir`,
/// runs Fix 2 (`prune_subsumed_prefetch_packs` to `unlink` all subsumed intraday/daily packs),
/// and optionally runs Fix 3 (`write_multi_pack_index`).
pub fn apply_prefetch_pack_to_workstation(
    pack_dir: &Path,
    start_ts: u64,
    end_ts: u64,
    pack_bytes: &[u8],
    idx_bytes: &[u8],
    run_midx: bool,
) -> Result<WorkstationPrefetchOutcome, DaemonError> {
    fs::create_dir_all(pack_dir).map_err(DaemonError::from)?;
    let stem = format!("prefetch-{start_ts}-{end_ts}");
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();

    let tmp_pack = pack_dir.join(format!("tmp_pack_{pid}_{seq}"));
    let tmp_idx = pack_dir.join(format!("tmp_idx_{pid}_{seq}"));
    {
        let mut f = fs::File::create(&tmp_pack).map_err(DaemonError::from)?;
        f.write_all(pack_bytes).map_err(DaemonError::from)?;
        f.sync_all().map_err(DaemonError::from)?;
    }
    {
        let mut f = fs::File::create(&tmp_idx).map_err(DaemonError::from)?;
        f.write_all(idx_bytes).map_err(DaemonError::from)?;
        f.sync_all().map_err(DaemonError::from)?;
    }

    // Write `.pack` first, then `.idx` last (standard Git visibility order), then unlink subsumed packs!
    fs::rename(&tmp_pack, pack_dir.join(format!("{stem}.pack"))).map_err(DaemonError::from)?;
    fs::rename(&tmp_idx, pack_dir.join(format!("{stem}.idx"))).map_err(DaemonError::from)?;

    let unlinked_files = prune_subsumed_prefetch_packs(pack_dir, start_ts, end_ts, &stem)
        .map_err(DaemonError::from)?;

    let mut remaining_packs = 0usize;
    for entry_res in fs::read_dir(pack_dir).map_err(DaemonError::from)? {
        let entry = entry_res.map_err(DaemonError::from)?;
        if entry.path().extension().and_then(|e| e.to_str()) == Some("pack") {
            remaining_packs = remaining_packs.saturating_add(1);
        }
    }

    let mut midx_updated = false;
    if run_midx {
        if let Some(objects_dir) = pack_dir.parent() {
            write_multi_pack_index(objects_dir)?;
            midx_updated = true;
        }
    }

    Ok(WorkstationPrefetchOutcome {
        written_pack_stem: stem,
        start_ts,
        end_ts,
        unlinked_files,
        remaining_packs,
        midx_updated,
    })
}
