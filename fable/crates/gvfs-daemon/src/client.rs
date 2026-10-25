//! The client-side prefetch hook (`gvfs-daemon client-prefetch`): a zero-GC workstation.
//!
//! One run does `GET /gvfs/prefetch?lastPackTimestamp=<cursor>` against the edge (or the
//! localhost proxy), streams the pack and its idx straight into the repository's pack
//! directory as `prefetch-<start>-<end>.pack/.idx` — the interval from
//! `X-GVFS-Pack-Start`/`End` — and then unlinks every local prefetch pack whose interval lies
//! inside another one's: a rollup is a strict superset of the slabs it replaced, so the
//! subsumed packs are pure redundancy. When more prefetch packs than `--budget` remain (a
//! server keeping leaves), it asks for the covering rollups with `compact=1`, which collapses
//! the run of small packs into one. Optionally `git multi-pack-index write`. Never `git gc`,
//! never `index-pack`: the idx arrives with the pack.
//!
//! The cursor is the newest interval end on disk, so the hook is stateless; stock
//! `prefetch-<timestamp>-<sha>.pack` files (from a VFS for Git / Scalar client) contribute
//! their timestamp to the cursor but are never deleted (their start is unknown).

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::transport::{Auth, SmartHttp, Url};

pub type ClientError = Box<dyn std::error::Error + Send + Sync>;

fn err(msg: impl Into<String>) -> ClientError {
    msg.into().into()
}

const PREFIX: &str = "prefetch-";
const TMP_PREFIX: &str = "tmp-prefetch-";

/// A prefetch pack on disk.
#[derive(Debug, Clone)]
pub struct LocalPack {
    /// Path without extension.
    pub stem: PathBuf,
    pub start: u64,
    pub end: u64,
    /// Named by this hook (`prefetch-<start>-<end>`), so its interval is known exactly.
    pub ours: bool,
    pub bytes: u64,
}

pub struct Options<'a> {
    /// Edge base URL, e.g. `http://127.0.0.1:8080` or `https://edge/org/proj/_git/repo`.
    pub server: &'a str,
    pub auth: Auth,
    pub pack_dir: PathBuf,
    /// Compact when more hook-named packs than this remain; 0 disables.
    pub budget: usize,
    /// Ask for whole rollups instead of leaves on the regular fetch too.
    pub compact: bool,
    /// Run `git multi-pack-index write` afterwards.
    pub midx: bool,
}

#[derive(Debug, Default)]
pub struct Summary {
    pub cursor: u64,
    pub received: usize,
    pub bytes: u64,
    pub interval: Option<(u64, u64)>,
    pub unlinked: usize,
    pub compaction_bytes: u64,
    pub local_packs: usize,
}

/// The pack directory git reads prefetch packs from: the first alternate's (the shared
/// object cache of Scalar / VFS for Git) when there is one, else the repository's own.
pub fn pack_dir_for(repo: &Path) -> Result<PathBuf, ClientError> {
    let dot = repo.join(".git");
    let gitdir = if dot.is_dir() {
        dot
    } else if dot.is_file() {
        let text = fs::read_to_string(&dot)?;
        let p = text
            .trim()
            .strip_prefix("gitdir:")
            .ok_or_else(|| err(format!("{} is not a gitfile", dot.display())))?
            .trim();
        repo.join(p)
    } else if repo.join("objects").is_dir() && repo.join("HEAD").is_file() {
        repo.to_path_buf()
    } else {
        return Err(err(format!("{} is not a git repository", repo.display())));
    };
    let objects = gitdir.join("objects");
    if let Ok(alts) = fs::read_to_string(objects.join("info/alternates"))
        && let Some(first) = alts
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let alt = Path::new(first);
        let alt = if alt.is_absolute() {
            alt.to_path_buf()
        } else {
            objects.join(alt)
        };
        return Ok(alt.join("pack"));
    }
    Ok(objects.join("pack"))
}

/// Lists the prefetch packs in `dir`, sweeping leftovers of an interrupted run (temp files,
/// a pack without its idx — git ignores those too).
pub fn scan_local(dir: &Path) -> Result<Vec<LocalPack>, ClientError> {
    fs::create_dir_all(dir)?;
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with(TMP_PREFIX) {
            let _ = fs::remove_file(&path);
            continue;
        }
        let Some(stem) = name.strip_suffix(".pack") else {
            continue;
        };
        let Some(rest) = stem.strip_prefix(PREFIX) else {
            continue;
        };
        let idx = dir.join(format!("{stem}.idx"));
        if !idx.is_file() {
            let _ = fs::remove_file(&path);
            continue;
        }
        let bytes = entry
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0)
            .saturating_add(fs::metadata(&idx).map(|m| m.len()).unwrap_or(0));
        let (start, end, ours) = match rest.split_once('-') {
            Some((a, b)) => match (a.parse::<u64>(), b.parse::<u64>()) {
                (Ok(s), Ok(e)) if s <= e => (s, e, true),
                (Ok(ts), _) => (ts, ts, false),
                _ => continue,
            },
            None => match rest.parse::<u64>() {
                Ok(ts) => (ts, ts, false),
                Err(_) => continue,
            },
        };
        out.push(LocalPack {
            stem: dir.join(stem),
            start,
            end,
            ours,
            bytes,
        });
    }
    out.sort_by_key(|p| (p.start, p.end));
    Ok(out)
}

/// One prefetch run.
pub fn client_prefetch(opts: &Options<'_>, log: &mut dyn Write) -> Result<Summary, ClientError> {
    let url =
        Url::parse(opts.server).ok_or_else(|| err(format!("bad server url '{}'", opts.server)))?;
    let http = SmartHttp::new(url, opts.auth.clone())?;
    let mut local = scan_local(&opts.pack_dir)?;
    let cursor = local.iter().map(|p| p.end).max().unwrap_or(0);
    let mut summary = Summary {
        cursor,
        ..Summary::default()
    };
    match fetch(&http, cursor, opts.compact, &opts.pack_dir, log)? {
        Some(r) => {
            summary.received = r.packs;
            summary.bytes = r.bytes;
            summary.interval = Some((r.start, r.end));
            local = scan_local(&opts.pack_dir)?;
            summary.unlinked = tombstone(&mut local, log);
        }
        None => {
            let _ = writeln!(log, "up to date at {cursor}");
        }
    }
    if opts.budget > 0
        && local.iter().filter(|p| p.ours).count() > opts.budget
        && let Some(since) = compaction_cursor(&local, opts.budget)
    {
        let _ = writeln!(
            log,
            "{} prefetch packs exceed the budget of {}; compacting from {since}",
            local.len(),
            opts.budget
        );
        if let Some(r) = fetch(&http, since, true, &opts.pack_dir, log)? {
            summary.compaction_bytes = r.bytes;
            local = scan_local(&opts.pack_dir)?;
            summary.unlinked = summary.unlinked.saturating_add(tombstone(&mut local, log));
        }
    }
    summary.local_packs = local.len();
    if opts.midx {
        run_midx(&opts.pack_dir, log)?;
    }
    Ok(summary)
}

struct Received {
    start: u64,
    end: u64,
    packs: usize,
    bytes: u64,
}

/// Streams one prefetch response into `dir`; `None` when the server has nothing new.
fn fetch(
    http: &SmartHttp,
    cursor: u64,
    compact: bool,
    dir: &Path,
    log: &mut dyn Write,
) -> Result<Option<Received>, ClientError> {
    let query = format!(
        "/gvfs/prefetch?lastPackTimestamp={cursor}{}",
        if compact { "&compact=1" } else { "" }
    );
    let mut r = http.get(&query)?;
    if r.status != 200 {
        let mut excerpt = vec![0u8; 512];
        let n = r.body.read(&mut excerpt).unwrap_or(0);
        let text = String::from_utf8_lossy(excerpt.get(..n).unwrap_or(&[]))
            .trim()
            .to_string();
        return Err(err(format!("GET {query}: {} {text}", r.status_line)));
    }
    let hdr_start = r
        .header("x-gvfs-pack-start")
        .and_then(|v| v.parse::<u64>().ok());
    let hdr_end = r
        .header("x-gvfs-pack-end")
        .and_then(|v| v.parse::<u64>().ok());
    let mut head = [0u8; 8];
    r.body.read_exact(&mut head)?;
    if head.get(..6) != Some(b"GPRE \x01".as_slice()) {
        return Err(err("the response is not a GPRE prefetch stream"));
    }
    let count = usize::from(u16::from_le_bytes(
        head.get(6..8)
            .and_then(|b| b.try_into().ok())
            .unwrap_or([0, 0]),
    ));
    if count == 0 {
        return Ok(None);
    }
    let mut prev = cursor;
    let (mut first_start, mut last_end, mut total) = (None, 0u64, 0u64);
    for _ in 0..count {
        let mut h = [0u8; 24];
        r.body.read_exact(&mut h)?;
        let field = |i: usize| {
            h.get(i..i.saturating_add(8))
                .and_then(|b| b.try_into().ok())
                .map(i64::from_le_bytes)
                .unwrap_or(-1)
        };
        let (ts, pack_len, idx_len) = (field(0), field(8), field(16));
        let (Ok(ts), Ok(pack_len), Ok(idx_len)) = (
            u64::try_from(ts),
            u64::try_from(pack_len),
            u64::try_from(idx_len),
        ) else {
            return Err(err("malformed GPRE pack header"));
        };
        if pack_len == 0 || idx_len == 0 {
            return Err(err("GPRE pack without an idx"));
        }
        // A single pack carries the server's covered interval; a stock server's packs are
        // consecutive slices (previous timestamp, this timestamp].
        let (start, end) = if count == 1 {
            (hdr_start.unwrap_or(prev), hdr_end.unwrap_or(ts))
        } else {
            (prev, ts)
        };
        let stem = format!("{PREFIX}{start}-{end}");
        let tmp_pack = dir.join(format!("{TMP_PREFIX}{start}-{end}.pack"));
        let tmp_idx = dir.join(format!("{TMP_PREFIX}{start}-{end}.idx"));
        copy_exact(&mut r.body, &tmp_pack, pack_len)?;
        copy_exact(&mut r.body, &tmp_idx, idx_len)?;
        // Pack first, idx last: git never sees an idx without its pack.
        fs::rename(&tmp_pack, dir.join(format!("{stem}.pack")))?;
        fs::rename(&tmp_idx, dir.join(format!("{stem}.idx")))?;
        let _ = writeln!(
            log,
            "received {stem} ({pack_len} + {idx_len} bytes, timestamp {ts})"
        );
        total = total.saturating_add(pack_len).saturating_add(idx_len);
        first_start.get_or_insert(start);
        last_end = last_end.max(end);
        prev = ts;
    }
    Ok(Some(Received {
        start: first_start.unwrap_or(cursor),
        end: last_end,
        packs: count,
        bytes: total,
    }))
}

fn copy_exact(src: &mut dyn Read, path: &Path, len: u64) -> Result<(), ClientError> {
    let mut f = BufWriter::with_capacity(1 << 20, File::create(path)?);
    let n = io::copy(&mut src.take(len), &mut f)?;
    if n != len {
        return Err(err(format!(
            "truncated stream: {n} of {len} bytes for {}",
            path.display()
        )));
    }
    f.flush()?;
    Ok(())
}

/// Unlinks every hook-named pack whose interval lies inside another hook-named pack's.
fn tombstone(local: &mut Vec<LocalPack>, log: &mut dyn Write) -> usize {
    let mut removed = 0usize;
    let mut i = 0;
    while let Some(p) = local.get(i).cloned() {
        let subsumed = p.ours
            && local
                .iter()
                .enumerate()
                .any(|(j, q)| j != i && q.ours && q.start <= p.start && p.end <= q.end);
        if subsumed {
            let _ = writeln!(
                log,
                "unlink {} [{}..{}] ({} bytes)",
                p.stem.display(),
                p.start,
                p.end,
                p.bytes
            );
            // idx first: git never sees a pack without its idx... nor an idx without a pack.
            let a = fs::remove_file(p.stem.with_extension("idx")).is_ok();
            let b = fs::remove_file(p.stem.with_extension("pack")).is_ok();
            if a || b {
                removed = removed.saturating_add(1);
            }
            local.remove(i);
        } else {
            i = i.saturating_add(1);
        }
    }
    removed
}

/// Where a compaction request starts: keep the oldest half of the budget (the cold-clone
/// pack and the long rollups), collapse everything after it into one pack.
fn compaction_cursor(local: &[LocalPack], budget: usize) -> Option<u64> {
    let anchors = local.iter().filter(|p| p.ours && p.start == 0).count();
    let mut rest: Vec<&LocalPack> = local.iter().filter(|p| p.ours && p.start > 0).collect();
    rest.sort_by_key(|p| (p.start, p.end));
    let keep = (budget / 2).saturating_sub(anchors);
    if rest.len().saturating_sub(keep) < 2 {
        return None;
    }
    rest.get(keep).map(|p| p.start)
}

fn run_midx(pack_dir: &Path, log: &mut dyn Write) -> Result<(), ClientError> {
    let object_dir = pack_dir
        .parent()
        .ok_or_else(|| err("the pack directory has no parent"))?;
    let status = std::process::Command::new("git")
        .args(["multi-pack-index", "write", "--object-dir"])
        .arg(object_dir)
        .status()?;
    if !status.success() {
        return Err(err(format!("git multi-pack-index write failed: {status}")));
    }
    let _ = writeln!(log, "multi-pack-index written for {}", object_dir.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn pack(dir: &Path, start: u64, end: u64) -> LocalPack {
        LocalPack {
            stem: dir.join(format!("prefetch-{start}-{end}")),
            start,
            end,
            ours: true,
            bytes: 1,
        }
    }

    #[test]
    fn scan_names_and_sweeps_leftovers() {
        let dir = std::env::temp_dir().join(format!("gvfs-client-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        for n in [
            "prefetch-100-200",
            "prefetch-200-300",
            "prefetch-250-abcdef01",
            "pack-deadbeef",
        ] {
            fs::write(dir.join(format!("{n}.pack")), b"p").unwrap();
            fs::write(dir.join(format!("{n}.idx")), b"i").unwrap();
        }
        fs::write(dir.join("prefetch-300-400.pack"), b"orphan").unwrap();
        fs::write(dir.join("tmp-prefetch-1-2.pack"), b"tmp").unwrap();
        let local = scan_local(&dir).unwrap();
        let names: Vec<(u64, u64, bool)> = local.iter().map(|p| (p.start, p.end, p.ours)).collect();
        assert_eq!(
            names,
            vec![(100, 200, true), (200, 300, true), (250, 250, false)]
        );
        assert!(!dir.join("prefetch-300-400.pack").exists());
        assert!(!dir.join("tmp-prefetch-1-2.pack").exists());
        assert_eq!(local.iter().map(|p| p.end).max(), Some(300));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tombstone_is_containment_only() {
        let dir = std::env::temp_dir().join(format!("gvfs-client-t-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut local = vec![
            pack(&dir, 0, 100),
            pack(&dir, 100, 115),
            pack(&dir, 115, 130),
            pack(&dir, 100, 160),
            pack(&dir, 160, 175),
        ];
        for p in &local {
            fs::write(p.stem.with_extension("pack"), b"p").unwrap();
            fs::write(p.stem.with_extension("idx"), b"i").unwrap();
        }
        let mut log = Vec::new();
        assert_eq!(tombstone(&mut local, &mut log), 2);
        let left: Vec<(u64, u64)> = local.iter().map(|p| (p.start, p.end)).collect();
        assert_eq!(left, vec![(0, 100), (100, 160), (160, 175)]);
        assert!(
            !dir.join("prefetch-100-115.pack").exists()
                && !dir.join("prefetch-100-115.idx").exists()
        );
        assert!(dir.join("prefetch-160-175.pack").exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn compaction_keeps_the_old_half() {
        let dir = Path::new("/nonexistent");
        let mut local = vec![pack(dir, 0, 100)];
        for i in 0..10u64 {
            local.push(pack(dir, 100 + i * 10, 110 + i * 10));
        }
        // Budget 6: keep the anchor + 2 oldest rollups, collapse from the third.
        assert_eq!(compaction_cursor(&local, 6), Some(120));
        // Nothing worth collapsing.
        assert_eq!(compaction_cursor(&local[..3], 6), None);
        // Stock packs never count.
        local.iter_mut().for_each(|p| p.ours = false);
        assert_eq!(compaction_cursor(&local, 6), None);
    }
}
