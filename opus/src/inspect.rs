//! `gvfs-daemon inspect`: human-readable views of the manifest and of slabs.

use std::path::{Path, PathBuf};

use crate::manifest::{MANIFEST_KEY, Manifest};
use crate::pack::{OBJ_BLOB, OBJ_COMMIT, OBJ_TAG, OBJ_TREE};
use crate::slab::Slab;
use crate::splice::Resolver;
use crate::util::{Context, Result, human_bytes, u64_of};

pub fn main(args: &[String]) -> Result<()> {
    let (what, rest) = args
        .split_first()
        .ctx("inspect what? (manifest | slab <FILE.mphf>)")?;
    match what.as_str() {
        "manifest" => {
            let f = crate::cli::Flags::parse(rest, &["store", "store-auth-env"])?;
            let store = crate::store::Store::open(
                f.require("store")?,
                f.get("store-auth-env")
                    .unwrap_or("AZURE_STORAGE_CONNECTION_STRING"),
            )?;
            let (bytes, etag) = store
                .get(MANIFEST_KEY)?
                .ctx("no manifest.bin in the store")?;
            let m = Manifest::decode(&bytes)?;
            print!("{} etag {etag}\n{}", store.describe(), m.describe());
            Ok(())
        }
        "slab" => {
            let f = crate::cli::Flags::parse(rest, &["threads", "write-idx"])?;
            let path = PathBuf::from(f.positional.first().ctx("inspect slab <FILE.mphf>")?);
            let ix = std::fs::read(&path).ctx(&format!("reading {}", path.display()))?;
            let get32 = |at: usize| crate::util::le_u32(&ix, at).unwrap_or(0);
            let get64 = |at: usize| crate::util::le_u64(&ix, at).unwrap_or(0);
            println!(
                "{}: {} objects ({} pack entries), {} partitions x {} buckets, pack {} bytes, trailer {}",
                path.display(),
                get32(12),
                get32(60),
                get32(16),
                get32(20),
                get64(32),
                crate::util::hex_string(crate::util::sub(&ix, 40, 20).unwrap_or(&[]))
            );
            let pack = path.with_extension("pack");
            if pack.exists() {
                let t = std::time::Instant::now();
                let stats =
                    content_stats(&path, f.num("threads", crate::util::default_threads())?)?;
                print!("{}", stats.describe());
                println!("  (scanned in {:.2}s)", t.elapsed().as_secs_f64());
            }
            if let Some(out) = f.get("write-idx") {
                // The slab's pack as a one-slab prefetch plan: the same index a VFS for Git
                // client would be sent.
                let t = std::time::Instant::now();
                let plan = crate::splice::PackPlan::new(vec![Slab::open_file(&path)?]);
                let idx = plan
                    .idx()
                    .ctx("the slab's records do not cover every pack entry")?;
                std::fs::write(out, idx).ctx(&format!("writing {out}"))?;
                println!(
                    "  wrote {out} ({}) in {:.2}s",
                    human_bytes(u64_of(idx.len())),
                    t.elapsed().as_secs_f64()
                );
            }
            Ok(())
        }
        other => crate::bail!("unknown inspect target {other:?}"),
    }
}

/// What a slab holds, by object type, delta depth, and GitHFS pointer coverage.
#[derive(Clone, Copy, Default)]
struct Stats {
    kinds: [u64; 5],
    deltas: u64,
    depth_sum: u64,
    depth_max: u8,
    pointers: u64,
    /// Sum of the pointers' `size:` fields: the content the repository really holds.
    pointed_bytes: u64,
    other_blobs: u64,
    other_blob_bytes: u64,
    unreadable: u64,
}

impl Stats {
    fn merge(mut self, o: Stats) -> Stats {
        for (a, b) in self.kinds.iter_mut().zip(o.kinds) {
            *a = a.saturating_add(b);
        }
        self.deltas = self.deltas.saturating_add(o.deltas);
        self.depth_sum = self.depth_sum.saturating_add(o.depth_sum);
        self.depth_max = self.depth_max.max(o.depth_max);
        self.pointers = self.pointers.saturating_add(o.pointers);
        self.pointed_bytes = self.pointed_bytes.saturating_add(o.pointed_bytes);
        self.other_blobs = self.other_blobs.saturating_add(o.other_blobs);
        self.other_blob_bytes = self.other_blob_bytes.saturating_add(o.other_blob_bytes);
        self.unreadable = self.unreadable.saturating_add(o.unreadable);
        self
    }

    fn describe(&self) -> String {
        let k = |kind: u8| self.kinds.get(usize::from(kind)).copied().unwrap_or(0);
        let mean = if self.deltas == 0 {
            0.0
        } else {
            self.depth_sum as f64 / self.deltas as f64
        };
        format!(
            "  {} commits, {} trees, {} blobs, {} tags\n  {} deltas (mean depth {mean:.1}, max {})\n  {} GitHFS pointers -> {} of content; {} other blobs ({}){}\n",
            k(OBJ_COMMIT),
            k(OBJ_TREE),
            k(OBJ_BLOB),
            k(OBJ_TAG),
            self.deltas,
            self.depth_max,
            self.pointers,
            human_bytes(self.pointed_bytes),
            self.other_blobs,
            human_bytes(self.other_blob_bytes),
            if self.unreadable > 0 {
                format!("\n  {} UNREADABLE objects", self.unreadable)
            } else {
                String::new()
            },
        )
    }
}

/// Scans every record (in parallel); blobs small enough to be pointers are resolved and
/// parsed.
fn content_stats(index: &Path, threads: usize) -> Result<Stats> {
    let slab = Slab::open_file(index)?;
    let threads = u32::try_from(threads.max(1)).unwrap_or(1);
    let per = slab.count().div_ceil(threads).max(1);
    let slab = &slab;
    Ok(std::thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|t| {
                s.spawn(move || {
                    let mut st = Stats::default();
                    let mut resolver = Resolver::with_capacity(1 << 20);
                    let start = t.saturating_mul(per);
                    for slot in start..start.saturating_add(per).min(slab.count()) {
                        let Some(rec) = slab.rec(slot) else {
                            st.unreadable = st.unreadable.saturating_add(1);
                            continue;
                        };
                        if let Some(n) = st.kinds.get_mut(usize::from(rec.kind)) {
                            *n = n.saturating_add(1);
                        }
                        if rec.is_delta() {
                            st.deltas = st.deltas.saturating_add(1);
                            st.depth_sum = st.depth_sum.saturating_add(u64::from(rec.depth));
                            st.depth_max = st.depth_max.max(rec.depth);
                        }
                        if rec.kind != OBJ_BLOB {
                            continue;
                        }
                        let pointer = if rec.size > u64_of(crate::hfs::MAX_POINTER_LEN) {
                            None
                        } else {
                            match resolver.resolve(slab, slot) {
                                Ok((_, content)) => crate::hfs::parse_git_hfs(content),
                                Err(_) => {
                                    st.unreadable = st.unreadable.saturating_add(1);
                                    continue;
                                }
                            }
                        };
                        match pointer {
                            Some(p) => {
                                st.pointers = st.pointers.saturating_add(1);
                                st.pointed_bytes = st.pointed_bytes.saturating_add(p.size);
                            }
                            None => {
                                st.other_blobs = st.other_blobs.saturating_add(1);
                                st.other_blob_bytes = st.other_blob_bytes.saturating_add(rec.size);
                            }
                        }
                    }
                    st
                })
            })
            .collect();
        workers.into_iter().fold(Stats::default(), |acc, w| {
            acc.merge(w.join().unwrap_or_default())
        })
    }))
}
