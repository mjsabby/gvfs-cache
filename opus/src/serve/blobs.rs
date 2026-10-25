//! Blobs at an edge of a `--blobs separate` store.
//!
//! * [`BlobSet`]: the blob packs `blobs.bin` lists, as of one generation. Every segment
//!   index is local (mapped in place in a directory store, else downloaded once: 48 bytes
//!   per blob). Packs of a directory store are mapped in place and leave through
//!   `sendfile(2)`; packs in object storage stay there and are read by range when a
//!   request needs them (cold path), never downloaded whole.
//! * [`Proxy`]: blobs the store does not have (the L0 overlay's, versions that only
//!   commits before genesis reference) come from the upstream: once per blob per edge
//!   (requests for a blob already being fetched wait for that fetch), verified by id as
//!   they stream in, and kept in a local cache with a byte budget. Only blobs the store's
//!   trees name are proxied ([`super::view::View::blob_size`]); anything else is a 404.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::blobs::{BlobManifest, BlobRec, Segment, pack_key};
use crate::blobstream::{BlobSink, FileSpool};
use crate::store::Store;
use crate::upstream::{Auth, BATCH, Upstream};
use crate::util::{Context, Oid, Result, sub, us};

/// One blob pack: mapped (local) or in object storage (remote).
pub struct BlobPack {
    pub id: String,
    pub len: u64,
    local: Option<(File, gvfs_mmap::Map)>,
    remote: Option<Remote>,
    /// Removed when the last user drops it (a proxied pack evicted from the cache).
    unlink: Option<PathBuf>,
}

/// Where a remote pack's bytes come from.
enum Remote {
    /// Ranged reads straight from the store.
    Direct(Arc<Store>, String),
    /// Through the edge's block cache ([`super::blobcache`]).
    Cached(Arc<super::blobcache::BlockCache>, Arc<str>, String),
}

impl BlobPack {
    pub fn open(path: &Path, id: &str) -> Result<BlobPack> {
        let file = File::open(path).ctx(&format!("opening {}", path.display()))?;
        let map = gvfs_mmap::Map::open(&file, false).ctx("mapping blob pack")?;
        Ok(BlobPack {
            id: id.to_string(),
            len: crate::util::u64_of(map.bytes().len()),
            local: Some((file, map)),
            remote: None,
            unlink: None,
        })
    }

    /// A pack in object storage, read through `cache` when there is one.
    pub fn remote(
        store: Arc<Store>,
        cache: Option<&Arc<super::blobcache::BlockCache>>,
        id: &str,
        len: u64,
    ) -> BlobPack {
        let key = pack_key(id);
        BlobPack {
            id: id.to_string(),
            len,
            local: None,
            remote: Some(match cache {
                Some(c) => Remote::Cached(Arc::clone(c), Arc::from(id), key),
                None => Remote::Direct(store, key),
            }),
            unlink: None,
        }
    }

    /// The mapped bytes of a local pack.
    #[inline]
    pub fn bytes(&self) -> Option<&[u8]> {
        self.local.as_ref().map(|(_, m)| m.bytes())
    }

    pub fn file(&self) -> Option<&File> {
        self.local.as_ref().map(|(f, _)| f)
    }

    /// `len` bytes at `at`, handed to `sink` in pieces (a remote pack: one ranged read).
    pub fn read(&self, at: u64, len: u64, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        if let Some(b) = self.bytes() {
            return sink(sub(b, us(at), us(len)).ctx("blob pack range")?);
        }
        match &self.remote {
            Some(Remote::Direct(store, key)) => store.read_range(key, at, len, sink),
            Some(Remote::Cached(cache, id, key)) => cache.read(id, key, self.len, at, len, sink),
            None => crate::bail!("blob pack {} has no bytes", self.id),
        }
    }
}

impl Drop for BlobPack {
    fn drop(&mut self) {
        if let Some(p) = &self.unlink {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// The blob packs of one `blobs.bin` generation.
pub struct BlobSet {
    pub generation: u64,
    /// Every commit since genesis has its blobs in the store (`tip complete`).
    pub complete: bool,
    pub etag: String,
    /// Newest segment first, each with its packs (indexed by a record's `pack`).
    segments: Vec<(Arc<Segment>, Vec<Arc<BlobPack>>)>,
}

impl BlobSet {
    /// Opens what `bm` lists, reusing what `prev` already has open.
    pub fn load(
        store: &Arc<Store>,
        cache: &Path,
        blocks: Option<&Arc<super::blobcache::BlockCache>>,
        bm: &BlobManifest,
        etag: String,
        prev: Option<&BlobSet>,
    ) -> Result<BlobSet> {
        let mut segs: HashMap<&str, &Arc<Segment>> = HashMap::new();
        let mut packs: HashMap<&str, &Arc<BlobPack>> = HashMap::new();
        for (s, ps) in prev.map(|p| p.segments.as_slice()).unwrap_or(&[]) {
            segs.insert(s.id.as_str(), s);
            for p in ps {
                packs.insert(p.id.as_str(), p);
            }
        }
        std::fs::create_dir_all(cache).ctx(&format!("creating {}", cache.display()))?;
        let mut segments = Vec::with_capacity(bm.segments.len());
        for meta in bm.segments.iter().rev() {
            let seg = match segs.get(meta.id.as_str()) {
                Some(s) => Arc::clone(s),
                None => Arc::new(crate::blobcopy::open_segment(store, cache, meta)?),
            };
            let mut list = Vec::with_capacity(seg.packs().len());
            for p in seg.packs() {
                let pack = match packs.get(p.id.as_str()) {
                    Some(x) => Arc::clone(x),
                    None => Arc::new(match store.local_path(&pack_key(&p.id)) {
                        Some(path) => BlobPack::open(&path, &p.id)?,
                        None => BlobPack::remote(Arc::clone(store), blocks, &p.id, p.len),
                    }),
                };
                if pack.len != p.len {
                    crate::bail!(
                        "blob pack {} is {} bytes, its segment says {}",
                        p.id,
                        pack.len,
                        p.len
                    );
                }
                list.push(pack);
            }
            segments.push((seg, list));
        }
        Ok(BlobSet {
            generation: bm.generation,
            complete: bm.complete(),
            etag,
            segments,
        })
    }

    /// The pack and record of blob `oid`, if the store has it.
    #[inline]
    pub fn find(&self, oid: &Oid) -> Option<(&Arc<BlobPack>, BlobRec)> {
        self.segments.iter().find_map(|(s, packs)| {
            let r = s.find(oid)?;
            Some((packs.get(us(u64::from(r.pack)))?, r))
        })
    }

    pub fn segments(&self) -> usize {
        self.segments.len()
    }

    /// Distinct packs.
    pub fn packs(&self) -> usize {
        let mut ids = HashSet::new();
        for (_, ps) in &self.segments {
            for p in ps {
                ids.insert(p.id.as_str());
            }
        }
        ids.len()
    }
}

struct ProxyState {
    cache: HashMap<Oid, (Arc<BlobPack>, BlobRec)>,
    /// Fetched packs, oldest first, with their blobs (eviction order).
    order: VecDeque<(Arc<BlobPack>, Vec<Oid>)>,
    bytes: u64,
    fetching: HashSet<Oid>,
}

/// Read-through of blobs the store lacks (see the module docs).
pub struct Proxy {
    upstream: String,
    branch: String,
    auth: Auth,
    dir: PathBuf,
    budget: u64,
    idle: Mutex<Vec<Upstream>>,
    state: Mutex<ProxyState>,
    done: Condvar,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

impl Proxy {
    /// Fetched blobs are kept under `dir` (emptied now) up to `budget` bytes.
    pub fn new(
        upstream: &str,
        branch: &str,
        auth: &Auth,
        dir: &Path,
        budget: u64,
    ) -> Result<Proxy> {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
        Ok(Proxy {
            upstream: upstream.to_string(),
            branch: branch.to_string(),
            auth: auth.clone(),
            dir: dir.to_path_buf(),
            budget,
            idle: Mutex::new(Vec::new()),
            state: Mutex::new(ProxyState {
                cache: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                fetching: HashSet::new(),
            }),
            done: Condvar::new(),
        })
    }

    /// A blob fetched earlier (no allocation).
    #[inline]
    pub fn cached(&self, oid: &Oid) -> Option<(Arc<BlobPack>, BlobRec)> {
        lock(&self.state).cache.get(oid).cloned()
    }

    /// The blobs `oids` (distinct): from the cache, else from the upstream in one request
    /// for all of those not being fetched already (others wait for those fetches). Cold
    /// path. What the call has in hand it keeps (an `Arc` keeps a pack open, cached or
    /// not), so the cache evicting one pack for another never makes it start over.
    pub fn fetch(&self, oids: &[Oid]) -> Result<Vec<(Arc<BlobPack>, BlobRec)>> {
        let _cold = gvfs_alloc_guard::cold_section();
        let mut found: HashMap<Oid, (Arc<BlobPack>, BlobRec)> = HashMap::with_capacity(oids.len());
        loop {
            let mine: Vec<Oid> = {
                let mut st = lock(&self.state);
                loop {
                    for o in oids {
                        if !found.contains_key(o) {
                            if let Some(hit) = st.cache.get(o) {
                                found.insert(*o, hit.clone());
                            }
                        }
                    }
                    let missing: Vec<Oid> = oids
                        .iter()
                        .filter(|o| !found.contains_key(o))
                        .copied()
                        .collect();
                    if missing.is_empty() {
                        return oids
                            .iter()
                            .map(|o| found.get(o).cloned().ctx("proxied blob"))
                            .collect();
                    }
                    let mine: Vec<Oid> = missing
                        .into_iter()
                        .filter(|o| !st.fetching.contains(o))
                        .collect();
                    if !mine.is_empty() {
                        st.fetching.extend(mine.iter().copied());
                        break mine;
                    }
                    st = match self.done.wait(st) {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                }
            };
            let got = self.fetch_now(&mine);
            let mut st = lock(&self.state);
            for o in &mine {
                st.fetching.remove(o);
            }
            if let Ok((pack, recs)) = &got {
                for r in recs {
                    st.cache.insert(r.oid, (Arc::clone(pack), *r));
                    found.insert(r.oid, (Arc::clone(pack), *r));
                }
                st.bytes = st.bytes.saturating_add(pack.len);
                st.order
                    .push_back((Arc::clone(pack), recs.iter().map(|r| r.oid).collect()));
                // Oldest packs go first; the one just fetched always stays.
                while st.bytes > self.budget && st.order.len() > 1 {
                    let Some((old, ids)) = st.order.pop_front() else {
                        break;
                    };
                    for id in &ids {
                        st.cache.remove(id);
                    }
                    st.bytes = st.bytes.saturating_sub(old.len);
                }
            }
            self.done.notify_all();
            drop(st);
            got?;
        }
    }

    fn connection(&self) -> Result<Upstream> {
        match lock(&self.idle).pop() {
            Some(u) => Ok(u),
            None => Upstream::open(&self.upstream, &self.branch, &self.auth),
        }
    }

    fn fetch_now(&self, oids: &[Oid]) -> Result<(Arc<BlobPack>, Vec<BlobRec>)> {
        let t0 = std::time::Instant::now();
        let mut up = self.connection()?;
        let id = crate::blobcopy::fresh_id(oids);
        let path = self.dir.join(format!("{id}.pack"));
        let result = (|| -> Result<(BlobPack, Vec<BlobRec>)> {
            let mut sink = BlobSink::new(FileSpool::create(&path)?, oids)?;
            for chunk in oids.chunks(BATCH) {
                if let Err(e) = up.fetch_objects(chunk, &mut sink) {
                    return Err(sink.failure().unwrap_or(e));
                }
                sink.end_input()?;
            }
            let f = sink.finish()?;
            f.out.close()?;
            let mut pack = BlobPack::open(&path, &id)?;
            pack.unlink = Some(path.clone());
            Ok((pack, f.recs))
        })();
        match result {
            Ok((pack, recs)) => {
                lock(&self.idle).push(up);
                crate::log!(
                    "serve: proxied {} blobs ({}) from the upstream in {:.2}s",
                    recs.len(),
                    crate::util::human_bytes(pack.len),
                    t0.elapsed().as_secs_f64()
                );
                Ok((Arc::new(pack), recs))
            }
            Err(e) => {
                let _ = std::fs::remove_file(&path);
                Err(e)
            }
        }
    }

    /// Sizes the upstream knows (cold path).
    pub fn sizes(&self, oids: &[Oid]) -> Result<Vec<Option<u64>>> {
        let _cold = gvfs_alloc_guard::cold_section();
        let mut up = self.connection()?;
        let r = up.object_sizes(oids)?;
        lock(&self.idle).push(up);
        Ok(r)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    fn git(dir: &Path, args: &[&str]) -> String {
        let mut cmd = Command::new("git");
        cmd.args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        crate::upstream::hermetic_git_env(&mut cmd);
        let out = cmd.output().unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// A request needing a proxied blob that is cached and one that is not, with a cache
    /// that can hold one pack: both come back (the first is not evicted from under it).
    #[test]
    fn a_request_spanning_proxied_packs_completes_whatever_the_budget() {
        let dir = std::env::temp_dir().join(format!("gvd-proxy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let work = dir.join("w");
        std::fs::create_dir_all(&work).unwrap();
        git(&dir, &["init", "-q", "-b", "main", work.to_str().unwrap()]);
        let blob = |n: usize, seed: u8| -> Vec<u8> {
            (0..n).map(|i| (i as u8).wrapping_mul(seed)).collect()
        };
        std::fs::write(work.join("a.bin"), blob(70_000, 3)).unwrap();
        std::fs::write(work.join("b.bin"), blob(90_000, 5)).unwrap();
        git(&work, &["add", "."]);
        git(
            &work,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "-q",
                "-m",
                "c",
            ],
        );
        let id = |p: &str| {
            Oid::from_hex(
                git(&work, &["rev-parse", &format!("HEAD:{p}")])
                    .trim()
                    .as_bytes(),
            )
            .unwrap()
        };
        let (a, b) = (id("a.bin"), id("b.bin"));
        let up = work.join(".git");
        let proxy = Proxy::new(
            up.to_str().unwrap(),
            "main",
            &Auth::default(),
            &dir.join("proxy"),
            0,
        )
        .unwrap();
        assert_eq!(proxy.fetch(&[a]).unwrap()[0].1.oid, a);
        let both = proxy.fetch(&[a, b]).unwrap();
        assert_eq!(both.iter().map(|x| x.1.oid).collect::<Vec<_>>(), vec![a, b]);
        assert_eq!((both[0].1.size, both[1].1.size), (70_000, 90_000));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
