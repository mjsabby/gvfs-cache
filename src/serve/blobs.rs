//! Blobs at an edge of a `--blobs separate` store.
//!
//! * [`BlobSet`]: the blob packs `blobs.bin` lists, as of one generation. Every segment
//!   index is local (mapped in place in a directory store, else downloaded once: 48 bytes
//!   per blob). Packs of a directory store are mapped in place and leave through
//!   `sendfile(2)`; packs in object storage stay there and are read by range when a
//!   request needs them (cold path), never downloaded whole.
//! * [`Proxy`]: blobs the store does not have come from the upstream: the L0 overlay's
//!   (the pipeline copies them when it seals their trees), and, as a fault the edge counts,
//!   any a sealed tree names that a complete store lacks. Once per blob per edge (requests
//!   for a blob already being fetched wait for that fetch), verified by id as they stream
//!   in, and kept in a local cache with a byte budget. Only blobs the store's trees name
//!   are proxied ([`super::view::View::blob_size`]); anything else is a 404.
//!
//! Remote reads and the proxy's fetches are async: a request waiting on object storage or
//! the upstream parks its task, not a thread.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

use crate::blobs::{BlobManifest, BlobRec, Segment, pack_key};
use crate::blobstream::{BlobSink, FileSpool};
use crate::store::Store;
use crate::upstream::{AsyncUpstream, Auth, BATCH};
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

    /// `len` bytes at `at`, handed to `sink` in pieces as they arrive (a remote pack:
    /// through the block cache when there is one, else a ranged GET per
    /// [`super::blobcache::WINDOW`] blocks). Pass whole ranges: the cache decides by a read's
    /// length, and fetches a window at a time.
    pub async fn read<S: crate::store::Sink>(&self, at: u64, len: u64, sink: &mut S) -> Result<()> {
        if let Some(b) = self.bytes() {
            return sink
                .put(sub(b, us(at), us(len)).ctx("blob pack range")?)
                .await;
        }
        match &self.remote {
            Some(Remote::Direct(store, key)) => {
                let window = super::blobcache::WINDOW.saturating_mul(super::blobcache::BLOCK);
                let end = at.saturating_add(len);
                let mut pos = at;
                while pos < end {
                    let n = end.saturating_sub(pos).min(window);
                    store.read_range_async(key, pos, n, sink).await?;
                    pos = pos.saturating_add(n);
                }
                Ok(())
            }
            Some(Remote::Cached(cache, id, key)) => {
                cache.read(id, key, self.len, at, len, sink).await
            }
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

/// A segment's packs, indexed by a record's `pack` (`None`: a local pack the store lost).
type SegmentPacks = Vec<Option<Arc<BlobPack>>>;

/// The blob packs of one `blobs.bin` generation.
pub struct BlobSet {
    pub generation: u64,
    /// Every blob of every sealed tree is in the store (`blobs.bin` complete).
    pub complete: bool,
    pub etag: String,
    /// Biggest segment first (most lookups end there), each with its packs.
    segments: Vec<(Arc<Segment>, SegmentPacks)>,
}

impl BlobSet {
    /// Opens what `bm` lists, reusing what `prev` already has open. What the store lost (a
    /// segment, a local pack: gone, or another length) is left out with a warning, and its
    /// blobs come through the proxy, counted as missing, until `gvfs-daemon mirror` repairs
    /// `blobs.bin`. (A lost pack in object storage shows only when it is read.)
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
            for p in ps.iter().flatten() {
                packs.insert(p.id.as_str(), p);
            }
        }
        let lost = |what: &str, id: &str, why: &str| {
            crate::log!(
                "serve: warning: blobs.bin generation {} lists {what} {id}, which the store does \
                 not hold ({why}); its blobs come through the proxy, counted as missing, until \
                 gvfs-daemon mirror repairs blobs.bin",
                bm.generation
            );
        };
        std::fs::create_dir_all(cache).ctx(&format!("creating {}", cache.display()))?;
        let mut segments = Vec::with_capacity(bm.segments.len());
        for meta in bm.segments.iter().rev() {
            let seg = match segs.get(meta.id.as_str()) {
                Some(s) => Arc::clone(s),
                None => match crate::blobcopy::open_segment(store, cache, meta) {
                    Ok(s) => Arc::new(s),
                    Err(e) if e.is_not_found() => {
                        lost("segment", &meta.id, &e.to_string());
                        continue;
                    }
                    Err(e) => return Err(e),
                },
            };
            let mut list = Vec::with_capacity(seg.packs().len());
            for p in seg.packs() {
                let pack = match packs.get(p.id.as_str()) {
                    Some(x) => Arc::clone(x),
                    None => Arc::new(match store.local_path(&pack_key(&p.id)) {
                        Some(path) => match BlobPack::open(&path, &p.id) {
                            Ok(pack) => pack,
                            Err(e) if e.is_not_found() => {
                                lost("pack", &p.id, &e.to_string());
                                list.push(None);
                                continue;
                            }
                            Err(e) => return Err(e),
                        },
                        None => BlobPack::remote(Arc::clone(store), blocks, &p.id, p.len),
                    }),
                };
                if pack.len != p.len {
                    let why = format!("{} bytes, its segment says {}", pack.len, p.len);
                    lost("pack", &p.id, &why);
                    list.push(None);
                    continue;
                }
                list.push(Some(pack));
            }
            segments.push((seg, list));
        }
        segments.sort_by_key(|(s, _)| std::cmp::Reverse(s.count()));
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
            Some((packs.get(us(u64::from(r.pack)))?.as_ref()?, r))
        })
    }

    /// Like [`BlobSet::find`], with the segment (for [`BlobSet::pack`]) in place of the pack:
    /// a request can note thousands of blobs without touching their packs' reference counts,
    /// which every core would otherwise contend for.
    #[inline]
    pub fn locate(&self, oid: &Oid) -> Option<(u32, BlobRec)> {
        self.segments
            .iter()
            .enumerate()
            .find_map(|(i, (s, packs))| {
                let r = s.find(oid)?;
                packs.get(us(u64::from(r.pack)))?.as_ref()?;
                Some((u32::try_from(i).ok()?, r))
            })
    }

    /// Starts loading what [`BlobSet::locate`] reads for `oid` in the biggest segment.
    #[inline]
    pub fn preload(&self, oid: &Oid) {
        if let Some((s, _)) = self.segments.first() {
            s.preload(oid);
        }
    }

    /// The pack of a record [`BlobSet::locate`] found in segment `seg`.
    #[inline]
    pub fn pack(&self, seg: u32, rec: &BlobRec) -> Option<&Arc<BlobPack>> {
        self.segments
            .get(us(u64::from(seg)))?
            .1
            .get(us(u64::from(rec.pack)))?
            .as_ref()
    }

    pub fn segments(&self) -> usize {
        self.segments.len()
    }

    /// Distinct packs.
    pub fn packs(&self) -> usize {
        let mut ids = HashSet::new();
        for (_, ps) in &self.segments {
            for p in ps.iter().flatten() {
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

impl ProxyState {
    /// Caches the blobs of `pack`, just fetched, then evicts the oldest packs beyond
    /// `budget` and returns them (the caller drops them outside the lock: the last
    /// reference to one unlinks its file). The pack just fetched always stays, since the
    /// requests that waited for its blobs look for them here when they wake: the cache can
    /// exceed its budget by one fetch. An id a newer pack holds too keeps that entry.
    fn keep(&mut self, pack: &Arc<BlobPack>, recs: &[BlobRec], budget: u64) -> Vec<Arc<BlobPack>> {
        for r in recs {
            self.cache.insert(r.oid, (Arc::clone(pack), *r));
        }
        self.bytes = self.bytes.saturating_add(pack.len);
        self.order
            .push_back((Arc::clone(pack), recs.iter().map(|r| r.oid).collect()));
        let mut evicted = Vec::new();
        while self.bytes > budget && self.order.len() > 1 {
            let Some((old, ids)) = self.order.pop_front() else {
                break;
            };
            for id in &ids {
                if self
                    .cache
                    .get(id)
                    .is_some_and(|(p, _)| Arc::ptr_eq(p, &old))
                {
                    self.cache.remove(id);
                }
            }
            self.bytes = self.bytes.saturating_sub(old.len);
            evicted.push(old);
        }
        evicted
    }
}

/// Upstream connections the proxy keeps for its next fetches (a burst opens more, which
/// go once used).
const PROXY_IDLE_MAX: usize = 8;

/// Read-through of blobs the store lacks (see the module docs).
pub struct Proxy {
    upstream: String,
    auth: Auth,
    dir: PathBuf,
    budget: u64,
    idle: Mutex<Vec<AsyncUpstream>>,
    state: Mutex<ProxyState>,
    /// Woken when a fetch ends (blobs cached, or the fetch failed).
    done: Notify,
}

/// Blobs a request is fetching: if the request is dropped before the fetch ends, they are
/// no longer marked, and requests waiting for them are woken (one of them fetches them).
struct Fetching<'a> {
    proxy: &'a Proxy,
    oids: &'a [Oid],
    armed: bool,
}

impl Drop for Fetching<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut st = lock(&self.proxy.state);
            for o in self.oids {
                st.fetching.remove(o);
            }
            drop(st);
            self.proxy.done.notify_waiters();
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

impl Proxy {
    /// Fetched blobs are kept under `dir` (emptied now) up to `budget` bytes.
    pub fn new(upstream: &str, auth: &Auth, dir: &Path, budget: u64) -> Result<Proxy> {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
        Ok(Proxy {
            upstream: upstream.to_string(),
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
            done: Notify::new(),
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
    pub async fn fetch(&self, oids: &[Oid]) -> Result<Vec<(Arc<BlobPack>, BlobRec)>> {
        let mut found: HashMap<Oid, (Arc<BlobPack>, BlobRec)> = HashMap::with_capacity(oids.len());
        loop {
            let mine: Vec<Oid> = loop {
                // Waiting is registered before the state is read: a fetch that ends in
                // between still wakes this request.
                let mut ended = std::pin::pin!(self.done.notified());
                ended.as_mut().enable();
                {
                    let mut st = lock(&self.state);
                    for o in oids {
                        if !found.contains_key(o)
                            && let Some(hit) = st.cache.get(o)
                        {
                            found.insert(*o, hit.clone());
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
                }
                ended.await;
            };
            let mut guard = Fetching {
                proxy: self,
                oids: &mine,
                armed: true,
            };
            let got = self.fetch_now(&mine).await;
            guard.armed = false;
            let mut st = lock(&self.state);
            for o in &mine {
                st.fetching.remove(o);
            }
            let mut evicted = Vec::new();
            if let Ok((pack, recs)) = &got {
                for r in recs {
                    found.insert(r.oid, (Arc::clone(pack), *r));
                }
                evicted = st.keep(pack, recs, self.budget);
            }
            drop(st);
            drop(evicted);
            self.done.notify_waiters();
            got?;
        }
    }

    fn connection(&self) -> Result<AsyncUpstream> {
        match lock(&self.idle).pop() {
            Some(u) => Ok(u),
            None => AsyncUpstream::open(&self.upstream, &self.auth),
        }
    }

    /// A connection back for the next fetch, unless enough are kept already.
    fn reuse(&self, up: AsyncUpstream) {
        let mut idle = lock(&self.idle);
        if idle.len() < PROXY_IDLE_MAX {
            idle.push(up);
        }
    }

    async fn fetch_now(&self, oids: &[Oid]) -> Result<(Arc<BlobPack>, Vec<BlobRec>)> {
        let t0 = std::time::Instant::now();
        let mut up = self.connection()?;
        let id = crate::blobcopy::fresh_id(oids);
        let path = self.dir.join(format!("{id}.pack"));
        match fetch_into(&mut up, oids, &path, &id).await {
            Ok((pack, recs)) => {
                self.reuse(up);
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
}

/// What [`fetch_into`]'s network side hands its blocking side.
enum Piece {
    Data(bytes::Bytes),
    /// One request's pack has ended (the sink checks it before the next).
    EndPack,
    /// Every pack has arrived.
    Done,
}

/// Hands pack pieces to [`fetch_into`]'s blocking side (waiting while its queue is full).
struct ToSink<'a>(&'a tokio::sync::mpsc::Sender<Piece>);

impl crate::upstream::PackPieces for ToSink<'_> {
    async fn put(&mut self, piece: bytes::Bytes) -> Result<()> {
        self.0
            .send(Piece::Data(piece))
            .await
            .map_err(|_| crate::err!("the blob sink stopped"))
    }
}

/// Fetches the blobs `oids` from `up` into a blob pack at `path` (id `id`), each verified
/// by id as it streams in. The network side is async (the request's task waits, not a
/// thread); verifying (inflating, hashing, re-deflating deltas) and writing run on tokio's
/// blocking pool, fed through a bounded queue, so the runtime's workers keep serving.
pub async fn fetch_into(
    up: &mut AsyncUpstream,
    oids: &[Oid],
    path: &Path,
    id: &str,
) -> Result<(BlobPack, Vec<BlobRec>)> {
    use std::io::Write;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Piece>(16);
    let (want, path, id) = (oids.to_vec(), path.to_path_buf(), id.to_string());
    // `Err((true, e))`: the sink failed (which explains a failed send); `(false, _)`: the
    // network side stopped first.
    let sink = tokio::task::spawn_blocking(move || {
        let spool = FileSpool::create(&path).map_err(|e| (true, e))?;
        let mut sink = BlobSink::new(spool, &want).map_err(|e| (true, e))?;
        loop {
            match rx.blocking_recv() {
                Some(Piece::Data(b)) => {
                    if let Err(e) = sink.write_all(&b) {
                        let e = sink.failure().unwrap_or_else(|| e.into());
                        return Err((true, e));
                    }
                }
                Some(Piece::EndPack) => sink.end_input().map_err(|e| (true, e))?,
                Some(Piece::Done) => break,
                None => return Err((false, crate::err!("the fetch stopped"))),
            }
        }
        let f = sink.finish().map_err(|e| (true, e))?;
        f.out.close().map_err(|e| (true, e))?;
        let mut pack = BlobPack::open(&path, &id).map_err(|e| (true, e))?;
        pack.unlink = Some(path);
        Ok((pack, f.recs))
    });
    let sent: Result<()> = async {
        for chunk in oids.chunks(BATCH) {
            up.fetch_objects(chunk, &mut ToSink(&tx)).await?;
            tx.send(Piece::EndPack)
                .await
                .map_err(|_| crate::err!("the blob sink stopped"))?;
        }
        tx.send(Piece::Done)
            .await
            .map_err(|_| crate::err!("the blob sink stopped"))
    }
    .await;
    drop(tx);
    let done = sink
        .await
        .map_err(|e| crate::err!("the blob sink failed: {e}"))?;
    match (sent, done) {
        (Ok(()), Ok(v)) => Ok(v),
        (_, Err((true, e))) | (Err(e), Err((false, _))) => Err(e),
        (Ok(()), Err((false, e))) | (Err(e), Ok(_)) => Err(e),
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

    /// A proxied pack of `len` bytes holding `ids`, its file unlinked with its last user.
    fn proxied(dir: &Path, name: &str, len: usize, ids: &[u8]) -> (Arc<BlobPack>, Vec<BlobRec>) {
        let path = dir.join(format!("{name}.pack"));
        std::fs::write(&path, vec![0u8; len]).unwrap();
        let mut pack = BlobPack::open(&path, name).unwrap();
        pack.unlink = Some(path);
        let recs = ids
            .iter()
            .map(|&i| BlobRec {
                oid: Oid([i; 20]),
                pack: 0,
                offset: 0,
                entry_len: 1,
                size: 1,
                hdr_len: 0,
                stored: false,
            })
            .collect();
        (Arc::new(pack), recs)
    }

    /// Evicting a pack leaves the entries of its ids that a newer pack holds; an evicted
    /// pack's file goes with its last user; the newest pack stays, whatever its size.
    #[test]
    fn proxy_eviction_keeps_newer_entries() {
        let dir = std::env::temp_dir().join(format!("gvd-proxy-evict-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut st = ProxyState {
            cache: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            fetching: HashSet::new(),
        };
        let (p1, r1) = proxied(&dir, "p1", 100, &[1, 2]);
        let (p2, r2) = proxied(&dir, "p2", 100, &[2, 3]);
        assert!(st.keep(&p1, &r1, 150).is_empty());
        let evicted = st.keep(&p2, &r2, 150);
        assert_eq!(evicted.len(), 1);
        assert!(Arc::ptr_eq(&evicted[0], &p1));
        let at = |st: &ProxyState, i: u8| st.cache.get(&Oid([i; 20])).map(|(p, _)| p.id.clone());
        assert_eq!(at(&st, 1), None);
        assert_eq!(
            at(&st, 2).as_deref(),
            Some("p2"),
            "the newer pack's entry stays"
        );
        assert_eq!(at(&st, 3).as_deref(), Some("p2"));
        assert_eq!(st.bytes, 100);
        let p1_file = dir.join("p1.pack");
        drop((evicted, p1, r1));
        assert!(!p1_file.exists(), "unlinked with its last user");
        // Larger than the whole budget: it stays until a newer one arrives.
        let (p3, r3) = proxied(&dir, "p3", 400, &[4]);
        assert_eq!(st.keep(&p3, &r3, 150).len(), 1);
        assert_eq!(at(&st, 4).as_deref(), Some("p3"));
        assert_eq!((st.order.len(), st.bytes), (1, 400));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The proxy keeps a few upstream connections for its next fetches, not every one a
    /// burst opened.
    #[test]
    fn proxy_keeps_a_few_idle_connections() {
        let dir = std::env::temp_dir().join(format!("gvd-proxy-idle-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let spec = dir.to_str().unwrap();
        let proxy = Proxy::new(spec, &Auth::default(), &dir.join("cache"), 1 << 20).unwrap();
        let ups: Vec<AsyncUpstream> = (0..3 * PROXY_IDLE_MAX)
            .map(|_| AsyncUpstream::open(spec, &Auth::default()).unwrap())
            .collect();
        for up in ups {
            proxy.reuse(up);
        }
        assert_eq!(lock(&proxy.idle).len(), PROXY_IDLE_MAX);
        let _ = std::fs::remove_dir_all(&dir);
    }

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
            &Auth::default(),
            &dir.join("proxy"),
            0,
        )
        .unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(rt.block_on(proxy.fetch(&[a])).unwrap()[0].1.oid, a);
        let both = rt.block_on(proxy.fetch(&[a, b])).unwrap();
        assert_eq!(both.iter().map(|x| x.1.oid).collect::<Vec<_>>(), vec![a, b]);
        assert_eq!((both[0].1.size, both[1].1.size), (70_000, 90_000));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
