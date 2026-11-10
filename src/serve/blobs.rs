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
//!   for a blob already being fetched wait for that fetch, and fail with it if it fails),
//!   verified by id as they stream in, and kept, one pack per fetch, until the store has
//!   them: small packs in memory, larger ones as files, bounded in bytes and in number
//!   (each holds a descriptor and a mapping). Only blobs the store's trees name are proxied
//!   ([`super::view::View::blob_size`]); anything else is a 404.
//!
//! Remote reads and the proxy's fetches are async: a request waiting on object storage or
//! the upstream parks its task, not a thread.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

use super::blobcache::Attempt;
use super::view::Ahead;
use crate::blobs::{BlobManifest, BlobRec, Segment, pack_key};
use crate::blobstream::{BlobSink, FileSpool};
use crate::store::Store;
use crate::upstream::{AsyncUpstream, Auth, BATCH};
use crate::util::{Context, Oid, Result, sub, us};

/// One blob pack: mapped or in memory (local), or in object storage (remote).
pub struct BlobPack {
    pub id: String,
    pub len: u64,
    local: Option<Local>,
    remote: Option<Remote>,
    /// Removed when the last user drops it (a proxied pack evicted from the cache).
    unlink: Option<PathBuf>,
}

/// Where a local pack's bytes are.
enum Local {
    Mapped(File, gvfs_mmap::Map),
    /// A small proxied pack ([`HEAP_MAX`]).
    Heap(Box<[u8]>),
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
            local: Some(Local::Mapped(file, map)),
            remote: None,
            unlink: None,
        })
    }

    /// A pack held in memory.
    fn heap(bytes: Vec<u8>, id: &str) -> BlobPack {
        BlobPack {
            id: id.to_string(),
            len: crate::util::u64_of(bytes.len()),
            local: Some(Local::Heap(bytes.into_boxed_slice())),
            remote: None,
            unlink: None,
        }
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

    /// The bytes of a local pack.
    #[inline]
    pub fn bytes(&self) -> Option<&[u8]> {
        match &self.local {
            Some(Local::Mapped(_, map)) => Some(map.bytes()),
            Some(Local::Heap(bytes)) => Some(bytes),
            None => None,
        }
    }

    /// The file of a mapped pack (to `sendfile(2)` from).
    pub fn file(&self) -> Option<&File> {
        match &self.local {
            Some(Local::Mapped(file, _)) => Some(file),
            _ => None,
        }
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

/// The most blobs of a segment that gets an id filter ([`Segment::prepare_filter`]: two
/// to four bytes per blob). A filter pays where lookups mostly miss, and while its lines
/// stay in the cache: the small segments, which hold the newest blobs.
const FILTER_MAX: u32 = 4 << 20;

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
    /// blobs come through the proxy, counted as missing, until `gvfs-cache mirror` repairs
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
                 gvfs-cache mirror repairs blobs.bin",
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
        // A lookup probes the segments in this order. Most end in the first; the ones
        // that go on (a new blob, in one of the small segments; an id that is no blob of
        // the store) then cost a filter's cache line in each segment that does not have
        // the id, not a slot computation and a record read. Built once per segment.
        for (s, _) in segments.iter().skip(1) {
            if s.count() <= FILTER_MAX {
                s.prepare_filter();
            }
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
            Some((packs.get(us(u64::from(r.pack)))?.as_ref()?, r))
        })
    }

    /// Like [`BlobSet::find`], with the segment (for [`BlobSet::pack`]) in place of the pack:
    /// a request can note thousands of blobs without touching their packs' reference counts,
    /// which every core would otherwise contend for.
    #[inline]
    pub fn locate(&self, oid: &Oid) -> Option<(u32, BlobRec)> {
        self.locate_ahead(oid, Ahead::Unknown)
    }

    /// [`BlobSet::locate`], given what [`BlobSet::preload`] learned of `oid`: its slot in
    /// the biggest segment is not computed again.
    #[inline]
    pub fn locate_ahead(&self, oid: &Oid, ahead: Ahead) -> Option<(u32, BlobRec)> {
        self.segments
            .iter()
            .enumerate()
            .find_map(|(i, (s, packs))| {
                let r = match (i, ahead) {
                    (0, Ahead::Slot(slot)) => s.find_at(oid, slot?)?,
                    _ => s.find(oid)?,
                };
                packs.get(us(u64::from(r.pack)))?.as_ref()?;
                Some((u32::try_from(i).ok()?, r))
            })
    }

    /// Starts loading what [`BlobSet::locate`] reads for `oid` in the biggest segment;
    /// returns its slot there, for [`BlobSet::locate_ahead`].
    #[inline]
    pub fn preload(&self, oid: &Oid) -> Ahead {
        let Some((s, _)) = self.segments.first() else {
            return Ahead::Unknown;
        };
        let slot = s.slot(oid);
        if let Some(slot) = slot {
            s.preload_rec(slot);
        }
        Ahead::Slot(slot)
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

    /// The segments' indexes, biggest first.
    pub fn indexes(&self) -> impl Iterator<Item = &Segment> {
        self.segments.iter().map(|(s, _)| &**s)
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

/// A proxied pack of at most this many bytes is kept in memory and never becomes a file.
/// Ranges this short are copied into the response, not sent with `sendfile(2)`, so a file
/// gives the edge nothing, while its descriptor and its mapping are what the cache has
/// fewest of: with a file each, single-blob requests for more blobs than
/// `--proxy-cache-packs` evicted one another, and every one became a fetch from the
/// upstream (9 ms each from a local one, where a cached blob takes 70 µs).
const HEAP_MAX: usize = 16 << 10;
/// What the packs kept in memory may add up to (within `--proxy-cache-mib`), and their
/// number: beside its bytes each costs a few hundred bytes of bookkeeping.
const HEAP_BYTES: u64 = 64 << 20;
const HEAP_PACKS: usize = 1 << 16;

/// Cached packs of one kind, oldest first (eviction order), with their blobs.
#[derive(Default)]
struct Kept {
    order: VecDeque<(Arc<BlobPack>, Arc<[Oid]>)>,
    bytes: u64,
}

struct ProxyState {
    cache: HashMap<Oid, (Arc<BlobPack>, BlobRec)>,
    /// Fetched packs: those in files, which the byte budget and `--proxy-cache-packs`
    /// bound, and the small ones in memory, which have bounds of their own.
    files: Kept,
    heap: Kept,
    /// The blobs being fetched, each with the fetch that will bring it.
    fetching: HashMap<Oid, Arc<Attempt>>,
}

/// Takes `pack`, no longer kept, out of `cache`: its blobs' entries (an id a newer pack
/// holds too keeps that entry).
fn forget(cache: &mut HashMap<Oid, (Arc<BlobPack>, BlobRec)>, pack: &Arc<BlobPack>, ids: &[Oid]) {
    for id in ids {
        if cache.get(id).is_some_and(|(p, _)| Arc::ptr_eq(p, pack)) {
            cache.remove(id);
        }
    }
}

impl ProxyState {
    fn new() -> ProxyState {
        ProxyState {
            cache: HashMap::new(),
            files: Kept::default(),
            heap: Kept::default(),
            fetching: HashMap::new(),
        }
    }

    /// Caches the blobs of `pack`, just fetched, then evicts the oldest packs of its kind
    /// beyond their bounds (files: `budget` bytes and `max_packs` packs) and returns them
    /// (the caller drops them outside the lock: the last reference to one unlinks its
    /// file). The pack just fetched always stays, since the requests that waited for its
    /// blobs look for them here when they wake: the cache can exceed its bounds by one
    /// fetch. An id a newer pack holds too keeps that entry.
    fn keep(
        &mut self,
        pack: &Arc<BlobPack>,
        recs: &[BlobRec],
        budget: u64,
        max_packs: usize,
    ) -> Vec<Arc<BlobPack>> {
        let ProxyState {
            cache, files, heap, ..
        } = self;
        for r in recs {
            cache.insert(r.oid, (Arc::clone(pack), *r));
        }
        let (kept, budget, max_packs) = match pack.file() {
            Some(_) => (files, budget, max_packs),
            None => (heap, budget.min(HEAP_BYTES), HEAP_PACKS),
        };
        kept.bytes = kept.bytes.saturating_add(pack.len);
        kept.order
            .push_back((Arc::clone(pack), recs.iter().map(|r| r.oid).collect()));
        let mut evicted = Vec::new();
        while (kept.bytes > budget || kept.order.len() > max_packs) && kept.order.len() > 1 {
            let Some((old, ids)) = kept.order.pop_front() else {
                break;
            };
            kept.bytes = kept.bytes.saturating_sub(old.len);
            forget(cache, &old, &ids);
            evicted.push(old);
        }
        evicted
    }

    /// Lets go of the kept packs `stored` picks. Returns how many went.
    fn release(&mut self, stored: &HashSet<*const BlobPack>) -> usize {
        let ProxyState {
            cache, files, heap, ..
        } = self;
        let mut released = 0usize;
        for kept in [files, heap] {
            let mut order = std::mem::take(&mut kept.order);
            order.retain(|(pack, ids)| {
                let stays = !stored.contains(&Arc::as_ptr(pack));
                if !stays {
                    kept.bytes = kept.bytes.saturating_sub(pack.len);
                    forget(cache, pack, ids);
                    released = released.saturating_add(1);
                }
                stays
            });
            kept.order = order;
        }
        released
    }

    /// Every kept pack with its blobs.
    fn kept(&self) -> impl Iterator<Item = &(Arc<BlobPack>, Arc<[Oid]>)> {
        self.files.order.iter().chain(&self.heap.order)
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
    /// Most packs cached as files: each holds a descriptor and a mapping.
    max_packs: usize,
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
    /// Fetched blobs are kept (one pack per fetch) up to `budget` bytes: packs of at most
    /// [`HEAP_MAX`] bytes in memory, the others as files under `dir` (emptied now), at
    /// most `max_packs` of them.
    pub fn new(
        upstream: &str,
        auth: &Auth,
        dir: &Path,
        budget: u64,
        max_packs: usize,
    ) -> Result<Proxy> {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).ctx(&format!("creating {}", dir.display()))?;
        Ok(Proxy {
            upstream: upstream.to_string(),
            auth: auth.clone(),
            dir: dir.to_path_buf(),
            budget,
            max_packs,
            idle: Mutex::new(Vec::new()),
            state: Mutex::new(ProxyState::new()),
            done: Notify::new(),
        })
    }

    /// A blob fetched earlier (no allocation).
    #[inline]
    pub fn cached(&self, oid: &Oid) -> Option<(Arc<BlobPack>, BlobRec)> {
        lock(&self.state).cache.get(oid).cloned()
    }

    /// The packs kept, those of them kept as files (a descriptor and a mapping each), and
    /// the bytes of them all (`/metrics`).
    pub fn kept(&self) -> (usize, usize, u64) {
        let st = lock(&self.state);
        let files = st.files.order.len();
        (
            files.saturating_add(st.heap.order.len()),
            files,
            st.files.bytes.saturating_add(st.heap.bytes),
        )
    }

    /// Lets go of the cached packs whose blobs are all in `set`, the store's blobs as just
    /// loaded: the pipeline has copied what a window sealed, and requests look in the store
    /// first. Returns how many went. (They used to stay, a descriptor and a mapping each,
    /// until the byte budget pushed them out.) A request on an older view that still
    /// misses one in its store fetches it again.
    pub fn release_stored(&self, set: &BlobSet) -> usize {
        // Looked up outside the lock, which a request takes for every cached blob it reads.
        let packs: Vec<(Arc<BlobPack>, Arc<[Oid]>)> = lock(&self.state).kept().cloned().collect();
        let stored: HashSet<*const BlobPack> = packs
            .iter()
            .filter(|(_, ids)| ids.iter().all(|o| set.find(o).is_some()))
            .map(|(p, _)| Arc::as_ptr(p))
            .collect();
        if stored.is_empty() {
            return 0;
        }
        let released = lock(&self.state).release(&stored);
        // Dropped outside the lock: the last reference to a pack unlinks its file.
        drop(packs);
        released
    }

    /// The blobs `oids` (distinct): from the cache, else from the upstream in one request
    /// for all of those not being fetched already (others wait for those fetches, and fail
    /// with one that fails). Cold path. What the call has in hand it keeps (an `Arc` keeps a
    /// pack open, cached or not), so the cache evicting one pack for another never makes it
    /// start over.
    pub async fn fetch(&self, oids: &[Oid]) -> Result<Vec<(Arc<BlobPack>, BlobRec)>> {
        let mut found: HashMap<Oid, (Arc<BlobPack>, BlobRec)> = HashMap::with_capacity(oids.len());
        // The fetches this request waited for last, by blob.
        let mut waited: HashMap<Oid, Arc<Attempt>> = HashMap::new();
        loop {
            let (mine, attempt): (Vec<Oid>, Arc<Attempt>) = loop {
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
                    let (mut mine, mut waiting) = (Vec::new(), HashMap::new());
                    for o in oids.iter().filter(|o| !found.contains_key(o)) {
                        match st.fetching.get(o) {
                            Some(a) => {
                                waiting.insert(*o, Arc::clone(a));
                            }
                            // Neither cached nor being fetched: the fetch this request
                            // waited for failed (so does the request), or was dropped.
                            None => match waited.get(o).and_then(|a| a.failure()) {
                                Some(e) => return Err(e),
                                None => mine.push(*o),
                            },
                        }
                    }
                    if mine.is_empty() && waiting.is_empty() {
                        return oids
                            .iter()
                            .map(|o| found.get(o).cloned().ctx("proxied blob"))
                            .collect();
                    }
                    if !mine.is_empty() {
                        let attempt = Arc::new(Attempt::default());
                        for o in &mine {
                            st.fetching.insert(*o, Arc::clone(&attempt));
                        }
                        break (mine, attempt);
                    }
                    waited = waiting;
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
            match &got {
                Ok((pack, recs)) => {
                    for r in recs {
                        found.insert(r.oid, (Arc::clone(pack), *r));
                    }
                    evicted = st.keep(pack, recs, self.budget, self.max_packs);
                }
                // Those waiting for these blobs fail with this request.
                Err(e) => attempt.fail(e),
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
        let (pack, recs) = fetch_into(&mut up, oids, &path, &id).await?;
        self.reuse(up);
        crate::log!(
            "serve: proxied {} blobs ({}) from the upstream in {:.2}s",
            recs.len(),
            crate::util::human_bytes(pack.len),
            t0.elapsed().as_secs_f64()
        );
        Ok((Arc::new(pack), recs))
    }
}

/// A proxied pack as it arrives: in memory while it is small, in a file at `path` from
/// [`HEAP_MAX`] bytes on. The file is a cache's (never synced), and goes if the pack does
/// not become whole: a fetch that failed, or one whose request went away mid-flight.
struct ProxySpool {
    path: PathBuf,
    mem: Vec<u8>,
    file: Option<FileSpool>,
}

impl ProxySpool {
    fn new(path: &Path) -> ProxySpool {
        ProxySpool {
            path: path.to_path_buf(),
            mem: Vec::new(),
            file: None,
        }
    }

    /// The pack, whole: in memory, or mapped from its file, which goes with its last user.
    fn pack(mut self, id: &str) -> Result<BlobPack> {
        let Some(file) = self.file.take() else {
            return Ok(BlobPack::heap(std::mem::take(&mut self.mem), id));
        };
        let opened = file.end().and_then(|_| BlobPack::open(&self.path, id));
        match opened {
            Ok(mut pack) => {
                pack.unlink = Some(std::mem::take(&mut self.path));
                Ok(pack)
            }
            Err(e) => {
                let _ = std::fs::remove_file(&self.path);
                Err(e)
            }
        }
    }
}

impl std::io::Write for ProxySpool {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.file.is_none() && self.mem.len().saturating_add(buf.len()) > HEAP_MAX {
            let mut file = FileSpool::create(&self.path).map_err(std::io::Error::other)?;
            // Kept before it is written to: a file that fails halfway still goes.
            let mem = std::mem::take(&mut self.mem);
            let written = file.write_all(&mem);
            self.file = Some(file);
            written?;
        }
        match &mut self.file {
            Some(file) => file.write(buf),
            None => {
                self.mem.extend_from_slice(buf);
                Ok(buf.len())
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.file {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

impl crate::blobstream::Spool for ProxySpool {
    fn read_back(&mut self, at: u64, buf: &mut [u8]) -> Result<()> {
        match &mut self.file {
            Some(file) => file.read_back(at, buf),
            None => self.mem.read_back(at, buf),
        }
    }
}

impl Drop for ProxySpool {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// What [`fetch_into`]'s network side hands its blocking side.
enum Piece {
    Data(bytes::Bytes),
    /// The answer whose pieces follow is one object, loose, not a pack.
    Loose,
    /// One request's answer has ended (the sink checks it before the next).
    EndPack,
    /// Every answer has arrived.
    Done,
}

/// Hands an answer's pieces, as they arrived, to [`fetch_into`]'s blocking side (waiting
/// while its queue is full).
struct ToSink<'a>(&'a tokio::sync::mpsc::Sender<Piece>);

impl ToSink<'_> {
    async fn send(&mut self, piece: Piece) -> Result<()> {
        self.0
            .send(piece)
            .await
            .map_err(|_| crate::err!("the blob sink stopped"))
    }
}

impl crate::upstream::PackPieces for ToSink<'_> {
    async fn put(&mut self, piece: bytes::Bytes) -> Result<()> {
        self.send(Piece::Data(piece)).await
    }

    async fn loose(&mut self) -> Result<()> {
        self.send(Piece::Loose).await
    }
}

/// Fetches the blobs `oids` from `up` into a blob pack (id `id`), each verified by id as
/// it streams in: a pack in memory if it is small, else a file at `path`
/// ([`ProxySpool`]). The network side is async (the request's task waits, not a
/// thread) and hands on what arrives as it is, a pack's pieces or a loose object's;
/// verifying (inflating, hashing, deflating what came as a delta or loose) and writing
/// run on tokio's blocking pool, fed through a bounded queue, so the runtime's workers
/// keep serving.
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
        let mut sink = BlobSink::new(ProxySpool::new(&path), &want).map_err(|e| (true, e))?;
        loop {
            match rx.blocking_recv() {
                Some(Piece::Data(b)) => {
                    if let Err(e) = sink.write_all(&b) {
                        let e = sink.failure().unwrap_or_else(|| e.into());
                        return Err((true, e));
                    }
                }
                Some(Piece::Loose) => sink.loose().map_err(|e| (true, e))?,
                Some(Piece::EndPack) => sink.end_input().map_err(|e| (true, e))?,
                Some(Piece::Done) => break,
                None => return Err((false, crate::err!("the fetch stopped"))),
            }
        }
        let f = sink.finish().map_err(|e| (true, e))?;
        let pack = f.out.pack(&id).map_err(|e| (true, e))?;
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
    /// pack's file goes with its last user; the newest pack stays, whatever its size; and
    /// the packs kept as files are bounded in number as well as in bytes. Packs in memory
    /// are not among them: they neither count against those bounds nor go for them.
    #[test]
    fn proxy_eviction_keeps_newer_entries() {
        let dir = std::env::temp_dir().join(format!("gvd-proxy-evict-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut st = ProxyState::new();
        let (p1, r1) = proxied(&dir, "p1", 100, &[1, 2]);
        let (p2, r2) = proxied(&dir, "p2", 100, &[2, 3]);
        assert!(st.keep(&p1, &r1, 150, 8).is_empty());
        let evicted = st.keep(&p2, &r2, 150, 8);
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
        assert_eq!(st.files.bytes, 100);
        let p1_file = dir.join("p1.pack");
        drop((evicted, p1, r1));
        assert!(!p1_file.exists(), "unlinked with its last user");
        // Larger than the whole budget: it stays until a newer one arrives.
        let (p3, r3) = proxied(&dir, "p3", 400, &[4]);
        assert_eq!(st.keep(&p3, &r3, 150, 8).len(), 1);
        assert_eq!(at(&st, 4).as_deref(), Some("p3"));
        assert_eq!((st.files.order.len(), st.files.bytes), (1, 400));
        // Packs in memory, more of them than files may be kept, and older than those.
        let in_memory = |i: u8| {
            let (_, recs) = proxied(&dir, &format!("m{i}"), 1, &[i]);
            (
                Arc::new(BlobPack::heap(vec![0u8; 50], &format!("m{i}"))),
                recs,
            )
        };
        for i in 20..30u8 {
            let (p, r) = in_memory(i);
            assert!(st.keep(&p, &r, 1 << 20, 3).is_empty());
        }
        assert_eq!((st.heap.order.len(), st.heap.bytes), (10, 500));
        // Within its bytes, the cache is bounded in files: the oldest go first, and the
        // newest stays even where none may.
        let mut evicted = st.keep(&p2, &r2, 1 << 20, 8);
        for i in 5..9u8 {
            let (p, r) = proxied(&dir, &format!("q{i}"), 10, &[i]);
            evicted.extend(st.keep(&p, &r, 1 << 20, 3));
        }
        let gone: Vec<&str> = evicted.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(gone, ["p3", "p2", "q5"]);
        assert_eq!((at(&st, 4), at(&st, 2), at(&st, 5)), (None, None, None));
        assert_eq!((st.files.order.len(), st.files.bytes), (3, 30));
        let (p9, r9) = proxied(&dir, "q9", 10, &[9]);
        assert_eq!(st.keep(&p9, &r9, 1 << 20, 0).len(), 3);
        assert_eq!(at(&st, 9).as_deref(), Some("q9"));
        assert_eq!((st.files.order.len(), st.files.bytes), (1, 10));
        assert_eq!((st.heap.order.len(), st.cache.len()), (10, 11));
        // Their own bound is in bytes too: the budget, where it is the smaller.
        let (p, r) = in_memory(30);
        let evicted = st.keep(&p, &r, 120, 3);
        let gone: Vec<&str> = evicted.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(gone.len(), 9, "{gone:?}");
        assert_eq!((st.heap.order.len(), st.heap.bytes), (2, 100));
        assert_eq!(at(&st, 9).as_deref(), Some("q9"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A proxied pack is in memory while it is small and a file from then on. The file
    /// goes with the pack's last user, or at once if the pack does not become whole: its
    /// fetch failed, or the request that made it went away. (A fetch abandoned mid-flight
    /// left its file, which nothing removed until the edge started again.)
    #[test]
    fn a_proxied_pack_is_a_file_only_once_it_is_large() {
        use crate::blobstream::Spool;
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!("gvd-proxy-spool-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("p.pack");
        let mut back = [0u8; 4];

        let mut small = ProxySpool::new(&path);
        small.write_all(&[7u8; 100]).unwrap();
        small.read_back(10, &mut back).unwrap();
        assert_eq!(back, [7; 4]);
        let pack = small.pack("p").unwrap();
        assert!(pack.file().is_none() && pack.bytes() == Some(&[7u8; 100][..]));
        assert!(!path.exists());

        let mut large = ProxySpool::new(&path);
        large.write_all(&[1u8; HEAP_MAX]).unwrap();
        assert!(!path.exists(), "still in memory");
        large.write_all(&[2u8; 10]).unwrap();
        assert!(path.exists());
        large.read_back(HEAP_MAX as u64 - 2, &mut back).unwrap();
        assert_eq!(back, [1, 1, 2, 2]);
        let pack = large.pack("p").unwrap();
        assert_eq!(pack.len, HEAP_MAX as u64 + 10);
        assert!(pack.file().is_some() && path.exists());
        drop(pack);
        assert!(!path.exists(), "gone with its last user");

        let mut abandoned = ProxySpool::new(&path);
        abandoned.write_all(&vec![3u8; 2 * HEAP_MAX]).unwrap();
        assert!(path.exists());
        drop(abandoned);
        assert!(
            !path.exists(),
            "a pack that did not become whole leaves no file"
        );
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
        let cache = dir.join("cache");
        let proxy = Proxy::new(spec, &Auth::default(), &cache, 1 << 20, 8).unwrap();
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

    /// A repository of `small` small files and `large` files of 24 KiB of noise (a pack
    /// each of more than [`HEAP_MAX`]) in one commit: its git directory and the blob ids
    /// of the small files, then of the large ones.
    fn repo_of_files(dir: &Path, small: usize, large: usize) -> (PathBuf, Vec<Oid>, Vec<Oid>) {
        let work = dir.join("w");
        std::fs::create_dir_all(&work).unwrap();
        git(dir, &["init", "-q", "-b", "main", work.to_str().unwrap()]);
        for i in 0..small {
            let text = format!("file {i}\n").repeat(50);
            std::fs::write(work.join(format!("f{i:03}.txt")), text).unwrap();
        }
        for i in 0..large {
            let mut x = 0x9E37_79B9_7F4A_7C15u64 + i as u64;
            let noise: Vec<u8> = (0..24 << 10)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect();
            std::fs::write(work.join(format!("g{i:03}.bin")), noise).unwrap();
        }
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
        let mut ids: Vec<Oid> = git(&work, &["ls-tree", "-r", "HEAD"])
            .lines()
            .map(|l| Oid::from_hex(l.split_whitespace().nth(2).unwrap().as_bytes()).unwrap())
            .collect();
        let large = ids.split_off(small);
        (work.join(".git"), ids, large)
    }

    /// Blobs fetched one at a time are a pack each. A small one is kept in memory: it
    /// holds no file, no descriptor and no mapping, so however many there are they do not
    /// push one another out. (With a file each, and the files bounded in number, a client
    /// asking for more blobs than the bound fetched every one from the upstream, every
    /// time.) A larger one is a file with a descriptor and a mapping: the cache keeps
    /// `max_packs` of those, the newest, however far below its byte budget they are.
    /// (Bounded in bytes alone, every fetch kept its file, descriptor and mapping, and so
    /// on up to the descriptor limit, where every fetch fails.)
    #[test]
    fn single_blob_fetches_hold_a_bounded_number_of_descriptors_and_mappings() {
        let dir = std::env::temp_dir().join(format!("gvd-proxy-fds-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (up, small, large) = repo_of_files(&dir, 40, 12);
        let cache = dir.join("proxy");
        let proxy =
            Proxy::new(up.to_str().unwrap(), &Auth::default(), &cache, 10 << 30, 8).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for id in small.iter().chain(&large) {
            rt.block_on(proxy.fetch(std::slice::from_ref(id))).unwrap();
        }
        // Of this cache's files only: other tests open and map files of their own meanwhile.
        let descriptors = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
            .filter(|target| target.starts_with(&cache))
            .count();
        let mappings = std::fs::read_to_string("/proc/self/maps")
            .unwrap()
            .lines()
            .filter(|l| l.contains(cache.to_str().unwrap()))
            .count();
        let files = std::fs::read_dir(&cache).unwrap().count();
        assert_eq!((descriptors, mappings, files), (8, 8, 8));
        assert!(small.iter().all(|o| proxy.cached(o).is_some()));
        let (old, new) = large.split_at(large.len() - 8);
        assert!(old.iter().all(|o| proxy.cached(o).is_none()));
        assert!(new.iter().all(|o| proxy.cached(o).is_some()));
        let st = lock(&proxy.state);
        let kept = (st.heap.order.len(), st.files.order.len(), st.cache.len());
        assert_eq!(kept, (40, 8, 48));
        assert!(
            st.heap.bytes < 40 << 10,
            "{} bytes in memory",
            st.heap.bytes
        );
        assert!(
            st.files.bytes > 8 * (24 << 10),
            "{} in files",
            st.files.bytes
        );
        drop(st);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Requests waiting for a blob whose fetch fails fail with it: one request to the
    /// upstream, one failure, for all of them. (Each used to ask the upstream in its turn,
    /// one after another: 40 requests here, and against a stalled upstream a timeout each.)
    /// The failure is that fetch's alone: the next request asks again.
    #[test]
    fn requests_waiting_for_a_failed_fetch_fail_with_it() {
        use crate::testhttp::{Reply, serve};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = std::env::temp_dir().join(format!("gvd-proxy-waiters-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let asked = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let (seen, gate) = (Arc::clone(&asked), Arc::clone(&release));
        let port = serve(move |r| {
            if r.line.contains("/gvfs/config") {
                return Reply::ok(b"{}");
            }
            // The first answer waits for the test to have its requests waiting.
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                while !gate.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            }
            Reply::Whole("500 Internal Server Error".to_string(), b"no".to_vec())
        });
        let spec = format!("http://127.0.0.1:{port}/r");
        let proxy = Arc::new(Proxy::new(&spec, &Auth::default(), &dir, 1 << 20, 8).unwrap());
        let blob = Oid([0x77; 20]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let errors: Vec<String> = rt.block_on(async {
            let started = Arc::new(AtomicUsize::new(0));
            let mut tasks = Vec::new();
            for _ in 0..40 {
                let (proxy, started) = (Arc::clone(&proxy), Arc::clone(&started));
                tasks.push(tokio::spawn(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let fetched = proxy.fetch(&[blob]).await;
                    fetched.map(|_| ()).unwrap_err().to_string()
                }));
            }
            // One request is fetching and the others wait for it when the upstream answers.
            while started.load(Ordering::SeqCst) < 40 {
                tokio::task::yield_now().await;
            }
            release.store(true, Ordering::SeqCst);
            let mut errors = Vec::new();
            for t in tasks {
                errors.push(t.await.unwrap());
            }
            errors
        });
        assert_eq!(asked.load(Ordering::SeqCst), 1, "{errors:?}");
        assert!(errors.iter().all(|e| e.contains("HTTP 500")), "{errors:?}");
        let waited = |e: &&String| e.contains("the fetch it waited for failed");
        assert_eq!(errors.iter().filter(waited).count(), 39, "{errors:?}");
        assert!(lock(&proxy.state).fetching.is_empty());
        let again = rt.block_on(proxy.fetch(&[blob])).map(|_| ());
        let e = again.unwrap_err().to_string();
        assert!(e.contains("HTTP 500") && !e.contains("waited"), "{e}");
        assert_eq!(asked.load(Ordering::SeqCst), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fetch that is dropped (its client went away) is not a failure: a request waiting
    /// for its blob makes the fetch its own.
    #[test]
    fn a_dropped_fetch_is_taken_over_by_a_request_waiting_for_it() {
        use crate::testhttp::{Reply, serve};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = std::env::temp_dir().join(format!("gvd-proxy-takeover-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let asked = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let (seen, gate) = (Arc::clone(&asked), Arc::clone(&release));
        let port = serve(move |r| {
            if r.line.contains("/gvfs/config") {
                return Reply::ok(b"{}");
            }
            // The first request gets no answer while the test runs.
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                while !gate.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            }
            Reply::Whole("500 Internal Server Error".to_string(), b"no".to_vec())
        });
        let spec = format!("http://127.0.0.1:{port}/r");
        let proxy = Arc::new(Proxy::new(&spec, &Auth::default(), &dir, 1 << 20, 8).unwrap());
        let blob = Oid([0x77; 20]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let e = rt.block_on(async {
            let fetching = Arc::clone(&proxy);
            let fetcher = tokio::spawn(async move { fetching.fetch(&[blob]).await.map(|_| ()) });
            while asked.load(Ordering::SeqCst) < 1 {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            let (waiting, started) = (Arc::clone(&proxy), Arc::new(AtomicBool::new(false)));
            let begun = Arc::clone(&started);
            let waiter = tokio::spawn(async move {
                begun.store(true, Ordering::SeqCst);
                waiting.fetch(&[blob]).await.map(|_| ())
            });
            while !started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            fetcher.abort();
            assert!(fetcher.await.unwrap_err().is_cancelled());
            waiter.await.unwrap().unwrap_err().to_string()
        });
        assert!(e.contains("HTTP 500") && !e.contains("waited"), "{e}");
        assert_eq!(asked.load(Ordering::SeqCst), 2);
        assert!(lock(&proxy.state).fetching.is_empty());
        release.store(true, Ordering::SeqCst);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The CPU seconds the calling thread has used.
    fn thread_cpu() -> f64 {
        use nix::sys::resource::{UsageWho, getrusage};
        use nix::sys::time::TimeValLike;
        let u = getrusage(UsageWho::RUSAGE_THREAD).unwrap();
        (u.user_time().num_microseconds() + u.system_time().num_microseconds()) as f64 / 1e6
    }

    /// `zlib("blob <size>\0" ++ content)`, as an upstream sends a blob loose.
    fn loose_blob(content: &[u8], level: i32) -> (Oid, Vec<u8>) {
        use sha1::{Digest, Sha1};
        let head = format!("blob {}\0", content.len());
        let mut z = vec![0u8; crate::zlib::compress_bound(head.len() + content.len())];
        let n = crate::zlib::Deflater::new(level)
            .deflate_parts(&[head.as_bytes(), content], &mut z)
            .unwrap();
        z.truncate(n);
        let mut h = Sha1::new();
        h.update(head.as_bytes());
        h.update(content);
        (Oid(h.finalize().into()), z)
    }

    fn noise(n: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    /// A blob the proxy asks Azure DevOps for alone arrives loose. The request's task
    /// hands the answer's pieces on as they come; inflating, hashing and writing them is
    /// the blocking pool's work, as for a pack. Here the runtime's one thread is the
    /// test's: fetching 24 MiB of noise costs it the reading of the answer, less CPU than
    /// deflating a sixth of the blob takes. (It inflated all of it and deflated it again at
    /// level 6 there, 15 ms per MiB with the worker held, before the blocking side saw a
    /// byte.) The blob is verified all the same: git indexes its pack; an answer that
    /// is another blob, or that breaks off, fails the fetch and leaves no file.
    #[test]
    fn a_loose_answer_is_not_the_request_tasks_work() {
        use crate::testhttp::{Reply, serve};
        let dir = std::env::temp_dir().join(format!("gvd-proxy-loose-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let content = noise(24 << 20, 5);
        let (oid, loose) = loose_blob(&content, 0);
        let (other, _) = loose_blob(b"another blob\n", 6);
        let (whole, cut_at) = (loose.clone(), loose.len() / 2);
        let port = serve(move |r| {
            if r.line.contains("/gvfs/config") {
                return Reply::ok(b"{}");
            }
            let status = "200 OK\r\nContent-Type: application/x-git-loose-object".to_string();
            // Whichever blob is asked for, the answer is the first one; for one id, half
            // of it.
            let body = String::from_utf8_lossy(&r.body);
            if body.contains(&Oid([0x33; 20]).to_string()) {
                Reply::Cut(status, whole.clone(), cut_at)
            } else {
                Reply::Whole(status, whole.clone())
            }
        });
        let spec = format!("http://127.0.0.1:{port}/r");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut up = AsyncUpstream::open(&spec, &Auth::default()).unwrap();
        let path = dir.join("p.pack");

        // What deflating a sixth of the blob costs this thread, here and now.
        let began = thread_cpu();
        let mut z = vec![0u8; crate::zlib::compress_bound(4 << 20)];
        crate::zlib::Deflater::new(6)
            .deflate_parts(&[&content[..4 << 20]], &mut z)
            .unwrap();
        let a_sixth = thread_cpu() - began;

        let began = thread_cpu();
        let (pack, recs) = rt
            .block_on(fetch_into(&mut up, &[oid], &path, "p"))
            .unwrap();
        let spent = thread_cpu() - began;
        assert!(
            spent < a_sixth,
            "the request's thread used {spent:.3} s of CPU; deflating a sixth of the blob \
             takes {a_sixth:.3} s"
        );
        assert_eq!(recs.len(), 1);
        let rec = recs[0];
        assert_eq!((rec.oid, rec.size), (oid, content.len() as u64));
        assert!(rec.stored, "noise is written as stored blocks");
        let bytes = pack.bytes().unwrap();
        let (at, len) = rec.zlib();
        let mut back = vec![0u8; content.len()];
        crate::zlib::Inflater::new()
            .inflate_exact(&bytes[at as usize..(at + len) as usize], &mut back)
            .unwrap();
        assert!(back == content);
        git(
            &dir,
            &[
                "index-pack",
                "--strict",
                "-o",
                "p.idx",
                path.to_str().unwrap(),
            ],
        );
        drop(pack);
        assert!(!path.exists(), "the pack goes with its last user");

        for (asked, why) in [(other, "which was not asked for"), (Oid([0x33; 20]), "")] {
            let e = rt
                .block_on(fetch_into(&mut up, &[asked], &path, "p"))
                .map(|_| ())
                .unwrap_err()
                .to_string();
            assert!(e.contains(why), "{e}");
            assert!(!path.exists(), "a fetch that failed left its file");
        }
        let _ = std::fs::remove_dir_all(&dir);
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
        // Either bound alone leaves room for one pack.
        for (budget, max_packs) in [(0, 8), (1 << 30, 1)] {
            let proxy = Proxy::new(
                up.to_str().unwrap(),
                &Auth::default(),
                &dir.join("proxy"),
                budget,
                max_packs,
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
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
