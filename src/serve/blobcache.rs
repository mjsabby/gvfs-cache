//! The edge's local cache of blob packs that stay in object storage.
//!
//! A remote pack is read in fixed blocks ([`BLOCK`] bytes, the last one shorter), at most
//! [`WINDOW`] blocks at a time. The blocks a window lacks are fetched together, one ranged
//! GET per run of consecutive missing blocks, and kept on local disk. Blobs are packed in
//! path order, so the blobs of one checkout share blocks: a batch of thousands of small
//! blobs costs a few requests, and a repeated read costs none. A block being fetched is
//! fetched once: other readers wait for that fetch, and fail with it if it fails.
//!
//! A cached block is a file that stays open and mapped ([`Block`]), so a read of cached
//! blocks (a hit) is what a read of a local pack is: a copy from the mapping, or
//! `sendfile(2)` from the file, with no wait and no allocation. A hit does not take the
//! cache's lock: each pack has a slot per block ([`Pack`]), read under a lock of its own,
//! and a read notes its use of a block in the block (a stamp of the cache's clock, which
//! hits only read), not in an order kept for all blocks. A request looks a block up once
//! for the blobs it reads there, and counts its hits itself ([`Reader`]). (A hit used to
//! take the cache's lock twice, move the block in an ordered map under it, open, read
//! and close the block's file by a name formatted for it, and pass the bytes on through
//! a buffer allocated and zeroed for the read: 3.8 µs for 300 bytes. It takes 0.03 µs,
//! of which the copy from the mapping is half.)
//!
//! Blocks are evicted least recently used beyond the byte budget (`--blob-cache-mib`) and
//! beyond a number of blocks (each holds a descriptor and a mapping). Eviction sorts the
//! blocks by stamp when it needs room, and takes them in that order until it is used up:
//! O(log n) a block. A read holds the block it is sending: one evicted meanwhile stays
//! readable, and its file is removed, unmapped and closed with its last reader, so every
//! read finishes whatever the bounds (the blocks open may exceed them by those meanwhile).
//! A read of more than a quarter of the budget (one huge blob) streams straight from the
//! store, a window at a time: keeping it would only evict everything else.
//!
//! Misses are async (the edge's handlers await them): fetches go through the store's async
//! client, and a reader waiting for another's fetch parks its task, not a thread.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tokio::sync::Notify;

use crate::store::{Sink, Store};
use crate::util::{Context, Error, Result, sub, u64_of, us};

/// Bytes per cached block.
pub const BLOCK: u64 = 4 << 20;
/// Most blocks one read fetches at a time.
pub const WINDOW: u64 = 8;

/// A cached block: its file, open and mapped until the cache has evicted it and the last
/// read of it has ended. The file is removed then. It was written once, under another
/// name, before it was opened here, and is never written again: what
/// [`gvfs_mmap::Map::open`] asks of a file.
pub struct Block {
    file: File,
    map: gvfs_mmap::Map,
    /// The cache's clock when it was last read: eviction takes the lowest.
    used: AtomicU64,
    path: PathBuf,
}

impl Block {
    /// `path`, a block of `len` bytes just written, opened and mapped; removed if it
    /// cannot be, or is not that long.
    fn open(path: PathBuf, len: u64) -> Result<Arc<Block>> {
        let opened = File::open(&path).and_then(|file| {
            let map = gvfs_mmap::Map::open(&file, false)?;
            Ok((file, map))
        });
        match opened {
            Ok((file, map)) if u64_of(map.bytes().len()) == len => Ok(Arc::new(Block {
                file,
                map,
                used: AtomicU64::new(0),
                path,
            })),
            Ok(_) => {
                let _ = std::fs::remove_file(&path);
                crate::bail!("a cached block is not the {len} bytes fetched for it")
            }
            Err(e) => {
                let _ = std::fs::remove_file(&path);
                Err(e).ctx("opening a cached block")
            }
        }
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        self.map.bytes()
    }

    /// Its file (to `sendfile(2)` from).
    #[inline]
    pub fn file(&self) -> &File {
        &self.file
    }

    fn len(&self) -> u64 {
        u64_of(self.map.bytes().len())
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        // The last reader of an evicted block may be a request: removing the file (std
        // copies a path of 384 bytes or more to the heap for it) is a cold section there.
        let _cold = gvfs_alloc_guard::cold_section();
        let _ = std::fs::remove_file(&self.path);
    }
}

/// What the cache keeps of one pack: where it is, and, once a block of it has been
/// cached, a slot per block.
pub struct Pack {
    /// Among this cache's packs: with a block's number, the block's key.
    serial: u64,
    /// The pack's id (its block files are `<id>.<block>.<file>`) and its key in the store.
    id: String,
    object: String,
    len: u64,
    /// The cached block, where there is one. A hit reads its block's slot under that
    /// slot's lock, which the readers of other blocks do not touch; a slot is written
    /// under the cache's lock too. Made when the pack's first block is cached, and kept
    /// as long as the pack is listed: a pack that is listed and never read has none.
    /// (Every pack had its slots from when it was listed, 24 bytes a block: 6 MB for
    /// each TiB of packs in the store, 403 MB for 64 TiB, before a byte was read.)
    slots: OnceLock<Box<[Slot]>>,
}

type Slot = RwLock<Option<Arc<Block>>>;

impl Pack {
    /// Block `b`'s slot, if the pack has its slots.
    #[inline]
    fn slot(&self, b: u64) -> Option<&Slot> {
        self.slots.get()?.get(us(b))
    }

    /// Block `b`'s slot, made with the pack's others if there are none yet: called under
    /// the cache's lock, when a block is cached. `None` for a pack of more blocks than
    /// [`PACK_BLOCKS_MAX`], or than memory has room to list.
    fn slot_or_make(&self, b: u64) -> Option<&Slot> {
        if self.slots.get().is_none() {
            let blocks = self.len.div_ceil(BLOCK);
            if blocks > PACK_BLOCKS_MAX {
                return None;
            }
            let blocks = usize::try_from(blocks).ok()?;
            let mut slots = Vec::new();
            slots.try_reserve_exact(blocks).ok()?;
            slots.resize_with(blocks, || RwLock::new(None));
            let _ = self.slots.set(slots.into_boxed_slice());
        }
        self.slot(b)
    }
}

/// Most blocks of a pack that the cache keeps any of: 256 TiB, more than the largest
/// object a store holds (a block blob ends at 190.7 TiB). A pack listed as longer is a
/// listing gone wrong: it is read all the same, and nothing of it is kept.
const PACK_BLOCKS_MAX: u64 = 1 << 26;

/// A pack's serial and a block's number in it.
type Key = (u64, u64);

struct Cached {
    pack: Arc<Pack>,
    block: Arc<Block>,
}

struct State {
    /// The cached blocks, each in its pack's slot too.
    cached: HashMap<Key, Cached>,
    /// The blocks being fetched, each with the fetch that will bring it.
    fetching: HashMap<Key, Arc<Attempt>>,
    /// Blocks in the order eviction takes them, least recently used first, as last sorted:
    /// each with its stamp then. One read since has another stamp, and is passed over.
    queue: VecDeque<(u64, Key)>,
    bytes: u64,
}

/// One fetch that others may be waiting for (a read's missing blocks here; the blob
/// proxy's request to the upstream). A fetch that fails says so here, while it holds the
/// lock under which its marks go, and those who waited for it fail with it. Each used to
/// make the fetch its own instead, one after another: against a stalled origin the last
/// of N waiters got its error after N timeouts. A fetch that is dropped says nothing: one
/// of its waiters takes over.
#[derive(Default)]
pub(super) struct Attempt {
    failed: OnceLock<Error>,
}

impl Attempt {
    pub(super) fn fail(&self, e: &Error) {
        let _ = self.failed.set(Error::with_kind(e.kind, e.to_string()));
    }

    /// The failure, as the error of one who waited for this fetch.
    pub(super) fn failure(&self) -> Option<Error> {
        let e = self.failed.get()?;
        Some(Error::with_kind(
            e.kind,
            format!("the fetch it waited for failed: {e}"),
        ))
    }
}

pub struct BlockCache {
    store: Arc<Store>,
    dir: PathBuf,
    /// The bytes it keeps at most, and the blocks.
    budget: u64,
    max_blocks: usize,
    state: Mutex<State>,
    /// Woken when a fetch ends (blocks cached, or the fetch failed).
    done: Notify,
    /// Moves on when blocks are cached and when eviction sorts them. A read stamps its
    /// block with it, and moves nothing itself.
    clock: AtomicU64,
    /// Reads answered from cached blocks (a longer one counts once per window), and
    /// windows that fetched blocks.
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// Ranged GETs (into the cache, and for reads streamed past it).
    pub fetches: AtomicU64,
    /// Reads too large to keep, streamed straight from the store.
    pub bypassed: AtomicU64,
    /// Generations handed to block files, and serials to packs.
    files: AtomicU64,
    packs: AtomicU64,
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn read<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    match l.read() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn write<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    match l.write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// The blocks of one window of a read (ascending, consecutive), each held from when it
/// is found or fetched until it has been handed on.
struct Window {
    blocks: [Option<Arc<Block>>; WINDOW as usize],
    /// The numbers of its first and last block, and the part of the pack read from them.
    first: u64,
    last: u64,
    from: u64,
    end: u64,
}

impl Window {
    /// The window of the read `[pos, end)` that starts at `pos`: up to [`WINDOW`] blocks
    /// from the one `pos` is in. `None` if the read is empty.
    fn over(pos: u64, end: u64) -> Option<Window> {
        let first = pos.checked_div(BLOCK)?;
        let last = end.checked_sub(1).filter(|&l| l >= pos)?;
        let last = last
            .checked_div(BLOCK)?
            .min(first.saturating_add(WINDOW).saturating_sub(1));
        Some(Window {
            blocks: Default::default(),
            first,
            last,
            from: pos,
            end: end.min(last.saturating_add(1).saturating_mul(BLOCK)),
        })
    }

    /// Each block's place with its number.
    fn places(&mut self) -> impl Iterator<Item = (&mut Option<Arc<Block>>, u64)> {
        self.blocks.iter_mut().zip(self.first..=self.last)
    }
}

/// A request's reads of cached blocks.
///
/// Looking a block up takes its slot's lock and a reference to the block, and counting
/// a hit adds to the cache's count: writes to memory that every worker reading the same
/// block shares. A request therefore keeps the block it read last, so that the blobs it
/// reads there cost one lookup between them (a batch's blobs are mostly neighbours in
/// their packs: a checkout's, in path order), and counts its hits here, added to the
/// cache's when it ends. The block is let go when the request reads another, or ends:
/// a request in flight holds one block, as when it held one block's file open.
pub struct Reader<'a> {
    cache: Option<&'a BlockCache>,
    /// The block read last: its pack's serial, its number, and the block.
    held: Option<(u64, u64, Arc<Block>)>,
    hits: u64,
}

/// Block `b` of `pack` if it is the one `held`, else from `cache` (and held from then
/// on); `None` if it is not cached.
#[inline]
fn find<'h>(
    cache: Option<&BlockCache>,
    held: &'h mut Option<(u64, u64, Arc<Block>)>,
    pack: &Pack,
    b: u64,
) -> Option<&'h Block> {
    if !held
        .as_ref()
        .is_some_and(|(serial, index, _)| *serial == pack.serial && *index == b)
    {
        *held = Some((pack.serial, b, cache?.cached(pack, b)?));
    }
    held.as_ref().map(|(_, _, block)| &**block)
}

/// `len` bytes of `pack` at `at`, if one cached block has them all and a read of that
/// length is not one the cache streams past.
#[inline]
fn within<'h>(
    cache: Option<&BlockCache>,
    held: &'h mut Option<(u64, u64, Arc<Block>)>,
    pack: &Pack,
    at: u64,
    len: u64,
) -> Option<&'h [u8]> {
    if cache.is_none_or(|c| c.bypasses(len)) {
        return None;
    }
    let b = at.checked_div(BLOCK)?;
    let from = at.saturating_sub(b.saturating_mul(BLOCK));
    sub(find(cache, held, pack, b)?.bytes(), us(from), us(len))
}

impl<'a> Reader<'a> {
    /// `cache` is the edge's, if it has one.
    pub fn new(cache: Option<&'a BlockCache>) -> Reader<'a> {
        Reader {
            cache,
            held: None,
            hits: 0,
        }
    }

    /// `len` bytes of `pack` at `at`, if one cached block has them all (and a read of
    /// that length is not one the cache streams past): a hit.
    #[inline]
    pub fn range(&mut self, pack: &Pack, at: u64, len: u64) -> Option<&[u8]> {
        let Reader { cache, held, hits } = self;
        let range = within(*cache, held, pack, at, len)?;
        *hits = hits.saturating_add(1);
        Some(range)
    }

    /// [`Reader::range`], not counted: a few bytes of a read that is counted as a whole.
    #[inline]
    pub fn peek(&mut self, pack: &Pack, at: u64, len: u64) -> Option<&[u8]> {
        within(self.cache, &mut self.held, pack, at, len)
    }

    /// The cached block that holds byte `at` of `pack`, and where `at` is in it.
    #[inline]
    pub fn block(&mut self, pack: &Pack, at: u64) -> Option<(&Block, u64)> {
        let b = at.checked_div(BLOCK)?;
        let block = find(self.cache, &mut self.held, pack, b)?;
        Some((block, at.saturating_sub(b.saturating_mul(BLOCK))))
    }

    /// Runs `f` on the bytes of the cached block that holds byte `at` of `pack`, and on
    /// where `at` is in them, if that block is cached: a look ahead at what the request
    /// reads soon (to start loading it). The block held stays held: another is looked
    /// up for the look and let go.
    #[inline]
    pub fn ahead(&self, pack: &Pack, at: u64, f: impl FnOnce(&[u8], usize)) {
        let Some(b) = at.checked_div(BLOCK) else {
            return;
        };
        let from = us(at.saturating_sub(b.saturating_mul(BLOCK)));
        match &self.held {
            Some((serial, index, block)) if *serial == pack.serial && *index == b => {
                f(block.bytes(), from);
            }
            _ => {
                if let Some(block) = self.cache.and_then(|c| c.cached(pack, b)) {
                    f(block.bytes(), from);
                }
            }
        }
    }

    /// Counts a read of `blocks` blocks got through [`Reader::block`] (all of it if
    /// `whole`, else as far as the cache had them): a hit per window, as
    /// [`BlockCache::read`] counts them.
    pub fn count(&mut self, blocks: u64, whole: bool) {
        let windows = if whole {
            blocks.div_ceil(WINDOW)
        } else {
            blocks.checked_div(WINDOW).unwrap_or(0)
        };
        self.hits = self.hits.saturating_add(windows);
    }
}

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        if let Some(cache) = self.cache
            && self.hits > 0
        {
            cache.hits.fetch_add(self.hits, Ordering::Relaxed);
        }
    }
}

/// Blocks a read is fetching: if the read is dropped before the fetch ends, they are no
/// longer marked, and readers waiting for them are woken (one of them fetches them).
struct Fetching<'a> {
    cache: &'a BlockCache,
    pack: u64,
    blocks: &'a [u64],
    armed: bool,
}

impl Drop for Fetching<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut st = lock(&self.cache.state);
            for &b in self.blocks {
                st.fetching.remove(&(self.pack, b));
            }
            drop(st);
            self.cache.done.notify_waiters();
        }
    }
}

/// A block as it arrives: a file under a temporary name, which goes unless the block
/// becomes whole (the fetch failed, or the read that made it was dropped).
struct Arriving {
    block: u64,
    file: File,
    tmp: PathBuf,
    path: PathBuf,
}

impl Arriving {
    /// The block, whole (`len` bytes): under its own name, open and mapped.
    fn whole(mut self, len: u64) -> Result<Arc<Block>> {
        // No fsync: the cache starts empty after a restart anyway.
        std::fs::rename(&self.tmp, &self.path).ctx("caching a block")?;
        self.tmp = PathBuf::new();
        Block::open(std::mem::take(&mut self.path), len)
    }
}

impl Drop for Arriving {
    fn drop(&mut self) {
        if !self.tmp.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

impl BlockCache {
    /// A cache under `dir` (emptied now) holding up to `budget` bytes in up to
    /// `max_blocks` blocks: each is a file with a descriptor and a mapping of its own.
    pub fn new(
        store: Arc<Store>,
        dir: PathBuf,
        budget: u64,
        max_blocks: usize,
    ) -> Result<BlockCache> {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ctx(&format!("creating {}", dir.display()))?;
        Ok(BlockCache {
            store,
            dir,
            // What that many blocks hold at most: reads too long for it stream past.
            budget: budget.min(u64_of(max_blocks).saturating_mul(BLOCK)),
            max_blocks,
            state: Mutex::new(State {
                cached: HashMap::new(),
                fetching: HashMap::new(),
                queue: VecDeque::new(),
                bytes: 0,
            }),
            done: Notify::new(),
            clock: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetches: AtomicU64::new(0),
            bypassed: AtomicU64::new(0),
            files: AtomicU64::new(0),
            packs: AtomicU64::new(0),
        })
    }

    /// What the cache keeps of the pack `id` of `len` bytes: nothing yet, and nothing
    /// that grows with `len` until a block of it is cached.
    pub fn pack(&self, id: &str, len: u64) -> Arc<Pack> {
        Arc::new(Pack {
            serial: self.packs.fetch_add(1, Ordering::Relaxed),
            id: id.to_string(),
            object: crate::blobs::pack_key(id),
            len,
            slots: OnceLock::new(),
        })
    }

    /// The file of generation `file` of block `b` of `pack`: a block fetched again after
    /// it was evicted is another file, so one never takes the name of one still read.
    fn path(&self, pack: &Pack, b: u64, file: u64) -> PathBuf {
        self.dir.join(format!("{}.{b}.{file}", pack.id))
    }

    /// Whether a read of `len` bytes streams straight from the store.
    #[inline]
    pub fn bypasses(&self, len: u64) -> bool {
        len > self.budget.checked_div(4).unwrap_or(0)
    }

    /// Block `b` of `pack` if it is cached, noted as read now. No wait and no allocation;
    /// the only lock is the block's slot's.
    #[inline]
    pub fn cached(&self, pack: &Pack, b: u64) -> Option<Arc<Block>> {
        let block = read(pack.slot(b)?).clone()?;
        // Written once per tick of the clock: most hits leave the block's memory as it is.
        let now = self.clock.load(Ordering::Relaxed);
        if block.used.load(Ordering::Relaxed) != now {
            block.used.store(now, Ordering::Relaxed);
        }
        Some(block)
    }

    /// `len` bytes of `pack` from `at`, handed to `sink` in pieces: the cold path of a
    /// read, which fetches what is not cached (handlers send what is cached themselves,
    /// through a [`Reader`]). Callers pass a whole range, or all that is left of one: its
    /// length decides whether it streams past the cache, and its blocks are fetched a
    /// window at a time.
    pub async fn read<S: Sink>(
        &self,
        pack: &Arc<Pack>,
        at: u64,
        len: u64,
        sink: &mut S,
    ) -> Result<()> {
        let Some(end) = at.checked_add(len).filter(|&e| e <= pack.len) else {
            crate::bail!("read past the end of blob pack {}", pack.id);
        };
        if len == 0 {
            return Ok(());
        }
        if self.bypasses(len) {
            self.bypassed.fetch_add(1, Ordering::Relaxed);
            let window = WINDOW.saturating_mul(BLOCK);
            let mut pos = at;
            while pos < end {
                let n = end.saturating_sub(pos).min(window);
                self.fetches.fetch_add(1, Ordering::Relaxed);
                self.store
                    .read_range_async(&pack.object, pos, n, sink)
                    .await?;
                pos = pos.saturating_add(n);
            }
            return Ok(());
        }
        let mut pos = at;
        while pos < end {
            let mut window = self.hold(pack, pos, end).await?;
            let (from, upto) = (window.from, window.end);
            for (place, b) in window.places() {
                // Let go of once it is sent: a block the cache has evicted meanwhile
                // then goes with this read's use of it, not with its whole window's.
                let Some(block) = place.take() else { continue };
                let start = b.saturating_mul(BLOCK);
                let lo = from.max(start).saturating_sub(start);
                let hi = upto.min(start.saturating_add(BLOCK)).saturating_sub(start);
                let piece = sub(block.bytes(), us(lo), us(hi.saturating_sub(lo)));
                sink.put(piece.ctx("a cached block's range")?).await?;
            }
            pos = upto;
        }
        Ok(())
    }

    /// The blocks of the window of the read `[pos, end)` that starts at `pos`, held:
    /// fetching those not cached (or waiting for another reader's fetch of them, and
    /// failing with it if it fails).
    async fn hold(&self, pack: &Arc<Pack>, pos: u64, end: u64) -> Result<Window> {
        let mut window = Window::over(pos, end).ctx("an empty read")?;
        let mut fetched = false;
        // The fetches this read waited for last, by block.
        let mut waited: HashMap<u64, Arc<Attempt>> = HashMap::new();
        loop {
            let (mine, attempt): (Vec<u64>, Arc<Attempt>) = loop {
                // Waiting is registered before the state is read: a fetch that ends in
                // between still wakes this reader.
                let mut ended = std::pin::pin!(self.done.notified());
                ended.as_mut().enable();
                {
                    let mut st = lock(&self.state);
                    let (mut missing, mut waiting) = (Vec::new(), HashMap::new());
                    for (place, b) in window.places() {
                        if place.is_some() {
                            continue;
                        }
                        if let Some(block) = self.cached(pack, b) {
                            *place = Some(block);
                        } else if let Some(a) = st.fetching.get(&(pack.serial, b)) {
                            waiting.insert(b, Arc::clone(a));
                        } else if let Some(e) = waited.get(&b).and_then(|a| a.failure()) {
                            // Neither cached nor being fetched, and the fetch this read
                            // waited for failed: so does the read.
                            return Err(e);
                        } else {
                            missing.push(b);
                        }
                    }
                    if missing.is_empty() && waiting.is_empty() {
                        let c = if fetched { &self.misses } else { &self.hits };
                        c.fetch_add(1, Ordering::Relaxed);
                        return Ok(window);
                    }
                    if !missing.is_empty() {
                        let attempt = Arc::new(Attempt::default());
                        for &b in &missing {
                            st.fetching.insert((pack.serial, b), Arc::clone(&attempt));
                        }
                        break (missing, attempt);
                    }
                    waited = waiting;
                }
                ended.await;
            };
            fetched = true;
            // Should this read be dropped mid-fetch, the blocks stop being "fetching".
            let mut guard = Fetching {
                cache: self,
                pack: pack.serial,
                blocks: &mine,
                armed: true,
            };
            let (got, outcome) = self.fetch(pack, &mine).await;
            guard.armed = false;
            let mut gone = Vec::new();
            {
                let mut st = lock(&self.state);
                for &b in &mine {
                    st.fetching.remove(&(pack.serial, b));
                }
                // What arrived is cached, also when a later run failed.
                if !got.is_empty() {
                    let now = self.clock.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
                    for (&b, block) in mine.iter().zip(got) {
                        block.used.store(now, Ordering::Relaxed);
                        self.keep(&mut st, pack, b, &block, &mut gone);
                        let at = us(b.saturating_sub(window.first));
                        if let Some(place) = window.blocks.get_mut(at) {
                            *place = Some(block);
                        }
                    }
                    self.evict(&mut st, &mut gone);
                }
                // Those waiting for the blocks that did not arrive fail with this read.
                if let Err(e) = &outcome {
                    attempt.fail(e);
                }
            }
            // Out of the lock: a block's last reference removes, unmaps and closes its file.
            drop(gone);
            self.done.notify_waiters();
            outcome?;
        }
    }

    /// Fetches `blocks` (ascending), one ranged GET per run of consecutive ones. Returns the
    /// blocks fetched (the first of `blocks`, in order), and how the fetch ended. When a
    /// run fails, the blocks of the runs before it are whole and are returned with the
    /// error, for the caller to cache (they used to stay on disk untracked: never read
    /// again, never evicted); no block of the failed run is kept. What is not returned
    /// leaves no file, also when the read that fetches is dropped on the way.
    async fn fetch(&self, pack: &Pack, blocks: &[u64]) -> (Vec<Arc<Block>>, Result<()>) {
        let mut got = Vec::with_capacity(blocks.len());
        let mut i = 0usize;
        while let Some(&b0) = blocks.get(i) {
            let mut j = i.saturating_add(1);
            while blocks
                .get(j)
                .is_some_and(|&b| b == b0.saturating_add(u64_of(j.saturating_sub(i))))
            {
                j = j.saturating_add(1);
            }
            let n = u64_of(j.saturating_sub(i));
            let start = b0.saturating_mul(BLOCK);
            let end = b0.saturating_add(n).saturating_mul(BLOCK).min(pack.len);
            // Blocks are written as the bytes arrive, each opened and mapped when whole.
            let mut cur: Option<Arriving> = None;
            let mut whole: Vec<Arc<Block>> = Vec::new();
            let mut written = 0u64;
            let finish = |cur: &mut Option<Arriving>, whole: &mut Vec<Arc<Block>>| -> Result<()> {
                if let Some(arriving) = cur.take() {
                    let at = arriving.block.saturating_mul(BLOCK);
                    let len = at.saturating_add(BLOCK).min(pack.len).saturating_sub(at);
                    whole.push(arriving.whole(len)?);
                }
                Ok(())
            };
            self.fetches.fetch_add(1, Ordering::Relaxed);
            let run = self
                .store
                .read_range_async(
                    &pack.object,
                    start,
                    end.saturating_sub(start),
                    &mut |mut chunk: &[u8]| {
                        while !chunk.is_empty() {
                            let pos = start.saturating_add(written);
                            let b = pos.checked_div(BLOCK).unwrap_or(0);
                            if cur.as_ref().is_none_or(|c| c.block != b) {
                                finish(&mut cur, &mut whole)?;
                                let file = self.files.fetch_add(1, Ordering::Relaxed);
                                let path = self.path(pack, b, file);
                                let tmp = crate::fsutil::temp_path(&path);
                                cur = Some(Arriving {
                                    block: b,
                                    file: File::create(&tmp).ctx("creating a cache block")?,
                                    tmp,
                                    path,
                                });
                            }
                            let room = us(b
                                .saturating_add(1)
                                .saturating_mul(BLOCK)
                                .saturating_sub(pos));
                            let (now, rest) = chunk.split_at(room.min(chunk.len()));
                            if let Some(arriving) = cur.as_mut() {
                                std::io::Write::write_all(&mut arriving.file, now)
                                    .ctx("writing a cache block")?;
                            }
                            written = written.saturating_add(u64_of(now.len()));
                            chunk = rest;
                        }
                        Ok(())
                    },
                )
                .await
                .and_then(|()| finish(&mut cur, &mut whole));
            if let Err(e) = run {
                return (got, Err(e));
            }
            got.append(&mut whole);
            i = j;
        }
        (got, Ok(()))
    }

    /// Caches `block` as block `b` of `pack`; what it replaces goes to `gone`. The pack
    /// gets its slots here if this is its first block. A pack that can have none keeps
    /// no block: the read that fetched this one has it, and the next fetches again.
    fn keep(
        &self,
        st: &mut State,
        pack: &Arc<Pack>,
        b: u64,
        block: &Arc<Block>,
        gone: &mut Vec<Arc<Block>>,
    ) {
        let Some(slot) = pack.slot_or_make(b) else {
            return;
        };
        let cached = Cached {
            pack: Arc::clone(pack),
            block: Arc::clone(block),
        };
        if let Some(old) = st.cached.insert((pack.serial, b), cached) {
            st.bytes = st.bytes.saturating_sub(old.block.len());
            gone.push(old.block);
        }
        st.bytes = st.bytes.saturating_add(block.len());
        gone.extend(write(slot).replace(Arc::clone(block)));
    }

    /// Least recently used blocks leave the cache until it is within its bounds. They go
    /// to `gone`, to be dropped once the lock is let go (a read that holds one keeps it).
    fn evict(&self, st: &mut State, gone: &mut Vec<Arc<Block>>) {
        // Whether the order is one made in this call.
        let mut fresh = false;
        while st.bytes > self.budget || st.cached.len() > self.max_blocks {
            let Some((stamp, key)) = st.queue.pop_front() else {
                // The order is used up: every block by its stamp, and a tick, so that a
                // read from now on gives its block another stamp than the order has.
                let mut order: Vec<(u64, Key)> = st
                    .cached
                    .iter()
                    .map(|(key, c)| (c.block.used.load(Ordering::Relaxed), *key))
                    .collect();
                if order.is_empty() {
                    break;
                }
                order.sort_unstable();
                st.queue = order.into();
                self.clock.fetch_add(1, Ordering::Relaxed);
                fresh = true;
                continue;
            };
            // Gone since the order was made, or read since: no longer the least recently
            // used. (In an order just made, a read in the instant since changes nothing.)
            let read_since = |c: &Cached| c.block.used.load(Ordering::Relaxed) != stamp;
            if st.cached.get(&key).is_none_or(|c| !fresh && read_since(c)) {
                continue;
            }
            if let Some(c) = st.cached.remove(&key) {
                st.bytes = st.bytes.saturating_sub(c.block.len());
                if let Some(slot) = c.pack.slot(key.1) {
                    let mut slot = write(slot);
                    if slot.as_ref().is_some_and(|b| Arc::ptr_eq(b, &c.block)) {
                        gone.extend(slot.take());
                    }
                }
                gone.push(c.block);
            }
        }
    }

    /// The blocks cached now (a descriptor and a mapping each), and their bytes.
    pub fn kept(&self) -> (usize, u64) {
        let st = lock(&self.state);
        (st.cached.len(), st.bytes)
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

    #[test]
    fn reads_fetch_runs_once_then_hit() {
        let (dir, cache, pack, data) = cache_over("runs", BLOCK * 64);
        let n = data.len() as u64;
        // Blocks 0..=2 in one request; then hits.
        read_all(&cache, &pack, &data, 10, BLOCK * 2 + 5);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 1);
        read_all(&cache, &pack, &data, BLOCK - 1, 2);
        read_all(&cache, &pack, &data, 0, BLOCK * 3);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 1);
        // The short last block.
        read_all(&cache, &pack, &data, n - 100, 100);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 2);
        assert_eq!(cache.misses.load(Ordering::Relaxed), 2);
        assert_eq!(cache.hits.load(Ordering::Relaxed), 2);
        assert!(
            rt().block_on(cache.read(&pack, n - 1, 2, &mut |_: &[u8]| Ok(())))
                .is_err()
        );
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn least_recently_used_blocks_go_first() {
        let (dir, cache, pack, data) = cache_over("lru", BLOCK * 4);
        for b in 0..4 {
            read_all(&cache, &pack, &data, b * BLOCK, BLOCK);
        }
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 4);
        read_all(&cache, &pack, &data, 4 * BLOCK, BLOCK);
        assert_eq!(cache.kept(), (4, BLOCK * 4), "block 0 went");
        read_all(&cache, &pack, &data, 0, 10);
        assert_eq!(
            cache.fetches.load(Ordering::Relaxed),
            6,
            "block 0 again; block 1 went"
        );
        read_all(&cache, &pack, &data, 2 * BLOCK, 10);
        read_all(&cache, &pack, &data, 3 * BLOCK, 10);
        assert_eq!(
            cache.fetches.load(Ordering::Relaxed),
            6,
            "blocks 2 and 3 stayed"
        );
        read_all(&cache, &pack, &data, BLOCK, 10);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 7);
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Readers racing over a cache smaller than what they read: every byte right.
    #[test]
    fn concurrent_readers_over_a_small_cache() {
        let (dir, cache, pack, data) = cache_over("race", BLOCK * 2);
        let n = data.len() as u64;
        let (cache, data) = (Arc::new(cache), Arc::new(data));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut tasks = Vec::new();
            for t in 0..8u64 {
                let (cache, data) = (Arc::clone(&cache), Arc::clone(&data));
                let pack = Arc::clone(&pack);
                tasks.push(tokio::spawn(async move {
                    let mut x = t * 7919 + 1;
                    for _ in 0..40 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let at = x % n;
                        let len = ((x >> 20) % (BLOCK * 2).min(n - at) + 1).min(n - at);
                        // What the cache has is read as a request reads it, the rest
                        // through the cold path.
                        let mut reader = Reader::new(Some(&*cache));
                        match hit(&mut reader, &pack, at, len) {
                            Some(got) => assert!(got == data[at as usize..(at + len) as usize]),
                            None => read_all_async(&cache, &pack, &data, at, len).await,
                        }
                    }
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }
        });
        assert!(cache.kept().1 <= BLOCK * 2, "within budget");
        assert!(cache.bypassed.load(Ordering::Relaxed) > 0);
        assert!(cache.hits.load(Ordering::Relaxed) > 0);
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Once no read is running: nothing is being fetched, the bytes add up within the
    /// bounds, every cached block is in its pack's slot and held by the cache alone, no
    /// other slot of `pack` holds one, and the directory holds exactly the cached blocks'
    /// files (none leaked, none missing).
    fn consistent(cache: &BlockCache, pack: &Pack) {
        let st = lock(&cache.state);
        assert!(st.fetching.is_empty());
        assert_eq!(
            st.bytes,
            st.cached.values().map(|c| c.block.len()).sum::<u64>()
        );
        assert!(st.bytes <= cache.budget && st.cached.len() <= cache.max_blocks);
        for ((serial, b), c) in &st.cached {
            assert_eq!(*serial, c.pack.serial);
            let slot = read(c.pack.slot(*b).unwrap());
            assert!(slot.as_ref().is_some_and(|s| Arc::ptr_eq(s, &c.block)));
            assert_eq!(Arc::strong_count(&c.block), 2, "block {b}");
        }
        let slots = pack
            .slots
            .get()
            .map_or(0, |s| s.iter().filter(|s| read(s).is_some()).count());
        let of_pack = st.cached.keys().filter(|k| k.0 == pack.serial).count();
        assert_eq!(slots, of_pack);
        let mut want: Vec<PathBuf> = st.cached.values().map(|c| c.block.path.clone()).collect();
        want.sort();
        assert_eq!(files(cache), want);
    }

    /// The files in the cache's directory.
    fn files(cache: &BlockCache) -> Vec<PathBuf> {
        let mut have: Vec<PathBuf> = std::fs::read_dir(&cache.dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        have.sort();
        have
    }

    /// The descriptors and the mappings this process holds of files in the cache's
    /// directory (of this cache only: other tests open and map files of their own).
    fn open_and_mapped(cache: &BlockCache) -> (usize, usize) {
        let descriptors = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
            .filter(|target| target.starts_with(&cache.dir))
            .count();
        let mappings = std::fs::read_to_string("/proc/self/maps")
            .unwrap()
            .lines()
            .filter(|l| l.contains(cache.dir.to_str().unwrap()))
            .count();
        (descriptors, mappings)
    }

    /// A store with one pack `p` of five blocks and 1,234 bytes, and a cache over it.
    fn cache_over(tag: &str, budget: u64) -> (PathBuf, BlockCache, Arc<Pack>, Vec<u8>) {
        cache_of(tag, budget, usize::MAX)
    }

    fn cache_of(
        tag: &str,
        budget: u64,
        max_blocks: usize,
    ) -> (PathBuf, BlockCache, Arc<Pack>, Vec<u8>) {
        let dir = std::env::temp_dir().join(format!("gvd-bcache-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store =
            Arc::new(Store::open(&format!("fs:{}", dir.join("store").display()), "").unwrap());
        let data: Vec<u8> = (0..(BLOCK * 5 + 1234))
            .map(|i| (i * 7 % 253) as u8)
            .collect();
        let mut w = store.writer("blobs/p.pack").unwrap();
        std::io::Write::write_all(&mut w, &data).unwrap();
        w.finish().unwrap();
        let cache = BlockCache::new(store, dir.join("cache"), budget, max_blocks).unwrap();
        let pack = cache.pack("p", data.len() as u64);
        (dir, cache, pack, data)
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn read_all(cache: &BlockCache, pack: &Arc<Pack>, data: &[u8], at: u64, len: u64) {
        rt().block_on(read_all_async(cache, pack, data, at, len));
    }

    async fn read_all_async(cache: &BlockCache, pack: &Arc<Pack>, data: &[u8], at: u64, len: u64) {
        let mut got = Vec::new();
        cache
            .read(pack, at, len, &mut |b: &[u8]| {
                got.extend_from_slice(b);
                Ok(())
            })
            .await
            .unwrap();
        assert!(got == data[at as usize..(at + len) as usize], "{at}+{len}");
    }

    /// `len` bytes at `at` as a request gets them without waiting, if the cache has them
    /// all: from one block, else block by block (as `send_pack_range` goes).
    fn hit(reader: &mut Reader<'_>, pack: &Pack, at: u64, len: u64) -> Option<Vec<u8>> {
        if let Some(range) = reader.range(pack, at, len) {
            return Some(range.to_vec());
        }
        if reader.cache.is_none_or(|c| c.bypasses(len)) {
            return None;
        }
        let (mut got, mut blocks) = (Vec::new(), 0);
        while (got.len() as u64) < len {
            let pos = at + got.len() as u64;
            let Some((block, from)) = reader.block(pack, pos) else {
                break;
            };
            let from = from as usize;
            let n = (len as usize - got.len()).min(block.bytes().len() - from);
            if n == 0 {
                break;
            }
            got.extend_from_slice(&block.bytes()[from..from + n]);
            blocks += 1;
        }
        let whole = got.len() as u64 == len;
        reader.count(blocks, whole);
        whole.then_some(got)
    }

    /// Readers waiting for a block whose fetch fails fail with it: one ranged GET for all
    /// of them, not one each in turn. The next read asks the store again.
    #[test]
    fn readers_waiting_for_a_failed_fetch_fail_with_it() {
        use crate::testhttp::{Reply, serve};
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let dir = std::env::temp_dir().join(format!("gvd-bcache-waiters-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let asked = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let (seen, gate) = (Arc::clone(&asked), Arc::clone(&release));
        // An object store that has lost the pack; its first answer waits for the test to
        // have its readers waiting.
        let port = serve(move |_| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                while !gate.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            }
            Reply::status("404 Not Found")
        });
        let account =
            format!("BlobEndpoint=http://127.0.0.1:{port}/acct;AccountName=acct;AccountKey=a2V5");
        let store = Store::Azure(crate::azure::AzureStore::new(&account, "packs").unwrap());
        let cache = BlockCache::new(Arc::new(store), dir.clone(), BLOCK * 64, usize::MAX).unwrap();
        let pack = cache.pack("p", BLOCK * 8);
        let cache = Arc::new(cache);
        let read = |cache: Arc<BlockCache>, pack: Arc<Pack>| async move {
            let sink = &mut |_: &[u8]| Ok(());
            let read = cache.read(&pack, 5, 10, sink);
            read.await.unwrap_err().to_string()
        };
        let rt = rt();
        let errors: Vec<String> = rt.block_on(async {
            let started = Arc::new(AtomicUsize::new(0));
            let mut tasks = Vec::new();
            for _ in 0..20 {
                let (cache, started) = (Arc::clone(&cache), Arc::clone(&started));
                let pack = Arc::clone(&pack);
                tasks.push(tokio::spawn(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    read(cache, pack).await
                }));
            }
            while started.load(Ordering::SeqCst) < 20 {
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
        assert!(errors.iter().all(|e| e.contains("HTTP 404")), "{errors:?}");
        let waited = |e: &&String| e.contains("the fetch it waited for failed");
        assert_eq!(errors.iter().filter(waited).count(), 19, "{errors:?}");
        consistent(&cache, &pack);
        let e = rt.block_on(read(Arc::clone(&cache), Arc::clone(&pack)));
        assert!(e.contains("HTTP 404") && !e.contains("waited"), "{e}");
        assert_eq!(asked.load(Ordering::SeqCst), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A read that fetches two runs of blocks, the second of which fails: the first run's
    /// blocks are whole on disk, and are cached. (They were left there untracked: never
    /// read again, never evicted, and written again by every retry, 8 MiB at a time here.)
    /// The failed run leaves nothing, and a retry fetches only that run.
    #[test]
    fn a_failed_run_keeps_the_blocks_of_the_runs_before_it() {
        let (dir, cache, pack, data) = cache_over("runs-fail", BLOCK * 64);
        // Block 2 cached alone: a read of blocks 0..=4 then fetches the runs [0, 1] and
        // [3, 4].
        read_all(&cache, &pack, &data, 2 * BLOCK + 5, 10);
        // The store loses the pack's tail: the second run's ranged read fails.
        let lost = dir.join("store/blobs/p.pack");
        let lost = std::fs::OpenOptions::new().write(true).open(lost).unwrap();
        lost.set_len(3 * BLOCK + 100).unwrap();
        for fetches in [3, 4, 5] {
            let sink = &mut |_: &[u8]| Ok(());
            let read = cache.read(&pack, 0, 5 * BLOCK, sink);
            assert!(rt().block_on(read).is_err());
            assert_eq!(cache.fetches.load(Ordering::Relaxed), fetches);
            assert_eq!(cache.kept(), (3, 3 * BLOCK), "blocks 0, 1 and 2");
            consistent(&cache, &pack);
        }
        read_all(&cache, &pack, &data, 0, 3 * BLOCK);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 5, "all three cached");
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Readers waiting for a fetch of two runs, the second of which fails: the one that
    /// needed a block of the first run reads it from the cache, the one that needed a block
    /// of the second fails with the fetch, and neither asks the store.
    #[test]
    fn readers_waiting_for_the_runs_before_a_failed_one_are_served() {
        use crate::testhttp::{Reply, serve};
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let dir = std::env::temp_dir().join(format!("gvd-bcache-partial-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let asked = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let (seen, gate) = (Arc::clone(&asked), Arc::clone(&release));
        // A pack of three blocks, block `b` all bytes `b`, whose third the store has lost.
        // The answer for block 0 waits for the test to have its readers waiting.
        let port = serve(move |r| {
            seen.fetch_add(1, Ordering::SeqCst);
            let range = r.header("x-ms-range").unwrap().to_string();
            let block =
                (0..3u64).find(|b| range == format!("bytes={}-{}", b * BLOCK, (b + 1) * BLOCK - 1));
            match block.unwrap() {
                0 => {
                    while !gate.load(Ordering::SeqCst) {
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                    Reply::Whole("206 Partial Content".to_string(), vec![0u8; BLOCK as usize])
                }
                1 => Reply::Whole("206 Partial Content".to_string(), vec![1u8; BLOCK as usize]),
                _ => Reply::status("404 Not Found"),
            }
        });
        let account =
            format!("BlobEndpoint=http://127.0.0.1:{port}/acct;AccountName=acct;AccountKey=a2V5");
        let store = Store::Azure(crate::azure::AzureStore::new(&account, "packs").unwrap());
        let cache = BlockCache::new(Arc::new(store), dir.clone(), BLOCK * 64, usize::MAX).unwrap();
        let pack = cache.pack("p", BLOCK * 3);
        let cache = Arc::new(cache);
        // The bytes of blocks `from..=to`, or the error of reading them.
        let read = |cache: Arc<BlockCache>, pack: Arc<Pack>, from: u64, to: u64| async move {
            let mut got = Vec::new();
            let sink = &mut |b: &[u8]| {
                got.extend_from_slice(b);
                Ok(())
            };
            let (at, len) = (from * BLOCK, (to + 1 - from) * BLOCK);
            let read = cache.read(&pack, at, len, sink);
            read.await.map(|()| got).map_err(|e| e.to_string())
        };
        let rt = rt();
        // Block 1 cached: a read of all three then fetches the runs [0] and [2].
        let one = rt.block_on(read(Arc::clone(&cache), Arc::clone(&pack), 1, 1));
        assert!(one.unwrap() == vec![1u8; BLOCK as usize]);
        let (all, first, last) = rt.block_on(async {
            let started = Arc::new(AtomicUsize::new(0));
            let mut tasks = Vec::new();
            for (from, to) in [(0, 2), (0, 0), (2, 2)] {
                let (cache, begun) = (Arc::clone(&cache), Arc::clone(&started));
                let pack = Arc::clone(&pack);
                tasks.push(tokio::spawn(async move {
                    begun.fetch_add(1, Ordering::SeqCst);
                    read(cache, pack, from, to).await
                }));
                // The read of all three is the one fetching.
                while started.load(Ordering::SeqCst) < tasks.len() {
                    tokio::task::yield_now().await;
                }
            }
            release.store(true, Ordering::SeqCst);
            let mut results = Vec::new();
            for t in tasks {
                results.push(t.await.unwrap());
            }
            let last = results.pop().unwrap();
            let first = results.pop().unwrap();
            (results.pop().unwrap(), first, last)
        });
        let (all, last) = (all.unwrap_err(), last.unwrap_err());
        assert!(all.contains("HTTP 404") && !all.contains("waited"), "{all}");
        assert!(first.unwrap() == vec![0u8; BLOCK as usize]);
        assert!(last.contains("the fetch it waited for failed"), "{last}");
        assert_eq!(
            asked.load(Ordering::SeqCst),
            3,
            "block 1, then the two runs"
        );
        assert_eq!(cache.kept(), (2, 2 * BLOCK));
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// One blob larger than the whole cache.
    #[test]
    fn a_read_larger_than_the_budget_completes() {
        let (dir, cache, pack, data) = cache_over("big", BLOCK * 2);
        read_all(&cache, &pack, &data, 0, data.len() as u64);
        assert_eq!(cache.bypassed.load(Ordering::Relaxed), 1);
        assert_eq!(cache.kept(), (0, 0), "streamed past the cache");
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A small read across a block boundary, with room for one block only.
    #[test]
    fn a_read_spanning_more_blocks_than_the_budget_completes() {
        let (dir, cache, pack, data) = cache_over("span", BLOCK);
        read_all(&cache, &pack, &data, BLOCK - 10, 20);
        read_all(&cache, &pack, &data, BLOCK * 3 - 7, 9);
        assert!(cache.kept().1 <= BLOCK);
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What a request reads of cached blocks without waiting ([`Reader`]) are the pack's
    /// bytes: within a block, up to its last byte and from its first, in the pack's short
    /// last block, and block by block for a range that goes on into the next. A range in
    /// a block that is not cached, one past the pack's end and one the cache streams
    /// past are not answered. None of it asks the store, waits (there is no runtime
    /// here) or takes the cache's lock (held here meanwhile), and the hits are counted
    /// when the request ends.
    #[test]
    fn hits_are_the_packs_bytes_within_a_block_across_blocks_and_at_the_end() {
        let (dir, cache, pack, data) = cache_over("hits", BLOCK * 64);
        let n = data.len() as u64;
        let mut reader = Reader::new(Some(&cache));
        assert!(reader.range(&pack, 10, 100).is_none());
        assert!(reader.block(&pack, 10).is_none());
        drop(reader);
        // Blocks 0..=3 and the last one (1,234 bytes); block 4 is not cached.
        read_all(&cache, &pack, &data, 0, 4 * BLOCK);
        read_all(&cache, &pack, &data, n - 1, 1);
        let fetches = cache.fetches.load(Ordering::Relaxed);
        let hits = cache.hits.load(Ordering::Relaxed);
        let request = || {
            let mut reader = Reader::new(Some(&cache));
            let mut counted = 0;
            for (at, len) in [
                (0, 300),
                (BLOCK - 300, 300),
                (BLOCK, 1),
                (3 * BLOCK + 77, 16 << 10),
                (2 * BLOCK, BLOCK),
                (n - 1234, 1234),
                (n - 1, 1),
            ] {
                let got = reader.range(&pack, at, len).unwrap();
                assert!(got == &data[at as usize..(at + len) as usize], "{at}+{len}");
                counted += 1;
            }
            // Into the next block: not one block's, so block by block.
            for (at, len) in [
                (BLOCK - 10, 20),
                (BLOCK - 1, 2),
                (5, 4 * BLOCK - 5),
                (2 * BLOCK + 9, BLOCK),
            ] {
                assert!(reader.range(&pack, at, len).is_none());
                let got = hit(&mut reader, &pack, at, len).unwrap();
                assert!(got == data[at as usize..(at + len) as usize], "{at}+{len}");
                counted += 1;
            }
            // Not cached, in part or at all; past the end; too long for the cache.
            assert!(hit(&mut reader, &pack, 4 * BLOCK - 5, 10).is_none());
            assert!(hit(&mut reader, &pack, 4 * BLOCK + 5, 10).is_none());
            assert!(hit(&mut reader, &pack, n - 10, 11).is_none());
            assert!(reader.block(&pack, 6 * BLOCK).is_none());
            assert!(reader.range(&pack, 0, 16 * BLOCK + 1).is_none());
            // One block is held, the one read last; the others are the cache's alone. A
            // look ahead at another leaves it so.
            assert!(reader.range(&pack, 0, 1).is_some());
            let refs = |b: u64| Arc::strong_count(read(pack.slot(b).unwrap()).as_ref().unwrap());
            let mut looked = 0;
            for at in [7, BLOCK + 7, 4 * BLOCK + 7] {
                reader.ahead(&pack, at, |bytes, from| {
                    assert!(bytes[from..from + 100] == data[at as usize..at as usize + 100]);
                    looked += 1;
                });
            }
            assert_eq!(looked, 2, "block 4 is not cached");
            assert_eq!(
                (refs(0), refs(1), refs(2), refs(3), refs(5)),
                (3, 2, 2, 2, 2)
            );
            assert_eq!(cache.hits.load(Ordering::Relaxed), hits, "not counted yet");
            counted + 1
        };
        let state = lock(&cache.state);
        let counted = std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();
            let reads = s.spawn(move || {
                let counted = request();
                let _ = tx.send(());
                counted
            });
            let unlocked = rx.recv_timeout(std::time::Duration::from_secs(60)).is_ok();
            drop(state);
            assert!(unlocked, "a hit waited for the cache's lock");
            reads.join().unwrap()
        });
        assert_eq!(cache.hits.load(Ordering::Relaxed), hits + counted);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), fetches);
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A block evicted while a request reads it stays readable, its file open and mapped,
    /// until the request lets go of it: the file is removed then, with its descriptor and
    /// its mapping.
    #[test]
    fn a_block_evicted_while_it_is_read_goes_with_its_reader() {
        let (dir, cache, pack, data) = cache_over("held", BLOCK * 2);
        read_all(&cache, &pack, &data, 100, 10);
        read_all(&cache, &pack, &data, BLOCK, 10);
        let mut reader = Reader::new(Some(&cache));
        let bytes = reader.range(&pack, 100, 1 << 20).unwrap();
        let file = read(pack.slot(0).unwrap()).as_ref().unwrap().path.clone();
        // Two more blocks: the cache has room for two.
        read_all(&cache, &pack, &data, 2 * BLOCK, 10);
        read_all(&cache, &pack, &data, 3 * BLOCK, 10);
        assert_eq!(cache.kept(), (2, 2 * BLOCK));
        assert!(read(pack.slot(0).unwrap()).is_none() && read(pack.slot(1).unwrap()).is_none());
        assert!(bytes == &data[100..100 + (1 << 20)], "the bytes stay");
        assert!(file.exists());
        assert_eq!(open_and_mapped(&cache), (3, 3));
        assert_eq!(files(&cache).len(), 3);
        // The request moves on to a block that is cached: the evicted one goes.
        assert!(reader.range(&pack, 2 * BLOCK + 7, 100).is_some());
        assert!(!file.exists());
        assert_eq!(open_and_mapped(&cache), (2, 2));
        drop(reader);
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An edge lists every pack of its store and reads few of them: a pack has no slots
    /// until a block of it is cached. Looking one up makes none; the pack's first cached
    /// block makes them all, hits read them from then on, and they stay when its blocks
    /// are evicted. (Each pack had a slot per block from when it was listed.) A pack
    /// listed as longer than any object is read all the same: its blocks are fetched for
    /// each read, and none is kept.
    #[test]
    fn a_pack_has_its_slots_from_its_first_cached_block() {
        let (dir, cache, pack, data) = cache_over("slots", BLOCK);
        let unread = cache.pack("q", 1 << 40);
        assert!(pack.slots.get().is_none() && unread.slots.get().is_none());
        assert!(cache.cached(&pack, 3).is_none());
        let mut reader = Reader::new(Some(&cache));
        assert!(reader.range(&pack, 100, 50).is_none());
        assert!(reader.block(&pack, 100).is_none());
        drop(reader);
        assert!(pack.slots.get().is_none(), "a look made the slots");

        read_all(&cache, &pack, &data, BLOCK + 5, 100);
        assert_eq!(pack.slots.get().map(|s| s.len()), Some(6));
        assert!(unread.slots.get().is_none());
        let mut reader = Reader::new(Some(&cache));
        assert!(hit(&mut reader, &pack, BLOCK + 5, 100).is_some());
        drop(reader);
        // Another block: the cache has room for one, and the first one's slot is empty.
        read_all(&cache, &pack, &data, 5, 100);
        assert_eq!(pack.slots.get().map(|s| s.len()), Some(6));
        assert!(read(pack.slot(1).unwrap()).is_none() && read(pack.slot(0).unwrap()).is_some());
        consistent(&cache, &pack);

        // The same object, listed as 2^62 bytes long.
        let wrong = cache.pack("p", 1 << 62);
        let fetches = cache.fetches.load(Ordering::Relaxed);
        for _ in 0..3 {
            read_all(&cache, &wrong, &data, 2 * BLOCK + 9, 1000);
        }
        assert_eq!(cache.fetches.load(Ordering::Relaxed), fetches + 3);
        assert!(wrong.slots.get().is_none());
        assert_eq!(cache.kept(), (1, BLOCK));
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The cache keeps no more bytes than its budget and no more blocks than its bound,
    /// whichever is met first: a block is a file, a descriptor and a mapping, a short
    /// one (a pack's last, or all there is of a small pack) as much as a whole one. Bytes
    /// alone bound the files only where blocks are whole: here ten in the bytes of three.
    #[test]
    fn the_cache_is_bounded_in_bytes_and_in_blocks() {
        // Eight more packs of 1,000 bytes each: a block each.
        let small = |cache: &BlockCache| -> Vec<(Arc<Pack>, Vec<u8>)> {
            (0..8u8)
                .map(|i| {
                    let bytes = vec![i; 1000];
                    let mut w = cache.store.writer(&format!("blobs/q{i}.pack")).unwrap();
                    std::io::Write::write_all(&mut w, &bytes).unwrap();
                    w.finish().unwrap();
                    (cache.pack(&format!("q{i}"), 1000), bytes)
                })
                .collect()
        };
        // Room for three blocks' bytes, in any number of blocks.
        let (dir, cache, pack, data) = cache_of("bytes", BLOCK * 3, usize::MAX);
        for b in 0..5 {
            read_all(&cache, &pack, &data, b * BLOCK + 1, 10);
            assert!(cache.kept().1 <= 3 * BLOCK);
        }
        assert_eq!(cache.kept(), (3, 3 * BLOCK));
        let packs = small(&cache);
        for (q, bytes) in &packs {
            read_all(&cache, q, bytes, 0, 1000);
            assert!(cache.kept().1 <= 3 * BLOCK);
        }
        assert_eq!(cache.kept(), (10, 2 * BLOCK + 8000));
        assert_eq!(open_and_mapped(&cache), (10, 10));
        consistent(&cache, &pack);
        drop((cache, packs));
        let _ = std::fs::remove_dir_all(&dir);
        // Room for 64 blocks' bytes, in three blocks at most.
        let (dir, cache, pack, data) = cache_of("blocks", BLOCK * 64, 3);
        let packs = small(&cache);
        for (q, bytes) in &packs {
            read_all(&cache, q, bytes, 0, 1000);
            assert!(cache.kept().0 <= 3);
        }
        assert_eq!(cache.kept(), (3, 3000));
        for b in [0, 1, 5, 2] {
            read_all(&cache, &pack, &data, b * BLOCK + 1, 10);
            assert!(cache.kept().0 <= 3);
        }
        assert_eq!(cache.kept(), (3, 2 * BLOCK + 1234), "blocks 1, 5 and 2");
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 12);
        assert_eq!(open_and_mapped(&cache), (3, 3));
        assert_eq!(files(&cache).len(), 3);
        // What three blocks hold is all the cache has room for: a read of more than a
        // quarter of that goes past it (one block, here).
        read_all(&cache, &pack, &data, 0, BLOCK);
        assert_eq!(cache.bypassed.load(Ordering::Relaxed), 1);
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The block that goes is the one read longest ago, by the stamp a read leaves in it,
    /// whether the read fetched it (under the cache's lock) or was a request's read of a
    /// cached block (which takes no lock but the block's slot's).
    #[test]
    fn the_block_read_longest_ago_goes_first() {
        let (dir, cache, pack, data) = cache_over("stamps", BLOCK * 4);
        for b in 0..4 {
            read_all(&cache, &pack, &data, b * BLOCK, 10);
        }
        let cached = |b: u64| read(pack.slot(b).unwrap()).is_some();
        let touch = |b: u64| {
            let mut reader = Reader::new(Some(&cache));
            assert!(reader.range(&pack, b * BLOCK + 3, 300).is_some());
        };
        // Block 0, the first fetched, is read again; a fifth block needs room.
        touch(0);
        read_all(&cache, &pack, &data, 4 * BLOCK, 10);
        assert!(!cached(1), "block 1 is the one read longest ago");
        assert!(cached(0) && cached(2) && cached(3) && cached(4));
        // Blocks 2 and 3 are read again; block 1 comes back.
        touch(2);
        touch(3);
        read_all(&cache, &pack, &data, BLOCK, 10);
        assert!(!cached(0), "block 0: every other was read or fetched since");
        assert!(cached(1) && cached(2) && cached(3) && cached(4));
        // Block 0 comes back: of the others, block 4 was read longest ago.
        read_all(&cache, &pack, &data, 0, 10);
        assert!(!cached(4));
        assert!(cached(0) && cached(1) && cached(2) && cached(3));
        // Block 2 is read again; block 4 comes back: block 3 goes.
        touch(2);
        read_all(&cache, &pack, &data, 4 * BLOCK, 10);
        assert!(!cached(3));
        assert!(cached(0) && cached(1) && cached(2) && cached(4));
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 8);
        consistent(&cache, &pack);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A read that goes away while it fetches (its client did) leaves nothing: neither
    /// the blocks that had arrived whole, which no one would read or evict, nor the one
    /// being written. (Those files stayed until the edge started again.)
    #[test]
    fn a_read_dropped_while_it_fetches_leaves_no_file() {
        use crate::testhttp::{Reply, serve};
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let dir = std::env::temp_dir().join(format!("gvd-bcache-dropped-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let asked = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let (seen, gate) = (Arc::clone(&asked), Arc::clone(&release));
        // A pack of three blocks. The first answer breaks off in the second block; the
        // request for the rest gets no answer while the test runs.
        let port = serve(move |_| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                let sent = (BLOCK + BLOCK / 2) as usize;
                let body = vec![7u8; 3 * BLOCK as usize];
                return Reply::Cut("206 Partial Content".to_string(), body, sent);
            }
            while !gate.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Reply::status("404 Not Found")
        });
        let account =
            format!("BlobEndpoint=http://127.0.0.1:{port}/acct;AccountName=acct;AccountKey=a2V5");
        let store = Store::Azure(crate::azure::AzureStore::new(&account, "packs").unwrap());
        let cache = BlockCache::new(Arc::new(store), dir.clone(), BLOCK * 64, usize::MAX).unwrap();
        let pack = cache.pack("p", BLOCK * 3);
        let cache = Arc::new(cache);
        rt().block_on(async {
            let (reading, wanted) = (Arc::clone(&cache), Arc::clone(&pack));
            let reader = tokio::spawn(async move {
                let sink = &mut |_: &[u8]| Ok(());
                reading.read(&wanted, 0, 3 * BLOCK, sink).await
            });
            while asked.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            // The first block is whole, the second is arriving.
            assert_eq!(files(&cache).len(), 2);
            reader.abort();
            assert!(reader.await.unwrap_err().is_cancelled());
        });
        assert!(files(&cache).is_empty(), "{:?}", files(&cache));
        assert_eq!(cache.kept(), (0, 0));
        consistent(&cache, &pack);
        release.store(true, Ordering::SeqCst);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What a read of cached blocks costs, beside a copy of the same bytes from a mapping
    /// of the pack: reads of four lengths at random places in a pack of 20 MiB, each
    /// copied into a buffer as a response's bytes are, five rounds of each. `request` is
    /// how a request reads ([`Reader`]); `read()` is the cold path's read of blocks that
    /// are cached after all.
    ///
    /// `cargo test --release --lib -- --ignored --nocapture hit_cost`
    #[test]
    #[ignore = "a timing; run explicitly"]
    fn hit_cost() {
        let (dir, cache, pack, data) = cache_over("cost", BLOCK * 64);
        let n = data.len() as u64;
        read_all(&cache, &pack, &data, 0, n);
        let fetches = cache.fetches.load(Ordering::Relaxed);
        let file = File::open(dir.join("store/blobs/p.pack")).unwrap();
        let map = gvfs_mmap::Map::open(&file, false).unwrap();
        let rt = rt();
        for (len, reads) in [
            (300u64, 400_000u64),
            (2 << 10, 400_000),
            (16 << 10, 100_000),
            (1 << 20, 5_000),
        ] {
            let mut x = 0x9E37_79B9_7F4A_7C15u64;
            let offsets: Vec<u64> = (0..reads)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x % (n - len)
                })
                .collect();
            let mut out: Vec<u8> = Vec::with_capacity(len as usize);
            let each = |t0: std::time::Instant| t0.elapsed().as_secs_f64() * 1e6 / reads as f64;
            let (mut request, mut through, mut copied) = (Vec::new(), Vec::new(), Vec::new());
            for _ in 0..5 {
                let mut reader = Reader::new(Some(&cache));
                let t0 = std::time::Instant::now();
                for &at in &offsets {
                    out.clear();
                    match reader.range(&pack, at, len) {
                        Some(range) => out.extend_from_slice(range),
                        None => {
                            // Into the next block.
                            while (out.len() as u64) < len {
                                let pos = at + out.len() as u64;
                                let (block, from) = reader.block(&pack, pos).unwrap();
                                let from = from as usize;
                                let n = (len as usize - out.len()).min(block.bytes().len() - from);
                                out.extend_from_slice(&block.bytes()[from..from + n]);
                            }
                            reader.count(2, true);
                        }
                    }
                    std::hint::black_box(&out);
                }
                request.push(each(t0));
                drop(reader);
                let t0 = std::time::Instant::now();
                rt.block_on(async {
                    for &at in &offsets {
                        out.clear();
                        let sink = &mut |b: &[u8]| {
                            out.extend_from_slice(b);
                            Ok(())
                        };
                        cache.read(&pack, at, len, sink).await.unwrap();
                        std::hint::black_box(&out);
                    }
                });
                through.push(each(t0));
                let t0 = std::time::Instant::now();
                for &at in &offsets {
                    out.clear();
                    out.extend_from_slice(&map.bytes()[at as usize..(at + len) as usize]);
                    std::hint::black_box(&out);
                }
                copied.push(each(t0));
            }
            println!(
                "{len:>8} B: request {request:.3?} us; read() {through:.3?} us; \
                 copy from a mapping {copied:.3?} us"
            );
        }
        assert_eq!(cache.fetches.load(Ordering::Relaxed), fetches);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
