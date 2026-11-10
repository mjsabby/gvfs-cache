//! The edge's local cache of blob packs that stay in object storage.
//!
//! A remote pack is read in fixed blocks ([`BLOCK`] bytes, the last one shorter), at most
//! [`WINDOW`] blocks at a time. The blocks a window lacks are fetched together, one ranged
//! GET per run of consecutive missing blocks, and kept on local disk. Blobs are packed in
//! path order, so the blobs of one checkout share blocks: a batch of thousands of small
//! blobs costs a few requests, and a repeated read costs none. A block being fetched is
//! fetched once: other readers wait for that fetch, and fail with it if it fails.
//!
//! Blocks are evicted least recently used beyond the byte budget (`--blob-cache-mib`),
//! except the blocks a read is streaming: those are pinned until it moves on, so every read
//! finishes whatever the budget (the cache may exceed it by the pinned windows meanwhile).
//! The unpinned blocks are kept in order of last use, so an eviction costs O(log n), and
//! their files are unlinked after the lock is let go: each block file has a generation in
//! its name, so the unlink never removes a block fetched again meanwhile.
//! A read of more than a quarter of the budget (one huge blob) streams straight from the
//! store, a window at a time: keeping it would only evict everything else.
//!
//! Reads are async (the edge's handlers await them): fetches go through the store's async
//! client, and a reader waiting for another's fetch parks its task, not a thread.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use tokio::sync::Notify;

use crate::store::{Sink, Store};
use crate::util::{Context, Error, Result, u64_of, us};

/// Bytes per cached block.
pub const BLOCK: u64 = 4 << 20;
/// Most blocks one read pins (and fetches) at a time.
pub const WINDOW: u64 = 8;
/// Bytes of a cached block read per piece handed to a sink.
const PIECE: u64 = 1 << 20;

type Key = (Arc<str>, u64);

struct Entry {
    len: u64,
    used: u64,
    /// Reads streaming this block now: eviction leaves it.
    pins: u32,
    /// Its file's generation (`<pack>.<block>.<file>`).
    file: u64,
}

struct State {
    blocks: HashMap<Key, Entry>,
    /// The unpinned blocks by last use (`Entry::used`, unique): eviction takes the first.
    lru: BTreeMap<u64, Key>,
    /// The blocks being fetched, each with the fetch that will bring it.
    fetching: HashMap<Key, Arc<Attempt>>,
    bytes: u64,
    clock: u64,
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
    budget: u64,
    state: Mutex<State>,
    /// Woken when a fetch ends (blocks cached, or the fetch failed).
    done: Notify,
    /// Windows read without fetching, and windows that fetched blocks.
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// Ranged GETs (into the cache, and for reads streamed past it).
    pub fetches: AtomicU64,
    /// Reads too large to keep, streamed straight from the store.
    pub bypassed: AtomicU64,
    /// Generations handed to block files.
    files: AtomicU64,
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Drops one pin of each of `blocks`; a block no read pins any more joins the LRU order.
fn release(st: &mut State, id: &Arc<str>, blocks: impl Iterator<Item = u64>) {
    let State {
        blocks: all, lru, ..
    } = st;
    for b in blocks {
        let key: Key = (Arc::clone(id), b);
        if let Some(e) = all.get_mut(&key) {
            e.pins = e.pins.saturating_sub(1);
            if e.pins == 0 {
                lru.insert(e.used, key);
            }
        }
    }
}

/// Blocks a read has pinned (block, file generation), ascending: released, and the cache
/// trimmed to its budget, when the read moves on, fails or is dropped.
struct Pinned<'a> {
    cache: &'a BlockCache,
    id: &'a Arc<str>,
    blocks: Vec<(u64, u64)>,
}

impl Drop for Pinned<'_> {
    fn drop(&mut self) {
        if !self.blocks.is_empty() {
            let mut st = lock(&self.cache.state);
            release(&mut st, self.id, self.blocks.drain(..).map(|(b, _)| b));
            let evicted = self.cache.evict(&mut st);
            drop(st);
            self.cache.unlink(evicted);
        }
    }
}

/// Blocks a read is fetching: if the read is dropped before the fetch ends, they are no
/// longer marked, and readers waiting for them are woken (one of them fetches them).
struct Fetching<'a> {
    cache: &'a BlockCache,
    id: &'a Arc<str>,
    blocks: &'a [u64],
    armed: bool,
}

impl Drop for Fetching<'_> {
    fn drop(&mut self) {
        if self.armed {
            let mut st = lock(&self.cache.state);
            for &b in self.blocks {
                st.fetching.remove(&(Arc::clone(self.id), b));
            }
            drop(st);
            self.cache.done.notify_waiters();
        }
    }
}

impl BlockCache {
    /// A cache under `dir` (emptied now) holding up to `budget` bytes.
    pub fn new(store: Arc<Store>, dir: PathBuf, budget: u64) -> Result<BlockCache> {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ctx(&format!("creating {}", dir.display()))?;
        Ok(BlockCache {
            store,
            dir,
            budget,
            state: Mutex::new(State {
                blocks: HashMap::new(),
                lru: BTreeMap::new(),
                fetching: HashMap::new(),
                bytes: 0,
                clock: 0,
            }),
            done: Notify::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetches: AtomicU64::new(0),
            bypassed: AtomicU64::new(0),
            files: AtomicU64::new(0),
        })
    }

    /// The file of generation `file` of block `key`.
    fn path(&self, key: &Key, file: u64) -> PathBuf {
        self.dir.join(format!("{}.{}.{file}", key.0, key.1))
    }

    /// Removes evicted blocks' files (after the lock is let go: a block fetched again since
    /// has a file of another generation).
    fn unlink(&self, evicted: Vec<(Key, u64)>) {
        for (key, file) in evicted {
            let _ = std::fs::remove_file(self.path(&key, file));
        }
    }

    /// `len` bytes of pack `id` (`pack_len` bytes, at store key `object`) from `at`,
    /// handed to `sink` in pieces. Callers pass a whole range: its length decides whether
    /// it streams past the cache, and its blocks are fetched a window at a time.
    pub async fn read<S: Sink>(
        &self,
        id: &Arc<str>,
        object: &str,
        pack_len: u64,
        at: u64,
        len: u64,
        sink: &mut S,
    ) -> Result<()> {
        let Some(end) = at.checked_add(len).filter(|&e| e <= pack_len) else {
            crate::bail!("read past the end of blob pack {id}");
        };
        if len == 0 {
            return Ok(());
        }
        if len > self.budget.checked_div(4).unwrap_or(0) {
            self.bypassed.fetch_add(1, Ordering::Relaxed);
            let window = WINDOW.saturating_mul(BLOCK);
            let mut pos = at;
            while pos < end {
                let n = end.saturating_sub(pos).min(window);
                self.fetches.fetch_add(1, Ordering::Relaxed);
                self.store.read_range_async(object, pos, n, sink).await?;
                pos = pos.saturating_add(n);
            }
            return Ok(());
        }
        let last = end.saturating_sub(1).checked_div(BLOCK).unwrap_or(0);
        let mut buf = vec![0u8; us(PIECE.min(len))];
        let mut pos = at;
        while pos < end {
            let b0 = pos.checked_div(BLOCK).unwrap_or(0);
            let b1 = last.min(b0.saturating_add(WINDOW).saturating_sub(1));
            let pinned = self.pin(id, object, pack_len, b0, b1).await?;
            pos = self.stream(&pinned, pos, end, &mut buf, sink).await?;
        }
        Ok(())
    }

    /// Streams `[pos, end)` within `pinned` blocks (ascending, consecutive); returns where
    /// it stopped.
    async fn stream<S: Sink>(
        &self,
        pinned: &Pinned<'_>,
        mut pos: u64,
        end: u64,
        buf: &mut [u8],
        sink: &mut S,
    ) -> Result<u64> {
        for &(b, file_gen) in &pinned.blocks {
            let start = b.saturating_mul(BLOCK);
            let upto = end.min(start.saturating_add(BLOCK));
            if pos >= upto {
                continue;
            }
            let file = File::open(self.path(&(Arc::clone(pinned.id), b), file_gen))
                .ctx("opening a cached block")?;
            while pos < upto {
                let n = upto.saturating_sub(pos).min(u64_of(buf.len()));
                let piece = buf.get_mut(..us(n)).ctx("cache read buffer")?;
                std::os::unix::fs::FileExt::read_exact_at(&file, piece, pos.saturating_sub(start))
                    .ctx("reading a cached block")?;
                sink.put(piece).await?;
                pos = pos.saturating_add(n);
            }
        }
        Ok(pos)
    }

    /// Pins blocks `b0..=b1` of the pack, fetching those not cached (or waiting for another
    /// reader's fetch of them, and failing with it if it fails). On an error, or if the read
    /// is dropped meanwhile, nothing stays pinned.
    async fn pin<'a>(
        &'a self,
        id: &'a Arc<str>,
        object: &str,
        pack_len: u64,
        b0: u64,
        b1: u64,
    ) -> Result<Pinned<'a>> {
        let mut held = Pinned {
            cache: self,
            id,
            blocks: Vec::new(),
        };
        let mut fetched = false;
        // The fetches this read waited for last, by block.
        let mut waited: HashMap<u64, Arc<Attempt>> = HashMap::new();
        loop {
            let (mine, attempt): (Vec<u64>, Arc<Attempt>) = loop {
                // Waiting is registered before the state is read: a fetch that ends in
                // between still wakes this reader.
                let mut ended = std::pin::pin!(self.done.notified());
                ended.as_mut().enable();
                let failure = {
                    let mut st = lock(&self.state);
                    let State {
                        blocks,
                        lru,
                        fetching,
                        clock,
                        ..
                    } = &mut *st;
                    let (mut missing, mut waiting) = (Vec::new(), HashMap::new());
                    let mut failure = None;
                    for b in b0..=b1 {
                        if held.blocks.iter().any(|&(x, _)| x == b) {
                            continue;
                        }
                        let key: Key = (Arc::clone(id), b);
                        // One tick per block: equal ages would make eviction arbitrary.
                        *clock = clock.wrapping_add(1);
                        let now = *clock;
                        if let Some(e) = blocks.get_mut(&key) {
                            if e.pins == 0 {
                                lru.remove(&e.used);
                            }
                            e.used = now;
                            e.pins = e.pins.saturating_add(1);
                            held.blocks.push((b, e.file));
                        } else if let Some(a) = fetching.get(&key) {
                            waiting.insert(b, Arc::clone(a));
                        } else if let Some(e) = waited.get(&b).and_then(|a| a.failure()) {
                            // Neither cached nor being fetched, and the fetch this read
                            // waited for failed: so does the read.
                            failure = Some(e);
                            break;
                        } else {
                            missing.push(b);
                        }
                    }
                    if failure.is_none() {
                        if missing.is_empty() && waiting.is_empty() {
                            let c = if fetched { &self.misses } else { &self.hits };
                            c.fetch_add(1, Ordering::Relaxed);
                            held.blocks.sort_unstable();
                            return Ok(held);
                        }
                        if !missing.is_empty() {
                            let attempt = Arc::new(Attempt::default());
                            for &b in &missing {
                                fetching.insert((Arc::clone(id), b), Arc::clone(&attempt));
                            }
                            break (missing, attempt);
                        }
                        waited = waiting;
                    }
                    failure
                };
                // Out of the lock: dropping `held` releases what it pinned.
                if let Some(e) = failure {
                    return Err(e);
                }
                ended.await;
            };
            fetched = true;
            // Should this read be dropped mid-fetch, the blocks stop being "fetching".
            let mut guard = Fetching {
                cache: self,
                id,
                blocks: &mine,
                armed: true,
            };
            let (got, outcome) = self.fetch(id, object, pack_len, &mine).await;
            guard.armed = false;
            let mut st = lock(&self.state);
            for &b in &mine {
                st.fetching.remove(&(Arc::clone(id), b));
            }
            let mut replaced = Vec::new();
            // What arrived is cached, also when a later run failed.
            for (&b, &(len, file)) in mine.iter().zip(&got) {
                st.clock = st.clock.wrapping_add(1);
                let used = st.clock;
                let key: Key = (Arc::clone(id), b);
                let entry = Entry {
                    len,
                    used,
                    pins: 1,
                    file,
                };
                if let Some(old) = st.blocks.insert(key.clone(), entry) {
                    st.bytes = st.bytes.saturating_sub(old.len);
                    if old.pins == 0 {
                        st.lru.remove(&old.used);
                    }
                    replaced.push((key, old.file));
                }
                st.bytes = st.bytes.saturating_add(len);
                held.blocks.push((b, file));
            }
            // Those waiting for the blocks that did not arrive fail with this read.
            if let Err(e) = &outcome {
                attempt.fail(e);
            }
            drop(st);
            self.unlink(replaced);
            self.done.notify_waiters();
            // On an error, dropping `held` releases what it pinned.
            outcome?;
        }
    }

    /// Fetches `blocks` (ascending), one ranged GET per run of consecutive ones. Returns the
    /// length and file generation of each block fetched, in order, and how the fetch ended.
    /// When a run fails, the blocks of the runs before it are whole and are returned with
    /// the error, for the caller to cache (they used to stay on disk untracked: never read
    /// again, never evicted); no block of the failed run is left on disk.
    async fn fetch(
        &self,
        id: &Arc<str>,
        object: &str,
        pack_len: u64,
        blocks: &[u64],
    ) -> (Vec<(u64, u64)>, Result<()>) {
        let mut lens = Vec::with_capacity(blocks.len());
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
            let end = b0.saturating_add(n).saturating_mul(BLOCK).min(pack_len);
            // Blocks are written as the bytes arrive, each renamed into place when whole.
            let mut cur: Option<(u64, u64, File, PathBuf)> = None;
            let mut whole: Vec<(u64, u64)> = Vec::new();
            let mut written = 0u64;
            let finish = |cur: &mut Option<(u64, u64, File, PathBuf)>,
                          whole: &mut Vec<(u64, u64)>|
             -> Result<()> {
                // No fsync: the cache starts empty after a restart anyway.
                if let Some((b, file_gen, f, tmp)) = cur.take() {
                    drop(f);
                    std::fs::rename(&tmp, self.path(&(Arc::clone(id), b), file_gen))
                        .ctx("caching a block")?;
                    whole.push((b, file_gen));
                }
                Ok(())
            };
            self.fetches.fetch_add(1, Ordering::Relaxed);
            let got = self
                .store
                .read_range_async(
                    object,
                    start,
                    end.saturating_sub(start),
                    &mut |mut chunk: &[u8]| {
                        while !chunk.is_empty() {
                            let pos = start.saturating_add(written);
                            let b = pos.checked_div(BLOCK).unwrap_or(0);
                            if cur.as_ref().is_none_or(|c| c.0 != b) {
                                finish(&mut cur, &mut whole)?;
                                let file_gen = self.files.fetch_add(1, Ordering::Relaxed);
                                let path = self.path(&(Arc::clone(id), b), file_gen);
                                let tmp = crate::fsutil::temp_path(&path);
                                let f = File::create(&tmp).ctx("creating a cache block")?;
                                cur = Some((b, file_gen, f, tmp));
                            }
                            let room = us(b
                                .saturating_add(1)
                                .saturating_mul(BLOCK)
                                .saturating_sub(pos));
                            let (now, rest) = chunk.split_at(room.min(chunk.len()));
                            if let Some((_, _, f, _)) = cur.as_mut() {
                                std::io::Write::write_all(f, now).ctx("writing a cache block")?;
                            }
                            written = written.saturating_add(u64_of(now.len()));
                            chunk = rest;
                        }
                        Ok(())
                    },
                )
                .await
                .and_then(|()| finish(&mut cur, &mut whole));
            if let Err(e) = got {
                if let Some((_, _, _, tmp)) = cur.take() {
                    let _ = std::fs::remove_file(tmp);
                }
                for (b, file_gen) in whole {
                    let _ = std::fs::remove_file(self.path(&(Arc::clone(id), b), file_gen));
                }
                return (lens, Err(e));
            }
            for (b, file_gen) in whole {
                let bs = b.saturating_mul(BLOCK);
                lens.push((
                    bs.saturating_add(BLOCK).min(pack_len).saturating_sub(bs),
                    file_gen,
                ));
            }
            i = j;
        }
        (lens, Ok(()))
    }

    /// Least recently used unpinned blocks go until the cache fits its budget; returns
    /// them, for their files to be unlinked once the lock is let go.
    fn evict(&self, st: &mut State) -> Vec<(Key, u64)> {
        let mut evicted = Vec::new();
        while st.bytes > self.budget {
            let Some((_, key)) = st.lru.pop_first() else {
                break;
            };
            if let Some(e) = st.blocks.remove(&key) {
                st.bytes = st.bytes.saturating_sub(e.len);
                evicted.push((key, e.file));
            }
        }
        evicted
    }

    /// Bytes cached now.
    pub fn bytes(&self) -> u64 {
        lock(&self.state).bytes
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
        let (dir, cache, data) = cache_over("runs", BLOCK * 64);
        let n = data.len() as u64;
        // Blocks 0..=2 in one request; then hits.
        read_all(&cache, &data, 10, BLOCK * 2 + 5);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 1);
        read_all(&cache, &data, BLOCK - 1, 2);
        read_all(&cache, &data, 0, BLOCK * 3);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 1);
        // The short last block.
        read_all(&cache, &data, n - 100, 100);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 2);
        assert_eq!(cache.misses.load(Ordering::Relaxed), 2);
        assert_eq!(cache.hits.load(Ordering::Relaxed), 2);
        let id: Arc<str> = Arc::from("p");
        assert!(
            rt().block_on(cache.read(&id, "blobs/p.pack", n, n - 1, 2, &mut |_: &[u8]| Ok(())))
                .is_err()
        );
        consistent(&cache);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn least_recently_used_blocks_go_first() {
        let (dir, cache, data) = cache_over("lru", BLOCK * 4);
        for b in 0..4 {
            read_all(&cache, &data, b * BLOCK, BLOCK);
        }
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 4);
        read_all(&cache, &data, 4 * BLOCK, BLOCK);
        assert_eq!(cache.bytes(), BLOCK * 4, "block 0 went");
        read_all(&cache, &data, 0, 10);
        assert_eq!(
            cache.fetches.load(Ordering::Relaxed),
            6,
            "block 0 again; block 1 went"
        );
        read_all(&cache, &data, 2 * BLOCK, 10);
        read_all(&cache, &data, 3 * BLOCK, 10);
        assert_eq!(
            cache.fetches.load(Ordering::Relaxed),
            6,
            "blocks 2 and 3 stayed"
        );
        read_all(&cache, &data, BLOCK, 10);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 7);
        consistent(&cache);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Readers racing over a cache smaller than what they read: every byte right.
    #[test]
    fn concurrent_readers_over_a_small_cache() {
        let (dir, cache, data) = cache_over("race", BLOCK * 2);
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
                tasks.push(tokio::spawn(async move {
                    let mut x = t * 7919 + 1;
                    for _ in 0..40 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let at = x % n;
                        let len = (x >> 20) % (BLOCK * 2).min(n - at) + 1;
                        read_all_async(&cache, &data, at, len.min(n - at)).await;
                    }
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }
        });
        assert!(cache.bytes() <= BLOCK * 2, "back within budget once idle");
        consistent(&cache);
        assert!(cache.bypassed.load(Ordering::Relaxed) > 0);
        consistent(&cache);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Once no read is running: every block is in the LRU order, the bytes add up, and the
    /// directory holds exactly the cached blocks' files (none leaked, none missing).
    fn consistent(cache: &BlockCache) {
        let st = lock(&cache.state);
        assert!(st.blocks.values().all(|e| e.pins == 0));
        assert_eq!(st.lru.len(), st.blocks.len());
        for (used, key) in &st.lru {
            assert_eq!(st.blocks.get(key).map(|e| e.used), Some(*used));
        }
        assert_eq!(st.bytes, st.blocks.values().map(|e| e.len).sum::<u64>());
        let mut want: Vec<PathBuf> = st
            .blocks
            .iter()
            .map(|(k, e)| cache.path(k, e.file))
            .collect();
        let mut have: Vec<PathBuf> = std::fs::read_dir(&cache.dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        want.sort();
        have.sort();
        assert_eq!(have, want);
    }

    fn cache_over(tag: &str, budget: u64) -> (std::path::PathBuf, BlockCache, Vec<u8>) {
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
        let cache = BlockCache::new(store, dir.join("cache"), budget).unwrap();
        (dir, cache, data)
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn read_all(cache: &BlockCache, data: &[u8], at: u64, len: u64) {
        rt().block_on(read_all_async(cache, data, at, len));
    }

    async fn read_all_async(cache: &BlockCache, data: &[u8], at: u64, len: u64) {
        let id: Arc<str> = Arc::from("p");
        let mut got = Vec::new();
        cache
            .read(
                &id,
                "blobs/p.pack",
                data.len() as u64,
                at,
                len,
                &mut |b: &[u8]| {
                    got.extend_from_slice(b);
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert!(got == data[at as usize..(at + len) as usize], "{at}+{len}");
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
        let cache = Arc::new(BlockCache::new(Arc::new(store), dir.clone(), BLOCK * 64).unwrap());
        let read = |cache: Arc<BlockCache>| async move {
            let id: Arc<str> = Arc::from("p");
            let sink = &mut |_: &[u8]| Ok(());
            let read = cache.read(&id, "blobs/p.pack", BLOCK * 8, 5, 10, sink);
            read.await.unwrap_err().to_string()
        };
        let rt = rt();
        let errors: Vec<String> = rt.block_on(async {
            let started = Arc::new(AtomicUsize::new(0));
            let mut tasks = Vec::new();
            for _ in 0..20 {
                let (cache, started) = (Arc::clone(&cache), Arc::clone(&started));
                tasks.push(tokio::spawn(async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    read(cache).await
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
        assert!(lock(&cache.state).fetching.is_empty());
        consistent(&cache);
        let e = rt.block_on(read(Arc::clone(&cache)));
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
        let (dir, cache, data) = cache_over("runs-fail", BLOCK * 64);
        let n = data.len() as u64;
        // Block 2 cached alone: a read of blocks 0..=4 then fetches the runs [0, 1] and
        // [3, 4].
        read_all(&cache, &data, 2 * BLOCK + 5, 10);
        // The store loses the pack's tail: the second run's ranged read fails.
        let pack = dir.join("store/blobs/p.pack");
        let lost = std::fs::OpenOptions::new().write(true).open(pack).unwrap();
        lost.set_len(3 * BLOCK + 100).unwrap();
        let id: Arc<str> = Arc::from("p");
        for fetches in [3, 4, 5] {
            let sink = &mut |_: &[u8]| Ok(());
            let read = cache.read(&id, "blobs/p.pack", n, 0, 5 * BLOCK, sink);
            assert!(rt().block_on(read).is_err());
            assert_eq!(cache.fetches.load(Ordering::Relaxed), fetches);
            assert_eq!(cache.bytes(), 3 * BLOCK, "blocks 0, 1 and 2");
            consistent(&cache);
        }
        read_all(&cache, &data, 0, 3 * BLOCK);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 5, "all three cached");
        consistent(&cache);
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
        let cache = Arc::new(BlockCache::new(Arc::new(store), dir.clone(), BLOCK * 64).unwrap());
        // The bytes of blocks `from..=to`, or the error of reading them.
        let read = |cache: Arc<BlockCache>, from: u64, to: u64| async move {
            let id: Arc<str> = Arc::from("p");
            let mut got = Vec::new();
            let sink = &mut |b: &[u8]| {
                got.extend_from_slice(b);
                Ok(())
            };
            let (at, len) = (from * BLOCK, (to + 1 - from) * BLOCK);
            let read = cache.read(&id, "blobs/p.pack", BLOCK * 3, at, len, sink);
            read.await.map(|()| got).map_err(|e| e.to_string())
        };
        let rt = rt();
        // Block 1 cached: a read of all three then fetches the runs [0] and [2].
        assert!(rt.block_on(read(Arc::clone(&cache), 1, 1)).unwrap() == vec![1u8; BLOCK as usize]);
        let (all, first, last) = rt.block_on(async {
            let started = Arc::new(AtomicUsize::new(0));
            let mut tasks = Vec::new();
            for (from, to) in [(0, 2), (0, 0), (2, 2)] {
                let (cache, begun) = (Arc::clone(&cache), Arc::clone(&started));
                tasks.push(tokio::spawn(async move {
                    begun.fetch_add(1, Ordering::SeqCst);
                    read(cache, from, to).await
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
        assert_eq!(cache.bytes(), 2 * BLOCK);
        consistent(&cache);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// One blob larger than the whole cache.
    #[test]
    fn a_read_larger_than_the_budget_completes() {
        let (dir, cache, data) = cache_over("big", BLOCK * 2);
        read_all(&cache, &data, 0, data.len() as u64);
        assert_eq!(cache.bypassed.load(Ordering::Relaxed), 1);
        assert_eq!(cache.bytes(), 0, "streamed past the cache");
        consistent(&cache);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A small read across a block boundary, with room for one block only.
    #[test]
    fn a_read_spanning_more_blocks_than_the_budget_completes() {
        let (dir, cache, data) = cache_over("span", BLOCK);
        read_all(&cache, &data, BLOCK - 10, 20);
        read_all(&cache, &data, BLOCK * 3 - 7, 9);
        assert!(cache.bytes() <= BLOCK);
        consistent(&cache);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
