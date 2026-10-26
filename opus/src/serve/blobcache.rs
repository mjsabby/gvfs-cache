//! The edge's local cache of blob packs that stay in object storage.
//!
//! A remote pack is read in fixed blocks ([`BLOCK`] bytes, the last one shorter), at most
//! [`WINDOW`] blocks at a time. The blocks a window lacks are fetched together, one ranged
//! GET per run of consecutive missing blocks, and kept on local disk. Blobs are packed in
//! path order, so the blobs of one checkout share blocks: a batch of thousands of small
//! blobs costs a few requests, and a repeated read costs none. A block being fetched is
//! fetched once: other readers wait for it.
//!
//! Blocks are evicted least recently used beyond the byte budget (`--blob-cache-mib`),
//! except the blocks a read is streaming: those are pinned until it moves on, so every read
//! finishes whatever the budget (the cache may exceed it by the pinned windows meanwhile).
//! A read of more than a quarter of the budget (one huge blob) streams straight from the
//! store, a window at a time: keeping it would only evict everything else.
//!
//! Reads are async (the edge's handlers await them): fetches go through the store's async
//! client, and a reader waiting for another's fetch parks its task, not a thread.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

use crate::store::Store;
use crate::util::{Context, Result, u64_of, us};

/// Bytes per cached block.
pub const BLOCK: u64 = 4 << 20;
/// Most blocks one read pins (and fetches) at a time.
pub const WINDOW: u64 = 8;

type Key = (Arc<str>, u64);

struct Entry {
    len: u64,
    used: u64,
    /// Reads streaming this block now: eviction leaves it.
    pins: u32,
}

struct State {
    blocks: HashMap<Key, Entry>,
    fetching: HashSet<Key>,
    bytes: u64,
    clock: u64,
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
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Drops one pin of each of `blocks`.
fn release(st: &mut State, id: &Arc<str>, blocks: impl Iterator<Item = u64>) {
    for b in blocks {
        if let Some(e) = st.blocks.get_mut(&(Arc::clone(id), b)) {
            e.pins = e.pins.saturating_sub(1);
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
                fetching: HashSet::new(),
                bytes: 0,
                clock: 0,
            }),
            done: Notify::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetches: AtomicU64::new(0),
            bypassed: AtomicU64::new(0),
        })
    }

    fn path(&self, key: &Key) -> PathBuf {
        self.dir.join(format!("{}.{}", key.0, key.1))
    }

    /// `len` bytes of pack `id` (`pack_len` bytes, at store key `object`) from `at`,
    /// handed to `sink` in pieces.
    pub async fn read(
        &self,
        id: &Arc<str>,
        object: &str,
        pack_len: u64,
        at: u64,
        len: u64,
        sink: &mut (dyn FnMut(&[u8]) -> Result<()> + Send),
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
        let mut buf = vec![0u8; us(BLOCK.min(len))];
        let mut pos = at;
        while pos < end {
            let b0 = pos.checked_div(BLOCK).unwrap_or(0);
            let b1 = last.min(b0.saturating_add(WINDOW).saturating_sub(1));
            self.pin(id, object, pack_len, b0, b1).await?;
            let streamed = self.stream(id, (b0, b1), pos, end, &mut buf, sink);
            let mut st = lock(&self.state);
            release(&mut st, id, b0..=b1);
            self.evict(&mut st);
            drop(st);
            pos = streamed?;
        }
        Ok(())
    }

    /// Streams `[pos, end)` within pinned blocks `b0..=b1`; returns where it stopped.
    fn stream(
        &self,
        id: &Arc<str>,
        (b0, b1): (u64, u64),
        mut pos: u64,
        end: u64,
        buf: &mut [u8],
        sink: &mut (dyn FnMut(&[u8]) -> Result<()> + Send),
    ) -> Result<u64> {
        for b in b0..=b1 {
            let start = b.saturating_mul(BLOCK);
            let upto = end.min(start.saturating_add(BLOCK));
            if pos >= upto {
                continue;
            }
            let piece = buf
                .get_mut(..us(upto.saturating_sub(pos)))
                .ctx("cache read buffer")?;
            let file = File::open(self.path(&(Arc::clone(id), b))).ctx("opening a cached block")?;
            std::os::unix::fs::FileExt::read_exact_at(&file, piece, pos.saturating_sub(start))
                .ctx("reading a cached block")?;
            sink(piece)?;
            pos = upto;
        }
        Ok(pos)
    }

    /// Pins blocks `b0..=b1` of the pack, fetching those not cached (or waiting for another
    /// reader's fetch of them). On an error nothing stays pinned.
    async fn pin(
        &self,
        id: &Arc<str>,
        object: &str,
        pack_len: u64,
        b0: u64,
        b1: u64,
    ) -> Result<()> {
        let mut held: Vec<u64> = Vec::new();
        let mut fetched = false;
        loop {
            let mine: Vec<u64> = loop {
                // Waiting is registered before the state is read: a fetch that ends in
                // between still wakes this reader.
                let mut ended = std::pin::pin!(self.done.notified());
                ended.as_mut().enable();
                {
                    let mut st = lock(&self.state);
                    let (mut missing, mut waiting) = (Vec::new(), false);
                    for b in b0..=b1 {
                        if held.contains(&b) {
                            continue;
                        }
                        let key: Key = (Arc::clone(id), b);
                        // One tick per block: equal ages would make eviction arbitrary.
                        st.clock = st.clock.wrapping_add(1);
                        let now = st.clock;
                        if let Some(e) = st.blocks.get_mut(&key) {
                            e.used = now;
                            e.pins = e.pins.saturating_add(1);
                            held.push(b);
                        } else if st.fetching.contains(&key) {
                            waiting = true;
                        } else {
                            missing.push(b);
                        }
                    }
                    if missing.is_empty() && !waiting {
                        let c = if fetched { &self.misses } else { &self.hits };
                        c.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    if !missing.is_empty() {
                        for &b in &missing {
                            st.fetching.insert((Arc::clone(id), b));
                        }
                        break missing;
                    }
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
            let got = self.fetch(id, object, pack_len, &mine).await;
            guard.armed = false;
            let mut st = lock(&self.state);
            for &b in &mine {
                st.fetching.remove(&(Arc::clone(id), b));
            }
            match &got {
                Ok(lens) => {
                    for (&b, &len) in mine.iter().zip(lens) {
                        st.clock = st.clock.wrapping_add(1);
                        let used = st.clock;
                        let entry = Entry { len, used, pins: 1 };
                        if let Some(old) = st.blocks.insert((Arc::clone(id), b), entry) {
                            st.bytes = st.bytes.saturating_sub(old.len);
                        }
                        st.bytes = st.bytes.saturating_add(len);
                        held.push(b);
                    }
                }
                Err(_) => release(&mut st, id, held.iter().copied()),
            }
            drop(st);
            self.done.notify_waiters();
            got?;
        }
    }

    /// Fetches `blocks` (ascending), one ranged GET per run of consecutive ones; returns
    /// each block's length. On an error no block of the failed run is left on disk.
    async fn fetch(
        &self,
        id: &Arc<str>,
        object: &str,
        pack_len: u64,
        blocks: &[u64],
    ) -> Result<Vec<u64>> {
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
            let mut cur: Option<(u64, File, PathBuf)> = None;
            let mut whole: Vec<u64> = Vec::new();
            let mut written = 0u64;
            let finish =
                |cur: &mut Option<(u64, File, PathBuf)>, whole: &mut Vec<u64>| -> Result<()> {
                    // No fsync: the cache starts empty after a restart anyway.
                    if let Some((b, f, tmp)) = cur.take() {
                        drop(f);
                        std::fs::rename(&tmp, self.path(&(Arc::clone(id), b)))
                            .ctx("caching a block")?;
                        whole.push(b);
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
                    &mut |mut chunk| {
                        while !chunk.is_empty() {
                            let pos = start.saturating_add(written);
                            let b = pos.checked_div(BLOCK).unwrap_or(0);
                            if cur.as_ref().is_none_or(|c| c.0 != b) {
                                finish(&mut cur, &mut whole)?;
                                let tmp =
                                    crate::fsutil::temp_path(&self.path(&(Arc::clone(id), b)));
                                let f = File::create(&tmp).ctx("creating a cache block")?;
                                cur = Some((b, f, tmp));
                            }
                            let room = us(b
                                .saturating_add(1)
                                .saturating_mul(BLOCK)
                                .saturating_sub(pos));
                            let (now, rest) = chunk.split_at(room.min(chunk.len()));
                            if let Some((_, f, _)) = cur.as_mut() {
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
                if let Some((_, _, tmp)) = cur.take() {
                    let _ = std::fs::remove_file(tmp);
                }
                for b in whole {
                    let _ = std::fs::remove_file(self.path(&(Arc::clone(id), b)));
                }
                return Err(e);
            }
            for k in 0..n {
                let bs = b0.saturating_add(k).saturating_mul(BLOCK);
                lens.push(bs.saturating_add(BLOCK).min(pack_len).saturating_sub(bs));
            }
            i = j;
        }
        Ok(lens)
    }

    /// Least recently used unpinned blocks go until the cache fits its budget.
    fn evict(&self, st: &mut State) {
        if st.bytes <= self.budget {
            return;
        }
        let mut by_age: Vec<(u64, Key)> = st
            .blocks
            .iter()
            .filter(|(_, e)| e.pins == 0)
            .map(|(k, e)| (e.used, k.clone()))
            .collect();
        by_age.sort_unstable_by_key(|x| x.0);
        for (_, key) in by_age {
            if st.bytes <= self.budget {
                break;
            }
            if let Some(e) = st.blocks.remove(&key) {
                st.bytes = st.bytes.saturating_sub(e.len);
                let _ = std::fs::remove_file(self.path(&key));
            }
        }
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
            rt().block_on(cache.read(&id, "blobs/p.pack", n, n - 1, 2, &mut |_| Ok(())))
                .is_err()
        );
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
        assert!(cache.bypassed.load(Ordering::Relaxed) > 0);
        let _ = std::fs::remove_dir_all(&dir);
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
            .read(&id, "blobs/p.pack", data.len() as u64, at, len, &mut |b| {
                got.extend_from_slice(b);
                Ok(())
            })
            .await
            .unwrap();
        assert!(got == data[at as usize..(at + len) as usize], "{at}+{len}");
    }

    /// One blob larger than the whole cache.
    #[test]
    fn a_read_larger_than_the_budget_completes() {
        let (dir, cache, data) = cache_over("big", BLOCK * 2);
        read_all(&cache, &data, 0, data.len() as u64);
        assert_eq!(cache.bypassed.load(Ordering::Relaxed), 1);
        assert_eq!(cache.bytes(), 0, "streamed past the cache");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A small read across a block boundary, with room for one block only.
    #[test]
    fn a_read_spanning_more_blocks_than_the_budget_completes() {
        let (dir, cache, data) = cache_over("span", BLOCK);
        read_all(&cache, &data, BLOCK - 10, 20);
        read_all(&cache, &data, BLOCK * 3 - 7, 9);
        assert!(cache.bytes() <= BLOCK);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
