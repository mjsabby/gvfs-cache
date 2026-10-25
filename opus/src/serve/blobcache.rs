//! The edge's local cache of blob packs that stay in object storage.
//!
//! A remote pack is read in fixed blocks ([`BLOCK`] bytes, the last one shorter). A read
//! is answered from blocks on local disk; the blocks it lacks are fetched together, with
//! one ranged GET per run of consecutive missing blocks, and kept. Blobs are packed in
//! path order, so the blobs of one checkout share blocks: a batch of thousands of small
//! blobs costs a few requests, and a repeated read costs none. Blocks are evicted least
//! recently used beyond a byte budget (`--blob-cache-mib`). A block being fetched is
//! fetched once: other readers wait for it.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::store::Store;
use crate::util::{Context, Result, u64_of, us};

/// Bytes per cached block.
pub const BLOCK: u64 = 4 << 20;
/// Most blocks fetched by one request.
const RUN_MAX: u64 = 16;

type Key = (Arc<str>, u64);

struct Entry {
    len: u64,
    used: u64,
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
    done: Condvar,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub fetches: AtomicU64,
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
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
            done: Condvar::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetches: AtomicU64::new(0),
        })
    }

    fn path(&self, key: &Key) -> PathBuf {
        self.dir.join(format!("{}.{}", key.0, key.1))
    }

    /// `len` bytes of pack `id` (`pack_len` bytes, at store key `object`) from `at`,
    /// handed to `sink` in pieces.
    pub fn read(
        &self,
        id: &Arc<str>,
        object: &str,
        pack_len: u64,
        at: u64,
        len: u64,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let end = at.saturating_add(len);
        if end > pack_len {
            crate::bail!("read past the end of blob pack {id}");
        }
        if len == 0 {
            return Ok(());
        }
        let first = at.checked_div(BLOCK).unwrap_or(0);
        let last = end.saturating_sub(1).checked_div(BLOCK).unwrap_or(0);
        self.ensure(id, object, pack_len, first, last)?;
        let mut buf = vec![0u8; us(BLOCK.min(len))];
        let mut pos = at;
        for b in first..=last {
            let key: Key = (Arc::clone(id), b);
            let start = b.saturating_mul(BLOCK);
            let from = pos.saturating_sub(start);
            let upto = end.min(start.saturating_add(BLOCK)).saturating_sub(start);
            let n = us(upto.saturating_sub(from));
            let piece = buf.get_mut(..n).ctx("cache read buffer")?;
            // Another reader may have evicted the block since: fetch it again. (An open
            // block stays readable when evicted.)
            let mut tries = 0u32;
            let file = loop {
                match File::open(self.path(&key)) {
                    Ok(f) => break f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound && tries < 3 => {
                        tries = tries.saturating_add(1);
                        self.ensure(id, object, pack_len, b, b)?;
                    }
                    Err(e) => {
                        return Err(crate::util::Error::from(e)).ctx("opening a cached block");
                    }
                }
            };
            std::os::unix::fs::FileExt::read_exact_at(&file, piece, from)
                .ctx("reading a cached block")?;
            sink(piece)?;
            pos = pos.saturating_add(u64_of(n));
        }
        Ok(())
    }

    /// Makes blocks `first..=last` of the pack local, fetching each run of missing ones
    /// with one request (or waiting for another reader's fetch).
    fn ensure(
        &self,
        id: &Arc<str>,
        object: &str,
        pack_len: u64,
        first: u64,
        last: u64,
    ) -> Result<()> {
        loop {
            let mine: Vec<u64> = {
                let mut st = lock(&self.state);
                loop {
                    let mut missing = Vec::new();
                    let mut waiting = false;
                    for b in first..=last {
                        let key: Key = (Arc::clone(id), b);
                        // One tick per block: equal ages would make eviction arbitrary.
                        st.clock = st.clock.wrapping_add(1);
                        let now = st.clock;
                        if let Some(e) = st.blocks.get_mut(&key) {
                            e.used = now;
                        } else if st.fetching.contains(&key) {
                            waiting = true;
                        } else {
                            missing.push(b);
                        }
                    }
                    if missing.is_empty() && !waiting {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    if !missing.is_empty() {
                        for &b in &missing {
                            st.fetching.insert((Arc::clone(id), b));
                        }
                        break missing;
                    }
                    st = match self.done.wait(st) {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                }
            };
            self.misses.fetch_add(1, Ordering::Relaxed);
            let fetched = self.fetch(id, object, pack_len, &mine);
            let mut st = lock(&self.state);
            for &b in &mine {
                st.fetching.remove(&(Arc::clone(id), b));
            }
            if let Ok(lens) = &fetched {
                for (&b, &len) in mine.iter().zip(lens) {
                    st.clock = st.clock.wrapping_add(1);
                    let now = st.clock;
                    st.blocks
                        .insert((Arc::clone(id), b), Entry { len, used: now });
                    st.bytes = st.bytes.saturating_add(len);
                }
                self.evict(&mut st);
            }
            self.done.notify_all();
            drop(st);
            fetched?;
        }
    }

    /// Fetches `blocks` (ascending), one ranged GET per run of consecutive ones; returns
    /// each block's length.
    fn fetch(
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
            while blocks.get(j).is_some_and(|&b| {
                b == b0.saturating_add(u64_of(j.saturating_sub(i)))
                    && u64_of(j.saturating_sub(i)) < RUN_MAX
            }) {
                j = j.saturating_add(1);
            }
            let n = u64_of(j.saturating_sub(i));
            let start = b0.saturating_mul(BLOCK);
            let end = b0.saturating_add(n).saturating_mul(BLOCK).min(pack_len);
            // Blocks are written as the bytes arrive, each renamed into place when whole.
            let mut cur: Option<(u64, File, PathBuf)> = None;
            let mut written = 0u64;
            let finish = |cur: &mut Option<(u64, File, PathBuf)>| -> Result<()> {
                if let Some((b, f, tmp)) = cur.take() {
                    f.sync_all().ok();
                    let key: Key = (Arc::clone(id), b);
                    std::fs::rename(&tmp, self.path(&key)).ctx("caching a block")?;
                }
                Ok(())
            };
            self.fetches.fetch_add(1, Ordering::Relaxed);
            let got = self.store.read_range(
                object,
                start,
                end.saturating_sub(start),
                &mut |mut chunk| {
                    while !chunk.is_empty() {
                        let pos = start.saturating_add(written);
                        let b = pos.checked_div(BLOCK).unwrap_or(0);
                        if cur.as_ref().is_none_or(|c| c.0 != b) {
                            finish(&mut cur)?;
                            let key: Key = (Arc::clone(id), b);
                            let tmp = crate::fsutil::temp_path(&self.path(&key));
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
            );
            if let Err(e) = got {
                if let Some((_, _, tmp)) = cur.take() {
                    let _ = std::fs::remove_file(tmp);
                }
                return Err(e);
            }
            finish(&mut cur)?;
            for k in 0..n {
                let bs = b0.saturating_add(k).saturating_mul(BLOCK);
                lens.push(bs.saturating_add(BLOCK).min(pack_len).saturating_sub(bs));
            }
            i = j;
        }
        Ok(lens)
    }

    /// Least recently used blocks go until the cache fits its budget.
    fn evict(&self, st: &mut State) {
        if st.bytes <= self.budget {
            return;
        }
        let mut by_age: Vec<(u64, Key)> = st
            .blocks
            .iter()
            .filter(|(k, _)| !st.fetching.contains(*k))
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
    fn reads_fetch_runs_once_and_evict_lru() {
        let dir = std::env::temp_dir().join(format!("gvd-bcache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store =
            Arc::new(Store::open(&format!("fs:{}", dir.join("store").display()), "").unwrap());
        let data: Vec<u8> = (0..(BLOCK * 5 + 1234))
            .map(|i| (i * 31 % 251) as u8)
            .collect();
        let mut w = store.writer("blobs/p.pack").unwrap();
        std::io::Write::write_all(&mut w, &data).unwrap();
        w.finish().unwrap();
        let cache = BlockCache::new(Arc::clone(&store), dir.join("cache"), BLOCK * 3).unwrap();
        let id: Arc<str> = Arc::from("p");
        let n = data.len() as u64;
        let read = |at: u64, len: u64| {
            let mut got = Vec::new();
            cache
                .read(&id, "blobs/p.pack", n, at, len, &mut |b| {
                    got.extend_from_slice(b);
                    Ok(())
                })
                .unwrap();
            assert!(got == data[at as usize..(at + len) as usize], "{at}+{len}");
        };
        // Blocks 0..=2 in one request; then hits.
        read(10, BLOCK * 2 + 5);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 1);
        read(BLOCK - 1, 2);
        read(0, BLOCK * 3);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 1);
        // The short last block; then over budget: the oldest blocks go.
        read(n - 100, 100);
        assert_eq!(cache.fetches.load(Ordering::Relaxed), 2);
        read(BLOCK * 3, BLOCK);
        assert!(cache.bytes() <= BLOCK * 3);
        read(0, 10);
        assert_eq!(
            cache.fetches.load(Ordering::Relaxed),
            4,
            "block 0 was evicted"
        );
        assert!(
            cache
                .read(&id, "blobs/p.pack", n, n - 1, 2, &mut |_| Ok(()))
                .is_err()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
