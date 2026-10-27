//! Request scratch memory: allocated once at startup, leased per request, reset in O(1).
//!
//! Every per-request structure of the planning (parsed ids, the dedupe sets, the splice
//! plan, resolver buffers, a resolved loose object) lives in an [`Arena`]. A request leases
//! one from the [`ArenaPool`] (waiting briefly when all are busy: built-in backpressure)
//! once its body is in (the body is read into the connection's own buffer, so that a slow
//! client holds no arena), and returns it on drop, so steady-state serving performs no heap
//! allocation.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit};

use crate::splice::{ObjectSplice, Resolver};
use crate::util::{Oid, Result};

/// Open-addressed set of object ids with fixed capacity. Ids are uniformly random, so a
/// slot is simply some bytes of the id (no hashing); the all-zero id marks a free slot.
pub struct OidSet {
    slots: Vec<Oid>,
    used: Vec<u32>,
    mask: usize,
}

impl OidSet {
    /// Room for `n` ids at <= 50% load.
    pub fn with_capacity(n: usize) -> OidSet {
        let cap = n.saturating_mul(2).max(16).next_power_of_two();
        OidSet {
            slots: vec![Oid::ZERO; cap],
            used: Vec::with_capacity(n),
            mask: cap.saturating_sub(1),
        }
    }

    #[inline]
    fn home(&self, oid: &Oid) -> usize {
        let [a, b, c, d, e, f, g, h, ..] = oid.0;
        (u64::from_le_bytes([a, b, c, d, e, f, g, h]) as usize) & self.mask
    }

    /// Inserts `oid`; `Ok(false)` if it was already present, `Err` when full (unless
    /// `grow`, which doubles the table off the hot path's books).
    pub fn insert(&mut self, oid: &Oid) -> Result<bool> {
        self.insert_grow(oid, false)
    }

    pub fn insert_grow(&mut self, oid: &Oid, grow: bool) -> Result<bool> {
        if self.used.len() >= self.used.capacity() {
            if !grow {
                crate::bail!("request exceeds the arena's object capacity");
            }
            self.double();
        }
        let mut i = self.home(oid);
        loop {
            let Some(slot) = self.slots.get_mut(i) else {
                crate::bail!("oid set slot out of range")
            };
            if slot.is_zero() {
                *slot = *oid;
                self.used.push(crate::util::u32_of(i));
                return Ok(true);
            }
            if slot == oid {
                return Ok(false);
            }
            i = i.wrapping_add(1) & self.mask;
        }
    }

    pub fn contains(&self, oid: &Oid) -> bool {
        let mut i = self.home(oid);
        loop {
            match self.slots.get(i) {
                Some(s) if s == oid => return true,
                Some(s) if s.is_zero() => return false,
                Some(_) => i = i.wrapping_add(1) & self.mask,
                None => return false,
            }
        }
    }

    /// Doubles the capacity, re-inserting every id (cold path).
    fn double(&mut self) {
        let _cold = gvfs_alloc_guard::cold_section();
        let ids: Vec<Oid> = self
            .used
            .iter()
            .filter_map(|&i| self.slots.get(i as usize).copied())
            .collect();
        let cap = self.slots.len().saturating_mul(2);
        *self = OidSet {
            slots: vec![Oid::ZERO; cap],
            used: Vec::with_capacity(cap / 2),
            mask: cap.saturating_sub(1),
        };
        for id in &ids {
            let _ = self.insert(id);
        }
    }

    /// Back to `n`-id capacity if a request grew it far beyond (cold path).
    pub fn shrink(&mut self, n: usize) {
        let normal = n.saturating_mul(2).max(16).next_power_of_two();
        if self.slots.len() > normal.saturating_mul(2) {
            let _cold = gvfs_alloc_guard::cold_section();
            *self = OidSet::with_capacity(n);
        }
    }

    pub fn len(&self) -> usize {
        self.used.len()
    }

    pub fn is_empty(&self) -> bool {
        self.used.is_empty()
    }

    /// O(len) reset: only the used slots are cleared.
    pub fn clear(&mut self) {
        for &i in &self.used {
            if let Some(s) = self.slots.get_mut(i as usize) {
                *s = Oid::ZERO;
            }
        }
        self.used.clear();
    }
}

/// `v.push(item)`: within the preallocated capacity, or (when `grow`) by growing off the
/// hot path's books. Returns false if it does not fit.
pub fn push<T>(v: &mut Vec<T>, item: T, grow: bool) -> bool {
    if v.len() >= v.capacity() {
        if !grow {
            return false;
        }
        let _cold = gvfs_alloc_guard::cold_section();
        v.reserve(v.capacity().max(1024));
    }
    v.push(item);
    true
}

/// Shrinks `v` back to `n` items of capacity if a request grew it far beyond.
fn shrink<T>(v: &mut Vec<T>, n: usize) {
    if v.capacity() > n.saturating_mul(2) {
        let _cold = gvfs_alloc_guard::cold_section();
        v.clear();
        v.shrink_to(n);
    }
}

/// Scratch memory for one request.
pub struct Arena {
    /// Object ids parsed from the request.
    pub oids: Vec<Oid>,
    /// The request position of each of `oids` once the store's blobs have been taken out
    /// of them (`--blobs separate` stores), so that blobs go out in request order.
    pub oid_at: Vec<u32>,
    /// Where the first of them are (`View::find`), looked up once per request.
    pub found: Vec<Option<(u16, u32)>>,
    /// Ids already planned (dedupe across requested objects and delta chains).
    pub seen: OidSet,
    /// Commits and trees already expanded.
    pub visited: OidSet,
    /// Work list for tree expansion.
    pub work: Vec<Oid>,
    /// Commits of the current / next `commitDepth` level.
    pub commits: Vec<Oid>,
    pub next_commits: Vec<Oid>,
    pub splice: ObjectSplice,
    pub resolver: Resolver,
    /// Small response bodies built in place (sizes JSON, resolved loose objects).
    pub out: Vec<u8>,
    pub deflater: crate::zlib::Deflater,
    /// Blobs of the request (`--blobs separate` stores), in the order they go out: request
    /// position, where the entry is, and its record.
    pub blobs: Vec<(u32, BlobSrc, crate::blobs::BlobRec)>,
    /// The packs of the proxied ones (the live tip's blobs, not in the store yet).
    pub proxied: Vec<std::sync::Arc<crate::serve::blobs::BlobPack>>,
    /// The request's blobs already planned. Apart from `seen` (blobs and slab objects never
    /// share an id) and sized for a blob batch, not the arena's whole object capacity, so
    /// it stays in cache.
    pub blob_seen: OidSet,
    /// Commit expansion may outgrow the arena: its structures may grow (counted as cold
    /// allocations) for this request, as far as [`ArenaPool::charge`] allows;
    /// [`Arena::shrink`] takes them back when the lease ends.
    pub grow: bool,
    pub growth: Growth,
    objects: usize,
    bytes: usize,
}

/// What a commit expansion has taken from its pool's growth budget ([`ArenaPool::charge`]).
pub struct Growth {
    /// Objects the arena plans without growing (at most `--max-request-objects`).
    base: usize,
    /// Room beyond `base` charged to the pool.
    charged: usize,
}

/// Why [`ArenaPool::charge`] refused.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    /// Past `--max-request-objects`: the request is never served (413).
    TooLarge,
    /// Grown arenas hold the whole `--grow-objects` budget: retry later (503).
    Busy,
}

/// Where a blob entry of a response is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobSrc {
    /// In a segment of the view's blob set ([`crate::serve::blobs::BlobSet::locate`]): no
    /// reference count is taken per blob, which every core would contend for.
    Store(u32),
    /// In [`Arena::proxied`] at this index.
    Proxied(u32),
}

/// Blobs one request holds without growing (gvfs-helper asks for a few thousand at once).
const BLOBS: usize = 1 << 13;
/// Proxied blobs one request holds without growing.
const PROXIED: usize = 1 << 8;
/// Requested ids whose lookups a request keeps (a gvfs-helper batch is at most 4,000).
const FOUND: usize = 1 << 13;

impl Arena {
    /// `bytes` is split between the body, response and splice buffers; `objects` bounds
    /// the ids one request may name or expand to.
    pub fn new(bytes: usize, objects: usize) -> Arena {
        let quarter = bytes / 4;
        let mut a = Arena {
            oids: Vec::with_capacity(objects),
            oid_at: Vec::with_capacity(objects),
            found: Vec::with_capacity(FOUND),
            seen: OidSet::with_capacity(objects),
            visited: OidSet::with_capacity(objects),
            work: Vec::with_capacity(objects),
            commits: Vec::with_capacity(objects.min(1 << 16)),
            next_commits: Vec::with_capacity(objects.min(1 << 16)),
            splice: ObjectSplice::with_capacity(objects, quarter),
            resolver: Resolver::with_capacity(quarter.min(4 << 20)),
            out: Vec::with_capacity(quarter.saturating_mul(2)),
            deflater: crate::zlib::Deflater::new(1),
            blobs: Vec::with_capacity(BLOBS),
            proxied: Vec::with_capacity(PROXIED),
            blob_seen: OidSet::with_capacity(BLOBS),
            grow: false,
            growth: Growth {
                base: objects,
                charged: 0,
            },
            objects,
            bytes,
        };
        // Touch every page now so requests never take first-touch faults.
        a.out.resize(a.out.capacity(), 0);
        a.reset();
        a
    }

    pub fn reset(&mut self) {
        self.oids.clear();
        self.oid_at.clear();
        self.found.clear();
        self.seen.clear();
        self.visited.clear();
        self.work.clear();
        self.commits.clear();
        self.next_commits.clear();
        self.splice.clear();
        self.out.clear();
        self.blobs.clear();
        self.proxied.clear();
        self.blob_seen.clear();
    }

    /// Every structure back to its size at startup if a request grew it far beyond (a commit
    /// closure, a large object, a large blob batch): grown memory lasts one request (cold
    /// path).
    pub fn shrink(&mut self) {
        let (n, quarter) = (self.objects, self.bytes / 4);
        self.seen.shrink(n);
        self.visited.shrink(n);
        shrink(&mut self.work, n);
        let few = n.min(1 << 16);
        shrink(&mut self.commits, few);
        shrink(&mut self.next_commits, few);
        self.splice.shrink(n, quarter);
        self.resolver.shrink(quarter.min(4 << 20));
        if self.out.capacity() > quarter.saturating_mul(4) {
            let _cold = gvfs_alloc_guard::cold_section();
            self.out.clear();
            self.out.shrink_to(quarter.saturating_mul(2));
            self.out.resize(quarter.saturating_mul(2), 0);
        }
        self.blob_seen.shrink(BLOBS);
        shrink(&mut self.proxied, PROXIED);
        shrink(&mut self.blobs, BLOBS);
        self.grow = false;
    }
}

/// A fixed set of arenas leased by requests.
pub struct ArenaPool {
    // Boxed on purpose: a lease moves its box out of the pool.
    #[allow(clippy::vec_box)]
    free: Mutex<Vec<Box<Arena>>>,
    /// One permit per arena in `free`: a request waits for one without holding a thread.
    permits: Semaphore,
    /// Room for objects that grown arenas have beyond their size, together, and the most
    /// they may have.
    grown: AtomicUsize,
    grow_budget: usize,
    /// The most objects one commit expansion may plan.
    max_objects: usize,
}

/// A leased arena; returned to the pool on drop.
pub struct Lease<'a> {
    arena: Option<Box<Arena>>,
    pool: &'a ArenaPool,
    /// Released after the arena is back in the pool (fields drop after [`Drop::drop`]).
    _permit: SemaphorePermit<'a>,
}

impl ArenaPool {
    /// `count` arenas of `bytes` and `objects`. Commit expansions may grow them: each to at
    /// most `max_objects`, all together by at most `grow_budget` objects beyond their size.
    pub fn new(
        count: usize,
        bytes: usize,
        objects: usize,
        grow_budget: usize,
        max_objects: usize,
    ) -> ArenaPool {
        // One that could never fit the budget is refused as too large, not as busy (which a
        // client retries).
        let max_objects = max_objects.min(objects.saturating_add(grow_budget));
        let mut free = Vec::with_capacity(count);
        for _ in 0..count {
            let mut a = Box::new(Arena::new(bytes, objects));
            a.growth.base = objects.min(max_objects);
            free.push(a);
        }
        ArenaPool {
            free: Mutex::new(free),
            permits: Semaphore::new(count),
            grown: AtomicUsize::new(0),
            grow_budget,
            max_objects,
        }
    }

    /// Waits up to `timeout` for a free arena.
    pub async fn lease(&self, timeout: Duration) -> Option<Lease<'_>> {
        let permit = tokio::time::timeout(timeout, self.permits.acquire())
            .await
            .ok()?
            .ok()?;
        let mut arena = self.free.lock().ok()?.pop()?;
        // Reset outside the lock: clearing a large request's sets under it serialized every
        // lease and return behind it.
        arena.reset();
        Some(Lease {
            arena: Some(arena),
            pool: self,
            _permit: permit,
        })
    }

    /// Lets a commit expansion plan `objects` objects in all: what grown arenas hold
    /// together stays within the budget, without making expansions wait for each other.
    #[inline]
    pub fn charge(&self, g: &mut Growth, objects: usize) -> std::result::Result<(), Refused> {
        if objects <= g.base.saturating_add(g.charged) {
            return Ok(());
        }
        self.charge_more(g, objects)
    }

    #[cold]
    fn charge_more(&self, g: &mut Growth, objects: usize) -> std::result::Result<(), Refused> {
        if objects > self.max_objects {
            return Err(Refused::TooLarge);
        }
        // Its structures grow by doubling: charge the room they grow to.
        let mut room = g.base.max(1);
        while room < objects {
            room = room.saturating_mul(2);
        }
        let want = room
            .saturating_sub(g.base)
            .min(self.max_objects.saturating_sub(g.base));
        let more = want.saturating_sub(g.charged);
        self.grown
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |t| {
                t.checked_add(more).filter(|&n| n <= self.grow_budget)
            })
            .map_err(|_| Refused::Busy)?;
        g.charged = want;
        Ok(())
    }

    /// Room for objects that grown arenas have beyond their size now.
    pub fn grown(&self) -> usize {
        self.grown.load(Ordering::Relaxed)
    }
}

impl Lease<'_> {
    pub fn arena(&mut self) -> Option<&mut Arena> {
        self.arena.as_deref_mut()
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if let Some(mut a) = self.arena.take() {
            // Grown memory goes before its charge does: what grown arenas hold stays within
            // the budget.
            a.shrink();
            let charged = std::mem::take(&mut a.growth.charged);
            if charged > 0 {
                self.pool.grown.fetch_sub(charged, Ordering::Relaxed);
            }
            if let Ok(mut free) = self.pool.free.lock() {
                // Capacity was reserved for every arena at startup: never reallocates.
                free.push(a);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_set_insert_contains_clear() {
        let mut s = OidSet::with_capacity(4);
        let a = Oid([1; 20]);
        let mut b = Oid([1; 20]);
        b.0[19] = 2; // same home slot as `a`
        assert!(s.insert(&a).unwrap());
        assert!(!s.insert(&a).unwrap());
        assert!(s.insert(&b).unwrap());
        assert!(s.contains(&a) && s.contains(&b) && !s.contains(&Oid([3; 20])));
        s.insert(&Oid([4; 20])).unwrap();
        s.insert(&Oid([5; 20])).unwrap();
        assert!(s.insert(&Oid([6; 20])).is_err(), "capacity is enforced");
        s.clear();
        assert!(!s.contains(&a) && s.is_empty());
    }

    #[test]
    fn pool_leases_and_returns() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = ArenaPool::new(1, 4096, 16, 0, 16);
            let l = pool.lease(Duration::from_millis(10)).await.unwrap();
            assert!(pool.lease(Duration::from_millis(10)).await.is_none());
            drop(l);
            assert!(pool.lease(Duration::from_millis(10)).await.is_some());
        });
    }

    /// Commit expansions grow arenas within a shared budget, each up to its own cap; a
    /// grown arena is back to its size, and its charge back in the budget, when its lease
    /// ends.
    #[test]
    fn growth_is_bounded_and_ends_with_the_lease() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let wait = Duration::from_millis(10);
            // Base 16 objects, a budget of 48, each request at most 56.
            let pool = ArenaPool::new(2, 4096, 16, 48, 56);
            let mut a = pool.lease(wait).await.unwrap();
            let mut b = pool.lease(wait).await.unwrap();
            let ga = &mut a.arena().unwrap().growth;
            assert_eq!(pool.charge(ga, 16), Ok(()));
            assert_eq!(pool.grown(), 0);
            assert_eq!(pool.charge(ga, 17), Ok(()));
            assert_eq!(pool.grown(), 16, "room doubles");
            assert_eq!(pool.charge(ga, 32), Ok(()));
            assert_eq!(pool.grown(), 16);
            assert_eq!(pool.charge(ga, 33), Ok(()));
            assert_eq!(pool.grown(), 40, "never past the request's own cap");
            assert_eq!(pool.charge(ga, 57), Err(Refused::TooLarge));
            let gb = &mut b.arena().unwrap().growth;
            assert_eq!(pool.charge(gb, 17), Err(Refused::Busy));
            let grown = a.arena().unwrap();
            grown.grow = true;
            for i in 0..1000u32 {
                let mut id = Oid::ZERO;
                id.0[..4].copy_from_slice(&(i + 1).to_le_bytes());
                assert!(grown.seen.insert_grow(&id, true).unwrap());
                assert!(push(&mut grown.work, id, true));
            }
            assert!(grown.seen.len() == 1000 && grown.work.capacity() >= 1000);
            drop(a);
            assert_eq!(pool.grown(), 0);
            assert_eq!(pool.charge(&mut b.arena().unwrap().growth, 17), Ok(()));
            drop(b);
            assert_eq!(pool.grown(), 0);
            for _ in 0..2 {
                let mut l = pool.lease(wait).await.unwrap();
                let arena = l.arena().unwrap();
                assert!(!arena.grow);
                assert!(arena.work.capacity() <= 32, "{}", arena.work.capacity());
                assert!(arena.seen.is_empty());
            }
        });
    }
}
