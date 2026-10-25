//! Request scratch memory: allocated once at startup, leased per request, reset in O(1).
//!
//! Every per-request structure (the request body, parsed ids, the dedupe set, the splice
//! plan, resolver buffers, small response bodies) lives in an [`Arena`]. A request leases
//! one from the [`ArenaPool`] (blocking briefly when all are busy: built-in backpressure)
//! and returns it on drop, so steady-state serving performs no heap allocation.

use std::sync::{Condvar, Mutex};
use std::time::Duration;

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
    /// Request body.
    pub body: Vec<u8>,
    /// Object ids parsed from the request.
    pub oids: Vec<Oid>,
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
    /// Blobs of the request (`--blobs separate` stores): their packs and records.
    pub blobs: Vec<(
        std::sync::Arc<crate::serve::blobs::BlobPack>,
        crate::blobs::BlobRec,
    )>,
    /// Commit expansion may outgrow the arena: allow its structures to grow (counted as
    /// cold allocations) for this request; `reset` shrinks them back.
    pub grow: bool,
    objects: usize,
}

/// Blobs one request holds without growing (gvfs-helper asks for a few thousand at once).
const BLOBS: usize = 1 << 13;

impl Arena {
    /// `bytes` is split between the body, response and splice buffers; `objects` bounds
    /// the ids one request may name or expand to.
    pub fn new(bytes: usize, objects: usize) -> Arena {
        let quarter = bytes / 4;
        let mut a = Arena {
            body: Vec::with_capacity(quarter),
            oids: Vec::with_capacity(objects),
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
            grow: false,
            objects,
        };
        // Touch every page now so requests never take first-touch faults.
        a.body.resize(a.body.capacity(), 0);
        a.out.resize(a.out.capacity(), 0);
        a.reset();
        a
    }

    pub fn reset(&mut self) {
        if self.grow {
            let n = self.objects;
            self.seen.shrink(n);
            self.visited.shrink(n);
            shrink(&mut self.work, n);
            self.splice.shrink(n);
            self.grow = false;
        }
        self.body.clear();
        self.oids.clear();
        self.seen.clear();
        self.visited.clear();
        self.work.clear();
        self.commits.clear();
        self.next_commits.clear();
        self.splice.clear();
        self.out.clear();
        self.blobs.clear();
        if self.blobs.capacity() > BLOBS.saturating_mul(2) {
            let _cold = gvfs_alloc_guard::cold_section();
            self.blobs.shrink_to(BLOBS);
        }
    }
}

/// A fixed set of arenas leased by requests.
pub struct ArenaPool {
    free: Mutex<Vec<Arena>>,
    available: Condvar,
}

/// A leased arena; returned to the pool on drop.
pub struct Lease<'a> {
    arena: Option<Arena>,
    pool: &'a ArenaPool,
}

impl ArenaPool {
    pub fn new(count: usize, bytes: usize, objects: usize) -> ArenaPool {
        let mut free = Vec::with_capacity(count);
        for _ in 0..count {
            free.push(Arena::new(bytes, objects));
        }
        ArenaPool {
            free: Mutex::new(free),
            available: Condvar::new(),
        }
    }

    /// Waits up to `timeout` for a free arena.
    pub fn lease(&self, timeout: Duration) -> Option<Lease<'_>> {
        let guard = self.free.lock().ok()?;
        let (mut guard, _) = self
            .available
            .wait_timeout_while(guard, timeout, |f| f.is_empty())
            .ok()?;
        let mut arena = guard.pop()?;
        arena.reset();
        Some(Lease {
            arena: Some(arena),
            pool: self,
        })
    }
}

impl Lease<'_> {
    pub fn arena(&mut self) -> Option<&mut Arena> {
        self.arena.as_mut()
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if let Some(a) = self.arena.take() {
            if let Ok(mut free) = self.pool.free.lock() {
                // Capacity was reserved for every arena at startup: never reallocates.
                free.push(a);
            }
            self.pool.available.notify_one();
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
        let pool = ArenaPool::new(1, 4096, 16);
        let l = pool.lease(Duration::from_millis(10)).unwrap();
        assert!(pool.lease(Duration::from_millis(10)).is_none());
        drop(l);
        assert!(pool.lease(Duration::from_millis(10)).is_some());
    }
}
