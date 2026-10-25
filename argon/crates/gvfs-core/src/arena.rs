//! Pre-allocated arena & slab primitives (DESIGN.md §2):
//!
//! 1. [`ScratchArena`] — resettable per-worker byte + OID scratch buffer ($O(1)$ reset).
//! 2. [`SeenOidSet`] — pre-allocated open-addressed `Oid20` deduplication set that resets in
//!    $O(\text{touched})$ without ever touching the heap (used by `plan_deduplicated_range_splice`).
//! 3. [`PackSlabPool`] — startup-allocated, pre-faulted pool of shared `BytesMut` slabs so
//!    `split().freeze()` produces zero-copy `Bytes` slices with zero heap allocations.

use bytes::{Bytes, BytesMut};

use crate::buf::{prefault, prefault_capacity};
use crate::oid::Oid20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArenaSpan {
    pub offset: u32,
    pub len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArenaError {
    OutOfByteCapacity,
    OutOfOidCapacity,
    InvalidSliceBounds,
}

impl core::fmt::Display for ArenaError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::OutOfByteCapacity => "scratch arena byte capacity exhausted",
            Self::OutOfOidCapacity => "scratch arena oid capacity exhausted",
            Self::InvalidSliceBounds => "invalid scratch arena span",
        })
    }
}

impl std::error::Error for ArenaError {}

/// Write handle on a [`ScratchArena`]'s OID area so the caller can read a byte span in
/// `bytes` while simultaneously pushing parsed `Oid20`s into `oids`.
#[derive(Debug)]
pub struct OidSink<'a> {
    oids: &'a mut [Oid20],
    cursor: &'a mut usize,
}

impl OidSink<'_> {
    #[inline]
    pub fn push(&mut self, oid: Oid20) -> Result<(), ArenaError> {
        let slot = self
            .oids
            .get_mut(*self.cursor)
            .ok_or(ArenaError::OutOfOidCapacity)?;
        *slot = oid;
        *self.cursor = self.cursor.saturating_add(1);
        Ok(())
    }
}

/// Safe resettable scratch arena (DESIGN.md §2.1).
#[derive(Debug)]
pub struct ScratchArena {
    bytes: Vec<u8>,
    bytes_cursor: usize,
    oids: Vec<Oid20>,
    oids_cursor: usize,
}

impl ScratchArena {
    /// Called ONCE at startup per worker / task. Pre-faults all pages immediately.
    pub fn with_capacity(byte_cap: usize, oid_cap: usize) -> Self {
        let mut bytes = vec![0u8; byte_cap];
        prefault(&mut bytes);
        let mut oids = vec![Oid20([0u8; 20]); oid_cap];
        prefault(&mut oids);
        Self {
            bytes,
            bytes_cursor: 0,
            oids,
            oids_cursor: 0,
        }
    }

    /// $O(1)$ reset at the end of every HTTP request or pipeline step. Zero deallocations.
    #[inline]
    pub fn reset(&mut self) {
        self.bytes_cursor = 0;
        self.oids_cursor = 0;
    }

    /// Allocates a mutable byte slice from the pre-allocated slab.
    #[inline]
    pub fn alloc_bytes(&mut self, len: usize) -> Result<(&mut [u8], ArenaSpan), ArenaError> {
        let start = self.bytes_cursor;
        let end = start
            .checked_add(len)
            .ok_or(ArenaError::OutOfByteCapacity)?;
        let offset = u32::try_from(start).map_err(|_| ArenaError::OutOfByteCapacity)?;
        let len_u32 = u32::try_from(len).map_err(|_| ArenaError::OutOfByteCapacity)?;
        let slice = self
            .bytes
            .get_mut(start..end)
            .ok_or(ArenaError::OutOfByteCapacity)?;
        self.bytes_cursor = end;
        Ok((
            slice,
            ArenaSpan {
                offset,
                len: len_u32,
            },
        ))
    }

    pub fn shrink_last(&mut self, span: ArenaSpan, used: u32) -> Result<ArenaSpan, ArenaError> {
        let start = usize::try_from(span.offset).map_err(|_| ArenaError::InvalidSliceBounds)?;
        let end = start
            .checked_add(usize::try_from(span.len).map_err(|_| ArenaError::InvalidSliceBounds)?)
            .ok_or(ArenaError::InvalidSliceBounds)?;
        if end != self.bytes_cursor || used > span.len {
            return Err(ArenaError::InvalidSliceBounds);
        }
        let used_usize = usize::try_from(used).map_err(|_| ArenaError::InvalidSliceBounds)?;
        self.bytes_cursor = start
            .checked_add(used_usize)
            .ok_or(ArenaError::InvalidSliceBounds)?;
        Ok(ArenaSpan {
            offset: span.offset,
            len: used,
        })
    }

    #[inline]
    pub fn bytes(&self, span: ArenaSpan) -> Result<&[u8], ArenaError> {
        let start = usize::try_from(span.offset).map_err(|_| ArenaError::InvalidSliceBounds)?;
        let len = usize::try_from(span.len).map_err(|_| ArenaError::InvalidSliceBounds)?;
        let end = start
            .checked_add(len)
            .ok_or(ArenaError::InvalidSliceBounds)?;
        self.bytes
            .get(start..end)
            .ok_or(ArenaError::InvalidSliceBounds)
    }

    #[inline]
    pub fn bytes_mut(&mut self, span: ArenaSpan) -> Result<&mut [u8], ArenaError> {
        let start = usize::try_from(span.offset).map_err(|_| ArenaError::InvalidSliceBounds)?;
        let len = usize::try_from(span.len).map_err(|_| ArenaError::InvalidSliceBounds)?;
        let end = start
            .checked_add(len)
            .ok_or(ArenaError::InvalidSliceBounds)?;
        self.bytes
            .get_mut(start..end)
            .ok_or(ArenaError::InvalidSliceBounds)
    }

    #[inline]
    pub fn bytes_remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.bytes_cursor)
    }

    #[inline]
    pub fn push_oid(&mut self, oid: Oid20) -> Result<(), ArenaError> {
        let slot = self
            .oids
            .get_mut(self.oids_cursor)
            .ok_or(ArenaError::OutOfOidCapacity)?;
        *slot = oid;
        self.oids_cursor = self.oids_cursor.saturating_add(1);
        Ok(())
    }

    #[inline]
    pub fn active_oids(&self) -> &[Oid20] {
        self.oids.get(..self.oids_cursor).unwrap_or(&[])
    }

    /// Simultaneously borrows a byte span in `self.bytes` and a mutable [`OidSink`] into `self.oids`.
    #[inline]
    pub fn bytes_and_oid_sink(
        &mut self,
        span: ArenaSpan,
    ) -> Result<(&[u8], OidSink<'_>), ArenaError> {
        let start = usize::try_from(span.offset).map_err(|_| ArenaError::InvalidSliceBounds)?;
        let len = usize::try_from(span.len).map_err(|_| ArenaError::InvalidSliceBounds)?;
        let end = start
            .checked_add(len)
            .ok_or(ArenaError::InvalidSliceBounds)?;
        let Self {
            bytes,
            oids,
            oids_cursor,
            ..
        } = self;
        let slice = bytes
            .get(start..end)
            .ok_or(ArenaError::InvalidSliceBounds)?;
        Ok((
            slice,
            OidSink {
                oids,
                cursor: oids_cursor,
            },
        ))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SeenSlot {
    oid: Oid20,
    occupied: bool,
}

/// Pre-allocated, zero-allocation open-addressed set of `Oid20`s.
///
/// Because `Oid20` is already a cryptographic SHA-1, `oid.prefix_u64()` indexes directly
/// into the power-of-two slot table without any SipHash overhead, and `touched` tracks
/// modified indices so `clear()` runs in $O(\text{touched})$ time.
#[derive(Debug)]
pub struct SeenOidSet {
    slots: Vec<SeenSlot>,
    mask: usize,
    touched: Vec<u32>,
    max_entries: usize,
}

impl SeenOidSet {
    pub fn with_capacity_log2(log2: u32) -> Self {
        let cap = 1usize.checked_shl(log2.clamp(4, 28)).unwrap_or(1 << 16);
        let max_entries = cap.wrapping_div(4).saturating_mul(3).max(1);
        let mut slots = vec![SeenSlot::default(); cap];
        prefault(&mut slots);
        let mut touched = Vec::with_capacity(max_entries);
        prefault_capacity(&mut touched, 0u32);
        Self {
            slots,
            mask: cap.wrapping_sub(1),
            touched,
            max_entries,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.touched.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.touched.is_empty()
    }

    /// Clears only the slots touched during the current operation in $O(\text{touched})$.
    #[inline]
    pub fn clear(&mut self) {
        for &idx in &self.touched {
            if let Some(slot) = self
                .slots
                .get_mut(usize::try_from(idx).unwrap_or(usize::MAX))
            {
                slot.occupied = false;
            }
        }
        self.touched.clear();
    }

    #[inline]
    pub fn contains(&self, oid: &Oid20) -> bool {
        let mut idx = usize::try_from(oid.prefix_u64()).unwrap_or(0) & self.mask;
        for _ in 0..self.slots.len() {
            let Some(slot) = self.slots.get(idx) else {
                return false;
            };
            if !slot.occupied {
                return false;
            }
            if slot.oid == *oid {
                return true;
            }
            idx = idx.wrapping_add(1) & self.mask;
        }
        false
    }

    /// Inserts `oid`. Returns `Some(true)` if newly inserted, `Some(false)` if already
    /// present, or `None` if capacity is reached.
    #[inline]
    pub fn insert(&mut self, oid: Oid20) -> Option<bool> {
        let mut idx = usize::try_from(oid.prefix_u64()).unwrap_or(0) & self.mask;
        let cap = self.slots.len();
        for _ in 0..cap {
            let slot = self.slots.get_mut(idx)?;
            if !slot.occupied {
                if self.touched.len() >= self.max_entries {
                    return None;
                }
                slot.oid = oid;
                slot.occupied = true;
                self.touched.push(u32::try_from(idx).ok()?);
                return Some(true);
            }
            if slot.oid == oid {
                return Some(false);
            }
            idx = idx.wrapping_add(1) & self.mask;
        }
        None
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct MapSlot {
    oid: Oid20,
    value: u32,
    occupied: bool,
}

/// Pre-allocated, zero-allocation open-addressed map from `Oid20` to `u32` (such as an
/// object's index in `IngestArena::decoded` or its byte offset in an output pack).
/// Resets in $O(\text{touched})$ time without ever touching the heap.
#[derive(Debug)]
pub struct OidIdxMap {
    slots: Vec<MapSlot>,
    mask: usize,
    touched: Vec<u32>,
    max_entries: usize,
}

impl OidIdxMap {
    pub fn with_capacity_log2(log2: u32) -> Self {
        let cap = 1usize.checked_shl(log2.clamp(4, 28)).unwrap_or(1 << 16);
        let max_entries = cap.wrapping_div(4).saturating_mul(3).max(1);
        let mut slots = vec![MapSlot::default(); cap];
        prefault(&mut slots);
        let mut touched = Vec::with_capacity(max_entries);
        prefault_capacity(&mut touched, 0u32);
        Self {
            slots,
            mask: cap.wrapping_sub(1),
            touched,
            max_entries,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        for &idx in &self.touched {
            if let Some(slot) = self
                .slots
                .get_mut(usize::try_from(idx).unwrap_or(usize::MAX))
            {
                slot.occupied = false;
            }
        }
        self.touched.clear();
    }

    #[inline]
    pub fn get(&self, oid: &Oid20) -> Option<u32> {
        let mut idx = usize::try_from(oid.prefix_u64()).unwrap_or(0) & self.mask;
        for _ in 0..self.slots.len() {
            let slot = self.slots.get(idx)?;
            if !slot.occupied {
                return None;
            }
            if slot.oid == *oid {
                return Some(slot.value);
            }
            idx = idx.wrapping_add(1) & self.mask;
        }
        None
    }

    #[inline]
    pub fn insert(&mut self, oid: Oid20, value: u32) -> Option<bool> {
        let mut idx = usize::try_from(oid.prefix_u64()).unwrap_or(0) & self.mask;
        let cap = self.slots.len();
        for _ in 0..cap {
            let slot = self.slots.get_mut(idx)?;
            if !slot.occupied {
                if self.touched.len() >= self.max_entries {
                    return None;
                }
                slot.oid = oid;
                slot.value = value;
                slot.occupied = true;
                self.touched.push(u32::try_from(idx).ok()?);
                return Some(true);
            }
            if slot.oid == oid {
                return Some(false);
            }
            idx = idx.wrapping_add(1) & self.mask;
        }
        None
    }
}

/// Pre-allocated, pre-faulted pool of shared `BytesMut` slabs (DESIGN.md §2).
#[derive(Debug)]
pub struct PackSlabPool {
    slabs: Vec<BytesMut>,
    slab_size: usize,
}

impl PackSlabPool {
    pub fn new(count: usize, slab_size: usize) -> Self {
        let mut slabs = Vec::with_capacity(count);
        for _ in 0..count {
            let mut b = BytesMut::with_capacity(slab_size);
            b.resize(slab_size, 0);
            b.clear();
            drop(b.split_to(0)); // promote to shared Arc backing once at boot
            slabs.push(b);
        }
        Self { slabs, slab_size }
    }

    /// Emits `data` as a zero-copy `Bytes` slice from the pre-allocated slab pool,
    /// reclaiming slabs whose previous readers have finished.
    pub fn alloc_slice(&mut self, data: &[u8]) -> Option<Bytes> {
        for slab in &mut self.slabs {
            if slab.capacity().saturating_sub(slab.len()) < data.len() && slab.is_empty() {
                let _ = slab.try_reclaim(self.slab_size);
            }
            if slab.capacity().saturating_sub(slab.len()) >= data.len() {
                slab.extend_from_slice(data);
                return Some(slab.split().freeze());
            }
        }
        None
    }

    /// Fills a slice of up to `max_len` bytes in place inside a pre-allocated slab and
    /// returns the frozen zero-copy `Bytes`.
    pub fn alloc_with<F>(&mut self, max_len: usize, fill: F) -> Option<Bytes>
    where
        F: FnOnce(&mut BytesMut) -> Option<()>,
    {
        for slab in &mut self.slabs {
            if slab.capacity().saturating_sub(slab.len()) < max_len && slab.is_empty() {
                let _ = slab.try_reclaim(self.slab_size);
            }
            if slab.capacity().saturating_sub(slab.len()) >= max_len {
                if fill(slab).is_none() {
                    slab.clear();
                    return None;
                }
                return Some(slab.split().freeze());
            }
        }
        None
    }

    pub fn slab_size(&self) -> usize {
        self.slab_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seen_oid_set_deduplicates_and_resets_without_allocating() {
        let mut set = SeenOidSet::with_capacity_log2(8);
        let a = Oid20([1u8; 20]);
        let b = Oid20([2u8; 20]);
        assert_eq!(set.insert(a), Some(true));
        assert_eq!(set.insert(a), Some(false));
        assert_eq!(set.insert(b), Some(true));
        assert!(set.contains(&a));
        assert!(set.contains(&b));
        set.clear();
        assert!(!set.contains(&a));
        assert_eq!(set.insert(a), Some(true));
    }
}
