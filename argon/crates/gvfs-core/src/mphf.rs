//! Packed-u64 Minimal Perfect Hash Function (`MphfSlabIndex`) and `PackedObjLoc` (PDF §4.2).
//!
//! Gives `gvfs-daemon` $O(1)$ (~15 ns) lookups into any Merkle-ordered `.pack` slab up to
//! 16 GiB using a single bit-packed `u64` descriptor per object:
//!
//! ```text
//! ┌────────────────────┬──────────────────────────────────────────┬────────────────────┐
//! │   16-bit SHA-1 FP  │          34-bit Pack Byte Offset         │  14-bit Packed Len │
//! │     (bits 63..48)  │               (bits 47..14)              │     (bits 13..0)   │
//! └────────────────────┴──────────────────────────────────────────┴────────────────────┘
//! ```
//!
//! - `16-bit FP`: `u16::from_be_bytes([oid[18], oid[19]])` -> 99.9985% false-positive rejection.
//! - `34-bit Offset`: Supports single monolithic L3 pack slabs up to 16 GiB (17,179,869,184 bytes).
//! - `14-bit Length`: Direct compressed byte length up to 16,382 bytes (`0x3FFF` = overflow table
//!   lookup for objects exceeding 16,382 bytes).

use crate::buf::{ByteWriter, Overflow, be32, be64, sub};
use crate::oid::Oid20;

pub const MPHF_MAGIC: &[u8; 8] = b"GMPH\x00\x00\x00\x01";
pub const MPHF_HEADER_LEN: usize = 32;

/// Packs `(fp16, offset34, len14)` into a single 64-bit integer (PDF §4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct PackedObjLoc(pub u64);

impl PackedObjLoc {
    pub const MAX_INLINE_LEN: u32 = 0x3FFE; // 16,382 bytes
    pub const OVERFLOW_LEN_MARKER: u32 = 0x3FFF;
    pub const MAX_OFFSET: u64 = (1u64 << 34) - 1;

    #[inline]
    pub fn new(oid: &Oid20, offset: u64, compressed_len: u32) -> Option<Self> {
        if offset > Self::MAX_OFFSET {
            return None;
        }
        let fp16 = oid.fp16();
        let len_field = if compressed_len > Self::MAX_INLINE_LEN {
            u64::from(Self::OVERFLOW_LEN_MARKER)
        } else {
            u64::from(compressed_len)
        };
        let packed = (u64::from(fp16) << 48) | ((offset & 0x3_FFFF_FFFF) << 14) | len_field;
        Some(Self(packed))
    }

    #[inline]
    pub fn matches_fp(self, oid: &Oid20) -> bool {
        ((self.0 >> 48) as u16) == oid.fp16()
    }

    #[inline]
    pub fn byte_offset(self) -> u64 {
        (self.0 >> 14) & 0x3_FFFF_FFFF
    }

    #[inline]
    pub fn inline_len(self) -> Option<u32> {
        let raw = (self.0 & 0x3FFF) as u32;
        if raw == Self::OVERFLOW_LEN_MARKER {
            None
        } else {
            Some(raw)
        }
    }
}

/// PTHash-style Minimal Perfect Hash table over a Merkle-DFS `.pack` slab (PDF §4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MphfSlabIndex {
    pub seed: u64,
    pub num_buckets: u32,
    /// Pilot values (1 byte per bucket, ~3.2 bits/key) resolving bucket collisions.
    pub pilots: Box<[u8]>,
    /// Displaced free slots for keys whose pilot hash falls in `[n .. total_table)`.
    pub remap: Box<[u32]>,
    /// 1 `PackedObjLoc` (`u64`) per object, indexed by `mphf_slot(oid)`.
    pub descriptors: Box<[PackedObjLoc]>,
    /// Sorted `(byte_offset, compressed_len)` entries for the <0.1% of objects with `compressed_len > 16,382`.
    pub overflow: Box<[(u64, u32)]>,
}

impl Default for MphfSlabIndex {
    fn default() -> Self {
        Self::empty()
    }
}

impl MphfSlabIndex {
    pub fn empty() -> Self {
        Self {
            seed: 0,
            num_buckets: 0,
            pilots: Box::new([]),
            remap: Box::new([]),
            descriptors: Box::new([]),
            overflow: Box::new([]),
        }
    }

    #[inline]
    pub fn hash64(oid: &Oid20, seed: u64) -> u64 {
        let [
            a0,
            a1,
            a2,
            a3,
            a4,
            a5,
            a6,
            a7,
            b0,
            b1,
            b2,
            b3,
            b4,
            b5,
            b6,
            b7,
            c0,
            c1,
            c2,
            c3,
        ] = oid.0;
        let a = u64::from_le_bytes([a0, a1, a2, a3, a4, a5, a6, a7]).wrapping_add(seed);
        let b = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7])
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let c = u64::from(u32::from_le_bytes([c0, c1, c2, c3])).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let mut h = a ^ b.rotate_left(27) ^ c;
        h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        h ^ (h >> 31)
    }

    /// Builds a PTHash Minimal Perfect Hash table over `(oid, offset, compressed_len)` triples.
    pub fn build(items: &[(Oid20, u64, u32)]) -> Option<Self> {
        let n = items.len();
        if n == 0 {
            return Some(Self::empty());
        }
        // Number of buckets ~ ceil(n / 2.5), at least 1.
        let num_buckets_usize = n.saturating_mul(2).div_ceil(5).max(1);
        let num_buckets = u32::try_from(num_buckets_usize).ok()?;
        // Total table size ~ 1.25 * n (at least n + 8) so 1-byte pilots (0..=255) always succeed.
        let total_table = n.saturating_add(n.div_ceil(4)).max(n.saturating_add(8));

        let mut overflow_vec: Vec<(u64, u32)> = items
            .iter()
            .filter(|&&(_, _, len)| len > PackedObjLoc::MAX_INLINE_LEN)
            .map(|&(_, off, len)| (off, len))
            .collect();
        overflow_vec.sort_unstable_by_key(|&(off, _)| off);

        for attempt in 0..64u64 {
            let seed =
                0x517C_C1B7_2722_0A95u64.wrapping_add(attempt.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            if let Some(index) =
                Self::try_build_with_seed(items, seed, num_buckets, total_table, &overflow_vec)
            {
                return Some(index);
            }
        }
        None
    }

    fn try_build_with_seed(
        items: &[(Oid20, u64, u32)],
        seed: u64,
        num_buckets: u32,
        total_table: usize,
        overflow_vec: &[(u64, u32)],
    ) -> Option<Self> {
        let n = items.len();
        let nb = usize::try_from(num_buckets).ok()?;
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); nb];
        for (idx, (oid, _, _)) in items.iter().enumerate() {
            let h = Self::hash64(oid, seed);
            let rem = h.checked_rem(u64::from(num_buckets))?;
            let b = usize::try_from(rem).ok()?;
            buckets.get_mut(b)?.push(idx);
        }

        let mut bucket_order: Vec<usize> = (0..nb).collect();
        bucket_order.sort_unstable_by(|&a, &b| {
            let la = buckets.get(a).map_or(0, Vec::len);
            let lb = buckets.get(b).map_or(0, Vec::len);
            lb.cmp(&la).then_with(|| a.cmp(&b))
        });

        let mut pilots = vec![0u8; nb];
        let mut occupied: Vec<Option<usize>> = vec![None; total_table];
        let mut trial_positions: Vec<usize> = Vec::with_capacity(32);

        for b in bucket_order {
            let keys = buckets.get(b)?;
            if keys.is_empty() {
                continue;
            }
            let mut found_pilot = None;
            'pilot_search: for pilot in 0..=255u8 {
                trial_positions.clear();
                let pilot_seed = seed.wrapping_add(u64::from(pilot));
                for &key_idx in keys {
                    let (oid, _, _) = items.get(key_idx)?;
                    let ph = Self::hash64(oid, pilot_seed);
                    let rem = ph.checked_rem(total_table as u64)?;
                    let pos = usize::try_from(rem).ok()?;
                    if occupied.get(pos)?.is_some() || trial_positions.contains(&pos) {
                        continue 'pilot_search;
                    }
                    trial_positions.push(pos);
                }
                found_pilot = Some(pilot);
                break;
            }
            let pilot = found_pilot?;
            *pilots.get_mut(b)? = pilot;
            for (&key_idx, &pos) in keys.iter().zip(trial_positions.iter()) {
                *occupied.get_mut(pos)? = Some(key_idx);
            }
        }

        // Collect free slots in `[0 .. n)` and remap displaced slots in `[n .. total_table)`.
        let mut free_slots: Vec<u32> = Vec::new();
        for (slot, entry) in occupied.get(..n)?.iter().enumerate() {
            if entry.is_none() {
                free_slots.push(u32::try_from(slot).ok()?);
            }
        }

        let remap_len = total_table.saturating_sub(n);
        let mut remap = vec![0u32; remap_len];
        let mut free_cursor = 0usize;
        let mut descriptors = vec![PackedObjLoc::default(); n];

        for (pos, entry) in occupied.iter().enumerate() {
            let Some(key_idx) = *entry else {
                continue;
            };
            let target_slot = if pos < n {
                pos
            } else {
                let free_slot = *free_slots.get(free_cursor)?;
                free_cursor = free_cursor.saturating_add(1);
                let remap_idx = pos.saturating_sub(n);
                *remap.get_mut(remap_idx)? = free_slot;
                usize::try_from(free_slot).ok()?
            };
            let &(ref oid, offset, compressed_len) = items.get(key_idx)?;
            *descriptors.get_mut(target_slot)? = PackedObjLoc::new(oid, offset, compressed_len)?;
        }

        Some(Self {
            seed,
            num_buckets,
            pilots: pilots.into_boxed_slice(),
            remap: remap.into_boxed_slice(),
            descriptors: descriptors.into_boxed_slice(),
            overflow: overflow_vec.to_vec().into_boxed_slice(),
        })
    }

    /// $O(1)$ lookup returning `(byte_offset, Option<inline_len>)` exactly as in PDF §4.2.
    #[inline]
    pub fn lookup(&self, oid: &Oid20) -> Option<(u64, Option<u32>)> {
        let n = self.descriptors.len();
        if n == 0 || self.num_buckets == 0 {
            return None;
        }
        let h = Self::hash64(oid, self.seed);
        let bucket_rem = h.checked_rem(u64::from(self.num_buckets))?;
        let bucket = usize::try_from(bucket_rem).ok()?;
        let pilot = *self.pilots.get(bucket)?;
        let pilot_hash = Self::hash64(oid, self.seed.wrapping_add(u64::from(pilot)));
        let total_table = n.saturating_add(self.remap.len());
        let pos_rem = pilot_hash.checked_rem(total_table as u64)?;
        let raw_pos = usize::try_from(pos_rem).ok()?;

        let slot = if raw_pos < n {
            raw_pos
        } else {
            let remap_idx = raw_pos.saturating_sub(n);
            usize::try_from(*self.remap.get(remap_idx)?).ok()?
        };

        let desc = *self.descriptors.get(slot)?;
        if desc.matches_fp(oid) {
            Some((desc.byte_offset(), desc.inline_len()))
        } else {
            None
        }
    }

    /// Resolves `(byte_offset, compressed_len)`, handling both inline (`<= 16,382`) and
    /// overflow (`> 16,382`) lengths in $O(1)$ / $O(\log N_{\text{overflow}})$ with zero allocations.
    #[inline]
    pub fn lookup_resolved(&self, oid: &Oid20) -> Option<(u64, u32)> {
        let (offset, inline) = self.lookup(oid)?;
        if let Some(len) = inline {
            return Some((offset, len));
        }
        let idx = self
            .overflow
            .binary_search_by_key(&offset, |&(off, _)| off)
            .ok()?;
        let &(_, len) = self.overflow.get(idx)?;
        Some((offset, len))
    }

    /// Exact byte size of the serialized `.mphf` binary representation.
    pub fn encoded_len(&self) -> Option<usize> {
        MPHF_HEADER_LEN
            .checked_add(self.pilots.len())?
            .checked_add(self.remap.len().checked_mul(4)?)?
            .checked_add(self.descriptors.len().checked_mul(8)?)?
            .checked_add(self.overflow.len().checked_mul(12)?)
    }

    /// Serializes the MPHF index into a caller-provided slice (`slab-XXXX.mphf`).
    pub fn encode_into(&self, out: &mut [u8]) -> Result<usize, Overflow> {
        let mut w = ByteWriter::new(out);
        w.put(MPHF_MAGIC)?;
        w.put_u64_be(self.seed)?;
        w.put_u32_be(self.num_buckets)?;
        w.put_u32_be(u32::try_from(self.remap.len()).map_err(|_| Overflow)?)?;
        w.put_u32_be(u32::try_from(self.descriptors.len()).map_err(|_| Overflow)?)?;
        w.put_u32_be(u32::try_from(self.overflow.len()).map_err(|_| Overflow)?)?;
        w.put(&self.pilots)?;
        for &r in &self.remap {
            w.put_u32_be(r)?;
        }
        for &d in &self.descriptors {
            w.put_u64_be(d.0)?;
        }
        for &(off, len) in &self.overflow {
            w.put_u64_be(off)?;
            w.put_u32_be(len)?;
        }
        Ok(w.len())
    }

    /// Deserializes a `.mphf` binary buffer.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.get(..8) != Some(&MPHF_MAGIC[..]) {
            return None;
        }
        let seed = be64(data, 8)?;
        let num_buckets = be32(data, 16)?;
        let remap_len = usize::try_from(be32(data, 20)?).ok()?;
        let desc_len = usize::try_from(be32(data, 24)?).ok()?;
        let overflow_len = usize::try_from(be32(data, 28)?).ok()?;
        let pilots_len = usize::try_from(num_buckets).ok()?;

        let mut at = MPHF_HEADER_LEN;
        let pilots = sub(data, at, pilots_len)?.to_vec().into_boxed_slice();
        at = at.checked_add(pilots_len)?;

        let mut remap = Vec::with_capacity(remap_len);
        for _ in 0..remap_len {
            remap.push(be32(data, at)?);
            at = at.checked_add(4)?;
        }

        let mut descriptors = Vec::with_capacity(desc_len);
        for _ in 0..desc_len {
            descriptors.push(PackedObjLoc(be64(data, at)?));
            at = at.checked_add(8)?;
        }

        let mut overflow = Vec::with_capacity(overflow_len);
        for _ in 0..overflow_len {
            let off = be64(data, at)?;
            let len = be32(data, at.checked_add(8)?)?;
            overflow.push((off, len));
            at = at.checked_add(12)?;
        }

        Some(Self {
            seed,
            num_buckets,
            pilots,
            remap: remap.into_boxed_slice(),
            descriptors: descriptors.into_boxed_slice(),
            overflow: overflow.into_boxed_slice(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mphf_builds_and_resolves_all_keys_with_zero_collisions() {
        let items: Vec<(Oid20, u64, u32)> = (0..2048u32)
            .map(|i| {
                let mut raw = [0u8; 20];
                let h = MphfSlabIndex::hash64(&Oid20([i as u8; 20]), u64::from(i));
                raw[0..8].copy_from_slice(&h.to_le_bytes());
                raw[8..12].copy_from_slice(&i.to_be_bytes());
                raw[18..20].copy_from_slice(&(i as u16 ^ 0x5A5A).to_be_bytes());
                let offset = u64::from(i) * 1000 + 12;
                let len = if i % 257 == 0 { 50_000 } else { 80 + (i % 500) };
                (Oid20(raw), offset, len)
            })
            .collect();

        let mphf = MphfSlabIndex::build(&items).unwrap();
        for &(ref oid, off, len) in &items {
            assert_eq!(mphf.lookup_resolved(oid), Some((off, len)));
        }

        let mut encoded = vec![0u8; mphf.encoded_len().unwrap()];
        let n = mphf.encode_into(&mut encoded).unwrap();
        assert_eq!(n, encoded.len());
        let decoded = MphfSlabIndex::decode(&encoded).unwrap();
        assert_eq!(decoded, mphf);
    }
}
