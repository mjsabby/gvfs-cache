//! Git pack reverse index (`.rev`, `RIDX` v1) zero-copy view.
//!
//! A `.rev` file maps pack-order position `k in 0..N` to its `.idx` position `rev[k]`,
//! sorted by ascending byte offset in the `.pack` file. Combined with [`IdxView`], it
//! resolves the exact `(offset, compressed_len)` of any entry in a memory-mapped pack
//! (like `/home/muks/SearchGold/.git/objects/pack/pack-*.pack`) with zero heap allocations.

use crate::buf::be32;
use crate::idx::IdxView;
use crate::oid::Oid20;

pub const REV_MAGIC: &[u8; 4] = b"RIDX";
pub const REV_VERSION: u32 = 1;
const REV_HEADER_LEN: usize = 12;
const REV_TRAILER_LEN: usize = 40;

#[derive(Debug, Clone, Copy)]
pub struct RevView<'a> {
    data: &'a [u8],
    n: usize,
}

impl<'a> RevView<'a> {
    pub fn parse(data: &'a [u8], expected_objects: usize) -> Option<Self> {
        if data.get(..4) != Some(&REV_MAGIC[..]) {
            return None;
        }
        if be32(data, 4) != Some(REV_VERSION) {
            return None;
        }
        // Hash version 1 = SHA-1
        if be32(data, 8) != Some(1) {
            return None;
        }
        let body_bytes = expected_objects.checked_mul(4)?;
        let expected_len = REV_HEADER_LEN
            .checked_add(body_bytes)?
            .checked_add(REV_TRAILER_LEN)?;
        if data.len() != expected_len {
            return None;
        }
        Some(Self {
            data,
            n: expected_objects,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.n
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Returns the `.idx` index of the `k`-th object in pack order (`0..n`).
    #[inline]
    pub fn idx_pos_at_pack_order(&self, k: usize) -> Option<usize> {
        if k >= self.n {
            return None;
        }
        let at = REV_HEADER_LEN.checked_add(k.checked_mul(4)?)?;
        usize::try_from(be32(self.data, at)?).ok()
    }

    /// Returns `(oid, offset, compressed_len, crc32)` of the `k`-th object in pack order.
    #[inline]
    pub fn entry_at_pack_order(
        &self,
        k: usize,
        idx: &IdxView<'_>,
        pack_total_len: u64,
    ) -> Option<(Oid20, u64, u32, u32)> {
        let idx_pos = self.idx_pos_at_pack_order(k)?;
        let oid = idx.oid(idx_pos)?;
        let crc = idx.crc32(idx_pos)?;
        let offset = idx.offset(idx_pos)?;
        let next_offset = if k.saturating_add(1) < self.n {
            let next_idx_pos = self.idx_pos_at_pack_order(k.saturating_add(1))?;
            idx.offset(next_idx_pos)?
        } else {
            pack_total_len.checked_sub(20)?
        };
        let len_u64 = next_offset.checked_sub(offset)?;
        let compressed_len = u32::try_from(len_u64).ok()?;
        Some((oid, offset, compressed_len, crc))
    }

    /// Finds `(offset, compressed_len, crc32)` for `oid` by looking up `oid` in `idx`
    /// and binary-searching `rev` by `offset` to find the next entry's offset.
    pub fn lookup_oid(
        &self,
        oid: &Oid20,
        idx: &IdxView<'_>,
        pack_total_len: u64,
    ) -> Option<(u64, u32, u32)> {
        let idx_pos = idx.find(oid)?;
        let target_offset = idx.offset(idx_pos)?;
        let crc = idx.crc32(idx_pos)?;

        let mut lo = 0usize;
        let mut hi = self.n;
        let mut found_k = None;
        while lo < hi {
            let mid = lo.checked_add(hi.checked_sub(lo)?.wrapping_div(2))?;
            let mid_idx = self.idx_pos_at_pack_order(mid)?;
            let mid_off = idx.offset(mid_idx)?;
            match mid_off.cmp(&target_offset) {
                core::cmp::Ordering::Less => lo = mid.checked_add(1)?,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => {
                    found_k = Some(mid);
                    break;
                }
            }
        }
        let k = found_k?;
        let next_offset = if k.saturating_add(1) < self.n {
            let next_idx = self.idx_pos_at_pack_order(k.saturating_add(1))?;
            idx.offset(next_idx)?
        } else {
            pack_total_len.checked_sub(20)?
        };
        let compressed_len = u32::try_from(next_offset.checked_sub(target_offset)?).ok()?;
        Some((target_offset, compressed_len, crc))
    }
}
