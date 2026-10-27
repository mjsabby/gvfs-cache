//! A blocked Bloom filter over object ids, so a lookup can rule a slab out in one cache line
//! instead of computing its MPHF slot and reading the record there.
//!
//! [`crate::serve::view::View::find`] probes the newest slab first and the baseline last, so
//! an id the baseline holds (most of what clients ask for) used to cost a slot computation
//! and a record read in every newer slab before it. Ids are digests, uniform already, so a
//! block and the bits in it are just bytes of the id. There are no false negatives (every
//! id of the slab is in its filter), so a lookup answers exactly as it would without it.

use crate::util::{Oid, u64_of, us};

/// Bits per id: about 0.2% false positives with four bits per id.
const BITS_PER_ID: usize = 16;
/// Bits of a block (one cache line).
const BLOCK_BITS: usize = 512;

pub struct IdFilter {
    blocks: Vec<[u64; 8]>,
    mask: u64,
}

impl IdFilter {
    /// A filter holding `ids` (`n` of them).
    pub fn new(n: usize, ids: impl Iterator<Item = Oid>) -> IdFilter {
        let blocks = n
            .saturating_mul(BITS_PER_ID)
            .div_ceil(BLOCK_BITS)
            .max(1)
            .next_power_of_two();
        let mut f = IdFilter {
            blocks: vec![[0u64; 8]; blocks],
            mask: u64_of(blocks.saturating_sub(1)),
        };
        for id in ids {
            let (b, bits) = f.place(&id);
            if let Some(block) = f.blocks.get_mut(b) {
                for i in bits {
                    if let Some(w) = block.get_mut(usize::from(i.wrapping_shr(6))) {
                        *w |= 1u64.wrapping_shl(u32::from(i & 63));
                    }
                }
            }
        }
        f
    }

    /// The block (bytes 0..8 of the id) and four bit positions in it (bytes 8..16).
    #[inline]
    fn place(&self, oid: &Oid) -> (usize, [u16; 4]) {
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
            ..,
        ] = oid.0;
        let block = us(u64::from_le_bytes([a0, a1, a2, a3, a4, a5, a6, a7]) & self.mask);
        let h = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]);
        let bit = |k: u32| (h.wrapping_shr(k.wrapping_mul(9)) & 511) as u16;
        (block, [bit(0), bit(1), bit(2), bit(3)])
    }

    /// False: the id is not in the filter. True: it may be.
    #[inline]
    pub fn may_contain(&self, oid: &Oid) -> bool {
        let (b, bits) = self.place(oid);
        let Some(block) = self.blocks.get(b) else {
            return true;
        };
        bits.iter().all(|&i| {
            block
                .get(usize::from(i.wrapping_shr(6)))
                .is_none_or(|w| w.wrapping_shr(u32::from(i & 63)) & 1 == 1)
        })
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

    fn ids(n: usize, salt: u8) -> Vec<Oid> {
        (0..n)
            .map(|i| {
                let mut h = ring::digest::Context::new(&ring::digest::SHA256);
                h.update(&[salt]);
                h.update(&(i as u64).to_le_bytes());
                Oid::from_slice(&h.finish().as_ref()[..20]).unwrap()
            })
            .collect()
    }

    #[test]
    fn no_false_negatives_few_false_positives() {
        for n in [0usize, 1, 7, 1000, 50_000] {
            let have = ids(n, 1);
            let f = IdFilter::new(n, have.iter().copied());
            assert!(
                have.iter().all(|o| f.may_contain(o)),
                "false negative at n={n}"
            );
            let others = ids(20_000, 2);
            let fp = others.iter().filter(|o| f.may_contain(o)).count();
            assert!(fp < 200, "{fp} false positives of 20000 at n={n}");
        }
    }
}
