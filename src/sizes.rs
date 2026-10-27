//! Sizes tables: the side table of blob sizes that travels with commits and trees when a
//! store keeps blobs in their own packs (`--blobs separate`).
//!
//! Every metadata slab of such a store has one (`slabs/<id>.sizes`): the sizes of the blobs
//! its new trees introduce. A rollup's table is the union of its parts', so a prefetch plan
//! carries the sizes for exactly the trees it carries, and a client that sent
//! `X-GVFS-Sizes: 1` gets them after the packs (a `GSZ1` section).
//!
//! ```text
//! "GVDSIZE1" | count u64 | count x (object id [20] | size u64), sorted by id | SHA-256 [32]
//! ```
//!
//! Records are fixed-size and sorted, so a lookup is a binary search over the mapped file,
//! and a `GSZ1` section is the records copied verbatim.

use std::fs::File;
use std::path::Path;

use crate::slab::Region;
use crate::util::{Context, Oid, Result, le_u64, sub, u64_of, us};

pub const MAGIC: &[u8; 8] = b"GVDSIZE1";
pub const HEADER_LEN: usize = 16;
pub const REC_LEN: usize = 28;
const DIGEST_LEN: usize = 32;
/// The `GSZ1` section after a prefetch response's packs: magic, count u64, records.
pub const WIRE_MAGIC: &[u8; 4] = b"GSZ1";

/// Builds a table from `(id, size)` pairs (any order; duplicates keep the first size).
pub fn build(pairs: &mut Vec<(Oid, u64)>) -> Vec<u8> {
    pairs.sort_by_key(|p| p.0);
    pairs.dedup_by(|b, a| a.0 == b.0);
    encode_sorted(pairs.iter().copied(), pairs.len())
}

fn encode_sorted(pairs: impl Iterator<Item = (Oid, u64)>, n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        HEADER_LEN
            .saturating_add(n.saturating_mul(REC_LEN))
            .saturating_add(DIGEST_LEN),
    );
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&u64_of(n).to_le_bytes());
    for (oid, size) in pairs {
        out.extend_from_slice(&oid.0);
        out.extend_from_slice(&size.to_le_bytes());
    }
    let digest = ring::digest::digest(&ring::digest::SHA256, &out);
    out.extend_from_slice(digest.as_ref());
    out
}

/// An open sizes table (mapped from its file, or in memory).
pub struct Sizes {
    region: Region,
    count: usize,
}

impl Sizes {
    /// Validates `bytes` (header, length and checksum).
    pub fn from_heap(bytes: Vec<u8>) -> Result<Sizes> {
        Sizes::new(Region::Heap(bytes))
    }

    /// Maps an immutable table file.
    pub fn open(path: &Path) -> Result<Sizes> {
        let file = File::open(path).ctx(&format!("opening {}", path.display()))?;
        let map = gvfs_mmap::Map::open(&file, false).ctx("mapping sizes table")?;
        Sizes::new(Region::Map(map))
    }

    /// An empty table.
    pub fn empty() -> Sizes {
        let mut none = Vec::new();
        Sizes {
            region: Region::Heap(build(&mut none)),
            count: 0,
        }
    }

    fn new(region: Region) -> Result<Sizes> {
        let b = region.bytes();
        if b.get(..8) != Some(&MAGIC[..]) {
            crate::bail!("not a sizes table");
        }
        let count = us(le_u64(b, 8).ctx("sizes table header")?);
        let body = HEADER_LEN
            .checked_add(count.checked_mul(REC_LEN).ctx("sizes table count")?)
            .ctx("sizes table count")?;
        if b.len() != body.saturating_add(DIGEST_LEN) {
            crate::bail!("sizes table length does not match its count");
        }
        let (head, digest) = b.split_at_checked(body).ctx("sizes table length")?;
        if ring::digest::digest(&ring::digest::SHA256, head).as_ref() != digest {
            crate::bail!("sizes table checksum mismatch");
        }
        let s = Sizes { region, count };
        // Sorted and distinct: lookups binary-search, merges assume it.
        let mut prev: Option<&[u8]> = None;
        for i in 0..count {
            let id = s.id_at(i).ctx("sizes record")?;
            if prev.is_some_and(|p| p >= id) {
                crate::bail!("sizes table is not sorted");
            }
            prev = Some(id);
        }
        Ok(s)
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The whole file (what is uploaded).
    pub fn bytes(&self) -> &[u8] {
        self.region.bytes()
    }

    /// The records, `len() * 28` bytes (a `GSZ1` section's body).
    pub fn records(&self) -> &[u8] {
        sub(
            self.region.bytes(),
            HEADER_LEN,
            self.count.saturating_mul(REC_LEN),
        )
        .unwrap_or(&[])
    }

    #[inline]
    fn id_at(&self, i: usize) -> Option<&[u8]> {
        if i >= self.count {
            return None;
        }
        sub(
            self.region.bytes(),
            HEADER_LEN.checked_add(i.checked_mul(REC_LEN)?)?,
            20,
        )
    }

    #[inline]
    fn size_at(&self, i: usize) -> Option<u64> {
        if i >= self.count {
            return None;
        }
        le_u64(
            self.region.bytes(),
            HEADER_LEN
                .checked_add(i.checked_mul(REC_LEN)?)?
                .checked_add(20)?,
        )
    }

    /// The size of blob `oid`, if this table has it.
    pub fn get(&self, oid: &Oid) -> Option<u64> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo.wrapping_add(hi.wrapping_sub(lo).wrapping_shr(1));
            match self.id_at(mid)?.cmp(&oid.0[..]) {
                std::cmp::Ordering::Less => lo = mid.wrapping_add(1),
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return self.size_at(mid),
            }
        }
        None
    }

    /// Every `(id, size)`, in id order.
    pub fn iter(&self) -> impl Iterator<Item = (Oid, u64)> + '_ {
        (0..self.count).filter_map(|i| Some((Oid::from_slice(self.id_at(i)?)?, self.size_at(i)?)))
    }
}

/// The union of `tables` (each sorted): one sorted table, every id once.
pub fn merge(tables: &[&Sizes]) -> Vec<u8> {
    let total: usize = tables.iter().map(|t| t.len()).sum();
    let mut pos = vec![0usize; tables.len()];
    let mut out: Vec<(Oid, u64)> = Vec::with_capacity(total);
    loop {
        // Smallest current id across the tables (k is small: a plan's slabs).
        let mut best: Option<(usize, &[u8])> = None;
        for (k, t) in tables.iter().enumerate() {
            let Some(id) = pos.get(k).and_then(|&p| t.id_at(p)) else {
                continue;
            };
            if best.is_none_or(|(_, b)| id < b) {
                best = Some((k, id));
            }
        }
        let Some((k, id)) = best else {
            break;
        };
        let oid = Oid::from_slice(id).unwrap_or(Oid::ZERO);
        let size = tables
            .get(k)
            .zip(pos.get(k))
            .and_then(|(t, &p)| t.size_at(p))
            .unwrap_or(0);
        if out.last().is_none_or(|(last, _)| *last != oid) {
            out.push((oid, size));
        }
        // Advance every table past this id.
        for (j, t) in tables.iter().enumerate() {
            if let Some(p) = pos.get_mut(j)
                && t.id_at(*p) == Some(&oid.0[..])
            {
                *p = p.saturating_add(1);
            }
        }
    }
    let n = out.len();
    encode_sorted(out.into_iter(), n)
}

/// The `GSZ1` section of a response carrying `tables`: `"GSZ1" | count u64 | records`
/// (the union, sorted by id, 28 bytes each: id, size u64).
pub fn wire_section(tables: &[&Sizes]) -> Vec<u8> {
    let merged;
    let (count, records): (usize, &[u8]) = match tables {
        [] => (0, &[]),
        [one] => (one.len(), one.records()),
        _ => {
            merged = merge(tables);
            let n = us(le_u64(&merged, 8).unwrap_or(0));
            (
                n,
                sub(&merged, HEADER_LEN, n.saturating_mul(REC_LEN)).unwrap_or(&[]),
            )
        }
    };
    let mut out = Vec::with_capacity(records.len().saturating_add(12));
    out.extend_from_slice(WIRE_MAGIC);
    out.extend_from_slice(&u64_of(count).to_le_bytes());
    out.extend_from_slice(records);
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn oid(b: u8) -> Oid {
        let mut o = [0u8; 20];
        o[0] = b;
        o[19] = b.wrapping_mul(7);
        Oid(o)
    }

    #[test]
    fn build_lookup_merge() {
        let mut a = vec![(oid(9), 90), (oid(1), 10), (oid(5), 50), (oid(1), 11)];
        let ta = Sizes::from_heap(build(&mut a)).unwrap();
        assert_eq!(ta.len(), 3);
        assert_eq!(ta.get(&oid(1)), Some(10));
        assert_eq!(ta.get(&oid(5)), Some(50));
        assert_eq!(ta.get(&oid(9)), Some(90));
        assert_eq!(ta.get(&oid(2)), None);
        let mut b = vec![(oid(2), 20), (oid(5), 50), (oid(200), 2000)];
        let tb = Sizes::from_heap(build(&mut b)).unwrap();
        let m = Sizes::from_heap(merge(&[&ta, &tb])).unwrap();
        let all: Vec<(Oid, u64)> = m.iter().collect();
        assert_eq!(
            all,
            vec![
                (oid(1), 10),
                (oid(2), 20),
                (oid(5), 50),
                (oid(9), 90),
                (oid(200), 2000)
            ]
        );
        assert_eq!(m.records().len(), 5 * REC_LEN);
        let w = wire_section(&[&ta, &tb]);
        assert_eq!(&w[..4], WIRE_MAGIC);
        assert_eq!(le_u64(&w, 4), Some(5));
        assert_eq!(&w[12..], m.records());
        assert_eq!(&wire_section(&[&ta])[12..], ta.records());
        assert_eq!(wire_section(&[]), b"GSZ1\0\0\0\0\0\0\0\0");
        assert!(Sizes::empty().is_empty());
        // Corruption is detected.
        let mut bad = build(&mut vec![(oid(3), 3)]);
        bad[HEADER_LEN + 20] ^= 1;
        assert!(Sizes::from_heap(bad).is_err());
    }
}
