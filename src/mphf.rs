//! Partitioned PTHash-style minimal perfect hash over object ids.
//!
//! The keys are SHA-1 object ids, already uniformly random, so "hashing" is reading three
//! words of the id and re-randomizing them with the seed (one `fmix64` each). Keys are
//! split into partitions of ~`PART_KEYS` keys; each partition is an independent PTHash
//! table with `load factor = 1` (a bijection onto its slot range), so partitions build in
//! parallel with an L1-resident "taken" bitmap.
//!
//! Lookup: `p = part(oid)`, `base = offsets[p]`, `m = offsets[p+1] - base`,
//! `pilot = pilots[p * B + bucket(oid)]`, `slot = base + mul_hi(fmix(pos(oid) ^ pilot), m)`.
//! That is two small array reads and no binary search; the caller verifies the full id at
//! `records[slot]` (keys outside the set land on an arbitrary slot).

use crate::util::{Oid, Result, fmix64, le_u16, le_u32, mul_hi};

/// Average keys per partition.
pub const PART_KEYS: u64 = 2048;
/// Average keys per bucket (PTHash's `n / buckets`). Higher packs pilots tighter but makes
/// the search for the first (largest) buckets longer.
const BUCKET_KEYS: u64 = 4;
const PHI: u64 = 0x9E37_79B9_7F4A_7C15;
const DEFAULT_SEED: u64 = 0x6776_6673_2d6d_7068; // "gvfs-mph"

/// The three independent hash words of a key.
#[derive(Clone, Copy, Default)]
struct KeyHash {
    part: u64,
    bucket: u64,
    pos: u64,
}

#[inline(always)]
fn key_hash(oid: &Oid, seed: u64) -> KeyHash {
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
    let k0 = u64::from_le_bytes([a0, a1, a2, a3, a4, a5, a6, a7]);
    let k1 = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]);
    let k2 = u64::from(u32::from_le_bytes([c0, c1, c2, c3]));
    KeyHash {
        part: fmix64(k0 ^ seed),
        bucket: fmix64(k1 ^ seed.rotate_left(17)),
        pos: fmix64(k2 ^ k0.rotate_left(32) ^ seed.rotate_left(41)),
    }
}

/// Skewed bucket choice (PTHash): 60% of keys go to the first 30% of buckets, so the
/// dense buckets are placed first while the table is still empty.
#[inline(always)]
fn bucket_of(h: u64, buckets: u64, dense: u64) -> u64 {
    let sparse = buckets.saturating_sub(dense);
    if sparse == 0 || (h & 0xFFFF_FFFF) < 0x9999_9999 {
        mul_hi(h, dense)
    } else {
        dense.wrapping_add(mul_hi(h, sparse))
    }
}

#[inline(always)]
fn position(pos_hash: u64, pilot: u16, m: u64) -> u64 {
    let ph = u64::from(pilot).wrapping_add(1).wrapping_mul(PHI);
    mul_hi(fmix64(pos_hash ^ ph), m)
}

/// The first 30% of the buckets (at least one; all of them when there is only one).
fn dense_buckets(buckets: u64) -> u64 {
    buckets.wrapping_mul(3).wrapping_div(10).max(1).min(buckets)
}

/// Buckets per partition for `parts` partitions over `n` keys.
fn buckets_per_part(n: u64, parts: u64) -> u64 {
    let avg = n.div_ceil(parts.max(1));
    avg.div_ceil(BUCKET_KEYS).max(1)
}

/// A built table, ready to serialize.
pub struct Built {
    pub seed: u64,
    pub parts: u32,
    pub buckets: u32,
    /// `parts + 1` prefix sums of partition sizes.
    pub offsets: Vec<u32>,
    /// `parts * buckets` pilots.
    pub pilots: Vec<u16>,
    /// The slot of every input key, in input order.
    pub slots: Vec<u32>,
}

/// Seeds tried as constants of this file before one is derived from the keys.
const FIXED_SEEDS: u32 = 8;
/// Seeds tried after that, starting at [`derived_seed`].
const DERIVED_SEEDS: u32 = 8;

/// Builds the table for `oids` using up to `threads` threads. The ids must be distinct:
/// a duplicate fails fast with an error of kind `Conflict`.
///
/// The seed is `DEFAULT_SEED` for any ordinary key set, so an index is a function of its
/// keys alone. Ids are not always ordinary: whoever can push to the branch can make
/// objects whose ids land in one bucket under a seed it knows (300 blobs per seed, 41M
/// SHA-1 trials for all the fixed ones), and no pilot places such a bucket. When the
/// fixed seeds have all failed, the seed is a digest of the ids, which cannot be aimed
/// at: changing an id to suit a seed changes the seed.
pub fn build(oids: &[Oid], threads: usize) -> Result<Built> {
    build_from(
        oids.len(),
        |i| oids.get(i).copied().unwrap_or(Oid::ZERO),
        threads,
    )
}

/// [`build`] for ids that are not in one slice: `key(i)` is the `i`th of the `n`. Under
/// a fixed seed the table (its seed, offsets and pilots, and so every id's slot) is a
/// function of the set of ids; under a derived one, of their order too.
///
/// Memory: 8 bytes an id while it builds (which ids each partition has, and where each
/// landed), then the 4 of its slot too. The ids are hashed where they are, as each is
/// needed. (Three hash words were kept for each first: 24 bytes more.)
pub fn build_from(n: usize, key: impl Fn(usize) -> Oid + Sync, threads: usize) -> Result<Built> {
    let mut seed = DEFAULT_SEED;
    for attempt in 0..FIXED_SEEDS.saturating_add(DERIVED_SEEDS) {
        if attempt == FIXED_SEEDS {
            crate::log!(
                "mphf: no fixed seed places these {n} ids (made to collide?); \
                 deriving the seed from them"
            );
            seed = derived_seed((0..n).map(&key));
        }
        match try_build(n, &key, seed, threads.max(1))? {
            Outcome::Built(b) => return Ok(b),
            Outcome::Duplicate => {
                return Err(crate::util::Error::with_kind(
                    crate::util::ErrorKind::Conflict,
                    "duplicate object ids",
                ));
            }
            Outcome::Retry => seed = seed.wrapping_add(PHI),
        }
    }
    crate::bail!("minimal perfect hash construction failed")
}

/// A seed that depends on every key: the first 8 bytes of the SHA-1 of the ids in order.
fn derived_seed(oids: impl Iterator<Item = Oid>) -> u64 {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    for oid in oids {
        h.update(oid.0);
    }
    let digest = h.finalize();
    let mut word = [0u8; 8];
    for (w, d) in word.iter_mut().zip(digest.iter()) {
        *w = *d;
    }
    u64::from_le_bytes(word)
}

enum Outcome {
    Built(Built),
    Retry,
    Duplicate,
}

#[derive(PartialEq, Eq)]
enum Placement {
    Placed,
    Failed,
    Duplicate,
}

struct PartScratch {
    keys: Vec<KeyHash>,
    bucket: Vec<u32>,
    bucket_start: Vec<u32>,
    members: Vec<u32>,
    by_size: Vec<u32>,
    taken: Vec<u64>,
    placed: Vec<u32>,
    pos: Vec<u32>,
    fill: Vec<u32>,
}

impl PartScratch {
    fn new() -> PartScratch {
        PartScratch {
            keys: Vec::new(),
            bucket: Vec::new(),
            bucket_start: Vec::new(),
            members: Vec::new(),
            by_size: Vec::new(),
            taken: Vec::new(),
            placed: Vec::new(),
            pos: Vec::new(),
            fill: Vec::new(),
        }
    }
}

fn try_build(
    keys: usize,
    key: &(impl Fn(usize) -> Oid + Sync),
    seed: u64,
    threads: usize,
) -> Result<Outcome> {
    let n = crate::util::u64_of(keys);
    if n > u64::from(u32::MAX) {
        crate::bail!("too many objects for one slab index: {n}");
    }
    let parts = n.div_ceil(PART_KEYS).max(1);
    let buckets = buckets_per_part(n, parts);
    let parts_us = crate::util::us(parts);
    let buckets_us = crate::util::us(buckets);
    let part_of = |i: usize| crate::util::us(mul_hi(key_hash(&key(i), seed).part, parts));

    // 1. Count keys per partition.
    let mut counts = vec![0u32; parts_us];
    for i in 0..keys {
        if let Some(c) = counts.get_mut(part_of(i)) {
            *c = c.wrapping_add(1);
        }
    }
    let mut offsets = Vec::with_capacity(parts_us.saturating_add(1));
    let mut acc = 0u32;
    offsets.push(0u32);
    for &c in &counts {
        acc = acc.wrapping_add(c);
        offsets.push(acc);
    }

    // 2. Group key indices by partition (stable counting sort).
    let mut cursor: Vec<u32> = offsets.iter().take(parts_us).copied().collect();
    let mut grouped = vec![0u32; keys];
    for i in 0..keys {
        if let Some(c) = cursor.get_mut(part_of(i)) {
            if let Some(g) = grouped.get_mut(crate::util::us(u64::from(*c))) {
                *g = crate::util::u32_of(i);
            }
            *c = c.wrapping_add(1);
        }
    }

    // 3. Build partitions in parallel: each thread owns a contiguous run of partitions,
    //    their pilot rows and their grouped-key positions.
    let mut pilots = vec![0u16; parts_us.saturating_mul(buckets_us)];
    let mut local_pos = vec![0u32; keys];
    let threads = threads.min(parts_us).max(1);
    let per_thread = parts_us.div_ceil(threads);
    let failed = std::sync::atomic::AtomicBool::new(false);
    let duplicate = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        let mut pilot_rest: &mut [u16] = &mut pilots;
        let mut pos_rest: &mut [u32] = &mut local_pos;
        let mut first = 0usize;
        while first < parts_us {
            let last = first.saturating_add(per_thread).min(parts_us);
            let key_lo = offsets.get(first).copied().unwrap_or(0) as usize;
            let key_hi = offsets.get(last).copied().unwrap_or(0) as usize;
            let (pilot_mine, pilot_tail) = pilot_rest.split_at_mut(
                last.wrapping_sub(first)
                    .saturating_mul(buckets_us)
                    .min(pilot_rest.len()),
            );
            let (pos_mine, pos_tail) =
                pos_rest.split_at_mut(key_hi.saturating_sub(key_lo).min(pos_rest.len()));
            pilot_rest = pilot_tail;
            pos_rest = pos_tail;
            let (offsets, grouped, failed, duplicate) = (&offsets, &grouped, &failed, &duplicate);
            s.spawn(move || {
                let mut scratch = PartScratch::new();
                for p in first..last {
                    let lo = offsets.get(p).copied().unwrap_or(0) as usize;
                    let hi = offsets.get(p.wrapping_add(1)).copied().unwrap_or(0) as usize;
                    let members = grouped.get(lo..hi).unwrap_or(&[]);
                    scratch.keys.clear();
                    scratch
                        .keys
                        .extend(members.iter().map(|&i| key_hash(&key(i as usize), seed)));
                    let row_lo = p.wrapping_sub(first).saturating_mul(buckets_us);
                    let row = pilot_mine
                        .get_mut(row_lo..row_lo.saturating_add(buckets_us))
                        .unwrap_or(&mut []);
                    let out = pos_mine
                        .get_mut(lo.wrapping_sub(key_lo)..hi.wrapping_sub(key_lo))
                        .unwrap_or(&mut []);
                    match build_partition(&mut scratch, buckets, row, out) {
                        Placement::Placed => {}
                        Placement::Failed => {
                            failed.store(true, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }
                        Placement::Duplicate => {
                            duplicate.store(true, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }
                    }
                }
            });
            first = last;
        }
    });
    if duplicate.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(Outcome::Duplicate);
    }
    if failed.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(Outcome::Retry);
    }

    // 4. Slot of every key: partition base + local position.
    let mut slots = vec![0u32; keys];
    for p in 0..parts_us {
        let lo = offsets.get(p).copied().unwrap_or(0);
        let hi = offsets.get(p.wrapping_add(1)).copied().unwrap_or(0);
        for j in lo..hi {
            let key = grouped.get(j as usize).copied().unwrap_or(0);
            let pos = local_pos.get(j as usize).copied().unwrap_or(0);
            if let Some(s) = slots.get_mut(key as usize) {
                *s = lo.wrapping_add(pos);
            }
        }
    }
    Ok(Outcome::Built(Built {
        seed,
        parts: crate::util::u32_of(parts_us),
        buckets: crate::util::u32_of(buckets_us),
        offsets,
        pilots,
        slots,
    }))
}

/// Places one partition's keys (`scratch.keys`). Writes `row` (pilots) and `out` (each
/// key's position within the partition).
fn build_partition(
    scratch: &mut PartScratch,
    buckets: u64,
    row: &mut [u16],
    out: &mut [u32],
) -> Placement {
    let m = crate::util::u64_of(scratch.keys.len());
    if m == 0 {
        return Placement::Placed;
    }
    let dense = dense_buckets(buckets);
    let nb = crate::util::us(buckets);
    let PartScratch {
        keys,
        bucket,
        bucket_start,
        members,
        by_size,
        taken,
        placed,
        pos,
        fill,
    } = scratch;

    // Counting sort of keys by bucket.
    bucket.clear();
    bucket.extend(
        keys.iter()
            .map(|k| crate::util::u32_of(crate::util::us(bucket_of(k.bucket, buckets, dense)))),
    );
    bucket_start.clear();
    bucket_start.resize(nb.saturating_add(1), 0);
    for &b in bucket.iter() {
        match bucket_start.get_mut((b as usize).wrapping_add(1)) {
            Some(c) => *c = c.wrapping_add(1),
            None => return Placement::Failed,
        }
    }
    for i in 1..bucket_start.len() {
        let prev = bucket_start.get(i.wrapping_sub(1)).copied().unwrap_or(0);
        if let Some(c) = bucket_start.get_mut(i) {
            *c = c.wrapping_add(prev);
        }
    }
    members.clear();
    members.resize(keys.len(), 0);
    fill.clear();
    fill.extend(bucket_start.iter().take(nb).copied());
    for (k, &b) in bucket.iter().enumerate() {
        if let Some(f) = fill.get_mut(b as usize) {
            if let Some(slot) = members.get_mut(*f as usize) {
                *slot = crate::util::u32_of(k);
            }
            *f = f.wrapping_add(1);
        }
    }

    // Buckets by size, largest first (stable counting sort on min(size, 63)).
    by_size.clear();
    let size_of = |b: usize| -> u32 {
        let lo = bucket_start.get(b).copied().unwrap_or(0);
        let hi = bucket_start.get(b.wrapping_add(1)).copied().unwrap_or(0);
        hi.wrapping_sub(lo)
    };
    let mut size_counts = [0u32; 64];
    for b in 0..nb {
        let s = size_of(b).min(63) as usize;
        if let Some(c) = size_counts.get_mut(s) {
            *c = c.wrapping_add(1);
        }
    }
    let mut size_cursor = [0u32; 64];
    let mut run = 0u32;
    for s in (0..64usize).rev() {
        if let (Some(cur), Some(&cnt)) = (size_cursor.get_mut(s), size_counts.get(s)) {
            *cur = run;
            run = run.wrapping_add(cnt);
        }
    }
    by_size.resize(nb, 0);
    for b in 0..nb {
        let s = size_of(b).min(63) as usize;
        if let Some(cur) = size_cursor.get_mut(s) {
            if let Some(slot) = by_size.get_mut(*cur as usize) {
                *slot = crate::util::u32_of(b);
            }
            *cur = cur.wrapping_add(1);
        }
    }

    // Pilot search.
    taken.clear();
    taken.resize(crate::util::us(m.div_ceil(64)), 0);
    pos.clear();
    pos.resize(keys.len(), 0);
    for &b in by_size.iter() {
        let b = b as usize;
        let lo = bucket_start.get(b).copied().unwrap_or(0) as usize;
        let hi = bucket_start.get(b.wrapping_add(1)).copied().unwrap_or(0) as usize;
        if lo == hi {
            continue;
        }
        let bucket_members = members.get(lo..hi).unwrap_or(&[]);
        let mut found = None;
        'pilot: for pilot in 0..=u16::MAX {
            placed.clear();
            for &k in bucket_members {
                let h = keys.get(k as usize).map(|k| k.pos).unwrap_or(0);
                let p = position(h, pilot, m);
                let word = crate::util::us(p.wrapping_shr(6));
                let bit = 1u64.wrapping_shl((p & 63) as u32);
                let w = taken.get_mut(word);
                match w {
                    Some(w) if *w & bit == 0 => {
                        *w |= bit;
                        placed.push(crate::util::u32_of(crate::util::us(p)));
                    }
                    _ => {
                        for &q in placed.iter() {
                            let q = u64::from(q);
                            if let Some(w) = taken.get_mut(crate::util::us(q.wrapping_shr(6))) {
                                *w &= !1u64.wrapping_shl((q & 63) as u32);
                            }
                        }
                        continue 'pilot;
                    }
                }
            }
            found = Some(pilot);
            break;
        }
        let Some(pilot) = found else {
            // Two identical keys can never be separated: report it instead of reseeding.
            let same = |a: u32, b: u32| {
                let (ka, kb) = (keys.get(a as usize), keys.get(b as usize));
                matches!((ka, kb), (Some(x), Some(y)) if x.part == y.part && x.bucket == y.bucket && x.pos == y.pos)
            };
            let dup = bucket_members.iter().enumerate().any(|(i, &a)| {
                bucket_members
                    .iter()
                    .skip(i.wrapping_add(1))
                    .any(|&b| same(a, b))
            });
            return if dup {
                Placement::Duplicate
            } else {
                Placement::Failed
            };
        };
        if let Some(r) = row.get_mut(b) {
            *r = pilot;
        }
        for (&k, &p) in bucket_members.iter().zip(placed.iter()) {
            if let Some(slot) = pos.get_mut(k as usize) {
                *slot = p;
            }
        }
    }
    for (o, &p) in out.iter_mut().zip(pos.iter()) {
        *o = p;
    }
    Placement::Placed
}

/// A read-only view of a serialized table (inside a `.mphf` slab index).
#[derive(Clone, Copy)]
pub struct Mphf<'a> {
    pub seed: u64,
    pub parts: u32,
    pub buckets: u32,
    /// `parts + 1` little-endian u32.
    pub offsets: &'a [u8],
    /// `parts * buckets` little-endian u16.
    pub pilots: &'a [u8],
}

impl Mphf<'_> {
    /// The only slot `oid` can occupy, or `None` for an empty partition.
    #[inline]
    pub fn slot(&self, oid: &Oid) -> Option<u32> {
        let h = key_hash(oid, self.seed);
        let parts = u64::from(self.parts);
        let p = crate::util::us(mul_hi(h.part, parts));
        let base = le_u32(self.offsets, p.checked_mul(4)?)?;
        let end = le_u32(self.offsets, p.checked_add(1)?.checked_mul(4)?)?;
        let m = u64::from(end.checked_sub(base)?);
        if m == 0 {
            return None;
        }
        let buckets = u64::from(self.buckets);
        let b = crate::util::us(bucket_of(h.bucket, buckets, dense_buckets(buckets)));
        let pilot_at = p
            .checked_mul(crate::util::us(buckets))?
            .checked_add(b)?
            .checked_mul(2)?;
        let pilot = le_u16(self.pilots, pilot_at)?;
        let local = u32::try_from(position(h.pos, pilot, m)).ok()?;
        base.checked_add(local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_oids(n: usize, salt: u64) -> Vec<Oid> {
        let mut s = salt;
        (0..n)
            .map(|_| {
                let mut o = [0u8; 20];
                for chunk in o.chunks_mut(8) {
                    s = fmix64(s.wrapping_add(PHI));
                    let b = s.to_le_bytes();
                    chunk.copy_from_slice(&b[..chunk.len()]);
                }
                Oid(o)
            })
            .collect()
    }

    fn view(b: &Built) -> (Vec<u8>, Vec<u8>) {
        (
            b.offsets.iter().flat_map(|v| v.to_le_bytes()).collect(),
            b.pilots.iter().flat_map(|v| v.to_le_bytes()).collect(),
        )
    }

    #[test]
    fn bijection_and_lookup() {
        for n in [0usize, 1, 2, 3, 100, 2048, 2049, 50_000] {
            let oids = random_oids(n, n as u64);
            let b = build(&oids, 4).unwrap();
            let mut seen = vec![false; n];
            for &s in &b.slots {
                assert!(!seen[s as usize], "duplicate slot n={n}");
                seen[s as usize] = true;
            }
            let (offsets, pilots) = view(&b);
            let m = Mphf {
                seed: b.seed,
                parts: b.parts,
                buckets: b.buckets,
                offsets: &offsets,
                pilots: &pilots,
            };
            for (i, oid) in oids.iter().enumerate() {
                assert_eq!(m.slot(oid), Some(b.slots[i]));
            }
        }
    }

    #[test]
    fn duplicates_fail_fast() {
        let mut oids = random_oids(5000, 3);
        oids[4000] = oids[17];
        let e = build(&oids, 2).err().unwrap();
        assert!(e.is_conflict());
    }

    #[test]
    fn thread_count_does_not_change_the_table() {
        let oids = random_oids(20_000, 7);
        let a = build(&oids, 1).unwrap();
        let b = build(&oids, 7).unwrap();
        assert_eq!((a.seed, &a.pilots, &a.slots), (b.seed, &b.pilots, &b.slots));
    }

    /// Under a fixed seed the table is a function of the set of ids. Given in another
    /// order (as an index made of another gives them: the old index's in slot order,
    /// then the new ones), the same seed, offsets and pilots come out, and each id has
    /// the slot it had.
    #[test]
    fn the_order_of_the_ids_does_not_change_the_table() {
        let oids = random_oids(50_000, 21);
        let a = build(&oids, 3).unwrap();
        let order: Vec<usize> = (20_000..50_000).rev().chain(0..20_000).collect();
        let b = build_from(oids.len(), |i| oids[order[i]], 2).unwrap();
        assert_eq!((a.seed, a.parts, a.buckets), (b.seed, b.parts, b.buckets));
        assert_eq!(a.seed, DEFAULT_SEED);
        assert!(a.offsets == b.offsets && a.pilots == b.pilots);
        for (i, &was) in order.iter().enumerate() {
            assert_eq!(b.slots[i], a.slots[was]);
        }
    }

    /// The inverse of `fmix64`.
    fn unmix64(mut k: u64) -> u64 {
        // The inverse of an odd number modulo 2^64, by Newton's iteration.
        let inverse = |x: u64| {
            (0..6).fold(x, |i, _| {
                i.wrapping_mul(2u64.wrapping_sub(x.wrapping_mul(i)))
            })
        };
        k ^= k.wrapping_shr(33);
        k = k.wrapping_mul(inverse(0xc4ce_b9fe_1a85_ec53));
        k ^= k.wrapping_shr(33);
        k = k.wrapping_mul(inverse(0xff51_afd7_ed55_8ccd));
        k ^ k.wrapping_shr(33)
    }

    /// Ids made to order: 400 per fixed seed that share partition 0 and bucket 0 under it
    /// (one object in 131,072 has such an id), so that no pilot places that bucket and
    /// every fixed seed fails. The table is then built with a seed derived from the ids,
    /// and is the same table every time.
    #[test]
    fn ids_aimed_at_the_fixed_seeds_still_build() {
        let mut oids = random_oids(20_000, 11);
        let mut words = random_oids(8 * 400, 12).into_iter();
        let mut seed = DEFAULT_SEED;
        for _ in 0..FIXED_SEEDS {
            for _ in 0..400 {
                let w = words.next().unwrap().0;
                let word = |at: usize| u64::from_le_bytes(w[at..at + 8].try_into().unwrap());
                // Hashes with their top byte clear: partition 0 of up to 256, and the
                // first of up to 256 dense buckets (bit 31 clear picks the dense ones).
                let part = word(0).wrapping_shr(8);
                let bucket = word(8).wrapping_shr(8) & !0x8000_0000;
                let mut id = w;
                id[..8].copy_from_slice(&(unmix64(part) ^ seed).to_le_bytes());
                id[8..16].copy_from_slice(&(unmix64(bucket) ^ seed.rotate_left(17)).to_le_bytes());
                let h = key_hash(&Oid(id), seed);
                assert_eq!((h.part, h.bucket), (part, bucket));
                oids.push(Oid(id));
            }
            let tried = try_build(oids.len(), &|i: usize| oids[i], seed, 4);
            assert!(matches!(tried, Ok(Outcome::Retry)));
            seed = seed.wrapping_add(PHI);
        }
        let b = build(&oids, 4).unwrap();
        assert_eq!(b.seed, derived_seed(oids.iter().copied()));
        let mut seen = vec![false; oids.len()];
        for &s in &b.slots {
            assert!(!std::mem::replace(&mut seen[s as usize], true));
        }
        let (offsets, pilots) = view(&b);
        let m = Mphf {
            seed: b.seed,
            parts: b.parts,
            buckets: b.buckets,
            offsets: &offsets,
            pilots: &pilots,
        };
        for (i, oid) in oids.iter().enumerate() {
            assert_eq!(m.slot(oid), Some(b.slots[i]));
        }
        let again = build(&oids, 2).unwrap();
        assert_eq!((again.seed, &again.pilots), (b.seed, &b.pilots));
    }
}
