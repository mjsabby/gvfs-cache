//! Minimal perfect hash index over a slab (`slab-<seq>.mphf`).
//!
//! A PTHash-style construction: every key (object id) hashes to one of `m` buckets; each
//! bucket stores a 16-bit *pilot* chosen at build time so that
//! `position(key, pilot)` is distinct for every key of the slab. Positions ≥ `n` are
//! redirected through a small `remap` table (the table is 1/α ≈ 3% larger than `n`, which
//! keeps pilot searches short). The lookup is therefore one pilot load, optionally one remap
//! load, and one 16-byte descriptor load: `[offset:36 | len:28][fingerprint:64]`.
//!
//! The fingerprint is the last 8 bytes of the OID, independent of the bytes that chose the
//! bucket and the position, so a lookup for a key that is not in the slab is rejected
//! with probability 1 − 2⁻⁶⁴ — good enough to probe every slab for every requested OID
//! without ever inflating anything to double-check.
//!
//! ```text
//!  0   "GMPH" magic, u32 version
//!  8   u64 num_keys n, u64 table_size, u64 num_buckets m, u64 seed, u64 pack_len
//! 64   [u16; m] pilots          (padded to 8 bytes)
//!      [u32; table_size - n] remap (padded to 8 bytes)
//!      [[u64; 2]; n] descriptors
//! ```
//!
//! All integers little-endian. Buckets use the "skew" split from the PTHash paper: 60% of
//! keys go to the first 30% of buckets, so the large buckets (placed first, while the table
//! is empty) are dense and the many tiny ones placed last find free slots quickly.

use core::fmt;

use crate::oid::Oid;

pub const MPHF_MAGIC: [u8; 4] = *b"GMPH";
pub const MPHF_VERSION: u32 = 1;
pub const HEADER_LEN: usize = 64;
const OFFSET_BITS: u32 = 36;
const LEN_BITS: u32 = 28;
pub const MAX_PACK_OFFSET: u64 = (1u64 << OFFSET_BITS) - 1;
pub const MAX_ENTRY_LEN: u64 = (1u64 << LEN_BITS) - 1;
/// Buckets per key × log2(n): PTHash's `c`.
const BUCKETS_C: f64 = 6.0;
/// Table load factor α.
const ALPHA: f64 = 0.97;
/// 60% of the keys land in the first 30% of the buckets.
const SKEW_KEYS: u64 = (0.6 * 4_294_967_296.0) as u64;
const SKEW_BUCKETS_NUM: u64 = 3;
const SKEW_BUCKETS_DEN: u64 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MphfError {
    BadMagic,
    BadVersion,
    Truncated,
    Inconsistent,
    TooManyKeys,
    OffsetTooLarge,
    LenTooLarge,
    DuplicateKey,
    BucketTooLarge,
    /// No pilot in `0..65536` placed a bucket; retry with another seed.
    NoPilot,
    OutputTooSmall,
}

impl fmt::Display for MphfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::BadMagic => "bad mphf magic",
            Self::BadVersion => "unsupported mphf version",
            Self::Truncated => "truncated mphf",
            Self::Inconsistent => "inconsistent mphf header",
            Self::TooManyKeys => "too many keys for an mphf",
            Self::OffsetTooLarge => "pack offset exceeds 36 bits",
            Self::LenTooLarge => "entry length exceeds 28 bits",
            Self::DuplicateKey => "duplicate object id",
            Self::BucketTooLarge => "mphf bucket exceeds 255 keys (retry with another seed)",
            Self::NoPilot => "no pilot found for a bucket (retry with another seed)",
            Self::OutputTooSmall => "mphf output buffer too small",
        })
    }
}

impl std::error::Error for MphfError {}

/// Where an object lives inside its slab's pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Loc {
    pub offset: u64,
    pub len: u32,
}

#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 32;
    x = x.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    x ^= x >> 32;
    x = x.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    x ^= x >> 32;
    x
}

#[inline]
fn fastrange(h: u64, range: u64) -> u64 {
    (u128::from(h).wrapping_mul(u128::from(range)) >> 64) as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    pub num_keys: u64,
    pub table_size: u64,
    pub num_buckets: u64,
    pub seed: u64,
}

impl Params {
    fn for_keys(n: u64, seed: u64) -> Self {
        if n == 0 {
            return Self {
                num_keys: 0,
                table_size: 0,
                num_buckets: 0,
                seed,
            };
        }
        let nf = n as f64;
        let m = (BUCKETS_C * nf / (nf + 1.0).log2()).ceil().max(1.0) as u64;
        let table_size = ((nf / ALPHA).ceil() as u64).max(n);
        Self {
            num_keys: n,
            table_size,
            num_buckets: m,
            seed,
        }
    }

    #[inline]
    fn dense_buckets(&self) -> u64 {
        self.num_buckets.saturating_mul(SKEW_BUCKETS_NUM) / SKEW_BUCKETS_DEN
    }

    #[inline]
    fn bucket(&self, oid: &Oid) -> u64 {
        let h = mix64(oid.word(0) ^ self.seed);
        let sel = h & 0xFFFF_FFFF;
        let pos = h >> 32;
        let p1 = self.dense_buckets();
        if sel < SKEW_KEYS {
            pos.wrapping_mul(p1) >> 32
        } else {
            p1.saturating_add(pos.wrapping_mul(self.num_buckets.saturating_sub(p1)) >> 32)
        }
    }

    /// The per-key half of the position hash (independent of the pilot).
    #[inline]
    fn key_hash(&self, oid: &Oid) -> u64 {
        mix64(oid.word(8) ^ self.seed.rotate_left(17))
    }

    #[inline]
    fn pilot_hash(&self, pilot: u16) -> u64 {
        mix64(self.seed ^ (u64::from(pilot) << 20) ^ 0x9E37_79B9_7F4A_7C15)
    }

    #[inline]
    fn position(&self, key_hash: u64, pilot_hash: u64) -> u64 {
        fastrange(mix64(key_hash ^ pilot_hash), self.table_size)
    }
}

#[inline]
fn fingerprint(oid: &Oid) -> u64 {
    oid.word(12)
}

#[inline]
fn pad8(n: usize) -> usize {
    n.saturating_add(7) & !7
}

#[inline]
fn pilots_len(m: u64) -> usize {
    pad8((m as usize).saturating_mul(2))
}

#[inline]
fn remap_len(p: &Params) -> usize {
    pad8((p.table_size.saturating_sub(p.num_keys) as usize).saturating_mul(4))
}

/// Exact file size for `n` keys.
pub fn file_len_for_keys(n: u64) -> usize {
    let p = Params::for_keys(n, 0);
    HEADER_LEN
        .saturating_add(pilots_len(p.num_buckets))
        .saturating_add(remap_len(&p))
        .saturating_add((n as usize).saturating_mul(16))
}

/// A parsed view over an mphf file's bytes (an mmap or an in-memory slab).
#[derive(Debug, Clone, Copy)]
pub struct MphfView<'a> {
    params: Params,
    pack_len: u64,
    pilots: &'a [u8],
    remap: &'a [u8],
    descriptors: &'a [u8],
}

impl<'a> MphfView<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, MphfError> {
        let hdr = bytes.get(..HEADER_LEN).ok_or(MphfError::Truncated)?;
        if hdr.get(..4) != Some(&MPHF_MAGIC[..]) {
            return Err(MphfError::BadMagic);
        }
        let u32_at = |at: usize| -> Result<u32, MphfError> {
            let b: &[u8; 4] = hdr
                .get(at..at.saturating_add(4))
                .and_then(|s| s.try_into().ok())
                .ok_or(MphfError::Truncated)?;
            Ok(u32::from_le_bytes(*b))
        };
        let u64_at = |at: usize| -> Result<u64, MphfError> {
            let b: &[u8; 8] = hdr
                .get(at..at.saturating_add(8))
                .and_then(|s| s.try_into().ok())
                .ok_or(MphfError::Truncated)?;
            Ok(u64::from_le_bytes(*b))
        };
        if u32_at(4)? != MPHF_VERSION {
            return Err(MphfError::BadVersion);
        }
        let params = Params {
            num_keys: u64_at(8)?,
            table_size: u64_at(16)?,
            num_buckets: u64_at(24)?,
            seed: u64_at(32)?,
        };
        let pack_len = u64_at(40)?;
        if params.table_size < params.num_keys || (params.num_keys > 0 && params.num_buckets == 0) {
            return Err(MphfError::Inconsistent);
        }
        let pl = pilots_len(params.num_buckets);
        let rl = remap_len(&params);
        let dl = usize::try_from(params.num_keys)
            .map_err(|_| MphfError::TooManyKeys)?
            .checked_mul(16)
            .ok_or(MphfError::TooManyKeys)?;
        let pilots_at = HEADER_LEN;
        let remap_at = pilots_at.saturating_add(pl);
        let desc_at = remap_at.saturating_add(rl);
        let end = desc_at.saturating_add(dl);
        if bytes.len() < end {
            return Err(MphfError::Truncated);
        }
        Ok(Self {
            params,
            pack_len,
            pilots: bytes.get(pilots_at..remap_at).unwrap_or(&[]),
            remap: bytes.get(remap_at..desc_at).unwrap_or(&[]),
            descriptors: bytes.get(desc_at..end).unwrap_or(&[]),
        })
    }

    #[inline]
    pub fn num_keys(&self) -> u64 {
        self.params.num_keys
    }

    #[inline]
    pub fn pack_len(&self) -> u64 {
        self.pack_len
    }

    #[inline]
    fn u16_at(bytes: &[u8], i: usize) -> Option<u16> {
        let at = i.checked_mul(2)?;
        let b: &[u8; 2] = bytes.get(at..at.checked_add(2)?)?.try_into().ok()?;
        Some(u16::from_le_bytes(*b))
    }

    #[inline]
    fn u32_at(bytes: &[u8], i: usize) -> Option<u32> {
        let at = i.checked_mul(4)?;
        let b: &[u8; 4] = bytes.get(at..at.checked_add(4)?)?.try_into().ok()?;
        Some(u32::from_le_bytes(*b))
    }

    #[inline]
    fn descriptor(&self, slot: usize) -> Option<(u64, u64)> {
        let at = slot.checked_mul(16)?;
        let d: &[u8; 16] = self
            .descriptors
            .get(at..at.checked_add(16)?)?
            .try_into()
            .ok()?;
        let (w0, w1) = d.split_at(8);
        let w0: [u8; 8] = w0.try_into().ok()?;
        let w1: [u8; 8] = w1.try_into().ok()?;
        Some((u64::from_le_bytes(w0), u64::from_le_bytes(w1)))
    }

    /// The slot the function assigns to `oid` (meaningful only for keys of the slab).
    #[inline]
    pub fn slot(&self, oid: &Oid) -> Option<usize> {
        let p = &self.params;
        if p.num_keys == 0 {
            return None;
        }
        let b = usize::try_from(p.bucket(oid)).ok()?;
        let pilot = Self::u16_at(self.pilots, b)?;
        let pos = p.position(p.key_hash(oid), p.pilot_hash(pilot));
        let slot = if pos < p.num_keys {
            pos
        } else {
            u64::from(Self::u32_at(
                self.remap,
                usize::try_from(pos.saturating_sub(p.num_keys)).ok()?,
            )?)
        };
        usize::try_from(slot).ok()
    }

    /// O(1): `Some(loc)` iff `oid` is in the slab (up to a 2⁻⁶⁴ fingerprint collision).
    #[inline]
    pub fn lookup(&self, oid: &Oid) -> Option<(usize, Loc)> {
        let slot = self.slot(oid)?;
        let (w0, fp) = self.descriptor(slot)?;
        if fp != fingerprint(oid) {
            return None;
        }
        Some((
            slot,
            Loc {
                offset: w0 >> LEN_BITS,
                len: (w0 & MAX_ENTRY_LEN) as u32,
            },
        ))
    }

    /// The location stored at `slot` (no fingerprint check).
    #[inline]
    pub fn loc_at(&self, slot: usize) -> Option<Loc> {
        let (w0, _) = self.descriptor(slot)?;
        Some(Loc {
            offset: w0 >> LEN_BITS,
            len: (w0 & MAX_ENTRY_LEN) as u32,
        })
    }
}

/// Scratch memory for one build, sized for `n` keys and reusable across builds.
#[derive(Debug, Default)]
pub struct Builder {
    bucket_of: Vec<u32>,
    key_hash: Vec<u64>,
    bucket_start: Vec<u32>,
    keys_by_bucket: Vec<u32>,
    order: Vec<u32>,
    taken: Vec<u64>,
    slot_of: Vec<u32>,
    pilots: Vec<u16>,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-sizes every scratch vector for `n` keys (the only allocations of a build).
    pub fn reserve(&mut self, n: usize) {
        let p = Params::for_keys(n as u64, 0);
        let m = (p.num_buckets as usize).saturating_add(1);
        let words = (p.table_size as usize).saturating_add(63) / 64;
        fn ensure<T>(v: &mut Vec<T>, want: usize) {
            v.clear();
            if v.capacity() < want {
                v.reserve_exact(want.saturating_sub(v.capacity()));
            }
        }
        ensure(&mut self.bucket_of, n);
        ensure(&mut self.keys_by_bucket, n);
        ensure(&mut self.slot_of, n);
        ensure(&mut self.key_hash, n);
        ensure(&mut self.bucket_start, m);
        ensure(&mut self.order, m);
        ensure(&mut self.taken, words);
        ensure(&mut self.pilots, m);
    }

    /// Builds the index of `oids` into `out`, which must be at least [`file_len_for_keys`]
    /// bytes; `loc_of(i)` is the location of `oids[i]`. Returns the number of bytes written.
    pub fn build(
        &mut self,
        oids: &[Oid],
        loc_of: &dyn Fn(usize) -> Loc,
        pack_len: u64,
        out: &mut [u8],
    ) -> Result<usize, MphfError> {
        let n = oids.len();
        if n >= (1usize << 32) {
            return Err(MphfError::TooManyKeys);
        }
        for i in 0..n {
            let l = loc_of(i);
            if l.offset > MAX_PACK_OFFSET {
                return Err(MphfError::OffsetTooLarge);
            }
            if u64::from(l.len) > MAX_ENTRY_LEN {
                return Err(MphfError::LenTooLarge);
            }
        }
        self.reserve(n);
        let mut seed: u64 = 0x5EED_0F6E_571D_0000 ^ (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for _attempt in 0..64 {
            match self.try_build(oids, Params::for_keys(n as u64, seed)) {
                Ok(p) => return self.write(oids, loc_of, &p, pack_len, out),
                Err(MphfError::NoPilot | MphfError::BucketTooLarge) => {
                    seed = mix64(seed.wrapping_add(0xA5A5_5A5A_1234_5678))
                }
                Err(e) => return Err(e),
            }
        }
        Err(MphfError::NoPilot)
    }

    fn try_build(&mut self, oids: &[Oid], p: Params) -> Result<Params, MphfError> {
        let n = oids.len();
        let m = p.num_buckets as usize;
        self.bucket_of.clear();
        self.key_hash.clear();
        self.bucket_start.clear();
        self.bucket_start.resize(m.saturating_add(1), 0);
        for oid in oids {
            let b = p.bucket(oid) as u32;
            self.bucket_of.push(b);
            self.key_hash.push(p.key_hash(oid));
            if let Some(c) = self.bucket_start.get_mut((b as usize).saturating_add(1)) {
                *c = c.saturating_add(1);
            }
        }
        // Bucket sizes → prefix sums; a bucket above 255 keys means an adversarial key set.
        let mut max_size = 0u32;
        for i in 1..=m {
            let (prev, cur) = (
                self.bucket_start
                    .get(i.saturating_sub(1))
                    .copied()
                    .unwrap_or(0),
                self.bucket_start.get(i).copied().unwrap_or(0),
            );
            max_size = max_size.max(cur);
            if let Some(s) = self.bucket_start.get_mut(i) {
                *s = prev.saturating_add(cur);
            }
        }
        if max_size > 255 {
            return Err(MphfError::BucketTooLarge);
        }
        self.keys_by_bucket.clear();
        self.keys_by_bucket.resize(n, 0);
        {
            // Fill positions: reuse `order` as a per-bucket cursor copy of bucket_start.
            self.order.clear();
            self.order
                .extend_from_slice(self.bucket_start.get(..m).unwrap_or(&[]));
            for (k, &b) in self.bucket_of.iter().enumerate() {
                if let Some(cur) = self.order.get_mut(b as usize) {
                    if let Some(slot) = self.keys_by_bucket.get_mut(*cur as usize) {
                        *slot = k as u32;
                    }
                    *cur = cur.saturating_add(1);
                }
            }
        }
        // Process buckets largest first (counting sort on size).
        let mut size_count = [0u32; 257];
        for b in 0..m {
            let size = self.bucket_size(b);
            if let Some(c) = size_count.get_mut(size) {
                *c = c.saturating_add(1);
            }
        }
        let mut size_start = [0u32; 257];
        let mut acc = 0u32;
        for size in (0..=256usize).rev() {
            if let Some(s) = size_start.get_mut(size) {
                *s = acc;
            }
            acc = acc.saturating_add(size_count.get(size).copied().unwrap_or(0));
        }
        self.order.clear();
        self.order.resize(m, 0);
        for b in 0..m {
            let size = self.bucket_size(b);
            if let Some(cur) = size_start.get_mut(size) {
                if let Some(slot) = self.order.get_mut(*cur as usize) {
                    *slot = b as u32;
                }
                *cur = cur.saturating_add(1);
            }
        }
        // Pilot search.
        let words = (p.table_size as usize).saturating_add(63) / 64;
        self.taken.clear();
        self.taken.resize(words, 0);
        self.pilots.clear();
        self.pilots.resize(m, 0);
        self.slot_of.clear();
        self.slot_of.resize(n, 0);
        for i in 0..m {
            let b = self.order.get(i).copied().unwrap_or(0) as usize;
            let (start, end) = (
                self.bucket_start.get(b).copied().unwrap_or(0) as usize,
                self.bucket_start
                    .get(b.saturating_add(1))
                    .copied()
                    .unwrap_or(0) as usize,
            );
            if start == end {
                continue;
            }
            let mut found = false;
            'pilots: for pilot in 0..=u16::MAX {
                let ph = p.pilot_hash(pilot);
                let mut placed = start;
                while placed < end {
                    let k = self.keys_by_bucket.get(placed).copied().unwrap_or(0) as usize;
                    let pos = p.position(self.key_hash.get(k).copied().unwrap_or(0), ph);
                    if self.is_taken(pos) {
                        // Undo this attempt's marks.
                        for j in start..placed {
                            let kj = self.keys_by_bucket.get(j).copied().unwrap_or(0) as usize;
                            let pj = p.position(self.key_hash.get(kj).copied().unwrap_or(0), ph);
                            self.clear_taken(pj);
                        }
                        continue 'pilots;
                    }
                    self.set_taken(pos);
                    placed = placed.saturating_add(1);
                }
                if let Some(s) = self.pilots.get_mut(b) {
                    *s = pilot;
                }
                for j in start..end {
                    let kj = self.keys_by_bucket.get(j).copied().unwrap_or(0) as usize;
                    let pj = p.position(self.key_hash.get(kj).copied().unwrap_or(0), ph);
                    if let Some(s) = self.slot_of.get_mut(kj) {
                        *s = pj as u32; // raw position; remapped below
                    }
                }
                found = true;
                break;
            }
            if !found {
                return Err(MphfError::NoPilot);
            }
        }
        Ok(p)
    }

    #[inline]
    fn bucket_size(&self, b: usize) -> usize {
        let s = self.bucket_start.get(b).copied().unwrap_or(0);
        let e = self
            .bucket_start
            .get(b.saturating_add(1))
            .copied()
            .unwrap_or(0);
        e.saturating_sub(s) as usize
    }

    #[inline]
    fn is_taken(&self, pos: u64) -> bool {
        let w = self
            .taken
            .get((pos / 64) as usize)
            .copied()
            .unwrap_or(u64::MAX);
        w & (1u64 << (pos % 64)) != 0
    }

    #[inline]
    fn set_taken(&mut self, pos: u64) {
        if let Some(w) = self.taken.get_mut((pos / 64) as usize) {
            *w |= 1u64 << (pos % 64);
        }
    }

    #[inline]
    fn clear_taken(&mut self, pos: u64) {
        if let Some(w) = self.taken.get_mut((pos / 64) as usize) {
            *w &= !(1u64 << (pos % 64));
        }
    }

    fn write(
        &mut self,
        oids: &[Oid],
        loc_of: &dyn Fn(usize) -> Loc,
        p: &Params,
        pack_len: u64,
        out: &mut [u8],
    ) -> Result<usize, MphfError> {
        let n = oids.len();
        let m = p.num_buckets as usize;
        let total = file_len_for_keys(n as u64);
        let out = out.get_mut(..total).ok_or(MphfError::OutputTooSmall)?;
        out.fill(0);
        // Header.
        let put64 = |out: &mut [u8], at: usize, v: u64| {
            if let Some(s) = out.get_mut(at..at.saturating_add(8)) {
                s.copy_from_slice(&v.to_le_bytes());
            }
        };
        if let Some(s) = out.get_mut(..4) {
            s.copy_from_slice(&MPHF_MAGIC);
        }
        if let Some(s) = out.get_mut(4..8) {
            s.copy_from_slice(&MPHF_VERSION.to_le_bytes());
        }
        put64(out, 8, p.num_keys);
        put64(out, 16, p.table_size);
        put64(out, 24, p.num_buckets);
        put64(out, 32, p.seed);
        put64(out, 40, pack_len);
        let pilots_at = HEADER_LEN;
        let remap_at = pilots_at.saturating_add(pilots_len(p.num_buckets));
        let desc_at = remap_at.saturating_add(remap_len(p));
        for (b, &pilot) in self.pilots.iter().enumerate().take(m) {
            let at = pilots_at.saturating_add(b.saturating_mul(2));
            if let Some(s) = out.get_mut(at..at.saturating_add(2)) {
                s.copy_from_slice(&pilot.to_le_bytes());
            }
        }
        // Remap: taken positions ≥ n get the free positions < n, both in ascending order.
        let n64 = n as u64;
        let mut next_free: u64 = 0;
        let mut spill = n64;
        while spill < p.table_size {
            if self.is_taken(spill) {
                while next_free < n64 && self.is_taken(next_free) {
                    next_free = next_free.saturating_add(1);
                }
                if next_free >= n64 {
                    return Err(MphfError::Inconsistent);
                }
                let at =
                    remap_at.saturating_add((spill.saturating_sub(n64) as usize).saturating_mul(4));
                if let Some(s) = out.get_mut(at..at.saturating_add(4)) {
                    s.copy_from_slice(&(next_free as u32).to_le_bytes());
                }
                self.set_taken(next_free);
                next_free = next_free.saturating_add(1);
            }
            spill = spill.saturating_add(1);
        }
        // Descriptors at the final slots.
        for (k, oid) in oids.iter().enumerate() {
            let loc = loc_of(k);
            let raw = u64::from(self.slot_of.get(k).copied().unwrap_or(0));
            let slot = if raw < n64 {
                raw
            } else {
                let at =
                    remap_at.saturating_add((raw.saturating_sub(n64) as usize).saturating_mul(4));
                let b: &[u8; 4] = out
                    .get(at..at.saturating_add(4))
                    .and_then(|s| s.try_into().ok())
                    .ok_or(MphfError::Inconsistent)?;
                u64::from(u32::from_le_bytes(*b))
            };
            let at = desc_at.saturating_add((slot as usize).saturating_mul(16));
            let w0 = (loc.offset << LEN_BITS) | u64::from(loc.len);
            let d = out
                .get_mut(at..at.saturating_add(16))
                .ok_or(MphfError::Inconsistent)?;
            let (d0, d1) = d.split_at_mut(8);
            if d0.iter().any(|&x| x != 0) || d1.iter().any(|&x| x != 0) {
                return Err(MphfError::DuplicateKey);
            }
            d0.copy_from_slice(&w0.to_le_bytes());
            d1.copy_from_slice(&fingerprint(oid).to_le_bytes());
        }
        Ok(total)
    }
}

/// Builds an index into a freshly allocated, exactly sized `Vec` (pipeline / tests).
pub fn build_vec(oids: &[Oid], locs: &[Loc], pack_len: u64) -> Result<Vec<u8>, MphfError> {
    if oids.len() != locs.len() {
        return Err(MphfError::Inconsistent);
    }
    let mut out = vec![0u8; file_len_for_keys(oids.len() as u64)];
    let mut b = Builder::new();
    let loc_of = |i: usize| locs.get(i).copied().unwrap_or(Loc { offset: 0, len: 0 });
    let n = b.build(oids, &loc_of, pack_len, &mut out)?;
    out.truncate(n);
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    fn fake_oid(i: u64) -> Oid {
        let mut o = [0u8; 20];
        let a = mix64(i.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1));
        let b = mix64(a ^ 0xFACE);
        let c = mix64(b ^ 0xBEEF);
        o[..8].copy_from_slice(&a.to_le_bytes());
        o[8..16].copy_from_slice(&b.to_le_bytes());
        o[16..20].copy_from_slice(&(c as u32).to_le_bytes());
        Oid(o)
    }

    fn check(n: u64) {
        let oids: Vec<Oid> = (0..n).map(fake_oid).collect();
        let locs: Vec<Loc> = (0..n)
            .map(|i| Loc {
                offset: i * 97 + 12,
                len: (i % 5000) as u32 + 1,
            })
            .collect();
        let bytes = build_vec(&oids, &locs, n * 97 + 32).unwrap();
        assert_eq!(bytes.len(), file_len_for_keys(n));
        let v = MphfView::parse(&bytes).unwrap();
        assert_eq!(v.num_keys(), n);
        let mut seen = vec![false; n as usize];
        for (i, o) in oids.iter().enumerate() {
            let (slot, loc) = v.lookup(o).unwrap_or_else(|| panic!("key {i} missing"));
            assert_eq!(loc, locs[i]);
            assert!(!seen[slot], "slot {slot} used twice");
            seen[slot] = true;
        }
        // Keys not in the set are rejected.
        for i in n..n + 1000 {
            assert!(v.lookup(&fake_oid(i)).is_none());
        }
    }

    #[test]
    fn builds_and_looks_up_small_and_medium_sets() {
        for n in [0u64, 1, 2, 3, 17, 100, 1000, 65_537] {
            check(n);
        }
    }

    #[test]
    fn rejects_bad_input() {
        let o = [fake_oid(1), fake_oid(1)];
        let l = [Loc { offset: 0, len: 1 }, Loc { offset: 1, len: 1 }];
        assert!(matches!(
            build_vec(&o, &l, 0),
            Err(MphfError::NoPilot) | Err(MphfError::DuplicateKey)
        ));
        let l2 = [Loc {
            offset: MAX_PACK_OFFSET + 1,
            len: 1,
        }];
        assert_eq!(build_vec(&o[..1], &l2, 0), Err(MphfError::OffsetTooLarge));
        assert_eq!(MphfView::parse(b"nope").err(), Some(MphfError::Truncated));
        let mut bad = build_vec(&o[..1], &l[..1], 0).unwrap();
        bad[0] = b'X';
        assert_eq!(MphfView::parse(&bad).err(), Some(MphfError::BadMagic));
    }
}
