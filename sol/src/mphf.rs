//! CHD displacement hashing. Descriptors reject most misses with a fingerprint;
//! a mapped full-key comparison always proves membership before serving bytes.

use crate::arena::{append, reserve};
use crate::codec::{Oid, be32, be64, checksum, take, verify_checksum};
use crate::slab::{MetaView, Record};
use crate::{Error, Result};

const MAGIC: &[u8; 8] = b"GMPH\0\0\0\x01";
const HEADER: usize = 48;
const DIRECT: u32 = 1 << 31;
const EMPTY: u32 = u32::MAX;

#[inline]
pub fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[inline]
pub fn hash(oid: Oid, seed: u64) -> u64 {
    let [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t] = oid.0;
    let lo = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
    let hi = u64::from_le_bytes([i, j, k, l, m, n, o, p]);
    let tail = u64::from(u32::from_le_bytes([q, r, s, t]));
    mix(lo ^ hi.rotate_left(23) ^ tail.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ seed)
}

#[inline]
fn reduce(value: u64, len: usize) -> usize {
    ((u128::from(value).wrapping_mul(len as u128)) >> 64) as usize
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackedObjLoc(pub u64);

impl PackedObjLoc {
    pub const MAX_INLINE_LEN: u32 = 0x3ffe;

    pub fn new(oid: Oid, offset: u64, len: u32) -> Result<Self> {
        if len == 0 {
            return Err(Error::Invalid("zero descriptor length"));
        }
        let [.., a, b] = oid.0;
        let fingerprint = u64::from(u16::from_be_bytes([a, b]));
        let offset_overflow = offset >= 0x3_ffff_ffff;
        let length = if len > Self::MAX_INLINE_LEN || offset_overflow {
            0x3fff
        } else {
            u64::from(len)
        };
        let offset = if offset_overflow {
            0x3_ffff_ffff
        } else {
            offset
        };
        Ok(Self((fingerprint << 48) | (offset << 14) | length))
    }

    pub fn matches(self, oid: Oid) -> bool {
        let [.., a, b] = oid.0;
        self.0 >> 48 == u64::from(u16::from_be_bytes([a, b]))
    }

    pub fn offset(self) -> u64 {
        (self.0 >> 14) & 0x3_ffff_ffff
    }

    pub fn inline_len(self) -> Option<u32> {
        let len = (self.0 & 0x3fff) as u32;
        (len != 0x3fff).then_some(len)
    }
}

pub struct BuildScratch {
    heads: Vec<u32>,
    next: Vec<u32>,
    counts: Vec<u32>,
    order: Vec<u32>,
    pilots: Vec<u32>,
    slots: Vec<u32>,
    hashes: Vec<u64>,
}

impl BuildScratch {
    pub fn new(max_objects: usize) -> Result<Self> {
        if max_objects >= DIRECT as usize {
            return Err(Error::Capacity("MPHF key count"));
        }
        let buckets = max_objects.div_ceil(3).max(1);
        Ok(Self {
            heads: reserve(buckets)?,
            next: reserve(max_objects)?,
            counts: reserve(buckets)?,
            order: reserve(buckets)?,
            pilots: reserve(buckets)?,
            slots: reserve(max_objects)?,
            hashes: reserve(max_objects)?,
        })
    }

    pub fn encode(&mut self, records: &[Record], pack_id: Oid, out: &mut Vec<u8>) -> Result<()> {
        let n = records.len();
        let buckets = n.div_ceil(3).max(1);
        self.heads.resize(buckets, EMPTY);
        self.counts.resize(buckets, 0);
        self.pilots.resize(buckets, 0);
        self.next.resize(n, EMPTY);
        self.slots.resize(n, EMPTY);
        self.hashes.resize(n, 0);
        self.order.clear();
        for i in 0..buckets {
            self.order.push(i as u32);
        }
        let mut chosen_seed = None;
        for attempt in 0..16u64 {
            let seed = mix(attempt.wrapping_add(0xa076_1d64_78bd_642f));
            self.heads.fill(EMPTY);
            self.counts.fill(0);
            self.slots.fill(EMPTY);
            self.pilots.fill(0);
            for (rank, record) in records.iter().enumerate() {
                let value = hash(record.oid, seed);
                *self
                    .hashes
                    .get_mut(rank)
                    .ok_or(Error::Invalid("MPHF hash scratch"))? = value;
                let bucket = reduce(value, buckets);
                let head = self
                    .heads
                    .get_mut(bucket)
                    .ok_or(Error::Invalid("MPHF bucket"))?;
                *self.next.get_mut(rank).ok_or(Error::Invalid("MPHF next"))? = *head;
                *head = rank as u32;
                let count = self
                    .counts
                    .get_mut(bucket)
                    .ok_or(Error::Invalid("MPHF count"))?;
                *count = count.saturating_add(1);
            }
            let counts = &self.counts;
            self.order.sort_unstable_by_key(|bucket| {
                std::cmp::Reverse(counts.get(*bucket as usize).copied().unwrap_or(0))
            });
            if self.place(n)? {
                chosen_seed = Some(seed);
                break;
            }
        }
        let seed = chosen_seed.ok_or(Error::Capacity("MPHF pilot search; use a smaller slab"))?;
        out.clear();
        append(out, MAGIC)?;
        append(out, &(n as u32).to_be_bytes())?;
        append(out, &(buckets as u32).to_be_bytes())?;
        append(out, &seed.to_be_bytes())?;
        append(out, &pack_id.0)?;
        append(out, &[0; 4])?;
        for pilot in &self.pilots {
            append(out, &pilot.to_be_bytes())?;
        }
        for rank in &self.slots {
            append(out, &rank.to_be_bytes())?;
        }
        for rank in &self.slots {
            let record = records
                .get(*rank as usize)
                .ok_or(Error::Invalid("MPHF ordinal"))?;
            append(
                out,
                &PackedObjLoc::new(record.oid, record.offset, record.len)?
                    .0
                    .to_be_bytes(),
            )?;
        }
        let sum = checksum(out);
        append(out, &sum)
    }

    fn place(&mut self, n: usize) -> Result<bool> {
        let mut candidate = [0usize; 128];
        for &bucket in &self.order {
            let bucket = bucket as usize;
            let count = *self
                .counts
                .get(bucket)
                .ok_or(Error::Invalid("MPHF bucket count"))? as usize;
            if count <= 1 {
                continue;
            }
            if count > candidate.len() {
                return Ok(false);
            }
            let mut placed = false;
            for pilot in 0..1_000_000u32 {
                let mut key = *self
                    .heads
                    .get(bucket)
                    .ok_or(Error::Invalid("MPHF bucket head"))?;
                let mut used = 0usize;
                let salt = mix(u64::from(pilot).wrapping_add(0xe703_7ed1_a0b4_28db));
                let mut collision = false;
                while key != EMPTY {
                    let value = *self
                        .hashes
                        .get(key as usize)
                        .ok_or(Error::Invalid("MPHF cached hash"))?;
                    let pos = reduce(mix(value ^ salt), n);
                    if self.slots.get(pos).copied() != Some(EMPTY)
                        || candidate
                            .get(..used)
                            .ok_or(Error::Invalid("MPHF candidates"))?
                            .contains(&pos)
                    {
                        collision = true;
                        break;
                    }
                    *candidate
                        .get_mut(used)
                        .ok_or(Error::Invalid("MPHF candidate count"))? = pos;
                    used = used.saturating_add(1);
                    key = *self
                        .next
                        .get(key as usize)
                        .ok_or(Error::Invalid("MPHF chain"))?;
                }
                if collision {
                    continue;
                }
                key = *self.heads.get(bucket).ok_or(Error::Invalid("MPHF head"))?;
                for &pos in candidate
                    .get(..used)
                    .ok_or(Error::Invalid("MPHF candidates"))?
                {
                    *self
                        .slots
                        .get_mut(pos)
                        .ok_or(Error::Invalid("MPHF placement"))? = key;
                    key = *self
                        .next
                        .get(key as usize)
                        .ok_or(Error::Invalid("MPHF chain"))?;
                }
                *self
                    .pilots
                    .get_mut(bucket)
                    .ok_or(Error::Invalid("MPHF pilot"))? = pilot;
                placed = true;
                break;
            }
            if !placed {
                return Ok(false);
            }
        }
        let mut free = 0usize;
        for &bucket in &self.order {
            let bucket = bucket as usize;
            if self.counts.get(bucket).copied() != Some(1) {
                continue;
            }
            while self.slots.get(free).copied() != Some(EMPTY) {
                free = free.saturating_add(1);
                if free >= n {
                    return Err(Error::Invalid("MPHF singleton capacity"));
                }
            }
            *self
                .pilots
                .get_mut(bucket)
                .ok_or(Error::Invalid("MPHF singleton pilot"))? = DIRECT | (free as u32);
            *self
                .slots
                .get_mut(free)
                .ok_or(Error::Invalid("MPHF singleton slot"))? = *self
                .heads
                .get(bucket)
                .ok_or(Error::Invalid("MPHF singleton key"))?;
        }
        Ok(true)
    }
}

#[derive(Clone, Copy)]
pub struct MphfView<'a> {
    bytes: &'a [u8],
    n: usize,
    buckets: usize,
    seed: u64,
    ranks_at: usize,
    descriptors_at: usize,
}

impl<'a> MphfView<'a> {
    pub(crate) fn validated(
        bytes: &'a [u8],
        n: usize,
        buckets: usize,
        ranks_at: usize,
    ) -> Result<Self> {
        Ok(Self {
            bytes,
            n,
            buckets,
            seed: be64(bytes, 16)?,
            ranks_at,
            descriptors_at: ranks_at.saturating_add(n.saturating_mul(4)),
        })
    }

    pub fn parse(bytes: &'a [u8], pack_id: Oid, count: usize) -> Result<Self> {
        let body = verify_checksum(bytes)?;
        if take(body, 0, 8)? != MAGIC || Oid::from_bytes(take(body, 24, 20)?)? != pack_id {
            return Err(Error::Invalid("MPHF header or pack identity"));
        }
        let n = be32(body, 8)? as usize;
        let buckets = be32(body, 12)? as usize;
        if n != count || n >= DIRECT as usize || buckets != n.div_ceil(3).max(1) {
            return Err(Error::Invalid("MPHF dimensions"));
        }
        let ranks_at = HEADER
            .checked_add(buckets.checked_mul(4).ok_or(Error::Invalid("MPHF size"))?)
            .ok_or(Error::Invalid("MPHF size"))?;
        let descriptors_at = ranks_at
            .checked_add(n.checked_mul(4).ok_or(Error::Invalid("MPHF size"))?)
            .ok_or(Error::Invalid("MPHF size"))?;
        if body.len()
            != descriptors_at
                .checked_add(n.checked_mul(8).ok_or(Error::Invalid("MPHF size"))?)
                .ok_or(Error::Invalid("MPHF size"))?
        {
            return Err(Error::Invalid("MPHF length"));
        }
        Ok(Self {
            bytes: body,
            n,
            buckets,
            seed: be64(body, 16)?,
            ranks_at,
            descriptors_at,
        })
    }

    pub fn lookup(&self, oid: Oid, meta: MetaView<'_>) -> Result<Option<u32>> {
        if self.n == 0 {
            return Ok(None);
        }
        let h = hash(oid, self.seed);
        let bucket = reduce(h, self.buckets);
        let pilot = be32(self.bytes, HEADER.saturating_add(bucket.saturating_mul(4)))?;
        let slot = if pilot & DIRECT != 0 {
            (pilot & !DIRECT) as usize
        } else {
            reduce(
                mix(h ^ mix(u64::from(pilot).wrapping_add(0xe703_7ed1_a0b4_28db))),
                self.n,
            )
        };
        if slot >= self.n {
            return Err(Error::Invalid("MPHF direct slot"));
        }
        let desc = PackedObjLoc(be64(
            self.bytes,
            self.descriptors_at.saturating_add(slot.saturating_mul(8)),
        )?);
        if !desc.matches(oid) {
            return Ok(None);
        }
        let rank = be32(
            self.bytes,
            self.ranks_at.saturating_add(slot.saturating_mul(4)),
        )?;
        let record = meta.record(rank)?;
        if record.oid != oid {
            return Ok(None);
        }
        if desc.offset() != 0x3_ffff_ffff && record.offset != desc.offset()
            || desc.inline_len().is_some_and(|len| record.len != len)
        {
            return Err(Error::Invalid("MPHF descriptor"));
        }
        Ok(Some(rank))
    }
}

/// Reusable generation-stamped open addressing. Values are ordinals; full keys stay
/// in the caller's slab/record arrays instead of being duplicated in the hash table.
pub struct OrdinalMap {
    slots: Vec<u64>,
    generation: u32,
    mask: usize,
}

impl OrdinalMap {
    pub fn new(max_entries: usize) -> Result<Self> {
        let len = max_entries
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(Error::Capacity("OID table"))?
            .max(2);
        let mut slots = reserve(len)?;
        slots.resize(len, 0);
        Ok(Self {
            slots,
            generation: 1,
            mask: len.saturating_sub(1),
        })
    }

    pub fn reset(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.slots.fill(0);
            self.generation = 1;
        }
    }

    pub fn find(&self, oid: Oid, key: impl Fn(u32) -> Result<Oid>) -> Result<Option<u32>> {
        let mut slot = (hash(oid, 0) as usize) & self.mask;
        for _ in 0..self.slots.len() {
            let word = *self
                .slots
                .get(slot)
                .ok_or(Error::Invalid("OID table slot"))?;
            if (word >> 32) as u32 != self.generation {
                return Ok(None);
            }
            let ordinal = word as u32;
            if key(ordinal)? == oid {
                return Ok(Some(ordinal));
            }
            slot = slot.wrapping_add(1) & self.mask;
        }
        Err(Error::Capacity("OID table probe"))
    }

    pub fn insert(
        &mut self,
        oid: Oid,
        ordinal: u32,
        key: impl Fn(u32) -> Result<Oid>,
    ) -> Result<Option<u32>> {
        let mut slot = (hash(oid, 0) as usize) & self.mask;
        for _ in 0..self.slots.len() {
            let word = self
                .slots
                .get_mut(slot)
                .ok_or(Error::Invalid("OID insert slot"))?;
            if (*word >> 32) as u32 != self.generation {
                *word = (u64::from(self.generation) << 32) | u64::from(ordinal);
                return Ok(None);
            }
            let prior = *word as u32;
            if key(prior)? == oid {
                return Ok(Some(prior));
            }
            slot = slot.wrapping_add(1) & self.mask;
        }
        Err(Error::Capacity("OID table"))
    }
}
