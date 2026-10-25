//! Git pack index (`.idx`) version 2: writer and zero-copy reader.

use crate::buf::{ByteWriter, be32, be64, sub};
use crate::oid::Oid20;

pub const IDX_MAGIC: [u8; 4] = [0xFF, b't', b'O', b'c'];
pub const IDX_VERSION: u32 = 2;
const HEADER_LEN: usize = 8;
const FANOUT_LEN: usize = 256 * 4;
const LARGE_OFFSET_THRESHOLD: u64 = 0x7FFF_FFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdxEntry {
    pub oid: Oid20,
    pub crc32: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdxError {
    Overflow,
    Duplicate,
    Truncated,
    BadMagic,
    BadVersion,
    BadFanout,
}

impl core::fmt::Display for IdxError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Overflow => "idx buffer too small",
            Self::Duplicate => "duplicate object in idx",
            Self::Truncated => "truncated idx",
            Self::BadMagic => "bad idx signature",
            Self::BadVersion => "unsupported idx version",
            Self::BadFanout => "inconsistent idx fanout",
        })
    }
}

impl std::error::Error for IdxError {}

pub fn idx_len(n: usize, n_large: usize) -> Option<usize> {
    HEADER_LEN
        .checked_add(FANOUT_LEN)?
        .checked_add(n.checked_mul(28)?)?
        .checked_add(n_large.checked_mul(8)?)?
        .checked_add(40)
}

/// Sorts `entries` in place (zero allocation) and writes the `.idx` v2 stream into `out`.
pub fn write_idx_v2(
    entries: &mut [IdxEntry],
    pack_checksum: &[u8; 20],
    out: &mut [u8],
) -> Result<usize, IdxError> {
    entries.sort_unstable_by_key(|e| e.oid);
    for pair in entries.windows(2) {
        if let [a, b] = pair {
            if a.oid == b.oid {
                return Err(IdxError::Duplicate);
            }
        }
    }
    let n_large = entries
        .iter()
        .filter(|e| e.offset > LARGE_OFFSET_THRESHOLD)
        .count();
    let total = idx_len(entries.len(), n_large).ok_or(IdxError::Overflow)?;
    let out = out.get_mut(..total).ok_or(IdxError::Overflow)?;
    let mut w = ByteWriter::new(out);
    let ov = |_| IdxError::Overflow;
    w.put(&IDX_MAGIC).map_err(ov)?;
    w.put_u32_be(IDX_VERSION).map_err(ov)?;
    let mut counts = [0u32; 256];
    for e in entries.iter() {
        if let Some(c) = counts.get_mut(usize::from(e.oid.first_byte())) {
            *c = c.saturating_add(1);
        }
    }
    let mut cum = 0u32;
    for c in counts {
        cum = cum.saturating_add(c);
        w.put_u32_be(cum).map_err(ov)?;
    }
    for e in entries.iter() {
        w.put(&e.oid.0).map_err(ov)?;
    }
    for e in entries.iter() {
        w.put_u32_be(e.crc32).map_err(ov)?;
    }
    let mut large_i: u32 = 0;
    for e in entries.iter() {
        if e.offset > LARGE_OFFSET_THRESHOLD {
            w.put_u32_be(0x8000_0000 | large_i).map_err(ov)?;
            large_i = large_i.saturating_add(1);
        } else {
            w.put_u32_be(u32::try_from(e.offset).map_err(|_| IdxError::Overflow)?)
                .map_err(ov)?;
        }
    }
    for e in entries.iter() {
        if e.offset > LARGE_OFFSET_THRESHOLD {
            w.put_u64_be(e.offset).map_err(ov)?;
        }
    }
    w.put(pack_checksum).map_err(ov)?;
    let digest = crate::sha1_parts(&[w.written()]);
    w.put(&digest).map_err(ov)?;
    Ok(w.len())
}

#[derive(Debug, Clone, Copy)]
pub struct IdxView<'a> {
    data: &'a [u8],
    n: usize,
    oids_at: usize,
    crcs_at: usize,
    offs_at: usize,
    large_at: usize,
    n_large: usize,
}

impl<'a> IdxView<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self, IdxError> {
        if data.get(..4) != Some(&IDX_MAGIC[..]) {
            return Err(IdxError::BadMagic);
        }
        if be32(data, 4) != Some(IDX_VERSION) {
            return Err(IdxError::BadVersion);
        }
        let mut prev = 0u32;
        for i in 0..256usize {
            let v = be32(data, HEADER_LEN.saturating_add(i.saturating_mul(4)))
                .ok_or(IdxError::Truncated)?;
            if v < prev {
                return Err(IdxError::BadFanout);
            }
            prev = v;
        }
        let n = usize::try_from(prev).map_err(|_| IdxError::BadFanout)?;
        let oids_at = HEADER_LEN
            .checked_add(FANOUT_LEN)
            .ok_or(IdxError::Truncated)?;
        let crcs_at = oids_at
            .checked_add(n.checked_mul(20).ok_or(IdxError::Truncated)?)
            .ok_or(IdxError::Truncated)?;
        let offs_at = crcs_at
            .checked_add(n.checked_mul(4).ok_or(IdxError::Truncated)?)
            .ok_or(IdxError::Truncated)?;
        let large_at = offs_at
            .checked_add(n.checked_mul(4).ok_or(IdxError::Truncated)?)
            .ok_or(IdxError::Truncated)?;
        let min_len = large_at.checked_add(40).ok_or(IdxError::Truncated)?;
        let extra = data.len().checked_sub(min_len).ok_or(IdxError::Truncated)?;
        if extra.wrapping_rem(8) != 0 {
            return Err(IdxError::Truncated);
        }
        Ok(Self {
            data,
            n,
            oids_at,
            crcs_at,
            offs_at,
            large_at,
            n_large: extra.wrapping_div(8),
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

    #[inline]
    pub fn oid(&self, i: usize) -> Option<Oid20> {
        if i >= self.n {
            return None;
        }
        Oid20::from_slice(sub(
            self.data,
            self.oids_at.checked_add(i.checked_mul(20)?)?,
            20,
        )?)
    }

    #[inline]
    pub fn crc32(&self, i: usize) -> Option<u32> {
        if i >= self.n {
            return None;
        }
        be32(self.data, self.crcs_at.checked_add(i.checked_mul(4)?)?)
    }

    #[inline]
    pub fn offset(&self, i: usize) -> Option<u64> {
        if i >= self.n {
            return None;
        }
        let v = be32(self.data, self.offs_at.checked_add(i.checked_mul(4)?)?)?;
        if v & 0x8000_0000 == 0 {
            return Some(u64::from(v));
        }
        let li = usize::try_from(v & 0x7FFF_FFFF).ok()?;
        if li >= self.n_large {
            return None;
        }
        be64(self.data, self.large_at.checked_add(li.checked_mul(8)?)?)
    }

    pub fn pack_checksum(&self) -> Option<[u8; 20]> {
        let at = self.data.len().checked_sub(40)?;
        sub(self.data, at, 20)?.try_into().ok()
    }

    pub fn verify_checksum(&self) -> bool {
        let Some(body_len) = self.data.len().checked_sub(20) else {
            return false;
        };
        match (self.data.get(..body_len), self.data.get(body_len..)) {
            (Some(body), Some(sum)) => crate::sha1_parts(&[body]) == sum,
            _ => false,
        }
    }

    pub fn find(&self, oid: &Oid20) -> Option<usize> {
        let fb = usize::from(oid.first_byte());
        let hi = usize::try_from(be32(
            self.data,
            HEADER_LEN.checked_add(fb.checked_mul(4)?)?,
        )?)
        .ok()?;
        let lo = if fb == 0 {
            0
        } else {
            usize::try_from(be32(
                self.data,
                HEADER_LEN.checked_add(fb.checked_sub(1)?.checked_mul(4)?)?,
            )?)
            .ok()?
        };
        let (mut lo, mut hi) = (lo, hi.min(self.n));
        while lo < hi {
            let mid = lo.checked_add(hi.checked_sub(lo)?.wrapping_div(2))?;
            match self.oid(mid)?.cmp(oid) {
                core::cmp::Ordering::Less => lo = mid.checked_add(1)?,
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}
