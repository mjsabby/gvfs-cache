//! Git pack index (`.idx`) version 2: a streaming writer and a zero-copy reader.
//!
//! Every slab carries a stock `.idx` so that the client gets a ready-made index with each
//! prefetch pack and never has to run `git index-pack` over gigabytes. Spliced packs get
//! their `.idx` merged from the members' sorted tables (see `splice.rs`).

use core::fmt;
use std::io::Write;

use sha1::{Digest, Sha1};

use crate::oid::Oid;

pub const IDX_MAGIC: [u8; 4] = [0xFF, b't', b'O', b'c'];
pub const IDX_VERSION: u32 = 2;
pub const IDX_HEADER_LEN: usize = 8;
pub const FANOUT_LEN: usize = 256 * 4;
pub const IDX_TRAILER_LEN: usize = 40;
/// Offsets at or above this go to the 8-byte table.
pub const LARGE_OFFSET_THRESHOLD: u64 = 0x8000_0000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdxError {
    Truncated,
    BadMagic,
    BadVersion,
    BadFanout,
    Inconsistent,
    Io,
}

impl fmt::Display for IdxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "truncated idx",
            Self::BadMagic => "bad idx signature",
            Self::BadVersion => "unsupported idx version",
            Self::BadFanout => "inconsistent idx fanout",
            Self::Inconsistent => "inconsistent idx input",
            Self::Io => "idx write failed",
        })
    }
}

impl std::error::Error for IdxError {}

/// Exact size of an idx v2 file with `n` objects of which `n_large` need 8-byte offsets.
pub fn idx_len(n: u64, n_large: u64) -> u64 {
    (IDX_HEADER_LEN as u64)
        .saturating_add(FANOUT_LEN as u64)
        .saturating_add(n.saturating_mul(28))
        .saturating_add(n_large.saturating_mul(8))
        .saturating_add(IDX_TRAILER_LEN as u64)
}

/// A writer that hashes everything it forwards (the idx ends with its own SHA-1).
struct Hashing<'a, W: Write> {
    w: &'a mut W,
    h: Sha1,
}

impl<W: Write> Hashing<'_, W> {
    fn put(&mut self, b: &[u8]) -> Result<(), IdxError> {
        self.h.update(b);
        self.w.write_all(b).map_err(|_| IdxError::Io)
    }
}

/// One object's index record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdxEntry {
    pub oid: Oid,
    pub crc: u32,
    pub offset: u64,
}

/// Writes an idx v2 for `n = order.len()` objects. `order` lists entry indices in ascending
/// OID order and `entry(i)` describes entry `i`. `buf` is staging memory (any size ≥ 32
/// bytes; 1 MiB is plenty). Never allocates.
pub fn write_idx_v2<W: Write>(
    w: &mut W,
    buf: &mut [u8],
    order: &[u32],
    entry: &dyn Fn(usize) -> Option<IdxEntry>,
    pack_sha: &[u8; 20],
) -> Result<(), IdxError> {
    let n = order.len();
    if buf.len() < 32 {
        return Err(IdxError::Inconsistent);
    }
    let mut out = Hashing { w, h: Sha1::new() };
    out.put(&IDX_MAGIC)?;
    out.put(&IDX_VERSION.to_be_bytes())?;
    // Fanout.
    let mut fanout = [0u32; 256];
    for &i in order {
        let first = entry(i as usize)
            .ok_or(IdxError::Inconsistent)?
            .oid
            .0
            .first()
            .copied()
            .unwrap_or(0);
        if let Some(c) = fanout.get_mut(usize::from(first)) {
            *c = c.saturating_add(1);
        }
    }
    let mut acc = 0u32;
    let mut fan_bytes = [0u8; FANOUT_LEN];
    for (i, c) in fanout.iter().enumerate() {
        acc = acc.saturating_add(*c);
        if let Some(s) =
            fan_bytes.get_mut(i.saturating_mul(4)..i.saturating_mul(4).saturating_add(4))
        {
            s.copy_from_slice(&acc.to_be_bytes());
        }
    }
    if acc as usize != n {
        return Err(IdxError::Inconsistent);
    }
    out.put(&fan_bytes)?;
    // Tables, staged through `buf`.
    let mut stage = Stage { buf, len: 0 };
    for &i in order {
        let e = entry(i as usize).ok_or(IdxError::Inconsistent)?;
        stage.push(&mut out, &e.oid.0)?;
    }
    stage.flush(&mut out)?;
    for &i in order {
        let e = entry(i as usize).ok_or(IdxError::Inconsistent)?;
        stage.push(&mut out, &e.crc.to_be_bytes())?;
    }
    stage.flush(&mut out)?;
    let mut n_large = 0u32;
    for &i in order {
        let off = entry(i as usize).ok_or(IdxError::Inconsistent)?.offset;
        let v = if off >= LARGE_OFFSET_THRESHOLD {
            let idx = 0x8000_0000u32 | n_large;
            n_large = n_large.saturating_add(1);
            idx
        } else {
            off as u32
        };
        stage.push(&mut out, &v.to_be_bytes())?;
    }
    stage.flush(&mut out)?;
    if n_large > 0 {
        for &i in order {
            let off = entry(i as usize).ok_or(IdxError::Inconsistent)?.offset;
            if off >= LARGE_OFFSET_THRESHOLD {
                stage.push(&mut out, &off.to_be_bytes())?;
            }
        }
        stage.flush(&mut out)?;
    }
    out.put(pack_sha)?;
    let digest: [u8; 20] = out.h.clone().finalize().into();
    out.w.write_all(&digest).map_err(|_| IdxError::Io)
}

struct Stage<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl Stage<'_> {
    fn push<W: Write>(&mut self, out: &mut Hashing<'_, W>, b: &[u8]) -> Result<(), IdxError> {
        if self.len.saturating_add(b.len()) > self.buf.len() {
            self.flush(out)?;
        }
        let end = self.len.saturating_add(b.len());
        self.buf
            .get_mut(self.len..end)
            .ok_or(IdxError::Inconsistent)?
            .copy_from_slice(b);
        self.len = end;
        Ok(())
    }

    fn flush<W: Write>(&mut self, out: &mut Hashing<'_, W>) -> Result<(), IdxError> {
        if self.len > 0 {
            out.put(self.buf.get(..self.len).unwrap_or(&[]))?;
            self.len = 0;
        }
        Ok(())
    }
}

/// Zero-copy view of an idx v2 file.
#[derive(Debug, Clone, Copy)]
pub struct IdxView<'a> {
    pub n: usize,
    fanout: &'a [u8],
    oids: &'a [u8],
    crcs: &'a [u8],
    offsets: &'a [u8],
    large: &'a [u8],
    pub pack_sha: [u8; 20],
}

impl<'a> IdxView<'a> {
    pub fn parse(b: &'a [u8]) -> Result<Self, IdxError> {
        if b.get(..4) != Some(&IDX_MAGIC[..]) {
            return Err(IdxError::BadMagic);
        }
        let ver: [u8; 4] = b
            .get(4..8)
            .and_then(|s| s.try_into().ok())
            .ok_or(IdxError::Truncated)?;
        if u32::from_be_bytes(ver) != IDX_VERSION {
            return Err(IdxError::BadVersion);
        }
        let fanout = b
            .get(IDX_HEADER_LEN..IDX_HEADER_LEN.saturating_add(FANOUT_LEN))
            .ok_or(IdxError::Truncated)?;
        let last: [u8; 4] = fanout
            .get(FANOUT_LEN - 4..)
            .and_then(|s| s.try_into().ok())
            .ok_or(IdxError::Truncated)?;
        let n = u32::from_be_bytes(last) as usize;
        let oids_at = IDX_HEADER_LEN.saturating_add(FANOUT_LEN);
        let crcs_at = oids_at.saturating_add(n.saturating_mul(20));
        let offs_at = crcs_at.saturating_add(n.saturating_mul(4));
        let large_at = offs_at.saturating_add(n.saturating_mul(4));
        let trailer_at = b
            .len()
            .checked_sub(IDX_TRAILER_LEN)
            .ok_or(IdxError::Truncated)?;
        if trailer_at < large_at {
            return Err(IdxError::Truncated);
        }
        let pack_sha: [u8; 20] = b
            .get(trailer_at..trailer_at.saturating_add(20))
            .and_then(|s| s.try_into().ok())
            .ok_or(IdxError::Truncated)?;
        Ok(Self {
            n,
            fanout,
            oids: b.get(oids_at..crcs_at).ok_or(IdxError::Truncated)?,
            crcs: b.get(crcs_at..offs_at).ok_or(IdxError::Truncated)?,
            offsets: b.get(offs_at..large_at).ok_or(IdxError::Truncated)?,
            large: b.get(large_at..trailer_at).ok_or(IdxError::Truncated)?,
            pack_sha,
        })
    }

    #[inline]
    pub fn fanout(&self, first_byte: u8) -> u32 {
        let at = usize::from(first_byte).saturating_mul(4);
        let b: [u8; 4] = self
            .fanout
            .get(at..at.saturating_add(4))
            .and_then(|s| s.try_into().ok())
            .unwrap_or([0; 4]);
        u32::from_be_bytes(b)
    }

    #[inline]
    pub fn oid_bytes(&self, i: usize) -> Option<&'a [u8]> {
        let at = i.checked_mul(20)?;
        self.oids.get(at..at.checked_add(20)?)
    }

    #[inline]
    pub fn oid(&self, i: usize) -> Option<Oid> {
        Oid::from_slice(self.oid_bytes(i)?)
    }

    /// The sorted OID table, `20 * n` bytes.
    #[inline]
    pub fn oid_table(&self) -> &'a [u8] {
        self.oids
    }

    /// The CRC table, `4 * n` bytes, same order.
    #[inline]
    pub fn crc_table(&self) -> &'a [u8] {
        self.crcs
    }

    #[inline]
    pub fn crc(&self, i: usize) -> Option<u32> {
        let at = i.checked_mul(4)?;
        let b: [u8; 4] = self.crcs.get(at..at.checked_add(4)?)?.try_into().ok()?;
        Some(u32::from_be_bytes(b))
    }

    #[inline]
    pub fn offset(&self, i: usize) -> Option<u64> {
        let at = i.checked_mul(4)?;
        let b: [u8; 4] = self.offsets.get(at..at.checked_add(4)?)?.try_into().ok()?;
        let v = u32::from_be_bytes(b);
        if v & 0x8000_0000 == 0 {
            return Some(u64::from(v));
        }
        let li = ((v & 0x7FFF_FFFF) as usize).checked_mul(8)?;
        let b8: [u8; 8] = self.large.get(li..li.checked_add(8)?)?.try_into().ok()?;
        Some(u64::from_be_bytes(b8))
    }

    /// The 4-byte offset table, `4 * n` bytes, same order.
    #[inline]
    pub fn offsets_table(&self) -> &'a [u8] {
        self.offsets
    }

    /// Index into the 8-byte table if entry `i` uses it.
    #[inline]
    pub fn large_index(&self, i: usize) -> Option<u32> {
        let at = i.checked_mul(4)?;
        let b: [u8; 4] = self.offsets.get(at..at.checked_add(4)?)?.try_into().ok()?;
        let v = u32::from_be_bytes(b);
        if v & 0x8000_0000 != 0 {
            Some(v & 0x7FFF_FFFF)
        } else {
            None
        }
    }

    /// The 8-byte offset table (`8 * large_count()` bytes).
    #[inline]
    pub fn large_table(&self) -> &'a [u8] {
        self.large
    }

    /// Number of entries in the 8-byte offset table.
    #[inline]
    pub fn large_count(&self) -> usize {
        self.large.len() / 8
    }

    /// Binary search for `oid`; returns its sorted position.
    pub fn find(&self, oid: &Oid) -> Option<usize> {
        let first = oid.0.first().copied()?;
        let lo = if first == 0 {
            0
        } else {
            self.fanout(first.wrapping_sub(1)) as usize
        };
        let hi = self.fanout(first) as usize;
        let (mut lo, mut hi) = (lo, hi);
        while lo < hi {
            let mid = lo.saturating_add(hi.saturating_sub(lo) / 2);
            let cand = self.oid_bytes(mid)?;
            match cand.cmp(&oid.0[..]) {
                core::cmp::Ordering::Less => lo = mid.saturating_add(1),
                core::cmp::Ordering::Greater => hi = mid,
                core::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}

/// Fills `order` with `0..oids.len()` sorted by OID. `scratch` is reused between calls
/// (it holds `(oid, index)` pairs so the sort runs over contiguous memory). The first byte
/// is counting-sorted, then every one of the 256 runs is sorted on its own.
pub fn sort_by_oid(oids: &[Oid], scratch: &mut Vec<(Oid, u32)>, order: &mut Vec<u32>) {
    let n = oids.len();
    let mut counts = [0usize; 257];
    for o in oids {
        let b = usize::from(o.0.first().copied().unwrap_or(0));
        if let Some(c) = counts.get_mut(b.saturating_add(1)) {
            *c = c.saturating_add(1);
        }
    }
    for i in 1..257usize {
        let prev = counts.get(i.saturating_sub(1)).copied().unwrap_or(0);
        if let Some(c) = counts.get_mut(i) {
            *c = c.saturating_add(prev);
        }
    }
    scratch.clear();
    scratch.resize(n, (Oid::ZERO, 0));
    let mut cursor = counts;
    for (i, o) in oids.iter().enumerate() {
        let b = usize::from(o.0.first().copied().unwrap_or(0));
        if let Some(c) = cursor.get_mut(b) {
            if let Some(slot) = scratch.get_mut(*c) {
                *slot = (*o, i as u32);
            }
            *c = c.saturating_add(1);
        }
    }
    for b in 0..256usize {
        let (s, e) = (
            counts.get(b).copied().unwrap_or(0),
            counts.get(b.saturating_add(1)).copied().unwrap_or(0),
        );
        if let Some(run) = scratch.get_mut(s..e) {
            run.sort_unstable();
        }
    }
    order.clear();
    order.extend(scratch.iter().map(|(_, i)| *i));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    fn oid(i: u32) -> Oid {
        let mut o = [0u8; 20];
        let h = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        o[..8].copy_from_slice(&h.to_be_bytes());
        o[8..12].copy_from_slice(&i.to_be_bytes());
        Oid(o)
    }

    #[test]
    fn writes_and_reads_back_with_large_offsets() {
        let n = 5000u32;
        let oids: Vec<Oid> = (0..n).map(oid).collect();
        let crcs: Vec<u32> = (0..n).map(|i| i.wrapping_mul(2654435761)).collect();
        let offsets: Vec<u64> = (0..n)
            .map(|i| {
                if i % 7 == 0 {
                    0x8000_0000 + u64::from(i) * 1000
                } else {
                    u64::from(i) * 30 + 12
                }
            })
            .collect();
        let mut scratch = Vec::new();
        let mut order = Vec::new();
        sort_by_oid(&oids, &mut scratch, &mut order);
        assert!(
            order
                .windows(2)
                .all(|w| oids[w[0] as usize] < oids[w[1] as usize])
        );
        let mut out = Vec::new();
        let mut buf = vec![0u8; 777];
        let sha = [7u8; 20];
        let entry = |i: usize| {
            Some(IdxEntry {
                oid: oids[i],
                crc: crcs[i],
                offset: offsets[i],
            })
        };
        write_idx_v2(&mut out, &mut buf, &order, &entry, &sha).unwrap();
        let n_large = offsets
            .iter()
            .filter(|&&o| o >= LARGE_OFFSET_THRESHOLD)
            .count() as u64;
        assert_eq!(out.len() as u64, idx_len(n as u64, n_large));
        let v = IdxView::parse(&out).unwrap();
        assert_eq!(v.n, n as usize);
        assert_eq!(v.pack_sha, sha);
        assert_eq!(v.large_count() as u64, n_large);
        for (i, o) in oids.iter().enumerate() {
            let pos = v.find(o).unwrap();
            assert_eq!(v.oid(pos).unwrap(), *o);
            assert_eq!(v.crc(pos).unwrap(), crcs[i]);
            assert_eq!(v.offset(pos).unwrap(), offsets[i]);
        }
        assert!(v.find(&oid(n + 1)).is_none());
        // The trailing idx checksum is the SHA-1 of everything before it.
        let digest: [u8; 20] = Sha1::digest(&out[..out.len() - 20]).into();
        assert_eq!(&out[out.len() - 20..], &digest);
        // Parsing garbage fails cleanly.
        assert_eq!(IdxView::parse(b"xx").err(), Some(IdxError::BadMagic));
        assert_eq!(IdxView::parse(&out[..100]).err(), Some(IdxError::Truncated));
    }
}
