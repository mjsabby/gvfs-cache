//! Bounded writers over pre-allocated memory. None of these ever reallocate: when the
//! remaining capacity is insufficient they return [`Overflow`] instead of growing.

use bytes::BytesMut;

use crate::oid::Oid20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overflow;

impl core::fmt::Display for Overflow {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("pre-allocated buffer capacity exceeded")
    }
}

impl std::error::Error for Overflow {}

/// Appends `data` to `v` only if it fits in the existing capacity.
#[inline]
pub fn try_extend(v: &mut Vec<u8>, data: &[u8]) -> Result<(), Overflow> {
    if v.capacity().saturating_sub(v.len()) < data.len() {
        return Err(Overflow);
    }
    v.extend_from_slice(data);
    Ok(())
}

/// Pushes `item` only if it fits in the existing capacity.
#[inline]
pub fn try_push<T>(v: &mut Vec<T>, item: T) -> Result<(), Overflow> {
    if v.len() >= v.capacity() {
        return Err(Overflow);
    }
    v.push(item);
    Ok(())
}

/// Appends `data` to a `BytesMut` only if it fits in the remaining capacity.
#[inline]
pub fn bm_try_extend(b: &mut BytesMut, data: &[u8]) -> Result<(), Overflow> {
    if b.capacity().saturating_sub(b.len()) < data.len() {
        return Err(Overflow);
    }
    b.extend_from_slice(data);
    Ok(())
}

/// Resizes `v` to `len` (zero-filling) only if `len` fits in the existing capacity.
#[inline]
pub fn try_resize(v: &mut Vec<u8>, len: usize) -> Result<(), Overflow> {
    if len > v.capacity() {
        return Err(Overflow);
    }
    v.resize(len, 0);
    Ok(())
}

/// Touches one element per 4 KiB page so the kernel backs the memory at boot. Needed for
/// zero-initialised buffers: `vec![0; n]` is `calloc`, whose pages are mapped lazily.
pub fn prefault<T: Copy>(s: &mut [T]) {
    let stride = 4096usize
        .checked_div(core::mem::size_of::<T>().max(1))
        .unwrap_or(1)
        .max(1);
    for x in s.iter_mut().step_by(stride) {
        *x = core::hint::black_box(*x);
    }
}

/// Pre-faults a `Vec`'s spare capacity by filling it with `fill` and truncating back.
pub fn prefault_capacity<T: Clone>(v: &mut Vec<T>, fill: T) {
    let len = v.len();
    v.resize(v.capacity(), fill);
    v.truncate(len);
}

/// Cursor writer over a fixed slice.
#[derive(Debug)]
pub struct ByteWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> ByteWriter<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pos == 0
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn written(&self) -> &[u8] {
        self.buf.get(..self.pos).unwrap_or(&[])
    }

    #[inline]
    pub fn put(&mut self, data: &[u8]) -> Result<(), Overflow> {
        let end = self.pos.checked_add(data.len()).ok_or(Overflow)?;
        let dst = self.buf.get_mut(self.pos..end).ok_or(Overflow)?;
        dst.copy_from_slice(data);
        self.pos = end;
        Ok(())
    }

    #[inline]
    pub fn put_u8(&mut self, b: u8) -> Result<(), Overflow> {
        self.put(&[b])
    }

    pub fn put_u16_le(&mut self, v: u16) -> Result<(), Overflow> {
        self.put(&v.to_le_bytes())
    }

    pub fn put_u32_be(&mut self, v: u32) -> Result<(), Overflow> {
        self.put(&v.to_be_bytes())
    }

    pub fn put_u64_be(&mut self, v: u64) -> Result<(), Overflow> {
        self.put(&v.to_be_bytes())
    }

    pub fn put_i64_le(&mut self, v: i64) -> Result<(), Overflow> {
        self.put(&v.to_le_bytes())
    }

    /// Writes `v` in decimal ASCII.
    pub fn put_dec(&mut self, v: u64) -> Result<(), Overflow> {
        let mut tmp = [0u8; 20];
        let n = fmt_dec(v, &mut tmp);
        self.put(tmp.get(..n).unwrap_or(&[]))
    }

    pub fn put_hex_oid(&mut self, oid: &Oid20) -> Result<(), Overflow> {
        self.put(&oid.to_hex())
    }

    /// Overwrites bytes previously written at `at`.
    pub fn patch(&mut self, at: usize, data: &[u8]) -> Result<(), Overflow> {
        let end = at.checked_add(data.len()).ok_or(Overflow)?;
        if end > self.pos {
            return Err(Overflow);
        }
        self.buf
            .get_mut(at..end)
            .ok_or(Overflow)?
            .copy_from_slice(data);
        Ok(())
    }
}

/// `"00"..="99"`: decimal formatting emits two digits per division.
static DEC_PAIRS: [[u8; 2]; 100] = build_dec_pairs();

#[allow(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
const fn build_dec_pairs() -> [[u8; 2]; 100] {
    let mut t = [[0u8; 2]; 100];
    let mut i = 0;
    while i < 100 {
        t[i] = [b'0' + (i / 10) as u8, b'0' + (i % 10) as u8];
        i += 1;
    }
    t
}

/// Formats `v` as decimal into `out`, returning the number of bytes written.
pub fn fmt_dec(mut v: u64, out: &mut [u8; 20]) -> usize {
    let mut rev = [0u8; 20];
    let mut pos = rev.len();
    while v >= 100 {
        let pair = DEC_PAIRS
            .get(usize::try_from(v.wrapping_rem(100)).unwrap_or(0))
            .copied()
            .unwrap_or([b'0'; 2]);
        v = v.wrapping_div(100);
        pos = pos.saturating_sub(2);
        if let Some(d) = rev.get_mut(pos..pos.saturating_add(2)) {
            d.copy_from_slice(&pair);
        }
    }
    let last = DEC_PAIRS
        .get(usize::try_from(v).unwrap_or(0))
        .copied()
        .unwrap_or([b'0'; 2]);
    if v >= 10 {
        pos = pos.saturating_sub(2);
        if let Some(d) = rev.get_mut(pos..pos.saturating_add(2)) {
            d.copy_from_slice(&last);
        }
    } else {
        pos = pos.saturating_sub(1);
        if let (Some(d), [_, digit]) = (rev.get_mut(pos), last) {
            *d = digit;
        }
    }
    let digits = rev.get(pos..).unwrap_or(&[]);
    for (dst, src) in out.iter_mut().zip(digits) {
        *dst = *src;
    }
    digits.len()
}

/// Parses an unsigned decimal ASCII integer (no sign, no whitespace).
pub fn parse_dec_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 20 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(d))?;
    }
    Some(v)
}

/// Big-endian u32 at `at`.
#[inline]
pub fn be32(data: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    let b: [u8; 4] = data.get(at..end)?.try_into().ok()?;
    Some(u32::from_be_bytes(b))
}

/// Big-endian u64 at `at`.
#[inline]
pub fn be64(data: &[u8], at: usize) -> Option<u64> {
    let end = at.checked_add(8)?;
    let b: [u8; 8] = data.get(at..end)?.try_into().ok()?;
    Some(u64::from_be_bytes(b))
}

/// Little-endian i64 at `at`.
#[inline]
pub fn le_i64(data: &[u8], at: usize) -> Option<i64> {
    let end = at.checked_add(8)?;
    let b: [u8; 8] = data.get(at..end)?.try_into().ok()?;
    Some(i64::from_le_bytes(b))
}

/// `data[at..at + len]`.
#[inline]
pub fn sub(data: &[u8], at: usize, len: usize) -> Option<&[u8]> {
    data.get(at..at.checked_add(len)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_roundtrip() {
        for v in [
            0u64,
            1,
            9,
            10,
            99,
            100,
            101,
            999,
            1000,
            12345,
            99_999,
            100_000,
            u64::MAX / 10,
            u64::MAX,
        ] {
            let mut b = [0u8; 20];
            let n = fmt_dec(v, &mut b);
            assert_eq!(parse_dec_u64(&b[..n]), Some(v));
            assert_eq!(std::str::from_utf8(&b[..n]).unwrap(), v.to_string());
        }
        for v in 0..100_000u64 {
            let mut b = [0u8; 20];
            let n = fmt_dec(v, &mut b);
            assert_eq!(std::str::from_utf8(&b[..n]).unwrap(), v.to_string());
        }
        assert_eq!(parse_dec_u64(b"18446744073709551616"), None);
        assert_eq!(parse_dec_u64(b"-1"), None);
    }

    #[test]
    fn bounded_writers_never_grow() {
        let mut v = Vec::with_capacity(4);
        try_extend(&mut v, b"abcd").unwrap();
        assert_eq!(try_extend(&mut v, b"e"), Err(Overflow));
        assert_eq!(v.capacity(), 4);
        let mut buf = [0u8; 3];
        let mut w = ByteWriter::new(&mut buf);
        w.put(b"ab").unwrap();
        assert!(w.put(b"cd").is_err());
        assert_eq!(w.written(), b"ab");
    }
}
