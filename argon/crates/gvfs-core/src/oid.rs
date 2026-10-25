//! 20-byte SHA-1 object IDs (`Oid20`) and branch-free SWAR hex conversion.

use core::fmt;

/// Zero OID (`[0u8; 20]`), used to signal a Day-0 genesis full walk in `ingest_git_delta`.
pub const ZERO_OID: Oid20 = Oid20([0u8; 20]);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Oid20(pub [u8; 20]);

static HEX_PAIRS: [[u8; 2]; 256] = build_hex_pairs();

#[allow(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
const fn build_hex_pairs() -> [[u8; 2]; 256] {
    let digits = b"0123456789abcdef";
    let mut t = [[0u8; 2]; 256];
    let mut i = 0;
    while i < 256 {
        t[i] = [digits[i >> 4], digits[i & 15]];
        i += 1;
    }
    t
}

pub(crate) const HEX_LUT_ANY: [u8; 256] = build_hex_lut(true);

#[allow(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
pub(crate) const fn build_hex_lut(upper: bool) -> [u8; 256] {
    let mut lut = [0xFFu8; 256];
    let mut i = b'0';
    while i <= b'9' {
        lut[i as usize] = i - b'0';
        i += 1;
    }
    let mut j = b'a';
    while j <= b'f' {
        lut[j as usize] = (j - b'a') + 10;
        if upper {
            lut[(j - 32) as usize] = (j - b'a') + 10;
        }
        j += 1;
    }
    lut
}

/// Branch-free nibble of an ASCII hex digit (either case) and whether it is valid.
#[inline(always)]
pub fn hex_nibble_branchless(c: u8) -> (u8, bool) {
    let d = c.wrapping_sub(b'0');
    let l = (c | 0x20).wrapping_sub(b'a');
    let is_digit = d < 10;
    let is_letter = l < 6;
    (
        if is_digit { d } else { l.wrapping_add(10) },
        is_digit | is_letter,
    )
}

/// Lower-case hex of 4 bytes using 64-bit SWAR arithmetic.
#[inline(always)]
fn hex4(b: [u8; 4]) -> [u8; 8] {
    let x = u64::from(u32::from_be_bytes(b));
    let y = ((x & 0xFFFF_0000).wrapping_shl(16)) | (x & 0x0000_FFFF);
    let y = ((y & 0x0000_FF00_0000_FF00).wrapping_shl(8)) | (y & 0x0000_00FF_0000_00FF);
    let n = ((y & 0x00F0_00F0_00F0_00F0).wrapping_shl(4)) | (y & 0x000F_000F_000F_000F);
    let letters = (n.wrapping_add(0x0606_0606_0606_0606).wrapping_shr(4)) & 0x0101_0101_0101_0101;
    n.wrapping_add(0x3030_3030_3030_3030)
        .wrapping_add(letters.wrapping_mul(0x27))
        .to_be_bytes()
}

#[inline]
fn decode_hex40(hex: &[u8; 40]) -> Option<[u8; 20]> {
    let mut nib = [0u8; 40];
    let mut ok = true;
    for (n, &c) in nib.iter_mut().zip(hex.iter()) {
        let (v, valid) = hex_nibble_branchless(c);
        *n = v;
        ok &= valid;
    }
    if !ok {
        return None;
    }
    let mut out = [0u8; 20];
    for (o, pair) in out.iter_mut().zip(nib.chunks_exact(2)) {
        if let [h, l] = *pair {
            *o = h.wrapping_shl(4) | l;
        }
    }
    Some(out)
}

impl Oid20 {
    pub const ZERO: Self = Self([0; 20]);

    #[inline]
    pub fn from_slice(b: &[u8]) -> Option<Self> {
        let arr: [u8; 20] = b.try_into().ok()?;
        Some(Self(arr))
    }

    #[inline]
    pub fn from_hex(hex: &[u8]) -> Option<Self> {
        let h: &[u8; 40] = hex.try_into().ok()?;
        decode_hex40(h).map(Self)
    }

    #[inline]
    pub fn to_hex(&self) -> [u8; 40] {
        let mut out = [0u8; 40];
        for (dst, src) in out.chunks_exact_mut(8).zip(self.0.chunks_exact(4)) {
            if let Ok(b) = <[u8; 4]>::try_from(src) {
                dst.copy_from_slice(&hex4(b));
            }
        }
        out
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// First 8 bytes as a big-endian u64 (already uniformly distributed SHA-1 bits).
    #[inline]
    pub fn prefix_u64(&self) -> u64 {
        let [a, b, c, d, e, f, g, h, ..] = self.0;
        u64::from_be_bytes([a, b, c, d, e, f, g, h])
    }

    /// Last 2 bytes as a 16-bit fingerprint (`u16::from_be_bytes([oid[18], oid[19]])`),
    /// used in `PackedObjLoc` (PDF §4.2).
    #[inline]
    pub fn fp16(&self) -> u16 {
        let [.., hi, lo] = self.0;
        u16::from_be_bytes([hi, lo])
    }

    #[inline]
    pub fn first_byte(&self) -> u8 {
        let [a, ..] = self.0;
        a
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        self.0 == [0; 20]
    }
}

impl fmt::Display for Oid20 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = self.to_hex();
        f.write_str(core::str::from_utf8(&hex).unwrap_or("<invalid>"))
    }
}

impl fmt::Debug for Oid20 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[inline]
pub fn hex_encode_into(bytes: &[u8], out: &mut [u8]) -> bool {
    if out.len() != bytes.len().saturating_mul(2) {
        return false;
    }
    for (pair, byte) in out.chunks_exact_mut(2).zip(bytes.iter()) {
        if let Some(digits) = HEX_PAIRS.get(usize::from(*byte)) {
            pair.copy_from_slice(digits);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let hex = b"0123456789abcdef0123456789abcdef01234567";
        let oid = Oid20::from_hex(hex).unwrap();
        assert_eq!(&oid.to_hex(), hex);
        assert_eq!(oid.fp16(), 0x4567);
        assert_eq!(
            Oid20::from_hex(b"0123456789ABCDEF0123456789abcdef01234567"),
            Some(oid)
        );
        assert!(Oid20::from_hex(b"0123456789abcdef0123456789abcdef0123456g").is_none());
    }
}
