//! 20-byte SHA-1 object ids and branch-free hex conversion.

use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Oid20(pub [u8; 20]);

/// Two lower-case hex digits for every byte value: encoding is one table load per byte.
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

/// Nibble value of an ASCII hex digit (either case), `0xFF` for anything else.
pub(crate) const HEX_LUT_ANY: [u8; 256] = build_hex_lut(true);

// Evaluated at compile time: an out-of-range index or overflow is a build error, never a
// runtime panic.
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

#[cfg(test)]
#[inline]
fn nibble(lut: &[u8; 256], c: u8) -> u8 {
    lut.get(usize::from(c)).copied().unwrap_or(0xFF)
}

/// Table-driven decoder kept as the reference for [`Oid20::from_hex`]'s tests: decodes
/// `hex` (2 chars per byte) into `out`; returns false on any invalid digit.
#[cfg(test)]
fn decode_hex_into(lut: &[u8; 256], hex: &[u8], out: &mut [u8]) -> bool {
    if hex.len() != out.len().saturating_mul(2) {
        return false;
    }
    let mut invalid: u8 = 0;
    for (dst, pair) in out.iter_mut().zip(hex.chunks_exact(2)) {
        if let [h, l] = *pair {
            let hi = nibble(lut, h);
            let lo = nibble(lut, l);
            invalid |= hi | lo;
            *dst = hi.wrapping_shl(4) | (lo & 0x0F);
        }
    }
    invalid & 0xF0 == 0
}

/// Branch-free nibble of an ASCII hex digit (either case) and whether it is one; written
/// as plain byte arithmetic so LLVM vectorizes loops over it.
#[inline(always)]
fn hex_value(c: u8) -> (u8, bool) {
    let d = c.wrapping_sub(b'0');
    let l = (c | 0x20).wrapping_sub(b'a');
    let is_digit = d < 10;
    let is_letter = l < 6;
    (
        if is_digit { d } else { l.wrapping_add(10) },
        is_digit | is_letter,
    )
}

/// Lower-case hex of 4 bytes with 64-bit SWAR arithmetic: the nibbles are spread into the
/// 8 byte lanes of a `u64`, then `'0' + n + 39 * (n > 9)` is applied to all lanes at once.
#[inline(always)]
fn hex4(b: [u8; 4]) -> [u8; 8] {
    let x = u64::from(u32::from_be_bytes(b));
    // b0 b1 b2 b3 -> one byte per 16-bit lane, b0 in the most significant lane.
    let y = ((x & 0xFFFF_0000).wrapping_shl(16)) | (x & 0x0000_FFFF);
    let y = ((y & 0x0000_FF00_0000_FF00).wrapping_shl(8)) | (y & 0x0000_00FF_0000_00FF);
    // High nibble into the lane's upper byte, low nibble into its lower byte.
    let n = ((y & 0x00F0_00F0_00F0_00F0).wrapping_shl(4)) | (y & 0x000F_000F_000F_000F);
    // Lanes holding 10..=15 get 0x27 more (`'a' - '0' - 10`); no lane can carry.
    let letters = (n.wrapping_add(0x0606_0606_0606_0606).wrapping_shr(4)) & 0x0101_0101_0101_0101;
    n.wrapping_add(0x3030_3030_3030_3030)
        .wrapping_add(letters.wrapping_mul(0x27))
        .to_be_bytes()
}

/// Decodes exactly 40 hex digits (either case).
#[inline]
fn decode_hex40(hex: &[u8; 40]) -> Option<[u8; 20]> {
    let mut nib = [0u8; 40];
    let mut ok = true;
    for (n, &c) in nib.iter_mut().zip(hex.iter()) {
        let (v, valid) = hex_value(c);
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

    /// Parses exactly 40 hex digits (either case).
    #[inline]
    pub fn from_hex(hex: &[u8]) -> Option<Self> {
        let h: &[u8; 40] = hex.try_into().ok()?;
        decode_hex40(h).map(Self)
    }

    /// Lower-case hex rendering.
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

    /// First 8 bytes as a big-endian integer: uniformly distributed, used as the index
    /// table's home-slot selector without any further hashing.
    #[inline]
    pub fn prefix_u64(&self) -> u64 {
        let [a, b, c, d, e, f, g, h, ..] = self.0;
        u64::from_be_bytes([a, b, c, d, e, f, g, h])
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

/// Hex-encodes arbitrary bytes into `out` (which must be exactly twice as long).
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
    fn hex_roundtrip_and_validation() {
        let hex = b"0123456789abcdef0123456789abcdef01234567";
        let oid = Oid20::from_hex(hex).unwrap();
        assert_eq!(&oid.to_hex(), hex);
        assert_eq!(oid.to_string(), std::str::from_utf8(hex).unwrap());
        assert_eq!(
            Oid20::from_hex(b"0123456789ABCDEF0123456789abcdef01234567"),
            Some(oid)
        );
        assert!(Oid20::from_hex(b"0123456789abcdef0123456789abcdef0123456g").is_none());
        assert!(Oid20::from_hex(b"0123").is_none());
        assert_eq!(oid.prefix_u64(), 0x0123_4567_89ab_cdef);
    }

    #[test]
    fn branch_free_decoder_matches_the_table_for_every_byte() {
        for c in 0..=255u8 {
            let (v, ok) = hex_value(c);
            let lut = HEX_LUT_ANY[c as usize];
            assert_eq!(ok, lut != 0xFF, "byte {c:#x}");
            if ok {
                assert_eq!(v, lut, "byte {c:#x}");
            }
            // Every position of a 40-digit id rejects/accepts it the same way.
            for pos in [0usize, 1, 19, 38, 39] {
                let mut hex = *b"00112233445566778899aabbccddeeff00112233";
                hex[pos] = c;
                let mut want = [0u8; 20];
                let ok_lut = decode_hex_into(&HEX_LUT_ANY, &hex, &mut want);
                assert_eq!(
                    Oid20::from_hex(&hex).map(|o| o.0),
                    ok_lut.then_some(want),
                    "byte {c:#x} at {pos}"
                );
            }
        }
        for b in 0..=255u8 {
            let mut out = [0u8; 2];
            hex_encode_into(&[b], &mut out);
            assert_eq!(out, *format!("{b:02x}").as_bytes());
        }
        // The SWAR encoder agrees with the table on every byte in every lane.
        for b in 0..=255u8 {
            for lane in 0..4 {
                let mut four = [0x5Au8; 4];
                four[lane] = b;
                let mut want = [0u8; 8];
                hex_encode_into(&four, &mut want);
                assert_eq!(hex4(four), want, "byte {b:#x} lane {lane}");
            }
        }
        let oid = Oid20(core::array::from_fn(|i| {
            (i as u8).wrapping_mul(37).wrapping_add(250)
        }));
        let mut want = [0u8; 40];
        hex_encode_into(&oid.0, &mut want);
        assert_eq!(oid.to_hex(), want);
    }
}
