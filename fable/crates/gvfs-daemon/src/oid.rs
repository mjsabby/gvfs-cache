//! 20-byte SHA-1 object ids and branchless hex conversion.

use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Oid(pub [u8; 20]);

/// Hex digit value; 0xFF marks a non-hex byte. Folding the values with `|` lets a whole
/// 40-digit decode run without a data-dependent branch (LLVM turns this into a table).
#[inline]
const fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c.wrapping_sub(b'0'),
        b'a'..=b'f' => c.wrapping_sub(b'a').wrapping_add(10),
        b'A'..=b'F' => c.wrapping_sub(b'A').wrapping_add(10),
        _ => 0xFF,
    }
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Decodes `src.len() / 2` bytes of hex into `dst`. Returns `None` on any non-hex byte or
/// if `src.len() != 2 * dst.len()`.
#[inline]
pub fn decode_hex(src: &[u8], dst: &mut [u8]) -> Option<()> {
    if src.len() != dst.len().checked_mul(2)? {
        return None;
    }
    let mut bad: u8 = 0;
    for (d, pair) in dst.iter_mut().zip(src.chunks_exact(2)) {
        let (hi, lo) = match pair {
            [h, l] => (hex_val(*h), hex_val(*l)),
            _ => (0xFF, 0xFF),
        };
        bad |= hi | lo;
        *d = (hi << 4) | (lo & 0x0F);
    }
    if bad & 0xF0 != 0 { None } else { Some(()) }
}

/// Encodes `src` as lowercase hex into `dst` (which must be exactly twice as long).
#[inline]
pub fn encode_hex(src: &[u8], dst: &mut [u8]) {
    for (pair, b) in dst.chunks_exact_mut(2).zip(src) {
        if let [h, l] = pair {
            *h = HEX_DIGITS.get(usize::from(b >> 4)).copied().unwrap_or(b'0');
            *l = HEX_DIGITS
                .get(usize::from(b & 0x0F))
                .copied()
                .unwrap_or(b'0');
        }
    }
}

impl Oid {
    pub const ZERO: Oid = Oid([0u8; 20]);

    #[inline]
    pub fn from_hex(hex: &[u8]) -> Option<Oid> {
        let mut out = [0u8; 20];
        decode_hex(hex, &mut out)?;
        Some(Oid(out))
    }

    #[inline]
    pub fn from_slice(b: &[u8]) -> Option<Oid> {
        Some(Oid(<[u8; 20]>::try_from(b).ok()?))
    }

    #[inline]
    pub fn to_hex(self) -> [u8; 40] {
        let mut out = [0u8; 40];
        encode_hex(&self.0, &mut out);
        out
    }

    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == [0u8; 20]
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Little-endian word of bytes `[at, at + 8)` (callers pass 0, 4, 8 or 12).
    #[inline]
    pub fn word(self, at: usize) -> u64 {
        let [a, b, c, d, e, f, g, h] = match self.0.get(at..at.saturating_add(8)) {
            Some(&[a, b, c, d, e, f, g, h]) => [a, b, c, d, e, f, g, h],
            _ => [0u8; 8],
        };
        u64::from_le_bytes([a, b, c, d, e, f, g, h])
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = self.to_hex();
        f.write_str(core::str::from_utf8(&hex).unwrap_or("<oid>"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseOidError;

impl fmt::Display for ParseOidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("expected 40 hex digits")
    }
}

impl std::error::Error for ParseOidError {}

impl core::str::FromStr for Oid {
    type Err = ParseOidError;

    fn from_str(s: &str) -> Result<Self, ParseOidError> {
        Oid::from_hex(s.as_bytes()).ok_or(ParseOidError)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn hex_roundtrip_and_rejects_garbage() {
        let o = Oid::from_hex(b"0123456789abcdefABCDEF0123456789abcdef01").unwrap();
        assert_eq!(&o.to_hex()[..], b"0123456789abcdefabcdef0123456789abcdef01");
        assert_eq!(o.to_string(), "0123456789abcdefabcdef0123456789abcdef01");
        assert!(Oid::from_hex(b"0123456789abcdefABCDEF0123456789abcdef0g").is_none());
        assert!(Oid::from_hex(b"0123").is_none());
        assert!(Oid::ZERO.is_zero());
        assert_eq!(
            o.word(0),
            u64::from_le_bytes([0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef])
        );
        assert_eq!(
            o.word(12),
            u64::from_le_bytes([0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01])
        );
        assert_eq!(o.word(16), 0, "out-of-range words read as zero");
    }
}
