//! Zero-allocation HFS pointer parsers and binary `.hfs` sidecar layout:
//!
//! 1. **Variable-length ADO `Git-hfs 1.0` / `~~Git-hfs 1.0` parser ([`parse_git_hfs_v1`])**
//!    (PDF §4.1 + real ADO `SearchGold` format):
//!    ```text
//!    ~~Git-hfs 1.0
//!    blob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100
//!    size:4167
//!    ```
//!    Handles both `~~Git-hfs 1.0` and `Git-hfs 1.0` headers, both `\r\n` and `\n` line
//!    endings, and both 64-hex SHA-256 and 66-hex ADO VsoHash (32-byte digest + 1-byte
//!    block flag).
//!
//! 2. **Canonical 100-byte HFS pointer parser ([`parse_hfs_pointer_100b`])** (DESIGN.md §3):
//!    `"v1 sha256:"` (10B) + 64 lowercase hex (64B) + `" size:"` (6B) + 16 ASCII digits (16B)
//!    + `"   \n"` (4B) = 100 bytes, using branchless autovectorized hex + SWAR decimal math.

use crate::buf::{ByteWriter, Overflow, be32, be64, sub};
use crate::oid::{Oid20, hex_encode_into, hex_nibble_branchless};

/// Size of every canonical 100-byte pointer blob.
pub const HFS_POINTER_LEN: usize = 100;
pub const HFS_POINTER_SIZE: u64 = 100;

/// Fixed 64-byte binary record in a `.hfs` sidecar file.
pub const HFS_SIDECAR_RECORD_LEN: usize = 64;
pub const HFS_SIDECAR_MAGIC: &[u8; 8] = b"GHFS\x00\x00\x00\x01";

/// Parsed binary representation of a `Git-hfs 1.0` / `~~Git-hfs 1.0` pointer blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct HfsPointerMeta {
    pub content_sha256: [u8; 32],
    pub real_size: u64,
    /// Present when the `blob:` field is a 66-hex (33-byte) ADO VsoHash.
    pub vso_flag: Option<u8>,
}

/// Canonical 100-byte HFS pointer (DESIGN.md §3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HfsPointer {
    pub sha256: [u8; 32],
    pub real_size: u64,
}

#[inline]
fn strip_line_prefix<'a>(bytes: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    bytes.strip_prefix(prefix)
}

#[inline]
fn trim_crlf(bytes: &[u8]) -> Option<&[u8]> {
    if let Some(rest) = bytes.strip_prefix(b"\r\n") {
        Some(rest)
    } else {
        bytes.strip_prefix(b"\n")
    }
}

#[inline]
fn decode_hex_pair(pair: &[u8]) -> Option<u8> {
    let [hi_ch, lo_ch] = *pair else {
        return None;
    };
    let (hi, ok_hi) = hex_nibble_branchless(hi_ch);
    let (lo, ok_lo) = hex_nibble_branchless(lo_ch);
    if ok_hi & ok_lo {
        Some(hi.wrapping_shl(4) | lo)
    } else {
        None
    }
}

/// Zero-allocation parser for ADO `~~Git-hfs 1.0` and `Git-hfs 1.0` pointer blobs (PDF §4.1).
///
/// Accepts both:
/// - `~~Git-hfs 1.0\nblob:<64-or-66-hex>\nsize:<int>[\n]`
/// - `Git-hfs 1.0\r\nblob:<64-or-66-hex>\r\nsize:<int>[\r\n]`
#[inline]
pub fn parse_git_hfs_v1(raw_blob: &[u8]) -> Option<HfsPointerMeta> {
    if raw_blob.len() > 256 {
        return None;
    }
    let after_hdr = strip_line_prefix(raw_blob, b"~~Git-hfs 1.0")
        .or_else(|| strip_line_prefix(raw_blob, b"Git-hfs 1.0"))?;
    let line2 = trim_crlf(after_hdr)?;
    let after_blob_key = strip_line_prefix(line2, b"blob:")?;

    // Extract first 64 hex chars (32 bytes) using branchless autovectorized loop.
    let hex_bytes = after_blob_key.get(..64)?;
    let mut nibbles = [0u8; 64];
    let mut all_valid = true;
    for (dst, &c) in nibbles.iter_mut().zip(hex_bytes.iter()) {
        let (v, ok) = hex_nibble_branchless(c);
        *dst = v;
        all_valid &= ok;
    }
    if !all_valid {
        return None;
    }
    let mut content_sha256 = [0u8; 32];
    for (dst, pair) in content_sha256.iter_mut().zip(nibbles.chunks_exact(2)) {
        if let [hi, lo] = *pair {
            *dst = hi.wrapping_shl(4) | lo;
        }
    }

    // Check if there is a 33rd byte (66 hex chars total = ADO VsoHash) before the newline.
    let after_64 = after_blob_key.get(64..)?;
    let (vso_flag, after_hex) = match after_64.first().copied() {
        Some(b'\r' | b'\n') => (None, after_64),
        Some(_) => {
            let tail_pair = after_64.get(..2)?;
            let flag = decode_hex_pair(tail_pair)?;
            (Some(flag), after_64.get(2..)?)
        }
        None => return None,
    };

    let line3 = trim_crlf(after_hex)?;
    let size_digits = strip_line_prefix(line3, b"size:")?;
    if size_digits.is_empty() {
        return None;
    }

    let mut real_size: u64 = 0;
    let mut parsed_any = false;
    for &b in size_digits {
        match b {
            b'0'..=b'9' => {
                parsed_any = true;
                real_size = real_size
                    .checked_mul(10)?
                    .checked_add(u64::from(b.wrapping_sub(b'0')))?;
            }
            b'\r' | b'\n' | b' ' => break,
            _ => return None,
        }
    }

    if parsed_any {
        Some(HfsPointerMeta {
            content_sha256,
            real_size,
            vso_flag,
        })
    } else {
        None
    }
}

/// SWAR + autovectorized parser for canonical 100-byte HFS pointers (DESIGN.md §3).
#[inline]
pub fn parse_hfs_pointer_100b(buf: &[u8]) -> Option<HfsPointer> {
    let (prefix, rest) = buf.split_first_chunk::<10>()?;
    let (rest, trailer) = rest.split_last_chunk::<4>()?;
    let (rest, digits) = rest.split_last_chunk::<16>()?;
    let (hex, size_tag) = rest.split_last_chunk::<6>()?;
    let hex: &[u8; 64] = hex.try_into().ok()?;
    let mut invalid =
        u8::from((prefix != b"v1 sha256:") | (size_tag != b" size:") | (trailer != b"   \n"));

    let mut nibbles = [0u8; 64];
    for (n, &c) in nibbles.iter_mut().zip(hex) {
        let digit = c.wrapping_sub(b'0');
        let letter = c.wrapping_sub(b'a');
        let is_digit = digit < 10;
        invalid |= u8::from(!is_digit & (letter >= 6));
        *n = if is_digit {
            digit
        } else {
            letter.wrapping_add(10)
        };
    }
    let mut sha256 = [0u8; 32];
    for (dst, pair) in sha256.iter_mut().zip(nibbles.chunks_exact(2)) {
        if let [hi, lo] = *pair {
            *dst = hi.wrapping_shl(4) | lo;
        }
    }

    let (high, low) = digits.split_first_chunk::<8>()?;
    let high = u64::from_le_bytes(*high);
    let low = u64::from_le_bytes(low.try_into().ok()?);
    invalid |= u8::from(!(all_ascii_digits(high) & all_ascii_digits(low)));
    if invalid != 0 {
        return None;
    }
    let real_size = swar_parse8(high)
        .wrapping_mul(100_000_000)
        .wrapping_add(swar_parse8(low));
    Some(HfsPointer { sha256, real_size })
}

/// Parses either a variable-length `Git-hfs 1.0` / `~~Git-hfs 1.0` pointer or a
/// canonical 100-byte `v1 sha256:` pointer.
#[inline]
pub fn parse_any_hfs_pointer(raw_blob: &[u8]) -> Option<HfsPointerMeta> {
    if let Some(meta) = parse_git_hfs_v1(raw_blob) {
        return Some(meta);
    }
    parse_hfs_pointer_100b(raw_blob).map(|p| HfsPointerMeta {
        content_sha256: p.sha256,
        real_size: p.real_size,
        vso_flag: None,
    })
}

#[inline]
const fn all_ascii_digits(x: u64) -> bool {
    const HIGH_NIBBLES: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    const THREES: u64 = 0x3030_3030_3030_3030;
    const SIXES: u64 = 0x0606_0606_0606_0606;
    ((x & HIGH_NIBBLES) == THREES) & ((x.wrapping_add(SIXES) & HIGH_NIBBLES) == THREES)
}

#[inline]
const fn swar_parse8(x: u64) -> u64 {
    let x = x & 0x0F0F_0F0F_0F0F_0F0F;
    let x = x.wrapping_mul(10).wrapping_add(x.wrapping_shr(8)) & 0x00FF_00FF_00FF_00FF;
    let x = x.wrapping_mul(100).wrapping_add(x.wrapping_shr(16)) & 0x0000_FFFF_0000_FFFF;
    x.wrapping_mul(10_000).wrapping_add(x.wrapping_shr(32)) & 0xFFFF_FFFF
}

/// Formats a canonical 100-byte HFS pointer.
pub fn format_hfs_pointer(sha256: &[u8; 32], real_size: u64) -> Option<[u8; HFS_POINTER_LEN]> {
    if real_size > 9_999_999_999_999_999 {
        return None;
    }
    let mut out = [0u8; HFS_POINTER_LEN];
    let mut w = ByteWriter::new(&mut out);
    w.put(b"v1 sha256:").ok()?;
    let mut hex = [0u8; 64];
    hex_encode_into(sha256, &mut hex);
    w.put(&hex).ok()?;
    w.put(b" size:").ok()?;
    let mut digits = [b'0'; 16];
    let mut v = real_size;
    for d in digits.iter_mut().rev() {
        *d = b'0'.wrapping_add(u8::try_from(v.wrapping_rem(10)).unwrap_or(0));
        v = v.wrapping_div(10);
    }
    w.put(&digits).ok()?;
    w.put(b"   \n").ok()?;
    Some(out)
}

/// Formats an ADO `~~Git-hfs 1.0` pointer into a caller-provided stack buffer; returns bytes written.
pub fn format_git_hfs_v1(meta: &HfsPointerMeta, out: &mut [u8; 160]) -> Option<usize> {
    let mut w = ByteWriter::new(out);
    w.put(b"~~Git-hfs 1.0\nblob:").ok()?;
    let mut hex = [0u8; 64];
    hex_encode_into(&meta.content_sha256, &mut hex);
    for b in &mut hex {
        *b = b.to_ascii_uppercase();
    }
    w.put(&hex).ok()?;
    if let Some(flag) = meta.vso_flag {
        let mut fhex = [0u8; 2];
        hex_encode_into(&[flag], &mut fhex);
        for b in &mut fhex {
            *b = b.to_ascii_uppercase();
        }
        w.put(&fhex).ok()?;
    }
    w.put(b"\nsize:").ok()?;
    w.put_dec(meta.real_size).ok()?;
    w.put(b"\n").ok()?;
    Some(w.len())
}

/// 64-byte cache-line-aligned binary sidecar record (`slab-XXXX.hfs`, PDF §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HfsBlobRecord {
    pub oid: Oid20,
    pub meta: HfsPointerMeta,
}

impl HfsBlobRecord {
    pub fn encode(&self, out: &mut [u8; HFS_SIDECAR_RECORD_LEN]) -> Result<(), Overflow> {
        let mut w = ByteWriter::new(out);
        w.put(&self.oid.0)?;
        w.put(&self.meta.content_sha256)?;
        w.put_u64_be(self.meta.real_size)?;
        let flags: u32 = match self.meta.vso_flag {
            Some(f) => 0x0100 | u32::from(f),
            None => 0,
        };
        w.put_u32_be(flags)?;
        Ok(())
    }

    pub fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() < HFS_SIDECAR_RECORD_LEN {
            return None;
        }
        let oid = Oid20::from_slice(sub(raw, 0, 20)?)?;
        let sha_slice = sub(raw, 20, 32)?;
        let content_sha256: [u8; 32] = sha_slice.try_into().ok()?;
        let real_size = be64(raw, 52)?;
        let flags = be32(raw, 60)?;
        let vso_flag = if (flags & 0x0100) != 0 {
            u8::try_from(flags & 0xFF).ok()
        } else {
            None
        };
        Some(Self {
            oid,
            meta: HfsPointerMeta {
                content_sha256,
                real_size,
                vso_flag,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_real_searchgold_git_hfs_pointers() {
        // Real README.md from /home/muks/SearchGold
        let searchgold_readme = b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167";
        let meta = parse_git_hfs_v1(searchgold_readme).unwrap();
        assert_eq!(meta.real_size, 4167);
        assert_eq!(meta.vso_flag, Some(0x00));
        assert_eq!(meta.content_sha256[0], 0xA0);
        assert_eq!(meta.content_sha256[31], 0x91);

        // Real spamattack_adid.txt from /home/muks/SearchGold
        let spam = b"~~Git-hfs 1.0\nblob:51E8E7AA7F0AC275BE60519A35125123B6DB946C67112B8A5BFA20908470FDD500\nsize:2007413";
        let meta2 = parse_git_hfs_v1(spam).unwrap();
        assert_eq!(meta2.real_size, 2_007_413);
        assert_eq!(meta2.vso_flag, Some(0x00));

        // PDF §4.1 example with CRLF and no ~~ prefix
        let pdf_example = b"Git-hfs 1.0\r\nblob:C4D2CDC68CD2335992A67B0C488270F4E4220835E4225B11E3E2B2CD1ACFCF9900\r\nsize:141\r\n";
        let meta3 = parse_git_hfs_v1(pdf_example).unwrap();
        assert_eq!(meta3.real_size, 141);
        assert_eq!(meta3.vso_flag, Some(0x00));

        // 64-hex (32-byte SHA-256 without VSO suffix)
        let sha64 = b"Git-hfs 1.0\nblob:c4d2cdc68cd2335992a67b0c488270f4e4220835e4225b11e3e2b2cd1acfcf99\nsize:999\n";
        let meta4 = parse_git_hfs_v1(sha64).unwrap();
        assert_eq!(meta4.real_size, 999);
        assert_eq!(meta4.vso_flag, None);
    }

    #[test]
    fn canonical_100b_and_sidecar_roundtrip() {
        let sha = [0xABu8; 32];
        let p = format_hfs_pointer(&sha, 7269).unwrap();
        let parsed = parse_hfs_pointer_100b(&p).unwrap();
        assert_eq!(
            parsed,
            HfsPointer {
                sha256: sha,
                real_size: 7269
            }
        );

        let rec = HfsBlobRecord {
            oid: Oid20([0x11; 20]),
            meta: HfsPointerMeta {
                content_sha256: sha,
                real_size: 7269,
                vso_flag: Some(0),
            },
        };
        let mut raw = [0u8; HFS_SIDECAR_RECORD_LEN];
        rec.encode(&mut raw).unwrap();
        assert_eq!(HfsBlobRecord::decode(&raw), Some(rec));
    }
}
