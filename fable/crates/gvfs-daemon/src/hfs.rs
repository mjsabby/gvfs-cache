//! Zero-allocation parser for Azure DevOps `Git-hfs 1.0` pointer blobs.
//!
//! Every file in a GitHFS repository is one of these (the real bytes live in the ADO
//! Artifacts BlobStore, keyed by the VSO blob id):
//!
//! ```text
//! ~~Git-hfs 1.0\n
//! blob:A04C2C77...180AC9100\n       (66 hex digits: 32-byte hash + 1 algorithm byte)
//! size:4167                          (no trailing newline, but one is tolerated)
//! ```
//!
//! The `~~` prefix and the `\r\n` line endings are optional, and 64-digit (32-byte) blob
//! ids are accepted too, matching the variants seen in the wild.

use crate::oid::decode_hex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HfsPointer {
    /// The decoded blob id (`blob_id_len` bytes used: 32 or 33).
    pub blob_id: [u8; 33],
    pub blob_id_len: u8,
    /// Real (hydrated) size of the file.
    pub size: u64,
}

/// Longest pointer we consider: anything bigger is a real file, not a pointer.
pub const MAX_POINTER_LEN: usize = 160;

#[inline]
fn eol(b: &[u8]) -> Option<&[u8]> {
    if let Some(r) = b.strip_prefix(b"\r\n") {
        Some(r)
    } else {
        b.strip_prefix(b"\n")
    }
}

#[inline]
pub fn parse_hfs_pointer(raw: &[u8]) -> Option<HfsPointer> {
    if raw.len() > MAX_POINTER_LEN {
        return None;
    }
    let rest = raw.strip_prefix(b"~~").unwrap_or(raw);
    let rest = rest.strip_prefix(b"Git-hfs 1.0")?;
    let rest = eol(rest)?;
    let rest = rest.strip_prefix(b"blob:")?;
    let hex_len = rest.iter().position(|&c| c == b'\r' || c == b'\n')?;
    let (hex, rest) = rest.split_at_checked(hex_len)?;
    let mut blob_id = [0u8; 33];
    let blob_id_len = match hex.len() {
        64 => {
            decode_hex(hex, blob_id.get_mut(..32)?)?;
            32u8
        }
        66 => {
            decode_hex(hex, &mut blob_id)?;
            33u8
        }
        _ => return None,
    };
    let rest = eol(rest)?;
    let digits = rest.strip_prefix(b"size:")?;
    let mut size: u64 = 0;
    let mut n_digits = 0usize;
    let mut tail: &[u8] = &[];
    for (i, &c) in digits.iter().enumerate() {
        match c {
            b'0'..=b'9' => {
                size = size
                    .checked_mul(10)?
                    .checked_add(u64::from(c.wrapping_sub(b'0')))?;
                n_digits = n_digits.saturating_add(1);
            }
            b'\r' | b'\n' => {
                tail = digits.get(i..)?;
                break;
            }
            _ => return None,
        }
    }
    if n_digits == 0 || n_digits > 20 {
        return None;
    }
    // Only line endings may follow the size.
    if !tail.iter().all(|&c| c == b'\r' || c == b'\n') {
        return None;
    }
    Some(HfsPointer {
        blob_id,
        blob_id_len,
        size,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn parses_real_searchgold_pointer() {
        let raw = b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167";
        let p = parse_hfs_pointer(raw).unwrap();
        assert_eq!(p.size, 4167);
        assert_eq!(p.blob_id_len, 33);
        assert_eq!(p.blob_id[0], 0xA0);
        assert_eq!(p.blob_id[32], 0x00);
        assert_eq!(parse_hfs_pointer(b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167\n"), Some(p));
    }

    #[test]
    fn parses_crlf_and_64_hex_variant() {
        let raw = b"Git-hfs 1.0\r\nblob:c4d2cdc68cd2335992a67b0c488270f4e4220835e4225b11e3e2b2cd1acfcf99\r\nsize:141\r\n";
        let p = parse_hfs_pointer(raw).unwrap();
        assert_eq!((p.size, p.blob_id_len), (141, 32));
        assert_eq!(p.blob_id[0], 0xc4);
    }

    #[test]
    fn rejects_non_pointers() {
        assert!(parse_hfs_pointer(b"hello world\n").is_none());
        assert!(parse_hfs_pointer(b"~~Git-hfs 1.0\nblob:ZZ\nsize:1").is_none());
        assert!(parse_hfs_pointer(b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:").is_none());
        assert!(parse_hfs_pointer(b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:12x").is_none());
        assert!(parse_hfs_pointer(&[b'a'; 200]).is_none());
    }
}
