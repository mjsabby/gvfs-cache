use crate::buf::parse_dec_u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobId {
    Vso([u8; 33]),
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pointer {
    pub blob: BlobId,
    pub size: u64,
}

/// Keep the original bytes in Git: VSO IDs are 33 bytes, not SHA-256 digests.
pub fn parse_git_hfs_v1(bytes: &[u8]) -> Option<Pointer> {
    if bytes.len() > 256 {
        return None;
    }
    let mut lines = bytes.split(|&b| b == b'\n');
    if clean(lines.next()?) != b"~~Git-hfs 1.0" {
        return None;
    }
    let id = clean(lines.next()?).strip_prefix(b"blob:")?;
    let blob = if id == b"InvalidBlobId" {
        BlobId::Invalid
    } else {
        let hex: &[u8; 66] = id.try_into().ok()?;
        let mut nibbles = [0; 66];
        let mut invalid = 0u8;
        for (dst, &b) in nibbles.iter_mut().zip(hex) {
            let digit = b.wrapping_sub(b'0');
            let letter = (b | 32).wrapping_sub(b'a');
            let decimal = digit < 10;
            invalid |= u8::from(!decimal && letter >= 6);
            *dst = if decimal {
                digit
            } else {
                letter.wrapping_add(10)
            };
        }

        if invalid != 0 {
            return None;
        }
        let mut value = [0; 33];
        for (dst, pair) in value.iter_mut().zip(nibbles.chunks_exact(2)) {
            if let [a, b] = pair {
                *dst = a.wrapping_shl(4) | b;
            }
        }
        BlobId::Vso(value)
    };
    let size = parse_dec_u64(clean(lines.next()?).strip_prefix(b"size:")?)?;
    match (lines.next(), lines.next()) {
        (None, None) | (Some(b""), None) => Some(Pointer { blob, size }),
        _ => None,
    }
}

fn clean(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_format_and_strict_trailing_bytes() {
        let p = b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167\n";
        assert_eq!(parse_git_hfs_v1(p).unwrap().size, 4167);
        assert_eq!(
            parse_git_hfs_v1(b"~~Git-hfs 1.0\r\nblob:InvalidBlobId\r\nsize:0\r\n"),
            Some(Pointer {
                blob: BlobId::Invalid,
                size: 0
            })
        );
        assert!(parse_git_hfs_v1(b"~~Git-hfs 1.0\nblob:InvalidBlobId\nsize:1\njunk").is_none());
        assert!(
            parse_git_hfs_v1(b"~~Git-hfs 1.0\nblob:InvalidBlobId\nsize:18446744073709551616\n")
                .is_none()
        );
    }
}
