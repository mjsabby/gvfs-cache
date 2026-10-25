//! GitHFS pointer files: what every repository file is in an HFS monorepo.
//!
//! ```text
//! ~~Git-hfs 1.0\n
//! blob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\n
//! size:4167
//! ```
//!
//! The blob id is a VsoHash: 32 hash bytes plus one algorithm byte (66 hex digits); 64-digit
//! ids are accepted too. Lines end in `\n` or `\r\n`; the final newline is optional. The
//! parser never allocates: it is slice patterns plus a branch-free hex decode that LLVM
//! vectorizes.

use crate::util::decode_hex;

/// The parsed content address and real size of a pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HfsPointer {
    pub blob_id: [u8; 33],
    pub blob_id_len: u8,
    pub size: u64,
}

impl HfsPointer {
    pub fn blob_id(&self) -> &[u8] {
        self.blob_id
            .get(..usize::from(self.blob_id_len))
            .unwrap_or(&[])
    }
}

/// Pointer files are tiny; anything larger is a real file.
pub const MAX_POINTER_LEN: usize = 256;

#[inline]
fn eol(b: &[u8]) -> Option<&[u8]> {
    match b {
        [b'\r', b'\n', rest @ ..] | [b'\n', rest @ ..] => Some(rest),
        _ => None,
    }
}

/// Parses a `~~Git-hfs 1.0` pointer; `None` for anything else.
#[inline]
pub fn parse_git_hfs(b: &[u8]) -> Option<HfsPointer> {
    if b.len() > MAX_POINTER_LEN {
        return None;
    }
    let rest = eol(b.strip_prefix(b"~~Git-hfs 1.0")?)?;
    let rest = rest.strip_prefix(b"blob:")?;
    let hex_len = rest.iter().position(|&c| c == b'\n' || c == b'\r')?;
    if hex_len != 64 && hex_len != 66 {
        return None;
    }
    let mut blob_id = [0u8; 33];
    let id_len = hex_len.wrapping_shr(1);
    if !decode_hex(rest.get(..hex_len)?, blob_id.get_mut(..id_len)?) {
        return None;
    }
    let rest = eol(rest.get(hex_len..)?)?;
    let digits = rest.strip_prefix(b"size:")?;
    let digits = match digits {
        [d @ .., b'\r', b'\n'] | [d @ .., b'\n'] => d,
        d => d,
    };
    if digits.is_empty() || digits.len() > 20 {
        return None;
    }
    let mut size = 0u64;
    for &c in digits {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        size = size.checked_mul(10)?.checked_add(u64::from(d))?;
    }
    Some(HfsPointer {
        blob_id,
        blob_id_len: u8::try_from(id_len).ok()?,
        size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100";

    #[test]
    fn parses_real_pointer_variants() {
        for (text, size) in [
            (format!("~~Git-hfs 1.0\nblob:{ID}\nsize:4167"), 4167),
            (format!("~~Git-hfs 1.0\nblob:{ID}\nsize:4167\n"), 4167),
            (
                format!("~~Git-hfs 1.0\r\nblob:{ID}\r\nsize:18944097\r\n"),
                18944097,
            ),
            (format!("~~Git-hfs 1.0\nblob:{}\nsize:0", &ID[..64]), 0),
        ] {
            let p = parse_git_hfs(text.as_bytes()).unwrap_or_else(|| panic!("{text:?}"));
            assert_eq!(p.size, size);
            assert_eq!(p.blob_id()[0], 0xA0);
        }
        assert_eq!(
            parse_git_hfs(format!("~~Git-hfs 1.0\nblob:{ID}\nsize:4167").as_bytes())
                .unwrap()
                .blob_id()
                .len(),
            33
        );
    }

    #[test]
    fn rejects_non_pointers() {
        for text in [
            String::from("hello"),
            format!("Git-hfs 1.0\nblob:{ID}\nsize:1"),
            format!("~~Git-hfs 1.0\nblob:{}\nsize:1", &ID[..65]),
            format!("~~Git-hfs 1.0\nblob:{}Z\nsize:1", &ID[..65]),
            format!("~~Git-hfs 1.0\nblob:{ID}\nsize:"),
            format!("~~Git-hfs 1.0\nblob:{ID}\nsize:12x"),
            format!("~~Git-hfs 1.0\nblob:{ID}\nsize:99999999999999999999"),
            format!("~~Git-hfs 1.0\nblob:{ID}\nsize:1\n\n"),
        ] {
            assert!(parse_git_hfs(text.as_bytes()).is_none(), "{text:?}");
        }
    }
}
