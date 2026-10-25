//! Git packfile (v2) framing: the 12-byte header, per-entry headers and the trailer.

use core::fmt;

use crate::oid::Oid;

pub const PACK_HEADER_LEN: usize = 12;
pub const PACK_TRAILER_LEN: usize = 20;
/// Longest entry header: 10 bytes of type/size varint + a 20-byte REF_DELTA base.
pub const MAX_ENTRY_HEADER_LEN: usize = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ObjType {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
    OfsDelta = 6,
    RefDelta = 7,
}

impl ObjType {
    #[inline]
    pub fn from_code(c: u8) -> Option<Self> {
        match c {
            1 => Some(Self::Commit),
            2 => Some(Self::Tree),
            3 => Some(Self::Blob),
            4 => Some(Self::Tag),
            6 => Some(Self::OfsDelta),
            7 => Some(Self::RefDelta),
            _ => None,
        }
    }

    #[inline]
    pub fn code(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn is_delta(self) -> bool {
        matches!(self, Self::OfsDelta | Self::RefDelta)
    }

    /// Loose-object type name (empty for delta kinds).
    pub fn name(self) -> &'static [u8] {
        match self {
            Self::Commit => b"commit",
            Self::Tree => b"tree",
            Self::Blob => b"blob",
            Self::Tag => b"tag",
            Self::OfsDelta | Self::RefDelta => b"",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackError {
    BadSignature,
    UnsupportedVersion(u32),
    BadObjectType(u8),
    SizeOverflow,
    BadDeltaOffset,
    Truncated,
}

impl fmt::Display for PackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadSignature => f.write_str("missing PACK signature"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported pack version {v}"),
            Self::BadObjectType(t) => write!(f, "invalid pack object type {t}"),
            Self::SizeOverflow => f.write_str("pack entry size overflows 64 bits"),
            Self::BadDeltaOffset => f.write_str("invalid OFS_DELTA offset"),
            Self::Truncated => f.write_str("truncated pack"),
        }
    }
}

impl std::error::Error for PackError {}

pub fn pack_header(count: u32) -> [u8; PACK_HEADER_LEN] {
    let [c0, c1, c2, c3] = count.to_be_bytes();
    [b'P', b'A', b'C', b'K', 0, 0, 0, 2, c0, c1, c2, c3]
}

/// Validates the 12-byte header and returns the object count.
pub fn parse_pack_header(b: &[u8]) -> Result<u32, PackError> {
    let [b'P', b'A', b'C', b'K', v0, v1, v2, v3, c0, c1, c2, c3, ..] = *b else {
        return Err(PackError::BadSignature);
    };
    let version = u32::from_be_bytes([v0, v1, v2, v3]);
    if version != 2 && version != 3 {
        return Err(PackError::UnsupportedVersion(version));
    }
    Ok(u32::from_be_bytes([c0, c1, c2, c3]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaBase {
    None,
    /// Distance back from the start of this entry to the start of the base entry.
    Ofs(u64),
    Ref(Oid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryHeader {
    pub kind: ObjType,
    /// Inflated size of the zlib payload (the delta's size for delta kinds).
    pub size: u64,
    pub base: DeltaBase,
    /// Bytes before the zlib stream starts.
    pub header_len: usize,
}

/// Parses an entry header from the front of `b`.
pub fn parse_entry_header(b: &[u8]) -> Result<EntryHeader, PackError> {
    let &first = b.first().ok_or(PackError::Truncated)?;
    let code = (first >> 4) & 0x7;
    let kind = ObjType::from_code(code).ok_or(PackError::BadObjectType(code))?;
    let mut size = u64::from(first & 0x0F);
    let mut shift: u32 = 4;
    let mut i: usize = 1;
    let mut c = first;
    while c & 0x80 != 0 {
        let &next = b.get(i).ok_or(PackError::Truncated)?;
        if shift > 57 {
            return Err(PackError::SizeOverflow);
        }
        size |= u64::from(next & 0x7F) << shift;
        shift = shift.saturating_add(7);
        i = i.saturating_add(1);
        c = next;
    }
    let base = match kind {
        ObjType::OfsDelta => {
            let &b0 = b.get(i).ok_or(PackError::Truncated)?;
            i = i.saturating_add(1);
            let mut c = b0;
            let mut ofs = u64::from(c & 0x7F);
            while c & 0x80 != 0 {
                let &next = b.get(i).ok_or(PackError::Truncated)?;
                i = i.saturating_add(1);
                ofs = ofs
                    .checked_add(1)
                    .and_then(|o| o.checked_mul(128))
                    .and_then(|o| o.checked_add(u64::from(next & 0x7F)))
                    .ok_or(PackError::BadDeltaOffset)?;
                c = next;
            }
            if ofs == 0 {
                return Err(PackError::BadDeltaOffset);
            }
            DeltaBase::Ofs(ofs)
        }
        ObjType::RefDelta => {
            let end = i.saturating_add(20);
            let raw = b.get(i..end).ok_or(PackError::Truncated)?;
            i = end;
            DeltaBase::Ref(Oid::from_slice(raw).ok_or(PackError::BadDeltaOffset)?)
        }
        _ => DeltaBase::None,
    };
    Ok(EntryHeader {
        kind,
        size,
        base,
        header_len: i,
    })
}

/// Encodes a type/size entry header; returns the number of bytes written (≤ 10).
pub fn encode_entry_header(kind: ObjType, size: u64, out: &mut [u8; 10]) -> usize {
    let mut n = 0usize;
    let mut rest = size >> 4;
    let mut byte = (kind.code() << 4) | ((size & 0x0F) as u8);
    loop {
        if rest != 0 {
            byte |= 0x80;
        }
        if let Some(slot) = out.get_mut(n) {
            *slot = byte;
        }
        n = n.saturating_add(1);
        if rest == 0 || n >= out.len() {
            break;
        }
        byte = (rest & 0x7F) as u8;
        rest >>= 7;
    }
    n
}

/// Encodes an OFS_DELTA distance (git's "offset encoding"); returns bytes written.
pub fn encode_ofs_distance(mut ofs: u64, out: &mut [u8; 10]) -> usize {
    let mut tmp = [0u8; 10];
    let mut pos = tmp.len().saturating_sub(1);
    if let Some(slot) = tmp.get_mut(pos) {
        *slot = (ofs & 0x7F) as u8;
    }
    ofs >>= 7;
    while ofs != 0 && pos > 0 {
        ofs = ofs.wrapping_sub(1);
        pos = pos.saturating_sub(1);
        if let Some(slot) = tmp.get_mut(pos) {
            *slot = 0x80 | ((ofs & 0x7F) as u8);
        }
        ofs >>= 7;
    }
    let used = tmp.get(pos..).unwrap_or(&[]);
    for (d, s) in out.iter_mut().zip(used) {
        *d = *s;
    }
    used.len()
}

/// Encodes a full OFS_DELTA entry header (type/size + distance) into `out`.
pub fn encode_ofs_delta_header(delta_size: u64, distance: u64, out: &mut [u8; 20]) -> usize {
    let mut a = [0u8; 10];
    let mut b = [0u8; 10];
    let na = encode_entry_header(ObjType::OfsDelta, delta_size, &mut a);
    let nb = encode_ofs_distance(distance, &mut b);
    let total = na.saturating_add(nb);
    for (d, s) in out
        .iter_mut()
        .zip(a.iter().take(na).chain(b.iter().take(nb)))
    {
        *d = *s;
    }
    total
}

/// Writes the loose-object header `"<type> <size>\0"`; returns its length.
pub fn loose_header(kind: ObjType, size: u64, out: &mut [u8; 32]) -> usize {
    let name = kind.name();
    let mut n = 0usize;
    for &c in name {
        if let Some(s) = out.get_mut(n) {
            *s = c;
        }
        n = n.saturating_add(1);
    }
    if let Some(s) = out.get_mut(n) {
        *s = b' ';
    }
    n = n.saturating_add(1);
    let mut digits = [0u8; 20];
    let mut d = digits.len();
    let mut v = size;
    loop {
        d = d.saturating_sub(1);
        if let Some(s) = digits.get_mut(d) {
            *s = b'0'.wrapping_add((v % 10) as u8);
        }
        v /= 10;
        if v == 0 {
            break;
        }
    }
    for &c in digits.get(d..).unwrap_or(&[]) {
        if let Some(s) = out.get_mut(n) {
            *s = c;
        }
        n = n.saturating_add(1);
    }
    if let Some(s) = out.get_mut(n) {
        *s = 0;
    }
    n.saturating_add(1)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn entry_header_roundtrip() {
        for (kind, size) in [
            (ObjType::Commit, 0u64),
            (ObjType::Tree, 15),
            (ObjType::Blob, 16),
            (ObjType::Blob, 100),
            (ObjType::Tag, 1 << 40),
        ] {
            let mut buf = [0u8; 10];
            let n = encode_entry_header(kind, size, &mut buf);
            let h = parse_entry_header(&buf[..n]).unwrap();
            assert_eq!(
                (h.kind, h.size, h.header_len, h.base),
                (kind, size, n, DeltaBase::None)
            );
            if n > 1 {
                assert_eq!(parse_entry_header(&buf[..n - 1]), Err(PackError::Truncated));
            }
        }
    }

    #[test]
    fn ofs_distance_roundtrip() {
        for ofs in [1u64, 127, 128, 16511, 16512, 1 << 30, (1 << 50) + 7] {
            let mut hdr = [0u8; 20];
            let n = encode_ofs_delta_header(5, ofs, &mut hdr);
            let h = parse_entry_header(&hdr[..n]).unwrap();
            assert_eq!(h.base, DeltaBase::Ofs(ofs));
            assert_eq!(h.size, 5);
            assert_eq!(h.header_len, n);
        }
    }

    #[test]
    fn pack_and_loose_headers() {
        assert_eq!(parse_pack_header(&pack_header(7)).unwrap(), 7);
        assert!(parse_pack_header(b"PACK\0\0\0\x09\0\0\0\0").is_err());
        let mut lh = [0u8; 32];
        let n = loose_header(ObjType::Blob, 100, &mut lh);
        assert_eq!(&lh[..n], b"blob 100\0");
        let n = loose_header(ObjType::Tree, 0, &mut lh);
        assert_eq!(&lh[..n], b"tree 0\0");
    }
}
