//! Git packfile (v2) framing: the 12-byte header and per-entry headers.

use crate::oid::Oid20;

pub const PACK_SIGNATURE: &[u8; 4] = b"PACK";
pub const PACK_HEADER_LEN: usize = 12;
pub const PACK_TRAILER_LEN: usize = 20;
/// Longest possible entry header we ever need to look at before the zlib stream:
/// 10 bytes of type/size varint + 20 bytes REF_DELTA base (or ~10 bytes OFS varint).
pub const MAX_ENTRY_HEADER_LEN: usize = 32;

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

    pub fn from_name(name: &[u8]) -> Option<Self> {
        match name {
            b"commit" => Some(Self::Commit),
            b"tree" => Some(Self::Tree),
            b"blob" => Some(Self::Blob),
            b"tag" => Some(Self::Tag),
            _ => None,
        }
    }

    #[inline]
    pub fn is_delta(self) -> bool {
        matches!(self, Self::OfsDelta | Self::RefDelta)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackError {
    BadSignature,
    UnsupportedVersion(u32),
    BadObjectType(u8),
    SizeOverflow,
    BadDeltaOffset,
}

impl core::fmt::Display for PackError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadSignature => f.write_str("missing PACK signature"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported pack version {v}"),
            Self::BadObjectType(t) => write!(f, "invalid pack object type {t}"),
            Self::SizeOverflow => f.write_str("pack entry size overflows 64 bits"),
            Self::BadDeltaOffset => f.write_str("invalid OFS_DELTA offset"),
        }
    }
}

impl std::error::Error for PackError {}

pub fn pack_header(count: u32) -> [u8; PACK_HEADER_LEN] {
    let [s0, s1, s2, s3] = *PACK_SIGNATURE;
    let [v0, v1, v2, v3] = 2u32.to_be_bytes();
    let [c0, c1, c2, c3] = count.to_be_bytes();
    [s0, s1, s2, s3, v0, v1, v2, v3, c0, c1, c2, c3]
}

/// Validates the 12-byte header and returns the object count.
pub fn parse_pack_header(b: &[u8]) -> Result<u32, PackError> {
    let [s0, s1, s2, s3, v0, v1, v2, v3, c0, c1, c2, c3, ..] = *b else {
        return Err(PackError::BadSignature);
    };
    if [s0, s1, s2, s3] != *PACK_SIGNATURE {
        return Err(PackError::BadSignature);
    }
    let version = u32::from_be_bytes([v0, v1, v2, v3]);
    if version != 2 && version != 3 {
        return Err(PackError::UnsupportedVersion(version));
    }
    Ok(u32::from_be_bytes([c0, c1, c2, c3]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaBase {
    None,
    /// Distance back from the start of this entry to the base entry.
    Ofs(u64),
    Ref(Oid20),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryHeader {
    pub kind: ObjType,
    /// Inflated size of the zlib payload (delta size for delta kinds).
    pub size: u64,
    pub base: DeltaBase,
    /// Bytes before the zlib stream starts.
    pub header_len: usize,
}

/// Parses an entry header from the front of `b`. `Ok(None)` means more bytes are needed.
pub fn parse_entry_header(b: &[u8]) -> Result<Option<EntryHeader>, PackError> {
    let Some(&first) = b.first() else {
        return Ok(None);
    };
    let code = first.wrapping_shr(4) & 0x7;
    let kind = ObjType::from_code(code).ok_or(PackError::BadObjectType(code))?;
    let mut size = u64::from(first & 0x0F);
    let mut shift: u32 = 4;
    let mut i: usize = 1;
    let mut c = first;
    while c & 0x80 != 0 {
        let Some(&next) = b.get(i) else {
            return Ok(None);
        };
        if shift > 57 {
            return Err(PackError::SizeOverflow);
        }
        size |= u64::from(next & 0x7F).wrapping_shl(shift);
        shift = shift.saturating_add(7);
        i = i.saturating_add(1);
        c = next;
    }
    let base = match kind {
        ObjType::OfsDelta => {
            let Some(&b0) = b.get(i) else {
                return Ok(None);
            };
            i = i.saturating_add(1);
            let mut c = b0;
            let mut ofs = u64::from(c & 0x7F);
            while c & 0x80 != 0 {
                let Some(&next) = b.get(i) else {
                    return Ok(None);
                };
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
            let Some(raw) = b.get(i..end) else {
                return Ok(None);
            };
            i = end;
            DeltaBase::Ref(Oid20::from_slice(raw).ok_or(PackError::BadDeltaOffset)?)
        }
        _ => DeltaBase::None,
    };
    Ok(Some(EntryHeader {
        kind,
        size,
        base,
        header_len: i,
    }))
}

/// Encodes a non-delta entry header; returns the number of bytes written (≤ 10).
pub fn encode_entry_header(kind: ObjType, size: u64, out: &mut [u8; 10]) -> usize {
    let mut n = 0usize;
    let mut rest = size.wrapping_shr(4);
    let mut byte = kind.code().wrapping_shl(4) | u8::try_from(size & 0x0F).unwrap_or(0);
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
        byte = u8::try_from(rest & 0x7F).unwrap_or(0);
        rest = rest.wrapping_shr(7);
    }
    n
}

/// Encodes an OFS_DELTA distance (git's "offset encoding"); returns bytes written.
pub fn encode_ofs_distance(mut ofs: u64, out: &mut [u8; 10]) -> usize {
    let mut tmp = [0u8; 10];
    let mut pos = tmp.len().saturating_sub(1);
    if let Some(slot) = tmp.get_mut(pos) {
        *slot = u8::try_from(ofs & 0x7F).unwrap_or(0);
    }
    ofs = ofs.wrapping_shr(7);
    while ofs != 0 && pos > 0 {
        ofs = ofs.wrapping_sub(1);
        pos = pos.saturating_sub(1);
        if let Some(slot) = tmp.get_mut(pos) {
            *slot = 0x80 | u8::try_from(ofs & 0x7F).unwrap_or(0);
        }
        ofs = ofs.wrapping_shr(7);
    }
    let used = tmp.get(pos..).unwrap_or(&[]);
    for (d, s) in out.iter_mut().zip(used) {
        *d = *s;
    }
    used.len()
}

/// Writes the loose-object header `"<type> <size>\0"`; returns its length.
pub fn loose_header(kind: ObjType, size: u64, out: &mut [u8; 32]) -> usize {
    let mut w = crate::buf::ByteWriter::new(out);
    let ok = w.put(kind.name()).is_ok()
        && w.put_u8(b' ').is_ok()
        && w.put_dec(size).is_ok()
        && w.put_u8(0).is_ok();
    if ok { w.len() } else { 0 }
}

/// Parses a loose-object header at the front of `b`: returns (type, size, header length).
pub fn parse_loose_header(b: &[u8]) -> Option<(ObjType, u64, usize)> {
    let nul = b.iter().take(32).position(|&c| c == 0)?;
    let hdr = b.get(..nul)?;
    let sp = hdr.iter().position(|&c| c == b' ')?;
    let kind = ObjType::from_name(hdr.get(..sp)?)?;
    let size = crate::buf::parse_dec_u64(hdr.get(sp.checked_add(1)?..)?)?;
    Some((kind, size, nul.checked_add(1)?))
}

#[cfg(test)]
mod tests {
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
            let h = parse_entry_header(&buf[..n]).unwrap().unwrap();
            assert_eq!(
                (h.kind, h.size, h.header_len, h.base),
                (kind, size, n, DeltaBase::None)
            );
            if n > 1 {
                assert_eq!(parse_entry_header(&buf[..n - 1]).unwrap(), None);
            }
        }
    }

    #[test]
    fn ofs_distance_roundtrip() {
        for ofs in [1u64, 127, 128, 16511, 16512, 1 << 30, (1 << 50) + 7] {
            let mut enc = [0u8; 10];
            let n = encode_ofs_distance(ofs, &mut enc);
            let mut entry = vec![(6u8 << 4) | 5];
            entry.extend_from_slice(&enc[..n]);
            let h = parse_entry_header(&entry).unwrap().unwrap();
            assert_eq!(h.base, DeltaBase::Ofs(ofs));
            assert_eq!(h.header_len, 1 + n);
        }
    }

    #[test]
    fn pack_header_and_loose_header() {
        let h = pack_header(7);
        assert_eq!(parse_pack_header(&h).unwrap(), 7);
        assert!(parse_pack_header(b"PACK\0\0\0\x09\0\0\0\0").is_err());
        let mut lh = [0u8; 32];
        let n = loose_header(ObjType::Blob, 100, &mut lh);
        assert_eq!(&lh[..n], b"blob 100\0");
        assert_eq!(parse_loose_header(&lh[..n]), Some((ObjType::Blob, 100, n)));
    }
}
