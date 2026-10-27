//! Git pack format primitives: object kinds, entry headers and the 12-byte pack header.
//!
//! An entry is `type+size varint` [`ofs varint` | `20-byte base oid`] `zlib stream`.

use crate::util::Oid;

pub const OBJ_COMMIT: u8 = 1;
pub const OBJ_TREE: u8 = 2;
pub const OBJ_BLOB: u8 = 3;
pub const OBJ_TAG: u8 = 4;
pub const OBJ_OFS_DELTA: u8 = 6;
pub const OBJ_REF_DELTA: u8 = 7;

pub const PACK_HEADER_LEN: usize = 12;
pub const PACK_TRAILER_LEN: usize = 20;
/// Smallest valid pack: header + trailer.
pub const PACK_MIN_LEN: usize = PACK_HEADER_LEN + PACK_TRAILER_LEN;

#[inline]
pub fn is_delta(kind: u8) -> bool {
    kind == OBJ_OFS_DELTA || kind == OBJ_REF_DELTA
}

pub fn type_name(kind: u8) -> &'static str {
    match kind {
        OBJ_COMMIT => "commit",
        OBJ_TREE => "tree",
        OBJ_BLOB => "blob",
        OBJ_TAG => "tag",
        OBJ_OFS_DELTA => "ofs-delta",
        OBJ_REF_DELTA => "ref-delta",
        _ => "bad",
    }
}

/// A parsed entry header.
#[derive(Clone, Copy, Debug, Default)]
pub struct EntryHeader {
    /// 1..=4 for whole objects, 6/7 for deltas.
    pub kind: u8,
    /// Inflated size of the entry data (object size, or delta size for deltas).
    pub size: u64,
    /// Bytes before the zlib stream (type/size varint plus the delta base reference).
    pub header_len: usize,
    /// OFS_DELTA: distance back from this entry's start to its base entry's start.
    pub base_distance: u64,
    /// REF_DELTA: the base object id.
    pub base_oid: Oid,
}

/// Parses the entry header at the start of `b`. `None` if truncated or malformed.
#[inline]
pub fn parse_entry_header(b: &[u8]) -> Option<EntryHeader> {
    let mut bytes = b.iter();
    let first = *bytes.next()?;
    let kind = first.wrapping_shr(4) & 7;
    if !matches!(kind, 1..=4 | 6 | 7) {
        return None;
    }
    let mut size = u64::from(first & 0x0F);
    let mut shift = 4u32;
    let mut len = 1usize;
    let mut byte = first;
    while byte & 0x80 != 0 {
        byte = *bytes.next()?;
        len = len.wrapping_add(1);
        if shift > 57 {
            return None;
        }
        size |= u64::from(byte & 0x7F).wrapping_shl(shift);
        shift = shift.wrapping_add(7);
    }
    let mut h = EntryHeader {
        kind,
        size,
        header_len: len,
        ..EntryHeader::default()
    };
    if kind == OBJ_OFS_DELTA {
        let mut byte = *bytes.next()?;
        len = len.wrapping_add(1);
        let mut dist = u64::from(byte & 0x7F);
        while byte & 0x80 != 0 {
            byte = *bytes.next()?;
            len = len.wrapping_add(1);
            if dist >= (1u64 << 56) {
                return None;
            }
            dist = dist.wrapping_add(1).wrapping_shl(7) | u64::from(byte & 0x7F);
        }
        h.base_distance = dist;
        h.header_len = len;
    } else if kind == OBJ_REF_DELTA {
        h.base_oid = Oid::from_slice(b.get(len..len.checked_add(20)?)?)?;
        h.header_len = len.checked_add(20)?;
    }
    Some(h)
}

/// Bytes of an entry header we synthesize (whole object, OFS_DELTA or REF_DELTA).
#[derive(Clone, Copy)]
pub struct HeaderBytes {
    buf: [u8; 40],
    len: usize,
}

impl HeaderBytes {
    pub fn as_slice(&self) -> &[u8] {
        self.buf.get(..self.len).unwrap_or(&[])
    }

    fn push(&mut self, byte: u8) {
        if let Some(slot) = self.buf.get_mut(self.len) {
            *slot = byte;
            self.len = self.len.wrapping_add(1);
        }
    }

    /// The type+size varint.
    pub fn whole(kind: u8, size: u64) -> HeaderBytes {
        let mut h = HeaderBytes {
            buf: [0u8; 40],
            len: 0,
        };
        let mut s = size.wrapping_shr(4);
        let mut c = kind.wrapping_shl(4) | (size & 0x0F) as u8;
        while s != 0 {
            h.push(c | 0x80);
            c = (s & 0x7F) as u8;
            s = s.wrapping_shr(7);
        }
        h.push(c);
        h
    }

    /// OFS_DELTA header: size varint, then the distance in git's "offset encoding".
    pub fn ofs_delta(size: u64, dist: u64) -> HeaderBytes {
        let mut h = HeaderBytes::whole(OBJ_OFS_DELTA, size);
        let mut tmp = [0u8; 10];
        let mut n = 0usize;
        let mut d = dist;
        if let Some(t) = tmp.get_mut(n) {
            *t = (d & 0x7F) as u8;
        }
        n = n.wrapping_add(1);
        loop {
            d = d.wrapping_shr(7);
            if d == 0 {
                break;
            }
            d = d.wrapping_sub(1);
            if let Some(t) = tmp.get_mut(n) {
                *t = 0x80 | (d & 0x7F) as u8;
            }
            n = n.wrapping_add(1);
        }
        // Most significant group first.
        for &b in tmp.get(..n).unwrap_or(&[]).iter().rev() {
            h.push(b);
        }
        h
    }

    pub fn ref_delta(size: u64, base: &Oid) -> HeaderBytes {
        let mut h = HeaderBytes::whole(OBJ_REF_DELTA, size);
        for &b in &base.0 {
            h.push(b);
        }
        h
    }
}

/// `PACK`, version 2, object count.
pub fn pack_header(count: u32) -> [u8; 12] {
    let mut h = [0u8; 12];
    let v = 2u32.to_be_bytes();
    let c = count.to_be_bytes();
    for (dst, src) in h
        .iter_mut()
        .zip(b"PACK".iter().chain(v.iter()).chain(c.iter()))
    {
        *dst = *src;
    }
    h
}

/// Validates a pack header and returns its object count.
pub fn parse_pack_header(b: &[u8]) -> Option<u32> {
    let [b'P', b'A', b'C', b'K', v0, v1, v2, v3, c0, c1, c2, c3, ..] = *b else {
        return None;
    };
    let version = u32::from_be_bytes([v0, v1, v2, v3]);
    if version != 2 && version != 3 {
        return None;
    }
    Some(u32::from_be_bytes([c0, c1, c2, c3]))
}

/// `"<type> <size>\0"` as hashed into object ids and stored at the head of loose objects.
#[derive(Clone, Copy)]
pub struct ObjectHeader {
    buf: [u8; 32],
    len: usize,
}

impl ObjectHeader {
    pub fn new(kind: u8, size: u64) -> ObjectHeader {
        let mut h = ObjectHeader {
            buf: [0u8; 32],
            len: 0,
        };
        let mut put = |b: u8| {
            if let Some(slot) = h.buf.get_mut(h.len) {
                *slot = b;
                h.len = h.len.wrapping_add(1);
            }
        };
        for &b in type_name(kind).as_bytes() {
            put(b);
        }
        put(b' ');
        let mut digits = [0u8; 20];
        let mut n = 0usize;
        let mut v = size;
        loop {
            if let Some(d) = digits.get_mut(n) {
                *d = b'0'.wrapping_add((v % 10) as u8);
            }
            n = n.wrapping_add(1);
            v /= 10;
            if v == 0 {
                break;
            }
        }
        for &d in digits.get(..n).unwrap_or(&[]).iter().rev() {
            put(d);
        }
        put(0);
        h
    }

    pub fn as_slice(&self) -> &[u8] {
        self.buf.get(..self.len).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        for kind in [OBJ_COMMIT, OBJ_TREE, OBJ_BLOB, OBJ_TAG] {
            for size in [
                0u64,
                1,
                15,
                16,
                127,
                128,
                4095,
                1 << 20,
                (1 << 35) + 7,
                u32::MAX as u64 * 3,
            ] {
                let h = HeaderBytes::whole(kind, size);
                let p = parse_entry_header(h.as_slice()).unwrap();
                assert_eq!(
                    (p.kind, p.size, p.header_len),
                    (kind, size, h.as_slice().len())
                );
            }
        }
        for dist in [
            1u64,
            127,
            128,
            129,
            16383,
            16384,
            16511,
            16512,
            1 << 30,
            (1 << 40) + 3,
        ] {
            let h = HeaderBytes::ofs_delta(99, dist);
            let p = parse_entry_header(h.as_slice()).unwrap();
            assert_eq!(
                (p.kind, p.size, p.base_distance, p.header_len),
                (OBJ_OFS_DELTA, 99, dist, h.as_slice().len())
            );
        }
        let oid = Oid([7u8; 20]);
        let h = HeaderBytes::ref_delta(5, &oid);
        let p = parse_entry_header(h.as_slice()).unwrap();
        assert_eq!(
            (p.kind, p.base_oid, p.header_len),
            (OBJ_REF_DELTA, oid, h.as_slice().len())
        );
        assert!(parse_entry_header(&[0x80]).is_none());
        assert!(parse_entry_header(&[0x50]).is_none());
        assert_eq!(parse_pack_header(&pack_header(77)), Some(77));
        assert_eq!(parse_pack_header(b"PACK\0\0\0\x09\0\0\0\x01"), None);
    }

    #[test]
    fn object_header_format() {
        assert_eq!(ObjectHeader::new(OBJ_BLOB, 1234).as_slice(), b"blob 1234\0");
        assert_eq!(ObjectHeader::new(OBJ_TREE, 0).as_slice(), b"tree 0\0");
        assert_eq!(
            ObjectHeader::new(OBJ_COMMIT, u64::MAX).as_slice(),
            b"commit 18446744073709551615\0"
        );
    }
}
