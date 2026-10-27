//! GVFS protocol framings: the `GPRE` timestamped multi-pack stream and the
//! `GVFS ` multi-loose-object stream (all integers little-endian).

use crate::buf::le_i64;
use crate::oid::Oid20;

pub const GPRE_MAGIC: &[u8; 6] = b"GPRE \x01";
pub const GPRE_FILE_HEADER_LEN: usize = 8;
pub const GPRE_PACK_HEADER_LEN: usize = 24;
pub const GVFS_LOOSE_MAGIC: &[u8; 6] = b"GVFS \x01";
pub const GVFS_LOOSE_OBJECT_HEADER_LEN: usize = 28;
pub const GVFS_LOOSE_TRAILER: [u8; 20] = [0u8; 20];

pub const CONTENT_TYPE_PREFETCH: &str = "application/x-gvfs-timestamped-packfiles-indexes";
pub const CONTENT_TYPE_PACKFILE: &str = "application/x-git-packfile";
pub const CONTENT_TYPE_LOOSE_OBJECT: &str = "application/x-git-loose-object";
pub const CONTENT_TYPE_GVFS_LOOSE_OBJECTS: &str = "application/x-gvfs-loose-objects";

/// A validator for one cursor's representation, without hashing multi-GB bodies.
pub fn etag(revision: Oid20, cursor: u64, include_index: bool) -> Oid20 {
    Oid20(crate::sha1_parts(&[
        b"gvfs-prefetch-v2",
        &revision.0,
        &cursor.to_le_bytes(),
        &[u8::from(include_index)],
    ]))
}

/// `"GPRE " 0x01` followed by the little-endian pack count.
pub fn gpre_file_header(num_packs: u16) -> [u8; GPRE_FILE_HEADER_LEN] {
    let [g, p, r, e, sp, v] = *GPRE_MAGIC;
    let [n0, n1] = num_packs.to_le_bytes();
    [g, p, r, e, sp, v, n0, n1]
}

/// Per-pack header: timestamp, pack length, idx length (-1 = no idx).
pub fn gpre_pack_header(timestamp: i64, pack_len: i64, idx_len: i64) -> [u8; GPRE_PACK_HEADER_LEN] {
    let mut out = [0u8; GPRE_PACK_HEADER_LEN];
    for (dst, src) in out.iter_mut().zip(
        timestamp
            .to_le_bytes()
            .iter()
            .chain(pack_len.to_le_bytes().iter())
            .chain(idx_len.to_le_bytes().iter()),
    ) {
        *dst = *src;
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GprePackHeader {
    pub timestamp: i64,
    pub pack_len: i64,
    /// `None` when the server sent no index (-1, or 0 as some servers do).
    pub idx_len: Option<i64>,
}

/// Parses the 8-byte file header; returns the pack count.
pub fn parse_gpre_file_header(b: &[u8]) -> Option<u16> {
    let [g, p, r, e, sp, v, n0, n1, ..] = *b else {
        return None;
    };
    if [g, p, r, e, sp, v] != *GPRE_MAGIC {
        return None;
    }
    Some(u16::from_le_bytes([n0, n1]))
}

pub fn parse_gpre_pack_header(b: &[u8]) -> Option<GprePackHeader> {
    let timestamp = le_i64(b, 0)?;
    let pack_len = le_i64(b, 8)?;
    let idx_raw = le_i64(b, 16)?;
    if pack_len <= 0 {
        return None;
    }
    let idx_len = if idx_raw <= 0 { None } else { Some(idx_raw) };
    Some(GprePackHeader {
        timestamp,
        pack_len,
        idx_len,
    })
}

/// Per-object header of the `application/x-gvfs-loose-objects` stream.
pub fn gvfs_loose_object_header(oid: &Oid20, len: i64) -> [u8; GVFS_LOOSE_OBJECT_HEADER_LEN] {
    let mut out = [0u8; GVFS_LOOSE_OBJECT_HEADER_LEN];
    for (dst, src) in out
        .iter_mut()
        .zip(oid.0.iter().chain(len.to_le_bytes().iter()))
    {
        *dst = *src;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_roundtrip() {
        let fh = gpre_file_header(513);
        assert_eq!(&fh[..6], b"GPRE \x01");
        assert_eq!(parse_gpre_file_header(&fh), Some(513));
        let ph = gpre_pack_header(1_700_000_000, 1234, -1);
        assert_eq!(
            parse_gpre_pack_header(&ph),
            Some(GprePackHeader {
                timestamp: 1_700_000_000,
                pack_len: 1234,
                idx_len: None
            })
        );
        let ph = gpre_pack_header(5, 10, 20);
        assert_eq!(parse_gpre_pack_header(&ph).unwrap().idx_len, Some(20));
        let lh = gvfs_loose_object_header(&Oid20([3; 20]), 77);
        assert_eq!(&lh[..20], &[3u8; 20]);
        assert_eq!(i64::from_le_bytes(lh[20..].try_into().unwrap()), 77);
    }
}
