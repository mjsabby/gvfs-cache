//! GVFS protocol framings: `GPRE` timestamped packfile envelope and `GVFS ` loose-object stream.

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

pub fn gpre_file_header(num_packs: u16) -> [u8; GPRE_FILE_HEADER_LEN] {
    let [g, p, r, e, sp, v] = *GPRE_MAGIC;
    let [n0, n1] = num_packs.to_le_bytes();
    [g, p, r, e, sp, v, n0, n1]
}

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
    pub idx_len: Option<i64>,
}

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
