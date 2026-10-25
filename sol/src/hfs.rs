use crate::codec::{decode_hex, number};
use crate::{Error, Result};

/// BlobStore IDs contain a 32-byte VSO digest plus an optional algorithm byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HfsPointer {
    pub blob_id: [u8; 33],
    pub blob_id_len: u8,
    pub real_size: u64,
}

impl Default for HfsPointer {
    fn default() -> Self {
        Self {
            blob_id: [0; 33],
            blob_id_len: 0,
            real_size: 0,
        }
    }
}

fn line(value: Option<&[u8]>) -> Result<&[u8]> {
    let value = value.ok_or(Error::Invalid("HFS pointer line"))?;
    Ok(value.strip_suffix(b"\r").unwrap_or(value))
}

pub fn parse_git_hfs_v1(bytes: &[u8]) -> Result<Option<HfsPointer>> {
    let bytes = bytes.strip_prefix(b"~~").unwrap_or(bytes);
    if !bytes.starts_with(b"Git-hfs") {
        return Ok(None);
    }
    if bytes.len() > 256 {
        return Err(Error::Invalid("HFS pointer length"));
    }
    let mut lines = bytes.split(|b| *b == b'\n');
    if line(lines.next())? != b"Git-hfs 1.0" {
        return Err(Error::Invalid("HFS version"));
    }
    let hash = line(lines.next())?
        .strip_prefix(b"blob:")
        .ok_or(Error::Invalid("HFS blob field"))?;
    let size = line(lines.next())?
        .strip_prefix(b"size:")
        .ok_or(Error::Invalid("HFS size field"))?;
    if lines.any(|line| !line.is_empty()) {
        return Err(Error::Invalid("HFS trailing data"));
    }
    let mut pointer = HfsPointer {
        real_size: number(size)?,
        ..HfsPointer::default()
    };
    if hash == b"InvalidBlobId" {
        return Ok(Some(pointer));
    }
    let len = match hash.len() {
        64 => 32,
        66 => 33,
        _ => return Err(Error::Invalid("HFS BlobId length")),
    };
    decode_hex(
        hash,
        pointer
            .blob_id
            .get_mut(..len)
            .ok_or(Error::Invalid("HFS BlobId"))?,
    )?;
    pointer.blob_id_len = len as u8;
    Ok(Some(pointer))
}
