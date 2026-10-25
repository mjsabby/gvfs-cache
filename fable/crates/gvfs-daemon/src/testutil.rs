//! Test helpers: a tiny pack builder with real zlib streams and OFS_DELTA entries.
#![allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

use sha1::{Digest, Sha1};

use crate::oid::Oid;
use crate::pack::{ObjType, encode_entry_header, encode_ofs_delta_header, pack_header};
use crate::zlib::{Deflater, compress_bound};

#[derive(Debug, Clone, Copy)]
pub struct TestObj {
    pub oid: Oid,
    pub kind: ObjType,
    pub offset: u64,
    pub content_len: usize,
}

pub struct PackBuilder {
    body: Vec<u8>,
    count: u32,
    deflater: Deflater,
    contents: Vec<Vec<u8>>,
}

impl Default for PackBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub fn object_id(kind: ObjType, content: &[u8]) -> Oid {
    let mut h = Sha1::new();
    h.update(kind.name());
    h.update(b" ");
    h.update(content.len().to_string().as_bytes());
    h.update([0u8]);
    h.update(content);
    Oid(h.finalize().into())
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// A simple delta: copy the common prefix, insert the middle, copy the common suffix.
pub fn make_delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    let mut d = Vec::new();
    varint(base.len() as u64, &mut d);
    varint(target.len() as u64, &mut d);
    let prefix = base.iter().zip(target).take_while(|(a, b)| a == b).count();
    let max_suffix = base.len().min(target.len()) - prefix;
    let suffix = base
        .iter()
        .rev()
        .zip(target.iter().rev())
        .take(max_suffix)
        .take_while(|(a, b)| a == b)
        .count();
    let copy = |d: &mut Vec<u8>, off: usize, len: usize| {
        if len == 0 {
            return;
        }
        let mut op = 0x80u8;
        let mut args = Vec::new();
        for i in 0..4 {
            let b = ((off >> (8 * i)) & 0xFF) as u8;
            if b != 0 {
                op |= 1 << i;
                args.push(b);
            }
        }
        for i in 0..3 {
            let b = ((len >> (8 * i)) & 0xFF) as u8;
            if b != 0 {
                op |= 0x10 << i;
                args.push(b);
            }
        }
        d.push(op);
        d.extend_from_slice(&args);
    };
    copy(&mut d, 0, prefix);
    let middle = &target[prefix..target.len() - suffix];
    for chunk in middle.chunks(127) {
        d.push(chunk.len() as u8);
        d.extend_from_slice(chunk);
    }
    copy(&mut d, base.len() - suffix, suffix);
    d
}

impl PackBuilder {
    pub fn new() -> Self {
        Self {
            body: Vec::new(),
            count: 0,
            deflater: Deflater::new(6),
            contents: Vec::new(),
        }
    }

    fn zlib(&mut self, data: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8; compress_bound(data.len()) + 16];
        let n = self.deflater.deflate(data, &mut out).unwrap();
        out.truncate(n);
        out
    }

    pub fn full(&mut self, kind: ObjType, content: &[u8]) -> TestObj {
        let offset = 12 + self.body.len() as u64;
        let mut hdr = [0u8; 10];
        let n = encode_entry_header(kind, content.len() as u64, &mut hdr);
        self.body.extend_from_slice(&hdr[..n]);
        let z = self.zlib(content);
        self.body.extend_from_slice(&z);
        self.count += 1;
        self.contents.push(content.to_vec());
        TestObj {
            oid: object_id(kind, content),
            kind,
            offset,
            content_len: content.len(),
        }
    }

    /// Adds `target` as an OFS_DELTA against `base` (which must already be in the pack).
    pub fn ofs_delta(&mut self, base: TestObj, target: &[u8]) -> TestObj {
        let base_content = self
            .contents
            .iter()
            .find(|c| object_id(base.kind, c) == base.oid)
            .cloned()
            .unwrap();
        let delta = make_delta(&base_content, target);
        let offset = 12 + self.body.len() as u64;
        let mut hdr = [0u8; 20];
        let n = encode_ofs_delta_header(delta.len() as u64, offset - base.offset, &mut hdr);
        self.body.extend_from_slice(&hdr[..n]);
        let z = self.zlib(&delta);
        self.body.extend_from_slice(&z);
        self.count += 1;
        self.contents.push(target.to_vec());
        TestObj {
            oid: object_id(base.kind, target),
            kind: base.kind,
            offset,
            content_len: target.len(),
        }
    }

    pub fn finish(self) -> Vec<u8> {
        let mut pack = pack_header(self.count).to_vec();
        pack.extend_from_slice(&self.body);
        let digest: [u8; 20] = Sha1::digest(&pack).into();
        pack.extend_from_slice(&digest);
        pack
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::apply_delta;

    #[test]
    fn make_delta_roundtrips() {
        let base = b"the quick brown fox jumps over the lazy dog";
        let target = b"the quick red fox jumps over the lazy dog";
        let d = make_delta(base, target);
        let mut out = [0u8; 64];
        let n = apply_delta(base, &d, &mut out).unwrap();
        assert_eq!(&out[..n], target);
        let d2 = make_delta(base, b"completely different");
        let n = apply_delta(base, &d2, &mut out).unwrap();
        assert_eq!(&out[..n], b"completely different");
    }
}
