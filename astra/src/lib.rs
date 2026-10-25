#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::panic
    )
)]

pub mod arena;
pub mod buf;
pub mod client;
pub mod delta;
pub mod error;
pub mod gpre;
pub mod hfs;
pub mod idx;
pub mod ingest;
pub mod json;
pub mod manifest;
pub mod mphf;
pub mod object;
pub mod oid;
pub mod pack;
pub mod pipeline;
pub mod serve;
pub mod slab;
pub mod source;
pub mod splice;
pub mod storage;
pub mod tree_delta;
pub mod zlib;

pub use error::{Error, Result};
pub use oid::Oid20;
pub use pack::ObjType;

pub fn sha1_parts(parts: &[&[u8]]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let mut hash = Sha1::new();
    for part in parts {
        hash.update(part);
    }
    hash.finalize().into()
}

pub fn object_id(kind: ObjType, content: &[u8]) -> Oid20 {
    let mut header = [0; 32];
    let len = pack::loose_header(kind, content.len() as u64, &mut header);
    Oid20(sha1_parts(&[header.get(..len).unwrap_or(&[]), content]))
}
