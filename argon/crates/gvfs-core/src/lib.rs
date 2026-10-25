//! `gvfs-core` — Zero-allocation wire formats, arena/slab pools, `Git-hfs 1.0` and 100-byte
//! pointer parsers, Packed-u64 Minimal Perfect Hash Function (`MphfSlabIndex`), HTTP Range
//! coalescing, and on-the-fly pack splicing (`plan_deduplicated_range_splice` +
//! `assemble_spliced_pack`).
#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

pub mod arena;
pub mod buf;
pub mod delta;
pub mod gpre;
pub mod hfs;
pub mod idx;
pub mod json;
pub mod manifest;
pub mod mphf;
pub mod object;
pub mod oid;
pub mod pack;
pub mod pktline;
pub mod rev;
pub mod slab;
pub mod tree_delta;
pub mod zlib;

pub use arena::{ArenaError, ArenaSpan, OidIdxMap, PackSlabPool, ScratchArena, SeenOidSet};
pub use hfs::{
    HfsBlobRecord, HfsPointer, HfsPointerMeta, format_git_hfs_v1, format_hfs_pointer,
    parse_any_hfs_pointer, parse_git_hfs_v1, parse_hfs_pointer_100b,
};
pub use manifest::{ManifestDescriptor, SlabManifestEntry, UnifiedManifest};
pub use mphf::{MphfSlabIndex, PackedObjLoc};
pub use oid::{Oid20, ZERO_OID};
pub use pack::ObjType;
pub use slab::{
    HttpRangeBatch, MmapPackBundle, ObjSliceMeta, Slab, SpliceArena, Tier, assemble_spliced_pack,
    plan_deduplicated_range_splice, splice_in_memory_slabs,
};
pub use tree_delta::{
    ClusterMemberMeta, ContiguousTreeCluster, MAX_TREE_CLUSTER_DELTA_DEPTH, RawTreeVersion,
    compute_tree_delta, encode_pack_base_tree, encode_pack_ofs_delta,
    pack_directory_history_clusters, pack_directory_history_clusters_with_deflater,
};

/// SHA-1 of `parts` concatenated.
pub fn sha1_parts(parts: &[&[u8]]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    for p in parts {
        h.update(p);
    }
    h.finalize().into()
}

/// Git object ID (`SHA-1("<type> <size>\0" + content)`).
pub fn object_id(kind: ObjType, content: &[u8]) -> Oid20 {
    use sha1::{Digest, Sha1};
    let mut hdr = [0u8; 32];
    let hdr_len = pack::loose_header(kind, content.len() as u64, &mut hdr);
    let mut h = Sha1::new();
    h.update(hdr.get(..hdr_len).unwrap_or(&[]));
    h.update(content);
    Oid20(h.finalize().into())
}
