//! Core Primitive 1: `ingest_git_delta` (and `ingest_ado_delta` alias) (PDF pp. 154–169).
//!
//! Uses a pre-allocated, slice-based [`IngestArena`] so pack decoding, delta resolution,
//! `Git-hfs 1.0` / `~~Git-hfs 1.0` pointer parsing, and strict **Merkle DFS ordering**
//! operate over contiguous arena byte slices (`ArenaSpan`) with reusable `Inflater` and
//! `Deflater` instances.
//!
//! Every entry written into the resulting [`Slab`] is a self-contained, position-independent
//! non-delta pack entry (`[type_and_size_varint][zlib_bytes]`), allowing Core Primitive 2
//! (`plan_deduplicated_range_splice` + `assemble_spliced_pack`) to splice arbitrary subsets
//! of objects across slabs with **zero zlib CPU**!

use bytes::Bytes;
use sha1::{Digest, Sha1};

use gvfs_core::arena::{ArenaSpan, OidIdxMap, ScratchArena, SeenOidSet};
use gvfs_core::buf::prefault;
use gvfs_core::delta::{DeltaError, apply_delta};
use gvfs_core::hfs::{HfsBlobRecord, parse_any_hfs_pointer};
use gvfs_core::object::{TreeIter, parse_commit};
use gvfs_core::pack::{
    DeltaBase, ObjType, PACK_HEADER_LEN, PACK_TRAILER_LEN, PackError, encode_entry_header,
    pack_header, parse_entry_header, parse_pack_header,
};
use gvfs_core::slab::{ObjSliceMeta, Slab, Tier};
use gvfs_core::tree_delta::{RawTreeVersion, pack_directory_history_clusters_with_deflater};
use gvfs_core::zlib::{Deflater, Inflater, ZError, compress_bound};
use gvfs_core::{Oid20, ZERO_OID, object_id};

use crate::upstream::{GitUpstream, UpstreamError};

#[derive(Debug)]
pub enum IngestError {
    Upstream(UpstreamError),
    Pack(PackError),
    Zlib(ZError),
    Delta(DeltaError),
    PackChecksumMismatch,
    UnresolvedDeltaBase(Oid20),
    ArenaExhausted,
    SlabBuildFailed,
}

impl core::fmt::Display for IngestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Upstream(e) => write!(f, "upstream error: {e}"),
            Self::Pack(e) => write!(f, "pack error: {e}"),
            Self::Zlib(e) => write!(f, "zlib error: {e}"),
            Self::Delta(e) => write!(f, "delta error: {e}"),
            Self::PackChecksumMismatch => f.write_str("incoming pack SHA-1 checksum mismatch"),
            Self::UnresolvedDeltaBase(oid) => write!(f, "unresolved delta base {oid}"),
            Self::ArenaExhausted => f.write_str("ingest arena capacity exhausted"),
            Self::SlabBuildFailed => f.write_str("failed to build slab index"),
        }
    }
}

impl std::error::Error for IngestError {}

impl From<UpstreamError> for IngestError {
    fn from(e: UpstreamError) -> IngestError {
        Self::Upstream(e)
    }
}
impl From<PackError> for IngestError {
    fn from(e: PackError) -> IngestError {
        Self::Pack(e)
    }
}
impl From<ZError> for IngestError {
    fn from(e: ZError) -> IngestError {
        Self::Zlib(e)
    }
}
impl From<DeltaError> for IngestError {
    fn from(e: DeltaError) -> IngestError {
        Self::Delta(e)
    }
}

/// Descriptor for one decoded object sitting inside [`IngestArena`]'s byte arenas.
#[derive(Debug, Clone, Copy)]
struct DecodedObj {
    oid: Oid20,
    kind: ObjType,
    /// Offset of this entry in the incoming pack stream (for `OFS_DELTA` resolution).
    in_pack_offset: u64,
    /// Span inside `IngestArena::uncompressed` holding the inflated object bytes.
    uncompressed: ArenaSpan,
    /// Span inside `IngestArena::compressed` holding the self-contained `[header][zlib]` slice.
    compressed: ArenaSpan,
    crc32: u32,
}

/// Pre-allocated, resettable arena for zero-allocation pack ingestion and Merkle DFS ordering.
#[derive(Debug)]
pub struct IngestArena {
    pub inflater: Inflater,
    pub deflater: Deflater,
    pub inflate_scratch: Vec<u8>,
    pub delta_scratch: Vec<u8>,
    pub deflate_scratch: Vec<u8>,
    pub uncompressed: ScratchArena,
    pub compressed: ScratchArena,
    decoded: Vec<DecodedObj>,
    oid_to_decoded: OidIdxMap,
    emitted: SeenOidSet,
    hfs_records: Vec<HfsBlobRecord>,
    ordered_indices: Vec<usize>,
    commit_indices: Vec<usize>,
    tree_stack: Vec<Oid20>,
    subtrees: Vec<Oid20>,
    blobs: Vec<Oid20>,
}

impl Default for IngestArena {
    fn default() -> Self {
        Self::with_capacity(8 << 20, 64 << 20, 32 << 20, 65_536)
    }
}

impl IngestArena {
    pub fn with_capacity(
        max_obj_bytes: usize,
        uncompressed_arena_bytes: usize,
        compressed_arena_bytes: usize,
        max_objects: usize,
    ) -> Self {
        let mut inflate_scratch = vec![0u8; max_obj_bytes];
        prefault(&mut inflate_scratch);
        let mut delta_scratch = vec![0u8; max_obj_bytes];
        prefault(&mut delta_scratch);
        let mut deflate_scratch = vec![0u8; compress_bound(max_obj_bytes).saturating_add(16)];
        prefault(&mut deflate_scratch);

        Self {
            inflater: Inflater::new(),
            deflater: Deflater::new(1),
            inflate_scratch,
            delta_scratch,
            deflate_scratch,
            uncompressed: ScratchArena::with_capacity(uncompressed_arena_bytes, max_objects),
            compressed: ScratchArena::with_capacity(compressed_arena_bytes, 16),
            decoded: Vec::with_capacity(max_objects),
            oid_to_decoded: OidIdxMap::with_capacity_log2(18),
            emitted: SeenOidSet::with_capacity_log2(18),
            hfs_records: Vec::with_capacity(max_objects),
            ordered_indices: Vec::with_capacity(max_objects),
            commit_indices: Vec::with_capacity(4096),
            tree_stack: Vec::with_capacity(4096),
            subtrees: Vec::with_capacity(4096),
            blobs: Vec::with_capacity(4096),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.inflater.reset();
        self.uncompressed.reset();
        self.compressed.reset();
        self.decoded.clear();
        self.oid_to_decoded.clear();
        self.emitted.clear();
        self.hfs_records.clear();
        self.ordered_indices.clear();
        self.commit_indices.clear();
        self.tree_stack.clear();
        self.subtrees.clear();
        self.blobs.clear();
    }

    #[inline]
    fn find_by_in_pack_offset(&self, offset: u64) -> Option<usize> {
        self.decoded
            .binary_search_by_key(&offset, |d| d.in_pack_offset)
            .ok()
    }

    #[inline]
    fn find_by_oid(&self, oid: &Oid20) -> Option<usize> {
        usize::try_from(self.oid_to_decoded.get(oid)?).ok()
    }
}

/// Core Primitive 1: Ingests a Git delta (`have_tip -> want_tip`) from `upstream` into a
/// Merkle-DFS ordered [`Slab`] with `.pack`, `.mphf`, `.idx` v2, and `.hfs` sidecar!
///
/// Handles the force-push / garbage-collected `have_tip` edge case (PDF p. 169): if
/// `upstream` responds with `UpstreamError::NotOurRef(have_tip)`, automatically retries
/// with `fallback_haves` (the previous 24h / 168h slab commit SHAs).
pub fn ingest_git_delta_with_arena(
    upstream: &dyn GitUpstream,
    have_tip: Oid20,
    want_tip: Oid20,
    fallback_haves: &[Oid20],
    tier: Tier,
    now_ts: u64,
    arena: &mut IngestArena,
) -> Result<Slab, IngestError> {
    let raw_pack = match upstream.fetch_delta_pack(have_tip, want_tip, &[]) {
        Ok(p) => p,
        Err(UpstreamError::NotOurRef(_)) if !fallback_haves.is_empty() => {
            upstream.fetch_delta_pack(ZERO_OID, want_tip, fallback_haves)?
        }
        Err(e) => return Err(IngestError::Upstream(e)),
    };
    ingest_pack_bytes_into_slab(
        &raw_pack,
        Some(upstream),
        have_tip,
        want_tip,
        tier,
        now_ts,
        arena,
    )
}

/// Decodes a raw Git Pack v2 stream in `arena`, parses `Git-hfs 1.0` / `~~Git-hfs 1.0`
/// pointers via `parse_any_hfs_pointer`, orders all objects in strict **Merkle DFS order**,
/// and seals a [`Slab`] (`.pack` + `.mphf` + `.idx` + `.hfs`).
pub fn ingest_pack_bytes_into_slab(
    raw_pack: &[u8],
    upstream: Option<&dyn GitUpstream>,
    have_tip: Oid20,
    want_tip: Oid20,
    tier: Tier,
    now_ts: u64,
    arena: &mut IngestArena,
) -> Result<Slab, IngestError> {
    arena.reset();

    if raw_pack.len() < PACK_HEADER_LEN.saturating_add(PACK_TRAILER_LEN) {
        return Err(IngestError::Pack(PackError::BadSignature));
    }
    let body_end = raw_pack.len().saturating_sub(PACK_TRAILER_LEN);
    let body = raw_pack
        .get(..body_end)
        .ok_or(IngestError::Pack(PackError::BadSignature))?;
    let expected_sum = raw_pack
        .get(body_end..)
        .ok_or(IngestError::Pack(PackError::BadSignature))?;
    let actual_sum: [u8; 20] = Sha1::digest(body).into();
    if actual_sum.as_slice() != expected_sum {
        return Err(IngestError::PackChecksumMismatch);
    }

    let entry_count = parse_pack_header(raw_pack)?;
    let mut pos = PACK_HEADER_LEN;

    for _ in 0..entry_count {
        let entry_start = pos;
        let in_pack_offset = u64::try_from(entry_start).map_err(|_| IngestError::ArenaExhausted)?;
        let tail = body
            .get(entry_start..)
            .ok_or(IngestError::Pack(PackError::BadSignature))?;
        let hdr = parse_entry_header(tail)?.ok_or(IngestError::Pack(PackError::BadSignature))?;
        let z_start = entry_start
            .checked_add(hdr.header_len)
            .ok_or(IngestError::ArenaExhausted)?;
        let z_tail = body
            .get(z_start..)
            .ok_or(IngestError::Zlib(ZError::Truncated))?;
        let inflated_len = usize::try_from(hdr.size).map_err(|_| IngestError::ArenaExhausted)?;

        if inflated_len > arena.inflate_scratch.len() {
            arena.inflate_scratch.resize(inflated_len, 0);
        }
        let z_consumed =
            arena
                .inflater
                .inflate_exact(z_tail, &mut arena.inflate_scratch, inflated_len)?;
        let entry_end = z_start
            .checked_add(z_consumed)
            .ok_or(IngestError::ArenaExhausted)?;
        pos = entry_end;

        if !hdr.kind.is_delta() {
            let payload = arena
                .inflate_scratch
                .get(..inflated_len)
                .ok_or(IngestError::ArenaExhausted)?;
            let oid = object_id(hdr.kind, payload);

            // Check if this blob is a `Git-hfs 1.0` / `~~Git-hfs 1.0` or 100-byte pointer.
            if hdr.kind == ObjType::Blob {
                if let Some(meta) = parse_any_hfs_pointer(payload) {
                    arena.hfs_records.push(HfsBlobRecord { oid, meta });
                }
            }

            let (u_dst, u_span) = arena
                .uncompressed
                .alloc_bytes(payload.len())
                .map_err(|_| IngestError::ArenaExhausted)?;
            u_dst.copy_from_slice(payload);

            // Copy the original compressed `[header][zlib]` slice verbatim (zero re-compression!).
            let raw_entry = body
                .get(entry_start..entry_end)
                .ok_or(IngestError::ArenaExhausted)?;
            let (c_dst, c_span) = arena
                .compressed
                .alloc_bytes(raw_entry.len())
                .map_err(|_| IngestError::ArenaExhausted)?;
            c_dst.copy_from_slice(raw_entry);
            let crc32 = crc32fast::hash(raw_entry);

            let decoded_idx = u32::try_from(arena.decoded.len()).unwrap_or(0);
            let _ = arena.oid_to_decoded.insert(oid, decoded_idx);
            arena.decoded.push(DecodedObj {
                oid,
                kind: hdr.kind,
                in_pack_offset,
                uncompressed: u_span,
                compressed: c_span,
                crc32,
            });
        } else {
            // Resolve `OFS_DELTA` or `REF_DELTA` into a self-contained non-delta entry!
            let (base_kind, base_bytes_vec) = match hdr.base {
                DeltaBase::Ofs(ofs) => {
                    let base_off = in_pack_offset
                        .checked_sub(ofs)
                        .ok_or(IngestError::Pack(PackError::BadDeltaOffset))?;
                    let base_idx = arena
                        .find_by_in_pack_offset(base_off)
                        .ok_or(IngestError::Pack(PackError::BadDeltaOffset))?;
                    let base_obj = *arena
                        .decoded
                        .get(base_idx)
                        .ok_or(IngestError::ArenaExhausted)?;
                    let b_slice = arena
                        .uncompressed
                        .bytes(base_obj.uncompressed)
                        .map_err(|_| IngestError::ArenaExhausted)?;
                    (base_obj.kind, b_slice.to_vec())
                }
                DeltaBase::Ref(base_oid) => {
                    if let Some(base_idx) = arena.find_by_oid(&base_oid) {
                        let base_obj = *arena
                            .decoded
                            .get(base_idx)
                            .ok_or(IngestError::ArenaExhausted)?;
                        let b_slice = arena
                            .uncompressed
                            .bytes(base_obj.uncompressed)
                            .map_err(|_| IngestError::ArenaExhausted)?;
                        (base_obj.kind, b_slice.to_vec())
                    } else if let Some(up) = upstream {
                        up.read_base_object(&base_oid)?
                            .ok_or(IngestError::UnresolvedDeltaBase(base_oid))?
                    } else {
                        return Err(IngestError::UnresolvedDeltaBase(base_oid));
                    }
                }
                DeltaBase::None => return Err(IngestError::Pack(PackError::BadDeltaOffset)),
            };

            let delta_instr = arena
                .inflate_scratch
                .get(..inflated_len)
                .ok_or(IngestError::ArenaExhausted)?;
            let (_, result_size, _) = gvfs_core::delta::delta_sizes(delta_instr)?;
            let result_len =
                usize::try_from(result_size).map_err(|_| IngestError::ArenaExhausted)?;
            if result_len > arena.delta_scratch.len() {
                arena.delta_scratch.resize(result_len, 0);
            }
            let written_len = apply_delta(&base_bytes_vec, delta_instr, &mut arena.delta_scratch)?;
            let resolved = arena
                .delta_scratch
                .get(..written_len)
                .ok_or(IngestError::ArenaExhausted)?;
            let oid = object_id(base_kind, resolved);

            if base_kind == ObjType::Blob {
                if let Some(meta) = parse_any_hfs_pointer(resolved) {
                    arena.hfs_records.push(HfsBlobRecord { oid, meta });
                }
            }

            let (u_dst, u_span) = arena
                .uncompressed
                .alloc_bytes(resolved.len())
                .map_err(|_| IngestError::ArenaExhausted)?;
            u_dst.copy_from_slice(resolved);

            // Re-encode resolved delta as a self-contained non-delta pack entry `[hdr][zlib]`
            // for intraday slabs (or non-tree objects). For `24h`/`168h` tree objects,
            // `assemble_merkle_tree_clustered_pack` packs them from `arena.uncompressed`
            // directly into contiguous tree-delta clusters, avoiding double zlib work!
            let (c_span, crc32) = if tier.uses_tree_deltas() && base_kind == ObjType::Tree {
                (ArenaSpan { offset: 0, len: 0 }, 0)
            } else {
                let mut new_hdr = [0u8; 10];
                let new_hdr_len = encode_entry_header(base_kind, result_size, &mut new_hdr);
                let max_z = compress_bound(resolved.len());
                if max_z > arena.deflate_scratch.len() {
                    arena.deflate_scratch.resize(max_z, 0);
                }
                let z_len = arena
                    .deflater
                    .deflate(resolved, &mut arena.deflate_scratch)?;
                let total_entry_len = new_hdr_len
                    .checked_add(z_len)
                    .ok_or(IngestError::ArenaExhausted)?;
                let (c_dst, span) = arena
                    .compressed
                    .alloc_bytes(total_entry_len)
                    .map_err(|_| IngestError::ArenaExhausted)?;
                c_dst
                    .get_mut(..new_hdr_len)
                    .ok_or(IngestError::ArenaExhausted)?
                    .copy_from_slice(new_hdr.get(..new_hdr_len).unwrap_or(&[]));
                c_dst
                    .get_mut(new_hdr_len..)
                    .ok_or(IngestError::ArenaExhausted)?
                    .copy_from_slice(arena.deflate_scratch.get(..z_len).unwrap_or(&[]));
                let crc = crc32fast::hash(c_dst);
                (span, crc)
            };

            let decoded_idx = u32::try_from(arena.decoded.len()).unwrap_or(0);
            let _ = arena.oid_to_decoded.insert(oid, decoded_idx);
            arena.decoded.push(DecodedObj {
                oid,
                kind: base_kind,
                in_pack_offset,
                uncompressed: u_span,
                compressed: c_span,
                crc32,
            });
        }
    }

    // ========================================================================
    // Strict Merkle DFS Ordering (Commit -> Root Tree -> Subtrees -> Blobs)
    // Rule 2 & 3: Intraday tiers (`L0Ram..12h`) keep all objects standalone;
    // `24h` and `168h` tiers pack `OBJ_TREE` versions by directory path into
    // contiguous reverse-chronological clusters (`[Base Tree + <=15 OFS_DELTAs]`)!
    // ========================================================================
    let (out_pack, objects) = if tier.uses_tree_deltas() {
        assemble_merkle_tree_clustered_pack(arena, want_tip)?
    } else {
        order_merkle_dfs(arena, want_tip);
        assemble_standalone_merkle_pack(arena)?
    };

    // Deduplicate HFS records.
    let mut seen_hfs = SeenOidSet::with_capacity_log2(14);
    let mut hfs_sidecar = Vec::with_capacity(arena.hfs_records.len());
    for rec in &arena.hfs_records {
        if seen_hfs.insert(rec.oid) == Some(true) {
            hfs_sidecar.push(*rec);
        }
    }

    let start_ts = now_ts.saturating_sub(tier.window_secs());
    Slab::from_pack_and_objects(
        format!("slab-{}-{now_ts}", tier.as_str()),
        tier,
        start_ts,
        now_ts,
        have_tip,
        want_tip,
        objects,
        hfs_sidecar,
        Bytes::from(out_pack),
    )
    .ok_or(IngestError::SlabBuildFailed)
}

fn order_merkle_dfs(arena: &mut IngestArena, want_tip: Oid20) {
    arena.commit_indices.clear();
    if let Some(tip_idx) = arena.find_by_oid(&want_tip) {
        arena.commit_indices.push(tip_idx);
    }
    for (idx, d) in arena.decoded.iter().enumerate() {
        if d.kind == ObjType::Commit && !arena.commit_indices.contains(&idx) {
            arena.commit_indices.push(idx);
        }
    }

    for ci in 0..arena.commit_indices.len() {
        let Some(&c_idx) = arena.commit_indices.get(ci) else {
            continue;
        };
        let Some(c_obj) = arena.decoded.get(c_idx).copied() else {
            continue;
        };
        if arena.emitted.insert(c_obj.oid) != Some(true) {
            continue;
        }
        arena.ordered_indices.push(c_idx);

        let Ok(c_bytes) = arena.uncompressed.bytes(c_obj.uncompressed) else {
            continue;
        };
        let Some(info) = parse_commit(c_bytes, |_| true) else {
            continue;
        };
        arena.tree_stack.clear();
        arena.tree_stack.push(info.tree);

        while let Some(tree_oid) = arena.tree_stack.pop() {
            let Some(t_idx) = arena.find_by_oid(&tree_oid) else {
                continue;
            };
            let Some(t_obj) = arena.decoded.get(t_idx).copied() else {
                continue;
            };
            if arena.emitted.insert(t_obj.oid) != Some(true) {
                continue;
            }
            arena.ordered_indices.push(t_idx);

            arena.subtrees.clear();
            arena.blobs.clear();
            if let Ok(t_bytes) = arena.uncompressed.bytes(t_obj.uncompressed) {
                for entry_res in TreeIter::new(t_bytes) {
                    let Ok(entry) = entry_res else {
                        break;
                    };
                    if entry.is_tree() {
                        arena.subtrees.push(entry.oid);
                    } else if entry.is_blob() {
                        arena.blobs.push(entry.oid);
                    }
                }
            }
            // Emit blobs in this directory immediately after their parent tree for spatial locality!
            for bi in 0..arena.blobs.len() {
                let Some(&b_oid) = arena.blobs.get(bi) else {
                    continue;
                };
                if let Some(b_idx) = arena.find_by_oid(&b_oid) {
                    if let Some(b_obj) = arena.decoded.get(b_idx).copied() {
                        if arena.emitted.insert(b_obj.oid) == Some(true) {
                            arena.ordered_indices.push(b_idx);
                        }
                    }
                }
            }
            // Push subtrees in reverse order so the first subtree is popped next (strict pre-order DFS).
            for si in (0..arena.subtrees.len()).rev() {
                if let Some(&st_oid) = arena.subtrees.get(si) {
                    arena.tree_stack.push(st_oid);
                }
            }
        }
    }

    // Emit any remaining decoded objects (e.g. tags or standalone trees/blobs).
    for idx in 0..arena.decoded.len() {
        let Some(d) = arena.decoded.get(idx).copied() else {
            continue;
        };
        if arena.emitted.insert(d.oid) == Some(true) {
            arena.ordered_indices.push(idx);
        }
    }
}

fn assemble_standalone_merkle_pack(
    arena: &IngestArena,
) -> Result<(Vec<u8>, Vec<ObjSliceMeta>), IngestError> {
    let unique_count = u32::try_from(arena.ordered_indices.len()).unwrap_or(0);
    let total_compressed: usize = arena
        .ordered_indices
        .iter()
        .filter_map(|&idx| arena.decoded.get(idx))
        .fold(0usize, |acc, d| {
            acc.saturating_add(usize::try_from(d.compressed.len).unwrap_or(0))
        });

    let mut out_pack = Vec::with_capacity(
        PACK_HEADER_LEN
            .saturating_add(total_compressed)
            .saturating_add(PACK_TRAILER_LEN),
    );
    out_pack.extend_from_slice(&pack_header(unique_count));

    let mut objects = Vec::with_capacity(arena.ordered_indices.len());
    for &idx in &arena.ordered_indices {
        let Some(d) = arena.decoded.get(idx).copied() else {
            continue;
        };
        let raw_entry = arena
            .compressed
            .bytes(d.compressed)
            .map_err(|_| IngestError::ArenaExhausted)?;
        let offset = u64::try_from(out_pack.len()).unwrap_or(0);
        let compressed_len = u32::try_from(raw_entry.len()).unwrap_or(0);
        out_pack.extend_from_slice(raw_entry);
        objects.push(ObjSliceMeta {
            oid: d.oid,
            offset,
            compressed_len,
            crc32: d.crc32,
        });
    }

    let trailer: [u8; 20] = Sha1::digest(&out_pack).into();
    out_pack.extend_from_slice(&trailer);
    Ok((out_pack, objects))
}

/// Assembles a `24h` or `168h` pack where:
/// - `OBJ_COMMIT` and `OBJ_BLOB` (`Git-hfs 1.0` pointers) are written 100% standalone.
/// - `OBJ_TREE` objects are grouped by canonical directory path (ordered newest -> oldest
///   starting from `want_tip` down the commit parent chain) and delta-compressed into
///   contiguous `[Base OBJ_TREE (Depth=0) + <=15 OBJ_OFS_DELTAs (Depth=1)]` clusters!
fn assemble_merkle_tree_clustered_pack(
    arena: &mut IngestArena,
    want_tip: Oid20,
) -> Result<(Vec<u8>, Vec<ObjSliceMeta>), IngestError> {
    use std::collections::HashMap;

    // 1. Order commits newest -> oldest by walking parent links starting from `want_tip`,
    //    then any remaining commits sorted by descending committer timestamp.
    let mut commit_order: Vec<usize> = Vec::new();
    let mut cur = want_tip;
    while !cur.is_zero() {
        let Some(c_idx) = arena.find_by_oid(&cur) else {
            break;
        };
        if commit_order.contains(&c_idx) {
            break;
        }
        commit_order.push(c_idx);
        let mut first_parent = ZERO_OID;
        if let Some(c_obj) = arena.decoded.get(c_idx) {
            if let Ok(c_bytes) = arena.uncompressed.bytes(c_obj.uncompressed) {
                let _ = parse_commit(c_bytes, |p| {
                    if first_parent.is_zero() {
                        first_parent = p;
                    }
                    true
                });
            }
        }
        cur = first_parent;
    }

    let mut extra_commits: Vec<(u64, usize)> = Vec::new();
    for (idx, d) in arena.decoded.iter().enumerate() {
        if d.kind == ObjType::Commit && !commit_order.contains(&idx) {
            let ts = arena
                .uncompressed
                .bytes(d.uncompressed)
                .ok()
                .and_then(|b| parse_commit(b, |_| true))
                .map_or(0, |info| info.commit_time);
            extra_commits.push((ts, idx));
        }
    }
    extra_commits.sort_unstable_by_key(|a| core::cmp::Reverse(a.0));
    for (_, idx) in extra_commits {
        commit_order.push(idx);
    }

    // 2. Walk each commit's tree hierarchy in DFS order, interning each directory as a
    //    dense `path_id: usize` (`0` = root `""`) so zero path strings are cloned during the walk!
    let mut path_children: HashMap<(usize, Vec<u8>), usize> = HashMap::new();
    let mut path_trees: Vec<Vec<usize>> = vec![Vec::new()];
    let mut path_blobs: Vec<Vec<usize>> = vec![Vec::new()];

    let mut dfs_stack: Vec<(usize, Oid20)> = Vec::with_capacity(64);
    let mut child_subtrees: Vec<(usize, Oid20)> = Vec::with_capacity(64);

    for &c_idx in &commit_order {
        let Some(c_obj) = arena.decoded.get(c_idx).copied() else {
            continue;
        };
        let Ok(c_bytes) = arena.uncompressed.bytes(c_obj.uncompressed) else {
            continue;
        };
        let Some(info) = parse_commit(c_bytes, |_| true) else {
            continue;
        };
        dfs_stack.clear();
        dfs_stack.push((0usize, info.tree));

        while let Some((path_id, tree_oid)) = dfs_stack.pop() {
            let Some(t_idx) = arena.find_by_oid(&tree_oid) else {
                continue;
            };
            let Some(versions) = path_trees.get_mut(path_id) else {
                continue;
            };
            // If `t_idx` is already in `path_trees[path_id]`, this exact `(path_id, tree_oid)`
            // subtree has already been traversed!
            if versions.contains(&t_idx) {
                continue;
            }
            versions.push(t_idx);

            let Some(t_obj) = arena.decoded.get(t_idx).copied() else {
                continue;
            };
            let Ok(t_bytes) = arena.uncompressed.bytes(t_obj.uncompressed) else {
                continue;
            };
            child_subtrees.clear();
            for entry_res in TreeIter::new(t_bytes) {
                let Ok(entry) = entry_res else {
                    break;
                };
                if entry.is_tree() {
                    let child_path_id =
                        if let Some(&id) = path_children.get(&(path_id, entry.name.to_vec())) {
                            id
                        } else {
                            let new_id = path_trees.len();
                            path_trees.push(Vec::new());
                            path_blobs.push(Vec::new());
                            path_children.insert((path_id, entry.name.to_vec()), new_id);
                            new_id
                        };
                    child_subtrees.push((child_path_id, entry.oid));
                } else if entry.is_blob() {
                    if let Some(b_idx) = arena.find_by_oid(&entry.oid) {
                        if let Some(blobs) = path_blobs.get_mut(path_id) {
                            if !blobs.contains(&b_idx) {
                                blobs.push(b_idx);
                            }
                        }
                    }
                }
            }
            for &st in child_subtrees.iter().rev() {
                dfs_stack.push(st);
            }
        }
    }

    // 3. Emit into `out_pack`:
    //    (a) All standalone `OBJ_COMMIT`s (newest -> oldest)
    //    (b) For each directory `path_id` in Merkle DFS discovery order (`0..path_trees.len()`):
    //        - Contiguous reverse-chronological tree clusters (`[Base Tree + <=15 OFS_DELTAs]`)
    //        - Standalone `OBJ_BLOB`s (`Git-hfs 1.0` pointers) in that directory
    //    (c) Any remaining unvisited objects
    let mut out_pack = Vec::with_capacity(64 * 1024);
    out_pack.extend_from_slice(&pack_header(0)); // Count patched at end
    let mut objects: Vec<ObjSliceMeta> = Vec::with_capacity(arena.decoded.len());

    // (a) Standalone `OBJ_COMMIT`s
    for &c_idx in &commit_order {
        let Some(d) = arena.decoded.get(c_idx).copied() else {
            continue;
        };
        if arena.emitted.insert(d.oid) != Some(true) {
            continue;
        }
        let raw_entry = arena
            .compressed
            .bytes(d.compressed)
            .map_err(|_| IngestError::ArenaExhausted)?;
        let offset = u64::try_from(out_pack.len()).unwrap_or(0);
        let compressed_len = u32::try_from(raw_entry.len()).unwrap_or(0);
        out_pack.extend_from_slice(raw_entry);
        objects.push(ObjSliceMeta {
            oid: d.oid,
            offset,
            compressed_len,
            crc32: d.crc32,
        });
    }

    // (b) Directory path clusters (`pack_directory_history_clusters_with_deflater`) + blobs
    let mut raw_versions: Vec<RawTreeVersion> = Vec::with_capacity(32);
    for path_id in 0..path_trees.len() {
        raw_versions.clear();
        if let Some(t_indices) = path_trees.get(path_id) {
            for &t_idx in t_indices {
                let Some(d) = arena.decoded.get(t_idx).copied() else {
                    continue;
                };
                if arena.emitted.insert(d.oid) != Some(true) {
                    continue;
                }
                let raw_bytes = arena
                    .uncompressed
                    .bytes(d.uncompressed)
                    .map_err(|_| IngestError::ArenaExhausted)?
                    .to_vec();
                raw_versions.push(RawTreeVersion {
                    oid: d.oid,
                    raw_bytes,
                });
            }
        }
        if !raw_versions.is_empty() {
            let clusters = pack_directory_history_clusters_with_deflater(
                &raw_versions,
                &mut arena.deflater,
                &mut arena.deflate_scratch,
            );
            for cluster in clusters {
                let cluster_base_offset = u64::try_from(out_pack.len()).unwrap_or(0);
                out_pack.extend_from_slice(&cluster.cluster_pack_bytes);
                for m in cluster.members {
                    objects.push(ObjSliceMeta {
                        oid: m.oid,
                        offset: cluster_base_offset.saturating_add(u64::from(m.rel_offset)),
                        compressed_len: m.compressed_len,
                        crc32: m.crc32,
                    });
                }
            }
        }

        // Emit standalone `OBJ_BLOB`s in this directory
        if let Some(b_indices) = path_blobs.get(path_id) {
            for &b_idx in b_indices {
                let Some(d) = arena.decoded.get(b_idx).copied() else {
                    continue;
                };
                if arena.emitted.insert(d.oid) != Some(true) {
                    continue;
                }
                let raw_entry = arena
                    .compressed
                    .bytes(d.compressed)
                    .map_err(|_| IngestError::ArenaExhausted)?;
                let offset = u64::try_from(out_pack.len()).unwrap_or(0);
                let compressed_len = u32::try_from(raw_entry.len()).unwrap_or(0);
                out_pack.extend_from_slice(raw_entry);
                objects.push(ObjSliceMeta {
                    oid: d.oid,
                    offset,
                    compressed_len,
                    crc32: d.crc32,
                });
            }
        }
    }

    // (c) Any remaining unvisited objects
    for idx in 0..arena.decoded.len() {
        let Some(d) = arena.decoded.get(idx).copied() else {
            continue;
        };
        if arena.emitted.insert(d.oid) != Some(true) {
            continue;
        }
        if d.kind == ObjType::Tree && d.compressed.len == 0 {
            let raw_bytes = arena
                .uncompressed
                .bytes(d.uncompressed)
                .map_err(|_| IngestError::ArenaExhausted)?;
            let offset = u64::try_from(out_pack.len()).unwrap_or(0);
            gvfs_core::encode_pack_base_tree(
                raw_bytes,
                &mut arena.deflater,
                &mut arena.deflate_scratch,
                &mut out_pack,
            );
            let start = usize::try_from(offset).unwrap_or(0);
            let slice = out_pack.get(start..).unwrap_or(&[]);
            let compressed_len = u32::try_from(slice.len()).unwrap_or(0);
            let crc32 = crc32fast::hash(slice);
            objects.push(ObjSliceMeta {
                oid: d.oid,
                offset,
                compressed_len,
                crc32,
            });
            continue;
        }
        let raw_entry = arena
            .compressed
            .bytes(d.compressed)
            .map_err(|_| IngestError::ArenaExhausted)?;
        let offset = u64::try_from(out_pack.len()).unwrap_or(0);
        let compressed_len = u32::try_from(raw_entry.len()).unwrap_or(0);
        out_pack.extend_from_slice(raw_entry);
        objects.push(ObjSliceMeta {
            oid: d.oid,
            offset,
            compressed_len,
            crc32: d.crc32,
        });
    }

    let unique_count = u32::try_from(objects.len()).unwrap_or(0);
    let hdr = pack_header(unique_count);
    out_pack
        .get_mut(..PACK_HEADER_LEN)
        .ok_or(IngestError::ArenaExhausted)?
        .copy_from_slice(&hdr);
    let trailer: [u8; 20] = Sha1::digest(&out_pack).into();
    out_pack.extend_from_slice(&trailer);
    Ok((out_pack, objects))
}

/// Convenience wrapper matching the PDF's standalone signature `ingest_git_delta(have_tip, want_tip, tier, now_ts)`.
pub fn ingest_git_delta(
    upstream: &dyn GitUpstream,
    have_tip: Oid20,
    want_tip: Oid20,
    tier: Tier,
    now_ts: u64,
) -> Result<Slab, IngestError> {
    let mut arena = IngestArena::default();
    ingest_git_delta_with_arena(upstream, have_tip, want_tip, &[], tier, now_ts, &mut arena)
}

/// Alias matching the PDF's original name `ingest_ado_delta` (PDF p. 157).
#[inline]
pub fn ingest_ado_delta(
    upstream: &dyn GitUpstream,
    have_tip: Oid20,
    want_tip: Oid20,
    tier: Tier,
    now_ts: u64,
) -> Result<Slab, IngestError> {
    ingest_git_delta(upstream, have_tip, want_tip, tier, now_ts)
}
