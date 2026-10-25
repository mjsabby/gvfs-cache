//! Contiguous Reverse-Chronological Tree-Cluster Delta Packer (PDF / Spec §3).
//!
//! Used by `24h` (Daily) and `168h` (Weekly "Aha" Delta & Weekly Full Baseline) slabs:
//! - Never delta-compresses `OBJ_BLOB` (`Git-hfs 1.0` pointers) or `OBJ_COMMIT`.
//! - Never delta-compresses intraday slabs (`L0Ram`, `15m`, `1h`, `6h`, `12h`).
//! - Groups `OBJ_TREE` versions by directory path (newest -> oldest) into contiguous clusters
//!   of `[1 Base OBJ_TREE (Depth = 0) + up to 15 OBJ_OFS_DELTAs (Depth = 1 from cluster head)]`.
//! - Computes Git binary deltas in $O(N)$ linear time without Rabin-Karp rolling hashes by
//!   exploiting the sorted structure of Git tree entries (`compute_tree_delta`).

use crate::object::TreeIter;
use crate::oid::Oid20;
use crate::pack::{ObjType, encode_entry_header, encode_ofs_distance};
use crate::zlib::{Deflater, compress_bound};

/// Maximum `OBJ_OFS_DELTA` children following a single cluster base `OBJ_TREE`.
pub const MAX_TREE_CLUSTER_DELTA_DEPTH: usize = 15;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTreeVersion {
    pub oid: Oid20,
    /// Uncompressed canonical Git tree bytes (`[mode] [name]\0[20B oid]...`).
    pub raw_bytes: Vec<u8>,
}

/// Member descriptor inside a [`ContiguousTreeCluster`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterMemberMeta {
    pub oid: Oid20,
    /// Byte offset of this entry relative to the start of `cluster_pack_bytes`.
    pub rel_offset: u32,
    /// Compressed byte length of this entry (`header + zlib_stream`).
    pub compressed_len: u32,
    /// CRC32 of the raw pack entry (`header + zlib_stream`).
    pub crc32: u32,
    /// `true` if encoded as `OBJ_OFS_DELTA` against the cluster's base tree at `rel_offset = 0`.
    pub is_ofs_delta: bool,
}

/// Represents a sealed contiguous cluster `[Base OBJ_TREE, OFS_DELTA_1, ..., OFS_DELTA_K]`
/// ready to be written sequentially into a `24h` or `168h` `.pack` slab.
#[derive(Debug, Clone)]
pub struct ContiguousTreeCluster {
    /// `(oid, rel_offset, compressed_len)` for compatibility with the spec signature.
    pub member_offsets: Vec<(Oid20, u32, u32)>,
    /// Richer per-member metadata including CRC32 and `is_ofs_delta`.
    pub members: Vec<ClusterMemberMeta>,
    /// Pre-compressed Pack v2 entries (1 Base `OBJ_TREE` + up to 15 `OBJ_OFS_DELTA`s).
    pub cluster_pack_bytes: Vec<u8>,
}

/// Packs all historical versions of a single directory path (ordered newest -> oldest)
/// into contiguous clusters of `[1 Base Tree + up to 15 OFS_DELTAs]`.
pub fn pack_directory_history_clusters(
    newest_to_oldest_trees: &[RawTreeVersion],
) -> Vec<ContiguousTreeCluster> {
    let mut deflater = Deflater::new(1);
    let mut z_scratch = Vec::with_capacity(64 * 1024);
    pack_directory_history_clusters_with_deflater(
        newest_to_oldest_trees,
        &mut deflater,
        &mut z_scratch,
    )
}

/// Arena/scratch-reusing version of [`pack_directory_history_clusters`].
pub fn pack_directory_history_clusters_with_deflater(
    newest_to_oldest_trees: &[RawTreeVersion],
    deflater: &mut Deflater,
    z_scratch: &mut Vec<u8>,
) -> Vec<ContiguousTreeCluster> {
    let mut clusters = Vec::new();

    for chunk in newest_to_oldest_trees.chunks(MAX_TREE_CLUSTER_DELTA_DEPTH.saturating_add(1)) {
        let Some(base_tree) = chunk.first() else {
            continue;
        };

        let mut cluster_bytes = Vec::with_capacity(16 * 1024);
        let mut member_offsets = Vec::with_capacity(chunk.len());
        let mut members = Vec::with_capacity(chunk.len());

        // 1. Write the newest tree in the chunk as a standalone base `OBJ_TREE` (type = 2, depth = 0)
        let base_start = 0u32;
        encode_pack_base_tree(
            &base_tree.raw_bytes,
            deflater,
            z_scratch,
            &mut cluster_bytes,
        );
        let base_len = u32::try_from(cluster_bytes.len()).unwrap_or(0);
        let base_crc = crc32fast::hash(&cluster_bytes);
        member_offsets.push((base_tree.oid, base_start, base_len));
        members.push(ClusterMemberMeta {
            oid: base_tree.oid,
            rel_offset: base_start,
            compressed_len: base_len,
            crc32: base_crc,
            is_ofs_delta: false,
        });

        // 2. Write up to 15 older versions of this exact directory as contiguous `OBJ_OFS_DELTA`s (type = 6)
        //    pointing directly back to `base_start` (depth = 1 from the cluster head!).
        for older_tree in chunk.get(1..).unwrap_or(&[]) {
            let delta_start_usize = cluster_bytes.len();
            let delta_start = u32::try_from(delta_start_usize).unwrap_or(0);
            let backward_distance = u64::from(delta_start.saturating_sub(base_start));

            let is_delta = if let Some(delta_instructions) =
                compute_tree_delta(&base_tree.raw_bytes, &older_tree.raw_bytes)
            {
                encode_pack_ofs_delta(
                    backward_distance,
                    &delta_instructions,
                    deflater,
                    z_scratch,
                    &mut cluster_bytes,
                );
                true
            } else {
                // Fallback if tree was completely rewritten (>50% changed): write as standalone OBJ_TREE
                encode_pack_base_tree(
                    &older_tree.raw_bytes,
                    deflater,
                    z_scratch,
                    &mut cluster_bytes,
                );
                false
            };

            let delta_end = u32::try_from(cluster_bytes.len()).unwrap_or(0);
            let compressed_len = delta_end.saturating_sub(delta_start);
            let crc32 = cluster_bytes
                .get(delta_start_usize..)
                .map_or(0, crc32fast::hash);
            member_offsets.push((older_tree.oid, delta_start, compressed_len));
            members.push(ClusterMemberMeta {
                oid: older_tree.oid,
                rel_offset: delta_start,
                compressed_len,
                crc32,
                is_ofs_delta: is_delta,
            });
        }

        clusters.push(ContiguousTreeCluster {
            member_offsets,
            members,
            cluster_pack_bytes: cluster_bytes,
        });
    }

    clusters
}

/// Writes a standalone `OBJ_TREE` (type = 2) Pack v2 entry (`[type_size_varint][zlib_stream]`).
pub fn encode_pack_base_tree(
    raw_tree: &[u8],
    deflater: &mut Deflater,
    z_scratch: &mut Vec<u8>,
    out: &mut Vec<u8>,
) {
    let mut hdr = [0u8; 10];
    let hdr_len = encode_entry_header(
        ObjType::Tree,
        u64::try_from(raw_tree.len()).unwrap_or(0),
        &mut hdr,
    );
    out.extend_from_slice(hdr.get(..hdr_len).unwrap_or(&[]));

    let need = compress_bound(raw_tree.len());
    if z_scratch.len() < need {
        z_scratch.resize(need, 0);
    }
    let z_len = deflater.deflate(raw_tree, z_scratch).unwrap_or(0);
    out.extend_from_slice(z_scratch.get(..z_len).unwrap_or(&[]));
}

/// Writes an `OBJ_OFS_DELTA` (type = 6) Pack v2 entry
/// (`[type_size_varint][ofs_distance_varint][zlib_delta_stream]`).
pub fn encode_pack_ofs_delta(
    backward_offset: u64,
    delta_payload: &[u8],
    deflater: &mut Deflater,
    z_scratch: &mut Vec<u8>,
    out: &mut Vec<u8>,
) {
    let mut hdr = [0u8; 10];
    let hdr_len = encode_entry_header(
        ObjType::OfsDelta,
        u64::try_from(delta_payload.len()).unwrap_or(0),
        &mut hdr,
    );
    let mut ofs_buf = [0u8; 10];
    let ofs_len = encode_ofs_distance(backward_offset, &mut ofs_buf);

    out.extend_from_slice(hdr.get(..hdr_len).unwrap_or(&[]));
    out.extend_from_slice(ofs_buf.get(..ofs_len).unwrap_or(&[]));

    let need = compress_bound(delta_payload.len());
    if z_scratch.len() < need {
        z_scratch.resize(need, 0);
    }
    let z_len = deflater.deflate(delta_payload, z_scratch).unwrap_or(0);
    out.extend_from_slice(z_scratch.get(..z_len).unwrap_or(&[]));
}

/// Computes a valid Git binary delta stream transforming `base` -> `target` for `OBJ_TREE`
/// objects in $O(N)$ linear time without Rabin-Karp rolling hashes:
///
/// 1. **Fast Path (Single-Span Prefix + Suffix Scan)**:
///    Scans common leading and trailing bytes (`common_prefix` and `common_suffix`).
///    When 1 file (or adjacent files) in a directory changed, `target_mid` is tiny ($\le 64$ B)
///    and we immediately emit `[COPY prefix] + [INSERT target_mid] + [COPY suffix]` at 10+ GB/s.
/// 2. **Multi-Edit Sorted-Entry Merge Path**:
///    When 2+ non-adjacent files in the same directory changed across a commit window, walks
///    the remaining middle slice entry-by-entry in lockstep (since both `base` and `target`
///    are sorted by filename!) to copy every unchanged intermediate span of entries as well.
pub fn compute_tree_delta(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    if base.len() < 32 || target.is_empty() {
        return None;
    }

    let common_prefix = base
        .iter()
        .zip(target.iter())
        .take_while(|(a, b)| a == b)
        .count();
    let base_rem = base.get(common_prefix..)?;
    let target_rem = target.get(common_prefix..)?;
    let common_suffix = base_rem
        .iter()
        .rev()
        .zip(target_rem.iter().rev())
        .take_while(|(a, b)| a == b)
        .count();

    let mut delta = Vec::with_capacity(128);
    encode_delta_varint(u64::try_from(base.len()).ok()?, &mut delta);
    encode_delta_varint(u64::try_from(target.len()).ok()?, &mut delta);

    let target_mid_end = target.len().checked_sub(common_suffix)?;
    let target_mid = target.get(common_prefix..target_mid_end)?;

    // Fast path: 0 or 1–2 adjacent entries changed (`target_mid.len() <= 80`)
    if target_mid.len() <= 80 {
        if common_prefix.saturating_add(common_suffix) < base.len().wrapping_shr(1) {
            return None;
        }
        if common_prefix > 0 {
            emit_copy(0, common_prefix, &mut delta);
        }
        if !target_mid.is_empty() {
            emit_insert(target_mid, &mut delta);
        }
        if common_suffix > 0 {
            let suffix_start = base.len().checked_sub(common_suffix)?;
            emit_copy(suffix_start, common_suffix, &mut delta);
        }
        return Some(delta);
    }

    // Multi-edit lockstep walk over canonical sorted Git tree entries (`[mode] [name]\0[20B oid]`):
    // Captures all unchanged spans between multiple modified files in the same directory!
    let base_spans = collect_tree_entry_spans(base)?;
    let target_spans = collect_tree_entry_spans(target)?;

    let mut b_idx = 0usize;
    let mut t_idx = 0usize;
    let mut total_copied = 0usize;

    // Pending operation state so adjacent matching entries coalesce into 1 single COPY opcode,
    // and adjacent modified entries coalesce into 1 single INSERT opcode.
    let mut pending_copy: Option<(usize, usize)> = None; // (base_start, len)
    let mut pending_insert: Option<(usize, usize)> = None; // (target_start, len)

    while t_idx < target_spans.len() {
        let &(t_start, t_end, t_is_tree, t_name) = target_spans.get(t_idx)?;
        let t_entry_bytes = target.get(t_start..t_end)?;

        // Advance `b_idx` while `base[b_idx]` sorts strictly before `target[t_idx]`.
        while let Some(&(_, _, b_is_tree, b_name)) = base_spans.get(b_idx) {
            if tree_name_cmp(b_name, b_is_tree, t_name, t_is_tree).is_lt() {
                b_idx = b_idx.saturating_add(1);
            } else {
                break;
            }
        }

        if let Some(&(b_start, b_end, b_is_tree, b_name)) = base_spans.get(b_idx) {
            let b_entry_bytes = base.get(b_start..b_end)?;
            if b_entry_bytes == t_entry_bytes {
                // Entire entry (including 20-byte OID) is identical!
                if let Some((ins_s, ins_l)) = pending_insert.take() {
                    emit_insert(target.get(ins_s..ins_s.saturating_add(ins_l))?, &mut delta);
                }
                let entry_len = b_end.saturating_sub(b_start);
                total_copied = total_copied.saturating_add(entry_len);
                match pending_copy.as_mut() {
                    Some((cp_s, cp_l)) if cp_s.saturating_add(*cp_l) == b_start => {
                        *cp_l = cp_l.saturating_add(entry_len);
                    }
                    Some((cp_s, cp_l)) => {
                        emit_copy(*cp_s, *cp_l, &mut delta);
                        *cp_s = b_start;
                        *cp_l = entry_len;
                    }
                    None => {
                        pending_copy = Some((b_start, entry_len));
                    }
                }
                b_idx = b_idx.saturating_add(1);
                t_idx = t_idx.saturating_add(1);
                continue;
            } else if b_is_tree == t_is_tree && b_name == t_name {
                // Same filename & mode, only the 20-byte OID changed!
                b_idx = b_idx.saturating_add(1);
            }
        }

        // Entry was modified or inserted: append to `pending_insert`.
        if let Some((cp_s, cp_l)) = pending_copy.take() {
            emit_copy(cp_s, cp_l, &mut delta);
        }
        let entry_len = t_end.saturating_sub(t_start);
        match pending_insert.as_mut() {
            Some((ins_s, ins_l)) if ins_s.saturating_add(*ins_l) == t_start => {
                *ins_l = ins_l.saturating_add(entry_len);
            }
            Some((ins_s, ins_l)) => {
                emit_insert(
                    target.get(*ins_s..ins_s.saturating_add(*ins_l))?,
                    &mut delta,
                );
                *ins_s = t_start;
                *ins_l = entry_len;
            }
            None => {
                pending_insert = Some((t_start, entry_len));
            }
        }
        t_idx = t_idx.saturating_add(1);
    }

    if let Some((cp_s, cp_l)) = pending_copy.take() {
        emit_copy(cp_s, cp_l, &mut delta);
    }
    if let Some((ins_s, ins_l)) = pending_insert.take() {
        emit_insert(target.get(ins_s..ins_s.saturating_add(ins_l))?, &mut delta);
    }

    if total_copied < base.len().wrapping_shr(1) {
        return None;
    }
    Some(delta)
}

type TreeSpan<'a> = (usize, usize, bool, &'a [u8]);

fn collect_tree_entry_spans(raw: &[u8]) -> Option<Vec<TreeSpan<'_>>> {
    let mut out = Vec::with_capacity(raw.len().wrapping_shr(5).max(8));
    let mut cursor = 0usize;
    for item in TreeIter::new(raw) {
        let entry = item.ok()?;
        let rest = raw.get(cursor..)?;
        let nul = rest.iter().position(|&c| c == 0)?;
        let entry_len = nul.checked_add(21)?;
        let end = cursor.checked_add(entry_len)?;
        out.push((cursor, end, entry.is_tree(), entry.name));
        cursor = end;
    }
    Some(out)
}

fn tree_name_cmp(
    a_name: &[u8],
    a_is_tree: bool,
    b_name: &[u8],
    b_is_tree: bool,
) -> core::cmp::Ordering {
    let common = a_name.len().min(b_name.len());
    let ap = a_name.get(..common).unwrap_or(&[]);
    let bp = b_name.get(..common).unwrap_or(&[]);
    match ap.cmp(bp) {
        core::cmp::Ordering::Equal => {}
        other => return other,
    }
    let ac = a_name
        .get(common)
        .copied()
        .unwrap_or(if a_is_tree { b'/' } else { 0 });
    let bc = b_name
        .get(common)
        .copied()
        .unwrap_or(if b_is_tree { b'/' } else { 0 });
    ac.cmp(&bc)
}

fn encode_delta_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let mut b = u8::try_from(v & 0x7F).unwrap_or(0);
        v = v.wrapping_shr(7);
        if v != 0 {
            b |= 0x80;
        }
        out.push(b);
        if v == 0 {
            break;
        }
    }
}

fn emit_copy(mut offset: usize, mut len: usize, out: &mut Vec<u8>) {
    while len > 0 {
        let chunk = len.min(0x10000);
        let mut op = 0x80u8;
        let mut params = [0u8; 7];
        let mut p_len = 0usize;

        let off_u32 = u32::try_from(offset).unwrap_or(0);
        for (bit, &b) in off_u32.to_le_bytes().iter().enumerate() {
            if b != 0 {
                op |= 1u8.wrapping_shl(u32::try_from(bit).unwrap_or(0));
                if let Some(slot) = params.get_mut(p_len) {
                    *slot = b;
                }
                p_len = p_len.saturating_add(1);
            }
        }

        // In Git's delta format, size == 0x10000 is encoded by omitting all 3 size bytes (`size == 0`).
        if chunk != 0x10000 {
            let sz_u32 = u32::try_from(chunk).unwrap_or(0);
            for (bit, &b) in sz_u32
                .to_le_bytes()
                .get(..3)
                .unwrap_or(&[])
                .iter()
                .enumerate()
            {
                if b != 0 {
                    op |= 0x10u8.wrapping_shl(u32::try_from(bit).unwrap_or(0));
                    if let Some(slot) = params.get_mut(p_len) {
                        *slot = b;
                    }
                    p_len = p_len.saturating_add(1);
                }
            }
        }

        out.push(op);
        out.extend_from_slice(params.get(..p_len).unwrap_or(&[]));
        offset = offset.saturating_add(chunk);
        len = len.saturating_sub(chunk);
    }
}

fn emit_insert(data: &[u8], out: &mut Vec<u8>) {
    for chunk in data.chunks(0x7F) {
        if let Ok(len_u8) = u8::try_from(chunk.len()) {
            if len_u8 > 0 {
                out.push(len_u8);
                out.extend_from_slice(chunk);
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use crate::delta::apply_delta;

    fn make_tree(num_files: usize, modified_indices: &[(usize, u8)]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(num_files * 36);
        for i in 0..num_files {
            buf.extend_from_slice(format!("100644 file_{i:04}.txt\0").as_bytes());
            let mut oid = [u8::try_from(i & 0xFF).unwrap_or(0); 20];
            for &(m_idx, seed) in modified_indices {
                if m_idx == i {
                    oid = [seed; 20];
                }
            }
            buf.extend_from_slice(&oid);
        }
        buf
    }

    #[test]
    fn single_and_multi_file_tree_deltas_roundtrip_via_apply_delta() {
        // 400-file directory (~14.8 KiB)
        let base_tree = make_tree(400, &[]);

        // Case 1: 1 file modified in the middle -> Fast prefix/suffix path
        let one_changed = make_tree(400, &[(200, 0xAA)]);
        let delta1 = compute_tree_delta(&base_tree, &one_changed).unwrap();
        assert!(
            delta1.len() < 45,
            "1-file tree delta should be < 45 bytes, got {}",
            delta1.len()
        );
        let mut out1 = vec![0u8; one_changed.len()];
        let n1 = apply_delta(&base_tree, &delta1, &mut out1).unwrap();
        assert_eq!(&out1[..n1], &one_changed[..]);

        // Case 2: 3 non-adjacent files modified (near start, middle, and end) -> Lockstep entry path
        let three_changed = make_tree(400, &[(5, 0xBB), (200, 0xCC), (390, 0xDD)]);
        let delta3 = compute_tree_delta(&base_tree, &three_changed).unwrap();
        assert!(
            delta3.len() < 160,
            "3-file tree delta should be < 160 bytes, got {}",
            delta3.len()
        );
        let mut out3 = vec![0u8; three_changed.len()];
        let n3 = apply_delta(&base_tree, &delta3, &mut out3).unwrap();
        assert_eq!(&out3[..n3], &three_changed[..]);

        // Case 3: Contiguous cluster packing of 16 versions (1 base + 15 OFS_DELTAs)
        let versions: Vec<RawTreeVersion> = (0..16u8)
            .map(|v| {
                let raw = make_tree(400, &[(usize::from(v) * 20, v.wrapping_add(1))]);
                let oid = crate::object_id(ObjType::Tree, &raw);
                RawTreeVersion {
                    oid,
                    raw_bytes: raw,
                }
            })
            .collect();

        let clusters = pack_directory_history_clusters(&versions);
        assert_eq!(clusters.len(), 1);
        let c = &clusters[0];
        assert_eq!(c.members.len(), 16);
        assert!(!c.members[0].is_ofs_delta);
        for m in &c.members[1..] {
            assert!(m.is_ofs_delta);
            assert!(
                m.compressed_len < 120,
                "compressed OFS_DELTA tree entry should be < 120B, got {}",
                m.compressed_len
            );
        }
    }
}
