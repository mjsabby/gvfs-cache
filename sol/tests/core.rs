#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod common;

use std::sync::Arc;

use gvfs_daemon::arena::reserve;
use gvfs_daemon::codec::{
    Kind, Oid, Zlib, apply_delta, entry_header, object_id, write_entry_header, write_ofs_distance,
};
use gvfs_daemon::hfs::parse_git_hfs_v1;
use gvfs_daemon::ingest::IngestArena;
use gvfs_daemon::manifest::Tier;
use gvfs_daemon::mphf::PackedObjLoc;
use gvfs_daemon::slab::{Builder, NO_BASE};
use gvfs_daemon::splice::SpliceArena;
use gvfs_daemon::tree::{TreeLayout, TreeReader, compute_tree_delta};

#[test]
fn actual_hfs_format_is_strict_and_allocation_free() {
    let hot = gvfs_platform::HotRegion::enter();
    let pointer = parse_git_hfs_v1(b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167\n").unwrap().unwrap();
    let count = hot.allocations();
    drop(hot);
    assert_eq!(count, 0);
    assert_eq!(pointer.blob_id_len, 33);
    assert_eq!(pointer.real_size, 4167);
    let pointer = parse_git_hfs_v1(b"~~Git-hfs 1.0\r\nblob:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\r\nsize:0\r\n").unwrap().unwrap();
    assert_eq!(pointer.blob_id_len, 32);
    assert_eq!(pointer.real_size, 0);
    assert_eq!(
        parse_git_hfs_v1(b"~~Git-hfs 1.0\nblob:InvalidBlobId\nsize:7269\n")
            .unwrap()
            .unwrap()
            .blob_id_len,
        0
    );
    for bad in [
        b"~~Git-hfs 1.0\nblob:InvalidBlobId\nsize:1junk\n".as_slice(),
        b"~~Git-hfs 1.0\nblob:InvalidBlobId\nsize:18446744073709551616\n",
        b"~~Git-hfs 1.0\nblob:InvalidBlobId\nsize:1\nextra",
        b"~~Git-hfs 2.0\nblob:InvalidBlobId\nsize:1\n",
        b"~~Git-hfs 1.0\nblob:no\nsize:1\n",
    ] {
        assert!(parse_git_hfs_v1(bad).is_err());
    }
    assert!(parse_git_hfs_v1(b"ordinary blob\n").unwrap().is_none());
}

#[test]
fn prefix_suffix_encoder_writes_real_git_delta_opcodes() {
    let mut base = vec![b'x'; 200000];
    for (i, byte) in base.iter_mut().enumerate() {
        *byte = (i.wrapping_mul(37) % 251) as u8;
    }
    let mut delta = reserve(400000).unwrap();
    let mut output = vec![0; 400000];
    for (offset, removed, inserted) in [(0, 0, 2), (65536, 3, 17), (100000, 10, 0), (199999, 1, 1)]
    {
        let mut target = base.clone();
        target.splice(
            offset..offset + removed,
            std::iter::repeat_n(b'z', inserted),
        );
        let hot = gvfs_platform::HotRegion::enter();
        assert!(compute_tree_delta(&base, &target, &mut delta).unwrap());
        let len = apply_delta(&base, &delta, &mut output).unwrap();
        let allocations = hot.allocations();
        drop(hot);
        assert_eq!(allocations, 0);
        assert_eq!(&output[..len], target);
    }
    assert!(apply_delta(b"x", &[1, 1, 0], &mut output).is_err());
    assert!(apply_delta(b"x", &[1, 1, 0x91, 7, 1], &mut output).is_err());
    let mut malformed = vec![0xff; 10];
    malformed.extend([0, 1, 0]);
    assert!(apply_delta(b"x", &malformed, &mut output).is_err());
}

#[test]
fn full_and_clustered_layouts_have_identical_native_objects() {
    let fixture = common::Fixture::new(22);
    let full = common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Full,
        Tier::Baseline,
        0,
        100,
        &[],
    );
    let clustered = common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Clustered,
        Tier::Baseline,
        0,
        100,
        &[],
    );
    assert_eq!(full.reference.count, clustered.reference.count);
    assert!(clustered.reference.pack_len < full.reference.pack_len);
    let mut reader = TreeReader::new(common::limits(TreeLayout::Clustered)).unwrap();
    let mut graph = gvfs_daemon::graph::Scratch::new(1 << 20).unwrap();
    let mut expected_graph = Vec::with_capacity(1 << 20);
    let mut graph_deltas = 0usize;
    let mut deltas = 0;
    for rank in 0..clustered.reference.count {
        let record = clustered.record(rank).unwrap();
        let full_rank = full.lookup(record.oid).unwrap().unwrap();
        assert_eq!(full.record(full_rank).unwrap().size, record.size);
        if record.kind == Kind::Tree {
            let len = reader.read_local(&clustered, rank).unwrap();
            expected_graph.clear();
            let mut entries = gvfs_daemon::codec::TreeEntries::new(reader.content(len).unwrap());
            while let Some(entry) = entries.next_entry().unwrap() {
                if entry.directory {
                    expected_graph.extend(entry.oid.0);
                }
            }
            let hot = gvfs_platform::HotRegion::enter();
            let children = graph.read(&clustered, record).unwrap();
            assert_eq!(children, expected_graph);
            let allocations = hot.allocations();
            drop(hot);
            assert_eq!(allocations, 0);
            if record.graph_offset != gvfs_daemon::graph::NO_GRAPH
                && gvfs_daemon::graph::View::validated(&clustered.links)
                    .node(record.graph_offset)
                    .unwrap()
                    .base
                    != NO_BASE
            {
                graph_deltas += 1;
            }
        }
        if record.base != NO_BASE {
            deltas += 1;
            assert_eq!(record.kind, Kind::Tree);
            assert!(rank - record.base <= 15);
            let base = clustered.record(record.base).unwrap();
            assert_eq!(base.base, NO_BASE);
            assert_eq!(base.path, record.path);
            let (start, span) = TreeReader::span(&clustered, rank).unwrap();
            assert_eq!(start, base.offset);
            assert!(span <= 65536);
            let len = reader.read_local(&clustered, rank).unwrap();
            assert_eq!(
                object_id(Kind::Tree, reader.content(len).unwrap()).unwrap(),
                record.oid
            );
        }
    }
    assert!(deltas >= 10);
    assert!(graph_deltas >= 10);
    for tree in &fixture.trees {
        assert!(clustered.lookup(*tree).unwrap().is_some());
    }
    // HEAD's newest version is always the full cluster head.
    assert_eq!(
        clustered
            .record(
                clustered
                    .lookup(*fixture.trees.last().unwrap())
                    .unwrap()
                    .unwrap()
            )
            .unwrap()
            .base,
        NO_BASE
    );
    common::verify_native(&full, Some(fixture.tip()));
    common::verify_native(&clustered, Some(fixture.tip()));
}

#[test]
fn every_intraday_tier_remains_undeltified_in_both_modes() {
    let fixture = common::Fixture::new(5);
    for tier in [Tier::L0, Tier::M15, Tier::H1, Tier::H6, Tier::H12] {
        let slab = common::decode(
            &fixture.pack,
            fixture.tip(),
            TreeLayout::Clustered,
            tier,
            1,
            100,
            &[],
        );
        for rank in 0..slab.reference.count {
            assert_eq!(slab.record(rank).unwrap().base, NO_BASE);
        }
    }
}

#[test]
fn perfect_hash_never_treats_a_matching_fingerprint_as_membership() {
    let limits = common::limits(TreeLayout::Full);
    let mut builder = Builder::new(limits).unwrap();
    let raw = common::full_entry(Kind::Blob, b"");
    for i in 0..256u32 {
        let mut record = common::record(Kind::Blob, b"");
        let mut key = [0; 20];
        key[..4].copy_from_slice(&i.to_be_bytes());
        key[18..].copy_from_slice(&[0xaa, 0xbb]);
        record.oid = Oid(key);
        builder.add(record, &raw).unwrap();
    }
    let slab = builder.finish(Tier::Baseline, 0, 1, Oid([1; 20])).unwrap();
    for i in 0..10000u32 {
        let mut key = [1; 20];
        key[..4].copy_from_slice(&i.to_be_bytes());
        key[18..].copy_from_slice(&[0xaa, 0xbb]);
        assert!(
            slab.lookup(Oid(key)).unwrap().is_none(),
            "every candidate fingerprint matches, but none of these full OIDs exists"
        );
    }
    let descriptor = PackedObjLoc::new(Oid([7; 20]), 1u64 << 40, 100000).unwrap();
    assert_eq!(descriptor.offset(), 0x3_ffff_ffff);
    assert!(descriptor.inline_len().is_none());
}

#[test]
fn forward_ref_deltas_and_deferred_ofs_dependents_resolve() {
    let base = vec![b'a'; 2000];
    let mut first = base.clone();
    first[100] = b'b';
    let mut second = first.clone();
    second[1000] = b'c';
    let base_oid = object_id(Kind::Blob, &base).unwrap();
    let first_oid = object_id(Kind::Blob, &first).unwrap();
    let second_oid = object_id(Kind::Blob, &second).unwrap();
    let mut delta = Vec::with_capacity(5000);
    let mut zlib = Zlib::new(1);
    let mut compressed = vec![0; 5000];
    compute_tree_delta(&base, &first, &mut delta).unwrap();
    let len = zlib.deflate(&delta, &mut compressed).unwrap();
    let mut header = [0; 10];
    let header_len = write_entry_header(Kind::RefDelta, delta.len(), &mut header).unwrap();
    let mut ref_entry = header[..header_len].to_vec();
    ref_entry.extend(base_oid.0);
    ref_entry.extend(&compressed[..len]);
    compute_tree_delta(&first, &second, &mut delta).unwrap();
    let len = zlib.deflate(&delta, &mut compressed).unwrap();
    let header_len = write_entry_header(Kind::OfsDelta, delta.len(), &mut header).unwrap();
    let mut ofs = [0; 10];
    let ofs_len = write_ofs_distance(ref_entry.len() as u64, &mut ofs).unwrap();
    let mut ofs_entry = header[..header_len].to_vec();
    ofs_entry.extend(&ofs[..ofs_len]);
    ofs_entry.extend(&compressed[..len]);
    let mut tree = Vec::new();
    for (name, oid) in [("a", first_oid), ("b", base_oid), ("c", second_oid)] {
        tree.extend(format!("100644 {name}\0").as_bytes());
        tree.extend(oid.0);
    }
    let tree_oid = object_id(Kind::Tree, &tree).unwrap();
    let commit = format!(
        "tree {tree_oid}\nauthor T <t@example.invalid> 1700000000 +0000\ncommitter T <t@example.invalid> 1700000000 +0000\n\nforward delta\n"
    );
    let tip = object_id(Kind::Commit, commit.as_bytes()).unwrap();
    let input = common::pack(&[
        ref_entry,
        ofs_entry,
        common::full_entry(Kind::Commit, commit.as_bytes()),
        common::full_entry(Kind::Tree, &tree),
        common::full_entry(Kind::Blob, &base),
    ]);
    let slab = common::decode(&input, tip, TreeLayout::Full, Tier::Baseline, 0, 1, &[]);
    assert_eq!(slab.reference.count, 5);
    common::verify_native(&slab, Some(tip));
}

#[test]
fn invalid_input_and_capacity_fail_explicitly_without_panics() {
    let fixture = common::Fixture::new(2);
    let limits = common::limits(TreeLayout::Full);
    let mut arena = IngestArena::new(limits).unwrap();
    let mut builder = Builder::new(limits).unwrap();
    for end in [0, 1, 11, 12, 20, fixture.pack.len() - 1] {
        builder.reset().unwrap();
        assert!(
            arena
                .decode(&fixture.pack[..end], fixture.tip(), &[], &mut builder)
                .is_err()
        );
    }
    let mut corrupt = fixture.pack.to_vec();
    corrupt[20] ^= 1;
    assert!(
        arena
            .decode(&corrupt, fixture.tip(), &[], &mut builder)
            .is_err()
    );
    let mut too_small = limits;
    too_small.objects = 2;
    let mut arena = IngestArena::new(too_small).unwrap();
    assert!(
        arena
            .decode(&fixture.pack, fixture.tip(), &[], &mut builder)
            .is_err()
    );
    let mut overflow = vec![0xb0];
    overflow.extend([0xff; 10]);
    assert!(entry_header(&overflow, 12).is_err());
}

#[test]
fn subset_splicing_rewrites_distances_and_deduplicates_cluster_bases() {
    let fixture = common::Fixture::new(20);
    let slab = Arc::new(common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Clustered,
        Tier::Baseline,
        0,
        100,
        &[],
    ));
    let sources = [Arc::clone(&slab), Arc::clone(&slab)];
    let mut plan = SpliceArena::new(4096, 4096).unwrap();
    let requested = [fixture.trees[2], fixture.trees[7], fixture.trees[2]];
    let hot = gvfs_platform::HotRegion::enter();
    plan.plan(&sources, Some(&requested), 4096, 65536).unwrap();
    let count = hot.allocations();
    drop(hot);
    assert_eq!(count, 0);
    let mut wire = gvfs_daemon::codec::pack_header(plan.entries.len() as u32).to_vec();
    for &selected in &plan.entries {
        let slab = &sources[selected.slab as usize];
        let record = slab.record(selected.ordinal).unwrap();
        let mut header = [0; 20];
        let (len, skip) = plan.rewrite(&sources, selected, &mut header).unwrap();
        assert_eq!(wire.len() as u64, selected.output_offset);
        wire.extend(&header[..len]);
        wire.extend(slab.raw(record).unwrap().get(skip..).unwrap());
    }
    assert_eq!(wire.len() as u64 + 20, plan.pack_len().unwrap());
    let checksum = gvfs_daemon::codec::checksum(&wire);
    wire.extend(checksum);
    let mut rows = reserve(4096).unwrap();
    let mut index = reserve(4096 * 36 + 1072).unwrap();
    let hot = gvfs_platform::HotRegion::enter();
    let index_len = plan.prepare_index(&sources, &mut rows).unwrap();
    gvfs_daemon::idx::write_sorted(&rows, Oid(checksum), &mut index).unwrap();
    let allocations = hot.allocations();
    drop(hot);
    assert_eq!(allocations, 0);
    assert_eq!(index.len(), index_len);
    let decoded = common::decode(
        &wire,
        requested[0],
        TreeLayout::Full,
        Tier::Baseline,
        0,
        1,
        &sources,
    );
    // All requested objects are already known, so the normalization is a valid empty delta.
    assert_eq!(decoded.reference.count, 0);
    let dir = tempfile::tempdir().unwrap();
    common::run(
        gvfs_daemon::upstream::git_command()
            .args(["init", "--bare", "--quiet"])
            .arg(dir.path()),
        b"",
    );
    common::run(
        common::command(dir.path()).args(["index-pack", "--stdin"]),
        &wire,
    );
    let native = dir
        .path()
        .join(format!("objects/pack/pack-{}.idx", Oid(checksum)));
    assert_eq!(index, std::fs::read(native).unwrap());
    for oid in requested {
        assert_eq!(
            common::run(
                common::command(dir.path()).args(["cat-file", "tree", &oid.to_string()]),
                b""
            )
            .stdout,
            fixture.bytes(oid)
        );
    }
}

#[test]
fn transport_future_does_not_embed_the_packet_arena() {
    let dir = tempfile::tempdir().unwrap();
    let mut git =
        gvfs_daemon::upstream::Upstream::new(".".into(), "main".into(), None, None).unwrap();
    let future = git.fetch(Oid::ZERO, Oid([1; 20]), dir.path(), 1 << 20);
    assert!(std::mem::size_of_val(&future) < 65536);
}

#[test]
fn scattered_tree_edits_copy_unchanged_middle_entries() {
    let mut base = Vec::new();
    for i in 0..200u32 {
        base.extend(format!("100644 file{i:04}\0").as_bytes());
        base.extend(object_id(Kind::Blob, &i.to_be_bytes()).unwrap().0);
    }

    let mut target = base.clone();
    let mut cursor = 0usize;
    for i in 0..200 {
        let nul = target[cursor..].iter().position(|byte| *byte == 0).unwrap() + cursor;
        if [1, 75, 198].contains(&i) {
            target[nul + 1..nul + 21].fill(i as u8);
        }
        cursor = nul + 21;
    }
    let mut delta = Vec::with_capacity(base.len() + 1024);
    let mut output = vec![0; base.len() + 1024];
    let hot = gvfs_platform::HotRegion::enter();
    assert!(compute_tree_delta(&base, &target, &mut delta).unwrap());
    let len = apply_delta(&base, &delta, &mut output).unwrap();
    let count = hot.allocations();
    drop(hot);
    assert_eq!(count, 0);
    assert_eq!(&output[..len], target);
    assert!(delta.len() < 200);
}

#[test]
fn opaque_hfs_history_is_preserved_and_strict_policy_is_explicit() {
    let content = b"~~Git-hfs 1.0\nblob:PLACEHOLDER_HASH_TO_REPLACE\nsize:123\n";
    let oid = object_id(Kind::Blob, content).unwrap();
    let input = common::pack(&[common::full_entry(Kind::Blob, content)]);
    let limits = common::limits(TreeLayout::Full);
    let mut arena = IngestArena::new(limits).unwrap();
    let mut builder = Builder::new(limits).unwrap();
    let hot = gvfs_platform::HotRegion::enter();
    arena.decode(&input, oid, &[], &mut builder).unwrap();
    let count = hot.allocations();
    drop(hot);
    assert_eq!(count, 0);
    assert_eq!(arena.diagnostics().unparsed_hfs, 1);
    assert_eq!(arena.diagnostics().first.unwrap().0, oid);
    let slab = builder.finish(Tier::Baseline, 0, 1, oid).unwrap();
    common::verify_native(&slab, None);
    let rank = slab.lookup(oid).unwrap().unwrap();
    assert!(slab.pointer(rank).unwrap().is_none());
    let mut strict = limits;
    strict.strict_hfs = true;
    let mut arena = IngestArena::new(strict).unwrap();
    let mut builder = Builder::new(strict).unwrap();
    assert!(matches!(
        arena.decode(&input, oid, &[], &mut builder),
        Err(gvfs_daemon::Error::MalformedHfs { .. })
    ));
}

#[test]
fn replacing_crc_prefix_matches_actual_bytes_even_when_header_width_changes() {
    let payload: Vec<u8> = (0..70000u32)
        .map(|i| (i.wrapping_mul(37) % 251) as u8)
        .collect();
    for size in [0, 1, 127, 4096, payload.len()] {
        for (old, new) in [
            (b"old header".as_slice(), b"x".as_slice()),
            (b"x".as_slice(), b"a much longer header".as_slice()),
            (b"".as_slice(), b"new".as_slice()),
        ] {
            let mut before = old.to_vec();
            before.extend(&payload[..size]);
            let mut after = new.to_vec();
            after.extend(&payload[..size]);
            let original = crc32fast::hash(&before);
            let expected = crc32fast::hash(&after);
            let hot = gvfs_platform::HotRegion::enter();
            let actual = gvfs_daemon::idx::replace_prefix_crc(original, old, new, size as u64);
            let allocations = hot.allocations();
            drop(hot);
            assert_eq!(allocations, 0);
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn complete_source_index_merge_matches_native_git_without_allocating() {
    let mut limits = common::limits(TreeLayout::Full);
    limits.objects = 8192;
    let mut big = Builder::new(limits).unwrap();
    let mut small = Builder::new(limits).unwrap();
    for i in 0..4112u32 {
        let bytes = i.to_be_bytes();
        let builder = if i < 4096 { &mut big } else { &mut small };
        builder
            .add(
                common::record(Kind::Blob, &bytes),
                &common::full_entry(Kind::Blob, &bytes),
            )
            .unwrap();
    }
    let big = Arc::new(big.finish(Tier::Baseline, 0, 100, Oid([1; 20])).unwrap());
    let small = Arc::new(small.finish(Tier::M15, 100, 200, Oid([2; 20])).unwrap());
    let sources = [small, big];
    let mut plan = SpliceArena::new(8192, 8192).unwrap();
    plan.plan(&sources, None, 0, 65536).unwrap();
    let mut wire = gvfs_daemon::codec::pack_header(plan.entries.len() as u32).to_vec();
    for source in &sources {
        let pack = source.pack.as_ref().unwrap();
        wire.extend(&pack[12..pack.len() - 20]);
    }
    let checksum = gvfs_daemon::codec::checksum(&wire);
    wire.extend(checksum);
    let mut rows = reserve(8192).unwrap();
    let mut index = reserve(8192 * 36 + 1072).unwrap();
    let hot = gvfs_platform::HotRegion::enter();
    let index_len = plan.prepare_index(&sources, &mut rows).unwrap();
    gvfs_daemon::idx::write_sorted(&rows, Oid(checksum), &mut index).unwrap();
    let allocations = hot.allocations();
    drop(hot);
    assert_eq!(allocations, 0);
    assert_eq!(index_len, index.len());
    let dir = tempfile::tempdir().unwrap();
    common::run(
        gvfs_daemon::upstream::git_command()
            .args(["init", "--bare", "--quiet"])
            .arg(dir.path()),
        b"",
    );
    common::run(
        common::command(dir.path()).args(["index-pack", "--stdin"]),
        &wire,
    );
    assert_eq!(
        index,
        std::fs::read(
            dir.path()
                .join(format!("objects/pack/pack-{}.idx", Oid(checksum)))
        )
        .unwrap()
    );
}

#[test]
fn cold_prefetch_includes_legacy_zero_epoch_baselines() {
    let mut builder = Builder::new(common::limits(TreeLayout::Full)).unwrap();
    let record = common::record(Kind::Blob, b"legacy");
    builder
        .add(record, &common::full_entry(Kind::Blob, b"legacy"))
        .unwrap();
    let slab = Arc::new(builder.finish(Tier::Baseline, 0, 0, record.oid).unwrap());
    slab.validate().unwrap();
    assert_eq!(slab.record(0).unwrap().epoch, 0);
    let mut plan = SpliceArena::new(4096, 4096).unwrap();
    plan.plan_after(&[slab], 0, 4096, 65536).unwrap();
    assert_eq!(plan.entries.len(), 1);
}

#[test]
fn idx_large_offsets_round_trip_and_cannot_reference_the_trailer() {
    let entries: Vec<_> = [12, 0x7fff_ffff, 0x8000_0000, 1u64 << 40]
        .into_iter()
        .enumerate()
        .map(|(i, offset)| gvfs_daemon::idx::Entry {
            oid: Oid([i as u8 + 1; 20]),
            crc: i as u32,
            offset,
        })
        .collect();
    let mut bytes = reserve(4096).unwrap();
    gvfs_daemon::idx::write_sorted(&entries, Oid([7; 20]), &mut bytes).unwrap();
    assert_eq!(bytes.len(), gvfs_daemon::idx::encoded_len(4, 2).unwrap());
    let view = gvfs_daemon::idx::View::parse(&bytes, 4).unwrap();
    for (i, expected) in entries.iter().enumerate() {
        let actual = view.entry(i).unwrap();
        assert_eq!(
            (actual.oid, actual.crc, actual.offset),
            (expected.oid, expected.crc, expected.offset)
        );
    }
    bytes[1136..1140].copy_from_slice(&0x8000_0002u32.to_be_bytes());
    assert!(
        gvfs_daemon::idx::View::parse(&bytes, 4)
            .unwrap()
            .entry(2)
            .is_err()
    );
}

#[tokio::test]
async fn weekly_rebuild_does_not_consume_the_http_range_budget() {
    let fixture = common::Fixture::new(20);
    let slab = Arc::new(common::decode(
        &fixture.pack,
        fixture.tip(),
        TreeLayout::Clustered,
        Tier::Baseline,
        0,
        100,
        &[],
    ));
    let sources = [Arc::clone(&slab)];
    let mut reader = TreeReader::new(common::limits(TreeLayout::Clustered)).unwrap();
    let mut plan = SpliceArena::new(4096, 1).unwrap();
    let hot = gvfs_platform::HotRegion::enter();
    plan.plan_merkle(&sources, fixture.tip(), &mut reader)
        .unwrap();
    let allocations = hot.allocations();
    drop(hot);
    assert_eq!(allocations, 0);
    assert_eq!(plan.entries.len(), slab.reference.count as usize);
    assert!(plan.ranges.is_empty());
    let dir = tempfile::tempdir().unwrap();
    let rebuilt = gvfs_daemon::splice::splice_slab_ranges(
        &common::storage(dir.path()),
        &sources,
        &plan,
        common::limits(TreeLayout::Clustered),
        Tier::Baseline,
        0,
        200,
        fixture.tip(),
    )
    .await
    .unwrap();
    common::verify_native(&rebuilt, Some(fixture.tip()));
}
