//! Microbenchmarks for `gvfs-core` zero-allocation primitives:
//! `cargo run --release -p gvfs-core --example microbench`
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::hint::black_box;
use std::time::Instant;

use gvfs_core::{
    MphfSlabIndex, ObjType, Oid20, format_hfs_pointer, object_id, parse_git_hfs_v1,
    parse_hfs_pointer_100b,
};

fn bench<F: FnMut(u64) -> u64>(name: &str, iters: u64, mut f: F) {
    let mut acc = 0u64;
    for i in 0..iters / 10 {
        acc = acc.wrapping_add(f(i));
    }
    let t = Instant::now();
    for i in 0..iters {
        acc = acc.wrapping_add(f(i));
    }
    let ns = (t.elapsed().as_nanos() as f64) / (iters as f64);
    println!("{name:<52} {ns:>8.2} ns/op  (checksum {})", acc & 0xFF);
}

fn main() {
    let git_hfs = b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167\n";
    bench(
        "parse_git_hfs_v1 (~~Git-hfs 1.0 VsoHash)",
        20_000_000,
        |_| black_box(parse_git_hfs_v1(black_box(git_hfs))).map_or(0, |m| m.real_size),
    );

    let pointers: Vec<[u8; 100]> = (0..1024u64)
        .map(|i| format_hfs_pointer(&[(i % 251) as u8; 32], i * 977).unwrap())
        .collect();
    bench(
        "parse_hfs_pointer_100b (SWAR 100B pointer)",
        20_000_000,
        |i| {
            let p = black_box(&pointers[(i & 1023) as usize]);
            black_box(parse_hfs_pointer_100b(p)).map_or(0, |h| h.real_size)
        },
    );

    let n = 250_000u32;
    let items: Vec<(Oid20, u64, u32)> = (0..n)
        .map(|i| {
            let oid = object_id(ObjType::Blob, &i.to_le_bytes());
            (oid, u64::from(i) * 100 + 12, 88)
        })
        .collect();
    let mphf = MphfSlabIndex::build(&items).unwrap();
    bench(
        "MphfSlabIndex::lookup_resolved (250k keys)",
        20_000_000,
        |i| {
            let (oid, _, _) =
                black_box(&items[(i.wrapping_mul(2_654_435_761) % u64::from(n)) as usize]);
            mphf.lookup_resolved(oid)
                .map_or(0, |(off, len)| off ^ u64::from(len))
        },
    );
}
