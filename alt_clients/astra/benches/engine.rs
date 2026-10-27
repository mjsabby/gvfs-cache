use std::hint::black_box;
use std::time::Instant;

#[global_allocator]
static ALLOC: alloc_guard::SealedAllocator = alloc_guard::SealedAllocator;

fn main() {
    let pointer = b"~~Git-hfs 1.0\nblob:A04C2C773557ACE05C5E0A0DF4D2AB9829107C495067968104E75F8B0180AC9100\nsize:4167\n";
    alloc_guard::seal();
    let start = Instant::now();
    {
        let _hot = alloc_guard::hot_path();
        for _ in 0..10_000_000 {
            black_box(gvfs_daemon::hfs::parse_git_hfs_v1(black_box(pointer)));
        }
    }
    println!(
        "hfs_ns_per_object={:.2} hot_allocations={}",
        start.elapsed().as_nanos() as f64 / 10_000_000.0,
        alloc_guard::stats().hot_path
    );
}
