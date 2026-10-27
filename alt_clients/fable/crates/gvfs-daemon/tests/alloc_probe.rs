//! Allocation-freedom probes for the frame staging buffers.
#![allow(clippy::unwrap_used)]

use gvfs_daemon::serve::pool::promoted;

#[global_allocator]
static GLOBAL: gvfs_alloc_guard::SealedAllocator = gvfs_alloc_guard::SealedAllocator;

#[test]
fn promoted_bytesmut_splits_without_allocating() {
    let mut b = promoted(4096);
    gvfs_alloc_guard::seal();
    let before = gvfs_alloc_guard::stats().hot_path;
    let f = gvfs_alloc_guard::hot_path(|| {
        b.extend_from_slice(b"hello");
        b.split().freeze()
    });
    assert_eq!(&f[..], b"hello");
    drop(f);
    let f2 = gvfs_alloc_guard::hot_path(|| {
        b.extend_from_slice(&[7u8; 100]);
        b.split().freeze()
    });
    assert_eq!(f2.len(), 100);
    drop(f2);
    b.clear();
    assert!(b.try_reclaim(4096));
    let f3 = gvfs_alloc_guard::hot_path(|| {
        b.extend_from_slice(&[1u8; 4096]);
        b.split().freeze()
    });
    assert_eq!(f3.len(), 4096);
    assert_eq!(
        gvfs_alloc_guard::stats().hot_path,
        before,
        "no allocation while splitting a promoted buffer"
    );
}
