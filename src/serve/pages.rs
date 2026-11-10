//! How much of the edge's indexes lies on 2 MiB pages.
//!
//! A lookup reads one record of an index at random: on 4 KiB pages that is a TLB miss as
//! well as a cache miss (51 ns against 34 on 2 MiB pages). Whether a file's pages are
//! mapped 2 MiB at a time is the kernel's doing ([`crate::slab::map_index`]); what it did
//! is in `/proc/self/smaps`, and the edge says it at start and in its metrics, so that an
//! operator knows which case an edge is in (and what `--index-pages huge` changed).

use super::view::View;
use crate::util::u64_of;

/// Bytes of the indexes lookups read: all of them, those read into memory
/// (`--index-pages huge`), and those on 2 MiB pages.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct IndexPages {
    pub bytes: u64,
    pub copied: u64,
    pub huge: u64,
}

/// Measures the indexes of `view`: its sealed slabs' and its blob segments'. It reads
/// `/proc/self/smaps` (milliseconds, and allocations): for the live thread, not a request.
pub fn measure(view: &View) -> IndexPages {
    let slabs = view
        .all
        .iter()
        .skip(view.l0_len)
        .map(|s| (s.index_bytes(), s.index_copied()));
    let segments = view
        .blobs
        .iter()
        .flat_map(|set| set.indexes())
        .map(|s| (s.bytes(), s.copied()));
    let mut pages = IndexPages::default();
    let mut ranges = Vec::new();
    for (bytes, copied) in slabs.chain(segments) {
        let len = u64_of(bytes.len());
        pages.bytes = pages.bytes.saturating_add(len);
        if copied {
            pages.copied = pages.copied.saturating_add(len);
        }
        ranges.push((bytes.as_ptr().addr(), bytes.len()));
    }
    let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap_or_default();
    pages.huge = huge_in(&smaps, &ranges).min(pages.bytes);
    pages
}

/// Of the mappings in `smaps` (the text of `/proc/<pid>/smaps`) that hold any of
/// `ranges` (address, length), the bytes mapped with 2 MiB entries: anonymous huge pages
/// and the page cache's (`AnonHugePages`, `ShmemPmdMapped`, `FilePmdMapped`).
fn huge_in(smaps: &str, ranges: &[(usize, usize)]) -> u64 {
    let mut total = 0u64;
    let mut ours = false;
    for line in smaps.lines() {
        let mut words = line.split_ascii_whitespace();
        let Some(first) = words.next() else { continue };
        // A mapping begins with `<start>-<end> <perms> ...`; its fields are `Name: N kB`.
        let bounds = first.split_once('-').and_then(|(start, end)| {
            let start = usize::from_str_radix(start, 16).ok()?;
            Some((start, usize::from_str_radix(end, 16).ok()?))
        });
        if let Some((start, end)) = bounds {
            ours = ranges
                .iter()
                .any(|&(at, len)| len > 0 && at < end && start < at.saturating_add(len));
            continue;
        }
        let pmd = matches!(
            first,
            "AnonHugePages:" | "ShmemPmdMapped:" | "FilePmdMapped:"
        );
        if ours
            && pmd
            && let Some(kb) = words.next().and_then(|n| n.parse::<u64>().ok())
        {
            total = total.saturating_add(kb.saturating_mul(1024));
        }
    }
    total
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;

    #[test]
    fn huge_bytes_of_the_mappings_that_hold_the_ranges() {
        let smaps = "\
7f0000000000-7f0010000000 r--p 00000000 fd:01 77 /store/slabs/base.mphf
Size:             262144 kB
Rss:              262144 kB
AnonHugePages:         0 kB
ShmemPmdMapped:        0 kB
FilePmdMapped:    229376 kB
VmFlags: rd mr mw me rr
7f0010000000-7f0010400000 r--p 00000000 00:00 0
Size:               4096 kB
AnonHugePages:      2048 kB
ShmemPmdMapped:        0 kB
FilePmdMapped:         0 kB
7f0020000000-7f0030000000 rw-p 00000000 00:00 0
Size:             262144 kB
AnonHugePages:    131072 kB
FilePmdMapped:         0 kB
";
        let base = (0x7f00_0000_0000usize, 256 << 20);
        let copy = (0x7f00_1000_0000usize, 4 << 20);
        let heap = (0x7f00_2000_0000usize, 256 << 20);
        assert_eq!(huge_in(smaps, &[base]), 229_376 << 10);
        assert_eq!(huge_in(smaps, &[copy]), 2048 << 10);
        assert_eq!(huge_in(smaps, &[base, copy]), (229_376 + 2048) << 10);
        // A range inside a mapping counts the mapping, once, however many ranges it holds.
        let inside = [(base.0 + 4096, 100), (base.0 + (8 << 20), 100)];
        assert_eq!(huge_in(smaps, &inside), 229_376 << 10);
        // Beside the mappings, or of no length: nothing.
        assert_eq!(huge_in(smaps, &[(0x7f00_1040_0000, 4096)]), 0);
        assert_eq!(huge_in(smaps, &[(base.0, 0)]), 0);
        assert_eq!(huge_in(smaps, &[]), 0);
        assert_eq!(huge_in("", &[base]), 0);
        // This process's own table reads without a surprise.
        let own = std::fs::read_to_string("/proc/self/smaps").unwrap_or_default();
        let stack = [0u8; 64];
        let _ = huge_in(&own, &[(stack.as_ptr().addr(), stack.len())]);
        assert!(huge_in(smaps, &[heap]) == 131_072 << 10);
    }
}
