//! The git pack index (`.idx` v2) of a prefetch pack.
//!
//! VFS for Git installs a server-sent index as is (`gvfs.trust-pack-indexes`, on by
//! default) instead of running `git index-pack` over the pack: for a cold clone that is
//! minutes of client CPU saved. gvfs-helper always indexes packs itself (it identifies as
//! `git/...`), so it is not sent one.
//!
//! An index lists every entry of the pack sorted by id, with the CRC-32 of the entry's
//! bytes and its offset. A prefetch pack is slab bodies back to back, so its index is a
//! merge: each slab sorts its ids once ([`SortedSlab`], kept with the open slab), and a
//! plan's index is one pass over the slabs' sorted runs, with offsets shifted to where each
//! body lands in the pack.
//!
//! ```text
//! ff 74 4f 63 | 2 | fanout[256] | ids[n] | crc32[n] | offset32[n] | offset64[m] | pack sha-1 | sha-1
//! ```
//!
//! Integers are big-endian; an `offset32` with the top bit set indexes `offset64`.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use sha1::{Digest, Sha1};

use crate::slab::Slab;
use crate::splice::PackPlan;
use crate::util::Oid;

const MAGIC: [u8; 8] = [0xFF, b't', b'O', b'c', 0, 0, 0, 2];
const IDS_AT: usize = 8 + 256 * 4;
/// Offsets above this go to the 64-bit table (git's `off32_limit`).
const MAX_OFFSET32: u64 = 0x7FFF_FFFF;

/// A slab's entries in id order, with their CRC-32s.
pub struct SortedSlab {
    /// Slots, in id order.
    pub order: Vec<u32>,
    /// CRC-32 of each slot's entry (header and zlib stream), by slot.
    pub crc: Vec<u32>,
}

impl SortedSlab {
    /// `None` when the pack holds entries its records do not describe (a second copy of
    /// an object), which an index would have to list as well.
    pub fn build(slab: &Slab) -> Option<SortedSlab> {
        if slab.entries() != slab.count() {
            return None;
        }
        let n = slab.count() as usize;
        let mut keys: Vec<(u64, u32)> = Vec::with_capacity(n);
        let mut crc: Vec<u32> = Vec::with_capacity(n);
        for slot in 0..slab.count() {
            let rec = slab.rec(slot)?;
            let [a, b, c, d, e, f, g, h, ..] = rec.oid.0;
            keys.push((u64::from_be_bytes([a, b, c, d, e, f, g, h]), slot));
            crc.push(zlib_rs::crc32::crc32(0, slab.entry(&rec)?));
        }
        keys.sort_unstable();
        // Ids sharing their first 8 bytes (vanishingly rare): order by the whole id.
        for run in keys.chunk_by_mut(|a, b| a.0 == b.0) {
            if run.len() > 1 {
                run.sort_by_key(|k| slab.rec(k.1).map(|r| r.oid));
            }
        }
        Some(SortedSlab {
            order: keys.into_iter().map(|k| k.1).collect(),
            crc,
        })
    }
}

fn put(out: &mut [u8], at: usize, bytes: &[u8]) -> Option<()> {
    out.get_mut(at..at.checked_add(bytes.len())?)?
        .copy_from_slice(bytes);
    Some(())
}

/// The `.idx` of `plan`'s pack, whose trailer is `sha`. `None` if a slab cannot be indexed
/// from its records.
pub fn build(plan: &PackPlan, sha: &[u8; 20]) -> Option<Vec<u8>> {
    // (slab, its sorted run, shift): an entry's pack offset is its slab offset + shift.
    let mut runs: Vec<(&Slab, &SortedSlab, u64)> = Vec::with_capacity(plan.slabs.len());
    let mut shift = 0u64;
    for (s, _, len) in plan.bodies() {
        runs.push((s, s.sorted()?, shift));
        shift = shift.checked_add(len)?;
    }
    let n = plan.entries as usize;
    let crcs_at = IDS_AT.checked_add(n.checked_mul(20)?)?;
    let offs_at = crcs_at.checked_add(n.checked_mul(4)?)?;
    let mut out = vec![0u8; offs_at.checked_add(n.checked_mul(4)?)?];
    put(&mut out, 0, &MAGIC)?;
    let mut fanout = [0u32; 256];
    let mut large: Vec<u8> = Vec::new();
    // A k-way merge on (id, slab, position in the slab's run).
    let mut heap: BinaryHeap<Reverse<(Oid, usize, usize)>> = BinaryHeap::with_capacity(runs.len());
    for (k, (s, sorted, _)) in runs.iter().enumerate() {
        if let Some(&slot) = sorted.order.first() {
            heap.push(Reverse((s.rec(slot)?.oid, k, 0)));
        }
    }
    let mut i = 0usize;
    while let Some(Reverse((oid, k, pos))) = heap.pop() {
        let &(s, sorted, shift) = runs.get(k)?;
        let slot = *sorted.order.get(pos)?;
        let offset = s.rec(slot)?.offset.checked_add(shift)?;
        put(&mut out, IDS_AT.checked_add(i.checked_mul(20)?)?, &oid.0)?;
        put(
            &mut out,
            crcs_at.checked_add(i.checked_mul(4)?)?,
            &sorted.crc.get(slot as usize)?.to_be_bytes(),
        )?;
        let off32 = if offset > MAX_OFFSET32 {
            let li = u32::try_from(large.len() / 8).ok()?;
            large.extend_from_slice(&offset.to_be_bytes());
            0x8000_0000 | li
        } else {
            offset as u32
        };
        put(
            &mut out,
            offs_at.checked_add(i.checked_mul(4)?)?,
            &off32.to_be_bytes(),
        )?;
        let bucket = fanout.get_mut(usize::from(oid.0[0]))?;
        *bucket = bucket.checked_add(1)?;
        i = i.checked_add(1)?;
        let next = pos.checked_add(1)?;
        if let Some(&slot) = sorted.order.get(next) {
            heap.push(Reverse((s.rec(slot)?.oid, k, next)));
        }
    }
    if i != n {
        return None;
    }
    let mut total = 0u32;
    for (b, count) in fanout.iter().enumerate() {
        total = total.checked_add(*count)?;
        put(
            &mut out,
            8usize.checked_add(b.checked_mul(4)?)?,
            &total.to_be_bytes(),
        )?;
    }
    out.extend_from_slice(&large);
    out.extend_from_slice(sha);
    let digest: [u8; 20] = Sha1::digest(&out).into();
    out.extend_from_slice(&digest);
    Some(out)
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use crate::ingest::{Indexer, memory_slab};
    use crate::slab::Tier;
    use std::process::Command;

    /// (id, crc, offset) of every entry of an index, sorted.
    fn entries(idx: &[u8]) -> Vec<(Vec<u8>, u32, u64)> {
        let n = u32::from_be_bytes(idx[8 + 255 * 4..8 + 256 * 4].try_into().unwrap()) as usize;
        let (crcs, offs) = (IDS_AT + 20 * n, IDS_AT + 24 * n);
        let large = offs + 4 * n;
        let mut v: Vec<(Vec<u8>, u32, u64)> = (0..n)
            .map(|i| {
                let be32 = |at: usize| u32::from_be_bytes(idx[at..at + 4].try_into().unwrap());
                let o = be32(offs + 4 * i);
                let off = if o & 0x8000_0000 == 0 {
                    u64::from(o)
                } else {
                    let at = large + 8 * (o & 0x7FFF_FFFF) as usize;
                    u64::from_be_bytes(idx[at..at + 8].try_into().unwrap())
                };
                (
                    idx[IDS_AT + 20 * i..IDS_AT + 20 * i + 20].to_vec(),
                    be32(crcs + 4 * i),
                    off,
                )
            })
            .collect();
        v.sort();
        v
    }

    /// Every plan's index is byte-identical to `git index-pack`'s for the same pack.
    #[test]
    fn plan_index_matches_git() {
        let dir = std::env::temp_dir().join(format!("gvd-idx-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let pack = crate::ingest::tests::sample_pack(&dir);
        let git = |args: &[&str], stdin: &[u8]| {
            let mut cmd = Command::new("git");
            cmd.args(args)
                .current_dir(&dir)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null");
            crate::upstream::hermetic_git_env(&mut cmd);
            let mut child = cmd
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            std::io::Write::write_all(&mut child.stdin.take().unwrap(), stdin).unwrap();
            let out = child.wait_with_output().unwrap();
            assert!(out.status.success(), "git {args:?}");
            out.stdout
        };
        // Three slabs: the sample pack, and the two halves of history fetched separately.
        let revs = String::from_utf8(git(&["rev-list", "main"], b"")).unwrap();
        let commits: Vec<&str> = revs.lines().collect();
        let mid = commits[commits.len() / 2];
        let old = git(
            &[
                "pack-objects",
                "--revs",
                "--stdout",
                "-q",
                "--delta-base-offset",
            ],
            format!("{mid}\n").as_bytes(),
        );
        let new = git(
            &[
                "pack-objects",
                "--revs",
                "--stdout",
                "-q",
                "--delta-base-offset",
            ],
            format!("main\n^{mid}\n").as_bytes(),
        );
        let mut ix = Indexer::new(2);
        let mut slab = |p: Vec<u8>| {
            memory_slab(&mut ix, p, Tier::M15, (0, 1), (Oid::ZERO, Oid::ZERO))
                .unwrap()
                .slab
        };
        let (all, old, new) = (slab(pack), slab(old), slab(new));
        for slabs in [
            vec![all.clone()],
            vec![old.clone(), new.clone()],
            vec![old, new, all],
        ] {
            let plan = PackPlan::new(slabs.clone());
            let mut bytes = plan.header.to_vec();
            for (s, start, len) in plan.bodies() {
                bytes.extend_from_slice(&s.pack()[start as usize..(start + len) as usize]);
            }
            bytes.extend_from_slice(&plan.trailer());
            let p = dir.join("plan.pack");
            std::fs::write(&p, &bytes).unwrap();
            git(
                &[
                    "index-pack",
                    "-o",
                    dir.join("plan.idx").to_str().unwrap(),
                    p.to_str().unwrap(),
                ],
                b"",
            );
            let want = std::fs::read(dir.join("plan.idx")).unwrap();
            let got = plan.idx().unwrap();
            if slabs.len() == 3 {
                // Every object twice: git orders equal ids arbitrarily, so compare entries.
                assert_eq!(got.len(), want.len());
                assert_eq!(entries(got), entries(&want));
            } else {
                assert_eq!(got, &want[..], "{} slabs", slabs.len());
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
