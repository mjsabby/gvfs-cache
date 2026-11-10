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
    /// CRC-32 of each entry (header and zlib stream), in id order too: a merge reads both
    /// arrays front to back.
    pub crc: Vec<u32>,
}

/// Entries one thread hashes when a slab's sorted run is built (about 30 ms of work).
const SORT_CHUNK: usize = 1 << 18;
/// Threads that share that work for a large slab.
const SORT_THREADS: usize = 8;
/// How far ahead of the entry being hashed, or merged, its successor's bytes are asked for.
const AHEAD: usize = 16;

impl SortedSlab {
    /// `None` when the pack holds entries its records do not describe (a second copy of
    /// an object), which an index would have to list as well.
    ///
    /// The CRC-32s are the work: every entry of the pack is read, in slot order, which is
    /// no order of the pack's, so each is a cache miss or several. A large slab's are
    /// hashed on [`SORT_THREADS`] threads, each asking for the entry [`AHEAD`] places on
    /// while it hashes one: SearchGold's baseline (9.25M entries, 2.4 GiB) takes 0.5 s
    /// where one thread without the hint took 3.1 s.
    pub fn build(slab: &Slab) -> Option<SortedSlab> {
        SortedSlab::build_in(slab, SORT_CHUNK)
    }

    /// [`SortedSlab::build`], with a thread for every `chunk` entries (up to
    /// [`SORT_THREADS`]).
    fn build_in(slab: &Slab, chunk: usize) -> Option<SortedSlab> {
        if slab.entries() != slab.count() {
            return None;
        }
        let n = slab.count() as usize;
        // (the id's first 8 bytes, slot, CRC-32), by slot.
        let mut keys: Vec<(u64, u32, u32)> = vec![(0, 0, 0); n];
        let threads = n.div_ceil(chunk.max(1)).clamp(1, SORT_THREADS);
        let per = n.div_ceil(threads).max(1);
        let hash = |first: usize, out: &mut [(u64, u32, u32)]| -> Option<()> {
            for (i, key) in out.iter_mut().enumerate() {
                let slot = u32::try_from(first.checked_add(i)?).ok()?;
                if let Some(next) = slab.rec(slot.saturating_add(AHEAD as u32)) {
                    crate::util::prefetch(slab.pack(), crate::util::us(next.offset));
                }
                let rec = slab.rec(slot)?;
                let [a, b, c, d, e, f, g, h, ..] = rec.oid.0;
                *key = (
                    u64::from_be_bytes([a, b, c, d, e, f, g, h]),
                    slot,
                    zlib_rs::crc32::crc32(0, slab.entry(&rec)?),
                );
            }
            Some(())
        };
        let hashed = std::thread::scope(|s| {
            let workers: Vec<_> = keys
                .chunks_mut(per)
                .enumerate()
                .map(|(i, out)| s.spawn(move || hash(i.saturating_mul(per), out)))
                .collect();
            workers
                .into_iter()
                .all(|w| w.join().ok().flatten().is_some())
        });
        if !hashed {
            return None;
        }
        keys.sort_unstable_by_key(|k| (k.0, k.1));
        // Ids sharing their first 8 bytes (vanishingly rare): order by the whole id.
        for run in keys.chunk_by_mut(|a, b| a.0 == b.0) {
            if run.len() > 1 {
                run.sort_by_key(|k| slab.rec(k.1).map(|r| r.oid));
            }
        }
        Some(SortedSlab {
            order: keys.iter().map(|k| k.1).collect(),
            crc: keys.iter().map(|k| k.2).collect(),
        })
    }
}

fn put(out: &mut [u8], at: usize, bytes: &[u8]) -> Option<()> {
    out.get_mut(at..at.checked_add(bytes.len())?)?
        .copy_from_slice(bytes);
    Some(())
}

/// One slab's part in a merge: its sorted run, where it is in it, and the entry there.
struct Run<'a> {
    slab: &'a Slab,
    sorted: &'a SortedSlab,
    /// An entry's pack offset is its slab offset plus this.
    shift: u64,
    pos: usize,
    rec: crate::slab::Rec,
}

impl Run<'_> {
    /// Moves to the run's entry at `pos`, if it has one, asking for the record [`AHEAD`]
    /// places on: records are read in id order, which is no order of theirs, so each is a
    /// cache miss, and the ones asked for ahead overlap.
    fn seek(&mut self, pos: usize) -> Option<()> {
        if let Some(&ahead) = self.sorted.order.get(pos.saturating_add(AHEAD)) {
            self.slab.preload_rec(ahead);
        }
        self.rec = self.slab.rec(*self.sorted.order.get(pos)?)?;
        self.pos = pos;
        Some(())
    }
}

/// The `.idx` of `plan`'s pack up to its tables' end: everything but the pack's trailer
/// and the index's own hash ([`finish`]), neither of which it needs, so that it can be
/// made while the trailer is hashed. `None` if a slab cannot be indexed from its records.
///
/// A merge of the slabs' sorted runs. SearchGold's cold plan (9.26M entries, a 281 MB
/// index) takes 0.5 s with its hash; it took 1.5 s reading each entry's record and CRC-32
/// where they lay, two cache misses an entry, one after the other.
pub fn merge(plan: &PackPlan) -> Option<Vec<u8>> {
    let mut runs: Vec<Run> = Vec::with_capacity(plan.slabs.len());
    // The runs that have an entry left, by that entry's id (then by slab).
    let mut heap: BinaryHeap<Reverse<(Oid, usize)>> = BinaryHeap::with_capacity(plan.slabs.len());
    let mut shift = 0u64;
    for (slab, _, len) in plan.bodies() {
        let mut run = Run {
            slab,
            sorted: slab.sorted()?,
            shift,
            pos: 0,
            rec: crate::slab::Rec::default(),
        };
        if !run.sorted.order.is_empty() {
            run.seek(0)?;
            heap.push(Reverse((run.rec.oid, runs.len())));
        }
        runs.push(run);
        shift = shift.checked_add(len)?;
    }
    let n = plan.entries as usize;
    let crcs_at = IDS_AT.checked_add(n.checked_mul(20)?)?;
    let offs_at = crcs_at.checked_add(n.checked_mul(4)?)?;
    let mut out = vec![0u8; offs_at.checked_add(n.checked_mul(4)?)?];
    put(&mut out, 0, &MAGIC)?;
    let mut fanout = [0u32; 256];
    let mut large: Vec<u8> = Vec::new();
    let mut i = 0usize;
    while let Some(mut top) = heap.peek_mut() {
        let Reverse((oid, k)) = *top;
        let run = runs.get_mut(k)?;
        let offset = run.rec.offset.checked_add(run.shift)?;
        put(&mut out, IDS_AT.checked_add(i.checked_mul(20)?)?, &oid.0)?;
        put(
            &mut out,
            crcs_at.checked_add(i.checked_mul(4)?)?,
            &run.sorted.crc.get(run.pos)?.to_be_bytes(),
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
        // The run's next entry takes this one's place at the top and sinks from there:
        // for the slab most entries come from, a comparison or two.
        let next = run.pos.checked_add(1)?;
        if next < run.sorted.order.len() {
            run.seek(next)?;
            *top = Reverse((run.rec.oid, k));
        } else {
            std::collections::binary_heap::PeekMut::pop(top);
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
    Some(out)
}

/// The `.idx` of `plan`'s pack, whose trailer is `sha`: [`merge`]'s part, then the two
/// hashes an index ends with. `None` if a slab cannot be indexed from its records.
pub fn build(plan: &PackPlan, sha: &[u8; 20]) -> Option<Vec<u8>> {
    Some(finish(merge(plan)?, sha))
}

/// Ends the index [`merge`] made with the pack's trailer and the index's own SHA-1.
pub fn finish(mut out: Vec<u8>, sha: &[u8; 20]) -> Vec<u8> {
    out.extend_from_slice(sha);
    let digest: [u8; 20] = Sha1::digest(&out).into();
    out.extend_from_slice(&digest);
    out
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
            // A slab's sorted run is the same on any number of threads (here one for every
            // five entries, up to eight): a large slab's is made on several.
            for s in &slabs {
                let (one, many) = (
                    SortedSlab::build_in(s, usize::MAX).unwrap(),
                    SortedSlab::build_in(s, 5).unwrap(),
                );
                assert!(s.count() > 40, "{} entries", s.count());
                assert!(one.order == many.order && one.crc == many.crc);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
