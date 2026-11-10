//! A new baseline as the one before it and a tail.
//!
//! The weekly run writes the new baseline as the old one, byte for byte, plus the week's
//! objects ([`crate::repack`], `frozen`): the new pack is a header with the new count, the
//! old pack's entries, the week's entries, and a trailer. An edge of an object store
//! downloaded it whole all the same: 2.6 GB for SearchGold, of which the week's entries
//! were 0.5 to 2.2 MiB; per edge, and per agent VM where the edge is the localhost proxy.
//!
//! So the weekly run also publishes the tail (`slabs/<id>.tail`): what the new pack holds
//! after the old one's entries, with the old baseline's id, length and trailer. An edge
//! that has the old pack makes the new one of the two ([`grow`]) and checks it as it
//! checks any pack it downloads: by its length, and its SHA-1 against the one the
//! manifest lists, which proves it byte for byte. An edge without the old pack, or a
//! store without the tail (one written by an older build), downloads the pack as before.
//!
//! The index is made the same way. The new pack's index is 40 bytes an object in the
//! slots a table of all the ids gives them (375 MB for SearchGold's 9.25M objects),
//! and after the pack it was all an edge still downloaded for a new baseline. It holds
//! nothing the edge lacks but the week's objects: the old ones are where they were. So
//! the tail carries the new index's records of the objects after the old pack's (3,720
//! of them, 149 KB, for a week of SearchGold), and the edge makes the index of those
//! and the old index ([`grow_index`], [`crate::slab::grow_index`]): the table again over
//! all the ids, the old records moved to their new slots. That is the index the weekly
//! run made whenever the table is a function of the ids, which the run has checked by
//! making it so itself before it publishes the records ([`index_tail`]); the edge checks
//! what it made by its length and by the SHA-256 the tail gives. Where either check
//! fails, the edge downloads the index as before.
//!
//! ```text
//! "GVDTAIL1" | old id length u16 | old id | old pack length u64 | old pack trailer [20]
//! | new pack length u64 | new pack entries u32
//! | new index length u64 | new index SHA-256 [32] | records u32   (all zero: no records)
//! | new pack bytes from (old pack length - 20) to its end
//! | records x 40 bytes: the new index's, of the objects after the old pack's, by offset
//! ```

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::pack::{PACK_HEADER_LEN, PACK_TRAILER_LEN, pack_header};
use crate::slab::{IndexHeader, REC_LEN, SlabMeta};
use crate::util::{Context, Result, le_u16, le_u32, le_u64, u64_of, us};

const MAGIC: &[u8; 8] = b"GVDTAIL1";

/// What a tail continues, and what the two make.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    pub old_id: String,
    pub old_len: u64,
    pub old_sha: [u8; 20],
    pub new_len: u64,
    pub entries: u32,
    /// The new index's length and SHA-256, and how many of its records the tail holds
    /// (the objects after the old pack's). All zero: none, the index is downloaded.
    pub index_len: u64,
    pub index_sha: [u8; 32],
    pub records: u32,
}

/// What a tail says of the new index: [`index_tail`].
pub struct IndexTail {
    pub len: u64,
    pub sha: [u8; 32],
    /// The new index's records of the objects after the old pack's, in the order of
    /// their entries.
    pub records: Vec<u8>,
}

impl Head {
    fn encode(&self) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        let id = self.old_id.as_bytes();
        out.extend_from_slice(&u16::try_from(id.len()).unwrap_or(u16::MAX).to_le_bytes());
        out.extend_from_slice(id);
        out.extend_from_slice(&self.old_len.to_le_bytes());
        out.extend_from_slice(&self.old_sha);
        out.extend_from_slice(&self.new_len.to_le_bytes());
        out.extend_from_slice(&self.entries.to_le_bytes());
        out.extend_from_slice(&self.index_len.to_le_bytes());
        out.extend_from_slice(&self.index_sha);
        out.extend_from_slice(&self.records.to_le_bytes());
        out
    }

    /// The head at the start of `bytes`.
    fn decode(bytes: &[u8]) -> Result<Head> {
        if bytes.get(..8) != Some(&MAGIC[..]) {
            crate::bail!("not a baseline tail");
        }
        let id_len = usize::from(le_u16(bytes, 8).ctx("tail head")?);
        let at = 10usize.saturating_add(id_len);
        let id = bytes.get(10..at).ctx("tail head")?;
        let old_id = std::str::from_utf8(id).ok().ctx("tail head: the old id")?;
        if !SlabMeta::is_id(old_id) {
            crate::bail!("tail head: {old_id:?} is no slab id");
        }
        let old_sha: [u8; 20] = bytes
            .get(at.saturating_add(8)..at.saturating_add(28))
            .and_then(|s| s.try_into().ok())
            .ctx("tail head")?;
        let index_sha: [u8; 32] = bytes
            .get(at.saturating_add(48)..at.saturating_add(80))
            .and_then(|s| s.try_into().ok())
            .ctx("tail head")?;
        Ok(Head {
            old_id: old_id.to_string(),
            old_len: le_u64(bytes, at).ctx("tail head")?,
            old_sha,
            new_len: le_u64(bytes, at.saturating_add(28)).ctx("tail head")?,
            entries: le_u32(bytes, at.saturating_add(36)).ctx("tail head")?,
            index_len: le_u64(bytes, at.saturating_add(40)).ctx("tail head")?,
            index_sha,
            records: le_u32(bytes, at.saturating_add(80)).ctx("tail head")?,
        })
    }

    /// The head's own length in a tail.
    fn len(&self) -> u64 {
        u64_of(self.old_id.len()).saturating_add(94)
    }

    /// Where the old pack's entries end, in both packs.
    fn joint(&self) -> Option<u64> {
        self.old_len
            .checked_sub(u64_of(PACK_TRAILER_LEN))
            .filter(|&joint| joint >= u64_of(PACK_HEADER_LEN) && joint <= self.new_len)
    }

    /// Where the records begin in the tail, and where the tail ends.
    fn records_at(&self) -> Option<(u64, u64)> {
        let pack = self.new_len.checked_sub(self.joint()?)?;
        let at = self.len().checked_add(pack)?;
        let records = u64::from(self.records).checked_mul(u64_of(REC_LEN))?;
        Some((at, at.checked_add(records)?))
    }
}

/// Bytes `from..to` of `src`, appended to `dst`.
fn copy_range(src: &mut File, dst: &mut File, from: u64, to: u64) -> Result<()> {
    let len = to.saturating_sub(from);
    src.seek(SeekFrom::Start(from))?;
    if std::io::copy(&mut src.take(len), dst)? != len {
        crate::bail!("a file is shorter than its length");
    }
    Ok(())
}

/// Writes at `out` the tail of baseline `new`, whose pack is the file `new_pack`, on
/// `old`, whose pack is `old_pack`; with `index`, what it says of the new index
/// ([`index_tail`]). `Ok(false)`, and no file, if the new pack is not the old one and
/// more (it always is, as the weekly run writes it: checked here, byte for byte, since a
/// tail that is wrong costs every edge a copy and a hash before it downloads the pack
/// after all).
pub fn write(
    old: &SlabMeta,
    old_pack: &[u8],
    new: &SlabMeta,
    new_pack: &Path,
    index: Option<&IndexTail>,
    out: &Path,
) -> Result<bool> {
    let mut pack = File::open(new_pack).ctx(&format!("opening {}", new_pack.display()))?;
    let mut header = [0u8; PACK_HEADER_LEN];
    pack.read_exact(&mut header)?;
    let entries = header_entries(&header);
    let records = index.map_or(&[][..], |i| &i.records);
    let head = Head {
        old_id: old.id.clone(),
        old_len: u64_of(old_pack.len()),
        old_sha: old.pack_sha,
        new_len: pack.metadata()?.len(),
        entries,
        index_len: index.map_or(0, |i| i.len),
        index_sha: index.map_or([0; 32], |i| i.sha),
        records: crate::util::u32_of(records.len().checked_div(REC_LEN).unwrap_or(0)),
    };
    let Some(joint) = head.joint() else {
        return Ok(false);
    };
    if head.new_len != new.pack_len || head.old_len != old.pack_len {
        return Ok(false);
    }
    // The old pack's entries, where the new pack must have them.
    let mut piece = vec![0u8; 8 << 20];
    let body = old_pack.get(PACK_HEADER_LEN..us(joint)).unwrap_or(&[]);
    for theirs in body.chunks(piece.len()) {
        let ours = piece.get_mut(..theirs.len()).unwrap_or(&mut []);
        pack.read_exact(ours)?;
        if ours != theirs {
            return Ok(false);
        }
    }
    let tmp = crate::fsutil::temp_path(out);
    let written = (|| -> Result<()> {
        let mut file = File::create(&tmp)?;
        file.write_all(&head.encode())?;
        copy_range(&mut pack, &mut file, joint, head.new_len)?;
        file.write_all(records)?;
        file.sync_all()?;
        std::fs::rename(&tmp, out)?;
        Ok(())
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    written.ctx(&format!("writing {}", out.display()))?;
    Ok(true)
}

/// What the tail of a new baseline says of its index, the file `new_index`, where the
/// old baseline's index is `old_index` and its pack `old_pack_len` bytes: the new
/// index's records of the objects after the old pack's, and the length and SHA-256 of
/// the whole. `None` where an edge could not make the new index of the old one and
/// those records: a pack that holds an object twice, or a table whose seed came of the
/// ids' order ([`crate::mphf::build_from`]). That is not reasoned about but tried: the
/// index is made here as an edge makes it ([`crate::slab::grow_index`], at `scratch`,
/// removed again), and must come out as the file, to the byte.
pub fn index_tail(
    old_index: &[u8],
    old_pack_len: u64,
    new: &SlabMeta,
    new_index: &Path,
    threads: usize,
    scratch: &Path,
) -> Result<Option<IndexTail>> {
    let file = File::open(new_index).ctx(&format!("opening {}", new_index.display()))?;
    let map = gvfs_mmap::Map::open(&file, false).ctx("mapping the new index")?;
    let ix = map.bytes();
    let was = IndexHeader::parse(old_index, "the old baseline's index")?;
    let now = IndexHeader::parse(ix, "the new baseline's index")?;
    let joint = old_pack_len.saturating_sub(u64_of(PACK_TRAILER_LEN));
    if was.count != was.entries || now.count != now.entries || now.pack_len != new.pack_len {
        return Ok(None);
    }
    // `chunks_exact`, not the `as_chunks` clippy prefers
    // (https://github.com/rust-lang/rust/issues/165050).
    #[allow(clippy::chunks_exact_to_as_chunks)]
    let mut after: Vec<(u64, &[u8])> = now
        .records(ix)
        .chunks_exact(REC_LEN)
        .filter_map(|rec| {
            let at = crate::slab::record_offset(rec)?;
            (at >= joint).then_some((at, rec))
        })
        .collect();
    if u64::from(was.count).saturating_add(u64_of(after.len())) != u64::from(now.count) {
        return Ok(None);
    }
    after.sort_unstable_by_key(|&(at, _)| at);
    let mut records = Vec::with_capacity(after.len().saturating_mul(REC_LEN));
    for (_, rec) in after {
        records.extend_from_slice(rec);
    }
    let sha: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, ix)
        .as_ref()
        .try_into()
        .ok()
        .ctx("a SHA-256")?;
    let made = crate::slab::grow_index(
        old_index,
        &records,
        now.entries,
        now.pack_len,
        &now.pack_sha,
        threads,
        scratch,
    );
    let _ = std::fs::remove_file(scratch);
    let (made_sha, made_len) = made?;
    if made_sha != sha || made_len != u64_of(ix.len()) {
        return Ok(None);
    }
    Ok(Some(IndexTail {
        len: made_len,
        sha,
        records,
    }))
}

/// The count in a pack header.
fn header_entries(header: &[u8; PACK_HEADER_LEN]) -> u32 {
    let [.., a, b, c, d] = *header;
    u32::from_be_bytes([a, b, c, d])
}

/// The head of the tail in `file`.
fn read_head(file: &mut File) -> Result<Head> {
    let mut start = vec![0u8; 4096];
    let mut got = 0usize;
    while got < start.len() {
        match file.read(start.get_mut(got..).unwrap_or(&mut []))? {
            0 => break,
            n => got = got.saturating_add(n),
        }
    }
    Head::decode(start.get(..got).unwrap_or(&[]))
}

/// Makes the pack of baseline `new` at `out`, of the tail at `tail` and the pack it
/// continues, which `old_pack` finds by its id (`None`: not here). Returns what the tail
/// says of itself; `None`, and no file, if this is not the tail of `new`, or the old pack
/// is not here or is another. What is made has the right length; its SHA-1 is for the
/// caller to check.
pub fn grow(
    tail: &Path,
    old_pack: impl Fn(&str) -> Option<std::path::PathBuf>,
    new: &SlabMeta,
    out: &Path,
) -> Result<Option<Head>> {
    let mut tail_file = File::open(tail).ctx(&format!("opening {}", tail.display()))?;
    let head = read_head(&mut tail_file)?;
    let tail_len = tail_file.metadata()?.len();
    let (Some(joint), Some((records_at, end))) = (head.joint(), head.records_at()) else {
        crate::bail!("tail head: lengths that cannot be");
    };
    if head.new_len != new.pack_len || tail_len != end {
        return Ok(None);
    }
    let Some(old_path) = old_pack(&head.old_id) else {
        return Ok(None);
    };
    let Ok(mut old) = File::open(&old_path) else {
        return Ok(None);
    };
    // The pack the tail continues: by its length and its trailer.
    let mut trailer = [0u8; PACK_TRAILER_LEN];
    if old.metadata()?.len() != head.old_len
        || old.seek(SeekFrom::Start(joint)).is_err()
        || old.read_exact(&mut trailer).is_err()
        || trailer != head.old_sha
    {
        return Ok(None);
    }
    let tmp = crate::fsutil::temp_path(out);
    let made = (|| -> Result<()> {
        let mut file = File::create(&tmp)?;
        file.write_all(&pack_header(head.entries))?;
        // The middle on block boundaries of both files, so that a file system that can
        // share blocks between files copies none.
        let header = u64_of(PACK_HEADER_LEN);
        let (a, b) = (4096u64.min(joint), (joint / 4096).saturating_mul(4096));
        if a < b {
            copy_range(&mut old, &mut file, header, a)?;
            copy_range(&mut old, &mut file, a, b)?;
            copy_range(&mut old, &mut file, b, joint)?;
        } else {
            copy_range(&mut old, &mut file, header, joint)?;
        }
        copy_range(&mut tail_file, &mut file, head.len(), records_at)?;
        // As a download is: on disk before it has its name.
        file.sync_all()?;
        std::fs::rename(&tmp, out)?;
        Ok(())
    })();
    if made.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    made.ctx(&format!("making {}", out.display()))?;
    Ok(Some(head))
}

/// Makes the index of baseline `new` at `out`, of the records in the tail at `tail`
/// (whose head is `head`) and the index of the baseline it continues, the file
/// `old_index`. False, and no file, if the tail holds no records, the old index is not
/// here or is another pack's, or what comes out is not the index the tail says (its
/// length, which is also the manifest's, and its SHA-256): the caller downloads it then.
pub fn grow_index(
    tail: &Path,
    head: &Head,
    old_index: &Path,
    new: &SlabMeta,
    threads: usize,
    out: &Path,
) -> Result<bool> {
    let Some((records_at, end)) = head.records_at() else {
        return Ok(false);
    };
    if head.records == 0 || head.index_len != new.index_len {
        return Ok(false);
    }
    let Ok(old_file) = File::open(old_index) else {
        return Ok(false);
    };
    let old = gvfs_mmap::Map::open(&old_file, false).ctx("mapping the old index")?;
    let was = IndexHeader::parse(old.bytes(), "the old baseline's index")?;
    if was.pack_len != head.old_len || was.pack_sha != head.old_sha {
        return Ok(false);
    }
    let mut tail_file = File::open(tail).ctx(&format!("opening {}", tail.display()))?;
    if tail_file.metadata()?.len() != end {
        return Ok(false);
    }
    let mut records = vec![0u8; us(end.saturating_sub(records_at))];
    tail_file.seek(SeekFrom::Start(records_at))?;
    tail_file.read_exact(&mut records)?;
    // Under another name until it is known to be the index.
    let made_at = crate::fsutil::temp_path(out);
    let made = crate::slab::grow_index(
        old.bytes(),
        &records,
        head.entries,
        head.new_len,
        &new.pack_sha,
        threads,
        &made_at,
    );
    let is_it = matches!(&made, Ok((sha, len)) if *sha == head.index_sha && *len == head.index_len);
    if is_it {
        std::fs::rename(&made_at, out).ctx(&format!("making {}", out.display()))?;
    } else {
        let _ = std::fs::remove_file(&made_at);
    }
    made.map(|_| is_it)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use crate::slab::Tier;
    use crate::util::Oid;

    /// A pack of `entries` whose body is `body`: header, body, SHA-1.
    fn pack(entries: u32, body: &[u8]) -> Vec<u8> {
        let mut p = pack_header(entries).to_vec();
        p.extend_from_slice(body);
        let sha: [u8; 20] = <sha1::Sha1 as sha1::Digest>::digest(&p).into();
        p.extend_from_slice(&sha);
        p
    }

    fn meta(end_ts: u64, pack: &[u8]) -> SlabMeta {
        let pack_sha: [u8; 20] = pack[pack.len() - 20..].try_into().unwrap();
        SlabMeta {
            id: SlabMeta::make_id(Tier::Base, end_ts, &pack_sha),
            tier: Tier::Base,
            start_ts: 0,
            end_ts,
            have: Oid::ZERO,
            want: Oid::ZERO,
            objects: 1,
            pack_len: pack.len() as u64,
            index_len: 0,
            pack_sha,
        }
    }

    /// The new pack is made of the old one and the tail, byte for byte, whatever their
    /// lengths (bodies that end before, on and after a block boundary); and nothing is
    /// made of a tail of another baseline, of another old pack, or of a tail cut short.
    #[test]
    fn a_baseline_is_made_of_the_one_before_it_and_its_tail() {
        let dir = std::env::temp_dir().join(format!("gvd-tail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let bytes: Vec<u8> = (0..300_000u32).map(|i| (i * 31 % 251) as u8).collect();
        for (n, (old_body, week)) in [
            (0, 0),
            (1, 1),
            (4063, 77),
            (4064, 5000),
            (4065, 1),
            (200_000, 90_000),
        ]
        .into_iter()
        .enumerate()
        {
            let old = pack(3, &bytes[..old_body]);
            let new = pack(5, &bytes[..old_body + week]);
            let (old_meta, new_meta) = (meta(100, &old), meta(200, &new));
            let old_path = dir.join(format!("{}.pack", old_meta.id));
            let new_path = dir.join(format!("new-{n}.pack"));
            std::fs::write(&old_path, &old).unwrap();
            std::fs::write(&new_path, &new).unwrap();
            let tail = dir.join(format!("{n}.tail"));
            assert!(write(&old_meta, &old, &new_meta, &new_path, None, &tail).unwrap());
            // The tail holds the week's entries and little more.
            let tail_len = std::fs::metadata(&tail).unwrap().len() as usize;
            assert!(tail_len < week + 170, "{tail_len} for {week}");

            let find = |id: &str| Some(dir.join(format!("{id}.pack")));
            let out = dir.join(format!("grown-{n}.pack"));
            let head = grow(&tail, find, &new_meta, &out).unwrap().unwrap();
            assert_eq!(
                (head.old_id.as_str(), head.entries),
                (old_meta.id.as_str(), 5)
            );
            assert!(std::fs::read(&out).unwrap() == new);

            // Not the tail of this baseline; an old pack that is not here, or another.
            let other = SlabMeta {
                pack_len: new_meta.pack_len + 1,
                ..new_meta.clone()
            };
            let nothing = dir.join("nothing.pack");
            let not = |made: Result<Option<Head>>| assert_eq!(made.unwrap(), None);
            not(grow(&tail, find, &other, &nothing));
            not(grow(&tail, |_| None, &new_meta, &nothing));
            let mut wrong = old.clone();
            let last = wrong.len() - 1;
            wrong[last] ^= 1;
            std::fs::write(&old_path, &wrong).unwrap();
            not(grow(&tail, find, &new_meta, &nothing));
            std::fs::write(&old_path, &old[..old.len() - 1]).unwrap();
            not(grow(&tail, find, &new_meta, &nothing));
            std::fs::write(&old_path, &old).unwrap();
            // A tail cut short, and one that is something else.
            let whole = std::fs::read(&tail).unwrap();
            std::fs::write(&tail, &whole[..whole.len() - 1]).unwrap();
            not(grow(&tail, find, &new_meta, &nothing));
            std::fs::write(&tail, b"GVDTAIL0 and so on").unwrap();
            assert!(grow(&tail, find, &new_meta, &nothing).is_err());
            assert!(!nothing.exists());

            // A new pack that is not the old one and more has no tail.
            let mut changed = new.clone();
            if old_body > 0 {
                changed[12] ^= 1;
                std::fs::write(&new_path, &changed).unwrap();
                let none = dir.join("none.tail");
                assert!(!write(&old_meta, &old, &new_meta, &new_path, None, &none).unwrap());
                assert!(!none.exists());
            }
        }
        // No temporary file is left behind.
        let left: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with('.'))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The new index, too, is made of what the edge has: the old index, and the records
    /// the tail carries of the objects after the old pack's. The weekly run takes them
    /// from the index it built, in the order of their entries, once it has made the
    /// index the edge's way and found it the same. The edge makes it, and it is the
    /// run's, byte for byte: a slab opens on it, and every object is where its record
    /// says, a delta's base with it, old or new. (The edge downloaded the index: 40
    /// bytes an object, all of them.) Nothing is made of a tail without records, of an
    /// old index that is not here or is another pack's, of records that are not the
    /// ones (a byte of a size, a byte of an id), or for a manifest that lists another
    /// index. And no records are published of an index that is not the old one's and
    /// more, or where a pack holds an object twice.
    #[test]
    fn a_baselines_index_is_made_of_the_one_before_it_and_the_tails_records() {
        use crate::slab::{Facts, NO_BASE, Slab, build_index, build_index_with_entries};
        const OLD: u32 = 5000;
        const NEW: u32 = 300;
        const ENTRY: usize = 50;
        let dir = std::env::temp_dir().join(format!("gvd-tail-index-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let bytes: Vec<u8> = (0..(OLD + NEW) as usize * ENTRY)
            .map(|i| (i * 31 % 251) as u8)
            .collect();
        let old = pack(OLD, &bytes[..OLD as usize * ENTRY]);
        let new = pack(OLD + NEW, &bytes);
        let id = |i: u32| Oid(<sha1::Sha1 as sha1::Digest>::digest(i.to_le_bytes()).into());
        // Objects one after another in the pack; two in three are deltas, of an object
        // before them: the new ones of old ones and of new ones.
        let fact = |i: u32| Facts {
            oid: id(i),
            offset: (PACK_HEADER_LEN + i as usize * ENTRY) as u64,
            entry_len: ENTRY as u32,
            size: 1000 + u64::from(i),
            kind: 1 + (i % 4) as u8,
            depth: (i % 5) as u8,
            base: if i.is_multiple_of(3) { NO_BASE } else { i / 2 },
        };
        let facts: Vec<Facts> = (0..OLD + NEW).map(fact).collect();
        let (mut old_meta, mut new_meta) = (meta(100, &old), meta(200, &new));
        let old_index = build_index(
            &facts[..OLD as usize],
            old_meta.pack_len,
            &old_meta.pack_sha,
            2,
        )
        .unwrap();
        let new_index = build_index(&facts, new_meta.pack_len, &new_meta.pack_sha, 3).unwrap();
        old_meta.index_len = old_index.len() as u64;
        new_meta.index_len = new_index.len() as u64;
        new_meta.objects = OLD + NEW;
        let old_index_path = dir.join(format!("{}.mphf", old_meta.id));
        let (new_path, new_index_path) = (dir.join("new.pack"), dir.join("new.mphf"));
        std::fs::write(dir.join(format!("{}.pack", old_meta.id)), &old).unwrap();
        std::fs::write(&old_index_path, &old_index).unwrap();
        std::fs::write(&new_path, &new).unwrap();
        std::fs::write(&new_index_path, &new_index).unwrap();

        // The weekly run's side.
        let scratch = dir.join("trial.mphf");
        let said = |old_index: &[u8], new: &SlabMeta, new_index: &Path| {
            let said = index_tail(old_index, old_meta.pack_len, new, new_index, 2, &scratch);
            assert!(!scratch.exists());
            said.unwrap()
        };
        let index = said(&old_index, &new_meta, &new_index_path).unwrap();
        assert_eq!(index.records.len(), NEW as usize * REC_LEN);
        assert_eq!(index.len, new_index.len() as u64);
        let tail = dir.join("t.tail");
        assert!(write(&old_meta, &old, &new_meta, &new_path, Some(&index), &tail).unwrap());
        let whole = std::fs::read(&tail).unwrap();
        let week = NEW as usize * ENTRY + PACK_TRAILER_LEN;
        assert_eq!(
            whole.len(),
            94 + old_meta.id.len() + week + NEW as usize * REC_LEN
        );

        // The edge's side.
        let find = |id: &str| Some(dir.join(format!("{id}.pack")));
        let (out_pack, out_index) = (dir.join("grown.pack"), dir.join("grown.mphf"));
        let head = grow(&tail, find, &new_meta, &out_pack).unwrap().unwrap();
        assert_eq!(head.records, NEW);
        assert!(std::fs::read(&out_pack).unwrap() == new);
        assert!(grow_index(&tail, &head, &old_index_path, &new_meta, 1, &out_index).unwrap());
        assert!(std::fs::read(&out_index).unwrap() == new_index);
        let slab = Slab::open(&out_pack, &out_index, new_meta.clone()).unwrap();
        assert_eq!(slab.count(), OLD + NEW);
        for f in &facts {
            let rec = slab.rec(slab.find(&f.oid).unwrap()).unwrap();
            assert_eq!(
                (rec.offset, rec.size, rec.kind, rec.depth, rec.entry_len),
                (f.offset, f.size, f.kind, f.depth, f.entry_len)
            );
            if f.base == NO_BASE {
                assert!(!rec.is_delta());
            } else {
                assert_eq!(slab.rec(rec.base).unwrap().oid, id(f.base));
            }
        }
        drop(slab);

        // What no index is made of.
        let not = |tail: &Path, head: &Head, old_index: &Path, new: &SlabMeta| {
            let out = dir.join("not.mphf");
            let made = grow_index(tail, head, old_index, new, 1, &out).unwrap();
            assert!(!made && !out.exists());
        };
        let bare = dir.join("bare.tail");
        assert!(write(&old_meta, &old, &new_meta, &new_path, None, &bare).unwrap());
        let again = dir.join("again.pack");
        let bare_head = grow(&bare, find, &new_meta, &again).unwrap().unwrap();
        assert_eq!((bare_head.records, bare_head.index_len), (0, 0));
        assert!(std::fs::read(&again).unwrap() == new);
        not(&bare, &bare_head, &old_index_path, &new_meta);
        not(&tail, &head, &dir.join("absent.mphf"), &new_meta);
        not(&tail, &head, &new_index_path, &new_meta);
        let other = SlabMeta {
            index_len: new_meta.index_len + REC_LEN as u64,
            ..new_meta.clone()
        };
        not(&tail, &head, &old_index_path, &other);
        // The last record's size, and its id.
        for at in [whole.len() - 7, whole.len() - 30] {
            let mut changed = whole.clone();
            changed[at] ^= 1;
            let path = dir.join("changed.tail");
            std::fs::write(&path, &changed).unwrap();
            not(&path, &head, &old_index_path, &new_meta);
        }

        // What no records are published of: an old object whose record is another in
        // the new index; an old pack with an object twice; an old index one object short.
        let mut moved = facts.clone();
        moved[7].size += 1;
        let moved = build_index(&moved, new_meta.pack_len, &new_meta.pack_sha, 2).unwrap();
        let moved_path = dir.join("moved.mphf");
        std::fs::write(&moved_path, &moved).unwrap();
        assert!(said(&old_index, &new_meta, &moved_path).is_none());
        let old_facts = &facts[..OLD as usize];
        let (len, sha) = (old_meta.pack_len, &old_meta.pack_sha);
        let twice = build_index_with_entries(old_facts, OLD + 1, len, sha, 2).unwrap();
        assert!(said(&twice, &new_meta, &new_index_path).is_none());
        let short = build_index(&old_facts[..OLD as usize - 1], len, sha, 2).unwrap();
        assert!(said(&short, &new_meta, &new_index_path).is_none());

        let left: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with('.'))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
