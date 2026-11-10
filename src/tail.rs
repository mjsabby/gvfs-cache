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
//! ```text
//! "GVDTAIL1" | old id length u16 | old id | old pack length u64 | old pack trailer [20]
//! | new pack length u64 | new pack entries u32
//! | new pack bytes from (old pack length - 20) to its end
//! ```

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::pack::{PACK_HEADER_LEN, PACK_TRAILER_LEN, pack_header};
use crate::slab::SlabMeta;
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
        out
    }

    /// The head at the start of `bytes`, and its length.
    fn decode(bytes: &[u8]) -> Result<(Head, usize)> {
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
        let head = Head {
            old_id: old_id.to_string(),
            old_len: le_u64(bytes, at).ctx("tail head")?,
            old_sha,
            new_len: le_u64(bytes, at.saturating_add(28)).ctx("tail head")?,
            entries: le_u32(bytes, at.saturating_add(36)).ctx("tail head")?,
        };
        Ok((head, at.saturating_add(40)))
    }

    /// Where the old pack's entries end, in both packs.
    fn joint(&self) -> Option<u64> {
        self.old_len
            .checked_sub(u64_of(PACK_TRAILER_LEN))
            .filter(|&joint| joint >= u64_of(PACK_HEADER_LEN) && joint <= self.new_len)
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
/// `old`, whose pack is `old_pack`. `Ok(false)`, and no file, if the new pack is not the
/// old one and more (it always is, as the weekly run writes it: checked here, byte for
/// byte, since a tail that is wrong costs every edge a copy and a hash before it
/// downloads the pack after all).
pub fn write(
    old: &SlabMeta,
    old_pack: &[u8],
    new: &SlabMeta,
    new_pack: &Path,
    out: &Path,
) -> Result<bool> {
    let mut pack = File::open(new_pack).ctx(&format!("opening {}", new_pack.display()))?;
    let mut header = [0u8; PACK_HEADER_LEN];
    pack.read_exact(&mut header)?;
    let entries = header_entries(&header);
    let head = Head {
        old_id: old.id.clone(),
        old_len: u64_of(old_pack.len()),
        old_sha: old.pack_sha,
        new_len: pack.metadata()?.len(),
        entries,
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

/// The count in a pack header.
fn header_entries(header: &[u8; PACK_HEADER_LEN]) -> u32 {
    let [.., a, b, c, d] = *header;
    u32::from_be_bytes([a, b, c, d])
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
    let mut start = vec![0u8; 4096];
    let mut got = 0usize;
    while got < start.len() {
        match tail_file.read(start.get_mut(got..).unwrap_or(&mut []))? {
            0 => break,
            n => got = got.saturating_add(n),
        }
    }
    let (head, head_len) = Head::decode(start.get(..got).unwrap_or(&[]))?;
    let tail_len = tail_file.metadata()?.len();
    let Some(joint) = head.joint() else {
        crate::bail!("tail head: lengths that cannot be");
    };
    let rest = head.new_len.saturating_sub(joint);
    if head.new_len != new.pack_len || tail_len != u64_of(head_len).saturating_add(rest) {
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
        copy_range(&mut tail_file, &mut file, u64_of(head_len), tail_len)?;
        std::fs::rename(&tmp, out)?;
        Ok(())
    })();
    if made.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    made.ctx(&format!("making {}", out.display()))?;
    Ok(Some(head))
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
            assert!(write(&old_meta, &old, &new_meta, &new_path, &tail).unwrap());
            // The tail holds the week's entries and little more.
            let tail_len = std::fs::metadata(&tail).unwrap().len() as usize;
            assert!(tail_len < week + 120, "{tail_len} for {week}");

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
                assert!(!write(&old_meta, &old, &new_meta, &new_path, &none).unwrap());
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
}
