use crate::buf::ByteWriter;
use crate::object::{TreeEntry, TreeIter, tree_entry_cmp};
use crate::{Error, Result};

pub const MAX_CLUSTER_BYTES: u64 = 1 << 20;
pub const MAX_CLUSTER_MEMBERS: usize = 16;
pub const HEAD_TREE: u64 = 1 << 63;
pub const FAMILY_MASK: u64 = HEAD_TREE - 1;

pub fn family(parent: u64, name: &[u8]) -> u64 {
    let mut h = parent ^ 0x9e37_79b9_7f4a_7c15;
    for &byte in name {
        h = h.wrapping_mul(0x100_0000_01b3) ^ u64::from(byte);
    }
    (crate::arena::mix(h) & FAMILY_MASK).max(1)
}

fn integer(w: &mut ByteWriter<'_>, mut value: usize) -> Result<()> {
    loop {
        let byte = (value & 127) as u8;
        value >>= 7;
        w.put_u8(if value == 0 { byte } else { byte | 128 })?;
        if value == 0 {
            return Ok(());
        }
    }
}

fn copy(w: &mut ByteWriter<'_>, mut offset: usize, mut len: usize) -> Result<()> {
    while len > 0 {
        let n = len.min(0x00ff_ffff);
        let off = u32::try_from(offset)
            .map_err(|_| Error::Capacity("delta offset"))?
            .to_le_bytes();
        let size = (n as u32).to_le_bytes();
        let mut op = 128u8;
        let mut arguments = [0u8; 7];
        let mut used = 0usize;
        for (i, &b) in off.iter().enumerate() {
            if b != 0 {
                op |= 1u8.wrapping_shl(i as u32);
                *arguments
                    .get_mut(used)
                    .ok_or(Error::Invalid("copy instruction"))? = b;
                used = used.saturating_add(1);
            }
        }
        if n != 65536 {
            for (i, &b) in size.iter().take(3).enumerate() {
                if b != 0 {
                    op |= 16u8.wrapping_shl(i as u32);
                    *arguments
                        .get_mut(used)
                        .ok_or(Error::Invalid("copy instruction"))? = b;
                    used = used.saturating_add(1);
                }
            }
        }
        w.put_u8(op)?;
        w.put(
            arguments
                .get(..used)
                .ok_or(Error::Invalid("copy instruction"))?,
        )?;
        offset = offset
            .checked_add(n)
            .ok_or(Error::Capacity("delta offset"))?;
        len = len.saturating_sub(n);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Segment {
    Copy(usize, usize),
    Insert(usize, usize),
}

fn flush(w: &mut ByteWriter<'_>, target: &[u8], segment: Option<Segment>) -> Result<()> {
    match segment {
        None => {}
        Some(Segment::Copy(offset, len)) => copy(w, offset, len)?,
        Some(Segment::Insert(offset, len)) => {
            for chunk in target
                .get(offset..offset.saturating_add(len))
                .ok_or(Error::Invalid("delta literal"))?
                .chunks(127)
            {
                w.put_u8(chunk.len() as u8)?;
                w.put(chunk)?;
            }
        }
    }
    Ok(())
}
fn append(
    w: &mut ByteWriter<'_>,
    target: &[u8],
    pending: &mut Option<Segment>,
    next: Segment,
) -> Result<()> {
    match (*pending, next) {
        (Some(Segment::Copy(a, n)), Segment::Copy(b, m)) if a.checked_add(n) == Some(b) => {
            *pending = Some(Segment::Copy(a, n.saturating_add(m)));
            return Ok(());
        }
        (Some(Segment::Insert(a, n)), Segment::Insert(b, m)) if a.checked_add(n) == Some(b) => {
            *pending = Some(Segment::Insert(a, n.saturating_add(m)));
            return Ok(());
        }
        _ => {}
    }
    flush(w, target, *pending)?;
    *pending = Some(next);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn next(&mut self) -> Result<Option<(TreeEntry<'a>, usize, &'a [u8])>> {
        if self.bytes.is_empty() {
            return Ok(None);
        }
        let entry = TreeIter::new(self.bytes)
            .next()
            .ok_or(Error::Invalid("tree"))?
            .map_err(|_| Error::Invalid("tree"))?;
        let len = self
            .bytes
            .iter()
            .position(|&b| b == 0)
            .and_then(|n| n.checked_add(21))
            .ok_or(Error::Invalid("tree entry"))?;
        let raw = self.bytes.get(..len).ok_or(Error::Invalid("tree entry"))?;
        let start = self.offset;
        self.offset = self
            .offset
            .checked_add(len)
            .ok_or(Error::Capacity("tree offset"))?;
        self.bytes = self.bytes.get(len..).ok_or(Error::Invalid("tree entry"))?;
        Ok(Some((entry, start, raw)))
    }
}

/// Linear merge of canonical tree entries. Adjacent equal records become one COPY;
/// scattered edits produce multiple short COPY/INSERT runs, not one giant literal.
pub fn encode(base: &[u8], target: &[u8], output: &mut [u8]) -> Result<usize> {
    let mut w = ByteWriter::new(output);
    integer(&mut w, base.len())?;
    integer(&mut w, target.len())?;
    let mut base_cursor = Cursor {
        bytes: base,
        offset: 0,
    };
    let mut target_cursor = Cursor {
        bytes: target,
        offset: 0,
    };
    let mut current = base_cursor.next()?;
    let mut pending = None;
    while let Some((entry, offset, raw)) = target_cursor.next()? {
        while current.as_ref().is_some_and(|(b, _, _)| {
            tree_entry_cmp(b.name, b.is_tree(), entry.name, entry.is_tree()).is_lt()
        }) {
            current = base_cursor.next()?;
        }
        let matching = current.as_ref().is_some_and(|(b, _, _)| {
            tree_entry_cmp(b.name, b.is_tree(), entry.name, entry.is_tree()).is_eq()
        });
        let segment = match current {
            Some((_, at, old)) if matching && old == raw => Segment::Copy(at, raw.len()),
            _ => Segment::Insert(offset, raw.len()),
        };
        append(&mut w, target, &mut pending, segment)?;
        if matching {
            current = base_cursor.next()?;
        }
    }
    // Validate the unconsumed base as well; malformed input is never a compression fallback.
    while base_cursor.next()?.is_some() {}
    flush(&mut w, target, pending)?;
    Ok(w.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scattered_edits_insert_delete_and_large_copies() {
        let mut base = Vec::new();
        let mut target = Vec::new();
        for i in 0..4000u32 {
            let name = format!("100644 file-{i:04}\0");
            base.extend_from_slice(name.as_bytes());
            base.extend_from_slice(&crate::sha1_parts(&[&i.to_le_bytes()]));
            if i == 137 {
                continue;
            }
            target.extend_from_slice(name.as_bytes());
            target.extend_from_slice(&crate::sha1_parts(&[&if i % 123 == 0 {
                i + 9000
            } else {
                i
            }
            .to_le_bytes()]));
        }
        target.extend_from_slice(b"100644 z-new\0");
        target.extend_from_slice(&[7; 20]);
        let mut delta = vec![0; target.len() * 2];
        let n = encode(&base, &target, &mut delta).unwrap();
        assert!(n < 3000, "{n}");
        let mut restored = vec![0; target.len()];
        let len = crate::delta::apply_delta(&base, &delta[..n], &mut restored).unwrap();
        assert_eq!(&restored[..len], target);
        let n = encode(&[], &target, &mut delta).unwrap();
        crate::delta::apply_delta(&[], &delta[..n], &mut restored).unwrap();
        assert_eq!(restored, target);
        assert!(encode(b"bad", b"", &mut delta).is_err());
    }
}
