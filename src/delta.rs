//! Git deltas (`OBJ_OFS_DELTA` / `OBJ_REF_DELTA` payloads): application, and encoding of
//! tree deltas.
//!
//! A delta is `base_size varint`, `result_size varint`, then ops: `1xxxxxxx` copies a
//! range of the base (offset/size bytes selected by the low 7 bits), `0nnnnnnn` inserts
//! the next `n` literal bytes (`n == 0` is reserved).
//!
//! **Tree deltas.** A tree is a list of `<mode> <name>\0<20-byte id>` entries sorted by
//! name, and the ids do not compress. Two versions of a directory differ in a few entries,
//! so [`tree_delta`] walks both lists in step: runs of identical entries become copy ops
//! and changed or added entries are inserted. That is git's delta search specialised to
//! trees: one linear pass, no hash index of the base.

use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaError {
    Header,
    BaseSize,
    ResultSize,
    BadOp,
    CopyOutOfRange,
    Truncated,
}

impl std::fmt::Display for DeltaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DeltaError::Header => "malformed delta header",
            DeltaError::BaseSize => "delta base size mismatch",
            DeltaError::ResultSize => "delta result size mismatch",
            DeltaError::BadOp => "reserved delta opcode",
            DeltaError::CopyOutOfRange => "delta copy outside its base",
            DeltaError::Truncated => "truncated delta",
        })
    }
}

impl From<DeltaError> for crate::util::Error {
    fn from(e: DeltaError) -> Self {
        crate::util::Error::new(e.to_string())
    }
}

fn varint(d: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let b = *d.get(*pos)?;
        *pos = pos.wrapping_add(1);
        if shift > 63 {
            return None;
        }
        v |= u64::from(b & 0x7F).wrapping_shl(shift);
        shift = shift.wrapping_add(7);
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
}

/// `(base_size, result_size, header_len)` of a delta.
pub fn delta_sizes(delta: &[u8]) -> Option<(u64, u64, usize)> {
    let mut pos = 0usize;
    let base = varint(delta, &mut pos)?;
    let result = varint(delta, &mut pos)?;
    Some((base, result, pos))
}

/// Applies `delta` to `base`, writing exactly `out.len()` (== the delta's result size) bytes.
pub fn apply_delta(base: &[u8], delta: &[u8], out: &mut [u8]) -> Result<(), DeltaError> {
    let (base_size, result_size, mut pos) = delta_sizes(delta).ok_or(DeltaError::Header)?;
    if base_size != crate::util::u64_of(base.len()) {
        return Err(DeltaError::BaseSize);
    }
    if result_size != crate::util::u64_of(out.len()) {
        return Err(DeltaError::ResultSize);
    }
    let mut w = 0usize;
    while let Some(&op) = delta.get(pos) {
        pos = pos.wrapping_add(1);
        if op & 0x80 != 0 {
            let mut off = 0u32;
            let mut size = 0u32;
            for i in 0..4u32 {
                if op & 1u8.wrapping_shl(i) != 0 {
                    let b = *delta.get(pos).ok_or(DeltaError::Truncated)?;
                    pos = pos.wrapping_add(1);
                    off |= u32::from(b).wrapping_shl(i.wrapping_mul(8));
                }
            }
            for i in 0..3u32 {
                if op & 0x10u8.wrapping_shl(i) != 0 {
                    let b = *delta.get(pos).ok_or(DeltaError::Truncated)?;
                    pos = pos.wrapping_add(1);
                    size |= u32::from(b).wrapping_shl(i.wrapping_mul(8));
                }
            }
            if size == 0 {
                size = 0x10000;
            }
            let off = off as usize;
            let size = size as usize;
            let src = base
                .get(off..off.checked_add(size).ok_or(DeltaError::CopyOutOfRange)?)
                .ok_or(DeltaError::CopyOutOfRange)?;
            let end = w.checked_add(size).ok_or(DeltaError::ResultSize)?;
            out.get_mut(w..end)
                .ok_or(DeltaError::ResultSize)?
                .copy_from_slice(src);
            w = end;
        } else if op != 0 {
            let n = usize::from(op);
            let lit_end = pos.checked_add(n).ok_or(DeltaError::Truncated)?;
            let src = delta.get(pos..lit_end).ok_or(DeltaError::Truncated)?;
            let end = w.checked_add(n).ok_or(DeltaError::ResultSize)?;
            out.get_mut(w..end)
                .ok_or(DeltaError::ResultSize)?
                .copy_from_slice(src);
            pos = lit_end;
            w = end;
        } else {
            return Err(DeltaError::BadOp);
        }
    }
    if w != out.len() {
        return Err(DeltaError::ResultSize);
    }
    Ok(())
}

/// Largest copy op git emits (older readers reject bigger ones).
const MAX_COPY: usize = 0x10000;
const MAX_INSERT: usize = 0x7F;

pub(crate) fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v & 0x7F) as u8 | 0x80);
        v = v.wrapping_shr(7);
    }
    out.push(v as u8);
}

/// `base[off .. off + len]`, as copy ops of at most 64 KiB.
pub(crate) fn put_copy(out: &mut Vec<u8>, mut off: usize, mut len: usize) {
    while len > 0 {
        let n = len.min(MAX_COPY);
        let mut op = 0x80u8;
        let mut args = [0u8; 7];
        let mut k = 0usize;
        let mut put = |bit: u8, byte: u8| {
            if byte != 0 {
                op |= bit;
                if let Some(slot) = args.get_mut(k) {
                    *slot = byte;
                }
                k = k.wrapping_add(1);
            }
        };
        for i in 0..4u32 {
            put(
                1u8.wrapping_shl(i),
                (off.wrapping_shr(i.wrapping_mul(8)) & 0xFF) as u8,
            );
        }
        // A size of exactly 64 KiB is encoded as no size bytes.
        if n != MAX_COPY {
            for i in 0..3u32 {
                put(
                    0x10u8.wrapping_shl(i),
                    (n.wrapping_shr(i.wrapping_mul(8)) & 0xFF) as u8,
                );
            }
        }
        out.push(op);
        out.extend_from_slice(args.get(..k).unwrap_or(&[]));
        off = off.wrapping_add(n);
        len = len.wrapping_sub(n);
    }
}

pub(crate) fn put_insert(out: &mut Vec<u8>, data: &[u8]) {
    for chunk in data.chunks(MAX_INSERT) {
        out.push(chunk.len() as u8);
        out.extend_from_slice(chunk);
    }
}

/// One tree entry starting at some offset: where it ends, its name, and whether it is a
/// directory (directories sort as if their name ended in `/`).
struct Entry<'a> {
    end: usize,
    name: &'a [u8],
    dir: bool,
}

fn entry_at(tree: &[u8], at: usize) -> Option<Entry<'_>> {
    let rest = tree.get(at..)?;
    let sp = rest.iter().position(|&b| b == b' ')?;
    let mode = rest.get(..sp)?;
    let after = rest.get(sp.checked_add(1)?..)?;
    let nul = after.iter().position(|&b| b == 0)?;
    let len = sp.checked_add(1)?.checked_add(nul)?.checked_add(21)?;
    if rest.len() < len {
        return None;
    }
    Some(Entry {
        end: at.checked_add(len)?,
        name: after.get(..nul)?,
        dir: mode == b"40000" || mode == b"040000",
    })
}

/// Git's tree order (`base_name_compare`).
fn tree_order(a: &Entry<'_>, b: &Entry<'_>) -> Ordering {
    let n = a.name.len().min(b.name.len());
    match a.name.get(..n).cmp(&b.name.get(..n)) {
        Ordering::Equal => {}
        o => return o,
    }
    let end = |e: &Entry<'_>| {
        e.name
            .get(n)
            .copied()
            .unwrap_or(if e.dir { b'/' } else { 0 })
    };
    end(a).cmp(&end(b))
}

/// Encodes `target` as a git delta against `base` (both trees) into `out`. Returns false
/// when the delta would be longer than `max_len`, which the caller sets to what storing
/// `target` whole would cost. Malformed trees still produce a correct (if poor) delta:
/// only byte-identical entries are ever copied.
pub fn tree_delta(base: &[u8], target: &[u8], max_len: usize, out: &mut Vec<u8>) -> bool {
    out.clear();
    put_varint(out, crate::util::u64_of(base.len()));
    put_varint(out, crate::util::u64_of(target.len()));
    if base.len() > u32::MAX as usize {
        return false;
    }
    let mut b = 0usize;
    let mut t = 0usize;
    // The pending copy run `base[copy_at .. copy_at + copy_len]`, and the pending literal
    // run `target[lit_at .. t]`; at most one of them is non-empty.
    let mut copy_at = 0usize;
    let mut copy_len = 0usize;
    let mut lit_at = 0usize;
    while let Some(te) = entry_at(target, t) {
        let mut same = None;
        while let Some(be) = entry_at(base, b) {
            match tree_order(&be, &te) {
                // Deleted in the target.
                Ordering::Less => b = be.end,
                Ordering::Equal => {
                    same = Some(be);
                    break;
                }
                Ordering::Greater => break,
            }
        }
        match same {
            Some(be) if base.get(b..be.end) == target.get(t..te.end) => {
                if lit_at < t {
                    put_insert(out, target.get(lit_at..t).unwrap_or(&[]));
                }
                let len = be.end.wrapping_sub(b);
                if copy_len > 0 && copy_at.wrapping_add(copy_len) == b {
                    copy_len = copy_len.wrapping_add(len);
                } else {
                    put_copy(out, copy_at, copy_len);
                    copy_at = b;
                    copy_len = len;
                }
                b = be.end;
                lit_at = te.end;
            }
            other => {
                // Changed (same name, new mode or id) or added: insert it.
                if let Some(be) = other {
                    b = be.end;
                }
                put_copy(out, copy_at, copy_len);
                copy_len = 0;
            }
        }
        t = te.end;
        if out.len() > max_len {
            return false;
        }
    }
    put_copy(out, copy_at, copy_len);
    // The literal run, plus anything after a malformed entry.
    put_insert(out, target.get(lit_at..).unwrap_or(&[]));
    out.len() <= max_len
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }

    #[test]
    fn apply_copy_and_insert() {
        let base = b"hello world, hello delta";
        let mut d = Vec::new();
        enc(base.len() as u64, &mut d);
        enc(16, &mut d);
        d.extend_from_slice(&[0x80 | 0x01 | 0x10, 13, 5]); // copy base[13..18] = "hello"
        d.push(3);
        d.extend_from_slice(b" to"); // insert
        d.extend_from_slice(&[0x80 | 0x10, 8]); // copy base[0..8] = "hello wo"
        let mut out = vec![0u8; 16];
        apply_delta(base, &d, &mut out).unwrap();
        assert_eq!(&out, b"hello tohello wo");
        assert_eq!(
            apply_delta(&base[1..], &d, &mut out),
            Err(DeltaError::BaseSize)
        );
        let mut short = vec![0u8; 15];
        assert_eq!(
            apply_delta(base, &d, &mut short),
            Err(DeltaError::ResultSize)
        );
        let mut bad = d.clone();
        bad.push(0);
        assert!(apply_delta(base, &bad, &mut out).is_err());
        let mut oob = Vec::new();
        enc(base.len() as u64, &mut oob);
        enc(4, &mut oob);
        oob.extend_from_slice(&[0x80 | 0x01 | 0x10, 30, 4]);
        assert_eq!(
            apply_delta(base, &oob, &mut out[..4]),
            Err(DeltaError::CopyOutOfRange)
        );
    }

    /// A tree from `(name, is_dir, id byte)` entries, sorted in git order.
    fn tree(entries: &[(String, bool, u8)]) -> Vec<u8> {
        let mut sorted: Vec<&(String, bool, u8)> = entries.iter().collect();
        let key = |e: &(String, bool, u8)| {
            let mut k = e.0.clone().into_bytes();
            if e.1 {
                k.push(b'/');
            }
            k
        };
        sorted.sort_by_key(|a| key(a));
        let mut out = Vec::new();
        for (name, dir, id) in sorted {
            out.extend_from_slice(if *dir { b"40000 " } else { b"100644 " });
            out.extend_from_slice(name.as_bytes());
            out.push(0);
            out.extend_from_slice(&[*id; 20]);
        }
        out
    }

    fn roundtrip(base: &[u8], target: &[u8]) -> usize {
        let mut d = Vec::new();
        assert!(tree_delta(base, target, usize::MAX, &mut d));
        let mut out = vec![0u8; target.len()];
        apply_delta(base, &d, &mut out).unwrap();
        assert_eq!(out, target);
        d.len()
    }

    #[test]
    fn tree_delta_roundtrips_and_is_small() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut rand = move |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        for round in 0..300 {
            let n = 1 + rand(400) as usize;
            let mut entries: Vec<(String, bool, u8)> = (0..n)
                .map(|i| {
                    (
                        format!("e{:05}{}", rand(100_000), i),
                        rand(4) == 0,
                        rand(256) as u8,
                    )
                })
                .collect();
            let base = tree(&entries);
            // A few edits: modify, delete, add, and flip file <-> directory.
            let edits = 1 + rand(if round % 10 == 0 { 50 } else { 4 }) as usize;
            for _ in 0..edits {
                let i = rand(entries.len() as u64) as usize;
                match rand(4) {
                    0 => entries[i].2 = entries[i].2.wrapping_add(1),
                    1 if entries.len() > 1 => {
                        entries.remove(i);
                    }
                    2 => entries.push((format!("new{}", rand(1_000_000)), rand(2) == 0, 7)),
                    _ => entries[i].1 = !entries[i].1,
                }
            }
            let target = tree(&entries);
            let len = roundtrip(&base, &target);
            if edits <= 4 {
                // Each edit inserts at most one entry and splits one copy.
                let entry_max = 7 + 13 + 1 + 20 + 1;
                assert!(
                    len <= 12 + edits * (entry_max + 2 * 6) + 6,
                    "delta of {len} bytes for {edits} edits"
                );
            }
        }
        // Degenerate inputs.
        let t = tree(&[("a".into(), false, 1), ("b".into(), true, 2)]);
        roundtrip(&[], &t);
        roundtrip(&t, &[]);
        roundtrip(&t, &t);
        roundtrip(&t, b"not a tree at all");
        roundtrip(b"\x00garbage", &t);
        // Copies longer than 64 KiB are split.
        let big: Vec<(String, bool, u8)> = (0..5000)
            .map(|i| (format!("file{i:06}"), false, 3))
            .collect();
        let base = tree(&big);
        let mut changed = big.clone();
        changed[4999].2 = 9;
        assert!(roundtrip(&base, &tree(&changed)) < 100);
        // A budget the delta cannot meet.
        let mut d = Vec::new();
        assert!(!tree_delta(&t, b"something else entirely", 4, &mut d));
    }
}
