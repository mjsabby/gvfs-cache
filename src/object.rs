//! Parsing of the two object types the server looks inside: trees and commits.

use crate::util::Oid;

/// One `<mode> SP <name> NUL <20-byte oid>` record of a tree.
#[derive(Clone, Copy, Debug)]
pub struct TreeEntry<'a> {
    pub mode: &'a [u8],
    pub name: &'a [u8],
    pub oid: Oid,
}

impl TreeEntry<'_> {
    /// Whether the entry is a directory, whatever the zero padding of its mode: git writes
    /// `40000` but reads `040000`, which other tools have written, as the same. (Read as a
    /// file, such a directory was left out of a commit's closure with everything under it.)
    #[inline]
    pub fn is_tree(&self) -> bool {
        self.mode == b"40000"
            || (self.mode.first() == Some(&b'0')
                && mode_kind(self.mode) == Some(crate::pack::OBJ_TREE))
    }
}

/// The type of object a tree entry's mode names: a directory is a tree, a gitlink a
/// commit (of another repository), anything else a blob, whatever the zero padding (as
/// git reads modes). `None` if the mode is not octal.
pub fn mode_kind(mode: &[u8]) -> Option<u8> {
    if mode.is_empty() || mode.len() > 7 {
        return None;
    }
    let mut v = 0u32;
    for &c in mode {
        if !(b'0'..=b'7').contains(&c) {
            return None;
        }
        v = v.wrapping_shl(3) | u32::from(c.wrapping_sub(b'0'));
    }
    Some(match v & 0o170_000 {
        0o040_000 => crate::pack::OBJ_TREE,
        0o160_000 => crate::pack::OBJ_COMMIT,
        _ => crate::pack::OBJ_BLOB,
    })
}

/// Iterates a tree's entries; stops (and sets `malformed`) at the first bad record.
pub struct TreeIter<'a> {
    rest: &'a [u8],
    pub malformed: bool,
}

impl<'a> TreeIter<'a> {
    pub fn new(tree: &'a [u8]) -> TreeIter<'a> {
        TreeIter {
            rest: tree,
            malformed: false,
        }
    }
}

impl<'a> Iterator for TreeIter<'a> {
    type Item = TreeEntry<'a>;

    fn next(&mut self) -> Option<TreeEntry<'a>> {
        if self.rest.is_empty() {
            return None;
        }
        let parsed = (|| {
            let sp = self.rest.iter().position(|&b| b == b' ')?;
            let mode = self.rest.get(..sp)?;
            let after_mode = self.rest.get(sp.checked_add(1)?..)?;
            let nul = after_mode.iter().position(|&b| b == 0)?;
            let name = after_mode.get(..nul)?;
            let oid_start = nul.checked_add(1)?;
            let oid = Oid::from_slice(after_mode.get(oid_start..oid_start.checked_add(20)?)?)?;
            let rest = after_mode.get(oid_start.checked_add(20)?..)?;
            Some((TreeEntry { mode, name, oid }, rest))
        })();
        match parsed {
            Some((entry, rest)) => {
                self.rest = rest;
                Some(entry)
            }
            None => {
                self.malformed = true;
                self.rest = &[];
                None
            }
        }
    }
}

/// The `tree` line of a commit.
pub fn commit_tree(commit: &[u8]) -> Option<Oid> {
    let line = commit.strip_prefix(b"tree ")?;
    Oid::from_hex(line.get(..40)?)
}

/// The `parent` lines of a commit, in order.
pub fn commit_parents(commit: &[u8]) -> impl Iterator<Item = Oid> + '_ {
    // Headers end at the first empty line; parents follow the tree line.
    let headers_end = commit
        .windows(2)
        .position(|w| w == b"\n\n")
        .unwrap_or(commit.len());
    commit
        .get(..headers_end)
        .unwrap_or(&[])
        .split(|&b| b == b'\n')
        .skip(1)
        .map_while(|line| line.strip_prefix(b"parent "))
        .filter_map(|hex| Oid::from_hex(hex.get(..40)?))
}

/// The committer timestamp (`committer <name> <email> <seconds> <zone>`), if parseable.
pub fn commit_time(commit: &[u8]) -> Option<i64> {
    let headers_end = commit
        .windows(2)
        .position(|w| w == b"\n\n")
        .unwrap_or(commit.len());
    let line = commit
        .get(..headers_end)?
        .split(|&b| b == b'\n')
        .find_map(|l| l.strip_prefix(b"committer "))?;
    let after_email = line.get(line.iter().rposition(|&b| b == b'>')?.checked_add(1)?..)?;
    let digits = after_email.split(|&b| b == b' ').find(|w| !w.is_empty())?;
    std::str::from_utf8(digits).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tree_and_commit() {
        let a = Oid([1u8; 20]);
        let b = Oid([2u8; 20]);
        let mut tree = Vec::new();
        tree.extend_from_slice(b"100644 file.txt\0");
        tree.extend_from_slice(&a.0);
        tree.extend_from_slice(b"40000 dir\0");
        tree.extend_from_slice(&b.0);
        let entries: Vec<_> = TreeIter::new(&tree).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            (entries[0].name, entries[0].oid, entries[0].is_tree()),
            (&b"file.txt"[..], a, false)
        );
        assert_eq!(
            (entries[1].name, entries[1].oid, entries[1].is_tree()),
            (&b"dir"[..], b, true)
        );
        // A directory is one whatever the zero padding of its mode, as git reads it.
        let entry = |mode: &'static [u8]| TreeEntry {
            mode,
            name: b"x",
            oid: a,
        };
        for (mode, tree) in [
            (&b"040000"[..], true),
            (b"0040000", true),
            (b"0100644", false),
            (b"160000", false),
            (b"0", false),
            (b"04000x", false),
        ] {
            assert_eq!(entry(mode).is_tree(), tree, "{mode:?}");
        }
        let mut bad = TreeIter::new(&tree[..tree.len() - 1]);
        assert_eq!(bad.by_ref().count(), 1);
        assert!(bad.malformed);

        let commit =
            format!("tree {a}\nparent {b}\nparent {a}\nauthor x <x> 1 +0000\n\nparent {b}\n");
        assert_eq!(commit_tree(commit.as_bytes()), Some(a));
        assert_eq!(
            commit_parents(commit.as_bytes()).collect::<Vec<_>>(),
            vec![b, a]
        );
        assert_eq!(commit_time(commit.as_bytes()), None);
        let commit = format!(
            "tree {a}\nauthor A <a@x> 5 +0000\ncommitter C D <c@x> 1700000123 -0700\n\nmsg\n"
        );
        assert_eq!(commit_time(commit.as_bytes()), Some(1_700_000_123));
    }
}
