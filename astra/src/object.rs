//! Commit and tree object parsing over borrowed bytes.

use crate::oid::Oid20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    pub tree: Oid20,
    pub parent_count: usize,
    /// Committer timestamp (seconds since the epoch); 0 if absent/unparseable, like git.
    pub commit_time: u64,
}

fn header_lines(content: &[u8]) -> impl Iterator<Item = &[u8]> {
    // Headers end at the first empty line; continuation lines start with a space.
    content.split(|&c| c == b'\n').take_while(|l| !l.is_empty())
}

/// Parses a commit's tree, parents (passed to `on_parent`; returning false aborts) and
/// committer time. Returns `None` for malformed commits.
pub fn parse_commit<F>(content: &[u8], mut on_parent: F) -> Option<CommitInfo>
where
    F: FnMut(Oid20) -> bool,
{
    let mut lines = header_lines(content);
    let tree = Oid20::from_hex(lines.next()?.strip_prefix(b"tree ")?)?;
    let mut parent_count = 0usize;
    let mut commit_time = 0u64;
    for line in lines {
        if let Some(hex) = line.strip_prefix(b"parent ") {
            if !on_parent(Oid20::from_hex(hex)?) {
                return None;
            }
            parent_count = parent_count.saturating_add(1);
        } else if let Some(rest) = line.strip_prefix(b"committer ") {
            commit_time = parse_signature_time(rest).unwrap_or(0);
        }
    }
    Some(CommitInfo {
        tree,
        parent_count,
        commit_time,
    })
}

/// `Name <email> 1234567890 +0000` → 1234567890 (git's `parse_commit_date` rules).
fn parse_signature_time(sig: &[u8]) -> Option<u64> {
    let gt = sig.iter().rposition(|&c| c == b'>')?;
    let rest = sig.get(gt.checked_add(1)?..)?;
    let rest = rest.strip_prefix(b" ").unwrap_or(rest);
    let end = rest
        .iter()
        .position(|&c| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    crate::buf::parse_dec_u64(rest.get(..end)?)
}

pub const MODE_TREE: u32 = 0o040000;
pub const MODE_GITLINK: u32 = 0o160000;

#[inline]
pub fn is_tree_mode(mode: u32) -> bool {
    mode & 0o170000 == MODE_TREE
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeEntry<'a> {
    pub mode: u32,
    pub name: &'a [u8],
    pub oid: Oid20,
}

impl TreeEntry<'_> {
    #[inline]
    pub fn is_tree(&self) -> bool {
        is_tree_mode(self.mode)
    }

    /// Blob-like entries (regular files and symlinks), excluding gitlinks.
    #[inline]
    pub fn is_blob(&self) -> bool {
        let kind = self.mode & 0o170000;
        kind == 0o100000 || kind == 0o120000
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedTree;

/// Iterator over `<octal mode> SP <name> NUL <20-byte oid>` records.
#[derive(Debug, Clone)]
pub struct TreeIter<'a> {
    data: &'a [u8],
    failed: bool,
}

impl<'a> TreeIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            failed: false,
        }
    }

    fn parse_one(&mut self) -> Option<TreeEntry<'a>> {
        let sp = self.data.iter().position(|&c| c == b' ')?;
        let mut mode = 0u32;
        let mode_digits = self.data.get(..sp)?;
        if mode_digits.is_empty() || mode_digits.len() > 7 {
            return None;
        }
        for &c in mode_digits {
            let d = c.wrapping_sub(b'0');
            if d > 7 {
                return None;
            }
            mode = mode.checked_mul(8)?.checked_add(u32::from(d))?;
        }
        let after_sp = self.data.get(sp.checked_add(1)?..)?;
        let nul = after_sp.iter().position(|&c| c == 0)?;
        let name = after_sp.get(..nul)?;
        if name.is_empty() {
            return None;
        }
        let oid_start = nul.checked_add(1)?;
        let oid = Oid20::from_slice(after_sp.get(oid_start..oid_start.checked_add(20)?)?)?;
        self.data = after_sp.get(oid_start.checked_add(20)?..)?;
        Some(TreeEntry { mode, name, oid })
    }
}

impl<'a> Iterator for TreeIter<'a> {
    type Item = Result<TreeEntry<'a>, MalformedTree>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.data.is_empty() {
            return None;
        }
        match self.parse_one() {
            Some(e) => Some(Ok(e)),
            None => {
                self.failed = true;
                Some(Err(MalformedTree))
            }
        }
    }
}

/// Git's tree-entry ordering (`base_name_compare`): directories sort as if their name
/// had a trailing `/`.
pub fn tree_entry_cmp(
    a_name: &[u8],
    a_is_tree: bool,
    b_name: &[u8],
    b_is_tree: bool,
) -> core::cmp::Ordering {
    let common = a_name.len().min(b_name.len());
    let (ap, bp) = (
        a_name.get(..common).unwrap_or(&[]),
        b_name.get(..common).unwrap_or(&[]),
    );
    match ap.cmp(bp) {
        core::cmp::Ordering::Equal => {}
        other => return other,
    }
    let ac = a_name
        .get(common)
        .copied()
        .unwrap_or(if a_is_tree { b'/' } else { 0 });
    let bc = b_name
        .get(common)
        .copied()
        .unwrap_or(if b_is_tree { b'/' } else { 0 });
    ac.cmp(&bc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_parsing() {
        let c = b"tree 0123456789abcdef0123456789abcdef01234567\n\
parent 1111111111111111111111111111111111111111\n\
parent 2222222222222222222222222222222222222222\n\
author A U Thor <a@example.com> 1112911993 -0700\n\
committer C O Mitter <c@example.com> 1700000000 +0100\n\
gpgsig -----BEGIN PGP SIGNATURE-----\n \n -----END PGP SIGNATURE-----\n\
\n\
parent 3333333333333333333333333333333333333333 (in the message body)\n";
        let mut parents = Vec::new();
        let info = parse_commit(c, |p| {
            parents.push(p);
            true
        })
        .unwrap();
        assert_eq!(info.parent_count, 2);
        assert_eq!(info.commit_time, 1_700_000_000);
        assert_eq!(parents[1], Oid20([0x22; 20]));
        assert!(parse_commit(b"parent x\n", |_| true).is_none());
    }

    #[test]
    fn tree_iteration_and_order() {
        let mut t = Vec::new();
        t.extend_from_slice(b"100644 a.txt\0");
        t.extend_from_slice(&[1u8; 20]);
        t.extend_from_slice(b"40000 dir\0");
        t.extend_from_slice(&[2u8; 20]);
        let entries: Vec<_> = TreeIter::new(&t).collect::<Result<_, _>>().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries[1].is_tree());
        assert!(entries[0].is_blob());
        assert_eq!(entries[1].name, b"dir");
        let mut bad = t.clone();
        bad.truncate(t.len() - 1);
        assert!(TreeIter::new(&bad).any(|e| e.is_err()));
        use core::cmp::Ordering::*;
        assert_eq!(tree_entry_cmp(b"foo", false, b"foo", true), Less);
        assert_eq!(tree_entry_cmp(b"foo", true, b"foo.c", false), Greater); // "foo/" > "foo."
        assert_eq!(tree_entry_cmp(b"foo", true, b"foo0", false), Less); // "foo/" < "foo0"
        assert_eq!(tree_entry_cmp(b"a", true, b"a", true), Equal);
    }
}
