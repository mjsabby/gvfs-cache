//! Commit and tree object parsing over borrowed slices.

use crate::oid::Oid20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    pub tree: Oid20,
    pub parent_count: usize,
    pub commit_time: u64,
}

fn header_lines(content: &[u8]) -> impl Iterator<Item = &[u8]> {
    content.split(|&c| c == b'\n').take_while(|l| !l.is_empty())
}

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

    #[inline]
    pub fn is_blob(&self) -> bool {
        let kind = self.mode & 0o170000;
        kind == 0o100000 || kind == 0o120000
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedTree;

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
