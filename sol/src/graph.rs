//! Compact structural adjacency, independent of the pack layout. Raw construction
//! scratch is mmap-backed; immutable lists use bounded depth-1 Git delta programs.

use std::path::Path;

use crate::arena::{append, push, reserve};
use crate::codec::{Kind, Oid, TreeEntries, apply_delta, be32, checksum, take, verify_checksum};
use crate::slab::{Limits, NO_BASE, PackArena, Record, Slab};
use crate::tree::{delta_copy, delta_varint};
use crate::{Error, Result};

pub const NO_GRAPH: u64 = u64::MAX;
pub const MAGIC: &[u8; 8] = b"GADJ\0\0\0\x01";
const HEADER: usize = 32;
const NODE: usize = 12;

fn arena(capacity: usize, directory: Option<&Path>) -> Result<PackArena> {
    Ok(match directory {
        Some(directory) => PackArena::File(gvfs_platform::FileArena::new(directory, capacity)?),
        None => PackArena::Heap(reserve(capacity)?),
    })
}

fn capacity(limits: Limits) -> Result<usize> {
    limits
        .links
        .checked_mul(20)
        .and_then(|n| n.checked_add(limits.objects.checked_mul(16)?))
        .and_then(|n| n.checked_add(64))
        .ok_or(Error::Capacity("graph arena"))
}

pub struct RawGraph {
    bytes: PackArena,
}

impl RawGraph {
    pub fn new(limits: Limits, directory: Option<&Path>) -> Result<Self> {
        Ok(Self {
            bytes: arena(capacity(limits)?, directory)?,
        })
    }
    pub fn reset(&mut self) {
        self.bytes.clear();
    }
    pub fn append(&mut self, bytes: &[u8]) -> Result<u64> {
        if bytes.is_empty() {
            return Ok(NO_GRAPH);
        }
        if !bytes.len().is_multiple_of(20) {
            return Err(Error::Invalid("graph OID list"));
        }
        let at = self.bytes.len() as u64;
        self.bytes.append(&(bytes.len() as u32).to_be_bytes())?;
        self.bytes.append(bytes)?;
        Ok(at)
    }
    pub fn get(&self, offset: u64) -> Result<&[u8]> {
        if offset == NO_GRAPH {
            return Ok(&[]);
        }
        let offset = usize::try_from(offset).map_err(|_| Error::Capacity("raw graph offset"))?;
        let len = be32(self.bytes.as_ref(), offset)? as usize;
        take(self.bytes.as_ref(), offset.saturating_add(4), len)
    }

    pub fn object(&mut self, kind: Kind, content: &[u8]) -> Result<u64> {
        if kind == Kind::Blob {
            return Ok(NO_GRAPH);
        }
        let at = self.bytes.len();
        self.bytes.append(&[0; 4])?;
        let start = self.bytes.len();
        match kind {
            Kind::Tree => {
                let mut tree = TreeEntries::new(content);
                while let Some(entry) = tree.next_entry()? {
                    if entry.directory {
                        self.bytes.append(&entry.oid.0)?;
                    }
                }
            }
            Kind::Commit | Kind::Tag => {
                let mut primary = false;
                for line in content
                    .split(|byte| *byte == b'\n')
                    .take_while(|line| !line.is_empty())
                {
                    let prefix = if kind == Kind::Commit {
                        b"tree ".as_slice()
                    } else {
                        b"object ".as_slice()
                    };
                    if let Some(hex) = line.strip_prefix(prefix) {
                        if primary {
                            return Err(Error::Invalid("duplicate primary object header"));
                        }
                        self.bytes.append(&Oid::parse(hex)?.0)?;
                        primary = true;
                    } else if kind == Kind::Commit
                        && let Some(hex) = line.strip_prefix(b"parent ")
                    {
                        if !primary {
                            return Err(Error::Invalid("parent before commit tree"));
                        }
                        self.bytes.append(&Oid::parse(hex)?.0)?;
                    }
                }
                if !primary {
                    return Err(Error::Invalid("missing tree/tag object header"));
                }
            }
            _ => return Err(Error::Invalid("unresolved graph object")),
        }
        let len = self.bytes.len().saturating_sub(start);
        if len == 0 {
            self.bytes.truncate(at)?;
            return Ok(NO_GRAPH);
        }
        let len = u32::try_from(len).map_err(|_| Error::Capacity("graph list size"))?;
        self.bytes
            .as_mut_slice()
            .get_mut(at..start)
            .ok_or(Error::Invalid("graph prefix"))?
            .copy_from_slice(&len.to_be_bytes());
        Ok(at as u64)
    }
}

pub struct Encoder {
    out: PackArena,
    base: Vec<u8>,
    delta: Vec<u8>,
}

impl Encoder {
    pub fn new(limits: Limits, directory: Option<&Path>) -> Result<Self> {
        Ok(Self {
            out: arena(capacity(limits)?, directory)?,
            base: reserve(limits.object_bytes)?,
            delta: reserve(
                limits
                    .object_bytes
                    .saturating_add(limits.object_bytes / 100)
                    .saturating_add(4096),
            )?,
        })
    }

    pub fn encode(
        mut self,
        records: &mut [Record],
        raw: &RawGraph,
        order: &mut Vec<u32>,
        pack: Oid,
    ) -> Result<PackArena> {
        order.clear();
        for (i, record) in records.iter().enumerate() {
            if record.graph_offset != NO_GRAPH {
                push(order, i as u32)?;
            }
        }
        order.sort_unstable_by_key(|rank| {
            records.get(*rank as usize).map(|record| {
                (
                    u8::from(record.kind == Kind::Tree),
                    if record.kind == Kind::Tree {
                        record.path
                    } else {
                        0
                    },
                    *rank,
                )
            })
        });
        self.out.append(MAGIC)?;
        self.out.append(&(order.len() as u32).to_be_bytes())?;
        self.out.append(&pack.0)?;
        let mut path = None;
        let mut base_rank = NO_BASE;
        let mut members = 0usize;
        for &rank in order.iter() {
            let record = *records
                .get(rank as usize)
                .ok_or(Error::Invalid("graph ordinal"))?;
            let target = raw.get(record.graph_offset)?;
            let use_delta = record.kind == Kind::Tree
                && path == Some(record.path)
                && members < 16
                && oid_delta(&self.base, target, &mut self.delta)?
                && self.delta.len() < target.len();
            let offset = self.out.len() as u64;
            let parent = if use_delta { base_rank } else { NO_BASE };
            let payload = if use_delta {
                self.delta.as_slice()
            } else {
                target
            };
            self.out.append(&parent.to_be_bytes())?;
            self.out.append(&(target.len() as u32).to_be_bytes())?;
            self.out.append(&(payload.len() as u32).to_be_bytes())?;
            self.out.append(payload)?;
            records
                .get_mut(rank as usize)
                .ok_or(Error::Invalid("graph output ordinal"))?
                .graph_offset = offset;
            if use_delta {
                members = members.saturating_add(1);
            } else {
                self.base.clear();
                append(&mut self.base, target)?;
                base_rank = rank;
                path = if record.kind == Kind::Tree {
                    Some(record.path)
                } else {
                    None
                };
                members = 1;
            }
        }
        let sum = checksum(self.out.as_ref());
        self.out.append(&sum)?;
        Ok(self.out)
    }
}

fn oid_delta(base: &[u8], target: &[u8], out: &mut Vec<u8>) -> Result<bool> {
    out.clear();
    delta_varint(base.len() as u64, out)?;
    delta_varint(target.len() as u64, out)?;
    if base.len() != target.len() {
        let prefix = base
            .chunks_exact(20)
            .zip(target.chunks_exact(20))
            .take_while(|(a, b)| a == b)
            .count()
            .saturating_mul(20);
        let source = base.get(prefix..).ok_or(Error::Invalid("graph prefix"))?;
        let destination = target.get(prefix..).ok_or(Error::Invalid("graph prefix"))?;
        let suffix = source
            .rchunks_exact(20)
            .zip(destination.rchunks_exact(20))
            .take_while(|(a, b)| a == b)
            .count()
            .saturating_mul(20);
        delta_copy(0, prefix, out)?;
        literal(
            target
                .get(prefix..target.len().saturating_sub(suffix))
                .ok_or(Error::Invalid("graph literal"))?,
            out,
        )?;
        delta_copy(base.len().saturating_sub(suffix), suffix, out)?;
        return Ok(true);
    }
    let mut start = 0usize;
    let mut equal = true;
    for (i, (a, b)) in base
        .chunks_exact(20)
        .zip(target.chunks_exact(20))
        .enumerate()
    {
        let same = a == b;
        let at = i.saturating_mul(20);
        if same != equal {
            if equal {
                delta_copy(start, at.saturating_sub(start), out)?;
            } else {
                literal(
                    target
                        .get(start..at)
                        .ok_or(Error::Invalid("graph changed span"))?,
                    out,
                )?;
            }
            start = at;
            equal = same;
        }
    }
    if equal {
        delta_copy(start, target.len().saturating_sub(start), out)?;
    } else {
        literal(
            target
                .get(start..)
                .ok_or(Error::Invalid("graph changed tail"))?,
            out,
        )?;
    }
    Ok(true)
}

fn literal(bytes: &[u8], out: &mut Vec<u8>) -> Result<()> {
    for chunk in bytes.chunks(127) {
        append(out, &[chunk.len() as u8])?;
        append(out, chunk)?;
    }
    Ok(())
}

pub struct View<'a> {
    bytes: &'a [u8],
}
pub struct Node<'a> {
    pub base: u32,
    pub size: usize,
    pub payload: &'a [u8],
}

impl<'a> View<'a> {
    pub fn parse(bytes: &'a [u8], pack: Oid) -> Result<Self> {
        let body = verify_checksum(bytes)?;
        if take(body, 0, 8)? != MAGIC || Oid::from_bytes(take(body, 12, 20)?)? != pack {
            return Err(Error::Invalid("graph identity"));
        }
        let mut cursor = HEADER;
        let mut count = 0u32;
        while cursor < body.len() {
            let size = be32(body, cursor.saturating_add(4))? as usize;
            let len = be32(body, cursor.saturating_add(8))? as usize;
            if size == 0 || !size.is_multiple_of(20) || len == 0 {
                return Err(Error::Invalid("graph dimensions"));
            }
            cursor = cursor
                .checked_add(NODE)
                .and_then(|n| n.checked_add(len))
                .ok_or(Error::Invalid("graph size overflow"))?;
            count = count
                .checked_add(1)
                .ok_or(Error::Invalid("graph count overflow"))?;
        }
        if cursor != body.len() || count != be32(body, 8)? {
            return Err(Error::Invalid("graph length or count"));
        }
        Ok(Self { bytes: body })
    }
    pub fn validated(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub fn node(&self, offset: u64) -> Result<Node<'a>> {
        let at = usize::try_from(offset).map_err(|_| Error::Invalid("graph offset"))?;
        if at < HEADER {
            return Err(Error::Invalid("graph offset before header"));
        }
        let base = be32(self.bytes, at)?;
        let size = be32(self.bytes, at.saturating_add(4))? as usize;
        let len = be32(self.bytes, at.saturating_add(8))? as usize;
        Ok(Node {
            base,
            size,
            payload: take(self.bytes, at.saturating_add(NODE), len)?,
        })
    }
}

pub struct Scratch {
    bytes: Vec<u8>,
}
impl Scratch {
    pub fn new(maximum: usize) -> Result<Self> {
        Ok(Self {
            bytes: reserve(maximum)?,
        })
    }
    pub fn read<'a>(&'a mut self, slab: &'a Slab, record: Record) -> Result<&'a [u8]> {
        if record.graph_offset == NO_GRAPH {
            return Ok(&[]);
        }
        let view = View::validated(&slab.links);
        let node = view.node(record.graph_offset)?;
        if node.base == NO_BASE {
            if node.payload.len() != node.size {
                return Err(Error::Invalid("full graph list"));
            }
            return Ok(node.payload);
        }
        let parent = slab.record(node.base)?;
        let base = view.node(parent.graph_offset)?;
        if base.base != NO_BASE || base.payload.len() != base.size {
            return Err(Error::Invalid("graph delta base"));
        }
        if node.size > self.bytes.capacity() {
            return Err(Error::Capacity("structural scratch"));
        }
        if self.bytes.len() < node.size {
            self.bytes.resize(node.size, 0);
        }
        let len = apply_delta(base.payload, node.payload, &mut self.bytes)?;
        if len != node.size || !len.is_multiple_of(20) {
            return Err(Error::Invalid("graph delta size"));
        }
        take(&self.bytes, 0, len)
    }
}
