use crate::buf::ByteWriter;
use crate::{Error, Oid20, Result};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

pub(super) const LIMIT: usize = 16384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PackFile {
    pub hash: Oid20,
    pub idx_hash: Oid20,
    pub metadata_hash: Oid20,
    pub count: u32,
    pub pack_len: u64,
    pub idx_len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    Anchor,
    Delta,
}
impl Role {
    pub fn prefix(self) -> &'static str {
        match self {
            Self::Anchor => "gvfs-base",
            Self::Delta => "gvfs-delta",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct State {
    pub server: String,
    pub sequence: u64,
    pub cursor: u64,
    pub etag: Option<Oid20>,
    pub tip: Option<Oid20>,
    pub anchor: Option<PackFile>,
    pub delta: Option<PackFile>,
    pub retired: Option<PackFile>,
}

impl State {
    pub fn new(server: String) -> Self {
        Self {
            server,
            sequence: 0,
            cursor: 0,
            etag: None,
            tip: None,
            anchor: None,
            delta: None,
            retired: None,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![0; LIMIT];
        let mut w = ByteWriter::new(&mut bytes);
        w.put(b"GVFSCL01")?;
        w.put(&(self.server.len() as u32).to_le_bytes())?;
        w.put(self.server.as_bytes())?;
        w.put(&self.sequence.to_le_bytes())?;
        w.put(&self.cursor.to_le_bytes())?;
        w.put_u8(u8::from(self.etag.is_some()))?;
        if let Some(tag) = self.etag {
            w.put(&tag.0)?;
        }
        w.put_u8(u8::from(self.tip.is_some()))?;
        if let Some(tip) = self.tip {
            w.put(&tip.0)?;
        }
        for pack in [self.anchor, self.delta, self.retired] {
            w.put_u8(u8::from(pack.is_some()))?;
            if let Some(pack) = pack {
                w.put(&pack.hash.0)?;
                w.put(&pack.idx_hash.0)?;
                w.put(&pack.metadata_hash.0)?;
                w.put(&pack.count.to_le_bytes())?;
                w.put(&pack.pack_len.to_le_bytes())?;
                w.put(&pack.idx_len.to_le_bytes())?;
            }
        }
        let hash = crate::sha1_parts(&[w.written()]);
        w.put(&hash)?;
        let len = w.len();
        bytes.truncate(len);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verified(bytes)?;
        let mut r = Reader(body);
        if r.take(8)? != b"GVFSCL01" {
            return Err(Error::Invalid("client state version"));
        }
        let len = r.u32()? as usize;
        let server = std::str::from_utf8(r.take(len)?)
            .map_err(|_| Error::Invalid("state URL"))?
            .to_owned();
        let sequence = r.u64()?;
        let cursor = r.u64()?;
        let etag = if r.flag()? { Some(r.oid()?) } else { None };
        let tip = if r.flag()? { Some(r.oid()?) } else { None };
        let anchor = r.pack()?;
        let delta = r.pack()?;
        let retired = r.pack()?;
        if !r.0.is_empty()
            || (anchor.is_none() && (delta.is_some() || retired.is_some() || cursor != 0))
            || delta.is_some_and(|d| retired.is_some_and(|r| d.hash == r.hash))
        {
            return Err(Error::Invalid("inconsistent client state"));
        }
        Ok(Self {
            server,
            sequence,
            cursor,
            etag,
            tip,
            anchor,
            delta,
            retired,
        })
    }
}

pub(super) fn read(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::metadata(path) {
        Ok(meta) => {
            if meta.len() > LIMIT as u64 {
                return Err(Error::Capacity("client state"));
            }
            Ok(Some(fs::read(path)?))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(super) fn save(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or(Error::Invalid("state path"))?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|e| Error::Io(e.error))?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn transaction(role: Role, state: &State) -> Result<Vec<u8>> {
    let state = state.encode()?;
    let mut bytes = Vec::with_capacity(state.len().saturating_add(29));
    bytes.extend_from_slice(b"GVFSTX01");
    bytes.push(match role {
        Role::Anchor => 1,
        Role::Delta => 2,
    });
    bytes.extend_from_slice(&state);
    let hash = crate::sha1_parts(&[&bytes]);
    bytes.extend_from_slice(&hash);
    Ok(bytes)
}

pub(super) fn parse_transaction(bytes: &[u8]) -> Result<(Role, State)> {
    let mut r = Reader(verified(bytes)?);
    if r.take(8)? != b"GVFSTX01" {
        return Err(Error::Invalid("client transaction version"));
    }
    let role = match r.take(1)? {
        [1] => Role::Anchor,
        [2] => Role::Delta,
        _ => return Err(Error::Invalid("client transaction role")),
    };
    let state = State::decode(r.0)?;
    if match role {
        Role::Anchor => state.anchor.is_none(),
        Role::Delta => state.delta.is_none(),
    } {
        return Err(Error::Invalid("transaction without replacement"));
    }
    Ok((role, state))
}

fn verified(bytes: &[u8]) -> Result<&[u8]> {
    let at = bytes
        .len()
        .checked_sub(20)
        .ok_or(Error::Invalid("client state checksum"))?;
    let body = bytes.get(..at).ok_or(Error::Invalid("client state"))?;
    if bytes.get(at..) != Some(&crate::sha1_parts(&[body])) {
        return Err(Error::Invalid("client state checksum"));
    }
    Ok(body)
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let result = self
            .0
            .get(..n)
            .ok_or(Error::Invalid("truncated client state"))?;
        self.0 = self
            .0
            .get(n..)
            .ok_or(Error::Invalid("truncated client state"))?;
        Ok(result)
    }
    fn flag(&mut self) -> Result<bool> {
        match self.take(1)? {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(Error::Invalid("client state flag")),
        }
    }
    fn oid(&mut self) -> Result<Oid20> {
        Oid20::from_slice(self.take(20)?).ok_or(Error::Invalid("state OID"))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| Error::Invalid("state integer"))?,
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| Error::Invalid("state integer"))?,
        ))
    }
    fn pack(&mut self) -> Result<Option<PackFile>> {
        if !self.flag()? {
            return Ok(None);
        }
        let pack = PackFile {
            hash: self.oid()?,
            idx_hash: self.oid()?,
            metadata_hash: self.oid()?,
            count: self.u32()?,
            pack_len: self.u64()?,
            idx_len: self.u64()?,
        };
        if pack.count == 0 || pack.pack_len < 32 || pack.idx_len < 1072 {
            return Err(Error::Invalid("client pack dimensions"));
        }
        Ok(Some(pack))
    }
}
