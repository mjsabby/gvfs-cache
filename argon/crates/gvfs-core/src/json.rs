//! Zero-allocation JSON state-machine byte scanner (DESIGN.md §1).

use crate::buf::{ByteWriter, Overflow, parse_dec_u64};
use crate::oid::Oid20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonError {
    UnexpectedEnd,
    Unexpected { at: usize },
    TooDeep,
    BadOid { at: usize },
    BadNumber { at: usize },
    Capacity,
}

impl core::fmt::Display for JsonError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnexpectedEnd => f.write_str("unexpected end of JSON"),
            Self::Unexpected { at } => write!(f, "unexpected JSON byte at offset {at}"),
            Self::TooDeep => f.write_str("JSON nesting too deep"),
            Self::BadOid { at } => write!(f, "invalid object id at offset {at}"),
            Self::BadNumber { at } => write!(f, "invalid number at offset {at}"),
            Self::Capacity => f.write_str("too many object ids"),
        }
    }
}

impl std::error::Error for JsonError {}

const MAX_DEPTH: u32 = 64;

#[derive(Clone, Copy, Debug)]
pub struct JsonStr<'a> {
    pub raw: &'a [u8],
    pub escaped: bool,
    pub at: usize,
}

impl JsonStr<'_> {
    pub fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.compare(expected, false)
    }

    pub fn eq_ignore_case(&self, expected: &[u8]) -> bool {
        self.compare(expected, true)
    }

    fn compare(&self, expected: &[u8], fold: bool) -> bool {
        let eq = |a: u8, b: u8| {
            if fold {
                a.eq_ignore_ascii_case(&b)
            } else {
                a == b
            }
        };
        if !self.escaped {
            return self.raw.len() == expected.len()
                && self.raw.iter().zip(expected).all(|(&a, &b)| eq(a, b));
        }
        let mut exp = expected.iter();
        let mut i = 0usize;
        while let Some(&c) = self.raw.get(i) {
            let (decoded, used) = if c == b'\\' {
                match decode_escape(self.raw, i) {
                    Some(v) => v,
                    None => return false,
                }
            } else {
                (c, 1)
            };
            match exp.next() {
                Some(&e) if eq(decoded, e) => {}
                _ => return false,
            }
            i = i.saturating_add(used);
        }
        exp.next().is_none()
    }
}

fn decode_escape(raw: &[u8], i: usize) -> Option<(u8, usize)> {
    let c = *raw.get(i.checked_add(1)?)?;
    let simple = match c {
        b'"' => Some(b'"'),
        b'\\' => Some(b'\\'),
        b'/' => Some(b'/'),
        b'b' => Some(0x08),
        b'f' => Some(0x0C),
        b'n' => Some(b'\n'),
        b'r' => Some(b'\r'),
        b't' => Some(b'\t'),
        _ => None,
    };
    if let Some(v) = simple {
        return Some((v, 2));
    }
    if c != b'u' {
        return None;
    }
    let start = i.checked_add(2)?;
    let hex = raw.get(start..start.checked_add(4)?)?;
    let mut v: u32 = 0;
    for &h in hex {
        let n = crate::oid::HEX_LUT_ANY
            .get(usize::from(h))
            .copied()
            .unwrap_or(0xFF);
        if n > 15 {
            return None;
        }
        v = v.wrapping_shl(4) | u32::from(n);
    }
    let byte = u8::try_from(v).ok().filter(|b| b.is_ascii())?;
    Some((byte, 6))
}

#[derive(Debug)]
pub struct Scanner<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Scanner<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    #[inline]
    fn bump(&mut self) {
        self.pos = self.pos.saturating_add(1);
    }

    pub fn skip_ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.peek() {
            self.bump();
        }
    }

    fn err(&self) -> JsonError {
        if self.pos >= self.buf.len() {
            JsonError::UnexpectedEnd
        } else {
            JsonError::Unexpected { at: self.pos }
        }
    }

    fn expect(&mut self, c: u8) -> Result<(), JsonError> {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.bump();
            Ok(())
        } else {
            Err(self.err())
        }
    }

    pub fn at_end(&mut self) -> bool {
        self.skip_ws();
        self.pos >= self.buf.len()
    }

    pub fn string(&mut self) -> Result<JsonStr<'a>, JsonError> {
        self.expect(b'"')?;
        let start = self.pos;
        let mut escaped = false;
        loop {
            match self.peek() {
                None => return Err(JsonError::UnexpectedEnd),
                Some(b'"') => {
                    let raw = self
                        .buf
                        .get(start..self.pos)
                        .ok_or(JsonError::UnexpectedEnd)?;
                    self.bump();
                    return Ok(JsonStr {
                        raw,
                        escaped,
                        at: start,
                    });
                }
                Some(b'\\') => {
                    escaped = true;
                    self.bump();
                    if self.peek().is_none() {
                        return Err(JsonError::UnexpectedEnd);
                    }
                    self.bump();
                }
                Some(c) if c < 0x20 => return Err(JsonError::Unexpected { at: self.pos }),
                Some(_) => self.bump(),
            }
        }
    }

    pub fn number(&mut self) -> Result<&'a [u8], JsonError> {
        self.skip_ws();
        let start = self.pos;
        while let Some(b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') = self.peek() {
            self.bump();
        }
        if start == self.pos {
            return Err(self.err());
        }
        self.buf
            .get(start..self.pos)
            .ok_or(JsonError::UnexpectedEnd)
    }

    pub fn uint(&mut self) -> Result<u64, JsonError> {
        self.skip_ws();
        let at = self.pos;
        let raw = self.number()?;
        parse_dec_u64(raw).ok_or(JsonError::BadNumber { at })
    }

    fn literal(&mut self, lit: &[u8]) -> Result<(), JsonError> {
        self.skip_ws();
        let end = self
            .pos
            .checked_add(lit.len())
            .ok_or(JsonError::UnexpectedEnd)?;
        match self.buf.get(self.pos..end) {
            Some(s) if s == lit => {
                self.pos = end;
                Ok(())
            }
            Some(_) => Err(JsonError::Unexpected { at: self.pos }),
            None => Err(JsonError::UnexpectedEnd),
        }
    }

    pub fn skip_value(&mut self) -> Result<(), JsonError> {
        self.skip_value_depth(0)
    }

    fn skip_value_depth(&mut self, depth: u32) -> Result<(), JsonError> {
        if depth > MAX_DEPTH {
            return Err(JsonError::TooDeep);
        }
        let next = depth.saturating_add(1);
        self.skip_ws();
        match self.peek() {
            Some(b'"') => self.string().map(|_| ()),
            Some(b'{') => self.object(|s, _| s.skip_value_depth(next)),
            Some(b'[') => self.array(|s| s.skip_value_depth(next)),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(b'-' | b'0'..=b'9') => self.number().map(|_| ()),
            _ => Err(self.err()),
        }
    }

    pub fn object<F>(&mut self, mut f: F) -> Result<(), JsonError>
    where
        F: FnMut(&mut Self, JsonStr<'a>) -> Result<(), JsonError>,
    {
        self.expect(b'{')?;
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(());
        }
        loop {
            let key = self.string()?;
            self.expect(b':')?;
            f(self, key)?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.bump(),
                Some(b'}') => {
                    self.bump();
                    return Ok(());
                }
                _ => return Err(self.err()),
            }
        }
    }

    pub fn array<F>(&mut self, mut f: F) -> Result<(), JsonError>
    where
        F: FnMut(&mut Self) -> Result<(), JsonError>,
    {
        self.expect(b'[')?;
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(());
        }
        loop {
            f(self)?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.bump(),
                Some(b']') => {
                    self.bump();
                    return Ok(());
                }
                _ => return Err(self.err()),
            }
        }
    }

    pub fn oid(&mut self) -> Result<Oid20, JsonError> {
        self.skip_ws();
        if let Some(end) = self.pos.checked_add(42) {
            if let Some([b'"', body @ .., b'"']) = self.buf.get(self.pos..end) {
                if let Some(oid) = Oid20::from_hex(body) {
                    self.pos = end;
                    return Ok(oid);
                }
            }
        }
        let s = self.string()?;
        if s.escaped {
            return Err(JsonError::BadOid { at: s.at });
        }
        Oid20::from_hex(s.raw).ok_or(JsonError::BadOid { at: s.at })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectsRequest {
    pub commit_depth: u64,
    pub object_count: usize,
}

pub fn scan_objects_request<F>(body: &[u8], mut on_oid: F) -> Result<ObjectsRequest, JsonError>
where
    F: FnMut(Oid20) -> bool,
{
    let mut s = Scanner::new(body);
    let mut req = ObjectsRequest {
        commit_depth: 1,
        object_count: 0,
    };
    s.object(|s, key| {
        if key.eq_ignore_case(b"objectIds") {
            s.array(|s| {
                let oid = s.oid()?;
                if !on_oid(oid) {
                    return Err(JsonError::Capacity);
                }
                req.object_count = req.object_count.saturating_add(1);
                Ok(())
            })
        } else if key.eq_ignore_case(b"commitDepth") {
            req.commit_depth = s.uint()?;
            Ok(())
        } else {
            s.skip_value()
        }
    })?;
    if !s.at_end() {
        return Err(s.err());
    }
    Ok(req)
}

pub fn scan_oid_array<F>(body: &[u8], mut on_oid: F) -> Result<usize, JsonError>
where
    F: FnMut(Oid20) -> bool,
{
    let mut s = Scanner::new(body);
    let mut n = 0usize;
    s.array(|s| {
        let oid = s.oid()?;
        if !on_oid(oid) {
            return Err(JsonError::Capacity);
        }
        n = n.saturating_add(1);
        Ok(())
    })?;
    if !s.at_end() {
        return Err(s.err());
    }
    Ok(n)
}

pub fn scan_ado_refs(body: &[u8], ref_name: &[u8]) -> Result<Option<Oid20>, JsonError> {
    let mut s = Scanner::new(body);
    let mut found = None;
    s.object(|s, key| {
        if !key.eq_bytes(b"value") {
            return s.skip_value();
        }
        s.array(|s| {
            let mut name_matches = false;
            let mut object_id = None;
            s.object(|s, k| {
                if k.eq_bytes(b"name") {
                    name_matches = s.string()?.eq_bytes(ref_name);
                    Ok(())
                } else if k.eq_bytes(b"objectId") {
                    object_id = Some(s.oid()?);
                    Ok(())
                } else {
                    s.skip_value()
                }
            })?;
            if name_matches && object_id.is_some() {
                found = object_id;
            }
            Ok(())
        })
    })?;
    Ok(found)
}

pub fn scan_sizes_response<F>(body: &[u8], mut on_entry: F) -> Result<usize, JsonError>
where
    F: FnMut(Oid20, u64) -> bool,
{
    let mut s = Scanner::new(body);
    let mut n = 0usize;
    s.array(|s| {
        let mut id = None;
        let mut size = None;
        s.object(|s, k| {
            if k.eq_ignore_case(b"Id") {
                id = Some(s.oid()?);
                Ok(())
            } else if k.eq_ignore_case(b"Size") {
                size = Some(s.uint()?);
                Ok(())
            } else {
                s.skip_value()
            }
        })?;
        if let (Some(id), Some(size)) = (id, size) {
            if !on_entry(id, size) {
                return Err(JsonError::Capacity);
            }
            n = n.saturating_add(1);
        }
        Ok(())
    })?;
    Ok(n)
}

pub fn write_json_string(w: &mut ByteWriter<'_>, s: &[u8]) -> Result<(), Overflow> {
    w.put_u8(b'"')?;
    for &c in s {
        match c {
            b'"' => w.put(b"\\\"")?,
            b'\\' => w.put(b"\\\\")?,
            b'\n' => w.put(b"\\n")?,
            b'\r' => w.put(b"\\r")?,
            b'\t' => w.put(b"\\t")?,
            c if c < 0x20 => {
                let mut esc = *b"\\u0000";
                let hex = b"0123456789abcdef";
                if let Some(d) = esc.get_mut(4) {
                    *d = hex
                        .get(usize::from(c.wrapping_shr(4)))
                        .copied()
                        .unwrap_or(b'0');
                }
                if let Some(d) = esc.get_mut(5) {
                    *d = hex.get(usize::from(c & 0x0F)).copied().unwrap_or(b'0');
                }
                w.put(&esc)?;
            }
            c => w.put_u8(c)?,
        }
    }
    w.put_u8(b'"')
}
