//! Zero-allocation JSON scanning (design §1: "state-machine byte scanner").
//!
//! The scanner walks a `&[u8]` with a cursor and hands out borrowed string spans. It
//! never builds a DOM, never allocates, bounds nesting depth, and validates structure
//! strictly enough to reject truncated or malformed bodies.

use crate::buf::{ByteWriter, Overflow, parse_dec_u64};
use crate::oid::Oid20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonError {
    UnexpectedEnd,
    Unexpected {
        at: usize,
    },
    TooDeep,
    BadOid {
        at: usize,
    },
    BadNumber {
        at: usize,
    },
    /// The caller's pre-allocated sink refused another element.
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

/// A string token: the raw bytes between the quotes (escapes not yet decoded).
#[derive(Clone, Copy, Debug)]
pub struct JsonStr<'a> {
    pub raw: &'a [u8],
    pub escaped: bool,
    pub at: usize,
}

impl JsonStr<'_> {
    /// Compares the decoded string with `expected`.
    pub fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.compare(expected, false)
    }

    /// Compares the decoded string with `expected`, ignoring ASCII case.
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

/// Decodes the escape at `raw[i]` (which is a backslash). Only ASCII results are
/// supported, which is all that object ids, ref names and protocol keys ever contain.
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

    /// True once only whitespace remains.
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

    /// Raw number token.
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

    /// Non-negative integer value.
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

    /// Skips any value (bounded nesting).
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

    /// Iterates the members of an object; `f` must consume each member's value.
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

    /// Iterates the elements of an array; `f` must consume each element.
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

    /// A string value that must be a 40-hex object id.
    pub fn oid(&mut self) -> Result<Oid20, JsonError> {
        self.skip_ws();
        // Fast path: `"` + 40 hex digits + `"` (hex digits are never quotes or escapes, so
        // this accepts exactly what the general path accepts for such input).
        if let Some(end) = self.pos.checked_add(42)
            && let Some([b'"', body @ .., b'"']) = self.buf.get(self.pos..end)
            && let Some(oid) = Oid20::from_hex(body)
        {
            self.pos = end;
            return Ok(oid);
        }
        let s = self.string()?;
        if s.escaped {
            return Err(JsonError::BadOid { at: s.at });
        }
        Oid20::from_hex(s.raw).ok_or(JsonError::BadOid { at: s.at })
    }
}

/// Parsed scalar parameters of a `POST /gvfs/objects` body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectsRequest {
    pub commit_depth: u64,
    pub object_count: usize,
}

/// Scans `{"commitDepth":N,"objectIds":["<hex>",...]}` (keys in any order, unknown keys
/// skipped). `on_oid` returns false when its pre-allocated sink is full.
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

/// Scans a bare array of object id strings (`POST /gvfs/sizes`).
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

/// Scans an Azure DevOps `GET .../refs?filter=...` response and returns the `objectId`
/// of the entry whose `name` is exactly `ref_name` (the filter is a prefix match).
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

/// Scans a `/gvfs/sizes` response: `[{"Id":"<hex>","Size":N},...]`.
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

/// Writes `s` as a JSON string literal (with quotes), escaping as required.
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

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn objects_request_compact_and_pretty() {
        let compact = format!(r#"{{"commitDepth":3,"objectIds":["{A}","{B}"]}}"#);
        let mut got = Vec::new();
        let req = scan_objects_request(compact.as_bytes(), |o| {
            got.push(o);
            true
        })
        .unwrap();
        assert_eq!(
            req,
            ObjectsRequest {
                commit_depth: 3,
                object_count: 2
            }
        );
        assert_eq!(got[1], Oid20::from_hex(B.as_bytes()).unwrap());

        let pretty = format!(
            "{{\n  \"objectIds\" : [\n    \"{A}\" ,\n \"{B}\"\n  ],\n  \"extra\": {{\"x\":[1,2,{{}}]}},\n  \"commitDepth\" : 1\n}}\n"
        );
        let req = scan_objects_request(pretty.as_bytes(), |_| true).unwrap();
        assert_eq!(req.object_count, 2);
        assert_eq!(req.commit_depth, 1);
    }

    #[test]
    fn objects_request_rejects_bad_input() {
        assert!(scan_objects_request(br#"{"objectIds":["abc"]}"#, |_| true).is_err());
        assert!(scan_objects_request(br#"{"objectIds":["#, |_| true).is_err());
        let body = format!(r#"{{"objectIds":["{A}","{B}"]}}"#);
        assert_eq!(
            scan_objects_request(body.as_bytes(), {
                let mut n = 0;
                move |_| {
                    n += 1;
                    n < 2
                }
            }),
            Err(JsonError::Capacity)
        );
        assert!(scan_objects_request(br#"{"commitDepth":-1}"#, |_| true).is_err());
        let deep = "[".repeat(200);
        assert!(scan_objects_request(format!(r#"{{"x":{deep}}}"#).as_bytes(), |_| true).is_err());
        let trailing = format!(r#"{{"objectIds":["{A}"]}} x"#);
        assert!(scan_objects_request(trailing.as_bytes(), |_| true).is_err());
    }

    #[test]
    fn ado_refs_exact_name_match() {
        let body = format!(
            r#"{{"value":[{{"name":"refs/heads/main2","objectId":"{B}"}},{{"objectId":"{A}","name":"refs\/heads\/main","creator":{{"displayName":"x \"q\""}}}}],"count":2}}"#
        );
        let got = scan_ado_refs(body.as_bytes(), b"refs/heads/main").unwrap();
        assert_eq!(got, Oid20::from_hex(A.as_bytes()));
        assert_eq!(
            scan_ado_refs(body.as_bytes(), b"refs/heads/dev").unwrap(),
            None
        );
    }

    #[test]
    fn sizes_roundtrip() {
        let body = format!(r#"[ {{"Id":"{A}","Size":123}}, {{"id":"{B}","size":456}} ]"#);
        let mut got = Vec::new();
        scan_sizes_response(body.as_bytes(), |o, s| {
            got.push((o, s));
            true
        })
        .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].1, 456);
        let mut n = 0;
        scan_oid_array(format!(r#"["{A}","{B}"]"#).as_bytes(), |_| {
            n += 1;
            true
        })
        .unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn string_escaping() {
        let mut buf = [0u8; 64];
        let mut w = ByteWriter::new(&mut buf);
        write_json_string(&mut w, b"a\"b\\c\n\x01").unwrap();
        assert_eq!(w.written(), br#""a\"b\\c\n\u0001""#);
    }
}
