use crate::arena::push;
use crate::codec::{Oid, number};
use crate::{Error, Result};

pub fn object_request(bytes: &[u8], out: &mut Vec<Oid>) -> Result<u32> {
    let mut scan = Scanner { bytes, cursor: 0 };
    out.clear();
    scan.expect(b'{')?;
    let mut ids = false;
    let mut depth_seen = false;
    let mut depth = 1u32;
    if scan.consume(b'}') {
        return Err(Error::Invalid("missing objectIds"));
    }
    loop {
        let key = scan.string()?;
        scan.expect(b':')?;
        if equal(key, b"objectIds")? {
            if ids {
                return Err(Error::Invalid("duplicate objectIds"));
            }
            ids = true;
            scan.oids(out)?;
        } else if equal(key, b"commitDepth")? {
            if depth_seen {
                return Err(Error::Invalid("duplicate commitDepth"));
            }
            depth_seen = true;
            depth = u32::try_from(scan.integer()?).map_err(|_| Error::Invalid("commitDepth"))?;
        } else {
            scan.skip(0)?;
        }
        if scan.consume(b'}') {
            break;
        }
        scan.expect(b',')?;
    }
    scan.whitespace();
    if !ids || scan.cursor != bytes.len() {
        return Err(Error::Invalid("object request JSON"));
    }
    Ok(depth)
}

pub fn sizes_request(bytes: &[u8], out: &mut Vec<Oid>) -> Result<()> {
    let mut scan = Scanner { bytes, cursor: 0 };
    out.clear();
    scan.oids(out)?;
    scan.whitespace();
    if scan.cursor != bytes.len() {
        return Err(Error::Invalid("sizes request trailing data"));
    }
    Ok(())
}

fn equal(raw: &[u8], expected: &[u8]) -> Result<bool> {
    let mut cursor = 0usize;
    for &byte in expected {
        let Some(decoded) = decoded(raw, &mut cursor)? else {
            return Ok(false);
        };
        if decoded > 127 || !(decoded as u8).eq_ignore_ascii_case(&byte) {
            return Ok(false);
        }
    }
    Ok(cursor == raw.len())
}

fn hex4(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let mut value = 0u32;
    for _ in 0..4 {
        let byte = *bytes
            .get(*cursor)
            .ok_or(Error::Invalid("JSON unicode escape"))?;
        *cursor = cursor.saturating_add(1);
        let digit = match byte {
            b'0'..=b'9' => byte.wrapping_sub(b'0'),
            b'a'..=b'f' => byte.wrapping_sub(b'a').wrapping_add(10),
            b'A'..=b'F' => byte.wrapping_sub(b'A').wrapping_add(10),
            _ => return Err(Error::Invalid("JSON unicode digit")),
        };
        value = (value << 4) | u32::from(digit);
    }
    Ok(value)
}

fn decoded(bytes: &[u8], cursor: &mut usize) -> Result<Option<u32>> {
    let Some(&byte) = bytes.get(*cursor) else {
        return Ok(None);
    };
    *cursor = cursor.saturating_add(1);
    if byte != b'\\' {
        return Ok(Some(u32::from(byte)));
    }
    let escape = *bytes.get(*cursor).ok_or(Error::Invalid("JSON escape"))?;
    *cursor = cursor.saturating_add(1);
    let value = match escape {
        b'"' | b'\\' | b'/' => u32::from(escape),
        b'b' => 8,
        b'f' => 12,
        b'n' => 10,
        b'r' => 13,
        b't' => 9,
        b'u' => {
            let first = hex4(bytes, cursor)?;
            if (0xd800..=0xdbff).contains(&first) {
                if bytes.get(*cursor..cursor.saturating_add(2)) != Some(b"\\u") {
                    return Err(Error::Invalid("JSON surrogate pair"));
                }
                *cursor = cursor.saturating_add(2);
                let second = hex4(bytes, cursor)?;
                if !(0xdc00..=0xdfff).contains(&second) {
                    return Err(Error::Invalid("JSON low surrogate"));
                }
                0x10000u32
                    .saturating_add(first.saturating_sub(0xd800).wrapping_shl(10))
                    .saturating_add(second.saturating_sub(0xdc00))
            } else if (0xdc00..=0xdfff).contains(&first) {
                return Err(Error::Invalid("unpaired JSON surrogate"));
            } else {
                first
            }
        }
        _ => return Err(Error::Invalid("JSON escape")),
    };
    Ok(Some(value))
}

struct Scanner<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Scanner<'a> {
    fn whitespace(&mut self) {
        while self
            .bytes
            .get(self.cursor)
            .is_some_and(|byte| matches!(byte, b' ' | b'\r' | b'\n' | b'\t'))
        {
            self.cursor = self.cursor.saturating_add(1);
        }
    }
    fn consume(&mut self, byte: u8) -> bool {
        self.whitespace();
        self.byte(byte)
    }
    fn byte(&mut self, byte: u8) -> bool {
        if self.bytes.get(self.cursor) == Some(&byte) {
            self.cursor = self.cursor.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn expect(&mut self, byte: u8) -> Result<()> {
        if self.consume(byte) {
            Ok(())
        } else {
            Err(Error::Invalid("JSON delimiter"))
        }
    }
    fn string(&mut self) -> Result<&'a [u8]> {
        self.expect(b'"')?;
        let start = self.cursor;
        loop {
            match self.bytes.get(self.cursor) {
                Some(b'"') => {
                    let value = self
                        .bytes
                        .get(start..self.cursor)
                        .ok_or(Error::Invalid("JSON string bounds"))?;
                    std::str::from_utf8(value).map_err(|_| Error::Invalid("JSON UTF-8"))?;
                    self.cursor = self.cursor.saturating_add(1);
                    return Ok(value);
                }
                Some(b'\\') => {
                    decoded(self.bytes, &mut self.cursor)?;
                }
                Some(byte) if *byte >= 32 => self.cursor = self.cursor.saturating_add(1),
                _ => return Err(Error::Invalid("JSON string")),
            }
        }
    }
    fn oid(&mut self) -> Result<Oid> {
        let raw = self.string()?;
        if !raw.contains(&b'\\') {
            return Oid::parse(raw);
        }
        let mut hex = [0; 40];
        let mut cursor = 0usize;
        for byte in &mut hex {
            *byte = u8::try_from(
                decoded(raw, &mut cursor)?.ok_or(Error::Invalid("short escaped OID"))?,
            )
            .map_err(|_| Error::Invalid("non-ASCII OID"))?;
        }
        if cursor != raw.len() {
            return Err(Error::Invalid("long escaped OID"));
        }
        Oid::parse(&hex)
    }
    fn oids(&mut self, out: &mut Vec<Oid>) -> Result<()> {
        self.expect(b'[')?;
        if self.consume(b']') {
            return Ok(());
        }
        loop {
            push(out, self.oid()?)?;
            if self.consume(b']') {
                return Ok(());
            }
            self.expect(b',')?;
        }
    }
    fn digits(&mut self) -> Result<()> {
        let start = self.cursor;
        while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
            self.cursor = self.cursor.saturating_add(1);
        }
        if self.cursor == start {
            return Err(Error::Invalid("JSON number digits"));
        }
        Ok(())
    }
    fn number(&mut self) -> Result<&'a [u8]> {
        self.whitespace();
        let start = self.cursor;
        self.byte(b'-');
        if !self.byte(b'0') {
            self.digits()?;
        }
        if self.byte(b'.') {
            self.digits()?;
        }
        if self.byte(b'e') || self.byte(b'E') {
            if !self.byte(b'+') {
                self.byte(b'-');
            }
            self.digits()?;
        }
        self.bytes
            .get(start..self.cursor)
            .ok_or(Error::Invalid("JSON number"))
    }
    fn integer(&mut self) -> Result<u64> {
        let raw = self.number()?;
        if raw.len() > 1 && raw.first() == Some(&b'0') {
            return Err(Error::Invalid("JSON leading zero"));
        }
        number(raw)
    }
    fn skip(&mut self, depth: u32) -> Result<()> {
        if depth >= 64 {
            return Err(Error::Invalid("JSON nesting depth"));
        }
        self.whitespace();
        match self.bytes.get(self.cursor) {
            Some(b'"') => {
                self.string()?;
            }
            Some(b'{') => {
                self.expect(b'{')?;
                if !self.consume(b'}') {
                    loop {
                        self.string()?;
                        self.expect(b':')?;
                        self.skip(depth.saturating_add(1))?;
                        if self.consume(b'}') {
                            break;
                        }
                        self.expect(b',')?;
                    }
                }
            }
            Some(b'[') => {
                self.expect(b'[')?;
                if !self.consume(b']') {
                    loop {
                        self.skip(depth.saturating_add(1))?;
                        if self.consume(b']') {
                            break;
                        }
                        self.expect(b',')?;
                    }
                }
            }
            Some(b't' | b'f' | b'n') => {
                let literal = match self.bytes.get(self.cursor) {
                    Some(b't') => b"true".as_slice(),
                    Some(b'f') => b"false".as_slice(),
                    _ => b"null".as_slice(),
                };
                let end = self.cursor.saturating_add(literal.len());
                if self.bytes.get(self.cursor..end) != Some(literal) {
                    return Err(Error::Invalid("JSON literal"));
                }
                self.cursor = end;
            }
            Some(b'-' | b'0'..=b'9') => {
                self.number()?;
            }
            _ => return Err(Error::Invalid("JSON value")),
        }
        Ok(())
    }
}
