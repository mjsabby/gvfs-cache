//! Zero-allocation JSON for the two request bodies the GVFS protocol uses:
//!
//! * `POST /gvfs/objects`: `{"commitDepth": 1, "objectIds": ["<40 hex>", ...]}`
//! * `POST /gvfs/sizes`:   `["<40 hex>", ...]`
//!
//! Both reduce to "every quoted 40-hex-digit string is an object id" plus one integer
//! after `"commitDepth"`, so a byte scanner over the body is all that is needed. Object ids
//! are decoded straight into a span of the request arena.

use crate::arena::{Arena, Span};
use crate::oid::{Oid, decode_hex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectsRequest {
    /// `count` decoded 20-byte ids, back to back.
    pub oids: Span,
    pub count: usize,
    pub commit_depth: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonError {
    Malformed,
    TooManyIds,
}

/// Scans the request body at `body` (the arena's last allocation) for object ids and
/// `commitDepth`, writing the decoded ids right above it.
pub fn scan_object_ids(arena: &mut Arena, body: Span) -> Result<ObjectsRequest, JsonError> {
    let oids_start = body.end();
    if arena.mark() != oids_start {
        return Err(JsonError::Malformed);
    }
    let (count, depth) = {
        let (lo, free) = arena.split_at(oids_start);
        let body = lo.get(body.start..body.end()).ok_or(JsonError::Malformed)?;
        scan_into(body, free)?
    };
    arena
        .commit(count.saturating_mul(20))
        .map_err(|_| JsonError::TooManyIds)?;
    Ok(ObjectsRequest {
        oids: Span::new(oids_start, count.saturating_mul(20)),
        count,
        commit_depth: depth,
    })
}

fn scan_into(body: &[u8], out: &mut [u8]) -> Result<(usize, u32), JsonError> {
    let mut depth: u32 = 1;
    let mut count = 0usize;
    let mut i = 0usize;
    let n = body.len();
    while i < n {
        let c = body.get(i).copied().unwrap_or(0);
        if c == b'"' {
            let close = body
                .get(i.saturating_add(1)..)
                .and_then(|rest| rest.iter().position(|&b| b == b'"'))
                .ok_or(JsonError::Malformed)?;
            let s = body
                .get(i.saturating_add(1)..i.saturating_add(1).saturating_add(close))
                .unwrap_or(&[]);
            if s.len() == 40 {
                let at = count.saturating_mul(20);
                let dst = out
                    .get_mut(at..at.saturating_add(20))
                    .ok_or(JsonError::TooManyIds)?;
                if decode_hex(s, dst).is_some() {
                    count = count.saturating_add(1);
                }
            } else if s == b"commitDepth" {
                let mut j = i.saturating_add(1).saturating_add(close).saturating_add(1);
                while body
                    .get(j)
                    .is_some_and(|b| *b == b':' || b.is_ascii_whitespace())
                {
                    j = j.saturating_add(1);
                }
                let mut v: u32 = 0;
                let mut any = false;
                while let Some(&d) = body.get(j) {
                    if !d.is_ascii_digit() {
                        break;
                    }
                    v = v
                        .saturating_mul(10)
                        .saturating_add(u32::from(d.wrapping_sub(b'0')));
                    any = true;
                    j = j.saturating_add(1);
                }
                if any {
                    depth = v;
                }
            }
            i = i.saturating_add(1).saturating_add(close).saturating_add(1);
        } else {
            i = i.saturating_add(1);
        }
    }
    Ok((count, depth))
}

#[inline]
pub fn oid_at(arena: &Arena, req: &ObjectsRequest, i: usize) -> Option<Oid> {
    let at = req.oids.start.saturating_add(i.saturating_mul(20));
    Oid::from_slice(arena.get(Span::new(at, 20)))
}

/// Appends `v` in decimal to `out`; returns bytes written.
pub fn put_decimal(v: u64, out: &mut [u8]) -> usize {
    let mut digits = [0u8; 20];
    let mut d = digits.len();
    let mut x = v;
    loop {
        d = d.saturating_sub(1);
        if let Some(s) = digits.get_mut(d) {
            *s = b'0'.wrapping_add((x % 10) as u8);
        }
        x /= 10;
        if x == 0 {
            break;
        }
    }
    let src = digits.get(d..).unwrap_or(&[]);
    for (o, s) in out.iter_mut().zip(src) {
        *o = *s;
    }
    src.len().min(out.len())
}

pub fn decimal_len(v: u64) -> usize {
    let mut n = 1usize;
    let mut x = v / 10;
    while x > 0 {
        n = n.saturating_add(1);
        x /= 10;
    }
    n
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn scans_objects_and_sizes_bodies() {
        let mut a = Arena::new(4096);
        let body = br#"{"commitDepth": 3, "objectIds": ["0123456789abcdef0123456789abcdef01234567", "not-an-oid", "ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD"]}"#;
        let span = a.push(body).unwrap();
        let r = scan_object_ids(&mut a, span).unwrap();
        assert_eq!((r.count, r.commit_depth), (2, 3));
        assert_eq!(
            oid_at(&a, &r, 0).unwrap().to_string(),
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(
            oid_at(&a, &r, 1).unwrap().to_string(),
            "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        a.reset();
        let span = a
            .push(br#"["0123456789abcdef0123456789abcdef01234567"]"#)
            .unwrap();
        let r = scan_object_ids(&mut a, span).unwrap();
        assert_eq!((r.count, r.commit_depth), (1, 1));
        a.reset();
        let span = a.push(br#"["0123"#).unwrap();
        assert_eq!(scan_object_ids(&mut a, span), Err(JsonError::Malformed));
        let mut tiny = Arena::new(120);
        let body = br#"["0123456789abcdef0123456789abcdef01234567","0123456789abcdef0123456789abcdef01234567"]"#;
        let span = tiny.push(body).unwrap();
        assert_eq!(scan_object_ids(&mut tiny, span), Err(JsonError::TooManyIds));
        let mut out = [0u8; 20];
        let n = put_decimal(0, &mut out);
        assert_eq!(&out[..n], b"0");
        let n = put_decimal(1234567, &mut out);
        assert_eq!(&out[..n], b"1234567");
        assert_eq!(decimal_len(0), 1);
        assert_eq!(decimal_len(999), 3);
        assert_eq!(decimal_len(1000), 4);
    }
}
