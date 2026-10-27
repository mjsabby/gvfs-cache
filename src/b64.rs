//! Standard base64 (RFC 4648, with padding).

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn sextet(i: u32) -> char {
    char::from(ALPHABET.get((i & 63) as usize).copied().unwrap_or(b'A'))
}

pub fn encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3).saturating_mul(4));
    for chunk in data.chunks(3) {
        let (b0, b1, b2) = match *chunk {
            [a, b, c] => (a, b, c),
            [a, b] => (a, b, 0),
            [a] => (a, 0, 0),
            _ => continue,
        };
        let n = u32::from(b0).wrapping_shl(16) | u32::from(b1).wrapping_shl(8) | u32::from(b2);
        out.push(sextet(n.wrapping_shr(18)));
        out.push(sextet(n.wrapping_shr(12)));
        out.push(if chunk.len() > 1 {
            sextet(n.wrapping_shr(6))
        } else {
            '='
        });
        out.push(if chunk.len() > 2 { sextet(n) } else { '=' });
    }
    out
}

fn value(c: u8) -> Option<u32> {
    let v = match c {
        b'A'..=b'Z' => c.wrapping_sub(b'A'),
        b'a'..=b'z' => c.wrapping_sub(b'a').wrapping_add(26),
        b'0'..=b'9' => c.wrapping_sub(b'0').wrapping_add(52),
        b'+' | b'-' => 62,
        b'/' | b'_' => 63,
        _ => return None,
    };
    Some(u32::from(v))
}

/// Decodes standard or URL-safe base64; padding optional, whitespace not allowed.
pub fn decode(s: &str) -> Option<Vec<u8>> {
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len().saturating_mul(3) / 4);
    for chunk in s.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= value(c)?.wrapping_shl(18u32.wrapping_sub(6u32.wrapping_mul(i as u32)));
        }
        let bytes = n.to_be_bytes();
        let keep = match chunk.len() {
            4 => 3,
            3 => 2,
            2 => 1,
            _ => return None,
        };
        out.extend_from_slice(bytes.get(1..1usize.wrapping_add(keep)).unwrap_or(&[]));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for (plain, enc) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foobar", "Zm9vYmFy"),
            (":pat", "OnBhdA=="),
        ] {
            assert_eq!(encode(plain.as_bytes()), enc);
            assert_eq!(decode(enc).unwrap(), plain.as_bytes());
        }
        let bin: Vec<u8> = (0..=255u8).collect();
        assert_eq!(decode(&encode(&bin)).unwrap(), bin);
        assert!(decode("Zm9v!").is_none());
    }
}
