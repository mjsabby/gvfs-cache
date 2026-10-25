//! Git binary delta application (copy/insert instruction stream).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaError {
    Truncated,
    BaseSizeMismatch,
    ResultTooLarge,
    BadCopy,
    ReservedOpcode,
    ResultSizeMismatch,
}

impl core::fmt::Display for DeltaError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Truncated => "truncated delta",
            Self::BaseSizeMismatch => "delta base size mismatch",
            Self::ResultTooLarge => "delta result exceeds buffer",
            Self::BadCopy => "delta copy out of base bounds",
            Self::ReservedOpcode => "reserved delta opcode 0",
            Self::ResultSizeMismatch => "delta result size mismatch",
        })
    }
}

impl std::error::Error for DeltaError {}

fn varint(d: &[u8], pos: &mut usize) -> Result<u64, DeltaError> {
    let mut v: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let c = *d.get(*pos).ok_or(DeltaError::Truncated)?;
        *pos = pos.saturating_add(1);
        if shift > 63 || (shift == 63 && c & 0x7F > 1) {
            return Err(DeltaError::Truncated);
        }
        v |= u64::from(c & 0x7F).wrapping_shl(shift);
        shift = shift.saturating_add(7);
        if c & 0x80 == 0 {
            return Ok(v);
        }
    }
}

/// Returns (base size, result size, header length) of a delta.
pub fn delta_sizes(delta: &[u8]) -> Result<(u64, u64, usize), DeltaError> {
    let mut pos = 0usize;
    let base = varint(delta, &mut pos)?;
    let result = varint(delta, &mut pos)?;
    Ok((base, result, pos))
}

/// Applies `delta` to `base`, writing into `out`. Returns the result length.
pub fn apply_delta(base: &[u8], delta: &[u8], out: &mut [u8]) -> Result<usize, DeltaError> {
    let (base_size, result_size, mut pos) = delta_sizes(delta)?;
    if u64::try_from(base.len()).ok() != Some(base_size) {
        return Err(DeltaError::BaseSizeMismatch);
    }
    let result_len = usize::try_from(result_size).map_err(|_| DeltaError::ResultTooLarge)?;
    let out = out
        .get_mut(..result_len)
        .ok_or(DeltaError::ResultTooLarge)?;
    let mut o = 0usize;
    while let Some(&op) = delta.get(pos) {
        pos = pos.saturating_add(1);
        if op & 0x80 != 0 {
            let mut off: usize = 0;
            let mut size: usize = 0;
            for bit in 0..4u32 {
                if op & (1u8.wrapping_shl(bit)) != 0 {
                    let b = *delta.get(pos).ok_or(DeltaError::Truncated)?;
                    pos = pos.saturating_add(1);
                    off |= usize::from(b).wrapping_shl(bit.saturating_mul(8));
                }
            }
            for bit in 0..3u32 {
                if op & (0x10u8.wrapping_shl(bit)) != 0 {
                    let b = *delta.get(pos).ok_or(DeltaError::Truncated)?;
                    pos = pos.saturating_add(1);
                    size |= usize::from(b).wrapping_shl(bit.saturating_mul(8));
                }
            }
            if size == 0 {
                size = 0x10000;
            }
            let src_end = off.checked_add(size).ok_or(DeltaError::BadCopy)?;
            let src = base.get(off..src_end).ok_or(DeltaError::BadCopy)?;
            let dst_end = o.checked_add(size).ok_or(DeltaError::ResultSizeMismatch)?;
            out.get_mut(o..dst_end)
                .ok_or(DeltaError::ResultSizeMismatch)?
                .copy_from_slice(src);
            o = dst_end;
        } else if op != 0 {
            let n = usize::from(op);
            let lit_end = pos.checked_add(n).ok_or(DeltaError::Truncated)?;
            let lit = delta.get(pos..lit_end).ok_or(DeltaError::Truncated)?;
            let dst_end = o.checked_add(n).ok_or(DeltaError::ResultSizeMismatch)?;
            out.get_mut(o..dst_end)
                .ok_or(DeltaError::ResultSizeMismatch)?
                .copy_from_slice(lit);
            o = dst_end;
            pos = lit_end;
        } else {
            return Err(DeltaError::ReservedOpcode);
        }
    }
    if o != result_len {
        return Err(DeltaError::ResultSizeMismatch);
    }
    Ok(o)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc_varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }

    #[test]
    fn copy_and_insert() {
        let base = b"hello, world";
        let mut d = Vec::new();
        enc_varint(base.len() as u64, &mut d);
        enc_varint(13, &mut d);
        d.extend_from_slice(&[0x80 | 0x01 | 0x10, 0, 5]); // copy base[0..5]
        d.extend_from_slice(&[3, b' ', b'm', b'y']); // insert " my"
        d.extend_from_slice(&[0x80 | 0x01 | 0x10, 6, 5]); // copy base[6..11]
        let mut out = [0u8; 32];
        let n = apply_delta(base, &d, &mut out).unwrap();
        assert_eq!(&out[..n], b"hello my worl");
        assert_eq!(
            apply_delta(b"short", &d, &mut out),
            Err(DeltaError::BaseSizeMismatch)
        );
        let mut small = [0u8; 4];
        assert_eq!(
            apply_delta(base, &d, &mut small),
            Err(DeltaError::ResultTooLarge)
        );
        let mut bad = d.clone();
        bad.push(0);
        assert_eq!(
            apply_delta(base, &bad, &mut out),
            Err(DeltaError::ReservedOpcode)
        );
    }
}
