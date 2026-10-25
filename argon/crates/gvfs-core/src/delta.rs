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
        if shift > 63 {
            return Err(DeltaError::Truncated);
        }
        v |= u64::from(c & 0x7F).wrapping_shl(shift);
        shift = shift.saturating_add(7);
        if c & 0x80 == 0 {
            return Ok(v);
        }
    }
}

pub fn delta_sizes(delta: &[u8]) -> Result<(u64, u64, usize), DeltaError> {
    let mut pos = 0usize;
    let base = varint(delta, &mut pos)?;
    let result = varint(delta, &mut pos)?;
    Ok((base, result, pos))
}

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
