use crate::{Error, Result};

pub fn reserve<T>(capacity: usize) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| Error::Capacity("startup allocation"))?;
    Ok(values)
}

pub fn zeroed(capacity: usize) -> Result<Vec<u8>> {
    let mut values = reserve(capacity)?;
    values.resize(capacity, 0);
    Ok(values)
}

#[inline]
pub fn push<T>(values: &mut Vec<T>, value: T) -> Result<()> {
    if values.len() == values.capacity() {
        return Err(Error::Capacity("arena elements"));
    }
    values.push(value);
    Ok(())
}

#[inline]
pub fn append(values: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if bytes.len() > values.capacity().saturating_sub(values.len()) {
        return Err(Error::Capacity("arena bytes"));
    }
    values.extend_from_slice(bytes);
    Ok(())
}

pub struct Writer<'a> {
    out: &'a mut [u8],
    cursor: usize,
}

impl<'a> Writer<'a> {
    pub fn new(out: &'a mut [u8]) -> Self {
        Self { out, cursor: 0 }
    }

    pub fn put(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self
            .cursor
            .checked_add(bytes.len())
            .ok_or(Error::Capacity("writer"))?;
        self.out
            .get_mut(self.cursor..end)
            .ok_or(Error::Capacity("writer"))?
            .copy_from_slice(bytes);
        self.cursor = end;
        Ok(())
    }

    pub fn decimal(&mut self, mut value: u64) -> Result<()> {
        let mut digits = [0u8; 20];
        let mut start = digits.len();
        loop {
            start = start.saturating_sub(1);
            *digits.get_mut(start).ok_or(Error::Capacity("decimal"))? =
                b'0'.wrapping_add((value % 10) as u8);
            value /= 10;
            if value == 0 {
                break;
            }
        }
        self.put(digits.get(start..).ok_or(Error::Capacity("decimal"))?)
    }

    pub fn written(&self) -> &[u8] {
        // The cursor is advanced only after a successful bounds-checked write.
        self.out.get(..self.cursor).unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.cursor
    }

    pub fn is_empty(&self) -> bool {
        self.cursor == 0
    }
}
