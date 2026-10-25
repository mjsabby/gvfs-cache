//! Pre-allocated, cursor-reset byte arenas (the only memory the hot paths use).
//!
//! An [`Arena`] is one `Vec<u8>` allocated (and pre-faulted) once. `alloc` bumps a cursor
//! and hands back a [`Span`]; `mark`/`truncate` pop everything allocated after a point, so
//! a depth-first walk can keep a stack of objects in it; `reset` is O(1). Nothing here ever
//! grows a buffer: running out of room is an ordinary error the caller maps to a bounded
//! response (HTTP 413/507) or a retry with a larger `--arena-mib`.

use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub len: usize,
}

impl Span {
    #[inline]
    pub const fn new(start: usize, len: usize) -> Self {
        Self { start, len }
    }

    #[inline]
    pub fn end(self) -> usize {
        self.start.saturating_add(self.len)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfCapacity {
    pub requested: usize,
    pub available: usize,
}

impl fmt::Display for OutOfCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "arena out of capacity: requested {} bytes, {} available",
            self.requested, self.available
        )
    }
}

impl std::error::Error for OutOfCapacity {}

#[derive(Debug)]
pub struct Arena {
    buf: Vec<u8>,
    top: usize,
}

impl Arena {
    /// Allocates `capacity` zeroed bytes once; every page is touched here, never later.
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0u8; capacity],
            top: 0,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn used(&self) -> usize {
        self.top
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.top)
    }

    /// Grows the backing store to at least `capacity` bytes (new pages zeroed). For the
    /// pipeline only: a hot-path arena is sized once at boot and never grows.
    pub fn grow_to(&mut self, capacity: usize) {
        if capacity > self.buf.len() {
            self.buf.resize(capacity, 0);
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.top = 0;
    }

    /// Current cursor, to be handed back to [`Arena::truncate`].
    #[inline]
    pub fn mark(&self) -> usize {
        self.top
    }

    /// Frees everything allocated after `mark`.
    #[inline]
    pub fn truncate(&mut self, mark: usize) {
        self.top = mark.min(self.top);
    }

    /// Bump-allocates `len` bytes (contents undefined: whatever was there before).
    #[inline]
    pub fn alloc(&mut self, len: usize) -> Result<Span, OutOfCapacity> {
        let end = self
            .top
            .checked_add(len)
            .filter(|&e| e <= self.buf.len())
            .ok_or(OutOfCapacity {
                requested: len,
                available: self.remaining(),
            })?;
        let span = Span::new(self.top, len);
        self.top = end;
        Ok(span)
    }

    /// Allocates a copy of `data`.
    #[inline]
    pub fn push(&mut self, data: &[u8]) -> Result<Span, OutOfCapacity> {
        let span = self.alloc(data.len())?;
        self.get_mut(span).copy_from_slice(data);
        Ok(span)
    }

    #[inline]
    pub fn get(&self, span: Span) -> &[u8] {
        self.buf.get(span.start..span.end()).unwrap_or(&[])
    }

    #[inline]
    pub fn get_mut(&mut self, span: Span) -> &mut [u8] {
        self.buf.get_mut(span.start..span.end()).unwrap_or(&mut [])
    }

    /// Two disjoint spans borrowed at once (base and output of a delta application).
    #[inline]
    pub fn get_two_mut(&mut self, a: Span, b: Span) -> (&[u8], &mut [u8]) {
        if a.end() <= b.start {
            let (lo, hi) = self.buf.split_at_mut(b.start);
            (
                lo.get(a.start..a.end()).unwrap_or(&[]),
                hi.get_mut(..b.len).unwrap_or(&mut []),
            )
        } else if b.end() <= a.start {
            let (lo, hi) = self.buf.split_at_mut(a.start);
            (
                hi.get(..a.len).unwrap_or(&[]),
                lo.get_mut(b.start..b.end()).unwrap_or(&mut []),
            )
        } else {
            (&[], &mut [])
        }
    }

    /// Splits the buffer at `at`: everything below (shared) and everything from `at` on
    /// (exclusive, including the free tail). Lets a delta read its base while writing
    /// its result above it.
    #[inline]
    pub fn split_at(&mut self, at: usize) -> (&[u8], &mut [u8]) {
        let at = at.min(self.buf.len());
        let (lo, hi) = self.buf.split_at_mut(at);
        (&*lo, hi)
    }

    /// Moves `src` so that it starts at `dst` (ranges may overlap).
    #[inline]
    pub fn copy_within(&mut self, src: Span, dst: usize) {
        let end = src.end().min(self.buf.len());
        if dst.saturating_add(src.len) <= self.buf.len() && src.start <= end {
            self.buf.copy_within(src.start..end, dst);
        }
    }

    /// Everything allocated so far, in order.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.buf.get(..self.top).unwrap_or(&[])
    }

    /// The free tail, for code that fills it directly and then calls [`Arena::commit`].
    #[inline]
    pub fn free_tail_mut(&mut self) -> &mut [u8] {
        self.buf.get_mut(self.top..).unwrap_or(&mut [])
    }

    /// Marks the first `len` bytes of the free tail as allocated.
    #[inline]
    pub fn commit(&mut self, len: usize) -> Result<Span, OutOfCapacity> {
        self.alloc(len)
    }
}

/// Appends to a fixed-capacity `Vec` without ever reallocating it.
#[inline]
pub fn try_extend(v: &mut Vec<u8>, data: &[u8]) -> Result<(), OutOfCapacity> {
    let room = v.capacity().saturating_sub(v.len());
    if data.len() > room {
        return Err(OutOfCapacity {
            requested: data.len(),
            available: room,
        });
    }
    v.extend_from_slice(data);
    Ok(())
}

/// Pushes onto a fixed-capacity `Vec<T>` without ever reallocating it.
#[inline]
pub fn try_push<T>(v: &mut Vec<T>, item: T) -> Result<(), OutOfCapacity> {
    if v.len() >= v.capacity() {
        return Err(OutOfCapacity {
            requested: 1,
            available: 0,
        });
    }
    v.push(item);
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::arithmetic_side_effects)]
    use super::*;

    #[test]
    fn bump_mark_truncate_and_capacity() {
        let mut a = Arena::new(16);
        let s1 = a.push(b"abcd").unwrap();
        let m = a.mark();
        let s2 = a.push(b"efgh").unwrap();
        assert_eq!(a.get(s1), b"abcd");
        assert_eq!(a.get(s2), b"efgh");
        assert_eq!(a.as_slice(), b"abcdefgh");
        let (x, y) = a.get_two_mut(s1, s2);
        y.copy_from_slice(x);
        assert_eq!(a.get(s2), b"abcd");
        a.truncate(m);
        assert_eq!(a.used(), 4);
        assert!(a.alloc(13).is_err());
        assert!(a.alloc(12).is_ok());
        assert_eq!(a.remaining(), 0);
        a.reset();
        assert_eq!(a.used(), 0);
        let mut v = Vec::with_capacity(4);
        assert!(try_extend(&mut v, b"abcd").is_ok());
        assert!(try_extend(&mut v, b"e").is_err());
        assert_eq!(v.capacity(), 4);
    }
}
