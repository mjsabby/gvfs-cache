//! Staging memory for generated response frames.
//!
//! A generated frame (a pack header, a re-encoded delta header, a converted object, a JSON
//! chunk, a merged idx run) is written into a staging `BytesMut` and handed to hyper as a
//! `split().freeze()` slice of it. hyper keeps a frame alive until it is written to the
//! socket, so a single staging buffer allocates each time it wraps while any earlier frame
//! is still in flight. A [`FrameRing`] rotates over several chunks and writes into a chunk
//! only once every frame split from it has been dropped (`try_reclaim`); the in-flight
//! window of a connection (~400 KiB for HTTP/1.1, the 1 MiB stream window for h2) is far
//! below the ring, so the hot path never allocates. Should every chunk still be in flight,
//! the body reports [`Busy`], yields, and is polled again after hyper flushed.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;

/// Size of one staging chunk; no generated frame is ever larger.
pub const FRAME_BUF_BYTES: usize = 1 << 20;
/// Chunks per ring.
pub const FRAME_CHUNKS: usize = 4;

/// Every staging chunk still backs frames the connection has not written yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Busy;

/// Times a body had to yield because its ring was fully in flight (`/metrics`).
pub static BUSY_TOTAL: AtomicU64 = AtomicU64::new(0);

/// A `BytesMut` already in its shared representation, so the first `split().freeze()` of
/// a response does not allocate the refcount block on the hot path. `split_to(0)` is a
/// no-op fast path, so one real byte is split off and the buffer reclaimed afterwards.
pub fn promoted(capacity: usize) -> BytesMut {
    let cap = capacity.max(1);
    let mut b = BytesMut::with_capacity(cap);
    b.resize(cap, 0); // touch every page now
    drop(b.split_to(1));
    b.clear();
    let _ = b.try_reclaim(cap);
    b
}

#[derive(Debug)]
pub struct FrameRing {
    chunks: Vec<BytesMut>,
    cur: usize,
}

impl Default for FrameRing {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameRing {
    /// Allocates and touches every chunk (boot, or a response built outside a hot region).
    pub fn new() -> Self {
        Self {
            chunks: (0..FRAME_CHUNKS)
                .map(|_| promoted(FRAME_BUF_BYTES))
                .collect(),
            cur: 0,
        }
    }

    /// A chunk with at least `need` free bytes (capped at a whole chunk), or `None` when
    /// every chunk still backs in-flight frames. Never allocates.
    pub fn chunk(&mut self, need: usize) -> Option<&mut BytesMut> {
        let need = need.min(FRAME_BUF_BYTES);
        for _ in 0..FRAME_CHUNKS {
            if self
                .chunks
                .get(self.cur)
                .is_some_and(|c| c.capacity().saturating_sub(c.len()) >= need)
            {
                break;
            }
            self.cur = self.cur.saturating_add(1);
            if self.cur >= FRAME_CHUNKS {
                self.cur = 0;
            }
            if let Some(c) = self.chunks.get_mut(self.cur) {
                c.clear();
                let _ = c.try_reclaim(FRAME_BUF_BYTES);
            }
        }
        let c = self.chunks.get_mut(self.cur)?;
        if c.capacity().saturating_sub(c.len()) >= need {
            Some(c)
        } else {
            BUSY_TOTAL.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Between requests: every frame of the previous response has been dropped, so each
    /// chunk is reclaimed whole (a replacement is allocated only if one somehow was not).
    pub fn reset(&mut self) {
        for c in &mut self.chunks {
            c.clear();
            if !c.try_reclaim(FRAME_BUF_BYTES) {
                *c = promoted(FRAME_BUF_BYTES);
            }
        }
        self.cur = 0;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::arithmetic_side_effects)]
    use super::*;
    use bytes::Bytes;

    #[test]
    fn ring_rotates_and_reports_busy() {
        let mut ring = FrameRing::new();
        let payload = vec![1u8; 300 << 10];
        let mut live: Vec<Bytes> = Vec::new();
        // 3 frames per chunk, 4 chunks: 12 frames fit while all stay alive, the 13th is busy.
        for _ in 0..12 {
            let c = ring.chunk(payload.len()).unwrap();
            c.extend_from_slice(&payload);
            live.push(c.split().freeze());
        }
        assert!(ring.chunk(payload.len()).is_none());
        assert_eq!(BUSY_TOTAL.load(Ordering::Relaxed), 1);
        // Dropping the first chunk's frames frees that chunk again.
        live.drain(..3);
        let c = ring.chunk(payload.len()).unwrap();
        assert!(c.capacity() >= FRAME_BUF_BYTES);
        // Small leftovers are still usable without rotating.
        c.extend_from_slice(&payload);
        live.push(c.split().freeze());
        let before = ring.cur;
        assert!(ring.chunk(64).is_some());
        assert_eq!(ring.cur, before);
        drop(live);
        ring.reset();
        for i in 0..FRAME_CHUNKS {
            assert_eq!(ring.chunks[i].capacity(), FRAME_BUF_BYTES);
        }
    }
}
