//! Objects a client asked for that no slab holds. Request tasks enqueue them (no
//! allocation: a pre-sized list under a mutex), the live thread — which owns the upstream
//! connection — fetches them into the live slab in one round and wakes the waiters, which
//! then look the objects up again. A round is one upstream fetch however many requests
//! piled up; a storm of unknown ids costs one upstream round trip per round.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::oid::Oid;

/// Ids queued at most between two rounds; beyond it, requests fail fast (404).
pub const MAX_PENDING: usize = 4096;

pub struct MissQueue {
    pending: Mutex<Pending>,
    wake: Condvar,
    done: Notify,
    completed: AtomicU64,
    /// An upstream is configured, so misses can be resolved at all.
    pub enabled: bool,
    pub rounds: AtomicU64,
    pub fetched_objects: AtomicU64,
}

struct Pending {
    oids: Vec<Oid>,
    /// Rounds started (the queue drained into one).
    taken: u64,
}

impl MissQueue {
    pub fn new(enabled: bool) -> Self {
        Self {
            pending: Mutex::new(Pending {
                oids: Vec::with_capacity(if enabled { MAX_PENDING } else { 0 }),
                taken: 0,
            }),
            wake: Condvar::new(),
            done: Notify::new(),
            completed: AtomicU64::new(0),
            enabled,
            rounds: AtomicU64::new(0),
            fetched_objects: AtomicU64::new(0),
        }
    }

    /// Enqueues `oids` for the next round and returns the ticket to wait for; `None` when
    /// disabled or when nothing could be queued.
    pub fn request(&self, oids: impl IntoIterator<Item = Oid>) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        let mut p = self.pending.lock().ok()?;
        for o in oids {
            if p.oids.len() >= p.oids.capacity() {
                break;
            }
            if !p.oids.contains(&o) {
                p.oids.push(o);
            }
        }
        if p.oids.is_empty() {
            return None;
        }
        let ticket = p.taken.saturating_add(1);
        drop(p);
        self.wake.notify_one();
        Some(ticket)
    }

    /// Live thread: waits up to `timeout` for work and drains it into `out` (pre-sized).
    /// Every `true` must be followed by one [`MissQueue::finish`].
    pub fn take(&self, out: &mut Vec<Oid>, timeout: Duration) -> bool {
        out.clear();
        let Ok(mut p) = self.pending.lock() else {
            return false;
        };
        if p.oids.is_empty() {
            let Ok((g, _)) = self.wake.wait_timeout(p, timeout) else {
                return false;
            };
            p = g;
        }
        if p.oids.is_empty() {
            return false;
        }
        out.extend(p.oids.drain(..).take(MAX_PENDING));
        p.taken = p.taken.saturating_add(1);
        true
    }

    /// Live thread: the round is over, whatever its outcome; waiters look up again.
    pub fn finish(&self) {
        self.completed.fetch_add(1, Ordering::Release);
        self.done.notify_waiters();
    }

    /// Request task: waits until round `ticket` completed; `false` on timeout.
    pub async fn wait(&self, ticket: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(tokio::time::Instant::now);
        loop {
            let notified = self.done.notified();
            if self.completed.load(Ordering::Acquire) >= ticket {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.completed.load(Ordering::Acquire) >= ticket;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn tickets_follow_rounds() {
        let q = MissQueue::new(true);
        let o = |b: u8| Oid::from_slice(&[b; 20]).unwrap();
        assert_eq!(q.request([o(1), o(1), o(2)]), Some(1));
        let mut buf = Vec::with_capacity(MAX_PENDING);
        assert!(q.take(&mut buf, Duration::from_millis(1)));
        assert_eq!(buf, vec![o(1), o(2)]);
        // Queued while round 1 runs: served by round 2.
        assert_eq!(q.request([o(3)]), Some(2));
        q.finish();
        assert_eq!(q.completed.load(Ordering::Relaxed), 1);
        assert!(q.take(&mut buf, Duration::from_millis(1)));
        assert_eq!(buf, vec![o(3)]);
        q.finish();
        assert!(!q.take(&mut buf, Duration::from_millis(1)));
        assert!(MissQueue::new(false).request([o(1)]).is_none());
    }

    #[test]
    fn waiters_wake_on_finish() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let q = std::sync::Arc::new(MissQueue::new(true));
            let ticket = q.request([Oid::from_slice(&[7; 20]).unwrap()]).unwrap();
            let q2 = std::sync::Arc::clone(&q);
            let waiter = tokio::spawn(async move { q2.wait(ticket, Duration::from_secs(5)).await });
            tokio::task::yield_now().await;
            let mut buf = Vec::new();
            assert!(q.take(&mut buf, Duration::from_millis(1)));
            q.finish();
            assert!(waiter.await.unwrap());
            assert!(!q.wait(ticket + 1, Duration::from_millis(20)).await);
        });
    }
}
