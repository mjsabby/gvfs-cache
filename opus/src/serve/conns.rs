//! Connection parking: idle keep-alive connections wait in one shared epoll set instead of
//! pinning a worker thread each.
//!
//! Every worker blocks in `epoll_wait` on the same set, which holds the listening socket
//! (level-triggered) and every parked connection (`EPOLLONESHOT`). A worker that gets a
//! connection keeps serving it while its requests keep coming: it waits up to `--linger-ms`
//! for the next one, so a busy client does not pay an epoll round trip per request. When
//! the client pauses for longer, the connection is parked again and the worker takes
//! whatever is ready next. So any number of idle clients (a fleet's connection pools) cost
//! a table slot each instead of a thread each.
//!
//! While no worker is free ([`Conns::all_busy`]), a worker also parks its connection between
//! requests once it has held it for `--hold-ms`. Connections that became ready meanwhile
//! are ahead of it in epoll's ready list, so busy connections beyond the worker count take
//! turns instead of waiting for the others to go idle.
//!
//! Tokens carry the slot and a generation, so an event that loses the race with the idle
//! reaper (which closes the connection and frees the slot) can never pick up the slot's next
//! tenant. A connection is moved into its slot before its interest is (re)armed, so the
//! worker that wins the event always finds it there.

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags, EpollTimeout};

const LISTENER: u64 = u64::MAX;
const SHARDS: usize = 64;

struct Slot {
    stream: Option<TcpStream>,
    generation: u32,
    since: Instant,
}

struct Shard {
    slots: Vec<Slot>,
    free: Vec<u32>,
}

/// The shared epoll set and the table of parked connections.
pub struct Conns {
    epoll: Epoll,
    shards: Vec<Mutex<Shard>>,
    /// Workers blocked in [`Conns::next`].
    waiting: AtomicU64,
    /// Connections waiting for their next request.
    pub parked: AtomicU64,
    /// Parked connections closed after `--idle-secs` without a request.
    pub reaped: AtomicU64,
    /// Idle connections closed because every slot was taken.
    pub overflow: AtomicU64,
}

fn io(e: Errno) -> std::io::Error {
    std::io::Error::from(e)
}

/// `shard:8 | generation:32 | slot:24`; shards < 64, so no token equals [`LISTENER`].
fn token(shard: usize, idx: u32, generation: u32) -> u64 {
    let shard = u64::try_from(shard).unwrap_or(0);
    shard.wrapping_shl(56) | u64::from(generation).wrapping_shl(24) | u64::from(idx & 0x00FF_FFFF)
}

fn split(t: u64) -> (usize, usize, u32) {
    let shard = usize::try_from(t.wrapping_shr(56)).unwrap_or(usize::MAX);
    let generation = u32::try_from(t.wrapping_shr(24) & 0xFFFF_FFFF).unwrap_or(0);
    let idx = usize::try_from(t & 0x00FF_FFFF).unwrap_or(usize::MAX);
    (shard, idx, generation)
}

impl Conns {
    /// Registers `listener` (made non-blocking) and allocates room for `max_parked`
    /// connections (at least 16 per shard, at most 2^24 per shard).
    pub fn new(listener: &TcpListener, max_parked: usize) -> std::io::Result<Conns> {
        listener.set_nonblocking(true)?;
        let epoll = Epoll::new(EpollCreateFlags::EPOLL_CLOEXEC).map_err(io)?;
        epoll
            .add(listener, EpollEvent::new(EpollFlags::EPOLLIN, LISTENER))
            .map_err(io)?;
        let per_shard = max_parked.div_ceil(SHARDS).clamp(16, 1 << 24);
        let per_shard_u32 = u32::try_from(per_shard).unwrap_or(1 << 24);
        let now = Instant::now();
        let shards = (0..SHARDS)
            .map(|_| {
                Mutex::new(Shard {
                    slots: (0..per_shard)
                        .map(|_| Slot {
                            stream: None,
                            generation: 0,
                            since: now,
                        })
                        .collect(),
                    free: (0..per_shard_u32).rev().collect(),
                })
            })
            .collect();
        Ok(Conns {
            epoll,
            shards,
            waiting: AtomicU64::new(0),
            parked: AtomicU64::new(0),
            reaped: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
        })
    }

    fn lock(&self, shard: usize) -> Option<MutexGuard<'_, Shard>> {
        let m = self.shards.get(shard)?;
        Some(match m.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        })
    }

    /// Blocks until a connection needs a worker: a newly accepted one (`true`: not yet in the
    /// epoll set) or a parked one whose next request has arrived (`false`).
    pub fn next(&self, listener: &TcpListener) -> Option<(TcpStream, bool)> {
        let mut events = [EpollEvent::empty()];
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let woke = self.epoll.wait(&mut events, EpollTimeout::NONE);
        self.waiting.fetch_sub(1, Ordering::Relaxed);
        match woke {
            Ok(1) => {}
            _ => return None,
        }
        let t = events.first().map(EpollEvent::data)?;
        if t == LISTENER {
            return match listener.accept() {
                Ok((s, _)) => Some((s, true)),
                Err(e) => {
                    // Out of descriptors: back off rather than spin on a readable listener.
                    let out_of_fds = [Errno::EMFILE, Errno::ENFILE]
                        .iter()
                        .any(|&n| e.raw_os_error() == Some(n as i32));
                    if out_of_fds {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    None
                }
            };
        }
        self.take(t)
    }

    /// No worker is waiting in [`Conns::next`]: a connection that becomes ready now (a new
    /// client, a parked client's next request) waits until some worker lets go of one.
    pub fn all_busy(&self) -> bool {
        self.waiting.load(Ordering::Relaxed) == 0
    }

    fn take(&self, t: u64) -> Option<(TcpStream, bool)> {
        let (shard, idx, generation) = split(t);
        let mut sh = self.lock(shard)?;
        let Shard { slots, free } = &mut *sh;
        let slot = slots.get_mut(idx)?;
        if slot.generation != generation {
            return None;
        }
        let stream = slot.stream.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        free.push(u32::try_from(idx).unwrap_or(0));
        self.parked.fetch_sub(1, Ordering::Relaxed);
        Some((stream, false))
    }

    /// Parks an idle connection until its next request arrives. `registered` = the
    /// connection is already in the epoll set (it came from [`Conns::next`] as parked).
    /// If every slot is taken the connection is closed (the client reconnects).
    pub fn park(&self, stream: TcpStream, registered: bool, hint: usize) {
        let mut stream = Some(stream);
        for k in 0..SHARDS {
            let shard = hint.wrapping_add(k).wrapping_rem(SHARDS);
            let Some(mut sh) = self.lock(shard) else {
                continue;
            };
            let Shard { slots, free } = &mut *sh;
            let Some(idx) = free.pop() else {
                continue;
            };
            let Some(slot) = slots.get_mut(usize::try_from(idx).unwrap_or(usize::MAX)) else {
                continue;
            };
            slot.stream = stream.take();
            slot.since = Instant::now();
            let t = token(shard, idx, slot.generation);
            let mut ev = EpollEvent::new(
                EpollFlags::EPOLLIN | EpollFlags::EPOLLRDHUP | EpollFlags::EPOLLONESHOT,
                t,
            );
            let armed = match slot.stream.as_ref() {
                Some(s) if registered => self.epoll.modify(s, &mut ev),
                Some(s) => self.epoll.add(s, ev),
                None => Err(Errno::EBADF),
            };
            if armed.is_err() {
                // Could not watch it: close it (dropping the stream) and free the slot.
                drop(slot.stream.take());
                slot.generation = slot.generation.wrapping_add(1);
                free.push(idx);
                return;
            }
            self.parked.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.overflow.fetch_add(1, Ordering::Relaxed);
    }

    /// Closes connections that have been parked for longer than `idle`. Closing the
    /// descriptor also removes it from the epoll set.
    pub fn reap(&self, idle: Duration) {
        let now = Instant::now();
        for shard in 0..SHARDS {
            let Some(mut sh) = self.lock(shard) else {
                continue;
            };
            let Shard { slots, free } = &mut *sh;
            for (i, slot) in slots.iter_mut().enumerate() {
                if slot.stream.is_some() && now.saturating_duration_since(slot.since) > idle {
                    drop(slot.stream.take());
                    slot.generation = slot.generation.wrapping_add(1);
                    free.push(u32::try_from(i).unwrap_or(0));
                    self.parked.fetch_sub(1, Ordering::Relaxed);
                    self.reaped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn tokens_round_trip() {
        for (s, i, g) in [(0usize, 0u32, 0u32), (63, 0x00FF_FFFF, u32::MAX), (5, 1234, 77)] {
            assert_eq!(split(token(s, i, g)), (s, usize::try_from(i).unwrap(), g));
        }
        assert_ne!(token(63, 0x00FF_FFFF, u32::MAX), LISTENER);
    }

    #[test]
    fn park_wake_take_and_reap() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let conns = Conns::new(&listener, 64).unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        let (server, fresh) = loop {
            if let Some(x) = conns.next(&listener) {
                break x;
            }
        };
        assert!(fresh);
        conns.park(server, false, 3);
        assert_eq!(conns.parked.load(Ordering::Relaxed), 1);
        client.write_all(b"ping").unwrap();
        let (mut server, fresh) = loop {
            if let Some(x) = conns.next(&listener) {
                break x;
            }
        };
        assert!(!fresh);
        assert_eq!(conns.parked.load(Ordering::Relaxed), 0);
        let mut b = [0u8; 4];
        server.read_exact(&mut b).unwrap();
        assert_eq!(&b, b"ping");
        // Re-park (already registered), then let it idle out.
        conns.park(server, true, 9);
        assert_eq!(conns.parked.load(Ordering::Relaxed), 1);
        conns.reap(Duration::ZERO);
        assert_eq!(conns.parked.load(Ordering::Relaxed), 0);
        assert_eq!(conns.reaped.load(Ordering::Relaxed), 1);
        let mut rest = Vec::new();
        assert_eq!(client.read_to_end(&mut rest).unwrap(), 0, "reaped connection is closed");
    }
}
