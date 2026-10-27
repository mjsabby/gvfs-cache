use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use http_body::{Body, Frame, SizeHint};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};

use crate::arena::{push, reserve, zeroed};
use crate::{Error, Result};

pub struct Buffer {
    pub bytes: Vec<u8>,
    pub spans: Vec<Range<usize>>,
}

pub struct BufferPool {
    buffers: Mutex<Vec<Buffer>>,
    permits: Arc<Semaphore>,
}

impl BufferPool {
    pub fn new(count: usize, bytes: usize, spans: usize) -> Result<Arc<Self>> {
        let mut buffers = reserve(count)?;
        for _ in 0..count {
            push(
                &mut buffers,
                Buffer {
                    bytes: zeroed(bytes)?,
                    spans: reserve(spans)?,
                },
            )?;
        }
        Ok(Arc::new(Self {
            buffers: Mutex::new(buffers),
            permits: Arc::new(Semaphore::new(count)),
        }))
    }

    pub async fn take(self: &Arc<Self>) -> Result<BufferLease> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| Error::Invalid("buffer pool closed"))?;
        let buffer = self
            .buffers
            .lock()
            .map_err(|_| Error::Invalid("buffer pool poisoned"))?
            .pop()
            .ok_or(Error::Invalid("buffer pool accounting"))?;
        Ok(BufferLease {
            buffer: Some(buffer),
            pool: Arc::clone(self),
            _permit: permit,
            span: 0,
            position: 0,
            remaining: 0,
        })
    }
}

pub struct BufferLease {
    buffer: Option<Buffer>,
    pool: Arc<BufferPool>,
    _permit: OwnedSemaphorePermit,
    span: usize,
    position: usize,
    remaining: usize,
}

impl BufferLease {
    pub fn buffer_mut(&mut self) -> Result<&mut Buffer> {
        self.buffer.as_mut().ok_or(Error::Invalid("leased buffer"))
    }

    pub fn buffer(&self) -> Result<&Buffer> {
        self.buffer.as_ref().ok_or(Error::Invalid("leased buffer"))
    }

    pub fn seal(&mut self) -> Result<()> {
        let buffer = self
            .buffer
            .as_ref()
            .ok_or(Error::Invalid("leased buffer"))?;
        let mut remaining = 0usize;
        let mut end = 0usize;
        for span in &buffer.spans {
            if span.start < end || span.start > span.end || span.end > buffer.bytes.len() {
                return Err(Error::Invalid("buffer span"));
            }
            remaining = remaining
                .checked_add(span.len())
                .ok_or(Error::Invalid("buffer length"))?;
            end = span.end;
        }
        self.remaining = remaining;
        self.span = 0;
        self.position = buffer.spans.first().map_or(0, |span| span.start);
        Ok(())
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        if let Some(mut buffer) = self.buffer.take() {
            buffer.spans.clear();
            match self.pool.buffers.lock() {
                Ok(mut buffers) => buffers.push(buffer),
                Err(_) => eprintln!("buffer pool poisoned while returning a response buffer"),
            }
        }
    }
}

pub enum Chunk {
    Shared(Bytes),
    Pinned {
        bytes: Bytes,
        _snapshot: Arc<super::runtime::Catalog>,
    },
    Inline {
        bytes: [u8; 64],
        position: usize,
        len: usize,
    },
    Pooled(BufferLease),
}

impl Chunk {
    pub fn inline(input: &[u8]) -> Result<Self> {
        let mut bytes = [0u8; 64];
        bytes
            .get_mut(..input.len())
            .ok_or(Error::Capacity("inline framing"))?
            .copy_from_slice(input);
        Ok(Self::Inline {
            bytes,
            position: 0,
            len: input.len(),
        })
    }

    pub fn pin_snapshot(&mut self, snapshot: Arc<super::runtime::Catalog>) {
        if let Self::Shared(bytes) = self {
            *self = Self::Pinned {
                bytes: std::mem::take(bytes),
                _snapshot: snapshot,
            };
        }
    }
}

impl Buf for Chunk {
    fn remaining(&self) -> usize {
        match self {
            Self::Shared(bytes) | Self::Pinned { bytes, .. } => bytes.remaining(),
            Self::Inline { position, len, .. } => len.saturating_sub(*position),
            Self::Pooled(lease) => lease.remaining,
        }
    }

    fn chunk(&self) -> &[u8] {
        match self {
            Self::Shared(bytes) | Self::Pinned { bytes, .. } => bytes.chunk(),
            Self::Inline {
                bytes,
                position,
                len,
            } => bytes.get(*position..*len).unwrap_or_default(),
            Self::Pooled(lease) => {
                let Some(buffer) = &lease.buffer else {
                    return &[];
                };
                let Some(span) = buffer.spans.get(lease.span) else {
                    return &[];
                };
                buffer
                    .bytes
                    .get(lease.position..span.end)
                    .unwrap_or_default()
            }
        }
    }

    fn advance(&mut self, count: usize) {
        let mut count = count.min(self.remaining());
        match self {
            Self::Shared(bytes) | Self::Pinned { bytes, .. } => bytes.advance(count),
            Self::Inline { position, .. } => *position = position.saturating_add(count),
            Self::Pooled(lease) => {
                lease.remaining = lease.remaining.saturating_sub(count);
                let Some(buffer) = &lease.buffer else {
                    return;
                };
                while count != 0 {
                    let Some(span) = buffer.spans.get(lease.span) else {
                        return;
                    };
                    let used = count.min(span.end.saturating_sub(lease.position));
                    lease.position = lease.position.saturating_add(used);
                    count = count.saturating_sub(used);
                    if lease.position == span.end {
                        lease.span = lease.span.saturating_add(1);
                        lease.position = buffer.spans.get(lease.span).map_or(0, |span| span.start);
                    }
                }
            }
        }
    }
}

pub enum Message {
    Data(Chunk),
    End,
    Failed(Error),
}

pub struct ChannelReturn {
    pub returned: Mutex<Option<mpsc::Receiver<Message>>>,
    pub closed: AtomicBool,
    pub notify: Notify,
}

impl ChannelReturn {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            returned: Mutex::new(None),
            closed: AtomicBool::new(false),
            notify: Notify::new(),
        })
    }

    pub fn begin(&self) {
        // A canceled response can return its receiver without awaiting Notify's
        // stored permit. Drain that permit before reusing this startup channel.
        let notified = self.notify.notified();
        let mut notified = std::pin::pin!(notified);
        let mut context = Context::from_waker(std::task::Waker::noop());
        let _ = std::future::Future::poll(notified.as_mut(), &mut context);
        self.closed.store(false, Ordering::Release);
    }
}

// Boxing the direct variant would introduce an allocation on every small GET.
#[allow(clippy::large_enum_variant)]
pub enum WireBody {
    Direct {
        chunks: [Option<Chunk>; 4],
        cursor: usize,
        remaining: u64,
        keepalive: Option<Arc<super::runtime::Catalog>>,
    },
    Stream {
        receiver: Option<mpsc::Receiver<Message>>,
        state: Arc<ChannelReturn>,
        remaining: u64,
        ended: bool,
    },
}

impl WireBody {
    pub fn direct(chunks: [Option<Chunk>; 4]) -> Self {
        let remaining = chunks.iter().flatten().fold(0u64, |sum, chunk| {
            sum.saturating_add(chunk.remaining() as u64)
        });
        Self::Direct {
            chunks,
            cursor: 0,
            remaining,
            keepalive: None,
        }
    }

    pub fn bytes(bytes: Bytes) -> Self {
        Self::direct([Some(Chunk::Shared(bytes)), None, None, None])
    }
    pub fn text(text: &'static str) -> Self {
        Self::bytes(Bytes::from_static(text.as_bytes()))
    }

    pub fn pin_snapshot(&mut self, snapshot: Arc<super::runtime::Catalog>) {
        if let Self::Direct { keepalive, .. } = self {
            *keepalive = Some(snapshot);
        }
    }
}

impl Body for WireBody {
    type Data = Chunk;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Chunk>>>> {
        match self.get_mut() {
            Self::Direct {
                chunks,
                cursor,
                remaining,
                keepalive,
            } => {
                while let Some(slot) = chunks.get_mut(*cursor) {
                    *cursor = cursor.saturating_add(1);
                    if let Some(mut chunk) = slot.take() {
                        if let Some(snapshot) = keepalive {
                            chunk.pin_snapshot(Arc::clone(snapshot));
                        }
                        *remaining = remaining.saturating_sub(chunk.remaining() as u64);
                        return Poll::Ready(Some(Ok(Frame::data(chunk))));
                    }
                }
                Poll::Ready(None)
            }
            Self::Stream {
                receiver,
                remaining,
                ended,
                ..
            } => {
                if *ended {
                    return Poll::Ready(None);
                }
                let Some(receiver) = receiver else {
                    return Poll::Ready(Some(Err(Error::Invalid("stream receiver"))));
                };
                match receiver.poll_recv(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Some(Message::Data(chunk))) => {
                        *remaining = remaining.saturating_sub(chunk.remaining() as u64);
                        Poll::Ready(Some(Ok(Frame::data(chunk))))
                    }
                    Poll::Ready(Some(Message::End)) => {
                        *ended = true;
                        if *remaining == 0 {
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(Err(Error::Invalid("truncated response plan"))))
                        }
                    }
                    Poll::Ready(Some(Message::Failed(error))) => {
                        *ended = true;
                        Poll::Ready(Some(Err(error)))
                    }
                    Poll::Ready(None) => {
                        *ended = true;
                        Poll::Ready(Some(Err(Error::Invalid("response producer stopped"))))
                    }
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Direct { remaining, .. } => *remaining == 0,
            Self::Stream { ended, .. } => *ended,
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(match self {
            Self::Direct { remaining, .. } | Self::Stream { remaining, .. } => *remaining,
        })
    }
}

impl Drop for WireBody {
    fn drop(&mut self) {
        if let Self::Stream {
            receiver, state, ..
        } = self
        {
            state.closed.store(true, Ordering::Release);
            if let Some(mut receiver) = receiver.take() {
                while receiver.try_recv().is_ok() {}
                match state.returned.lock() {
                    Ok(mut slot) => *slot = Some(receiver),
                    Err(_) => eprintln!("response channel return lock poisoned"),
                }
            }
            state.notify.notify_one();
        }
    }
}

pub async fn send(
    sender: &mpsc::Sender<Message>,
    state: &ChannelReturn,
    message: Message,
) -> Result<()> {
    if state.closed.load(Ordering::Acquire) {
        return Err(Error::Invalid("client disconnected"));
    }

    tokio::select! {
        biased;
        _ = state.notify.notified() => Err(Error::Invalid("client disconnected")),
        result = sender.send(message) => result.map_err(|_| Error::Invalid("response receiver closed")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn canceled_response_does_not_poison_the_reused_channel() {
        let state = ChannelReturn::new();
        state.closed.store(true, Ordering::Release);
        state.notify.notify_one();
        state.begin();
        let (sender, mut receiver) = mpsc::channel(2);
        let result = send(
            &sender,
            &state,
            Message::Data(Chunk::inline(b"ok").unwrap()),
        )
        .await;
        assert!(result.is_ok());
        assert!(matches!(receiver.recv().await, Some(Message::Data(_))));
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn queued_mapped_data_keeps_its_generation_after_the_body_is_dropped() {
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let reference = crate::manifest::SlabRef {
            id: crate::Oid([1; 20]),
            tier: crate::manifest::Tier::Baseline,
            start: 0,
            end: 1,
            tip: crate::Oid([1; 20]),
            count: 0,
            pack_len: 32,
            times: false,
        };
        let snapshot = Arc::new(super::super::runtime::Catalog {
            manifest: crate::manifest::Manifest::genesis(reference).unwrap(),
            etag: String::new(),
            persistent: Vec::new(),
            live: None,
            _permit: permit,
        });
        let mut body = WireBody::bytes(Bytes::from_static(b"mapped bytes"));
        body.pin_snapshot(Arc::clone(&snapshot));
        drop(snapshot);
        let mut context = Context::from_waker(std::task::Waker::noop());
        let frame = match Pin::new(&mut body).poll_frame(&mut context) {
            Poll::Ready(Some(Ok(frame))) => frame,
            _ => panic!("direct data frame was not ready"),
        };
        drop(body);
        assert_eq!(permits.available_permits(), 0);
        drop(frame);
        assert_eq!(permits.available_permits(), 1);
    }
}
