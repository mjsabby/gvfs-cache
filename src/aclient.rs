//! The async HTTP/1.1 client: hyper's client connections on tokio sockets, TLS by rustls
//! (tokio-rustls), idle connections pooled per client.
//!
//! The edge's fetches while it serves (ranged reads of blob packs in object storage, the
//! blob proxy's requests to the upstream) use it, so that a slow or stalled origin parks a
//! task instead of holding a thread. Every request to the upstream goes through it: the
//! pipeline, the mirror and the edge's live loop make theirs through
//! [`crate::upstream::Upstream`], which runs them on a runtime of its own. Their calls to
//! object storage keep the blocking [`crate::http`] client. A client's connections run on
//! the runtime that made them: use one from a single runtime.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1::{self, SendRequest};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use crate::http::Url;
use crate::util::{Context, Result};

type Sender = SendRequest<Full<Bytes>>;

/// Idle connections kept per client.
const IDLE_MAX: usize = 16;

/// An origin's client; clones share its pool of idle connections.
#[derive(Clone)]
pub struct AClient {
    pub url: Url,
    timeout: Duration,
    idle: Arc<Mutex<Vec<Sender>>>,
}

/// A response whose body is read in pieces ([`AResponse::chunk`]).
pub struct AResponse {
    pub status: u16,
    headers: hyper::HeaderMap,
    body: Incoming,
    /// The connection: back to the pool once the body has been read to its end.
    sender: Option<Sender>,
    idle: Arc<Mutex<Vec<Sender>>>,
    timeout: Duration,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

impl AClient {
    pub fn new(url: Url, timeout: Duration) -> AClient {
        AClient {
            url,
            timeout,
            idle: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// A new connection: TCP, TLS and the HTTP/1 handshake, all within the timeout (an
    /// origin that accepts and then stalls must not hold a fetch forever).
    async fn connect(&self) -> Result<Sender> {
        let addr = format!("{}:{}", self.url.host, self.url.port);
        tokio::time::timeout(self.timeout, self.connect_to(&addr))
            .await
            .map_err(|_| crate::err!("connecting to {addr}: timed out"))?
    }

    async fn connect_to(&self, addr: &str) -> Result<Sender> {
        let tcp = TcpStream::connect(addr)
            .await
            .ctx(&format!("connecting to {addr}"))?;
        tcp.set_nodelay(true)?;
        let handshake = |e: hyper::Error| crate::err!("HTTP handshake with {addr}: {e}");
        if self.url.https {
            let name = rustls::pki_types::ServerName::try_from(self.url.host.clone())
                .map_err(|e| crate::err!("bad TLS server name {}: {e}", self.url.host))?;
            let tls = tokio_rustls::TlsConnector::from(crate::http::tls_config()?)
                .connect(name, tcp)
                .await
                .ctx(&format!("TLS with {addr}"))?;
            let (sender, conn) = http1::handshake(TokioIo::new(tls))
                .await
                .map_err(handshake)?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(sender)
        } else {
            let (sender, conn) = http1::handshake(TokioIo::new(tcp))
                .await
                .map_err(handshake)?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(sender)
        }
    }

    /// An idle connection ready for a request, if one is pooled.
    fn pooled(&self) -> Option<Sender> {
        let mut idle = lock(&self.idle);
        while let Some(s) = idle.pop() {
            if s.is_ready() {
                return Some(s);
            }
        }
        None
    }

    /// Sends a request; `path` is appended to the URL's base path, and the body goes with a
    /// Content-Length. A pooled connection that fails before answering (the origin closed
    /// it meanwhile) is retried once on a fresh one, as the blocking client does.
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> Result<AResponse> {
        // Without the query: an Azure SAS travels in it.
        let what = || {
            format!(
                "{method} {}{}{}",
                self.url.origin(),
                self.url.path,
                crate::http::without_query(path)
            )
        };
        for _ in 0..2 {
            let (mut sender, fresh) = match self.pooled() {
                Some(s) => (s, false),
                None => (self.connect().await.ctx(&what())?, true),
            };
            let mut req = hyper::Request::builder()
                .method(method)
                .uri(format!("{}{path}", self.url.path))
                .header("Host", self.url.host.as_str());
            for (k, v) in headers {
                req = req.header(*k, *v);
            }
            if body.is_empty() && (method == "POST" || method == "PUT") {
                req = req.header("Content-Length", "0");
            }
            let req = req
                .body(Full::new(body.clone()))
                .map_err(|e| crate::err!("{}: {e}", what()))?;
            match tokio::time::timeout(self.timeout, sender.send_request(req)).await {
                Ok(Ok(resp)) => {
                    let (parts, body) = resp.into_parts();
                    return Ok(AResponse {
                        status: parts.status.as_u16(),
                        headers: parts.headers,
                        body,
                        sender: Some(sender),
                        idle: Arc::clone(&self.idle),
                        timeout: self.timeout,
                    });
                }
                Ok(Err(e)) if fresh => crate::bail!("{}: {e}", what()),
                Ok(Err(_)) => {}
                Err(_) => crate::bail!("{}: timed out", what()),
            }
        }
        crate::bail!("{}: request failed", what())
    }
}

impl AResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// The next piece of the body; `None` at its end (the connection goes back to the
    /// pool). Each wait is bounded by the client's timeout.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>> {
        loop {
            let frame = tokio::time::timeout(self.timeout, self.body.frame())
                .await
                .map_err(|_| crate::err!("response body: timed out"))?;
            match frame {
                None => {
                    if let Some(s) = self.sender.take() {
                        let mut idle = lock(&self.idle);
                        if idle.len() < IDLE_MAX {
                            idle.push(s);
                        }
                    }
                    return Ok(None);
                }
                Some(Err(e)) => crate::bail!("response body: {e}"),
                Some(Ok(f)) => {
                    if let Ok(data) = f.into_data()
                        && !data.is_empty()
                    {
                        return Ok(Some(data));
                    }
                }
            }
        }
    }

    /// The whole body (at most `limit` bytes).
    pub async fn body_vec(&mut self, limit: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(c) = self.chunk().await? {
            if out.len().saturating_add(c.len()) > limit {
                crate::bail!("response body larger than {limit} bytes");
            }
            out.extend_from_slice(&c);
        }
        Ok(out)
    }

    /// `self` if the status is 2xx; else an error with the start of the body.
    pub async fn ensure_ok(mut self, what: &str) -> Result<Self> {
        if (200..300).contains(&self.status) {
            return Ok(self);
        }
        let status = self.status;
        let body = self.body_vec(1 << 20).await.unwrap_or_default();
        let text = String::from_utf8_lossy(body.get(..body.len().min(512)).unwrap_or(&[]))
            .trim()
            .to_string();
        crate::bail!("{what}: HTTP {status}: {text}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failed request names its target without the query (where an Azure SAS travels).
    #[test]
    fn errors_hold_no_query() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let url = Url::parse("http://127.0.0.1:1/acct").unwrap();
        let c = AClient::new(url, Duration::from_secs(5));
        let e = rt
            .block_on(c.request("GET", "/c/key?sv=2021&sig=s3cret", &[], Bytes::new()))
            .err()
            .unwrap();
        let text = format!("{e:#}");
        assert!(
            text.contains("/acct/c/key") && !text.contains("s3cret"),
            "{text}"
        );
    }
}
