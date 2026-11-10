//! A local HTTP/1.1 server for unit tests: an upstream or an object store that answers as
//! the test says (an error, a late answer, a body that breaks off, a connection that dies).

use std::io::{Read, Write};
use std::sync::Arc;

/// One request, as the server read it.
pub struct Request {
    /// `<method> <target> HTTP/1.1`.
    pub line: String,
    head: String,
    pub body: Vec<u8>,
    /// How many requests its connection carried before this one.
    pub earlier: usize,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.head.lines().skip(1).find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim().eq_ignore_ascii_case(name).then_some(v.trim())
        })
    }
}

/// What the server does with a request.
pub enum Reply {
    /// A response: its status (with any more header lines) and its body.
    Whole(String, Vec<u8>),
    /// The connection closes without an answer.
    Close,
}

impl Reply {
    pub fn ok(body: &[u8]) -> Reply {
        Reply::Whole("200 OK".to_string(), body.to_vec())
    }

    pub fn status(status: &str) -> Reply {
        Reply::Whole(status.to_string(), Vec::new())
    }
}

/// Starts a server on a free port (keep-alive, a thread per connection, for the rest of
/// the test process) that answers each request as `answer` says. Returns the port.
pub fn serve(answer: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let answer = Arc::new(answer);
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(conn) = conn else { continue };
            let answer = Arc::clone(&answer);
            std::thread::spawn(move || connection(conn, &*answer));
        }
    });
    port
}

fn connection(mut conn: std::net::TcpStream, answer: &dyn Fn(&Request) -> Reply) {
    let mut buf: Vec<u8> = Vec::new();
    let mut piece = [0u8; 16 << 10];
    let mut more = |buf: &mut Vec<u8>, conn: &mut std::net::TcpStream| match conn.read(&mut piece) {
        Ok(0) | Err(_) => false,
        Ok(n) => {
            buf.extend_from_slice(&piece[..n]);
            true
        }
    };
    for earlier in 0.. {
        let end = loop {
            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break p + 4;
            }
            if !more(&mut buf, &mut conn) {
                return;
            }
        };
        let head = String::from_utf8_lossy(&buf[..end]).to_string();
        let mut request = Request {
            line: head.lines().next().unwrap_or_default().to_string(),
            head,
            body: Vec::new(),
            earlier,
        };
        let len: usize = request
            .header("content-length")
            .map_or(0, |v| v.parse().unwrap());
        while buf.len() < end + len {
            if !more(&mut buf, &mut conn) {
                return;
            }
        }
        request.body = buf[end..end + len].to_vec();
        buf.drain(..end + len);
        let Reply::Whole(status, body) = answer(&request) else {
            return;
        };
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        if conn.write_all(head.as_bytes()).is_err() || conn.write_all(&body).is_err() {
            return;
        }
    }
}
