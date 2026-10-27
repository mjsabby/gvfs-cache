use super::*;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct State {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    parts: Mutex<BTreeMap<String, Vec<u8>>>,
    ranges: AtomicUsize,
    aborts: AtomicUsize,
    bad_range: AtomicBool,
    bad_complete: AtomicBool,
    stop: AtomicBool,
}
struct Mock {
    state: Arc<State>,
    url: String,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Mock {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(State::default());
        let shared = Arc::clone(&state);
        let thread = std::thread::spawn(move || {
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                if shared.stop.load(Ordering::Relaxed) {
                    break;
                }
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let fields: Vec<_> = line.split_whitespace().collect();
                let method = fields[0].to_owned();
                let uri = fields[1].to_owned();
                let key = uri
                    .split('?')
                    .next()
                    .unwrap()
                    .trim_start_matches('/')
                    .to_owned();
                let mut headers = BTreeMap::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    let (name, value) = line.split_once(':').unwrap();
                    headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
                }
                let len: usize = headers
                    .get("content-length")
                    .map_or(0, |s| s.parse().unwrap());
                let mut body = vec![0; len];
                reader.read_exact(&mut body).unwrap();
                let mut status = 200;
                let mut response = Vec::new();
                let mut extra = String::new();
                let mut objects = shared.objects.lock().unwrap();
                if method == "DELETE" {
                    shared.aborts.fetch_add(1, Ordering::Relaxed);
                    status = 204;
                } else if method == "POST" && uri.contains("uploads=") {
                    response.extend_from_slice(b"<InitiateMultipartUploadResult><UploadId>a+b/c=</UploadId></InitiateMultipartUploadResult>");
                } else if method == "PUT"
                    && (uri.contains("comp=block&") || uri.contains("partNumber="))
                {
                    shared.parts.lock().unwrap().insert(uri.clone(), body);
                    status = if uri.contains("comp=block") { 201 } else { 200 };
                    extra.push_str("ETag: \"part\"\r\n");
                } else if (method == "PUT" && uri.contains("comp=blocklist"))
                    || (method == "POST" && uri.contains("uploadId="))
                {
                    if shared.bad_complete.load(Ordering::Relaxed) {
                        response.extend_from_slice(b"<Error><Code>InternalError</Code></Error>");
                    } else {
                        let bytes = shared
                            .parts
                            .lock()
                            .unwrap()
                            .values()
                            .flatten()
                            .copied()
                            .collect();
                        objects.insert(key, bytes);
                        if method == "PUT" {
                            status = 201;
                        } else {
                            response.extend_from_slice(b"<CompleteMultipartUploadResult/>");
                        }
                    }
                } else if method == "PUT" {
                    let current = objects
                        .get(&key)
                        .map(|b| format!("\"{}\"", hex(&crate::sha1_parts(&[b]))));
                    let conflict = headers.get("if-none-match").is_some_and(|v| v == "*")
                        && current.is_some()
                        || headers
                            .get("if-match")
                            .is_some_and(|v| Some(v) != current.as_ref());
                    if conflict {
                        status = 412;
                    } else {
                        objects.insert(key, body);
                        status = 201;
                    }
                } else if let Some(bytes) = objects.get(&key) {
                    extra.push_str(&format!(
                        "ETag: \"{}\"\r\n",
                        hex(&crate::sha1_parts(&[bytes]))
                    ));
                    if let Some(range) = headers.get("range") {
                        shared.ranges.fetch_add(1, Ordering::Relaxed);
                        let (a, b) = range
                            .strip_prefix("bytes=")
                            .unwrap()
                            .split_once('-')
                            .unwrap();
                        let a: usize = a.parse().unwrap();
                        let b: usize = b.parse().unwrap();
                        if shared.bad_range.load(Ordering::Relaxed) {
                            response.extend_from_slice(bytes);
                        } else {
                            status = 206;
                            extra.push_str(&format!(
                                "Content-Range: bytes {a}-{b}/{}\r\n",
                                bytes.len()
                            ));
                            response.extend_from_slice(&bytes[a..=b]);
                        }
                    } else {
                        response.extend_from_slice(bytes);
                    }
                } else {
                    status = 404;
                }
                drop(objects);
                write!(stream,"HTTP/1.1 {status} Test\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",response.len()).unwrap();
                stream.write_all(&response).unwrap();
            }
        });
        Self {
            state,
            url,
            thread: Some(thread),
        }
    }
    fn store(&self, azure: bool, s3: bool) -> Storage {
        let base = self.url.parse().unwrap();
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(5)))
            .build();
        Storage::Http(Arc::new(HttpStore {
            base,
            query: None,
            auth: None,
            azure,
            s3: if s3 {
                Some(S3 {
                    access: "test".into(),
                    secret: "test".into(),
                    token: None,
                    region: "us-east-1".into(),
                })
            } else {
                None
            },
            agent: config.into(),
        }))
    }
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Relaxed);
        TcpStream::connect(self.url.strip_prefix("http://").unwrap()).unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}

#[test]
fn conditional_remote_publication_and_exact_ranges() {
    let mock = Mock::new();
    let store = mock.store(true, false);
    assert!(store.manifest().unwrap().is_none());
    store.publish_manifest(b"root-1", None).unwrap();
    assert!(matches!(
        store.publish_manifest(b"lost-race", None),
        Err(Error::Conflict)
    ));
    let (body, etag) = store.manifest().unwrap().unwrap();
    assert_eq!(body, b"root-1");
    store.publish_manifest(b"root-2", Some(&etag)).unwrap();
    assert!(matches!(
        store.publish_manifest(b"stale", Some(&etag)),
        Err(Error::Conflict)
    ));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    fs::write(&path, b"0123456789abcdef").unwrap();
    store.upload("test.pack", &path).unwrap();
    store.upload("test.pack", &path).unwrap();
    let reader = store.reader("test.pack", 16).unwrap();
    let mut bytes = [0; 5];
    reader.read(3, &mut bytes).unwrap();
    assert_eq!(&bytes, b"34567");
    mock.state.bad_range.store(true, Ordering::Relaxed);
    assert!(
        reader.read(3, &mut bytes).is_err(),
        "must reject a full-body 200 for a Range GET"
    );
    fs::write(&path, b"different content").unwrap();
    assert!(
        store.upload("test.pack", &path).is_err(),
        "immutable collisions must be errors"
    );
}

#[test]
fn azure_blocks_and_s3_multipart_are_atomic_and_bounded() {
    for (azure, s3) in [(true, false), (false, true)] {
        let mock = Mock::new();
        let store = mock.store(azure, s3);
        let Storage::Http(http) = &store else {
            panic!("expected HTTP store")
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input");
        let data: Vec<_> = (0..17000).map(|i| (i % 251) as u8).collect();
        fs::write(&path, &data).unwrap();
        http.multipart("large.pack", &path, 8192).unwrap();
        assert_eq!(mock.state.parts.lock().unwrap().len(), 3);
        assert_eq!(
            mock.state.objects.lock().unwrap().get("large.pack"),
            Some(&data)
        );
        if s3 {
            mock.state.bad_complete.store(true, Ordering::Relaxed);
            assert!(
                http.multipart("failed.pack", &path, 8192).is_err(),
                "S3 200-with-error is not success"
            );
            assert_eq!(mock.state.aborts.load(Ordering::Relaxed), 1);
            assert!(
                !mock
                    .state
                    .objects
                    .lock()
                    .unwrap()
                    .contains_key("failed.pack")
            );
        }
    }
}
