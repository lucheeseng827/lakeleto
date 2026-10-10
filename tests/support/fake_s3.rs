//! A stand-in for S3 on localhost, shared by the object-store tests: path-style `HEAD`, `GET`
//! (ranged, and with `If-Match`) and `ListObjectsV2`, served from objects held in memory and
//! logged as they are asked for.
//!
//! Each test crate that needs it includes it with `#[path = "support/fake_s3.rs"] mod fake_s3;`.
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lakeleto::objstore::StoreOptions;

/// A stand-in for S3 over objects held in memory: path-style `HEAD` and `GET` of `/bucket/key`,
/// ranged `GET`s and `If-Match`, as `object_store`'s S3 client sends them unsigned. It logs the
/// requests made, and counts the body bytes it sent.
pub struct FakeS3 {
    addr: SocketAddr,
    state: Arc<State>,
}

/// Something to run as each request arrives, with the line it is logged as.
type Hook = Box<dyn Fn(&str) + Send + Sync>;

#[derive(Default)]
struct State {
    objects: Mutex<HashMap<String, Stored>>,
    requests: Mutex<Vec<String>>,
    hook: Mutex<Option<Hook>>,
    /// The access key each request was signed with, `-` for an unsigned one, in request order.
    signers: Mutex<Vec<String>>,
    sent: AtomicU64,
}

/// An object's bytes, and the ETag a request names them by.
#[derive(Clone)]
struct Stored {
    body: Arc<Vec<u8>>,
    tag: String,
}

impl FakeS3 {
    /// A server on a port of its own, answering each connection on a thread of its own.
    pub fn start() -> FakeS3 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(State::default());
        let serving = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let state = serving.clone();
                std::thread::spawn(move || serve(stream, &state));
            }
        });
        FakeS3 { addr, state }
    }

    /// Store `body` at `key`, with an ETag made from its bytes.
    pub fn put(&self, key: &str, body: impl Into<Vec<u8>>) {
        let body = body.into();
        let tag = format!("\"{:x}\"", {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            body.hash(&mut hasher);
            hasher.finish()
        });
        self.state.objects.lock().unwrap().insert(
            key.to_string(),
            Stored {
                body: Arc::new(body),
                tag,
            },
        );
    }

    /// The stand-in's endpoint, as an `s3.endpoint` or `AWS_ENDPOINT` names it.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Credentials-free options for this endpoint: what `AWS_ENDPOINT` and friends say for a real
    /// S3-compatible store.
    pub fn options(&self) -> StoreOptions {
        StoreOptions::empty()
            .with_config("aws_endpoint", format!("http://{}", self.addr))
            .with_config("aws_allow_http", "true")
            .with_config("aws_skip_signature", "true")
            .with_config("aws_region", "us-east-1")
    }

    /// The requests made since the last [`Self::reset`]: `METHOD key`, and the `Range` asked for.
    pub fn requests(&self) -> Vec<String> {
        self.state.requests.lock().unwrap().clone()
    }

    /// The access key that signed each request since the last [`Self::reset`], in the order of
    /// [`Self::requests`]: `-` for an unsigned request. It says whose credentials a read used.
    pub fn signers(&self) -> Vec<String> {
        self.state.signers.lock().unwrap().clone()
    }

    /// The body bytes sent since the last [`Self::reset`] — an upper bound on what was read, since
    /// what a socket buffered for a reader that stopped counts too.
    pub fn sent(&self) -> u64 {
        self.state.sent.load(Ordering::SeqCst)
    }

    /// Run `hook` as each request arrives, before it is answered, with the line it is logged as:
    /// how a test acts at an exact point in a read, such as cancelling it.
    pub fn on_request(&self, hook: impl Fn(&str) + Send + Sync + 'static) {
        *self.state.hook.lock().unwrap() = Some(Box::new(hook));
    }

    /// Forget the requests and bytes counted so far.
    pub fn reset(&self) {
        self.state.requests.lock().unwrap().clear();
        self.state.signers.lock().unwrap().clear();
        self.state.sent.store(0, Ordering::SeqCst);
    }
}

/// Answer the requests on one connection until the client closes it, or stops reading a body.
fn serve(stream: TcpStream, state: &State) {
    let Ok(read) = stream.try_clone() else { return };
    let mut requests = BufReader::new(read);
    let mut out = stream;
    loop {
        let mut line = String::new();
        if requests.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut headers = HashMap::new();
        loop {
            let mut header = String::new();
            if requests.read_line(&mut header).unwrap_or(0) == 0 {
                return;
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }
        let mut words = line.split_whitespace();
        let method = words.next().unwrap_or_default().to_string();
        let target = words.next().unwrap_or_default();
        // `AWS4-HMAC-SHA256 Credential=<key id>/<date>/…`: the key id names the signer.
        let signer = headers
            .get("authorization")
            .and_then(|a| a.split("Credential=").nth(1))
            .and_then(|c| c.split('/').next())
            .unwrap_or("-")
            .to_string();
        state.signers.lock().unwrap().push(signer);
        // `GET /bucket?list-type=2&prefix=…`: a listing, not an object.
        if let Some(query) = target.split_once('?').map(|(_, q)| q) {
            if query.split('&').any(|pair| pair == "list-type=2") {
                let param = |name: &str| {
                    query
                        .split('&')
                        .filter_map(|pair| pair.split_once('='))
                        .find(|(k, _)| *k == name)
                        .map(|(_, v)| percent_decode(v))
                };
                let prefix = param("prefix").unwrap_or_default();
                let delimiter = param("delimiter");
                state
                    .requests
                    .lock()
                    .unwrap()
                    .push(format!("LIST {prefix}"));
                let body = list_body(state, &prefix, delimiter.as_deref());
                if respond(&mut out, "200 OK", &[], body.as_bytes(), None).is_err() {
                    return;
                }
                continue;
            }
        }
        // `/bucket/key`, the bucket dropped.
        let key = target
            .trim_start_matches('/')
            .split_once('/')
            .map_or(String::new(), |(_, key)| key.to_string());
        let range = headers.get("range").cloned();
        let logged = match &range {
            Some(range) => format!("{method} {key} {range}"),
            None => format!("{method} {key}"),
        };
        if let Some(hook) = &*state.hook.lock().unwrap() {
            hook(&logged);
        }
        state.requests.lock().unwrap().push(logged);

        let object = state.objects.lock().unwrap().get(&key).cloned();
        let Some(Stored { body, tag }) = object else {
            if respond(&mut out, "404 Not Found", &[], &[], None).is_err() {
                return;
            }
            continue;
        };
        if headers.get("if-match").is_some_and(|wanted| *wanted != tag) {
            if respond(&mut out, "412 Precondition Failed", &[], &[], None).is_err() {
                return;
            }
            continue;
        }
        let total = body.len();
        let (status, bytes) = match range.as_deref().and_then(|r| parse_range(r, total)) {
            Some((start, end)) => ("206 Partial Content", start..end),
            None => ("200 OK", 0..total),
        };
        let mut head = vec![
            ("ETag", tag),
            ("Last-Modified", "Thu, 01 Oct 2026 00:00:00 GMT".to_string()),
            ("Accept-Ranges", "bytes".to_string()),
        ];
        if status.starts_with("206") {
            let last = bytes.end - 1;
            head.push((
                "Content-Range",
                format!("bytes {}-{last}/{total}", bytes.start),
            ));
        }
        let content = &body[bytes];
        let sent = (method == "GET").then_some(&state.sent);
        let body = if method == "GET" { content } else { &[] };
        if respond_sized(&mut out, status, &head, content.len(), body, sent).is_err() {
            return;
        }
    }
}

/// `bytes=a-b` or `bytes=a-` as a half-open range within `total`.
fn parse_range(range: &str, total: usize) -> Option<(usize, usize)> {
    let (start, end) = range.strip_prefix("bytes=")?.split_once('-')?;
    let start: usize = start.parse().ok()?;
    let end = match end {
        "" => total,
        end => (end.parse::<usize>().ok()? + 1).min(total),
    };
    (start < end).then_some((start, end))
}

/// A response whose body is `body`.
fn respond(
    out: &mut TcpStream,
    status: &str,
    head: &[(&str, String)],
    body: &[u8],
    sent: Option<&AtomicU64>,
) -> std::io::Result<()> {
    respond_sized(out, status, head, body.len(), body, sent)
}

/// A response whose `Content-Length` is `length` — the object's for a `HEAD`, which sends no body.
fn respond_sized(
    out: &mut TcpStream,
    status: &str,
    head: &[(&str, String)],
    length: usize,
    body: &[u8],
    sent: Option<&AtomicU64>,
) -> std::io::Result<()> {
    let mut text = format!("HTTP/1.1 {status}\r\nContent-Length: {length}\r\n");
    for (name, value) in head {
        text.push_str(&format!("{name}: {value}\r\n"));
    }
    text.push_str("\r\n");
    out.write_all(text.as_bytes())?;
    for chunk in body.chunks(64 * 1024) {
        out.write_all(chunk)?;
        if let Some(sent) = sent {
            sent.fetch_add(chunk.len() as u64, Ordering::SeqCst);
        }
    }
    out.flush()
}

/// A `ListObjectsV2` response for the objects under `prefix`: every key under it, or with a
/// delimiter, the keys directly under it and the "directories" below those.
fn list_body(state: &State, prefix: &str, delimiter: Option<&str>) -> String {
    let objects = state.objects.lock().unwrap();
    let mut keys: Vec<(&String, &Stored)> = objects
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .collect();
    keys.sort_by(|a, b| a.0.cmp(b.0));
    let mut contents = String::new();
    let mut prefixes: Vec<String> = Vec::new();
    for (key, stored) in keys {
        let rest = &key[prefix.len()..];
        if let Some(delimiter) = delimiter.filter(|d| !d.is_empty()) {
            if let Some(at) = rest.find(delimiter) {
                let common = format!("{prefix}{}", &rest[..at + delimiter.len()]);
                if !prefixes.contains(&common) {
                    prefixes.push(common);
                }
                continue;
            }
        }
        contents.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>2026-10-01T00:00:00.000Z</LastModified>\
             <ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(key),
            xml_escape(&stored.tag),
            stored.body.len()
        ));
    }
    let common: String = prefixes
        .iter()
        .map(|p| {
            format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(p)
            )
        })
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>bucket</Name><Prefix>{}</Prefix><MaxKeys>1000</MaxKeys>\
         <IsTruncated>false</IsTruncated>{contents}{common}</ListBucketResult>",
        xml_escape(prefix)
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// `%XX` decoded, and `+` left as it is: what `object_store` sends in a query.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%' && i + 2 < bytes.len())
            .then(|| std::str::from_utf8(&bytes[i + 1..i + 3]).ok())
            .flatten()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
