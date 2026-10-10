//! A stand-in for an Iceberg REST catalog on localhost: `GET /v1/config`, the namespace and table
//! listings (paged when asked to), `loadTable`, a table's credentials, and an OAuth2 token
//! endpoint, over namespaces and tables held in memory. Every request is logged with the token it
//! carried.
//!
//! Its base URI has a path of its own (`/api/catalog`), as a catalog behind a gateway does, so the
//! client's URLs are checked to keep it.
//!
//! Included with `#[path = "support/rest_catalog.rs"] mod rest_catalog;`.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

/// The base path every catalog route is under.
pub const BASE: &str = "/api/catalog";

/// A request the catalog answered.
#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    /// The path as sent, percent-encoded, without the query.
    pub path: String,
    pub query: String,
    /// The bearer token it carried.
    pub bearer: Option<String>,
    /// Its `X-Iceberg-Access-Delegation` header.
    pub delegation: Option<String>,
    pub body: String,
}

/// How the catalog checks callers.
#[derive(Debug, Clone, Default)]
pub enum Login {
    /// Anyone may call.
    #[default]
    Open,
    /// One static bearer token.
    Token(String),
    /// OAuth2 client credentials, exchanged at either token endpoint for tokens that live
    /// `expires_in` seconds, when that is set.
    Client {
        id: String,
        secret: String,
        expires_in: Option<u64>,
    },
}

/// A table the catalog serves.
#[derive(Debug, Clone)]
pub struct Table {
    pub metadata_location: String,
    pub metadata: Value,
    pub config: BTreeMap<String, String>,
    pub read_restrictions: Option<Value>,
}

impl Table {
    pub fn new(metadata_location: &str, metadata: Value) -> Table {
        Table {
            metadata_location: metadata_location.to_string(),
            metadata,
            config: BTreeMap::new(),
            read_restrictions: None,
        }
    }
}

/// How the last page of a listing says there are no more.
#[derive(Clone, Copy, Debug, Default)]
pub enum LastPage {
    /// `next-page-token: null`, as the spec has it.
    #[default]
    Null,
    /// `next-page-token: ""`, as some catalogs send.
    Empty,
    /// The token the client just sent, again: a catalog whose pages would never end.
    Repeat,
}

/// Credentials the catalog vends for the locations under `prefix`, for an S3 stand-in at
/// `endpoint`. Each `loadTable` or credentials request vends a new access key, `VENDED-<n>`, that
/// expires `expires_in_ms` after it is vended (and so, when negative, before).
#[derive(Debug, Clone)]
pub struct Vending {
    pub prefix: String,
    pub endpoint: String,
    pub expires_in_ms: Option<i64>,
}

#[derive(Default)]
struct State {
    requests: Mutex<Vec<Request>>,
    login: Mutex<Login>,
    tokens: Mutex<Vec<String>>,
    issued: AtomicUsize,
    prefix: Mutex<Option<String>>,
    endpoints: Mutex<Option<Vec<String>>>,
    defaults: Mutex<BTreeMap<String, String>>,
    page_size: Mutex<Option<usize>>,
    last_page: Mutex<LastPage>,
    ignores_parent: Mutex<bool>,
    token_outage: Mutex<bool>,
    stall: Mutex<Option<Duration>>,
    namespaces: Mutex<BTreeSet<Vec<String>>>,
    tables: Mutex<BTreeMap<(Vec<String>, String), Table>>,
    vending: Mutex<Option<Vending>>,
    vended: AtomicUsize,
}

pub struct FakeCatalog {
    addr: SocketAddr,
    state: Arc<State>,
}

impl FakeCatalog {
    /// A catalog on a port of its own, answering each connection on a thread of its own.
    pub fn start() -> FakeCatalog {
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
        FakeCatalog { addr, state }
    }

    /// The catalog's base URI, as `catalogs.toml` names it.
    pub fn uri(&self) -> String {
        format!("http://{}{BASE}", self.addr)
    }

    /// The token endpoint an identity provider would run, beside the catalog.
    pub fn token_endpoint(&self) -> String {
        format!("http://{}/idp/token", self.addr)
    }

    pub fn set_login(&self, login: Login) {
        *self.state.login.lock().unwrap() = login;
    }

    /// Serve the API under `/v1/{prefix}`, as `GET /v1/config` says.
    pub fn set_prefix(&self, prefix: &str) {
        *self.state.prefix.lock().unwrap() = Some(prefix.to_string());
    }

    /// Say which endpoints are supported, as `GET /v1/config` lists them.
    pub fn set_endpoints(&self, endpoints: &[&str]) {
        *self.state.endpoints.lock().unwrap() =
            Some(endpoints.iter().map(|e| e.to_string()).collect());
    }

    /// A default `GET /v1/config` hands the client.
    pub fn set_default(&self, key: &str, value: &str) {
        self.state
            .defaults
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
    }

    /// Page listings `size` entries at a time when the client sends a `pageToken`.
    pub fn set_page_size(&self, size: usize) {
        *self.state.page_size.lock().unwrap() = Some(size);
    }

    /// Say a listing has no more pages the way `last` does.
    pub fn end_listings_with(&self, last: LastPage) {
        *self.state.last_page.lock().unwrap() = last;
    }

    /// Answer every namespace listing with the top-level namespaces, whatever `parent` asks for,
    /// as a catalog serving one flat namespace does.
    pub fn ignore_parent(&self) {
        *self.state.ignores_parent.lock().unwrap() = true;
    }

    /// Add a namespace, and the ones above it.
    pub fn add_namespace(&self, levels: &[&str]) {
        let mut namespaces = self.state.namespaces.lock().unwrap();
        for depth in 1..=levels.len() {
            namespaces.insert(levels[..depth].iter().map(|l| l.to_string()).collect());
        }
    }

    /// Add `table` to namespace `levels`, creating the namespace.
    pub fn add_table(&self, levels: &[&str], name: &str, table: Table) {
        self.add_namespace(levels);
        let levels = levels.iter().map(|l| l.to_string()).collect();
        self.state
            .tables
            .lock()
            .unwrap()
            .insert((levels, name.to_string()), table);
    }

    pub fn vend(&self, vending: Vending) {
        *self.state.vending.lock().unwrap() = Some(vending);
    }

    /// Make the token endpoint answer 503, as an identity provider that is down does.
    pub fn token_outage(&self) {
        *self.state.token_outage.lock().unwrap() = true;
    }

    /// Wait this long before answering each request, as a catalog that hangs does.
    pub fn stall(&self, wait: Duration) {
        *self.state.stall.lock().unwrap() = Some(wait);
    }

    /// Stop accepting every token issued so far, as their expiry would.
    pub fn revoke_tokens(&self) {
        self.state.tokens.lock().unwrap().clear();
    }

    /// How many access keys have been vended.
    pub fn vended(&self) -> usize {
        self.state.vended.load(Ordering::SeqCst)
    }

    pub fn requests(&self) -> Vec<Request> {
        self.state.requests.lock().unwrap().clone()
    }

    /// The requests, as `METHOD path` with the base path dropped.
    pub fn calls(&self) -> Vec<String> {
        self.requests()
            .iter()
            .map(|r| {
                let path = r.path.strip_prefix(BASE).unwrap_or(&r.path);
                format!("{} {path}", r.method)
            })
            .collect()
    }

    pub fn reset(&self) {
        self.state.requests.lock().unwrap().clear();
    }
}

/// Answer the requests on one connection until the client closes it.
fn serve(stream: TcpStream, state: &State) {
    let Ok(read) = stream.try_clone() else { return };
    let mut reader = BufReader::new(read);
    let mut out = stream;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut headers = HashMap::new();
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).unwrap_or(0) == 0 {
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
        let length: usize = headers
            .get("content-length")
            .and_then(|l| l.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        let mut words = line.split_whitespace();
        let method = words.next().unwrap_or_default().to_string();
        let target = words.next().unwrap_or_default();
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let request = Request {
            method,
            path: path.to_string(),
            query: query.to_string(),
            bearer: headers
                .get("authorization")
                .and_then(|a| a.strip_prefix("Bearer "))
                .map(str::to_string),
            delegation: headers.get("x-iceberg-access-delegation").cloned(),
            body: String::from_utf8_lossy(&body).into_owned(),
        };
        state.requests.lock().unwrap().push(request.clone());
        let stall = *state.stall.lock().unwrap();
        if let Some(wait) = stall {
            std::thread::sleep(wait);
        }
        let (status, body) = route(state, &request);
        let text = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        if out.write_all(text.as_bytes()).is_err() || out.write_all(body.as_bytes()).is_err() {
            return;
        }
    }
}

fn error(status: &str, kind: &str, message: &str) -> (String, String) {
    let code: u16 = status[..3].parse().unwrap();
    (
        status.to_string(),
        json!({"error": {"message": message, "type": kind, "code": code}}).to_string(),
    )
}

fn ok(body: Value) -> (String, String) {
    ("200 OK".to_string(), body.to_string())
}

fn route(state: &State, request: &Request) -> (String, String) {
    let path = request.path.as_str();
    if request.method == "POST"
        && (path == "/idp/token" || path == format!("{BASE}/v1/oauth/tokens"))
    {
        return token(state, request);
    }
    if !authorized(state, request) {
        return error(
            "401 Unauthorized",
            "NotAuthorizedException",
            "Not authorized",
        );
    }
    let Some(rest) = path.strip_prefix(&format!("{BASE}/v1/")) else {
        return error("404 Not Found", "NotFound", "no such route");
    };
    if rest == "config" {
        let mut overrides = serde_json::Map::new();
        if let Some(prefix) = state.prefix.lock().unwrap().clone() {
            overrides.insert("prefix".into(), Value::String(prefix));
        }
        let mut body = json!({
            "defaults": *state.defaults.lock().unwrap(),
            "overrides": overrides,
        });
        if let Some(endpoints) = state.endpoints.lock().unwrap().clone() {
            body["endpoints"] = json!(endpoints);
        }
        return ok(body);
    }
    let mut segments: Vec<String> = rest.split('/').map(decode).collect();
    if let Some(prefix) = state.prefix.lock().unwrap().clone() {
        if segments.first() != Some(&prefix) {
            return error("404 Not Found", "NotFound", "no such prefix");
        }
        segments.remove(0);
    }
    let parts: Vec<&str> = segments.iter().map(String::as_str).collect();
    let levels = |ns: &str| -> Vec<String> { ns.split('\u{1f}').map(str::to_string).collect() };
    match parts.as_slice() {
        ["namespaces"] => {
            let parent = query_param(&request.query, "parent")
                .filter(|p| !p.is_empty() && !*state.ignores_parent.lock().unwrap())
                .map(|p| levels(&p))
                .unwrap_or_default();
            let children: Vec<Value> = state
                .namespaces
                .lock()
                .unwrap()
                .iter()
                .filter(|ns| ns.len() == parent.len() + 1 && ns.starts_with(&parent))
                .map(|ns| json!(ns))
                .collect();
            paged(state, &request.query, "namespaces", children)
        }
        ["namespaces", ns] => {
            let ns = levels(ns);
            if state.namespaces.lock().unwrap().contains(&ns) {
                ok(json!({"namespace": ns, "properties": {}}))
            } else {
                error(
                    "404 Not Found",
                    "NoSuchNamespaceException",
                    "Namespace does not exist",
                )
            }
        }
        ["namespaces", ns, "tables"] => {
            let ns = levels(ns);
            if !state.namespaces.lock().unwrap().contains(&ns) {
                return error(
                    "404 Not Found",
                    "NoSuchNamespaceException",
                    "Namespace does not exist",
                );
            }
            let identifiers: Vec<Value> = state
                .tables
                .lock()
                .unwrap()
                .keys()
                .filter(|(levels, _)| *levels == ns)
                .map(|(levels, name)| json!({"namespace": levels, "name": name}))
                .collect();
            paged(state, &request.query, "identifiers", identifiers)
        }
        ["namespaces", ns, "tables", table] => load_table(state, request, &levels(ns), table),
        ["namespaces", ns, "tables", table, "credentials"] => {
            let ns = levels(ns);
            if !state
                .tables
                .lock()
                .unwrap()
                .contains_key(&(ns, table.to_string()))
            {
                return error(
                    "404 Not Found",
                    "NoSuchTableException",
                    "Table does not exist",
                );
            }
            let creds: Vec<Value> = vend(state).into_iter().collect();
            ok(json!({"storage-credentials": creds}))
        }
        _ => error("404 Not Found", "NotFound", "no such route"),
    }
}

fn load_table(state: &State, request: &Request, ns: &[String], table: &str) -> (String, String) {
    if !state.namespaces.lock().unwrap().contains(ns) {
        return error(
            "404 Not Found",
            "NoSuchNamespaceException",
            "Namespace does not exist",
        );
    }
    let Some(found) = state
        .tables
        .lock()
        .unwrap()
        .get(&(ns.to_vec(), table.to_string()))
        .cloned()
    else {
        return error(
            "404 Not Found",
            "NoSuchTableException",
            "Table does not exist",
        );
    };
    let delegated = request
        .delegation
        .as_deref()
        .is_some_and(|d| d.contains("vended-credentials"));
    let creds: Vec<Value> = if delegated {
        vend(state).into_iter().collect()
    } else {
        Vec::new()
    };
    let mut body = json!({
        "metadata-location": found.metadata_location,
        "metadata": found.metadata,
        "config": found.config,
        "storage-credentials": creds,
    });
    if let Some(restrictions) = found.read_restrictions {
        body["read-restrictions"] = restrictions;
    }
    ok(body)
}

/// A fresh access key for the vended prefix, when the catalog vends.
fn vend(state: &State) -> Option<Value> {
    let vending = state.vending.lock().unwrap().clone()?;
    let n = state.vended.fetch_add(1, Ordering::SeqCst) + 1;
    let mut config = json!({
        "s3.access-key-id": format!("VENDED-{n}"),
        "s3.secret-access-key": format!("secret-{n}"),
        "s3.session-token": format!("session-{n}"),
        "s3.endpoint": vending.endpoint,
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    });
    if let Some(ms) = vending.expires_in_ms {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        config["s3.session-token-expires-at-ms"] = json!((now + ms).to_string());
    }
    Some(json!({"prefix": vending.prefix, "config": config}))
}

fn token(state: &State, request: &Request) -> (String, String) {
    if *state.token_outage.lock().unwrap() {
        return (
            "503 Service Unavailable".to_string(),
            json!({"error": "temporarily_unavailable"}).to_string(),
        );
    }
    let form: HashMap<String, String> = url_form(&request.body);
    let Login::Client {
        id,
        secret,
        expires_in,
    } = state.login.lock().unwrap().clone()
    else {
        return (
            "400 Bad Request".to_string(),
            json!({"error": "unsupported_grant_type"}).to_string(),
        );
    };
    let good = form.get("grant_type").map(String::as_str) == Some("client_credentials")
        && form.get("client_id") == Some(&id)
        && form.get("client_secret") == Some(&secret);
    if !good {
        return (
            "401 Unauthorized".to_string(),
            json!({"error": "invalid_client", "error_description": "Bad client credentials"})
                .to_string(),
        );
    }
    let n = state.issued.fetch_add(1, Ordering::SeqCst) + 1;
    let token = format!("tok-{n}");
    state.tokens.lock().unwrap().push(token.clone());
    let mut body = json!({
        "access_token": token,
        "token_type": "bearer",
        "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
    });
    if let Some(secs) = expires_in {
        body["expires_in"] = json!(secs);
    }
    ok(body)
}

fn authorized(state: &State, request: &Request) -> bool {
    match &*state.login.lock().unwrap() {
        Login::Open => true,
        Login::Token(token) => request.bearer.as_ref() == Some(token),
        Login::Client { .. } => request
            .bearer
            .as_ref()
            .is_some_and(|b| state.tokens.lock().unwrap().contains(b)),
    }
}

/// `entries` under `key`, a page at a time when the client asks with a `pageToken` and the catalog
/// pages.
fn paged(state: &State, query: &str, key: &str, entries: Vec<Value>) -> (String, String) {
    let page = *state.page_size.lock().unwrap();
    let (Some(size), Some(token)) = (page, query_param(query, "pageToken")) else {
        return ok(json!({ key: entries }));
    };
    let start: usize = token
        .strip_prefix('p')
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let end = (start + size).min(entries.len());
    let mut body = json!({ key: entries[start.min(end)..end] });
    body["next-page-token"] = if end < entries.len() {
        json!(format!("p{end}"))
    } else {
        match *state.last_page.lock().unwrap() {
            LastPage::Null => Value::Null,
            LastPage::Empty => json!(""),
            LastPage::Repeat => json!(token),
        }
    };
    ok(body)
}

fn query_param(query: &str, name: &str) -> Option<String> {
    url_form(query).remove(name)
}

fn url_form(text: &str) -> HashMap<String, String> {
    text.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(&k.replace('+', " ")), decode(&v.replace('+', " ")))
        })
        .collect()
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(byte) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
