//! The `lakeleto serve` HTTP/JSON backend (`--features serve`).
//!
//! This is the API the Phase-2 localhost SPA calls, and — crucially — the **same contract**
//! the `RemoteEngine` (Lakeleto Cloud) client speaks (`GET /v1/schema`, `GET /v1/profile`,
//! …). Building it now means the endpoint surface exists before either the UI or the hosted
//! plane, so both are built against a fixed contract.
//!
//! Every handler holds an `Arc<dyn Engine>` and calls the trait — the server never knows
//! which backend answered. Engine calls run on `spawn_blocking` because they are synchronous
//! (and the DataFusion engine drives its own runtime); doing so keeps the async request
//! threads free and avoids nesting one runtime inside another.
//!
//! ## Endpoints
//! | Method | Path | Purpose |
//! |--------|------|---------|
//! | GET  | `/healthz` | liveness |
//! | GET  | `/v1/engines` | capabilities of the serving engine + endpoint list |
//! | GET  | `/v1/schema?path=&format=` | [`TableSchema`] |
//! | GET  | `/v1/info?path=&format=` | source info (format, size, rows, columns) |
//! | GET  | `/v1/preview?path=&limit=&format=` | first N rows as `{columns, rows}` |
//! | GET  | `/v1/profile?path=&scan=&format=` | [`TableProfile`] |
//! | GET  | `/v1/rows?path=&offset=&limit=&sort=&desc=&filter=&cols=` | grid window (filter→sort→page→project) |
//! | GET  | `/v1/stats?path=&filter=` | column profile over the *filtered* view |
//! | GET  | `/v1/export?path=&fmt=&sort=&filter=&cols=` | current view as a CSV/JSON/Parquet download |
//! | GET  | `/v1/list?dir=` | file browser: subdirs + readable data files |
//! | POST | `/v1/query` | `{sql, file?, tables[]}` → `{columns, rows}` (needs `sql`) |
//! | GET/POST | `/v1/workspaces` | list / create a workspace (the "Postman" data plane) |
//! | GET/PUT/DELETE | `/v1/workspaces/{id}` | fetch / save / delete a workspace |
//! | GET/POST | `/v1/workspaces/{id}/history` | run history (newest first) / sync-append a record |
//! | POST | `/v1/workspaces/{id}/runs` | run SQL/scan (root-confined), record + cache the result |
//! | GET  | `/v1/workspaces/{id}/runs/{run_id}` | a window over a cached run result |
//! | PUT/GET | `/v1/workspaces/{id}/runs/{run_id}/result` | raw Parquet result bytes (sync up/down) |
//! | GET  | `/v1/workspaces/{id}/export` | download a portable [`WorkspaceBundle`] |
//! | POST | `/v1/workspaces/import` | import a bundle (always mints a fresh id) |
//! | GET  | `/` and any non-API path | the embedded SPA shell (with SPA fallback) |
//!
//! Workspace endpoints persist through a [`WorkspaceStore`] (the data-plane seam mirroring
//! [`Engine`]): a local JSON+Parquet store today, a cloud store behind the same contract later.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{DefaultBodyLimit, Path as UrlPath, Query, RawQuery, Request, State},
    http::{header, StatusCode, Uri},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};

use crate::engine::{
    Capabilities, ColumnSchema, Engine, FilterOp, FilterSpec, NamedSource, RowBatch, ScanResult,
    ScanSpec, SortSpec, TableProfile, TableSchema,
};
use crate::error::EngineError;
use crate::source::{list_dir, DirListing, Format, Source};
use crate::workspace::{RunRecord, RunStatus, Workspace, WorkspaceBundle, WorkspaceStore};

/// Export view cap — the most rows `GET /v1/export` will materialize.
const EXPORT_CAP: usize = 1_000_000;

/// Export byte cap — the largest rendered `GET /v1/export` body we'll return. A companion to
/// [`EXPORT_CAP`] (rows): a wide table can blow past a reasonable memory/transfer budget long
/// before a million rows, so the response is bounded in bytes too and rejected (413) past this.
/// (End-to-end streaming would need a streaming-scan across the `Engine` trait — a later step.)
const MAX_EXPORT_BYTES: usize = 512 * 1024 * 1024;

/// The embedded SPA shell (`frontend/dist/`). Served at `/` with SPA fallback so the UI is
/// bundled into the single `lakeleto` binary — no separate web server, works air-gapped.
#[derive(RustEmbed)]
#[folder = "frontend/dist/"]
struct Assets;

/// Endpoints advertised by `GET /v1/engines` (kept next to the routes so they stay in sync).
const ENDPOINTS: &[&str] = &[
    "GET /healthz",
    "GET /v1/engines",
    "GET /v1/schema?path=&format=",
    "GET /v1/info?path=&format=",
    "GET /v1/preview?path=&limit=&format=",
    "GET /v1/profile?path=&scan=&format=",
    "GET /v1/rows?path=&offset=&limit=&sort=&desc=&filter=col:op:value&cols=a,b",
    "GET /v1/stats?path=&filter=col:op:value",
    "GET /v1/export?path=&fmt=csv|json|parquet&sort=&filter=&cols=",
    "GET /v1/list?dir=",
    "POST /v1/query",
    "GET|POST /v1/workspaces",
    "GET|PUT|DELETE /v1/workspaces/{id}",
    "GET|POST /v1/workspaces/{id}/history",
    "GET /v1/workspaces/{id}/export",
    "POST /v1/workspaces/import",
    "POST /v1/workspaces/{id}/runs",
    "GET /v1/workspaces/{id}/runs/{run_id}?offset=&limit=",
    "PUT|GET /v1/workspaces/{id}/runs/{run_id}/result",
];

/// Shared server state: the read engine (schema/preview/profile/info) and, when compiled with
/// `sql`, the SQL engine for `POST /v1/query`.
#[derive(Clone)]
pub struct AppState {
    read: Arc<dyn Engine>,
    sql: Option<Arc<dyn Engine>>,
    default_scan: usize,
    /// When set, `/v1/*` requires this bearer token (header always; `?token=` only on a loopback
    /// bind). The SPA + `/healthz` are exempt so the page can load and health checks stay open.
    token: Option<Arc<str>>,
    /// When set, `/v1/*` file access is confined to this (canonical) directory — reads/browse
    /// outside it are refused. Off by default (the local tool's "point at any file" behaviour).
    root: Option<Arc<PathBuf>>,
    /// Whether the server is bound to a loopback address. A `?token=` query credential is only
    /// honoured on loopback — over the network the header is required (a token in a URL leaks).
    loopback: bool,
    /// Persistent workspaces + query history + result cache (the "Postman" data plane). Behind the
    /// [`WorkspaceStore`] trait so a synced cloud store drops in later without a route change.
    store: Arc<dyn WorkspaceStore>,
}

/// Build the router. Exposed (not just `serve`) so it can be driven in tests without a socket.
#[allow(clippy::too_many_arguments)]
pub fn router(
    read: Arc<dyn Engine>,
    sql: Option<Arc<dyn Engine>>,
    default_scan: usize,
    token: Option<String>,
    root: Option<PathBuf>,
    loopback: bool,
    store: Arc<dyn WorkspaceStore>,
) -> Router {
    let state = AppState {
        read,
        sql,
        default_scan,
        token: token.map(Arc::from),
        root: root.map(Arc::new),
        loopback,
        store,
    };
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/engines", get(engines))
        .route("/v1/schema", get(schema))
        .route("/v1/info", get(info))
        .route("/v1/preview", get(preview))
        .route("/v1/profile", get(profile))
        .route("/v1/rows", get(rows))
        .route("/v1/stats", get(stats))
        .route("/v1/export", get(export))
        .route("/v1/list", get(list))
        .route("/v1/query", post(query))
        // Workspaces (persistent Postman-style data plane).
        .route("/v1/workspaces", get(ws_list).post(ws_create))
        .route("/v1/workspaces/import", post(ws_import))
        .route(
            "/v1/workspaces/{id}",
            get(ws_get).put(ws_save).delete(ws_delete),
        )
        .route(
            "/v1/workspaces/{id}/history",
            get(ws_history).post(ws_history_append),
        )
        .route("/v1/workspaces/{id}/export", get(ws_export))
        .route("/v1/workspaces/{id}/runs", post(ws_run))
        .route("/v1/workspaces/{id}/runs/{run_id}", get(ws_run_result))
        // Result sync: raw Parquet bytes of a cached result, up (PUT) and down (GET). This is
        // what lets a RemoteStore/cloud plane mirror the local cache byte-for-byte. The raised
        // body limit is scoped to this one route (uploads are parse-validated Parquet).
        .route(
            "/v1/workspaces/{id}/runs/{run_id}/result",
            put(ws_put_result)
                .get(ws_result_raw)
                .layer(DefaultBodyLimit::max(MAX_RESULT_UPLOAD_BYTES)),
        )
        // Everything else: embedded static assets, with SPA fallback to index.html.
        .fallback(spa)
        // Bearer-auth gate on /v1/* (no-op when no token is configured).
        .layer(from_fn_with_state(state.clone(), auth))
        .with_state(state)
}

/// Require the configured bearer token on `/v1/*`. The token may arrive as
/// `Authorization: Bearer <t>` or a `?token=<t>` query param (the latter lets the browser
/// authenticate `/v1/export` downloads and initial deep-links). Constant-time compared.
async fn auth(State(st): State<AppState>, req: Request, next: Next) -> Response {
    if let Some(expected) = &st.token {
        if req.uri().path().starts_with("/v1/") {
            let ok = token_from(&req, st.loopback)
                .map(|t| ct_eq(t.as_bytes(), expected.as_bytes()))
                .unwrap_or(false);
            if !ok {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "error": "unauthorized — send the bearer token as `Authorization: Bearer …` (the `?token=` query form is accepted only on a loopback bind)"
                    })),
                )
                    .into_response();
            }
        }
    }
    next.run(req).await
}

/// The bearer token carried by a request. The `Authorization: Bearer` header is always honoured;
/// the `?token=` query param is honoured **only on a loopback bind** (`allow_query`), because a
/// token in a URL leaks through browser history, shell history, and proxy/access logs.
fn token_from(req: &Request, allow_query: bool) -> Option<String> {
    if let Some(h) = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(t) = h.strip_prefix("Bearer ") {
            return Some(t.trim().to_string());
        }
    }
    if !allow_query {
        return None;
    }
    let pairs = serde_urlencoded::from_str::<Vec<(String, String)>>(req.uri().query()?).ok()?;
    pairs
        .into_iter()
        .find(|(k, _)| k == "token")
        .map(|(_, v)| v)
}

/// Length-checked constant-time byte comparison (avoids leaking token length via early exit
/// beyond what the length check already reveals).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Serve embedded assets; fall back to `index.html` for client-side routes. API namespaces
/// (`/v1/*`, `/healthz`) are handled above, so a miss there must 404 as JSON — never the SPA.
async fn spa(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path == "healthz" || path.starts_with("v1/") {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no such endpoint: /{path}") })),
        )
            .into_response();
    }
    let asset = if path.is_empty() { "index.html" } else { path };
    if let Some(file) = Assets::get(asset) {
        return asset_response(asset, &file.data);
    }
    // Unknown path → hand the SPA its index.html so client-side routing can take over.
    match Assets::get("index.html") {
        Some(file) => asset_response("index.html", &file.data),
        None => (StatusCode::NOT_FOUND, "no UI bundled\n").into_response(),
    }
}

fn asset_response(path: &str, body: &[u8]) -> Response {
    let mime = match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") | Some("mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("png") => "image/png",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    };
    ([(header::CONTENT_TYPE, mime)], body.to_vec()).into_response()
}

/// Bind `addr` and serve until Ctrl-C. Owns its own multi-thread Tokio runtime so the
/// synchronous [`crate::cli::run`] can call it directly. When `open_url` is set (`lakeleto open`),
/// the browser is launched once the socket is bound.
#[allow(clippy::too_many_arguments)]
pub fn serve(
    addr: &str,
    read: Arc<dyn Engine>,
    sql: Option<Arc<dyn Engine>>,
    default_scan: usize,
    open_url: Option<String>,
    token: Option<String>,
    root: Option<PathBuf>,
    store: Option<Arc<dyn WorkspaceStore>>,
) -> crate::error::Result<()> {
    let loopback = is_loopback(addr);
    let root_note = root.as_ref().map(|r| r.display().to_string());
    // The workspace store: the caller's override (e.g. a RemoteStore syncing to another server
    // or Lakeleto Cloud), else the on-disk default under $LAKELETO_HOME.
    let store: Arc<dyn WorkspaceStore> = match store {
        Some(s) => s,
        None => Arc::new(crate::workspace::LocalStore::open()?),
    };
    let app = router(
        read,
        sql,
        default_scan,
        token.clone(),
        root,
        loopback,
        store,
    );
    // A non-loopback bind with no token is an open API on the network — warn loudly.
    if token.is_none() && !is_loopback(addr) {
        eprintln!("lakeleto: WARNING binding {addr} (non-loopback) with no --token — the API is unauthenticated");
    }
    if let Some(r) = &root_note {
        eprintln!("lakeleto: /v1/* file access confined to --root {r}");
    }
    // Precompute the URL to open (`lakeleto open`) or print (`lakeleto serve` + token), with the token
    // appended so the browser authenticates. Done before the async move so `token` isn't
    // borrowed and moved at once.
    let browser_url = open_url.map(|u| append_token(&u, token.as_deref()));
    let serve_hint = if token.is_some() && browser_url.is_none() {
        Some(append_token(&format!("http://{addr}/"), token.as_deref()))
    } else {
        None
    };
    let require_note = token.is_some();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| EngineError::Other(format!("build server runtime: {e}")))?;
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| EngineError::Other(format!("bind {addr}: {e}")))?;
        eprintln!("lakeleto: listening on http://{addr}  (Ctrl-C to stop)");
        if require_note {
            eprintln!("lakeleto: bearer token required on /v1/*");
        }
        if let Some(url) = browser_url {
            open_browser(&url);
        } else if let Some(url) = serve_hint {
            eprintln!("lakeleto: open {url}");
        }
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| EngineError::Other(format!("server: {e}")))?;
        Ok::<(), EngineError>(())
    })
}

/// Append `?token=`/`&token=` to a URL when a token is configured (so the browser authenticates).
fn append_token(base: &str, token: Option<&str>) -> String {
    match token {
        Some(t) => format!(
            "{base}{}token={}",
            if base.contains('?') { "&" } else { "?" },
            encode_query(t)
        ),
        None => base.to_string(),
    }
}

/// Best-effort loopback check for the bind-without-token warning.
fn is_loopback(addr: &str) -> bool {
    addr.starts_with("127.")
        || addr.starts_with("localhost")
        || addr.starts_with("[::1]")
        || addr.starts_with("::1")
}

/// The uniform "outside `--root`" refusal. The message is deliberately the same whether the path
/// is out-of-root, non-existent, or unreadable, so a token holder can't use `/v1/*` as an
/// existence/type oracle over the filesystem outside the root.
fn out_of_root() -> ApiError {
    ApiError(EngineError::Forbidden(
        "path is outside the server root (--root)".to_string(),
    ))
}

/// **Pre-resolve** confinement of a request path to `--root`: refuses object-store URIs and
/// anything anchored outside the root *before* [`Source::resolve`]/`detect` touches the
/// filesystem — so an out-of-root path can't be used as an existence/type/readability oracle (via
/// `detect`'s `is_dir`/`read_dir`/`sniff_magic`), nor trigger a recursive-`read_dir` DoS. A
/// missing leaf *inside* the root is allowed through so the reader still returns a normal 404.
/// No-op when no root is configured (the default "point at any file" behaviour).
fn confine_entry(root: &Option<Arc<PathBuf>>, path: &str) -> Result<(), ApiError> {
    let Some(root) = root else { return Ok(()) };
    if crate::source::is_object_uri(path) {
        return Err(out_of_root()); // --root is local-filesystem only
    }
    // Walk up to the nearest existing ancestor and canonicalize it (symlinks resolved). If that
    // lies under the root the request is in-root (a missing leaf 404s later); otherwise — or when
    // nothing along the path exists — it's refused with the same error, leaking nothing.
    let mut cur = Path::new(path);
    loop {
        if let Ok(canon) = std::fs::canonicalize(cur) {
            return if canon.starts_with(root.as_ref()) {
                Ok(())
            } else {
                Err(out_of_root())
            };
        }
        match cur.parent() {
            Some(p) if !p.as_os_str().is_empty() => cur = p,
            _ => return Err(out_of_root()),
        }
    }
}

/// Confine every file the engine will actually **read** for `source` to `--root`. The entry path
/// is already gated by [`confine_entry`], but a directory dataset reads every member `.parquet`
/// (a symlink escaping the root is caught here by canonicalizing each), and an Iceberg table
/// reads whatever paths its manifests name — manifest list, manifests, delete files, and absolute
/// data-file paths — which can point outside the table dir. No-op without a root.
fn confine_members(root: &Option<Arc<PathBuf>>, source: &Source) -> Result<(), ApiError> {
    let Some(root) = root else { return Ok(()) };
    match source.format {
        Format::Parquet if source.path.is_dir() => {
            for f in crate::source::list_parquet_files(&source.path) {
                confine_canonical(root, &f)?;
            }
        }
        #[cfg(feature = "iceberg")]
        Format::Iceberg => {
            // Re-plan with the root so every path the reader will open (manifests + delete files
            // + data files) is validated *before* it is read — gating metadata too, not just data.
            crate::iceberg::plan_with_root(&source.path, Some(root.as_path())).map_err(ApiError)?;
        }
        _ => {}
    }
    Ok(())
}

/// Canonicalize `path` and require it under `root`; refuse (uniformly) on escape or any failure.
fn confine_canonical(root: &Arc<PathBuf>, path: &Path) -> Result<(), ApiError> {
    match std::fs::canonicalize(path) {
        Ok(canon) if canon.starts_with(root.as_ref()) => Ok(()),
        _ => Err(out_of_root()),
    }
}

/// Reject an export whose rendered body exceeds `max` bytes (→ 413). Extracted so the boundary
/// and the `TooLarge` mapping stay covered even though a real 512 MiB body is impractical to
/// materialize in a test.
fn export_cap_check(len: usize, max: usize) -> Result<(), EngineError> {
    if len > max {
        return Err(EngineError::TooLarge(format!(
            "export is {len} bytes, over the {max}-byte cap — narrow the view (filters / fewer \
             columns) or export a smaller window"
        )));
    }
    Ok(())
}

/// Default browse dir for `/v1/list`: the confinement root when one is set, else the cwd.
fn default_dir(root: &Option<Arc<PathBuf>>) -> String {
    match root {
        Some(r) => r.display().to_string(),
        None => ".".to_string(),
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Best-effort browser launch. Never fails the server — on error we just print the URL so the
/// user can open it manually (e.g. headless / CI, where there is no browser).
fn open_browser(url: &str) {
    let (cmd, args): (&str, &[&str]) = if cfg!(target_os = "macos") {
        ("open", &[])
    } else if cfg!(target_os = "windows") {
        ("cmd", &["/C", "start", ""])
    } else {
        ("xdg-open", &[])
    };
    let launched = std::process::Command::new(cmd)
        .args(args)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .is_ok();
    if launched {
        eprintln!("lakeleto: opening {url}");
    } else {
        eprintln!("lakeleto: open {url} in your browser");
    }
}

/// Percent-encode a query-parameter value (used to deep-link `lakeleto open <file>` to `?path=`).
pub fn encode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---- request / response wire types ----------------------------------------------------

#[derive(Deserialize)]
struct SourceQuery {
    path: String,
    format: Option<String>,
}

#[derive(Deserialize)]
struct PreviewQuery {
    path: String,
    format: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct ProfileQuery {
    path: String,
    format: Option<String>,
    scan: Option<usize>,
}

#[derive(Deserialize)]
struct QueryBody {
    sql: String,
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    tables: Vec<TableSpec>,
}

#[derive(Deserialize)]
struct TableSpec {
    name: String,
    path: String,
    #[serde(default)]
    format: Option<String>,
}

#[derive(Serialize)]
struct RowsResponse {
    columns: Vec<ColumnSchema>,
    num_rows: usize,
    rows: Vec<serde_json::Value>,
}

/// `GET /v1/rows` response: the row window plus counts to drive the virtual scrollbar.
#[derive(Serialize)]
struct RowsWindow {
    columns: Vec<ColumnSchema>,
    offset: usize,
    num_rows: usize,
    /// Total rows to size the scrollbar to (see `total_known`).
    matched_rows: usize,
    /// Whether `matched_rows` is exact (else the UI treats it as a lower bound and grows).
    total_known: bool,
    scanned_rows: usize,
    /// True when sort/filter ran over a capped working set (partial result for huge files).
    bounded: bool,
    rows: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct InfoResponse {
    path: String,
    format: String,
    engine: String,
    size_bytes: Option<u64>,
    row_count: Option<u64>,
    columns: usize,
}

#[derive(Serialize)]
struct EnginesResponse {
    engine: Capabilities,
    sql_available: bool,
    endpoints: Vec<&'static str>,
}

// ---- handlers -------------------------------------------------------------------------

async fn healthz() -> &'static str {
    "ok\n"
}

async fn engines(State(st): State<AppState>) -> Json<EnginesResponse> {
    Json(EnginesResponse {
        engine: st.read.capabilities(),
        sql_available: st.sql.is_some(),
        endpoints: ENDPOINTS.to_vec(),
    })
}

async fn schema(
    State(st): State<AppState>,
    Query(q): Query<SourceQuery>,
) -> Result<Json<TableSchema>, ApiError> {
    confine_entry(&st.root, &q.path)?;
    let source = Source::resolve(&q.path, q.format.as_deref())?;
    confine_members(&st.root, &source)?;
    let engine = st.read.clone();
    Ok(Json(blocking(move || engine.schema(&source)).await?))
}

async fn info(
    State(st): State<AppState>,
    Query(q): Query<SourceQuery>,
) -> Result<Json<InfoResponse>, ApiError> {
    confine_entry(&st.root, &q.path)?;
    let source = Source::resolve(&q.path, q.format.as_deref())?;
    confine_members(&st.root, &source)?;
    let size_bytes = std::fs::metadata(&source.path).map(|m| m.len()).ok();
    let format = source.format.to_string();
    let path = source.display();
    let engine_name = st.read.name().to_string();
    let engine = st.read.clone();
    let schema = blocking(move || engine.schema(&source)).await?;
    Ok(Json(InfoResponse {
        path,
        format,
        engine: engine_name,
        size_bytes,
        row_count: schema.row_count,
        columns: schema.columns.len(),
    }))
}

async fn preview(
    State(st): State<AppState>,
    Query(q): Query<PreviewQuery>,
) -> Result<Json<RowsResponse>, ApiError> {
    confine_entry(&st.root, &q.path)?;
    let source = Source::resolve(&q.path, q.format.as_deref())?;
    confine_members(&st.root, &source)?;
    let limit = q.limit.unwrap_or(50);
    let engine = st.read.clone();
    let resp = blocking(move || rows_response(engine.preview(&source, limit)?)).await?;
    Ok(Json(resp))
}

async fn profile(
    State(st): State<AppState>,
    Query(q): Query<ProfileQuery>,
) -> Result<Json<TableProfile>, ApiError> {
    confine_entry(&st.root, &q.path)?;
    let source = Source::resolve(&q.path, q.format.as_deref())?;
    confine_members(&st.root, &source)?;
    let scan = q.scan.unwrap_or(st.default_scan);
    let engine = st.read.clone();
    Ok(Json(blocking(move || engine.profile(&source, scan)).await?))
}

async fn query(
    State(st): State<AppState>,
    Json(body): Json<QueryBody>,
) -> Result<Json<RowsResponse>, ApiError> {
    // Confine every table a query would register to the server root (when set) — gated before the
    // source resolves *and* before requiring the SQL engine, so an out-of-root path is refused
    // regardless of the build.
    let mut named = Vec::new();
    if let Some(f) = &body.file {
        confine_entry(&st.root, f)?;
        let source = Source::detect(f)?;
        confine_members(&st.root, &source)?;
        named.push(NamedSource {
            name: "t".to_string(),
            source,
        });
    }
    for t in &body.tables {
        confine_entry(&st.root, &t.path)?;
        let source = Source::resolve(&t.path, t.format.as_deref())?;
        confine_members(&st.root, &source)?;
        named.push(NamedSource {
            name: t.name.clone(),
            source,
        });
    }
    if named.is_empty() {
        return Err(ApiError(EngineError::Other(
            "no tables — provide `file` or a non-empty `tables` array".to_string(),
        )));
    }

    let engine = st
        .sql
        .clone()
        .ok_or_else(|| ApiError(EngineError::missing_feature("run SQL", "sql")))?;
    let sql = body.sql.clone();
    let resp = blocking(move || rows_response(engine.query(&sql, &named)?)).await?;
    Ok(Json(resp))
}

/// `GET /v1/rows` — the grid's windowed scan: filter → sort → `offset`/`limit`.
async fn rows(
    State(st): State<AppState>,
    RawQuery(q): RawQuery,
) -> Result<Json<RowsWindow>, ApiError> {
    let params = parse_scan_params(&q.unwrap_or_default())?;
    confine_entry(&st.root, &params.path)?;
    let source = Source::resolve(&params.path, params.format.as_deref())?;
    confine_members(&st.root, &source)?;
    let spec = params.spec;
    let engine = scan_engine(&st, &spec);
    let res = blocking(move || engine.scan(&source, &spec)).await?;
    rows_window(res)
}

/// `GET /v1/stats` — column profile over the current *filtered* view (grid filters applied).
async fn stats(
    State(st): State<AppState>,
    RawQuery(q): RawQuery,
) -> Result<Json<TableProfile>, ApiError> {
    let params = parse_scan_params(&q.unwrap_or_default())?;
    confine_entry(&st.root, &params.path)?;
    let source = Source::resolve(&params.path, params.format.as_deref())?;
    confine_members(&st.root, &source)?;
    let filters = params.spec.filters;
    let scan = st.default_scan;
    let engine = st.read.clone();
    Ok(Json(
        blocking(move || engine.stats(&source, &filters, scan)).await?,
    ))
}

/// `GET /v1/export` — the current view (same sort/filter) as a downloadable CSV/JSON/Parquet.
async fn export(State(st): State<AppState>, RawQuery(q): RawQuery) -> Result<Response, ApiError> {
    let mut params = parse_scan_params(&q.unwrap_or_default())?;
    let fmt = params.fmt.clone().unwrap_or_else(|| "csv".to_string());
    // Export the whole current view, not just the visible window.
    params.spec.offset = 0;
    params.spec.limit = EXPORT_CAP;
    confine_entry(&st.root, &params.path)?;
    let source = Source::resolve(&params.path, params.format.as_deref())?;
    confine_members(&st.root, &source)?;
    let spec = params.spec;
    let engine = scan_engine(&st, &spec);

    let (body, mime, ext) = blocking(move || {
        let rb = engine.scan(&source, &spec)?.batch;
        let out: (Vec<u8>, &'static str, &'static str) = match fmt.as_str() {
            "parquet" => (
                crate::render::to_parquet(&rb)?,
                "application/vnd.apache.parquet",
                "parquet",
            ),
            "json" => (
                crate::render::rows(&rb, crate::render::Output::Json)?.into_bytes(),
                "application/json",
                "json",
            ),
            _ => (
                crate::render::rows(&rb, crate::render::Output::Csv)?.into_bytes(),
                "text/csv",
                "csv",
            ),
        };
        // Bound the response in bytes (not just rows): a wide view can exceed a sane budget
        // well under EXPORT_CAP rows. Reject past the cap rather than ship a multi-GB body.
        export_cap_check(out.0.len(), MAX_EXPORT_BYTES)?;
        Ok::<_, EngineError>(out)
    })
    .await?;

    let headers = [
        (header::CONTENT_TYPE, mime.to_string()),
        (
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"lakeleto-export.{ext}\""),
        ),
    ];
    Ok((headers, body).into_response())
}

/// `GET /v1/list?dir=` — the file browser: subdirs + readable data files.
async fn list(
    State(st): State<AppState>,
    RawQuery(q): RawQuery,
) -> Result<Json<DirListing>, ApiError> {
    let pairs = decode_pairs(&q.unwrap_or_default())?;
    let dir = pairs
        .iter()
        .find(|(k, _)| k == "dir")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| default_dir(&st.root));
    // Confine browsing to the root when set (also refuses object-store prefixes — `--root` is
    // local-filesystem only). Uses the same pre-access gate as the read handlers.
    confine_entry(&st.root, &dir)?;
    let listing = tokio::task::spawn_blocking(move || list_dir(&dir))
        .await
        .map_err(|e| ApiError(EngineError::Other(format!("worker task failed: {e}"))))?
        .map_err(ApiError)?;
    Ok(Json(listing))
}

// ---- workspaces (Postman-style data plane) --------------------------------------------

/// Most rows a `POST /v1/workspaces/:id/runs` will execute + cache for a single run.
const WORKSPACE_RUN_CAP: usize = 100_000;

/// Largest raw-Parquet result upload accepted by `PUT /v1/workspaces/:id/runs/:run_id/result`
/// (the sync path). Results are run-capped at [`WORKSPACE_RUN_CAP`] rows, so this is generous.
const MAX_RESULT_UPLOAD_BYTES: usize = 128 * 1024 * 1024;

#[derive(Deserialize)]
struct CreateWorkspace {
    name: String,
}

#[derive(Deserialize)]
struct RunRequest {
    /// SQL to run (source registered as table `t`); a raw scan of the source when absent/empty.
    #[serde(default)]
    sql: Option<String>,
    /// The source to read/query (path or object-store URI).
    path: String,
    #[serde(default)]
    format: Option<String>,
    /// Max rows to execute + cache (bounded by `WORKSPACE_RUN_CAP`).
    #[serde(default)]
    limit: Option<usize>,
    /// Rows to return inline in the response (the full result stays in the cache).
    #[serde(default)]
    preview: Option<usize>,
}

#[derive(Serialize)]
struct RunResponse {
    run: RunRecord,
    columns: Vec<ColumnSchema>,
    num_rows: usize,
    rows: Vec<serde_json::Value>,
}

async fn ws_list(State(st): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    let store = st.store.clone();
    let metas = blocking(move || store.list()).await?;
    Ok(Json(serde_json::json!({ "workspaces": metas })))
}

async fn ws_create(
    State(st): State<AppState>,
    Json(body): Json<CreateWorkspace>,
) -> Result<Json<Workspace>, ApiError> {
    let name = body.name;
    let store = st.store.clone();
    Ok(Json(blocking(move || store.create(&name)).await?))
}

async fn ws_get(
    State(st): State<AppState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<Workspace>, ApiError> {
    let store = st.store.clone();
    Ok(Json(blocking(move || store.get(&id)).await?))
}

async fn ws_save(
    State(st): State<AppState>,
    UrlPath(id): UrlPath<String>,
    Json(ws): Json<Workspace>,
) -> Result<Json<Workspace>, ApiError> {
    let store = st.store.clone();
    Ok(Json(blocking(move || store.save(&id, &ws)).await?))
}

async fn ws_delete(
    State(st): State<AppState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = st.store.clone();
    blocking(move || store.delete(&id)).await?;
    Ok(Json(serde_json::json!({ "deleted": true })))
}

async fn ws_history(
    State(st): State<AppState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = st.store.clone();
    let history = blocking(move || store.history(&id)).await?;
    Ok(Json(serde_json::json!({ "history": history })))
}

async fn ws_export(
    State(st): State<AppState>,
    UrlPath(id): UrlPath<String>,
) -> Result<Response, ApiError> {
    let store = st.store.clone();
    let bundle = blocking(move || store.export(&id)).await?;
    let fname = sanitize_filename(&bundle.workspace.name);
    let body = serde_json::to_vec_pretty(&bundle)
        .map_err(|e| ApiError(EngineError::Other(e.to_string())))?;
    let headers = [
        (header::CONTENT_TYPE, "application/json".to_string()),
        (
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{fname}.lakeleto-workspace.json\""),
        ),
    ];
    Ok((headers, body).into_response())
}

async fn ws_import(
    State(st): State<AppState>,
    Json(bundle): Json<WorkspaceBundle>,
) -> Result<Json<Workspace>, ApiError> {
    let store = st.store.clone();
    Ok(Json(blocking(move || store.import(&bundle)).await?))
}

/// `POST /v1/workspaces/:id/runs` — run a query/scan, record it in history, cache its result.
async fn ws_run(
    State(st): State<AppState>,
    UrlPath(id): UrlPath<String>,
    Json(req): Json<RunRequest>,
) -> Result<Json<RunResponse>, ApiError> {
    // Confine + resolve the source up front (same gate as the read handlers).
    confine_entry(&st.root, &req.path)?;
    let source = Source::resolve(&req.path, req.format.as_deref())?;
    confine_members(&st.root, &source)?;

    let sql = req.sql.filter(|s| !s.trim().is_empty());
    let cap = req.limit.unwrap_or(10_000).clamp(1, WORKSPACE_RUN_CAP);
    let preview_n = req.preview.unwrap_or(200).clamp(1, cap);
    let engine = if sql.is_some() {
        st.sql
            .clone()
            .ok_or_else(|| ApiError(EngineError::missing_feature("run SQL", "sql")))?
    } else {
        st.read.clone()
    };
    let store = st.store.clone();
    let source_path = source.display();
    let format = Some(source.format.to_string());

    let resp = blocking(move || {
        // 404 a bad workspace id BEFORE any engine work — a run against a workspace that
        // doesn't exist must not execute the query (nor could its record be stored).
        store.get(&id)?;
        let started = std::time::Instant::now();
        let result = match &sql {
            Some(q) => {
                let named = vec![NamedSource {
                    name: "t".to_string(),
                    source,
                }];
                // The cap is pushed into the engine (plan-level LIMIT on the SQL engine), so a
                // `SELECT *` over a huge table never materializes an unbounded result.
                engine.query_capped(q, &named, cap)
            }
            None => engine.preview(&source, cap),
        };
        let duration_ms = started.elapsed().as_millis() as u64;
        let mut rec = RunRecord {
            id: crate::workspace::new_run_id(),
            at_ms: crate::workspace::now_ms(),
            sql,
            source_path,
            format,
            status: RunStatus::Ok,
            error: None,
            row_count: None,
            duration_ms,
            cached: false,
        };
        match result {
            Ok(rb) => {
                rec.row_count = Some(rb.num_rows() as u64);
                rec.cached = true;
                store.append_run(&id, &rec, Some(&rb))?;
                let RowsResponse {
                    columns,
                    num_rows,
                    rows,
                } = rows_response(window_rowbatch(&rb, preview_n))?;
                Ok(RunResponse {
                    run: rec,
                    columns,
                    num_rows,
                    rows,
                })
            }
            Err(e) => {
                // Record the failure in history too, then surface the error to the client.
                rec.status = RunStatus::Error;
                rec.error = Some(e.to_string());
                store.append_run(&id, &rec, None)?;
                Err(e)
            }
        }
    })
    .await?;
    Ok(Json(resp))
}

/// `POST /v1/workspaces/:id/history` — sync: append a run record produced **elsewhere** (a
/// client that executed locally, or another Lakeleto instance). The record is stored verbatim;
/// its result bytes, if any, follow via `PUT …/runs/:run_id/result`.
async fn ws_history_append(
    State(st): State<AppState>,
    UrlPath(id): UrlPath<String>,
    Json(rec): Json<RunRecord>,
) -> Result<Json<RunRecord>, ApiError> {
    let store = st.store.clone();
    let echo = rec.clone();
    blocking(move || store.append_run(&id, &rec, None)).await?;
    Ok(Json(echo))
}

/// `PUT /v1/workspaces/:id/runs/:run_id/result` — sync upload: raw Parquet bytes of a run's
/// result (parse-validated before storage).
async fn ws_put_result(
    State(st): State<AppState>,
    UrlPath((id, run_id)): UrlPath<(String, String)>,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = st.store.clone();
    blocking(move || store.put_result_bytes(&id, &run_id, &body)).await?;
    Ok(Json(serde_json::json!({ "cached": true })))
}

/// `GET /v1/workspaces/:id/runs/:run_id/result` — sync download: the cached result's raw
/// Parquet bytes (the windowed-JSON view of the same result is the sibling route without
/// `/result`).
async fn ws_result_raw(
    State(st): State<AppState>,
    UrlPath((id, run_id)): UrlPath<(String, String)>,
) -> Result<Response, ApiError> {
    let store = st.store.clone();
    let bytes = blocking(move || store.run_result_bytes(&id, &run_id)).await?;
    Ok((
        [(header::CONTENT_TYPE, "application/vnd.apache.parquet")],
        bytes,
    )
        .into_response())
}

/// `GET /v1/workspaces/:id/runs/:run_id?offset=&limit=` — a window of a cached run result.
async fn ws_run_result(
    State(st): State<AppState>,
    UrlPath((id, run_id)): UrlPath<(String, String)>,
    RawQuery(q): RawQuery,
) -> Result<Json<RowsResponse>, ApiError> {
    let pairs = decode_pairs(&q.unwrap_or_default())?;
    let num = |k: &str, d: usize| {
        pairs
            .iter()
            .find(|(pk, _)| pk == k)
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(d)
    };
    let offset = num("offset", 0);
    let limit = num("limit", 200).clamp(1, 10_000);
    let store = st.store.clone();
    let rb = blocking(move || store.run_result(&id, &run_id, offset, limit)).await?;
    Ok(Json(rows_response(rb).map_err(ApiError)?))
}

// ---- helpers --------------------------------------------------------------------------

/// First `n` rows of a [`RowBatch`] (across batches), for capping a run and its inline preview.
fn window_rowbatch(rb: &RowBatch, n: usize) -> RowBatch {
    let mut out = Vec::new();
    let mut taken = 0usize;
    for b in &rb.batches {
        if taken >= n {
            break;
        }
        let take = (n - taken).min(b.num_rows());
        out.push(b.slice(0, take));
        taken += take;
    }
    RowBatch {
        schema: rb.schema.clone(),
        batches: out,
    }
}

/// Sanitize a workspace name into a safe download filename component.
fn sanitize_filename(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let s = s.trim_matches('-').to_string();
    if s.is_empty() {
        "workspace".to_string()
    } else {
        s
    }
}

fn decode_pairs(qs: &str) -> Result<Vec<(String, String)>, ApiError> {
    serde_urlencoded::from_str::<Vec<(String, String)>>(qs)
        .map_err(|e| ApiError(EngineError::Other(format!("bad query string: {e}"))))
}

struct ScanParams {
    path: String,
    format: Option<String>,
    spec: ScanSpec,
    fmt: Option<String>,
}

/// Parse the shared `path/format/offset/limit/sort/desc/filter/fmt` query params into a
/// [`ScanSpec`]. `filter` may repeat; each is `column:op:value` (op = eq/ne/lt/le/gt/ge/contains).
fn parse_scan_params(qs: &str) -> Result<ScanParams, ApiError> {
    let pairs = decode_pairs(qs)?;
    let mut path = None;
    let mut format = None;
    let mut offset = 0usize;
    let mut limit = 100usize;
    let mut sort = None;
    let mut desc = false;
    let mut fmt = None;
    let mut projection = None;
    let mut filters = Vec::new();

    for (k, v) in &pairs {
        match k.as_str() {
            "path" => path = Some(v.clone()),
            "format" if !v.is_empty() => format = Some(v.clone()),
            "offset" => offset = v.parse().unwrap_or(0),
            "limit" => limit = v.parse().unwrap_or(100),
            "sort" if !v.is_empty() => sort = Some(v.clone()),
            "desc" => desc = v == "1" || v.eq_ignore_ascii_case("true"),
            "fmt" if !v.is_empty() => fmt = Some(v.clone()),
            "cols" if !v.is_empty() => {
                projection = Some(
                    v.split(',')
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                )
            }
            "filter" => {
                let mut it = v.splitn(3, ':');
                if let (Some(c), Some(o), Some(val)) = (it.next(), it.next(), it.next()) {
                    if let Some(op) = FilterOp::parse(o) {
                        if !c.is_empty() {
                            filters.push(FilterSpec {
                                column: c.to_string(),
                                op,
                                value: val.to_string(),
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let path = path.ok_or_else(|| ApiError(EngineError::Other("missing `path`".to_string())))?;
    let sort = sort.map(|column| SortSpec {
        column,
        descending: desc,
    });
    Ok(ScanParams {
        path,
        format,
        spec: ScanSpec {
            offset,
            limit: limit.clamp(1, 10_000),
            sort,
            filters,
            projection,
        },
        fmt,
    })
}

/// Pick the engine for a scan: the DataFusion engine (external, unbounded sort/filter) when
/// it's compiled in and the request actually sorts or filters; otherwise the local reader
/// (fast plain-window reads with offset pushdown).
fn scan_engine(st: &AppState, spec: &ScanSpec) -> Arc<dyn Engine> {
    match &st.sql {
        Some(sql) if !spec.is_plain_window() => sql.clone(),
        _ => st.read.clone(),
    }
}

fn rows_window(res: ScanResult) -> Result<Json<RowsWindow>, ApiError> {
    let ScanResult {
        batch,
        matched_rows,
        total_known,
        scanned_rows,
        bounded,
        offset,
    } = res;
    let RowsResponse {
        columns,
        num_rows,
        rows,
    } = rows_response(batch).map_err(ApiError)?;
    Ok(Json(RowsWindow {
        columns,
        offset,
        num_rows,
        matched_rows,
        total_known,
        scanned_rows,
        bounded,
        rows,
    }))
}

/// Run a synchronous engine call off the async request threads.
async fn blocking<T, F>(f: F) -> Result<T, ApiError>
where
    F: FnOnce() -> Result<T, EngineError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ApiError(EngineError::Other(format!("worker task failed: {e}"))))?
        .map_err(ApiError)
}

fn rows_response(rb: RowBatch) -> Result<RowsResponse, EngineError> {
    let columns = rb
        .schema
        .fields()
        .iter()
        .map(|f| ColumnSchema {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();
    let num_rows = rb.num_rows();
    let rows = crate::render::row_values(&rb)?;
    Ok(RowsResponse {
        columns,
        num_rows,
        rows,
    })
}

/// Maps an [`EngineError`] to an HTTP status + `{ "error": ... }` body.
struct ApiError(EngineError);

impl From<EngineError> for ApiError {
    fn from(e: EngineError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            EngineError::UnsupportedFormat { .. } | EngineError::Other(_) => {
                StatusCode::BAD_REQUEST
            }
            EngineError::UnsupportedOperation { .. } => StatusCode::NOT_IMPLEMENTED,
            EngineError::Remote(_) => StatusCode::BAD_GATEWAY,
            EngineError::Forbidden(_) => StatusCode::FORBIDDEN,
            EngineError::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            EngineError::Io(e) if e.kind() == std::io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
            EngineError::Query(_) | EngineError::Arrow(_) | EngineError::Parquet(_) => {
                StatusCode::BAD_REQUEST
            }
            EngineError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_cap_rejects_over_limit() {
        assert!(export_cap_check(100, 100).is_ok());
        assert!(matches!(
            export_cap_check(101, 100),
            Err(EngineError::TooLarge(_))
        ));
    }

    #[test]
    fn error_status_mapping_covers_forbidden_and_too_large() {
        assert_eq!(
            ApiError(EngineError::Forbidden("x".into()))
                .into_response()
                .status(),
            StatusCode::FORBIDDEN,
        );
        assert_eq!(
            ApiError(EngineError::TooLarge("x".into()))
                .into_response()
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE,
        );
    }
}
