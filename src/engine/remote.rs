//! The `remote` engine: the over-HTTP seam, behind the same [`Engine`] trait.
//!
//! This is the load-bearing answer to "local engine or hosted engine first?" — it is *not* a
//! separate product, it is one more `Engine`. Because the (future) UI holds a
//! `Box<dyn Engine>`, pointing Lakeleto at another machine is a config change, not a rewrite.
//!
//! **What this speaks to today.** Any server that serves the [`crate::api`] `/v1/*` contract —
//! a self-hosted `lakeleto serve` on a workstation, a jump box, or a box that can see a data
//! lake this laptop cannot; and any hosted plane that chooses to speak part of the same
//! contract. A server may serve only *part* of it; this client discovers that the ordinary way,
//! as a `404` or `501` carrying the server's own message (see [`RemoteEngine::send`]). Note in
//! particular that `schema` and `profile` are separate endpoints from the row methods, so a
//! server can answer one and not the other. Nothing here is a claim about any hosted product.
//!
//! **The server resolves its own refs.** `path` is an **opaque string** to this client: it is
//! forwarded verbatim and interpreted by the peer, which is the only side that knows what its
//! own refs mean and the only side that can read the bytes. That is why
//! [`Source::unresolved`](crate::source::Source::unresolved) exists — a ref bound for a remote
//! engine must not be resolved against *this* filesystem — and why a source that carries no
//! locally-determined format is sent with **no `format=` parameter at all**, leaving the
//! inference to the server. A caller that does know the format still passes it, and it still
//! travels as the explicit override the contract says it is.
//!
//! **A server may impose limits this contract does not.** Row windows are already the server's
//! to bound (`limit` is clamped by its `max_query_rows`), and the same goes for anything else a
//! multi-tenant peer has to defend: a hosted plane may, for example, cap how many tables one
//! `POST /v1/query` may register, or refuse paths that are not its own governed refs. Those
//! arrive as an ordinary `4xx` carrying the server's own message; this client neither mirrors
//! nor pre-checks them, because it cannot know which peer it is talking to.
//!
//! Feature-gated (`--features remote`). Wire mapping (trait ⇄ HTTP), against the same
//! [`crate::api`] contract a local `lakeleto serve` speaks (`format=` is present only when the
//! client resolved one):
//!
//! | trait method | request | response |
//! |---|---|---|
//! | `schema` | `GET /v1/schema?path=&format=` | JSON [`TableSchema`] |
//! | `profile` | `GET /v1/profile?path=&format=&scan=` | JSON [`TableProfile`] |
//! | `preview` | `GET /v1/preview?path=&format=&limit=` | **Arrow IPC stream** |
//! | `query` / `query_capped` | `POST /v1/query` `{sql, tables[], limit?}` | **Arrow IPC stream** |
//!
//! Rows travel as an uncompressed Arrow IPC **stream** ([`crate::render::to_arrow_ipc`] /
//! [`crate::render::from_arrow_ipc`]), requested with
//! `Accept: application/vnd.apache.arrow.stream`. That codec is the reason the row methods are
//! implementable at all: the JSON body those endpoints return by default is a *rendering* — an
//! `Int64` past 2^53, a decimal, a timestamp and a nested list all come back as something a
//! client has to guess at, and a zero-row window loses its columns entirely. Arrow gives the
//! remote engine the same `RowBatch` the local reader produces, types intact, so everything
//! above the trait — the grid, `--output`, profiling — behaves identically whoever read the
//! bytes. JSON stays the default for browsers; Arrow is the engine-to-engine codec.

use std::io::Read;
use std::sync::{Arc, Mutex};

use super::{Capabilities, Engine, NamedSource, RowBatch, TableProfile, TableSchema};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::{is_database_uri, Format, Source};

/// The media type that asks a Lakeleto server for rows as an Arrow IPC stream.
const ARROW_STREAM_MIME: &str = "application/vnd.apache.arrow.stream";

/// Largest response body this engine will read back from a server, in bytes.
///
/// This buffers bytes it did not produce, sent by a machine it does not run. Without a cap the
/// peer decides how much of *this* client's memory to spend, and
/// [`RemoteEngine::send`] has to read the body before it can act on the status — the server's
/// `{"error": …}` message is *in* that body — so an error response would be buffered as
/// unconditionally as a result. `reqwest`'s blocking client carries a 30-second default timeout,
/// which bounds how *long* a hostile peer can take but says nothing about how large it gets.
///
/// The number. The windows this engine asks for are bounded by the server before they are
/// encoded — `/v1/preview` and `POST /v1/query` clamp to its `max_query_rows` (100,000 on a
/// `lakeleto serve`, advertised on `GET /v1/engines`) — and 100k rows of Arrow IPC is a few
/// megabytes for an ordinary table and tens of megabytes for a very wide one. 256 MiB leaves an
/// order of magnitude of headroom over any legitimate window and still fits in a laptop's spare
/// memory. It sits deliberately between the two caps already in the tree — the 128 MiB
/// `MAX_RESULT_UPLOAD_BYTES` and the 512 MiB `MAX_EXPORT_BYTES` — because what crosses this wire
/// is a result *window*, not a bulk export: a caller who wants the latter should be using
/// `GET /v1/export` and writing a file. A client pointed at a server with larger row limits can
/// raise it with [`RemoteEngine::with_max_response_bytes`].
///
/// **Not the only such client.** `workspace_remote::RemoteStore` — the other HTTP client in this
/// crate, behind the same `remote` feature and pointed at a server the user likewise does not
/// run — still buffers `get_bytes` responses with no cap of its own. That is a real gap, not a
/// distinction: it is simply outside this change, which is the engine seam. Cap it there too
/// before either client is aimed at anything untrusted.
const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

/// How long [`RemoteEngine::capabilities`] waits for `GET /v1/engines`. The answer is a few
/// hundred bytes the server has to hand, and the accessor can't fail, so a server slower than
/// this is reported as capabilities unknown rather than holding up the caller.
const CAPABILITIES_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The `?path=` / `?format=` pair every `/v1/*` read takes — with `format` **omitted** when the
/// client never resolved one.
///
/// A [`Source`] built by [`Source::unresolved`] deliberately carries no format: the caller named
/// a ref only the server can interpret, so the server infers the format the same way it does for
/// any of its own paths. Sending a placeholder instead would be worse than sending nothing — the
/// contract treats `format` as an explicit **override**, so a guess here becomes an instruction
/// there, and the peer would be told to misread its own bytes.
/// Refuse a source whose URI carries a connection credential, before any of it reaches the wire.
///
/// Every path below sends `source.uri()` verbatim, because the peer has to resolve the string — so
/// a database source would hand its password to another server as a query parameter or a JSON
/// field, and land it in that server's request log. This used to be stated as a caveat on
/// `wire_params` and left to caller discipline; a caveat is not a control, and the only way for a
/// reader to know it holds is for the code to enforce it.
///
/// Both tests matter. `Format::Database` catches a source the caller resolved explicitly, and
/// [`is_database_uri`] catches the case the format check alone would miss: a `postgres://…` ref
/// passed with no `--format` at all, which stays [`Source::unresolved`] and keeps `Format`'s
/// default.
fn refuse_database_source(source: &Source) -> Result<()> {
    if source.format == Format::Database || is_database_uri(&source.uri()) {
        return Err(EngineError::Forbidden(
            "the remote engine will not read a database source: its URI carries the connection \
             credential, and the peer resolves the path verbatim, so sending it would disclose \
             that credential to another server"
                .to_string(),
        ));
    }
    Ok(())
}

fn wire_params(source: &Source) -> Vec<(&'static str, String)> {
    // `uri`, not `display`: the peer has to resolve this string, so it travels verbatim. A
    // database URI must never get this far — `refuse_database_source` above rejects it at every
    // entry point, which is what makes sending `uri()` here safe.
    let mut params = vec![("path", source.uri())];
    if !source.is_unresolved() {
        params.push(("format", source.format.to_string()));
    }
    // An explicit records path and flattening are the caller's choices, like an explicit format:
    // they travel.
    if let Some(json_path) = &source.json_path {
        params.push(("json_path", json_path.clone()));
    }
    if let Some(flatten) = source.flatten {
        params.push(("flatten", flatten.as_param()));
    }
    params
}

/// What a server says about itself at `GET /v1/engines`: the part a client acts on.
#[derive(Debug, Clone, serde::Deserialize)]
#[non_exhaustive]
pub struct ServerInfo {
    /// The server's Lakeleto version.
    #[serde(default)]
    pub version: Option<String>,
    /// The `/v1` contract it speaks (see [`crate::protocol`]). `None` from a server that
    /// predates versioning, which speaks a subset of `1.0`.
    #[serde(default)]
    pub protocol: Option<String>,
    /// What its read engine does.
    pub engine: Capabilities,
    /// What each of its engines does. Empty from a server that predates the list.
    #[serde(default)]
    pub engines: Vec<Capabilities>,
    /// Whether it runs SQL over files.
    #[serde(default)]
    pub sql_available: bool,
}

/// A client for a server speaking the Lakeleto `/v1/*` HTTP contract (`lakeleto serve`).
pub struct RemoteEngine {
    endpoint: String,
    token: Option<String>,
    client: reqwest::blocking::Client,
    max_response_bytes: usize,
    /// What the server said at `GET /v1/engines`, once it has said it.
    server: Mutex<Option<Arc<ServerInfo>>>,
}

impl RemoteEngine {
    pub fn new(endpoint: impl Into<String>, token: Option<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token,
            client: reqwest::blocking::Client::new(),
            max_response_bytes: MAX_RESPONSE_BYTES,
            server: Mutex::new(None),
        }
    }

    /// What the server says about itself at `GET /v1/engines`: its version, the protocol it
    /// speaks, and what its engines do. Asked once and kept; asked again after a failure, since a
    /// server that was down may be up.
    pub fn server(&self, ctx: &RequestContext) -> Result<Arc<ServerInfo>> {
        if let Some(info) = self.lock_server().clone() {
            return Ok(info);
        }
        let url = self.url("v1/engines");
        let body = self.send(ctx, self.client.get(&url), &url)?;
        let info: ServerInfo = serde_json::from_slice(&body)
            .map_err(|e| EngineError::Remote(format!("decoding response from {url}: {e}")))?;
        let info = Arc::new(info);
        *self.lock_server() = Some(info.clone());
        Ok(info)
    }

    fn lock_server(&self) -> std::sync::MutexGuard<'_, Option<Arc<ServerInfo>>> {
        self.server.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The server this engine sends its requests to, as it was given.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Override the response byte cap ([`MAX_RESPONSE_BYTES`]).
    ///
    /// For a caller that knows its server's row limits are higher than a stock `lakeleto serve`'s
    /// — and for the tests, which would otherwise have to move a quarter of a gigabyte to prove
    /// the cap exists at all.
    pub fn with_max_response_bytes(mut self, bytes: usize) -> Self {
        self.max_response_bytes = bytes;
        self
    }

    /// NB: `path` is given **without** a leading slash (`"v1/preview"`), unlike
    /// [`crate::workspace_remote::RemoteStore`]'s equivalent. Passing one here yields `//v1/…`.
    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.endpoint.trim_end_matches('/'), path)
    }

    /// Attach the bearer token, if this client has one.
    fn auth(&self, req: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
        match &self.token {
            Some(token) => req.bearer_auth(token),
            None => req,
        }
    }

    /// Send, bound the body, and turn a non-2xx into an error carrying the **server's own**
    /// message. Every request this engine makes goes through here — there is no second path.
    ///
    /// Worth the few lines: every `/v1/*` failure answers `{"error": "…"}`, and `reqwest`'s
    /// `error_for_status` throws that body away. The case that matters most is a server built
    /// without the `sql` feature — `POST /v1/query` answers `501` with a body that says exactly
    /// which feature is missing and how to rebuild — which would otherwise reach the user as
    /// "HTTP status client error (501 Not Implemented)". An unauthorized request is the same
    /// story: the server says what is wrong with the token, and the status alone does not.
    ///
    /// The body is read under [`MAX_RESPONSE_BYTES`] in two steps, because a peer can withhold
    /// the information the first step needs: a declared `Content-Length` over the cap is refused
    /// before a single body byte is read, and the read itself is then bounded anyway so a
    /// chunked (or simply lying) response cannot go past it either.
    fn send(
        &self,
        ctx: &RequestContext,
        req: reqwest::blocking::RequestBuilder,
        url: &str,
    ) -> Result<Vec<u8>> {
        // Never open a connection for work whose answer is already unwanted.
        ctx.check()?;
        let cap = self.max_response_bytes;
        // A deadline becomes the request's own timeout, so it bounds the network wait rather
        // than only being noticed once the bytes are already back. `remaining()` is recomputed
        // per request, which is what makes an absolute deadline shared across several calls
        // behave as one budget instead of resetting for each.
        let req = match ctx.remaining() {
            Some(left) => req.timeout(left),
            None => req,
        };
        let mut resp = self.auth(req).send().map_err(|e| {
            // Prefer the context's explanation over the transport's: if the deadline is what
            // elapsed, "cancelled: the deadline for this request passed" is the true cause and
            // `Cancelled` is the variant a caller should branch on. A timeout with no deadline
            // set is the client's own configured timeout and stays a transport error.
            if e.is_timeout() && ctx.deadline().is_some() {
                return EngineError::Cancelled(crate::error::CancelReason::Deadline);
            }
            EngineError::Remote(format!("request to {url} failed: {e}"))
        })?;
        let status = resp.status();
        if let Some(declared) = resp.content_length() {
            if declared > cap as u64 {
                return Err(EngineError::TooLarge(format!(
                    "{url}: response declares {declared} bytes, over this client's {cap}-byte \
                     cap — ask for a smaller window, or raise the cap"
                )));
            }
        }
        // One byte past the cap is exactly enough to tell "at the limit" from "over it", and
        // stops the transfer there rather than draining whatever the peer wants to send.
        let mut body = Vec::new();
        (&mut resp)
            .take(cap as u64 + 1)
            .read_to_end(&mut body)
            .map_err(|e| EngineError::Remote(format!("reading response from {url}: {e}")))?;
        if body.len() > cap {
            return Err(EngineError::TooLarge(format!(
                "{url}: response exceeds this client's {cap}-byte cap — ask for a smaller \
                 window, or raise the cap"
            )));
        }
        if status.is_success() {
            return Ok(body);
        }
        let msg = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
            .unwrap_or_else(|| format!("HTTP {status}"));
        Err(EngineError::Remote(format!("{url}: {msg}")))
    }

    /// A `GET /v1/<path>?path=&format=` whose answer is a JSON document.
    ///
    /// Routed through [`Self::send`] like every other call, rather than through
    /// `error_for_status`: the doc above says why the server's `{"error": …}` body is the part
    /// worth keeping, and `schema`/`profile` have no more right to throw it away than the row
    /// methods do. It also means the byte cap covers metadata responses too.
    fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        ctx: &RequestContext,
        path: &str,
        source: &Source,
        extra: &[(&str, String)],
    ) -> Result<T> {
        refuse_database_source(source)?;
        let url = self.url(path);
        let req = self
            .client
            .get(&url)
            .query(&wire_params(source))
            .query(extra);
        let body = self.send(ctx, req, &url)?;
        serde_json::from_slice(&body)
            .map_err(|e| EngineError::Remote(format!("decoding response from {url}: {e}")))
    }

    /// The row twin of [`Self::get_json`]: same `?path=&format=` shape, but asks for — and
    /// decodes — an Arrow IPC stream instead of a JSON rendering.
    fn get_arrow(
        &self,
        ctx: &RequestContext,
        path: &str,
        source: &Source,
        extra: &[(&str, String)],
    ) -> Result<RowBatch> {
        refuse_database_source(source)?;
        let url = self.url(path);
        let req = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT, ARROW_STREAM_MIME)
            .query(&wire_params(source))
            .query(extra);
        let body = self.send(ctx, req, &url)?;
        crate::render::from_arrow_ipc(&body)
            .map_err(|e| EngineError::Remote(format!("decoding Arrow stream from {url}: {e}")))
    }

    /// `POST /v1/query` — [`Self::get_json`] is GET-only, and the SQL endpoint takes a body.
    ///
    /// [`NamedSource`] is not `Serialize` (it holds a resolved [`Source`]), so the body is built
    /// by hand from the fields the endpoint reads: the registered name, and the same path, format
    /// and read options [`wire_params`] sends for a single source.
    fn post_query(
        &self,
        ctx: &RequestContext,
        sql: &str,
        tables: &[NamedSource],
        limit: Option<usize>,
    ) -> Result<RowBatch> {
        for named in tables {
            refuse_database_source(&named.source)?;
        }
        let tables: Vec<serde_json::Value> = tables
            .iter()
            .map(|n| {
                // The same keys `wire_params` sends, so a table here and a source there cannot
                // disagree about how it is read — an unresolved source still carries no `format`.
                let mut spec = serde_json::json!({ "name": n.name });
                for (key, value) in wire_params(&n.source) {
                    spec[key] = serde_json::json!(value);
                }
                spec
            })
            .collect();
        let mut body = serde_json::json!({ "sql": sql, "tables": tables });
        if let Some(limit) = limit {
            body["limit"] = serde_json::json!(limit);
        }
        let url = self.url("v1/query");
        let req = self
            .client
            .post(&url)
            .header(reqwest::header::ACCEPT, ARROW_STREAM_MIME)
            .json(&body);
        let bytes = self.send(ctx, req, &url)?;
        crate::render::from_arrow_ipc(&bytes)
            .map_err(|e| EngineError::Remote(format!("decoding Arrow stream from {url}: {e}")))
    }
}

impl Engine for RemoteEngine {
    fn name(&self) -> &str {
        "remote"
    }

    /// What the server's engines do, as `GET /v1/engines` says, less what this client can't ask
    /// for yet.
    ///
    /// The formats are the server's read engine's, and `sql` is whether the server runs SQL.
    /// `scan` and `filtered_stats` stay false whatever the server does, because this client
    /// doesn't implement either: a grid window needs `scan`, and `stats` would drop its filters.
    ///
    /// The server is asked once, through [`Self::server`], and the answer is kept. One that
    /// doesn't answer within [`CAPABILITIES_TIMEOUT`], because it is down or serves only part of
    /// `/v1` as a hosted plane does, is reported as capabilities unknown: no formats, no SQL.
    /// Its methods still send their requests, and the server answers each in its own words.
    fn capabilities(&self) -> Capabilities {
        // Not "Lakeleto Cloud": this points at whatever speaks the `/v1/*` contract, which today
        // means a `lakeleto serve`. Naming the endpoint says more than a brand would.
        let engine = format!("remote (HTTP @ {})", self.endpoint);
        let ctx = RequestContext::detached().with_timeout(CAPABILITIES_TIMEOUT);
        match self.server(&ctx) {
            Ok(server) => Capabilities {
                engine,
                formats: server.engine.formats.clone(),
                sql: server.sql_available,
                profile: server.engine.profile,
                remote: true,
                scan: false,
                filtered_stats: false,
            },
            Err(_) => Capabilities {
                engine: format!("{engine}, capabilities unknown"),
                formats: Vec::new(),
                sql: false,
                profile: false,
                remote: true,
                scan: false,
                filtered_stats: false,
            },
        }
    }

    fn schema(&self, ctx: &RequestContext, source: &Source) -> Result<TableSchema> {
        self.get_json(ctx, "v1/schema", source, &[])
    }

    fn preview(&self, ctx: &RequestContext, source: &Source, limit: usize) -> Result<RowBatch> {
        self.get_arrow(ctx, "v1/preview", source, &[("limit", limit.to_string())])
    }

    fn profile(
        &self,
        ctx: &RequestContext,
        source: &Source,
        scan_limit: usize,
    ) -> Result<TableProfile> {
        // Forward the scan limit so `--fast` (scan=0, footer-stats) and `--scan N` reach the
        // server instead of silently falling back to the server's default scan.
        self.get_json(
            ctx,
            "v1/profile",
            source,
            &[("scan", scan_limit.to_string())],
        )
    }

    fn query(&self, ctx: &RequestContext, sql: &str, tables: &[NamedSource]) -> Result<RowBatch> {
        // No `limit` in the body: the server applies its own default and hard ceiling
        // (advertised on `GET /v1/engines` as `limits.default_query_rows` / `max_query_rows`).
        // A remote result is bounded by the plane, not by this client — the wire contract offers
        // no "give me everything", and inventing a large number here would only pretend it does.
        self.post_query(ctx, sql, tables, None)
    }

    fn query_capped(
        &self,
        ctx: &RequestContext,
        sql: &str,
        tables: &[NamedSource],
        cap: usize,
    ) -> Result<RowBatch> {
        // Push the cap into the request rather than trimming a default-sized result locally, so
        // the server can plan with it (a plan-level LIMIT on its SQL engine) and never
        // materialize — or transfer — rows this caller is going to throw away.
        self.post_query(ctx, sql, tables, Some(cap))
    }

    // `scan` — the grid's filter → sort → window over `GET /v1/rows` — is deliberately NOT
    // implemented yet and falls through to the trait default (`UnsupportedOperation`, which the
    // API layer reports as a 501). This is the next step for this engine, and it is a separate
    // change on purpose: the server already speaks the Arrow arm of `/v1/rows`, so what is
    // missing is the *request* half — re-encoding a `ScanSpec`'s filters back into
    // `filter=col:op:value` strings needs an exact inverse of `FilterOp::parse`, and a codec
    // whose two halves can drift is a different, larger risk than the result encoding landed
    // here. Until then the grid is served by the local and SQL engines, as it is today.
}
