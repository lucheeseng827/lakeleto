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

use super::{Capabilities, Engine, NamedSource, RowBatch, TableProfile, TableSchema};
use crate::error::{EngineError, Result};
use crate::source::Source;

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

/// The `?path=` / `?format=` pair every `/v1/*` read takes — with `format` **omitted** when the
/// client never resolved one.
///
/// A [`Source`] built by [`Source::unresolved`] deliberately carries no format: the caller named
/// a ref only the server can interpret, so the server infers the format the same way it does for
/// any of its own paths. Sending a placeholder instead would be worse than sending nothing — the
/// contract treats `format` as an explicit **override**, so a guess here becomes an instruction
/// there, and the peer would be told to misread its own bytes.
fn wire_params(source: &Source) -> Vec<(&'static str, String)> {
    let mut params = vec![("path", source.display())];
    if !source.is_unresolved() {
        params.push(("format", source.format.to_string()));
    }
    params
}

/// A client for a server speaking the Lakeleto `/v1/*` HTTP contract (`lakeleto serve`).
pub struct RemoteEngine {
    endpoint: String,
    token: Option<String>,
    client: reqwest::blocking::Client,
    max_response_bytes: usize,
}

impl RemoteEngine {
    pub fn new(endpoint: impl Into<String>, token: Option<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token,
            client: reqwest::blocking::Client::new(),
            max_response_bytes: MAX_RESPONSE_BYTES,
        }
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
    fn send(&self, req: reqwest::blocking::RequestBuilder, url: &str) -> Result<Vec<u8>> {
        let cap = self.max_response_bytes;
        let mut resp = self
            .auth(req)
            .send()
            .map_err(|e| EngineError::Remote(format!("request to {url} failed: {e}")))?;
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
        path: &str,
        source: &Source,
        extra: &[(&str, String)],
    ) -> Result<T> {
        let url = self.url(path);
        let req = self
            .client
            .get(&url)
            .query(&wire_params(source))
            .query(extra);
        let body = self.send(req, &url)?;
        serde_json::from_slice(&body)
            .map_err(|e| EngineError::Remote(format!("decoding response from {url}: {e}")))
    }

    /// The row twin of [`Self::get_json`]: same `?path=&format=` shape, but asks for — and
    /// decodes — an Arrow IPC stream instead of a JSON rendering.
    fn get_arrow(&self, path: &str, source: &Source, extra: &[(&str, String)]) -> Result<RowBatch> {
        let url = self.url(path);
        let req = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT, ARROW_STREAM_MIME)
            .query(&wire_params(source))
            .query(extra);
        let body = self.send(req, &url)?;
        crate::render::from_arrow_ipc(&body)
            .map_err(|e| EngineError::Remote(format!("decoding Arrow stream from {url}: {e}")))
    }

    /// `POST /v1/query` — [`Self::get_json`] is GET-only, and the SQL endpoint takes a body.
    ///
    /// [`NamedSource`] is not `Serialize` (it holds a resolved [`Source`]), so the body is built
    /// by hand from the three fields the endpoint actually reads: the registered name, the
    /// source's display path, and its format.
    fn post_query(
        &self,
        sql: &str,
        tables: &[NamedSource],
        limit: Option<usize>,
    ) -> Result<RowBatch> {
        let tables: Vec<serde_json::Value> = tables
            .iter()
            .map(|n| {
                // Same rule as `wire_params`: an unresolved source carries no `format` key at
                // all, rather than a made-up one the server would be told to honour.
                let mut spec = serde_json::json!({
                    "name": n.name,
                    "path": n.source.display(),
                });
                if !n.source.is_unresolved() {
                    spec["format"] = serde_json::json!(n.source.format.to_string());
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
        let bytes = self.send(req, &url)?;
        crate::render::from_arrow_ipc(&bytes)
            .map_err(|e| EngineError::Remote(format!("decoding Arrow stream from {url}: {e}")))
    }
}

impl Engine for RemoteEngine {
    fn name(&self) -> &str {
        "remote"
    }

    /// What this *client* can ask for — not a report on the server, which is not consulted here.
    ///
    /// `sql: true` is the honest reading of that: `query`/`query_capped` are implemented and
    /// will issue `POST /v1/query`. Whether the peer can answer is the peer's answer to give,
    /// and it gives a good one — a server built without the `sql` feature replies `501` with a
    /// body naming the missing feature, which [`Self::send`] surfaces verbatim. The server does
    /// publish the truth as `sql_available` on `GET /v1/engines`, but reading it would put a
    /// network round trip (and a failure mode) inside an infallible accessor, and the answer
    /// would still be a guess by the time a query was actually issued. `formats` says
    /// `server-defined` for the same reason.
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            // Not "Lakeleto Cloud": this points at whatever speaks the `/v1/*` contract, which
            // today means a `lakeleto serve`. Naming the endpoint says more than a brand would.
            engine: format!("remote (HTTP @ {})", self.endpoint),
            formats: vec!["server-defined".to_string()],
            sql: true,
            profile: true,
            remote: true,
        }
    }

    fn schema(&self, source: &Source) -> Result<TableSchema> {
        self.get_json("v1/schema", source, &[])
    }

    fn preview(&self, source: &Source, limit: usize) -> Result<RowBatch> {
        self.get_arrow("v1/preview", source, &[("limit", limit.to_string())])
    }

    fn profile(&self, source: &Source, scan_limit: usize) -> Result<TableProfile> {
        // Forward the scan limit so `--fast` (scan=0, footer-stats) and `--scan N` reach the
        // server instead of silently falling back to the server's default scan.
        self.get_json("v1/profile", source, &[("scan", scan_limit.to_string())])
    }

    fn query(&self, sql: &str, tables: &[NamedSource]) -> Result<RowBatch> {
        // No `limit` in the body: the server applies its own default and hard ceiling
        // (advertised on `GET /v1/engines` as `limits.default_query_rows` / `max_query_rows`).
        // A remote result is bounded by the plane, not by this client — the wire contract offers
        // no "give me everything", and inventing a large number here would only pretend it does.
        self.post_query(sql, tables, None)
    }

    fn query_capped(&self, sql: &str, tables: &[NamedSource], cap: usize) -> Result<RowBatch> {
        // Push the cap into the request rather than trimming a default-sized result locally, so
        // the server can plan with it (a plan-level LIMIT on its SQL engine) and never
        // materialize — or transfer — rows this caller is going to throw away.
        self.post_query(sql, tables, Some(cap))
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
