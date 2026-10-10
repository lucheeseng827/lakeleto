//! [`RestCatalog`]: a read-only client for the Iceberg REST catalog protocol.
//!
//! It speaks the part of the protocol a reader needs: `GET /v1/config`, the namespace and table
//! listings, `loadTable`, and a table's vended credentials. It logs in with a bearer token or with
//! OAuth2 client credentials, keeps the token in memory, and fetches a new one before it expires.
//!
//! The protocol is asynchronous HTTP and [`Catalog`](super::Catalog) is synchronous, so each call
//! runs on the object-store runtime, as the object-store reads do. A credential provider that
//! refreshes a table's vended credentials calls the asynchronous half directly, from inside the
//! store's own request.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Method, StatusCode};
use serde_json::Value;
use url::Url;

use super::config::CatalogConfig;
use crate::context::RequestContext;
use crate::error::{CancelReason, EngineError, Result};

/// How long one request to a catalog may take when the call sets no shorter deadline.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The most entries one listing returns. A catalog with more is listed up to here.
pub(crate) const LIST_CAP: usize = 10_000;

/// The most pages one listing follows, so a catalog whose pages never end cannot hold a read.
const MAX_PAGES: usize = 1_000;

/// The largest response Lakeleto reads from a catalog. A table's metadata with many snapshots
/// runs to megabytes, so this is generous, and it is still a bound.
const MAX_RESPONSE: usize = 64 * 1024 * 1024;

/// The header that asks a catalog for credentials to read a table's files with.
const DELEGATION: &str = "x-iceberg-access-delegation";

/// The REST spec's namespace separator, when a catalog advertises none: the unit separator.
const UNIT_SEPARATOR: &str = "\u{1f}";

/// The endpoint that hands out a table's credentials again, which a catalog may advertise.
const CREDENTIALS_ENDPOINT: &str =
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

/// A client for one configured REST catalog.
pub(crate) struct RestCatalog {
    config: CatalogConfig,
    base: Url,
    http: reqwest::Client,
    login: Login,
    headers: HeaderMap,
    state: Mutex<State>,
}

/// How the client logs in.
enum Login {
    /// No login: the catalog is open.
    None,
    /// A static bearer token (`token`).
    Token(String),
    /// OAuth2 client credentials (`credential`), exchanged at `server` for a bearer token.
    Client {
        id: Option<String>,
        secret: String,
        server: Url,
        scope: String,
    },
}

#[derive(Default)]
struct State {
    server: Option<Arc<ServerConfig>>,
    token: Option<Token>,
}

/// A bearer token from the client-credentials flow, and when to fetch the next one.
#[derive(Clone)]
struct Token {
    bearer: String,
    refresh_after: Option<Instant>,
}

/// What `GET /v1/config` said, merged with this client's configuration as the spec orders it: the
/// server's defaults, then the client's settings, then the server's overrides.
pub(crate) struct ServerConfig {
    /// The route prefix, as path segments written as the server gave them.
    prefix: Vec<String>,
    /// The namespace separator, decoded.
    separator: String,
    /// The endpoints the server says it supports, when it says.
    endpoints: Option<HashSet<String>>,
    /// Every property, merged.
    pub(crate) props: BTreeMap<String, String>,
}

/// What `loadTable` returned.
pub(crate) struct LoadedTable {
    /// Where the table's current metadata file is.
    pub(crate) metadata_location: String,
    /// The table's current metadata, as the catalog handed it over.
    pub(crate) metadata: Value,
    /// Configuration for the table's files, which can carry credentials.
    pub(crate) config: BTreeMap<String, String>,
    /// Credentials for the table's files, each for the locations under its prefix.
    pub(crate) storage_credentials: Vec<StorageCredential>,
    /// Row filters and column masks the catalog requires a reader to apply, when it sets any.
    pub(crate) read_restrictions: Option<Value>,
}

/// Credentials a catalog vends for the locations under `prefix`.
#[derive(Clone)]
pub(crate) struct StorageCredential {
    pub(crate) prefix: String,
    pub(crate) config: BTreeMap<String, String>,
}

/// One listing: at most [`LIST_CAP`] names, and whether the catalog had more.
pub(crate) struct Names {
    pub(crate) names: Vec<String>,
    pub(crate) truncated: bool,
}

impl Names {
    /// `names` cut to [`LIST_CAP`]: a server that does not page can answer with any number at
    /// once. `more` says pages were left unread.
    fn capped(mut names: Vec<String>, more: bool) -> Names {
        let truncated = more || names.len() > LIST_CAP;
        names.truncate(LIST_CAP);
        Names { names, truncated }
    }
}

impl RestCatalog {
    /// A client for `config`, which has been validated. Nothing is sent until the first call.
    pub(crate) fn new(config: CatalogConfig) -> Result<RestCatalog> {
        let name = config.name().to_string();
        let invalid = |detail: String| EngineError::Other(format!("catalog `{name}`: {detail}"));
        let uri = config
            .uri()
            .ok_or_else(|| invalid("no `uri`".into()))?
            .trim_end_matches('/');
        let base = Url::parse(uri).map_err(|e| invalid(format!("`uri` `{uri}`: {e}")))?;
        let login = match (config.get("token"), config.get("credential")) {
            (Some(token), _) => Login::Token(token.to_string()),
            (None, Some(credential)) => {
                let (id, secret) = match credential.split_once(':') {
                    Some((id, secret)) => (Some(id.to_string()), secret.to_string()),
                    None => (None, credential.to_string()),
                };
                let server = match config.get("oauth2-server-uri") {
                    Some(server) => Url::parse(server)
                        .map_err(|e| invalid(format!("`oauth2-server-uri` `{server}`: {e}")))?,
                    None => {
                        eprintln!(
                            "lakeleto: catalog `{name}` has no `oauth2-server-uri`, so it logs in \
                             at the catalog's own /v1/oauth/tokens, which the Iceberg REST spec \
                             deprecates for removal. Set `oauth2-server-uri` to your identity \
                             provider's token endpoint."
                        );
                        join(&base, &["v1", "oauth", "tokens"])
                    }
                };
                Login::Client {
                    id,
                    secret,
                    server,
                    scope: config.get("scope").unwrap_or("catalog").to_string(),
                }
            }
            (None, None) => Login::None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(DELEGATION),
            HeaderValue::from_static("vended-credentials"),
        );
        for (key, value) in config.props() {
            let Some(header) = key.strip_prefix("header.") else {
                continue;
            };
            let header = HeaderName::from_bytes(header.as_bytes())
                .map_err(|e| invalid(format!("`{key}` is not a header name: {e}")))?;
            if value.is_empty() {
                // An empty value turns a header off, the delegation request included.
                headers.remove(&header);
                continue;
            }
            let mut value = HeaderValue::from_str(value)
                .map_err(|e| invalid(format!("`{key}` is not a header value: {e}")))?;
            value.set_sensitive(true);
            headers.insert(header, value);
        }
        let http = reqwest::Client::builder()
            .user_agent(concat!("lakeleto/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            // A login must not follow a redirect to wherever it points.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| invalid(format!("cannot build an HTTP client: {e}")))?;
        Ok(RestCatalog {
            config,
            base,
            http,
            login,
            headers,
            state: Mutex::new(State::default()),
        })
    }

    /// The catalog's configured name.
    pub(crate) fn name(&self) -> &str {
        self.config.name()
    }

    /// The catalog's configuration.
    pub(crate) fn config(&self) -> &CatalogConfig {
        &self.config
    }

    // ---- the calls, synchronously -----------------------------------------------------------

    /// What `GET /v1/config` says, fetched once.
    pub(crate) fn server(&self, ctx: &RequestContext) -> Result<Arc<ServerConfig>> {
        crate::objstore::block_on(self.server_config(Some(ctx)))
    }

    /// The namespaces directly under `parent` (top-level ones when it is empty), by name.
    pub(crate) fn namespaces(&self, ctx: &RequestContext, parent: &[String]) -> Result<Names> {
        crate::objstore::block_on(async {
            let server = self.server_config(Some(ctx)).await?;
            let mut names = Vec::new();
            let mut seen = HashSet::new();
            let truncated = self
                .paged(
                    ctx,
                    &server,
                    &["namespaces"],
                    parent,
                    "namespaces",
                    |page| {
                        let list = page.get("namespaces").and_then(Value::as_array);
                        for ns in list.into_iter().flatten() {
                            let levels: Vec<&str> = ns
                                .as_array()
                                .map(|l| l.iter().filter_map(Value::as_str).collect())
                                .unwrap_or_default();
                            // A namespace comes back as its full path. Take the level directly under
                            // `parent`: older servers list nested namespaces under a top-level request
                            // too, and a server that ignores `parent` answers with namespaces that are
                            // not under it at all, which are skipped.
                            let under_parent = levels.len() > parent.len()
                                && levels.iter().zip(parent).all(|(a, b)| a == b);
                            if !under_parent {
                                continue;
                            }
                            let child = levels[parent.len()];
                            if seen.insert(child.to_string()) {
                                names.push(child.to_string());
                            }
                        }
                    },
                )
                .await?;
            Ok(Names::capped(names, truncated))
        })
    }

    /// The tables in `namespace`, by name.
    pub(crate) fn tables(&self, ctx: &RequestContext, namespace: &[String]) -> Result<Names> {
        crate::objstore::block_on(async {
            let server = self.server_config(Some(ctx)).await?;
            let ns = server.namespace(namespace);
            let mut names = Vec::new();
            let truncated = self
                .paged(
                    ctx,
                    &server,
                    &["namespaces", &ns, "tables"],
                    &[],
                    "identifiers",
                    |page| {
                        let list = page.get("identifiers").and_then(Value::as_array);
                        for ident in list.into_iter().flatten() {
                            if let Some(name) = ident.get("name").and_then(Value::as_str) {
                                names.push(name.to_string());
                            }
                        }
                    },
                )
                .await?;
            Ok(Names::capped(names, truncated))
        })
    }

    /// `loadTable`: the table's current metadata, where it is, and how to read its files. `None`
    /// when the catalog has no such table in a namespace it has.
    pub(crate) fn load_table(
        &self,
        ctx: &RequestContext,
        namespace: &[String],
        table: &str,
    ) -> Result<Option<LoadedTable>> {
        crate::objstore::block_on(self.load_table_async(Some(ctx), namespace, table))
    }

    /// Does `namespace` exist?
    pub(crate) fn namespace_exists(
        &self,
        ctx: &RequestContext,
        namespace: &[String],
    ) -> Result<bool> {
        crate::objstore::block_on(async {
            let server = self.server_config(Some(ctx)).await?;
            let url = server.url(&self.base, &["namespaces", &server.namespace(namespace)]);
            let (status, _) = self.send(Some(ctx), Method::GET, url, false).await?;
            Ok(status.is_success())
        })
    }

    // ---- the calls, asynchronously ----------------------------------------------------------

    async fn server_config(&self, ctx: Option<&RequestContext>) -> Result<Arc<ServerConfig>> {
        if let Some(server) = self.lock().server.clone() {
            return Ok(server);
        }
        let mut url = join(&self.base, &["v1", "config"]);
        if let Some(warehouse) = self.config.get("warehouse") {
            url.query_pairs_mut().append_pair("warehouse", warehouse);
        }
        let body = self.get_json(ctx, url, false).await?;
        let server = Arc::new(ServerConfig::merge(&self.config, &body));
        self.lock().server = Some(server.clone());
        Ok(server)
    }

    pub(crate) async fn load_table_async(
        &self,
        ctx: Option<&RequestContext>,
        namespace: &[String],
        table: &str,
    ) -> Result<Option<LoadedTable>> {
        let server = self.server_config(ctx).await?;
        let url = server.url(
            &self.base,
            &["namespaces", &server.namespace(namespace), "tables", table],
        );
        let (status, body) = self.send(ctx, Method::GET, url.clone(), true).await?;
        if status == StatusCode::NOT_FOUND {
            // A missing namespace is an answer about the namespace. Anything else that is not
            // found is the table, whether or not the catalog named the exception.
            let (kind, _) = iceberg_error(&body);
            if kind.as_deref() != Some("NoSuchNamespaceException") {
                return Ok(None);
            }
        }
        if !status.is_success() {
            return Err(self.error(
                status,
                &body,
                &format!("loading {}", dotted(namespace, table)),
            ));
        }
        let body = parse_json(&body).map_err(|e| self.bad_response(&url, e))?;
        LoadedTable::parse(&body)
            .map(Some)
            .map_err(|e| self.bad_response(&url, e))
    }

    /// Fresh credentials for the table, for a store whose vended ones are about to expire: from
    /// the credentials endpoint when the catalog advertises one, else by loading the table again.
    pub(crate) async fn refresh_credentials(
        &self,
        namespace: &[String],
        table: &str,
    ) -> Result<(Vec<StorageCredential>, BTreeMap<String, String>)> {
        let server = self.server_config(None).await?;
        if server.supports(CREDENTIALS_ENDPOINT) {
            let url = server.url(
                &self.base,
                &[
                    "namespaces",
                    &server.namespace(namespace),
                    "tables",
                    table,
                    "credentials",
                ],
            );
            let body = self.get_json(None, url.clone(), true).await?;
            let creds = storage_credentials(&body).map_err(|e| self.bad_response(&url, e))?;
            return Ok((creds, BTreeMap::new()));
        }
        let loaded = self
            .load_table_async(None, namespace, table)
            .await?
            .ok_or_else(|| {
                EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "catalog `{}` no longer has table `{}`",
                        self.name(),
                        dotted(namespace, table)
                    ),
                ))
            })?;
        Ok((loaded.storage_credentials, loaded.config))
    }

    /// Follow a listing's pages, handing each to `each`, until there are no more, or [`LIST_CAP`]
    /// entries or [`MAX_PAGES`] pages have come back. `true` when it stopped with pages left.
    async fn paged(
        &self,
        ctx: &RequestContext,
        server: &ServerConfig,
        path: &[&str],
        parent: &[String],
        key: &str,
        mut each: impl FnMut(&Value),
    ) -> Result<bool> {
        let mut token = String::new();
        let mut seen = 0usize;
        for _ in 0..MAX_PAGES {
            let mut url = server.url(&self.base, path);
            {
                let mut query = url.query_pairs_mut();
                // An empty token asks a server that pages to page; one that does not ignores it.
                query.append_pair("pageToken", &token);
                if !parent.is_empty() {
                    query.append_pair("parent", &parent.join(&server.separator));
                }
            }
            let page = self.get_json(Some(ctx), url, false).await?;
            seen += page.get(key).and_then(Value::as_array).map_or(0, Vec::len);
            each(&page);
            match page.get("next-page-token").and_then(Value::as_str) {
                Some(next) if !next.is_empty() && next != token => {
                    if seen >= LIST_CAP {
                        return Ok(true);
                    }
                    token = next.to_string();
                }
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// `GET url` and parse its JSON body, or the catalog's error.
    async fn get_json(
        &self,
        ctx: Option<&RequestContext>,
        url: Url,
        delegate: bool,
    ) -> Result<Value> {
        let (status, body) = self.send(ctx, Method::GET, url.clone(), delegate).await?;
        if !status.is_success() {
            return Err(self.error(status, &body, &format!("GET {}", self.show(&url))));
        }
        parse_json(&body).map_err(|e| self.bad_response(&url, e))
    }

    /// Send a request, logged in, within the call's deadline. A token the catalog no longer takes
    /// is replaced once and the request sent again.
    async fn send(
        &self,
        ctx: Option<&RequestContext>,
        method: Method,
        url: Url,
        delegate: bool,
    ) -> Result<(StatusCode, Bytes)> {
        let mut renewed = false;
        loop {
            let bearer = self.bearer(ctx).await?;
            let mut headers = self.headers.clone();
            if !delegate {
                headers.remove(DELEGATION);
            }
            if let Some(bearer) = &bearer {
                let mut value = HeaderValue::from_str(&format!("Bearer {bearer}"))
                    .map_err(|_| self.refused("its token is not a valid header value"))?;
                value.set_sensitive(true);
                headers.insert(AUTHORIZATION, value);
            }
            let request = self
                .http
                .request(method.clone(), url.clone())
                .headers(headers);
            let (status, body) = self.exchange(ctx, request, &url).await?;
            // 419 is what older catalogs answer for an expired token.
            let expired = status == StatusCode::UNAUTHORIZED || status.as_u16() == 419;
            if expired && !renewed && matches!(self.login, Login::Client { .. }) {
                self.lock().token = None;
                renewed = true;
                continue;
            }
            return Ok((status, body));
        }
    }

    /// The bearer token to send, if the catalog takes one: the configured token, or one from the
    /// client-credentials flow, fetched again before it expires.
    async fn bearer(&self, ctx: Option<&RequestContext>) -> Result<Option<String>> {
        let (id, secret, server, scope) = match &self.login {
            Login::None => return Ok(None),
            Login::Token(token) => return Ok(Some(token.clone())),
            Login::Client {
                id,
                secret,
                server,
                scope,
            } => (id, secret, server, scope),
        };
        if let Some(token) = self.lock().token.clone() {
            if token.refresh_after.is_none_or(|at| Instant::now() < at) {
                return Ok(Some(token.bearer));
            }
        }
        // In a block of its own: the serializer is not `Sync`, so it must not live across an await.
        let form = {
            let mut form = url::form_urlencoded::Serializer::new(String::new());
            form.append_pair("grant_type", "client_credentials");
            if let Some(id) = id {
                form.append_pair("client_id", id);
            }
            form.append_pair("client_secret", secret);
            form.append_pair("scope", scope);
            form.finish()
        };
        let request = self
            .http
            .post(server.clone())
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(form);
        let (status, body) = self.exchange(ctx, request, server).await?;
        if !status.is_success() {
            let reason = oauth_error(&body).unwrap_or_else(|| status.to_string());
            let failed = format!(
                "logging in at {} with its client credentials failed: {status}: {reason}",
                self.show(server)
            );
            // A refusal is about the credentials; a server error is about the server, and says so.
            return Err(if status.is_client_error() {
                self.refused(&failed)
            } else {
                EngineError::Remote(format!("catalog `{}`: {failed}", self.name()))
            });
        }
        let body = parse_json(&body).map_err(|e| self.bad_response(server, e))?;
        let bearer = body
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                self.bad_response(server, "no `access_token` in the token response".into())
            })?
            .to_string();
        let refresh_after = body
            .get("expires_in")
            .and_then(Value::as_u64)
            .and_then(|secs| next_refresh(Instant::now(), secs));
        self.lock().token = Some(Token {
            bearer: bearer.clone(),
            refresh_after,
        });
        Ok(Some(bearer))
    }

    /// Send `request` within the call's deadline and read its body, up to [`MAX_RESPONSE`].
    async fn exchange(
        &self,
        ctx: Option<&RequestContext>,
        request: reqwest::RequestBuilder,
        url: &Url,
    ) -> Result<(StatusCode, Bytes)> {
        let deadline = ctx.and_then(RequestContext::remaining);
        if let Some(ctx) = ctx {
            ctx.check()?;
        }
        let budget = deadline.map_or(REQUEST_TIMEOUT, |left| left.min(REQUEST_TIMEOUT));
        let started = Instant::now();
        let timed_out = |e: reqwest::Error| -> EngineError {
            if deadline.is_some_and(|left| started.elapsed() >= left) {
                EngineError::Cancelled(CancelReason::Deadline)
            } else {
                self.unreachable(url, &e.without_url().to_string())
            }
        };
        let mut response = request.timeout(budget).send().await.map_err(|e| {
            if e.is_timeout() {
                timed_out(e)
            } else {
                self.unreachable(url, &error_chain(e))
            }
        })?;
        let status = response.status();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            if e.is_timeout() {
                timed_out(e)
            } else {
                self.unreachable(url, &error_chain(e))
            }
        })? {
            if body.len() + chunk.len() > MAX_RESPONSE {
                return Err(self.bad_response(
                    url,
                    format!(
                        "the response is larger than {} MiB",
                        MAX_RESPONSE / 1024 / 1024
                    ),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, Bytes::from(body)))
    }

    // ---- errors -----------------------------------------------------------------------------

    /// The error for a response that is not a success, as the catalog explained it.
    pub(crate) fn error(&self, status: StatusCode, body: &[u8], what: &str) -> EngineError {
        let (kind, message) = iceberg_error(body);
        let reason = match (&kind, &message) {
            (Some(kind), Some(message)) => format!("{kind}: {message}"),
            (None, Some(message)) => message.clone(),
            (Some(kind), None) => kind.clone(),
            (None, None) => String::from_utf8_lossy(&body[..body.len().min(200)]).into_owned(),
        };
        let name = self.name();
        match status {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                self.refused(&format!("{what}: {status}: {reason}"))
            }
            StatusCode::NOT_FOUND => EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("catalog `{name}`: {what}: {reason}"),
            )),
            _ => EngineError::Remote(format!("catalog `{name}`: {what}: {status}: {reason}")),
        }
    }

    fn refused(&self, detail: &str) -> EngineError {
        EngineError::Forbidden(format!(
            "catalog `{}` refused the request: {detail}",
            self.name()
        ))
    }

    fn unreachable(&self, url: &Url, detail: &str) -> EngineError {
        EngineError::Remote(format!(
            "catalog `{}`: cannot reach {}: {detail}",
            self.name(),
            self.show(url)
        ))
    }

    fn bad_response(&self, url: &Url, detail: String) -> EngineError {
        EngineError::Remote(format!(
            "catalog `{}`: {} answered with something that is not an Iceberg REST response: \
             {detail}",
            self.name(),
            self.show(url)
        ))
    }

    /// A URL as a message shows it: without its query, which may carry a warehouse name nobody
    /// asked to see repeated in every error.
    fn show(&self, url: &Url) -> String {
        let mut shown = url.clone();
        shown.set_query(None);
        shown.to_string()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ServerConfig {
    /// Merge `GET /v1/config`'s answer over `config`, as the spec orders it.
    fn merge(config: &CatalogConfig, body: &Value) -> ServerConfig {
        let map = |key: &str| -> BTreeMap<String, String> {
            body.get(key)
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut props = map("defaults");
        props.extend(config.props().clone());
        props.extend(map("overrides"));
        let prefix = props
            .get("prefix")
            .map(|p| {
                p.split('/')
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let separator = props
            .get("namespace-separator")
            .and_then(|s| {
                url::form_urlencoded::parse(format!("s={s}").as_bytes())
                    .next()
                    .map(|(_, v)| v.into_owned())
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| UNIT_SEPARATOR.to_string());
        let endpoints = body.get("endpoints").and_then(Value::as_array).map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        });
        ServerConfig {
            prefix,
            separator,
            endpoints,
            props,
        }
    }

    /// Does the server support `endpoint`? A server that lists none supports the spec's default
    /// set, which does not include the optional endpoints.
    fn supports(&self, endpoint: &str) -> bool {
        self.endpoints
            .as_ref()
            .is_some_and(|e| e.contains(endpoint))
    }

    /// `namespace` as one path segment: its levels joined by the separator.
    fn namespace(&self, namespace: &[String]) -> String {
        namespace.join(&self.separator)
    }

    /// The URL of `parts` under `/v1/{prefix}`. The prefix is written as the server gave it, and
    /// each part is percent-encoded whole.
    fn url(&self, base: &Url, parts: &[&str]) -> Url {
        let mut url = base.clone();
        let mut path = base.path().trim_end_matches('/').to_string();
        path.push_str("/v1");
        for segment in &self.prefix {
            path.push('/');
            path.push_str(segment);
        }
        for part in parts {
            path.push('/');
            path.push_str(&encode_segment(part));
        }
        url.set_path(&path);
        url
    }
}

impl LoadedTable {
    fn parse(body: &Value) -> std::result::Result<LoadedTable, String> {
        let metadata = body
            .get("metadata")
            .filter(|m| m.is_object())
            .ok_or("no `metadata` object")?
            .clone();
        let metadata_location = match body.get("metadata-location") {
            Some(Value::String(location)) => location.clone(),
            _ => return Err("no `metadata-location`: the table is staged, not committed".into()),
        };
        let config = string_map(body.get("config"));
        let storage_credentials = storage_credentials(body)?;
        Ok(LoadedTable {
            metadata_location,
            metadata,
            config,
            storage_credentials,
            read_restrictions: body.get("read-restrictions").cloned(),
        })
    }
}

/// The `storage-credentials` of a `loadTable` or `loadCredentials` response.
fn storage_credentials(body: &Value) -> std::result::Result<Vec<StorageCredential>, String> {
    let Some(list) = body.get("storage-credentials") else {
        return Ok(Vec::new());
    };
    let list = list
        .as_array()
        .ok_or("`storage-credentials` is not a list")?;
    list.iter()
        .map(|entry| {
            let prefix = entry
                .get("prefix")
                .and_then(Value::as_str)
                .ok_or("a storage credential has no `prefix`")?
                .to_string();
            Ok(StorageCredential {
                prefix,
                config: string_map(entry.get("config")),
            })
        })
        .collect()
}

/// When to fetch the token after one that lives `expires_in` seconds: once nine tenths of its life
/// has passed, so no request is sent with a token that expires on its way. The identity provider's
/// number is not trusted to fit: `None`, never, when that moment is further off than a clock can
/// say. A token the catalog stops taking is replaced anyway.
fn next_refresh(now: Instant, expires_in: u64) -> Option<Instant> {
    let life = Duration::from_secs(expires_in).checked_mul(9)? / 10;
    now.checked_add(life)
}

/// A JSON object of strings as a map; values that are not strings are skipped.
pub(crate) fn string_map(value: Option<&Value>) -> BTreeMap<String, String> {
    value
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// `base` with `parts` appended to its path.
fn join(base: &Url, parts: &[&str]) -> Url {
    let mut url = base.clone();
    let mut path = base.path().trim_end_matches('/').to_string();
    for part in parts {
        path.push('/');
        path.push_str(&encode_segment(part));
    }
    url.set_path(&path);
    url
}

/// Percent-encode everything in a path segment but the unreserved characters, so a `/`, the
/// namespace separator or a space in a name reaches the catalog as part of the name.
fn encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn parse_json(body: &[u8]) -> std::result::Result<Value, String> {
    serde_json::from_slice(body).map_err(|e| format!("not JSON: {e}"))
}

/// The `type` and `message` of an Iceberg error envelope: `{"error": {"message", "type", "code"}}`.
fn iceberg_error(body: &[u8]) -> (Option<String>, Option<String>) {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return (None, None);
    };
    let error = value.get("error");
    let field = |key: &str| {
        error
            .and_then(|e| e.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    (field("type"), field("message"))
}

/// An OAuth2 error response's code and description (RFC 6749 §5.2), or an Iceberg one's.
fn oauth_error(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    if let Some(code) = value.get("error").and_then(Value::as_str) {
        return Some(
            match value.get("error_description").and_then(Value::as_str) {
                Some(description) => format!("{code}: {description}"),
                None => code.to_string(),
            },
        );
    }
    let (kind, message) = iceberg_error(body);
    message.or(kind)
}

/// An HTTP error and its causes, which say what actually failed (a refused connection, a
/// certificate nobody trusts). Without the URL, which the message names already, less its query.
fn error_chain(e: reqwest::Error) -> String {
    let e = e.without_url();
    let mut out = e.to_string();
    let mut source = std::error::Error::source(&e);
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

fn dotted(namespace: &[String], table: &str) -> String {
    let mut parts: Vec<&str> = namespace.iter().map(String::as_str).collect();
    parts.push(table);
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(body: Value) -> ServerConfig {
        let config = CatalogConfig::new("prod")
            .unwrap()
            .with("uri", "https://c.example.com/api/catalog")
            .with("prefix", "from-client")
            .with("s3.region", "client-region");
        ServerConfig::merge(&config, &body)
    }

    /// The spec's merge order: the server's defaults, then the client's settings, then its
    /// overrides. The prefix and the separator come out of the merged result.
    #[test]
    fn config_merges_defaults_then_the_client_then_overrides() {
        let merged = server(serde_json::json!({
            "defaults": {"s3.region": "default-region", "s3.endpoint": "https://s3.example.com"},
            "overrides": {"prefix": "warehouse/abc"},
        }));
        assert_eq!(merged.props["s3.region"], "client-region");
        assert_eq!(merged.props["s3.endpoint"], "https://s3.example.com");
        assert_eq!(merged.prefix, ["warehouse", "abc"]);
        assert_eq!(merged.separator, UNIT_SEPARATOR);
        assert!(!merged.supports(CREDENTIALS_ENDPOINT));

        let dotted = server(serde_json::json!({
            "overrides": {"namespace-separator": "%2E"},
            "endpoints": [CREDENTIALS_ENDPOINT],
        }));
        assert_eq!(dotted.separator, ".");
        assert!(dotted.supports(CREDENTIALS_ENDPOINT));
    }

    /// Paths keep the base URI's own path, write the prefix as given, and encode each part whole:
    /// the separator and a `/` inside a name both stay inside their segment.
    #[test]
    fn urls_encode_each_part_whole_under_the_prefix() {
        let merged = server(serde_json::json!({"overrides": {"prefix": "cat%20one"}}));
        let base = Url::parse("https://c.example.com/api/catalog").unwrap();
        let url = merged.url(
            &base,
            &[
                "namespaces",
                &merged.namespace(&["sales".into(), "e/mea".into()]),
                "tables",
                "odd name",
            ],
        );
        assert_eq!(
            url.as_str(),
            "https://c.example.com/api/catalog/v1/cat%20one/namespaces/sales%1Fe%2Fmea/tables/odd%20name"
        );
        let bare = server(serde_json::json!({})).url(&base, &["namespaces"]);
        assert_eq!(
            bare.as_str(),
            "https://c.example.com/api/catalog/v1/from-client/namespaces"
        );
    }

    #[test]
    fn errors_read_both_envelopes() {
        let iceberg = br#"{"error": {"message": "Table does not exist: sales.orders", "type": "NoSuchTableException", "code": 404}}"#;
        assert_eq!(
            iceberg_error(iceberg),
            (
                Some("NoSuchTableException".to_string()),
                Some("Table does not exist: sales.orders".to_string())
            )
        );
        assert_eq!(
            oauth_error(br#"{"error": "invalid_client", "error_description": "bad secret"}"#),
            Some("invalid_client: bad secret".to_string())
        );
        assert_eq!(oauth_error(b"not json"), None);
    }

    /// A listing never returns more than the cap, whether the catalog paged to it or answered with
    /// everything at once, and says when it stopped short.
    #[test]
    fn a_listing_is_cut_to_the_cap_and_says_so() {
        let names = |n: usize| (0..n).map(|i| format!("t{i}")).collect::<Vec<_>>();
        let all_at_once = Names::capped(names(LIST_CAP + 1), false);
        assert_eq!(
            (all_at_once.names.len(), all_at_once.truncated),
            (LIST_CAP, true)
        );
        let pages_left = Names::capped(names(3), true);
        assert_eq!((pages_left.names.len(), pages_left.truncated), (3, true));
        let complete = Names::capped(names(LIST_CAP), false);
        assert_eq!(
            (complete.names.len(), complete.truncated),
            (LIST_CAP, false)
        );
    }

    /// A token is replaced at nine tenths of its life. A life no clock can hold, which a broken or
    /// hostile identity provider can send, means never rather than a panic.
    #[test]
    fn a_token_is_replaced_at_nine_tenths_of_its_life() {
        let now = Instant::now();
        assert_eq!(next_refresh(now, 100), Some(now + Duration::from_secs(90)));
        assert_eq!(next_refresh(now, 0), Some(now));
        assert_eq!(next_refresh(now, u64::MAX), None);
        // Nine tenths of this fits a duration; whether it fits the clock depends on the platform,
        // and either answer is fine as long as there is one.
        let _ = next_refresh(now, u64::MAX / 9);
    }

    /// A staged table has no metadata location, and is refused rather than read from metadata no
    /// file holds.
    #[test]
    fn a_load_table_result_needs_metadata_and_its_location() {
        let ok = LoadedTable::parse(&serde_json::json!({
            "metadata-location": "s3://b/t/metadata/1.metadata.json",
            "metadata": {"format-version": 2},
            "config": {"s3.region": "eu-west-1", "ignored": 1},
            "storage-credentials": [{"prefix": "s3://b/t", "config": {"s3.access-key-id": "AK"}}],
        }))
        .unwrap();
        assert_eq!(ok.config.len(), 1, "non-string values are skipped");
        assert_eq!(ok.storage_credentials[0].prefix, "s3://b/t");

        for (body, says) in [
            (
                serde_json::json!({"metadata-location": "x"}),
                "no `metadata` object",
            ),
            (
                serde_json::json!({"metadata-location": null, "metadata": {}}),
                "staged",
            ),
            (
                serde_json::json!({"metadata-location": "x", "metadata": {}, "storage-credentials": {}}),
                "not a list",
            ),
        ] {
            let err = LoadedTable::parse(&body).err().unwrap();
            assert!(err.contains(says), "{body}: {err}");
        }
    }

    /// Building a client sends nothing, and reads its login and headers from the configuration.
    #[test]
    fn a_client_reads_its_login_and_headers_from_the_configuration() {
        let base = || {
            CatalogConfig::new("prod")
                .unwrap()
                .with("uri", "https://c.example.com/")
        };
        let client = RestCatalog::new(base().with("credential", "id:s3cret")).unwrap();
        match &client.login {
            Login::Client {
                id,
                secret,
                server,
                scope,
            } => {
                assert_eq!(id.as_deref(), Some("id"));
                assert_eq!(secret, "s3cret");
                assert_eq!(server.as_str(), "https://c.example.com/v1/oauth/tokens");
                assert_eq!(scope, "catalog");
            }
            _ => panic!("client credentials expected"),
        }
        let secret_only = RestCatalog::new(base().with("credential", "s3cret")).unwrap();
        assert!(matches!(&secret_only.login, Login::Client { id: None, .. }));

        let headers = RestCatalog::new(
            base()
                .with("token", "t")
                .with("header.x-tenant", "acme")
                .with("header.x-iceberg-access-delegation", ""),
        )
        .unwrap();
        assert!(matches!(&headers.login, Login::Token(t) if t == "t"));
        assert_eq!(headers.headers.get("x-tenant").unwrap(), "acme");
        assert!(
            headers.headers.get(DELEGATION).is_none(),
            "an empty header value turns the delegation request off"
        );
        assert_eq!(
            RestCatalog::new(base())
                .unwrap()
                .headers
                .get(DELEGATION)
                .unwrap(),
            "vended-credentials"
        );
        assert!(RestCatalog::new(base().with("header.bad name", "x")).is_err());
    }
}
