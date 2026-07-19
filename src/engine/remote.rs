//! The `remote` engine: the **Lakeleto Cloud** seam, behind the same [`Engine`] trait.
//!
//! This is the load-bearing answer to "local engine or hosted engine first?" — it is *not* a
//! separate product, it is one more `Engine`. Because the (future) UI holds a
//! `Box<dyn Engine>`, pointing Lakeleto at a hosted catalog is a config change, not a rewrite.
//!
//! Feature-gated (`--features remote`). In this MVP the metadata calls (`schema`,
//! `profile`) are wired over HTTP against a documented JSON contract, while row streaming
//! (`preview`/`query`) — which needs an Arrow-over-the-wire codec — is Phase 4. That split
//! is deliberate: it proves the trait binds a remote backend without pretending the hosted
//! service already exists.

use super::{Capabilities, Engine, NamedSource, RowBatch, TableProfile, TableSchema};
use crate::error::{EngineError, Result};
use crate::source::Source;

/// A client for a hosted Lakeleto Cloud endpoint.
pub struct RemoteEngine {
    endpoint: String,
    token: Option<String>,
    client: reqwest::blocking::Client,
}

impl RemoteEngine {
    pub fn new(endpoint: impl Into<String>, token: Option<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token,
            client: reqwest::blocking::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.endpoint.trim_end_matches('/'), path)
    }

    fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        source: &Source,
        extra: &[(&str, String)],
    ) -> Result<T> {
        let url = self.url(path);
        let mut req = self
            .client
            .get(&url)
            .query(&[
                ("path", source.display()),
                ("format", source.format.to_string()),
            ])
            .query(extra);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .map_err(|e| EngineError::Remote(format!("request to {url} failed: {e}")))?
            .error_for_status()
            .map_err(|e| EngineError::Remote(e.to_string()))?;
        resp.json::<T>()
            .map_err(|e| EngineError::Remote(format!("decoding response from {url}: {e}")))
    }
}

impl Engine for RemoteEngine {
    fn name(&self) -> &str {
        "remote"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            engine: format!("remote (Lakeleto Cloud @ {})", self.endpoint),
            formats: vec!["server-defined".to_string()],
            sql: true,
            profile: true,
            remote: true,
        }
    }

    fn schema(&self, source: &Source) -> Result<TableSchema> {
        self.get_json("v1/schema", source, &[])
    }

    fn preview(&self, _source: &Source, _limit: usize) -> Result<RowBatch> {
        Err(EngineError::Remote(
            "row streaming (preview) over the wire is Phase 4 — the RemoteEngine metadata \
             path is wired, but Arrow-over-HTTP result streaming is not implemented yet"
                .to_string(),
        ))
    }

    fn profile(&self, source: &Source, scan_limit: usize) -> Result<TableProfile> {
        // Forward the scan limit so `--fast` (scan=0, footer-stats) and `--scan N` reach the
        // server instead of silently falling back to the server's default scan.
        self.get_json("v1/profile", source, &[("scan", scan_limit.to_string())])
    }

    fn query(&self, _sql: &str, _tables: &[NamedSource]) -> Result<RowBatch> {
        Err(EngineError::Remote(
            "remote SQL result streaming is Phase 4"
                .to_string(),
        ))
    }
}
