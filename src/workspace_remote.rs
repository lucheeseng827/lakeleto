//! `RemoteStore` — the [`WorkspaceStore`] over HTTP (`--features remote`).
//!
//! Point it at **any** server speaking the `/v1/workspaces/*` contract — another
//! `lakeleto serve`, or the hosted Lakeleto Cloud plane — and the workbench
//! persists there instead of the local `$LAKELETO_HOME`: saved connections, queries, tabs,
//! history, and cached results become shared/synced state. This is the second load-bearing
//! seam doing exactly what the first ([`RemoteEngine`](crate::engine::remote)) does for reads:
//! **a store swap, not a rewrite** — `lakeleto serve --workspace-remote <url>` wires it in and
//! nothing above the trait changes.
//!
//! Wire mapping (trait ⇄ HTTP):
//! - `list/create/get/save/delete/history/export/import` — the CRUD verbs 1:1.
//! - `append_run(rec, result)` — `POST …/history` (the record), then, when a result batch is
//!   attached, `PUT …/runs/{run_id}/result` with the Snappy-Parquet bytes.
//! - `run_result(…)` / `run_result_bytes(…)` — `GET …/runs/{run_id}/result` (raw Parquet),
//!   windowed locally via [`parquet_window_from_bytes`].
//!
//! Remote error statuses map back onto the same [`EngineError`] classes the local store uses,
//! so the serving API re-derives the right HTTP status (a remote 404 stays a 404 here).

use std::io;

use serde::de::DeserializeOwned;

use crate::engine::RowBatch;
use crate::error::{EngineError, Result};
use crate::workspace::{
    parquet_window_from_bytes, RunRecord, Workspace, WorkspaceBundle, WorkspaceMeta, WorkspaceStore,
};

/// A client for a remote `/v1/workspaces/*` endpoint (another `lakeleto serve` or Lakeleto Cloud).
pub struct RemoteStore {
    endpoint: String,
    token: Option<String>,
    client: reqwest::blocking::Client,
}

impl RemoteStore {
    pub fn new(endpoint: impl Into<String>, token: Option<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token,
            client: reqwest::blocking::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint.trim_end_matches('/'))
    }

    /// Send with bearer auth; map non-2xx onto the same error classes the local store yields,
    /// so `ApiError` re-derives the right status when this store backs a `lakeleto serve`.
    fn send(&self, req: reqwest::blocking::RequestBuilder) -> Result<reqwest::blocking::Response> {
        let req = match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        };
        let resp = req
            .send()
            .map_err(|e| EngineError::Remote(format!("workspace sync: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let msg = resp
            .json::<serde_json::Value>()
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
            .unwrap_or_else(|| format!("HTTP {status}"));
        Err(match status.as_u16() {
            404 => EngineError::Io(io::Error::new(io::ErrorKind::NotFound, msg)),
            401..=403 => EngineError::Forbidden(format!("workspace sync: {msg}")),
            _ => EngineError::Remote(format!("workspace sync: {msg}")),
        })
    }

    fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.send(self.client.get(self.url(path)))?
            .json()
            .map_err(|e| EngineError::Remote(format!("workspace sync: bad response: {e}")))
    }

    fn send_json<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T> {
        self.send(self.client.request(method, self.url(path)).json(body))?
            .json()
            .map_err(|e| EngineError::Remote(format!("workspace sync: bad response: {e}")))
    }

    fn get_bytes(&self, path: &str) -> Result<Vec<u8>> {
        Ok(self
            .send(self.client.get(self.url(path)))?
            .bytes()
            .map_err(|e| EngineError::Remote(format!("workspace sync: bad response: {e}")))?
            .to_vec())
    }
}

#[derive(serde::Deserialize)]
struct WorkspacesEnvelope {
    workspaces: Vec<WorkspaceMeta>,
}

#[derive(serde::Deserialize)]
struct HistoryEnvelope {
    history: Vec<RunRecord>,
}

impl WorkspaceStore for RemoteStore {
    fn list(&self) -> Result<Vec<WorkspaceMeta>> {
        Ok(self
            .get_json::<WorkspacesEnvelope>("/v1/workspaces")?
            .workspaces)
    }

    fn create(&self, name: &str) -> Result<Workspace> {
        self.send_json(
            reqwest::Method::POST,
            "/v1/workspaces",
            &serde_json::json!({ "name": name }),
        )
    }

    fn get(&self, id: &str) -> Result<Workspace> {
        self.get_json(&format!("/v1/workspaces/{id}"))
    }

    fn save(&self, id: &str, ws: &Workspace) -> Result<Workspace> {
        self.send_json(reqwest::Method::PUT, &format!("/v1/workspaces/{id}"), ws)
    }

    fn delete(&self, id: &str) -> Result<()> {
        self.send(
            self.client
                .delete(self.url(&format!("/v1/workspaces/{id}"))),
        )?;
        Ok(())
    }

    fn history(&self, id: &str) -> Result<Vec<RunRecord>> {
        Ok(self
            .get_json::<HistoryEnvelope>(&format!("/v1/workspaces/{id}/history"))?
            .history)
    }

    fn append_run(&self, id: &str, rec: &RunRecord, result: Option<&RowBatch>) -> Result<()> {
        // Upload the result bytes FIRST, record second — the same ordering LocalStore uses
        // (cache file before history line). If the upload fails, no history entry ever claims
        // `cached: true`; the worst failure mode is an orphaned blob nothing points at.
        if let Some(rb) = result {
            let bytes = crate::render::to_parquet(rb)?;
            self.put_result_bytes(id, &rec.id, &bytes)?;
        }
        let _: RunRecord = self.send_json(
            reqwest::Method::POST,
            &format!("/v1/workspaces/{id}/history"),
            rec,
        )?;
        Ok(())
    }

    fn run_result(&self, id: &str, run_id: &str, offset: usize, limit: usize) -> Result<RowBatch> {
        let bytes = self.run_result_bytes(id, run_id)?;
        parquet_window_from_bytes(&bytes, offset, limit)
    }

    fn put_result_bytes(&self, id: &str, run_id: &str, parquet: &[u8]) -> Result<()> {
        self.send(
            self.client
                .put(self.url(&format!("/v1/workspaces/{id}/runs/{run_id}/result")))
                .header("content-type", "application/vnd.apache.parquet")
                .body(parquet.to_vec()),
        )?;
        Ok(())
    }

    fn run_result_bytes(&self, id: &str, run_id: &str) -> Result<Vec<u8>> {
        self.get_bytes(&format!("/v1/workspaces/{id}/runs/{run_id}/result"))
    }

    fn export(&self, id: &str) -> Result<WorkspaceBundle> {
        self.get_json(&format!("/v1/workspaces/{id}/export"))
    }

    fn import(&self, bundle: &WorkspaceBundle) -> Result<Workspace> {
        self.send_json(reqwest::Method::POST, "/v1/workspaces/import", bundle)
    }
}
