//! Workspaces — Lakeleto's "Postman for lakehouse tables" data plane.
//!
//! A **workspace** groups the sources/tables a user works across (Connections), their saved
//! queries, their open tabs, and an append-only **history** of runs — each run's result is cached
//! as Parquet so a history entry re-opens instantly without re-executing. Everything is a plain
//! serde document plus a Parquet result cache under a local home dir, so it is human-readable,
//! portable (export == the JSON), and trivially syncable later.
//!
//! The [`WorkspaceStore`] trait is the load-bearing seam — exactly like [`Engine`](crate::engine::Engine):
//! [`LocalStore`] is the on-disk implementation shipped here (Apache-2.0), and a synced cloud store
//! (Lakeleto Cloud) later drops in behind the same trait and the same `/v1/workspaces/*`
//! HTTP contract, so "sync my workspace to the cloud" becomes a store swap, not a rewrite.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::engine::RowBatch;
use crate::error::{EngineError, Result};

/// The current export-bundle schema version (bumped on incompatible bundle changes).
pub const BUNDLE_VERSION: u32 = 1;

// ---- model ----------------------------------------------------------------------------

/// A workspace: a named grouping of connections, saved queries, and open tabs. History and cached
/// results are stored alongside it but are not part of this document (see [`WorkspaceStore`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub connections: Vec<Connection>,
    #[serde(default)]
    pub saved_queries: Vec<SavedQuery>,
    #[serde(default)]
    pub tabs: Vec<Tab>,
    /// Environment variables (`{{key}}`) the SPA substitutes into SQL and source paths before a
    /// request. Client-resolved (like Postman variables), so the backend contract is unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variables: Vec<Variable>,
}

/// A workspace variable — `{{key}}` in SQL/paths resolves to `value` client-side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Variable {
    pub key: String,
    pub value: String,
}

/// A saved source reference — the "different source/table/database" a user deals with. `path` is
/// whatever a [`Source`](crate::source::Source) resolves (a file/dir path or an object-store URI).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub id: String,
    pub label: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub pinned: bool,
}

/// A saved query — SQL bound (optionally) to a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    pub id: String,
    pub name: String,
    pub sql: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional folder/collection label; the SPA groups queries by it (Postman-style collections).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub pinned: bool,
}

/// serde `skip_serializing_if` helper: keep the JSON clean by omitting `false` bools.
fn is_false(b: &bool) -> bool {
    !*b
}

/// An open tab. `kind` is `"connection"` or `"query"`; `ref_id` points at the corresponding
/// [`Connection`] or [`SavedQuery`]. `view` is opaque grid state (sort/filter/columns) the SPA
/// owns — the backend persists it verbatim so the UI can round-trip without a backend change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tab {
    pub id: String,
    pub kind: String,
    pub ref_id: String,
    #[serde(default)]
    pub view: serde_json::Value,
}

/// Compact per-workspace summary for the list endpoint (no connections/queries payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceMeta {
    pub id: String,
    pub name: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub connection_count: usize,
    pub query_count: usize,
}

/// One entry in a workspace's run history. A failed run is recorded too (with `status = Error`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: String,
    pub at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    pub source_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    pub status: RunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    pub duration_ms: u64,
    /// Whether a result Parquet was cached for this run (re-openable via `run_result`).
    pub cached: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Ok,
    Error,
}

/// A portable workspace export: the definition + history metadata (Postman-collection style —
/// re-runnable, not the cached result bytes). `import` mints a fresh id, so bundles never collide.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceBundle {
    pub bundle_version: u32,
    pub workspace: Workspace,
    #[serde(default)]
    pub history: Vec<RunRecord>,
}

// ---- the store seam -------------------------------------------------------------------

/// Persistence + history for workspaces. `LocalStore` is the on-disk impl; a synced cloud store
/// implements the same trait behind the same `/v1/workspaces/*` contract (the hosted extension).
pub trait WorkspaceStore: Send + Sync {
    fn list(&self) -> Result<Vec<WorkspaceMeta>>;
    fn create(&self, name: &str) -> Result<Workspace>;
    fn get(&self, id: &str) -> Result<Workspace>;
    /// Update an existing workspace's definition (bumps `updated_at_ms`). 404 if it doesn't exist.
    fn save(&self, id: &str, ws: &Workspace) -> Result<Workspace>;
    fn delete(&self, id: &str) -> Result<()>;
    fn history(&self, id: &str) -> Result<Vec<RunRecord>>;
    /// Append a run to the history; when `result` is set, cache it as the run's Parquet result.
    fn append_run(&self, id: &str, rec: &RunRecord, result: Option<&RowBatch>) -> Result<()>;
    /// A window of a cached run result. 404 if the run has no cached result (expired/never cached).
    fn run_result(&self, id: &str, run_id: &str, offset: usize, limit: usize) -> Result<RowBatch>;
    /// Attach (or replace) a run's cached result from raw Parquet bytes — the **sync upload**
    /// path: a client that executed a run locally pushes the result it already has. The bytes
    /// are parse-validated as Parquet before they are stored.
    fn put_result_bytes(&self, id: &str, run_id: &str, parquet: &[u8]) -> Result<()>;
    /// The raw Parquet bytes of a cached run result — the **sync download** path (a windowed
    /// JSON view of the same result is [`run_result`](WorkspaceStore::run_result)). 404 if absent.
    fn run_result_bytes(&self, id: &str, run_id: &str) -> Result<Vec<u8>>;
    fn export(&self, id: &str) -> Result<WorkspaceBundle>;
    fn import(&self, bundle: &WorkspaceBundle) -> Result<Workspace>;
}

// ---- local (on-disk) store ------------------------------------------------------------

/// On-disk store rooted at a home dir:
/// `<home>/workspaces/<id>/{workspace.json, history.jsonl, results/<run-id>.parquet}`.
pub struct LocalStore {
    home: PathBuf,
    /// Per-workspace write locks: `save` is a read-modify-write on `workspace.json` and
    /// `append_run` appends to `history.jsonl`, and the store is shared as `Arc<dyn
    /// WorkspaceStore>` across concurrent requests (e.g. two autosaves, or an autosave racing a
    /// run). Serializing writers per id keeps last-writer-wins updates whole and history lines
    /// unfragmented.
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl LocalStore {
    /// Open the store at `$LAKELETO_HOME` (else `$HOME/.lakeleto`, `%USERPROFILE%\.lakeleto`, or a
    /// temp fallback), creating the `workspaces/` root.
    pub fn open() -> Result<Self> {
        Self::at(default_home())
    }

    /// Open a store rooted at an explicit dir (used by tests and `--workspace-home`).
    pub fn at(home: impl Into<PathBuf>) -> Result<Self> {
        let home = home.into();
        fs::create_dir_all(home.join("workspaces"))?;
        Ok(Self {
            home,
            locks: Mutex::new(HashMap::new()),
        })
    }

    /// The write lock for one workspace id (created on first use). Callers hold the returned
    /// guard for the whole read-modify-write / append. Lock poisoning is ignored — the state
    /// behind the lock is on-disk and every write is atomic (tmp + rename) or a single append.
    fn ws_lock(&self, id: &str) -> Arc<Mutex<()>> {
        let mut map = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(id.to_string()).or_default().clone()
    }

    fn workspaces_dir(&self) -> PathBuf {
        self.home.join("workspaces")
    }

    fn ws_dir(&self, id: &str) -> Result<PathBuf> {
        Ok(self.workspaces_dir().join(safe_id(id)?))
    }

    fn ws_json(&self, id: &str) -> Result<PathBuf> {
        Ok(self.ws_dir(id)?.join("workspace.json"))
    }

    fn load(&self, id: &str) -> Result<Workspace> {
        let path = self.ws_json(id)?;
        let bytes = fs::read(&path)?; // NotFound → 404 at the API layer
        serde_json::from_slice(&bytes)
            .map_err(|e| EngineError::Other(format!("workspace {id}: bad json: {e}")))
    }

    fn store_def(&self, ws: &Workspace) -> Result<()> {
        let dir = self.ws_dir(&ws.id)?;
        fs::create_dir_all(&dir)?;
        let bytes = serde_json::to_vec_pretty(ws).map_err(|e| EngineError::Other(e.to_string()))?;
        write_atomic(&dir.join("workspace.json"), &bytes)
    }
}

impl WorkspaceStore for LocalStore {
    fn list(&self) -> Result<Vec<WorkspaceMeta>> {
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(self.workspaces_dir()) else {
            return Ok(out);
        };
        for e in entries.flatten() {
            if !e.path().is_dir() {
                continue;
            }
            let id = e.file_name().to_string_lossy().to_string();
            if let Ok(ws) = self.load(&id) {
                out.push(WorkspaceMeta {
                    id: ws.id,
                    name: ws.name,
                    created_at_ms: ws.created_at_ms,
                    updated_at_ms: ws.updated_at_ms,
                    connection_count: ws.connections.len(),
                    query_count: ws.saved_queries.len(),
                });
            }
        }
        out.sort_by(|a, b| b.updated_at_ms.cmp(&a.updated_at_ms));
        Ok(out)
    }

    fn create(&self, name: &str) -> Result<Workspace> {
        let now = now_ms();
        let ws = Workspace {
            id: new_id("ws"),
            name: name.trim().to_string(),
            created_at_ms: now,
            updated_at_ms: now,
            connections: Vec::new(),
            saved_queries: Vec::new(),
            tabs: Vec::new(),
            variables: Vec::new(),
        };
        self.store_def(&ws)?;
        Ok(ws)
    }

    fn get(&self, id: &str) -> Result<Workspace> {
        self.load(id)
    }

    fn save(&self, id: &str, incoming: &Workspace) -> Result<Workspace> {
        let lock = self.ws_lock(id);
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let existing = self.load(id)?; // 404 if missing
        let ws = Workspace {
            id: existing.id, // the path id is authoritative; the body can't rename the dir
            name: incoming.name.trim().to_string(),
            created_at_ms: existing.created_at_ms,
            updated_at_ms: now_ms(),
            connections: incoming.connections.clone(),
            saved_queries: incoming.saved_queries.clone(),
            tabs: incoming.tabs.clone(),
            variables: incoming.variables.clone(),
        };
        self.store_def(&ws)?;
        Ok(ws)
    }

    fn delete(&self, id: &str) -> Result<()> {
        let lock = self.ws_lock(id);
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let dir = self.ws_dir(id)?;
        if dir.is_dir() {
            fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    fn history(&self, id: &str) -> Result<Vec<RunRecord>> {
        let path = self.ws_dir(id)?.join("history.jsonl");
        let Ok(text) = fs::read_to_string(&path) else {
            // No history file yet, but the workspace must exist.
            self.load(id)?;
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            if let Ok(rec) = serde_json::from_str::<RunRecord>(line) {
                out.push(rec);
            }
        }
        out.reverse(); // newest first
        Ok(out)
    }

    fn append_run(&self, id: &str, rec: &RunRecord, result: Option<&RowBatch>) -> Result<()> {
        let lock = self.ws_lock(id);
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let dir = self.ws_dir(id)?;
        if !dir.is_dir() {
            self.load(id)?; // 404 if the workspace doesn't exist
        }
        if let Some(rb) = result {
            let results = dir.join("results");
            fs::create_dir_all(&results)?;
            let bytes = crate::render::to_parquet(rb)?;
            write_atomic(
                &results.join(format!("{}.parquet", safe_id(&rec.id)?)),
                &bytes,
            )?;
        }
        let mut line = serde_json::to_string(rec).map_err(|e| EngineError::Other(e.to_string()))?;
        line.push('\n');
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("history.jsonl"))?;
        f.write_all(line.as_bytes())?;
        Ok(())
    }

    fn run_result(&self, id: &str, run_id: &str, offset: usize, limit: usize) -> Result<RowBatch> {
        let path = self
            .ws_dir(id)?
            .join("results")
            .join(format!("{}.parquet", safe_id(run_id)?));
        read_parquet_window(&path, offset, limit)
    }

    fn put_result_bytes(&self, id: &str, run_id: &str, parquet: &[u8]) -> Result<()> {
        // Parse-validate BEFORE storing: a bad upload must never poison the cache. (This also
        // rejects non-Parquet bytes outright — cheap for the bounded result sizes we cache.)
        parquet_window_from_bytes(parquet, 0, 1)
            .map_err(|e| EngineError::Other(format!("not a valid Parquet result: {e}")))?;
        let lock = self.ws_lock(id);
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        self.load(id)?; // 404 if the workspace doesn't exist
        let results = self.ws_dir(id)?.join("results");
        fs::create_dir_all(&results)?;
        write_atomic(
            &results.join(format!("{}.parquet", safe_id(run_id)?)),
            parquet,
        )
    }

    fn run_result_bytes(&self, id: &str, run_id: &str) -> Result<Vec<u8>> {
        let path = self
            .ws_dir(id)?
            .join("results")
            .join(format!("{}.parquet", safe_id(run_id)?));
        Ok(fs::read(&path)?) // NotFound → 404 at the API layer
    }

    fn export(&self, id: &str) -> Result<WorkspaceBundle> {
        Ok(WorkspaceBundle {
            bundle_version: BUNDLE_VERSION,
            workspace: self.load(id)?,
            history: self.history(id)?,
        })
    }

    fn import(&self, bundle: &WorkspaceBundle) -> Result<Workspace> {
        if bundle.bundle_version > BUNDLE_VERSION {
            return Err(EngineError::Other(format!(
                "workspace bundle version {} is newer than supported ({BUNDLE_VERSION}) — upgrade Lakeleto",
                bundle.bundle_version
            )));
        }
        let now = now_ms();
        // A fresh id (never trust the bundle's id as a path) and fresh timestamps.
        let ws = Workspace {
            id: new_id("ws"),
            name: bundle.workspace.name.trim().to_string(),
            created_at_ms: now,
            updated_at_ms: now,
            connections: bundle.workspace.connections.clone(),
            saved_queries: bundle.workspace.saved_queries.clone(),
            tabs: bundle.workspace.tabs.clone(),
            variables: bundle.workspace.variables.clone(),
        };
        self.store_def(&ws)?;
        // Re-attach history, but the cached result files did not travel in the bundle, so mark the
        // records uncached (they remain a re-runnable record, just not re-openable).
        if !bundle.history.is_empty() {
            let dir = self.ws_dir(&ws.id)?;
            let mut buf = String::new();
            for rec in bundle.history.iter().rev() {
                let mut r = rec.clone();
                r.cached = false;
                let line =
                    serde_json::to_string(&r).map_err(|e| EngineError::Other(e.to_string()))?;
                buf.push_str(&line);
                buf.push('\n');
            }
            write_atomic(&dir.join("history.jsonl"), buf.as_bytes())?;
        }
        Ok(ws)
    }
}

// ---- helpers --------------------------------------------------------------------------

/// Reject ids that aren't safe to use as a path component (defends the store against traversal via
/// caller-supplied workspace/run ids).
fn safe_id(id: &str) -> Result<&str> {
    let ok = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(id)
    } else {
        Err(EngineError::Other(format!("invalid id: {id:?}")))
    }
}

fn new_id(prefix: &str) -> String {
    static CTR: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let c = CTR.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{c:x}")
}

/// A fresh, unique run id (for a [`RunRecord`] before [`WorkspaceStore::append_run`]).
pub fn new_run_id() -> String {
    new_id("run")
}

/// Milliseconds since the Unix epoch — the timestamp unit used throughout the workspace model.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The default home dir: `$LAKELETO_HOME`, else `$HOME/.lakeleto` (unix) / `%USERPROFILE%\.lakeleto`
/// (windows), else a temp fallback so the store always has somewhere to live.
fn default_home() -> PathBuf {
    if let Some(h) = std::env::var_os("LAKELETO_HOME") {
        return PathBuf::from(h);
    }
    let base = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join(".lakeleto")
}

/// Atomic write via a unique temp file + rename, so a crash mid-write can't corrupt the target.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", new_id("t")));
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Read an `offset..offset+limit` window from a cached-result Parquet file into a [`RowBatch`].
fn read_parquet_window(path: &Path, offset: usize, limit: usize) -> Result<RowBatch> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = fs::File::open(path)?; // NotFound → 404
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(EngineError::parquet)?;
    collect_parquet_window(builder, offset, limit)
}

/// A row window over an in-memory Parquet file — the sync path's counterpart to
/// [`read_parquet_window`]: a `RemoteStore` downloads a cached result's raw bytes and windows
/// them locally, and `put_result_bytes` uses it to parse-validate an upload.
pub fn parquet_window_from_bytes(parquet: &[u8], offset: usize, limit: usize) -> Result<RowBatch> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let bytes = bytes::Bytes::copy_from_slice(parquet);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(EngineError::parquet)?;
    collect_parquet_window(builder, offset, limit)
}

fn collect_parquet_window<T: parquet::file::reader::ChunkReader + 'static>(
    mut builder: parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<T>,
    offset: usize,
    limit: usize,
) -> Result<RowBatch> {
    let schema = builder.schema().clone();
    let bs = limit.clamp(1, 8192);
    builder = builder.with_batch_size(bs);
    if offset > 0 {
        builder = builder.with_offset(offset);
    }
    builder = builder.with_limit(limit);
    let reader = builder.build().map_err(EngineError::parquet)?;
    let mut batches = Vec::new();
    let mut rows = 0usize;
    for b in reader {
        let b = b.map_err(EngineError::arrow)?;
        rows += b.num_rows();
        batches.push(b);
        if rows >= limit {
            break;
        }
    }
    Ok(RowBatch { schema, batches })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn store() -> (tempfile::TempDir, LocalStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::at(dir.path()).unwrap();
        (dir, store)
    }

    fn sample_batch() -> RowBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as _],
        )
        .unwrap();
        RowBatch {
            schema,
            batches: vec![batch],
        }
    }

    #[test]
    fn create_get_save_delete_round_trip() {
        let (_d, s) = store();
        let mut ws = s.create("My Lakehouse").unwrap();
        assert_eq!(ws.name, "My Lakehouse");
        assert_eq!(s.get(&ws.id).unwrap().name, "My Lakehouse");

        ws.connections.push(Connection {
            id: "c1".into(),
            label: "events".into(),
            path: "/data/events.parquet".into(),
            format: None,
            description: Some("event stream".into()),
            pinned: true,
        });
        ws.saved_queries.push(SavedQuery {
            id: "q1".into(),
            name: "recent".into(),
            sql: "SELECT * FROM t WHERE city = '{{city}}'".into(),
            connection_id: Some("c1".into()),
            description: None,
            folder: Some("dashboards".into()),
            pinned: false,
        });
        ws.variables.push(Variable {
            key: "city".into(),
            value: "London".into(),
        });
        let saved = s.save(&ws.id, &ws).unwrap();
        assert_eq!(saved.connections.len(), 1);
        assert!(saved.updated_at_ms >= saved.created_at_ms);
        // The new organizational fields round-trip through a reload.
        let reloaded = s.get(&ws.id).unwrap();
        assert_eq!(reloaded.connections[0].label, "events");
        assert!(reloaded.connections[0].pinned);
        assert_eq!(
            reloaded.connections[0].description.as_deref(),
            Some("event stream")
        );
        assert_eq!(
            reloaded.saved_queries[0].folder.as_deref(),
            Some("dashboards")
        );
        assert_eq!(reloaded.variables[0].key, "city");
        assert_eq!(reloaded.variables[0].value, "London");

        assert_eq!(s.list().unwrap().len(), 1);
        s.delete(&ws.id).unwrap();
        assert!(s.get(&ws.id).is_err());
        assert_eq!(s.list().unwrap().len(), 0);
    }

    #[test]
    fn history_and_cached_result_round_trip() {
        let (_d, s) = store();
        let ws = s.create("hist").unwrap();
        let run = RunRecord {
            id: new_id("run"),
            at_ms: now_ms(),
            sql: Some("SELECT * FROM t".into()),
            source_path: "/data/t.parquet".into(),
            format: None,
            status: RunStatus::Ok,
            error: None,
            row_count: Some(3),
            duration_ms: 5,
            cached: true,
        };
        s.append_run(&ws.id, &run, Some(&sample_batch())).unwrap();

        let hist = s.history(&ws.id).unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].id, run.id);
        assert_eq!(hist[0].row_count, Some(3));

        let rb = s.run_result(&ws.id, &run.id, 0, 10).unwrap();
        assert_eq!(rb.num_rows(), 3);
        // A missing run result is a NotFound (→ 404), not a panic.
        assert!(s.run_result(&ws.id, "run-nope", 0, 10).is_err());
    }

    #[test]
    fn export_import_mints_fresh_id() {
        let (_d, s) = store();
        let ws = s.create("exp").unwrap();
        s.append_run(
            &ws.id,
            &RunRecord {
                id: new_id("run"),
                at_ms: now_ms(),
                sql: None,
                source_path: "/data/t.parquet".into(),
                format: Some("parquet".into()),
                status: RunStatus::Ok,
                error: None,
                row_count: Some(3),
                duration_ms: 1,
                cached: true,
            },
            Some(&sample_batch()),
        )
        .unwrap();

        let bundle = s.export(&ws.id).unwrap();
        assert_eq!(bundle.bundle_version, BUNDLE_VERSION);
        assert_eq!(bundle.history.len(), 1);

        let imported = s.import(&bundle).unwrap();
        assert_ne!(imported.id, ws.id, "import mints a fresh id");
        assert_eq!(imported.name, "exp");
        // History travels; cached results do not (marked uncached, re-runnable).
        let ih = s.history(&imported.id).unwrap();
        assert_eq!(ih.len(), 1);
        assert!(!ih[0].cached);
    }

    #[test]
    fn ids_are_path_safe() {
        let (_d, s) = store();
        assert!(s.get("../../etc/passwd").is_err());
        assert!(s.run_result("ws", "../evil", 0, 1).is_err());
    }
}
