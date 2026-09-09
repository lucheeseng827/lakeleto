//! Self-contained **Delta Lake** table reader (`--features delta`).
//!
//! Mirrors the Iceberg reader ([`crate::iceberg`]) in spirit: given a local table directory it
//! reconstructs the table's *active* data-file set and current schema from the transaction log,
//! then reads the Parquet data files with the existing arrow-58 Parquet reader. No `deltalake` /
//! `delta-rs` crate (which rides its own arrow/parquet line and would collide with the workspace's
//! arrow 58) — on-thesis for #25 ("the engine is a commodity; just read the Parquet the table
//! points to"). Read-only, local filesystem.
//!
//! ## What a Delta table looks like on disk
//!
//! The table root holds a `_delta_log/` directory containing an ordered sequence of commit files
//! `00000000000000000000.json`, `…001.json`, … (20-digit zero-padded version). **Each line** of a
//! commit file is one JSON *action*. The actions this reader consumes:
//!
//! - `{"metaData": {"schemaString": "<JSON schema>", "partitionColumns": [...]}}` — the table
//!   schema (a JSON-encoded Spark/Delta struct schema) plus the partition columns. The **latest**
//!   `metaData` in version order wins.
//! - `{"add": {"path": "part-….parquet", "partitionValues": {...}}}` — a data file added. `path`
//!   is relative to the table root and **URL-encoded** (`%XX` escapes are decoded).
//! - `{"remove": {"path": "…"}}` — a data file tombstoned.
//! - `{"protocol": {"minReaderVersion": N, "readerFeatures": [...]}}` — the reader-side
//!   requirements the table declares. Read to decide what this reader may honestly answer; see
//!   the refusals below.
//! - `{"txn"|"commitInfo": …}` — ignored.
//!
//! The **active file set** is every `add` path minus any later `remove` of that same path: commits
//! are replayed in ascending version order, and because a `remove` references the *identical*
//! (still URL-encoded) `path` string an `add` used, adds/removes are matched on that raw string
//! before it is decoded to a filesystem path.
//!
//! ## Partition columns live *outside* the Parquet
//!
//! Delta stores partition-column values in each `add` action's `partitionValues`, **not** inside
//! the data-file Parquet — a partitioned file's Parquet carries only the non-partition columns. So
//! the reader appends each partition column as a constant array (built from that file's
//! `partitionValues`, cast to the column's Arrow type; null when the value is null/absent) and
//! reorders every file's columns to the canonical schema order declared by `schemaString` (which
//! already lists partition columns in their logical positions).
//!
//! ## Two tables this reader refuses rather than answers wrongly
//!
//! Both are cases where the data files on disk do not mean what their contents say, so reading
//! them would produce a confidently wrong answer rather than a partial one. See
//! [`check_reader_requirements`] for the full reasoning and the exact line between refusing and
//! warning.
//!
//! - **Deletion vectors** (`add.deletionVector`, Delta's answer to merge-on-read): the deleted
//!   rows are still physically present in the Parquet and the vector that marks them is a Puffin
//!   blob this reader does not read — so they would come back as live rows, with [`row_count`]
//!   still reporting the footer sum as exact.
//! - **Column mapping** (`delta.columnMapping.mode` = `name`/`id`): physical Parquet column names
//!   are opaque ids, so every logical column would null-fill.
//!
//! Anything else a `protocol` action lists in `readerFeatures` is noted on stderr and read anyway.
//!
//! ## Checkpoint limitation
//!
//! Delta writers periodically emit a `_delta_log/_last_checkpoint` pointer to a
//! `…N.checkpoint.parquet` that snapshots the state at version `N`, letting readers skip replaying
//! commits `0..=N`. **This reader does not consult checkpoints** — it replays *all* `*.json`
//! commits from version 0. That is correct for any table whose json commits are still present
//! (the default for delta-rs / pyarrow writers that have not run `VACUUM`/log-retention cleanup,
//! which is the overwhelmingly common case). It would under-report only for a table whose early
//! json commits have been physically deleted while a checkpoint retained their state — that case
//! is intentionally out of scope for this JSON-only reader.

#![cfg(feature = "delta")]

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{new_null_array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{EngineError, Result};

/// Batch size for streaming a data file. The reader is self-contained (it does not see the
/// engine's configured batch size), so it uses the same default the local engine does.
const DEFAULT_BATCH_SIZE: usize = 8192;

/// One column of the table's current schema, in the order declared by `schemaString` (partition
/// columns included in their logical positions).
#[derive(Debug, Clone)]
pub struct DeltaField {
    pub name: String,
    /// The Arrow type the Spark/Delta type maps to (nested/unknown types → `Utf8`, best-effort).
    pub data_type: DataType,
    pub nullable: bool,
    /// `true` when this column is a partition column (its values come from each file's
    /// `partitionValues`, not from the file's Parquet).
    pub is_partition: bool,
}

/// The table's current schema: its columns (in `schemaString` order) plus the partition-column
/// names. Derived from the latest `metaData` action.
#[derive(Debug, Clone)]
pub struct DeltaSchema {
    pub fields: Vec<DeltaField>,
    pub partition_columns: Vec<String>,
}

/// An active data file of the table: its resolved filesystem path plus this file's partition
/// values (`column -> Some(value)`, or `None` for a null/absent partition value).
#[derive(Debug, Clone)]
pub struct AddFile {
    pub path: PathBuf,
    pub partition_values: HashMap<String, Option<String>>,
}

/// A read plan for the table's current state: the active (non-tombstoned) data files and the
/// current schema (including partition columns).
#[derive(Debug, Clone)]
pub struct TablePlan {
    pub files: Vec<AddFile>,
    pub schema: DeltaSchema,
}

/// Memoized [`plan`] results, keyed by table dir and a fingerprint of its `_delta_log/`.
///
/// Replaying the log means reading and JSON-parsing **every** commit file, and the cost grows with
/// the table's history rather than its size. `serve --root` makes that bite twice per request —
/// once to confine the plan ([`plan_with_root`]) and once for the engine's own read — which is
/// exactly the deployment shape most likely to be pointed at a table with a long history. The
/// memo collapses the pair back to one replay.
///
/// The fingerprint is `(highest version, commit count, mtime of the highest commit)`. A new commit
/// changes the version and the count; a commit rewritten in place changes its mtime. It does not
/// notice a rewrite that preserves both mtime and version, which is not something a Delta writer
/// does — commits are append-only by construction.
static PLAN_CACHE: std::sync::OnceLock<std::sync::Mutex<PlanCache>> = std::sync::OnceLock::new();

/// Canonical table dir -> the fingerprint the plan was built from, and the plan.
type PlanCache = HashMap<PathBuf, (LogFingerprint, Arc<TablePlan>)>;

/// Tables kept in [`PLAN_CACHE`] before it is cleared wholesale. A viewer looks at a handful of
/// tables at a time, and a plan is cheap to rebuild, so a flat cap and a full clear beat the
/// bookkeeping of a real eviction policy.
const PLAN_CACHE_MAX_TABLES: usize = 8;

/// See [`PLAN_CACHE`]. `(highest version, commit count, mtime nanos of the highest commit)`.
type LogFingerprint = (u64, usize, u128);

fn log_fingerprint(log_dir: &Path, versions: &[u64]) -> LogFingerprint {
    let highest = versions.last().copied().unwrap_or(0);
    let mtime = std::fs::metadata(log_dir.join(format!("{highest:020}.json")))
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    (highest, versions.len(), mtime)
}

/// Resolve the table's current read plan by replaying every `*.json` commit in `_delta_log/` in
/// ascending version order: apply `metaData` (latest wins), collect `add` paths, drop any later
/// `remove`d path. Errors if the directory is not a Delta table (no `_delta_log/`), has no
/// commits, or never declares a `metaData`.
///
/// Results are memoized per table — see [`PLAN_CACHE`].
pub fn plan(table_dir: &Path) -> Result<TablePlan> {
    Ok((*plan_cached(table_dir)?).clone())
}

/// Like [`plan`], but when `root` is `Some`, every file the reader will open — each `_delta_log/`
/// commit and every active data file the log names — is canonicalized and required to lie within
/// `root`, refusing anything that escapes with [`EngineError::Forbidden`].
///
/// This is what lets `serve --root` confine a Delta table. It is not hypothetical: `resolve` keeps
/// an absolute `add.path` as-is and joins a relative one, so a log naming `/etc/passwd.parquet` or
/// `../../elsewhere/x.parquet` reads outside the root unless it is checked here. `None` (the
/// engine's own call) skips the check.
///
/// Mirrors [`crate::iceberg::plan_with_root`], including its limitation: the caller validates and
/// discards, then the engine re-plans unconfined, so this proves the file set was in-root at check
/// time rather than handing the reader pre-canonicalized paths.
pub fn plan_with_root(table_dir: &Path, root: Option<&Path>) -> Result<TablePlan> {
    let plan = plan_cached(table_dir)?;
    let Some(root) = root else {
        return Ok((*plan).clone());
    };
    // The refusal message is path-free so a client-facing 403 does not disclose what was resolved.
    let refuse = || {
        EngineError::Forbidden("delta: table references a file outside the server root".to_string())
    };
    let guard = |p: &Path| -> Result<()> {
        match std::fs::canonicalize(p) {
            Ok(c) if c.starts_with(root) => Ok(()),
            _ => Err(refuse()),
        }
    };
    // The log itself first: a symlinked `_delta_log/` would otherwise let an in-root table dir
    // source its commits — and therefore its file list — from outside the root.
    let log_dir = table_dir.join("_delta_log");
    guard(&log_dir)?;
    for version in commit_versions(&log_dir)? {
        guard(&log_dir.join(format!("{version:020}.json")))?;
    }
    for file in &plan.files {
        guard(&file.path)?;
    }
    Ok((*plan).clone())
}

/// The memoized planner body. Returns a shared plan so a cache hit costs a clone of an `Arc`
/// rather than of the file list.
fn plan_cached(table_dir: &Path) -> Result<Arc<TablePlan>> {
    let log_dir = table_dir.join("_delta_log");
    let key = std::fs::canonicalize(table_dir).unwrap_or_else(|_| table_dir.to_path_buf());
    let fingerprint = commit_versions(&log_dir)
        .ok()
        .map(|v| log_fingerprint(&log_dir, &v));

    if let Some(fp) = fingerprint {
        let cache = PLAN_CACHE.get_or_init(Default::default);
        if let Ok(guard) = cache.lock() {
            if let Some((cached_fp, cached)) = guard.get(&key) {
                if *cached_fp == fp {
                    return Ok(Arc::clone(cached));
                }
            }
        }
    }

    let plan = Arc::new(plan_uncached(table_dir)?);

    if let Some(fp) = fingerprint {
        let cache = PLAN_CACHE.get_or_init(Default::default);
        if let Ok(mut guard) = cache.lock() {
            if guard.len() >= PLAN_CACHE_MAX_TABLES && !guard.contains_key(&key) {
                guard.clear();
            }
            guard.insert(key, (fp, Arc::clone(&plan)));
        }
    }
    Ok(plan)
}

fn plan_uncached(table_dir: &Path) -> Result<TablePlan> {
    let log_dir = table_dir.join("_delta_log");
    if !log_dir.is_dir() {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "delta: {} is not a Delta table (no _delta_log/ directory)",
                table_dir.display()
            ),
        });
    }
    let versions = commit_versions(&log_dir)?;
    if versions.is_empty() {
        return Err(EngineError::UnsupportedFormat {
            detail: format!("delta: {} has no *.json commits", log_dir.display()),
        });
    }

    let mut schema: Option<DeltaSchema> = None;
    // The latest `protocol` action wins, exactly like `metaData`. Read for the reader-side
    // requirements it declares — see `check_reader_requirements`.
    let mut protocol: Option<serde_json::Value> = None;
    // Column-mapping mode from the latest `metaData`'s configuration, kept beside the schema
    // because the refusal it drives needs the *table* config, not the parsed field list.
    let mut column_mapping = String::new();
    // Keyed by the RAW (still URL-encoded) `path` string from the log so an `add` and its later
    // `remove` — which carry the identical string — match exactly. Decoded only when the final
    // filesystem path is built. `BTreeMap` gives a deterministic file order.
    let mut active: BTreeMap<String, HashMap<String, Option<String>>> = BTreeMap::new();
    // Active data files carrying a `deletionVector`. Tracked by raw path so a later `remove` of
    // the same file drops it from this set too — a DV on a tombstoned file is not our problem.
    let mut with_deletion_vectors: BTreeMap<String, ()> = BTreeMap::new();

    for version in &versions {
        let commit = log_dir.join(format!("{version:020}.json"));
        let content = std::fs::read_to_string(&commit)?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let action: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                EngineError::Other(format!(
                    "delta: bad action json in {}: {e}",
                    commit.display()
                ))
            })?;
            if let Some(meta) = action.get("metaData") {
                schema = Some(parse_metadata(meta)?);
                column_mapping = meta
                    .get("configuration")
                    .and_then(|c| c.get("delta.columnMapping.mode"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("none")
                    .to_ascii_lowercase();
            } else if let Some(proto) = action.get("protocol") {
                protocol = Some(proto.clone());
            } else if let Some(add) = action.get("add") {
                if let Some(path) = add.get("path").and_then(|v| v.as_str()) {
                    active.insert(
                        path.to_string(),
                        parse_partition_values(add.get("partitionValues")),
                    );
                    // `deletionVector` is null/absent on an ordinary add; present means some of
                    // this file's rows are logically deleted without being rewritten.
                    if add.get("deletionVector").is_some_and(|v| !v.is_null()) {
                        with_deletion_vectors.insert(path.to_string(), ());
                    } else {
                        with_deletion_vectors.remove(path);
                    }
                }
            } else if let Some(remove) = action.get("remove") {
                if let Some(path) = remove.get("path").and_then(|v| v.as_str()) {
                    active.remove(path);
                    with_deletion_vectors.remove(path);
                }
            }
            // txn / commitInfo / anything else → ignored.
        }
    }

    let schema = schema.ok_or_else(|| EngineError::UnsupportedFormat {
        detail: format!(
            "delta: {} declares no metaData (schema) in its log",
            log_dir.display()
        ),
    })?;
    check_reader_requirements(
        table_dir,
        protocol.as_ref(),
        &column_mapping,
        with_deletion_vectors.len(),
    )?;
    let files = active
        .into_iter()
        .map(|(raw_path, partition_values)| AddFile {
            path: resolve(&percent_decode(&raw_path), table_dir),
            partition_values,
        })
        .collect();
    Ok(TablePlan { files, schema })
}

/// The full Arrow schema of the table — data columns **and** partition columns, in the canonical
/// `schemaString` order. This is the schema every window read returns.
pub fn schema(plan: &TablePlan) -> Result<SchemaRef> {
    let fields: Vec<Field> = plan
        .schema
        .fields
        .iter()
        .map(|f| Field::new(&f.name, f.data_type.clone(), f.nullable))
        .collect();
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// The table's live row count: the sum of the active data files' Parquet footer row counts.
///
/// Exact, and it stays exact because [`plan`] refuses the one case that would break it: a `remove`
/// drops a whole file, so the only row-level delete Delta has is the deletion vector, and a table
/// carrying one never reaches this function.
pub fn row_count(plan: &TablePlan) -> Result<u64> {
    let mut total: u64 = 0;
    for file in &plan.files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&file.path)?)
            .map_err(EngineError::parquet)?;
        let rows = builder.metadata().file_metadata().num_rows().max(0) as u64;
        total += rows;
    }
    Ok(total)
}

/// Read a `offset..offset+limit` row window of the table. Walks the active data files in order,
/// skipping whole files by their footer row counts and pushing the residual offset/limit into the
/// Parquet reader (row-group skipping); each file's batches are reordered/padded to the full
/// schema and have their partition columns appended from the file's `partitionValues`. Returns the
/// full schema plus the windowed batches (all matching that schema).
pub fn read_window(
    plan: &TablePlan,
    offset: usize,
    limit: usize,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let target = schema(plan)?;
    let mut batches = Vec::new();
    let mut to_skip = offset;
    let mut remaining = limit;
    for file in &plan.files {
        if remaining == 0 {
            break;
        }
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&file.path)?)
            .map_err(EngineError::parquet)?;
        let frows = builder.metadata().file_metadata().num_rows().max(0) as usize;
        if to_skip >= frows {
            to_skip -= frows; // whole file precedes the window
            continue;
        }
        let bs = remaining.clamp(1, DEFAULT_BATCH_SIZE);
        builder = builder.with_batch_size(bs);
        if to_skip > 0 {
            builder = builder.with_offset(to_skip);
        }
        builder = builder.with_limit(remaining);
        let reader = builder.build().map_err(EngineError::parquet)?;
        let mut got = 0usize;
        for b in reader {
            let b = b.map_err(EngineError::arrow)?;
            got += b.num_rows();
            batches.push(project_file_batch(&b, &target, plan, file)?);
            if got >= remaining {
                break;
            }
        }
        remaining = remaining.saturating_sub(got);
        to_skip = 0;
    }
    Ok((target, batches))
}

/// Reorder/pad one data-file batch to the full `target` schema: partition columns become constant
/// arrays from this file's `partitionValues`; data columns are taken by name (cast if the Parquet
/// type differs, null-filled if the file omits the column — schema evolution best-effort).
fn project_file_batch(
    batch: &RecordBatch,
    target: &SchemaRef,
    plan: &TablePlan,
    file: &AddFile,
) -> Result<RecordBatch> {
    let src = batch.schema();
    let n = batch.num_rows();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    // `target` is built from `plan.schema.fields` in the same order, so they zip 1:1.
    for (field, dfield) in target.fields().iter().zip(&plan.schema.fields) {
        let dt = field.data_type();
        if dfield.is_partition {
            let value = file
                .partition_values
                .get(&dfield.name)
                .and_then(|v| v.as_deref());
            cols.push(partition_array(value, dt, n));
        } else {
            match src.index_of(field.name()) {
                Ok(i) => {
                    let col = batch.column(i).clone();
                    if col.data_type() == dt {
                        cols.push(col);
                    } else {
                        cols.push(arrow_cast::cast(&col, dt).map_err(EngineError::arrow)?);
                    }
                }
                Err(_) => cols.push(new_null_array(dt, n)), // file predates this column
            }
        }
    }
    RecordBatch::try_new(target.clone(), cols).map_err(EngineError::arrow)
}

/// Build a length-`n` constant array for a partition column: the string value cast to the column's
/// Arrow type (`null`-filled when the value is null/absent, or when the cast fails — best-effort,
/// never an error so one odd partition value can't fail the whole read).
fn partition_array(value: Option<&str>, dt: &DataType, n: usize) -> ArrayRef {
    match value {
        Some(v) => {
            let strings = StringArray::from(vec![v.to_string(); n]);
            if matches!(dt, DataType::Utf8) {
                Arc::new(strings)
            } else {
                arrow_cast::cast(&strings, dt).unwrap_or_else(|_| new_null_array(dt, n))
            }
        }
        None => new_null_array(dt, n),
    }
}

/// Parse a `metaData` action into the table's [`DeltaSchema`].
/// Reader features this reader is known to handle correctly, so an unrecognised one can be named
/// in the warning below. `appendOnly`/`invariants`/`checkConstraints` are writer-side concerns a
/// read-only reader may ignore; `timestampNtz` and `typeWidening` land in the schema, which is
/// parsed from `schemaString` either way.
const KNOWN_READER_FEATURES: &[&str] = &[
    "appendOnly",
    "invariants",
    "checkConstraints",
    "timestampNtz",
    "typeWidening",
    "v2Checkpoint",
    "vacuumProtocolCheck",
];

/// Decide whether the table's declared reader requirements let this reader answer *correctly*.
///
/// The line drawn here is deliberate: **refuse only where a wrong answer is proven**, warn
/// otherwise. Over-refusing breaks tables that read exactly right today, and under-refusing is how
/// a viewer ends up confidently reporting rows that were deleted.
///
/// - **Deletion vectors** (`add.deletionVector`): the file's Parquet still physically contains the
///   deleted rows, and the DV that marks them lives in a separate Puffin blob this reader does not
///   read. Reading the file returns those rows as live and [`row_count`] still calls the footer
///   sum exact. Proven wrong ⇒ refuse.
/// - **Column mapping** (`delta.columnMapping.mode` = `name`/`id`): the physical Parquet column
///   names are opaque ids, so matching them against `schemaString`'s logical names finds nothing
///   and every data column comes back null-filled. Proven wrong ⇒ refuse.
/// - **Anything else in `readerFeatures`**: named on stderr, not refused. `v2Checkpoint` in
///   particular interacts with the checkpoint limitation documented at the top of this module
///   rather than adding a new one.
fn check_reader_requirements(
    table_dir: &Path,
    protocol: Option<&serde_json::Value>,
    column_mapping: &str,
    deletion_vector_files: usize,
) -> Result<()> {
    if deletion_vector_files > 0 {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "delta: {} has {deletion_vector_files} active data file(s) with deletion \
                 vectors, which this reader cannot apply — reading it would return deleted rows \
                 as live rows, so the read is refused rather than answered wrongly. Rewrite the \
                 deletes into the data files (`REORG TABLE … APPLY (PURGE)`) and re-open it.",
                table_dir.display()
            ),
        });
    }
    if !column_mapping.is_empty() && column_mapping != "none" {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "delta: {} uses column mapping (delta.columnMapping.mode = {column_mapping}), \
                 which this reader does not resolve — its data columns would all read as null. \
                 The read is refused rather than answered wrongly.",
                table_dir.display()
            ),
        });
    }
    if let Some(features) = protocol
        .and_then(|p| p.get("readerFeatures"))
        .and_then(|v| v.as_array())
    {
        let unknown: Vec<&str> = features
            .iter()
            .filter_map(|v| v.as_str())
            .filter(|f| !KNOWN_READER_FEATURES.contains(f) && *f != "deletionVectors")
            .filter(|f| !f.eq_ignore_ascii_case("columnMapping"))
            .collect();
        if !unknown.is_empty() {
            eprintln!(
                "lakeleto: NOTE {} declares reader feature(s) this reader does not model: {}. \
                 Its data files are read as-is; verify anything surprising against the writer.",
                table_dir.display(),
                unknown.join(", ")
            );
        }
    }
    Ok(())
}

fn parse_metadata(meta: &serde_json::Value) -> Result<DeltaSchema> {
    let schema_string = meta
        .get("schemaString")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EngineError::Other("delta: metaData is missing schemaString".into()))?;
    let partition_columns: Vec<String> = meta
        .get("partitionColumns")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let parsed: serde_json::Value = serde_json::from_str(schema_string)
        .map_err(|e| EngineError::Other(format!("delta: bad schemaString: {e}")))?;
    let raw_fields = parsed
        .get("fields")
        .and_then(|v| v.as_array())
        .ok_or_else(|| EngineError::Other("delta: schemaString has no `fields` array".into()))?;

    let mut fields = Vec::with_capacity(raw_fields.len());
    for f in raw_fields {
        let name = f
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| EngineError::Other("delta: a schema field is missing `name`".into()))?
            .to_string();
        let nullable = f.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
        let data_type = f
            .get("type")
            .map(delta_type_to_arrow)
            .unwrap_or(DataType::Utf8);
        let is_partition = partition_columns.iter().any(|p| p == &name);
        fields.push(DeltaField {
            name,
            data_type,
            nullable,
            is_partition,
        });
    }
    Ok(DeltaSchema {
        fields,
        partition_columns,
    })
}

/// Parse an `add` action's `partitionValues` object into `column -> Some(value)` / `None` (null).
fn parse_partition_values(v: Option<&serde_json::Value>) -> HashMap<String, Option<String>> {
    let mut out = HashMap::new();
    if let Some(serde_json::Value::Object(map)) = v {
        for (k, val) in map {
            let value = match val {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s.clone()),
                // Delta writes partition values as strings, but tolerate a bare number/bool.
                other => Some(other.to_string()),
            };
            out.insert(k.clone(), value);
        }
    }
    out
}

/// Map a Delta/Spark schema `type` to an Arrow [`DataType`]. A primitive is a type-name string; a
/// nested type (struct/array/map) arrives as a JSON object and maps to `Utf8` (best-effort).
fn delta_type_to_arrow(t: &serde_json::Value) -> DataType {
    match t {
        serde_json::Value::String(s) => spark_primitive_to_arrow(s),
        _ => DataType::Utf8, // struct / array / map → best-effort Utf8
    }
}

/// Map a Delta/Spark primitive type name to Arrow. Unknown names (and any nested/unsupported type)
/// fall back to `Utf8`.
fn spark_primitive_to_arrow(s: &str) -> DataType {
    match s {
        "long" => DataType::Int64,
        "integer" => DataType::Int32,
        "short" => DataType::Int16,
        "byte" => DataType::Int8,
        "double" => DataType::Float64,
        "float" => DataType::Float32,
        "string" => DataType::Utf8,
        "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        // Delta timestamps are microsecond precision; `timestamp_ntz` is the no-timezone variant.
        "timestamp" | "timestamp_ntz" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "binary" => DataType::Binary,
        other => parse_decimal(other).unwrap_or(DataType::Utf8),
    }
}

/// Parse a `decimal(p,s)` type name into `Decimal128(p, s)`. `None` for any other string.
fn parse_decimal(s: &str) -> Option<DataType> {
    let inner = s.strip_prefix("decimal(")?.strip_suffix(')')?;
    let (p, sc) = inner.split_once(',')?;
    Some(DataType::Decimal128(
        p.trim().parse().ok()?,
        sc.trim().parse().ok()?,
    ))
}

/// List the commit versions in `_delta_log/`: files named `<20 digits>.json`, ascending. Sidecar
/// files (`_last_checkpoint`, `*.checkpoint.parquet`, `*.crc`) are ignored.
fn commit_versions(log_dir: &Path) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(log_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".json") {
            if stem.len() == 20 && stem.bytes().all(|b| b.is_ascii_digit()) {
                if let Ok(v) = stem.parse::<u64>() {
                    out.push(v);
                }
            }
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Resolve a data-file path from the log: strip a `file://` scheme; join a relative path to the
/// table dir; keep an absolute path as-is.
fn resolve(raw: &str, table_dir: &Path) -> PathBuf {
    let s = raw.strip_prefix("file://").unwrap_or(raw);
    let p = Path::new(s);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        table_dir.join(p)
    }
}

/// Decode `%XX` percent-escapes in a Delta `add`/`remove` path. `+` is left literal (Delta paths
/// encode a space as `%20`, not `+`); a malformed escape is passed through unchanged.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A single hex digit's value (`0-9a-fA-F` → `0..=15`), or `None`.
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Array, Int64Array, StringArray};
    use parquet::arrow::ArrowWriter;
    use std::fs;

    /// Write a tiny Parquet file with `id: Int64` + `name: Utf8` (the non-partition columns).
    fn write_parquet(path: &Path, ids: &[i64], names: &[&str]) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            ],
        )
        .unwrap();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Build a Delta table by hand: schema with 2 data cols + 1 partition col (`region`, placed
    /// *between* the data cols to exercise reordering), and two `add`ed files in different
    /// partitions. Returns the temp dir (kept alive by the caller).
    fn build_table() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let log = root.join("_delta_log");
        fs::create_dir_all(&log).unwrap();

        // schemaString: partition column `region` sits in the middle of the field order.
        let schema_string = serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": true, "metadata": {}},
                {"name": "region", "type": "string", "nullable": true, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        })
        .to_string();
        let meta = serde_json::json!({
            "metaData": {
                "id": "test-table",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": ["region"],
                "configuration": {},
                "createdTime": 0
            }
        });
        let add_us = serde_json::json!({
            "add": {
                "path": "region=us/part-0.parquet",
                "partitionValues": {"region": "us"},
                "size": 1, "modificationTime": 0, "dataChange": true
            }
        });
        let add_eu = serde_json::json!({
            "add": {
                "path": "region=eu/part-1.parquet",
                "partitionValues": {"region": "eu"},
                "size": 1, "modificationTime": 0, "dataChange": true
            }
        });
        let commit0 = format!("{meta}\n{add_us}\n{add_eu}\n");
        fs::write(log.join("00000000000000000000.json"), commit0).unwrap();

        // Data files carry ONLY the non-partition columns (id, name).
        write_parquet(&root.join("region=us/part-0.parquet"), &[1, 2], &["a", "b"]);
        write_parquet(&root.join("region=eu/part-1.parquet"), &[3], &["c"]);
        dir
    }

    /// Collect every row of a window read as `(id, region)` pairs (region taken by name so column
    /// order can't be assumed).
    fn rows(plan: &TablePlan) -> Vec<(i64, String)> {
        let (schema, batches) = read_window(plan, 0, 100).unwrap();
        let region_idx = schema.index_of("region").unwrap();
        let id_idx = schema.index_of("id").unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let ids = b
                .column(id_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let regions = b
                .column(region_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push((ids.value(i), regions.value(i).to_string()));
            }
        }
        out.sort();
        out
    }

    /// Build a one-file table whose commit carries an extra action / add-field, so the reader's
    /// protocol handling can be exercised without rebuilding the whole fixture each time.
    /// `add_extra` is merged into the `add` action; `protocol` and `configuration` go where their
    /// names say.
    fn build_table_with(
        protocol: Option<serde_json::Value>,
        configuration: serde_json::Value,
        add_extra: serde_json::Value,
    ) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let log = root.join("_delta_log");
        fs::create_dir_all(&log).unwrap();

        let schema_string = serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": true, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        })
        .to_string();
        let meta = serde_json::json!({
            "metaData": {
                "id": "test-table",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": configuration,
                "createdTime": 0
            }
        });
        let mut add = serde_json::json!({
            "path": "part-0.parquet",
            "partitionValues": {},
            "size": 1, "modificationTime": 0, "dataChange": true
        });
        if let (Some(obj), Some(extra)) = (add.as_object_mut(), add_extra.as_object()) {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        }
        let add = serde_json::json!({ "add": add });

        let mut commit = String::new();
        if let Some(p) = protocol {
            commit.push_str(&serde_json::json!({ "protocol": p }).to_string());
            commit.push('\n');
        }
        commit.push_str(&format!("{meta}\n{add}\n"));
        fs::write(log.join("00000000000000000000.json"), commit).unwrap();
        write_parquet(&root.join("part-0.parquet"), &[1, 2], &["a", "b"]);
        dir
    }

    /// Build a table whose single `add` names `path` verbatim, so a test can point the log
    /// outside the table dir the way a hostile or merely relocated log would.
    fn build_table_at_path(root: &Path, add_path: &str) {
        let log = root.join("_delta_log");
        fs::create_dir_all(&log).unwrap();
        let schema_string = serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": true, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        })
        .to_string();
        let meta = serde_json::json!({
            "metaData": {
                "id": "test-table",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            }
        });
        let add = serde_json::json!({
            "add": {
                "path": add_path, "partitionValues": {},
                "size": 1, "modificationTime": 0, "dataChange": true
            }
        });
        fs::write(
            log.join("00000000000000000000.json"),
            format!("{meta}\n{add}\n"),
        )
        .unwrap();
    }

    #[test]
    fn plan_with_root_confines_data_files_to_root() {
        // `resolve` keeps an absolute `add.path` as-is, so without this check a Delta table sitting
        // inside --root can name — and the reader will open — any file on the machine.
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().join("secret.parquet");
        write_parquet(&secret, &[9], &["leak"]);

        let dir = tempfile::tempdir().unwrap();
        build_table_at_path(dir.path(), &secret.display().to_string());
        let root = fs::canonicalize(dir.path()).unwrap();

        let err = plan_with_root(dir.path(), Some(root.as_path())).unwrap_err();
        assert!(
            matches!(err, EngineError::Forbidden(_)),
            "expected Forbidden, got: {err:?}"
        );
        // The refusal must not echo the resolved path back to a client.
        assert!(
            !err.to_string().contains("secret.parquet"),
            "refusal leaked the resolved path: {err}"
        );
        // Unconfined (the engine's own call path) is unchanged.
        assert!(plan_with_root(dir.path(), None).is_ok());
    }

    #[test]
    fn plan_with_root_confines_relative_escapes_too() {
        // The other half of `resolve`: a relative path is joined to the table dir, so `..`
        // segments walk out of it without ever looking absolute.
        let outer = tempfile::tempdir().unwrap();
        write_parquet(&outer.path().join("secret.parquet"), &[9], &["leak"]);
        let tbl = outer.path().join("tbl");
        fs::create_dir_all(&tbl).unwrap();
        build_table_at_path(&tbl, "../secret.parquet");

        let root = fs::canonicalize(&tbl).unwrap();
        assert!(matches!(
            plan_with_root(&tbl, Some(root.as_path())).unwrap_err(),
            EngineError::Forbidden(_)
        ));
    }

    #[test]
    fn plan_with_root_allows_an_in_root_table() {
        // The complement: confinement must not break the ordinary case it is guarding.
        let dir = tempfile::tempdir().unwrap();
        build_table_at_path(dir.path(), "part-0.parquet");
        write_parquet(&dir.path().join("part-0.parquet"), &[1, 2], &["a", "b"]);

        let root = fs::canonicalize(dir.path()).unwrap();
        let plan = plan_with_root(dir.path(), Some(root.as_path())).unwrap();
        assert_eq!(plan.files.len(), 1);
    }

    #[test]
    fn the_plan_cache_notices_a_new_commit() {
        // The memo is keyed on the log's (highest version, count, mtime). A table that gains a
        // commit must re-plan — a viewer that keeps showing a stale file set would be its own bug.
        let dir = tempfile::tempdir().unwrap();
        build_table_at_path(dir.path(), "part-0.parquet");
        write_parquet(&dir.path().join("part-0.parquet"), &[1, 2], &["a", "b"]);
        assert_eq!(plan(dir.path()).unwrap().files.len(), 1);
        // A second call with nothing changed is served from the memo — same answer either way,
        // which is the point: the cache must be invisible except in cost.
        assert_eq!(plan(dir.path()).unwrap().files.len(), 1);

        let remove = serde_json::json!({
            "remove": {"path": "part-0.parquet", "dataChange": true, "deletionTimestamp": 1}
        });
        fs::write(
            dir.path().join("_delta_log/00000000000000000001.json"),
            format!("{remove}\n"),
        )
        .unwrap();
        assert!(
            plan(dir.path()).unwrap().files.is_empty(),
            "a new commit must invalidate the memo"
        );
    }

    #[test]
    fn deletion_vectors_are_refused_rather_than_silently_dropped() {
        // The rows a DV marks are still physically in the Parquet. Reading the file without
        // applying the vector returns them as live rows AND reports the footer sum as an exact
        // row count — a confidently wrong answer, which is worse than no answer.
        let dir = build_table_with(
            Some(serde_json::json!({
                "minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]
            })),
            serde_json::json!({}),
            serde_json::json!({
                "deletionVector": {
                    "storageType": "u", "pathOrInlineDv": "xyz",
                    "offset": 1, "sizeInBytes": 32, "cardinality": 1
                }
            }),
        );
        let err = plan(dir.path()).unwrap_err();
        assert!(
            matches!(err, EngineError::UnsupportedFormat { .. }),
            "expected UnsupportedFormat, got: {err:?}"
        );
        assert!(
            err.to_string().contains("deletion vector"),
            "refusal should name the mechanism; got: {err}"
        );
    }

    #[test]
    fn a_deletion_vector_on_a_removed_file_does_not_refuse() {
        // The refusal keys off *active* files. A DV attached to a file that a later commit
        // tombstoned describes rows nobody reads, so refusing on it would be a false positive.
        let dir = build_table_with(
            None,
            serde_json::json!({}),
            serde_json::json!({
                "deletionVector": {"storageType": "u", "pathOrInlineDv": "xyz", "cardinality": 1}
            }),
        );
        let log = dir.path().join("_delta_log");
        let remove = serde_json::json!({
            "remove": {"path": "part-0.parquet", "dataChange": true, "deletionTimestamp": 1}
        });
        fs::write(log.join("00000000000000000001.json"), format!("{remove}\n")).unwrap();

        let plan = plan(dir.path()).unwrap();
        assert!(plan.files.is_empty());
    }

    #[test]
    fn declaring_the_deletion_vector_feature_without_using_it_still_reads() {
        // `readerFeatures` says what the table MAY use, not what it does. A table that enabled the
        // feature but has written no vector yet reads correctly, so it is not refused.
        let dir = build_table_with(
            Some(serde_json::json!({
                "minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": ["deletionVectors"]
            })),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let plan = plan(dir.path()).unwrap();
        assert_eq!(plan.files.len(), 1);
        assert_eq!(read_window(&plan, 0, 100).unwrap().1[0].num_rows(), 2);
    }

    #[test]
    fn column_mapping_is_refused_rather_than_null_filling_every_column() {
        // Under column mapping the physical Parquet columns are named by id, so matching them
        // against schemaString's logical names finds nothing and every column reads as null.
        for mode in ["name", "id"] {
            let dir = build_table_with(
                None,
                serde_json::json!({ "delta.columnMapping.mode": mode }),
                serde_json::json!({}),
            );
            let err = plan(dir.path()).unwrap_err();
            assert!(
                matches!(err, EngineError::UnsupportedFormat { .. }),
                "mode {mode}: expected UnsupportedFormat, got: {err:?}"
            );
            assert!(
                err.to_string().contains("column mapping"),
                "mode {mode}: refusal should name the mechanism; got: {err}"
            );
        }
        // `none` is the default and must stay readable.
        let dir = build_table_with(
            None,
            serde_json::json!({ "delta.columnMapping.mode": "none" }),
            serde_json::json!({}),
        );
        assert_eq!(plan(dir.path()).unwrap().files.len(), 1);
    }

    #[test]
    fn an_unmodelled_reader_feature_is_noted_but_not_refused() {
        // The line between refusing and warning: a wrong answer is proven for deletion vectors and
        // column mapping, and is NOT proven for an arbitrary future feature. Refusing everything
        // unrecognised would break tables that read exactly right today.
        let dir = build_table_with(
            Some(serde_json::json!({
                "minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": ["someFutureThing", "v2Checkpoint"]
            })),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        assert_eq!(plan(dir.path()).unwrap().files.len(), 1);
    }

    #[test]
    fn schema_includes_partition_column_in_declared_order() {
        let dir = build_table();
        let plan = plan(dir.path()).unwrap();
        let schema = schema(&plan).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Canonical schemaString order preserved, partition column interleaved (not appended).
        assert_eq!(names, vec!["id", "region", "name"]);
        assert_eq!(
            schema.field_with_name("region").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn window_read_fills_partition_values_from_add_actions() {
        let dir = build_table();
        let plan = plan(dir.path()).unwrap();
        assert_eq!(row_count(&plan).unwrap(), 3);
        // Union of both files, each row carrying its file's partition value.
        assert_eq!(
            rows(&plan),
            vec![
                (1, "us".to_string()),
                (2, "us".to_string()),
                (3, "eu".to_string()),
            ]
        );
    }

    #[test]
    fn remove_in_a_later_commit_drops_the_file() {
        let dir = build_table();
        let log = dir.path().join("_delta_log");
        // Version 1 tombstones the `eu` file — its rows must vanish from the active set.
        let remove_eu = serde_json::json!({
            "remove": {
                "path": "region=eu/part-1.parquet",
                "deletionTimestamp": 1, "dataChange": true
            }
        });
        fs::write(
            log.join("00000000000000000001.json"),
            format!("{remove_eu}\n"),
        )
        .unwrap();

        let plan = plan(dir.path()).unwrap();
        assert_eq!(plan.files.len(), 1);
        assert_eq!(row_count(&plan).unwrap(), 2);
        assert_eq!(
            rows(&plan),
            vec![(1, "us".to_string()), (2, "us".to_string())]
        );
    }

    #[test]
    fn percent_decode_handles_escapes_and_passes_through_plain() {
        assert_eq!(percent_decode("a%20b/c.parquet"), "a b/c.parquet");
        assert_eq!(
            percent_decode("region=a+b/part.parquet"),
            "region=a+b/part.parquet"
        );
        assert_eq!(percent_decode("no-escapes.parquet"), "no-escapes.parquet");
        // A malformed escape at the very end is passed through unchanged.
        assert_eq!(percent_decode("bad%2"), "bad%2");
    }

    #[test]
    fn spark_types_map_to_expected_arrow_types() {
        assert_eq!(spark_primitive_to_arrow("long"), DataType::Int64);
        assert_eq!(spark_primitive_to_arrow("integer"), DataType::Int32);
        assert_eq!(spark_primitive_to_arrow("double"), DataType::Float64);
        assert_eq!(spark_primitive_to_arrow("date"), DataType::Date32);
        assert_eq!(
            spark_primitive_to_arrow("timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            spark_primitive_to_arrow("decimal(10,2)"),
            DataType::Decimal128(10, 2)
        );
        // Unknown / nested → best-effort Utf8.
        assert_eq!(spark_primitive_to_arrow("interval"), DataType::Utf8);
    }
}
