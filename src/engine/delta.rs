//! Self-contained **Delta Lake** table reader (`--features delta`).
//!
//! Mirrors the Iceberg reader ([`crate::iceberg`]) in spirit: it reconstructs the table's *active*
//! data-file set and current schema from the transaction log, and the engine then reads the
//! Parquet data files the log names with the existing arrow-58 Parquet reader. No `deltalake` /
//! `delta-rs` crate (which rides its own arrow/parquet line and would collide with the workspace's
//! arrow 58) — on-thesis for #25 ("the engine is a commodity; just read the Parquet the table
//! points to"). Read-only.
//!
//! A table is a directory on this machine ([`plan`]) or a prefix in an object store
//! ([`plan_in_store`], with `--features object-store`). Both are planned by the same replay,
//! reading the log through [`DeltaLog`]; the engine then reads the data files from disk, or from
//! the store by ranged requests. Nothing is copied to disk.
//!
//! ## What a Delta table looks like
//!
//! The table root holds a `_delta_log/` directory containing an ordered sequence of commit files
//! `00000000000000000000.json`, `…001.json`, … (20-digit zero-padded version). **Each line** of a
//! commit file is one JSON *action*. The actions this reader consumes:
//!
//! - `{"metaData": {"schemaString": "<JSON schema>", "partitionColumns": [...]}}` — the table
//!   schema (a JSON-encoded Spark/Delta struct schema) plus the partition columns. The **latest**
//!   `metaData` in version order wins.
//! - `{"add": {"path": "part-….parquet", "partitionValues": {...}}}` — a data file added. `path`
//!   is relative to the table root and **URL-encoded** (`%XX` escapes are decoded). Its `stats`,
//!   when written, record the file's rows (`numRecords`), which a count and a window read instead
//!   of the file's footer.
//! - `{"remove": {"path": "…"}}` — a data file tombstoned.
//! - `{"protocol": {"minReaderVersion": N, "readerFeatures": [...]}}` — the reader-side
//!   requirements the table declares. Read to decide what this reader may honestly answer; see
//!   the refusals below.
//! - `{"commitInfo": {"timestamp": …}}` — read for when the table last changed, with each
//!   `add`'s `modificationTime` and each `remove`'s `deletionTimestamp`; nothing else in it is.
//! - `{"txn": …}` — ignored.
//!
//! The **active file set** is every `add` path minus any later `remove` of that same path: commits
//! are replayed in ascending version order, and because a `remove` references the *identical*
//! (still URL-encoded) `path` string an `add` used, adds/removes are matched on that raw string
//! before it is decoded to a filesystem path.
//!
//! ## Checkpoints
//!
//! A writer periodically snapshots the table's state at version `N` into a **checkpoint**: one
//! Parquet file, `N.checkpoint.parquet`, or several, `N.checkpoint.<part>.<parts>.parquet`, whose
//! rows are the actions a replay to `N` leaves standing. Log cleanup then deletes the commits
//! before it. So the replay starts from the newest complete checkpoint and replays only the
//! commits after it, every one of which must be there: a missing commit is an error, never a
//! table read short. With no checkpoint, the replay starts at version 0.
//!
//! `_delta_log/_last_checkpoint` names the newest checkpoint, and is a hint. In an object store the
//! log is listed from the version it names, rather than from the start, but the listing decides:
//! a newer checkpoint it finds is used, and a hint that leads to none is dropped for a listing of
//! the whole log.
//!
//! A checkpoint is read by its Parquet columns, only those the replay uses
//! ([`CHECKPOINT_COLUMNS`]), and each of its rows is turned into the JSON action a commit would
//! carry, so one replay serves both.
//!
//! **V2 checkpoints** are not read: `N.checkpoint.<uuid>.json|parquet`, and a checkpoint that keeps
//! its files in `_delta_log/_sidecars/`. A table that can be rebuilt only through one is refused,
//! naming it. One with a classic checkpoint, or every commit since, reads as usual.
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
//!   rows are still physically present in the Parquet and the vector that marks them is a blob
//!   this reader does not read — so they would come back as live rows, with the row count still
//!   reported as exact.
//! - **Column mapping** (`delta.columnMapping.mode` = `name`/`id`): physical Parquet column names
//!   are opaque ids, so every logical column would null-fill.
//!
//! Anything else a `protocol` action lists in `readerFeatures` is noted on stderr and read anyway.

#![cfg(feature = "delta")]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{new_null_array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::error::{EngineError, Result};

/// Batch size for reading a checkpoint. The reader is self-contained (it does not see the
/// engine's configured batch size), so it uses the same default the local engine does.
const DEFAULT_BATCH_SIZE: usize = 8192;

/// The log file that names the newest checkpoint.
const LAST_CHECKPOINT: &str = "_last_checkpoint";

/// The columns of a checkpoint the replay reads: what [`Replay::apply`] takes from each action,
/// and `sidecar`, which marks a V2 checkpoint that keeps its files elsewhere. The rest, each
/// file's `tags` and a V2 checkpoint's `checkpointMetadata` among them, are not read.
///
/// A file's row count is in `add.stats`, a JSON string, or `add.stats_parsed`, a struct, as the
/// writer chose (`delta.checkpoint.writeStatsAsJson` / `…AsStruct`).
const CHECKPOINT_COLUMNS: &[&str] = &[
    "add.path",
    "add.partitionValues",
    "add.size",
    "add.modificationTime",
    "add.stats",
    "add.stats_parsed.numRecords",
    "add.deletionVector.storageType",
    "remove.path",
    "remove.deletionTimestamp",
    "metaData",
    "protocol",
    "sidecar.path",
];

/// The checkpoint columns a plan asked for its files' statistics ([`Want::stats`]) reads besides
/// [`CHECKPOINT_COLUMNS`]: the statistics as a struct, when the writer kept them as one. A JSON
/// `add.stats` is read either way.
const CHECKPOINT_STATS_COLUMNS: &[&str] = &["add.stats_parsed"];

/// What a plan is asked for, besides the table as it is now with what its reads need.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Want {
    /// The version to stand at: the table as it was after this commit. `None` for the newest.
    pub version: Option<u64>,
    /// Keep each file's statistics ([`AddFile::stats`]), for a table described by its log
    /// ([`describe`]). Off for a read, which needs only the rows each file holds.
    pub stats: bool,
    /// Stand at the version current at this instant, in milliseconds since the epoch, by the
    /// table's in-commit timestamps ([`version_at`]), instead of at [`Self::version`].
    pub instant_ms: Option<i64>,
}

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

/// An active data file of the table: where it is, plus this file's partition values
/// (`column -> Some(value)`, or `None` for a null/absent partition value).
#[derive(Debug, Clone)]
pub struct AddFile {
    /// The file's path on this machine, or its URI in an object store.
    pub path: PathBuf,
    pub partition_values: HashMap<String, Option<String>>,
    /// The file's size in bytes, as its `add` action records it.
    pub size: Option<u64>,
    /// The file's rows, as its `add` action's statistics record them (`numRecords`), when they do.
    /// A count and a window take them instead of opening the file's footer, which in an object
    /// store is a request per file.
    pub num_records: Option<u64>,
    /// The file's statistics as its `add` action records them, as JSON, when the plan was asked
    /// for them ([`Want::stats`]) and the log records them.
    pub stats: Option<String>,
}

/// A read plan for the table's current state: the active (non-tombstoned) data files and the
/// current schema (including partition columns).
#[derive(Debug, Clone)]
pub struct TablePlan {
    pub files: Vec<AddFile>,
    pub schema: DeltaSchema,
    /// When the table last changed, in milliseconds since the epoch: the latest time its log
    /// records — a commit's `commitInfo.timestamp`, an `add`'s `modificationTime`, a `remove`'s
    /// `deletionTimestamp`. From the log, never a file's modification time, which a copy or a
    /// checkout resets. `None` when the log records none.
    pub committed_ms: Option<i64>,
    /// The plan's version's in-commit timestamp (`commitInfo.inCommitTimestamp`), when its commit
    /// records one and was read: the time the protocol dates that version by. Read for a plan asked
    /// for its statistics ([`Want::stats`]) only.
    pub in_commit_ms: Option<i64>,
    /// The `_delta_log/` files the plan was read from, by name: `_last_checkpoint`, the
    /// checkpoint's files and the commits after it. What [`plan_with_root`] confines.
    pub log_files: Vec<String>,
    /// The version the plan stands at: the newest the log holds, or the one asked for.
    pub version: u64,
}

/// Memoized [`plan`] results, keyed by table dir and a fingerprint of its `_delta_log/`.
///
/// Replaying the log means reading a checkpoint and JSON-parsing every commit after it, and the
/// cost grows with the table's history rather than its size. `serve --root` makes that bite twice
/// per request — once to confine the plan ([`plan_with_root`]) and once for the engine's own read
/// — which is exactly the deployment shape most likely to be pointed at a table with a long
/// history. The memo collapses the pair back to one replay.
///
/// The fingerprint is `(highest commit, log files, mtime of the highest commit)`. A new commit
/// changes the version and the count; a commit rewritten in place changes its mtime. It does not
/// notice a rewrite that preserves both mtime and version, which is not something a Delta writer
/// does — commits are append-only by construction. A checkpoint written, or commits cleaned up,
/// change the count and so cost a replay, though the state they describe is the same.
///
/// Only tables on this machine are memoized. One in an object store is read as the caller's
/// identity, and a memo keyed by its URI would hand one caller's plan to another.
static PLAN_CACHE: std::sync::OnceLock<std::sync::Mutex<PlanCache>> = std::sync::OnceLock::new();

/// Canonical table dir -> the fingerprint the plan was built from, and the plan.
type PlanCache = HashMap<PathBuf, (LogFingerprint, Arc<TablePlan>)>;

/// Tables kept in [`PLAN_CACHE`] before it is cleared wholesale. A viewer looks at a handful of
/// tables at a time, and a plan is cheap to rebuild, so a flat cap and a full clear beat the
/// bookkeeping of a real eviction policy.
const PLAN_CACHE_MAX_TABLES: usize = 8;

/// See [`PLAN_CACHE`]. `(highest commit, log files, mtime nanos of the highest commit)`.
type LogFingerprint = (u64, usize, u128);

fn log_fingerprint(log_dir: &Path, names: &[String]) -> LogFingerprint {
    let parsed: Vec<LogName> = names.iter().filter_map(|n| parse_log_name(n)).collect();
    let highest = parsed
        .iter()
        .filter_map(|n| match n {
            LogName::Commit(v) => Some(*v),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let mtime = std::fs::metadata(log_dir.join(format!("{highest:020}.json")))
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    (highest, parsed.len(), mtime)
}

/// Resolve the current read plan of the table in directory `table_dir`: replay its log from the
/// newest checkpoint (see the module docs), applying `metaData` (latest wins), collecting `add`
/// paths, dropping any later `remove`d path. Errors if the directory is not a Delta table (no
/// `_delta_log/`), its log cannot be rebuilt (a commit missing with no checkpoint after it), or it
/// never declares a `metaData`.
///
/// Results are memoized per table — see [`PLAN_CACHE`].
pub fn plan(table_dir: &Path) -> Result<TablePlan> {
    Ok((*plan_cached(table_dir)?).clone())
}

/// Like [`plan`], but when `root` is `Some`, every file the reader will open — each `_delta_log/`
/// file the plan was read from and every active data file the log names — is canonicalized and
/// required to lie within `root`, refusing anything that escapes with [`EngineError::Forbidden`].
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
    // source its commits — and therefore its file list — from outside the root. Then each file of
    // it the plan was read from: a checkpoint names files as surely as a commit does.
    let log_dir = table_dir.join("_delta_log");
    guard(&log_dir)?;
    for name in &plan.log_files {
        guard(&log_dir.join(name))?;
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
    let fingerprint = log_names(&log_dir)
        .ok()
        .map(|names| log_fingerprint(&log_dir, &names));

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
    if !table_dir.join("_delta_log").is_dir() {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "delta: {} is not a Delta table (no _delta_log/ directory)",
                table_dir.display()
            ),
        });
    }
    replay(&LocalLog { table_dir }, &Want::default())
}

/// The table in directory `table_dir` as [`Want`] asks: at an older version, or with its files'
/// statistics. Not memoized: the memo holds the newest plan, for reads.
pub fn plan_at(table_dir: &Path, want: &Want) -> Result<TablePlan> {
    if !table_dir.join("_delta_log").is_dir() {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "delta: {} is not a Delta table (no _delta_log/ directory)",
                table_dir.display()
            ),
        });
    }
    replay(&LocalLog { table_dir }, want)
}

/// [`plan`] for the table at `table`, the URI of a prefix in an object store, as `want` asks. Its
/// log, and the checkpoint the replay starts from, are read through `stores` as the identity they
/// were built for, and each data file's path is the URI the engine then reads it from, by ranged
/// requests.
///
/// A relative `add.path` is joined to the table's URI. An absolute one must be an object-store
/// URI: a log in a store that names a file on this machine (`file:///…`, `/…`) is refused, since
/// reading it would hand the caller a file of the host's.
///
/// Not memoized — see [`PLAN_CACHE`].
#[cfg(feature = "object-store")]
pub(crate) fn plan_in_store(
    table: &str,
    stores: &crate::objstore::Stores,
    ctx: &crate::context::RequestContext,
    want: &Want,
) -> Result<TablePlan> {
    replay(
        &StoreLog {
            table: table.trim_end_matches('/'),
            stores,
            ctx,
        },
        want,
    )
}

/// A Delta table's `_delta_log/`, wherever the table is: a directory on this machine
/// ([`LocalLog`]), or a prefix in an object store (`StoreLog`). [`replay`] reads through this
/// alone, so a table plans the same wherever it is.
trait DeltaLog {
    /// The table, as messages name it.
    fn table(&self) -> String;
    /// The names of the files directly in `_delta_log/`. With `from`, a log that can list from a
    /// version on (an object store) may leave out the files before it; a directory lists them all.
    fn names(&self, from: Option<u64>) -> Result<Vec<String>>;
    /// Whether [`Self::names`] with a `from` may leave files out, so that a listing which found
    /// nothing from there is worth repeating whole. A directory lists everything either way.
    fn lists_from(&self) -> bool {
        false
    }
    /// The contents of the log file `name`, or `None` when there is no such file.
    fn read(&self, name: &str) -> Result<Option<Vec<u8>>>;
    /// Hand each batch of checkpoint file `name`'s `columns` (see [`CHECKPOINT_COLUMNS`]) to
    /// `each`.
    fn checkpoint(
        &self,
        name: &str,
        columns: &[&str],
        each: &mut dyn FnMut(&RecordBatch) -> Result<()>,
    ) -> Result<()>;
    /// Where the data file an `add.path` names is: `raw` is the path as the log records it,
    /// still URL-encoded.
    fn data_path(&self, raw: &str) -> Result<PathBuf>;
}

/// A table on this machine: `_delta_log/` is a directory.
struct LocalLog<'a> {
    table_dir: &'a Path,
}

impl LocalLog<'_> {
    fn log_dir(&self) -> PathBuf {
        self.table_dir.join("_delta_log")
    }
}

impl DeltaLog for LocalLog<'_> {
    fn table(&self) -> String {
        self.table_dir.display().to_string()
    }

    fn names(&self, _from: Option<u64>) -> Result<Vec<String>> {
        log_names(&self.log_dir())
    }

    fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        match std::fs::read(self.log_dir().join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn checkpoint(
        &self,
        name: &str,
        columns: &[&str],
        each: &mut dyn FnMut(&RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(self.log_dir().join(name))?)
                .map_err(EngineError::parquet)?;
        let mask = ProjectionMask::columns(
            builder.metadata().file_metadata().schema_descr(),
            columns.iter().copied(),
        );
        let reader = builder
            .with_projection(mask)
            .with_batch_size(DEFAULT_BATCH_SIZE)
            .build()
            .map_err(EngineError::parquet)?;
        for batch in reader {
            each(&batch.map_err(EngineError::arrow)?)?;
        }
        Ok(())
    }

    fn data_path(&self, raw: &str) -> Result<PathBuf> {
        Ok(resolve(&percent_decode(raw), self.table_dir))
    }
}

/// A table in an object store: `_delta_log/` is a prefix, read as the identity `stores` was built
/// for. The call's deadline and cancellation are checked before each request.
#[cfg(feature = "object-store")]
struct StoreLog<'a> {
    /// The table's URI, without a trailing `/`.
    table: &'a str,
    stores: &'a crate::objstore::Stores,
    ctx: &'a crate::context::RequestContext,
}

#[cfg(feature = "object-store")]
impl StoreLog<'_> {
    fn uri(&self, name: &str) -> String {
        format!("{}/_delta_log/{name}", self.table)
    }
}

#[cfg(feature = "object-store")]
impl DeltaLog for StoreLog<'_> {
    fn table(&self) -> String {
        self.table.to_string()
    }

    fn lists_from(&self) -> bool {
        true
    }

    fn names(&self, from: Option<u64>) -> Result<Vec<String>> {
        self.ctx.check()?;
        let dir = self.uri("");
        match from {
            // Every file of version `from` and after sorts after its bare 20-digit version.
            Some(version) => self.stores.names_after(&dir, &format!("{version:020}")),
            None => self.stores.names(&dir),
        }
    }

    fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.ctx.check()?;
        Ok(self
            .stores
            .find(&self.uri(name))?
            .map(|bytes| bytes.to_vec()))
    }

    fn checkpoint(
        &self,
        name: &str,
        columns: &[&str],
        each: &mut dyn FnMut(&RecordBatch) -> Result<()>,
    ) -> Result<()> {
        use futures::StreamExt;
        self.ctx.check()?;
        // By ranged requests, like a data file: the footer, then only the columns the replay reads.
        let builder = self.stores.parquet(&self.uri(name), None)?;
        let mask = ProjectionMask::columns(
            builder.metadata().file_metadata().schema_descr(),
            columns.iter().copied(),
        );
        let mut stream = builder
            .with_projection(mask)
            .with_batch_size(DEFAULT_BATCH_SIZE)
            .build()
            .map_err(EngineError::parquet)?;
        loop {
            self.ctx.check()?;
            let Some(batch) = crate::objstore::block_on(stream.next()) else {
                break;
            };
            each(&batch.map_err(EngineError::parquet)?)?;
        }
        Ok(())
    }

    fn data_path(&self, raw: &str) -> Result<PathBuf> {
        // The path is a URI reference (RFC 2396), so it stays encoded: the store decodes it once,
        // to the object's key, exactly as the protocol asks of a reader.
        if crate::source::is_object_uri(raw) {
            return Ok(PathBuf::from(raw));
        }
        if raw.contains("://") || raw.starts_with('/') {
            // Path-free, like `plan_with_root`'s refusal: the log is the caller's to read, but
            // what it names on the host is not.
            return Err(EngineError::Forbidden(
                "delta: a table in an object store names a data file that is not in an object \
                 store, which this reader will not read"
                    .to_string(),
            ));
        }
        Ok(PathBuf::from(format!("{}/{raw}", self.table)))
    }
}

/// What a file in `_delta_log/` is, by its name.
#[derive(Debug, Clone, PartialEq)]
enum LogName {
    /// `<version>.json`: a commit.
    Commit(u64),
    /// `<version>.checkpoint.parquet`: a single-file checkpoint.
    Checkpoint(u64),
    /// `<version>.checkpoint.<part>.<parts>.parquet`: one part of a multi-part checkpoint, each
    /// number ten digits.
    Part { version: u64, part: u32, parts: u32 },
    /// `<version>.checkpoint.<uuid>.json|parquet`: a V2 checkpoint, not read.
    V2(u64),
}

impl LogName {
    /// The version the file is of.
    fn version(&self) -> u64 {
        match *self {
            LogName::Commit(v) | LogName::Checkpoint(v) | LogName::V2(v) => v,
            LogName::Part { version, .. } => version,
        }
    }
}

/// Classify a `_delta_log/` file name. `None` for anything else in the log: `_last_checkpoint`,
/// `.crc` files, compacted logs (`<from>.<to>.compacted.json`), and directories.
fn parse_log_name(name: &str) -> Option<LogName> {
    let (stem, rest) = (name.get(..20)?, name.get(20..)?);
    if !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let version: u64 = stem.parse().ok()?;
    if rest == ".json" {
        return Some(LogName::Commit(version));
    }
    let rest = rest.strip_prefix(".checkpoint.")?;
    if rest == "parquet" {
        return Some(LogName::Checkpoint(version));
    }
    let ten_digits = |s: &str| s.len() == 10 && s.bytes().all(|b| b.is_ascii_digit());
    if let Some((part, parts)) = rest
        .strip_suffix(".parquet")
        .and_then(|mid| mid.split_once('.'))
        .filter(|(part, parts)| ten_digits(part) && ten_digits(parts))
    {
        let (part, parts): (u32, u32) = (part.parse().ok()?, parts.parse().ok()?);
        return (1..=parts).contains(&part).then_some(LogName::Part {
            version,
            part,
            parts,
        });
    }
    let id = rest
        .strip_suffix(".parquet")
        .or_else(|| rest.strip_suffix(".json"))?;
    (!id.is_empty() && !id.contains('.')).then_some(LogName::V2(version))
}

/// The names of the files directly in a `_delta_log/` directory.
fn log_names(log_dir: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(log_dir)? {
        out.push(entry?.file_name().to_string_lossy().into_owned());
    }
    Ok(out)
}

/// What a listing of `_delta_log/` holds that the replay can start from.
#[derive(Debug, Default)]
struct Listing {
    commits: BTreeSet<u64>,
    /// Complete checkpoints by version: the file to read, or every part, in order.
    checkpoints: BTreeMap<u64, Vec<String>>,
    /// Versions with a V2 checkpoint, which is not read but is named when it is the only way:
    /// one named with a UUID, or a classic one found to keep its files in sidecars.
    v2: BTreeSet<u64>,
    /// Versions with a multi-part checkpoint missing a part, named when nothing else is there.
    incomplete: BTreeSet<u64>,
}

/// Where a replay starts, and the commits it then reads.
#[derive(Debug, PartialEq)]
struct Route {
    /// The checkpoint's version and its files, or `None` to replay from version 0.
    checkpoint: Option<(u64, Vec<String>)>,
    commits: Vec<u64>,
}

impl Listing {
    /// What `names` holds, up to version `upto` when one is given: what a replay standing there
    /// can start from.
    fn of(names: &[String], upto: Option<u64>) -> Listing {
        let mut listing = Listing::default();
        // (version, parts) -> the parts present, by number. A part set is complete when all of
        // its numbers are there; a writer that crashed halfway leaves one that is not.
        let mut parts: BTreeMap<(u64, u32), BTreeMap<u32, String>> = BTreeMap::new();
        let names = names.iter().filter(|name| {
            upto.is_none_or(|n| parse_log_name(name).is_some_and(|l| l.version() <= n))
        });
        for name in names {
            match parse_log_name(name) {
                Some(LogName::Commit(v)) => {
                    listing.commits.insert(v);
                }
                Some(LogName::Checkpoint(v)) => {
                    listing.checkpoints.insert(v, vec![name.clone()]);
                }
                Some(LogName::Part {
                    version,
                    part,
                    parts: n,
                }) => {
                    parts
                        .entry((version, n))
                        .or_default()
                        .insert(part, name.clone());
                }
                Some(LogName::V2(v)) => {
                    listing.v2.insert(v);
                }
                None => {}
            }
        }
        for ((version, n), got) in parts {
            if got.len() == n as usize {
                // A single file, when a version has both, is the one request rather than several.
                listing
                    .checkpoints
                    .entry(version)
                    .or_insert_with(|| got.into_values().collect());
            } else {
                listing.incomplete.insert(version);
            }
        }
        // A version whose complete checkpoint was found is not one with only an incomplete one.
        let complete: Vec<u64> = listing.checkpoints.keys().copied().collect();
        for version in complete {
            listing.incomplete.remove(&version);
        }
        listing
    }

    /// Start from the newest complete checkpoint, and read every commit after it, up to the
    /// newest. A commit missing from that run is an error: the state it changed is in no file
    /// this reader reads, and replaying around it would leave the table's files wrong. An older
    /// checkpoint is no way round it, since the gap is in its run too.
    fn route(&self, table: &str) -> Result<Route> {
        let newest_checkpoint = self.checkpoints.last_key_value();
        let Some(head) = self
            .commits
            .last()
            .copied()
            .max(newest_checkpoint.map(|(v, _)| *v))
        else {
            // Nothing this reader reads. Say what is there, when something is: a log cleaned up
            // to a checkpoint this reader cannot read is not "not a Delta table".
            let detail = if self.v2.is_empty() && self.incomplete.is_empty() {
                format!(
                    "delta: {table} has no commits or checkpoints in _delta_log/, so it is not a \
                     Delta table"
                )
            } else {
                format!(
                    "delta: {table}'s log has no commits and no checkpoint this reader reads, so \
                     the table cannot be rebuilt from its log.{}",
                    self.unread()
                )
            };
            return Err(EngineError::UnsupportedFormat { detail });
        };
        let first = newest_checkpoint.map_or(0, |(v, _)| v + 1);
        if let Some(missing) = (first..=head).find(|v| !self.commits.contains(v)) {
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "delta: {table}'s log has no commit {missing}, and no checkpoint after it \
                     that this reader reads, so the table cannot be rebuilt from its log: \
                     reading the commits that are there would leave files out.{}",
                    self.unread()
                ),
            });
        }
        Ok(Route {
            checkpoint: newest_checkpoint.map(|(v, files)| (*v, files.clone())),
            commits: (first..=head).collect(),
        })
    }

    /// The checkpoints that are there but not read, for a refusal to name: the newest V2 one,
    /// and the newest one missing a part.
    fn unread(&self) -> String {
        let mut out = String::new();
        if let Some(v) = self.v2.last() {
            out.push_str(&format!(
                " It has a V2 checkpoint (named with a UUID, or keeping its files in \
                 _delta_log/_sidecars/), at version {v}, which this reader does not read."
            ));
        }
        if let Some(v) = self.incomplete.last() {
            out.push_str(&format!(
                " Its multi-part checkpoint at version {v} is missing a part."
            ));
        }
        out
    }

    /// The checkpoint at `version` turned out to keep its files in sidecars: route around it,
    /// as a V2 checkpoint, which this reader does not read.
    fn passed_over(&mut self, version: u64) {
        self.checkpoints.remove(&version);
        self.v2.insert(version);
    }
}

/// The version `_last_checkpoint` names, if it names one. Anything unreadable is no hint at all.
fn last_checkpoint_version(bytes: &[u8]) -> Option<u64> {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()?
        .get("version")?
        .as_u64()
}

/// Rebuild the table's state from its log: the newest checkpoint, then the commits after it. With
/// [`Want::version`], the newest checkpoint at or before that version, and the commits up to it.
fn replay(log: &dyn DeltaLog, want: &Want) -> Result<TablePlan> {
    let table = log.table();
    let want = &match want.instant_ms {
        Some(ms) => Want {
            version: Some(version_at(log, &table, ms)?),
            instant_ms: None,
            ..*want
        },
        None => *want,
    };
    let mut log_files = Vec::new();
    let hint = match log.read(LAST_CHECKPOINT)? {
        Some(bytes) => {
            log_files.push(LAST_CHECKPOINT.to_string());
            last_checkpoint_version(&bytes)
        }
        None => None,
    };
    // A checkpoint past the version asked for is no start for it.
    let hint = hint.filter(|h| want.version.is_none_or(|n| *h <= n));
    let mut names = log.names(hint)?;
    // Whether `names` may leave out the files before the hint, as a listing from it in a store does.
    let mut partial = log.lists_from() && hint.is_some();
    // The hint led to no checkpoint: it is stale (the one it names was cleaned up) or wrong. A
    // listing of the whole log decides instead, where the first one could have left files out.
    if partial
        && hint.is_some_and(|from| {
            Listing::of(&names, want.version)
                .checkpoints
                .range(from..)
                .next()
                .is_none()
        })
    {
        names = log.names(None)?;
        partial = false;
    }
    let mut listing = Listing::of(&names, want.version);
    let mut passed = Vec::new();
    // A checkpoint found to keep its files in sidecars is passed over for the route without it:
    // an older checkpoint, or the commits from 0, when they are all there. Each pass drops one
    // checkpoint, so this ends. The version asked for is checked on every pass, since the route
    // around a checkpoint at it may end before it.
    loop {
        if let Some(n) = want.version {
            check_version_listed(&table, n, &names, &listing)?;
        }
        let route = listing.route(&table)?;
        match replay_route(log, &table, &route, log_files.clone(), want.stats)? {
            Replayed::Plan(plan) => return Ok(plan),
            Replayed::Sidecars(version) => {
                passed.push(version);
                if partial {
                    // Listed from the checkpoint that cannot be read: the route around it needs
                    // the files before it too.
                    names = log.names(None)?;
                    listing = Listing::of(&names, want.version);
                    partial = false;
                }
                for v in &passed {
                    listing.passed_over(*v);
                }
            }
        }
    }
}

/// Whether version `n` can be rebuilt from what the log lists, said as what is wrong when not: a
/// version past the newest is not there yet, and one older than anything the log still holds was
/// cleaned up. A route that then fails names what it lacks.
fn check_version_listed(table: &str, n: u64, names: &[String], upto: &Listing) -> Result<()> {
    let versions: BTreeSet<u64> = names
        .iter()
        .filter_map(|name| parse_log_name(name))
        .map(|l| l.version())
        .collect();
    let (Some(oldest), Some(newest)) = (versions.first(), versions.last()) else {
        return Ok(()); // the route says what an empty log is
    };
    if n > *newest {
        return Err(EngineError::Other(format!(
            "delta: {table} has no version {n}: its log ends at version {newest}"
        )));
    }
    let reachable = upto.checkpoints.contains_key(&n) || upto.commits.contains(&n);
    if !reachable && n < *oldest {
        return Err(EngineError::Other(format!(
            "delta: {table}'s version {n} is no longer in its log, whose oldest file is version \
             {oldest}: log cleanup removed it, and the version cannot be rebuilt"
        )));
    }
    if !reachable && upto.v2.contains(&n) {
        return Err(EngineError::Other(format!(
            "delta: {table}'s log has no commit {n}, and its checkpoint at version {n} is a V2 one \
             (named with a UUID, or keeping its files in _delta_log/_sidecars/), which this \
             reader does not read, so version {n} cannot be rebuilt"
        )));
    }
    if !reachable {
        return Err(EngineError::Other(format!(
            "delta: {table}'s log has no commit {n} and no checkpoint at it, so version {n} \
             cannot be rebuilt"
        )));
    }
    Ok(())
}

/// What reading one route gave: the plan, or the version of a checkpoint that turned out to keep
/// its files in sidecars, which the route has to go around.
enum Replayed {
    Plan(TablePlan),
    Sidecars(u64),
}

/// Replay `route`: its checkpoint, then its commits. `log_files` holds what was read before it.
fn replay_route(
    log: &dyn DeltaLog,
    table: &str,
    route: &Route,
    mut log_files: Vec<String>,
    stats: bool,
) -> Result<Replayed> {
    let mut state = Replay {
        keep_stats: stats,
        ..Replay::default()
    };
    let mut columns = CHECKPOINT_COLUMNS.to_vec();
    if stats {
        columns.extend_from_slice(CHECKPOINT_STATS_COLUMNS);
    }
    if let Some((version, files)) = &route.checkpoint {
        for name in files {
            let mut sidecars = false;
            let read = log.checkpoint(name, &columns, &mut |batch| {
                each_action(batch, |action| {
                    if action.get("sidecar").is_some() {
                        // Stop reading: this checkpoint's files are elsewhere.
                        sidecars = true;
                        return Err(EngineError::Other("delta: a sidecar checkpoint".into()));
                    }
                    state.apply(&action)
                })
            });
            if sidecars {
                return Ok(Replayed::Sidecars(*version));
            }
            read?;
            log_files.push(name.clone());
        }
    }
    let last = route.commits.last();
    let mut in_commit_ms = None;
    for version in &route.commits {
        let name = format!("{version:020}.json");
        // Listed, then gone: log cleanup ran between the two. Said so, rather than read short.
        let bytes = log.read(&name)?.ok_or_else(|| {
            EngineError::Other(format!(
                "delta: {table}'s commit {version} was removed while the log was read, as log \
                 cleanup does; read the table again"
            ))
        })?;
        let content = std::str::from_utf8(&bytes).map_err(|e| {
            EngineError::Other(format!(
                "delta: {table}'s commit {version} is not UTF-8: {e}"
            ))
        })?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let action: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                EngineError::Other(format!(
                    "delta: bad action json in {table}'s commit {version}: {e}"
                ))
            })?;
            state.apply(&action)?;
        }
        if stats && Some(version) == last {
            in_commit_ms = in_commit_timestamp(&bytes, table, *version)?;
        }
        log_files.push(name);
    }
    let version = route
        .commits
        .last()
        .copied()
        .or(route.checkpoint.as_ref().map(|(v, _)| *v))
        .unwrap_or(0);
    // Only a plan asked for its statistics, which is a description of it ([`describe`]), is dated
    // by its in-commit timestamp. A version rebuilt from its checkpoint alone is dated by its
    // commit, when still there.
    if stats && route.commits.is_empty() {
        if let Some(bytes) = log.read(&format!("{version:020}.json"))? {
            in_commit_ms = in_commit_timestamp(&bytes, table, version)?;
        }
    }
    let mut plan = state.finish(log, table, log_files, version)?;
    plan.in_commit_ms = in_commit_ms;
    Ok(Replayed::Plan(plan))
}

/// Hand each row of a checkpoint batch to `each` as the JSON action a commit would carry: the one
/// non-null column of the row (`add`, `remove`, `metaData`, …) as an object, its null fields left
/// out, and a map such as `partitionValues` as an object of its entries.
fn each_action(
    batch: &RecordBatch,
    mut each: impl FnMut(serde_json::Value) -> Result<()>,
) -> Result<()> {
    let mut writer = arrow_json::WriterBuilder::new()
        .with_explicit_nulls(false)
        .build::<_, arrow_json::writer::LineDelimited>(Vec::new());
    writer.write(batch).map_err(EngineError::arrow)?;
    writer.finish().map_err(EngineError::arrow)?;
    let lines = writer.into_inner();
    for line in lines.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let action = serde_json::from_slice(line)
            .map_err(|e| EngineError::Other(format!("delta: a checkpoint row as json: {e}")))?;
        each(action)?;
    }
    Ok(())
}

/// A live data file, as the replay holds it until the end: keyed by its raw `add.path`.
#[derive(Debug)]
struct Live {
    partition_values: HashMap<String, Option<String>>,
    size: Option<u64>,
    num_records: Option<u64>,
    /// `add.deletionVector` is set: some of the file's rows are deleted without being rewritten.
    deletion_vector: bool,
    /// The file's statistics, as JSON, when the replay keeps them ([`Replay::keep_stats`]).
    stats: Option<String>,
}

/// The table's state as the replay builds it, action by action.
#[derive(Debug, Default)]
struct Replay {
    schema: Option<DeltaSchema>,
    /// The latest `protocol` action wins, exactly like `metaData`. Read for the reader-side
    /// requirements it declares — see `check_reader_requirements`.
    protocol: Option<serde_json::Value>,
    /// Column-mapping mode from the latest `metaData`'s configuration, kept beside the schema
    /// because the refusal it drives needs the *table* config, not the parsed field list.
    column_mapping: String,
    /// Keyed by the RAW (still URL-encoded) `path` string from the log so an `add` and its later
    /// `remove` — which carry the identical string — match exactly. Resolved only when the plan
    /// is built. `BTreeMap` gives a deterministic file order.
    active: BTreeMap<String, Live>,
    committed_ms: Option<i64>,
    /// Keep each live file's statistics ([`Want::stats`]).
    keep_stats: bool,
}

impl Replay {
    fn saw(&mut self, ms: Option<i64>) {
        if let Some(ms) = ms {
            self.committed_ms = Some(self.committed_ms.map_or(ms, |at| at.max(ms)));
        }
    }

    fn apply(&mut self, action: &serde_json::Value) -> Result<()> {
        if let Some(meta) = action.get("metaData") {
            self.schema = Some(parse_metadata(meta)?);
            self.column_mapping = meta
                .get("configuration")
                .and_then(|c| c.get("delta.columnMapping.mode"))
                .and_then(|v| v.as_str())
                .unwrap_or("none")
                .to_ascii_lowercase();
        } else if let Some(proto) = action.get("protocol") {
            self.protocol = Some(proto.clone());
        } else if let Some(add) = action.get("add") {
            self.saw(add.get("modificationTime").and_then(|v| v.as_i64()));
            if let Some(path) = add.get("path").and_then(|v| v.as_str()) {
                // A re-`add` of a path replaces it whole: its partition values, size, rows and
                // deletion vector are the new action's.
                self.active.insert(
                    path.to_string(),
                    Live {
                        partition_values: parse_partition_values(add.get("partitionValues")),
                        size: add.get("size").and_then(|v| v.as_u64()),
                        num_records: num_records(add),
                        deletion_vector: add.get("deletionVector").is_some_and(|v| !v.is_null()),
                        stats: self.keep_stats.then(|| file_stats(add)).flatten(),
                    },
                );
            }
        } else if let Some(remove) = action.get("remove") {
            self.saw(remove.get("deletionTimestamp").and_then(|v| v.as_i64()));
            if let Some(path) = remove.get("path").and_then(|v| v.as_str()) {
                self.active.remove(path);
            }
        } else if let Some(info) = action.get("commitInfo") {
            self.saw(info.get("timestamp").and_then(|v| v.as_i64()));
        }
        // txn / anything else → ignored.
        Ok(())
    }

    fn finish(
        self,
        log: &dyn DeltaLog,
        table: &str,
        log_files: Vec<String>,
        version: u64,
    ) -> Result<TablePlan> {
        let schema = self.schema.ok_or_else(|| EngineError::UnsupportedFormat {
            detail: format!("delta: {table} declares no metaData (schema) in its log"),
        })?;
        // A DV on a tombstoned file is not our problem: only live files count.
        let with_deletion_vectors = self.active.values().filter(|f| f.deletion_vector).count();
        check_reader_requirements(
            table,
            self.protocol.as_ref(),
            &self.column_mapping,
            with_deletion_vectors,
        )?;
        let files = self
            .active
            .into_iter()
            .map(|(raw_path, live)| {
                Ok(AddFile {
                    path: log.data_path(&raw_path)?,
                    partition_values: live.partition_values,
                    size: live.size,
                    num_records: live.num_records,
                    stats: live.stats,
                })
            })
            .collect::<Result<_>>()?;
        Ok(TablePlan {
            files,
            schema,
            committed_ms: self.committed_ms,
            in_commit_ms: None,
            log_files,
            version,
        })
    }
}

/// An `add` action's statistics as JSON: `stats` as written, or a checkpoint's `stats_parsed`
/// struct written out as JSON.
fn file_stats(add: &serde_json::Value) -> Option<String> {
    match add.get("stats") {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        _ => add.get("stats_parsed").map(|s| s.to_string()),
    }
}

/// The rows an `add` action's statistics record, from `stats` (a JSON string) or, in a checkpoint
/// that writes them as a struct, `stats_parsed`. `None` when it records none.
fn num_records(add: &serde_json::Value) -> Option<u64> {
    #[derive(serde::Deserialize)]
    struct Stats {
        #[serde(rename = "numRecords")]
        num_records: Option<u64>,
    }
    add.get("stats")
        .and_then(|s| s.as_str())
        .and_then(|s| serde_json::from_str::<Stats>(s).ok())
        .and_then(|s| s.num_records)
        .or_else(|| add.get("stats_parsed")?.get("numRecords")?.as_u64())
}

/// The version current at instant `ms`: the newest commit made at or before it, by the table's
/// **in-commit timestamps** (`commitInfo.inCommitTimestamp`), which the protocol tells a reader
/// to date a commit by, and which only grow from one commit to the next.
///
/// A table that records none is refused, and so is an instant before they were turned on: the
/// protocol's other clock is each commit file's modification time, which a copy or a checkout
/// resets, and a time travel by it would quietly land on the wrong version. A version is always
/// named by its number (`@<n>`). Two binary searches over the commits still in the log: about
/// 2·log₂ of their number read, each whole, however long the log.
fn version_at(log: &dyn DeltaLog, table: &str, ms: i64) -> Result<u64> {
    let names = log.names(None)?;
    let commits: Vec<u64> = Listing::of(&names, None).commits.into_iter().collect();
    let Some(&newest) = commits.last() else {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "delta: {table} has no commits in _delta_log/, so it is not a Delta table"
            ),
        });
    };
    let mut times: HashMap<u64, Option<i64>> = HashMap::new();
    let mut time = |v: u64| -> Result<Option<i64>> {
        if let Some(t) = times.get(&v) {
            return Ok(*t);
        }
        let t = commit_time(log, table, v)?;
        times.insert(v, t);
        Ok(t)
    };
    let refused = || EngineError::UnsupportedOperation {
        engine: "delta".to_string(),
        op: format!("find the version of {table} current at an instant"),
        hint: "the table does not date its commits with in-commit timestamps \
               (`delta.enableInCommitTimestamps`), and the protocol's other clock, each commit \
               file's modification time, is reset by a copy or a checkout. Name a version \
               (`@<n>`) instead"
            .to_string(),
    };
    if time(newest)?.is_none() {
        return Err(refused());
    }
    // The first commit dated by an in-commit timestamp: they are turned on at one version and
    // kept on, so those before it have none and those after it all do.
    let (mut lo, mut hi) = (0usize, commits.len() - 1);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if time(commits[mid])?.is_some() {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    let first = lo;
    let first_time = time(commits[first])?.unwrap_or(i64::MAX);
    if first_time > ms {
        let detail = format!(
            "delta: {table} has no version known to be current at {}: its oldest commit dated by \
             an in-commit timestamp still in the log, version {}, was made at {}{}",
            crate::snapshot::instant(ms),
            commits[first],
            crate::snapshot::instant(first_time),
            if first > 0 {
                ", and the commits before it are dated only by their files' modification times"
            } else {
                ""
            }
        );
        return Err(EngineError::Other(detail));
    }
    // The newest commit made at or before the instant.
    let (mut lo, mut hi) = (first, commits.len());
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if time(commits[mid])?.is_some_and(|t| t <= ms) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let version = commits[lo];
    // A gap right after it: the commits between were cleaned up, and one of them may be the one.
    // Not when it was made at the instant itself: in-commit timestamps only grow, so any commit
    // after it was made after the instant.
    if commits.get(lo + 1).is_some_and(|next| *next != version + 1) && time(version)? != Some(ms) {
        return Err(EngineError::Other(format!(
            "delta: {table}'s log has no commits between version {version} and the next it holds, \
             so the version current at {} cannot be told",
            crate::snapshot::instant(ms)
        )));
    }
    Ok(version)
}

/// Commit `version`'s in-commit timestamp, or `None` when it records none. The protocol puts the
/// `commitInfo` first in a commit that records one, so only the commit's first action is parsed;
/// the commit is fetched whole, as the log reads every file.
fn commit_time(log: &dyn DeltaLog, table: &str, version: u64) -> Result<Option<i64>> {
    let bytes = log.read(&format!("{version:020}.json"))?.ok_or_else(|| {
        EngineError::Other(format!(
            "delta: {table}'s commit {version} was removed while the log was read, as log \
                 cleanup does; read the table again"
        ))
    })?;
    in_commit_timestamp(&bytes, table, version)
}

/// The in-commit timestamp a commit's `bytes` record in their first action, if any.
fn in_commit_timestamp(bytes: &[u8], table: &str, version: u64) -> Result<Option<i64>> {
    let first = bytes
        .split(|b| *b == b'\n')
        .find(|line| line.iter().any(|b| !b.is_ascii_whitespace()));
    let Some(first) = first else {
        return Ok(None);
    };
    let action: serde_json::Value = serde_json::from_slice(first).map_err(|e| {
        EngineError::Other(format!(
            "delta: bad action json in {table}'s commit {version}: {e}"
        ))
    })?;
    Ok(action
        .get("commitInfo")
        .and_then(|info| info.get("inCommitTimestamp"))
        .and_then(|t| t.as_i64()))
}

/// A file's statistics as its log records them: the parts [`describe`] reads.
#[derive(Debug, Default, serde::Deserialize)]
struct FileStats {
    #[serde(rename = "numRecords")]
    num_records: Option<u64>,
    /// `false` when the bounds are wide: no longer the file's least and greatest values.
    #[serde(rename = "tightBounds")]
    tight_bounds: Option<bool>,
    #[serde(rename = "nullCount", default)]
    null_count: serde_json::Map<String, serde_json::Value>,
    /// Each value kept as written, so a decimal is read as its digits, never through a float.
    #[serde(rename = "minValues", default)]
    min_values: HashMap<String, Box<serde_json::value::RawValue>>,
    #[serde(rename = "maxValues", default)]
    max_values: HashMap<String, Box<serde_json::value::RawValue>>,
}

/// Why the log does not make a column's least and greatest values exact, for a column of type
/// `dt`, or `None` when it does. The protocol has writers cut text to a prefix and timestamps to
/// milliseconds, and says nothing of how a float's bounds treat NaN.
fn inexact_bounds(dt: &DataType) -> Option<&'static str> {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Decimal128(..) => None,
        DataType::Utf8 | DataType::Binary => Some(
            "text and binary columns' minimum and maximum are left out: a Delta writer cuts \
             them to a prefix",
        ),
        DataType::Timestamp(..) => Some(
            "timestamp columns' minimum and maximum are left out: a Delta writer cuts them to \
             milliseconds",
        ),
        DataType::Float32 | DataType::Float64 => Some(
            "float columns' minimum and maximum are left out: the Delta protocol does not say \
             how a writer's bounds treat NaN",
        ),
        // Booleans and nested columns: writers record no bounds for them.
        _ => Some(
            "boolean and nested columns' minimum and maximum are left out: a Delta writer \
             records none for them",
        ),
    }
}

/// One value of a file's bounds, as written in its statistics, as a one-value array of type `dt`.
fn bound_value(raw: &serde_json::value::RawValue, dt: &DataType) -> Option<ArrayRef> {
    let text = raw.get();
    let text = if text.starts_with('"') {
        serde_json::from_str::<String>(text).ok()?
    } else {
        text.to_string()
    };
    typed_value(&text, dt)
}

/// `text` cast to `dt`, as a one-value array: `None` when it is not one of its values.
fn typed_value(text: &str, dt: &DataType) -> Option<ArrayRef> {
    let strings = StringArray::from(vec![text]);
    let cast = arrow_cast::cast::cast_with_options(
        &strings,
        dt,
        &arrow_cast::CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .ok()?;
    cast.is_valid(0).then_some(cast)
}

/// The table at `plan`, described by its log alone, for `diff` by metadata: its columns, its live
/// rows, and each column's null count, minimum and maximum where the log makes them exact. `plan`
/// must have kept its files' statistics ([`Want::stats`]); no data file is read.
///
/// - **Rows** are the files' `numRecords`, when every file records it.
/// - **A data column's** nulls are the files' `nullCount`s, and its minimum and maximum the least
///   and greatest of their bounds, when every file records them and its type makes them exact
///   ([`inexact_bounds`]). A file whose values are all null has none, and needs none.
/// - **A partition column's** values are its files' partition values, exact for every type.
pub(crate) fn describe(plan: &TablePlan) -> Result<crate::snapshot::TableMetadata> {
    use crate::diff::ColumnStats;
    let mut out = crate::snapshot::TableMetadata {
        snapshot: Some(plan.version.to_string()),
        // The version's in-commit timestamp, which `@<instant>` resolves by. A table without them
        // is dated by the latest time its log records up to the version (a commit's timestamp, a
        // file's `add` or `remove` time, as the writer recorded them), never by a file's
        // modification time on the store.
        committed_ms: plan.in_commit_ms.or(plan.committed_ms),
        files: plan.files.len() as u64,
        columns: plan
            .schema
            .fields
            .iter()
            .map(|f| crate::engine::ColumnSchema {
                name: f.name.clone(),
                data_type: format!("{}", f.data_type),
                nullable: f.nullable,
            })
            .collect(),
        rows: plan.files.iter().map(|f| f.num_records).sum(),
        ..Default::default()
    };
    if out.rows.is_none() {
        out.note(
            "a data file records no row count in the log, so the rows are not counted by metadata",
        );
    }
    let stats: Vec<Option<FileStats>> = plan
        .files
        .iter()
        .map(|f| {
            f.stats
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
        })
        .collect();
    if stats.iter().any(Option::is_none) {
        out.note(
            "a data file records no statistics in the log, so its columns' nulls, minimum and \
             maximum are not taken by metadata",
        );
    }
    for field in &plan.schema.fields {
        let dt = &field.data_type;
        let stat = if field.is_partition {
            // Each file's one value of the column, for every row it holds.
            let mut nulls = Some(0u64);
            let mut values = Vec::new();
            for file in &plan.files {
                match file
                    .partition_values
                    .get(&field.name)
                    .and_then(|v| v.as_deref())
                {
                    None => nulls = nulls.zip(file.num_records).map(|(a, b)| a + b),
                    // A file of no rows holds no value of its partition.
                    Some(_) if file.num_records == Some(0) => {}
                    Some(v) => values.push(v),
                }
            }
            values.sort_unstable();
            values.dedup();
            let typed: Option<Vec<ArrayRef>> = values.iter().map(|v| typed_value(v, dt)).collect();
            let (min, max) = match typed {
                Some(typed) => crate::snapshot::extremes(&typed)?,
                None => {
                    out.note(format!(
                        "partition column `{}` holds a value that does not read as {dt}, so its \
                         minimum and maximum are not taken by metadata",
                        field.name
                    ));
                    (None, None)
                }
            };
            ColumnStats { nulls, min, max }
        } else {
            let why = inexact_bounds(dt);
            if let Some(why) = why {
                out.note(why);
            }
            let mut nulls = Some(0u64);
            let mut bounds = why.is_none().then(Vec::new);
            for (file, st) in plan.files.iter().zip(&stats) {
                // Wide bounds (deletion vectors, which a plan refuses anyway) say nothing exact.
                let Some(st) = st.as_ref().filter(|st| st.tight_bounds != Some(false)) else {
                    nulls = None;
                    bounds = None;
                    continue;
                };
                let n = st.null_count.get(&field.name).and_then(|v| v.as_u64());
                if n.is_none() {
                    out.note(
                        "a data file's statistics give no null count for a column, so its nulls \
                         are not taken by metadata",
                    );
                }
                nulls = nulls.zip(n).map(|(a, b)| a + b);
                let records = st.num_records.or(file.num_records);
                if matches!((n, records), (Some(n), Some(r)) if n == r) {
                    continue; // every value null: no bounds, and none needed
                }
                let pair = st
                    .min_values
                    .get(&field.name)
                    .zip(st.max_values.get(&field.name))
                    .and_then(|(lo, hi)| bound_value(lo, dt).zip(bound_value(hi, dt)));
                match (pair, bounds.as_mut()) {
                    (Some((lo, hi)), Some(b)) => b.extend([lo, hi]),
                    (None, Some(_)) => {
                        out.note(
                            "a data file's statistics give no minimum and maximum for a column, so \
                             its bounds are not taken by metadata",
                        );
                        bounds = None;
                    }
                    _ => bounds = None,
                }
            }
            let (min, max) = match bounds {
                Some(b) => crate::snapshot::extremes(&b)?,
                None => (None, None),
            };
            ColumnStats { nulls, min, max }
        };
        out.stats.push((field.name.clone(), stat));
    }
    Ok(out)
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

/// Reorder/pad one data-file batch to the full `target` schema: partition columns become constant
/// arrays from this file's `partitionValues`; data columns are taken by name (cast if the Parquet
/// type differs, null-filled if the file omits the column — schema evolution best-effort).
pub(crate) fn project_file_batch(
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
///   deleted rows, and the DV that marks them lives in a separate file this reader does not
///   read. Reading the file returns those rows as live and the row count still calls the footer
///   sum exact. Proven wrong ⇒ refuse.
/// - **Column mapping** (`delta.columnMapping.mode` = `name`/`id`): the physical Parquet column
///   names are opaque ids, so matching them against `schemaString`'s logical names finds nothing
///   and every data column comes back null-filled. Proven wrong ⇒ refuse.
/// - **Anything else in `readerFeatures`**: named on stderr, not refused. `v2Checkpoint` is
///   known: the replay itself refuses a table that only a V2 checkpoint can rebuild.
fn check_reader_requirements(
    table: &str,
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
                table
            ),
        });
    }
    if !column_mapping.is_empty() && column_mapping != "none" {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "delta: {} uses column mapping (delta.columnMapping.mode = {column_mapping}), \
                 which this reader does not resolve — its data columns would all read as null. \
                 The read is refused rather than answered wrongly.",
                table
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
                table,
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
                // "An empty string for any type translates to a null partition value."
                serde_json::Value::String(s) if s.is_empty() => None,
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

    /// The live rows, by each file's footer: how the tests count a plan, from disk.
    fn row_count(plan: &TablePlan) -> Result<u64> {
        let mut total: u64 = 0;
        for file in &plan.files {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&file.path)?)
                .map_err(EngineError::parquet)?;
            let rows = builder.metadata().file_metadata().num_rows().max(0) as u64;
            total += rows;
        }
        Ok(total)
    }

    /// Read a window of a plan from disk, the way the engine does: what the tests read with.
    fn read_window(
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

    /// The live files carry the sizes their `add`s recorded, and the table changed at the latest
    /// time the log records anywhere: a `commitInfo`, an `add` or a `remove`.
    #[test]
    fn the_plan_counts_live_files_and_the_latest_time_the_log_records() {
        let dir = build_table();
        let plan = plan_uncached(dir.path()).unwrap();
        let sizes: Vec<_> = plan.files.iter().map(|f| f.size).collect();
        assert_eq!(sizes, vec![Some(1), Some(1)]);
        assert_eq!(plan.committed_ms, Some(0));
        let commit1 = format!(
            "{}\n{}\n",
            serde_json::json!({"remove": {
                "path": "region=us/part-0.parquet", "deletionTimestamp": 5000, "dataChange": true
            }}),
            serde_json::json!({"commitInfo": {"timestamp": 4000}})
        );
        fs::write(
            dir.path()
                .join("_delta_log")
                .join("00000000000000000001.json"),
            commit1,
        )
        .unwrap();
        let plan = plan_uncached(dir.path()).unwrap();
        assert_eq!(plan.files.len(), 1);
        assert_eq!(plan.files[0].size, Some(1));
        assert_eq!(plan.committed_ms, Some(5000));
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

    /// The table `tests/data/delta_checkpointed.py` wrote with delta-rs: a checkpoint at version
    /// 2, commit 3 after it, and commits 0–2 cleaned up. Live ids 1, 3, 4, 5 and 6.
    fn checkpointed() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/delta_checkpointed")
    }

    /// A copy of [`checkpointed`], to change.
    fn checkpointed_copy() -> tempfile::TempDir {
        fn copy_dir(from: &Path, to: &Path) {
            for entry in fs::read_dir(from).unwrap() {
                let entry = entry.unwrap();
                let target = to.join(entry.file_name());
                if entry.file_type().unwrap().is_dir() {
                    fs::create_dir_all(&target).unwrap();
                    copy_dir(&entry.path(), &target);
                } else {
                    fs::copy(entry.path(), &target).unwrap();
                }
            }
        }
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&checkpointed(), dir.path());
        dir
    }

    const CHECKPOINT: &str = "00000000000000000002.checkpoint.parquet";

    fn checkpointed_rows() -> Vec<(i64, String)> {
        [
            (1, "eu"),
            (3, "eu"),
            (4, "us west:%x"),
            (5, "us west:%x"),
            (6, "eu"),
        ]
        .into_iter()
        .map(|(id, region)| (id, region.to_string()))
        .collect()
    }

    fn ids(plan: &TablePlan) -> Vec<i64> {
        rows(plan).into_iter().map(|(id, _)| id).collect()
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// The fixture's commits 0–2 are gone. Its checkpoint holds their state: the files they added,
    /// less the one the delete rewrote, which is still on disk. The partition value with a space,
    /// a colon and a percent sign is the checkpoint's map entry, decoded once in the file's path.
    #[test]
    fn a_table_whose_early_commits_were_cleaned_up_reads_from_its_checkpoint() {
        let plan = plan_uncached(&checkpointed()).unwrap();
        assert_eq!(rows(&plan), checkpointed_rows());
        assert_eq!(plan.files.len(), 3);
        assert!(plan.files.iter().all(|f| f.path.is_file()), "{plan:#?}");
        assert!(
            !plan
                .files
                .iter()
                .any(|f| f.path.to_string_lossy().contains("6a8c34f0")),
            "the file the delete removed is not read"
        );
        assert_eq!(plan.schema.partition_columns, ["region"]);
        // The rows each file holds are its statistics', so nothing need open a footer to count.
        let mut records: Vec<_> = plan.files.iter().map(|f| f.num_records).collect();
        records.sort();
        assert_eq!(records, [Some(1), Some(2), Some(2)]);
        assert_eq!(row_count(&plan).unwrap(), 5);
        assert_eq!(
            plan.log_files,
            names(&["_last_checkpoint", CHECKPOINT, "00000000000000000003.json"])
        );
        // Commit 3's time: the latest the log records, checkpoint included.
        assert_eq!(plan.committed_ms, Some(1_795_703_408_135));
    }

    #[test]
    fn a_checkpoint_with_no_commits_after_it_is_the_whole_table() {
        let dir = checkpointed_copy();
        fs::remove_file(dir.path().join("_delta_log/00000000000000000003.json")).unwrap();
        let plan = plan_uncached(dir.path()).unwrap();
        assert_eq!(ids(&plan), [1, 3, 4, 5]);
        assert_eq!(plan.log_files, names(&["_last_checkpoint", CHECKPOINT]));
    }

    /// `_last_checkpoint` is a hint: wrong, or not JSON at all, it changes nothing on disk, where
    /// the whole log is listed whatever it says.
    #[test]
    fn a_wrong_last_checkpoint_is_no_harm() {
        let dir = checkpointed_copy();
        let hint = dir.path().join("_delta_log/_last_checkpoint");
        for wrong in ["{\"version\":99,\"size\":1}", "not json", ""] {
            fs::write(&hint, wrong).unwrap();
            assert_eq!(
                rows(&plan_uncached(dir.path()).unwrap()),
                checkpointed_rows()
            );
        }
        fs::remove_file(&hint).unwrap();
        let plan = plan_uncached(dir.path()).unwrap();
        assert_eq!(rows(&plan), checkpointed_rows());
        assert_eq!(plan.log_files[0], CHECKPOINT, "no hint to read");
    }

    /// Rewrite the fixture's checkpoint as `parts` parts, splitting its rows between them.
    fn split_checkpoint(log: &Path, parts: usize) -> Vec<String> {
        let classic = log.join(CHECKPOINT);
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&classic).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let all = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
        let per = all.num_rows().div_ceil(parts);
        let mut written = Vec::new();
        for part in 0..parts {
            let name = format!(
                "00000000000000000002.checkpoint.{:010}.{parts:010}.parquet",
                part + 1
            );
            let start = (part * per).min(all.num_rows());
            let slice = all.slice(start, per.min(all.num_rows() - start));
            let mut writer =
                ArrowWriter::try_new(File::create(log.join(&name)).unwrap(), all.schema(), None)
                    .unwrap();
            writer.write(&slice).unwrap();
            writer.close().unwrap();
            written.push(name);
        }
        fs::remove_file(classic).unwrap();
        written
    }

    #[test]
    fn a_multi_part_checkpoint_is_read_when_every_part_is_there() {
        let dir = checkpointed_copy();
        let log = dir.path().join("_delta_log");
        let parts = split_checkpoint(&log, 2);
        let plan = plan_uncached(dir.path()).unwrap();
        assert_eq!(rows(&plan), checkpointed_rows());
        assert_eq!(plan.log_files[1..3], parts[..]);

        // Half a checkpoint is none: with commits 0–2 gone, the table cannot be rebuilt.
        fs::remove_file(log.join(&parts[1])).unwrap();
        let err = plan_uncached(dir.path()).unwrap_err();
        assert!(err.to_string().contains("no commit 0"), "{err}");
    }

    #[test]
    fn a_commit_missing_after_the_checkpoint_is_an_error_not_a_short_read() {
        let dir = checkpointed_copy();
        let log = dir.path().join("_delta_log");
        // Commit 5 with no commit 4: whatever 4 did is in no file left.
        fs::copy(
            log.join("00000000000000000003.json"),
            log.join("00000000000000000005.json"),
        )
        .unwrap();
        let err = plan_uncached(dir.path()).unwrap_err();
        assert!(
            matches!(err, EngineError::UnsupportedFormat { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("no commit 4"), "{err}");
    }

    #[test]
    fn a_table_only_a_v2_checkpoint_can_rebuild_is_refused_naming_it() {
        let dir = checkpointed_copy();
        let log = dir.path().join("_delta_log");
        fs::rename(
            log.join(CHECKPOINT),
            log.join(
                "00000000000000000002.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet",
            ),
        )
        .unwrap();
        let err = plan_uncached(dir.path()).unwrap_err().to_string();
        assert!(err.contains("no commit 0"), "{err}");
        assert!(err.contains("V2 checkpoint"), "{err}");
        assert!(err.contains("at version 2"), "{err}");
    }

    /// Write `log/name` as a checkpoint whose one row is a `sidecar` action: a V2 checkpoint
    /// under a classic name, keeping the table's files in `_delta_log/_sidecars/`.
    fn write_sidecar_checkpoint(log: &Path, name: &str) {
        let path = Field::new("path", DataType::Utf8, false);
        let sidecar = arrow_array::StructArray::from(vec![(
            Arc::new(path),
            Arc::new(StringArray::from(vec!["0001.parquet"])) as ArrayRef,
        )]);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "sidecar",
            sidecar.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(sidecar)]).unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(log.join(name)).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// A V2 checkpoint may carry a classic name. Its rows then point at sidecar files rather than
    /// listing the table's files, and reading it as a classic one would find a table with none.
    /// With the commits before it cleaned up, it is the only way to rebuild the table: refused.
    #[test]
    fn a_checkpoint_that_keeps_its_files_in_sidecars_is_refused_when_it_is_the_only_way() {
        let dir = checkpointed_copy();
        write_sidecar_checkpoint(&dir.path().join("_delta_log"), CHECKPOINT);
        let err = plan_uncached(dir.path()).unwrap_err().to_string();
        assert!(err.contains("no commit 0"), "{err}");
        assert!(err.contains("_sidecars"), "{err}");
        assert!(err.contains("at version 2"), "{err}");
    }

    /// The same checkpoint over a log whose commits are all still there is passed over: the
    /// table is rebuilt from its commits, as it would be with no checkpoint at all.
    #[test]
    fn a_sidecar_checkpoint_is_passed_over_when_the_commits_are_all_there() {
        let dir = build_table();
        let log = dir.path().join("_delta_log");
        write_sidecar_checkpoint(&log, "00000000000000000000.checkpoint.parquet");
        let plan = plan_uncached(dir.path()).unwrap();
        assert_eq!(ids(&plan), [1, 2, 3]);
        assert_eq!(plan.log_files, names(&["00000000000000000000.json"]));
    }

    /// A log cleaned up to a checkpoint this reader cannot read is named for what it holds, not
    /// called "not a Delta table".
    #[test]
    fn a_log_with_only_unread_checkpoints_says_what_they_are() {
        let route = |list: &[&str]| {
            Listing::of(&names(list), None)
                .route("t")
                .unwrap_err()
                .to_string()
        };
        let v2 = route(&[
            "00000000000000000002.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet",
        ]);
        assert!(!v2.contains("not a Delta table"), "{v2}");
        assert!(v2.contains("V2 checkpoint"), "{v2}");
        assert!(v2.contains("at version 2"), "{v2}");
        let partial = route(&["00000000000000000004.checkpoint.0000000001.0000000002.parquet"]);
        assert!(!partial.contains("not a Delta table"), "{partial}");
        assert!(
            partial.contains("checkpoint at version 4 is missing a part"),
            "{partial}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn plan_with_root_confines_the_checkpoint_too() {
        // A checkpoint names files as a commit does, so one from outside the root is refused.
        let outside = checkpointed_copy();
        let dir = checkpointed_copy();
        let in_log = dir.path().join("_delta_log").join(CHECKPOINT);
        let real = outside.path().join("_delta_log").join(CHECKPOINT);
        fs::remove_file(&in_log).unwrap();
        std::os::unix::fs::symlink(&real, &in_log).unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        assert!(matches!(
            plan_with_root(dir.path(), Some(&root)).unwrap_err(),
            EngineError::Forbidden(_)
        ));
        fs::remove_file(&in_log).unwrap();
        fs::copy(&real, &in_log).unwrap();
        assert_eq!(
            plan_with_root(dir.path(), Some(&root)).unwrap().files.len(),
            3
        );
    }

    #[test]
    fn log_file_names_are_told_apart() {
        let parse = parse_log_name;
        assert_eq!(parse("00000000000000000007.json"), Some(LogName::Commit(7)));
        assert_eq!(
            parse("00000000000000000010.checkpoint.parquet"),
            Some(LogName::Checkpoint(10))
        );
        assert_eq!(
            parse("00000000000000000010.checkpoint.0000000002.0000000003.parquet"),
            Some(LogName::Part {
                version: 10,
                part: 2,
                parts: 3
            })
        );
        for v2 in [
            "00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json",
            "00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet",
        ] {
            assert_eq!(parse(v2), Some(LogName::V2(10)), "{v2}");
        }
        for other in [
            "_last_checkpoint",
            "_sidecars",
            "00000000000000000007.crc",
            "00000000000000000004.00000000000000000007.compacted.json",
            "00000000000000000010.checkpoint.0000000004.0000000003.parquet",
            "00000000000000000007.json.tmp",
            "0000000000000000000x.json",
            "7.json",
            "ééééééééééé.json",
        ] {
            assert_eq!(parse(other), None, "{other}");
        }
    }

    #[test]
    fn the_newest_complete_checkpoint_is_the_start() {
        let route = |list: &[&str]| Listing::of(&names(list), None).route("t");
        // A newer part set missing a part is passed over for the older complete checkpoint.
        assert_eq!(
            route(&[
                "00000000000000000005.checkpoint.parquet",
                "00000000000000000006.json",
                "00000000000000000007.json",
                "00000000000000000007.checkpoint.0000000002.0000000002.parquet",
            ])
            .unwrap(),
            Route {
                checkpoint: Some((5, names(&["00000000000000000005.checkpoint.parquet"]))),
                commits: vec![6, 7],
            }
        );
        // A complete one is read whole, its parts in order; the single file, when a version has
        // both, as the one request.
        assert_eq!(
            route(&[
                "00000000000000000007.checkpoint.0000000002.0000000002.parquet",
                "00000000000000000007.checkpoint.0000000001.0000000002.parquet",
            ])
            .unwrap()
            .checkpoint,
            Some((
                7,
                names(&[
                    "00000000000000000007.checkpoint.0000000001.0000000002.parquet",
                    "00000000000000000007.checkpoint.0000000002.0000000002.parquet",
                ])
            ))
        );
        assert_eq!(
            route(&[
                "00000000000000000007.checkpoint.0000000001.0000000001.parquet",
                "00000000000000000007.checkpoint.parquet",
            ])
            .unwrap()
            .checkpoint,
            Some((7, names(&["00000000000000000007.checkpoint.parquet"])))
        );
        // No checkpoint: from version 0.
        assert_eq!(
            route(&["00000000000000000001.json", "00000000000000000000.json"])
                .unwrap()
                .commits,
            [0, 1]
        );
        assert!(route(&["_last_checkpoint"])
            .unwrap_err()
            .to_string()
            .contains("not a Delta table"));
    }

    #[test]
    fn a_files_rows_are_its_statistics_whichever_way_they_are_written() {
        let add = |v: serde_json::Value| num_records(&v);
        assert_eq!(
            add(serde_json::json!({"stats": "{\"numRecords\":4}"})),
            Some(4)
        );
        assert_eq!(
            add(serde_json::json!({"stats_parsed": {"numRecords": 5}})),
            Some(5)
        );
        assert_eq!(
            add(serde_json::json!({"stats": "{\"minValues\":{}}"})),
            None
        );
        assert_eq!(add(serde_json::json!({"stats": "not json"})), None);
        assert_eq!(add(serde_json::json!({})), None);
    }

    /// Replace the copy's checkpoint with one holding `actions`, in the schema delta-rs wrote the
    /// fixture's in: what a checkpoint carrying them would look like.
    fn write_checkpoint(table: &Path, actions: &[serde_json::Value]) {
        let fixture = checkpointed().join("_delta_log").join(CHECKPOINT);
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(fixture).unwrap())
            .unwrap()
            .schema()
            .clone();
        let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
            .build_decoder()
            .unwrap();
        let lines: String = actions.iter().map(|a| format!("{a}\n")).collect();
        decoder.decode(lines.as_bytes()).unwrap();
        let batch = decoder.flush().unwrap().unwrap();
        let file = File::create(table.join("_delta_log").join(CHECKPOINT)).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// The fixture's state at version 2 as checkpoint actions, with `protocol`, the `metaData`
    /// configuration and the first file's `add` changed as given.
    fn checkpoint_actions(
        protocol: serde_json::Value,
        configuration: serde_json::Value,
        add_extra: serde_json::Value,
    ) -> Vec<serde_json::Value> {
        let schema_string = serde_json::json!({"type": "struct", "fields": [
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
            {"name": "name", "type": "string", "nullable": true, "metadata": {}},
            {"name": "region", "type": "string", "nullable": true, "metadata": {}}
        ]})
        .to_string();
        let mut add = serde_json::json!({
            "path": "region=eu/part-00000-aa8efbac-5875-4706-a888-b50559065548-c000.snappy.parquet",
            "partitionValues": {"region": "eu"}, "size": 759, "modificationTime": 0,
            "dataChange": true, "stats": "{\"numRecords\":2}"
        });
        for (k, v) in add_extra.as_object().unwrap() {
            add[k.as_str()] = v.clone();
        }
        vec![
            serde_json::json!({"protocol": protocol}),
            serde_json::json!({"metaData": {
                "id": "t", "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string, "partitionColumns": ["region"],
                "configuration": configuration, "createdTime": 0
            }}),
            serde_json::json!({ "add": add }),
        ]
    }

    /// A deletion vector reaches the replay through a checkpoint as surely as through a commit,
    /// and is refused the same: its rows would read as live.
    #[test]
    fn a_deletion_vector_in_a_checkpoint_is_refused() {
        let dir = checkpointed_copy();
        write_checkpoint(
            dir.path(),
            &checkpoint_actions(
                serde_json::json!({
                    "minReaderVersion": 3, "minWriterVersion": 7,
                    "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]
                }),
                serde_json::json!({}),
                serde_json::json!({"deletionVector": {
                    "storageType": "u", "pathOrInlineDv": "ab^-aqEH.-t@S}K{vb[*k^",
                    "offset": 1, "sizeInBytes": 36, "cardinality": 1
                }}),
            ),
        );
        let err = plan_uncached(dir.path()).unwrap_err();
        assert!(
            matches!(err, EngineError::UnsupportedFormat { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("deletion vector"), "{err}");

        // The same checkpoint without the vector reads: the refusal is the vector's.
        write_checkpoint(
            dir.path(),
            &checkpoint_actions(
                serde_json::json!({"minReaderVersion": 1, "minWriterVersion": 2}),
                serde_json::json!({}),
                serde_json::json!({}),
            ),
        );
        assert_eq!(ids(&plan_uncached(dir.path()).unwrap()), [1, 3, 6]);
    }

    #[test]
    fn column_mapping_in_a_checkpoint_is_refused() {
        let dir = checkpointed_copy();
        write_checkpoint(
            dir.path(),
            &checkpoint_actions(
                serde_json::json!({
                    "minReaderVersion": 2, "minWriterVersion": 5
                }),
                serde_json::json!({"delta.columnMapping.mode": "name"}),
                serde_json::json!({}),
            ),
        );
        let err = plan_uncached(dir.path()).unwrap_err();
        assert!(err.to_string().contains("column mapping"), "{err}");
    }

    fn at_version(dir: &Path, version: u64) -> Result<TablePlan> {
        plan_at(
            dir,
            &Want {
                version: Some(version),
                ..Want::default()
            },
        )
    }

    /// The fixture at each version it still holds: 2, from its checkpoint, and 3, the newest.
    /// Version 1 was cleaned up and version 4 is not written yet, and each says so.
    #[test]
    fn a_plan_stands_at_any_version_the_log_still_holds() {
        let two = at_version(&checkpointed(), 2).unwrap();
        assert_eq!(ids(&two), [1, 3, 4, 5]);
        assert_eq!(two.version, 2);
        assert_eq!(two.log_files, names(&["_last_checkpoint", CHECKPOINT]));
        let three = at_version(&checkpointed(), 3).unwrap();
        assert_eq!(ids(&three), [1, 3, 4, 5, 6]);
        assert_eq!(three.version, 3);
        assert_eq!(plan_uncached(&checkpointed()).unwrap().version, 3);

        let gone = at_version(&checkpointed(), 1).unwrap_err().to_string();
        assert!(gone.contains("version 1 is no longer in its log"), "{gone}");
        assert!(gone.contains("oldest file is version 2"), "{gone}");
        let ahead = at_version(&checkpointed(), 4).unwrap_err().to_string();
        assert!(
            ahead.contains("no version 4: its log ends at version 3"),
            "{ahead}"
        );

        // A version inside the log whose commit is gone, with no checkpoint at it, is a gap: said
        // as one, not as cleaned up.
        let dir = build_table();
        let log = dir.path().join("_delta_log");
        fs::write(log.join("00000000000000000002.json"), "").unwrap();
        let gap = at_version(dir.path(), 1).unwrap_err().to_string();
        assert!(
            gap.contains("has no commit 1 and no checkpoint at it"),
            "{gap}"
        );
    }

    /// A version before the newest checkpoint replays from an older one, or from version 0: the
    /// newest checkpoint is past it.
    #[test]
    fn a_version_before_the_newest_checkpoint_replays_without_it() {
        let dir = build_table();
        let log = dir.path().join("_delta_log");
        let remove = serde_json::json!({"remove": {
            "path": "region=us/part-0.parquet", "deletionTimestamp": 5, "dataChange": true
        }});
        fs::write(log.join("00000000000000000001.json"), format!("{remove}\n")).unwrap();
        // A checkpoint at 1, and a hint naming it: no start for version 0.
        let at_one = plan_uncached(dir.path()).unwrap();
        assert_eq!(ids(&at_one), [3]);
        fs::copy(
            checkpointed().join("_delta_log").join(CHECKPOINT),
            log.join("00000000000000000001.checkpoint.parquet"),
        )
        .unwrap();
        fs::write(log.join("_last_checkpoint"), r#"{"version":1,"size":5}"#).unwrap();
        let zero = at_version(dir.path(), 0).unwrap();
        assert_eq!(ids(&zero), [1, 2, 3]);
        assert_eq!(
            zero.log_files,
            names(&["_last_checkpoint", "00000000000000000000.json"])
        );
    }

    fn described(plan: &TablePlan) -> crate::snapshot::TableMetadata {
        describe(plan).unwrap()
    }

    fn stat(meta: &crate::snapshot::TableMetadata, column: &str) -> crate::diff::ColumnStats {
        meta.stats
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, s)| s.clone())
            .unwrap()
    }

    /// The fixture described by its log at each version: rows from `numRecords`, the integer
    /// column's bounds from the files' statistics, the partition column's from the files'
    /// partition values. The text column's bounds are left out, with a note: a writer cuts them.
    #[test]
    fn a_version_is_described_by_its_log_alone() {
        let want = |version| Want {
            version: Some(version),
            stats: true,
            instant_ms: None,
        };
        let three = described(&plan_at(&checkpointed(), &want(3)).unwrap());
        assert_eq!(three.rows, Some(5));
        assert_eq!(three.files, 3);
        assert_eq!(three.snapshot.as_deref(), Some("3"));
        assert_eq!(three.committed_ms, Some(1_795_703_408_135));
        let id = stat(&three, "id");
        assert_eq!(
            (id.nulls, id.min.as_deref(), id.max.as_deref()),
            (Some(0), Some("1"), Some("6"))
        );
        let name = stat(&three, "name");
        assert_eq!((name.nulls, name.min, name.max), (Some(1), None, None));
        assert!(
            three.notes.iter().any(|n| n.contains("text and binary")),
            "{:?}",
            three.notes
        );
        let region = stat(&three, "region");
        assert_eq!(
            (region.nulls, region.min.as_deref(), region.max.as_deref()),
            (Some(0), Some("eu"), Some("us west:%x"))
        );
        let columns: Vec<_> = three.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(columns, ["id", "name", "region"]);

        let two = described(&plan_at(&checkpointed(), &want(2)).unwrap());
        assert_eq!(two.rows, Some(4));
        assert_eq!(stat(&two, "id").max.as_deref(), Some("5"));

        // A plan not asked for its files' statistics describes no column: each is unknown.
        let bare = described(&plan_uncached(&checkpointed()).unwrap());
        assert_eq!(stat(&bare, "id").min, None);
        assert_eq!(stat(&bare, "id").nulls, None);
    }

    /// A table whose commits record in-commit timestamps, at 1000, 2000 and 3000 ms.
    fn table_with_in_commit_timestamps(times: &[Option<i64>]) -> tempfile::TempDir {
        let dir = build_table();
        let log = dir.path().join("_delta_log");
        let first = fs::read_to_string(log.join("00000000000000000000.json")).unwrap();
        for (version, at) in times.iter().enumerate() {
            let info = match at {
                Some(t) => {
                    serde_json::json!({"commitInfo": {"timestamp": 1, "inCommitTimestamp": t}})
                }
                None => serde_json::json!({"commitInfo": {"timestamp": 1}}),
            };
            let body = if version == 0 {
                first.clone()
            } else {
                String::new()
            };
            fs::write(
                log.join(format!("{version:020}.json")),
                format!("{info}\n{body}"),
            )
            .unwrap();
        }
        dir
    }

    #[test]
    fn an_instant_names_the_newest_commit_made_by_then() {
        let dir = table_with_in_commit_timestamps(&[Some(1000), Some(2000), Some(3000)]);
        let log = LocalLog {
            table_dir: dir.path(),
        };
        assert_eq!(version_at(&log, "t", 1000).unwrap(), 0);
        assert_eq!(version_at(&log, "t", 2500).unwrap(), 1);
        assert_eq!(version_at(&log, "t", 3000).unwrap(), 2);
        assert_eq!(version_at(&log, "t", 9000).unwrap(), 2);
        let before = version_at(&log, "t", 999).unwrap_err().to_string();
        assert!(before.contains("version 0, was made at"), "{before}");
        // Through a plan, as `@<instant>` asks.
        let plan = plan_at(
            dir.path(),
            &Want {
                instant_ms: Some(2500),
                ..Want::default()
            },
        )
        .unwrap();
        assert_eq!(plan.version, 1);

        // Turned on at version 1: an instant only version 0 could answer is refused, since its
        // commit is dated by nothing but its file's modification time.
        let dir = table_with_in_commit_timestamps(&[None, Some(2000), Some(3000)]);
        let log = LocalLog {
            table_dir: dir.path(),
        };
        assert_eq!(version_at(&log, "t", 2000).unwrap(), 1);
        let early = version_at(&log, "t", 1500).unwrap_err().to_string();
        assert!(early.contains("modification times"), "{early}");
    }

    /// The fixture's writer dates no commit by an in-commit timestamp: `@<instant>` is refused,
    /// naming the clock the protocol would fall back to, and what to name instead.
    #[test]
    fn an_instant_is_refused_for_a_table_without_in_commit_timestamps() {
        let err = plan_at(
            &checkpointed(),
            &Want {
                instant_ms: Some(1_795_703_408_135),
                ..Want::default()
            },
        )
        .unwrap_err();
        assert!(
            matches!(err, EngineError::UnsupportedOperation { .. }),
            "{err:?}"
        );
        let err = err.to_string();
        assert!(err.contains("in-commit timestamps"), "{err}");
        assert!(err.contains("`@<n>`"), "{err}");
    }

    /// A directory read as a store reads its log: a listing from a version leaves out every file
    /// of an older one.
    struct ListsFrom<'a>(LocalLog<'a>);

    impl DeltaLog for ListsFrom<'_> {
        fn table(&self) -> String {
            self.0.table()
        }
        fn names(&self, from: Option<u64>) -> Result<Vec<String>> {
            let all = self.0.names(None)?;
            Ok(all
                .into_iter()
                .filter(|n| from.is_none_or(|v| parse_log_name(n).is_none_or(|l| l.version() >= v)))
                .collect())
        }
        fn lists_from(&self) -> bool {
            true
        }
        fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
            self.0.read(name)
        }
        fn checkpoint(
            &self,
            name: &str,
            columns: &[&str],
            each: &mut dyn FnMut(&RecordBatch) -> Result<()>,
        ) -> Result<()> {
            self.0.checkpoint(name, columns, each)
        }
        fn data_path(&self, raw: &str) -> Result<PathBuf> {
            self.0.data_path(raw)
        }
    }

    /// The hint names a checkpoint that keeps its files in sidecars, and every commit is still
    /// there: a store, which lists from the hint, lists the whole log to route around it.
    #[test]
    fn a_sidecar_checkpoint_the_hint_names_is_routed_around_in_a_store_too() {
        let dir = table_with_in_commit_timestamps(&[Some(1000), Some(2000), Some(3000)]);
        let log_dir = dir.path().join("_delta_log");
        write_sidecar_checkpoint(&log_dir, "00000000000000000002.checkpoint.parquet");
        fs::write(log_dir.join(LAST_CHECKPOINT), r#"{"version":2,"size":1}"#).unwrap();
        let log = ListsFrom(LocalLog {
            table_dir: dir.path(),
        });
        for want in [
            Want::default(),
            Want {
                version: Some(2),
                ..Want::default()
            },
        ] {
            let plan = replay(&log, &want).unwrap();
            assert_eq!(plan.version, 2, "{want:?}");
            assert_eq!(ids(&plan), [1, 2, 3], "{want:?}");
        }
    }

    /// A version whose commit is gone and whose checkpoint keeps its files in sidecars cannot be
    /// rebuilt: refused, rather than answered with the version before it.
    #[test]
    fn a_version_only_an_unread_checkpoint_holds_is_refused_not_the_one_before() {
        let dir = table_with_in_commit_timestamps(&[Some(1000), Some(2000), Some(3000)]);
        let log_dir = dir.path().join("_delta_log");
        write_sidecar_checkpoint(&log_dir, "00000000000000000001.checkpoint.parquet");
        fs::remove_file(log_dir.join("00000000000000000001.json")).unwrap();
        let err = plan_at(
            dir.path(),
            &Want {
                version: Some(1),
                ..Want::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("version 1 cannot be rebuilt"), "{err}");
        assert!(err.contains("V2"), "{err}");
    }

    /// With a commit cleaned up after it, a commit is still the answer for the instant it was made
    /// at, since in-commit timestamps only grow; an instant after it is refused.
    #[test]
    fn an_instant_a_retained_commit_was_made_at_is_answered_across_a_gap() {
        let dir = table_with_in_commit_timestamps(&[Some(1000), Some(2000), Some(3000)]);
        fs::remove_file(dir.path().join("_delta_log/00000000000000000001.json")).unwrap();
        let log = LocalLog {
            table_dir: dir.path(),
        };
        assert_eq!(version_at(&log, "t", 1000).unwrap(), 0);
        assert_eq!(version_at(&log, "t", 3000).unwrap(), 2);
        let gap = version_at(&log, "t", 1500).unwrap_err().to_string();
        assert!(gap.contains("no commits between version 0"), "{gap}");
    }

    /// A version is dated by its own in-commit timestamp, not by the latest time its log records:
    /// here each commit's `commitInfo.timestamp` is 1.
    #[test]
    fn a_version_is_dated_by_its_in_commit_timestamp() {
        let dir = table_with_in_commit_timestamps(&[Some(1000), Some(2000), Some(3000)]);
        let at = |version| {
            let want = Want {
                version: Some(version),
                stats: true,
                ..Want::default()
            };
            described(&plan_at(dir.path(), &want).unwrap()).committed_ms
        };
        assert_eq!(at(1), Some(2000));
        assert_eq!(at(2), Some(3000));
        // A table without them keeps the latest time its log records.
        let plain = described(&plan_uncached(&checkpointed()).unwrap());
        assert_eq!(plain.committed_ms, Some(1_795_703_408_135));
    }

    /// Every column whose bounds the log does not make exact says why, booleans and nested
    /// columns among them.
    #[test]
    fn every_inexact_bound_has_a_reason() {
        for dt in [
            DataType::Boolean,
            DataType::Utf8,
            DataType::Float64,
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        ] {
            assert!(inexact_bounds(&dt).is_some_and(|w| !w.is_empty()), "{dt}");
        }
        assert_eq!(inexact_bounds(&DataType::Int64), None);
    }

    /// "An empty string for any type translates to a null partition value."
    #[test]
    fn an_empty_partition_value_is_null() {
        let values = parse_partition_values(Some(&serde_json::json!({
            "region": "", "day": "2026-10-01", "gone": null
        })));
        assert_eq!(values["region"], None);
        assert_eq!(values["day"].as_deref(), Some("2026-10-01"));
        assert_eq!(values["gone"], None);
    }

    /// A file whose statistics count its rows but give nothing for a column leaves that column's
    /// nulls and bounds unknown, and the report says why.
    #[test]
    fn a_column_a_files_statistics_leave_out_is_unknown_and_said_so() {
        let dir = build_table_with(
            None,
            serde_json::json!({}),
            serde_json::json!({"stats": "{\"numRecords\":2}"}),
        );
        let want = Want {
            stats: true,
            ..Want::default()
        };
        let meta = described(&plan_at(dir.path(), &want).unwrap());
        assert_eq!(meta.rows, Some(2));
        let id = stat(&meta, "id");
        assert_eq!((id.nulls, id.min, id.max), (None, None, None));
        let notes = meta.notes.join("\n");
        assert!(notes.contains("no null count"), "{notes}");
        assert!(notes.contains("no minimum and maximum"), "{notes}");
    }
}
