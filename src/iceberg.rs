//! Self-contained Apache Iceberg reader (`--features iceberg`).
//!
//! Deliberately does **not** use `iceberg-datafusion` — that crate is pinned to DataFusion 52
//! (arrow 55) while Lakeleto rides DataFusion 54 / arrow 58 (in lockstep with sibling modules). A
//! DataFusion-52 `TableProvider` can't register into a DataFusion-54 `SessionContext`, so instead
//! we resolve the table's current-snapshot Parquet data files ourselves and read them with the
//! existing arrow-58 Parquet reader — on-thesis for #25 ("the engine is a commodity; just read
//! the Parquet the table points to").
//!
//! Path: `<table>/metadata/` → current `*.metadata.json` (JSON) → current snapshot's Avro
//! **manifest-list** → each **manifest** (Avro) → the `data_file.file_path` of live Parquet data
//! files, plus any **merge-on-read positional delete files** (`content = 1`) whose `(file_path,
//! pos)` rows mark physical positions to drop. Manifests compressed with deflate / snappy / zstd
//! are read transparently. Handles append-only / copy-on-write **and** merge-on-read (positional
//! `content = 1` + **equality** `content = 2`, the latter with sequence-number semantics) v1 & v2
//! tables, plus **schema evolution** (files unified to the current schema by field-id: rename /
//! add-null-fill / drop / type-promotion) and **statistics/partition pruning** (a filtered scan
//! skips data files whose manifest bounds — or, for an equality filter, whose bucket/truncate/
//! identity **partition** value — prove they can't match; conservatively, and consistent with
//! Arrow's `total_cmp` float order, so a hidden NaN never causes a wrong skip).
//!
//! ## Format versions, and the one case this reader refuses
//!
//! v1 and v2 are read in full. **v3** (GA across Snowflake / Databricks / AWS during 2026) is read
//! too, with two carve-outs, because its additions land in different places:
//!
//! - **Deletion vectors are refused.** v3 replaces positional delete *files* with Puffin deletion
//!   vectors, which arrive as delete-manifest entries whose `file_format` is `puffin`. This reader
//!   parses Parquet delete files only, so a DV would be dropped — and dropping a delete makes the
//!   deleted rows reappear as live rows while the row count is still reported as exact. An
//!   under-report is a partial answer; this would be a *wrong* answer, so [`plan`] returns
//!   [`EngineError::UnsupportedFormat`] instead of a plan. (Contrast the non-Parquet **data** file
//!   case, which under-reports and is only warned about.)
//! - **Row lineage and the VARIANT / GEOMETRY types are not modelled**, and a v3 table is noted on
//!   stderr for that reason. It is not refused: a v3 table using none of them reads exactly right,
//!   and refusing the version wholesale would break tables that work today.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::Reader;
use arrow_array::{
    new_null_array, Array, ArrayRef, Date32Array, Decimal128Array, Float64Array, RecordBatch,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::engine::{FilterOp, FilterSpec};
use crate::error::{EngineError, Result};

/// Per-data-file statistics from its manifest entry, used to skip files that cannot contain a
/// filter's matching rows (statistics/partition pruning). All maps are keyed by **field-id**;
/// any of them may be absent (writers don't always emit stats), in which case pruning conserv-
/// atively keeps the file.
#[derive(Debug, Clone, Default)]
pub struct FileStats {
    /// Physical row count of the data file (from the manifest, not the footer).
    pub record_count: i64,
    /// field-id -> count of null values in this file.
    pub null_counts: HashMap<i32, i64>,
    /// field-id -> count of NaN values (float columns). Iceberg **excludes NaN from bounds**, and
    /// Arrow compares floats with `total_cmp` (NaN is the maximum), so a hidden NaN can satisfy
    /// `!=` / `>` / `>=` — pruning those ops on a float column is only safe when this is `0`.
    pub nan_counts: HashMap<i32, i64>,
    /// field-id -> serialized lower bound (Iceberg single-value binary).
    pub lower_bounds: HashMap<i32, Vec<u8>>,
    /// field-id -> serialized upper bound.
    pub upper_bounds: HashMap<i32, Vec<u8>>,
}

/// A partition value read from a data file's manifest `partition` struct (one per partition-spec
/// field, in spec order). Only the shapes the pruner compares are kept; anything else is `None`.
#[derive(Debug, Clone, PartialEq)]
pub enum PartVal {
    Int(i64),
    Str(String),
}

/// A data file of the current snapshot plus the physical row positions deleted from it by
/// merge-on-read positional delete files (empty for append-only / copy-on-write files).
#[derive(Debug, Clone)]
pub struct DataFileEntry {
    pub path: PathBuf,
    /// Deleted **physical** row positions within this file, sorted + deduped.
    pub deletes: BTreeSet<i64>,
    /// Data sequence number — an equality delete applies to this file only if its sequence
    /// number is strictly greater (so re-inserted rows written *after* a delete survive).
    pub seq: i64,
    /// Manifest statistics for file skipping (bounds, null counts, record count).
    pub stats: FileStats,
    /// Partition tuple (one slot per partition-spec field, in order; `None` where the value is
    /// null or an unhandled type). Used for transform-partition pruning.
    pub partition: Vec<Option<PartVal>>,
}

/// One partition-spec field: which source column it partitions and by what transform.
#[derive(Debug, Clone)]
pub struct PartField {
    pub source_id: i32,
    pub transform: Transform,
}

/// An Iceberg partition transform (the subset the pruner reasons about; others → `Other`, kept).
#[derive(Debug, Clone, PartialEq)]
pub enum Transform {
    Identity,
    Bucket(u32),
    Truncate(u32),
    /// `day(date|timestamp)` → days since epoch (exact integer, no calendar math).
    Day,
    /// `hour(timestamp)` → hours since epoch.
    Hour,
    /// year / month / void / unknown — not used for pruning (order-preserving temporals are
    /// covered by the source column's bounds anyway).
    Other,
}

/// A merge-on-read **equality** delete: rows are deleted from a data file when, on the equality
/// field-ids, their values match a row in the delete file — and only for data files with a
/// strictly lower sequence number.
#[derive(Debug, Clone)]
pub struct EqualityDelete {
    pub field_ids: Vec<i32>,
    /// Encoded keys (one per delete row) over the equality columns.
    pub keys: HashSet<String>,
    pub seq: i64,
    /// The delete file's own partition tuple, in the same shape as [`DataFileEntry::partition`].
    /// Empty when the delete was written under an unpartitioned spec, in which case it applies
    /// table-wide. See [`EqualityDelete::applies_to_partition`].
    pub partition: Vec<Option<PartVal>>,
}

/// An equality-delete file located during the manifest walk, before its keys are read.
///
/// A named struct rather than a tuple because it carries four fields whose order is not
/// self-evident — and `partition` in particular is easy to lose, which is exactly how the
/// partition-scope condition came to be missing in the first place.
struct EqDeleteSpec {
    path: PathBuf,
    field_ids: Vec<i32>,
    seq: i64,
    partition: Vec<Option<PartVal>>,
}

impl EqualityDelete {
    /// The spec's partition-scope condition, which the sequence-number check alone does not cover.
    ///
    /// Iceberg v2 scan planning applies a delete file to a data file only when **both** hold: the
    /// data file's sequence number is strictly lower (implemented at the call site), **and** the
    /// two share a partition — or the delete file is unpartitioned. Without the second half an
    /// equality delete written for `region=EU` also deletes the matching keys out of `region=US`,
    /// which is a silent over-delete: rows vanish, no error is raised, and the row count is still
    /// presented as exact.
    ///
    /// **Precondition: both tuples come from the same partition spec.** Nothing in a tuple records
    /// which spec produced it, so the caller establishes that — see `TablePlan::single_partition_spec`
    /// and the filter in `engine/local.rs`. Comparing tuples across specs is not merely imprecise:
    /// two same-arity tuples can be over different source columns, so "provably different" would be
    /// a false conclusion in the unsafe direction.
    ///
    /// Conservative in the direction this module already chose. `PartVal` decodes only ints and
    /// strings, so a slot is `None` for a null value *or* for a type this reader does not model —
    /// and those two cases are indistinguishable here. A delete is therefore skipped **only when
    /// the two tuples are provably different**: same arity, and some slot where both sides are
    /// decoded and unequal. Anything less certain keeps the previous behaviour of applying the
    /// delete, because under-reporting live rows is recoverable while resurfacing deleted rows is
    /// the failure this file refuses elsewhere (see the `content == 1` arm of `plan`).
    pub fn applies_to_partition(&self, data_partition: &[Option<PartVal>]) -> bool {
        if self.partition.is_empty() {
            return true; // unpartitioned delete spec — applies table-wide
        }
        if self.partition.len() != data_partition.len() {
            return true; // spec evolution or a shape we cannot line up — cannot prove difference
        }
        let provably_different =
            self.partition
                .iter()
                .zip(data_partition)
                .any(|(d, f)| match (d, f) {
                    (Some(d), Some(f)) => d != f,
                    // A `None` on either side is "unknown", never "equal to the other side".
                    _ => false,
                });
        !provably_different
    }
}

/// A column of the table's **current** schema (field-id, name, nullability, and the Arrow type
/// it maps to — `None` for nested/unmapped Iceberg types, resolved from a data file instead).
#[derive(Debug, Clone)]
pub struct IcebergField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    pub arrow_type: Option<DataType>,
}

/// The table's current schema, used to unify data files written under older schemas
/// (schema evolution: match by field-id, cast promoted types, null-fill added columns).
#[derive(Debug, Clone)]
pub struct IcebergSchema {
    pub fields: Vec<IcebergField>,
}

/// A read plan for the table's current snapshot: the live data files (with positional deletes
/// resolved against them) and the current schema for evolution-aware projection.
#[derive(Debug, Clone, Default)]
pub struct TablePlan {
    pub files: Vec<DataFileEntry>,
    /// Current table schema, when the metadata declares one (real Iceberg tables always do).
    /// `None` for bare fixtures without a `schema`/`schemas` block — then files read as-is.
    pub schema: Option<IcebergSchema>,
    /// Equality delete files of the current snapshot (applied per data file by sequence number).
    pub equality_deletes: Vec<EqualityDelete>,
    /// The default partition spec's fields (in order, matching each file's `partition` tuple),
    /// for transform-partition pruning. Empty for unpartitioned tables.
    pub partition_spec: Vec<PartField>,
    /// Count of data files skipped because they aren't Parquet (this reader is Parquet-only, so
    /// an ORC/Avro data file in a migrated table is dropped). Surfaced so callers know row counts
    /// may under-report rather than silently trusting an incomplete plan.
    pub skipped_non_parquet: usize,
    /// Can a partition tuple in this table be attributed to a single partition spec?
    ///
    /// True when the metadata declares at most one spec. Partition tuples here carry no spec id, so
    /// comparing two of them is only sound when there is only one spec they could belong to; see
    /// [`EqualityDelete::applies_to_partition`] and its caller in `engine/local.rs`. `false` is the
    /// conservative value and therefore the right `Default`: it makes the reader apply deletes it
    /// cannot scope rather than skip them.
    pub single_partition_spec: bool,
    /// The table's `format-version` (1, 2, or 3). Iceberg v3 is GA across the major platforms as
    /// of 2026 and adds features this reader does not model — **deletion vectors** (refused at
    /// plan time, see below), row lineage, and the VARIANT/GEOMETRY types. Recorded so callers can
    /// say which version they are looking at rather than guessing. `0` when the metadata omits it.
    pub format_version: i64,
}

impl TablePlan {
    /// Does the table carry any merge-on-read deletes (positional or equality)? When true the
    /// reader takes the read-from-start filter path instead of the footer-skip fast path.
    pub fn has_deletes(&self) -> bool {
        !self.equality_deletes.is_empty() || self.files.iter().any(|f| !f.deletes.is_empty())
    }
}

/// Resolve the current-snapshot read plan: live Parquet data files + their positional deletes.
pub fn plan(table_dir: &Path) -> Result<TablePlan> {
    plan_inner(table_dir, None, None)
}

/// Plan a table whose files were mirrored from object-store URI `origin` into local `table_dir`
/// (the object-store shim). Absolute object URIs inside the metadata are remapped to the mirror.
pub fn plan_object(table_dir: &Path, origin: &str) -> Result<TablePlan> {
    plan_inner(table_dir, None, Some(origin))
}

/// Like [`plan`], but when `root` is `Some`, every file the reader will open — the manifest
/// list, each manifest, positional-delete files, and data files — is validated (canonicalized)
/// to lie within `root`, refusing anything that escapes with [`EngineError::Forbidden`]. This is
/// what lets `serve --root` fully confine an Iceberg table: a table dir inside the root can't be
/// used to read data — *or metadata* — from outside it. `None` (the engine's own call) skips the
/// check.
pub fn plan_with_root(table_dir: &Path, root: Option<&Path>) -> Result<TablePlan> {
    plan_inner(table_dir, root, None)
}

/// The planner body. `origin` (object-store shim) remaps absolute object URIs to the local mirror.
fn plan_inner(table_dir: &Path, root: Option<&Path>, origin: Option<&str>) -> Result<TablePlan> {
    // Resolve a manifest/data/delete path relative to the table dir. When confined, refuse it if
    // it canonicalizes outside the root and return the **canonical** path, so the subsequent
    // `File::open` follows the already-validated target (a symlink swapped after the check can't
    // redirect the open) and delete-file keys match data-file keys. The refusal message is
    // path-free so the client-facing 403 doesn't disclose the resolved filesystem path.
    let guard = |raw: &str| -> Result<PathBuf> {
        let p = resolve(raw, table_dir, origin);
        match root {
            Some(r) => match std::fs::canonicalize(&p) {
                Ok(c) if c.starts_with(r) => Ok(c),
                _ => Err(EngineError::Forbidden(
                    "iceberg: table references a file outside the server root".to_string(),
                )),
            },
            None => Ok(p),
        }
    };
    let meta_path = current_metadata(table_dir)?;
    let meta: serde_json::Value = serde_json::from_reader(BufReader::new(File::open(&meta_path)?))
        .map_err(|e| EngineError::Other(format!("iceberg: bad metadata json: {e}")))?;

    // Recorded before anything else is read: it decides how a delete file that this reader cannot
    // parse should be described, and callers want to be able to state which version they opened.
    let format_version = meta
        .get("format-version")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // An empty table (no snapshot) is legal — return no files.
    let current = match meta.get("current-snapshot-id").and_then(|v| v.as_i64()) {
        Some(id) if id >= 0 => id,
        _ => {
            return Ok(TablePlan {
                format_version,
                ..Default::default()
            })
        }
    };
    let manifest_list = meta
        .get("snapshots")
        .and_then(|v| v.as_array())
        .and_then(|snaps| {
            snaps
                .iter()
                .find(|s| s.get("snapshot-id").and_then(|v| v.as_i64()) == Some(current))
        })
        .and_then(|s| s.get("manifest-list"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            EngineError::Other("iceberg: current snapshot has no manifest-list".into())
        })?;

    // First pass over the manifest-list: split data manifests (content 0) from delete
    // manifests (content 1), carrying each entry's sequence number (inherited from the
    // manifest-list entry when the manifest-entry leaves it null).
    #[allow(clippy::type_complexity)]
    let mut data_files: Vec<(PathBuf, i64, FileStats, Vec<Option<PartVal>>)> = Vec::new();
    let mut pos_delete_files: Vec<PathBuf> = Vec::new();
    let mut eq_delete_specs: Vec<EqDeleteSpec> = Vec::new();
    let mut skipped_non_parquet = 0usize;
    // Delete files this reader cannot parse — in practice **Iceberg v3 deletion vectors**, which
    // are Puffin blobs (`file_format = "puffin"`) carried in a delete manifest. Dropping a *data*
    // file under-reports, which is recoverable and merely warned about; dropping a *delete* file
    // over-reports, and it does so invisibly: the deleted rows come back as live rows and the row
    // count is still stated as exact. That is a wrong answer, not a partial one, so it is refused.
    let mut skipped_delete_formats: BTreeSet<String> = BTreeSet::new();
    let mut skipped_delete_files = 0usize;
    for (manifest_path, content, manifest_seq) in read_manifest_list(&guard(manifest_list)?)? {
        for mut e in read_manifest_entries(&guard(&manifest_path)?)? {
            if e.status == 2 {
                continue; // DELETED manifest entry — not part of this snapshot
            }
            if !e.file_format.eq_ignore_ascii_case("parquet") {
                // Parquet-only reader: a non-Parquet data file (ORC/Avro, common in Hive→Iceberg
                // migrations) is dropped, but counted so the caller can flag the under-report.
                if content == 0 && e.content == 0 {
                    skipped_non_parquet += 1;
                } else if content == 1 {
                    skipped_delete_files += 1;
                    skipped_delete_formats.insert(e.file_format.to_ascii_lowercase());
                }
                continue;
            }
            let seq = e.sequence_number.unwrap_or(manifest_seq);
            match (content, e.content) {
                (0, 0) => data_files.push((
                    guard(&e.file_path)?,
                    seq,
                    std::mem::take(&mut e.stats),
                    std::mem::take(&mut e.partition),
                )),
                (1, 1) => pos_delete_files.push(guard(&e.file_path)?),
                (1, 2) => eq_delete_specs.push(EqDeleteSpec {
                    path: guard(&e.file_path)?,
                    field_ids: std::mem::take(&mut e.equality_ids),
                    seq,
                    partition: std::mem::take(&mut e.partition),
                }),
                _ => {}
            }
        }
    }

    // Read every positional-delete file into a map: resolved data-file path -> deleted positions.
    // The referenced data-file path goes through the same `guard` as the data files themselves, so
    // the map keys match `DataFileEntry::path` under both the raw (unconfined) and canonical
    // (confined) representations.
    let mut deletes: HashMap<PathBuf, BTreeSet<i64>> = HashMap::new();
    for df in &pos_delete_files {
        for (referenced, pos) in read_positional_deletes(df)? {
            deletes.entry(guard(&referenced)?).or_default().insert(pos);
        }
    }

    // Read every equality-delete file into its key set over the equality columns.
    let mut equality_deletes = Vec::new();
    for spec in eq_delete_specs {
        if spec.field_ids.is_empty() {
            continue;
        }
        let keys = read_equality_delete_keys(&spec.path, &spec.field_ids)?;
        equality_deletes.push(EqualityDelete {
            field_ids: spec.field_ids,
            keys,
            seq: spec.seq,
            partition: spec.partition,
        });
    }

    let files = data_files
        .into_iter()
        .map(|(path, seq, stats, partition)| {
            let deletes = deletes.get(&path).cloned().unwrap_or_default();
            DataFileEntry {
                path,
                deletes,
                seq,
                stats,
                partition,
            }
        })
        .collect();
    if skipped_non_parquet > 0 {
        eprintln!(
            "lakeleto: WARNING {skipped_non_parquet} non-Parquet data file(s) in {} were skipped \
             (this reader is Parquet-only) — row counts and scans under-report the table",
            table_dir.display()
        );
    }
    // Refuse rather than answer wrongly. The message names the mechanism and the way out, because
    // "unsupported" on its own leaves the user unable to tell a broken table from a new one.
    if skipped_delete_files > 0 {
        let formats: Vec<&str> = skipped_delete_formats.iter().map(String::as_str).collect();
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "iceberg: table has {skipped_delete_files} delete file(s) this reader cannot \
                 apply (format: {}){} — reading it would return deleted rows as live rows, so \
                 the read is refused rather than answered wrongly. Rewrite the table's deletes \
                 into the data files (compaction / `rewrite_data_files`) and re-open it.",
                formats.join(", "),
                if format_version >= 3 {
                    ", i.e. Iceberg v3 deletion vectors"
                } else {
                    ""
                }
            ),
        });
    }
    if format_version >= 3 {
        // v3 reads correctly here *once* deletion vectors are ruled out above — but it also adds
        // row lineage and the VARIANT/GEOMETRY types, which this reader does not model. Warn
        // rather than refuse: a v3 table that uses none of them is read exactly right, and
        // refusing every v3 table would break tables that work today.
        //
        // Once per table per process, not once per plan: this planner runs on every read, and
        // `--root` runs it twice per request — under `serve`, stderr is the operator's log, and a
        // note repeated hundreds of times is a note nobody reads.
        static NOTED_V3: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<PathBuf>>> =
            std::sync::OnceLock::new();
        let mut noted = NOTED_V3
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if noted.insert(table_dir.to_path_buf()) {
            eprintln!(
                "lakeleto: NOTE {} is an Iceberg v3 table. Its data files are read correctly, \
                 but row lineage and the VARIANT/GEOMETRY types are not modelled by this reader.",
                table_dir.display()
            );
        }
    }
    Ok(TablePlan {
        files,
        schema: parse_current_schema(&meta),
        equality_deletes,
        partition_spec: parse_partition_spec(&meta),
        single_partition_spec: has_single_partition_spec(&meta),
        skipped_non_parquet,
        format_version,
    })
}

/// Does the metadata declare at most one partition spec?
///
/// This reader stores each file's partition tuple as bare values with no spec id, so two tuples are
/// comparable only when the table has never evolved its spec. With two specs, `[EU]` written under
/// spec 0 and `[US]` under spec 1 need not even be tuples over the same source column, so
/// concluding they differ would skip an equality delete that should apply — and resurfacing deleted
/// rows is the one direction this module refuses.
///
/// v1 metadata carries a single `partition-spec` and no history, so its absence of
/// `partition-specs` means one spec, not an unknown number.
fn has_single_partition_spec(meta: &serde_json::Value) -> bool {
    match meta.get("partition-specs").and_then(|v| v.as_array()) {
        Some(specs) => specs.len() <= 1,
        None => true,
    }
}

/// Parse the default partition spec (v2 `partition-specs` + `default-spec-id`, else v1
/// `partition-spec`) into its ordered fields — one per slot in each file's `partition` tuple.
fn parse_partition_spec(meta: &serde_json::Value) -> Vec<PartField> {
    let fields = if let (Some(id), Some(specs)) = (
        meta.get("default-spec-id").and_then(|v| v.as_i64()),
        meta.get("partition-specs").and_then(|v| v.as_array()),
    ) {
        specs
            .iter()
            .find(|s| s.get("spec-id").and_then(|v| v.as_i64()) == Some(id))
            .and_then(|s| s.get("fields"))
            .and_then(|v| v.as_array())
    } else {
        meta.get("partition-spec").and_then(|v| v.as_array())
    };
    let Some(fields) = fields else {
        return Vec::new();
    };
    fields
        .iter()
        .filter_map(|f| {
            Some(PartField {
                source_id: f.get("source-id")?.as_i64()? as i32,
                transform: parse_transform(f.get("transform")?.as_str()?),
            })
        })
        .collect()
}

fn parse_transform(s: &str) -> Transform {
    match s {
        "identity" => Transform::Identity,
        "day" => Transform::Day,
        "hour" => Transform::Hour,
        _ => {
            if let Some(n) = s.strip_prefix("bucket[").and_then(|r| r.strip_suffix(']')) {
                n.parse().map(Transform::Bucket).unwrap_or(Transform::Other)
            } else if let Some(w) = s
                .strip_prefix("truncate[")
                .and_then(|r| r.strip_suffix(']'))
            {
                w.parse()
                    .map(Transform::Truncate)
                    .unwrap_or(Transform::Other)
            } else {
                Transform::Other // year/month/void/unknown — kept (bounds cover order-preserving)
            }
        }
    }
}

/// Parse the table's current schema (v2 `schemas` + `current-schema-id`, else v1 `schema`).
fn parse_current_schema(meta: &serde_json::Value) -> Option<IcebergSchema> {
    let schema_obj = current_schema_object(meta)?;
    let raw_fields = schema_obj.get("fields")?.as_array()?;
    let mut fields = Vec::with_capacity(raw_fields.len());
    for f in raw_fields {
        let id = f.get("id")?.as_i64()? as i32;
        let name = f.get("name")?.as_str()?.to_string();
        let required = f.get("required").and_then(|v| v.as_bool()).unwrap_or(false);
        let arrow_type = f.get("type").and_then(iceberg_type_to_arrow);
        fields.push(IcebergField {
            id,
            name,
            required,
            arrow_type,
        });
    }
    (!fields.is_empty()).then_some(IcebergSchema { fields })
}

fn current_schema_object(meta: &serde_json::Value) -> Option<&serde_json::Value> {
    if let (Some(id), Some(schemas)) = (
        meta.get("current-schema-id").and_then(|v| v.as_i64()),
        meta.get("schemas").and_then(|v| v.as_array()),
    ) {
        if let Some(s) = schemas
            .iter()
            .find(|s| s.get("schema-id").and_then(|v| v.as_i64()) == Some(id))
        {
            return Some(s);
        }
    }
    meta.get("schema") // v1 fallback
}

/// Map an Iceberg primitive type name to its Arrow type. Nested/unknown types return `None`
/// (the reader keeps the data file's physical type for that column).
fn iceberg_type_to_arrow(t: &serde_json::Value) -> Option<DataType> {
    let s = t.as_str()?;
    Some(match s {
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        "string" => DataType::Utf8,
        "uuid" => DataType::FixedSizeBinary(16),
        "binary" => DataType::Binary,
        other => {
            if let Some(n) = other
                .strip_prefix("fixed[")
                .and_then(|r| r.strip_suffix(']'))
            {
                DataType::FixedSizeBinary(n.trim().parse().ok()?)
            } else {
                // Anything that is not `fixed[..]` has to be `decimal(p,s)`; `?` on the prefix
                // strip is what turns an unrecognised type name into `None`.
                let inner = other
                    .strip_prefix("decimal(")
                    .and_then(|r| r.strip_suffix(')'))?;
                let (p, sc) = inner.split_once(',')?;
                DataType::Decimal128(p.trim().parse().ok()?, sc.trim().parse().ok()?)
            }
        }
    })
}

/// Field-id → column-index map from a data file's Arrow schema (`PARQUET:field_id` metadata).
fn field_id_index(schema: &ArrowSchema) -> HashMap<i32, usize> {
    let mut m = HashMap::new();
    for (i, f) in schema.fields().iter().enumerate() {
        if let Some(id) = f
            .metadata()
            .get("PARQUET:field_id")
            .and_then(|v| v.parse::<i32>().ok())
        {
            m.insert(id, i);
        }
    }
    m
}

/// Build the fixed Arrow output schema for the current Iceberg schema. Types Iceberg marks as
/// nested/unmapped are resolved from `sample` (a data file's Arrow schema) by field-id or name.
pub fn target_schema(schema: &IcebergSchema, sample: &ArrowSchema) -> Result<SchemaRef> {
    let by_id = field_id_index(sample);
    let mut fields = Vec::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let dt = match &f.arrow_type {
            Some(dt) => dt.clone(),
            None => {
                let idx = by_id
                    .get(&f.id)
                    .copied()
                    .or_else(|| sample.fields().iter().position(|sf| sf.name() == &f.name));
                match idx {
                    Some(i) => sample.field(i).data_type().clone(),
                    None => {
                        return Err(EngineError::UnsupportedFormat {
                            detail: format!(
                                "iceberg: cannot resolve a type for column `{}` (field-id {})",
                                f.name, f.id
                            ),
                        })
                    }
                }
            }
        };
        let mut md = HashMap::new();
        md.insert("PARQUET:field_id".to_string(), f.id.to_string());
        fields.push(Field::new(&f.name, dt, !f.required).with_metadata(md));
    }
    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Project a data-file batch onto `target` (schema evolution): match each target column to the
/// file's by field-id (falling back to name when the file carries no field-ids), cast promoted
/// types, and null-fill columns the file predates.
pub fn project_batch(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let src = batch.schema();
    let by_id = field_id_index(&src);
    let n = batch.num_rows();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for tf in target.fields() {
        let want = tf.data_type();
        let tid = tf
            .metadata()
            .get("PARQUET:field_id")
            .and_then(|v| v.parse::<i32>().ok());
        let src_idx = tid
            .and_then(|id| by_id.get(&id).copied())
            .or_else(|| src.fields().iter().position(|sf| sf.name() == tf.name()));
        let col = match src_idx {
            Some(i) => {
                let c = batch.column(i).clone();
                if c.data_type() == want {
                    c
                } else {
                    arrow_cast::cast(&c, want).map_err(EngineError::arrow)?
                }
            }
            None => new_null_array(want, n),
        };
        cols.push(col);
    }
    RecordBatch::try_new(target.clone(), cols).map_err(EngineError::arrow)
}

/// Locate the current `*.metadata.json` (via `version-hint.text`, else the highest version).
fn current_metadata(table_dir: &Path) -> Result<PathBuf> {
    let mdir = table_dir.join("metadata");
    if let Ok(hint) = std::fs::read_to_string(mdir.join("version-hint.text")) {
        let n = hint.trim();
        for cand in [format!("v{n}.metadata.json"), format!("{n}.metadata.json")] {
            let p = mdir.join(cand);
            if p.is_file() {
                return Ok(p);
            }
        }
    }
    // Fall back to the highest-versioned metadata file.
    //
    // The tie-break is load-bearing, not tidiness. `metadata_version` takes the leading digits,
    // so the `<version>-<uuid>.metadata.json` naming that Hive- and Glue-style writers use puts
    // several files at the SAME version in one directory. Comparing with `>` alone let whichever
    // one `read_dir` happened to yield first win — and `read_dir` order is filesystem-dependent,
    // so the same table could resolve to a different snapshot on two machines, or on the same
    // machine after a copy. Ordering by `(version, file name)` makes the choice stable.
    //
    // Stable is not the same as correct: a uuid carries no ordering, so with a tie there is no
    // way to tell which of them is current. That is a table that needs a `version-hint.text` or
    // a catalog, and the read says so rather than quietly picking one.
    let mut candidates: Vec<(u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(&mdir)? {
        let p = entry?.path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if let Some(ver) = metadata_version(name) {
            candidates.push((ver, p));
        }
    }
    candidates.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.file_name().cmp(&b.1.file_name()))
    });
    let Some((top_version, chosen)) = candidates.pop() else {
        return Err(EngineError::UnsupportedFormat {
            detail: format!("iceberg: no *.metadata.json under {}", mdir.display()),
        });
    };
    let tied: Vec<&PathBuf> = candidates
        .iter()
        .rev()
        .take_while(|(v, _)| *v == top_version)
        .map(|(_, p)| p)
        .collect();

    // A tie is only a problem when the tied files disagree about what is current. Several
    // `<version>-<uuid>.metadata.json` naming the same `current-snapshot-id` describe one state,
    // and reading any of them is correct — that is a leftover file, not an ambiguous table, and
    // refusing it would reject tables that are perfectly well-defined.
    //
    // Genuine disagreement is different in kind: two files claim the same version and different
    // current snapshots, so nothing in the directory says which one the table is at, and the
    // greatest file name is a coin flip dressed as an answer. A uuid carries no ordering. This
    // used to warn on stderr and read one anyway, which is not a control — under `lakeleto serve`
    // that warning reaches a log nobody is reading per request, while the caller receives rows
    // that may be from the wrong snapshot with nothing marking them as such. Refusing is the same
    // choice this reader makes for deletion vectors: say what is missing, rather than answer
    // confidently from an unverified state.
    if !tied.is_empty() {
        let snapshot_of = |p: &Path| -> Result<Option<i64>> {
            let v: serde_json::Value = serde_json::from_reader(BufReader::new(File::open(p)?))
                .map_err(|e| EngineError::UnsupportedFormat {
                    detail: format!("iceberg: {} is not readable metadata: {e}", p.display()),
                })?;
            Ok(v.get("current-snapshot-id").and_then(|x| x.as_i64()))
        };
        let want = snapshot_of(&chosen)?;
        for other in &tied {
            if snapshot_of(other)? != want {
                return Err(EngineError::UnsupportedFormat {
                    detail: format!(
                        "iceberg: {} holds {} metadata files at version {top_version} that name \
                         different current snapshots ({} and {}), and there is no \
                         metadata/version-hint.text to say which is current. Refusing rather than \
                         reading one at random — add metadata/version-hint.text, or read this \
                         table through its catalog.",
                        mdir.display(),
                        tied.len() + 1,
                        chosen.file_name().and_then(|n| n.to_str()).unwrap_or("?"),
                        other.file_name().and_then(|n| n.to_str()).unwrap_or("?"),
                    ),
                });
            }
        }
    }
    Ok(chosen)
}

/// Parse the version out of a metadata filename (`v3.metadata.json` / `3.metadata.json`).
fn metadata_version(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".metadata.json")?;
    let digits = stem.strip_prefix('v').unwrap_or(stem);
    // Some writers use `<version>-<uuid>` — take the leading digits.
    let lead: String = digits.chars().take_while(|c| c.is_ascii_digit()).collect();
    lead.parse().ok()
}

/// Manifest-list entries → `(manifest_path, content, sequence_number)` (content: 0 = data,
/// 1 = deletes; v1 = 0). The sequence number is inherited by manifest entries that leave theirs null.
fn read_manifest_list(path: &Path) -> Result<Vec<(String, i64, i64)>> {
    let reader = Reader::new(BufReader::new(File::open(path)?)).map_err(|e| {
        EngineError::Other(format!("iceberg: manifest-list {}: {e}", path.display()))
    })?;
    let mut out = Vec::new();
    for record in reader {
        let record = record.map_err(|e| EngineError::Other(format!("iceberg: avro: {e}")))?;
        if let Some(mp) = field(&record, "manifest_path").and_then(as_str) {
            let content = field(&record, "content").and_then(as_i64).unwrap_or(0);
            let seq = field(&record, "sequence_number")
                .and_then(as_i64)
                .unwrap_or(0);
            out.push((mp.to_string(), content, seq));
        }
    }
    Ok(out)
}

/// One `manifest_entry` flattened to the fields the reader needs.
struct ManifestEntry {
    /// 0=EXISTING, 1=ADDED, 2=DELETED (entry status, not the file's content).
    status: i64,
    /// data_file.content: 0=data, 1=positional deletes, 2=equality deletes.
    content: i64,
    file_path: String,
    file_format: String,
    /// Entry sequence number (nullable → inherit the manifest's).
    sequence_number: Option<i64>,
    /// data_file.equality_ids — the field-ids an equality-delete file matches on.
    equality_ids: Vec<i32>,
    /// Per-column bounds / null counts / record count (for statistics pruning).
    stats: FileStats,
    /// data_file.partition tuple values (spec-field order) for transform-partition pruning.
    partition: Vec<Option<PartVal>>,
}

/// Read a manifest's entries (works for both data and delete manifests).
fn read_manifest_entries(path: &Path) -> Result<Vec<ManifestEntry>> {
    let reader = Reader::new(BufReader::new(File::open(path)?))
        .map_err(|e| EngineError::Other(format!("iceberg: manifest {}: {e}", path.display())))?;
    let mut out = Vec::new();
    for record in reader {
        let record = record.map_err(|e| EngineError::Other(format!("iceberg: avro: {e}")))?;
        let status = field(&record, "status").and_then(as_i64).unwrap_or(1);
        let sequence_number = field(&record, "sequence_number").and_then(as_i64);
        let Some(df) = field(&record, "data_file") else {
            continue;
        };
        let content = field(df, "content").and_then(as_i64).unwrap_or(0);
        let file_format = field(df, "file_format")
            .and_then(as_str)
            .unwrap_or("PARQUET")
            .to_string();
        let equality_ids = field(df, "equality_ids")
            .map(as_i32_list)
            .unwrap_or_default();
        let stats = FileStats {
            record_count: field(df, "record_count").and_then(as_i64).unwrap_or(0),
            null_counts: field(df, "null_value_counts")
                .map(as_int_long_map)
                .unwrap_or_default(),
            nan_counts: field(df, "nan_value_counts")
                .map(as_int_long_map)
                .unwrap_or_default(),
            lower_bounds: field(df, "lower_bounds")
                .map(as_int_bytes_map)
                .unwrap_or_default(),
            upper_bounds: field(df, "upper_bounds")
                .map(as_int_bytes_map)
                .unwrap_or_default(),
        };
        let partition = field(df, "partition").map(as_partition).unwrap_or_default();
        let Some(file_path) = field(df, "file_path").and_then(as_str) else {
            continue;
        };
        out.push(ManifestEntry {
            status,
            content,
            file_path: file_path.to_string(),
            file_format,
            sequence_number,
            equality_ids,
            stats,
            partition,
        });
    }
    Ok(out)
}

/// Extract a manifest `partition` struct's field values (in order) into the pruner's [`PartVal`]s.
/// Nulls and unhandled Avro shapes become `None` (so the pruner keeps the file, never over-skips).
fn as_partition(v: &Value) -> Vec<Option<PartVal>> {
    match unwrap(v) {
        Value::Record(fields) => fields
            .iter()
            .map(|(_, val)| match unwrap(val) {
                Value::Int(i) => Some(PartVal::Int(*i as i64)),
                Value::Long(i) => Some(PartVal::Int(*i)),
                Value::String(s) => Some(PartVal::Str(s.clone())),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Encode each row of `batch` into a key over the columns identified by `field_ids` (in order),
/// using type-aware value formatting — so an equality-delete key and a data-row key compare equal
/// exactly when the equality-column values do (across int↔long promotion etc.).
pub fn row_keys(batch: &RecordBatch, field_ids: &[i32]) -> Result<Vec<String>> {
    let by_id = field_id_index(&batch.schema());
    // Distinct sentinels so a NULL never collides with an empty string or a literal separator.
    let opts = FormatOptions::default().with_null("\u{0}∅");
    let mut formatters: Vec<Option<ArrayFormatter>> = Vec::with_capacity(field_ids.len());
    for id in field_ids {
        match by_id.get(id) {
            Some(&i) => formatters.push(Some(
                ArrayFormatter::try_new(batch.column(i), &opts).map_err(EngineError::arrow)?,
            )),
            None => formatters.push(None), // column absent → treated as null
        }
    }
    let n = batch.num_rows();
    let mut keys = Vec::with_capacity(n);
    for r in 0..n {
        let mut key = String::new();
        for f in &formatters {
            match f {
                Some(fmt) => {
                    let _ = write!(key, "{}", fmt.value(r));
                }
                None => key.push_str("\u{0}∅"),
            }
            key.push('\u{1f}'); // unit separator between columns
        }
        keys.push(key);
    }
    Ok(keys)
}

/// Read an equality-delete Parquet file → the set of encoded keys over `field_ids`.
fn read_equality_delete_keys(path: &Path, field_ids: &[i32]) -> Result<HashSet<String>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
        .map_err(EngineError::parquet)?;
    let reader = builder.build().map_err(EngineError::parquet)?;
    let mut keys = HashSet::new();
    for batch in reader {
        let batch = batch.map_err(EngineError::arrow)?;
        for k in row_keys(&batch, field_ids)? {
            keys.insert(k);
        }
    }
    Ok(keys)
}

/// Read an Iceberg positional-delete Parquet file → `(referenced_data_file_path, position)`
/// pairs. The file has a `file_path` (string) and a `pos` (long) column.
fn read_positional_deletes(path: &Path) -> Result<Vec<(String, i64)>> {
    use arrow_array::{Array, Int64Array, StringArray};

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
        .map_err(EngineError::parquet)?;
    let reader = builder.build().map_err(EngineError::parquet)?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(EngineError::arrow)?;
        let paths = batch
            .column_by_name("file_path")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let poss = batch
            .column_by_name("pos")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let (Some(paths), Some(poss)) = (paths, poss) else {
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "iceberg: positional-delete file {} lacks string `file_path` + long `pos`",
                    path.display()
                ),
            });
        };
        for i in 0..batch.num_rows() {
            if paths.is_valid(i) && poss.is_valid(i) {
                out.push((paths.value(i).to_string(), poss.value(i)));
            }
        }
    }
    Ok(out)
}

// ---- statistics / partition pruning ---------------------------------------------------
// Skip data files a filter cannot match, using the manifest's per-column bounds + null counts.
// INVARIANT: pruning is *conservative* — a file is dropped ONLY when its statistics PROVE it
// contains zero matching rows. Every uncertain case keeps the file. A wrong keep is a perf
// miss; a wrong skip would silently drop rows, so the predicate must stay consistent with the
// Arrow row filter (numeric columns compared as f64, others byte-lexicographically as Utf8;
// a null cell never matches any op).

/// A bound/value decoded into the domain the row filter compares in.
#[derive(Debug, Clone, PartialEq)]
enum PruneVal {
    /// Numeric columns are cast to f64 by the filter, so bounds compare as f64 too. i64→f64
    /// rounding is monotonic, and the filter kernel rounds identically, so this never over-skips.
    Num(f64),
    /// Utf8 columns compare byte-lexicographically (matching Arrow's Utf8 `cmp` and Iceberg's
    /// UTF-8 bound order); truncated bounds stay valid (lower ≤ min, upper ≥ max).
    Str(String),
}

/// A column type Lakeleto can prune on — one where the decoded bound compares consistently with the
/// Arrow row filter. `numeric` = the filter casts it to f64 (Int/Float/Decimal); otherwise the
/// filter casts it to Utf8 and compares byte-lexically (Utf8/Date/Time/Timestamp).
fn prunable_kind(dt: &DataType) -> Option<bool /* numeric */> {
    match dt {
        DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64 => Some(true),
        DataType::Decimal128(_, _) => Some(true),
        DataType::Utf8 => Some(false),
        DataType::Date32 => Some(false),
        DataType::Time64(TimeUnit::Microsecond) => Some(false),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(false),
        _ => None,
    }
}

/// Decode an Iceberg single-value binary bound into the domain the row filter compares in. The
/// primitives decode directly; decimal / date / time / timestamp are decoded *through the same
/// `arrow_cast` the row filter applies* (Decimal→Float64, temporal→Utf8), so a bound and a data
/// cell always land in the identical comparison space — no bespoke formatting to drift out of sync.
fn decode_bound(bytes: &[u8], dt: &DataType) -> Option<PruneVal> {
    match dt {
        DataType::Int32 => Some(PruneVal::Num(
            i32::from_le_bytes(bytes.try_into().ok()?) as f64
        )),
        DataType::Int64 => Some(PruneVal::Num(
            i64::from_le_bytes(bytes.try_into().ok()?) as f64
        )),
        DataType::Float32 => Some(PruneVal::Num(
            f32::from_le_bytes(bytes.try_into().ok()?) as f64
        )),
        DataType::Float64 => Some(PruneVal::Num(f64::from_le_bytes(bytes.try_into().ok()?))),
        DataType::Utf8 => std::str::from_utf8(bytes)
            .ok()
            .map(|s| PruneVal::Str(s.to_string())),
        DataType::Decimal128(p, s) => {
            // Iceberg stores decimals as minimum-length two's-complement big-endian unscaled bytes.
            let arr = Decimal128Array::from(vec![decode_i128_be(bytes)?])
                .with_precision_and_scale(*p, *s)
                .ok()?;
            let f = arrow_cast::cast(&arr, &DataType::Float64).ok()?;
            let f = f.as_any().downcast_ref::<Float64Array>()?;
            f.is_valid(0).then(|| PruneVal::Num(f.value(0)))
        }
        DataType::Date32 | DataType::Time64(TimeUnit::Microsecond) | DataType::Timestamp(..) => {
            let arr: ArrayRef = match dt {
                DataType::Date32 => Arc::new(Date32Array::from(vec![i32::from_le_bytes(
                    bytes.try_into().ok()?,
                )])),
                DataType::Time64(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from(
                    vec![i64::from_le_bytes(bytes.try_into().ok()?)],
                )),
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    let a = TimestampMicrosecondArray::from(vec![i64::from_le_bytes(
                        bytes.try_into().ok()?,
                    )]);
                    Arc::new(match tz {
                        Some(z) => a.with_timezone(z.clone()),
                        None => a,
                    })
                }
                _ => return None,
            };
            let s = arrow_cast::cast(&arr, &DataType::Utf8).ok()?;
            let s = s.as_any().downcast_ref::<StringArray>()?;
            s.is_valid(0).then(|| PruneVal::Str(s.value(0).to_string()))
        }
        _ => None,
    }
}

/// Decode a two's-complement big-endian integer (Iceberg's decimal unscaled encoding), sign-
/// extended into an `i128`.
fn decode_i128_be(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let mut v: i128 = if bytes[0] & 0x80 != 0 { -1 } else { 0 }; // sign extension
    for &b in bytes {
        v = (v << 8) | (b as i128);
    }
    Some(v)
}

/// Parse a filter value into the column's domain. Numeric columns need an f64-parseable value
/// (else the row filter falls back to a string compare we don't model → keep the file); NaN is
/// rejected so we never reason about it (a NaN filter matches nothing but we stay conservative).
fn parse_filter_val(dt: &DataType, value: &str) -> Option<PruneVal> {
    match prunable_kind(dt)? {
        true => value
            .parse::<f64>()
            .ok()
            .filter(|v| !v.is_nan())
            .map(PruneVal::Num),
        false => Some(PruneVal::Str(value.to_string())),
    }
}

fn pcmp(a: &PruneVal, b: &PruneVal) -> Option<std::cmp::Ordering> {
    match (a, b) {
        // `total_cmp` matches Arrow's float comparison exactly (it uses total_cmp too): this is
        // what makes `-0.0 < 0.0` agree with the kernel. NaN is impossible here — bounds exclude
        // it and NaN filter values are rejected upstream — so total order == numeric order.
        (PruneVal::Num(x), PruneVal::Num(y)) => Some(x.total_cmp(y)),
        (PruneVal::Str(x), PruneVal::Str(y)) => Some(x.cmp(y)),
        _ => None, // domain mismatch → incomparable → caller keeps the file
    }
}

/// The conservative predicate core shared by file-level (Iceberg manifest bounds) and row-group-
/// level (Parquet statistics) pruning: given one column's decoded `[lower, upper]` plus its
/// null/NaN facts for one chunk and one filter, could the chunk contain a matching row? `false`
/// only when provably impossible. `nan_free` is only consulted for float columns.
fn chunk_might_match(
    op: FilterOp,
    is_float: bool,
    all_null: bool,
    nan_free: bool,
    lower: Option<&PruneVal>,
    upper: Option<&PruneVal>,
    value: Option<&PruneVal>,
) -> bool {
    if all_null {
        // No non-null row, so nothing matches any op that compares a VALUE — including `Ne` and
        // the text predicates, since a null matches neither direction of a substring test.
        //
        // `IsNull` inverts, and it is the one case where pruning here would be a WRONG ANSWER
        // rather than a missed optimisation: an all-null file is precisely the file where every
        // row matches, so dropping it returns zero rows for data sitting on disk, with nothing in
        // the keep-mask to say it happened. `NotNull` is safe to prune — an all-null file has no
        // matching row by definition.
        return op == FilterOp::IsNull;
    }
    if op == FilterOp::Contains {
        return true; // substring — bounds can't prove absence
    }
    // Under Arrow's total order NaN is the maximum, so `!=` / `>` / `>=` can be satisfied by a
    // NaN that the finite bounds exclude — only prune those on a float column proven NaN-free.
    if is_float && matches!(op, FilterOp::Ne | FilterOp::Gt | FilterOp::Ge) && !nan_free {
        return true;
    }
    match (lower, upper, value) {
        (Some(lower), Some(upper), Some(value)) => range_might_match(op, lower, upper, value),
        _ => true, // missing bound or unmodellable value → keep
    }
}

/// Could a file whose column ranges over `[lower, upper]` contain a row satisfying `op value`?
/// `false` ONLY when provably impossible. Incomparable bounds (NaN / domain mismatch) → `true`.
fn range_might_match(op: FilterOp, lower: &PruneVal, upper: &PruneVal, value: &PruneVal) -> bool {
    use std::cmp::Ordering::*;
    let (Some(lo), Some(hi)) = (pcmp(lower, value), pcmp(upper, value)) else {
        return true; // can't compare → keep
    };
    match op {
        FilterOp::Eq => lo != Greater && hi != Less, // lower ≤ v ≤ upper
        FilterOp::Ne => !(lo == Equal && hi == Equal), // skip only if every value == v
        FilterOp::Lt => lo == Less,                  // some value < v needs lower < v
        FilterOp::Le => lo != Greater,               // needs lower ≤ v
        FilterOp::Gt => hi == Greater,               // some value > v needs upper > v
        FilterOp::Ge => hi != Less,                  // needs upper ≥ v
        // Everything below is a text or null predicate that min/max bounds cannot disprove:
        // a file whose values range "apple".."cherry" may still hold one starting with "b", and
        // Iceberg's bounds say nothing about which rows are null. Keep the file — pruning is only
        // ever allowed to drop a file it can PROVE cannot match.
        FilterOp::Contains
        | FilterOp::NotContains
        | FilterOp::StartsWith
        | FilterOp::EndsWith
        | FilterOp::In
        | FilterOp::IsNull
        | FilterOp::NotNull => true,
    }
}

/// Can this data file possibly contain a row matching `filter`? Conservative — `true` unless the
/// stats prove otherwise. `field` is the current-schema column named by the filter.
fn file_might_match(filter: &FilterSpec, stats: &FileStats, field: &IcebergField) -> bool {
    let Some(dt) = &field.arrow_type else {
        return true;
    };
    // Only prune the types whose comparison matches the row filter exactly.
    if prunable_kind(dt).is_none() {
        return true;
    }
    let id = field.id;
    // NaN is counted separately from nulls, so an all-null file stays all-null for float columns.
    let all_null = stats.record_count > 0
        && stats
            .null_counts
            .get(&id)
            .is_some_and(|&n| n >= stats.record_count);
    let is_float = matches!(dt, DataType::Float32 | DataType::Float64);
    let nan_free = stats.nan_counts.get(&id).is_some_and(|&c| c == 0);
    let value = parse_filter_val(dt, &filter.value);
    let lower = stats
        .lower_bounds
        .get(&id)
        .and_then(|b| decode_bound(b, dt));
    let upper = stats
        .upper_bounds
        .get(&id)
        .and_then(|b| decode_bound(b, dt));
    chunk_might_match(
        filter.op,
        is_float,
        all_null,
        nan_free,
        lower.as_ref(),
        upper.as_ref(),
        value.as_ref(),
    )
}

/// Select the row groups of a Parquet data file that may contain rows matching `filters`, using
/// the file's OWN column statistics — the in-file analogue of file-level pruning. Returns the
/// indices to keep, or `None` when nothing can be pruned (read every row group). Only Int/Long/Utf8
/// columns are considered — their Parquet typed stats align exactly with the row filter's f64 /
/// byte-lex comparison, so pruning stays provably conservative. Float/decimal/temporal columns are
/// left to file-level pruning (Parquet float stats have no reliable NaN signal).
pub fn select_row_groups(
    meta: &parquet::file::metadata::ParquetMetaData,
    parquet_schema: &ArrowSchema,
    schema: Option<&IcebergSchema>,
    filters: &[FilterSpec],
) -> Option<Vec<usize>> {
    let ischema = schema?;
    let by_name: HashMap<&str, &IcebergField> = ischema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();
    let by_id = field_id_index(parquet_schema);

    // Precompute (parquet column index, op, value) for each filter we can evaluate at row-group
    // granularity (Int32/Int64/Utf8 columns present in this file).
    struct RgFilter {
        col: usize,
        op: FilterOp,
        string: bool,
        value: PruneVal,
    }
    let mut plans: Vec<RgFilter> = Vec::new();
    for f in filters {
        let Some(field) = by_name.get(f.column.as_str()) else {
            continue;
        };
        let string = match field.arrow_type {
            Some(DataType::Int32) | Some(DataType::Int64) => false,
            Some(DataType::Utf8) => true,
            _ => continue, // only int/long/utf8 prune cleanly against Parquet typed stats
        };
        let (Some(&col), Some(dt)) = (by_id.get(&field.id), field.arrow_type.as_ref()) else {
            continue; // column absent from this file (e.g. added later) → can't prune on it
        };
        let Some(value) = parse_filter_val(dt, &f.value) else {
            continue;
        };
        plans.push(RgFilter {
            col,
            op: f.op,
            string,
            value,
        });
    }
    if plans.is_empty() {
        return None;
    }

    let mut keep = Vec::new();
    let mut pruned = false;
    for i in 0..meta.num_row_groups() {
        let rg = meta.row_group(i);
        let survives = plans.iter().all(|p| {
            let Some(stats) = rg.column(p.col).statistics() else {
                return true; // no stats → can't prune this row group on this filter
            };
            let all_null = rg.num_rows() > 0
                && stats
                    .null_count_opt()
                    .is_some_and(|nc| nc as i64 >= rg.num_rows());
            let (lower, upper) = stat_bounds(stats, p.string);
            // is_float=false / nan_free=true: only int/utf8 reach here, so the float guard is moot.
            chunk_might_match(
                p.op,
                false,
                all_null,
                true,
                lower.as_ref(),
                upper.as_ref(),
                Some(&p.value),
            )
        });
        if survives {
            keep.push(i);
        } else {
            pruned = true;
        }
    }
    pruned.then_some(keep)
}

/// Decode a Parquet row-group's typed min/max into the pruning domain (Int→f64, Utf8→String).
fn stat_bounds(
    stats: &parquet::file::statistics::Statistics,
    string: bool,
) -> (Option<PruneVal>, Option<PruneVal>) {
    use parquet::file::statistics::Statistics;
    match stats {
        Statistics::Int32(v) => (
            v.min_opt().map(|m| PruneVal::Num(*m as f64)),
            v.max_opt().map(|m| PruneVal::Num(*m as f64)),
        ),
        Statistics::Int64(v) => (
            v.min_opt().map(|m| PruneVal::Num(*m as f64)),
            v.max_opt().map(|m| PruneVal::Num(*m as f64)),
        ),
        Statistics::ByteArray(v) if string => {
            let dec = |b: &parquet::data_type::ByteArray| {
                std::str::from_utf8(b.data())
                    .ok()
                    .map(|s| PruneVal::Str(s.to_string()))
            };
            (v.min_opt().and_then(dec), v.max_opt().and_then(dec))
        }
        _ => (None, None),
    }
}

/// Transform-partition pruning for an **equality** filter `col = v`: a row with `col == v` lands
/// in partition value `transform(v)`, so a file whose partition for that spec field is a *different*
/// value provably contains no matching row. Exact for every transform; only `=` is handled (range
/// pruning across non-order-preserving transforms like bucket is unsound). Anything we can't
/// compute keeps the file. This complements column-bounds pruning — it's the one that defeats a
/// `bucket[N]` scatter, where the source column's bounds span the whole range.
fn partition_might_match(
    filter: &FilterSpec,
    entry: &DataFileEntry,
    spec: &[PartField],
    field: &IcebergField,
) -> bool {
    if filter.op != FilterOp::Eq || spec.is_empty() {
        return true;
    }
    let Some(dt) = &field.arrow_type else {
        return true;
    };
    for (i, pf) in spec.iter().enumerate() {
        if pf.source_id != field.id {
            continue;
        }
        let (Some(expected), Some(Some(actual))) = (
            transform_value(&pf.transform, dt, &filter.value),
            entry.partition.get(i),
        ) else {
            continue; // unhandled transform/type or null/absent partition value → can't prune
        };
        if expected != *actual {
            return false; // this partition can't hold `col = v`
        }
    }
    true
}

/// Compute the partition value `transform(v)` for a filter value `v` (as a string in column type
/// `dt`). `None` when the transform/type isn't one we model (then the caller keeps the file).
fn transform_value(t: &Transform, dt: &DataType, value: &str) -> Option<PartVal> {
    match t {
        Transform::Identity => match dt {
            DataType::Int32 | DataType::Int64 => Some(PartVal::Int(value.parse().ok()?)),
            DataType::Utf8 => Some(PartVal::Str(value.to_string())),
            _ => None,
        },
        Transform::Bucket(n) if *n > 0 => {
            // Iceberg bucket: (murmur3_x86_32(ser(v)) & Int32::MAX) % N. int/long serialize as an
            // 8-byte little-endian long; string as UTF-8.
            let hash = match dt {
                DataType::Int32 | DataType::Int64 => {
                    murmur3_32(&value.parse::<i64>().ok()?.to_le_bytes())
                }
                DataType::Utf8 => murmur3_32(value.as_bytes()),
                _ => return None,
            };
            Some(PartVal::Int(((hash & i32::MAX as u32) % n) as i64))
        }
        Transform::Truncate(w) if *w > 0 => match dt {
            DataType::Int32 | DataType::Int64 => {
                let v: i64 = value.parse().ok()?;
                Some(PartVal::Int(v - v.rem_euclid(*w as i64)))
            }
            DataType::Utf8 => Some(PartVal::Str(value.chars().take(*w as usize).collect())),
            _ => None,
        },
        // Temporal transforms are exact integer divisions of the epoch value (floor, via
        // `div_euclid`) — no calendar decomposition, so no leap-year edge cases.
        Transform::Day => match dt {
            DataType::Date32 => Some(PartVal::Int(parse_date_days(value)? as i64)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Some(PartVal::Int(
                parse_ts_micros(value, dt)?.div_euclid(86_400_000_000),
            )),
            _ => None,
        },
        Transform::Hour => match dt {
            DataType::Timestamp(TimeUnit::Microsecond, _) => Some(PartVal::Int(
                parse_ts_micros(value, dt)?.div_euclid(3_600_000_000),
            )),
            _ => None,
        },
        _ => None,
    }
}

/// Parse an ISO date string to its Date32 value (days since epoch) via Arrow's own cast.
fn parse_date_days(value: &str) -> Option<i32> {
    let arr = arrow_cast::cast(&StringArray::from(vec![value]), &DataType::Date32).ok()?;
    let arr = arr.as_any().downcast_ref::<Date32Array>()?;
    arr.is_valid(0).then(|| arr.value(0))
}

/// Parse an ISO timestamp string to microseconds since epoch (in the column's timestamp type).
fn parse_ts_micros(value: &str, dt: &DataType) -> Option<i64> {
    let arr = arrow_cast::cast(&StringArray::from(vec![value]), dt).ok()?;
    let arr = arr.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    arr.is_valid(0).then(|| arr.value(0))
}

/// 32-bit x86 MurmurHash3 (seed 0) — Iceberg's bucket-transform hash.
fn murmur3_32(data: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut h: u32 = 0;
    let (blocks, tail) = data.as_chunks::<4>();
    for c in blocks {
        let mut k = u32::from_le_bytes(*c);
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let mut k = 0u32;
        for (i, &b) in tail.iter().enumerate() {
            k ^= (b as u32) << (8 * i);
        }
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

/// A keep-mask over `plan.files`: `true` = the file may contain rows matching **all** `filters`
/// (ANDed), `false` = provably cannot (safe to skip). Files whose column has no usable stats, and
/// every undecidable case, are kept. With no current schema nothing is prunable → all kept.
pub fn keep_mask(plan: &TablePlan, filters: &[FilterSpec]) -> Vec<bool> {
    let Some(schema) = &plan.schema else {
        return vec![true; plan.files.len()];
    };
    let by_name: HashMap<&str, &IcebergField> =
        schema.fields.iter().map(|f| (f.name.as_str(), f)).collect();
    plan.files
        .iter()
        .map(|entry| {
            // AND: keep the file only if it might match every filter — by column statistics AND
            // by its partition (transform-partition pruning).
            filters
                .iter()
                .all(|filter| match by_name.get(filter.column.as_str()) {
                    Some(field) => {
                        file_might_match(filter, &entry.stats, field)
                            && partition_might_match(filter, entry, &plan.partition_spec, field)
                    }
                    None => true, // filter on an unknown column → can't prune
                })
        })
        .collect()
}

/// Build a pruned plan keeping only files that may match `filters`; returns it plus the number of
/// files skipped. Shares the schema + equality deletes with the original plan.
pub fn prune(plan: &TablePlan, filters: &[FilterSpec]) -> (TablePlan, usize) {
    let keep = keep_mask(plan, filters);
    let skipped = keep.iter().filter(|k| !**k).count();
    let files = plan
        .files
        .iter()
        .zip(&keep)
        .filter(|(_, k)| **k)
        .map(|(f, _)| f.clone())
        .collect();
    (
        TablePlan {
            files,
            schema: plan.schema.clone(),
            equality_deletes: plan.equality_deletes.clone(),
            partition_spec: plan.partition_spec.clone(),
            // Carried, not recomputed: pruning drops files, it does not change how many partition
            // specs the table has, and defaulting this to `false` here would silently widen every
            // pruned scan's deletes.
            single_partition_spec: plan.single_partition_spec,
            skipped_non_parquet: plan.skipped_non_parquet,
            format_version: plan.format_version,
        },
        skipped,
    )
}

/// Resolve an Iceberg path (strip a `file://` scheme; join relative paths to the table dir).
///
/// `origin` supports the object-store shim: when a table has been mirrored from an object-store URI
/// (`s3://bucket/prefix/…`) into a local `table_dir`, the manifest/data URIs stored inside are
/// absolute object URIs. Any that live under `origin` are remapped to the local mirror by stripping
/// the prefix and joining the remainder to `table_dir`.
fn resolve(raw: &str, table_dir: &Path, origin: Option<&str>) -> PathBuf {
    if let Some(orig) = origin {
        if let Some(rel) = raw.strip_prefix(orig) {
            return table_dir.join(rel.trim_start_matches(['/', '\\']));
        }
    }
    let s = raw.strip_prefix("file://").unwrap_or(raw);
    let p = Path::new(s);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        table_dir.join(p)
    }
}

// ---- Avro Value navigation (nullable fields arrive as unions) --------------------------

fn unwrap(v: &Value) -> &Value {
    match v {
        Value::Union(_, b) => unwrap(b),
        other => other,
    }
}

fn field<'a>(record: &'a Value, name: &str) -> Option<&'a Value> {
    match unwrap(record) {
        Value::Record(fields) => fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| unwrap(v)),
        _ => None,
    }
}

fn as_str(v: &Value) -> Option<&str> {
    match unwrap(v) {
        Value::String(s) => Some(s),
        _ => None,
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match unwrap(v) {
        Value::Int(i) => Some(*i as i64),
        Value::Long(i) => Some(*i),
        _ => None,
    }
}

/// An Avro array of ints → `Vec<i32>` (the `equality_ids` field-id list).
fn as_i32_list(v: &Value) -> Vec<i32> {
    match unwrap(v) {
        Value::Array(items) => items
            .iter()
            .filter_map(|i| as_i64(i).map(|n| n as i32))
            .collect(),
        _ => Vec::new(),
    }
}

fn as_bytes(v: &Value) -> Option<Vec<u8>> {
    match unwrap(v) {
        Value::Bytes(b) | Value::Fixed(_, b) => Some(b.clone()),
        _ => None,
    }
}

/// Iterate an Iceberg int-keyed "map" — encoded in Avro as an array of `{key:int, value:V}`
/// records (Avro maps need string keys, so Iceberg uses this logical form) — or, defensively, a
/// native `Value::Map` with stringified int keys. `pick` extracts the value.
fn int_keyed_map<T>(v: &Value, pick: impl Fn(&Value) -> Option<T>) -> HashMap<i32, T> {
    let mut out = HashMap::new();
    match unwrap(v) {
        Value::Array(items) => {
            for it in items {
                if let (Some(k), Some(val)) = (
                    field(it, "key").and_then(as_i64),
                    field(it, "value").and_then(&pick),
                ) {
                    out.insert(k as i32, val);
                }
            }
        }
        Value::Map(m) => {
            for (k, val) in m {
                if let (Ok(k), Some(val)) = (k.parse::<i32>(), pick(val)) {
                    out.insert(k, val);
                }
            }
        }
        _ => {}
    }
    out
}

/// field-id → long map (`null_value_counts`, `value_counts`).
fn as_int_long_map(v: &Value) -> HashMap<i32, i64> {
    int_keyed_map(v, as_i64)
}

/// field-id → serialized-bound-bytes map (`lower_bounds`, `upper_bounds`).
fn as_int_bytes_map(v: &Value) -> HashMap<i32, Vec<u8>> {
    int_keyed_map(v, as_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur3_matches_iceberg_spec_vectors() {
        // Apache Iceberg spec, Appendix B (32-bit MurmurHash3 test values).
        assert_eq!(murmur3_32(&34i64.to_le_bytes()), 2017239379, "int/long 34");
        assert_eq!(
            murmur3_32("iceberg".as_bytes()),
            1210000089,
            "string \"iceberg\""
        );
    }

    #[test]
    fn transforms_compute_expected_partition_values() {
        // bucket[16] of 34 = (2017239379 & i32::MAX) % 16 = 3.
        assert_eq!(
            transform_value(&Transform::Bucket(16), &DataType::Int64, "34"),
            Some(PartVal::Int(3))
        );
        // truncate[10] of 123 → 120 (floor to a multiple of 10).
        assert_eq!(
            transform_value(&Transform::Truncate(10), &DataType::Int64, "123"),
            Some(PartVal::Int(120))
        );
        // truncate with a negative uses floor semantics: truncate[10](-1) = -10.
        assert_eq!(
            transform_value(&Transform::Truncate(10), &DataType::Int64, "-1"),
            Some(PartVal::Int(-10))
        );
        // truncate[3] of "hello" → "hel".
        assert_eq!(
            transform_value(&Transform::Truncate(3), &DataType::Utf8, "hello"),
            Some(PartVal::Str("hel".into()))
        );
        assert_eq!(
            transform_value(&Transform::Identity, &DataType::Int64, "42"),
            Some(PartVal::Int(42))
        );
    }

    #[test]
    fn temporal_transforms_use_epoch_ordinals() {
        // day(date) = days since 1970-01-01.
        assert_eq!(
            transform_value(&Transform::Day, &DataType::Date32, "1970-01-01"),
            Some(PartVal::Int(0))
        );
        assert_eq!(
            transform_value(&Transform::Day, &DataType::Date32, "1970-01-03"),
            Some(PartVal::Int(2))
        );
        // hour(timestamp) = hours since epoch.
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            transform_value(&Transform::Hour, &ts, "1970-01-01T00:00:00"),
            Some(PartVal::Int(0))
        );
        assert_eq!(
            transform_value(&Transform::Hour, &ts, "1970-01-02T01:00:00"),
            Some(PartVal::Int(25))
        );
        // day(timestamp) floors to the day.
        assert_eq!(
            transform_value(&Transform::Day, &ts, "1970-01-02T23:59:59"),
            Some(PartVal::Int(1))
        );
    }

    /// Helper: an equality delete carrying a partition tuple.
    fn eq_delete(partition: Vec<Option<PartVal>>) -> EqualityDelete {
        EqualityDelete {
            field_ids: vec![1],
            keys: HashSet::new(),
            seq: 10,
            partition,
        }
    }

    /// An unpartitioned delete spec applies table-wide — the pre-existing behaviour, and the
    /// only case in which a delete legitimately reaches every file.
    #[test]
    fn unpartitioned_equality_delete_applies_everywhere() {
        let d = eq_delete(vec![]);
        assert!(d.applies_to_partition(&[]));
        assert!(d.applies_to_partition(&[Some(PartVal::Str("EU".into()))]));
        assert!(d.applies_to_partition(&[Some(PartVal::Int(3)), None]));
    }

    /// The bug this fixes: a delete written for one partition must not delete matching keys out
    /// of another. Before the partition-scope check, both of these returned true.
    #[test]
    fn equality_delete_does_not_cross_partitions() {
        let eu = eq_delete(vec![Some(PartVal::Str("EU".into()))]);
        assert!(eu.applies_to_partition(&[Some(PartVal::Str("EU".into()))]));
        assert!(!eu.applies_to_partition(&[Some(PartVal::Str("US".into()))]));

        let bucket7 = eq_delete(vec![Some(PartVal::Int(7))]);
        assert!(bucket7.applies_to_partition(&[Some(PartVal::Int(7))]));
        assert!(!bucket7.applies_to_partition(&[Some(PartVal::Int(8))]));
    }

    /// Multi-field specs: differing in ANY decoded slot is proof enough to skip.
    #[test]
    fn equality_delete_multi_field_partition() {
        let d = eq_delete(vec![
            Some(PartVal::Str("EU".into())),
            Some(PartVal::Int(2026)),
        ]);
        assert!(
            d.applies_to_partition(&[Some(PartVal::Str("EU".into())), Some(PartVal::Int(2026))])
        );
        assert!(
            !d.applies_to_partition(&[Some(PartVal::Str("EU".into())), Some(PartVal::Int(2025))])
        );
        assert!(
            !d.applies_to_partition(&[Some(PartVal::Str("US".into())), Some(PartVal::Int(2026))])
        );
    }

    /// The conservative direction, stated as a test because it is a deliberate asymmetry: an
    /// undecoded slot (`None` — a null value, or a partition type this reader does not model) is
    /// "unknown", never "equal". Unknown keeps the delete, because resurfacing a deleted row is
    /// the failure this module refuses elsewhere, while dropping a live row is recoverable.
    #[test]
    fn undecoded_partition_slots_keep_the_delete() {
        let d = eq_delete(vec![None]);
        assert!(d.applies_to_partition(&[Some(PartVal::Str("EU".into()))]));

        let d = eq_delete(vec![Some(PartVal::Str("EU".into()))]);
        assert!(d.applies_to_partition(&[None]));
        assert!(
            d.applies_to_partition(&[None, Some(PartVal::Int(1))]),
            "arity mismatch keeps it"
        );
    }

    /// Same-version metadata files must resolve identically on every run and every filesystem.
    /// `<version>-<uuid>` naming puts several files at one version, and before the tie-break the
    /// winner was whichever `read_dir` yielded first.
    #[test]
    fn current_metadata_tie_breaks_deterministically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mdir = dir.path().join("metadata");
        std::fs::create_dir_all(&mdir).expect("mkdir");
        // Written in an order that does not match the sorted order, so a read_dir-order
        // implementation has a good chance of disagreeing with the expected answer.
        for name in [
            "00003-ffff.metadata.json",
            "00002-zzzz.metadata.json",
            "00003-aaaa.metadata.json",
            "00003-cccc.metadata.json",
        ] {
            std::fs::write(mdir.join(name), "{}").expect("write");
        }
        let chosen = current_metadata(dir.path()).expect("resolve");
        assert_eq!(
            chosen.file_name().and_then(|n| n.to_str()),
            Some("00003-ffff.metadata.json"),
            "highest version, then greatest file name"
        );
        // And it is stable across repeated calls.
        for _ in 0..5 {
            assert_eq!(current_metadata(dir.path()).expect("resolve"), chosen);
        }
    }

    /// `version-hint.text` still wins over the fallback — the tie-break is only for its absence.
    #[test]
    fn version_hint_beats_the_tie_break() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mdir = dir.path().join("metadata");
        std::fs::create_dir_all(&mdir).expect("mkdir");
        std::fs::write(mdir.join("v7.metadata.json"), "{}").expect("write");
        std::fs::write(mdir.join("v9.metadata.json"), "{}").expect("write");
        std::fs::write(mdir.join("version-hint.text"), "7\n").expect("write");
        assert_eq!(
            current_metadata(dir.path())
                .expect("resolve")
                .file_name()
                .and_then(|n| n.to_str()),
            Some("v7.metadata.json"),
        );
    }
}

#[cfg(test)]
mod partition_spec_identity_tests {
    use super::*;

    /// One spec, or v1's single `partition-spec`, means a tuple is attributable; two specs mean it
    /// is not, and the reader must stop trusting tuple comparisons.
    #[test]
    fn spec_uniqueness_is_read_from_the_metadata() {
        let one = serde_json::json!({ "partition-specs": [{ "spec-id": 0, "fields": [] }] });
        assert!(has_single_partition_spec(&one));

        let evolved = serde_json::json!({
            "partition-specs": [
                { "spec-id": 0, "fields": [] },
                { "spec-id": 1, "fields": [] },
            ]
        });
        assert!(
            !has_single_partition_spec(&evolved),
            "a table with two specs cannot attribute a bare partition tuple to either"
        );

        // v1 carries one `partition-spec` and no history of them; absence is one spec, not unknown.
        let v1 =
            serde_json::json!({ "partition-spec": [{ "source-id": 1, "transform": "identity" }] });
        assert!(has_single_partition_spec(&v1));
    }

    /// The conservative value has to be the `Default`, because a plan built any other way than by
    /// `plan_inner` must not be assumed to have a stable spec.
    #[test]
    fn the_default_plan_does_not_claim_a_unique_spec() {
        assert!(!TablePlan::default().single_partition_spec);
    }
}

#[cfg(test)]
mod metadata_tie_tests {
    use super::*;

    fn table_with(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let mdir = dir.path().join("metadata");
        std::fs::create_dir_all(&mdir).expect("mkdir");
        for (name, body) in files {
            std::fs::write(mdir.join(name), body).expect("write");
        }
        dir
    }

    /// Duplicates that name the same snapshot describe one state, so reading either is correct.
    /// Refusing these would reject well-defined tables over a leftover file.
    #[test]
    fn tied_metadata_naming_the_same_snapshot_still_reads() {
        let dir = table_with(&[
            ("00007-aaaa.metadata.json", r#"{"current-snapshot-id": 42}"#),
            ("00007-bbbb.metadata.json", r#"{"current-snapshot-id": 42}"#),
        ]);
        let chosen = current_metadata(dir.path()).expect("agreeing duplicates must resolve");
        assert_eq!(
            chosen.file_name().and_then(|n| n.to_str()),
            Some("00007-bbbb.metadata.json"),
            "still the deterministic choice: highest version, then greatest file name"
        );
    }

    /// Two files at the same version naming *different* snapshots leave nothing in the directory
    /// to say which the table is at. The old code warned on stderr and read one anyway; under
    /// `serve` that reaches a log nobody reads per request while the caller gets rows from a
    /// possibly-wrong snapshot. It must refuse instead.
    #[test]
    fn tied_metadata_naming_different_snapshots_is_refused() {
        let dir = table_with(&[
            ("00007-aaaa.metadata.json", r#"{"current-snapshot-id": 42}"#),
            ("00007-bbbb.metadata.json", r#"{"current-snapshot-id": 99}"#),
        ]);
        let err = current_metadata(dir.path()).expect_err("a contradictory tie must not resolve");
        let msg = err.to_string();
        assert!(
            msg.contains("different current snapshots"),
            "the error must name the actual problem: {msg}"
        );
        assert!(
            msg.contains("version-hint.text"),
            "and the remedy the reader needs: {msg}"
        );
    }

    /// `version-hint.text` is authoritative, so a contradiction below it is not consulted at all.
    #[test]
    fn a_version_hint_resolves_what_would_otherwise_be_a_contradiction() {
        let dir = table_with(&[
            ("00007-aaaa.metadata.json", r#"{"current-snapshot-id": 42}"#),
            ("00007-bbbb.metadata.json", r#"{"current-snapshot-id": 99}"#),
            ("v7.metadata.json", r#"{"current-snapshot-id": 7}"#),
            ("version-hint.text", "7\n"),
        ]);
        assert_eq!(
            current_metadata(dir.path())
                .expect("the hint decides")
                .file_name()
                .and_then(|n| n.to_str()),
            Some("v7.metadata.json")
        );
    }
}
