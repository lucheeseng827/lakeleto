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
//! - `{"protocol"|"txn"|"commitInfo": …}` — ignored.
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

/// Resolve the table's current read plan by replaying every `*.json` commit in `_delta_log/` in
/// ascending version order: apply `metaData` (latest wins), collect `add` paths, drop any later
/// `remove`d path. Errors if the directory is not a Delta table (no `_delta_log/`), has no
/// commits, or never declares a `metaData`.
pub fn plan(table_dir: &Path) -> Result<TablePlan> {
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
    // Keyed by the RAW (still URL-encoded) `path` string from the log so an `add` and its later
    // `remove` — which carry the identical string — match exactly. Decoded only when the final
    // filesystem path is built. `BTreeMap` gives a deterministic file order.
    let mut active: BTreeMap<String, HashMap<String, Option<String>>> = BTreeMap::new();

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
            } else if let Some(add) = action.get("add") {
                if let Some(path) = add.get("path").and_then(|v| v.as_str()) {
                    active.insert(
                        path.to_string(),
                        parse_partition_values(add.get("partitionValues")),
                    );
                }
            } else if let Some(remove) = action.get("remove") {
                if let Some(path) = remove.get("path").and_then(|v| v.as_str()) {
                    active.remove(path);
                }
            }
            // protocol / txn / commitInfo / anything else → ignored.
        }
    }

    let schema = schema.ok_or_else(|| EngineError::UnsupportedFormat {
        detail: format!(
            "delta: {} declares no metaData (schema) in its log",
            log_dir.display()
        ),
    })?;
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

/// The table's live row count: the sum of the active data files' Parquet footer row counts. Delta
/// has no row-level deletes in this reader's scope (a `remove` drops a whole file), so the footer
/// sum is exact.
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
