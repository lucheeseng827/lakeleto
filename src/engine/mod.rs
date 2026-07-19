//! The `Engine` trait — the one seam the whole product is built around.
//!
//! #25 Lakeleto's thesis is "the engine is a commodity, the value is the UX." So Lakeleto defines
//! a single [`Engine`] trait and every backend implements it:
//!
//! | backend | feature | reads | SQL | role |
//! |---------|---------|-------|-----|------|
//! | [`local::LocalReaderEngine`] | *(default)* | Parquet, CSV | no | the pure-Rust MVP engine |
//! | `sql::DataFusionEngine` | `sql` | Parquet, CSV | yes | the SQL power engine |
//! | `remote::RemoteEngine` | `remote` | (server-defined) | yes | the **Lakeleto Cloud** seam |
//!
//! The (future) UI — a localhost SPA per the ROADMAP (egui/Tauri stays an option for a
//! native shell) — is meant to hold a `Box<dyn Engine>` and never name a
//! concrete engine — so swapping the local engine for DuckDB, or adding the hosted engine,
//! is additive, not a rewrite. That is the concrete answer to "which engine first when we
//! build the UI": the **local** engine, with the hosted engine dropping in behind this trait
//! later.

pub mod local;

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "remote")]
pub mod remote;

use std::collections::HashSet;

use arrow_array::{Array, BooleanArray, Float64Array, RecordBatch, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, SchemaRef, SortOptions};
use serde::Serialize;

use crate::error::{EngineError, Result};
use crate::source::Source;

/// One column's declared shape.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// A table's schema plus a little provenance (which engine, which source, how many rows).
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct TableSchema {
    pub source: String,
    pub format: String,
    pub engine: String,
    /// Total row count when the engine can supply it cheaply (Parquet footer); else `None`.
    pub row_count: Option<u64>,
    pub columns: Vec<ColumnSchema>,
}

/// A batch of rows, still in native Arrow form. Rendering lives in [`crate::render`].
pub struct RowBatch {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl RowBatch {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// The first `n` rows (zero-copy `RecordBatch::slice`s). Used to enforce result caps.
    pub fn first(&self, n: usize) -> RowBatch {
        let mut batches = Vec::new();
        let mut remaining = n;
        for b in &self.batches {
            if remaining == 0 {
                break;
            }
            let take = b.num_rows().min(remaining);
            batches.push(b.slice(0, take));
            remaining -= take;
        }
        RowBatch {
            schema: self.schema.clone(),
            batches,
        }
    }
}

/// A single column's profile from a bounded scan (approximate for distinct/min/max when the
/// scan window is smaller than the table).
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: String,
    pub null_count: u64,
    pub null_fraction: f64,
    /// Distinct values observed in the scan window (`distinct_capped` = the cap was hit).
    pub distinct: u64,
    pub distinct_capped: bool,
    pub min: Option<String>,
    pub max: Option<String>,
    pub sample: Vec<String>,
}

/// A whole-table profile: the per-column stats plus how much was actually scanned.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct TableProfile {
    pub source: String,
    pub engine: String,
    pub row_count: Option<u64>,
    pub scanned_rows: u64,
    pub columns: Vec<ColumnProfile>,
}

/// What an engine can do — surfaced by `lakeleto engines` and (later) used by the UI to
/// enable/disable affordances instead of hard-coding engine names.
#[derive(Debug, Clone, Serialize)]
pub struct Capabilities {
    pub engine: String,
    pub formats: Vec<String>,
    pub sql: bool,
    pub profile: bool,
    pub remote: bool,
}

/// A source registered under a name, for multi-table SQL (`FROM orders JOIN customers`).
pub struct NamedSource {
    pub name: String,
    pub source: Source,
}

/// The seam. Every backend implements this; the UI binds to `dyn Engine`.
///
/// `Send + Sync` so an `Arc<dyn Engine>` can be shared across the HTTP server's request
/// handlers (the `serve` feature).
pub trait Engine: Send + Sync {
    /// Short, stable identifier (`"local"`, `"sql"`, `"remote"`).
    fn name(&self) -> &str;

    fn capabilities(&self) -> Capabilities;

    /// Read just the schema (+ cheap row count) without scanning data.
    fn schema(&self, source: &Source) -> Result<TableSchema>;

    /// Read the first `limit` rows.
    fn preview(&self, source: &Source, limit: usize) -> Result<RowBatch>;

    /// Profile columns from a bounded scan of up to `scan_limit` rows.
    fn profile(&self, source: &Source, scan_limit: usize) -> Result<TableProfile>;

    /// Run SQL over the named tables. Default: unsupported (the local reader has no planner).
    fn query(&self, _sql: &str, _tables: &[NamedSource]) -> Result<RowBatch> {
        Err(crate::error::EngineError::UnsupportedOperation {
            engine: self.name().to_string(),
            op: "run SQL".to_string(),
            hint: "use `--engine sql` (build with `--features sql`) or `--engine remote`"
                .to_string(),
        })
    }

    /// Like [`Engine::query`] but bounded: the result never exceeds `cap` rows. Engines that can
    /// should push the cap *into the plan* (the SQL engine adds a plan-level `LIMIT`, so a
    /// `SELECT *` over a huge table never materializes unbounded); the default runs `query` and
    /// trims afterwards, which is correct but only bounds what the caller sees.
    fn query_capped(&self, sql: &str, tables: &[NamedSource], cap: usize) -> Result<RowBatch> {
        Ok(self.query(sql, tables)?.first(cap))
    }

    /// Windowed scan for the grid: filter → sort → `offset`/`limit` a row range. Default:
    /// unsupported (implemented by the local reader via Arrow kernels, and the SQL engine).
    fn scan(&self, _source: &Source, _spec: &ScanSpec) -> Result<ScanResult> {
        Err(crate::error::EngineError::UnsupportedOperation {
            engine: self.name().to_string(),
            op: "windowed scan".to_string(),
            hint: "the grid scan (sort/filter/window) needs the local or sql engine".to_string(),
        })
    }

    /// Profile columns over the *filtered* view (the grid's current filter). Default:
    /// falls back to profiling the whole source, ignoring filters.
    fn stats(
        &self,
        source: &Source,
        _filters: &[FilterSpec],
        scan_limit: usize,
    ) -> Result<TableProfile> {
        self.profile(source, scan_limit)
    }
}

/// A comparison used by a column filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Contains,
}

impl FilterOp {
    pub fn parse(s: &str) -> Option<FilterOp> {
        Some(match s {
            "eq" | "=" => FilterOp::Eq,
            "ne" | "!=" => FilterOp::Ne,
            "lt" | "<" => FilterOp::Lt,
            "le" | "<=" => FilterOp::Le,
            "gt" | ">" => FilterOp::Gt,
            "ge" | ">=" => FilterOp::Ge,
            "contains" | "~" => FilterOp::Contains,
            _ => return None,
        })
    }
}

/// One column filter (`column op value`), all ANDed together in a [`ScanSpec`].
#[derive(Debug, Clone)]
pub struct FilterSpec {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
}

/// Sort the (filtered) rows by one column before windowing.
#[derive(Debug, Clone)]
pub struct SortSpec {
    pub column: String,
    pub descending: bool,
}

/// A grid request: which rows/columns to return after filtering + sorting.
#[derive(Debug, Clone, Default)]
pub struct ScanSpec {
    pub offset: usize,
    pub limit: usize,
    pub sort: Option<SortSpec>,
    pub filters: Vec<FilterSpec>,
    /// Column projection (names, in display order). `None` = all columns.
    pub projection: Option<Vec<String>>,
}

impl ScanSpec {
    /// A plain window is a straight `offset..limit` read — no sort, no filter. (Projection is
    /// applied on top and does not, by itself, need the sort/filter engine.)
    pub fn is_plain_window(&self) -> bool {
        self.sort.is_none() && self.filters.is_empty()
    }
}

/// Project a [`RowBatch`] down to `cols` (by name, in order). `None` returns it unchanged.
pub fn project_rows(rb: RowBatch, cols: Option<&[String]>) -> Result<RowBatch> {
    let Some(cols) = cols else { return Ok(rb) };
    let indices: Vec<usize> = cols
        .iter()
        .filter_map(|name| rb.schema.index_of(name).ok())
        .collect();
    if indices.is_empty() {
        return Ok(rb);
    }
    let mut batches = Vec::with_capacity(rb.batches.len());
    for b in &rb.batches {
        batches.push(b.project(&indices).map_err(EngineError::arrow)?);
    }
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| std::sync::Arc::new(rb.schema.project(&indices).unwrap()));
    Ok(RowBatch { schema, batches })
}

/// Result of a [`Engine::scan`]: the row window plus enough counts to drive a virtual scrollbar.
pub struct ScanResult {
    pub batch: RowBatch,
    /// Rows the scrollbar should size to: the file's total (plain window) or the number of
    /// rows matching the filter (within the scanned set when `bounded`).
    pub matched_rows: usize,
    /// Whether `matched_rows` is exact. `false` for a CSV plain-scroll (no cheap total) or a
    /// `bounded` sort/filter — the UI then treats it as a lower bound and grows as it pages.
    pub total_known: bool,
    /// How many rows the engine actually read.
    pub scanned_rows: usize,
    /// True when sort/filter ran over a capped working set (result may be partial for huge files).
    pub bounded: bool,
    pub offset: usize,
}

/// Skip `offset` rows across a batch list, then take `limit` — the CSV plain-window path.
pub fn window_batches(batches: Vec<RecordBatch>, offset: usize, limit: usize) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut to_skip = offset;
    let mut remaining = limit;
    for b in batches {
        if remaining == 0 {
            break;
        }
        let rows = b.num_rows();
        if to_skip >= rows {
            to_skip -= rows;
            continue;
        }
        let start = to_skip;
        let take = (rows - start).min(remaining);
        out.push(b.slice(start, take));
        remaining -= take;
        to_skip = 0;
    }
    out
}

// ---------------------------------------------------------------------------------------
// Shared helpers reused by every engine so profiling/schema logic lives in exactly one place.
// ---------------------------------------------------------------------------------------

/// Build a [`TableSchema`] from an Arrow schema.
pub fn build_table_schema(
    source: &Source,
    engine: &str,
    row_count: Option<u64>,
    schema: &SchemaRef,
) -> TableSchema {
    let columns = schema
        .fields()
        .iter()
        .map(|f| ColumnSchema {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();
    TableSchema {
        source: source.display(),
        format: source.format.as_str().to_string(),
        engine: engine.to_string(),
        row_count,
        columns,
    }
}

/// Slice a batch list down to exactly `limit` rows total (used to trim a final over-read batch).
pub fn truncate_batches(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut remaining = limit;
    for b in batches {
        if remaining == 0 {
            break;
        }
        if b.num_rows() <= remaining {
            remaining -= b.num_rows();
            out.push(b);
        } else {
            out.push(b.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

const DISTINCT_CAP: usize = 50_000;
const SAMPLE_N: usize = 5;

/// Column profiling over a set of already-read batches. Engine-agnostic: the local reader,
/// the DataFusion engine, and (a decoded) remote engine all funnel through here so the
/// stats are computed identically no matter who read the bytes.
pub fn profile_columns(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<ColumnProfile> {
    let opts = FormatOptions::default();
    let mut out = Vec::with_capacity(schema.fields().len());

    for (ci, field) in schema.fields().iter().enumerate() {
        let class = num_class(field.data_type());

        let mut null_count: u64 = 0;
        let mut total: u64 = 0;
        let mut distinct: HashSet<String> = HashSet::new();
        let mut distinct_capped = false;
        let mut sample: Vec<String> = Vec::new();

        // Track min/max in the column's own domain so large Int64 IDs / epoch-nanos above
        // 2^53 are not rounded (an f64 accumulator would silently collapse them).
        let mut min_i: Option<i128> = None;
        let mut max_i: Option<i128> = None;
        let mut min_f: Option<f64> = None;
        let mut max_f: Option<f64> = None;
        let mut min_str: Option<String> = None;
        let mut max_str: Option<String> = None;

        for batch in batches {
            let col = batch.column(ci);
            let fmt = ArrayFormatter::try_new(col.as_ref(), &opts).ok();
            for row in 0..col.len() {
                total += 1;
                if col.is_null(row) {
                    null_count += 1;
                    continue;
                }
                let s = match &fmt {
                    Some(f) => f.value(row).try_to_string().unwrap_or_default(),
                    None => String::new(),
                };

                if distinct.len() < DISTINCT_CAP {
                    distinct.insert(s.clone());
                } else {
                    distinct_capped = true;
                }
                if sample.len() < SAMPLE_N {
                    sample.push(s.clone());
                }

                match class {
                    NumClass::Int => {
                        if let Ok(v) = s.parse::<i128>() {
                            min_i = Some(min_i.map_or(v, |m| m.min(v)));
                            max_i = Some(max_i.map_or(v, |m| m.max(v)));
                        }
                    }
                    NumClass::Float => {
                        // Skip NaN/±inf so an all-NaN column reports no min/max (not "inf").
                        if let Ok(v) = s.parse::<f64>() {
                            if v.is_finite() {
                                min_f = Some(min_f.map_or(v, |m| m.min(v)));
                                max_f = Some(max_f.map_or(v, |m| m.max(v)));
                            }
                        }
                    }
                    NumClass::Other => {
                        // Lexicographic min/max (correct for ISO dates/timestamps and strings).
                        if min_str.as_ref().is_none_or(|m| &s < m) {
                            min_str = Some(s.clone());
                        }
                        if max_str.as_ref().is_none_or(|m| &s > m) {
                            max_str = Some(s.clone());
                        }
                    }
                }
            }
        }

        let (min, max) = match class {
            NumClass::Int => (min_i.map(|v| v.to_string()), max_i.map(|v| v.to_string())),
            NumClass::Float => (min_f.map(fmt_num), max_f.map(fmt_num)),
            NumClass::Other => (min_str, max_str),
        };

        let null_fraction = if total == 0 {
            0.0
        } else {
            null_count as f64 / total as f64
        };

        out.push(ColumnProfile {
            name: field.name().clone(),
            data_type: format!("{}", field.data_type()),
            null_count,
            null_fraction,
            distinct: distinct.len() as u64,
            distinct_capped,
            min,
            max,
            sample,
        });
    }

    out
}

/// How a column's min/max should be accumulated.
enum NumClass {
    /// Exact integers — tracked as `i128` (holds all Int64/UInt64 without rounding).
    Int,
    /// Floats and decimals — tracked as `f64` (finite values only).
    Float,
    /// Everything else — lexicographic on the formatted value (ISO dates/timestamps sort right).
    Other,
}

fn num_class(dt: &DataType) -> NumClass {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => NumClass::Int,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => NumClass::Float,
        _ => NumClass::Other,
    }
}

/// Render an `f64` back without a spurious `.0` for integer-valued numbers.
fn fmt_num(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

// ---------------------------------------------------------------------------------------
// Grid scan: filter → sort → window over a set of batches, all via Arrow compute kernels.
// Shared by the local engine (its default path) and the SQL engine.
// ---------------------------------------------------------------------------------------

/// Apply a [`ScanSpec`] to already-read batches: filter (Arrow `filter`), sort (Arrow
/// `sort_to_indices` + `take`), then slice the `offset..offset+limit` window. Returns the
/// window and the count of rows matching the filter.
pub fn apply_scan(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    spec: &ScanSpec,
) -> Result<(RowBatch, usize)> {
    let combined = if batches.is_empty() {
        RecordBatch::new_empty(schema.clone())
    } else {
        arrow_select::concat::concat_batches(schema, batches).map_err(EngineError::arrow)?
    };

    let filtered = if spec.filters.is_empty() {
        combined
    } else {
        let mask = combined_mask(&combined, &spec.filters)?;
        arrow_select::filter::filter_record_batch(&combined, &mask).map_err(EngineError::arrow)?
    };
    let matched = filtered.num_rows();

    let ordered = match &spec.sort {
        None => filtered,
        Some(s) => {
            let col = filtered.column_by_name(&s.column).ok_or_else(|| {
                EngineError::Other(format!("sort column `{}` not found", s.column))
            })?;
            let options = SortOptions {
                descending: s.descending,
                nulls_first: false,
            };
            let idx = arrow_ord::sort::sort_to_indices(col, Some(options), None)
                .map_err(EngineError::arrow)?;
            arrow_select::take::take_record_batch(&filtered, &idx).map_err(EngineError::arrow)?
        }
    };

    let start = spec.offset.min(ordered.num_rows());
    let len = spec.limit.min(ordered.num_rows() - start);
    let window = ordered.slice(start, len);
    let schema = window.schema();
    Ok((
        RowBatch {
            schema,
            batches: vec![window],
        },
        matched,
    ))
}

/// Concatenate `batches` and apply `filters` (Arrow `filter` kernel). Returns the filtered
/// schema + a single batch. With no filters, returns the concatenated batch unchanged.
pub fn filter_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    filters: &[FilterSpec],
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let combined = if batches.is_empty() {
        RecordBatch::new_empty(schema.clone())
    } else {
        arrow_select::concat::concat_batches(schema, batches).map_err(EngineError::arrow)?
    };
    if filters.is_empty() {
        let s = combined.schema();
        return Ok((s, vec![combined]));
    }
    let mask = combined_mask(&combined, filters)?;
    let filtered =
        arrow_select::filter::filter_record_batch(&combined, &mask).map_err(EngineError::arrow)?;
    let s = filtered.schema();
    Ok((s, vec![filtered]))
}

/// AND together one boolean mask per filter.
fn combined_mask(batch: &RecordBatch, filters: &[FilterSpec]) -> Result<BooleanArray> {
    let mut acc: Option<BooleanArray> = None;
    for f in filters {
        let col = batch
            .column_by_name(&f.column)
            .ok_or_else(|| EngineError::Other(format!("filter column `{}` not found", f.column)))?;
        let m = column_mask(col, f.op, &f.value)?;
        acc = Some(match acc {
            None => m,
            Some(prev) => and_mask(&prev, &m),
        });
    }
    acc.ok_or_else(|| EngineError::Other("no filters".to_string()))
}

/// Build a boolean mask for `column op value`, pushing the comparison to Arrow's `cmp`
/// kernels (numeric columns compared as f64; everything else lexically as Utf8).
fn column_mask(column: &arrow_array::ArrayRef, op: FilterOp, value: &str) -> Result<BooleanArray> {
    let numeric = matches!(
        num_class(column.data_type()),
        NumClass::Int | NumClass::Float
    );
    if numeric {
        if let Ok(v) = value.parse::<f64>() {
            let casted =
                arrow_cast::cast(column, &DataType::Float64).map_err(EngineError::arrow)?;
            let col = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| EngineError::Other("numeric cast failed".to_string()))?;
            let scalar = Float64Array::new_scalar(v);
            return cmp_apply(op, col, &scalar);
        }
        // Non-numeric filter value against a numeric column: fall through to string compare.
    }
    let casted = arrow_cast::cast(column, &DataType::Utf8).map_err(EngineError::arrow)?;
    let col = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EngineError::Other("utf8 cast failed".to_string()))?;
    if op == FilterOp::Contains {
        return Ok(col
            .iter()
            .map(|opt| Some(opt.is_some_and(|s| s.contains(value))))
            .collect());
    }
    let scalar = StringArray::new_scalar(value);
    cmp_apply(op, col, &scalar)
}

/// Dispatch a comparison op to the matching Arrow `cmp` kernel.
fn cmp_apply<T>(op: FilterOp, lhs: &T, rhs: &arrow_array::Scalar<T>) -> Result<BooleanArray>
where
    T: arrow_array::Array + arrow_array::Datum,
{
    use arrow_ord::cmp;
    let r = match op {
        // `Contains` on a numeric column has no substring sense — treat it as equality.
        FilterOp::Eq | FilterOp::Contains => cmp::eq(lhs, rhs),
        FilterOp::Ne => cmp::neq(lhs, rhs),
        FilterOp::Lt => cmp::lt(lhs, rhs),
        FilterOp::Le => cmp::lt_eq(lhs, rhs),
        FilterOp::Gt => cmp::gt(lhs, rhs),
        FilterOp::Ge => cmp::gt_eq(lhs, rhs),
    };
    r.map_err(EngineError::arrow)
}

/// Element-wise AND, treating nulls as `false` (a null never passes a filter).
fn and_mask(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    (0..a.len().min(b.len()))
        .map(|i| {
            let av = !a.is_null(i) && a.value(i);
            let bv = !b.is_null(i) && b.value(i);
            Some(av && bv)
        })
        .collect()
}
