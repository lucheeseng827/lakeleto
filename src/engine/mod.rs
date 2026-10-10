//! The `Engine` trait — the one seam the whole product is built around.
//!
//! #25 Lakeleto's thesis is "the engine is a commodity, the value is the UX." So Lakeleto defines
//! a single [`Engine`] trait and every backend implements it:
//!
//! | backend | feature | reads | SQL | role |
//! |---------|---------|-------|-----|------|
//! | [`local::LocalReaderEngine`] | *(default)* | Parquet, CSV | no | the pure-Rust MVP engine |
//! | `sql::DataFusionEngine` | `sql` | Parquet, CSV/TSV natively; JSON, Iceberg, Delta through the local reader | yes | the SQL power engine |
//! | `remote::RemoteEngine` | `remote` | the server's, from `GET /v1/engines` | the server's | the **Lakeleto Cloud** seam |
//!
//! Which of a process's engines answers a request is [`registry::EngineRegistry`]'s decision,
//! for the CLI and the API alike.
//!
//! The (future) UI — a localhost SPA per the ROADMAP (egui/Tauri stays an option for a
//! native shell) — is meant to hold a `Box<dyn Engine>` and never name a
//! concrete engine — so swapping the local engine for DuckDB, or adding the hosted engine,
//! is additive, not a rewrite. That is the concrete answer to "which engine first when we
//! build the UI": the **local** engine, with the hosted engine dropping in behind this trait
//! later.

pub mod flatten;
pub mod local;
pub mod registry;

// Self-contained Delta Lake reader (JSON transaction log → active Parquet files).
#[cfg(feature = "delta")]
pub mod delta;

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "remote")]
pub mod remote;

// BYO-database engine (read-only, over sqlx). Compiled when any DB backend feature is on; the
// module body is `#![cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]` and
// dispatches per dialect at runtime, so the declaration and every caller gate on the same set.
#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
pub mod database;

use std::collections::HashSet;

use arrow_array::{Array, BooleanArray, Float64Array, RecordBatch, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, SchemaRef, SortOptions};
use serde::{Deserialize, Serialize};

use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::{Format, Source};

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
    /// Where inside the file the rows were read from, as a JSON Pointer (`/data`) — set when a
    /// JSON document's records member was unwrapped, so the rows' origin is never silent.
    /// Omitted otherwise, and defaulted when a peer that predates it omits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_path: Option<String>,
    /// Whose credentials read the files, for a table a catalog serves from an object store:
    /// `vended` (by the catalog, for this table), `catalog` (the storage keys configured for it) or
    /// `ambient` (this machine's own). Omitted for every other source, and defaulted when a peer
    /// that predates it omits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<String>,
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
///
/// **`Deserialize` is as load-bearing as `Serialize`.** Without it a client could publish its
/// own capabilities and never read a peer's, which is why [`remote::RemoteEngine`] *fabricates*
/// a `Capabilities` describing what it hopes the far side can do rather than reporting what the
/// far side said. Round-tripping is what lets a caller ask instead of probe — the difference
/// between planning against a peer and discovering its gaps by collecting 501s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capabilities {
    pub engine: String,
    pub formats: Vec<String>,
    pub sql: bool,
    pub profile: bool,
    pub remote: bool,
    /// Does this engine implement [`Engine::scan`] — the windowed grid read with sort, filter
    /// and projection? `false` means the trait default answers `UnsupportedOperation`, i.e. the
    /// grid cannot be served by this engine at all.
    ///
    /// This bit exists because its absence was actively misleading: the fields above advertise
    /// `sql` and `profile`, both of which the remote engine really does, so a client reasonably
    /// concluded it could drive a grid and then took a 501 on the first scroll.
    #[serde(default)]
    pub scan: bool,
    /// Does [`Engine::stats`] honour the filters it is handed?
    ///
    /// Deliberately **not** "does `stats` answer". Every engine answers: the trait default
    /// discards `filters` and returns a whole-table [`Engine::profile`], so an engine without
    /// this bit reports a profile of the *table* while the caller asked about the *filtered
    /// view*. A wrong answer is worse than a refusal, so the capability names the property that
    /// matters — the filters were applied — rather than the method's mere existence.
    #[serde(default)]
    pub filtered_stats: bool,
}

/// The formats a file-reading engine can actually open in **this** build.
///
/// Both the local reader and the SQL engine answered a hardcoded `["parquet", "csv"]` regardless
/// of which features were compiled in, so a binary built with `iceberg,delta,object-store` told
/// `GET /v1/engines` — and therefore the SPA, and `lakeleto engines` — that it could read two
/// formats when it could read six. The list is derived from the feature flags instead, so it
/// cannot drift from what the build can do.
///
/// This is the **local** reader's set: Parquet, which it reads itself, then every format in the
/// `crate::format` registry, then the table formats compiled in — so a reader added to the
/// registry is listed without an edit here. The SQL engine reports its own list,
/// [`sql_readable_formats`], of what it can register. The two coincide today, because SQL reads
/// what DataFusion cannot through the local reader, but they answer different questions.
pub fn readable_formats() -> Vec<String> {
    let mut formats = vec![Format::Parquet];
    formats.extend(crate::format::formats());
    if cfg!(feature = "iceberg") {
        formats.push(Format::Iceberg);
    }
    if cfg!(feature = "delta") {
        formats.push(Format::Delta);
    }
    // `object-store` is deliberately NOT in this list: it is a source of files, not a format of
    // them, and a client iterating formats to build an "openable types" list would choke on it.
    // The capability is already visible on its own terms — `lakeleto engines` gives it a row, and
    // an `s3://` path answers with a clear rebuild-with-the-feature error when it is absent.
    formats.iter().map(|f| f.as_str().to_string()).collect()
}

/// Can the SQL engine register a source of this format as a table?
///
/// One predicate for the two things that must agree: `DataFusionEngine::register` refuses every
/// format this says no to, and the engine's `capabilities().formats` is [`sql_readable_formats`],
/// derived from it. The API routes a sorted or filtered grid window to SQL only when that list
/// names the source's format, so a capability that claimed more than `register` accepts sent
/// every sorted JSON grid to an engine that then refused it.
///
/// Compiled in every build, not just `sql`, so `lakeleto engines` can describe the SQL engine
/// truthfully in a binary that does not carry it. A registry format always registers — natively
/// or through the local reader, as its `FormatReader::sql` says — so only the formats named here
/// can be refused.
pub fn sql_registers(format: Format) -> bool {
    match format {
        // A native DataFusion listing table.
        Format::Parquet => true,
        // Read through the local reader into a `MemTable` (`register_via_local`), so only when
        // that reader was compiled with the format.
        Format::Iceberg => cfg!(feature = "iceberg"),
        Format::Delta => cfg!(feature = "delta"),
        // Not files: the `database` engine owns the first, and the second is never readable.
        Format::Database | Format::Unknown => false,
        // CSV and TSV natively, JSON through the local reader, and whatever the registry adds.
        registry => crate::format::reader(registry).is_some(),
    }
}

/// The formats the SQL engine can read in **this** build: [`readable_formats`] narrowed by
/// [`sql_registers`], in the same order.
pub fn sql_readable_formats() -> Vec<String> {
    readable_formats()
        .into_iter()
        .filter(|f| Format::parse(f).is_some_and(sql_registers))
        .collect()
}

/// A result delivered **incrementally**: the schema up front, then batches as they are produced.
///
/// # Why this exists
///
/// Every other read method returns a complete owned [`RowBatch`], and two consequences follow with
/// no further cause: the result is capped by RAM, and first-row latency equals *last*-row latency —
/// nothing can be shown, written or cancelled until the final split is read. A `SELECT *` over a
/// table larger than memory has no answer at all, however patient the caller is.
///
/// # Why an `Iterator` and not a `Stream`
///
/// [`Engine`] is synchronous, deliberately: the CLI and the rest of Lakeleto call it directly and
/// no `async` leaks across the seam. A synchronous consumer can only pull, so the streaming shape
/// of a synchronous trait is an iterator. The async engines bridge underneath exactly as they
/// already do for the buffered methods — `runtime().block_on(...)`, once per batch instead of once
/// per query — which means this adds **no new constraint**: like every existing method, it must be
/// driven from a thread that is not a Tokio worker (`spawn_blocking`, or the CLI's own thread).
/// Driving it from inside an async task panics with "Cannot start a runtime from within a runtime",
/// the same way `preview` already would.
///
/// The schema is available **before** the first batch, which is what an Arrow IPC writer and a CSV
/// header both need in order to emit anything at all.
///
/// # Backpressure
///
/// There is none to arrange: the consumer's `next()` *is* the pull. No queue, no spawned pump, no
/// bound to tune — a slow writer simply calls `next()` later, and the plan underneath stops
/// producing until it does.
pub struct RowStream {
    schema: SchemaRef,
    batches: Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
}

impl RowStream {
    pub fn new(
        schema: SchemaRef,
        batches: impl Iterator<Item = Result<RecordBatch>> + Send + 'static,
    ) -> Self {
        RowStream {
            schema,
            batches: Box::new(batches),
        }
    }

    /// A stream over an already-materialized result. The trait's default
    /// [`Engine::query_stream`] is this, and so is any engine that cannot produce incrementally.
    pub fn from_batch(rb: RowBatch) -> Self {
        RowStream::new(rb.schema, rb.batches.into_iter().map(Ok))
    }

    /// The result schema, known before any row is read.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Drain the whole stream into a [`RowBatch`].
    ///
    /// The bridge back for callers that genuinely want everything — and the line where the
    /// memory guarantee is given up, which is why it is a named method rather than something a
    /// caller falls into.
    pub fn collect_batch(self) -> Result<RowBatch> {
        let schema = self.schema.clone();
        let batches = self.batches.collect::<Result<Vec<_>>>()?;
        Ok(RowBatch { schema, batches })
    }
}

impl Iterator for RowStream {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next()
    }
}

impl std::fmt::Debug for RowStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
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
/// Every read method takes a [`RequestContext`] first: what the caller knows that the engine
/// cannot — how long it may run, whether anyone still wants the answer, who asked. It is
/// backend-neutral by construction (see [`crate::context`] for why that matters), and honouring
/// it is best-effort: an engine that ignores the context is still correct, just uninterruptible.
///
/// `ctx` comes first, consistently, because it describes the call rather than the data. A caller
/// with nothing to say passes [`RequestContext::detached`].
pub trait Engine: Send + Sync {
    /// Short, stable identifier (`"local"`, `"sql"`, `"remote"`).
    fn name(&self) -> &str;

    /// What this engine reads and does. A remote engine answers by asking its server, once, so
    /// call this off an async runtime's threads, as the other methods are.
    fn capabilities(&self) -> Capabilities;

    /// Read just the schema (+ cheap row count) without scanning data.
    fn schema(&self, ctx: &RequestContext, source: &Source) -> Result<TableSchema>;

    /// Read the first `limit` rows.
    fn preview(&self, ctx: &RequestContext, source: &Source, limit: usize) -> Result<RowBatch>;

    /// Profile columns from a bounded scan of up to `scan_limit` rows.
    fn profile(
        &self,
        ctx: &RequestContext,
        source: &Source,
        scan_limit: usize,
    ) -> Result<TableProfile>;

    /// Run SQL over the named tables. Default: unsupported (the local reader has no planner).
    fn query(
        &self,
        _ctx: &RequestContext,
        _sql: &str,
        _tables: &[NamedSource],
    ) -> Result<RowBatch> {
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
    fn query_capped(
        &self,
        ctx: &RequestContext,
        sql: &str,
        tables: &[NamedSource],
        cap: usize,
    ) -> Result<RowBatch> {
        Ok(self.query(ctx, sql, tables)?.first(cap))
    }

    /// Windowed scan for the grid: filter → sort → `offset`/`limit` a row range. Default:
    /// unsupported (implemented by the local reader via Arrow kernels, and the SQL engine).
    fn scan(
        &self,
        _ctx: &RequestContext,
        _source: &Source,
        _spec: &ScanSpec,
    ) -> Result<ScanResult> {
        Err(crate::error::EngineError::UnsupportedOperation {
            engine: self.name().to_string(),
            op: "windowed scan".to_string(),
            hint: "the grid scan (sort/filter/window) needs the local or sql engine".to_string(),
        })
    }

    /// [`Engine::query_capped`], delivered incrementally — see [`RowStream`].
    ///
    /// **Default: materialize, then hand back the batches.** Correct for every backend, and buys
    /// nothing: the whole result is in memory before the first `next()` returns. That is the right
    /// default precisely because it is honest — an engine that cannot produce incrementally
    /// (`database` fetches eagerly through `sqlx`; `remote` receives one complete IPC body) says so
    /// by not overriding, rather than by pretending through a seam that hides the difference.
    ///
    /// Only the SQL engine overrides it today, and only for `query`. `preview` and `scan` are
    /// bounded by construction — a screenful and a window — so streaming them would add a shape
    /// without removing a limit.
    /// `cap` is `None` for "everything the query asks for" — what [`Engine::query`] means, and what
    /// a CLI invocation on the user's own machine wants. A server passes `Some`.
    fn query_stream(
        &self,
        ctx: &RequestContext,
        sql: &str,
        tables: &[NamedSource],
        cap: Option<usize>,
    ) -> Result<RowStream> {
        let rb = match cap {
            Some(cap) => self.query_capped(ctx, sql, tables, cap)?,
            None => self.query(ctx, sql, tables)?,
        };
        Ok(RowStream::from_batch(rb))
    }

    /// Profile columns over the *filtered* view (the grid's current filter). Default:
    /// falls back to profiling the whole source, ignoring filters.
    fn stats(
        &self,
        ctx: &RequestContext,
        source: &Source,
        _filters: &[FilterSpec],
        scan_limit: usize,
    ) -> Result<TableProfile> {
        self.profile(ctx, source, scan_limit)
    }
}

/// A comparison used by a column filter.
///
/// The serde encoding is deliberately the **same vocabulary** [`FilterOp::parse`] accepts:
/// `rename_all = "lowercase"` produces exactly the canonical word form of every variant
/// (`eq`, `notcontains`, `startswith`, `isnull`, …), so a `FilterSpec` that travels as JSON and
/// one that travels as a `col:op:value` query param name their operators identically. A codec
/// whose two halves can drift is the risk this avoids; [`FilterOp::as_str`] is the inverse of
/// `parse`, and `filterop_wire_roundtrip` in the tests below pins every variant through both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Contains,
    /// Substring absent. Not the negation of [`FilterOp::Contains`] over nulls: both are false for
    /// a null cell, matching the SQL-ish rule the comparison kernels already follow, so filtering
    /// for "not X" never surfaces rows whose value is unknown.
    NotContains,
    StartsWith,
    EndsWith,
    /// Value is one of a comma-separated list (`status:in:new,open,pending`).
    In,
    /// The cell is null. Evaluated on the column as it was read, before any cast — a cast can
    /// manufacture nulls, and a null filter that reported those would be answering about the
    /// cast rather than about the data.
    IsNull,
    NotNull,
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
            "notcontains" | "not_contains" | "!~" => FilterOp::NotContains,
            "startswith" | "starts_with" | "^" => FilterOp::StartsWith,
            "endswith" | "ends_with" | "$" => FilterOp::EndsWith,
            "in" => FilterOp::In,
            "isnull" | "is_null" => FilterOp::IsNull,
            "notnull" | "not_null" | "isnotnull" => FilterOp::NotNull,
            _ => return None,
        })
    }

    /// The canonical wire token for this op — the exact inverse of [`FilterOp::parse`].
    ///
    /// `parse` accepts symbol aliases (`=`, `!~`, `^`) as a convenience for humans typing a
    /// query string; this returns the **word** form, so re-encoding is single-valued and a
    /// round-trip is stable. Without it there is no way to write a `ScanSpec` back onto the
    /// `/v1/rows` grammar, which is why [`Engine::scan`] could not be served remotely: an
    /// encoder written by hand beside the parser is a codec whose halves drift.
    pub fn as_str(&self) -> &'static str {
        match self {
            FilterOp::Eq => "eq",
            FilterOp::Ne => "ne",
            FilterOp::Lt => "lt",
            FilterOp::Le => "le",
            FilterOp::Gt => "gt",
            FilterOp::Ge => "ge",
            FilterOp::Contains => "contains",
            FilterOp::NotContains => "notcontains",
            FilterOp::StartsWith => "startswith",
            FilterOp::EndsWith => "endswith",
            FilterOp::In => "in",
            FilterOp::IsNull => "isnull",
            FilterOp::NotNull => "notnull",
        }
    }

    /// Does this op ignore the filter's `value` entirely? The null tests do, so the grid can offer
    /// them without demanding a value the user has nothing to type into.
    pub fn is_unary(&self) -> bool {
        matches!(self, FilterOp::IsNull | FilterOp::NotNull)
    }

    /// Ops with no numeric meaning — they compare text, so a numeric column is rendered to text
    /// first rather than taking the f64 fast path.
    fn is_textual(&self) -> bool {
        matches!(
            self,
            FilterOp::Contains
                | FilterOp::NotContains
                | FilterOp::StartsWith
                | FilterOp::EndsWith
                | FilterOp::In
        )
    }
}

/// One column filter (`column op value`), all ANDed together in a [`ScanSpec`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSpec {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
}

impl FilterSpec {
    /// Encode back onto the `/v1/rows` query grammar: `column:op:value`.
    ///
    /// The inverse of the parser in `api.rs`, and the reason a [`ScanSpec`] can now cross a
    /// wire at all. Note the asymmetry it inherits from that grammar: the column name and the
    /// value are **not** escaped, because the parser splits on the first two `:` and takes the
    /// rest verbatim — so a value containing `:` round-trips, and a *column* containing one
    /// does not. That is a property of the existing wire format, not of this encoder; it is
    /// stated here rather than silently worked around, because a caller with such a column
    /// needs to know before it builds a request rather than after it gets the wrong rows.
    pub fn to_wire(&self) -> String {
        format!("{}:{}:{}", self.column, self.op.as_str(), self.value)
    }
}

/// Sort the (filtered) rows by one column before windowing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    pub column: String,
    pub descending: bool,
}

/// A grid request: which rows/columns to return after filtering + sorting.
///
/// `serde(default)` at the struct level so a peer may omit any field it does not set — an
/// empty `{}` is the plain first-window request, which is what a minimal client should be able
/// to send without knowing this type's full shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
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
        records_path: None,
        credentials: None,
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
///
/// Values are counted and compared as they print, so a timestamp in a zone this build cannot
/// print fails the profile, as it fails `head` (see `crate::zone`).
pub fn profile_columns(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<ColumnProfile>> {
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
            let col = crate::zone::printable(batch.column(ci))?;
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

    Ok(out)
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
            let (key, idx) = match arrow_ord::sort::sort_to_indices(col, Some(options), None) {
                Ok(idx) => (col.clone(), idx),
                // Arrow orders scalars and lists but not a struct or a map. Order those by the
                // JSON the grid shows for them rather than refusing the click.
                Err(_) if is_nested(col.data_type()) => {
                    let text: arrow_array::ArrayRef = std::sync::Arc::new(as_text(col)?);
                    let idx = arrow_ord::sort::sort_to_indices(&text, Some(options), None)
                        .map_err(EngineError::arrow)?;
                    (text, idx)
                }
                Err(e) => return Err(EngineError::arrow(e)),
            };
            let idx = ties_in_read_order(&key, idx, options)?;
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

/// `idx`, a sort of `key`, with each run of equal keys put back in the order the rows were read.
///
/// Arrow's sort is unstable, so ties came out in an order of its own. That was consistent from one
/// request to the next, but not the SQL engine's order, which breaks ties by position in the file
/// — so a lean build and a `sql` build showed a tied sort differently. Equal keys are adjacent in
/// the sorted order, so one pass finds each run and sorting its indices restores read order.
fn ties_in_read_order(
    key: &arrow_array::ArrayRef,
    idx: arrow_array::UInt32Array,
    options: SortOptions,
) -> Result<arrow_array::UInt32Array> {
    let cmp = arrow_ord::ord::make_comparator(key.as_ref(), key.as_ref(), options)
        .map_err(EngineError::arrow)?;
    let mut order = idx.values().to_vec();
    let mut start = 0;
    while start < order.len() {
        let mut end = start + 1;
        while end < order.len() && cmp(order[start] as usize, order[end] as usize).is_eq() {
            end += 1;
        }
        order[start..end].sort_unstable();
        start = end;
    }
    Ok(arrow_array::UInt32Array::from(order))
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

/// A row-at-a-time text test, for the predicates Arrow has no scalar kernel for. The lifetime is
/// the filter value it borrows — these live only for the length of one `column_mask` call.
type TextPredicate<'a> = Box<dyn Fn(&str) -> bool + 'a>;

/// Build a boolean mask for `column op value`, pushing the comparison to Arrow's `cmp`
/// kernels (numeric columns compared as f64; everything else lexically as Utf8).
fn column_mask(column: &arrow_array::ArrayRef, op: FilterOp, value: &str) -> Result<BooleanArray> {
    // Null tests read the column's own validity, before any cast — see `FilterOp::IsNull`.
    if op.is_unary() {
        let want_null = op == FilterOp::IsNull;
        return Ok((0..column.len())
            .map(|i| Some(column.is_null(i) == want_null))
            .collect());
    }
    let numeric = !op.is_textual()
        && matches!(
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
    let text = as_text(column)?;
    let col = &text;
    // The text predicates Arrow has no scalar kernel for. Each is false on a null cell, so a
    // negative filter never turns "unknown" into a match.
    let textual: Option<TextPredicate<'_>> = match op {
        FilterOp::Contains => Some(Box::new(|s: &str| s.contains(value))),
        FilterOp::NotContains => Some(Box::new(|s: &str| !s.contains(value))),
        FilterOp::StartsWith => Some(Box::new(|s: &str| s.starts_with(value))),
        FilterOp::EndsWith => Some(Box::new(|s: &str| s.ends_with(value))),
        FilterOp::In => {
            // Split once, not per row. Empty members are dropped so a trailing comma is a typo
            // rather than a filter that matches the empty string.
            let members: Vec<&str> = value
                .split(',')
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .collect();
            Some(Box::new(move |s: &str| members.contains(&s)))
        }
        _ => None,
    };
    if let Some(pred) = textual {
        return Ok(col.iter().map(|opt| Some(opt.is_some_and(&pred))).collect());
    }
    let scalar = StringArray::new_scalar(value);
    cmp_apply(op, col, &scalar)
}

/// Does this type hold other values — a struct, a list, a map — rather than being one?
fn is_nested(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
    )
}

/// A column as text, for the text predicates and as the sort key of last resort.
///
/// A scalar is cast. A nested value is written as the JSON the grid receives for it (see
/// [`json_text`]), so a filter matches what the user is reading — `{"name":"Ada"}`, not Arrow's
/// `{name: Ada}` — and a struct, which Arrow can neither cast to text nor compare, can still be
/// filtered at all. Nulls stay null.
fn as_text(column: &arrow_array::ArrayRef) -> Result<StringArray> {
    if !is_nested(column.data_type()) {
        let cast = arrow_cast::cast(&crate::zone::printable(column)?, &DataType::Utf8)
            .map_err(EngineError::arrow)?;
        return cast
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .ok_or_else(|| EngineError::Other("utf8 cast failed".to_string()));
    }
    json_text(column)
}

/// Each value of `column` as the compact JSON the grid receives for it — `{"name":"Ada"}`, `[1,2]`
/// — with nulls left null. This is what a nested value is wherever it has to become text: the
/// filters and the sort of last resort here, and a cell in a CSV or TSV (`crate::render`), so a
/// nested cell reads the same in the grid, in a filter and in a download.
///
/// A row counts as null by its logical validity, which for a dictionary also looks through to the
/// value its key points at.
pub(crate) fn json_text(column: &arrow_array::ArrayRef) -> Result<StringArray> {
    let column = &crate::zone::printable(column)?;
    let field = std::sync::Arc::new(arrow_schema::Field::new(
        "",
        column.data_type().clone(),
        true,
    ));
    let options = arrow_json::writer::EncoderOptions::default();
    let mut encoder = arrow_json::writer::make_encoder(&field, column.as_ref(), &options)
        .map_err(EngineError::arrow)?;
    let nulls = column.logical_nulls();
    let mut buf = Vec::new();
    Ok((0..column.len())
        .map(|i| {
            if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                return None;
            }
            buf.clear();
            encoder.encode(i, &mut buf);
            Some(String::from_utf8_lossy(&buf).into_owned())
        })
        .collect())
}

/// Dispatch a comparison op to the matching Arrow `cmp` kernel.
fn cmp_apply<T>(op: FilterOp, lhs: &T, rhs: &arrow_array::Scalar<T>) -> Result<BooleanArray>
where
    T: arrow_array::Array + arrow_array::Datum,
{
    use arrow_ord::cmp;
    let r = match op {
        FilterOp::Eq => cmp::eq(lhs, rhs),
        FilterOp::Ne => cmp::neq(lhs, rhs),
        FilterOp::Lt => cmp::lt(lhs, rhs),
        FilterOp::Le => cmp::lt_eq(lhs, rhs),
        FilterOp::Gt => cmp::gt(lhs, rhs),
        FilterOp::Ge => cmp::gt_eq(lhs, rhs),
        // The text predicates and the null tests are answered in `column_mask` before it reaches
        // a scalar kernel — there is no Arrow `cmp` for "starts with" or "is null". Listing them
        // as unreachable keeps the match exhaustive, so a future op cannot be added without
        // deciding here what it means.
        FilterOp::Contains
        | FilterOp::NotContains
        | FilterOp::StartsWith
        | FilterOp::EndsWith
        | FilterOp::In
        | FilterOp::IsNull
        | FilterOp::NotNull => {
            return Err(EngineError::Other(format!(
                "internal: {op:?} reached the comparison kernel; it is handled in column_mask"
            )))
        }
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

#[cfg(test)]
mod wire_tests {
    use super::*;

    /// Every `FilterOp` variant, so adding one without deciding its wire token fails here.
    const ALL_OPS: &[FilterOp] = &[
        FilterOp::Eq,
        FilterOp::Ne,
        FilterOp::Lt,
        FilterOp::Le,
        FilterOp::Gt,
        FilterOp::Ge,
        FilterOp::Contains,
        FilterOp::NotContains,
        FilterOp::StartsWith,
        FilterOp::EndsWith,
        FilterOp::In,
        FilterOp::IsNull,
        FilterOp::NotNull,
    ];

    /// `as_str` is the inverse of `parse`, for every variant.
    ///
    /// This is the property that was missing and that kept `Engine::scan` off the wire: an
    /// encoder written beside a parser drifts unless something asserts they are inverses.
    #[test]
    fn filterop_wire_roundtrip() {
        for op in ALL_OPS {
            assert_eq!(
                FilterOp::parse(op.as_str()),
                Some(*op),
                "{op:?} does not survive as_str -> parse"
            );
        }
    }

    /// The serde encoding uses the *same* tokens as the query-param grammar, so a `FilterSpec`
    /// names its operator identically whether it travels as JSON or as `col:op:value`. If these
    /// ever diverge, two encodings of one concept exist and only one of them is tested.
    #[test]
    fn filterop_serde_matches_wire_token() {
        for op in ALL_OPS {
            let json = serde_json::to_string(op).expect("serialize");
            assert_eq!(
                json,
                format!("\"{}\"", op.as_str()),
                "{op:?} serde token differs from its wire token"
            );
            let back: FilterOp = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, *op);
        }
    }

    #[test]
    fn filterspec_to_wire_is_parseable_shape() {
        let f = FilterSpec {
            column: "city".to_string(),
            op: FilterOp::StartsWith,
            value: "Ams".to_string(),
        };
        let wire = f.to_wire();
        assert_eq!(wire, "city:startswith:Ams");

        // Split exactly as the `/v1/rows` parser does: on the first two `:` only.
        let mut parts = wire.splitn(3, ':');
        assert_eq!(parts.next(), Some("city"));
        assert_eq!(
            FilterOp::parse(parts.next().unwrap()),
            Some(FilterOp::StartsWith)
        );
        assert_eq!(parts.next(), Some("Ams"));
    }

    /// A value containing `:` survives, because the parser takes the remainder verbatim. Pinned
    /// because `to_wire`'s doc comment promises it.
    #[test]
    fn filterspec_to_wire_keeps_colons_in_the_value() {
        let f = FilterSpec {
            column: "ts".to_string(),
            op: FilterOp::Ge,
            value: "2026-09-12T10:30:00".to_string(),
        };
        let wire = f.to_wire();
        let mut parts = wire.splitn(3, ':');
        assert_eq!(parts.next(), Some("ts"));
        assert_eq!(parts.next(), Some("ge"));
        assert_eq!(parts.next(), Some("2026-09-12T10:30:00"));
    }

    /// A full `ScanSpec` survives a JSON round trip — the thing that makes a windowed read
    /// expressible to a peer at all.
    #[test]
    fn scanspec_json_roundtrip() {
        let spec = ScanSpec {
            offset: 400,
            limit: 200,
            sort: Some(SortSpec {
                column: "score".to_string(),
                descending: true,
            }),
            filters: vec![
                FilterSpec {
                    column: "city".to_string(),
                    op: FilterOp::Contains,
                    value: "dam".to_string(),
                },
                FilterSpec {
                    column: "score".to_string(),
                    op: FilterOp::Ge,
                    value: "10".to_string(),
                },
            ],
            projection: Some(vec!["city".to_string(), "score".to_string()]),
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        let back: ScanSpec = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.offset, 400);
        assert_eq!(back.limit, 200);
        assert_eq!(back.sort.as_ref().unwrap().column, "score");
        assert!(back.sort.as_ref().unwrap().descending);
        assert_eq!(back.filters.len(), 2);
        assert_eq!(back.filters[0].op, FilterOp::Contains);
        assert_eq!(back.filters[1].op, FilterOp::Ge);
        assert_eq!(
            back.projection.as_deref(),
            Some(["city".to_string(), "score".to_string()].as_slice())
        );
    }

    /// `{}` is a valid minimal request: a plain first window. A client should not have to know
    /// this type's full shape to ask for the default.
    #[test]
    fn scanspec_deserializes_from_empty_object() {
        let spec: ScanSpec = serde_json::from_str("{}").expect("deserialize");
        assert_eq!(spec.offset, 0);
        assert_eq!(spec.limit, 0);
        assert!(spec.is_plain_window());
        assert!(spec.projection.is_none());
    }

    /// `Capabilities` round-trips, so a client can read a peer's instead of assuming one.
    /// Also pins that the two new bits default to `false` when an older peer omits them —
    /// the safe direction, since a missing bit must not read as "yes, I can do that".
    #[test]
    fn capabilities_roundtrip_and_older_peer_defaults_to_false() {
        let caps = Capabilities {
            engine: "local (built-in reader)".to_string(),
            formats: vec!["parquet".to_string(), "csv".to_string()],
            sql: false,
            profile: true,
            remote: false,
            scan: true,
            filtered_stats: true,
        };
        let back: Capabilities =
            serde_json::from_str(&serde_json::to_string(&caps).expect("serialize"))
                .expect("deserialize");
        assert!(back.scan);
        assert!(back.filtered_stats);
        assert_eq!(back.formats.len(), 2);

        let older = r#"{"engine":"x","formats":[],"sql":true,"profile":true,"remote":true}"#;
        let back: Capabilities = serde_json::from_str(older).expect("deserialize older peer");
        assert!(!back.scan, "a missing bit must not read as capable");
        assert!(
            !back.filtered_stats,
            "a missing bit must not read as capable"
        );
    }

    /// The leak this closes, at the exact line that had it.
    ///
    /// `TableSchema.source` is `Serialize` and is the body of `GET /v1/schema`. It was built from
    /// the source's path verbatim, and for a database source the path *is* the connection string —
    /// so the response echoed the password, and so did the persisted run record built beside it.
    /// `Source::display` is now the redacting renderer and this is its most important caller.
    #[test]
    fn a_table_schemas_source_never_carries_a_database_password() {
        use arrow_schema::{DataType, Field, Schema};
        let source = crate::source::Source::with_format(
            "postgres://alice:hunter2@db.internal/orders?table=events",
            crate::source::Format::Database,
        );
        let arrow: SchemaRef =
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let ts = build_table_schema(&source, "database", None, &arrow);
        assert!(!ts.source.contains("hunter2"), "leaked: {}", ts.source);
        assert_eq!(
            ts.source, "postgres://alice:***@db.internal/orders?table=events",
            "the host, database, table and account must survive — they are what identifies it"
        );

        // And it really does serialize, which is the half that made this a leak rather than a
        // smell: a struct nobody sends cannot publish anything.
        let json = serde_json::to_string(&ts).unwrap();
        assert!(!json.contains("hunter2"), "leaked through serde: {json}");
    }
}

#[cfg(test)]
mod stream_default_tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::context::RequestContext;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(from: i64, n: i64) -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from((from..from + n).collect::<Vec<_>>()));
        RecordBatch::try_new(schema(), vec![ids]).unwrap()
    }

    /// A backend that can answer SQL but cannot produce incrementally — `database` (eager `sqlx`
    /// `fetch_all`) and `remote` (one complete IPC body) are both this shape.
    struct BufferedOnlyEngine;

    impl Engine for BufferedOnlyEngine {
        fn name(&self) -> &str {
            "buffered-only"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                engine: "buffered-only".into(),
                formats: vec![],
                sql: true,
                profile: false,
                remote: false,
                scan: false,
                filtered_stats: false,
            }
        }
        fn schema(&self, _: &RequestContext, _: &Source) -> Result<TableSchema> {
            unimplemented!("not exercised")
        }
        fn preview(&self, _: &RequestContext, _: &Source, _: usize) -> Result<RowBatch> {
            unimplemented!("not exercised")
        }
        fn profile(&self, _: &RequestContext, _: &Source, _: usize) -> Result<TableProfile> {
            unimplemented!("not exercised")
        }
        fn query(&self, _: &RequestContext, _: &str, _: &[NamedSource]) -> Result<RowBatch> {
            Ok(RowBatch {
                schema: schema(),
                batches: vec![batch(0, 3), batch(3, 3)],
            })
        }
    }

    /// An engine that overrides nothing still answers `query_stream`, which is what makes the
    /// method addable to a trait every backend implements without touching any of them.
    #[test]
    fn the_default_implementation_streams_an_engine_that_cannot() {
        let stream = BufferedOnlyEngine
            .query_stream(&RequestContext::detached(), "SELECT 1", &[], None)
            .unwrap();
        assert_eq!(
            stream.schema().fields()[0].name(),
            "id",
            "the schema must be there before the batches, even when it came from a buffer"
        );
        let batches: Vec<RecordBatch> = stream.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
    }

    /// And `collect_batch` is the round trip back, so a caller holding a stream can always fall
    /// back to the old shape — the bridge that lets consumers migrate one at a time.
    #[test]
    fn a_stream_collects_back_into_the_batch_it_came_from() {
        let original = RowBatch {
            schema: schema(),
            batches: vec![batch(0, 2), batch(2, 2)],
        };
        let round_tripped = RowStream::from_batch(RowBatch {
            schema: original.schema.clone(),
            batches: original.batches.clone(),
        })
        .collect_batch()
        .unwrap();
        assert_eq!(round_tripped.num_rows(), original.num_rows());
        assert_eq!(round_tripped.batches.len(), original.batches.len());
    }

    /// A capped default goes through `query_capped`, whose own default trims afterwards — so the
    /// cap is honoured even by a backend that can neither stream nor push a limit down.
    #[test]
    fn the_default_implementation_honours_a_cap() {
        let stream = BufferedOnlyEngine
            .query_stream(&RequestContext::detached(), "SELECT 1", &[], Some(4))
            .unwrap();
        let rows: usize = stream.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 4);
    }
}

#[cfg(test)]
mod sql_format_tests {
    use super::*;

    /// The SQL engine's list is the local reader's list narrowed, never widened: the API routes
    /// sorted and filtered grid windows on it, so claiming a format the build cannot read at all is
    /// what broke the JSON grid in every `sql` build. JSON is on it now because SQL registers it
    /// through the local reader, not because DataFusion reads it.
    #[test]
    fn sql_formats_are_the_readable_formats_sql_can_register() {
        let local = readable_formats();
        let sql = sql_readable_formats();
        for f in &sql {
            assert!(
                local.contains(f),
                "sql claims {f}, which this build cannot read at all"
            );
        }
        for always in ["parquet", "csv", "tsv", "json"] {
            assert!(
                local.iter().any(|f| f == always),
                "local must list {always}"
            );
            assert!(sql.iter().any(|f| f == always), "sql must list {always}");
        }
        assert!(!sql_registers(Format::Database) && !sql_registers(Format::Unknown));
        assert_eq!(
            sql.iter().any(|f| f == "iceberg"),
            cfg!(feature = "iceberg")
        );
        assert_eq!(sql.iter().any(|f| f == "delta"), cfg!(feature = "delta"));
    }
}

/// A timestamp labelled `UTC` is profiled and filtered on the text it prints, in every build, and
/// a zone the build cannot print fails the profile rather than profiling blanks. See `crate::zone`.
#[cfg(test)]
mod time_zone_tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, StructArray, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema, TimeUnit};

    use super::*;
    use crate::zone::has_tz_database;

    /// `ts`, in `zone`, and `nested`, a struct holding it as `at`: 2024-01-02T03:04:05.123456Z,
    /// 2024-06-30T23:59:59Z, a null, and the first again.
    fn zoned(zone: &str) -> (SchemaRef, RecordBatch) {
        let ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some(zone.into()));
        let ts: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(1_704_164_645_123_456),
                Some(1_719_791_999_000_000),
                None,
                Some(1_704_164_645_123_456),
            ])
            .with_timezone(zone),
        );
        let at = Arc::new(Field::new("at", ts_type.clone(), true));
        let nested: ArrayRef = Arc::new(StructArray::from(vec![(at.clone(), ts.clone())]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", ts_type, true),
            Field::new("nested", DataType::Struct(vec![at].into()), true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ts, nested]).unwrap();
        (schema, batch)
    }

    #[test]
    fn a_utc_column_is_profiled_as_it_prints() {
        let (schema, batch) = zoned("UTC");
        let profile = profile_columns(&schema, &[batch]).unwrap();
        let ts = &profile[0];
        assert_eq!((ts.null_count, ts.distinct), (1, 2));
        assert_eq!(ts.min.as_deref(), Some("2024-01-02T03:04:05.123456Z"));
        assert_eq!(ts.max.as_deref(), Some("2024-06-30T23:59:59Z"));
        assert_eq!(
            ts.sample,
            [
                "2024-01-02T03:04:05.123456Z",
                "2024-06-30T23:59:59Z",
                "2024-01-02T03:04:05.123456Z"
            ]
        );
    }

    /// The grid's filters compare the text a cell shows: Arrow's for a scalar, JSON for a struct.
    #[test]
    fn a_utc_column_is_filtered_on_the_text_it_prints() {
        let (schema, batch) = zoned("UTC");
        for column in ["ts", "nested"] {
            let filters = [FilterSpec {
                column: column.to_string(),
                op: FilterOp::Contains,
                value: "2024-06-30T23:59:59Z".to_string(),
            }];
            let (_, kept) =
                filter_batches(&schema, std::slice::from_ref(&batch), &filters).unwrap();
            let rows: usize = kept.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 1, "{column}");
        }
    }

    #[test]
    fn a_zone_this_build_cannot_print_fails_the_profile() {
        let (schema, batch) = zoned("Europe/Paris");
        match profile_columns(&schema, &[batch]) {
            Ok(profile) => {
                assert!(has_tz_database());
                assert_eq!(profile[0].max.as_deref(), Some("2024-07-01T01:59:59+02:00"));
            }
            Err(e) => {
                assert!(!has_tz_database(), "{e}");
                assert!(e.to_string().contains("`Europe/Paris`"), "{e}");
            }
        }
    }
}
