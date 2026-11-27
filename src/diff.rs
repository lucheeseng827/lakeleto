//! `lakeleto diff`: what changed between two tables.
//!
//! Two readable tables — any two, across formats and sources — are compared in three layers:
//!
//! 1. **Schema**: columns added, removed, retyped, or changed in nullability, by name, and whether
//!    the columns both hold are in a different order.
//! 2. **Row counts**.
//! 3. **Column statistics**: each shared column's null count, minimum and maximum.
//!
//! Statistics are **exact** by default: every row of each side, through the SQL engine, one
//! aggregate query per side ([`StatsMethod::Exact`]). `--fast` takes them from Parquet footers
//! instead, which reads no rows ([`StatsMethod::Footer`]); `--scan N` from the first `N` rows of
//! each side ([`StatsMethod::Sample`]), which is also what a build without SQL does, saying so in
//! the report. A sample compares the first rows of each side, not the tables, so a difference it
//! finds may not be one between the tables.
//!
//! The comparison itself is plain Rust over two descriptions ([`compare_schemas`],
//! [`compare_stats`]); only reading the descriptions touches an engine.
//!
//! **What a difference is, for `--fail-on`, is [`Change`].** The report lists every kind it found
//! (`found`), and the command exits [`EXIT_FOUND`] when one of the kinds asked for is among them.

use std::collections::BTreeMap;

use arrow_array::{Array, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::DataType;
use serde::{Deserialize, Serialize};

use crate::context::RequestContext;
use crate::engine::registry::{EngineRegistry, Need};
use crate::engine::{ColumnSchema, Engine, NamedSource, RowBatch, TableProfile};
use crate::error::{EngineError, Result};
use crate::snapshot::{SnapshotRef, TableMetadata};
use crate::source::{Format, Source};

/// The report's shape, as `-o json` writes it. Raised when a field changes meaning or goes away;
/// a field added leaves it as it is.
pub const REPORT_VERSION: u32 = 1;

/// The exit code of a `diff` that found a kind of difference `--fail-on` names. `0` is none of
/// them, `1` an error (as for every command), `2` a usage error (clap's).
pub const EXIT_FOUND: i32 = 3;

/// The names the two sides are registered under for SQL.
pub const LEFT: &str = "lakeleto_left";
/// See [`LEFT`].
pub const RIGHT: &str = "lakeleto_right";

/// How the statistics were taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StatsMethod {
    /// Every row of each side, read by the SQL engine.
    Exact,
    /// Parquet footer statistics: exact over each file, with no row read.
    Footer,
    /// The first rows of each side, up to a limit.
    Sample,
    /// Each side's table metadata (an Iceberg table's manifests, a Delta table's log), at the
    /// snapshot it names: exact where given, with no row read. What a snapshot is compared by.
    Metadata,
}

/// How to take the statistics, as the command line asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsMode {
    /// [`StatsMethod::Exact`] when the build runs SQL over both sides; otherwise a sample of
    /// [`crate::cli::DEFAULT_SCAN`] rows, which the report says.
    Auto,
    /// [`StatsMethod::Footer`] (`--fast`).
    Footer,
    /// [`StatsMethod::Sample`] of this many rows (`--scan N`).
    Sample(usize),
}

/// What to compare.
#[derive(Debug, Clone)]
pub struct DiffOptions {
    pub stats: StatsMode,
    /// The columns that name one row on each side, to compare the rows by. Empty compares no
    /// rows.
    pub key: Vec<String>,
    /// How many rows of each kind — added, removed, changed — to show.
    pub sample_rows: usize,
}

/// The rows of each kind a keyed diff shows when nobody says otherwise.
pub const SAMPLE_ROWS: usize = 5;

impl Default for DiffOptions {
    fn default() -> Self {
        DiffOptions {
            stats: StatsMode::Auto,
            key: Vec::new(),
            sample_rows: SAMPLE_ROWS,
        }
    }
}

/// A kind of difference, as `--fail-on` names it and the report's `found` lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Change {
    /// A column only the right side has.
    AddedColumn,
    /// A column only the left side has.
    RemovedColumn,
    /// A column both have, with a different type.
    TypeChange,
    /// A column both have, nullable on one side only.
    Nullability,
    /// The columns both have, in a different order.
    ColumnOrder,
    /// Different row counts.
    RowCount,
    /// A shared column whose null count, minimum or maximum differs.
    Stats,
    /// A key only the right side has.
    AddedRows,
    /// A key only the left side has.
    RemovedRows,
    /// A key both have, whose row differs in a compared column.
    ChangedRows,
}

impl Change {
    /// The name `--fail-on` takes and the report writes.
    pub fn name(self) -> &'static str {
        match self {
            Change::AddedColumn => "added-column",
            Change::RemovedColumn => "removed-column",
            Change::TypeChange => "type-change",
            Change::Nullability => "nullability",
            Change::ColumnOrder => "column-order",
            Change::RowCount => "row-count",
            Change::Stats => "stats",
            Change::AddedRows => "added-rows",
            Change::RemovedRows => "removed-rows",
            Change::ChangedRows => "changed-rows",
        }
    }
}

/// What `--fail-on` takes: a [`Change`], or a group of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum FailOn {
    AddedColumn,
    RemovedColumn,
    TypeChange,
    Nullability,
    ColumnOrder,
    RowCount,
    Stats,
    AddedRows,
    RemovedRows,
    ChangedRows,
    /// Any change to the columns: added, removed, retyped, or in nullability — not their order,
    /// which a reader that finds columns by name never sees; `column-order` names it.
    Schema,
    /// Any row added, removed or changed, compared by `--key`.
    Rows,
    /// Any difference at all.
    Any,
}

impl FailOn {
    /// The kinds of difference this names.
    pub fn changes(self) -> &'static [Change] {
        use Change::*;
        match self {
            FailOn::AddedColumn => &[AddedColumn],
            FailOn::RemovedColumn => &[RemovedColumn],
            FailOn::TypeChange => &[TypeChange],
            FailOn::Nullability => &[Nullability],
            FailOn::ColumnOrder => &[ColumnOrder],
            FailOn::RowCount => &[RowCount],
            FailOn::Stats => &[Stats],
            FailOn::AddedRows => &[AddedRows],
            FailOn::RemovedRows => &[RemovedRows],
            FailOn::ChangedRows => &[ChangedRows],
            FailOn::Schema => &[AddedColumn, RemovedColumn, TypeChange, Nullability],
            FailOn::Rows => &[AddedRows, RemovedRows, ChangedRows],
            FailOn::Any => &[
                AddedColumn,
                RemovedColumn,
                TypeChange,
                Nullability,
                ColumnOrder,
                RowCount,
                Stats,
                AddedRows,
                RemovedRows,
                ChangedRows,
            ],
        }
    }
}

/// One side of a diff, as it was read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Side {
    pub source: String,
    pub format: String,
    pub engine: String,
    /// The snapshot compared, as the table names it (an Iceberg snapshot id, a Delta version),
    /// when the side was compared by its metadata: the one an `@<instant>` resolved to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
    /// When that snapshot was committed, in milliseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_ms: Option<i64>,
}

/// A column both sides hold, differing in type or nullability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnChange {
    pub name: String,
    pub left_type: String,
    pub right_type: String,
    pub left_nullable: bool,
    pub right_nullable: bool,
}

/// How the columns differ.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Columns only the right side has, in its order.
    pub added: Vec<ColumnSchema>,
    /// Columns only the left side has, in its order.
    pub removed: Vec<ColumnSchema>,
    /// Columns both hold, differing in type or nullability, in the left side's order.
    pub changed: Vec<ColumnChange>,
    /// Whether the columns both hold are in a different order.
    pub reordered: bool,
}

/// Each side's row count, when it is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowCounts {
    pub left: Option<u64>,
    pub right: Option<u64>,
}

/// One column's statistics on one side. `None` is not computed, never zero.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub nulls: Option<u64>,
    pub min: Option<String>,
    pub max: Option<String>,
}

/// A shared column's statistics on both sides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStatsDiff {
    pub name: String,
    pub left: ColumnStats,
    pub right: ColumnStats,
    /// Whether a statistic computed on both sides differs.
    pub changed: bool,
    /// Why its statistics are not compared, when its types on the two sides hold different kinds
    /// of value: never then a change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_compared: Option<String>,
}

/// The statistics of every column both sides hold, in the left side's order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsDiff {
    pub method: StatsMethod,
    /// The engine that took them.
    pub engine: String,
    /// Rows read for them on each side: every row for [`StatsMethod::Exact`], none for
    /// [`StatsMethod::Footer`], up to the limit for [`StatsMethod::Sample`].
    pub left_rows_read: u64,
    pub right_rows_read: u64,
    pub columns: Vec<ColumnStatsDiff>,
}

/// A shared column a keyed diff does not compare, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotCompared {
    pub name: String,
    pub reason: String,
}

/// How many rows changed in one compared column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnCount {
    pub name: String,
    pub changed: u64,
}

/// A row only one side has: its key, and its compared columns, in `compared`'s order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleRow {
    pub key: Vec<Option<String>>,
    pub values: Vec<Option<String>>,
}

/// One compared column of a changed row, on each side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellChange {
    pub column: String,
    pub left: Option<String>,
    pub right: Option<String>,
}

/// A row both sides have that differs: its key, and the columns that differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangedRow {
    pub key: Vec<Option<String>>,
    pub changes: Vec<CellChange>,
}

/// A few rows of each kind, by key, least first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Samples {
    pub added: Vec<SampleRow>,
    pub removed: Vec<SampleRow>,
    pub changed: Vec<ChangedRow>,
}

/// The rows of the two sides, matched by key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedDiff {
    pub key: Vec<String>,
    /// The shared columns compared, beyond the key, in the left side's order.
    pub compared: Vec<String>,
    /// The shared columns not compared, and why: never reported as changed.
    pub not_compared: Vec<NotCompared>,
    /// Keys only the right side has.
    pub added: u64,
    /// Keys only the left side has.
    pub removed: u64,
    /// Keys both have, whose rows differ in a compared column.
    pub changed: u64,
    /// Keys both have, whose rows are the same in every compared column.
    pub unchanged: u64,
    /// For each compared column, the rows that differ in it.
    pub changed_by_column: Vec<ColumnCount>,
    pub samples: Samples,
}

/// What changed between two tables. `-o json` writes it as it is.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffReport {
    pub version: u32,
    pub left: Side,
    pub right: Side,
    pub schema: SchemaDiff,
    pub rows: RowCounts,
    pub stats: StatsDiff,
    /// The rows compared by key, when a key was given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyed: Option<KeyedDiff>,
    /// Every kind of difference found, as `--fail-on` names them.
    pub found: Vec<Change>,
    /// What a reader should know about how the report was made.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl DiffReport {
    /// Whether any kind of difference `fail_on` names was found.
    pub fn fails(&self, fail_on: &[FailOn]) -> bool {
        fail_on
            .iter()
            .flat_map(|f| f.changes())
            .any(|c| self.found.contains(c))
    }
}

// ---- the comparison -------------------------------------------------------------------------

/// How the columns of `left` and `right` differ, matched by name.
pub fn compare_schemas(left: &[ColumnSchema], right: &[ColumnSchema]) -> SchemaDiff {
    let in_right: BTreeMap<&str, &ColumnSchema> =
        right.iter().map(|c| (c.name.as_str(), c)).collect();
    let in_left: BTreeMap<&str, &ColumnSchema> =
        left.iter().map(|c| (c.name.as_str(), c)).collect();
    let removed = left
        .iter()
        .filter(|c| !in_right.contains_key(c.name.as_str()))
        .cloned()
        .collect();
    let added = right
        .iter()
        .filter(|c| !in_left.contains_key(c.name.as_str()))
        .cloned()
        .collect();
    let changed = left
        .iter()
        .filter_map(|l| {
            let r = in_right.get(l.name.as_str())?;
            (l.data_type != r.data_type || l.nullable != r.nullable).then(|| ColumnChange {
                name: l.name.clone(),
                left_type: l.data_type.clone(),
                right_type: r.data_type.clone(),
                left_nullable: l.nullable,
                right_nullable: r.nullable,
            })
        })
        .collect();
    let shared_left: Vec<&str> = left
        .iter()
        .map(|c| c.name.as_str())
        .filter(|n| in_right.contains_key(n))
        .collect();
    let shared_right: Vec<&str> = right
        .iter()
        .map(|c| c.name.as_str())
        .filter(|n| in_left.contains_key(n))
        .collect();
    SchemaDiff {
        added,
        removed,
        changed,
        reordered: shared_left != shared_right,
    }
}

/// The statistics of every column both sides hold, in the left side's order, each marked changed
/// when a statistic computed on both sides differs. A statistic one side lacks is not a change:
/// it was not computed, which says nothing about the data.
///
/// `types` gives each column's type on the left and on the right, as [`ColumnSchema`] names
/// them. A column whose two types hold different kinds of value — text on one side, numbers on
/// the other — has its statistics shown and not compared. Numbers compare as numbers, so `2` and
/// `2.0` are one value, in the order rows are compared by (`0.0` and `-0.0` differ, `NaN` is
/// `NaN`).
pub fn compare_stats(
    left: &[(String, ColumnStats)],
    right: &[(String, ColumnStats)],
    types: &BTreeMap<String, (String, String)>,
) -> Vec<ColumnStatsDiff> {
    let in_right: BTreeMap<&str, &ColumnStats> =
        right.iter().map(|(n, s)| (n.as_str(), s)).collect();
    left.iter()
        .filter_map(|(name, l)| {
            let r = *in_right.get(name.as_str())?;
            let rule = types
                .get(name)
                .map_or(Ok(false), |(lt, rt)| stats_rule(lt, rt));
            let (changed, not_compared) = match rule {
                Ok(numeric) => {
                    let value = |a: &Option<String>, b: &Option<String>| match (a, b) {
                        (Some(a), Some(b)) => !same_value(a, b, numeric),
                        _ => false,
                    };
                    (
                        differs(&l.nulls, &r.nulls)
                            || value(&l.min, &r.min)
                            || value(&l.max, &r.max),
                        None,
                    )
                }
                Err(why) => (false, Some(why)),
            };
            Some(ColumnStatsDiff {
                name: name.clone(),
                left: l.clone(),
                right: r.clone(),
                changed,
                not_compared,
            })
        })
        .collect()
}

/// Whether a statistic computed on both sides differs.
fn differs<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
    a.is_some() && b.is_some() && a != b
}

/// The kind of value a type, as [`ColumnSchema`] names it, holds: its whole name for anything
/// that compares only with itself.
fn type_kind(t: &str) -> String {
    // By whole name: `Interval(…)` starts with `Int` and is no number.
    let number = matches!(
        t,
        "Int8"
            | "Int16"
            | "Int32"
            | "Int64"
            | "UInt8"
            | "UInt16"
            | "UInt32"
            | "UInt64"
            | "Float16"
            | "Float32"
            | "Float64"
    ) || t.starts_with("Decimal");
    if number {
        "number".to_string()
    } else if matches!(t, "Utf8" | "LargeUtf8" | "Utf8View") {
        "text".to_string()
    } else if let Some(rest) = t.strip_prefix("Timestamp(") {
        // The unit does not matter; the zone, after the unit, does.
        format!(
            "timestamp{}",
            rest.split_once(',').map_or("", |(_, zone)| zone)
        )
    } else if t.starts_with("Date") {
        "date".to_string()
    } else if t.starts_with("Time32") || t.starts_with("Time64") {
        "time".to_string()
    } else {
        t.to_string()
    }
}

/// Whether statistics of a column typed `left` on one side and `right` on the other compare, and
/// as numbers; or why not.
fn stats_rule(left: &str, right: &str) -> std::result::Result<bool, String> {
    let kind = type_kind(left);
    if left == right || kind == type_kind(right) {
        Ok(kind == "number")
    } else {
        Err(format!("types differ: {left} and {right}"))
    }
}

/// Whether two printed values are one value: as numbers when `numeric`, and as text otherwise.
/// A number written in plain digits — an integer or a decimal, however long — compares exactly;
/// anything else (`NaN`, `inf`, an exponent) in Arrow's total order of floats, as rows compare.
fn same_value(a: &str, b: &str, numeric: bool) -> bool {
    if a == b {
        return true;
    }
    if !numeric {
        return false;
    }
    if let (Some(x), Some(y)) = (exact_number(a), exact_number(b)) {
        return x == y;
    }
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => x.total_cmp(&y) == std::cmp::Ordering::Equal,
        _ => false,
    }
}

/// A number in plain digits, written one way: its sign, its integer digits without leading
/// zeros, and its fraction without trailing ones — `2`, `2.0` and `02.00` alike are `(false,
/// "2", "")`. The sign is kept as written, so `-0.0` stays apart from `0.0`, as in Arrow's total
/// order. `None` for anything else.
fn exact_number(s: &str) -> Option<(bool, String, String)> {
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let plain = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if whole.is_empty() || !plain(whole) || !plain(fraction) {
        return None;
    }
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    Some((
        negative,
        whole.to_string(),
        fraction.trim_end_matches('0').to_string(),
    ))
}

/// Every kind of difference in a report's parts, in [`Change`]'s order.
pub fn found(
    schema: &SchemaDiff,
    rows: &RowCounts,
    stats: &[ColumnStatsDiff],
    keyed: Option<&KeyedDiff>,
) -> Vec<Change> {
    let mut found = Vec::new();
    let mut note = |hit: bool, change: Change| {
        if hit {
            found.push(change);
        }
    };
    note(!schema.added.is_empty(), Change::AddedColumn);
    note(!schema.removed.is_empty(), Change::RemovedColumn);
    note(
        schema.changed.iter().any(|c| c.left_type != c.right_type),
        Change::TypeChange,
    );
    note(
        schema
            .changed
            .iter()
            .any(|c| c.left_nullable != c.right_nullable),
        Change::Nullability,
    );
    note(schema.reordered, Change::ColumnOrder);
    note(
        matches!((rows.left, rows.right), (Some(l), Some(r)) if l != r),
        Change::RowCount,
    );
    note(stats.iter().any(|c| c.changed), Change::Stats);
    if let Some(k) = keyed {
        note(k.added > 0, Change::AddedRows);
        note(k.removed > 0, Change::RemovedRows);
        note(k.changed > 0, Change::ChangedRows);
    }
    found
}

// ---- reading the two sides ------------------------------------------------------------------

/// A side's table, and the snapshot it names when it names one: the rule `lakeleto diff` reads
/// its arguments by, and `GET /v1/diff` its `left` and `right`. `<table>@<snapshot>` is a
/// snapshot address unless the whole of it is a table or file that exists, which is a path
/// (ADR-0002):
///
/// - On disk, the filesystem tells.
/// - In an object store, the probes a bare prefix always takes tell, run on the whole name with no
///   format, since naming one skips them. Only their finding nothing there makes it a snapshot
///   address; any other failure is the error. A format no snapshot is kept in (`csv`, `parquet`,
///   …) names an object read whole.
/// - A database URI is a connection, whose `@` belongs to it. With `opaque`, the sources are
///   another process's to resolve (a remote engine's), and an object URI is taken whole too.
///
/// `resolve` turns a name and a format into the [`Source`] the caller reads, with whatever else
/// it applies to every side (`--json-path`, `--flatten`, a server's `--root`).
///
/// # Errors
///
/// Whatever `resolve` fails with.
pub fn side(
    reference: &std::path::Path,
    format: Option<&str>,
    opaque: bool,
    resolve: impl Fn(&std::path::Path, Option<&str>) -> Result<Source>,
) -> Result<(Source, Option<SnapshotRef>)> {
    let whole = || Ok((resolve(reference, format)?, None));
    let Some((table, at)) = reference.to_str().and_then(crate::snapshot::split) else {
        return whole();
    };
    if reference.exists() || crate::source::is_database_uri(table) {
        return whole();
    }
    if crate::source::is_object_uri(table) {
        let snapshot_kept = |f: Format| matches!(f, Format::Iceberg | Format::Delta);
        if opaque
            || format
                .and_then(Format::parse)
                .is_some_and(|f| !snapshot_kept(f))
        {
            return whole();
        }
        match resolve(reference, None) {
            // Probed once: a format named reads it as that, which takes no probe.
            Ok(found) if format.is_none() => return Ok((found, None)),
            Ok(_) => return whole(),
            Err(e) if crate::snapshot::is_only_diff_reads(&e) => {}
            Err(e) => return Err(e),
        }
    }
    Ok((resolve(std::path::Path::new(table), format)?, Some(at)))
}

/// Compare `left` with `right`, reading each with the engines `engines` resolves for it.
///
/// # Errors
///
/// Whatever reading either side fails with; [`StatsMode::Footer`] on a side that is not a single
/// Parquet file.
pub fn diff(
    ctx: &RequestContext,
    engines: &EngineRegistry,
    left: &Source,
    right: &Source,
    opts: &DiffOptions,
) -> Result<DiffReport> {
    let left_engine = engines.resolve(Need::Read(left))?;
    let right_engine = engines.resolve(Need::Read(right))?;
    let left_schema = left_engine.schema(ctx, left)?;
    let right_schema = right_engine.schema(ctx, right)?;
    let schema = compare_schemas(&left_schema.columns, &right_schema.columns);
    let types: BTreeMap<String, (String, String)> = left_schema
        .columns
        .iter()
        .filter_map(|l| {
            let r = right_schema.columns.iter().find(|r| r.name == l.name)?;
            Some((l.name.clone(), (l.data_type.clone(), r.data_type.clone())))
        })
        .collect();
    let mut notes = Vec::new();

    let named = [
        NamedSource {
            name: LEFT.to_string(),
            source: left.clone(),
        },
        NamedSource {
            name: RIGHT.to_string(),
            source: right.clone(),
        },
    ];
    let mode = match opts.stats {
        StatsMode::Auto => match exact_engine(engines, &named) {
            Some(engine) => Mode::Exact(engine),
            None => {
                notes.push(format!(
                    "statistics are from the first {} rows of each side, not every row: exact \
                     statistics need an engine that runs SQL over both (the release binaries \
                     have one; `--features sql` builds it), and `--fast` reads Parquet footers",
                    crate::cli::DEFAULT_SCAN
                ));
                Mode::Sample(crate::cli::DEFAULT_SCAN)
            }
        },
        StatsMode::Footer => {
            // Only a Parquet file has a footer: any other side would answer a profile of no rows,
            // which would read as exact statistics.
            for (side, source) in [("left", left), ("right", right)] {
                if source.format != Format::Parquet {
                    return Err(EngineError::Other(format!(
                        "`--fast` reads Parquet footers, and the {side} side is {}: drop `--fast` \
                         for exact statistics, or `--scan N` for a sample",
                        source.format
                    )));
                }
            }
            Mode::Footer
        }
        StatsMode::Sample(0) => {
            return Err(EngineError::Other(
                "a sample of no rows compares nothing: `--scan` takes 1 or more".to_string(),
            ))
        }
        StatsMode::Sample(n) => Mode::Sample(n),
    };

    let mut probes = Probes::default();
    let (stats, rows) = match mode {
        Mode::Exact(ref engine) => {
            let engine = engine.as_ref();
            let left_columns = probes.get(ctx, engine, &named, 0)?;
            let right_columns = probes.get(ctx, engine, &named, 1)?;
            let (left_rows, left_stats) = exact_stats(ctx, engine, &named[0], &left_columns)?;
            let (right_rows, right_stats) = exact_stats(ctx, engine, &named[1], &right_columns)?;
            (
                StatsDiff {
                    method: StatsMethod::Exact,
                    engine: engine.name().to_string(),
                    left_rows_read: left_rows,
                    right_rows_read: right_rows,
                    columns: compare_stats(&left_stats, &right_stats, &types),
                },
                RowCounts {
                    left: Some(left_rows),
                    right: Some(right_rows),
                },
            )
        }
        Mode::Footer | Mode::Sample(_) => {
            let limit = match mode {
                Mode::Sample(n) => n,
                _ => 0,
            };
            let left_profile = left_engine.profile(ctx, left, limit)?;
            let right_profile = right_engine.profile(ctx, right, limit)?;
            // A sample that stopped short of its limit read every row there is: the table's row
            // count, and statistics as exact as any.
            let whole = |p: &TableProfile| limit > 0 && (p.scanned_rows as usize) < limit;
            let count = |p: &TableProfile, schema: Option<u64>| {
                p.row_count
                    .or(schema)
                    .or_else(|| whole(p).then_some(p.scanned_rows))
            };
            let method = if limit == 0 {
                StatsMethod::Footer
            } else if whole(&left_profile) && whole(&right_profile) {
                StatsMethod::Exact
            } else {
                StatsMethod::Sample
            };
            if method == StatsMethod::Exact {
                notes.clear();
            }
            (
                StatsDiff {
                    method,
                    // Each side's own engine: two formats may be read by two.
                    engine: if left_profile.engine == right_profile.engine {
                        left_profile.engine.clone()
                    } else {
                        format!("{} and {}", left_profile.engine, right_profile.engine)
                    },
                    left_rows_read: left_profile.scanned_rows,
                    right_rows_read: right_profile.scanned_rows,
                    columns: compare_stats(
                        &profile_stats(&left_profile),
                        &profile_stats(&right_profile),
                        &types,
                    ),
                },
                RowCounts {
                    left: count(&left_profile, left_schema.row_count),
                    right: count(&right_profile, right_schema.row_count),
                },
            )
        }
    };

    let keyed = if opts.key.is_empty() {
        None
    } else {
        Some(keyed(
            ctx,
            engines,
            &named,
            &mut probes,
            &opts.key,
            opts.sample_rows,
        )?)
    };
    // Matching every row by key counts every row: a count the statistics did not take is that.
    let mut rows = rows;
    if let Some(k) = &keyed {
        rows.left = rows.left.or(Some(k.removed + k.changed + k.unchanged));
        rows.right = rows.right.or(Some(k.added + k.changed + k.unchanged));
    }
    let found = found(&schema, &rows, &stats.columns, keyed.as_ref());
    Ok(DiffReport {
        version: REPORT_VERSION,
        left: Side {
            source: left_schema.source,
            format: left_schema.format,
            engine: left_schema.engine,
            snapshot: None,
            committed_ms: None,
        },
        right: Side {
            source: right_schema.source,
            format: right_schema.format,
            engine: right_schema.engine,
            snapshot: None,
            committed_ms: None,
        },
        schema,
        rows,
        stats,
        keyed,
        found,
        notes,
    })
}

/// Compare `left` with `right` where either names a snapshot (`<table>@<snapshot>`): both sides
/// are then compared by their table metadata ([`crate::snapshot`]), the side that names none at
/// the table's current snapshot, so the two are measured alike. No row is read, and no data file:
/// a snapshot whose files were since removed still compares. Both sides must be tables with
/// metadata of their own, Iceberg or Delta.
///
/// With no snapshot on either side this is [`diff`].
///
/// # Errors
///
/// `--key`, `--fast` and `--scan` with a snapshot, which read rows or footers; a side that is not
/// an Iceberg or Delta table; a snapshot the table does not have; whatever reading the metadata
/// fails with.
pub fn diff_snapshots(
    ctx: &RequestContext,
    engines: &EngineRegistry,
    (left, left_at): (&Source, Option<&SnapshotRef>),
    (right, right_at): (&Source, Option<&SnapshotRef>),
    opts: &DiffOptions,
) -> Result<DiffReport> {
    if left_at.is_none() && right_at.is_none() {
        return diff(ctx, engines, left, right, opts);
    }
    let refuse = |what: &str| {
        Err(EngineError::Other(format!(
            "{what} reads rows, and a snapshot is compared by its table metadata, with no row \
             read: drop it, or drop the `@<snapshot>`"
        )))
    };
    if !opts.key.is_empty() {
        return refuse("`--key`");
    }
    match opts.stats {
        StatsMode::Footer => return refuse("`--fast`"),
        StatsMode::Sample(_) => return refuse("`--scan`"),
        StatsMode::Auto => {}
    }
    let left_meta = engines
        .resolve(Need::Read(left))?
        .metadata(ctx, left, left_at)?;
    let right_meta = engines
        .resolve(Need::Read(right))?
        .metadata(ctx, right, right_at)?;
    let schema = compare_schemas(&left_meta.columns, &right_meta.columns);
    let types: BTreeMap<String, (String, String)> = left_meta
        .columns
        .iter()
        .filter_map(|l| {
            let r = right_meta.columns.iter().find(|r| r.name == l.name)?;
            Some((l.name.clone(), (l.data_type.clone(), r.data_type.clone())))
        })
        .collect();
    let stats = StatsDiff {
        method: StatsMethod::Metadata,
        engine: if left_meta.engine == right_meta.engine {
            left_meta.engine.clone()
        } else {
            format!("{} and {}", left_meta.engine, right_meta.engine)
        },
        left_rows_read: 0,
        right_rows_read: 0,
        columns: compare_stats(&left_meta.stats, &right_meta.stats, &types),
    };
    let rows = RowCounts {
        left: left_meta.rows,
        right: right_meta.rows,
    };
    // A note both sides give is said once.
    let mut notes: Vec<String> = Vec::new();
    for (label, meta) in [("left", &left_meta), ("right", &right_meta)] {
        for note in &meta.notes {
            let twice = right_meta.notes.contains(note) && left_meta.notes.contains(note);
            let note = if twice {
                note.clone()
            } else {
                format!("{label}: {note}")
            };
            if !notes.contains(&note) {
                notes.push(note);
            }
        }
    }
    let found = found(&schema, &rows, &stats.columns, None);
    let side = |meta: TableMetadata| Side {
        source: meta.source,
        format: meta.format,
        engine: meta.engine,
        snapshot: meta.snapshot,
        committed_ms: meta.committed_ms,
    };
    Ok(DiffReport {
        version: REPORT_VERSION,
        left: side(left_meta),
        right: side(right_meta),
        schema,
        rows,
        stats,
        keyed: None,
        found,
        notes,
    })
}

/// Each side's columns as the SQL engine reads them, asked once and shared by the statistics and
/// the rows: every query is a read of the side, and for a table loaded whole to be queried —
/// Iceberg, Delta, JSON, a Parquet directory — the probe is one too.
#[derive(Default)]
struct Probes([Option<Vec<(String, DataType)>>; 2]);

impl Probes {
    fn get(
        &mut self,
        ctx: &RequestContext,
        engine: &dyn Engine,
        named: &[NamedSource; 2],
        side: usize,
    ) -> Result<Vec<(String, DataType)>> {
        if self.0[side].is_none() {
            self.0[side] = Some(probe(ctx, engine, &named[side])?);
        }
        Ok(self.0[side].clone().unwrap_or_default())
    }
}

enum Mode {
    Exact(std::sync::Arc<dyn Engine>),
    Footer,
    Sample(usize),
}

/// The engine that runs SQL over both sides, when there is one. A database table is left to its
/// own engine's reads: its SQL is the database's dialect, not the one these queries are written in.
pub(crate) fn exact_engine(
    engines: &EngineRegistry,
    named: &[NamedSource],
) -> Option<std::sync::Arc<dyn Engine>> {
    if named.iter().any(|n| n.source.format == Format::Database) {
        return None;
    }
    engines.resolve(Need::Query(named)).ok()
}

/// A profile's statistics, by column.
fn profile_stats(profile: &TableProfile) -> Vec<(String, ColumnStats)> {
    profile
        .columns
        .iter()
        .map(|c| {
            (
                c.name.clone(),
                ColumnStats {
                    // A footer that did not record a column's nulls counted none: not 0.
                    nulls: c.null_count_known.then_some(c.null_count),
                    min: c.min.clone(),
                    max: c.max.clone(),
                },
            )
        })
        .collect()
}

/// A SQL identifier, quoted: any name a column has, case and all.
pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Whether the SQL engine's `min` and `max` take a column of this type and give a value worth
/// printing. Anything nested, binary or dictionary-encoded gets its null count only.
fn orderable(t: &DataType) -> bool {
    t.is_numeric()
        || t.is_temporal()
        || matches!(
            t,
            DataType::Boolean | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
}

/// Every row of `side`, by SQL: its row count, and each column's null count, minimum and maximum.
/// One query: `count(*)`, then per column `count(c)` and, when it is [`orderable`], `min(c)` and
/// `max(c)`.
pub(crate) fn exact_stats(
    ctx: &RequestContext,
    engine: &dyn Engine,
    side: &NamedSource,
    columns: &[(String, DataType)],
) -> Result<(u64, Vec<(String, ColumnStats)>)> {
    let table = quote(&side.name);
    let fields: Vec<(String, bool)> = columns
        .iter()
        .map(|(name, t)| (name.clone(), orderable(t)))
        .collect();
    let mut select = vec!["count(*)".to_string()];
    for (name, ordered) in &fields {
        let column = quote(name);
        select.push(format!("count({column})"));
        if *ordered {
            select.push(format!("min({column})"));
            select.push(format!("max({column})"));
        }
    }
    let select: Vec<String> = select
        .into_iter()
        .enumerate()
        .map(|(i, aggregate)| format!("{aggregate} AS a{i}"))
        .collect();
    let answer = engine.query(
        ctx,
        &format!("SELECT {} FROM {table}", select.join(", ")),
        std::slice::from_ref(side),
    )?;
    let row = one_row(&answer)?;
    let rows = count_at(&row, 0)?;
    let mut at = 1;
    let mut stats = Vec::with_capacity(fields.len());
    for (name, ordered) in fields {
        let non_null = count_at(&row, at)?;
        at += 1;
        let (min, max) = if ordered {
            let range = (
                cell(row.column(at).as_ref()),
                cell(row.column(at + 1).as_ref()),
            );
            at += 2;
            range
        } else {
            (None, None)
        };
        stats.push((
            name,
            ColumnStats {
                nulls: Some(rows.saturating_sub(non_null)),
                min,
                max,
            },
        ));
    }
    Ok((rows, stats))
}

/// `side`'s columns as the SQL engine reads them: names and Arrow types, with no row read.
pub(crate) fn probe(
    ctx: &RequestContext,
    engine: &dyn Engine,
    side: &NamedSource,
) -> Result<Vec<(String, DataType)>> {
    let answer = engine.query(
        ctx,
        &format!("SELECT * FROM {} LIMIT 0", quote(&side.name)),
        std::slice::from_ref(side),
    )?;
    Ok(answer
        .schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect())
}

/// The one row an aggregate query answers, as a batch of one row.
fn one_row(answer: &RowBatch) -> Result<RecordBatch> {
    answer
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| b.slice(0, 1))
        .ok_or_else(|| EngineError::Query("an aggregate query answered no row".to_string()))
}

/// The count in column `at` of a one-row batch: `count` answers a 64-bit integer, which a null
/// (an aggregate over nothing) reads as zero.
fn count_at(row: &RecordBatch, at: usize) -> Result<u64> {
    let column = row.column(at);
    if column.is_null(0) {
        return Ok(0);
    }
    let value = cell(column.as_ref()).unwrap_or_default();
    value
        .parse::<u64>()
        .map_err(|_| EngineError::Query(format!("a count answered `{value}`, not a number")))
}

/// The first value of `column`, as Lakeleto prints values everywhere, or `None` for a null.
pub(crate) fn cell(column: &dyn Array) -> Option<String> {
    cell_at(column, 0)
}

/// Value `row` of `column`, as Lakeleto prints values everywhere, or `None` for a null.
pub(crate) fn cell_at(column: &dyn Array, row: usize) -> Option<String> {
    if column.is_null(row) {
        return None;
    }
    let opts = FormatOptions::default();
    ArrayFormatter::try_new(column, &opts)
        .ok()
        .map(|f| f.value(row).to_string())
}

// ---- rows by key ----------------------------------------------------------------------------

/// What a column's values are, for whether two columns compare: two columns compare when their
/// values are of one kind, whatever the width or encoding of each — an `Int32` and an `Int64`, a
/// `Utf8` and a `Utf8View` — and the SQL engine compares them as such.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Number,
    Text,
    Bool,
    Date,
    Time,
    /// A timestamp in a time zone, or in none: two zones are two kinds.
    Timestamp(Option<std::sync::Arc<str>>),
    Duration,
    Binary,
    /// Nested, or anything else: never compared.
    Other,
}

fn kind(t: &DataType) -> Kind {
    match t {
        DataType::Dictionary(_, value) => kind(value),
        t if t.is_numeric() => Kind::Number,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Kind::Text,
        DataType::Boolean => Kind::Bool,
        DataType::Date32 | DataType::Date64 => Kind::Date,
        DataType::Time32(_) | DataType::Time64(_) => Kind::Time,
        DataType::Timestamp(_, zone) => Kind::Timestamp(zone.clone()),
        DataType::Duration(_) => Kind::Duration,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Kind::Binary,
        _ => Kind::Other,
    }
}

/// Why a column of type `left` and one of type `right` are not compared, or `None` when they are.
pub fn incomparable(left: &DataType, right: &DataType) -> Option<String> {
    let (l, r) = (kind(left), kind(right));
    if l == Kind::Other || r == Kind::Other {
        return Some(if left == right {
            format!("a nested type, {left}, which is not compared")
        } else {
            format!("types differ: {left} and {right}")
        });
    }
    if l != r {
        return Some(format!("types differ: {left} and {right}"));
    }
    // A fixed-size binary compares only with its own width: the SQL engine has no common type for
    // two widths, and refuses the whole query.
    let fixed = |t: &DataType| matches!(values(t), DataType::FixedSizeBinary(_));
    ((fixed(left) || fixed(right)) && left != right)
        .then(|| format!("types differ: {left} and {right}"))
}

/// The type of the values a column holds: a dictionary's values, or the column's own type.
fn values(t: &DataType) -> &DataType {
    match t {
        DataType::Dictionary(_, value) => values(value),
        t => t,
    }
}

/// Why a key column of type `left` and one of type `right` are not matched, or `None` when they
/// are. Stricter than [`incomparable`]: keys are matched on equal values, so a key of integers or
/// decimals is never matched against one of floats, which hold every integer only up to 2^53 —
/// two keys would become one, and a row would be counted twice and another not at all.
pub fn key_incomparable(left: &DataType, right: &DataType) -> Option<String> {
    if let Some(why) = incomparable(left, right) {
        return Some(why);
    }
    let float = |t: &DataType| values(t).is_floating();
    (kind(left) == Kind::Number && float(left) != float(right)).then(|| {
        format!(
            "{left} and {right}: a float holds every integer only up to 2^53, so two keys could \
             match as one — make both sides integers, or both floats"
        )
    })
}

/// The SQL shared by every keyed query: each side's key and compared columns under positional
/// names (`k0…`, `c0…`), joined by key — `IS NOT DISTINCT FROM`, so a null key matches a null key,
/// which the SQL engine plans as a hash join — into `d`, which holds for each key whether each
/// side has it (`lp`, `rp`), both sides' values, and whether each compared column differs (`d0…`).
fn keyed_cte(key: &[String], compared: &[String]) -> String {
    let side = |table: &str| {
        let mut columns: Vec<String> = key
            .iter()
            .enumerate()
            .map(|(i, k)| format!("{} AS k{i}", quote(k)))
            .collect();
        columns.extend(
            compared
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{} AS c{i}", quote(c))),
        );
        columns.push("TRUE AS present".to_string());
        format!("SELECT {} FROM {}", columns.join(", "), quote(table))
    };
    let on: Vec<String> = (0..key.len())
        .map(|i| format!("(l.k{i} IS NOT DISTINCT FROM r.k{i})"))
        .collect();
    let mut joined = vec!["l.present AS lp".to_string(), "r.present AS rp".to_string()];
    for i in 0..key.len() {
        joined.push(format!("l.k{i} AS lk{i}"));
        joined.push(format!("r.k{i} AS rk{i}"));
    }
    for i in 0..compared.len() {
        joined.push(format!("l.c{i} AS lc{i}"));
        joined.push(format!("r.c{i} AS rc{i}"));
        joined.push(format!("(l.c{i} IS DISTINCT FROM r.c{i}) AS d{i}"));
    }
    format!(
        "WITH l AS ({}), r AS ({}), d AS (SELECT {} FROM l FULL OUTER JOIN r ON {}) ",
        side(LEFT),
        side(RIGHT),
        joined.join(", "),
        on.join(" AND ")
    )
}

/// Whether a row both sides have differs in any compared column, in SQL over `d`.
fn any_differs(compared: usize) -> String {
    if compared == 0 {
        "FALSE".to_string()
    } else {
        format!(
            "({})",
            (0..compared)
                .map(|i| format!("d{i}"))
                .collect::<Vec<_>>()
                .join(" OR ")
        )
    }
}

/// `prefix0, prefix1, …` for `n` columns.
fn listed(prefix: &str, n: usize) -> String {
    (0..n)
        .map(|i| format!("{prefix}{i}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The rows of the two sides, matched by `key`, compared column by column by the SQL engine.
///
/// The key must be on both sides, of one kind on each, and name one row on each side: a key that
/// repeats would match rows many to many and miscount, so it is refused instead. A shared column
/// whose two types are not of one kind is not compared, and is listed with why.
fn keyed(
    ctx: &RequestContext,
    engines: &EngineRegistry,
    named: &[NamedSource; 2],
    probes: &mut Probes,
    key: &[String],
    sample_rows: usize,
) -> Result<KeyedDiff> {
    if named.iter().any(|n| n.source.format == Format::Database) {
        return Err(EngineError::UnsupportedOperation {
            engine: "database".to_string(),
            op: "compare rows by key".to_string(),
            hint: "rows are compared in one SQL engine, and a database table is read by its own: \
                   write it to a file first (`lakeleto head <table> -n <rows> -o parquet --out \
                   t.parquet`), and compare that"
                .to_string(),
        });
    }
    let engine = engines.resolve(Need::Query(named))?;
    let engine = engine.as_ref();
    let left: Vec<(String, DataType)> = probes.get(ctx, engine, named, 0)?;
    let right: Vec<(String, DataType)> = probes.get(ctx, engine, named, 1)?;
    let type_in = |side: &[(String, DataType)], name: &str| {
        side.iter().find(|(n, _)| n == name).map(|(_, t)| t.clone())
    };

    let mut seen = Vec::new();
    for k in key {
        if seen.contains(&k) {
            return Err(EngineError::Other(format!(
                "`{k}` is in the key twice: name each column once"
            )));
        }
        seen.push(k);
        let (Some(l), Some(r)) = (type_in(&left, k), type_in(&right, k)) else {
            let missing = if type_in(&left, k).is_none() {
                "left"
            } else {
                "right"
            };
            return Err(EngineError::Other(format!(
                "the key column `{k}` is not on the {missing} side: a key names columns both \
                 sides have"
            )));
        };
        if let Some(why) = key_incomparable(&l, &r) {
            return Err(EngineError::Other(format!(
                "the key column `{k}` cannot be matched: {why}"
            )));
        }
    }
    for (side, label) in [(&named[0], "left"), (&named[1], "right")] {
        unique_key(ctx, engine, side, key, label)?;
    }

    let mut compared = Vec::new();
    let mut not_compared = Vec::new();
    for (name, l) in &left {
        if key.contains(name) {
            continue;
        }
        let Some(r) = type_in(&right, name) else {
            continue;
        };
        match incomparable(l, &r) {
            None => compared.push(name.clone()),
            Some(reason) => not_compared.push(NotCompared {
                name: name.clone(),
                reason,
            }),
        }
    }

    let cte = keyed_cte(key, &compared);
    let any = any_differs(compared.len());
    let mut counts = vec![
        "sum(CASE WHEN rp IS NULL THEN 1 ELSE 0 END)".to_string(),
        "sum(CASE WHEN lp IS NULL THEN 1 ELSE 0 END)".to_string(),
        format!("sum(CASE WHEN lp AND rp AND {any} THEN 1 ELSE 0 END)"),
        format!("sum(CASE WHEN lp AND rp AND NOT {any} THEN 1 ELSE 0 END)"),
    ];
    counts.extend(
        (0..compared.len()).map(|i| format!("sum(CASE WHEN lp AND rp AND d{i} THEN 1 ELSE 0 END)")),
    );
    // Each named for its place: with one compared column, the changed count and that column's
    // are the same expression, which a projection may not hold twice unnamed.
    let counts: Vec<String> = counts
        .into_iter()
        .enumerate()
        .map(|(i, sum)| format!("{sum} AS n{i}"))
        .collect();
    let answer = engine.query(
        ctx,
        &format!("{cte}SELECT {} FROM d", counts.join(", ")),
        named,
    )?;
    let row = one_row(&answer)?;
    let removed = count_at(&row, 0)?;
    let added = count_at(&row, 1)?;
    let changed = count_at(&row, 2)?;
    let unchanged = count_at(&row, 3)?;
    let changed_by_column = compared
        .iter()
        .enumerate()
        .map(|(i, name)| {
            Ok(ColumnCount {
                name: name.clone(),
                changed: count_at(&row, 4 + i)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut samples = Samples::default();
    if sample_rows > 0 {
        let (k, c) = (key.len(), compared.len());
        let rows = |sql: String| -> Result<Vec<Vec<Option<String>>>> {
            let answer = engine.query(ctx, &format!("{cte}{sql} LIMIT {sample_rows}"), named)?;
            Ok(answer
                .batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        b.columns()
                            .iter()
                            .map(|col| cell_at(col.as_ref(), r))
                            .collect()
                    })
                })
                .collect())
        };
        let one_side = |side: &str, absent: &str| -> String {
            let mut columns = vec![listed(&format!("{side}k"), k)];
            if c > 0 {
                columns.push(listed(&format!("{side}c"), c));
            }
            format!(
                "SELECT {} FROM d WHERE {absent} IS NULL ORDER BY {}",
                columns.join(", "),
                listed(&format!("{side}k"), k)
            )
        };
        let split = |mut row: Vec<Option<String>>| {
            let values = row.split_off(k);
            SampleRow { key: row, values }
        };
        if removed > 0 {
            samples.removed = rows(one_side("l", "rp"))?.into_iter().map(split).collect();
        }
        if added > 0 {
            samples.added = rows(one_side("r", "lp"))?.into_iter().map(split).collect();
        }
        if changed > 0 {
            let mut columns = vec![listed("lk", k)];
            columns.extend((0..c).map(|i| format!("lc{i}, rc{i}, d{i}")));
            let sql = format!(
                "SELECT {} FROM d WHERE lp AND rp AND {any} ORDER BY {}",
                columns.join(", "),
                listed("lk", k)
            );
            samples.changed = rows(sql)?
                .into_iter()
                .map(|mut row| {
                    let cells = row.split_off(k);
                    let changes = cells
                        .chunks(3)
                        .zip(&compared)
                        .filter(|(cell, _)| cell[2].as_deref() == Some("true"))
                        .map(|(cell, name)| CellChange {
                            column: name.clone(),
                            left: cell[0].clone(),
                            right: cell[1].clone(),
                        })
                        .collect();
                    ChangedRow { key: row, changes }
                })
                .collect();
        }
    }

    Ok(KeyedDiff {
        key: key.to_vec(),
        compared,
        not_compared,
        added,
        removed,
        changed,
        unchanged,
        changed_by_column,
        samples,
    })
}

/// Refuse a key that names more than one row on `side`, saying how many keys repeat and one of
/// them, the most repeated.
fn unique_key(
    ctx: &RequestContext,
    engine: &dyn Engine,
    side: &NamedSource,
    key: &[String],
    label: &str,
) -> Result<()> {
    let columns = key.iter().map(|k| quote(k)).collect::<Vec<_>>().join(", ");
    let table = quote(&side.name);
    // No alias of its own: a key column may be called anything an alias would be.
    let grouped = format!("FROM {table} GROUP BY {columns} HAVING count(*) > 1");
    let answer = engine.query(
        ctx,
        &format!("SELECT count(*) FROM (SELECT {columns} {grouped}) AS repeated"),
        std::slice::from_ref(side),
    )?;
    let repeated = count_at(&one_row(&answer)?, 0)?;
    if repeated == 0 {
        return Ok(());
    }
    let example = engine.query(
        ctx,
        &format!("SELECT {columns}, count(*) {grouped} ORDER BY count(*) DESC LIMIT 1"),
        std::slice::from_ref(side),
    )?;
    let row = one_row(&example)?;
    let named: Vec<String> = key
        .iter()
        .enumerate()
        .map(|(i, k)| {
            format!(
                "{k}={}",
                cell_at(row.column(i).as_ref(), 0).unwrap_or("null".into())
            )
        })
        .collect();
    let times = count_at(&row, key.len())?;
    Err(EngineError::Other(format!(
        "the key ({}) names more than one row on the {label} side: {repeated} key{} repeat{}, \
         {} {times} times. A key must name one row on each side: add a column to it",
        key.join(", "),
        if repeated == 1 { "" } else { "s" },
        if repeated == 1 { "s" } else { "" },
        named.join(", ")
    )))
}

// ---- rendering ------------------------------------------------------------------------------

/// The longest value printed in a table or Markdown cell; JSON carries every value whole.
const MAX_VALUE: usize = 60;

/// Text a table or a path supplied, safe to print: each control character written as its escape
/// (`\n`, `\t`, `\r`, `\x1b`, `\u{9b}`), and the invisible characters that reorder or hide text
/// (bidirectional overrides and isolates, zero-width marks, the byte-order mark) as `\u{…}` — so
/// nothing in the data can move the cursor, clear a line, recolour the report or disguise a value.
/// Everything else as it is. JSON carries values whole and never goes through this.
pub(crate) fn visible(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.chars().any(hidden) {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        push_visible(&mut out, c);
    }
    std::borrow::Cow::Owned(out)
}

/// Whether [`visible`] writes `c` as an escape.
fn hidden(c: char) -> bool {
    c.is_control()
        || matches!(c, '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}')
}

/// `c` as [`visible`] writes it.
fn push_visible(out: &mut String, c: char) {
    match c {
        '\n' => out.push_str("\\n"),
        '\t' => out.push_str("\\t"),
        '\r' => out.push_str("\\r"),
        c if hidden(c) && (c as u32) < 0x80 => out.push_str(&format!("\\x{:02x}", c as u32)),
        c if hidden(c) => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
        c => out.push(c),
    }
}

/// A value as a report shows it: [`visible`], and cut to [`MAX_VALUE`] characters. A value is cut
/// between its own characters, each written whole, so an escape is never cut in two.
fn shown(value: &Option<String>) -> String {
    let Some(v) = value else {
        return "—".to_string();
    };
    let whole = visible(v);
    if whole.chars().count() <= MAX_VALUE {
        return whole.into_owned();
    }
    let (mut cut, mut width, mut one) = (String::new(), 0, String::new());
    for c in v.chars() {
        one.clear();
        push_visible(&mut one, c);
        width += one.chars().count();
        if width > MAX_VALUE - 1 {
            break;
        }
        cut.push_str(&one);
    }
    format!("{cut}…")
}

fn count(value: Option<u64>) -> String {
    value.map_or_else(|| "unknown".to_string(), |v| v.to_string())
}

fn delta(rows: &RowCounts) -> String {
    match (rows.left, rows.right) {
        (Some(l), Some(r)) if l == r => "no change".to_string(),
        (Some(l), Some(r)) => {
            let d = i128::from(r) - i128::from(l);
            let pct = if l == 0 {
                String::new()
            } else {
                format!(", {:+.2}%", d as f64 / l as f64 * 100.0)
            };
            format!("{d:+}{pct}")
        }
        _ => "not comparable".to_string(),
    }
}

fn method_line(stats: &StatsDiff) -> String {
    match stats.method {
        StatsMethod::Exact => format!("exact: every row of each side, {} engine", stats.engine),
        StatsMethod::Footer => "Parquet footer statistics: exact, no row read".to_string(),
        StatsMethod::Sample => format!(
            "a sample: the first {} rows of the left side and {} of the right, {} engine — a \
             difference here may not be one between the tables",
            stats.left_rows_read, stats.right_rows_read, stats.engine
        ),
        StatsMethod::Metadata => {
            "each side's table metadata: exact where given, no row read".to_string()
        }
    }
}

/// A side's snapshot, when it was compared at one: `@3`.
fn at_of(s: &Side) -> String {
    s.snapshot
        .as_deref()
        .map_or(String::new(), |id| format!("@{}", visible(id)))
}

/// When a side's snapshot was committed, when it was compared at one: `, committed 2026-…`.
fn committed_of(s: &Side) -> String {
    match (&s.snapshot, s.committed_ms) {
        (Some(_), Some(ms)) => format!(", committed {}", crate::snapshot::instant(ms)),
        _ => String::new(),
    }
}

fn nullable(n: bool) -> &'static str {
    if n {
        "nullable"
    } else {
        "not null"
    }
}

fn type_of(c: &ColumnSchema) -> String {
    format!("{} ({})", visible(&c.data_type), nullable(c.nullable))
}

fn change_of(c: &ColumnChange) -> String {
    let mut parts = Vec::new();
    if c.left_type != c.right_type {
        parts.push(format!(
            "{} → {}",
            visible(&c.left_type),
            visible(&c.right_type)
        ));
    }
    if c.left_nullable != c.right_nullable {
        parts.push(format!(
            "{} → {}",
            nullable(c.left_nullable),
            nullable(c.right_nullable)
        ));
    }
    parts.join(", ")
}

fn stat_pair(l: &Option<String>, r: &Option<String>) -> String {
    if l == r {
        shown(l)
    } else {
        format!("{} → {}", shown(l), shown(r))
    }
}

fn nulls_pair(l: Option<u64>, r: Option<u64>) -> String {
    let n = |v: Option<u64>| v.map_or_else(|| "—".to_string(), |v| v.to_string());
    if l == r {
        n(l)
    } else {
        format!("{} → {}", n(l), n(r))
    }
}

fn found_line(report: &DiffReport) -> String {
    if report.found.is_empty() {
        "no difference".to_string()
    } else {
        report
            .found
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The report for a terminal: what differs, and nothing that does not.
pub fn render_text(report: &DiffReport) -> String {
    let mut out = String::new();
    let side = |s: &Side| {
        format!(
            "{}{}  ({}, {} engine{})",
            visible(&s.source),
            at_of(s),
            visible(&s.format),
            visible(&s.engine),
            committed_of(s)
        )
    };
    out.push_str(&format!("left   {}\n", side(&report.left)));
    out.push_str(&format!("right  {}\n", side(&report.right)));
    out.push_str(&format!("found  {}\n", found_line(report)));

    out.push_str("\nschema\n");
    let s = &report.schema;
    if s.added.is_empty() && s.removed.is_empty() && s.changed.is_empty() && !s.reordered {
        out.push_str("  the same columns\n");
    }
    let width = s
        .added
        .iter()
        .chain(&s.removed)
        .map(|c| visible(&c.name).chars().count())
        .chain(s.changed.iter().map(|c| visible(&c.name).chars().count()))
        .max()
        .unwrap_or(0);
    for c in &s.added {
        out.push_str(&format!(
            "  + {:width$}  {}\n",
            visible(&c.name),
            type_of(c)
        ));
    }
    for c in &s.removed {
        out.push_str(&format!(
            "  - {:width$}  {}\n",
            visible(&c.name),
            type_of(c)
        ));
    }
    for c in &s.changed {
        out.push_str(&format!(
            "  ~ {:width$}  {}\n",
            visible(&c.name),
            change_of(c)
        ));
    }
    if s.reordered {
        out.push_str("  the shared columns are in a different order\n");
    }

    out.push_str("\nrows\n");
    out.push_str(&format!(
        "  {} → {}  ({})\n",
        count(report.rows.left),
        count(report.rows.right),
        delta(&report.rows)
    ));

    out.push_str(&format!("\nstatistics  ({})\n", method_line(&report.stats)));
    let changed: Vec<&ColumnStatsDiff> =
        report.stats.columns.iter().filter(|c| c.changed).collect();
    if changed.is_empty() {
        out.push_str(&format!(
            "  no change in the {} shared columns compared\n",
            report
                .stats
                .columns
                .iter()
                .filter(|c| c.not_compared.is_none())
                .count()
        ));
    } else {
        let header = ["column", "nulls", "min", "max"];
        let rows: Vec<[String; 4]> = changed
            .iter()
            .map(|c| {
                [
                    visible(&c.name).into_owned(),
                    nulls_pair(c.left.nulls, c.right.nulls),
                    stat_pair(&c.left.min, &c.right.min),
                    stat_pair(&c.left.max, &c.right.max),
                ]
            })
            .collect();
        let widths: Vec<usize> = (0..4)
            .map(|i| {
                rows.iter()
                    .map(|r| r[i].chars().count())
                    .chain([header[i].len()])
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let line = |cells: [&str; 4]| {
            let mut l = String::from(" ");
            for (i, cell) in cells.iter().enumerate() {
                let pad = widths[i].saturating_sub(cell.chars().count());
                l.push(' ');
                l.push_str(cell);
                if i < 3 {
                    l.push_str(&" ".repeat(pad + 1));
                }
            }
            l.trim_end().to_string() + "\n"
        };
        out.push_str(&line(header));
        for r in &rows {
            out.push_str(&line([&r[0], &r[1], &r[2], &r[3]]));
        }
        let same = report
            .stats
            .columns
            .iter()
            .filter(|c| !c.changed && c.not_compared.is_none())
            .count();
        if same > 0 {
            out.push_str(&format!("  ({same} more shared columns unchanged)\n"));
        }
    }
    for c in &report.stats.columns {
        if let Some(why) = &c.not_compared {
            out.push_str(&format!(
                "  not compared: {} ({})\n",
                visible(&c.name),
                visible(why)
            ));
        }
    }

    if let Some(k) = &report.keyed {
        let key: Vec<String> = k
            .key
            .iter()
            .map(|name| visible(name).into_owned())
            .collect();
        out.push_str(&format!("\nrows by key  ({})\n", key.join(", ")));
        out.push_str(&format!("  {}\n", tally(k)));
        let changed_in = changed_in(k);
        if !changed_in.is_empty() {
            out.push_str(&format!("  changed in: {changed_in}\n"));
        }
        for n in &k.not_compared {
            out.push_str(&format!(
                "  not compared: {} ({})\n",
                visible(&n.name),
                visible(&n.reason)
            ));
        }
        for r in &k.samples.added {
            out.push_str(&format!(
                "  + {}  {}\n",
                key_of(k, &r.key),
                values_of(k, &r.values)
            ));
        }
        for r in &k.samples.removed {
            out.push_str(&format!(
                "  - {}  {}\n",
                key_of(k, &r.key),
                values_of(k, &r.values)
            ));
        }
        for r in &k.samples.changed {
            out.push_str(&format!(
                "  ~ {}  {}\n",
                key_of(k, &r.key),
                changes_of(&r.changes)
            ));
        }
        let shown = k.samples.added.len() + k.samples.removed.len() + k.samples.changed.len();
        if (shown as u64) < k.added + k.removed + k.changed {
            out.push_str("  (a few of each kind, least key first: `--sample N` shows more)\n");
        }
    }
    for note in &report.notes {
        out.push_str(&format!("\nnote: {note}\n"));
    }
    out
}

/// `added 1 · removed 2 · changed 3 · unchanged 4`.
fn tally(k: &KeyedDiff) -> String {
    format!(
        "added {} · removed {} · changed {} · unchanged {}",
        k.added, k.removed, k.changed, k.unchanged
    )
}

/// The compared columns rows changed in, most first: `amount 10, city 2`.
fn changed_in(k: &KeyedDiff) -> String {
    let mut counts: Vec<&ColumnCount> = k
        .changed_by_column
        .iter()
        .filter(|c| c.changed > 0)
        .collect();
    counts.sort_by_key(|c| std::cmp::Reverse(c.changed));
    counts
        .iter()
        .map(|c| format!("{} {}", visible(&c.name), c.changed))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A row's key as `id=4` or `id=4, day=2026-01-01`.
fn key_of(k: &KeyedDiff, key: &[Option<String>]) -> String {
    k.key
        .iter()
        .zip(key)
        .map(|(name, value)| format!("{}={}", visible(name), shown(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A row's compared columns as `name=ada score=9.5`.
fn values_of(k: &KeyedDiff, values: &[Option<String>]) -> String {
    k.compared
        .iter()
        .zip(values)
        .map(|(name, value)| format!("{}={}", visible(name), shown(value)))
        .collect::<Vec<_>>()
        .join("  ")
}

/// A changed row's columns as `score: 7.25 → 8`.
fn changes_of(changes: &[CellChange]) -> String {
    changes
        .iter()
        .map(|c| {
            format!(
                "{}: {} → {}",
                visible(&c.column),
                shown(&c.left),
                shown(&c.right)
            )
        })
        .collect::<Vec<_>>()
        .join("  ")
}

/// Our own prose in a Markdown cell: pipes escaped and newlines flattened, so it cannot break the
/// table. Never for anything read from a table: that goes through [`code`].
fn md(value: &str) -> String {
    value.replace('|', "\\|").replace(['\n', '\r'], " ")
}

/// Anything a table or a path supplied — a value, a column name, a type, a source — as inline
/// code, so it shows exactly as stored and nothing in it renders: no mention, link, image or
/// HTML. The fence is one backtick longer than the longest run of them inside; a value that
/// starts or ends with a backtick or a space is padded with one, which the reader strips again.
/// Pipes are escaped for the table, newlines flattened, and an empty string says so.
fn code(value: &str) -> String {
    if value.is_empty() {
        return "(empty)".to_string();
    }
    let flat = visible(value);
    let mut longest = 0;
    let mut run = 0;
    for c in flat.chars() {
        run = if c == '`' { run + 1 } else { 0 };
        longest = longest.max(run);
    }
    let fence = "`".repeat(longest + 1);
    let pad = flat.starts_with('`')
        || flat.ends_with('`')
        || (flat.starts_with(' ') && flat.ends_with(' '));
    let inner = if pad {
        format!(" {flat} ")
    } else {
        flat.into_owned()
    };
    format!("{fence}{}{fence}", inner.replace('|', "\\|"))
}

/// A statistic or a cell as [`code`], cut as [`shown`] cuts it; `—` for none.
fn code_value(value: &Option<String>) -> String {
    match value {
        None => "—".to_string(),
        Some(_) => code(&shown(value)),
    }
}

/// `a`, or `a` → `b` when the two sides differ.
fn code_pair(left: &Option<String>, right: &Option<String>) -> String {
    if left == right {
        code_value(left)
    } else {
        format!("{} → {}", code_value(left), code_value(right))
    }
}

/// A column's type for the schema table: `Utf8` (nullable).
fn code_type(c: &ColumnSchema) -> String {
    format!("{} ({})", code(&c.data_type), nullable(c.nullable))
}

/// A shared column's change for the schema table: `Int64` → `Int32`, not null → nullable.
fn code_change(c: &ColumnChange) -> String {
    let mut parts = Vec::new();
    if c.left_type != c.right_type {
        parts.push(format!("{} → {}", code(&c.left_type), code(&c.right_type)));
    }
    if c.left_nullable != c.right_nullable {
        parts.push(format!(
            "{} → {}",
            nullable(c.left_nullable),
            nullable(c.right_nullable)
        ));
    }
    parts.join(", ")
}

/// `name` = `value`, for each pair, comma-separated.
fn code_fields(names: &[String], values: &[Option<String>]) -> String {
    names
        .iter()
        .zip(values)
        .map(|(name, value)| format!("{} = {}", code(name), code_value(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A changed row's columns: `score`: `7.25` → `8`.
fn code_changes(changes: &[CellChange]) -> String {
    changes
        .iter()
        .map(|c| {
            format!(
                "{}: {} → {}",
                code(&c.column),
                code_value(&c.left),
                code_value(&c.right)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The report for a pull request or an issue: the same content as [`render_text`], in Markdown.
/// Everything a table supplied is inline code ([`code`]): a report posted as a comment cannot
/// mention anyone, show an image or link anywhere because of what the data holds.
pub fn render_markdown(report: &DiffReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "### lakeleto diff: {}\n\n",
        md(&found_line(report))
    ));
    out.push_str("| | source | format | engine |\n|---|---|---|---|\n");
    for (label, s) in [("left", &report.left), ("right", &report.right)] {
        out.push_str(&format!(
            "| {label} | {}{} | {} | {} |\n",
            code(&format!("{}{}", s.source, at_of(s))),
            md(&committed_of(s)),
            code(&s.format),
            code(&s.engine)
        ));
    }

    out.push_str("\n**Schema**\n\n");
    let s = &report.schema;
    if s.added.is_empty() && s.removed.is_empty() && s.changed.is_empty() {
        out.push_str("The same columns.\n");
    } else {
        out.push_str("| | column | change |\n|---|---|---|\n");
        for c in &s.added {
            out.push_str(&format!(
                "| added | {} | {} |\n",
                code(&c.name),
                code_type(c)
            ));
        }
        for c in &s.removed {
            out.push_str(&format!(
                "| removed | {} | {} |\n",
                code(&c.name),
                code_type(c)
            ));
        }
        for c in &s.changed {
            out.push_str(&format!(
                "| changed | {} | {} |\n",
                code(&c.name),
                code_change(c)
            ));
        }
    }
    if s.reordered {
        out.push_str("\nThe shared columns are in a different order.\n");
    }

    out.push_str(&format!(
        "\n**Rows**: {} → {} ({})\n",
        count(report.rows.left),
        count(report.rows.right),
        delta(&report.rows)
    ));

    out.push_str(&format!(
        "\n**Statistics** ({})\n\n",
        md(&method_line(&report.stats))
    ));
    let changed: Vec<&ColumnStatsDiff> =
        report.stats.columns.iter().filter(|c| c.changed).collect();
    if changed.is_empty() {
        out.push_str(&format!(
            "No change in the {} shared columns compared.\n",
            report
                .stats
                .columns
                .iter()
                .filter(|c| c.not_compared.is_none())
                .count()
        ));
    } else {
        out.push_str("| column | nulls | min | max |\n|---|---|---|---|\n");
        for c in &changed {
            out.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                code(&c.name),
                md(&nulls_pair(c.left.nulls, c.right.nulls)),
                code_pair(&c.left.min, &c.right.min),
                code_pair(&c.left.max, &c.right.max)
            ));
        }
    }
    for c in &report.stats.columns {
        if let Some(why) = &c.not_compared {
            out.push_str(&format!(
                "\nNot compared: {} ({})\n",
                code(&c.name),
                code(why)
            ));
        }
    }
    if let Some(k) = &report.keyed {
        let key: Vec<String> = k.key.iter().map(|name| code(name)).collect();
        out.push_str(&format!(
            "\n**Rows by key** ({}): {}\n",
            key.join(", "),
            tally(k)
        ));
        let mut counts: Vec<&ColumnCount> = k
            .changed_by_column
            .iter()
            .filter(|c| c.changed > 0)
            .collect();
        counts.sort_by_key(|c| std::cmp::Reverse(c.changed));
        if !counts.is_empty() {
            let changed_in: Vec<String> = counts
                .iter()
                .map(|c| format!("{} {}", code(&c.name), c.changed))
                .collect();
            out.push_str(&format!("\nChanged in: {}\n", changed_in.join(", ")));
        }
        for n in &k.not_compared {
            out.push_str(&format!(
                "\nNot compared: {} ({})\n",
                code(&n.name),
                code(&n.reason)
            ));
        }
        let samples = &k.samples;
        if !samples.added.is_empty() || !samples.removed.is_empty() || !samples.changed.is_empty() {
            out.push_str("\n| | key | row |\n|---|---|---|\n");
            for r in &samples.added {
                out.push_str(&format!(
                    "| added | {} | {} |\n",
                    code_fields(&k.key, &r.key),
                    code_fields(&k.compared, &r.values)
                ));
            }
            for r in &samples.removed {
                out.push_str(&format!(
                    "| removed | {} | {} |\n",
                    code_fields(&k.key, &r.key),
                    code_fields(&k.compared, &r.values)
                ));
            }
            for r in &samples.changed {
                out.push_str(&format!(
                    "| changed | {} | {} |\n",
                    code_fields(&k.key, &r.key),
                    code_changes(&r.changes)
                ));
            }
        }
    }
    for note in &report.notes {
        out.push_str(&format!("\n> {}\n", md(note)));
    }
    out.push_str(&format!(
        "\n<sub>lakeleto {} · report version {}</sub>\n",
        env!("CARGO_PKG_VERSION"),
        report.version
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, t: &str, nullable: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            data_type: t.to_string(),
            nullable,
        }
    }

    fn stats(nulls: u64, min: &str, max: &str) -> ColumnStats {
        ColumnStats {
            nulls: Some(nulls),
            min: Some(min.to_string()),
            max: Some(max.to_string()),
        }
    }

    #[test]
    fn columns_are_matched_by_name_and_each_kind_of_change_is_told_apart() {
        let left = [
            col("id", "Int64", false),
            col("city", "Utf8", true),
            col("amount", "Float64", true),
            col("gone", "Utf8", true),
        ];
        let right = [
            col("id", "Int32", false),
            col("amount", "Float64", false),
            col("city", "Utf8", true),
            col("new", "Boolean", true),
        ];
        let d = compare_schemas(&left, &right);
        assert_eq!(d.added, [col("new", "Boolean", true)]);
        assert_eq!(d.removed, [col("gone", "Utf8", true)]);
        let changed: Vec<(&str, &str, &str, bool, bool)> = d
            .changed
            .iter()
            .map(|c| {
                (
                    c.name.as_str(),
                    c.left_type.as_str(),
                    c.right_type.as_str(),
                    c.left_nullable,
                    c.right_nullable,
                )
            })
            .collect();
        assert_eq!(
            changed,
            [
                ("id", "Int64", "Int32", false, false),
                ("amount", "Float64", "Float64", true, false)
            ]
        );
        assert!(d.reordered, "city and amount swapped places");
        let rows = RowCounts {
            left: Some(10),
            right: Some(10),
        };
        assert_eq!(
            found(&d, &rows, &[], None),
            [
                Change::AddedColumn,
                Change::RemovedColumn,
                Change::TypeChange,
                Change::Nullability,
                Change::ColumnOrder
            ]
        );
    }

    #[test]
    fn the_same_columns_in_the_same_order_are_no_difference() {
        let cols = [col("a", "Int64", true), col("b", "Utf8", true)];
        let d = compare_schemas(&cols, &cols);
        assert_eq!(d, SchemaDiff::default());
        let rows = RowCounts {
            left: Some(3),
            right: Some(3),
        };
        assert!(found(&d, &rows, &[], None).is_empty());
        let unknown = RowCounts {
            left: None,
            right: Some(3),
        };
        assert!(
            found(&d, &unknown, &[], None).is_empty(),
            "an unknown count is not a different one"
        );
    }

    #[test]
    fn a_statistic_one_side_lacks_is_not_a_change() {
        let left = vec![
            ("a".to_string(), stats(0, "1", "9")),
            ("b".to_string(), stats(2, "x", "z")),
            (
                "c".to_string(),
                ColumnStats {
                    nulls: Some(0),
                    min: None,
                    max: None,
                },
            ),
            ("only_left".to_string(), stats(0, "1", "1")),
        ];
        let right = vec![
            ("b".to_string(), stats(5, "x", "z")),
            ("a".to_string(), stats(0, "1", "9")),
            ("c".to_string(), stats(0, "1", "2")),
        ];
        let d = compare_stats(&left, &right, &BTreeMap::new());
        let changed: Vec<(&str, bool)> = d.iter().map(|c| (c.name.as_str(), c.changed)).collect();
        assert_eq!(changed, [("a", false), ("b", true), ("c", false)]);
    }

    #[test]
    fn fail_on_names_kinds_and_groups_of_them() {
        let report = |found: Vec<Change>| DiffReport {
            version: REPORT_VERSION,
            left: Side {
                source: "l".into(),
                format: "csv".into(),
                engine: "local".into(),
                snapshot: None,
                committed_ms: None,
            },
            right: Side {
                source: "r".into(),
                format: "csv".into(),
                engine: "local".into(),
                snapshot: None,
                committed_ms: None,
            },
            schema: SchemaDiff::default(),
            rows: RowCounts {
                left: None,
                right: None,
            },
            stats: StatsDiff {
                method: StatsMethod::Exact,
                engine: "sql".into(),
                left_rows_read: 0,
                right_rows_read: 0,
                columns: vec![],
            },
            keyed: None,
            found,
            notes: vec![],
        };
        let added = report(vec![Change::AddedColumn]);
        assert!(!added.fails(&[FailOn::RemovedColumn, FailOn::TypeChange]));
        assert!(added.fails(&[FailOn::Schema]));
        assert!(added.fails(&[FailOn::Any]));
        assert!(!added.fails(&[]));
        let none = report(vec![]);
        assert!(!none.fails(&[FailOn::Any]));
    }

    #[test]
    fn columns_compare_when_their_values_are_of_one_kind() {
        use arrow_schema::{IntervalUnit, TimeUnit};
        let compares = |l: DataType, r: DataType| incomparable(&l, &r).is_none();
        assert!(compares(DataType::Int32, DataType::Int64));
        assert!(compares(DataType::Int64, DataType::Float64));
        assert!(compares(DataType::Utf8, DataType::Utf8View));
        assert!(compares(
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Utf8
        ));
        assert!(compares(
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        ));
        assert!(!compares(
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into()))
        ));
        assert!(!compares(DataType::Utf8, DataType::Int64));
        assert!(!compares(DataType::Date32, DataType::Utf8));
        let list = DataType::new_list(DataType::Int32, true);
        assert_eq!(
            incomparable(&list, &list).unwrap(),
            format!("a nested type, {list}, which is not compared")
        );
        assert!(!compares(
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::DayTime)
        ));
    }

    #[test]
    fn statistics_compare_by_kind_and_numbers_as_numbers() {
        assert_eq!(stats_rule("Int64", "Float64"), Ok(true));
        assert_eq!(stats_rule("Int32", "Int32"), Ok(true));
        assert_eq!(stats_rule("Utf8", "Utf8View"), Ok(false));
        assert_eq!(stats_rule("Timestamp(s)", "Timestamp(ns)"), Ok(false));
        assert!(stats_rule("Timestamp(s)", "Timestamp(s, \"UTC\")").is_err());
        assert_eq!(
            stats_rule("Utf8", "Int64"),
            Err("types differ: Utf8 and Int64".to_string())
        );
        assert!(same_value("2", "2.0", true));
        assert!(
            !same_value("2", "2.0", false),
            "text is compared as written"
        );
        assert!(same_value("NaN", "NaN", true));
        assert!(!same_value("0.0", "-0.0", true), "as rows compare them");
        assert!(
            !same_value("9007199254740993", "9007199254740992", true),
            "integers exactly, past a float's precision"
        );
        assert!(
            !same_value("123456789012345678.01", "123456789012345678.02", true),
            "decimals exactly, past a float's precision"
        );
        assert!(same_value("02.50", "2.5", true));
        assert!(
            same_value("1e3", "1000", true),
            "an exponent falls back to floats"
        );
        assert_eq!(stats_rule("Interval(DayTime)", "Int64").ok(), None);
        assert_eq!(stats_rule("Decimal128(38, 2)", "Int64"), Ok(true));
        let types: BTreeMap<String, (String, String)> = [
            (
                "n".to_string(),
                ("Int64".to_string(), "Float64".to_string()),
            ),
            (
                "when".to_string(),
                ("Utf8".to_string(), "Int64".to_string()),
            ),
        ]
        .into_iter()
        .collect();
        let d = compare_stats(
            &[
                ("n".to_string(), stats(0, "2", "3")),
                ("when".to_string(), stats(0, "a", "b")),
            ],
            &[
                ("n".to_string(), stats(0, "2.0", "3.0")),
                ("when".to_string(), stats(0, "10", "20")),
            ],
            &types,
        );
        assert!(!d[0].changed && d[0].not_compared.is_none());
        assert!(!d[1].changed);
        assert_eq!(
            d[1].not_compared.as_deref(),
            Some("types differ: Utf8 and Int64")
        );
    }

    #[test]
    fn keys_match_exactly_or_not_at_all() {
        let matches = |l: DataType, r: DataType| key_incomparable(&l, &r).is_none();
        assert!(matches(DataType::Int32, DataType::Int64));
        assert!(matches(DataType::Float32, DataType::Float64));
        assert!(matches(DataType::Int64, DataType::Decimal128(38, 2)));
        assert!(!matches(DataType::Int64, DataType::Float64));
        assert!(!matches(DataType::Decimal128(38, 2), DataType::Float64));
        // A dictionary is judged by the values it holds.
        let dictionary = |v: DataType| DataType::Dictionary(Box::new(DataType::Int32), Box::new(v));
        assert!(!matches(dictionary(DataType::Float64), DataType::Int64));
        assert!(!matches(DataType::Int64, dictionary(DataType::Float64)));
        assert!(matches(dictionary(DataType::Float64), DataType::Float64));
        assert!(matches(dictionary(DataType::Int64), DataType::Int64));
        assert!(incomparable(
            &dictionary(DataType::FixedSizeBinary(2)),
            &dictionary(DataType::FixedSizeBinary(3))
        )
        .is_some());
        assert!(matches(DataType::Utf8, DataType::Utf8View));
        // Values of an integer and a float column still compare, as floats.
        assert!(incomparable(&DataType::Int64, &DataType::Float64).is_none());
        // Fixed-size binaries compare only with their own width.
        assert!(
            incomparable(&DataType::FixedSizeBinary(2), &DataType::FixedSizeBinary(2)).is_none()
        );
        assert!(
            incomparable(&DataType::FixedSizeBinary(2), &DataType::FixedSizeBinary(3)).is_some()
        );
        assert!(incomparable(&DataType::FixedSizeBinary(2), &DataType::Binary).is_some());
        assert!(incomparable(&DataType::Binary, &DataType::LargeBinary).is_none());
    }

    #[test]
    fn what_a_table_holds_cannot_drive_the_terminal() {
        assert_eq!(visible("plain"), "plain");
        assert_eq!(visible("a\nb\tc\rd"), "a\\nb\\tc\\rd");
        assert_eq!(visible("\u{1b}[2Jgone"), "\\x1b[2Jgone");
        assert_eq!(visible("\u{9b}31m"), "\\u{9b}31m");
        assert_eq!(visible("evil\u{202E}gpj.exe"), "evil\\u{202e}gpj.exe");
        assert_eq!(visible("zero\u{200B}width"), "zero\\u{200b}width");
        assert_eq!(visible("café ✓"), "café ✓", "printable text is left alone");
        // A long value is cut, never its escapes: an escape is shown whole or not at all.
        let long = format!("{}\u{1b}", "x".repeat(MAX_VALUE));
        assert!(!shown(&Some(long)).contains('\u{1b}'));
        let x = |n: usize| "x".repeat(n);
        let cut = shown(&Some(format!("{}\u{1b}[2J", x(MAX_VALUE - 2))));
        assert_eq!(
            cut,
            format!("{}…", x(MAX_VALUE - 2)),
            "the escape would not fit: left out"
        );
        let cut = shown(&Some(format!("{}\u{1b}{}", x(MAX_VALUE - 5), x(9))));
        assert_eq!(
            cut,
            format!("{}\\x1b…", x(MAX_VALUE - 5)),
            "it fits: shown whole"
        );
        assert_eq!(cut.chars().count(), MAX_VALUE);
        let fits = format!("{}\u{1b}", x(MAX_VALUE - 4));
        assert_eq!(
            shown(&Some(fits)),
            format!("{}\\x1b", x(MAX_VALUE - 4)),
            "not cut at all"
        );
    }

    #[test]
    fn identifiers_are_quoted_whatever_they_hold() {
        assert_eq!(quote("id"), "\"id\"");
        assert_eq!(quote("Order ID"), "\"Order ID\"");
        assert_eq!(quote("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn markdown_cells_cannot_break_their_table() {
        assert_eq!(md("a|b\nc"), "a\\|b c");
    }

    #[test]
    fn anything_a_table_supplies_is_inline_code_whatever_it_holds() {
        assert_eq!(code("Berlin"), "`Berlin`");
        assert_eq!(code("na`me"), "``na`me``");
        assert_eq!(code("a``b"), "```a``b```");
        assert_eq!(code("`edge"), "`` `edge ``");
        assert_eq!(code(" both "), "`  both  `");
        assert_eq!(code("a|b\nc"), "`a\\|b\\nc`");
        assert_eq!(code(""), "(empty)");
        assert_eq!(
            code("@team ![x](https://h/p.png) [l](https://h)"),
            "`@team ![x](https://h/p.png) [l](https://h)`"
        );
    }
}
