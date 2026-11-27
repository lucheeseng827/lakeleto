//! `lakeleto check`: a table against what it was, as a gate for CI and pipelines.
//!
//! A check measures one table — its columns, its rows counted one by one, each column's nulls,
//! its data files and their bytes, and when it last changed — into a [`Fingerprint`]. Saved as a
//! baseline file, a fingerprint is what the next check compares with:
//!
//! - **schema**: a column removed, retyped or changed in nullability fails; a column added, or the
//!   columns in a new order, is reported and passes, since a table that grows is healthy;
//! - **row-count**: fewer rows by more than a share fails;
//! - **nulls**: a column whose share of nulls rose by more than some points fails;
//! - **files**: the data files, or their bytes, growing or shrinking by more than a factor fails;
//! - **freshness**: with `--max-age`, a table last changed longer ago than that fails — this one
//!   needs no baseline;
//! - **contract**: with `--contract`, every row checked against a data contract by Covenant's
//!   validator, failing past the violations the contract allows (feature `contracts`).
//!
//! Every gate ends one of four ways: it passed, it failed, it was skipped (nothing to compare: no
//! baseline yet, or files this kind of table has none of), or it is unknown — asked for, and it
//! could not be told. A check fails when a gate fails (exit 3), and errs when a gate it was asked
//! for is unknown (exit 1): a gate it could not judge never reads as one that passed.
//!
//! Every count is of the whole table: a gate over a sample would be a guess. The rows and nulls
//! are counted one by one in the SQL engine, which the release binaries have, or, for a single
//! Parquet file, read from its footer (`--fast`, and the way a build without SQL counts one) —
//! where a column whose nulls the footer did not record is not counted, never counted as none.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::context::RequestContext;
use crate::diff::{compare_schemas, SchemaDiff, StatsMethod};
use crate::engine::registry::{EngineRegistry, Need};
use crate::engine::{ColumnSchema, NamedSource};
use crate::error::{EngineError, Result};
use crate::source::{Format, Source};

/// The version of the [`Fingerprint`] a baseline file holds. A baseline of another version is
/// refused, never read as this one.
pub const FINGERPRINT_VERSION: u32 = 1;

/// The version of the [`CheckReport`] `-o json` writes.
pub const REPORT_VERSION: u32 = 1;

/// The exit code of a check with a gate that failed: `diff`'s, for the same meaning. `0` is every
/// gate passed or skipped, `1` an error — a gate asked for and unknown among them.
pub const EXIT_FAILED: i32 = crate::diff::EXIT_FOUND;

/// The name the table is registered under for SQL.
const TABLE: &str = "lakeleto_check";

/// The share of rows a check allows to go missing when nobody says otherwise, in percent.
pub const DEFAULT_MAX_ROW_DROP: u32 = 10;
/// The rise in a column's share of nulls a check allows when nobody says otherwise, in
/// percentage points.
pub const DEFAULT_MAX_NULL_RISE: u32 = 5;
/// The factor the files and their bytes may grow or shrink by when nobody says otherwise, in
/// hundredths: 2×.
pub const DEFAULT_MAX_FILE_JUMP: u64 = 200;

/// One column's null count. `None` is not counted, never zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnNulls {
    pub name: String,
    pub nulls: Option<u64>,
}

/// A table, measured: what a baseline file holds, and what a check compares with it. No value the
/// table holds is in it — no minimum, maximum or sample — so a baseline can be committed beside
/// the code that checks it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fingerprint {
    /// [`FINGERPRINT_VERSION`].
    pub version: u32,
    pub source: String,
    pub format: String,
    /// When it was measured, in milliseconds since the epoch.
    pub taken_ms: i64,
    /// How the rows were counted: every one, by SQL.
    pub method: StatsMethod,
    pub rows: u64,
    pub columns: Vec<ColumnSchema>,
    pub nulls: Vec<ColumnNulls>,
    /// The table's data files, when the reader can count them: an Iceberg table's current
    /// snapshot's (here, in a store or behind a catalog), a Delta table's from its log, a Parquet
    /// directory's `.parquet` files, a single file or object as one.
    #[serde(default)]
    pub files: Option<u64>,
    /// Those files' bytes, when every one's size is known.
    #[serde(default)]
    pub bytes: Option<u64>,
    /// When the table last changed, in milliseconds since the epoch: an Iceberg table's current
    /// snapshot, a Delta table's latest commit, a file's modification time.
    #[serde(default)]
    pub updated_ms: Option<i64>,
}

/// A gate, by the name the report gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GateName {
    Schema,
    RowCount,
    Nulls,
    Files,
    Freshness,
    /// Every row against a data contract (`--contract`).
    Contract,
}

impl GateName {
    /// The name the report writes.
    pub fn name(self) -> &'static str {
        match self {
            GateName::Schema => "schema",
            GateName::RowCount => "row-count",
            GateName::Nulls => "nulls",
            GateName::Files => "files",
            GateName::Freshness => "freshness",
            GateName::Contract => "contract",
        }
    }
}

/// How a gate ended, and how a check did: the worst of its gates, in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Pass,
    /// Nothing to compare, and nobody asked for it: no baseline yet, or files this kind of table
    /// has none of that this machine can count.
    Skipped,
    /// Asked for, and it could not be told. The check errs (exit 1).
    Unknown,
    /// The check fails (exit [`EXIT_FAILED`]).
    Fail,
}

impl Status {
    /// The name the report writes.
    pub fn name(self) -> &'static str {
        match self {
            Status::Pass => "pass",
            Status::Skipped => "skipped",
            Status::Unknown => "unknown",
            Status::Fail => "fail",
        }
    }
}

/// One gate's verdict: a line saying why, and a line for each thing it found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gate {
    pub name: GateName,
    pub status: Status,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<String>,
    /// Whether the gate judged only part of what it was asked: a contract with rules this checker
    /// cannot enforce, checked under `--allow-unenforced`. Its pass is a partial one.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub partial: bool,
}

/// How far a table may move before a gate fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The share of rows that may go missing, in percent.
    pub max_row_drop_pct: u32,
    /// The rise in a column's share of nulls allowed, in percentage points.
    pub max_null_rise_points: u32,
    /// The factor the files and their bytes may grow or shrink by, in hundredths (200 is 2×).
    pub max_file_jump: u64,
    /// Whether the file factor was asked for by name: then a table whose files are not counted
    /// cannot pass it, and the gate is unknown rather than skipped.
    pub files_asked: bool,
    /// How long ago the table may have last changed, in milliseconds. `None` checks no age.
    pub max_age_ms: Option<u64>,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_row_drop_pct: DEFAULT_MAX_ROW_DROP,
            max_null_rise_points: DEFAULT_MAX_NULL_RISE,
            max_file_jump: DEFAULT_MAX_FILE_JUMP,
            files_asked: false,
            max_age_ms: None,
        }
    }
}

/// What a check found. `-o json` writes it as it is.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckReport {
    /// [`REPORT_VERSION`].
    pub version: u32,
    /// The engine that counted the rows.
    pub engine: String,
    pub measured: Fingerprint,
    /// The baseline file compared with, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_path: Option<String>,
    /// What it held.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<Fingerprint>,
    /// How the columns moved since the baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDiff>,
    pub gates: Vec<Gate>,
    /// The worst of the gates.
    pub status: Status,
    /// Whether this check wrote its measurement as the new baseline (`--update`).
    pub baseline_written: bool,
    /// What a data contract found, when one was checked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<ContractResult>,
    /// What a reader should know about how the check was made.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

impl CheckReport {
    /// `0` when every gate passed or was skipped, [`EXIT_FAILED`] when one failed, `1` when one
    /// asked for could not be told and none failed.
    pub fn exit_code(&self) -> i32 {
        match self.status {
            Status::Pass | Status::Skipped => 0,
            Status::Unknown => 1,
            Status::Fail => EXIT_FAILED,
        }
    }
}

// ---- measuring ------------------------------------------------------------------------------

/// How to count the rows and the nulls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counting {
    /// Every row, by the SQL engine; with no SQL engine, a single Parquet file's footer.
    Auto,
    /// A single Parquet file's footer, with no row read (`--fast`).
    Footer,
}

/// A table as [`measure`] found it, and what it could not find this time.
#[derive(Debug, Clone)]
pub struct Measurement {
    pub fingerprint: Fingerprint,
    /// The engine that counted the rows.
    pub engine: String,
    /// What went missing from the fingerprint, and why: a listing of the table's files that
    /// failed.
    pub notes: Vec<String>,
    /// Whether the table's files were listed, when its reader lists them. A fingerprint without
    /// them is no baseline: the files gate would compare nothing in every check after it.
    pub listed: bool,
}

/// Measure `source`: its columns as its reader reads them, its rows and each column's nulls —
/// every row counted by the SQL engine, or as a Parquet file's footer records them — and what this
/// machine can see of its files.
///
/// # Errors
///
/// No way to count every row (no SQL engine, and not a single Parquet file or a database table);
/// whatever reading the table fails with.
pub fn measure(
    ctx: &RequestContext,
    engines: &EngineRegistry,
    source: &Source,
    counting: Counting,
    now_ms: i64,
) -> Result<Measurement> {
    let reader = engines.resolve(Need::Read(source))?;
    let schema = reader.schema(ctx, source)?;
    // A database table is counted in its own database, in its own dialect: the SQL engine does not
    // query one. It has no files here, and no time it last changed.
    if source.format == Format::Database {
        if counting == Counting::Footer {
            return Err(no_count(source, counting));
        }
        let (rows, nulls) = database_counts(ctx, source)?;
        return Ok(Measurement {
            fingerprint: Fingerprint {
                version: FINGERPRINT_VERSION,
                source: schema.source,
                format: schema.format,
                taken_ms: now_ms,
                method: StatsMethod::Exact,
                rows,
                columns: schema.columns,
                nulls: nulls
                    .into_iter()
                    .map(|(name, n)| ColumnNulls {
                        name,
                        nulls: Some(n),
                    })
                    .collect(),
                files: None,
                bytes: None,
                updated_ms: None,
            },
            engine: reader.name().to_string(),
            notes: Vec::new(),
            listed: true,
        });
    }
    let named = [NamedSource {
        name: TABLE.to_string(),
        source: source.clone(),
    }];
    let sql = match counting {
        Counting::Auto => crate::diff::exact_engine(engines, &named),
        Counting::Footer => None,
    };
    let (method, engine, rows, nulls) = match sql {
        Some(engine) => {
            let columns = crate::diff::probe(ctx, engine.as_ref(), &named[0])?;
            let (rows, stats) =
                crate::diff::exact_stats(ctx, engine.as_ref(), &named[0], &columns)?;
            let nulls = stats
                .into_iter()
                .map(|(name, s)| ColumnNulls {
                    name,
                    nulls: s.nulls,
                })
                .collect();
            (StatsMethod::Exact, engine.name().to_string(), rows, nulls)
        }
        None => {
            if !has_footer(source) {
                return Err(no_count(source, counting));
            }
            // A scan limit of 0 is the footer: every row group's statistics, no row read.
            let profile = reader.profile(ctx, source, 0)?;
            let rows = profile
                .row_count
                .or(schema.row_count)
                .ok_or_else(|| no_count(source, counting))?;
            let nulls = profile
                .columns
                .into_iter()
                .map(|c| ColumnNulls {
                    nulls: c.null_count_known.then_some(c.null_count),
                    name: c.name,
                })
                .collect();
            (StatsMethod::Footer, profile.engine, rows, nulls)
        }
    };
    // What the reader sees of the table's files: the files it reads, as the call's identity. A
    // listing that fails costs the files and freshness gates, not the rows already counted: they
    // skip, or are unknown when asked for, and a note says why.
    let mut notes = Vec::new();
    let (seen, listed) = match reader.files(ctx, source) {
        Ok(seen) => (seen, true),
        Err(e) => {
            notes.push(format!("the table's files could not be listed: {e}"));
            (crate::engine::TableFiles::default(), false)
        }
    };
    Ok(Measurement {
        fingerprint: Fingerprint {
            version: FINGERPRINT_VERSION,
            source: schema.source,
            format: schema.format,
            taken_ms: now_ms,
            method,
            rows,
            columns: schema.columns,
            nulls,
            files: seen.files,
            bytes: seen.bytes,
            updated_ms: seen.updated_ms,
        },
        engine,
        notes,
        listed,
    })
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
fn database_counts(ctx: &RequestContext, source: &Source) -> Result<(u64, Vec<(String, u64)>)> {
    crate::engine::database::count_rows_and_nulls(ctx, source)
}

/// A build with no database engine never reaches here: reading the table's schema refused first.
#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
fn database_counts(_: &RequestContext, _: &Source) -> Result<(u64, Vec<(String, u64)>)> {
    Err(EngineError::missing_feature(
        "count a database table",
        "sqlite",
    ))
}

/// Whether `source` has a footer that counts it whole: a single Parquet file on this machine.
fn has_footer(source: &Source) -> bool {
    source.format == Format::Parquet
        && !source.is_unresolved()
        && !source.is_remote()
        && source.path.is_file()
}

/// The refusal when a table cannot be counted whole the way `counting` asks.
fn no_count(source: &Source, counting: Counting) -> EngineError {
    match counting {
        Counting::Footer => EngineError::Other(format!(
            "`--fast` counts from a single Parquet file's footer, and this is {}: drop `--fast` \
             to count every row",
            match source.format {
                Format::Parquet => "a Parquet directory or a file elsewhere".to_string(),
                Format::Database => "a database table".to_string(),
                format => format!("{format} data"),
            }
        )),
        Counting::Auto if cfg!(feature = "sql") => EngineError::Other(
            "`check` counts every row in the SQL engine, which cannot read this table here — \
             `--engine local` runs none: drop `--engine local`, or use `--fast` on a single \
             Parquet file"
                .to_string(),
        ),
        Counting::Auto => EngineError::UnsupportedOperation {
            engine: "sql".to_string(),
            op: "count every row for `check`".to_string(),
            hint: "this binary was built without the `sql` feature — rebuild with `cargo build \
                   --features sql` (the release binaries have it); a single Parquet \
                   file is counted from its footer without it"
                .to_string(),
        },
    }
}

/// Now, in milliseconds since the epoch.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

// ---- the baseline file ----------------------------------------------------------------------

/// The fingerprint a baseline file holds, or `None` when there is no file and `missing_ok` (a
/// check that will write one).
///
/// # Errors
///
/// No file, unless `missing_ok`; a file that does not read, or holds a fingerprint of another
/// version.
pub fn read_baseline(path: &Path, missing_ok: bool) -> Result<Option<Fingerprint>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && missing_ok => return Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(EngineError::Other(format!(
                "there is no baseline at {}: run the check once with `--update` to write it",
                crate::diff::visible(&path.display().to_string())
            )))
        }
        Err(e) => return Err(e.into()),
    };
    let bad = |why: String| {
        EngineError::Other(format!(
            "{} is not a baseline `check` wrote: {why}",
            crate::diff::visible(&path.display().to_string())
        ))
    };
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|e| bad(e.to_string()))?;
    match value.get("version").and_then(|v| v.as_u64()) {
        Some(v) if v == u64::from(FINGERPRINT_VERSION) => {}
        Some(v) => {
            return Err(bad(format!(
                "it is of version {v}, and this lakeleto reads version {FINGERPRINT_VERSION}: \
                 write it again with `--update` after removing it"
            )))
        }
        None => return Err(bad("it has no `version`".to_string())),
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| bad(e.to_string()))
}

/// Write `fingerprint` to `path` whole or not at all: into a file beside it, synced to disk, then
/// renamed over it, so a reader never finds half a baseline and a failed write leaves the old one.
/// The directories it is in are made when they are missing.
///
/// # Errors
///
/// Whatever writing or renaming fails with.
pub fn write_baseline(path: &Path, fingerprint: &Fingerprint) -> Result<()> {
    let mut text = serde_json::to_string_pretty(fingerprint)
        .map_err(|e| EngineError::Other(format!("writing the baseline: {e}")))?;
    text.push('\n');
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "baseline".to_string());
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    let written = (|| {
        use std::io::Write as _;
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(text.as_bytes())?;
        // On disk before it takes the baseline's name: a crash never leaves an empty baseline.
        file.sync_all()?;
        std::fs::rename(&temporary, path)
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&temporary);
        return Err(e.into());
    }
    Ok(())
}

// ---- the gates ------------------------------------------------------------------------------

/// Judge `measured` against `baseline` and `limits` at `now_ms`. `no_baseline` says why there is
/// no baseline, for the gates that need one.
pub fn judge(
    measured: &Fingerprint,
    baseline: Option<&Fingerprint>,
    limits: &Limits,
    now_ms: i64,
    no_baseline: &str,
) -> (Vec<Gate>, Option<SchemaDiff>) {
    let mut gates = Vec::with_capacity(5);
    let schema = match baseline {
        Some(base) => {
            let schema = compare_schemas(&base.columns, &measured.columns);
            gates.push(schema_gate(&schema));
            gates.push(row_gate(base.rows, measured.rows, limits.max_row_drop_pct));
            gates.push(null_gate(base, measured, limits.max_null_rise_points));
            gates.push(file_gate(base, measured, limits));
            Some(schema)
        }
        None => {
            for name in [
                GateName::Schema,
                GateName::RowCount,
                GateName::Nulls,
                GateName::Files,
            ] {
                gates.push(Gate {
                    name,
                    status: Status::Skipped,
                    reason: no_baseline.to_string(),
                    details: Vec::new(),
                    partial: false,
                });
            }
            None
        }
    };
    gates.push(freshness_gate(
        measured.updated_ms,
        limits.max_age_ms,
        now_ms,
    ));
    (gates, schema)
}

/// The worst of `gates`: [`Status::Pass`] when every one passed or was skipped.
pub fn overall(gates: &[Gate]) -> Status {
    gates
        .iter()
        .map(|g| g.status)
        .filter(|s| *s != Status::Skipped)
        .max()
        .unwrap_or(Status::Pass)
}

fn schema_gate(schema: &SchemaDiff) -> Gate {
    let mut details = Vec::new();
    for c in &schema.removed {
        details.push(format!("removed  {}  {}", c.name, c.data_type));
    }
    for c in &schema.changed {
        let mut what = Vec::new();
        if c.left_type != c.right_type {
            what.push(format!("{} → {}", c.left_type, c.right_type));
        }
        if c.left_nullable != c.right_nullable {
            what.push(format!(
                "{} → {}",
                nullable(c.left_nullable),
                nullable(c.right_nullable)
            ));
        }
        details.push(format!("changed  {}  {}", c.name, what.join(", ")));
    }
    let breaking = !schema.removed.is_empty() || !schema.changed.is_empty();
    for c in &schema.added {
        details.push(format!("added    {}  {}  (allowed)", c.name, c.data_type));
    }
    if schema.reordered {
        details.push("the columns are in a new order  (allowed)".to_string());
    }
    let reason = if breaking {
        format!(
            "{} removed, {} changed",
            columns(schema.removed.len()),
            columns(schema.changed.len())
        )
    } else if schema.added.is_empty() && !schema.reordered {
        "unchanged".to_string()
    } else {
        "no column removed or changed".to_string()
    };
    Gate {
        name: GateName::Schema,
        status: if breaking { Status::Fail } else { Status::Pass },
        reason,
        details,
        partial: false,
    }
}

fn nullable(n: bool) -> &'static str {
    if n {
        "nullable"
    } else {
        "not null"
    }
}

fn columns(n: usize) -> String {
    match n {
        1 => "1 column".to_string(),
        n => format!("{n} columns"),
    }
}

/// Whether going from `before` rows to `after` loses more than `pct` percent of them. Exactly
/// `pct` percent is allowed; a table that had no rows cannot lose any.
pub fn rows_dropped(before: u64, after: u64, pct: u32) -> bool {
    after < before && u128::from(before - after) * 100 > u128::from(before) * u128::from(pct)
}

fn row_gate(before: u64, after: u64, pct: u32) -> Gate {
    let failed = rows_dropped(before, after, pct);
    let reason = if after >= before {
        format!("{before} → {after} rows")
    } else {
        format!(
            "{before} → {after} rows: {} fewer ({}), {} the {pct}% allowed",
            before - after,
            hundredths_percent(u128::from(before - after), u128::from(before)),
            if failed { "more than" } else { "within" }
        )
    };
    Gate {
        name: GateName::RowCount,
        status: if failed { Status::Fail } else { Status::Pass },
        reason,
        details: Vec::new(),
        partial: false,
    }
}

/// `part` of `whole` as a percentage to two places: `12.34%`. `whole` is never zero here.
fn hundredths_percent(part: u128, whole: u128) -> String {
    let h = part * 10_000 / whole.max(1);
    format!("{}.{:02}%", h / 100, h % 100)
}

/// Whether a column's share of nulls rose by more than `points` percentage points, from
/// `before_nulls` of `before_rows` to `after_nulls` of `after_rows`. Exactly `points` is allowed.
/// `None` when there is no share to compare: no rows on a side.
pub fn nulls_rose(
    (before_nulls, before_rows): (u64, u64),
    (after_nulls, after_rows): (u64, u64),
    points: u32,
) -> Option<bool> {
    if before_rows == 0 || after_rows == 0 {
        return None;
    }
    // after/after_rows − before/before_rows > points/100, over the common denominator: exact.
    let exact = || {
        let rise = i128::from(after_nulls)
            .checked_mul(i128::from(before_rows))?
            .checked_sub(i128::from(before_nulls).checked_mul(i128::from(after_rows))?)?;
        let allowed = i128::from(points)
            .checked_mul(i128::from(before_rows))?
            .checked_mul(i128::from(after_rows))?;
        Some(rise.checked_mul(100)? > allowed)
    };
    // Past 2^63 rows a side the products outgrow 128 bits: the shares in parts per billion then.
    let per_billion = |nulls: u64, rows: u64| {
        i128::try_from(u128::from(nulls) * 1_000_000_000 / u128::from(rows)).unwrap_or(i128::MAX)
    };
    Some(exact().unwrap_or_else(|| {
        per_billion(after_nulls, after_rows) - per_billion(before_nulls, before_rows)
            > i128::from(points) * 10_000_000
    }))
}

fn null_gate(base: &Fingerprint, now: &Fingerprint, points: u32) -> Gate {
    let (mut failed, mut unknown) = (Vec::new(), Vec::new());
    for column in &now.nulls {
        let Some(before) = base.nulls.iter().find(|c| c.name == column.name) else {
            continue; // a column the baseline has not: nothing to compare
        };
        let (Some(b), Some(a)) = (before.nulls, column.nulls) else {
            let footer = [base.method, now.method].contains(&StatsMethod::Footer);
            unknown.push(format!(
                "{}: its nulls are not counted{}",
                column.name,
                if footer {
                    " — a Parquet footer did not record them; without `--fast` every row is \
                     counted"
                } else {
                    ""
                }
            ));
            continue;
        };
        if nulls_rose((b, base.rows), (a, now.rows), points) == Some(true) {
            failed.push(format!(
                "{}: {} → {} null",
                column.name,
                hundredths_percent(u128::from(b), u128::from(base.rows)),
                hundredths_percent(u128::from(a), u128::from(now.rows))
            ));
        }
    }
    let (status, reason) = if !failed.is_empty() {
        (
            Status::Fail,
            format!(
                "{} rose more than the {points} points allowed",
                columns(failed.len())
            ),
        )
    } else if !unknown.is_empty() {
        (
            Status::Unknown,
            format!("{} not counted", columns(unknown.len())),
        )
    } else {
        (
            Status::Pass,
            format!("no column's share of nulls rose more than {points} points"),
        )
    };
    failed.extend(unknown);
    Gate {
        name: GateName::Nulls,
        status,
        reason,
        details: failed,
        partial: false,
    }
}

/// Whether going from `before` to `after` grows or shrinks by more than `factor` hundredths (200
/// is 2×). Exactly the factor is allowed; growing from nothing is not a jump.
pub fn jumped(before: u64, after: u64, factor: u64) -> bool {
    let (b, a, f) = (u128::from(before), u128::from(after), u128::from(factor));
    before > 0 && (a * 100 > b * f || a * f < b * 100)
}

fn file_gate(base: &Fingerprint, now: &Fingerprint, limits: &Limits) -> Gate {
    let factor = factor_text(limits.max_file_jump);
    let (Some(before), Some(after)) = (base.files, now.files) else {
        let why = "this table's files are not counted here: the reader reads none for it (a \
                   database table), or cannot see them (another machine's)";
        return Gate {
            name: GateName::Files,
            status: if limits.files_asked {
                Status::Unknown
            } else {
                Status::Skipped
            },
            reason: why.to_string(),
            details: Vec::new(),
            partial: false,
        };
    };
    let mut details = vec![format!("files  {before} → {after}")];
    let mut failed = jumped(before, after, limits.max_file_jump);
    match (base.bytes, now.bytes) {
        (Some(b), Some(a)) => {
            details.push(format!("bytes  {b} → {a}"));
            failed |= jumped(b, a, limits.max_file_jump);
        }
        _ => details.push("bytes  not known on both sides: the files' count alone".to_string()),
    }
    Gate {
        name: GateName::Files,
        status: if failed { Status::Fail } else { Status::Pass },
        reason: if failed {
            format!("grew or shrank by more than the {factor} allowed")
        } else {
            format!("within {factor}")
        },
        details,
        partial: false,
    }
}

/// A factor in hundredths, as it was written: `2×`, `1.5×`, `1.25×`.
fn factor_text(hundredths: u64) -> String {
    match hundredths % 100 {
        0 => format!("{}×", hundredths / 100),
        r if r % 10 == 0 => format!("{}.{}×", hundredths / 100, r / 10),
        r => format!("{}.{r:02}×", hundredths / 100),
    }
}

fn freshness_gate(updated_ms: Option<i64>, max_age_ms: Option<u64>, now_ms: i64) -> Gate {
    let Some(max) = max_age_ms else {
        return Gate {
            name: GateName::Freshness,
            status: Status::Skipped,
            reason: "no `--max-age`".to_string(),
            details: Vec::new(),
            partial: false,
        };
    };
    let Some(updated) = updated_ms else {
        return Gate {
            name: GateName::Freshness,
            status: Status::Unknown,
            reason: "when this table last changed is not known here: an Iceberg table records \
                     it, a Delta table's log, and a file or an object its own"
                .to_string(),
            details: Vec::new(),
            partial: false,
        };
    };
    // A change in the future — another machine's clock — is no age at all.
    let age = u64::try_from(now_ms.saturating_sub(updated)).unwrap_or(0);
    let failed = age > max;
    Gate {
        name: GateName::Freshness,
        status: if failed { Status::Fail } else { Status::Pass },
        reason: format!(
            "last changed {} ago, {} the {} allowed",
            duration_text(age),
            if failed { "more than" } else { "within" },
            duration_text(max)
        ),
        details: Vec::new(),
        partial: false,
    }
}

/// A length of time, to the two largest units: `2d 3h`, `3h 12m`, `45m`, `30s`.
pub fn duration_text(ms: u64) -> String {
    let s = ms / 1000;
    let (d, h, m) = (s / 86_400, s / 3600 % 24, s / 60 % 60);
    match (d, h, m) {
        (0, 0, 0) => format!("{s}s"),
        (0, 0, m) => format!("{m}m"),
        (0, h, 0) => format!("{h}h"),
        (0, h, m) => format!("{h}h {m}m"),
        (d, 0, _) => format!("{d}d"),
        (d, h, _) => format!("{d}d {h}h"),
    }
}

/// Read `--max-file-jump`: a factor of 1 or more with up to two decimals, as hundredths (`2` is
/// 200, `1.5` is 150).
pub fn parse_factor(s: &str) -> std::result::Result<u64, String> {
    let bad = || format!("`{s}` is not a factor of 1 or more: `2`, `1.5` or `3.25`");
    let s = s.trim().trim_end_matches(['x', '×']);
    let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
    if whole.is_empty()
        || fraction.len() > 2
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(bad());
    }
    let whole: u64 = whole.parse().map_err(|_| bad())?;
    let fraction: u64 = format!("{fraction:0<2}").parse().map_err(|_| bad())?;
    let hundredths = whole
        .checked_mul(100)
        .and_then(|w| w.checked_add(fraction))
        .ok_or_else(bad)?;
    if hundredths < 100 {
        return Err(bad());
    }
    Ok(hundredths)
}

/// Read `--max-age`: a whole number of seconds, minutes, hours or days (`90s`, `30m`, `24h`, `7d`),
/// as milliseconds.
pub fn parse_age(s: &str) -> std::result::Result<u64, String> {
    let bad = || format!("`{s}` is not an age: a number and a unit, as `30m`, `24h` or `7d`");
    let s = s.trim();
    let digits = s.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
    let (number, unit) = s.split_at(digits);
    let number: u64 = number.parse().map_err(|_| bad())?;
    let unit_ms: u64 = match unit {
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err(bad()),
    };
    number.checked_mul(unit_ms).ok_or_else(bad)
}

// ---- a data contract ------------------------------------------------------------------------

/// A data contract to check every row against (`--contract`): an ODCS v3 document, or Covenant's
/// own, judged by Covenant's Arrow validator.
#[derive(Debug, Clone)]
pub struct ContractArgs {
    pub path: PathBuf,
    /// The model of the contract the table is; the contract's only one when `None`.
    pub model: Option<String>,
    /// Check the rules the validator can enforce when the contract has others too, and say the
    /// check was partial; without it such a contract is refused.
    pub allow_unenforced: bool,
    /// The most memory the contract's `unique` rules may hold the values they have seen in
    /// (`--unique-memory`): past it they stop being checked, and the gate says from which row.
    pub unique_memory: u64,
}

/// What `--unique-memory` is unless it is given: room for some five million short distinct values.
pub const DEFAULT_UNIQUE_MEMORY: u64 = 512 << 20;

/// Where a contract's `unique` rules stopped being checked: the values they hold reached the
/// memory allowed them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueStop {
    /// The first data row not checked for uniqueness, counted from 0 (the text report counts from
    /// 1). Duplicates before it were counted.
    pub from_row: u64,
    /// The memory allowed, in bytes.
    pub limit: u64,
    /// The `unique` fields left unchecked from there.
    pub fields: Vec<String>,
    /// Whether `--allow-unenforced` accepts the check as partial; without it the gate is unknown.
    pub accepted: bool,
}

/// How many rows broke one rule of a contract, and the first that did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleTally {
    /// The field the rule is on; empty for a rule on the whole row.
    pub field: String,
    pub rule: String,
    pub count: u64,
    /// The first data row that broke it, counted from 0 (the text report counts from 1); `None`
    /// for a rule on the table's columns themselves, such as a required column missing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_row: Option<u64>,
}

/// What a data contract found. Counts and where — never a value the table holds, which
/// Covenant's own messages carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractResult {
    /// The contract's id and version.
    pub contract: String,
    pub version: String,
    pub model: String,
    pub rows: u64,
    pub violations: u64,
    /// The violations the contract's policy allows (`max_violations`).
    pub allowed: u64,
    pub rules: Vec<RuleTally>,
    /// The contract's rules left unchecked under `--allow-unenforced`: where each is, and why.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unenforced: Vec<String>,
    /// Where the `unique` rules stopped, when the values they hold reached `--unique-memory`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_stopped: Option<UniqueStop>,
}

/// A contract read, compiled and resolved to a model, before any row is: a contract that does not
/// read, names no such model, or has rules this checker cannot enforce stops the check before it
/// spends a scan.
#[cfg(feature = "contracts")]
pub struct PreparedContract {
    compiled: covenant::prelude::CompiledContract,
    model: String,
    unenforced: Vec<String>,
    allow_unenforced: bool,
    unique_memory: u64,
}

/// Without the validator there is no contract to prepare: [`prepare_contract`] refuses.
#[cfg(not(feature = "contracts"))]
pub struct PreparedContract {
    _never: std::convert::Infallible,
}

impl PreparedContract {
    /// Every rule the contract's model holds a row to, by field, in the contract's order: what an
    /// OpenLineage event names as its assertions, as Covenant's own events do.
    #[cfg(feature = "contracts")]
    pub fn declared_rules(&self) -> Vec<(Option<String>, &'static str)> {
        self.compiled
            .resolve_model(Some(&self.model))
            .map(|model| {
                covenant::lineage::declared_rules(model)
                    .into_iter()
                    .map(|(field, rule)| (field, rule.name()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// See the `contracts` build's: never reached, since no contract is prepared without it.
    #[cfg(not(feature = "contracts"))]
    pub fn declared_rules(&self) -> Vec<(Option<String>, &'static str)> {
        match self._never {}
    }

    /// Whether the contract's policy only warns (`on_violation: warn`). The gate fails on a broken
    /// rule all the same; OpenLineage events give such a rule Covenant's severity, `warn`.
    #[cfg(feature = "contracts")]
    pub fn warn_only(&self) -> bool {
        self.compiled.policy.on_violation == covenant::spec::OnViolation::Warn
    }

    /// See the `contracts` build's.
    #[cfg(not(feature = "contracts"))]
    pub fn warn_only(&self) -> bool {
        match self._never {}
    }
}

/// Read and compile the contract `args` names, and resolve its model.
///
/// # Errors
///
/// A contract that does not read, compile, or name the model asked for; one with rules this
/// checker cannot enforce, without `--allow-unenforced`; a build without the `contracts` feature.
#[cfg(feature = "contracts")]
pub fn prepare_contract(args: &ContractArgs) -> Result<PreparedContract> {
    use covenant::prelude::{CompiledContract, Contract, CovenantError};
    let shown = crate::diff::visible(&args.path.display().to_string()).into_owned();
    // Covenant's errors name the contract's path themselves.
    let bad =
        |e: CovenantError| EngineError::Other(crate::diff::visible(&e.to_string()).into_owned());
    let loaded = Contract::load_path(&args.path).map_err(bad)?;
    let unenforced: Vec<String> = loaded
        .unenforced
        .iter()
        .map(|u| format!("{}: {} ({})", u.path, u.rule, u.reason))
        .collect();
    if !unenforced.is_empty() && !args.allow_unenforced {
        return Err(EngineError::Other(format!(
            "the contract {shown} has {} rule(s) this checker cannot enforce — {}: pass \
             `--allow-unenforced` to check the rest, and the check is reported as partial",
            unenforced.len(),
            crate::diff::visible(&unenforced.join("; "))
        )));
    }
    let compiled = CompiledContract::compile(&loaded.contract).map_err(bad)?;
    let model = compiled
        .resolve_model(args.model.as_deref())
        .map_err(bad)?
        .name
        .clone();
    Ok(PreparedContract {
        compiled,
        model,
        unenforced,
        allow_unenforced: args.allow_unenforced,
        unique_memory: args.unique_memory,
    })
}

/// See the `contracts` build's.
#[cfg(not(feature = "contracts"))]
pub fn prepare_contract(_: &ContractArgs) -> Result<PreparedContract> {
    Err(EngineError::missing_feature(
        "check a data contract",
        "contracts",
    ))
}

/// Check every row of `source` against `contract`, streamed through the SQL engine one batch at
/// a time. Memory holds a batch, and, for a field the contract says is `unique`, every distinct
/// value seen so far — up to the contract's `unique_memory`, by [`unique_cost`]'s estimate.
/// The batch that would pass it, and every one after, is checked for all but uniqueness, and the
/// result says from which row.
///
/// # Errors
///
/// A database table, whose rows the SQL engine does not read; no SQL engine to stream the rows;
/// whatever reading the table fails with.
#[cfg(feature = "contracts")]
pub fn run_contract(
    ctx: &RequestContext,
    engines: &EngineRegistry,
    source: &Source,
    contract: &PreparedContract,
) -> Result<ContractResult> {
    use covenant::prelude::{validate_source_batch, SchemaFindings, UniqueTracker};
    let compiled = &contract.compiled;
    let model = compiled
        .resolve_model(Some(&contract.model))
        .map_err(|e| EngineError::Other(e.to_string()))?;
    if source.format == Format::Database {
        return Err(database_contract());
    }
    let named = [NamedSource {
        name: TABLE.to_string(),
        source: source.clone(),
    }];
    let engine = crate::diff::exact_engine(engines, &named).ok_or_else(|| {
        EngineError::Other(
            "`--contract` reads every row through the SQL engine, which cannot read this table \
             here — `--engine local` runs none: drop `--engine local`"
                .to_string(),
        )
    })?;
    let stream = engine.query_stream(
        ctx,
        &format!("SELECT * FROM {}", crate::diff::quote(TABLE)),
        &named,
        None,
    )?;
    let schema = stream.schema().clone();
    // One sample per rule is all the report keeps: where it first broke. The counts are exact.
    let mut collector = covenant::prelude::Collector::new(1);
    let unique_fields: Vec<String> = model
        .fields
        .iter()
        .filter(|f| f.unique)
        .map(|f| f.name.clone())
        .collect();
    let mut unique = Some(UniqueTracker::new(model));
    let (mut held, mut unique_stopped) = (0u64, None);
    let mut findings = SchemaFindings::new();
    let (mut rows, mut batches) = (0u64, 0usize);
    for batch in stream {
        let batch = batch?;
        batches += 1;
        if unique.is_some() && !unique_fields.is_empty() {
            held = held.saturating_add(unique_cost(&batch, &unique_fields));
            if held > contract.unique_memory {
                // Dropped, what it held is freed; the rest is checked for all but uniqueness.
                unique = None;
                unique_stopped = Some(UniqueStop {
                    from_row: rows,
                    limit: contract.unique_memory,
                    fields: unique_fields.clone(),
                    accepted: contract.allow_unenforced,
                });
            }
        }
        rows += validate_source_batch(
            model,
            &batch,
            rows,
            unique.as_mut(),
            &mut findings,
            &mut collector,
        );
    }
    // A table of no rows still has columns: a required one missing is still a violation.
    if batches == 0 {
        validate_source_batch(
            model,
            &arrow_array::RecordBatch::new_empty(schema),
            0,
            unique.as_mut(),
            &mut findings,
            &mut collector,
        );
    }
    let report = collector.into_report(
        covenant::prelude::ReportHeader {
            contract_id: compiled.id.clone(),
            contract_version: compiled.version.clone(),
            owner: compiled.owner.clone(),
            model: model.name.clone(),
            source: TABLE.to_string(),
        },
        rows,
    );
    let rules = report
        .per_rule
        .iter()
        .map(|r| RuleTally {
            field: r.field.clone(),
            rule: r.rule.to_string(),
            count: r.count,
            first_row: report
                .samples
                .iter()
                .find(|v| {
                    v.field.as_deref().unwrap_or_default() == r.field && v.rule.name() == r.rule
                })
                .and_then(|v| v.row),
        })
        .collect();
    Ok(ContractResult {
        contract: compiled.id.clone(),
        version: compiled.version.clone(),
        model: model.name.clone(),
        rows,
        violations: report.violations,
        allowed: compiled.policy.max_violations,
        rules,
        unenforced: contract.unenforced.clone(),
        unique_stopped,
    })
}

/// What holding `batch`'s values of the `unique` fields costs a uniqueness check, estimated on
/// the high side: each value's text, and what a set of strings spends to hold one more. Every
/// value is charged, repeats included — the tracker holds a repeat once — so a column with many
/// repeats reaches the budget before memory does; its repeats are violations already. A value
/// that is not text is taken at the longest a number, a date or a time renders to, and a 16-byte
/// UUID at its 36 characters; a column the validator does not hold (a dictionary, other binary:
/// a type mismatch to Covenant) is charged as numbers are, which overcounts it. A field the batch
/// has no column for costs nothing, as the validator checks none of it.
///
/// An estimate, since the validator's tracker (Covenant 0.2.1) does not say what it holds: the
/// next Covenant's own budget replaces it.
#[cfg(feature = "contracts")]
fn unique_cost(batch: &arrow_array::RecordBatch, fields: &[String]) -> u64 {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use arrow_schema::DataType;
    // A `String` in a hash set's slot, the slots a set keeps spare and copies as it grows, and
    // the allocator's rounding of the text behind it: 2 million 36-character ids measured
    // 240 MiB at their peak, which this puts at 259. Keep in step with Covenant's own
    // `PER_VALUE` (module_75 `src/engine/mod.rs`), which counts the same way, until Lakeleto
    // reads Covenant's budget instead of estimating it.
    const PER_VALUE: u64 = 100;
    const NOT_TEXT: u64 = 32;
    fields
        .iter()
        .filter_map(|name| batch.column_by_name(name))
        .map(|column| {
            let values = (column.len() - column.null_count()) as u64;
            let text = match column.data_type() {
                DataType::Utf8 => {
                    let offsets = column.as_string::<i32>().value_offsets();
                    (offsets[offsets.len() - 1] - offsets[0]) as u64
                }
                DataType::LargeUtf8 => {
                    let offsets = column.as_string::<i64>().value_offsets();
                    (offsets[offsets.len() - 1] - offsets[0]) as u64
                }
                DataType::Utf8View => column
                    .as_string_view()
                    .iter()
                    .flatten()
                    .map(|v| v.len() as u64)
                    .sum(),
                // Covenant holds a 16-byte UUID as its text: 36 characters.
                DataType::FixedSizeBinary(16) => values.saturating_mul(36),
                _ => values.saturating_mul(NOT_TEXT),
            };
            values.saturating_mul(PER_VALUE).saturating_add(text)
        })
        .fold(0u64, u64::saturating_add)
}

/// See the `contracts` build's: never reached, since no contract is prepared without it.
#[cfg(not(feature = "contracts"))]
pub fn run_contract(
    _: &RequestContext,
    _: &EngineRegistry,
    _: &Source,
    contract: &PreparedContract,
) -> Result<ContractResult> {
    match contract._never {}
}

/// The refusal of a contract on a database table.
pub fn database_contract() -> EngineError {
    EngineError::Other(
        "`--contract` streams every row through the SQL engine, which does not query a \
         database's tables: check a database table's contract with Covenant against an export of \
         it for now"
            .to_string(),
    )
}

/// The contract gate: it fails when the violations are more than the contract allows. A pass
/// with rules left unchecked says it is partial, in every form of the report.
pub fn contract_gate(result: &ContractResult) -> Gate {
    let failed = result.violations > result.allowed;
    let stopped = result.unique_stopped.as_ref();
    let mut reason = if result.violations == 0 {
        format!("no violation in {} rows", result.rows)
    } else {
        format!(
            "{} violation{} in {} rows, {} allowed",
            result.violations,
            if result.violations == 1 { "" } else { "s" },
            result.rows,
            result.allowed
        )
    };
    if !result.unenforced.is_empty() {
        reason.push_str(&format!(
            " — partial: {} contract rule{} not enforced",
            result.unenforced.len(),
            if result.unenforced.len() == 1 {
                ""
            } else {
                "s"
            }
        ));
    }
    if let Some(stop) = stopped {
        reason.push_str(&format!(
            " — `unique` not checked from data row {}: the values it holds reached the {} \
             `--unique-memory` allows{}",
            stop.from_row + 1,
            bytes_text(stop.limit),
            if stop.accepted {
                ""
            } else {
                "; raise it, or pass `--allow-unenforced` to accept a partial check"
            }
        ));
    }
    let partial = !result.unenforced.is_empty() || stopped.is_some();
    let mut details: Vec<String> = result
        .rules
        .iter()
        .map(|r| {
            let field = if r.field.is_empty() {
                "(row)"
            } else {
                &r.field
            };
            // `first_row` counts data rows from 0, as Covenant does; a person counts them from 1.
            match r.first_row {
                Some(row) => format!(
                    "{field}  {}: {}, first at data row {}",
                    r.rule,
                    r.count,
                    row + 1
                ),
                None => format!("{field}  {}: {}", r.rule, r.count),
            }
        })
        .collect();
    details.extend(
        result
            .unenforced
            .iter()
            .map(|u| format!("not enforced  {u}")),
    );
    if let Some(stop) = stopped {
        details.push(format!(
            "not checked from data row {}  unique: {}",
            stop.from_row + 1,
            stop.fields.join(", ")
        ));
    }
    Gate {
        name: GateName::Contract,
        // Duplicates found before the stop still fail it; with none, a check that could not
        // finish is not a pass unless it was allowed to be partial.
        status: if failed {
            Status::Fail
        } else if stopped.is_some_and(|s| !s.accepted) {
            Status::Unknown
        } else {
            Status::Pass
        },
        reason,
        details,
        partial,
    }
}

// ---- rendering ------------------------------------------------------------------------------

fn bytes_text(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut unit = 0;
    let mut tenths = u128::from(n) * 10;
    while tenths >= 10_240 && unit < UNITS.len() - 1 {
        tenths /= 1024;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{}.{} {}", tenths / 10, tenths % 10, UNITS[unit])
    }
}

fn shape(f: &Fingerprint, now_ms: i64) -> String {
    let mut parts = vec![format!("{} rows", f.rows)];
    match (f.files, f.bytes) {
        (Some(files), Some(bytes)) => parts.push(format!(
            "{files} file{}, {}",
            if files == 1 { "" } else { "s" },
            bytes_text(bytes)
        )),
        (Some(files), None) => {
            parts.push(format!("{files} file{}", if files == 1 { "" } else { "s" }))
        }
        _ => {}
    }
    if let Some(updated) = f.updated_ms {
        parts.push(format!(
            "changed {} ago",
            duration_text(u64::try_from(now_ms.saturating_sub(updated)).unwrap_or(0))
        ));
    }
    parts.join(" · ")
}

/// The report for a person at a terminal. Whatever the table supplied — names, types, paths — is
/// written through [`crate::diff::visible`], so it cannot drive the terminal.
pub fn render_text(report: &CheckReport, now_ms: i64) -> String {
    use crate::diff::visible;
    use std::fmt::Write as _;
    let m = &report.measured;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "check     {}  ({}, {} {})",
        visible(&m.source),
        visible(&m.format),
        match m.method {
            StatsMethod::Footer => "counted from its footer by",
            _ => "rows counted by",
        },
        visible(&report.engine)
    );
    let _ = writeln!(out, "now       {}", shape(m, now_ms));
    match (&report.baseline_path, &report.baseline) {
        (Some(path), Some(base)) => {
            let _ = writeln!(
                out,
                "baseline  {}  ({}, taken {} ago)",
                visible(path),
                shape(base, now_ms),
                duration_text(u64::try_from(now_ms.saturating_sub(base.taken_ms)).unwrap_or(0))
            );
        }
        (Some(path), None) => {
            let _ = writeln!(out, "baseline  {}  (none yet)", visible(path));
        }
        _ => {}
    }
    if let Some(c) = &report.contract {
        let _ = writeln!(
            out,
            "contract  {} {}  (model {})",
            visible(&c.contract),
            visible(&c.version),
            visible(&c.model)
        );
    }
    out.push('\n');
    for gate in &report.gates {
        let _ = writeln!(
            out,
            "  {:<8} {:<10} {}",
            gate.status.name().to_ascii_uppercase(),
            gate.name.name(),
            visible(&gate.reason)
        );
        for detail in &gate.details {
            let _ = writeln!(out, "  {:<8} {:<10}   {}", "", "", visible(detail));
        }
    }
    out.push('\n');
    let _ = writeln!(
        out,
        "result    {}",
        match report.status {
            Status::Pass | Status::Skipped if report.gates.iter().any(|g| g.partial) => {
                "PASS, partially: a contract's rules were left unchecked"
            }
            Status::Pass | Status::Skipped => "PASS",
            Status::Fail => "FAIL",
            Status::Unknown => "UNKNOWN: a gate asked for could not be told",
        }
    );
    if report.baseline_written {
        if let Some(path) = &report.baseline_path {
            let _ = writeln!(out, "baseline  written to {}", visible(path));
        }
    }
    for note in &report.notes {
        let _ = writeln!(out, "note      {}", visible(note));
    }
    out
}

/// Text for an XML attribute or element: [`crate::diff::visible`] first, so no character XML 1.0
/// forbids (`ESC`, `NUL` and the rest of C0 but tab and line ends, which it escapes too) reaches a
/// CI's parser, then the five XML escapes. The two noncharacters XML also forbids are escaped as
/// `visible` writes the rest.
fn xml(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in crate::diff::visible(text).chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\u{FFFE}' | '\u{FFFF}' => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// The report as JUnit XML, the form CI systems show as tests: one test case a gate, a failed gate
/// a `<failure>`, an unknown one an `<error>`, a skipped one `<skipped>`.
pub fn render_junit(report: &CheckReport) -> String {
    use std::fmt::Write as _;
    let count = |s: Status| report.gates.iter().filter(|g| g.status == s).count();
    let (tests, failures, errors, skipped) = (
        report.gates.len(),
        count(Status::Fail),
        count(Status::Unknown),
        count(Status::Skipped),
    );
    let suite = format!("lakeleto check {}", report.measured.source);
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        out,
        "<testsuites name=\"lakeleto check\" tests=\"{tests}\" failures=\"{failures}\" \
         errors=\"{errors}\" skipped=\"{skipped}\">"
    );
    let _ = writeln!(
        out,
        "  <testsuite name=\"{}\" tests=\"{tests}\" failures=\"{failures}\" errors=\"{errors}\" \
         skipped=\"{skipped}\">",
        xml(&suite)
    );
    for gate in &report.gates {
        let _ = write!(
            out,
            "    <testcase classname=\"lakeleto.check\" name=\"{}\"",
            gate.name.name()
        );
        let body = std::iter::once(gate.reason.as_str())
            .chain(gate.details.iter().map(String::as_str))
            .map(xml)
            .collect::<Vec<_>>()
            .join("\n");
        let element = match gate.status {
            // A partial pass says what it left unchecked, where a CI shows a test's output.
            Status::Pass if gate.partial => {
                let _ = writeln!(
                    out,
                    ">\n      <system-out>{body}</system-out>\n    </testcase>"
                );
                continue;
            }
            Status::Pass => {
                out.push_str("/>\n");
                continue;
            }
            Status::Fail => "failure",
            Status::Unknown => "error",
            Status::Skipped => "skipped",
        };
        let _ = writeln!(
            out,
            ">\n      <{element} message=\"{}\">{body}</{element}>\n    </testcase>",
            xml(&gate.reason)
        );
    }
    out.push_str("  </testsuite>\n</testsuites>\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_may_drop_by_the_share_allowed_and_no_more() {
        assert!(!rows_dropped(100, 90, 10), "exactly 10% is allowed");
        assert!(rows_dropped(100, 89, 10));
        assert!(!rows_dropped(100, 100, 10));
        assert!(!rows_dropped(100, 150, 10), "growing is no drop");
        assert!(!rows_dropped(0, 0, 10), "no rows cannot lose any");
        assert!(rows_dropped(1, 0, 10));
        assert!(!rows_dropped(1, 0, 100), "100% allows every row to go");
        assert!(rows_dropped(u64::MAX, 0, 99), "no overflow at the top");
    }

    #[test]
    fn a_share_of_nulls_may_rise_by_the_points_allowed_and_no_more() {
        assert_eq!(
            nulls_rose((0, 100), (5, 100), 5),
            Some(false),
            "exactly 5 points"
        );
        assert_eq!(nulls_rose((0, 100), (6, 100), 5), Some(true));
        assert_eq!(
            nulls_rose((1, 1000), (400, 1000), 5),
            Some(true),
            "0.1% → 40%"
        );
        assert_eq!(
            nulls_rose((50, 100), (10, 100), 5),
            Some(false),
            "falling is fine"
        );
        // The share, not the count: twice the rows with twice the nulls is the same share.
        assert_eq!(nulls_rose((10, 100), (20, 200), 0), Some(false));
        assert_eq!(
            nulls_rose((0, 0), (5, 10), 5),
            None,
            "no rows before: no share"
        );
        assert_eq!(
            nulls_rose((0, 10), (0, 0), 5),
            None,
            "no rows now: no share"
        );
        assert_eq!(
            nulls_rose((0, u64::MAX), (u64::MAX, u64::MAX), 99),
            Some(true)
        );
    }

    #[test]
    fn files_may_grow_or_shrink_by_the_factor_allowed_and_no_more() {
        assert!(!jumped(10, 20, 200), "exactly 2× is allowed");
        assert!(jumped(10, 21, 200));
        assert!(!jumped(10, 5, 200), "exactly half is allowed");
        assert!(jumped(10, 4, 200));
        assert!(!jumped(0, 1000, 200), "growing from nothing is no jump");
        assert!(jumped(4, 7, 150));
        assert!(!jumped(4, 6, 150));
        assert!(jumped(u64::MAX, 0, 200), "no overflow at the top");
    }

    #[test]
    fn factors_and_ages_read_as_written() {
        assert_eq!(parse_factor("2"), Ok(200));
        assert_eq!(parse_factor("1.5"), Ok(150));
        assert_eq!(parse_factor("3.25"), Ok(325));
        assert_eq!(parse_factor("2x"), Ok(200));
        assert!(
            parse_factor("0.5").is_err(),
            "a factor below 1 would fail any table"
        );
        assert!(parse_factor("1.234").is_err());
        assert!(parse_factor("-2").is_err());
        assert!(parse_factor("").is_err());
        assert_eq!(factor_text(200), "2×");
        assert_eq!(factor_text(150), "1.5×");
        assert_eq!(factor_text(325), "3.25×");
        assert_eq!(parse_age("90s"), Ok(90_000));
        assert_eq!(parse_age("30m"), Ok(1_800_000));
        assert_eq!(parse_age("24h"), Ok(86_400_000));
        assert_eq!(parse_age("7d"), Ok(604_800_000));
        assert!(parse_age("24").is_err(), "a unit is needed");
        assert!(parse_age("1w").is_err());
        assert!(parse_age("h").is_err());
        assert_eq!(duration_text(3 * 3_600_000 + 12 * 60_000), "3h 12m");
        assert_eq!(duration_text(2 * 86_400_000 + 3 * 3_600_000), "2d 3h");
        assert_eq!(duration_text(45_000), "45s");
    }

    #[test]
    fn junit_carries_nothing_a_ci_parser_refuses() {
        assert_eq!(xml("a<b>&\"c'"), "a&lt;b&gt;&amp;&quot;c&apos;");
        assert_eq!(xml("\u{1b}[2J\u{0}"), "\\x1b[2J\\x00");
        assert_eq!(xml("x\u{FFFE}"), "x\\u{fffe}");
        assert_eq!(xml("tab\there"), "tab\\there");
    }

    fn fingerprint(files: Option<u64>, updated_ms: Option<i64>) -> Fingerprint {
        Fingerprint {
            version: FINGERPRINT_VERSION,
            source: "t".to_string(),
            format: "csv".to_string(),
            taken_ms: 0,
            method: StatsMethod::Exact,
            rows: 10,
            columns: Vec::new(),
            nulls: vec![ColumnNulls {
                name: "v".to_string(),
                nulls: None,
            }],
            files,
            bytes: None,
            updated_ms,
        }
    }

    /// A gate that cannot be told is skipped when nobody asked for it, and unknown when somebody
    /// did — never passed.
    #[test]
    fn what_cannot_be_told_is_skipped_unless_asked_for_and_never_passes() {
        let uncounted = fingerprint(None, None);
        let limits = Limits::default();
        assert_eq!(
            file_gate(&uncounted, &uncounted, &limits).status,
            Status::Skipped
        );
        let asked = Limits {
            files_asked: true,
            ..limits
        };
        assert_eq!(
            file_gate(&uncounted, &uncounted, &asked).status,
            Status::Unknown
        );
        // Counted files with no bytes on a side: the count alone.
        let counted = fingerprint(Some(3), None);
        let gate = file_gate(&counted, &counted, &limits);
        assert_eq!(gate.status, Status::Pass);
        assert!(
            gate.details[1].contains("the files' count alone"),
            "{gate:?}"
        );

        assert_eq!(freshness_gate(None, None, 0).status, Status::Skipped);
        assert_eq!(freshness_gate(None, Some(1), 0).status, Status::Unknown);
        assert_eq!(
            freshness_gate(Some(0), Some(1000), 1000).status,
            Status::Pass
        );
        assert_eq!(
            freshness_gate(Some(0), Some(1000), 1001).status,
            Status::Fail
        );
        assert_eq!(
            freshness_gate(Some(5000), Some(1000), 0).status,
            Status::Pass,
            "a change in the future is no age"
        );

        // Nulls not counted on a side: unknown, for that column.
        let gate = null_gate(&uncounted, &uncounted, 5);
        assert_eq!(gate.status, Status::Unknown);
        assert_eq!(gate.details, vec!["v: its nulls are not counted"]);

        // And with no baseline at all, the gates that need one are skipped, saying why.
        let (gates, schema) = judge(&uncounted, None, &limits, 0, "no `--baseline`");
        assert!(schema.is_none());
        assert_eq!(gates.len(), 5);
        assert!(gates[..4]
            .iter()
            .all(|g| g.status == Status::Skipped && g.reason == "no `--baseline`"));
        let report = CheckReport {
            version: REPORT_VERSION,
            engine: "sql".to_string(),
            measured: uncounted.clone(),
            baseline_path: None,
            baseline: None,
            schema: None,
            status: overall(&gates),
            gates,
            baseline_written: false,
            contract: None,
            notes: Vec::new(),
        };
        assert_eq!(report.exit_code(), 0);
        // A gate asked for that cannot be told errs; one that failed fails.
        let unknown = CheckReport {
            status: Status::Unknown,
            ..report.clone()
        };
        assert_eq!(unknown.exit_code(), 1);
        let failed = CheckReport {
            status: Status::Fail,
            ..report
        };
        assert_eq!(failed.exit_code(), EXIT_FAILED);
    }

    #[test]
    fn the_worst_gate_is_the_check() {
        let gate = |status| Gate {
            name: GateName::Schema,
            status,
            reason: String::new(),
            details: Vec::new(),
            partial: false,
        };
        assert_eq!(overall(&[]), Status::Pass);
        assert_eq!(overall(&[gate(Status::Skipped)]), Status::Pass);
        assert_eq!(
            overall(&[gate(Status::Pass), gate(Status::Unknown)]),
            Status::Unknown
        );
        assert_eq!(
            overall(&[gate(Status::Unknown), gate(Status::Fail)]),
            Status::Fail
        );
    }
}
