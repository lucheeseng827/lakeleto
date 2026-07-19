//! The `sql` engine: DataFusion behind the same [`Engine`] trait.
//!
//! Feature-gated (`--features sql`) because DataFusion is a heavy compile; the default
//! build stays lean. When present it gives Lakeleto a real SQL planner over Parquet/CSV —
//! `lakeleto query "SELECT ..."` — while `schema`/`head`/`profile` are expressed as SQL and
//! funnel back through the *same* [`profile_columns`](super::profile_columns) helper the
//! local engine uses, so stats never diverge between engines.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;

use super::{
    build_table_schema, profile_columns, truncate_batches, Capabilities, Engine, FilterOp,
    FilterSpec, NamedSource, RowBatch, ScanResult, ScanSpec, TableProfile, TableSchema,
};
use crate::error::{EngineError, Result};
use crate::source::{Format, Source};

/// Normalize a local filesystem path into a form DataFusion's `ListingTableUrl` can parse.
///
/// On Windows, canonicalized paths (e.g. from `--root` confinement or `fs::canonicalize`) carry the
/// extended-length verbatim prefix `\\?\` (or `\\?\UNC\` for shares). DataFusion round-trips the path
/// through `Url::from_file_path`/`to_file_path`, which rejects that prefix — surfacing as the panic
/// `to_file_path() failed to produce an absolute Path`. Strip the prefix so the plain drive path is
/// used. On non-Windows this is a no-op (no such prefix ever appears).
fn datafusion_path(path: &str) -> String {
    if let Some(rest) = path.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{rest}")
    } else if let Some(rest) = path.strip_prefix(r"\\?\") {
        rest.to_string()
    } else {
        path.to_string()
    }
}

/// DataFusion-backed engine. Owns its own single-worker multi-threaded Tokio runtime so callers
/// use the same synchronous [`Engine`] API as the local engine (no `async` leaks across the seam).
pub struct DataFusionEngine {
    rt: tokio::runtime::Runtime,
}

impl DataFusionEngine {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build tokio runtime for DataFusion engine");
        Self { rt }
    }

    fn register(&self, ctx: &SessionContext, table: &NamedSource) -> Result<()> {
        let path = datafusion_path(&table.source.path.to_string_lossy());
        self.rt.block_on(async {
            match table.source.format {
                Format::Parquet => ctx
                    .register_parquet(&table.name, &path, ParquetReadOptions::default())
                    .await
                    .map_err(|e| EngineError::Query(e.to_string())),
                Format::Csv | Format::Tsv => ctx
                    .register_csv(
                        &table.name,
                        &path,
                        CsvReadOptions::default().delimiter(table.source.format.delimiter()),
                    )
                    .await
                    .map_err(|e| EngineError::Query(e.to_string())),
                other => Err(EngineError::unsupported_format(other, "sql")),
            }
        })
    }

    /// Register a single source as table `t` for the schema/head/profile helpers.
    fn ctx_for(&self, source: &Source) -> Result<SessionContext> {
        let ctx = SessionContext::new();
        self.register(
            &ctx,
            &NamedSource {
                name: "t".to_string(),
                source: source.clone(),
            },
        )?;
        Ok(ctx)
    }

    fn collect_sql(&self, ctx: &SessionContext, sql: &str) -> Result<RowBatch> {
        self.rt.block_on(async {
            let df = ctx
                .sql(sql)
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let batches = df
                .collect()
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok(RowBatch { schema, batches })
        })
    }
}

impl Default for DataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine for DataFusionEngine {
    fn name(&self) -> &str {
        "sql"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            engine: "sql (DataFusion)".to_string(),
            formats: vec!["parquet".to_string(), "csv".to_string()],
            sql: true,
            profile: true,
            remote: false,
        }
    }

    fn schema(&self, source: &Source) -> Result<TableSchema> {
        let ctx = self.ctx_for(source)?;
        let arrow_schema = self.rt.block_on(async {
            let df = ctx
                .table("t")
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok::<SchemaRef, EngineError>(Arc::new(df.schema().as_arrow().clone()))
        })?;
        Ok(build_table_schema(source, self.name(), None, &arrow_schema))
    }

    fn preview(&self, source: &Source, limit: usize) -> Result<RowBatch> {
        let ctx = self.ctx_for(source)?;
        let rb = self.collect_sql(&ctx, &format!("SELECT * FROM t LIMIT {limit}"))?;
        Ok(RowBatch {
            schema: rb.schema,
            batches: truncate_batches(rb.batches, limit),
        })
    }

    fn profile(&self, source: &Source, scan_limit: usize) -> Result<TableProfile> {
        if scan_limit == 0 {
            // scan==0 is the "footer-stats, no scan" fast path — the renderer treats
            // `scanned_rows == 0` as exact footer-derived stats. The SQL engine has no footer
            // path, so `LIMIT 0` would render an empty scan as if it were an exact profile.
            return Err(EngineError::Query(
                "`--fast` footer-only profiling is not supported by the sql engine; drop \
                 `--fast` (or use the default local engine) to profile via a scan"
                    .to_string(),
            ));
        }
        let ctx = self.ctx_for(source)?;
        let rb = self.collect_sql(&ctx, &format!("SELECT * FROM t LIMIT {scan_limit}"))?;
        let scanned_rows = rb.num_rows() as u64;
        let columns = profile_columns(&rb.schema, &rb.batches);
        Ok(TableProfile {
            source: source.display(),
            engine: self.name().to_string(),
            row_count: None,
            scanned_rows,
            columns,
        })
    }

    fn query(&self, sql: &str, tables: &[NamedSource]) -> Result<RowBatch> {
        // Lakeleto is an *explorer*: user SQL must never mutate. Reject anything that isn't a
        // read query (guard ported from module_62/src/sql.rs).
        ensure_read_only(sql)?;
        let ctx = SessionContext::new();
        for t in tables {
            self.register(&ctx, t)?;
        }
        self.collect_sql(&ctx, sql)
    }

    /// Bounded query with the cap pushed **into the plan** (`DataFrame::limit`), so a
    /// `SELECT *` over a huge table materializes at most `cap` rows instead of buffering the
    /// full result and trimming afterwards.
    fn query_capped(&self, sql: &str, tables: &[NamedSource], cap: usize) -> Result<RowBatch> {
        ensure_read_only(sql)?;
        let ctx = SessionContext::new();
        for t in tables {
            self.register(&ctx, t)?;
        }
        self.rt.block_on(async {
            let df = ctx
                .sql(sql)
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?
                .limit(0, Some(cap))
                .map_err(|e| EngineError::Query(e.to_string()))?;
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let batches = df
                .collect()
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok(RowBatch { schema, batches })
        })
    }

    /// Grid scan with the filter/sort/window/projection **pushed into DataFusion** — WHERE,
    /// ORDER BY (DataFusion's external, spilling sort), LIMIT/OFFSET, and a `count(*)` for the
    /// exact match total. Unlike the local engine this is *not* bounded by a working set, so
    /// sort/filter over files larger than `scan_cap` is correct and complete.
    fn scan(&self, source: &Source, spec: &ScanSpec) -> Result<ScanResult> {
        let ctx = self.ctx_for(source)?;
        let where_sql = build_where(&spec.filters);

        let count_rb =
            self.collect_sql(&ctx, &format!("SELECT count(*) AS c FROM t{where_sql}"))?;
        let matched = count_value(&count_rb);

        let proj = match &spec.projection {
            Some(cols) if !cols.is_empty() => cols
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", "),
            _ => "*".to_string(),
        };
        let order = match &spec.sort {
            Some(s) => format!(
                " ORDER BY {} {}",
                quote_ident(&s.column),
                if s.descending { "DESC" } else { "ASC" }
            ),
            None => String::new(),
        };
        let sql = format!(
            "SELECT {proj} FROM t{where_sql}{order} LIMIT {} OFFSET {}",
            spec.limit, spec.offset
        );
        let batch = self.collect_sql(&ctx, &sql)?;
        Ok(ScanResult {
            batch,
            matched_rows: matched,
            total_known: true,
            scanned_rows: matched,
            bounded: false,
            offset: spec.offset,
        })
    }
}

/// `"ident"` with embedded quotes doubled — safe SQL identifier quoting.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// `'value'` with embedded quotes doubled — a SQL string literal. DataFusion coerces it to the
/// column's type for comparisons (so `"score" > '90'` works on a numeric column).
fn sql_str(v: &str) -> String {
    format!("'{}'", v.replace('\'', "''"))
}

fn build_where(filters: &[FilterSpec]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| {
            let id = quote_ident(&f.column);
            match f.op {
                FilterOp::Contains => format!("{id} LIKE {}", sql_str(&format!("%{}%", f.value))),
                FilterOp::Eq => format!("{id} = {}", sql_str(&f.value)),
                FilterOp::Ne => format!("{id} <> {}", sql_str(&f.value)),
                FilterOp::Lt => format!("{id} < {}", sql_str(&f.value)),
                FilterOp::Le => format!("{id} <= {}", sql_str(&f.value)),
                FilterOp::Gt => format!("{id} > {}", sql_str(&f.value)),
                FilterOp::Ge => format!("{id} >= {}", sql_str(&f.value)),
            }
        })
        .collect();
    format!(" WHERE {}", parts.join(" AND "))
}

fn count_value(rb: &RowBatch) -> usize {
    rb.batches
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
        })
        .map(|a| {
            if a.is_empty() {
                0
            } else {
                a.value(0).max(0) as usize
            }
        })
        .unwrap_or(0)
}

/// Reject any SQL that is not a single read-only query. `COPY TO`, `CREATE`, any DML/DDL,
/// `EXPLAIN ANALYZE` (which executes), and multi-statement smuggling are all denied.
pub fn ensure_read_only(sql: &str) -> Result<()> {
    let stmts = DFParser::parse_sql(sql)
        .map_err(|e| EngineError::Query(format!("could not parse SQL: {e}")))?;
    if stmts.len() != 1 {
        return Err(EngineError::Query(format!(
            "exactly one SQL statement is allowed (got {})",
            stmts.len()
        )));
    }
    if !statement_is_read_only(&stmts[0]) {
        return Err(EngineError::Query(
            "only read queries are allowed — SELECT / WITH, or EXPLAIN (without ANALYZE)"
                .to_string(),
        ));
    }
    Ok(())
}

fn statement_is_read_only(stmt: &DfStatement) -> bool {
    match stmt {
        DfStatement::Statement(s) => matches!(s.as_ref(), SqlStatement::Query(_)),
        DfStatement::Explain(e) => !e.analyze && statement_is_read_only(&e.statement),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{datafusion_path, ensure_read_only, DataFusionEngine};
    use crate::engine::Engine;
    use crate::error::EngineError;
    use crate::source::{Format, Source};

    #[test]
    fn strips_windows_verbatim_prefix() {
        // A canonicalized Windows path (from `--root` or fs::canonicalize) reaching the SQL engine
        // must lose its `\\?\` prefix, else DataFusion's ListingTableUrl panics with
        // "to_file_path() failed to produce an absolute Path".
        assert_eq!(
            datafusion_path(r"\\?\C:\data\orders.parquet"),
            r"C:\data\orders.parquet"
        );
        assert_eq!(
            datafusion_path(r"\\?\UNC\server\share\t.csv"),
            r"\\server\share\t.csv"
        );
        // Plain paths (the non-Windows case, and already-clean Windows paths) pass through.
        assert_eq!(datafusion_path("/home/u/t.parquet"), "/home/u/t.parquet");
        assert_eq!(datafusion_path(r"C:\data\t.csv"), r"C:\data\t.csv");
    }

    #[test]
    fn profile_rejects_scan_zero() {
        // scan==0 is the footer-stats fast path (renderer treats scanned_rows==0 as exact); the
        // SQL engine has no footer path, so it must reject rather than return an empty LIMIT 0.
        let csv = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/people.csv");
        let source = Source::with_format(csv, Format::Csv);
        let err = DataFusionEngine::new().profile(&source, 0).unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "got: {err}");
        // A positive scan limit still profiles fine.
        assert!(DataFusionEngine::new().profile(&source, 100).is_ok());
    }

    #[test]
    fn allows_read_queries() {
        assert!(ensure_read_only("SELECT 1").is_ok());
        assert!(ensure_read_only("WITH a AS (SELECT 1) SELECT * FROM a").is_ok());
        assert!(ensure_read_only("EXPLAIN SELECT 1").is_ok());
    }

    #[test]
    fn rejects_writes_and_smuggling() {
        for bad in [
            "INSERT INTO t VALUES (1)",
            "DROP TABLE t",
            "CREATE TABLE x AS SELECT 1",
            "COPY (SELECT 1) TO 'x.parquet'",
            "EXPLAIN ANALYZE SELECT 1",
            "SELECT 1; DROP TABLE t",
        ] {
            assert!(ensure_read_only(bad).is_err(), "should reject: {bad}");
        }
    }
}
