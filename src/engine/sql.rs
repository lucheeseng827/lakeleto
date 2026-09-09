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

/// The Tokio runtime the DataFusion engine drives its async reads on. It is a **process-wide
/// static** (never dropped) on purpose: an owned `Runtime` stored on the engine would be dropped
/// when the server's `AppState` drops at graceful shutdown — which happens *inside* the serve
/// runtime's async context — panicking with "Cannot drop a runtime in a context where blocking is
/// not allowed". A leaked static sidesteps that entirely. (Same pattern as `objstore::runtime`.)
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build tokio runtime for DataFusion engine")
    })
}

/// DataFusion-backed engine. Reads run on the shared static [`runtime`] via `block_on`, so callers
/// use the same synchronous [`Engine`] API as the local engine (no `async` leaks across the seam).
pub struct DataFusionEngine {
    /// How remote (`s3://`, `gs://`, `az://`) stores are configured for this engine's sessions.
    ///
    /// A field rather than a per-call argument because [`Engine`] is a stable object-safe trait
    /// shared by every backend and must not grow a cloud-specific parameter; an engine value is
    /// cheap, so a caller that reads for a particular identity constructs an engine for it with
    /// [`DataFusionEngine::with_store_options`]. Defaults to the process environment, which is
    /// what every existing caller gets.
    #[cfg(feature = "object-store")]
    store_options: crate::objstore::StoreOptions,
}

impl DataFusionEngine {
    pub fn new() -> Self {
        // Touch the runtime so it is built eagerly (a bad build surfaces here, not mid-request).
        let _ = runtime();
        Self {
            #[cfg(feature = "object-store")]
            store_options: crate::objstore::StoreOptions::from_env(),
        }
    }

    /// An engine whose remote reads are configured by `options` rather than by the environment.
    #[cfg(feature = "object-store")]
    pub fn with_store_options(options: crate::objstore::StoreOptions) -> Self {
        let _ = runtime();
        Self {
            store_options: options,
        }
    }

    /// The store configuration this engine hands to DataFusion for remote URLs.
    #[cfg(feature = "object-store")]
    pub fn store_options(&self) -> &crate::objstore::StoreOptions {
        &self.store_options
    }

    /// Teach `ctx` how to reach the object store behind `path`, if `path` is a remote URI.
    ///
    /// A bare `SessionContext` knows only the local filesystem: `register_parquet("s3://…")`
    /// against one fails with "No suitable object store found for s3://…" before a single byte is
    /// fetched, which is why SQL over a bucket did not work at all. DataFusion resolves stores
    /// through a registry keyed by scheme+authority, so the fix is to put the store there first.
    ///
    /// The store comes from [`crate::objstore::store_for_url`] — the *same* builder, and the same
    /// [`crate::objstore::StoreOptions`], that every non-SQL read goes through. That is the point:
    /// a per-caller credential configured once reaches the SQL planner and the local reader
    /// identically, instead of SQL quietly keeping a second, ambient credential path.
    #[cfg(feature = "object-store")]
    fn register_remote_store(&self, ctx: &SessionContext, path: &str) -> Result<()> {
        if !crate::source::is_object_uri(path) {
            return Ok(());
        }
        let url = url::Url::parse(path).map_err(|e| {
            EngineError::Query(format!("not a valid object-store URL `{path}`: {e}"))
        })?;
        let store = crate::objstore::store_for_url(path, &self.store_options)?;
        // Keyed on scheme + authority by DataFusion, so one registration serves every object in
        // the bucket; re-registering the same bucket replaces the entry, which is what a
        // re-registration with different options should do.
        ctx.register_object_store(&url, store);
        Ok(())
    }

    /// Without the `object-store` feature there is no store to register, so a remote URI gets the
    /// same targeted "rebuild with the feature" answer the local engine gives instead of
    /// DataFusion's opaque "No suitable object store found".
    #[cfg(not(feature = "object-store"))]
    fn register_remote_store(&self, _ctx: &SessionContext, path: &str) -> Result<()> {
        if crate::source::is_object_uri(path) {
            return Err(EngineError::missing_feature(
                "read an object-store URI",
                "object-store",
            ));
        }
        Ok(())
    }

    fn register(&self, ctx: &SessionContext, table: &NamedSource) -> Result<()> {
        let path = datafusion_path(&table.source.path.to_string_lossy());
        // DataFusion's register_csv validates the path against CsvReadOptions.file_extension
        // (default ".csv") and rejects anything else — a `.tsv` (or `--format tsv` over any name)
        // errors with "File path '...' does not match the expected extension '.csv'". Tell it the
        // file's real extension so the delimiter-driven reader accepts it. An extensionless path
        // gets "" (ends_with("") is always true → no gate), which is what we want for an explicit
        // single file.
        // DataFusion's listing tables don't cover two cases the local engine does:
        //   • Iceberg — no native provider (we deliberately avoid iceberg-datafusion's old pin);
        //   • a Hive-partitioned Parquet *directory* — `register_parquet` reads the data files but
        //     drops the `key=value` partition columns.
        // Read those through the local engine (which handles both, partition columns included) into
        // an in-memory table so SQL sees the full, correct schema. Trade-off: the table is
        // materialized in memory — fine for the explorer's tables, not a streaming path.
        let via_local = matches!(table.source.format, Format::Iceberg | Format::Delta)
            || (matches!(table.source.format, Format::Parquet) && table.source.path.is_dir());
        if via_local {
            return self.register_via_local(ctx, table);
        }
        // Must precede the register_* call below: DataFusion resolves the store while *listing*
        // the URL, so a registry that does not yet know the bucket fails the registration itself.
        self.register_remote_store(ctx, &path)?;
        let ext = std::path::Path::new(&path)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{e}"))
            .unwrap_or_default();
        runtime().block_on(async {
            match table.source.format {
                Format::Parquet => ctx
                    .register_parquet(&table.name, &path, ParquetReadOptions::default())
                    .await
                    .map_err(|e| EngineError::Query(e.to_string())),
                Format::Csv | Format::Tsv => ctx
                    .register_csv(
                        &table.name,
                        &path,
                        CsvReadOptions::default()
                            .delimiter(table.source.format.delimiter())
                            .file_extension(&ext),
                    )
                    .await
                    .map_err(|e| EngineError::Query(e.to_string())),
                other => Err(EngineError::unsupported_format(other, "sql")),
            }
        })
    }

    /// Register a source that DataFusion can't list natively (Iceberg, a partitioned Parquet dir) by
    /// reading it fully through the local engine into a [`MemTable`]. The local engine already
    /// resolves Iceberg snapshots and Hive partition columns, so SQL gets the correct schema.
    fn register_via_local(&self, ctx: &SessionContext, table: &NamedSource) -> Result<()> {
        use datafusion::datasource::MemTable;
        // Hand this engine's identity down. Without it the local reader mirrors a remote Iceberg
        // prefix as whatever principal the process environment names, so a SQL query that joins a
        // Parquet table (registered above under `store_options`) to an Iceberg one would read the
        // two as DIFFERENT principals — and the Iceberg half would ignore the caller entirely.
        #[cfg(feature = "object-store")]
        let local = crate::engine::local::LocalReaderEngine::default()
            .with_store_options(self.store_options.clone());
        #[cfg(not(feature = "object-store"))]
        let local = crate::engine::local::LocalReaderEngine::default();
        let rb = local.preview(&table.source, usize::MAX)?; // full read (no row cap)
        let mem = MemTable::try_new(rb.schema.clone(), vec![rb.batches])
            .map_err(|e| EngineError::Query(e.to_string()))?;
        ctx.register_table(table.name.as_str(), Arc::new(mem))
            .map_err(|e| EngineError::Query(e.to_string()))?;
        Ok(())
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
        runtime().block_on(async {
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
            formats: crate::engine::readable_formats(),
            sql: true,
            profile: true,
            remote: false,
        }
    }

    fn schema(&self, source: &Source) -> Result<TableSchema> {
        let ctx = self.ctx_for(source)?;
        let arrow_schema = runtime().block_on(async {
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
        runtime().block_on(async {
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

/// Escape the `LIKE` metacharacters in a filter value so it is matched **literally**, using
/// `esc` as the escape character.
///
/// The grid's filter box takes free text, and `%` and `_` are ordinary characters in it — a user
/// filtering a `discount` column for `50%` means those two characters, not "anything". Interpolated
/// raw, `%` becomes "match anything" and `_` becomes "match one character", so the SQL engines
/// answer a different question from the Arrow kernel path, which compares literally. Which answer a
/// user got would depend on how their binary was built.
///
/// The escape character itself is escaped first, or a value containing it would shift the meaning
/// of whatever follows.
fn like_escape(v: &str, esc: char) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        if c == esc || c == '%' || c == '_' {
            out.push(esc);
        }
        out.push(c);
    }
    out
}

/// DataFusion accepts **only** `\` as the `ESCAPE` character and errors on any other, so the
/// choice is made for us here. (The database builder uses `#` instead, for a reason spelled out
/// there: a backslash is itself special inside a MySQL string literal.) DataFusion does not treat
/// `\` as an escape inside a plain string literal, so a single backslash reaches `LIKE` intact.
const LIKE_ESC: char = '\\';

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
                // Cast to text first: the grid's "contains" box is applied to any column, but
                // `LIKE` only plans over Utf8 — DataFusion refuses to coerce e.g. Float64 to Utf8
                // ("There isn't a common type to coerce Float64 and Utf8 in LIKE expression").
                // CAST(col AS VARCHAR) makes substring search work on numeric/bool/temporal columns
                // too (NULLs cast to NULL → excluded, as expected).
                FilterOp::Contains => format!(
                    "CAST({id} AS VARCHAR) LIKE {} ESCAPE '{LIKE_ESC}'",
                    sql_str(&format!("%{}%", like_escape(&f.value, LIKE_ESC)))
                ),
                FilterOp::Eq => format!("{id} = {}", sql_str(&f.value)),
                FilterOp::Ne => format!("{id} <> {}", sql_str(&f.value)),
                FilterOp::Lt => format!("{id} < {}", sql_str(&f.value)),
                FilterOp::Le => format!("{id} <= {}", sql_str(&f.value)),
                FilterOp::Gt => format!("{id} > {}", sql_str(&f.value)),
                FilterOp::Ge => format!("{id} >= {}", sql_str(&f.value)),
                FilterOp::NotContains => format!(
                    "CAST({id} AS VARCHAR) NOT LIKE {} ESCAPE '{LIKE_ESC}'",
                    sql_str(&format!("%{}%", like_escape(&f.value, LIKE_ESC)))
                ),
                FilterOp::StartsWith => format!(
                    "CAST({id} AS VARCHAR) LIKE {} ESCAPE '{LIKE_ESC}'",
                    sql_str(&format!("{}%", like_escape(&f.value, LIKE_ESC)))
                ),
                FilterOp::EndsWith => format!(
                    "CAST({id} AS VARCHAR) LIKE {} ESCAPE '{LIKE_ESC}'",
                    sql_str(&format!("%{}", like_escape(&f.value, LIKE_ESC)))
                ),
                // Compared as text, matching the in-memory path: the grid's filter box is one
                // string, and splitting it into typed literals per column would make the two
                // engines disagree about `007` in an integer column.
                FilterOp::In => {
                    let members: Vec<String> = f
                        .value
                        .split(',')
                        .map(str::trim)
                        .filter(|m| !m.is_empty())
                        .map(sql_str)
                        .collect();
                    if members.is_empty() {
                        // An empty list matches nothing; `IN ()` is a syntax error in every dialect.
                        "1 = 0".to_string()
                    } else {
                        format!("CAST({id} AS VARCHAR) IN ({})", members.join(", "))
                    }
                }
                FilterOp::IsNull => format!("{id} IS NULL"),
                FilterOp::NotNull => format!("{id} IS NOT NULL"),
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

    /// P3: a bare `SessionContext` cannot reach a bucket at all — `register_parquet("s3://…")`
    /// fails with "No suitable object store found" before any byte moves — so these assert on the
    /// *registry*, which needs no credentials and no network.
    #[cfg(feature = "object-store")]
    mod remote_store {
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::prelude::SessionContext;

        use super::DataFusionEngine;
        use crate::objstore::{StoreCredentials, StoreOptions};

        fn bucket() -> ObjectStoreUrl {
            ObjectStoreUrl::parse("s3://bucket").unwrap()
        }

        #[test]
        fn a_remote_url_gets_an_object_store_registered_on_the_session() {
            let ctx = SessionContext::new();
            // The bug, reproduced: nothing knows how to reach s3:// yet.
            assert!(ctx.runtime_env().object_store(bucket()).is_err());

            DataFusionEngine::new()
                .register_remote_store(&ctx, "s3://bucket/t.parquet")
                .unwrap();
            assert!(ctx.runtime_env().object_store(bucket()).is_ok());
        }

        #[test]
        fn a_local_path_registers_nothing() {
            let ctx = SessionContext::new();
            DataFusionEngine::new()
                .register_remote_store(&ctx, "/data/t.parquet")
                .unwrap();
            assert!(ctx.runtime_env().object_store(bucket()).is_err());
        }

        #[test]
        fn registration_goes_through_the_engines_own_store_options() {
            // A provider for the wrong family is rejected by `objstore`, so seeing that error come
            // back out of the SQL path proves the SQL engine resolves its store through the same
            // seam — and therefore that a per-caller credential will reach the planner — rather
            // than keeping a second, ambient credential path of its own.
            let provider: object_store::aws::AwsCredentialProvider = std::sync::Arc::new(
                object_store::StaticCredentialProvider::new(object_store::aws::AwsCredential {
                    key_id: "AKIAEXAMPLE".to_string(),
                    secret_key: "TOP-SECRET-VALUE".to_string(),
                    token: None,
                }),
            );
            let engine = DataFusionEngine::with_store_options(
                StoreOptions::empty()
                    .with_scope("tenant-a")
                    .with_credentials(StoreCredentials::S3(provider)),
            );
            assert_eq!(engine.store_options().scope_id(), "tenant-a");

            let ctx = SessionContext::new();
            let err = engine
                .register_remote_store(&ctx, "gs://bucket/t.parquet")
                .unwrap_err();
            assert!(err.to_string().contains("S3"), "{err}");
        }
    }

    /// Without the feature there is no store to register, so the SQL path must say so rather than
    /// letting DataFusion fail with "No suitable object store found".
    #[cfg(not(feature = "object-store"))]
    #[test]
    fn a_remote_url_without_the_feature_names_the_missing_feature() {
        use datafusion::prelude::SessionContext;
        let err = DataFusionEngine::new()
            .register_remote_store(&SessionContext::new(), "s3://bucket/t.parquet")
            .unwrap_err();
        assert!(err.to_string().contains("object-store"), "{err}");
    }

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
    fn contains_filter_casts_column_to_text() {
        use super::build_where;
        use crate::engine::{FilterOp, FilterSpec};
        // "contains" must plan over any column type — cast to VARCHAR so LIKE doesn't fail
        // coercing a numeric column against a text pattern.
        let w = build_where(&[FilterSpec {
            column: "amount_usd".into(),
            op: FilterOp::Contains,
            value: "12".into(),
        }]);
        assert_eq!(
            w,
            r#" WHERE CAST("amount_usd" AS VARCHAR) LIKE '%12%' ESCAPE '\'"#
        );
    }

    #[test]
    fn like_metacharacters_in_a_filter_value_are_matched_literally() {
        use super::build_where;
        use crate::engine::{FilterOp, FilterSpec};
        // A user filtering for `50%` means those two characters. Unescaped, `%` becomes "match
        // anything" and the SQL path answers a different question from the Arrow path.
        let w = build_where(&[FilterSpec {
            column: "discount".into(),
            op: FilterOp::Contains,
            value: "50%_".into(),
        }]);
        assert_eq!(
            w,
            r#" WHERE CAST("discount" AS VARCHAR) LIKE '%50\%\_%' ESCAPE '\'"#
        );
    }

    #[test]
    fn contains_filter_matches_numeric_rows() {
        // End-to-end: a substring filter over a numeric column returns rows instead of erroring.
        let csv = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/people.csv");
        let source = Source::with_format(csv, Format::Csv);
        use crate::engine::{FilterOp, FilterSpec, ScanSpec};
        let spec = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "score".into(), // numeric column in people.csv (id,name,city,score,active)
                op: FilterOp::Contains,
                value: "3".into(),
            }],
            ..ScanSpec::default()
        };
        // The point is that planning succeeds (no type-coercion error), not the exact count.
        DataFusionEngine::new().scan(&source, &spec).unwrap();
    }

    #[test]
    fn reads_tsv_via_sql_engine() {
        // DataFusion rejected non-.csv extensions ("does not match the expected extension
        // '.csv'"); the sql engine must read a .tsv (tab-delimited) file.
        let tsv = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/mini.tsv");
        let source = Source::with_format(tsv, Format::Tsv);
        let eng = DataFusionEngine::new();
        let schema = eng.schema(&source).unwrap();
        // Tab-split gives the 4 real columns, not one giant column.
        assert_eq!(schema.columns.len(), 4, "schema: {schema:?}");
        let rb = eng
            .query(
                "SELECT count(*) AS c FROM t",
                &[crate::engine::NamedSource {
                    name: "t".into(),
                    source,
                }],
            )
            .unwrap();
        assert_eq!(rb.num_rows(), 1);
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
