//! The `database` engine: read-only SQL databases behind the same [`Engine`] trait, via `sqlx`.
//!
//! Feature-gated so a lean build never pulls a driver in. Three dialects can be compiled in, each
//! behind its own feature (`sqlite` / `postgres` / `mysql`; the umbrella `database` turns all on),
//! and all three are routed through one [`Dialect`] discriminator parsed from the URI scheme. A
//! scheme whose driver was *not* compiled in returns a clear "rebuild with --features …" error
//! instead of a confusing failure. All dialects are strictly **read-only** (see [`ensure_read_only`]).
//!
//! ## The async→sync bridge
//!
//! `sqlx` is async, but the [`Engine`] trait is synchronous (the CLI and the rest of Lakeleto call
//! it directly, no `async` leaks across the seam). We bridge exactly like the DataFusion engine in
//! [`super::sql`]: a **process-wide static** Tokio runtime accessed through [`runtime`], driven with
//! `runtime().block_on(async { … })`. The runtime is a leaked static on purpose — an owned runtime
//! stored on the engine would be dropped inside the serve runtime's async context at graceful
//! shutdown and panic with "Cannot drop a runtime in a context where blocking is not allowed".
//!
//! ## The value → Arrow mapping
//!
//! **SQLite** is dynamically typed: a column's storage class is decided per *value*, not per column,
//! so there is no schema to read off a result set the way Parquet has one. We fetch every row
//! eagerly (`fetch_all`), then for each column probe `Column::type_info().name()` across rows for the
//! first non-`NULL` storage class, build the matching Arrow array pulling each cell as `Option<T>`
//! (so `NULL`→`append_null`), and — if `try_get::<Option<T>>` fails for the chosen `T` — fall back to
//! reading the cell as a string (and coercing), then to a null. This path never panics. The
//! [`Engine::schema`] path (which must work on an **empty** table) instead reads `PRAGMA table_info`
//! and maps the *declared* column types.
//!
//! **Postgres / MySQL** are statically typed: every value in a column shares one type, so the Arrow
//! schema is built up front from `Column::type_info()` (and, for the `schema()` path on an empty
//! table, from a `describe()` of `SELECT * … LIMIT 0` — no PRAGMA needed). Each cell is still pulled
//! defensively (an integer column tries `i64`, then narrower widths, then a string parse, then null),
//! so a surprising server type never panics. The `postgres`/`mysql` features pull in `sqlx`'s
//! `bigdecimal` / `chrono` / `uuid` decoders so real columns populate: **NUMERIC / DECIMAL** →
//! `Float64` (via `BigDecimal`), all **temporal** types (`TIMESTAMP(TZ)` / `DATE` / `TIME` /
//! `DATETIME`) → `Utf8` ISO-ish strings (via `chrono`), **UUID** → `Utf8` (Postgres), and
//! **JSON/JSONB** → `Utf8` (text/raw-bytes). SQLite stays lean and needs none of these (its dynamic
//! typing already maps such values to text). See the per-dialect `*_arrow_kind` mappers.
//!
//! The SQLite path is exercised by the tests below; Postgres and MySQL need a live server and are
//! verified separately (there are no live PG/MySQL integration tests here — only pure SQL-building
//! unit tests that need no database).

#![cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]

use std::collections::HashMap;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
#[cfg(feature = "mysql")]
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
#[cfg(feature = "sqlite")]
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};
#[cfg(any(feature = "postgres", feature = "mysql"))]
use sqlx::Executor;
use sqlx::{Column, Row, TypeInfo};

use super::{
    build_table_schema, profile_columns, truncate_batches, Capabilities, Engine, FilterOp,
    FilterSpec, NamedSource, RowBatch, ScanResult, ScanSpec, TableProfile, TableSchema,
};
use crate::error::{EngineError, Result};
use crate::source::Source;

// ---------------------------------------------------------------------------------------
// Async runtime
// ---------------------------------------------------------------------------------------

/// The Tokio runtime the database engine drives its async queries on. A **process-wide static**
/// (never dropped) for the same reason as [`super::sql`]'s runtime: a runtime dropped inside the
/// serve runtime's async context at graceful shutdown panics ("Cannot drop a runtime in a context
/// where blocking is not allowed"). A leaked static sidesteps that entirely, and it also keeps the
/// pooled connections' background tasks alive for the process lifetime.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build tokio runtime for database engine")
    })
}

// ---------------------------------------------------------------------------------------
// Per-dialect pool caches + getters
// ---------------------------------------------------------------------------------------

/// Process-wide cache of open SQLite pools, keyed by **connection URL** (the URI with any
/// `?table=` stripped), so repeated `schema`/`preview`/`scan`/`query` calls against the same file
/// reuse one pool instead of reopening the database each time. `SqlitePool` is `Arc`-backed, so a
/// cached clone is cheap.
#[cfg(feature = "sqlite")]
fn pools() -> &'static Mutex<HashMap<String, SqlitePool>> {
    static POOLS: OnceLock<Mutex<HashMap<String, SqlitePool>>> = OnceLock::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Open (or reuse) a **read-only** SQLite pool for `conn_url`.
///
/// The pool is opened with `read_only(true)` so even a query that slips past [`ensure_read_only`]
/// (e.g. a CTE-wrapped write) is rejected by SQLite itself — defence in depth. `immutable(false)`
/// keeps normal locking (we are read-only, not asserting the file never changes).
#[cfg(feature = "sqlite")]
fn get_pool(conn_url: &str) -> Result<SqlitePool> {
    let mut guard = pools()
        .lock()
        .map_err(|e| EngineError::Other(format!("database pool cache poisoned: {e}")))?;
    if let Some(pool) = guard.get(conn_url) {
        return Ok(pool.clone());
    }
    let opts = SqliteConnectOptions::from_str(conn_url)
        .map_err(|e| {
            EngineError::Query(format!("invalid sqlite connection url `{conn_url}`: {e}"))
        })?
        .read_only(true)
        .immutable(false);
    let pool = runtime()
        .block_on(async {
            SqlitePoolOptions::new()
                .max_connections(4)
                .connect_with(opts)
                .await
        })
        .map_err(|e| {
            EngineError::Query(format!("could not open sqlite database `{conn_url}`: {e}"))
        })?;
    guard.insert(conn_url.to_string(), pool.clone());
    Ok(pool)
}

/// Run a query on `pool` and collect every row (the async→sync bridge in one place).
#[cfg(feature = "sqlite")]
fn fetch_all(pool: &SqlitePool, sql: &str, params: &[Bound]) -> Result<Vec<SqliteRow>> {
    runtime()
        .block_on(async {
            let mut q = sqlx::query(sql);
            for p in params {
                q = match p {
                    Bound::Text(v) => q.bind(v.as_str()),
                    Bound::Int(v) => q.bind(*v),
                    Bound::Num(v) => q.bind(*v),
                };
            }
            q.fetch_all(pool).await
        })
        .map_err(|e| EngineError::Query(format!("query failed: {e}")))
}

/// Like [`fetch_all`] but stops pulling rows at `cap`: the stream is dropped mid-flight, which
/// closes the statement, so neither this process's memory nor (past the driver's buffer) the
/// wire carries more than `cap` rows. The server still *executes* the full query — bounding that
/// would mean rewriting the user's SQL, and a wrapped `LIMIT` subquery is exactly the kind of
/// rewrite that breaks on dialect corners (MySQL ignores an inner `ORDER BY` without an inner
/// `LIMIT`, for one). Stopping the fetch is the bound we can make honestly.
#[cfg(feature = "sqlite")]
fn fetch_capped(
    pool: &SqlitePool,
    sql: &str,
    params: &[Bound],
    cap: usize,
) -> Result<Vec<SqliteRow>> {
    use futures::TryStreamExt;
    if cap == 0 {
        return Ok(Vec::new());
    }
    runtime()
        .block_on(async {
            let mut q = sqlx::query(sql);
            for p in params {
                q = match p {
                    Bound::Text(v) => q.bind(v.as_str()),
                    Bound::Int(v) => q.bind(*v),
                    Bound::Num(v) => q.bind(*v),
                };
            }
            let mut stream = q.fetch(pool);
            let mut out = Vec::new();
            while let Some(row) = stream.try_next().await? {
                out.push(row);
                if out.len() >= cap {
                    break;
                }
            }
            Ok(out)
        })
        .map_err(|e: sqlx::Error| EngineError::Query(format!("query failed: {e}")))
}

/// Begin an explicit **read-only transaction** before every Postgres/MySQL query.
///
/// This is the real read-only guarantee for the networked engines, and it is stronger than a
/// session default. Connections are pooled and reused, so a crafted `SELECT
/// set_config('default_transaction_read_only','off', false)` (Postgres) or a `MODIFIES SQL DATA`
/// function call (MySQL) could otherwise leave a *writable* connection in the pool for the next
/// query. Re-declaring `READ ONLY` at the start of each transaction pins the property per query
/// regardless of session state, and makes the server itself refuse `SELECT … INTO`, data-modifying
/// CTEs, and any write side-effect — no matter what the statement text looks like. Every query is
/// rolled back afterwards (a read-only transaction has nothing to commit). Both dialects accept
/// this exact statement. SQLite needs none of this: its pool is opened `SQLITE_OPEN_READONLY` at
/// the file handle, which no statement can flip.
#[cfg(any(feature = "postgres", feature = "mysql"))]
const BEGIN_READ_ONLY: &str = "START TRANSACTION READ ONLY";

/// Process-wide cache of open Postgres pools, keyed by connection URL (mirrors [`pools`]).
///
/// Unlike SQLite's `read_only(true)` file open, Postgres's read-only guarantee is set on the
/// *session*: every connection this pool opens runs `default_transaction_read_only = on` (see
/// [`get_pg_pool`]), so the server itself refuses any write — each autocommit statement is its own
/// transaction, so the setting applies to all of them. [`ensure_read_only`] is then defence in
/// depth on top of that, not the only line. `max_connections` is kept small: the engine is an
/// interactive explorer, not a fan-out service.
#[cfg(feature = "postgres")]
fn pg_pools() -> &'static Mutex<HashMap<String, PgPool>> {
    static POOLS: OnceLock<Mutex<HashMap<String, PgPool>>> = OnceLock::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Open (or reuse) a Postgres pool for `conn_url` (`postgres://user:pass@host:port/db`).
///
/// The connection is opened with `default_transaction_read_only = on` passed as a startup
/// `options` parameter, so the **server** enforces read-only for the whole session: a statement
/// that somehow slipped past [`ensure_read_only`] (a data-modifying CTE, `EXPLAIN ANALYZE DELETE`)
/// is rejected by Postgres itself with `cannot execute … in a read-only transaction`. This is the
/// pool-level guarantee SQLite gets from `read_only(true)`.
#[cfg(feature = "postgres")]
fn get_pg_pool(conn_url: &str) -> Result<PgPool> {
    let mut guard = pg_pools()
        .lock()
        .map_err(|e| EngineError::Other(format!("database pool cache poisoned: {e}")))?;
    if let Some(pool) = guard.get(conn_url) {
        return Ok(pool.clone());
    }
    let opts = PgConnectOptions::from_str(conn_url)
        .map_err(|e| {
            EngineError::Query(format!("invalid postgres connection url `{conn_url}`: {e}"))
        })?
        // Startup `options` are applied by the server before the first query runs, so every
        // statement on every connection from this pool is read-only at the source.
        .options([("default_transaction_read_only", "on")]);
    let pool = runtime()
        .block_on(async {
            PgPoolOptions::new()
                .max_connections(4)
                .connect_with(opts)
                .await
        })
        .map_err(|e| {
            EngineError::Query(format!(
                "could not open postgres database `{conn_url}`: {e}"
            ))
        })?;
    guard.insert(conn_url.to_string(), pool.clone());
    Ok(pool)
}

/// Run a query on a Postgres `pool` and collect every row.
#[cfg(feature = "postgres")]
fn pg_fetch_all(pool: &PgPool, sql: &str, params: &[Bound]) -> Result<Vec<PgRow>> {
    runtime()
        .block_on(async {
            let mut conn = pool.acquire().await?;
            sqlx::query(BEGIN_READ_ONLY).execute(&mut *conn).await?;
            let mut q = sqlx::query(sql);
            for p in params {
                q = match p {
                    Bound::Text(v) => q.bind(v.as_str()),
                    Bound::Int(v) => q.bind(*v),
                    Bound::Num(v) => q.bind(*v),
                };
            }
            let res = q.fetch_all(&mut *conn).await;
            // Roll back on every path (a read-only transaction has nothing to commit). The only
            // early returns above are a failed acquire/BEGIN, where no transaction is open.
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            res
        })
        .map_err(|e| EngineError::Query(format!("query failed: {e}")))
}

/// See the SQLite `fetch_capped` for why this streams and stops rather than rewriting the SQL.
#[cfg(feature = "postgres")]
fn pg_fetch_capped(pool: &PgPool, sql: &str, params: &[Bound], cap: usize) -> Result<Vec<PgRow>> {
    use futures::TryStreamExt;
    if cap == 0 {
        return Ok(Vec::new());
    }
    runtime()
        .block_on(async {
            let mut conn = pool.acquire().await?;
            sqlx::query(BEGIN_READ_ONLY).execute(&mut *conn).await?;
            let mut q = sqlx::query(sql);
            for p in params {
                q = match p {
                    Bound::Text(v) => q.bind(v.as_str()),
                    Bound::Int(v) => q.bind(*v),
                    Bound::Num(v) => q.bind(*v),
                };
            }
            // Collect up to `cap` rows, capturing any stream error rather than early-returning past
            // the ROLLBACK below. The stream is dropped before ROLLBACK so the `&mut conn` is free.
            let collected = {
                let mut stream = q.fetch(&mut *conn);
                let mut out = Vec::new();
                let mut err = None;
                loop {
                    match stream.try_next().await {
                        Ok(Some(row)) => {
                            out.push(row);
                            if out.len() >= cap {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            err = Some(e);
                            break;
                        }
                    }
                }
                match err {
                    Some(e) => Err(e),
                    None => Ok(out),
                }
            };
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            collected
        })
        .map_err(|e: sqlx::Error| EngineError::Query(format!("query failed: {e}")))
}

/// Process-wide cache of open MySQL pools, keyed by connection URL (mirrors [`pools`]).
///
/// MySQL, like Postgres, has no portable "read-only pool" option, so read-only is enforced on the
/// query path by [`ensure_read_only`].
#[cfg(feature = "mysql")]
fn mysql_pools() -> &'static Mutex<HashMap<String, MySqlPool>> {
    static POOLS: OnceLock<Mutex<HashMap<String, MySqlPool>>> = OnceLock::new();
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Open (or reuse) a MySQL pool for `conn_url` (`mysql://user:pass@host:port/db`). sqlx parses the
/// URL directly. See [`mysql_pools`] on why read-only is enforced on the query path, not the pool.
#[cfg(feature = "mysql")]
fn get_mysql_pool(conn_url: &str) -> Result<MySqlPool> {
    let mut guard = mysql_pools()
        .lock()
        .map_err(|e| EngineError::Other(format!("database pool cache poisoned: {e}")))?;
    if let Some(pool) = guard.get(conn_url) {
        return Ok(pool.clone());
    }
    let pool = runtime()
        .block_on(async {
            MySqlPoolOptions::new()
                .max_connections(4)
                .connect(conn_url)
                .await
        })
        .map_err(|e| {
            EngineError::Query(format!("could not open mysql database `{conn_url}`: {e}"))
        })?;
    guard.insert(conn_url.to_string(), pool.clone());
    Ok(pool)
}

/// Run a query on a MySQL `pool` and collect every row.
#[cfg(feature = "mysql")]
fn mysql_fetch_all(pool: &MySqlPool, sql: &str, params: &[Bound]) -> Result<Vec<MySqlRow>> {
    runtime()
        .block_on(async {
            let mut conn = pool.acquire().await?;
            sqlx::query(BEGIN_READ_ONLY).execute(&mut *conn).await?;
            let mut q = sqlx::query(sql);
            for p in params {
                q = match p {
                    Bound::Text(v) => q.bind(v.as_str()),
                    Bound::Int(v) => q.bind(*v),
                    Bound::Num(v) => q.bind(*v),
                };
            }
            let res = q.fetch_all(&mut *conn).await;
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            res
        })
        .map_err(|e| EngineError::Query(format!("query failed: {e}")))
}

/// See the SQLite `fetch_capped` for why this streams and stops rather than rewriting the SQL.
#[cfg(feature = "mysql")]
fn mysql_fetch_capped(
    pool: &MySqlPool,
    sql: &str,
    params: &[Bound],
    cap: usize,
) -> Result<Vec<MySqlRow>> {
    use futures::TryStreamExt;
    if cap == 0 {
        return Ok(Vec::new());
    }
    runtime()
        .block_on(async {
            let mut conn = pool.acquire().await?;
            sqlx::query(BEGIN_READ_ONLY).execute(&mut *conn).await?;
            let mut q = sqlx::query(sql);
            for p in params {
                q = match p {
                    Bound::Text(v) => q.bind(v.as_str()),
                    Bound::Int(v) => q.bind(*v),
                    Bound::Num(v) => q.bind(*v),
                };
            }
            let collected = {
                let mut stream = q.fetch(&mut *conn);
                let mut out = Vec::new();
                let mut err = None;
                loop {
                    match stream.try_next().await {
                        Ok(Some(row)) => {
                            out.push(row);
                            if out.len() >= cap {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            err = Some(e);
                            break;
                        }
                    }
                }
                match err {
                    Some(e) => Err(e),
                    None => Ok(out),
                }
            };
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            collected
        })
        .map_err(|e: sqlx::Error| EngineError::Query(format!("query failed: {e}")))
}

// ---------------------------------------------------------------------------------------
// URI parsing
// ---------------------------------------------------------------------------------------

/// The SQL dialect a connection URI names. Each variant exists only when its driver feature is
/// compiled in, so a lean single-backend build carries exactly one variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dialect {
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySql,
}

/// A parsed database URI: the sqlx connection string (no `?table=`) plus the table to read (absent
/// only for the "whole database" form used by [`list_tables`]).
#[derive(Debug)]
struct DbUri {
    /// The connection string handed to sqlx (e.g. `sqlite:///C:/data/app.db`).
    conn_url: String,
    /// The table named by `?table=<name>`, if any.
    table: Option<String>,
    /// The dialect the scheme named — routes the connect + row-mapping.
    dialect: Dialect,
}

/// Parse a Lakeleto database source URI.
///
/// Splits off a `?table=<name>` query param (the table to read); the remainder — including any
/// other query params — is the sqlx connection string. The dialect is taken from the scheme; a
/// scheme whose driver was not compiled in returns a clear "rebuild with --features …" error rather
/// than a confusing driver failure.
///
/// Examples: `sqlite:///C:/data/app.db?table=orders`, `postgres://u:p@host/db?table=orders`,
/// `mysql://u:p@host/db` (no table → whole-db).
fn parse_db_uri(raw: &str) -> Result<DbUri> {
    let scheme = raw
        .split_once("://")
        .map(|(s, _)| s.to_ascii_lowercase())
        .ok_or_else(|| {
            EngineError::Query(format!(
                "`{raw}` is not a database connection URI (expected e.g. \
                 `sqlite:///path/to.db?table=orders`)"
            ))
        })?;

    let dialect = match scheme.as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => Dialect::Sqlite,
        #[cfg(not(feature = "sqlite"))]
        "sqlite" => return Err(dialect_not_built("sqlite", "sqlite")),

        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => Dialect::Postgres,
        #[cfg(not(feature = "postgres"))]
        "postgres" | "postgresql" => return Err(dialect_not_built(&scheme, "postgres")),

        #[cfg(feature = "mysql")]
        "mysql" => Dialect::MySql,
        #[cfg(not(feature = "mysql"))]
        "mysql" => return Err(dialect_not_built("mysql", "mysql")),

        other => {
            return Err(EngineError::Query(format!(
                "`{other}://` is not a supported database dialect (expected sqlite://…, \
                 postgres://…, or mysql://…)"
            )))
        }
    };

    // Split the query string off, pull out `table`, keep any other params on the connection URL.
    let (base, query) = raw.split_once('?').unwrap_or((raw, ""));
    let mut table = None;
    let mut kept: Vec<&str> = Vec::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key.eq_ignore_ascii_case("table") {
            table = Some(value.to_string());
        } else {
            kept.push(pair);
        }
    }
    let conn_url = if kept.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept.join("&"))
    };

    Ok(DbUri {
        conn_url,
        table,
        dialect,
    })
}

/// The "you didn't compile that dialect" error for a driver feature that is off in this build. Only
/// present when at least one dialect feature is off (with all three built, no scheme is unbuilt).
#[cfg(not(all(feature = "sqlite", feature = "postgres", feature = "mysql")))]
fn dialect_not_built(scheme: &str, feature: &str) -> EngineError {
    EngineError::Query(format!(
        "database dialect `{scheme}` is not built into this binary — rebuild with \
         `cargo build --features {feature}` (the `database` feature enables all of \
         sqlite/postgres/mysql)"
    ))
}

/// The URI string backing a [`Source`] (a DB source stores its connection URI in `path`).
fn source_uri(source: &Source) -> String {
    source.path.to_string_lossy().to_string()
}

/// Require that `uri` names a concrete table (schema/preview/scan/profile all read one table).
fn require_table(uri: &DbUri, raw: &str) -> Result<String> {
    uri.table.clone().ok_or_else(|| {
        EngineError::Query(format!(
            "the database URI `{raw}` names a whole database, not a table — append \
             `?table=<name>` (e.g. `{raw}?table=orders`); the /v1/list endpoint enumerates \
             available tables"
        ))
    })
}

// ---------------------------------------------------------------------------------------
// Per-dialect dispatch (one place each engine method routes on `Dialect`)
// ---------------------------------------------------------------------------------------

/// The SQL syntax the dialect's identifier/`WHERE`-clause builders need.
fn engine_syntax(dialect: Dialect) -> SqlSyntax {
    match dialect {
        #[cfg(feature = "sqlite")]
        Dialect::Sqlite => SqlSyntax::SQLITE,
        #[cfg(feature = "postgres")]
        Dialect::Postgres => SqlSyntax::POSTGRES,
        #[cfg(feature = "mysql")]
        Dialect::MySql => SqlSyntax::MYSQL,
    }
}

/// Fetch every row of `sql` against `uri`'s database and map it to an Arrow [`RowBatch`].
fn fetch_batch(uri: &DbUri, sql: &str, params: &[Bound]) -> Result<RowBatch> {
    match uri.dialect {
        #[cfg(feature = "sqlite")]
        Dialect::Sqlite => {
            let pool = get_pool(&uri.conn_url)?;
            rows_to_batch(fetch_all(&pool, sql, params)?)
        }
        #[cfg(feature = "postgres")]
        Dialect::Postgres => {
            let pool = get_pg_pool(&uri.conn_url)?;
            pg_rows_to_batch(pg_fetch_all(&pool, sql, params)?)
        }
        #[cfg(feature = "mysql")]
        Dialect::MySql => {
            let pool = get_mysql_pool(&uri.conn_url)?;
            mysql_rows_to_batch(mysql_fetch_all(&pool, sql, params)?)
        }
    }
}

/// Read `table`'s Arrow schema, robust for an **empty** table (SQLite via `PRAGMA table_info`,
/// Postgres/MySQL via `describe(SELECT * … LIMIT 0)` — neither needs any rows to exist).
fn schema_of(uri: &DbUri, table: &str) -> Result<SchemaRef> {
    match uri.dialect {
        #[cfg(feature = "sqlite")]
        Dialect::Sqlite => {
            let pool = get_pool(&uri.conn_url)?;
            schema_via_pragma(&pool, table)
        }
        #[cfg(feature = "postgres")]
        Dialect::Postgres => {
            let pool = get_pg_pool(&uri.conn_url)?;
            pg_schema(&pool, table)
        }
        #[cfg(feature = "mysql")]
        Dialect::MySql => {
            let pool = get_mysql_pool(&uri.conn_url)?;
            mysql_schema(&pool, table)
        }
    }
}

/// Read the scalar from a `SELECT count(*)` query against `uri`'s database.
fn count_scalar(uri: &DbUri, sql: &str, params: &[Bound]) -> Result<usize> {
    match uri.dialect {
        #[cfg(feature = "sqlite")]
        Dialect::Sqlite => {
            let pool = get_pool(&uri.conn_url)?;
            Ok(count_value(&fetch_all(&pool, sql, params)?))
        }
        #[cfg(feature = "postgres")]
        Dialect::Postgres => {
            let pool = get_pg_pool(&uri.conn_url)?;
            Ok(pg_count_value(&pg_fetch_all(&pool, sql, params)?))
        }
        #[cfg(feature = "mysql")]
        Dialect::MySql => {
            let pool = get_mysql_pool(&uri.conn_url)?;
            Ok(mysql_count_value(&mysql_fetch_all(&pool, sql, params)?))
        }
    }
}

// ---------------------------------------------------------------------------------------
// Public free functions
// ---------------------------------------------------------------------------------------

/// List the user tables of a database (SQLite excludes its internal `sqlite_*` tables; Postgres
/// excludes the `pg_catalog`/`information_schema` system schemas; MySQL lists the current database).
///
/// Takes the **raw source path string** (a database URI, with or without a `?table=`); the api.rs
/// `/v1/list` handler calls this to enumerate tables for a whole-database source. Returns the table
/// names in alphabetical order.
pub fn list_tables(url_path: &str) -> Result<Vec<String>> {
    let uri = parse_db_uri(url_path)?;
    match uri.dialect {
        #[cfg(feature = "sqlite")]
        Dialect::Sqlite => {
            let pool = get_pool(&uri.conn_url)?;
            let rows = fetch_all(
                &pool,
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
                &[],
            )?;
            collect_names(rows.iter().map(|r| r.try_get::<String, _>(0)))
        }
        #[cfg(feature = "postgres")]
        Dialect::Postgres => {
            let pool = get_pg_pool(&uri.conn_url)?;
            let rows = pg_fetch_all(
                &pool,
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN \
                 ('pg_catalog','information_schema') ORDER BY tablename",
                &[],
            )?;
            collect_names(rows.iter().map(|r| r.try_get::<String, _>(0)))
        }
        #[cfg(feature = "mysql")]
        Dialect::MySql => {
            let pool = get_mysql_pool(&uri.conn_url)?;
            let rows = mysql_fetch_all(
                &pool,
                // CAST to CHAR: MySQL 8's information_schema returns table_name as VARBINARY, which
                // sqlx won't decode straight to String ("VARCHAR is not compatible with VARBINARY").
                "SELECT CAST(table_name AS CHAR) FROM information_schema.tables \
                 WHERE table_schema = DATABASE() ORDER BY table_name",
                &[],
            )?;
            collect_names(rows.iter().map(|r| r.try_get::<String, _>(0)))
        }
    }
}

/// Collect table names from a per-row `try_get` result, turning a decode failure into an
/// [`EngineError::Query`]. Shared by every dialect's [`list_tables`] arm.
fn collect_names<I>(rows: I) -> Result<Vec<String>>
where
    I: Iterator<Item = std::result::Result<String, sqlx::Error>>,
{
    rows.map(|r| r.map_err(|e| EngineError::Query(format!("reading table name failed: {e}"))))
        .collect()
}

// ---------------------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------------------

/// Read-only SQL database engine (SQLite / Postgres / MySQL, whichever features are compiled in).
/// Queries run on the shared static [`runtime`] via `block_on`, so callers use the same synchronous
/// [`Engine`] API as every other backend.
pub struct DatabaseEngine;

impl DatabaseEngine {
    pub fn new() -> Self {
        // Touch the runtime so it is built eagerly (a bad build surfaces here, not mid-request).
        let _ = runtime();
        Self
    }
}

impl Default for DatabaseEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine for DatabaseEngine {
    fn name(&self) -> &str {
        "database"
    }

    fn capabilities(&self) -> Capabilities {
        // Report exactly the dialects compiled into this build.
        let mut formats: Vec<String> = Vec::new();
        #[cfg(feature = "sqlite")]
        formats.push("sqlite".to_string());
        #[cfg(feature = "postgres")]
        formats.push("postgres".to_string());
        #[cfg(feature = "mysql")]
        formats.push("mysql".to_string());
        Capabilities {
            engine: format!("database ({})", formats.join("+")),
            formats,
            sql: true,
            profile: true,
            remote: false,
        }
    }

    fn schema(&self, source: &Source) -> Result<TableSchema> {
        let raw = source_uri(source);
        let uri = parse_db_uri(&raw)?;
        let table = require_table(&uri, &raw)?;
        let arrow_schema = schema_of(&uri, &table)?;
        Ok(build_table_schema(source, self.name(), None, &arrow_schema))
    }

    fn preview(&self, source: &Source, limit: usize) -> Result<RowBatch> {
        let raw = source_uri(source);
        let uri = parse_db_uri(&raw)?;
        let table = require_table(&uri, &raw)?;
        let syntax = engine_syntax(uri.dialect);
        let sql = format!(
            "SELECT * FROM {} LIMIT {limit}",
            quote_ident(syntax, &table)
        );
        let mut rb = fetch_batch(&uri, &sql, &[])?;
        // Empty result: the mapper produced an empty schema. Fall back to the described/PRAGMA schema
        // so the grid still shows column headers.
        if rb.batches.is_empty() {
            rb.schema = schema_of(&uri, &table)?;
        }
        Ok(rb)
    }

    fn profile(&self, source: &Source, scan_limit: usize) -> Result<TableProfile> {
        let raw = source_uri(source);
        let uri = parse_db_uri(&raw)?;
        let table = require_table(&uri, &raw)?;
        let syntax = engine_syntax(uri.dialect);
        let sql = format!(
            "SELECT * FROM {} LIMIT {scan_limit}",
            quote_ident(syntax, &table)
        );
        let rb = fetch_batch(&uri, &sql, &[])?;
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

    /// Run raw SQL against the database of the **first** table's source URI. The database engine
    /// ignores `tables` *registration* — the server already owns its tables, so the SQL references
    /// them by name directly; the sources only tell us which database to open.
    fn query(&self, sql: &str, tables: &[NamedSource]) -> Result<RowBatch> {
        // Lakeleto is an *explorer*: user SQL must never mutate. Reject anything that isn't a read.
        ensure_read_only(sql)?;
        let first = tables.first().ok_or_else(|| {
            EngineError::Query(
                "the database engine needs a source table to know which database to query \
                 (got none)"
                    .to_string(),
            )
        })?;
        let raw = source_uri(&first.source);
        let uri = parse_db_uri(&raw)?;
        fetch_batch(&uri, sql, &[])
    }

    /// Bounded query: rows are pulled from a **stream** that stops at `cap`, so a `SELECT *` over
    /// a huge table never materialises more than `cap` rows in this process regardless of what
    /// the query would return. The bound the server itself does is stated honestly rather than
    /// implied: the database still *executes* the full query (bounding that would mean rewriting
    /// the user's SQL, which breaks on dialect corners), but neither this process's memory nor —
    /// past the driver's buffer — the connection carries more than `cap` rows.
    fn query_capped(&self, sql: &str, tables: &[NamedSource], cap: usize) -> Result<RowBatch> {
        ensure_read_only(sql)?;
        let first = tables.first().ok_or_else(|| {
            EngineError::Query(
                "the database engine needs a source table to know which database to query \
                 (got none)"
                    .to_string(),
            )
        })?;
        let raw = source_uri(&first.source);
        let uri = parse_db_uri(&raw)?;
        let rb = match uri.dialect {
            #[cfg(feature = "sqlite")]
            Dialect::Sqlite => {
                let pool = get_pool(&uri.conn_url)?;
                rows_to_batch(fetch_capped(&pool, sql, &[], cap)?)?
            }
            #[cfg(feature = "postgres")]
            Dialect::Postgres => {
                let pool = get_pg_pool(&uri.conn_url)?;
                pg_rows_to_batch(pg_fetch_capped(&pool, sql, &[], cap)?)?
            }
            #[cfg(feature = "mysql")]
            Dialect::MySql => {
                let pool = get_mysql_pool(&uri.conn_url)?;
                mysql_rows_to_batch(mysql_fetch_capped(&pool, sql, &[], cap)?)?
            }
        };
        // Belt over the stream's own stop, and the bound the type signature promises.
        Ok(RowBatch {
            schema: rb.schema,
            batches: truncate_batches(rb.batches, cap),
        })
    }

    /// Grid scan with filter/sort/projection/window pushed into SQL, plus a `count(*)` for the
    /// exact match total. Unlike the local engine this is not bounded by a working set, so
    /// sort/filter over a large table is complete.
    fn scan(&self, source: &Source, spec: &ScanSpec) -> Result<ScanResult> {
        let raw = source_uri(source);
        let uri = parse_db_uri(&raw)?;
        let table = require_table(&uri, &raw)?;
        let syntax = engine_syntax(uri.dialect);
        let ident = quote_ident(syntax, &table);

        // The column schema tells `build_where` which columns compare numerically (so a numeric
        // column filtered with a number compares as a number, as the in-memory engine does, rather
        // than binding text and — on Postgres — erroring on `int = text`). Fetch it only when there
        // are filters; an unfiltered scan consults no column types, and the empty-result fallback
        // below still fetches lazily on its own.
        let schema = if spec.filters.is_empty() {
            None
        } else {
            Some(schema_of(&uri, &table)?)
        };
        let kind_of = |col: &str| -> ColKind {
            schema
                .as_ref()
                .and_then(|s| s.column_with_name(col))
                .map(|(_, field)| col_kind(field.data_type()))
                .unwrap_or(ColKind::Other)
        };
        let WhereClause {
            sql: where_sql,
            params,
        } = build_where(syntax, &spec.filters, &kind_of);

        let matched = count_scalar(
            &uri,
            &format!("SELECT count(*) AS c FROM {ident}{where_sql}"),
            &params,
        )?;

        let proj = match &spec.projection {
            Some(cols) if !cols.is_empty() => cols
                .iter()
                .map(|c| quote_ident(syntax, c))
                .collect::<Vec<_>>()
                .join(", "),
            _ => "*".to_string(),
        };
        let order = match &spec.sort {
            Some(s) => format!(
                " ORDER BY {} {}",
                quote_ident(syntax, &s.column),
                if s.descending { "DESC" } else { "ASC" }
            ),
            None => String::new(),
        };
        let sql = format!(
            "SELECT {proj} FROM {ident}{where_sql}{order} LIMIT {} OFFSET {}",
            spec.limit, spec.offset
        );
        let mut batch = fetch_batch(&uri, &sql, &params)?;
        if batch.batches.is_empty() {
            // Reuse the schema fetched for numeric detection if we have it; otherwise (unfiltered
            // scan) fetch it now so the grid still shows column headers.
            batch.schema = match &schema {
                Some(s) => Arc::clone(s),
                None => schema_of(&uri, &table)?,
            };
        }
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

// ---------------------------------------------------------------------------------------
// SQL building helpers (dialect-parameterised)
// ---------------------------------------------------------------------------------------

/// The bits of SQL syntax that differ between dialects. Constructed by [`engine_syntax`].
#[derive(Debug, Clone, Copy)]
struct SqlSyntax {
    /// Identifier quote char: `"` for SQLite/Postgres, `` ` `` for MySQL.
    ident_quote: char,
    /// The `CAST(<col> AS <text_cast>)` target for the "contains" filter: `TEXT` (SQLite/Postgres)
    /// or `CHAR` (MySQL — `CAST(x AS TEXT)` is a syntax error there).
    text_cast: &'static str,
    /// Whether bind placeholders are numbered (`$1`, `$2` — Postgres) or anonymous (`?` — SQLite,
    /// MySQL). Filter VALUES are bound, never interpolated, so a value can never break out of a
    /// literal and inject SQL; this only decides how the placeholders are spelled.
    numbered_placeholders: bool,
}

impl SqlSyntax {
    #[cfg(feature = "sqlite")]
    const SQLITE: SqlSyntax = SqlSyntax {
        ident_quote: '"',
        text_cast: "TEXT",
        numbered_placeholders: false,
    };
    #[cfg(feature = "postgres")]
    const POSTGRES: SqlSyntax = SqlSyntax {
        ident_quote: '"',
        text_cast: "TEXT",
        numbered_placeholders: true,
    };
    #[cfg(feature = "mysql")]
    const MYSQL: SqlSyntax = SqlSyntax {
        ident_quote: '`',
        text_cast: "CHAR",
        numbered_placeholders: false,
    };
}

/// Quote an identifier for `syntax`'s dialect, doubling any embedded quote char (safe quoting).
/// `"col"` for SQLite/Postgres, `` `col` `` for MySQL.
fn quote_ident(syntax: SqlSyntax, s: &str) -> String {
    let q = syntax.ident_quote;
    let doubled = format!("{q}{q}");
    format!("{q}{}{q}", s.replace(q, &doubled))
}

/// Escape the `LIKE` metacharacters in a filter value so it is matched **literally**, using
/// `esc` as the escape character. Mirrors the DataFusion builder's helper; see the reasoning there.
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

/// `#`, not the more usual `\`, and deliberately different from the DataFusion builder's choice.
///
/// MySQL processes backslash escapes **inside string literals** (unless `NO_BACKSLASH_ESCAPES` is
/// set), so `'foo\%bar'` reaches `LIKE` as `foo%bar` with the escape already eaten and the
/// wildcard live again — the exact bug this escaping exists to prevent, in one of the three
/// dialects. `#` is inert in a string literal in SQLite, Postgres and MySQL alike, and all three
/// accept an arbitrary `ESCAPE` character. DataFusion cannot use `#` (it accepts only `\`), which
/// is why the two builders differ rather than sharing one constant.
const LIKE_ESC: char = '#';

/// A value bound into a query. Numeric-column comparisons bind [`Bound::Num`] so the driver sends a
/// numeric parameter type — matching the in-memory engine's f64 comparison, and, on Postgres,
/// avoiding the `operator does not exist: integer = text` error a text-typed bind would raise
/// against a numeric column (Postgres does not implicitly cast text→numeric for an operator, unlike
/// the unknown-type literal an interpolated `'5'` would have been). Everything else binds
/// [`Bound::Text`].
#[derive(Debug, PartialEq)]
enum Bound {
    Text(String),
    /// An exact integer, for integer-column comparisons. Bound as `i64` so a value above 2^53
    /// (e.g. a Snowflake-style ID) compares exactly, which routing it through `f64` would not.
    Int(i64),
    Num(f64),
}

/// A `WHERE` clause and the values its **bind placeholders** stand for, in order.
///
/// Filter values are never interpolated into the SQL — they are bound. That is the whole security
/// story for this path: a value like `\' OR 1=1 #` cannot terminate a string literal it never
/// enters, so no dialect's string-escaping quirk (MySQL's backslash, say) can turn a filter into
/// an injected predicate. Identifiers still go through `quote_ident` because SQL cannot bind an
/// identifier, and column names come from the grid's own schema, not free text.
struct WhereClause {
    sql: String,
    params: Vec<Bound>,
}

/// How a column compares, mirroring the in-memory engine's `num_class` so the SQL and in-memory
/// paths agree: `Integer` and `Real` columns compare *numerically* (a numeric filter value binds a
/// number), `Other` compares as text. The Integer/Real split exists only so an integer column can
/// bind an exact `i64` rather than a lossy `f64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColKind {
    Integer,
    Real,
    Other,
}

/// Classify an Arrow [`DataType`] for comparison. Decimals fold into `Real` (bound as `f64`, matching
/// the in-memory engine, which also casts them to `f64`) — so a very-high-precision decimal filter
/// value is compared at `f64` precision on both paths, not exactly.
fn col_kind(dt: &DataType) -> ColKind {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ColKind::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => ColKind::Real,
        _ => ColKind::Other,
    }
}

/// Build a `WHERE` clause from the grid's filters (all ANDed together), quoting identifiers per
/// `syntax` and **binding** every value. See [`WhereClause`].
///
/// `numeric` reports whether a column compares as a number (mirrors the in-memory engine's
/// `num_class`); a numeric column filtered with a numeric value compares numerically, everything
/// else compares as text. `numeric` returning `false` for an unknown column is the safe default:
/// text comparison never raises a type error, it just may not match a numeric row a numeric
/// comparison would have.
fn build_where(
    syntax: SqlSyntax,
    filters: &[FilterSpec],
    kind_of: &dyn Fn(&str) -> ColKind,
) -> WhereClause {
    let mut params: Vec<Bound> = Vec::new();
    // The next placeholder, advancing `params`. `$1`,`$2` on Postgres; `?` elsewhere.
    let placeholder = |value: Bound, params: &mut Vec<Bound>| -> String {
        params.push(value);
        if syntax.numbered_placeholders {
            format!("${}", params.len())
        } else {
            "?".to_string()
        }
    };
    // An ordering/equality comparison. When the column is numeric *and* the value parses as a
    // number, bind a numeric parameter and compare the column directly (numeric comparison, as the
    // in-memory engine does). An integer column binds an exact `i64` so a value above 2^53 is not
    // rounded; a fractional value on an integer column still binds `f64` so ordering matches the
    // in-memory engine. Anything else renders the column to text and compares as text — which also
    // cannot raise a type error on any dialect.
    let comparison =
        |id: &str, cast: &str, op: &str, f: &FilterSpec, params: &mut Vec<Bound>| -> String {
            let bound = match kind_of(&f.column) {
                ColKind::Integer => f
                    .value
                    .parse::<i64>()
                    .map(Bound::Int)
                    .ok()
                    .or_else(|| f.value.parse::<f64>().map(Bound::Num).ok()),
                ColKind::Real => f.value.parse::<f64>().map(Bound::Num).ok(),
                ColKind::Other => None,
            };
            match bound {
                Some(b) => {
                    let ph = placeholder(b, params);
                    format!("{id} {op} {ph}")
                }
                None => {
                    let ph = placeholder(Bound::Text(f.value.clone()), params);
                    format!("CAST({id} AS {cast}) {op} {ph}")
                }
            }
        };

    if filters.is_empty() {
        return WhereClause {
            sql: String::new(),
            params,
        };
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| {
            let id = quote_ident(syntax, &f.column);
            let cast = syntax.text_cast;
            match f.op {
                // Cast to text so "contains" works on any column type: `LIKE` only applies to text.
                // The LIKE wildcards (`%`,`_`) in the *pattern* are escaped so a value matches
                // literally; the value itself is then bound, not interpolated.
                FilterOp::Contains => {
                    let ph = placeholder(
                        Bound::Text(format!("%{}%", like_escape(&f.value, LIKE_ESC))),
                        &mut params,
                    );
                    format!("CAST({id} AS {cast}) LIKE {ph} ESCAPE '{LIKE_ESC}'")
                }
                FilterOp::NotContains => {
                    let ph = placeholder(
                        Bound::Text(format!("%{}%", like_escape(&f.value, LIKE_ESC))),
                        &mut params,
                    );
                    format!("CAST({id} AS {cast}) NOT LIKE {ph} ESCAPE '{LIKE_ESC}'")
                }
                FilterOp::StartsWith => {
                    let ph = placeholder(
                        Bound::Text(format!("{}%", like_escape(&f.value, LIKE_ESC))),
                        &mut params,
                    );
                    format!("CAST({id} AS {cast}) LIKE {ph} ESCAPE '{LIKE_ESC}'")
                }
                FilterOp::EndsWith => {
                    let ph = placeholder(
                        Bound::Text(format!("%{}", like_escape(&f.value, LIKE_ESC))),
                        &mut params,
                    );
                    format!("CAST({id} AS {cast}) LIKE {ph} ESCAPE '{LIKE_ESC}'")
                }
                FilterOp::Eq => comparison(&id, cast, "=", f, &mut params),
                FilterOp::Ne => comparison(&id, cast, "<>", f, &mut params),
                FilterOp::Lt => comparison(&id, cast, "<", f, &mut params),
                FilterOp::Le => comparison(&id, cast, "<=", f, &mut params),
                FilterOp::Gt => comparison(&id, cast, ">", f, &mut params),
                FilterOp::Ge => comparison(&id, cast, ">=", f, &mut params),
                // Compared as text, matching the in-memory path: the grid's filter box is one
                // string, and splitting it into typed literals per column would make the two
                // engines disagree about `007` in an integer column.
                FilterOp::In => {
                    let members: Vec<&str> = f
                        .value
                        .split(',')
                        .map(str::trim)
                        .filter(|m| !m.is_empty())
                        .collect();
                    if members.is_empty() {
                        // An empty list matches nothing; `IN ()` is a syntax error in every dialect.
                        "1 = 0".to_string()
                    } else {
                        let phs: Vec<String> = members
                            .iter()
                            .map(|m| placeholder(Bound::Text((*m).to_string()), &mut params))
                            .collect();
                        format!("CAST({id} AS {cast}) IN ({})", phs.join(", "))
                    }
                }
                FilterOp::IsNull => format!("{id} IS NULL"),
                FilterOp::NotNull => format!("{id} IS NOT NULL"),
            }
        })
        .collect();
    WhereClause {
        sql: format!(" WHERE {}", parts.join(" AND ")),
        params,
    }
}

/// The write verbs a read-only query must never contain — *anywhere*, not just at the head.
///
/// The head keyword being `SELECT`/`WITH`/`EXPLAIN` is not enough: a data-modifying CTE
/// (`WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x`) and `EXPLAIN ANALYZE DELETE …` both
/// present an innocent head while executing a write on Postgres. So we reject the write verb as a
/// standalone word anywhere in the statement. `OUTFILE`/`DUMPFILE` cover MySQL's `SELECT … INTO
/// OUTFILE`, the one way a plain `SELECT` writes. Boundary-matching means `updated_at` /
/// `is_deleted` columns don't false-positive; a column literally *named* `delete` (quoted) would,
/// which is the acceptable tradeoff a parser-free guard makes (same as the `;`-in-a-string case).
const FORBIDDEN_WRITE_WORDS: &[&str] = &[
    "INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE", "TRUNCATE", "DROP", "CREATE", "ALTER",
    "GRANT", "REVOKE", "CALL", "COPY", "VACUUM", "ATTACH", "DETACH", "LOAD", "OUTFILE", "DUMPFILE",
];

/// The subset of [`FORBIDDEN_WRITE_WORDS`] that is *also* a scalar function name in some dialect —
/// `REPLACE(x,a,b)` is standard in SQLite/MySQL/Postgres, and the rest can appear as a function or
/// alias. When one of these is immediately followed by `(` it is a call, not a statement verb, so
/// it is allowed. Every *write* form of these words (`REPLACE INTO`, `MERGE INTO`, `COPY t FROM`,
/// `LOAD DATA`, `CALL proc`) puts whitespace after the verb, never `(`, so exempting the call form
/// closes no write vector.
const VERBS_ALSO_FUNCTIONS: &[&str] = &["REPLACE", "MERGE", "COPY", "LOAD", "CALL"];

/// Does `haystack` (already uppercased) contain `word` as a statement verb — delimited by non-word
/// characters on both sides, and (for a word that is also a function name) not immediately followed
/// by `(`? A "word char" is `[A-Za-z0-9_]`, so `UPDATE` matches in `... UPDATE t ...` but not inside
/// `updated_at`, and `REPLACE(` is read as the function, not the `REPLACE INTO` write. `word` is
/// ASCII-uppercase; the caller uppercases `haystack` once.
fn contains_word(haystack: &str, word: &str) -> bool {
    let bytes = haystack.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let call_exempt = VERBS_ALSO_FUNCTIONS.contains(&word);
    let mut from = 0;
    while let Some(rel) = haystack[from..].find(word) {
        let start = from + rel;
        let end = start + word.len();
        let before_ok = start == 0 || !is_word(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_word(bytes[end]);
        // `REPLACE(` is the function call; `REPLACE INTO` (whitespace) is the write.
        let is_call = call_exempt && bytes.get(end) == Some(&b'(');
        if before_ok && after_ok && !is_call {
            return true;
        }
        from = start + 1;
    }
    false
}

/// Reject any SQL that is not a single read-only query.
///
/// A lightweight guard (this build has no SQL parser — DataFusion lives behind the `sql` feature):
/// it allows a single `SELECT` / `WITH` / `EXPLAIN` statement, rejects any mid-statement `;` (naive
/// multi-statement smuggling), and — because an innocent head keyword is not proof of a read (a
/// data-modifying CTE or `EXPLAIN ANALYZE DELETE` both start `WITH`/`EXPLAIN` yet write on
/// Postgres) — rejects any [`FORBIDDEN_WRITE_WORDS`] verb appearing anywhere in the statement. For
/// SQLite this is *defence in depth* — the pool is opened `read_only`. For Postgres it layers over
/// the pool's `default_transaction_read_only`. For MySQL (no portable read-only pool) this guard
/// *is* the read-only guarantee.
pub fn ensure_read_only(sql: &str) -> Result<()> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    // A `;` that isn't the (already-stripped) trailing one implies multiple statements.
    // (A `;` inside a string literal would false-positive; acceptable for an explorer guard.)
    if trimmed.contains(';') {
        return Err(EngineError::Query(
            "exactly one SQL statement is allowed".to_string(),
        ));
    }
    let upper = trimmed.to_ascii_uppercase();
    let head = upper.split_whitespace().next().unwrap_or("");
    if !matches!(head, "SELECT" | "WITH" | "EXPLAIN") {
        return Err(EngineError::Query(
            "only read queries are allowed — SELECT / WITH, or EXPLAIN".to_string(),
        ));
    }
    // Close the CTE / EXPLAIN-ANALYZE write vectors: a write verb anywhere is a hard reject.
    if let Some(verb) = FORBIDDEN_WRITE_WORDS
        .iter()
        .find(|w| contains_word(&upper, w))
    {
        return Err(EngineError::Query(format!(
            "only read queries are allowed — this statement contains `{verb}`, which can write \
             (e.g. a data-modifying CTE or `EXPLAIN ANALYZE {verb}`)"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Value → Arrow: shared kinds
// ---------------------------------------------------------------------------------------

/// The Arrow array kind chosen for a result-set column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArrowKind {
    Int64,
    Float64,
    Utf8,
    Boolean,
    Binary,
}

/// The Arrow [`DataType`] an [`ArrowKind`] materialises to (for the up-front `schema()` path where
/// there are no rows to build an array from).
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn kind_to_datatype(kind: ArrowKind) -> DataType {
    match kind {
        ArrowKind::Int64 => DataType::Int64,
        ArrowKind::Float64 => DataType::Float64,
        ArrowKind::Boolean => DataType::Boolean,
        ArrowKind::Binary => DataType::Binary,
        ArrowKind::Utf8 => DataType::Utf8,
    }
}

/// Parse a textual boolean (for the fallback path when a boolean column decodes as text). Shared by
/// every dialect's boolean array builder.
fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "t" | "yes" | "y" => Some(true),
        "0" | "false" | "f" | "no" | "n" => Some(false),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------
// Dynamic SQLite value → Arrow RecordBatch mapping
// ---------------------------------------------------------------------------------------

/// Map a SQLite/sqlx storage-class or type name (e.g. from `type_info().name()`) to an Arrow kind.
#[cfg(feature = "sqlite")]
fn arrow_kind_for(type_name: &str) -> ArrowKind {
    match type_name {
        "INTEGER" | "INT" | "BIGINT" | "INT8" | "INT4" | "INT2" | "SMALLINT" | "TINYINT"
        | "MEDIUMINT" => ArrowKind::Int64,
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => ArrowKind::Float64,
        "BOOLEAN" | "BOOL" => ArrowKind::Boolean,
        "BLOB" | "BYTEA" => ArrowKind::Binary,
        // TEXT, DATETIME, DATE, TIME, NUMERIC, NULL, and anything unknown → text (lossless enough).
        _ => ArrowKind::Utf8,
    }
}

/// Map a *declared* column type (from `PRAGMA table_info`) to an Arrow [`DataType`] using SQLite's
/// type-affinity substring rules (so `VARCHAR(50)`, `BIG INT`, `DOUBLE PRECISION` all resolve).
#[cfg(feature = "sqlite")]
fn declared_type_to_arrow(declared: &str) -> DataType {
    let u = declared.to_ascii_uppercase();
    if u.contains("INT") {
        DataType::Int64
    } else if u.contains("BOOL") {
        DataType::Boolean
    } else if u.contains("REAL") || u.contains("FLOA") || u.contains("DOUB") {
        DataType::Float64
    } else if u.contains("BLOB") {
        DataType::Binary
    } else {
        // TEXT / CHAR / CLOB / DATE / DATETIME / NUMERIC / DECIMAL / empty → text.
        DataType::Utf8
    }
}

/// Read a SQLite table's Arrow schema via `PRAGMA table_info`, mapping *declared* column types. This
/// is robust for empty tables (unlike row-probing, which needs at least one row to infer from).
#[cfg(feature = "sqlite")]
fn schema_via_pragma(pool: &SqlitePool, table: &str) -> Result<SchemaRef> {
    let rows = fetch_all(
        pool,
        &format!(
            "PRAGMA table_info({})",
            quote_ident(SqlSyntax::SQLITE, table)
        ),
        &[],
    )?;
    if rows.is_empty() {
        return Err(EngineError::Query(format!(
            "table `{table}` was not found (or has no columns)"
        )));
    }
    let mut fields = Vec::with_capacity(rows.len());
    for row in &rows {
        let name: String = row
            .try_get("name")
            .map_err(|e| EngineError::Query(format!("reading column name failed: {e}")))?;
        let declared: String = row.try_get("type").unwrap_or_default();
        fields.push(Field::new(name, declared_type_to_arrow(&declared), true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Decide a column's Arrow kind by probing every row's storage class for the first non-`NULL` type
/// (SQLite storage class is per-value, so the first row alone can be `NULL` even in a typed column).
#[cfg(feature = "sqlite")]
fn column_kind(rows: &[SqliteRow], col: usize) -> ArrowKind {
    for row in rows {
        if let Some(c) = row.columns().get(col) {
            let name = c.type_info().name().to_ascii_uppercase();
            if !name.is_empty() && name != "NULL" {
                return arrow_kind_for(&name);
            }
        }
    }
    ArrowKind::Utf8
}

/// Best-effort read of a SQLite cell as a `String`: try `TEXT`, then coerce from
/// `INTEGER`/`REAL`/`BLOB`. Returns `None` for `NULL` (and for a value no type could decode) — the
/// caller appends a null.
#[cfg(feature = "sqlite")]
fn string_cell(row: &SqliteRow, col: usize) -> Option<String> {
    if let Ok(opt) = row.try_get::<Option<String>, _>(col) {
        return opt;
    }
    if let Ok(opt) = row.try_get::<Option<i64>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<f64>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<Vec<u8>>, _>(col) {
        return opt.map(|b| String::from_utf8_lossy(&b).into_owned());
    }
    None
}

/// Build one Arrow column (`col`) of the chosen [`ArrowKind`] from the fetched SQLite rows. Never
/// panics: a cell whose chosen type fails to decode falls back to a string coercion, then to a null.
#[cfg(feature = "sqlite")]
fn build_array(rows: &[SqliteRow], col: usize, kind: ArrowKind) -> (DataType, ArrayRef) {
    match kind {
        ArrowKind::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<i64>, _>(col) {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => {
                        match string_cell(row, col).and_then(|s| s.trim().parse::<i64>().ok()) {
                            Some(v) => b.append_value(v),
                            None => b.append_null(),
                        }
                    }
                }
            }
            (DataType::Int64, Arc::new(b.finish()))
        }
        ArrowKind::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<f64>, _>(col) {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => {
                        match string_cell(row, col).and_then(|s| s.trim().parse::<f64>().ok()) {
                            Some(v) => b.append_value(v),
                            None => b.append_null(),
                        }
                    }
                }
            }
            (DataType::Float64, Arc::new(b.finish()))
        }
        ArrowKind::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<bool>, _>(col) {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => match string_cell(row, col).as_deref().and_then(parse_bool) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                }
            }
            (DataType::Boolean, Arc::new(b.finish()))
        }
        ArrowKind::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                match row.try_get::<Option<Vec<u8>>, _>(col) {
                    Ok(Some(v)) => b.append_value(&v),
                    Ok(None) => b.append_null(),
                    Err(_) => match string_cell(row, col) {
                        Some(s) => b.append_value(s.as_bytes()),
                        None => b.append_null(),
                    },
                }
            }
            (DataType::Binary, Arc::new(b.finish()))
        }
        ArrowKind::Utf8 => {
            let mut b = StringBuilder::new();
            for row in rows {
                match string_cell(row, col) {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            (DataType::Utf8, Arc::new(b.finish()))
        }
    }
}

/// Map a fetched SQLite result set to an Arrow [`RowBatch`]. See the module docs for the strategy.
///
/// An empty result set carries no column metadata to infer types from, so it maps to an empty
/// schema + no batches; callers that know the table (`preview`/`scan`) substitute the PRAGMA schema
/// for nicer headers.
#[cfg(feature = "sqlite")]
fn rows_to_batch(rows: Vec<SqliteRow>) -> Result<RowBatch> {
    if rows.is_empty() {
        return Ok(RowBatch {
            schema: Arc::new(Schema::empty()),
            batches: vec![],
        });
    }
    let ncols = rows[0].columns().len();
    let mut fields = Vec::with_capacity(ncols);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
    for col in 0..ncols {
        let name = rows[0].columns()[col].name().to_string();
        let kind = column_kind(&rows, col);
        let (dt, arr) = build_array(&rows, col, kind);
        fields.push(Field::new(name, dt, true));
        arrays.push(arr);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(EngineError::arrow)?;
    Ok(RowBatch {
        schema,
        batches: vec![batch],
    })
}

/// Read the scalar from a SQLite `SELECT count(*)` result.
#[cfg(feature = "sqlite")]
fn count_value(rows: &[SqliteRow]) -> usize {
    rows.first()
        .and_then(|r| r.try_get::<i64, _>(0).ok())
        .map(|v| v.max(0) as usize)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------------------
// Postgres value → Arrow RecordBatch mapping
// ---------------------------------------------------------------------------------------

/// Map a Postgres type name (from `PgTypeInfo::name()`, e.g. `INT4`/`FLOAT8`/`TEXT`) to an Arrow
/// kind. `NUMERIC`/`DECIMAL` map to `Float64` (decoded via `BigDecimal`); temporal, `UUID`, `JSON(B)`
/// and other text-ish types map to `Utf8` (temporal via `chrono`, UUID via `uuid`). See the
/// per-cell readers ([`pg_float_cell`] / [`pg_string_cell`]) for how each is populated.
#[cfg(feature = "postgres")]
fn pg_arrow_kind(type_name: &str) -> ArrowKind {
    match type_name.to_ascii_uppercase().as_str() {
        "INT2" | "INT4" | "INT8" | "SMALLINT" | "INT" | "INTEGER" | "BIGINT" | "SERIAL2"
        | "SERIAL4" | "SERIAL8" | "SMALLSERIAL" | "SERIAL" | "BIGSERIAL" | "OID" => {
            ArrowKind::Int64
        }
        "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE PRECISION" | "NUMERIC" | "DECIMAL" => {
            ArrowKind::Float64
        }
        "BOOL" | "BOOLEAN" => ArrowKind::Boolean,
        "BYTEA" => ArrowKind::Binary,
        _ => ArrowKind::Utf8,
    }
}

/// Best-effort read of a Postgres cell as a `String`: text columns, then `UUID` and the temporal
/// types (`TIMESTAMPTZ`→`DateTime<Utc>`, `TIMESTAMP`→`NaiveDateTime`, `DATE`→`NaiveDate`,
/// `TIME`→`NaiveTime`, rendered ISO-ish via `Display`), then numeric→text coercions and a raw-bytes
/// fallback. Returns `None` for `NULL` and for any type nothing could decode.
#[cfg(feature = "postgres")]
fn pg_string_cell(row: &PgRow, col: usize) -> Option<String> {
    if let Ok(opt) = row.try_get::<Option<String>, _>(col) {
        return opt;
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::Uuid>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) =
        row.try_get::<Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>, _>(col)
    {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<i64>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<i32>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<f64>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<bool>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<Vec<u8>>, _>(col) {
        return opt.map(|b| String::from_utf8_lossy(&b).into_owned());
    }
    None
}

/// Read a Postgres integer cell as `i64`, widening from the column's real width (`INT2`→`i16`,
/// `INT4`→`i32`, `INT8`→`i64`), then a string parse — never panicking. `None` means NULL/undecodable.
#[cfg(feature = "postgres")]
fn pg_int_cell(row: &PgRow, col: usize) -> Option<i64> {
    if let Ok(opt) = row.try_get::<Option<i64>, _>(col) {
        return opt;
    }
    if let Ok(opt) = row.try_get::<Option<i32>, _>(col) {
        return opt.map(|v| v as i64);
    }
    if let Ok(opt) = row.try_get::<Option<i16>, _>(col) {
        return opt.map(|v| v as i64);
    }
    if let Ok(opt) = row.try_get::<Option<i8>, _>(col) {
        return opt.map(|v| v as i64);
    }
    pg_string_cell(row, col).and_then(|s| s.trim().parse::<i64>().ok())
}

/// Read a Postgres float cell as `f64` (`FLOAT4`→`f32`, `FLOAT8`→`f64`, `NUMERIC`/`DECIMAL`→
/// `BigDecimal` stringified then parsed — `BigDecimal` has no direct `to_f64`), then a string parse.
#[cfg(feature = "postgres")]
fn pg_float_cell(row: &PgRow, col: usize) -> Option<f64> {
    if let Ok(opt) = row.try_get::<Option<f64>, _>(col) {
        return opt;
    }
    if let Ok(opt) = row.try_get::<Option<f32>, _>(col) {
        return opt.map(|v| v as f64);
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::BigDecimal>, _>(col) {
        return opt.and_then(|d| d.to_string().parse::<f64>().ok());
    }
    pg_string_cell(row, col).and_then(|s| s.trim().parse::<f64>().ok())
}

/// Build one Arrow column of the chosen [`ArrowKind`] from fetched Postgres rows. Never panics.
#[cfg(feature = "postgres")]
fn pg_build_array(rows: &[PgRow], col: usize, kind: ArrowKind) -> (DataType, ArrayRef) {
    match kind {
        ArrowKind::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match pg_int_cell(row, col) {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            (DataType::Int64, Arc::new(b.finish()))
        }
        ArrowKind::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match pg_float_cell(row, col) {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            (DataType::Float64, Arc::new(b.finish()))
        }
        ArrowKind::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<bool>, _>(col) {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => match pg_string_cell(row, col).as_deref().and_then(parse_bool) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                }
            }
            (DataType::Boolean, Arc::new(b.finish()))
        }
        ArrowKind::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                match row.try_get::<Option<Vec<u8>>, _>(col) {
                    Ok(Some(v)) => b.append_value(&v),
                    Ok(None) => b.append_null(),
                    Err(_) => match pg_string_cell(row, col) {
                        Some(s) => b.append_value(s.as_bytes()),
                        None => b.append_null(),
                    },
                }
            }
            (DataType::Binary, Arc::new(b.finish()))
        }
        ArrowKind::Utf8 => {
            let mut b = StringBuilder::new();
            for row in rows {
                match pg_string_cell(row, col) {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            (DataType::Utf8, Arc::new(b.finish()))
        }
    }
}

/// Map a fetched Postgres result set to an Arrow [`RowBatch`]. The schema is built up front from the
/// (statically typed) column metadata; an empty result maps to an empty schema (callers substitute
/// [`pg_schema`] for headers).
#[cfg(feature = "postgres")]
fn pg_rows_to_batch(rows: Vec<PgRow>) -> Result<RowBatch> {
    if rows.is_empty() {
        return Ok(RowBatch {
            schema: Arc::new(Schema::empty()),
            batches: vec![],
        });
    }
    let ncols = rows[0].columns().len();
    let mut fields = Vec::with_capacity(ncols);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
    for col in 0..ncols {
        let column = &rows[0].columns()[col];
        let name = column.name().to_string();
        let kind = pg_arrow_kind(column.type_info().name());
        let (dt, arr) = pg_build_array(&rows, col, kind);
        fields.push(Field::new(name, dt, true));
        arrays.push(arr);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(EngineError::arrow)?;
    Ok(RowBatch {
        schema,
        batches: vec![batch],
    })
}

/// Read a Postgres table's Arrow schema from a `describe(SELECT * … LIMIT 0)` — no rows needed, so
/// it works on an empty table.
#[cfg(feature = "postgres")]
fn pg_schema(pool: &PgPool, table: &str) -> Result<SchemaRef> {
    let sql = format!(
        "SELECT * FROM {} LIMIT 0",
        quote_ident(SqlSyntax::POSTGRES, table)
    );
    let described = runtime()
        .block_on(async { pool.describe(sql.as_str()).await })
        .map_err(|e| EngineError::Query(format!("could not read schema of `{table}`: {e}")))?;
    let cols = described.columns();
    if cols.is_empty() {
        return Err(EngineError::Query(format!(
            "table `{table}` was not found (or has no columns)"
        )));
    }
    let fields = cols
        .iter()
        .map(|c| {
            Field::new(
                c.name().to_string(),
                kind_to_datatype(pg_arrow_kind(c.type_info().name())),
                true,
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

/// Read the scalar from a Postgres `SELECT count(*)` result (`count(*)` is `INT8` → `i64`).
#[cfg(feature = "postgres")]
fn pg_count_value(rows: &[PgRow]) -> usize {
    rows.first()
        .and_then(|r| pg_int_cell(r, 0))
        .map(|v| v.max(0) as usize)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------------------
// MySQL value → Arrow RecordBatch mapping
// ---------------------------------------------------------------------------------------

/// Map a MySQL type name (from `MySqlTypeInfo::name()`, e.g. `INT`/`BIGINT`/`VARCHAR`) to an Arrow
/// kind. MySQL has no real boolean (`BOOLEAN` is `TINYINT`, reported as `TINYINT` → `Int64`).
/// `DECIMAL` maps to `Float64` (decoded via `BigDecimal`); temporal (`DATE`/`TIME`/`DATETIME`/
/// `TIMESTAMP`) and `JSON` map to `Utf8` (temporal via `chrono`, JSON via the raw-bytes fallback).
#[cfg(feature = "mysql")]
fn mysql_arrow_kind(type_name: &str) -> ArrowKind {
    match type_name.to_ascii_uppercase().as_str() {
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT"
        | "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" | "YEAR" => ArrowKind::Int64,
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NEWDECIMAL" => ArrowKind::Float64,
        "BOOLEAN" | "BOOL" => ArrowKind::Boolean,
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => {
            ArrowKind::Binary
        }
        _ => ArrowKind::Utf8,
    }
}

/// Best-effort read of a MySQL cell as a `String`: text columns first, then the temporal types
/// (`DATETIME`/`TIMESTAMP`→`NaiveDateTime`, `DATE`→`NaiveDate`, `TIME`→`NaiveTime`, rendered via
/// `Display`), then a raw-bytes fallback (lossy) that captures `TEXT`/`JSON` even when `String` does
/// not decode, then numeric→text coercions. Returns `None` for `NULL` / undecodable.
#[cfg(feature = "mysql")]
fn mysql_string_cell(row: &MySqlRow, col: usize) -> Option<String> {
    if let Ok(opt) = row.try_get::<Option<String>, _>(col) {
        return opt;
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<Vec<u8>>, _>(col) {
        return opt.map(|b| String::from_utf8_lossy(&b).into_owned());
    }
    if let Ok(opt) = row.try_get::<Option<i64>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    if let Ok(opt) = row.try_get::<Option<f64>, _>(col) {
        return opt.map(|v| v.to_string());
    }
    None
}

/// Read a MySQL integer cell as `i64`, widening from the column's real width and including the
/// unsigned widths (so `BIGINT UNSIGNED`, and `count(*)`'s `BIGINT UNSIGNED`, decode), then a string
/// parse — never panicking. `None` means NULL/undecodable.
#[cfg(feature = "mysql")]
fn mysql_int_cell(row: &MySqlRow, col: usize) -> Option<i64> {
    if let Ok(opt) = row.try_get::<Option<i64>, _>(col) {
        return opt;
    }
    if let Ok(opt) = row.try_get::<Option<i32>, _>(col) {
        return opt.map(|v| v as i64);
    }
    if let Ok(opt) = row.try_get::<Option<i16>, _>(col) {
        return opt.map(|v| v as i64);
    }
    if let Ok(opt) = row.try_get::<Option<i8>, _>(col) {
        return opt.map(|v| v as i64);
    }
    if let Ok(opt) = row.try_get::<Option<u64>, _>(col) {
        return opt.map(|v| v as i64);
    }
    if let Ok(opt) = row.try_get::<Option<u32>, _>(col) {
        return opt.map(|v| v as i64);
    }
    mysql_string_cell(row, col).and_then(|s| s.trim().parse::<i64>().ok())
}

/// Read a MySQL float cell as `f64` (`FLOAT`→`f32`, `DOUBLE`→`f64`, `DECIMAL`→`BigDecimal`
/// stringified then parsed), then a string parse.
#[cfg(feature = "mysql")]
fn mysql_float_cell(row: &MySqlRow, col: usize) -> Option<f64> {
    if let Ok(opt) = row.try_get::<Option<f64>, _>(col) {
        return opt;
    }
    if let Ok(opt) = row.try_get::<Option<f32>, _>(col) {
        return opt.map(|v| v as f64);
    }
    if let Ok(opt) = row.try_get::<Option<sqlx::types::BigDecimal>, _>(col) {
        return opt.and_then(|d| d.to_string().parse::<f64>().ok());
    }
    mysql_string_cell(row, col).and_then(|s| s.trim().parse::<f64>().ok())
}

/// Build one Arrow column of the chosen [`ArrowKind`] from fetched MySQL rows. Never panics.
#[cfg(feature = "mysql")]
fn mysql_build_array(rows: &[MySqlRow], col: usize, kind: ArrowKind) -> (DataType, ArrayRef) {
    match kind {
        ArrowKind::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match mysql_int_cell(row, col) {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            (DataType::Int64, Arc::new(b.finish()))
        }
        ArrowKind::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match mysql_float_cell(row, col) {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            (DataType::Float64, Arc::new(b.finish()))
        }
        ArrowKind::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.try_get::<Option<bool>, _>(col) {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => match mysql_string_cell(row, col).as_deref().and_then(parse_bool) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                }
            }
            (DataType::Boolean, Arc::new(b.finish()))
        }
        ArrowKind::Binary => {
            let mut b = BinaryBuilder::new();
            for row in rows {
                match row.try_get::<Option<Vec<u8>>, _>(col) {
                    Ok(Some(v)) => b.append_value(&v),
                    Ok(None) => b.append_null(),
                    Err(_) => match mysql_string_cell(row, col) {
                        Some(s) => b.append_value(s.as_bytes()),
                        None => b.append_null(),
                    },
                }
            }
            (DataType::Binary, Arc::new(b.finish()))
        }
        ArrowKind::Utf8 => {
            let mut b = StringBuilder::new();
            for row in rows {
                match mysql_string_cell(row, col) {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            (DataType::Utf8, Arc::new(b.finish()))
        }
    }
}

/// Map a fetched MySQL result set to an Arrow [`RowBatch`] (schema built up front from the static
/// column metadata; empty result → empty schema, callers substitute [`mysql_schema`]).
#[cfg(feature = "mysql")]
fn mysql_rows_to_batch(rows: Vec<MySqlRow>) -> Result<RowBatch> {
    if rows.is_empty() {
        return Ok(RowBatch {
            schema: Arc::new(Schema::empty()),
            batches: vec![],
        });
    }
    let ncols = rows[0].columns().len();
    let mut fields = Vec::with_capacity(ncols);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
    for col in 0..ncols {
        let column = &rows[0].columns()[col];
        let name = column.name().to_string();
        let kind = mysql_arrow_kind(column.type_info().name());
        let (dt, arr) = mysql_build_array(&rows, col, kind);
        fields.push(Field::new(name, dt, true));
        arrays.push(arr);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(EngineError::arrow)?;
    Ok(RowBatch {
        schema,
        batches: vec![batch],
    })
}

/// Read a MySQL table's Arrow schema from a `describe(SELECT * … LIMIT 0)` — no rows needed.
#[cfg(feature = "mysql")]
fn mysql_schema(pool: &MySqlPool, table: &str) -> Result<SchemaRef> {
    let sql = format!(
        "SELECT * FROM {} LIMIT 0",
        quote_ident(SqlSyntax::MYSQL, table)
    );
    let described = runtime()
        .block_on(async { pool.describe(sql.as_str()).await })
        .map_err(|e| EngineError::Query(format!("could not read schema of `{table}`: {e}")))?;
    let cols = described.columns();
    if cols.is_empty() {
        return Err(EngineError::Query(format!(
            "table `{table}` was not found (or has no columns)"
        )));
    }
    let fields = cols
        .iter()
        .map(|c| {
            Field::new(
                c.name().to_string(),
                kind_to_datatype(mysql_arrow_kind(c.type_info().name())),
                true,
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

/// Read the scalar from a MySQL `SELECT count(*)` result (`count(*)` is `BIGINT UNSIGNED`).
#[cfg(feature = "mysql")]
fn mysql_count_value(rows: &[MySqlRow]) -> usize {
    rows.first()
        .and_then(|r| mysql_int_cell(r, 0))
        .map(|v| v.max(0) as usize)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------

/// Dialect-parameterised SQL-building unit tests — no database needed, so each runs under just its
/// dialect's feature (Postgres/MySQL are otherwise verified live, separately).
#[cfg(test)]
mod dialect_sql_tests {
    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_quotes_with_double_quotes_and_text_cast() {
        use super::{build_where, quote_ident, Bound, ColKind, SqlSyntax};
        use crate::engine::{FilterOp, FilterSpec};
        assert_eq!(quote_ident(SqlSyntax::POSTGRES, "co\"l"), "\"co\"\"l\"");
        let w = build_where(
            SqlSyntax::POSTGRES,
            &[FilterSpec {
                column: "amt".into(),
                op: FilterOp::Contains,
                value: "2".into(),
            }],
            &|_| ColKind::Other,
        );
        // The value is a bound placeholder (`$1`), never interpolated into the SQL.
        assert_eq!(w.sql, r#" WHERE CAST("amt" AS TEXT) LIKE $1 ESCAPE '#'"#);
        assert_eq!(w.params, vec![Bound::Text("%2%".to_string())]);

        // A real column filtered with a number compares numerically (no CAST), binding an f64.
        let n = build_where(
            SqlSyntax::POSTGRES,
            &[FilterSpec {
                column: "amt".into(),
                op: FilterOp::Gt,
                value: "2".into(),
            }],
            &|_| ColKind::Real,
        );
        assert_eq!(n.sql, r#" WHERE "amt" > $1"#);
        assert_eq!(n.params, vec![Bound::Num(2.0)]);

        // An integer column binds an exact i64, never an f64 — so a value above 2^53 is not rounded.
        let big = build_where(
            SqlSyntax::POSTGRES,
            &[FilterSpec {
                column: "id".into(),
                op: FilterOp::Eq,
                value: "9007199254740993".into(),
            }],
            &|_| ColKind::Integer,
        );
        assert_eq!(big.sql, r#" WHERE "id" = $1"#);
        assert_eq!(big.params, vec![Bound::Int(9007199254740993)]);
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_quotes_with_backticks_and_char_cast() {
        use super::{build_where, quote_ident, Bound, ColKind, SqlSyntax};
        use crate::engine::{FilterOp, FilterSpec};
        assert_eq!(quote_ident(SqlSyntax::MYSQL, "co`l"), "`co``l`");
        let w = build_where(
            SqlSyntax::MYSQL,
            &[FilterSpec {
                column: "amt".into(),
                op: FilterOp::Contains,
                value: "2".into(),
            }],
            &|_| ColKind::Other,
        );
        // The value is a bound placeholder (`?`), never interpolated into the SQL — this is what
        // keeps MySQL's backslash-in-string-literal handling from ever seeing a filter value.
        assert_eq!(w.sql, " WHERE CAST(`amt` AS CHAR) LIKE ? ESCAPE '#'");
        assert_eq!(w.params, vec![Bound::Text("%2%".to_string())]);

        // Text comparison against a non-numeric column casts to CHAR and binds text.
        let t = build_where(
            SqlSyntax::MYSQL,
            &[FilterSpec {
                column: "name".into(),
                op: FilterOp::Eq,
                value: "ada".into(),
            }],
            &|_| ColKind::Other,
        );
        assert_eq!(t.sql, " WHERE CAST(`name` AS CHAR) = ?");
        assert_eq!(t.params, vec![Bound::Text("ada".to_string())]);
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::engine::{FilterOp, FilterSpec, NamedSource, ScanSpec};
    use crate::source::{Format, Source};
    use std::path::Path;

    /// Build the sqlx-friendly connection URL for a file path (forward slashes; `sqlite:///`).
    /// Windows: `C:\a\b.db` → `sqlite:///C:/a/b.db`. Unix: `/tmp/x.db` → `sqlite:///tmp/x.db`.
    fn conn_url(path: &Path) -> String {
        let p = path.to_string_lossy().replace('\\', "/");
        format!("sqlite:///{}", p.trim_start_matches('/'))
    }

    /// The full Lakeleto DB source URI (connection URL + `?table=`).
    fn table_uri(path: &Path, table: &str) -> String {
        format!("{}?table={table}", conn_url(path))
    }

    /// Create a throwaway SQLite database with a `t(id, name, amt, ok)` table and 3 rows (one NULL).
    fn make_db(path: &Path) {
        let url = conn_url(path);
        runtime().block_on(async {
            let opts = SqliteConnectOptions::from_str(&url)
                .expect("parse conn url")
                .create_if_missing(true);
            let pool = SqlitePool::connect_with(opts)
                .await
                .expect("open writable db");
            sqlx::query("CREATE TABLE t(id INTEGER, name TEXT, amt REAL, ok BOOLEAN)")
                .execute(&pool)
                .await
                .expect("create table");
            sqlx::query("INSERT INTO t (id, name, amt, ok) VALUES (1,'alice',1.5,1)")
                .execute(&pool)
                .await
                .expect("insert 1");
            sqlx::query("INSERT INTO t (id, name, amt, ok) VALUES (2,'bob',2.5,0)")
                .execute(&pool)
                .await
                .expect("insert 2");
            sqlx::query("INSERT INTO t (id, name, amt, ok) VALUES (3,NULL,3.5,1)")
                .execute(&pool)
                .await
                .expect("insert 3 (null name)");
            pool.close().await;
        });
    }

    fn fixture() -> (tempfile::TempDir, Source) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("fixture.db");
        make_db(&db);
        let src = Source::with_format(table_uri(&db, "t"), Format::Database);
        (dir, src)
    }

    #[test]
    fn list_tables_returns_user_tables() {
        let (_dir, src) = fixture();
        let tables = list_tables(&source_uri(&src)).expect("list tables");
        assert_eq!(tables, vec!["t".to_string()]);
    }

    #[test]
    fn schema_has_four_columns() {
        let (_dir, src) = fixture();
        let schema = DatabaseEngine::new().schema(&src).expect("schema");
        assert_eq!(schema.columns.len(), 4, "schema: {schema:?}");
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "name", "amt", "ok"]);
    }

    #[test]
    fn preview_and_scan_return_all_rows() {
        let (_dir, src) = fixture();
        let eng = DatabaseEngine::new();

        let rb = eng.preview(&src, 100).expect("preview");
        assert_eq!(rb.num_rows(), 3);

        let spec = ScanSpec {
            limit: 100,
            ..ScanSpec::default()
        };
        let res = eng.scan(&src, &spec).expect("scan");
        assert_eq!(res.batch.num_rows(), 3);
        assert_eq!(res.matched_rows, 3);
        assert!(res.total_known);
    }

    #[test]
    fn contains_filter_on_numeric_column_runs() {
        let (_dir, src) = fixture();
        // "contains 2" over amt {1.5, 2.5, 3.5} matches only "2.5" once (CAST amt AS TEXT LIKE).
        let spec = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "amt".into(),
                op: FilterOp::Contains,
                value: "2".into(),
            }],
            ..ScanSpec::default()
        };
        let res = DatabaseEngine::new()
            .scan(&src, &spec)
            .expect("filtered scan");
        assert_eq!(res.matched_rows, 1);
        assert_eq!(res.batch.num_rows(), 1);
    }

    /// A numeric column filtered with a number compares **numerically**, not lexicographically.
    /// `amt > 10` over {1.5, 2.5, 3.5} must match zero rows — a text comparison would match `2.5`
    /// and `3.5` (since `'2.5' > '10'` as strings), so this pins the numeric-binding path.
    #[test]
    fn numeric_filters_compare_as_numbers() {
        let (_dir, src) = fixture();
        let eng = DatabaseEngine::new();
        let scan = |op, value: &str| {
            let spec = ScanSpec {
                limit: 100,
                filters: vec![FilterSpec {
                    column: "amt".into(),
                    op,
                    value: value.into(),
                }],
                ..ScanSpec::default()
            };
            eng.scan(&src, &spec).expect("numeric scan").matched_rows
        };
        assert_eq!(
            scan(FilterOp::Gt, "10"),
            0,
            "amt > 10 is numeric, matches none"
        );
        assert_eq!(scan(FilterOp::Gt, "2"), 2, "amt > 2 → 2.5, 3.5");
        assert_eq!(scan(FilterOp::Ge, "2.5"), 2, "amt >= 2.5 → 2.5, 3.5");
        assert_eq!(scan(FilterOp::Lt, "2"), 1, "amt < 2 → 1.5");

        // Eq on an integer column with a numeric value matches numerically.
        let by_id = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "id".into(),
                op: FilterOp::Eq,
                value: "2".into(),
            }],
            ..ScanSpec::default()
        };
        assert_eq!(eng.scan(&src, &by_id).expect("id scan").matched_rows, 1);

        // A non-numeric value against a numeric column falls back to text compare — no error, no
        // match (matching the in-memory engine, which also drops to text here).
        let nonnum = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "id".into(),
                op: FilterOp::Eq,
                value: "abc".into(),
            }],
            ..ScanSpec::default()
        };
        assert_eq!(
            eng.scan(&src, &nonnum).expect("nonnum scan").matched_rows,
            0
        );
    }

    /// An integer column binds an **exact** `i64`, never an `f64`. `9007199254740993` (2^53 + 1)
    /// is not representable in `f64` — it rounds to `9007199254740992` — so an `f64` bind would
    /// look for the wrong value and match zero rows. The exact `i64` bind finds the row.
    #[test]
    fn integer_filter_binds_exact_i64_above_2_pow_53() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("big.db");
        let url = conn_url(&db);
        runtime().block_on(async {
            let opts = SqliteConnectOptions::from_str(&url)
                .expect("parse url")
                .create_if_missing(true);
            let pool = SqlitePool::connect_with(opts).await.expect("open");
            sqlx::query("CREATE TABLE t(id INTEGER)")
                .execute(&pool)
                .await
                .expect("create");
            // A single row whose id is exactly 2^53 + 1.
            sqlx::query("INSERT INTO t(id) VALUES (9007199254740993)")
                .execute(&pool)
                .await
                .expect("insert");
            pool.close().await;
        });
        let src = Source::with_format(table_uri(&db, "t"), Format::Database);
        let spec = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "id".into(),
                op: FilterOp::Eq,
                value: "9007199254740993".into(),
            }],
            ..ScanSpec::default()
        };
        let res = DatabaseEngine::new()
            .scan(&src, &spec)
            .expect("big-int scan");
        assert_eq!(
            res.matched_rows, 1,
            "exact i64 must find 2^53+1; an f64 bind would round to 2^53 and miss it"
        );
    }

    #[test]
    fn query_counts_rows() {
        let (_dir, src) = fixture();
        let rb = DatabaseEngine::new()
            .query(
                "SELECT count(*) AS c FROM t",
                &[NamedSource {
                    name: "t".into(),
                    source: src.clone(),
                }],
            )
            .expect("count query");
        assert_eq!(rb.num_rows(), 1);
    }

    #[test]
    fn rejects_writes() {
        assert!(ensure_read_only("SELECT 1").is_ok());
        assert!(ensure_read_only("WITH a AS (SELECT 1) SELECT * FROM a").is_ok());
        assert!(ensure_read_only("EXPLAIN SELECT * FROM t").is_ok());
        // Boundary-matching must not false-positive on columns whose names embed a verb.
        assert!(
            ensure_read_only("SELECT id, updated_at, is_deleted FROM t WHERE created_at > 0")
                .is_ok(),
            "column names embedding a write verb must not trip the guard"
        );
        // A verb that is also a scalar function (`REPLACE(...)`) called in a read is allowed; its
        // whitespace-delimited write form (`REPLACE INTO`, `MERGE INTO`) is still rejected below.
        assert!(
            ensure_read_only("SELECT REPLACE(name, 'a', 'b') FROM t").is_ok(),
            "REPLACE() as a function call must be allowed"
        );
        for bad in [
            "INSERT INTO t VALUES (1)",
            "DROP TABLE t",
            "UPDATE t SET id = 1",
            "SELECT 1; DROP TABLE t",
            // The two vectors an innocent head keyword hides — both execute on Postgres.
            "EXPLAIN ANALYZE DELETE FROM t",
            "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
            "WITH x AS (UPDATE t SET id = 1 RETURNING *) SELECT * FROM x",
            "EXPLAIN (ANALYZE) INSERT INTO t VALUES (1)",
            // MySQL's one way a SELECT writes.
            "SELECT * FROM t INTO OUTFILE '/tmp/x'",
            // The write forms of the function-name verbs — whitespace after the verb, so still caught.
            "REPLACE INTO t VALUES (1)",
            "MERGE INTO t USING s ON t.id = s.id",
        ] {
            assert!(ensure_read_only(bad).is_err(), "should reject: {bad}");
        }
    }

    /// A filter value can never break out of its literal to inject SQL, because values are **bound**,
    /// not interpolated. A classic `' OR '1'='1` payload matches *literally* (zero rows named that)
    /// instead of turning the predicate into a tautology (which would return every row).
    #[test]
    fn filter_values_are_bound_not_injected() {
        let (_dir, src) = fixture();
        let eng = DatabaseEngine::new();

        // If this were interpolated, the WHERE would become `name = 'alice' OR '1'='1'` and match
        // all 3 rows. Bound, it looks for a row literally named `alice' OR '1'='1` — there is none.
        let inject = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "name".into(),
                op: FilterOp::Eq,
                value: "alice' OR '1'='1".into(),
            }],
            ..ScanSpec::default()
        };
        let res = eng
            .scan(&src, &inject)
            .expect("scan with injection payload");
        assert_eq!(
            res.matched_rows, 0,
            "injection payload must match literally"
        );
        assert_eq!(res.batch.num_rows(), 0);

        // A backslash-bearing value (the MySQL-specific escape hazard) is likewise inert and errors
        // on neither engine — bound, it is just text.
        let backslash = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "name".into(),
                op: FilterOp::Eq,
                value: "a\\' OR 1=1 -- ".into(),
            }],
            ..ScanSpec::default()
        };
        let res = eng
            .scan(&src, &backslash)
            .expect("scan with backslash payload");
        assert_eq!(res.matched_rows, 0);

        // Binding still matches a real value exactly — the fix does not over-escape.
        let exact = ScanSpec {
            limit: 100,
            filters: vec![FilterSpec {
                column: "name".into(),
                op: FilterOp::Eq,
                value: "alice".into(),
            }],
            ..ScanSpec::default()
        };
        let res = eng.scan(&src, &exact).expect("scan exact");
        assert_eq!(res.matched_rows, 1);
        assert_eq!(res.batch.num_rows(), 1);
    }

    /// When the Postgres driver is **not** compiled in, a `postgres://` URI is rejected with a
    /// clear "rebuild with --features …" error rather than a confusing failure. (Skipped when
    /// `postgres` is also built, where the URI parses fine.)
    #[cfg(not(feature = "postgres"))]
    #[test]
    fn postgres_uri_is_not_built() {
        let err = parse_db_uri("postgres://localhost/db?table=t").unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "got: {err}");
    }
}
