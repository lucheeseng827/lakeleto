//! The `lakeleto` command surface — thin glue over the [`Engine`](crate::engine::Engine) trait.
//!
//! Every subcommand picks a `Box<dyn Engine>` and calls the trait. That indirection is the
//! whole point: the same commands work against the local reader, the DataFusion engine, or
//! a remote Lakeleto Cloud endpoint — and the future UI reuses this exact selection logic.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::engine::local::LocalReaderEngine;
use crate::engine::{Engine, NamedSource};
use crate::error::{EngineError, Result};
use crate::render::{self, Output};
use crate::source::Source;

/// Lakeleto — instant local Parquet/Iceberg/CSV explorer ("the Postman of lakehouse tables").
///
/// This MVP reads Parquet and CSV locally (and Iceberg tables with `--features iceberg`); a
/// DuckDB backend and a hosted "Lakeleto Cloud" engine are on the roadmap (see `lakeleto engines`).
#[derive(Parser, Debug)]
#[command(name = "lakeleto", version, about)]
pub struct Cli {
    /// Output format for results.
    #[arg(short = 'o', long, global = true, value_enum, default_value_t = Output::Table)]
    pub output: Output,

    /// Which engine reads the data.
    #[arg(long, global = true, value_enum, default_value_t = EngineChoice::Auto)]
    pub engine: EngineChoice,

    /// Lakeleto Cloud endpoint (implies `--engine remote` when set). Env: LAKELETO_REMOTE_URL.
    #[arg(long, global = true, env = "LAKELETO_REMOTE_URL")]
    pub remote_url: Option<String>,

    /// Bearer token for Lakeleto Cloud. Env: LAKELETO_REMOTE_TOKEN.
    #[arg(
        long,
        global = true,
        env = "LAKELETO_REMOTE_TOKEN",
        hide_env_values = true
    )]
    pub remote_token: Option<String>,

    #[command(subcommand)]
    pub cmd: Cmd,
}

/// Engine selection. `auto` = local, unless `--remote-url` is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EngineChoice {
    Auto,
    Local,
    Sql,
    Remote,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Print the schema (columns, types, nullability, row count).
    Schema { path: PathBuf },

    /// Preview the first N rows.
    Head {
        path: PathBuf,
        #[arg(short = 'n', long, default_value_t = 10)]
        rows: usize,
    },

    /// Profile columns (null %, distinct, min/max, samples) from a bounded scan.
    Profile {
        path: PathBuf,
        /// Max rows to scan for the profile.
        #[arg(long, default_value_t = 10_000)]
        scan: usize,
        /// Near-instant profile from the Parquet footer statistics — no row scan (exact
        /// nulls/min/max over the whole file; distinct + samples aren't computed).
        #[arg(long)]
        fast: bool,
    },

    /// Quick source info (format, engine, row count, file size).
    Info { path: PathBuf },

    /// List the engine backends compiled into this binary and their capabilities.
    Engines,

    /// Run SQL over one or more tables (needs `--features sql`, or `--engine remote`).
    Query {
        /// The SQL to run, e.g. "SELECT city, count(*) FROM t GROUP BY city".
        sql: String,
        /// Register a table: `--table name=path` (repeatable).
        #[arg(long = "table", value_name = "NAME=PATH")]
        tables: Vec<String>,
        /// Shorthand for a single table registered as `t`.
        #[arg(long)]
        file: Option<PathBuf>,
    },

    /// Serve the HTTP/JSON API + embedded SPA (the backend the UI + Lakeleto Cloud speak). Needs `--features serve`.
    #[cfg(feature = "serve")]
    Serve {
        /// Address to bind.
        #[arg(long, default_value = "127.0.0.1:8080", env = "LAKELETO_ADDR")]
        addr: String,
        /// Default row cap for `/v1/profile` when the request omits `scan`.
        #[arg(long, default_value_t = 10_000)]
        default_scan: usize,
        /// Require this bearer token on `/v1/*` (else the API is open). Env: LAKELETO_TOKEN.
        #[arg(long, env = "LAKELETO_TOKEN", hide_env_values = true)]
        token: Option<String>,
        /// Confine `/v1/*` file access to this directory (reject reads/browse outside it). Off by
        /// default — set it when exposing the API beyond your own machine.
        #[arg(long)]
        root: Option<PathBuf>,
        /// Sync workspaces to a remote `/v1/workspaces/*` endpoint (another server, or the hosted
        /// cloud plane) instead of the local store. Needs `--features remote`.
        #[arg(long, env = "LAKELETO_WORKSPACE_REMOTE")]
        workspace_remote: Option<String>,
        /// Bearer token for `--workspace-remote`.
        #[arg(long, env = "LAKELETO_WORKSPACE_REMOTE_TOKEN", hide_env_values = true)]
        workspace_remote_token: Option<String>,
    },

    /// Open a file in the embedded SPA: start the server and launch a browser tab. Needs `--features serve`.
    #[cfg(feature = "serve")]
    Open {
        /// File to open (Parquet/CSV) — deep-linked into the UI via `?path=`.
        path: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8080", env = "LAKELETO_ADDR")]
        addr: String,
        #[arg(long, default_value_t = 10_000)]
        default_scan: usize,
        /// Require this bearer token on `/v1/*`. Env: LAKELETO_TOKEN.
        #[arg(long, env = "LAKELETO_TOKEN", hide_env_values = true)]
        token: Option<String>,
        /// Confine `/v1/*` file access to this directory (reject reads/browse outside it).
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

/// Run the CLI. Returns a process exit code.
pub fn run(cli: Cli) -> Result<i32> {
    match &cli.cmd {
        Cmd::Schema { path } => {
            let source = Source::detect(path)?;
            let engine = engine_for(&cli)?;
            let schema = engine.schema(&source)?;
            print!("{}", render::schema(&schema, cli.output)?);
        }
        Cmd::Head { path, rows } => {
            let source = Source::detect(path)?;
            let engine = engine_for(&cli)?;
            let batch = engine.preview(&source, *rows)?;
            print!("{}", render::rows(&batch, cli.output)?);
        }
        Cmd::Profile { path, scan, fast } => {
            let source = Source::detect(path)?;
            let engine = engine_for(&cli)?;
            // `--fast` selects the footer-statistics path (scan_limit 0).
            let scan_limit = if *fast { 0 } else { *scan };
            let prof = engine.profile(&source, scan_limit)?;
            print!("{}", render::profile(&prof, cli.output)?);
        }
        Cmd::Info { path } => {
            let source = Source::detect(path)?;
            let engine = engine_for(&cli)?;
            let schema = engine.schema(&source)?;
            let size = std::fs::metadata(&source.path).map(|m| m.len()).ok();
            println!("path   : {}", source.display());
            println!("format : {}", source.format);
            println!("engine : {}", engine.name());
            println!(
                "size   : {}",
                size.map(human_bytes).unwrap_or_else(|| "?".to_string())
            );
            println!(
                "rows   : {}",
                schema
                    .row_count
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            );
            println!("columns: {}", schema.columns.len());
        }
        Cmd::Engines => {
            print!("{}", render_engines());
        }
        Cmd::Query { sql, tables, file } => {
            let engine = query_engine(&cli)?;
            let named = build_named_sources(tables, file)?;
            let batch = engine.query(sql, &named)?;
            print!("{}", render::rows(&batch, cli.output)?);
        }
        #[cfg(feature = "serve")]
        Cmd::Serve {
            addr,
            default_scan,
            token,
            root,
            workspace_remote,
            workspace_remote_token,
        } => {
            let read: std::sync::Arc<dyn Engine> =
                std::sync::Arc::new(LocalReaderEngine::default());
            let store = remote_store(workspace_remote, workspace_remote_token)?;
            crate::api::serve(
                addr,
                read,
                sql_engine_arc(),
                db_engine_arc(),
                *default_scan,
                None,
                token.clone(),
                canon_root(root)?,
                store,
            )?;
        }
        #[cfg(feature = "serve")]
        Cmd::Open {
            path,
            addr,
            default_scan,
            token,
            root,
        } => {
            let read: std::sync::Arc<dyn Engine> =
                std::sync::Arc::new(LocalReaderEngine::default());
            // Absolutize so the server resolves the file regardless of its own working dir.
            let abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.clone());
            let url = format!(
                "http://{addr}/?path={}",
                crate::api::encode_query(&abs.to_string_lossy())
            );
            crate::api::serve(
                addr,
                read,
                sql_engine_arc(),
                db_engine_arc(),
                *default_scan,
                Some(url),
                token.clone(),
                canon_root(root)?,
                None,
            )?;
        }
    }
    Ok(0)
}

/// Canonicalize the optional `--root` confinement dir — it must exist and be a directory, so a
/// typo fails fast at startup rather than silently confining nothing.
#[cfg(feature = "serve")]
fn canon_root(root: &Option<PathBuf>) -> Result<Option<PathBuf>> {
    let Some(r) = root else { return Ok(None) };
    let canon = std::fs::canonicalize(r)
        .map_err(|e| EngineError::Other(format!("--root {}: {e}", r.display())))?;
    if !canon.is_dir() {
        return Err(EngineError::Other(format!(
            "--root {} is not a directory",
            r.display()
        )));
    }
    Ok(Some(canon))
}

/// Build the `--workspace-remote` store override: a [`RemoteStore`](crate::workspace_remote)
/// syncing the workbench to another `/v1/workspaces/*` server. `None` (the default) keeps the
/// local on-disk store; without `--features remote` the flag is a clear build-feature error.
#[cfg(feature = "serve")]
fn remote_store(
    url: &Option<String>,
    token: &Option<String>,
) -> Result<Option<std::sync::Arc<dyn crate::workspace::WorkspaceStore>>> {
    let Some(url) = url else { return Ok(None) };
    #[cfg(feature = "remote")]
    {
        Ok(Some(std::sync::Arc::new(
            crate::workspace_remote::RemoteStore::new(url.clone(), token.clone()),
        )))
    }
    #[cfg(not(feature = "remote"))]
    {
        let _ = (url, token);
        Err(EngineError::missing_feature(
            "sync workspaces to a remote store",
            "remote",
        ))
    }
}

/// The SQL engine for `/v1/query`, as a shared handle — `None` unless built with `sql`.
#[cfg(all(feature = "serve", feature = "sql"))]
fn sql_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    Some(std::sync::Arc::new(
        crate::engine::sql::DataFusionEngine::new(),
    ))
}

#[cfg(all(feature = "serve", not(feature = "sql")))]
fn sql_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    None
}

/// The BYO-database engine (sqlx) for `Format::Database` sources — `None` unless built with a DB
/// backend feature (`sqlite`/`postgres`/`mysql`); the engine dispatches per dialect. Gate matches
/// `engine::database` and `engine::mod`.
#[cfg(all(
    feature = "serve",
    any(feature = "sqlite", feature = "postgres", feature = "mysql")
))]
fn db_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    Some(std::sync::Arc::new(
        crate::engine::database::DatabaseEngine::new(),
    ))
}

#[cfg(all(
    feature = "serve",
    not(any(feature = "sqlite", feature = "postgres", feature = "mysql"))
))]
fn db_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    None
}

// ---- engine selection -----------------------------------------------------------------

/// Engine for read commands (schema/head/profile/info).
fn engine_for(cli: &Cli) -> Result<Box<dyn Engine>> {
    match cli.engine {
        EngineChoice::Local => Ok(Box::new(LocalReaderEngine::default())),
        EngineChoice::Sql => make_sql(),
        EngineChoice::Remote => make_remote(cli),
        EngineChoice::Auto => {
            if cli.remote_url.is_some() {
                make_remote(cli)
            } else {
                Ok(Box::new(LocalReaderEngine::default()))
            }
        }
    }
}

/// Engine for `query` — auto prefers remote (if a URL is set) then the SQL engine.
fn query_engine(cli: &Cli) -> Result<Box<dyn Engine>> {
    match cli.engine {
        EngineChoice::Remote => make_remote(cli),
        EngineChoice::Sql => make_sql(),
        EngineChoice::Local => Err(EngineError::UnsupportedOperation {
            engine: "local".to_string(),
            op: "run SQL".to_string(),
            hint: "the local reader has no SQL planner — use `--engine sql` \
                   (build with `--features sql`) or `--engine remote`"
                .to_string(),
        }),
        EngineChoice::Auto => {
            if cli.remote_url.is_some() {
                make_remote(cli)
            } else {
                make_sql()
            }
        }
    }
}

#[cfg(feature = "sql")]
fn make_sql() -> Result<Box<dyn Engine>> {
    Ok(Box::new(crate::engine::sql::DataFusionEngine::new()))
}

#[cfg(not(feature = "sql"))]
fn make_sql() -> Result<Box<dyn Engine>> {
    Err(EngineError::missing_feature("run SQL", "sql"))
}

#[cfg(feature = "remote")]
fn make_remote(cli: &Cli) -> Result<Box<dyn Engine>> {
    let url = cli.remote_url.clone().ok_or_else(|| {
        EngineError::Remote(
            "no endpoint — pass `--remote-url https://...` or set LAKELETO_REMOTE_URL".to_string(),
        )
    })?;
    Ok(Box::new(crate::engine::remote::RemoteEngine::new(
        url,
        cli.remote_token.clone(),
    )))
}

#[cfg(not(feature = "remote"))]
fn make_remote(_cli: &Cli) -> Result<Box<dyn Engine>> {
    Err(EngineError::missing_feature(
        "use the remote engine",
        "remote",
    ))
}

// ---- helpers --------------------------------------------------------------------------

fn build_named_sources(tables: &[String], file: &Option<PathBuf>) -> Result<Vec<NamedSource>> {
    let mut out = Vec::new();
    if let Some(path) = file {
        out.push(NamedSource {
            name: "t".to_string(),
            source: Source::detect(path)?,
        });
    }
    for spec in tables {
        let (name, path) = spec.split_once('=').ok_or_else(|| {
            EngineError::Other(format!("bad --table `{spec}` (expected name=path)"))
        })?;
        out.push(NamedSource {
            name: name.to_string(),
            source: Source::detect(path)?,
        });
    }
    if out.is_empty() {
        return Err(EngineError::Other(
            "no tables — pass `--file path` or `--table name=path`".to_string(),
        ));
    }
    Ok(out)
}

/// State of an engine backend, as shown by `lakeleto engines`.
enum CapState {
    /// Compiled in and functional.
    On,
    /// A real engine, gated behind a cargo feature that isn't enabled in this binary.
    Off,
}

fn render_engines() -> String {
    let mut out = String::from("Lakeleto engine backends:\n\n");

    out.push_str(&cap_line(
        "local (arrow/parquet/csv)",
        "parquet, csv",
        CapState::On,
    ));
    out.push_str(&cap_line(
        "sql (DataFusion)",
        "parquet, csv + read-only SQL",
        if cfg!(feature = "sql") {
            CapState::On
        } else {
            CapState::Off
        },
    ));
    out.push_str(&cap_line(
        "remote (Lakeleto Cloud) [optional]",
        "schema/profile today; row streaming = Phase 4",
        if cfg!(feature = "remote") {
            CapState::On
        } else {
            CapState::Off
        },
    ));
    out.push_str(&cap_line(
        "iceberg (reader)",
        "iceberg tables (current-snapshot Parquet)",
        if cfg!(feature = "iceberg") {
            CapState::On
        } else {
            CapState::Off
        },
    ));
    out.push_str(&cap_line(
        "object-store (s3/gs/az)",
        "remote parquet/csv via your own creds",
        if cfg!(feature = "object-store") {
            CapState::On
        } else {
            CapState::Off
        },
    ));
    out.push_str(
        "\nLegend: ✓ available · · not compiled (rebuild with the named --features).\n\
         The UI binds to the `Engine` trait, so every backend is interchangeable.\n",
    );
    out
}

fn cap_line(name: &str, formats: &str, state: CapState) -> String {
    let (mark, note) = match state {
        CapState::On => ("✓", ""),
        CapState::Off => ("·", " (not compiled)"),
    };
    format!("  {mark} {name:<32} reads: {formats}{note}\n")
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}
