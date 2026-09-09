//! The `lakeleto` command surface — thin glue over the [`Engine`](crate::engine::Engine) trait.
//!
//! Every subcommand picks a `Box<dyn Engine>` and calls the trait. That indirection is the
//! whole point: the same commands work against the local reader, the DataFusion engine, or
//! a remote Lakeleto Cloud endpoint — and the future UI reuses this exact selection logic.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};

use crate::engine::local::LocalReaderEngine;
use crate::engine::{Engine, NamedSource};
use crate::error::{EngineError, Result};
use crate::render::{self, Output};
use crate::source::Source;

/// Rows a profile scans when nobody says otherwise — the `--scan` default, the
/// `serve --default-scan` default, and what the desktop launcher serves with.
/// Shared so those three cannot drift into disagreeing about what "default"
/// means for the same computation.
pub const DEFAULT_SCAN: usize = 10_000;

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

    /// Lakeleto server endpoint — any server speaking the `/v1/*` contract (implies
    /// `--engine remote` when set). The path is then the SERVER's to resolve, not this
    /// machine's. Env: LAKELETO_REMOTE_URL.
    #[arg(long, global = true, env = "LAKELETO_REMOTE_URL")]
    pub remote_url: Option<String>,

    /// Bearer token for the remote Lakeleto server. Env: LAKELETO_REMOTE_TOKEN.
    #[arg(
        long,
        global = true,
        env = "LAKELETO_REMOTE_TOKEN",
        hide_env_values = true
    )]
    pub remote_token: Option<String>,

    /// Read the source as this format instead of inferring it
    /// (parquet/csv/tsv/json/iceberg/delta/database).
    ///
    /// Locally this is the same override `?format=` is on the API. Against `--remote-url` it is
    /// the only way to name a format at all: the ref is not resolved on this machine, so with
    /// no `--format` the server infers it (see `Source::unresolved`).
    #[arg(long, global = true, value_name = "FORMAT")]
    pub format: Option<String>,

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
        #[arg(long, default_value_t = DEFAULT_SCAN)]
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
        #[arg(long, default_value_t = DEFAULT_SCAN)]
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
        /// Keep workspaces, history and cached results under this directory instead of
        /// `$LAKELETO_HOME`. Lets two servers run side by side without sharing a store — and
        /// gives the tray launcher and `serve` a way not to collide over one home.
        #[arg(long, env = "LAKELETO_WORKSPACE_HOME")]
        workspace_home: Option<PathBuf>,
    },

    /// Open a file in the embedded SPA: start the server and launch a browser tab. Needs `--features serve`.
    #[cfg(feature = "serve")]
    Open {
        /// File to open (Parquet/CSV) — deep-linked into the UI via `?path=`.
        path: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8080", env = "LAKELETO_ADDR")]
        addr: String,
        #[arg(long, default_value_t = DEFAULT_SCAN)]
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
            let source = source_for(&cli, path)?;
            let engine = engine_for(&cli)?;
            let schema = engine.schema(&source)?;
            print!("{}", render::schema(&schema, cli.output)?);
        }
        Cmd::Head { path, rows } => {
            let source = source_for(&cli, path)?;
            let engine = engine_for(&cli)?;
            let batch = engine.preview(&source, *rows)?;
            print!("{}", render::rows(&batch, cli.output)?);
        }
        Cmd::Profile { path, scan, fast } => {
            let source = source_for(&cli, path)?;
            let engine = engine_for(&cli)?;
            // `--fast` selects the footer-statistics path (scan_limit 0).
            let scan_limit = if *fast { 0 } else { *scan };
            let prof = engine.profile(&source, scan_limit)?;
            print!("{}", render::profile(&prof, cli.output)?);
        }
        Cmd::Info { path } => {
            let source = source_for(&cli, path)?;
            let engine = engine_for(&cli)?;
            let schema = engine.schema(&source)?;
            let size = std::fs::metadata(&source.path).map(|m| m.len()).ok();
            println!("path   : {}", source.display());
            // An unresolved source has no format *here* — the server resolved the ref and read
            // the bytes. Printing the placeholder's name ("unknown") would read like a failed
            // detection rather than a deliberate absence.
            if source.is_unresolved() {
                println!("format : (resolved by the server)");
            } else {
                println!("format : {}", source.format);
            }
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
            let named = build_named_sources(&cli, tables, file)?;
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
            workspace_home,
        } => {
            let read: std::sync::Arc<dyn Engine> =
                std::sync::Arc::new(LocalReaderEngine::default());
            let store = workspace_store(workspace_remote, workspace_remote_token, workspace_home)?;
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

/// Pick the workspace store from the flags: a remote sync target, an explicit local directory, or
/// (the default) the on-disk store under `$LAKELETO_HOME` that `api::serve` builds for itself.
///
/// `--workspace-remote` and `--workspace-home` are mutually exclusive rather than layered: one
/// says "the store is over there", the other says "the store is in this directory", and silently
/// letting one win would put a user's workspaces somewhere they did not ask for.
#[cfg(feature = "serve")]
fn workspace_store(
    remote_url: &Option<String>,
    remote_token: &Option<String>,
    home: &Option<PathBuf>,
) -> Result<Option<std::sync::Arc<dyn crate::workspace::WorkspaceStore>>> {
    if remote_url.is_some() && home.is_some() {
        return Err(EngineError::Other(
            "--workspace-remote and --workspace-home both set: the store is either remote or in \
             a local directory, not both"
                .to_string(),
        ));
    }
    if let Some(dir) = home {
        return Ok(Some(std::sync::Arc::new(crate::workspace::LocalStore::at(
            dir,
        )?)));
    }
    remote_store(remote_url, remote_token)
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
pub(crate) fn sql_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    Some(std::sync::Arc::new(
        crate::engine::sql::DataFusionEngine::new(),
    ))
}

#[cfg(all(feature = "serve", not(feature = "sql")))]
pub(crate) fn sql_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    None
}

/// The BYO-database engine (sqlx) for `Format::Database` sources — `None` unless built with a DB
/// backend feature (`sqlite`/`postgres`/`mysql`); the engine dispatches per dialect. Gate matches
/// `engine::database` and `engine::mod`.
#[cfg(all(
    feature = "serve",
    any(feature = "sqlite", feature = "postgres", feature = "mysql")
))]
pub(crate) fn db_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    Some(std::sync::Arc::new(
        crate::engine::database::DatabaseEngine::new(),
    ))
}

#[cfg(all(
    feature = "serve",
    not(any(feature = "sqlite", feature = "postgres", feature = "mysql"))
))]
pub(crate) fn db_engine_arc() -> Option<std::sync::Arc<dyn Engine>> {
    None
}

// ---- engine selection -----------------------------------------------------------------

/// Will this invocation read through the `remote` engine?
///
/// Mirrors the `Auto` rule in [`engine_for`] and [`query_engine`] — which agree with each other
/// — so the source-resolution decision below cannot drift from the engine actually selected.
/// It is deliberately *not* `cli.remote_url.is_some()`: `--engine local --remote-url …` reads
/// locally, and handing that a source nothing resolved would be a confusing failure.
fn uses_remote(cli: &Cli) -> bool {
    match cli.engine {
        EngineChoice::Remote => true,
        EngineChoice::Auto => cli.remote_url.is_some(),
        EngineChoice::Local | EngineChoice::Sql => false,
    }
}

/// Turn a command-line path into the [`Source`] the selected engine should be handed.
///
/// This is the whole of the local-vs-remote resolution rule, in one place because every command
/// needs the same answer. **A remote engine resolves its own sources.** [`Source::detect`] is
/// local — it stats the path, walks directories, sniffs magic bytes — so running it for a ref
/// that only the *server* can interpret resolves it against the wrong machine: the command fails
/// on the laptop and never reaches the network. When the read is going out over HTTP the string
/// is therefore passed through opaquely (honouring an explicit `--format`, and otherwise leaving
/// the format for the server to infer) and the peer decides what it names.
///
/// Nothing here knows about any particular scheme or product: *any* opaque string a server
/// understands travels this way, and a plain path that happens to exist on both machines is
/// simply one of them.
fn source_for(cli: &Cli, path: &Path) -> Result<Source> {
    if uses_remote(cli) {
        Source::unresolved(path, cli.format.as_deref())
    } else {
        Source::resolve(path, cli.format.as_deref())
    }
}

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

fn build_named_sources(
    cli: &Cli,
    tables: &[String],
    file: &Option<PathBuf>,
) -> Result<Vec<NamedSource>> {
    let mut out = Vec::new();
    if let Some(path) = file {
        out.push(NamedSource {
            name: "t".to_string(),
            source: source_for(cli, path)?,
        });
    }
    for spec in tables {
        let (name, path) = spec.split_once('=').ok_or_else(|| {
            EngineError::Other(format!("bad --table `{spec}` (expected name=path)"))
        })?;
        out.push(NamedSource {
            name: name.to_string(),
            source: source_for(cli, Path::new(path))?,
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
    /// A feature flag that exists but wires nothing — the door is held open, not walked through.
    /// Distinguished from [`CapState::Off`] because "rebuild with --features x" is bad advice for
    /// a flag that would still do nothing afterwards.
    Planned,
}

fn render_engines() -> String {
    let mut out = String::from("Lakeleto engine backends:\n\n");

    // The format lists come from the same helper the `Engine::capabilities` surface uses, so
    // this command and `GET /v1/engines` cannot disagree about what the build can read.
    let formats = crate::engine::readable_formats().join(", ");
    out.push_str(&cap_line(
        "local (arrow/parquet/csv)",
        &formats,
        CapState::On,
    ));
    out.push_str(&cap_line(
        "sql (DataFusion)",
        &format!("{formats} + read-only SQL"),
        feature_state(cfg!(feature = "sql")),
    ));
    // Named by the CONTRACT rather than by any one server, because that is what the engine
    // binds to: a `lakeleto serve` speaks all of it, and a hosted plane may speak part of it —
    // an unserved route just answers 404/501 with the server's own message. `scan` (the grid's
    // filter → sort → window) is unimplemented on this engine, hence the exclusion.
    out.push_str(&cap_line(
        "remote (any `/v1/*` server)",
        "schema/profile/preview/SQL over HTTP (rows as Arrow IPC); no grid windowing",
        feature_state(cfg!(feature = "remote")),
    ));
    out.push_str(&cap_line(
        "iceberg (reader)",
        "iceberg tables (current-snapshot Parquet)",
        feature_state(cfg!(feature = "iceberg")),
    ));
    out.push_str(&cap_line(
        "delta (reader)",
        "delta lake tables (JSON transaction log)",
        feature_state(cfg!(feature = "delta")),
    ));
    out.push_str(&cap_line(
        "object-store (s3/gs/az)",
        "remote parquet/csv via your own creds",
        feature_state(cfg!(feature = "object-store")),
    ));
    // The BYO-database connectors. Listed per backend rather than as one "database" row because
    // each is its own cargo feature and a lean build can carry any subset of them.
    out.push_str(&cap_line(
        "sqlite (read-only)",
        "sqlite files + connection URIs",
        feature_state(cfg!(feature = "sqlite")),
    ));
    out.push_str(&cap_line(
        "postgres (read-only)",
        "postgres:// connections",
        feature_state(cfg!(feature = "postgres")),
    ));
    out.push_str(&cap_line(
        "mysql (read-only)",
        "mysql:// connections",
        feature_state(cfg!(feature = "mysql")),
    ));
    // ROADMAP Phase 5 records this as deliberately not built, and states that `lakeleto engines`
    // shows it as "planned" — it did not, so `--features duckdb` compiled and silently wired
    // nothing. Saying so here is what the recorded decision already promised.
    out.push_str(&cap_line(
        "duckdb",
        "not built — the `sql` (DataFusion) engine covers this",
        CapState::Planned,
    ));
    out.push_str(
        "\nLegend: ✓ available · · not compiled (rebuild with the named --features) · ○ planned \
         (the feature flag exists but wires nothing).\n\
         The UI binds to the `Engine` trait, so every backend is interchangeable.\n",
    );
    out
}

/// `cfg!(feature = ...)` -> the state its row should render in.
fn feature_state(compiled: bool) -> CapState {
    if compiled {
        CapState::On
    } else {
        CapState::Off
    }
}

fn cap_line(name: &str, formats: &str, state: CapState) -> String {
    let (mark, note) = match state {
        CapState::On => ("✓", ""),
        CapState::Off => ("·", " (not compiled)"),
        CapState::Planned => ("○", " (planned — the feature flag is inert)"),
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
