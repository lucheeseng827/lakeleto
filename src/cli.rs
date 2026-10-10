//! The `lakeleto` command surface — thin glue over the [`Engine`](crate::engine::Engine) trait.
//!
//! Every subcommand picks a `Box<dyn Engine>` and calls the trait. That indirection is the
//! whole point: the same commands work against the local reader, the DataFusion engine, or
//! a remote Lakeleto Cloud endpoint — and the future UI reuses this exact selection logic.

use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};

use crate::context::RequestContext;
use crate::engine::local::LocalReaderEngine;
use crate::engine::registry::{EngineRegistry, Need};
use crate::engine::{Engine, NamedSource, RowStream};
use crate::error::{EngineError, Result};
use crate::render::{self, Output};
use crate::source::{Flatten, Source};

/// Rows a profile scans when nobody says otherwise — the `--scan` default, the
/// `serve --default-scan` default, and what the desktop launcher serves with.
/// Shared so those three cannot drift into disagreeing about what "default"
/// means for the same computation.
pub const DEFAULT_SCAN: usize = 10_000;

/// Lakeleto — instant local Parquet/Iceberg/CSV explorer ("the Postman of lakehouse tables").
///
/// It reads Parquet, CSV/TSV, JSON and Arrow IPC locally, and with features Iceberg and Delta
/// tables, object stores and databases; a hosted "Lakeleto Cloud" engine is on the roadmap (see
/// `lakeleto engines`).
#[derive(Parser, Debug)]
#[command(name = "lakeleto", version, about)]
pub struct Cli {
    /// Output format: table (the default), json, ndjson, csv or tsv, and for rows (`head`,
    /// `query`, `catalog ls`) also arrow (Arrow IPC file), arrows (Arrow IPC stream) or parquet
    /// (in builds with the `parquet-out` feature, as the release binaries are). With `--out` and
    /// no `-o`, the file's extension picks it.
    #[arg(short = 'o', long, global = true, value_enum)]
    pub output: Option<Output>,

    /// Write the output to this file instead of stdout (`-` is stdout). The file appears only when
    /// the command succeeds, renamed into place from a temporary file beside it, so a failed run
    /// leaves no half-written file and keeps the one that was there.
    #[arg(long, global = true, value_name = "FILE")]
    pub out: Option<PathBuf>,

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

    /// Read a JSON source's rows from this place inside the document instead of detecting it:
    /// a JSON Pointer (`/data`, `/response/items`) or a top-level member name (`data`). `""`
    /// reads the document as it is, unwrapping nothing.
    ///
    /// Without it, a single JSON object with exactly one member holding an array of objects is
    /// read from that member (`lakeleto schema` shows which). Travels to `--remote-url` as
    /// `?json_path=`.
    #[arg(long, global = true, value_name = "POINTER")]
    pub json_path: Option<String>,

    /// Read struct columns as one column per field, named by its path (`user.geo.lat`), so nested
    /// data sorts, filters and exports like any other column. `--flatten` spreads every level,
    /// `--flatten=1` only the first; lists and maps stay whole.
    ///
    /// Takes its value after `=` only, so `--flatten data.json` still reads `data.json`. Travels to
    /// `--remote-url` as `?flatten=`.
    #[arg(
        long,
        global = true,
        value_name = "LEVELS",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "all"
    )]
    pub flatten: Option<String>,

    /// The most bytes one read decompresses a compressed file (`t.csv.gz`, `t.ndjson.zst`) to,
    /// before it stops with an error: the guard against a small file that inflates without end.
    /// A number of bytes, or one with a suffix in powers of 1024: `512M`, `8GiB`. Env:
    /// LAKELETO_MAX_DECOMPRESSED.
    #[arg(
        long,
        global = true,
        value_name = "BYTES",
        env = "LAKELETO_MAX_DECOMPRESSED",
        default_value = "4GiB",
        value_parser = parse_bytes
    )]
    pub max_decompressed: u64,

    #[command(subcommand)]
    pub cmd: Cmd,
}

impl Cli {
    /// The output format: `-o` when given, else the one `--out`'s extension names, else `table`.
    pub fn output(&self) -> Output {
        self.output
            .or_else(|| self.out_file().and_then(Output::for_path))
            .unwrap_or(Output::Table)
    }

    /// `--out`'s file, or `None` for stdout (no `--out`, or `--out -`).
    fn out_file(&self) -> Option<&Path> {
        self.out.as_deref().filter(|p| p.as_os_str() != "-")
    }
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

    /// List the engine backends compiled into this binary and their capabilities. With
    /// `--remote-url`, also the server's: its version, the protocol it speaks, and its engines.
    Engines,

    /// Browse the Iceberg REST catalogs configured in `$LAKELETO_HOME/catalogs.toml` or
    /// `LAKELETO_CATALOG__…` variables. Needs `--features catalog`.
    #[cfg(feature = "catalog")]
    Catalog {
        #[command(subcommand)]
        cmd: CatalogCmd,
    },

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

    /// Let an AI agent read your tables over the Model Context Protocol, on stdin and stdout:
    /// the tools `list`, `describe`, `preview`, `profile`, `query` and `catalog_ls`, all
    /// read-only. An MCP client starts this command itself. Needs `--features mcp`.
    #[cfg(feature = "mcp")]
    Mcp {
        /// Read only under this directory: anything outside it, and every object-store,
        /// database and catalog reference, is refused. Relative paths are taken from it.
        #[arg(long)]
        root: Option<PathBuf>,
        /// Rows `profile` scans when the call doesn't say.
        #[arg(long, default_value_t = DEFAULT_SCAN)]
        default_scan: usize,
        /// The most rows `preview` and `query` return in one call.
        #[arg(long, default_value_t = 1_000,
              value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..))]
        max_rows: usize,
        /// The most bytes of JSON one call returns; rows past it are left out, and the result
        /// says so.
        #[arg(long, default_value_t = 32 * 1024,
              value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1024..))]
        max_bytes: usize,
        /// Seconds a call may run before it is answered with an error. Reading files stops
        /// there; a database query that has started runs on, and its result is dropped.
        #[arg(long, default_value_t = 30,
              value_parser = clap::builder::RangedU64ValueParser::<u64>::new().range(1..))]
        timeout: u64,
    },
}

/// `lakeleto catalog …`.
#[cfg(feature = "catalog")]
#[derive(Subcommand, Debug)]
pub enum CatalogCmd {
    /// List what is under a reference: the configured catalogs (with no reference, or
    /// `catalog://`), a catalog's namespaces (`catalog://prod/`), or a namespace's namespaces and
    /// tables (`catalog://prod/sales/`). Prints names, types and URIs, never a credential.
    Ls {
        /// What to list: `catalog://`, `catalog://<catalog>/` or `catalog://<catalog>/<namespace>/`.
        reference: Option<String>,
    },
}

/// Run the CLI. Returns a process exit code.
pub fn run(cli: Cli) -> Result<i32> {
    // A one-shot CLI invocation genuinely has nothing to say about the call: no deadline, no
    // canceller, no tenant. `detached()` names that rather than pretending otherwise — and it
    // is the context on which `RequestContext::check` short-circuits, so threading it through
    // the readers costs a branch here.
    //
    // The obvious upgrade is Ctrl-C cancelling the read instead of killing the process
    // mid-write. That needs a portable signal handler, which std does not have and which is
    // not worth a dependency on the lean default build; when `serve` is compiled in, its
    // tokio signal handling could supply one.
    let ctx = RequestContext::detached();
    crate::source::set_max_decompressed(cli.max_decompressed);
    let output = cli.output();
    check_output(
        &cli.cmd,
        output,
        cli.out.is_some(),
        cli.out_file().is_none() && std::io::stdout().is_terminal(),
    )?;
    // Everything a command prints goes through this, so `--out` captures it whole or not at all.
    let mut sink = Sink::open(cli.out_file())?;
    match &cli.cmd {
        Cmd::Schema { path } => {
            let source = source_for(&cli, path)?;
            let engine = engines_for(&cli)?.resolve(Need::Read(&source))?;
            let schema = engine.schema(&ctx, &source)?;
            write!(sink, "{}", render::schema(&schema, output)?)?;
        }
        Cmd::Head { path, rows } => {
            let source = source_for(&cli, path)?;
            let engine = engines_for(&cli)?.resolve(Need::Read(&source))?;
            let batch = engine.preview(&ctx, &source, *rows)?;
            // The streaming writer, over a batch already in hand: byte for byte what the buffered
            // renderer prints for the text formats, and the only writer of the binary ones.
            render::stream_rows(RowStream::from_batch(batch), output, &mut sink)?;
        }
        Cmd::Profile { path, scan, fast } => {
            let source = source_for(&cli, path)?;
            let engine = engines_for(&cli)?.resolve(Need::Read(&source))?;
            // `--fast` selects the footer-statistics path (scan_limit 0).
            let scan_limit = if *fast { 0 } else { *scan };
            let prof = engine.profile(&ctx, &source, scan_limit)?;
            write!(sink, "{}", render::profile(&prof, output)?)?;
        }
        Cmd::Info { path } => {
            let source = source_for(&cli, path)?;
            let engine = engines_for(&cli)?.resolve(Need::Read(&source))?;
            let schema = engine.schema(&ctx, &source)?;
            let info = render::SourceInfo {
                path: source.display(),
                // With `--remote-url` the server detects the format, so there is none here.
                format: (!source.is_unresolved()).then(|| source.format.to_string()),
                engine: engine.name().to_string(),
                size_bytes: std::fs::metadata(&source.path).map(|m| m.len()).ok(),
                row_count: schema.row_count,
                columns: schema.columns.len(),
                credentials: schema.credentials,
            };
            write!(sink, "{}", render::info(&info, output)?)?;
        }
        Cmd::Engines => {
            write!(sink, "{}", render_engines())?;
            if uses_remote(&cli) {
                write!(sink, "{}", render_server(&cli, &ctx)?)?;
            }
        }
        #[cfg(feature = "catalog")]
        Cmd::Catalog {
            cmd: CatalogCmd::Ls { reference },
        } => {
            let catalogs = crate::catalog::Catalogs::configured();
            let rows = catalog_ls(&catalogs, &ctx, reference.as_deref())?;
            render::stream_rows(RowStream::from_batch(rows), output, &mut sink)?;
        }
        Cmd::Query { sql, tables, file } => {
            if cli.engine == EngineChoice::Local {
                return Err(EngineError::UnsupportedOperation {
                    engine: "local".to_string(),
                    op: "run SQL".to_string(),
                    hint: "the local reader has no SQL planner — use `--engine sql` \
                           (build with `--features sql`) or `--engine remote`"
                        .to_string(),
                });
            }
            let named = build_named_sources(&cli, tables, file)?;
            let engine = engines_for(&cli)?.resolve(Need::Query(&named))?;
            // Streamed, and uncapped: on your own machine a `SELECT *` you typed is a `SELECT *`
            // you meant. With `--output csv|tsv|ndjson|json` the rows reach stdout as the engine
            // produces them, so memory is one batch rather than the whole answer and `| head`
            // starts printing immediately. `--output table` still collects — column widths are a
            // property of every row, so alignment cannot begin before the last one arrives.
            //
            // An engine that cannot produce incrementally takes the trait's default here and
            // behaves exactly as it did before.
            let stream = engine.query_stream(&ctx, sql, &named, None)?;
            render::stream_rows(stream, output, &mut sink)?;
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
            let store = workspace_store(workspace_remote, workspace_remote_token, workspace_home)?;
            crate::api::serve(
                addr,
                EngineRegistry::local(),
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
            // Absolutize so the server resolves the file regardless of its own working dir.
            let abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.clone());
            let url = format!(
                "http://{addr}/?path={}",
                crate::api::encode_query(&abs.to_string_lossy())
            );
            crate::api::serve(
                addr,
                EngineRegistry::local(),
                *default_scan,
                Some(url),
                token.clone(),
                canon_root(root)?,
                None,
            )?;
        }
        #[cfg(feature = "mcp")]
        Cmd::Mcp {
            root,
            default_scan,
            max_rows,
            max_bytes,
            timeout,
        } => {
            let limits = crate::mcp::Limits {
                default_scan: *default_scan,
                max_rows: *max_rows,
                max_bytes: *max_bytes,
                timeout: std::time::Duration::from_secs(*timeout),
            };
            let tools = crate::mcp::Tools::new(EngineRegistry::local(), canon_root(root)?, limits);
            crate::mcp::serve_stdio(tools)?;
        }
    }
    sink.finish()?;
    Ok(0)
}

/// Refuse an output a command cannot give, before anything is read or any file is created.
///
/// The binary formats are rows, so only the commands that print rows take them: a schema, a
/// profile or `info` is a description, which Parquet and Arrow have no shape for. They are also
/// never written to a terminal, where they are noise that can leave it in a strange state, and
/// where nobody asked for bytes: `--out`, a redirect and a pipe all say where they should go.
/// The servers print a log, not a result, and `mcp` speaks its protocol on stdout, so none of
/// them takes `--out`. And a build without `parquet-out` refuses `-o parquet` here, naming the
/// feature, rather than after the read.
fn check_output(cmd: &Cmd, output: Output, out: bool, stdout_is_terminal: bool) -> Result<()> {
    let refuse = |msg: String| Err(EngineError::Other(msg));
    let name = command_name(cmd);
    if out && matches!(name, "serve" | "open" | "mcp") {
        return refuse(format!(
            "`{name}` takes no `--out`: it serves until stopped and prints a log, not a result"
        ));
    }
    if output.is_binary() {
        if !matches!(name, "head" | "query" | "catalog") {
            return refuse(format!(
                "`-o {}` writes rows, from `head`, `query` or `catalog ls`; `{name}` prints a \
                 description, which it has no shape for: use `-o json`",
                output.name()
            ));
        }
        if output == Output::Parquet && !render::WRITES_PARQUET {
            return Err(render::no_parquet_writer());
        }
        if stdout_is_terminal {
            return refuse(format!(
                "`-o {}` is binary, and stdout is a terminal: write it to a file with \
                 `--out <file>`, or redirect it or pipe it to the program that reads it",
                output.name()
            ));
        }
    }
    Ok(())
}

/// A size in bytes: a number, or one with a suffix in powers of 1024 (`K`, `M`, `G`, `T`, each
/// with or without `i` and `B`, in any case): `4096`, `512M`, `8GiB`.
fn parse_bytes(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim();
    let digits = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (number, unit) = s.split_at(digits);
    let number: u64 = number.parse().map_err(|_| {
        format!("`{s}` is not a size: a number of bytes, as `4096`, `512M` or `8GiB`")
    })?;
    let shift = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 10,
        "m" | "mb" | "mib" => 20,
        "g" | "gb" | "gib" => 30,
        "t" | "tb" | "tib" => 40,
        _ => {
            return Err(format!(
                "`{s}` has an unknown unit: use K, M, G or T (powers of 1024)"
            ))
        }
    };
    number
        .checked_mul(1u64 << shift)
        .ok_or_else(|| format!("`{s}` is more bytes than can be counted"))
}

/// A different 64-bit value on every call, from the random keys std seeds a `HashMap` with: no
/// crate needed for a name that only has to differ from its neighbours.
fn random_suffix() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

/// The subcommand's name as it is typed, for messages.
fn command_name(cmd: &Cmd) -> &'static str {
    match cmd {
        Cmd::Schema { .. } => "schema",
        Cmd::Head { .. } => "head",
        Cmd::Profile { .. } => "profile",
        Cmd::Info { .. } => "info",
        Cmd::Engines => "engines",
        #[cfg(feature = "catalog")]
        Cmd::Catalog { .. } => "catalog",
        Cmd::Query { .. } => "query",
        #[cfg(feature = "serve")]
        Cmd::Serve { .. } => "serve",
        #[cfg(feature = "serve")]
        Cmd::Open { .. } => "open",
        #[cfg(feature = "mcp")]
        Cmd::Mcp { .. } => "mcp",
    }
}

/// Where a command's output goes: stdout, or `--out`'s file.
enum Sink {
    Stdout(std::io::BufWriter<std::io::Stdout>),
    File(OutFile),
}

impl Sink {
    /// Stdout when `file` is `None`, else a temporary file beside it.
    fn open(file: Option<&Path>) -> Result<Sink> {
        Ok(match file {
            None => Sink::Stdout(std::io::BufWriter::new(std::io::stdout())),
            Some(dest) => Sink::File(OutFile::create(dest)?),
        })
    }

    /// The command succeeded: flush stdout, or put the file in place.
    fn finish(self) -> Result<()> {
        match self {
            Sink::Stdout(mut w) => Ok(w.flush()?),
            Sink::File(f) => f.commit(),
        }
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Sink::Stdout(w) => w.write(buf),
            Sink::File(f) => f.writer().write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Sink::Stdout(w) => w.flush(),
            Sink::File(f) => f.writer().flush(),
        }
    }
}

/// `--out`'s file, written under a temporary name beside its destination and renamed over it only
/// when the command succeeds.
///
/// A failed run removes the temporary file and leaves the destination as it was, which matters
/// most for the binary formats: a Parquet file cut off before its footer is not a shorter file but
/// an unreadable one. Beside the destination because a rename is only atomic within one file
/// system. A run killed outright (Ctrl-C included, as the CLI has no signal handler) still leaves
/// the destination as it was, but the hidden temporary file stays behind.
///
/// On Unix the temporary file is private (0600) from the moment it exists, and at the rename it
/// takes the destination's permission bits, as a destination a shell redirect truncates keeps
/// them: a report made private stays private. A new destination gets what a plain create gives
/// it. Ownership, ACLs and the setuid, setgid and sticky bits are not carried over.
struct OutFile {
    dest: PathBuf,
    tmp: PathBuf,
    file: Option<std::io::BufWriter<std::fs::File>>,
    committed: bool,
    /// The permission bits a file created plainly here gets, the umask applied: what a
    /// destination that does not exist yet is given at the rename.
    #[cfg(unix)]
    created_mode: u32,
}

impl OutFile {
    /// Refuse a directory or a path that names no file, then create the temporary file beside
    /// `dest`.
    fn create(dest: &Path) -> Result<OutFile> {
        OutFile::create_with(dest, random_suffix)
    }

    /// [`OutFile::create`], drawing each temporary name's suffix from `suffix`, which a test
    /// chooses.
    fn create_with(dest: &Path, mut suffix: impl FnMut() -> u64) -> Result<OutFile> {
        let named = |what: String| EngineError::Other(format!("--out {}: {what}", dest.display()));
        if dest.is_dir() {
            return Err(named("is a directory".to_string()));
        }
        if dest.file_name().is_none() {
            return Err(named("names no file".to_string()));
        }
        // The process ID alone is not a unique name: a run killed before its rename leaves its file
        // behind, and in a container lakeleto is PID 1 on every run, so the next run would find
        // that file and stop. Two containers writing to one volume can both be PID 1 at the same
        // time, too. So the name carries a random suffix, the file is created exclusively, and a
        // name already taken is a reason to draw another, never to delete a file that may be in
        // use. Any other failure, such as a directory that does not exist, is final. The name is
        // the same length whatever the destination is called, so a destination named as long as
        // the file system allows still has a temporary name beside it.
        let mut created = None;
        for _ in 0..8 {
            let tmp = dest.with_file_name(format!(
                ".lakeleto.{}-{:016x}.tmp",
                std::process::id(),
                suffix()
            ));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)
            {
                Ok(file) => {
                    created = Some((tmp, file));
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(named(e.to_string())),
            }
        }
        let (tmp, file) = created
            .ok_or_else(|| named("eight temporary names beside it were all taken".to_string()))?;
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut out = OutFile {
            dest: dest.to_path_buf(),
            tmp,
            file: Some(std::io::BufWriter::new(file)),
            committed: false,
            #[cfg(unix)]
            created_mode: 0,
        };
        // Built first, so that if this fails the file is removed as the value drops.
        #[cfg(unix)]
        out.make_private().map_err(|e| named(e.to_string()))?;
        Ok(out)
    }

    /// Note what a plain create gave the file, then make it private: no one else reads the output
    /// while it is written, or after a killed run leaves it behind.
    #[cfg(unix)]
    fn make_private(&mut self) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let file = self.file.as_ref().expect("made private once").get_ref();
        let created = file.metadata()?.permissions().mode() & 0o777;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        self.created_mode = created;
        Ok(())
    }

    /// Where the output is written until [`OutFile::commit`].
    fn writer(&mut self) -> &mut std::io::BufWriter<std::fs::File> {
        self.file
            .as_mut()
            .expect("written only before it is committed")
    }

    /// Flush the file to disk and rename it over the destination.
    fn commit(mut self) -> Result<()> {
        let named =
            |what: String| EngineError::Other(format!("--out {}: {what}", self.dest.display()));
        let file = self.file.take().expect("committed once");
        let file = file
            .into_inner()
            .map_err(|e| named(e.error().to_string()))?;
        file.sync_all().map_err(|e| named(e.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = match std::fs::metadata(&self.dest) {
                Ok(existing) => existing.permissions().mode() & 0o777,
                Err(_) => self.created_mode,
            };
            file.set_permissions(std::fs::Permissions::from_mode(mode))
                .map_err(|e| named(e.to_string()))?;
        }
        // Closed before the rename, which Windows refuses on a file that is still open.
        drop(file);
        std::fs::rename(&self.tmp, &self.dest).map_err(|e| named(e.to_string()))?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for OutFile {
    fn drop(&mut self) {
        if !self.committed {
            self.file.take();
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

/// Canonicalize the optional `--root` confinement dir — it must exist and be a directory, so a
/// typo fails fast at startup rather than silently confining nothing.
#[cfg(any(feature = "serve", feature = "mcp"))]
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

// ---- engine selection -----------------------------------------------------------------

/// Will this invocation read through the `remote` engine?
///
/// [`engines_for`] builds its remote registry exactly when this says so, so the
/// source-resolution decision below cannot drift from the engine actually selected.
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
    let source = if uses_remote(cli) {
        Source::unresolved(path, cli.format.as_deref())?
    } else {
        Source::resolve(path, cli.format.as_deref())?
    };
    let flatten = match cli.flatten.as_deref() {
        Some(value) => Flatten::parse(value)?,
        None => None,
    };
    Ok(source
        .with_json_path(cli.json_path.as_deref())?
        .with_flatten(flatten))
}

/// The engines this invocation reads with, as `--engine` and `--remote-url` choose them.
///
/// - **remote** (`--engine remote`, or `--remote-url` with `--engine auto`): one remote engine
///   answers every request, and the server resolves the sources.
/// - **sql:** DataFusion reads files as well as running SQL.
/// - **local:** the local reader, with no SQL.
/// - **auto:** this build's engines ([`EngineRegistry::local`]).
///
/// Every mode but remote reads a database table with the database engine when the build has one,
/// since it is the only engine that can.
fn engines_for(cli: &Cli) -> Result<EngineRegistry> {
    if uses_remote(cli) {
        let remote = make_remote(cli)?;
        return Ok(EngineRegistry::new(remote.clone())
            .with_sql(remote.clone())
            .with_database(remote));
    }
    Ok(match cli.engine {
        EngineChoice::Local => {
            EngineRegistry::new(std::sync::Arc::new(LocalReaderEngine::default()))
                .with_local_database()
        }
        EngineChoice::Sql => {
            let sql = make_sql()?;
            EngineRegistry::new(sql.clone())
                .with_sql(sql)
                .with_local_database()
        }
        EngineChoice::Auto | EngineChoice::Remote => EngineRegistry::local(),
    })
}

#[cfg(feature = "sql")]
fn make_sql() -> Result<std::sync::Arc<dyn Engine>> {
    Ok(std::sync::Arc::new(
        crate::engine::sql::DataFusionEngine::new(),
    ))
}

#[cfg(not(feature = "sql"))]
fn make_sql() -> Result<std::sync::Arc<dyn Engine>> {
    Err(EngineError::missing_feature("run SQL", "sql"))
}

#[cfg(feature = "remote")]
fn make_remote(cli: &Cli) -> Result<std::sync::Arc<dyn Engine>> {
    Ok(std::sync::Arc::new(remote_engine(cli)?))
}

#[cfg(feature = "remote")]
fn remote_engine(cli: &Cli) -> Result<crate::engine::remote::RemoteEngine> {
    let url = cli.remote_url.clone().ok_or_else(|| {
        EngineError::Remote(
            "no endpoint — pass `--remote-url https://...` or set LAKELETO_REMOTE_URL".to_string(),
        )
    })?;
    Ok(crate::engine::remote::RemoteEngine::new(
        url,
        cli.remote_token.clone(),
    ))
}

#[cfg(not(feature = "remote"))]
fn make_remote(_cli: &Cli) -> Result<std::sync::Arc<dyn Engine>> {
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
    /// a flag that would still do nothing afterwards, and not called "planned", because nothing is.
    Inert,
}

/// `lakeleto catalog ls`: the configured catalogs (name, type, URI), or what is under a catalog or
/// a namespace (name, kind, reference), as rows the `-o` formats all print.
#[cfg(feature = "catalog")]
fn catalog_ls(
    catalogs: &crate::catalog::Catalogs,
    ctx: &RequestContext,
    reference: Option<&str>,
) -> Result<crate::engine::RowBatch> {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let reference = match reference {
        None => crate::catalog::CatalogRef::root(),
        Some(r) => crate::catalog::CatalogRef::parse(r)?,
    };
    let columns: [(&str, Vec<String>); 3] = if reference.catalog().is_none() {
        let configs = catalogs.configs()?;
        [
            (
                "name",
                configs.iter().map(|c| c.name().to_string()).collect(),
            ),
            (
                "type",
                configs.iter().map(|c| c.kind().to_string()).collect(),
            ),
            (
                "uri",
                configs
                    .iter()
                    .map(|c| c.uri().unwrap_or_default().to_string())
                    .collect(),
            ),
        ]
    } else {
        let listing = catalogs.list(ctx, &reference)?;
        if listing.truncated {
            eprintln!(
                "lakeleto: {} holds more than {} entries; showing the first",
                listing.listing.dir,
                listing.listing.entries.len()
            );
        }
        let entries = &listing.listing.entries;
        [
            ("name", entries.iter().map(|e| e.name.clone()).collect()),
            (
                "kind",
                entries
                    .iter()
                    .map(|e| {
                        if e.kind == "dir" {
                            "namespace"
                        } else {
                            "table"
                        }
                        .to_string()
                    })
                    .collect(),
            ),
            (
                "reference",
                entries.iter().map(|e| e.path.clone()).collect(),
            ),
        ]
    };
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|(_, values)| Arc::new(StringArray::from(values)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(EngineError::arrow)?;
    Ok(crate::engine::RowBatch {
        schema,
        batches: vec![batch],
    })
}

/// `lakeleto engines`: a row for every backend this build has or could have, saying whether it is
/// compiled in and what it reads, then the legend for the marks.
fn render_engines() -> String {
    let mut out = String::from("Lakeleto engine backends:\n\n");

    // The format lists come from the same helpers the engines' `capabilities()` use, so this
    // command and `GET /v1/engines` cannot disagree about what the build can read.
    let formats = crate::engine::readable_formats().join(", ");
    out.push_str(&cap_line(LocalReaderEngine::LABEL, &formats, CapState::On));
    // The SQL engine's own list, not the local reader's: they answer different questions (today
    // with the same formats), and this row has to match what `DataFusionEngine::capabilities`
    // reports.
    out.push_str(&cap_line(
        "sql (DataFusion)",
        &format!(
            "{} + read-only SQL",
            crate::engine::sql_readable_formats().join(", ")
        ),
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
    // The file formats the local reader opens, read as objects. (An Iceberg table in a store is
    // the iceberg reader's to list; Delta is read from local disk only.)
    out.push_str(&cap_line(
        "object-store (s3/gs/az)",
        "remote parquet, csv, tsv, json and arrow via your own creds",
        feature_state(cfg!(feature = "object-store")),
    ));
    out.push_str(&cap_line(
        "catalog (Iceberg REST)",
        "catalog:// tables, with the credentials the catalog vends",
        feature_state(cfg!(feature = "catalog")),
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
    // ROADMAP Phase 5 records this as deliberately not built, and the flag as kept, inert:
    // `--features duckdb` compiles and wires nothing, and this row is what keeps that from being
    // silent.
    out.push_str(&cap_line(
        "duckdb",
        "not built — the `sql` (DataFusion) engine covers this",
        CapState::Inert,
    ));
    out.push_str(
        "\nLegend: ✓ available · · not compiled (rebuild with the named --features) · ○ inert \
         (the feature flag exists but wires nothing).\n\
         The UI binds to the `Engine` trait, so every backend is interchangeable.\n",
    );
    out
}

/// What the server `--remote-url` names says it runs, as its `GET /v1/engines` answers.
#[cfg(feature = "remote")]
fn render_server(cli: &Cli, ctx: &RequestContext) -> Result<String> {
    let remote = remote_engine(cli)?;
    let server = remote.server(ctx)?;
    Ok(server_listing(remote.endpoint(), &server))
}

#[cfg(not(feature = "remote"))]
fn render_server(cli: &Cli, _ctx: &RequestContext) -> Result<String> {
    make_remote(cli).map(|_| String::new())
}

/// The server's part of `lakeleto engines`: its version, its protocol and whether this client
/// speaks it, and a row per engine.
#[cfg(feature = "remote")]
fn server_listing(endpoint: &str, server: &crate::engine::remote::ServerInfo) -> String {
    use crate::protocol::{compatible, PROTOCOL_VERSION};
    let protocol = match server.protocol.as_deref() {
        Some(version) if compatible(version) => version.to_string(),
        Some(version) => format!(
            "{version} (this client speaks {PROTOCOL_VERSION}: a different major, so its routes \
             may not mean what this client expects)"
        ),
        None => "unversioned (the server predates protocol 1.0)".to_string(),
    };
    let mut out = format!(
        "\nThe server at {endpoint}:\n\n  version : {}\n  protocol: {protocol}\n\n",
        server.version.as_deref().unwrap_or("unknown"),
    );
    // A server that predates the list names only its read engine.
    let engines = match server.engines.as_slice() {
        [] => std::slice::from_ref(&server.engine),
        engines => engines,
    };
    for engine in engines {
        let mut reads = engine.formats.join(", ");
        if engine.sql {
            reads.push_str(" + read-only SQL");
        }
        out.push_str(&cap_line(&engine.engine, &reads, CapState::On));
    }
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

/// One row of `lakeleto engines`: the state's mark, the backend's name, what it reads, and a note
/// when it is not compiled in or is inert.
fn cap_line(name: &str, formats: &str, state: CapState) -> String {
    let (mark, note) = match state {
        CapState::On => ("✓", ""),
        CapState::Off => ("·", " (not compiled)"),
        CapState::Inert => ("○", " (inert — the feature flag wires nothing)"),
    };
    format!("  {mark} {name:<32} reads: {formats}{note}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `lakeleto engines` has a row for the catalog backend, which says whether this build has it.
    #[test]
    fn engines_lists_the_catalog_backend_as_this_build_has_it() {
        let listing = render_engines();
        let row = listing
            .lines()
            .find(|l| l.contains("catalog (Iceberg REST)"))
            .unwrap_or_else(|| panic!("no catalog row in:\n{listing}"));
        assert_eq!(
            row.contains("(not compiled)"),
            !cfg!(feature = "catalog"),
            "{row}"
        );
    }

    /// `lakeleto engines` says what each row is: the inert `duckdb` flag as inert, not planned,
    /// since nothing is; the local engine by the label `/v1/engines` gives it rather than by
    /// crates; and the object-store row by every file format it reads, not two of them.
    #[test]
    fn engines_names_each_backend_for_what_it_is() {
        let listing = render_engines();
        let row = |name: &str| {
            listing
                .lines()
                .find(|l| l.contains(name))
                .unwrap_or_else(|| panic!("no {name} row in:\n{listing}"))
                .to_string()
        };
        assert!(!listing.contains("planned"), "{listing}");
        assert!(row("duckdb").ends_with("(inert — the feature flag wires nothing)"));
        assert!(listing.contains("○ inert (the feature flag exists but wires nothing)"));
        assert!(row("local (built-in reader)").contains("json, arrow"));
        let remote = row("object-store (s3/gs/az)");
        for format in ["parquet", "csv", "tsv", "json", "arrow"] {
            assert!(remote.contains(format), "{format} missing from {remote}");
        }
    }

    /// Every mode but remote reads a database table with the database engine, the only one that
    /// can: `lakeleto head 'sqlite:///x.db?table=t'` used to reach the file reader, and
    /// `lakeleto query` over one the SQL engine, and neither reads a database. (`--engine local`
    /// runs no SQL, so `query` refuses it before asking for an engine.)
    #[test]
    #[cfg(feature = "sqlite")]
    fn a_database_table_goes_to_the_database_engine_in_every_local_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("x.db");
        std::fs::File::create(&db).unwrap();
        let uri = format!("sqlite://{}?table=t", db.display());
        let mut modes = vec!["auto", "local"];
        if cfg!(feature = "sql") {
            modes.push("sql");
        }
        for mode in modes {
            let cli = Cli::try_parse_from(["lakeleto", "--engine", mode, "head", &uri]).unwrap();
            let source = source_for(&cli, Path::new(&uri)).unwrap();
            assert_eq!(source.format, crate::source::Format::Database, "{mode}");
            let engines = engines_for(&cli).unwrap();
            let read = engines.resolve(Need::Read(&source)).unwrap();
            assert_eq!(read.name(), "database", "{mode}");
            if mode == "local" {
                continue;
            }
            let tables = [NamedSource {
                name: "t".to_string(),
                source,
            }];
            let query = engines.resolve(Need::Query(&tables)).unwrap();
            assert_eq!(query.name(), "database", "{mode}");
        }
    }

    /// `query` runs on the engine `--engine` chooses. `--engine local` has none, and refuses
    /// before it reads anything; `auto` runs it when the build has the SQL engine.
    #[test]
    fn query_runs_on_the_chosen_engine_and_local_refuses_it() {
        let csv = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/people.csv");
        let query = |mode: &str| {
            run(Cli::try_parse_from([
                "lakeleto",
                "--engine",
                mode,
                "-o",
                "csv",
                "query",
                "--file",
                csv,
                "SELECT count(*) AS n FROM t",
            ])
            .unwrap())
        };
        let err = query("local").unwrap_err().to_string();
        assert!(err.contains("no SQL planner"), "{err}");
        let auto = query("auto");
        assert_eq!(auto.is_ok(), cfg!(feature = "sql"), "{auto:?}");
        if let Err(e) = auto {
            assert!(!e.to_string().contains("no SQL planner"), "{e}");
            assert!(e.to_string().contains("`sql`"), "{e}");
        }
    }

    /// `--engine local` reads files with the local reader and runs no SQL; `auto` runs SQL when
    /// the build has the engine for it.
    #[test]
    fn the_engine_flag_chooses_the_read_engine_and_whether_sql_runs() {
        let engines = |mode: &str| {
            engines_for(&Cli::try_parse_from(["lakeleto", "--engine", mode, "engines"]).unwrap())
                .unwrap()
        };
        let local = engines("local");
        assert_eq!(local.read_engine().name(), "local");
        assert!(!local.has_sql());
        let auto = engines("auto");
        assert_eq!(auto.read_engine().name(), "local");
        assert_eq!(auto.has_sql(), cfg!(feature = "sql"));
        let sql =
            engines_for(&Cli::try_parse_from(["lakeleto", "--engine", "sql", "engines"]).unwrap());
        assert_eq!(sql.is_ok(), cfg!(feature = "sql"));
        match sql {
            Ok(sql) => {
                assert_eq!(sql.read_engine().name(), "sql");
                assert!(sql.has_sql());
            }
            Err(e) => assert!(e.to_string().contains("`sql`"), "{e}"),
        }
    }

    /// A build without the remote engine says so when asked to list a server, rather than list
    /// nothing.
    #[test]
    #[cfg(not(feature = "remote"))]
    fn listing_a_server_names_the_feature_this_build_lacks() {
        let cli =
            Cli::try_parse_from(["lakeleto", "--remote-url", "http://127.0.0.1:1", "engines"])
                .unwrap();
        let err = run(cli).unwrap_err().to_string();
        assert!(err.contains("`remote`"), "{err}");
    }

    /// The server's part of `lakeleto engines` names its version and protocol, says when this
    /// client speaks another major or the server predates versioning, and lists every engine.
    #[test]
    #[cfg(feature = "remote")]
    fn the_server_listing_names_its_protocol_and_every_engine() {
        let server = |protocol: serde_json::Value, engines: serde_json::Value| {
            let info: crate::engine::remote::ServerInfo =
                serde_json::from_value(serde_json::json!({
                    "version": "0.4.0",
                    "protocol": protocol,
                    "engine": { "engine": "reader", "formats": ["parquet", "csv"], "sql": false,
                                "profile": true, "remote": false },
                    "engines": engines,
                    "sql_available": true,
                }))
                .unwrap();
            server_listing("http://lake:7878", &info)
        };
        let both = serde_json::json!([
            { "engine": "reader", "formats": ["parquet", "csv"], "sql": false,
              "profile": true, "remote": false },
            { "engine": "sql (DataFusion)", "formats": ["parquet"], "sql": true,
              "profile": true, "remote": false },
        ]);

        let listing = server("1.0".into(), both.clone());
        assert!(
            listing.contains("The server at http://lake:7878:"),
            "{listing}"
        );
        assert!(listing.contains("version : 0.4.0\n"), "{listing}");
        assert!(listing.contains("protocol: 1.0\n"), "{listing}");
        let rows: Vec<&str> = listing.lines().filter(|l| l.contains("reads:")).collect();
        assert_eq!(rows.len(), 2, "{listing}");
        assert!(rows[0].contains("reader") && rows[0].ends_with("reads: parquet, csv"));
        assert!(
            rows[1].ends_with("reads: parquet + read-only SQL"),
            "{}",
            rows[1]
        );

        let later = server("1.4".into(), both.clone());
        assert!(
            later.contains("protocol: 1.4\n"),
            "a later minor only adds: {later}"
        );
        let other = server("2.0".into(), both);
        assert!(
            other.contains("protocol: 2.0 (this client speaks 1.0"),
            "{other}"
        );
        // A server from before versioning, and before the engine list: its read engine stands in.
        let old = server(serde_json::Value::Null, serde_json::json!([]));
        assert!(old.contains("protocol: unversioned"), "{old}");
        assert_eq!(
            old.lines().filter(|l| l.contains("reads:")).count(),
            1,
            "{old}"
        );
    }

    /// [`Cli::output`]: `-o` first, then `--out`'s extension, then `table`.
    #[test]
    fn the_format_is_o_then_the_out_extension_then_table() {
        let output = |args: &[&str]| {
            let mut argv = vec!["lakeleto"];
            argv.extend_from_slice(args);
            Cli::try_parse_from(argv).unwrap().output()
        };
        assert_eq!(output(&["head", "t.csv"]), Output::Table);
        assert_eq!(
            output(&["head", "t.csv", "--out", "r.parquet"]),
            Output::Parquet
        );
        assert_eq!(
            output(&["head", "t.csv", "--out", "r.arrow"]),
            Output::Arrow
        );
        assert_eq!(
            output(&["head", "t.csv", "--out", "r.arrows"]),
            Output::Arrows
        );
        // `-o` wins over the extension, wherever it is given.
        assert_eq!(
            output(&["head", "t.csv", "-o", "csv", "--out", "r.parquet"]),
            Output::Csv
        );
        assert_eq!(output(&["-o", "arrows", "head", "t.csv"]), Output::Arrows);
        // An extension that names no format, and stdout, leave the default.
        assert_eq!(output(&["head", "t.csv", "--out", "r.txt"]), Output::Table);
        assert_eq!(output(&["head", "t.csv", "--out", "-"]), Output::Table);
    }

    /// [`check_output`] over commands, formats and destinations.
    #[test]
    fn binary_output_is_for_rows_and_never_for_a_terminal() {
        let head = Cmd::Head {
            path: "t.csv".into(),
            rows: 10,
        };
        let query = Cmd::Query {
            sql: "SELECT 1".into(),
            tables: Vec::new(),
            file: None,
        };
        // A file or a pipe takes the bytes.
        for output in [Output::Arrow, Output::Arrows] {
            assert!(check_output(&head, output, true, false).is_ok());
            assert!(check_output(&head, output, false, false).is_ok());
            assert!(check_output(&query, output, false, false).is_ok());
        }
        // Parquet too, where the build writes it, and otherwise a refusal naming the feature.
        let parquet = check_output(&head, Output::Parquet, true, false);
        assert_eq!(parquet.is_ok(), render::WRITES_PARQUET, "{parquet:?}");
        if let Err(e) = parquet {
            assert!(e.to_string().contains("--features parquet-out"), "{e}");
        }
        // A terminal does not, and the refusal says where they can go instead.
        let err = check_output(&head, Output::Arrow, false, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("terminal") && err.contains("--out"), "{err}");
        assert!(err.starts_with("`-o arrow` is binary"), "{err}");
        // Text is fine anywhere.
        assert!(check_output(&head, Output::Csv, false, true).is_ok());
        assert!(check_output(&head, Output::Table, false, true).is_ok());
        // A description has no rows to write, whatever it would be written to.
        let schema = Cmd::Schema {
            path: "t.csv".into(),
        };
        let err = check_output(&schema, Output::Arrow, true, false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("writes rows") && err.contains("`schema`"),
            "{err}"
        );
        assert!(check_output(&Cmd::Engines, Output::Arrows, false, false).is_err());
        assert!(check_output(&schema, Output::Json, true, false).is_ok());
    }

    /// `mcp` speaks its protocol on stdout, so `--out` is refused, and a terminal is no matter.
    #[cfg(feature = "mcp")]
    #[test]
    fn a_server_takes_no_out() {
        let mcp = Cmd::Mcp {
            root: None,
            default_scan: DEFAULT_SCAN,
            max_rows: 1,
            max_bytes: 1024,
            timeout: 1,
        };
        let err = check_output(&mcp, Output::Table, true, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("`mcp` takes no `--out`"), "{err}");
        assert!(check_output(&mcp, Output::Table, false, true).is_ok());
    }

    /// A temporary file already there for the same destination, from the same process ID, does
    /// not block `--out`. It stands in for one a killed run left behind in a container, where
    /// lakeleto is PID 1 on every run, and for a second container writing to the same volume.
    #[test]
    fn a_temporary_file_already_there_does_not_block_out() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("report.arrow");
        let first = OutFile::create(&dest).unwrap();
        let mut second = OutFile::create(&dest).expect("a second temporary name");
        assert_ne!(first.tmp, second.tmp);
        second.writer().write_all(b"rows").unwrap();
        second.commit().unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"rows");
        // The first is still someone's: it is removed by its own run, not by the second.
        assert!(first.tmp.exists());
        drop(first);
        let left: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(left, ["report.arrow"]);
    }

    /// `--max-decompressed` takes bytes, or a size in powers of 1024 however its unit is written.
    #[test]
    fn a_size_is_bytes_or_a_power_of_1024() {
        for (given, bytes) in [
            ("4096", 4096),
            ("0", 0),
            ("512K", 512 << 10),
            ("512kib", 512 << 10),
            ("64M", 64 << 20),
            ("8GiB", 8 << 30),
            ("8 GB", 8 << 30),
            ("1T", 1 << 40),
        ] {
            assert_eq!(parse_bytes(given), Ok(bytes), "{given}");
        }
        for given in ["", "lots", "4X", "-1", "1.5G", "99999999999T"] {
            assert!(parse_bytes(given).is_err(), "{given}");
        }
        let cli = Cli::try_parse_from(["lakeleto", "engines"]).unwrap();
        assert_eq!(
            cli.max_decompressed,
            crate::source::DEFAULT_MAX_DECOMPRESSED
        );
    }

    /// The temporary name `--out` makes for `dest` from `suffix`.
    fn temp_name(dest: &Path, suffix: u64) -> PathBuf {
        dest.with_file_name(format!(
            ".lakeleto.{}-{suffix:016x}.tmp",
            std::process::id()
        ))
    }

    /// A temporary name already taken is passed over, and the file that has it is left alone: it
    /// may be another run's, still being written.
    #[test]
    fn a_taken_temporary_name_is_passed_over_and_its_file_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("report.arrow");
        let taken = temp_name(&dest, 7);
        std::fs::write(&taken, b"another run's").unwrap();
        let mut suffixes = [7, 7, 8].into_iter();
        let mut out = OutFile::create_with(&dest, || suffixes.next().expect("three names at most"))
            .expect("the third name is free");
        assert_eq!(suffixes.next(), None, "a name was drawn after a free one");
        assert_eq!(out.tmp, temp_name(&dest, 8));
        out.writer().write_all(b"rows").unwrap();
        out.commit().unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"rows");
        assert_eq!(std::fs::read(&taken).unwrap(), b"another run's");
    }

    /// Eight taken names in a row is not bad luck, so `--out` stops there, rather than drawing
    /// forever, and says why.
    #[test]
    fn out_stops_after_eight_taken_names() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("report.arrow");
        std::fs::write(temp_name(&dest, 7), b"another run's").unwrap();
        let mut drawn = 0;
        let err = OutFile::create_with(&dest, || {
            drawn += 1;
            7
        })
        .err()
        .expect("no free name");
        assert_eq!(drawn, 8);
        assert!(err.to_string().contains("were all taken"), "{err}");
        assert!(!dest.exists());
    }

    /// Any other failure, such as a directory that does not exist, is reported at once: another
    /// name would fail the same way.
    #[test]
    fn out_reports_a_missing_directory_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("missing").join("report.arrow");
        let mut drawn = 0;
        let err = OutFile::create_with(&dest, || {
            drawn += 1;
            7
        })
        .err()
        .expect("no directory to write in")
        .to_string();
        assert_eq!(drawn, 1, "{err}");
        assert!(
            err.starts_with(&format!("--out {}: ", dest.display())),
            "{err}"
        );
        assert!(!err.contains("taken"), "{err}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// The temporary file is hidden beside its destination, where the rename is atomic, and its
    /// name is the same length whatever the destination is called, so a destination named as long
    /// as a file system allows still gets one.
    #[test]
    fn the_temporary_file_is_beside_its_destination_whatever_that_is_called() {
        let dir = tempfile::tempdir().unwrap();
        // 250 bytes: within the 255 a name may have on Linux, macOS and Windows, and too long for a
        // temporary name made by adding to it.
        let dest = dir.path().join(format!("{}.arrow", "r".repeat(244)));
        let mut out = OutFile::create(&dest).unwrap();
        assert_eq!(out.tmp.parent(), Some(dir.path()));
        let name = out.tmp.file_name().unwrap().to_str().unwrap();
        assert!(
            name.starts_with(".lakeleto.") && name.ends_with(".tmp"),
            "{name}"
        );
        out.writer().write_all(b"rows").unwrap();
        out.commit().unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"rows");
    }

    /// The sink keeps `Write`'s promise for `flush`, which the Arrow and Parquet writers call when
    /// they finish: what was written leaves the buffer for the file, or for stdout.
    #[test]
    fn a_flush_through_the_sink_reaches_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("rows.csv");
        let mut sink = Sink::open(Some(&dest)).unwrap();
        sink.write_all(b"a,b\n").unwrap();
        let tmp = match &sink {
            Sink::File(f) => f.tmp.clone(),
            Sink::Stdout(_) => unreachable!("--out writes a file"),
        };
        assert_eq!(std::fs::read(&tmp).unwrap(), b"", "buffered until flushed");
        sink.flush().unwrap();
        assert_eq!(std::fs::read(&tmp).unwrap(), b"a,b\n");
        sink.finish().unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"a,b\n");
    }

    /// `--out` keeps a destination's permission bits, so a report made private stays private and
    /// one shared with a group stays shared, writes privately until the rename, and gives a new
    /// file what a plain create gives it.
    #[cfg(unix)]
    #[test]
    fn out_keeps_the_destinations_permissions_and_writes_privately() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let set = |p: &Path, m: u32| {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(m)).unwrap();
        };
        let dir = tempfile::tempdir().unwrap();

        let report = dir.path().join("report.arrow");
        std::fs::write(&report, b"old").unwrap();
        set(&report, 0o600);
        let mut out = OutFile::create(&report).unwrap();
        assert_eq!(mode(&out.tmp), 0o600, "private while it is written");
        out.writer().write_all(b"new").unwrap();
        out.commit().unwrap();
        assert_eq!(std::fs::read(&report).unwrap(), b"new");
        assert_eq!(mode(&report), 0o600, "a private report stays private");

        set(&report, 0o640);
        OutFile::create(&report).unwrap().commit().unwrap();
        assert_eq!(mode(&report), 0o640, "kept, not tightened");

        let plain = dir.path().join("plain");
        std::fs::File::create(&plain).unwrap();
        let fresh = dir.path().join("fresh.arrow");
        OutFile::create(&fresh).unwrap().commit().unwrap();
        assert_eq!(
            mode(&fresh),
            mode(&plain),
            "a new file is created as any other"
        );
    }
}
