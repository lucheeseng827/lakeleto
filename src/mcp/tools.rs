//! The MCP server's tools: what each one is, and what a call to it does.
//!
//! Each tool is a thin layer over the [`Engine`](crate::engine::Engine) trait and the building
//! blocks `lakeleto serve` uses: [`crate::confine`] for `--root`, [`PathCatalog`] to resolve a
//! path, and the [`EngineRegistry`] to pick an engine. What the layer adds is what an agent needs.
//! Results are small enough for a context window, with a row cap and a byte cap. Rows are arrays
//! in column order rather than objects that repeat every column's name. A call has a deadline.
//! And an error says what kind of refusal it is.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::catalog::{Catalog, PathCatalog};
use crate::context::RequestContext;
use crate::engine::registry::{EngineRegistry, Need};
use crate::engine::{NamedSource, RowBatch, ScanSpec};
use crate::error::{CancelReason, EngineError};
use crate::source::{Format, RemoteProbe, Source};

/// What a call may cost: the `lakeleto mcp` flags.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Rows `profile` scans when the call doesn't say.
    pub default_scan: usize,
    /// The most rows `preview` or `query` returns.
    pub max_rows: usize,
    /// The most bytes of JSON a call returns.
    pub max_bytes: usize,
    /// How long a call may run.
    pub timeout: Duration,
}

/// Rows `preview` returns when the call doesn't say.
const PREVIEW_ROWS: usize = 20;
/// Rows `query` returns when the call doesn't say.
const QUERY_ROWS: usize = 100;
/// The most rows `profile` scans, unless `--default-scan` is larger: `/v1/profile`'s cap.
const MAX_SCAN: usize = 100_000;
/// Bytes kept free for the `truncated` and `note` fields a cut result gains.
const NOTE_ROOM: usize = 256;

/// A tool for the loop's tests, not listed by `tools/list`: it sleeps for `ms` without looking at
/// its context, as an engine that never checks its deadline would.
#[cfg(test)]
pub(super) const SLEEP: &str = "test_sleep";

/// The tools, and what they read with.
pub struct Tools {
    engines: EngineRegistry,
    root: Option<PathBuf>,
    limits: Limits,
    #[cfg(feature = "catalog")]
    catalogs: std::sync::Arc<crate::catalog::Catalogs>,
}

/// A call that was refused or failed: a kind an agent can act on, and what happened.
#[derive(Debug)]
struct Refusal {
    kind: &'static str,
    message: String,
}

impl Refusal {
    /// A call whose arguments are wrong: the agent can fix them and call again.
    fn invalid(message: impl Into<String>) -> Self {
        Refusal {
            kind: "invalid_arguments",
            message: message.into(),
        }
    }
}

impl From<EngineError> for Refusal {
    /// An engine's error as a refusal, its kind named after the error's variant.
    fn from(e: EngineError) -> Self {
        let kind = match &e {
            EngineError::Forbidden(_) => "forbidden",
            EngineError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => "not_found",
            EngineError::Cancelled(CancelReason::Deadline) => "deadline",
            EngineError::Cancelled(CancelReason::Requested) => "cancelled",
            EngineError::Query(_) => "query",
            EngineError::UnsupportedFormat { .. } => "unsupported_format",
            EngineError::UnsupportedOperation { .. } => "unsupported",
            EngineError::TooLarge(_) => "too_large",
            EngineError::Remote(_) => "remote",
            _ => "failed",
        };
        Refusal {
            kind,
            message: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for Refusal {
    /// A result that couldn't be made into JSON: a failure of Lakeleto's, not of the call.
    fn from(e: serde_json::Error) -> Self {
        Refusal {
            kind: "failed",
            message: e.to_string(),
        }
    }
}

type Outcome = std::result::Result<Value, Refusal>;

impl Tools {
    /// The tools, reading with `engines`, confined to `root` when there is one, within `limits`.
    pub fn new(engines: EngineRegistry, root: Option<PathBuf>, limits: Limits) -> Self {
        Tools {
            engines,
            root,
            limits,
            #[cfg(feature = "catalog")]
            catalogs: crate::catalog::Catalogs::configured(),
        }
    }

    /// How long a call may run: `--timeout`.
    pub(super) fn timeout(&self) -> Duration {
        self.limits.timeout
    }

    /// The tools this build and configuration can run. `query` needs an SQL engine, and
    /// `catalog_ls` the `catalog` feature and no `--root`, which refuses every catalog reference.
    fn names(&self) -> Vec<&'static str> {
        let mut names = vec!["list", "describe", "preview", "profile"];
        if self.engines.can_query() {
            names.push("query");
        }
        #[cfg(feature = "catalog")]
        if self.root.is_none() {
            names.push("catalog_ls");
        }
        names
    }

    /// Is `name` a tool this server runs? Only an offered tool can be called.
    pub(super) fn offers(&self, name: &str) -> bool {
        #[cfg(test)]
        if name == SLEEP {
            return true;
        }
        self.names().contains(&name)
    }

    /// `tools/list`: the definitions of the tools offered.
    pub(super) fn definitions(&self) -> Vec<Value> {
        let names = self.names();
        names.iter().map(|name| self.definition(name)).collect()
    }

    /// The `instructions` an agent gets with `initialize`: how the tools fit together.
    pub(super) fn instructions(&self) -> String {
        let mut s = String::from(
            "Lakeleto reads tables, read-only. To find out what is in a table, `describe` it \
             first: its columns and types, its row count and, for a Parquet file, each column's \
             null count, min and max, all read from metadata rather than from the rows. Then \
             `preview` its rows or `profile` its columns from a bounded scan.",
        );
        if self.engines.can_query() {
            s.push_str(" `query` runs SQL over the tables you name.");
        }
        s.push_str(" `list` shows a directory's tables.");
        if self.offers("catalog_ls") {
            s.push_str(" `catalog_ls` browses the configured Iceberg REST catalogs.");
        }
        if let Some(root) = &self.root {
            s.push_str(&format!(
                " Only paths under {} can be read; relative paths are taken from there.",
                root.display()
            ));
        }
        s.push_str(&format!(
            " A result holds at most {} rows and {} bytes; one that was cut says `truncated`.",
            self.limits.max_rows, self.limits.max_bytes
        ));
        s
    }

    /// Run tool `name` and return its `tools/call` result. A refusal is a result too, with
    /// `isError` set, so the agent sees it and can correct the call.
    pub(super) fn call(&self, name: &str, args: &Value, ctx: &RequestContext) -> Value {
        let empty = Map::new();
        let args = args.as_object().unwrap_or(&empty);
        let outcome = match name {
            "list" => self.list(ctx, args),
            "describe" => self.describe(ctx, args),
            "preview" => self.preview(ctx, args),
            "profile" => self.profile(ctx, args),
            "query" => self.query(ctx, args),
            #[cfg(feature = "catalog")]
            "catalog_ls" => self.catalog_ls(ctx, args),
            #[cfg(test)]
            SLEEP => count(args, "ms").map(|ms| {
                let ms = ms.unwrap_or(0) as u64;
                std::thread::sleep(Duration::from_millis(ms));
                json!({ "slept": ms })
            }),
            _ => Err(Refusal::invalid(format!("unknown tool: {name}"))),
        };
        match outcome {
            Ok(value) => result(&value, false),
            Err(refusal) => refused(refusal),
        }
    }

    /// The answer to a call that ran past its deadline without its engine noticing.
    pub(super) fn overdue(&self) -> Value {
        refused(Refusal {
            kind: "deadline",
            message: format!(
                "the call ran past its {}-second deadline; narrow it (fewer rows, a smaller \
                 scan, a filter) or restart the server with a larger --timeout",
                self.limits.timeout.as_secs()
            ),
        })
    }

    /// The answer to a call made while `most` calls are already running.
    pub(super) fn busy(&self, most: usize) -> Value {
        refused(Refusal {
            kind: "busy",
            message: format!(
                "{most} calls are already running; call again when one of them has answered"
            ),
        })
    }

    /// The answer to a call whose thread panicked.
    pub(super) fn crashed(&self) -> Value {
        refused(Refusal {
            kind: "failed",
            message: "the call failed inside Lakeleto; the server's stderr has the details"
                .to_string(),
        })
    }

    // ---- reading a path ------------------------------------------------------------------

    /// `path` as the server reads it: from the root when there is one and the path is relative,
    /// so an agent can name `sales/orders.parquet`; otherwise as given. A URI is left alone, for
    /// confinement to refuse when there is a root.
    fn locate(&self, path: &str) -> String {
        match &self.root {
            Some(root) if !crate::confine::is_reference(path) && Path::new(path).is_relative() => {
                root.join(path).to_string_lossy().into_owned()
            }
            _ => path.to_string(),
        }
    }

    /// Resolve `path` to a source the engines read, confined to the root.
    fn open(
        &self,
        ctx: &RequestContext,
        path: &str,
        format: Option<&str>,
        json_path: Option<&str>,
    ) -> Result<Source, Refusal> {
        let path = self.locate(path);
        let root = self.root.as_deref();
        crate::confine::entry(root, &path)?;
        let source = PathCatalog::new(RemoteProbe::Ambient)
            .resolve(ctx, Path::new(&path), format)?
            .with_json_path(json_path)?;
        crate::confine::members(root, &source)?;
        Ok(source)
    }

    /// The source a call's `path`, `format` and `json_path` name.
    fn source(&self, ctx: &RequestContext, args: &Map<String, Value>) -> Result<Source, Refusal> {
        self.open(
            ctx,
            required(args, "path")?,
            string(args, "format")?,
            string(args, "json_path")?,
        )
    }

    // ---- the tools -----------------------------------------------------------------------

    /// `list`: what can be read in a directory, an object-store prefix, a database or a catalog
    /// namespace.
    fn list(&self, ctx: &RequestContext, args: &Map<String, Value>) -> Outcome {
        let dir = match string(args, "path")? {
            Some(path) => self.locate(path),
            None => match &self.root {
                Some(root) => root.display().to_string(),
                None => ".".to_string(),
            },
        };
        crate::confine::entry(self.root.as_deref(), &dir)?;
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "mysql"))]
        if crate::source::is_database_uri(&dir) {
            let listing = crate::engine::database::table_listing(ctx, &dir)?;
            return self.listing(listing, false);
        }
        #[cfg(feature = "catalog")]
        if crate::catalog::is_catalog_uri(&dir) {
            let reference = crate::catalog::CatalogRef::parse(&dir)?;
            let found = self.catalogs.list(ctx, &reference)?;
            return self.listing(found.listing, found.truncated);
        }
        let listing = PathCatalog::new(RemoteProbe::Ambient).list(ctx, Path::new(&dir))?;
        self.listing(listing, false)
    }

    /// A listing as a result: its entries without their empty fields, cut to the byte cap.
    fn listing(&self, listing: crate::source::DirListing, truncated: bool) -> Outcome {
        let mut out = serde_json::to_value(&listing)?;
        let entries = match out.get_mut("entries").map(Value::take) {
            Some(Value::Array(entries)) => entries.into_iter().map(without_nulls).collect(),
            _ => Vec::new(),
        };
        if let Some(map) = out.as_object_mut() {
            map.retain(|_, v| !v.is_null());
        }
        let (mut out, cut) = fit(out, "entries", entries, self.limits.max_bytes)?;
        if cut > 0 {
            cut_note(&mut out, cut, "entries", self.limits.max_bytes);
        } else if truncated {
            out["truncated"] = json!(true);
            out["note"] = json!("the catalog listed more than Lakeleto reads in one listing");
        }
        Ok(out)
    }

    /// `describe`: a table's columns, row count and stored statistics, without reading its rows.
    fn describe(&self, ctx: &RequestContext, args: &Map<String, Value>) -> Outcome {
        let source = self.source(ctx, args)?;
        let engine = self.engines.resolve(Need::Read(&source))?;
        let schema = engine.schema(ctx, &source)?;
        // The statistics a Parquet file keeps in its footer, read without reading a row. No other
        // source keeps them where they can be read that cheaply.
        let footer =
            source.format == Format::Parquet && !source.is_remote() && !source.path.is_dir();
        let stored = if footer {
            match engine.profile(ctx, &source, 0) {
                Ok(profile) => Some(profile),
                Err(e @ EngineError::Cancelled(_)) => return Err(e.into()),
                // A footer without statistics: the schema is still worth answering with.
                Err(_) => None,
            }
        } else {
            None
        };
        let columns = schema
            .columns
            .iter()
            .map(|c| {
                let mut column = json!({
                    "name": c.name,
                    "data_type": c.data_type,
                    "nullable": c.nullable,
                });
                let stats = stored
                    .as_ref()
                    .and_then(|p| p.columns.iter().find(|s| s.name == c.name));
                if let Some(stats) = stats {
                    // A null count of 0 is also what the footer gives when a row group didn't
                    // record one, so it is only reported alongside a min or max, or when not 0.
                    let known = stats.min.is_some() || stats.max.is_some();
                    if known || stats.null_count > 0 {
                        column["null_count"] = json!(stats.null_count);
                    }
                    if let Some(min) = &stats.min {
                        column["min"] = json!(min);
                    }
                    if let Some(max) = &stats.max {
                        column["max"] = json!(max);
                    }
                }
                column
            })
            .collect();
        let mut out = json!({
            "source": schema.source,
            "format": schema.format,
            "engine": schema.engine,
            "row_count": schema.row_count,
        });
        if let Some(size) = local_file_size(&source) {
            out["size_bytes"] = json!(size);
        }
        if let Some(records) = &schema.records_path {
            out["records_path"] = json!(records);
        }
        if let Some(credentials) = &schema.credentials {
            out["credentials"] = json!(credentials);
        }
        out["statistics"] = json!(match (&stored, footer) {
            (Some(_), _) => "from the Parquet footer: exact for the whole file",
            (None, true) => "the Parquet footer records none; profile scans rows for them",
            (None, false) => {
                "read only from one local Parquet file's footer; profile scans rows for them"
            }
        });
        let (mut out, cut) = fit(out, "columns", columns, self.limits.max_bytes)?;
        if cut > 0 {
            cut_note(&mut out, cut, "columns", self.limits.max_bytes);
        }
        Ok(out)
    }

    /// `preview`: rows of a table in its own order, from an offset, optionally some columns.
    fn preview(&self, ctx: &RequestContext, args: &Map<String, Value>) -> Outcome {
        let source = self.source(ctx, args)?;
        let rows = count(args, "rows")?
            .unwrap_or(PREVIEW_ROWS)
            .clamp(1, self.limits.max_rows);
        let offset = count(args, "offset")?.unwrap_or(0);
        // One row more than asked for, to tell whether there are more.
        let spec = ScanSpec {
            offset,
            limit: rows + 1,
            projection: strings(args, "columns")?,
            ..ScanSpec::default()
        };
        let engine = self.engines.resolve(Need::Scan(&source, &spec))?;
        let window = engine.scan(ctx, &source, &spec)?;
        let mut out = json!({ "offset": offset });
        if window.total_known {
            out["row_count"] = json!(window.matched_rows);
        }
        self.rows(
            out,
            &window.batch,
            rows,
            "there are more rows: raise `rows`, or page on with `offset`",
        )
    }

    /// `profile`: each column's statistics from a scan of up to `scan` rows.
    fn profile(&self, ctx: &RequestContext, args: &Map<String, Value>) -> Outcome {
        let source = self.source(ctx, args)?;
        let most = MAX_SCAN.max(self.limits.default_scan);
        let scan = count(args, "scan")?
            .unwrap_or(self.limits.default_scan)
            .min(most);
        let engine = self.engines.resolve(Need::Read(&source))?;
        let profile = engine.profile(ctx, &source, scan)?;
        let mut out = serde_json::to_value(&profile)?;
        let columns = match out.get_mut("columns").map(Value::take) {
            Some(Value::Array(columns)) => columns,
            _ => Vec::new(),
        };
        let (mut out, cut) = fit(out, "columns", columns, self.limits.max_bytes)?;
        if cut > 0 {
            cut_note(&mut out, cut, "columns", self.limits.max_bytes);
        }
        Ok(out)
    }

    /// `query`: one read-only SQL statement over the tables the call names.
    fn query(&self, ctx: &RequestContext, args: &Map<String, Value>) -> Outcome {
        let sql = required(args, "sql")?;
        let mut tables = Vec::new();
        if let Some(path) = string(args, "path")? {
            let source = self.open(
                ctx,
                path,
                string(args, "format")?,
                string(args, "json_path")?,
            )?;
            tables.push(NamedSource {
                name: "t".to_string(),
                source,
            });
        }
        match args.get("tables") {
            None | Some(Value::Null) => {}
            Some(Value::Array(named)) => {
                for table in named {
                    let Some(table) = table.as_object() else {
                        return Err(Refusal::invalid(
                            "each of `tables` must be an object: {\"name\": …, \"path\": …}",
                        ));
                    };
                    let source = self.open(
                        ctx,
                        required(table, "path")?,
                        string(table, "format")?,
                        string(table, "json_path")?,
                    )?;
                    tables.push(NamedSource {
                        name: required(table, "name")?.to_string(),
                        source,
                    });
                }
            }
            Some(_) => {
                return Err(Refusal::invalid(
                    "`tables` must be a list of {\"name\": …, \"path\": …}",
                ));
            }
        }
        if tables.is_empty() {
            return Err(Refusal::invalid(
                "name the tables to query: `path` (queried as `t`) or `tables`",
            ));
        }
        let rows = count(args, "rows")?
            .unwrap_or(QUERY_ROWS)
            .clamp(1, self.limits.max_rows);
        let engine = self.engines.resolve(Need::Query(&tables))?;
        // One row more than asked for, to tell whether there are more.
        let batch = engine.query_capped(ctx, sql, &tables, rows + 1)?;
        self.rows(
            json!({}),
            &batch,
            rows,
            "the query returns more rows: raise `rows`, or aggregate or filter in the SQL",
        )
    }

    #[cfg(feature = "catalog")]
    /// `catalog_ls`: the configured catalogs, a catalog's namespaces, or a namespace's
    /// namespaces and tables.
    fn catalog_ls(&self, ctx: &RequestContext, args: &Map<String, Value>) -> Outcome {
        let reference = string(args, "reference")?.unwrap_or("catalog://");
        crate::confine::entry(self.root.as_deref(), reference)?;
        let parsed = crate::catalog::CatalogRef::parse(reference)?;
        if parsed.catalog().is_none() {
            // The catalogs themselves come from configuration, with no network call.
            let catalogs = self
                .catalogs
                .configs()?
                .into_iter()
                .map(|c| {
                    json!({
                        "name": c.name(),
                        "type": c.kind(),
                        "uri": c.uri(),
                        "reference": format!("catalog://{}/", c.name()),
                    })
                })
                .collect();
            let (mut out, cut) = fit(
                json!({ "reference": "catalog://" }),
                "catalogs",
                catalogs,
                self.limits.max_bytes,
            )?;
            if cut > 0 {
                cut_note(&mut out, cut, "catalogs", self.limits.max_bytes);
            }
            return Ok(out);
        }
        let found = self.catalogs.list(ctx, &parsed)?;
        let entries = found
            .listing
            .entries
            .into_iter()
            .map(|e| {
                let kind = if e.kind == "dir" {
                    "namespace"
                } else {
                    "table"
                };
                json!({ "name": e.name, "kind": kind, "reference": e.path })
            })
            .collect();
        let out = json!({ "reference": parsed.to_string() });
        let (mut out, cut) = fit(out, "entries", entries, self.limits.max_bytes)?;
        if cut > 0 {
            cut_note(&mut out, cut, "entries", self.limits.max_bytes);
        } else if found.truncated {
            out["truncated"] = json!(true);
            out["note"] = json!("the catalog listed more than Lakeleto reads in one listing");
        }
        Ok(out)
    }

    // ---- results -------------------------------------------------------------------------

    /// `out` with `batch`'s columns and at most `cap` of its rows, each an array in column order,
    /// within the byte cap. `more` is the note for a batch that has rows past `cap`.
    fn rows(&self, mut out: Value, batch: &RowBatch, cap: usize, more: &str) -> Outcome {
        let fields = batch.schema.fields();
        out["columns"] = fields
            .iter()
            .map(|f| json!({ "name": f.name(), "data_type": f.data_type().to_string() }))
            .collect();
        let names: Vec<&String> = fields.iter().map(|f| f.name()).collect();
        let rows = crate::render::row_values(&batch.first(cap))?
            .into_iter()
            .map(|row| {
                // `row_values` leaves a null cell out of its row object; here it is a null.
                let cells = names
                    .iter()
                    .map(|name| row.get(name.as_str()).cloned().unwrap_or(Value::Null));
                Value::Array(cells.collect())
            })
            .collect();
        let (mut out, cut) = fit(out, "rows", rows, self.limits.max_bytes)?;
        if cut > 0 {
            cut_note(&mut out, cut, "rows", self.limits.max_bytes);
        } else if batch.num_rows() > cap {
            out["truncated"] = json!(true);
            out["note"] = json!(more);
        }
        Ok(out)
    }

    // ---- definitions ---------------------------------------------------------------------

    /// Tool `name` as `tools/list` describes it: its title, description, input schema and
    /// annotations.
    fn definition(&self, name: &str) -> Value {
        let path = self.path_schema();
        let format = json!({
            "type": "string",
            "enum": ["parquet", "csv", "tsv", "json", "arrow", "iceberg", "delta"],
            "description": "Read it as this format, when its name doesn't say.",
        });
        let json_path = json!({
            "type": "string",
            "description": "For a JSON document that holds its records inside it: the member \
                            that holds them, e.g. `data`, or a JSON Pointer such as `/data/items`.",
        });
        let max_rows = self.limits.max_rows;
        let (title, description, schema) = match name {
            "list" => (
                "List tables",
                "List what can be read in a directory: its subdirectories and its tables \
                 (Parquet, CSV, TSV and JSON files; Iceberg and Delta tables). Also lists an \
                 object-store prefix, a database's tables, or a catalog namespace. Each entry's \
                 `path` can be passed to the other tools.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": self.list_path_help() },
                    },
                }),
            ),
            "describe" => (
                "Describe a table",
                "What is in a table, without reading its rows: its columns and types, its row \
                 count when the table records one, and for a Parquet file each column's null \
                 count, min and max from the file's footer. A CSV, TSV or JSON file records \
                 neither, so its types are inferred from its first rows.",
                json!({
                    "type": "object",
                    "properties": { "path": path, "format": format, "json_path": json_path },
                    "required": ["path"],
                }),
            ),
            "preview" => (
                "Preview rows",
                "Rows of a table in its own order, as arrays in the order of `columns`: \
                 `rows` of them (default 20) from `offset`, optionally only some columns.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": path,
                        "rows": {
                            "type": "integer", "minimum": 1, "maximum": max_rows,
                            "description": "Rows to return (default 20).",
                        },
                        "offset": {
                            "type": "integer", "minimum": 0,
                            "description": "Rows to skip first.",
                        },
                        "columns": {
                            "type": "array", "items": { "type": "string" },
                            "description": "Only these columns.",
                        },
                        "format": format,
                        "json_path": json_path,
                    },
                    "required": ["path"],
                }),
            ),
            "profile" => (
                "Profile columns",
                "Statistics for each column from a bounded scan: null count and fraction, \
                 distinct values, min, max and a few sample values. Reads up to `scan` rows.",
                json!({
                    "type": "object",
                    "properties": {
                        "path": path,
                        "scan": {
                            "type": "integer", "minimum": 0,
                            "maximum": MAX_SCAN.max(self.limits.default_scan),
                            "description": format!(
                                "Rows to read (default {}). 0 reads only a Parquet file's \
                                 footer statistics.",
                                self.limits.default_scan
                            ),
                        },
                        "format": format,
                        "json_path": json_path,
                    },
                    "required": ["path"],
                }),
            ),
            "query" => (
                "Query with SQL",
                "Run one read-only SQL statement (SELECT, WITH, or EXPLAIN without ANALYZE) over \
                 the tables you name: `path` is the table `t`, and each of `tables` is queried by \
                 its name. A database's SQL runs on the database and names its own tables. \
                 Anything that writes is refused. Returns up to `rows` rows (default 100), each \
                 an array in the order of `columns`.",
                json!({
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string", "description": "The SQL statement." },
                        "path": path,
                        "tables": {
                            "type": "array",
                            "description": "Tables to query, each by its `name`.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": { "type": "string" },
                                    "path": path,
                                    "format": format,
                                    "json_path": json_path,
                                },
                                "required": ["name", "path"],
                            },
                        },
                        "rows": {
                            "type": "integer", "minimum": 1, "maximum": max_rows,
                            "description": "Rows to return (default 100).",
                        },
                        "format": format,
                        "json_path": json_path,
                    },
                    "required": ["sql"],
                }),
            ),
            _ => (
                "Browse catalogs",
                "Browse the Iceberg REST catalogs Lakeleto is configured with: with no \
                 `reference`, the catalogs; `catalog://<catalog>/`, its namespaces; \
                 `catalog://<catalog>/<namespace>/`, that namespace's namespaces and tables. A \
                 table's reference can be passed as `path` to the other tools.",
                json!({
                    "type": "object",
                    "properties": {
                        "reference": {
                            "type": "string",
                            "description": "What to list: `catalog://`, a catalog or a namespace.",
                        },
                    },
                }),
            ),
        };
        json!({
            "name": name,
            "title": title,
            "description": description,
            "inputSchema": schema,
            "annotations": { "readOnlyHint": true },
        })
    }

    /// How a table's `path` is given, for what this build and configuration can read.
    fn path_schema(&self) -> Value {
        let description = match &self.root {
            Some(_) => {
                "A file or table directory under the root, relative to it or absolute.".to_string()
            }
            None => {
                let mut kinds = vec!["a file or a table directory".to_string()];
                if cfg!(feature = "object-store") {
                    kinds.push("an object-store URI (s3://, gs://, az://)".to_string());
                }
                if cfg!(any(
                    feature = "sqlite",
                    feature = "postgres",
                    feature = "mysql"
                )) {
                    kinds.push(
                        "a database URI with the table, e.g. sqlite:///data/app.db?table=orders"
                            .to_string(),
                    );
                }
                if cfg!(feature = "catalog") {
                    kinds.push("a catalog://<catalog>/<namespace>/<table> reference".to_string());
                }
                let mut s = String::from("The table: ");
                s.push_str(&kinds.join(", "));
                s.push('.');
                s
            }
        };
        json!({ "type": "string", "description": description })
    }

    /// How `list`'s `path` is given, for what this configuration can list.
    fn list_path_help(&self) -> String {
        match &self.root {
            Some(_) => "A directory under the root (default: the root).".to_string(),
            None => "A directory (default: the server's working directory), an object-store \
                     prefix, a database URI, or a catalog:// namespace."
                .to_string(),
        }
    }
}

/// A `tools/call` result holding `value` as JSON text.
fn result(value: &Value, is_error: bool) -> Value {
    json!({
        "content": [{ "type": "text", "text": value.to_string() }],
        "isError": is_error,
    })
}

/// A `tools/call` result for a refused call: `isError`, with its kind and message as JSON.
fn refused(refusal: Refusal) -> Value {
    result(
        &json!({ "error": refusal.kind, "message": refusal.message }),
        true,
    )
}

/// `out` with as many leading `items` under `key` as fit in `max_bytes` of JSON, with the rest
/// of `out` and room for a note. Returns it and how many items were left out. When the rest of
/// `out` leaves no room by itself (a `columns` header of thousands of columns), that is a
/// `too_large` refusal: a result is never sent over the cap.
fn fit(
    mut out: Value,
    key: &str,
    items: Vec<Value>,
    max_bytes: usize,
) -> Result<(Value, usize), Refusal> {
    out[key] = Value::Array(Vec::new());
    let rest = out.to_string().len();
    let Some(mut room) = max_bytes.checked_sub(rest + NOTE_ROOM) else {
        return Err(Refusal {
            kind: "too_large",
            message: format!(
                "this result is {rest} bytes before any of its {key}, too close to the \
                 {max_bytes}-byte cap (--max-bytes) to hold them: ask for fewer columns, or \
                 restart the server with a larger --max-bytes"
            ),
        });
    };
    let total = items.len();
    let mut kept = Vec::with_capacity(total);
    for item in items {
        // Its JSON, and the comma before it.
        let size = item.to_string().len() + 1;
        if size > room {
            break;
        }
        room -= size;
        kept.push(item);
    }
    let cut = total - kept.len();
    out[key] = Value::Array(kept);
    Ok((out, cut))
}

/// Mark `out` as cut, saying how many `what` were left out to stay under the byte cap.
fn cut_note(out: &mut Value, cut: usize, what: &str, max_bytes: usize) {
    out["truncated"] = json!(true);
    out["note"] = json!(format!(
        "{cut} more {what} were left out to keep the result under {max_bytes} bytes"
    ));
}

/// `entry` without its null fields: a listing's directories have no format and no size.
fn without_nulls(mut entry: Value) -> Value {
    if let Some(map) = entry.as_object_mut() {
        map.retain(|_, v| !v.is_null());
    }
    entry
}

/// The size of the one local file `source` reads, if it reads one.
fn local_file_size(source: &Source) -> Option<u64> {
    if source.is_remote() || source.is_catalog() || source.format == Format::Database {
        return None;
    }
    let meta = std::fs::metadata(&source.path).ok()?;
    meta.is_file().then_some(meta.len())
}

// ---- arguments ---------------------------------------------------------------------------

/// Argument `key` as a string, if it is given.
fn string<'a>(args: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>, Refusal> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(Refusal::invalid(format!("`{key}` must be a string"))),
    }
}

/// Argument `key` as a string that isn't blank: a call without it is refused.
fn required<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, Refusal> {
    match string(args, key)? {
        Some(s) if !s.trim().is_empty() => Ok(s),
        _ => Err(Refusal::invalid(format!("`{key}` is required"))),
    }
}

/// A whole number, 0 or more. A numeral in a string is taken too: models send `"20"` as often
/// as `20`.
fn count(args: &Map<String, Value>, key: &str) -> Result<Option<usize>, Refusal> {
    let bad = || Refusal::invalid(format!("`{key}` must be a whole number, 0 or more"));
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => {
            let n = n.as_u64().ok_or_else(bad)?;
            Ok(Some(usize::try_from(n).unwrap_or(usize::MAX)))
        }
        Some(Value::String(s)) => s.trim().parse().map(Some).map_err(|_| bad()),
        Some(_) => Err(bad()),
    }
}

/// Argument `key` as a list of strings, if it is given.
fn strings(args: &Map<String, Value>, key: &str) -> Result<Option<Vec<String>>, Refusal> {
    let bad = || Refusal::invalid(format!("`{key}` must be a list of strings"));
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| item.as_str().map(str::to_string).ok_or_else(bad))
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(bad()),
    }
}
