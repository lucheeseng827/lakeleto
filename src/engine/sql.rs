//! The `sql` engine: DataFusion behind the same [`Engine`] trait.
//!
//! Feature-gated (`--features sql`) because DataFusion is a heavy compile; the default
//! build stays lean. When present it gives Lakeleto a real SQL planner over every format the local
//! reader opens — Parquet and CSV/TSV natively, JSON streamed through this crate's reader a pass
//! per query ([`streamed`]), Iceberg and Delta read through the local engine into memory —
//! `lakeleto query "SELECT ..."` — while `schema`/`head`/`profile` are expressed as SQL and
//! funnel back through the *same* [`profile_columns`](super::profile_columns) helper the
//! local engine uses, so stats never diverge between engines.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{
    cast, get_field, ident, lit, when, CsvReadOptions, Expr, ParquetReadOptions, SessionContext,
};
// `StreamExt::next` on the DataFusion batch stream — the streaming query path pulls one batch at a
// time rather than collecting.
use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use futures::StreamExt;

use self::positioned::Positioned;
use self::streamed::{retrying, At, Streams};
use super::flatten::{leaves, Leaf};
use super::{
    build_table_schema, profile_columns, truncate_batches, Capabilities, Engine, FilterOp,
    FilterSpec, NamedSource, RowBatch, RowStream, ScanResult, ScanSpec, SortSpec, TableProfile,
    TableSchema,
};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::format::{RemoteObject, SqlSupport};
use crate::source::{Flatten, Format, Source};

mod deep;
mod positioned;
mod streamed;

/// Table `t` again, with each row's place in the source as a column of its own ([`Positioned`]).
const POSITIONED: &str = "t_positioned";

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
        // Sized to the machine, not to 1. DataFusion's whole parallelism story is
        // partition-level: `target_partitions` defaults to the available core count and the
        // physical plan fans a scan/filter/aggregate across that many partitions — but every
        // one of those partitions is a task on THIS runtime, so a single worker thread
        // serialized them back into one core. That was a pure loss: the work was already
        // planned in parallel and then executed one partition at a time.
        //
        // `available_parallelism` respects cgroup CPU limits, which is what a container wants;
        // it only errs on platforms that cannot report, hence the `unwrap_or(1)` floor back to
        // the previous behaviour rather than a guess.
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .expect("build tokio runtime for DataFusion engine")
    })
}

/// How often the watchdog below wakes to re-read the context while a query runs.
///
/// 50 ms is chosen against what it is racing: a run that is going to be cancelled is by
/// definition one that has already run long enough for somebody to give up on it, so the
/// latency of noticing costs nothing, while a shorter interval would wake the runtime during
/// every fast query for no benefit.
const CANCEL_POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// Run `fut` to completion unless `ctx` says to stop first.
///
/// **What this genuinely buys, and what it does not.** Returning early drops `fut`, which drops
/// the DataFusion `RecordBatchStream` and with it the physical plan — so the scan stops pulling
/// and the operators are torn down. That is real cancellation of the pipeline, and it is
/// strictly more than the previous behaviour, which was none: a `block_on(collect())` ran to
/// completion no matter who had stopped waiting. What it does *not* promise is that every task
/// an operator spawned is dead the instant this returns; that is the operator's business, and
/// DataFusion's own `SpawnedTask` aborts on drop but a future operator need not. So: the query
/// is abandoned and its resources are released on the normal path, rather than "no work
/// continues, guaranteed".
///
/// A [`RequestContext::detached`] context skips the machinery entirely — no timer, no `select!`,
/// no extra wakeups — because it can never fire.
async fn guarded<T>(
    ctx: &RequestContext,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if !ctx.is_bounded() {
        return fut.await;
    }
    // A deadline that has already passed must not start the query at all.
    ctx.check()?;
    tokio::pin!(fut);
    loop {
        tokio::select! {
            // Biased so a ready result always wins the race against the timer: the query
            // finishing is the outcome we want, and polling it first means a fast query never
            // pays for the watchdog.
            biased;
            done = &mut fut => return done,
            _ = tokio::time::sleep(CANCEL_POLL) => ctx.check()?,
        }
    }
}

/// DataFusion-backed engine. Reads run on the shared static [`runtime`] via `block_on`, so callers
/// use the same synchronous [`Engine`] API as the local engine (no `async` leaks across the seam).
pub struct DataFusionEngine {
    /// The identity this engine's remote (`s3://`, `gs://`, `az://`) reads fall back to when the
    /// *call* names none.
    ///
    /// This field used to be the identity, and the reasoning for that — "a field rather than a
    /// per-call argument because [`Engine`] is a stable object-safe trait shared by every backend
    /// and must not grow a cloud-specific parameter" — is why the per-call channel is one neutral
    /// [`RequestContext`] instead of a cloud-shaped argument. Identity now travels on it; see
    /// [`crate::context`] for the configuration-vs-identity rule that admits it.
    ///
    /// What is left here is a **default** for the single-identity case: [`Self::new`] sets
    /// `Some(from_env())`, which is what every existing caller gets. `None` is *no ambient
    /// default* — see [`Self::without_ambient_identity`], which is how a shared multi-tenant
    /// engine is built.
    #[cfg(feature = "object-store")]
    default_store_options: Option<crate::objstore::StoreOptions>,
}

impl DataFusionEngine {
    pub fn new() -> Self {
        // Touch the runtime so it is built eagerly (a bad build surfaces here, not mid-request).
        let _ = runtime();
        Self {
            #[cfg(feature = "object-store")]
            default_store_options: Some(crate::objstore::StoreOptions::from_env()),
        }
    }

    /// An engine that falls back to `options` rather than to the environment when a call names no
    /// identity of its own.
    ///
    /// Prefer [`RequestContext::with_store_options`] for a per-caller identity — that is what lets
    /// one warm engine serve many tenants, and it is what the paid plane now does. This remains
    /// right for a process that reads as exactly one principal for its whole life.
    #[cfg(feature = "object-store")]
    pub fn with_store_options(options: crate::objstore::StoreOptions) -> Self {
        let _ = runtime();
        Self {
            default_store_options: Some(options),
        }
    }

    /// An engine with **no** ambient identity: a remote read is served only under an identity the
    /// [`RequestContext`] carries, and refused otherwise.
    ///
    /// The correct constructor for a shared, multi-tenant engine, and the reason one can now be
    /// shared at all. Defaulting to the environment instead would turn any bug that drops a
    /// tenant's vended credential into a successful read performed as the plane — the single
    /// outcome the credential seam exists to prevent.
    #[cfg(feature = "object-store")]
    pub fn without_ambient_identity() -> Self {
        let _ = runtime();
        Self {
            default_store_options: None,
        }
    }

    /// The fallback store configuration, if this engine has one. `None` means every remote read
    /// must bring its own identity.
    #[cfg(feature = "object-store")]
    pub fn default_store_options(&self) -> Option<&crate::objstore::StoreOptions> {
        self.default_store_options.as_ref()
    }

    /// The identity a remote read of `uri` runs under: the call's, else this engine's default,
    /// else a refusal. The twin of `LocalReaderEngine::identity`, and deliberately the same
    /// precedence — a query that registers one table through DataFusion and another through the
    /// local reader must resolve both the same way or it reads halves of itself as two principals.
    #[cfg(feature = "object-store")]
    fn identity<'a>(
        &'a self,
        ctx: &'a RequestContext,
        uri: &str,
    ) -> Result<&'a crate::objstore::StoreOptions> {
        if let Some(opts) = ctx.store_options() {
            return Ok(opts);
        }
        self.default_store_options.as_ref().ok_or_else(|| {
            EngineError::Forbidden(format!(
                "no credential identity for object-store read of `{uri}`: the request context \
                 carried none and this engine has no ambient default. Refusing rather than \
                 reading as the host process"
            ))
        })
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
    fn register_remote_store(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        path: &str,
    ) -> Result<()> {
        if !crate::source::is_object_uri(path) {
            return Ok(());
        }
        let url = url::Url::parse(path).map_err(|e| {
            EngineError::Query(format!("not a valid object-store URL `{path}`: {e}"))
        })?;
        let store = crate::objstore::store_for_url(path, self.identity(ctx, path)?)?;
        // Keyed on scheme + authority by DataFusion, so one registration serves every object in
        // the bucket; re-registering the same bucket replaces the entry, which is what a
        // re-registration with different options should do.
        session.register_object_store(&url, store);
        Ok(())
    }

    /// The object a streamed table reads from, looked up (a `HEAD`) under the same identity as every
    /// other remote read of this engine's: its version is fixed here, and every pass of a query
    /// asks for that version.
    #[cfg(feature = "object-store")]
    fn remote_object(
        &self,
        ctx: &RequestContext,
        source: &Source,
    ) -> Result<Arc<dyn RemoteObject>> {
        let uri = source.path.to_string_lossy();
        let opts = self.identity(ctx, uri.as_ref())?;
        Ok(Arc::new(crate::objstore::object_with(&uri, opts)?))
    }

    /// Without the `object-store` feature there is no store to look an object up in.
    #[cfg(not(feature = "object-store"))]
    fn remote_object(
        &self,
        _ctx: &RequestContext,
        _source: &Source,
    ) -> Result<Arc<dyn RemoteObject>> {
        Err(EngineError::missing_feature(
            "read an object-store URI",
            "object-store",
        ))
    }

    /// Without the `object-store` feature there is no store to register, so a remote URI gets the
    /// same targeted "rebuild with the feature" answer the local engine gives instead of
    /// DataFusion's opaque "No suitable object store found".
    #[cfg(not(feature = "object-store"))]
    fn register_remote_store(
        &self,
        _ctx: &RequestContext,
        _session: &SessionContext,
        path: &str,
    ) -> Result<()> {
        if crate::source::is_object_uri(path) {
            return Err(EngineError::missing_feature(
                "read an object-store URI",
                "object-store",
            ));
        }
        Ok(())
    }

    /// Register `table` on `session`. A streamed table joins `streams`, which notices when its
    /// file turns out wider than the schema it was registered with — see [`retrying`].
    fn register(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        table: &NamedSource,
        streams: &Streams,
    ) -> Result<()> {
        // Refuse by the predicate `capabilities()` reports from, so the list a caller routes on
        // and the refusal it would get here cannot disagree.
        if !crate::engine::sql_registers(table.source.format) {
            return Err(EngineError::unsupported_format(table.source.format, "sql"));
        }
        // DataFusion would read a `.csv.gz` as CSV, compressed bytes and all.
        table.source.require_uncompressed()?;
        let support = crate::format::reader(table.source.format).map(|reader| reader.sql());
        if let Some(SqlSupport::Streamed(pass)) = support {
            let at = if table.source.is_remote() {
                At::Object(self.remote_object(ctx, &table.source)?)
            } else {
                At::File(table.source.path.clone())
            };
            streamed::register(session, table, pass, at, streams)?;
            return match table.source.flatten {
                Some(flatten) => self.flatten_view(ctx, session, &table.name, flatten),
                None => Ok(()),
            };
        }
        let path = datafusion_path(&table.source.path.to_string_lossy());
        // DataFusion's register_csv validates the path against CsvReadOptions.file_extension
        // (default ".csv") and rejects anything else — a `.tsv` (or `--format tsv` over any name)
        // errors with "File path '...' does not match the expected extension '.csv'". Tell it the
        // file's real extension so the delimiter-driven reader accepts it. An extensionless path
        // gets "" (ends_with("") is always true → no gate), which is what we want for an explicit
        // single file.
        // DataFusion's listing tables don't cover what the local engine reads specially:
        //   • Iceberg and Delta — no native provider (we deliberately avoid iceberg-datafusion's
        //     old pin);
        //   • a Hive-partitioned Parquet *directory* — `register_parquet` reads the data files but
        //     drops the `key=value` partition columns.
        // Read those through the local engine into an in-memory table so SQL sees the same schema
        // and rows the grid does. Trade-off: the table is materialized in memory — fine for the
        // explorer's tables, not a streaming path.
        let via_local = matches!(table.source.format, Format::Iceberg | Format::Delta)
            || (matches!(table.source.format, Format::Parquet) && table.source.path.is_dir());
        if via_local {
            // Flattened as the local engine reads it, if it is to be — nothing more to do here.
            return self.register_via_local(ctx, session, table);
        }
        // Must precede the register_* call below: DataFusion resolves the store while *listing*
        // the URL, so a registry that does not yet know the bucket fails the registration itself.
        self.register_remote_store(ctx, session, &path)?;
        let ext = std::path::Path::new(&path)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{e}"))
            .unwrap_or_default();
        // Registration lists the table's files, which for a wide prefix is itself real I/O —
        // so it is guarded too, not just the query that follows.
        runtime().block_on(guarded(ctx, async {
            match table.source.format {
                Format::Parquet => session
                    .register_parquet(&table.name, &path, ParquetReadOptions::default())
                    .await
                    .map_err(|e| EngineError::Query(e.to_string())),
                Format::Csv | Format::Tsv => session
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
        }))?;
        match table.source.flatten {
            Some(flatten) => self.flatten_view(ctx, session, &table.name, flatten),
            None => Ok(()),
        }
    }

    /// Swap table `name` for a view of it with its struct columns flattened: the columns, names
    /// and nulls [`leaves`] gives every engine, as a projection DataFusion plans — so the table
    /// still streams, and a grid sort or filter still runs over all of it rather than a window.
    fn flatten_view(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        name: &str,
        flatten: Flatten,
    ) -> Result<()> {
        runtime().block_on(guarded(ctx, async {
            let query = |e: datafusion::error::DataFusionError| EngineError::Query(e.to_string());
            let table = session.table(name).await.map_err(query)?;
            let schema = table.schema().as_arrow().clone();
            let columns = leaves(&schema, flatten)?;
            if columns.iter().all(|c| c.path.len() == 1) {
                return Ok(()); // no struct to spread
            }
            let columns = columns
                .iter()
                .map(leaf_expr)
                .collect::<datafusion::error::Result<Vec<_>>>()
                .map_err(query)?;
            let view = table.select(columns).map_err(query)?.into_view();
            session.deregister_table(name).map_err(query)?;
            session.register_table(name, view).map_err(query)?;
            Ok(())
        }))
    }

    /// Register a source that DataFusion can't list natively (Iceberg, Delta, a partitioned Parquet
    /// dir) by reading it fully through the local engine into a [`MemTable`]. The local engine
    /// already resolves snapshots and Hive partition columns, and applies the source's flattening,
    /// so SQL gets the schema and rows the grid shows.
    fn register_via_local(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        table: &NamedSource,
    ) -> Result<()> {
        use datafusion::datasource::MemTable;
        // Hand this engine's FALLBACK down, `None` included. The caller's own identity already
        // reaches the local reader on `ctx`, so this is only about what happens when the call
        // named none — and the two engines have to agree on that, or a query joining a Parquet
        // table (registered through DataFusion) to an Iceberg one could resolve its two halves as
        // two different principals. Passing the default through, rather than letting
        // `LocalReaderEngine::default()` reinstate the environment, is what keeps them equal:
        // a plane engine built with no ambient identity must not acquire one here.
        #[cfg(feature = "object-store")]
        let local = match &self.default_store_options {
            Some(opts) => {
                crate::engine::local::LocalReaderEngine::default().with_store_options(opts.clone())
            }
            // Spelled out rather than defaulted: `LocalReaderEngine::default()` would reinstate
            // the environment here, and an engine built with no ambient identity must not acquire
            // one by being wrapped.
            None => crate::engine::local::LocalReaderEngine::default().without_ambient_identity(),
        };
        #[cfg(not(feature = "object-store"))]
        let local = crate::engine::local::LocalReaderEngine::default();
        let rb = local.preview(ctx, &table.source, usize::MAX)?; // full read (no row cap)
        let mem = MemTable::try_new(rb.schema.clone(), vec![rb.batches])
            .map_err(|e| EngineError::Query(e.to_string()))?;
        session
            .register_table(table.name.as_str(), Arc::new(mem))
            .map_err(|e| EngineError::Query(e.to_string()))?;
        Ok(())
    }

    /// Register a single source as table `t` for the schema/head/profile helpers.
    fn session_for(
        &self,
        ctx: &RequestContext,
        source: &Source,
        streams: &Streams,
    ) -> Result<SessionContext> {
        let session = streams.session();
        self.register(
            ctx,
            &session,
            &NamedSource {
                name: "t".to_string(),
                source: source.clone(),
            },
            streams,
        )?;
        Ok(session)
    }

    /// A session with every one of `tables` registered.
    fn session_with(
        &self,
        ctx: &RequestContext,
        tables: &[NamedSource],
        streams: &Streams,
    ) -> Result<SessionContext> {
        let session = streams.session();
        for t in tables {
            self.register(ctx, &session, t, streams)?;
        }
        Ok(session)
    }

    /// A sorted window every run agrees on: rows by key, and rows with equal keys in the order the
    /// source holds them.
    ///
    /// The parallel plan cannot give that by itself. Its partitions' TopKs share one strict
    /// threshold filter, so which of several equal keys survive depends on which partition got
    /// there first; its merge breaks ties round-robin by poll count; and even one TopK orders equal
    /// keys its own way for each LIMIT, so a page two could repeat page one.
    ///
    /// A CSV or TSV is sorted by key and then by each row's place in the file, which its scan
    /// numbers as it reads ([`Positioned`]). No two rows share both, so the parallel plan has one
    /// right answer, in one pass whether or not a tie touches the window.
    ///
    /// Any other table is asked of the parallel plan first, for one row either side of the window.
    /// When no two adjacent keys in what comes back are equal, every row in the window holds a key
    /// no other row has, so it is the only right answer — and a sort by a column of distinct values
    /// (an id, a timestamp) stays as fast as it was. Only when a tie touches the window is it read
    /// again on one partition, ties broken by a row number taken in the source's order. Measured on
    /// 5 million rows and 4 cores, that second read cost ~1 s on a 146 MB CSV, when a CSV was read
    /// this way, and ~0.5 s on Parquet, which loses the TopK's pruning to the row number — as it
    /// would on every sort, tied or not, were its rows numbered ([`sorts_in_file_order`]).
    ///
    /// A `streamed` table skips the probe. Each read of it decodes the whole file on one thread,
    /// so the probe is a pass of its own that a tie would add to, rather than a parallel scan
    /// cheaper than the read on one partition.
    ///
    /// A window of any table that reaches past [`deep::DEEP`] rows is read in two passes instead,
    /// which keep a sample of the rows and a band around the window rather than every row up to
    /// its end ([`deep`]). They sort by the same key and place: the place a listing table's scan
    /// numbers on the parallel plan, or the one `row_number()` gives any other table's rows on one
    /// partition ([`Numbering`]). If `spill`, the first pass writes the rows it reads to disk and
    /// the second reads them back, rather than reading the table again: a streamed table's, which
    /// would decode its file again, and a CSV or TSV object's, which would download it again.
    ///
    /// Each read but a probe counts the rows that match, and says how many beside the window: the
    /// two passes in the first of them, a read in one pass by its TopK, which takes in every row
    /// that matches to keep the best of them, as its plan reports ([`rows_into_topk`]). A probe's
    /// TopK may take in fewer, as a Parquet scan under it skips row groups by the threshold it
    /// hands down.
    #[allow(clippy::too_many_arguments)]
    fn sorted_window(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        columns: &[String],
        where_sql: &str,
        sort: &SortSpec,
        spec: &ScanSpec,
        read: SortedRead,
        spill: bool,
    ) -> Result<(RowBatch, Option<usize>)> {
        let taken = self.column_names(ctx, session)?;
        let key = quote_ident(&sort.column);
        let order = deep::order_sql(sort.descending);
        let numbering = deep::is_deep(spec.offset, spec.limit)
            .then(|| self.numbering(ctx, session))
            .transpose()?;
        let position = unused_name(&taken, "position");
        if read == SortedRead::InFileOrder || numbering == Some(Numbering::AsRead) {
            self.register_positioned(ctx, session, &position)?;
        }
        let position = quote_ident(&position);
        if let Some(numbering) = numbering {
            let key_alias = quote_ident(&unused_name(&taken, "key"));
            let numbered = match numbering {
                Numbering::AsRead => format!("{POSITIONED}{where_sql}"),
                Numbering::OnOnePartition => {
                    self.one_partition(ctx, session)?;
                    format!("(SELECT *, row_number() OVER () AS {position} FROM t{where_sql})")
                }
            };
            let keyed = format!("{key} AS {key_alias}, {position} FROM {numbered}");
            let found = runtime().block_on(guarded(
                ctx,
                deep::window(
                    session,
                    &format!("SELECT {keyed}"),
                    &format!("SELECT {}, {keyed}", select_list(columns)),
                    columns.len(),
                    sort.descending,
                    spec.offset,
                    spec.limit,
                    deep::SAMPLE,
                    spill,
                ),
            ))?;
            if let Some((window, matched)) = found {
                return Ok((window, Some(matched)));
            }
        }
        if read == SortedRead::InFileOrder {
            return self.collect_counted(
                ctx,
                session,
                &format!(
                    "SELECT {} FROM {POSITIONED}{where_sql} ORDER BY {key} {order}, {position} \
                     LIMIT {} OFFSET {}",
                    select_list(columns),
                    spec.limit,
                    spec.offset
                ),
            );
        }
        if read == SortedRead::Probed {
            let lead = spec.offset.min(1);
            let key_alias = unused_name(&taken, "key");
            let probe = self.collect_sql(
                ctx,
                session,
                &format!(
                    "SELECT {}, {key} AS {} FROM t{where_sql} ORDER BY {key} {order} LIMIT {} \
                     OFFSET {}",
                    select_list(columns),
                    quote_ident(&key_alias),
                    spec.limit + lead + 1,
                    spec.offset - lead
                ),
            )?;
            if let Some(window) = untied_window(probe, &key_alias, lead, spec.limit, columns.len())?
            {
                return Ok((window, None));
            }
        }
        self.one_partition(ctx, session)?;
        let row = quote_ident(&unused_name(&taken, "row"));
        self.collect_counted(
            ctx,
            session,
            &format!(
                "SELECT {} FROM (SELECT *, row_number() OVER () AS {row} FROM t{where_sql}) \
                 ORDER BY {key} {order}, {row} LIMIT {} OFFSET {}",
                select_list(columns),
                spec.limit,
                spec.offset
            ),
        )
    }

    /// How a deep window over table `t` numbers its rows ([`Numbering`]): as read, if `t` is a
    /// listing table, and on one partition if not.
    fn numbering(&self, ctx: &RequestContext, session: &SessionContext) -> Result<Numbering> {
        runtime().block_on(guarded(ctx, async {
            let table = session
                .table_provider("t")
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok(if table.is::<ListingTable>() {
                Numbering::AsRead
            } else {
                Numbering::OnOnePartition
            })
        }))
    }

    /// Register table `t` again as [`POSITIONED`], with each row's place in the source in a column
    /// called `name` that none of its own is ([`Positioned`]).
    fn register_positioned(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        name: &str,
    ) -> Result<()> {
        runtime().block_on(guarded(ctx, async {
            let query = |e: datafusion::error::DataFusionError| EngineError::Query(e.to_string());
            let table = session.table_provider("t").await.map_err(query)?;
            session
                .register_table(POSITIONED, Arc::new(Positioned::new(table, name)))
                .map_err(query)?;
            Ok(())
        }))
    }

    /// Plan the rest of `session`'s queries on one partition, so a scan reads the source in its own
    /// order. The tables stay registered: nothing is read twice to get here.
    fn one_partition(&self, ctx: &RequestContext, session: &SessionContext) -> Result<()> {
        self.collect_sql(
            ctx,
            session,
            "SET datafusion.execution.target_partitions = 1",
        )?;
        Ok(())
    }

    /// The column names of table `t`, in order.
    fn column_names(&self, ctx: &RequestContext, session: &SessionContext) -> Result<Vec<String>> {
        runtime().block_on(guarded(ctx, async {
            let table = session
                .table("t")
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok(table
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect())
        }))
    }

    fn collect_sql(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        sql: &str,
    ) -> Result<RowBatch> {
        runtime().block_on(guarded(ctx, async {
            let (schema, plan, task) = physical(session, sql, None).await?;
            let batches = datafusion::physical_plan::collect(plan, task)
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok(RowBatch { schema, batches })
        }))
    }

    /// [`Self::collect_sql`] for a query that ends in a TopK, with how many rows the TopK took in
    /// ([`rows_into_topk`]).
    fn collect_counted(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        sql: &str,
    ) -> Result<(RowBatch, Option<usize>)> {
        runtime().block_on(guarded(ctx, async {
            let (schema, plan, task) = physical(session, sql, None).await?;
            let batches = datafusion::physical_plan::collect(plan.clone(), task)
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok((RowBatch { schema, batches }, rows_into_topk(&plan)))
        }))
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
            // Not `readable_formats()`: that is the local reader's set, and this one is what
            // `register` accepts — the list the API routes grid windows on. Today they coincide.
            formats: crate::engine::sql_readable_formats(),
            sql: true,
            profile: true,
            remote: false,
            // `scan` is overridden (WHERE / ORDER BY / LIMIT+OFFSET pushed into DataFusion).
            // `stats` is not, so it inherits the default that drops the caller's filters —
            // reported honestly rather than advertised as working.
            scan: true,
            filtered_stats: false,
        }
    }

    fn schema(&self, ctx: &RequestContext, source: &Source) -> Result<TableSchema> {
        // Registering reads no rows, so nothing here can widen.
        let session = self.session_for(ctx, source, &Streams::default())?;
        let arrow_schema = runtime().block_on(guarded(ctx, async {
            let df = session
                .table("t")
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok::<SchemaRef, EngineError>(Arc::new(df.schema().as_arrow().clone()))
        }))?;
        Ok(build_table_schema(source, self.name(), None, &arrow_schema))
    }

    fn preview(&self, ctx: &RequestContext, source: &Source, limit: usize) -> Result<RowBatch> {
        retrying(ctx, |streams| {
            let session = self.session_for(ctx, source, streams)?;
            let rb = self.collect_sql(ctx, &session, &format!("SELECT * FROM t LIMIT {limit}"))?;
            Ok(RowBatch {
                schema: rb.schema,
                batches: truncate_batches(rb.batches, limit),
            })
        })
    }

    fn profile(
        &self,
        ctx: &RequestContext,
        source: &Source,
        scan_limit: usize,
    ) -> Result<TableProfile> {
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
        let rb = retrying(ctx, |streams| {
            let session = self.session_for(ctx, source, streams)?;
            self.collect_sql(
                ctx,
                &session,
                &format!("SELECT * FROM t LIMIT {scan_limit}"),
            )
        })?;
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

    fn query(&self, ctx: &RequestContext, sql: &str, tables: &[NamedSource]) -> Result<RowBatch> {
        // Lakeleto is an *explorer*: user SQL must never mutate. Reject anything that isn't a
        // read query (guard ported from module_62/src/sql.rs).
        ensure_read_only(sql)?;
        retrying(ctx, |streams| {
            let session = self.session_with(ctx, tables, streams)?;
            self.collect_sql(ctx, &session, sql)
        })
    }

    /// Bounded query with the cap pushed **into the plan** (`DataFrame::limit`), so a
    /// `SELECT *` over a huge table materializes at most `cap` rows instead of buffering the
    /// full result and trimming afterwards.
    fn query_capped(
        &self,
        ctx: &RequestContext,
        sql: &str,
        tables: &[NamedSource],
        cap: usize,
    ) -> Result<RowBatch> {
        ensure_read_only(sql)?;
        retrying(ctx, |streams| {
            let session = self.session_with(ctx, tables, streams)?;
            runtime().block_on(guarded(ctx, async {
                let (schema, plan, task) = physical(&session, sql, Some(cap)).await?;
                let batches = datafusion::physical_plan::collect(plan, task)
                    .await
                    .map_err(|e| EngineError::Query(e.to_string()))?;
                Ok(RowBatch { schema, batches })
            }))
        })
    }

    /// [`Engine::query_capped`] without buffering the answer — the real one, see [`RowStream`].
    ///
    /// The plan is built, the stream opened and its first batch pulled **synchronously**, before
    /// returning, so a bad query is an `Err` from this call rather than a surprise in the first
    /// `next()`. A caller that got `Ok` has a schema and a running plan. Pulling the first batch is
    /// also what lets a streamed table that widened its schema be planned again ([`retrying`]):
    /// until a batch has reached the caller, nothing would be handed over twice.
    ///
    /// Each `next()` then drives one batch under [`guarded`], which is what makes the deadline and
    /// the cancellation *better* here than on the buffered path rather than merely preserved: on
    /// `query_capped` the watchdog races one long `collect()`, so cancelling drops a result that
    /// was entirely unbuilt. Here everything already handed to the caller has been delivered, and
    /// stopping costs only the batch in flight. Streaming and cancellation are the same mechanism
    /// seen from two sides: both need the work to be divisible.
    ///
    /// The cap stays pushed into the plan (`DataFrame::limit`), so `SELECT *` over a huge table is
    /// bounded by the planner and not by the consumer remembering to stop pulling.
    fn query_stream(
        &self,
        ctx: &RequestContext,
        sql: &str,
        tables: &[NamedSource],
        cap: Option<usize>,
    ) -> Result<RowStream> {
        ensure_read_only(sql)?;
        retrying(ctx, |streams| {
            self.open_stream(ctx, sql, tables, cap, streams)
        })
    }

    /// Grid scan with the filter/sort/window/projection **pushed into DataFusion** — WHERE,
    /// ORDER BY (DataFusion's external, spilling sort), LIMIT/OFFSET, and a `count(*)` for the
    /// exact match total. Unlike the local engine this is *not* bounded by a working set, so
    /// sort/filter over files larger than `scan_cap` is correct and complete.
    fn scan(&self, ctx: &RequestContext, source: &Source, spec: &ScanSpec) -> Result<ScanResult> {
        retrying(ctx, |streams| self.scan_with(ctx, source, spec, streams))
    }
}

impl DataFusionEngine {
    /// [`Engine::query_stream`] once: plan `sql` over `tables`, open its stream and pull the first
    /// batch.
    fn open_stream(
        &self,
        ctx: &RequestContext,
        sql: &str,
        tables: &[NamedSource],
        cap: Option<usize>,
        streams: &Streams,
    ) -> Result<RowStream> {
        let session = self.session_with(ctx, tables, streams)?;
        let (schema, mut stream) = runtime().block_on(guarded(ctx, async {
            let (schema, plan, task) = physical(&session, sql, cap).await?;
            let stream = datafusion::physical_plan::execute_stream(plan, task)
                .map_err(|e| EngineError::Query(e.to_string()))?;
            Ok((schema, stream))
        }))?;
        let mut first = next_batch(ctx, &mut stream)?;

        // `session` is moved into the closure and kept alive for as long as the stream is pulled.
        // Dropping it at the end of this function would tear down the registered tables and the
        // runtime env the plan is still reading through — the plan holds what it needs, but the
        // object stores registered on the session's `RuntimeEnv` are reached *through* it, so an
        // `s3://` query would start failing partway. Holding it here is the lifetime the streaming
        // shape requires and the buffered one did not.
        let ctx = ctx.clone();
        let mut done = first.is_none();
        Ok(RowStream::new(
            schema,
            std::iter::from_fn(move || {
                if let Some(batch) = first.take() {
                    return Some(Ok(batch));
                }
                if done {
                    return None;
                }
                let _keep_session_alive = &session;
                match next_batch(&ctx, &mut stream) {
                    Ok(Some(b)) => Some(Ok(b)),
                    Ok(None) => {
                        done = true;
                        None
                    }
                    // One error, then the stream ends. Returning `Some(Err(..))` forever would make
                    // `for b in stream` an infinite loop on a failure — the kind of bug that only
                    // shows up under the conditions you least want an infinite loop in.
                    Err(e) => {
                        done = true;
                        Some(Err(e))
                    }
                }
            }),
        ))
    }

    /// [`Engine::scan`] once, over a table registered in `streams`.
    fn scan_with(
        &self,
        ctx: &RequestContext,
        source: &Source,
        spec: &ScanSpec,
        streams: &Streams,
    ) -> Result<ScanResult> {
        // The grid pages by re-running this query at a new OFFSET, which is only right if every run
        // puts every row in the same place — and DataFusion's parallel plan promises no order
        // beyond the ORDER BY. Unsorted, it delivers rows in whatever order its partitions finish;
        // sorted, it leaves ties to timing. Each window is pinned down below.
        let session = self.session_for(ctx, source, streams)?;
        let where_sql = build_where(&spec.filters);

        // A count has no order, so it keeps the parallel plan. A table read in passes is counted
        // while its window is read: each is a pass over the file on one thread, so side by side
        // the two take about as long as one. Sorted, it is counted by its window's own pass, which
        // numbers every row that matches — for an object in a store, a pass of the count's own
        // would download it twice. So is a sorted CSV or TSV, by the one pass that sorts it in
        // file order, which takes in every row that matches to keep the best of them; and a
        // window deep into any table, by the first of the two passes that read it ([`deep`]). Any
        // other table is counted first: its plan already spreads across every core, and two at
        // once measured slower than one after the other (a tied sort over a 146 MB CSV, 1.8 s
        // against 2.4 s).
        let read = if streams.any() {
            SortedRead::Streamed
        } else if sorts_in_file_order(source) {
            SortedRead::InFileOrder
        } else {
            SortedRead::Probed
        };
        let count_sql = format!("SELECT count(*) AS c FROM t{where_sql}");
        let count = match &spec.sort {
            Some(_) if read != SortedRead::Probed || deep::is_deep(spec.offset, spec.limit) => {
                Count::ByWindow
            }
            None if read == SortedRead::Streamed => {
                Count::Running(self.spawn_sql(ctx, &session, &count_sql)?)
            }
            _ => Count::Taken(self.collect_sql(ctx, &session, &count_sql)?),
        };

        let columns = match &spec.projection {
            Some(cols) if !cols.is_empty() => cols.clone(),
            _ => self.column_names(ctx, &session)?,
        };
        let (batch, counted) = match &spec.sort {
            // One partition reads the source in its own order.
            None => {
                self.one_partition(ctx, &session)?;
                let window = self.collect_sql(
                    ctx,
                    &session,
                    &format!(
                        "SELECT {} FROM t{where_sql} LIMIT {} OFFSET {}",
                        select_list(&columns),
                        spec.limit,
                        spec.offset
                    ),
                )?;
                (window, None)
            }
            Some(sort) => {
                let spill = read.keeps_first_pass(source);
                self.sorted_window(ctx, &session, &columns, &where_sql, sort, spec, read, spill)?
            }
        };
        let matched = match count {
            Count::Taken(rows) => count_value(&rows),
            Count::Running(running) => count_value(&running.join(ctx)?),
            // Passes whose plans do not say how many rows they took in are followed by a count.
            Count::ByWindow => match counted {
                Some(n) => n,
                None => count_value(&self.collect_sql(ctx, &session, &count_sql)?),
            },
        };
        Ok(ScanResult {
            batch,
            matched_rows: matched,
            total_known: true,
            scanned_rows: matched,
            bounded: false,
            offset: spec.offset,
        })
    }

    /// Plan `sql` on `session` and start it on the runtime, for [`Running::join`] to collect, so
    /// that another query can run meanwhile. It is planned before this returns, so a setting a
    /// later query changes ([`Self::one_partition`]) does not reach it.
    fn spawn_sql(
        &self,
        ctx: &RequestContext,
        session: &SessionContext,
        sql: &str,
    ) -> Result<Running> {
        let (schema, plan, task) =
            runtime().block_on(guarded(ctx, physical(session, sql, None)))?;
        let task = runtime().spawn(datafusion::physical_plan::collect(plan, task));
        Ok(Running { schema, task })
    }
}

/// A query running in the background ([`DataFusionEngine::spawn_sql`]). Dropped without being
/// joined — the query beside it failed, or its caller gave up — it is aborted, which drops its
/// plan and so stops any pass it was making.
struct Running {
    schema: SchemaRef,
    task: tokio::task::JoinHandle<datafusion::error::Result<Vec<RecordBatch>>>,
}

impl Running {
    /// Its rows, once it finishes, under `ctx`'s deadline and cancellation.
    fn join(mut self, ctx: &RequestContext) -> Result<RowBatch> {
        let batches = runtime().block_on(guarded(ctx, async {
            (&mut self.task)
                .await
                .map_err(|e| EngineError::Query(e.to_string()))?
                .map_err(|e| EngineError::Query(e.to_string()))
        }))?;
        Ok(RowBatch {
            schema: self.schema.clone(),
            batches,
        })
    }
}

impl Drop for Running {
    /// Abort the query if it is still running: nothing is left to read its rows.
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// How a sorted grid window over a table is read ([`DataFusionEngine::sorted_window`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortedRead {
    /// In one parallel pass, by key and then by each row's place in the file: a CSV or TSV
    /// ([`sorts_in_file_order`]).
    InFileOrder,
    /// A parallel probe, then a read on one partition when a tie touches the window.
    Probed,
    /// A read on one partition alone: a streamed table is read a pass on one thread anyway.
    Streamed,
}

impl SortedRead {
    /// Whether a deep window over `source`, read this way, keeps what its first pass reads on disk
    /// for the second to read back, rather than reading the table twice ([`deep`]): where reading
    /// it again costs more. A streamed table's pass decodes its file on one thread, and a CSV or
    /// TSV object's downloads it. A local file's is a parallel read of a file the page cache
    /// holds, and a Parquet object's first pass downloads only the columns it sorts and filters by.
    fn keeps_first_pass(self, source: &Source) -> bool {
        self == SortedRead::Streamed || (self == SortedRead::InFileOrder && source.is_remote())
    }
}

/// How a deep window numbers table `t`'s rows in the source's order, for its two passes to break
/// ties by ([`deep`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Numbering {
    /// As the parallel scan reads them ([`Positioned`]): a listing table's — a CSV, TSV or Parquet
    /// file's — which DataFusion reads in order, a range at a time.
    AsRead,
    /// By `row_number()` on one partition, as a probe's tie is read: any other table's. A streamed
    /// table is read on one thread anyway. A view, as a flattened table is, may deal its rows out
    /// among partitions above the scan, before any numbering on top of it sees them; and a table
    /// read into memory is spread over partitions by the size of its batches, not their order.
    OnOnePartition,
}

/// A grid window's count of the rows that match it ([`DataFusionEngine::scan_with`]).
enum Count {
    /// Taken before the window was read.
    Taken(RowBatch),
    /// Running while the window is read.
    Running(Running),
    /// Taken by the sorted window's own passes, which see every row that matches
    /// ([`DataFusionEngine::sorted_window`]).
    ByWindow,
}

/// How many rows the TopK in `plan` took in, once `plan` has run, as the operator under it reports
/// sending them. A sort reads all it is sent, a TopK as much as a full sort, which is every row
/// that matches unless something under it dropped rows by the threshold a TopK hands down: a
/// Parquet scan under a probe does ([`sorts_in_file_order`]), but no window function or numbered
/// scan lets it past, so a read that numbers its rows takes in every one. `None` if the plan has
/// no sort — a sort by a key the filter holds to one value is planned away, and its read stops at
/// the window — or the operator under it does not report.
fn rows_into_topk(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    match plan.downcast_ref::<SortExec>() {
        Some(sort) => sort.input().metrics()?.output_rows(),
        None => plan.children().into_iter().find_map(rows_into_topk),
    }
}

/// Whether a sorted window over `source` is read in file order: in one pass, by key and then by
/// each row's place in the file ([`Positioned`]). Numbering its rows keeps a TopK's threshold from
/// the scan, which a CSV or TSV scan has no use for anyway; a Parquet scan skips row groups by it,
/// so numbering would cost it that on every sort to save the pass only a tie needs. (A deep window
/// numbers a Parquet file's rows all the same: its two passes have no TopK — [`Numbering`].)
fn sorts_in_file_order(source: &Source) -> bool {
    matches!(source.format, Format::Csv | Format::Tsv)
}

/// `sql` planned on `session` as far as a physical plan — at most `cap` rows of it, if given — with
/// its output schema and the task context to run it in.
///
/// Split from running it because DataFusion checks a query's types while it plans it, after
/// [`SessionContext::sql`] has returned: a failure anywhere here is the query's planning, not its
/// rows, and is noted for a table it streams to be checked before the error is final
/// ([`streamed::planning_failed`]).
async fn physical(
    session: &SessionContext,
    sql: &str,
    cap: Option<usize>,
) -> Result<(SchemaRef, Arc<dyn ExecutionPlan>, Arc<TaskContext>)> {
    let unplanned = |e: datafusion::error::DataFusionError| {
        streamed::planning_failed(session, &e);
        EngineError::Query(e.to_string())
    };
    let mut df = session.sql(sql).await.map_err(unplanned)?;
    if let Some(cap) = cap {
        df = df.limit(0, Some(cap)).map_err(unplanned)?;
    }
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let task = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.map_err(unplanned)?;
    Ok((schema, plan, task))
}

/// The next batch of `stream`, under `ctx`'s deadline and cancellation; `None` at its end.
fn next_batch(
    ctx: &RequestContext,
    stream: &mut SendableRecordBatchStream,
) -> Result<Option<RecordBatch>> {
    runtime().block_on(guarded(ctx, async {
        match stream.next().await {
            Some(Ok(b)) => Ok(Some(b)),
            Some(Err(e)) => Err(EngineError::Query(e.to_string())),
            None => Ok(None),
        }
    }))
}

/// One flattened column as an expression over its top-level column — `user['geo']['lat']` — and
/// null wherever a struct above it is. `get_field` alone does not give that: it returns a child's
/// values as stored, and Arrow lets a null struct keep values in its children, which the local
/// engine (through `StructArray::flatten`) never shows.
fn leaf_expr(leaf: &Leaf<'_>) -> datafusion::error::Result<Expr> {
    let mut value = ident(leaf.path[0]);
    let mut parent_null: Option<Expr> = None;
    for (name, nullable) in leaf.path[1..].iter().zip(&leaf.nullable_parents) {
        if *nullable {
            let null = value.clone().is_null();
            parent_null = Some(match parent_null {
                Some(before) => before.or(null),
                None => null,
            });
        }
        value = get_field(value, *name);
    }
    let value = match parent_null {
        Some(null) => when(null, typed_null(leaf.field.data_type())).otherwise(value)?,
        None => value,
    };
    Ok(value.alias(leaf.field.name()))
}

/// A null of `data_type`, so both arms of a `CASE` agree on their type without coercion.
fn typed_null(data_type: &DataType) -> Expr {
    ScalarValue::try_from(data_type)
        .map(lit)
        .unwrap_or_else(|_| cast(lit(ScalarValue::Null), data_type.clone()))
}

/// The window out of a probe — `lead` rows before it, the window, then one row after — when no two
/// adjacent keys in the probe are equal, so every row in the window has a key of its own; `None`
/// when a tie touches the window, or its keys cannot be compared. `width` leading columns are the
/// window's; the key column after them is dropped.
fn untied_window(
    probe: RowBatch,
    key: &str,
    lead: usize,
    limit: usize,
    width: usize,
) -> Result<Option<RowBatch>> {
    let all = arrow_select::concat::concat_batches(&probe.schema, &probe.batches)
        .map_err(|e| EngineError::Query(e.to_string()))?;
    let Some(keys) = all.column_by_name(key) else {
        return Ok(None);
    };
    let Ok(cmp) = arrow_ord::ord::make_comparator(keys.as_ref(), keys.as_ref(), Default::default())
    else {
        return Ok(None);
    };
    if (1..all.num_rows()).any(|i| cmp(i - 1, i).is_eq()) {
        return Ok(None);
    }
    let start = lead.min(all.num_rows());
    let window = all
        .slice(start, limit.min(all.num_rows() - start))
        .project(&(0..width).collect::<Vec<_>>())
        .map_err(|e| EngineError::Query(e.to_string()))?;
    Ok(Some(RowBatch {
        schema: window.schema(),
        batches: vec![window],
    }))
}

/// `columns`, quoted, for a `SELECT`.
fn select_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A column name for the scan's own use that none of `taken` has: `__lakeleto_{what}`, lengthened
/// until it is free.
fn unused_name(taken: &[String], what: &str) -> String {
    let mut name = format!("__lakeleto_{what}");
    while taken.contains(&name) {
        name.push('_');
    }
    name
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
    use crate::context::RequestContext;
    use crate::engine::Engine;
    use crate::error::EngineError;
    use crate::source::{Format, Source};

    /// `capabilities().formats` is what the API routes sorted and filtered grid windows on, so it
    /// has to agree with what `register` accepts, in both directions: a format it lists must
    /// register, and one it refuses must not be listed.
    #[test]
    fn capabilities_agree_with_what_register_accepts() {
        let dir = tempfile::tempdir().unwrap();
        // Pretty-printed, which DataFusion's own JSON reader cannot infer — this registers through
        // the local reader.
        let json = dir.path().join("t.json");
        std::fs::write(&json, "[\n  {\"id\": 1},\n  {\"id\": 2}\n]").unwrap();
        let csv = dir.path().join("t.csv");
        std::fs::write(&csv, "id\n1\n2\n").unwrap();

        let engine = DataFusionEngine::new();
        let formats = engine.capabilities().formats;
        let ctx = RequestContext::detached();

        for (format, path) in [(Format::Json, &json), (Format::Csv, &csv)] {
            assert!(
                formats.iter().any(|f| f == format.as_str()),
                "listed: {formats:?}"
            );
            let schema = engine
                .schema(&ctx, &Source::with_format(path, format))
                .expect("a listed format registers");
            assert_eq!(schema.columns.len(), 1, "{format}: {schema:?}");
        }

        // Not a file at all: never listed, and refused as a format.
        assert!(
            !formats.iter().any(|f| f == "database"),
            "listed: {formats:?}"
        );
        let db = Source::with_format("sqlite:///x.db?table=t", Format::Database);
        let err = engine.schema(&ctx, &db).unwrap_err();
        assert!(
            matches!(err, EngineError::UnsupportedFormat { .. }),
            "a database source should be refused as a format, got: {err}"
        );
    }

    /// Arrow lets a null struct keep values in its children, and `get_field` returns them as
    /// stored — so without the view's null guard a flattened field of a null struct would show a
    /// value that the local engine, and the struct itself, say is not there.
    #[test]
    fn a_flattened_field_of_a_null_struct_is_null() {
        use std::sync::Arc;

        use arrow_array::builder::NullBufferBuilder;
        use arrow_array::cast::AsArray;
        use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
        use arrow_schema::{DataType, Field, Fields, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        use crate::source::Flatten;

        let fields = Fields::from(vec![Field::new("name", DataType::Utf8, false)]);
        let mut valid = NullBufferBuilder::new(2);
        valid.append(true);
        valid.append(false);
        let user = StructArray::new(
            fields.clone(),
            vec![Arc::new(StringArray::from(vec!["Ada", "Ghost"])) as ArrayRef],
            valid.finish(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user", DataType::Struct(fields), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(user)],
        )
        .unwrap();
        let session = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        session.register_table("t", Arc::new(table)).unwrap();

        let engine = DataFusionEngine::new();
        let ctx = RequestContext::detached();
        let names = |sql: &str| -> Vec<Option<String>> {
            let rb = engine.collect_sql(&ctx, &session, sql).unwrap();
            rb.batches
                .iter()
                .flat_map(|b| {
                    let a = b.column(0).as_string::<i32>();
                    (0..a.len())
                        .map(|i| a.is_valid(i).then(|| a.value(i).to_string()))
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        // The fixture really does hold a name under the null struct: a bare field access shows
        // it. (Should DataFusion start folding parent nulls in, this fails first — and the guard
        // in `leaf_expr` is then redundant rather than wrong.)
        let bare = names("SELECT get_field(\"user\", 'name') FROM t ORDER BY id");
        assert_eq!(bare, [Some("Ada".into()), Some("Ghost".into())]);

        engine
            .flatten_view(&ctx, &session, "t", Flatten::All)
            .unwrap();
        let flat = names(r#"SELECT "user.name" FROM t ORDER BY id"#);
        assert_eq!(flat, [Some("Ada".into()), None]);
    }

    /// P3: a bare `SessionContext` cannot reach a bucket at all — `register_parquet("s3://…")`
    /// fails with "No suitable object store found" before any byte moves — so these assert on the
    /// *registry*, which needs no credentials and no network.
    #[cfg(feature = "object-store")]
    mod remote_store {
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::prelude::SessionContext;

        use super::DataFusionEngine;
        use crate::context::RequestContext;
        use crate::error::EngineError;
        use crate::objstore::{StoreCredentials, StoreOptions};

        fn bucket() -> ObjectStoreUrl {
            ObjectStoreUrl::parse("s3://bucket").unwrap()
        }

        /// A live provider for a family that is *not* S3. Nothing here makes a request: the point
        /// is that `objstore` rejects a mismatched provider instead of falling back to ambient
        /// credentials, which makes "was this identity actually used?" observable with no network
        /// and no credentials — an s3:// registration that errors mentioning GCS can only have
        /// gone through the options carrying this.
        fn gcs_provider() -> crate::objstore::StoreCredentials {
            let provider: object_store::gcp::GcpCredentialProvider = std::sync::Arc::new(
                object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
                    bearer: "not-a-real-token".to_string(),
                }),
            );
            StoreCredentials::Gcs(provider)
        }

        #[test]
        fn a_remote_url_gets_an_object_store_registered_on_the_session() {
            let session = SessionContext::new();
            // The bug, reproduced: nothing knows how to reach s3:// yet.
            assert!(session.runtime_env().object_store(bucket()).is_err());

            DataFusionEngine::new()
                .register_remote_store(
                    &RequestContext::detached(),
                    &session,
                    "s3://bucket/t.parquet",
                )
                .unwrap();
            assert!(session.runtime_env().object_store(bucket()).is_ok());
        }

        #[test]
        fn a_local_path_registers_nothing() {
            let session = SessionContext::new();
            DataFusionEngine::new()
                .register_remote_store(&RequestContext::detached(), &session, "/data/t.parquet")
                .unwrap();
            assert!(session.runtime_env().object_store(bucket()).is_err());
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
            assert_eq!(
                engine.default_store_options().unwrap().scope_id(),
                "tenant-a"
            );

            let session = SessionContext::new();
            let err = engine
                .register_remote_store(
                    &RequestContext::detached(),
                    &session,
                    "gs://bucket/t.parquet",
                )
                .unwrap_err();
            assert!(err.to_string().contains("S3"), "{err}");
        }

        /// The precedence rule, stated once in `identity` and asserted here: the call wins, then
        /// the engine's default, then a refusal.
        #[test]
        fn the_calls_identity_wins_over_the_engines_default() {
            let engine = DataFusionEngine::with_store_options(
                StoreOptions::empty().with_scope("engine-default"),
            );
            assert_eq!(
                engine
                    .identity(&RequestContext::detached(), "s3://b/t")
                    .unwrap()
                    .scope_id(),
                "engine-default"
            );

            let ctx = RequestContext::detached()
                .with_store_options(StoreOptions::empty().with_scope("tenant-a"));
            assert_eq!(
                engine.identity(&ctx, "s3://b/t").unwrap().scope_id(),
                "tenant-a"
            );
        }

        /// The override is a replacement, not a merge — and it is observable all the way down at
        /// the store builder, not just in the resolver.
        ///
        /// The engine's default would fail this registration (a GCS provider cannot sign for
        /// s3://). The call supplies credential-free options, and the registration succeeds. If
        /// the two were merged — or if the default were consulted at all once the call spoke —
        /// the GCS provider would still be in play and this would error. Merging is the dangerous
        /// reading: it would produce a third principal that neither the operator nor the caller
        /// asked to read as.
        #[test]
        fn a_calls_identity_replaces_the_default_rather_than_merging_with_it() {
            let engine = DataFusionEngine::with_store_options(
                StoreOptions::empty().with_credentials(gcs_provider()),
            );
            let session = SessionContext::new();
            let err = engine
                .register_remote_store(
                    &RequestContext::detached(),
                    &session,
                    "s3://bucket/t.parquet",
                )
                .unwrap_err();
            assert!(err.to_string().contains("GCS"), "{err}");

            let ctx = RequestContext::detached().with_store_options(StoreOptions::empty());
            let session = SessionContext::new();
            engine
                .register_remote_store(&ctx, &session, "s3://bucket/t.parquet")
                .expect("the call's credential-free identity replaces the engine's GCS provider");
            assert!(session.runtime_env().object_store(bucket()).is_ok());
        }

        /// An engine built for many tenants refuses a remote read it has no identity for, and the
        /// refusal is a 403 rather than a read performed as the host. Losing a vended credential
        /// has to fail loudly; succeeding as the plane is the one outcome the seam exists to stop.
        #[test]
        fn without_an_ambient_identity_an_unattributed_remote_read_is_refused() {
            let engine = DataFusionEngine::without_ambient_identity();
            assert!(engine.default_store_options().is_none());

            let session = SessionContext::new();
            let err = engine
                .register_remote_store(
                    &RequestContext::detached(),
                    &session,
                    "s3://bucket/t.parquet",
                )
                .unwrap_err();
            assert!(
                matches!(err, EngineError::Forbidden(_)),
                "expected a refusal, got {err:?}"
            );
            // Nothing was registered: the refusal precedes the store build, so no request is
            // ever signed with the wrong identity.
            assert!(session.runtime_env().object_store(bucket()).is_err());

            // ...and the same engine serves the same URL once the call brings an identity.
            let ctx = RequestContext::detached().with_store_options(StoreOptions::empty());
            engine
                .register_remote_store(&ctx, &session, "s3://bucket/t.parquet")
                .unwrap();
            assert!(session.runtime_env().object_store(bucket()).is_ok());
        }

        /// A local path needs no identity, so an engine with no ambient default still reads one.
        /// Otherwise a plane configured with `--compute-local-root` and no credential vendor
        /// would refuse work that involves no object store at all.
        #[test]
        fn a_local_path_needs_no_identity_even_with_no_ambient_default() {
            let session = SessionContext::new();
            DataFusionEngine::without_ambient_identity()
                .register_remote_store(&RequestContext::detached(), &session, "/data/t.parquet")
                .unwrap();
        }
    }

    /// Without the feature there is no store to register, so the SQL path must say so rather than
    /// letting DataFusion fail with "No suitable object store found".
    #[cfg(not(feature = "object-store"))]
    #[test]
    fn a_remote_url_without_the_feature_names_the_missing_feature() {
        use datafusion::prelude::SessionContext;
        let err = DataFusionEngine::new()
            .register_remote_store(
                &crate::context::RequestContext::detached(),
                &SessionContext::new(),
                "s3://bucket/t.parquet",
            )
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
        let err = DataFusionEngine::new()
            .profile(&RequestContext::detached(), &source, 0)
            .unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "got: {err}");
        // A positive scan limit still profiles fine.
        assert!(DataFusionEngine::new()
            .profile(&RequestContext::detached(), &source, 100)
            .is_ok());
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
        DataFusionEngine::new()
            .scan(&RequestContext::detached(), &source, &spec)
            .unwrap();
    }

    #[test]
    fn reads_tsv_via_sql_engine() {
        // DataFusion rejected non-.csv extensions ("does not match the expected extension
        // '.csv'"); the sql engine must read a .tsv (tab-delimited) file.
        let tsv = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/mini.tsv");
        let source = Source::with_format(tsv, Format::Tsv);
        let eng = DataFusionEngine::new();
        let schema = eng.schema(&RequestContext::detached(), &source).unwrap();
        // Tab-split gives the 4 real columns, not one giant column.
        assert_eq!(schema.columns.len(), 4, "schema: {schema:?}");
        let rb = eng
            .query(
                &RequestContext::detached(),
                "SELECT count(*) AS c FROM t",
                &[crate::engine::NamedSource {
                    name: "t".into(),
                    source,
                }],
            )
            .unwrap();
        assert_eq!(rb.num_rows(), 1);
    }

    /// The TopK a sorted window ends in hands the threshold its best rows set to the scan under
    /// it. A Parquet scan takes it, to skip row groups by; a CSV or TSV scan does not, so it loses
    /// nothing when its rows are numbered for a sort in file order, which keeps the threshold from
    /// the scan. `sorts_in_file_order` has to say which.
    #[test]
    fn a_topk_hands_its_threshold_to_a_parquet_scan_but_not_to_a_csv_one() {
        use crate::engine::NamedSource;
        use crate::source::{Format, Source};
        use arrow_array::{ArrayRef, Int64Array, RecordBatch};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("t.csv");
        std::fs::write(&csv, "v\n1\n2\n3\n").unwrap();
        let tsv = dir.path().join("t.tsv");
        std::fs::write(&tsv, "v\tw\n1\ta\n2\tb\n3\tc\n").unwrap();
        let parquet = dir.path().join("t.parquet");
        let v: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter([("v", v)]).unwrap();
        let file = std::fs::File::create(&parquet).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let eng = DataFusionEngine::new();
        let ctx = RequestContext::detached();
        for (path, format) in [
            (csv, Format::Csv),
            (tsv, Format::Tsv),
            (parquet, Format::Parquet),
        ] {
            let source = Source::with_format(&path, format);
            let table = [NamedSource {
                name: "t".into(),
                source: source.clone(),
            }];
            let plan = eng
                .query(
                    &ctx,
                    "EXPLAIN SELECT v FROM t ORDER BY v DESC LIMIT 2",
                    &table,
                )
                .unwrap();
            let shown = datafusion::arrow::util::pretty::pretty_format_batches(&plan.batches)
                .unwrap()
                .to_string();
            let scan_takes_it = shown.contains("DynamicFilter");
            assert_eq!(
                super::sorts_in_file_order(&source),
                !scan_takes_it,
                "{format:?}:\n{shown}"
            );
        }
    }

    /// A deep window keeps what its first pass reads on disk where reading the table again costs
    /// more: a streamed table's, decoded on one thread, and a CSV or TSV object's, downloaded. Not
    /// a local file's, nor a Parquet object's, whose first pass reads only the columns it sorts
    /// and filters by.
    #[test]
    fn a_deep_window_keeps_its_first_pass_where_reading_again_costs_more() {
        use super::SortedRead;
        let local = Source::with_format("t.csv", Format::Csv);
        let object = Source::with_format("s3://bucket/t.csv", Format::Csv);
        for (read, source, keeps) in [
            (SortedRead::Streamed, &local, true),
            (SortedRead::Streamed, &object, true),
            (SortedRead::InFileOrder, &local, false),
            (SortedRead::InFileOrder, &object, true),
            (SortedRead::Probed, &local, false),
            (SortedRead::Probed, &object, false),
        ] {
            assert_eq!(
                read.keeps_first_pass(source),
                keeps,
                "{read:?} over {}",
                source.uri()
            );
        }
    }

    /// A deep window numbers a file's rows as its parallel scan reads them — a CSV's, a Parquet
    /// file's — and any other table's on one partition: a streamed NDJSON file's, a view's
    /// flattening one, and a Parquet directory's, read into memory.
    #[test]
    fn a_deep_window_numbers_a_files_rows_as_read_and_any_other_tables_on_one_partition() {
        use super::{Numbering, Streams};
        use crate::source::Flatten;
        use arrow_array::{ArrayRef, Int64Array, RecordBatch};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("t.csv");
        std::fs::write(&csv, "v\n1\n2\n").unwrap();
        let ndjson = dir.path().join("t.ndjson");
        std::fs::write(&ndjson, "{\"v\": 1}\n{\"v\": 2}\n").unwrap();
        let nested = dir.path().join("nested.ndjson");
        std::fs::write(&nested, "{\"v\": {\"w\": 1}}\n").unwrap();
        let parquet = |path: &std::path::Path| {
            let v: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
            let batch = RecordBatch::try_from_iter([("v", v)]).unwrap();
            let file = std::fs::File::create(path).unwrap();
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        };
        let file = dir.path().join("t.parquet");
        parquet(&file);
        let parts = dir.path().join("parts.parquet");
        std::fs::create_dir(&parts).unwrap();
        parquet(&parts.join("part-0.parquet"));

        let eng = DataFusionEngine::new();
        let ctx = RequestContext::detached();
        let flattened = Source::detect(&nested)
            .unwrap()
            .with_flatten(Some(Flatten::All));
        for (source, numbering) in [
            (Source::detect(&csv).unwrap(), Numbering::AsRead),
            (Source::detect(&file).unwrap(), Numbering::AsRead),
            (Source::detect(&ndjson).unwrap(), Numbering::OnOnePartition),
            (flattened, Numbering::OnOnePartition),
            (Source::detect(&parts).unwrap(), Numbering::OnOnePartition),
        ] {
            let session = eng.session_for(&ctx, &source, &Streams::default()).unwrap();
            let numbered = eng.numbering(&ctx, &session).unwrap();
            assert_eq!(numbered, numbering, "{}", source.path.display());
        }
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

#[cfg(test)]
mod stream_tests {
    use super::DataFusionEngine;
    use crate::context::{CancelToken, RequestContext};
    use crate::engine::{Engine, NamedSource, RowStream};
    use crate::error::{CancelReason, EngineError};
    use crate::source::{Format, Source};

    /// Rows enough for DataFusion to produce several batches (its default batch size is 8192), so
    /// "incremental" has something to be incremental over.
    const ROWS: usize = 30_000;

    fn csv_source(dir: &std::path::Path) -> Source {
        let path = dir.join("t.csv");
        let mut body = String::from("id,name\n");
        for i in 0..ROWS {
            body.push_str(&format!("{i},row-{i}\n"));
        }
        std::fs::write(&path, body).unwrap();
        Source::with_format(&path, Format::Csv)
    }

    fn named(source: Source) -> Vec<NamedSource> {
        vec![NamedSource {
            name: "t".to_string(),
            source,
        }]
    }

    fn drain(stream: RowStream) -> (usize, usize) {
        let mut rows = 0;
        let mut batches = 0;
        for b in stream {
            let b = b.expect("batch");
            rows += b.num_rows();
            batches += 1;
        }
        (rows, batches)
    }

    /// The stream and the buffered path agree — and the stream really arrives in pieces, which is
    /// the precondition for everything else here meaning anything.
    #[test]
    fn a_streamed_query_returns_the_same_rows_in_more_than_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let src = csv_source(dir.path());
        let eng = DataFusionEngine::new();
        let ctx = RequestContext::detached();

        let buffered = eng
            .query(&ctx, "SELECT id, name FROM t", &named(src.clone()))
            .unwrap();
        let (rows, batches) = drain(
            eng.query_stream(&ctx, "SELECT id, name FROM t", &named(src), None)
                .unwrap(),
        );

        assert_eq!(rows, ROWS);
        assert_eq!(rows, buffered.num_rows());
        assert!(
            batches > 1,
            "{ROWS} rows arrived as {batches} batch(es) — nothing below can distinguish streaming \
             from buffering unless the result is actually divisible"
        );
    }

    /// The schema is known before any row is, which is what an Arrow IPC writer and a CSV header
    /// both need in order to emit their first byte.
    #[test]
    fn the_schema_is_available_before_the_first_batch() {
        let dir = tempfile::tempdir().unwrap();
        let src = csv_source(dir.path());
        let stream = DataFusionEngine::new()
            .query_stream(
                &RequestContext::detached(),
                "SELECT id, name FROM t",
                &named(src),
                None,
            )
            .unwrap();
        let fields: Vec<&str> = stream
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(fields, vec!["id", "name"]);
    }

    /// **The test that separates a streaming engine from a buffering one.**
    ///
    /// The token is cancelled *after* the first batch has been received. A buffering
    /// implementation cannot fail this way: its whole result — every batch — exists before the
    /// first `next()` returns, so a cancellation arriving here comes too late to affect anything
    /// and the caller sees all of it. Getting rows, then a refusal, is only possible if the plan
    /// was still running when the caller was already holding output.
    ///
    /// It is also the honest statement of what the deadline buys on this path that it could not
    /// buy on `query_capped`: there, cancelling discards a result that was entirely unbuilt; here,
    /// everything already delivered is kept and only the batch in flight is lost.
    #[test]
    fn cancelling_after_the_first_batch_keeps_it_and_stops_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let src = csv_source(dir.path());
        let token = CancelToken::new();
        let ctx = RequestContext::detached().with_cancel(token.clone());

        let mut stream = DataFusionEngine::new()
            .query_stream(&ctx, "SELECT id, name FROM t", &named(src), None)
            .unwrap();

        let first = stream
            .next()
            .expect("a batch before any cancellation")
            .expect("the first batch is a real batch");
        assert!(first.num_rows() > 0);

        token.cancel();

        // The next pull refuses rather than continuing to produce.
        let after = stream.next().expect("a refusal, not end-of-stream");
        assert!(
            matches!(after, Err(EngineError::Cancelled(CancelReason::Requested))),
            "expected a cancellation, got {after:?}"
        );

        // And the stream is finished: one error, then nothing. A stream that kept yielding the
        // same error would turn `for b in stream` into an infinite loop.
        assert!(
            stream.next().is_none(),
            "an errored stream must end rather than repeat"
        );
    }

    /// A deadline already spent refuses before the plan is even built, so a caller that got `Ok`
    /// holds a live plan rather than a stream whose first pull is a foregone error.
    #[test]
    fn an_expired_deadline_refuses_at_open_time() {
        let dir = tempfile::tempdir().unwrap();
        let src = csv_source(dir.path());
        let ctx = RequestContext::detached()
            .with_deadline(std::time::Instant::now() - std::time::Duration::from_secs(1));
        let err = DataFusionEngine::new()
            .query_stream(&ctx, "SELECT id FROM t", &named(src), None)
            .expect_err("an expired deadline must not open a stream");
        assert!(
            matches!(err, EngineError::Cancelled(CancelReason::Deadline)),
            "got {err:?}"
        );
    }

    /// A cap is pushed into the plan, so it bounds what the *engine produces* rather than what the
    /// consumer remembers to stop pulling.
    #[test]
    fn a_cap_bounds_the_stream_and_none_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let src = csv_source(dir.path());
        let eng = DataFusionEngine::new();
        let ctx = RequestContext::detached();

        let (capped, _) = drain(
            eng.query_stream(&ctx, "SELECT id FROM t", &named(src.clone()), Some(100))
                .unwrap(),
        );
        assert_eq!(capped, 100);

        let (uncapped, _) = drain(
            eng.query_stream(&ctx, "SELECT id FROM t", &named(src), None)
                .unwrap(),
        );
        assert_eq!(uncapped, ROWS);
    }

    /// The read-only guard applies to the streaming path too. It would be an odd way to lose it —
    /// a second entry point into the planner that forgot the rule the first one enforces.
    #[test]
    fn the_read_only_guard_still_refuses_a_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let src = csv_source(dir.path());
        let err = DataFusionEngine::new()
            .query_stream(
                &RequestContext::detached(),
                "DELETE FROM t",
                &named(src),
                None,
            )
            .expect_err("a mutation must not stream either");
        assert!(matches!(err, EngineError::Query(_)), "got {err:?}");
    }

    /// A bad query is an error from `query_stream` itself, not a surprise in the first `next()`.
    #[test]
    fn a_planning_failure_is_returned_before_the_stream_exists() {
        let dir = tempfile::tempdir().unwrap();
        let src = csv_source(dir.path());
        let err = DataFusionEngine::new()
            .query_stream(
                &RequestContext::detached(),
                "SELECT no_such_column FROM t",
                &named(src),
                None,
            )
            .expect_err("planning must fail up front");
        assert!(matches!(err, EngineError::Query(_)), "got {err:?}");
    }
}
