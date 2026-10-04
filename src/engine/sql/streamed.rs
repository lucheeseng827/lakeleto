//! Tables of a registry format SQL reads in passes ([`SqlSupport::Streamed`]): one pass over the
//! file per execution, its rows decoded by this crate's reader on a blocking thread and handed to
//! DataFusion a batch at a time. A file in an object store is passed over the same way, a request
//! per pass, each one pinned to the version of the object the table was registered with.
//!
//! So a query holds the batches in flight, not the file: `count(*)` over a gigabyte of NDJSON, a
//! filter, a `GROUP BY` or the grid's sorted window (a top-k) run in memory set by the query, not
//! by the file. The price is a pass per query: a statement that reads its table twice decodes the
//! file twice — or for an object, downloads it twice — where a table read into memory decoded it
//! once.
//!
//! The table's schema is the one a grid read of the file uses, fixed when the table is registered.
//! A value past the sample it came from can disagree with it, and a planned query cannot change its
//! schema partway; see [`Streams`] for what happens then.
//!
//! [`SqlSupport::Streamed`]: crate::format::SqlSupport::Streamed

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::context::RequestContext;
use crate::engine::local::LocalReaderEngine;
use crate::engine::NamedSource;
use crate::error::{EngineError, Result};
use crate::format::{Input, Pass, PassFn, ReadOptions, RemoteObject};

/// Batches a pass may decode ahead of the query reading it: enough to keep the decoder busy while
/// the query works on the last one, and all the memory a pass holds beyond the query's own.
const IN_FLIGHT: usize = 2;

/// The streamed tables one attempt at a query registered, and whether one of them turned out to
/// have been planned with column types narrower than its file holds — the reason [`retrying`]
/// plans the query again.
///
/// Two things show it. A pass that meets a value its table's schema cannot hold says so
/// ([`Pass::Widened`]), the reader having made the file's wider schema the one to use. And a query
/// can fail to plan over the narrower types before any pass has run — a string compared with a
/// column the sample saw only integers in. Then each table's file is checked by a pass of its own,
/// which widens its schema if the file needs it. A syntax error, or a column the query cannot
/// resolve, is the query's own and is not checked; any other failure to plan, an unknown table or
/// function among them, costs that pass before it is reported.
#[derive(Debug, Clone, Default)]
pub(super) struct Streams(Arc<Attempt>);

#[derive(Debug, Default)]
struct Attempt {
    /// Raised by a pass over any of `tables`; shared with each, so no table holds the attempt.
    widened: Arc<AtomicBool>,
    /// The query failed to plan for a reason a column's type could explain.
    unplanned: AtomicBool,
    tables: Mutex<Vec<Arc<TablePass>>>,
}

impl Streams {
    /// A session whose planning failures these streams hear of ([`planning_failed`]).
    pub(super) fn session(&self) -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(self.clone())))
    }

    /// Whether any table was registered here, to be read in passes.
    pub(super) fn any(&self) -> bool {
        !self
            .0
            .tables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
    }

    /// Whether a table here was registered with a schema its file has since proven too narrow for:
    /// raised by a pass, or found by checking each file after a planning failure.
    fn widened(&self, ctx: &RequestContext) -> bool {
        if self.0.widened.load(Ordering::SeqCst) {
            return true;
        }
        if !self.0.unplanned.load(Ordering::SeqCst) {
            return false;
        }
        let tables = self
            .0
            .tables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        // Every table checked, not just up to the first that widens: the query is planned again
        // only once.
        let mut widened = false;
        for table in &tables {
            widened |= table.widens(ctx);
        }
        widened
    }
}

/// Run `op`, and once more if a table it streams turned out to have been registered with types
/// too narrow for its file — planned with the file's wider schema the second time, so the query
/// reads every value the grid would rather than failing on one the sample did not see. A second
/// widening is reported, not retried: it would mean the file changed under the query.
pub(super) fn retrying<T>(ctx: &RequestContext, op: impl Fn(&Streams) -> Result<T>) -> Result<T> {
    let streams = Streams::default();
    match op(&streams) {
        Err(_) if streams.widened(ctx) => op(&Streams::default()),
        done => done,
    }
}

/// Note that `session` failed to plan a query with `e`, for [`Streams`] to check its tables
/// before the failure is final — unless `e` is about the query's syntax or a column it cannot
/// resolve, which no column's type explains.
pub(super) fn planning_failed(session: &SessionContext, e: &DataFusionError) {
    if matches!(
        e.find_root(),
        DataFusionError::SQL(..) | DataFusionError::SchemaError(..)
    ) {
        return;
    }
    if let Some(streams) = session.state().config().get_extension::<Streams>() {
        streams.0.unplanned.store(true, Ordering::SeqCst);
    }
}

/// Where a streamed table's rows are: a local file, or an object in a store as of the version it
/// was looked up at. Each pass reads it afresh.
#[derive(Debug, Clone)]
pub(super) enum At {
    File(PathBuf),
    Object(Arc<dyn RemoteObject>),
}

impl At {
    /// What a pass reads: the file, or the object.
    fn input(&self) -> Input<'_> {
        match self {
            At::File(path) => Input::File(path),
            At::Object(object) => Input::Object(object.as_ref()),
        }
    }
}

impl std::fmt::Display for At {
    /// Its path or URI.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            At::File(path) => write!(f, "{}", path.display()),
            At::Object(object) => f.write_str(object.uri()),
        }
    }
}

/// Register `table` as a table read in passes by `pass` from `at`, its file or its object, with
/// the schema a grid read of it has. Its rows are not read here.
pub(super) fn register(
    session: &SessionContext,
    table: &NamedSource,
    pass: PassFn,
    at: At,
    streams: &Streams,
) -> Result<()> {
    let reader = crate::format::reader(table.source.format)
        .ok_or_else(|| EngineError::unsupported_format(table.source.format, "sql"))?;
    let opts = LocalReaderEngine::default().read_options(&table.source);
    let schema = reader.schema(at.input(), &opts)?.schema;
    let file = Arc::new(TablePass {
        pass,
        at,
        json_path: table.source.json_path.clone(),
        csv_infer_rows: opts.csv_infer_rows,
        schema: schema.clone(),
        widened: streams.0.widened.clone(),
    });
    streams
        .0
        .tables
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(file.clone());
    let provider = StreamingTable::try_new(schema, vec![file as Arc<dyn PartitionStream>])
        .map_err(|e| EngineError::Query(e.to_string()))?;
    session
        .register_table(table.name.as_str(), Arc::new(provider))
        .map_err(|e| EngineError::Query(e.to_string()))?;
    Ok(())
}

/// A streamed table's one partition: a pass over its file, or object, each time it executes.
struct TablePass {
    pass: PassFn,
    at: At,
    json_path: Option<String>,
    csv_infer_rows: usize,
    schema: SchemaRef,
    widened: Arc<AtomicBool>,
}

impl TablePass {
    /// How a pass reads the file: as a grid read of it does, `batch_size` rows to a batch.
    fn options(&self, batch_size: usize) -> ReadOptions<'_> {
        ReadOptions {
            batch_size,
            csv_infer_rows: self.csv_infer_rows,
            json_path: self.json_path.as_deref(),
        }
    }

    /// Whether a pass over the whole file finds it wider than this table's schema — making the
    /// wider one the file's, for the query planned again. Stops, finding nothing, if `ctx` is
    /// cancelled.
    fn widens(&self, ctx: &RequestContext) -> bool {
        let opts = self.options(SessionConfig::new().batch_size());
        let mut until_cancelled = |_| ctx.check().is_ok();
        matches!(
            (self.pass)(self.at.input(), &opts, &self.schema, &mut until_cancelled),
            Ok(Pass::Widened(_))
        )
    }
}

impl std::fmt::Debug for TablePass {
    /// The file or object and its records path: what names the table in a plan's debug output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TablePass")
            .field("at", &format_args!("{}", self.at))
            .field("json_path", &self.json_path)
            .finish_non_exhaustive()
    }
}

impl PartitionStream for TablePass {
    /// The schema the table was registered with, which every batch of a pass has.
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// A pass over the file, at most [`IN_FLIGHT`] batches ahead of the query. One that finds
    /// the file wider than the table's schema raises the attempt's flag and ends the stream with
    /// an error, for [`retrying`] to plan the query again.
    fn execute(&self, task: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema.clone(), IN_FLIGHT);
        let tx = builder.tx();
        let pass = self.pass;
        let at = self.at.clone();
        let json_path = self.json_path.clone();
        let csv_infer_rows = self.csv_infer_rows;
        let schema = self.schema.clone();
        let widened = self.widened.clone();
        let batch_size = task.session_config().batch_size();
        // A blocking thread, because decoding is CPU work and file reads that the async runtime
        // must not wait on. A query that drops its stream closes the channel, the next send fails
        // and the pass stops there, so no pass outlives its query by more than a batch.
        builder.spawn_blocking(move || {
            let opts = ReadOptions {
                batch_size,
                csv_infer_rows,
                json_path: json_path.as_deref(),
            };
            let mut each = |batch| tx.blocking_send(Ok(batch)).is_ok();
            match pass(at.input(), &opts, &schema, &mut each) {
                Ok(Pass::Done) => Ok(()),
                Ok(Pass::Widened(why)) => {
                    widened.store(true, Ordering::SeqCst);
                    Err(DataFusionError::Execution(format!(
                        "a value in {at} does not fit the column types sampled from the start of \
                         the file ({why}). The file's schema has been widened to hold it, so the \
                         query reads it when run again"
                    )))
                }
                Err(e) => Err(DataFusionError::External(Box::new(e))),
            }
        });
        builder.build()
    }
}
