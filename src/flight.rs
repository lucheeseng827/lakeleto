//! `lakeleto flight`: the tables named on its command line, read-only, to Flight SQL clients.
//!
//! ADBC (Python, R, Go, Java), JDBC tools and notebooks speak Flight SQL, so they reach Lakeleto's
//! tables with no client written for it. The server is a thin layer over the SQL engine, as the
//! other surfaces are: every statement is one read-only query (`SELECT`, `WITH`, `EXPLAIN`) over
//! the declared tables, bounded by a row cap and a deadline.
//!
//! # What it answers
//!
//! - **A statement** (`CommandStatementQuery`). The query runs when the client asks for its
//!   `FlightInfo`, so the schema it sends back is the result's own. The result is held for the
//!   client's `DoGet`, under a handle no client can guess, which one fetch uses up. At most
//!   [`MAX_OPEN`] statements are open at once, held or streaming. One held for
//!   [`HELD_FOR`] without a fetch is dropped. Past the limit a statement is refused as busy.
//!   Its deadline holds while its result is read, a client that stops reading included: past it,
//!   the read stops, the result ends in DEADLINE_EXCEEDED and the statement's place is free. Rows
//!   read out before the deadline may still reach the client after it: the read and the place
//!   are what the deadline ends, not the delivery of what was read in time.
//! - **The catalog.** `GetCatalogs`, `GetDbSchemas`, `GetTables` (with each table's schema when it
//!   is asked for), `GetTableTypes` and `GetSqlInfo`. Every table is in catalog
//!   [`CATALOG`](crate::engine::sql::CATALOG), schema [`SCHEMA`](crate::engine::sql::SCHEMA), and
//!   named as the SQL engine registers it, lowercase. The names a client reads there are the names
//!   its SQL resolves, quoted and qualified or not.
//! - **Everything else is UNIMPLEMENTED:** prepared statements, Substrait plans, updates, ingest,
//!   transactions and the key queries. That status, not another, is what lets a client fall back.
//!   ADBC's Python driver asks to prepare a plain `execute` first, and on UNIMPLEMENTED it sends
//!   the statement as a `CommandStatementQuery`.
//!
//! # Who may ask
//!
//! With a token (`--token`), every call carries it, checked in one place before any method runs:
//! `authorization: Bearer <token>`, or `Basic` credentials whose password is the token, which is
//! how a client that logs in with a user and password does its handshake. The handshake answers
//! with the bearer token, as Flight's authentication expects.
//!
//! # How it is reached
//!
//! With a certificate and key ([`Tls`]), the server speaks TLS (`grpc+tls://`). Without one it
//! speaks plaintext gRPC, which [`serve`] allows on a loopback address, and elsewhere only when
//! asked to (`--plaintext`): the token and the rows would otherwise cross the network in the
//! clear. HTTP/2 pings and TCP keepalive drop a connection whose client has gone, and with it any
//! result still waiting to be sent on it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use arrow_schema::Schema;
use futures::{Stream, StreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::context::RequestContext;
use crate::engine::registry::{EngineRegistry, Need};
use crate::engine::sql::{CATALOG, SCHEMA};
use crate::engine::{NamedSource, RowStream};
use crate::error::{CancelReason, EngineError};

/// Statements that may be open at once, held for a fetch or streaming. One more is refused as
/// busy, for the client to try again.
pub const MAX_OPEN: usize = 16;

/// How long a statement's result waits for its `DoGet` before it is dropped, unless
/// [`Limits::held_for`] says otherwise.
pub const HELD_FOR: Duration = Duration::from_secs(60);

/// The only table type there is.
const TABLE: &str = "TABLE";

/// The longest `LIKE` pattern a table listing takes, in characters.
const MAX_PATTERN: usize = 1024;

/// How often the server pings a client over HTTP/2, and how long it waits for the answer before it
/// drops the connection.
const PING_EVERY: Duration = Duration::from_secs(30);
const PING_ANSWER_WITHIN: Duration = Duration::from_secs(10);

/// TCP keepalive on every connection: the operating system's check that the peer is still there.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// A certificate chain and its private key, PEM: the server speaks TLS with them.
#[derive(Clone)]
pub struct Tls {
    cert: Vec<u8>,
    key: Vec<u8>,
}

impl Tls {
    /// TLS with `cert` (the chain, leaf first) and `key`, both PEM.
    pub fn from_pem(cert: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Tls {
        Tls {
            cert: cert.into(),
            key: key.into(),
        }
    }

    /// TLS with the PEM files at `cert` and `key` (`--tls-cert`, `--tls-key`).
    pub fn read(cert: &Path, key: &Path) -> crate::error::Result<Tls> {
        let read = |path: &Path, what: &str| {
            std::fs::read(path).map_err(|e| {
                EngineError::Other(format!(
                    "cannot read the TLS {what} {}: {e}",
                    path.display()
                ))
            })
        };
        Ok(Tls::from_pem(read(cert, "certificate")?, read(key, "key")?))
    }
}

/// What a statement may cost: the `lakeleto flight` flags.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// The most rows one statement returns. A result with more is an error, never a short one.
    pub max_rows: usize,
    /// How long a statement may take, from the request that runs it to the end of its fetch.
    pub timeout: Duration,
    /// How long a statement's result waits for its fetch: [`HELD_FOR`] from the command line.
    pub held_for: Duration,
}

/// The service: the tables, what reads them, and the statements open on it.
#[derive(Clone)]
pub struct Flight {
    inner: Arc<Inner>,
}

struct Inner {
    engines: EngineRegistry,
    tables: Vec<NamedSource>,
    limits: Limits,
    token: Option<Arc<str>>,
    held: Mutex<HashMap<[u8; 16], Held>>,
    open: Arc<AtomicUsize>,
}

/// A statement whose result waits for its fetch.
struct Held {
    stream: RowStream,
    expires: Instant,
    /// When the statement's time is up, its fetch included.
    deadline: Instant,
    /// What drops it when its wait is over; a fetch that takes it first ends it.
    timer: tokio::task::AbortHandle,
    _open: Open,
}

/// One open statement, counted until it is dropped: fetched to the end, failed, or expired.
struct Open(Arc<AtomicUsize>);

impl Drop for Open {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Flight {
    /// The service over `tables`, read with `engines`, within `limits`; with `token`, every call
    /// must carry it. A table's name must be an SQL identifier, and is kept as the engine
    /// registers it, lowercase; two that come out the same are refused. An empty token is refused
    /// too: a server that was meant to need one does not start open.
    pub fn new(
        engines: EngineRegistry,
        tables: Vec<NamedSource>,
        limits: Limits,
        token: Option<String>,
    ) -> crate::error::Result<Flight> {
        if token.as_deref() == Some("") {
            return Err(EngineError::Other(
                "the token is empty (--token or LAKELETO_TOKEN): give it a value, or leave both \
                 out to serve without one"
                    .to_string(),
            ));
        }
        let now = Instant::now();
        if now.checked_add(limits.timeout).is_none() || now.checked_add(limits.held_for).is_none() {
            return Err(EngineError::Other(
                "the timeout is too long to keep: give a number of seconds a statement may take"
                    .to_string(),
            ));
        }
        let mut named: Vec<NamedSource> = Vec::with_capacity(tables.len());
        for table in tables {
            let name = table_name(&table.name)?;
            // A database's tables are queried in the database, in its SQL, past the SQL engine's
            // names, its read-only check and its cap: not served here yet.
            if table.source.format == crate::source::Format::Database {
                return Err(EngineError::Other(format!(
                    "`{name}` is a database table, which `lakeleto flight` does not serve yet: \
                     a Flight SQL server reads files, objects and catalog tables. Point an ADBC \
                     driver for the database at it instead"
                )));
            }
            if named.iter().any(|t| t.name == name) {
                return Err(EngineError::Other(format!(
                    "two tables are named `{name}`: SQL names are not case-sensitive, so each \
                     `--table` needs a name of its own"
                )));
            }
            named.push(NamedSource {
                name,
                source: table.source,
            });
        }
        Ok(Flight {
            inner: Arc::new(Inner {
                engines,
                tables: named,
                limits,
                token: token.map(Arc::from),
                held: Mutex::new(HashMap::new()),
                open: Arc::new(AtomicUsize::new(0)),
            }),
        })
    }

    /// The tables, by the names SQL reaches them by.
    pub fn table_names(&self) -> Vec<&str> {
        self.inner.tables.iter().map(|t| t.name.as_str()).collect()
    }

    /// Count one more open statement, or refuse it as busy. Results no fetch came for are dropped
    /// first, so their places are free.
    fn admit(&self) -> Result<Open, Status> {
        self.purge();
        let open = &self.inner.open;
        // A compare-exchange loop rather than `fetch_update`, which newer stable deprecates
        // (renamed `try_update`); CI lints on stable with `-D warnings`, as in module_62.
        let mut n = open.load(Ordering::SeqCst);
        loop {
            if n >= MAX_OPEN {
                return Err(Status::resource_exhausted(format!(
                    "busy: {MAX_OPEN} statements are open on this server; fetch or drop one, and \
                     try again"
                )));
            }
            match open.compare_exchange(n, n + 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(current) => n = current,
            }
        }
        Ok(Open(open.clone()))
    }

    /// Drop every held result whose fetch is overdue.
    fn purge(&self) {
        self.inner.purge();
    }

    /// Hold `stream`, whose statement's time is up at `deadline`, for its fetch, under a new
    /// handle. A timer drops it when its wait is over if no request has: a client that never comes
    /// back does not keep its place.
    fn hold(&self, stream: RowStream, open: Open, deadline: Instant) -> Result<[u8; 16], Status> {
        let mut handle = [0u8; 16];
        getrandom::fill(&mut handle)
            .map_err(|e| Status::internal(format!("no handle for the statement: {e}")))?;
        let expires = Instant::now() + self.inner.limits.held_for;
        let inner = Arc::downgrade(&self.inner);
        // The timer starts under the lock, so it cannot purge before the entry it is for is in:
        // its purge waits for the lock.
        let mut held = lock(&self.inner.held);
        let timer = tokio::spawn(async move {
            tokio::time::sleep_until(expires.into()).await;
            if let Some(inner) = inner.upgrade() {
                inner.purge();
            }
        })
        .abort_handle();
        held.insert(
            handle,
            Held {
                stream,
                expires,
                deadline,
                timer,
                _open: open,
            },
        );
        Ok(handle)
    }

    /// The held result `handle` names, which this takes.
    fn take(&self, handle: &[u8]) -> Result<Held, Status> {
        self.purge();
        let gone = || {
            Status::not_found(
                "no statement has this ticket: it was fetched already, waited past its time, or \
                 never was",
            )
        };
        let handle: [u8; 16] = handle.try_into().map_err(|_| gone())?;
        let held = lock(&self.inner.held).remove(&handle).ok_or_else(gone)?;
        // Fetched: nothing is left for its timer to drop.
        held.timer.abort();
        Ok(held)
    }
}

impl Inner {
    /// Drop every held result whose fetch is overdue. They are taken out under the lock and
    /// dropped after it, on a blocking thread: dropping one ends its plan.
    fn purge(&self) {
        let now = Instant::now();
        let overdue: Vec<Held> = {
            let mut held = lock(&self.held);
            let handles: Vec<[u8; 16]> = held
                .iter()
                .filter(|(_, h)| h.expires <= now)
                .map(|(handle, _)| *handle)
                .collect();
            handles.iter().filter_map(|h| held.remove(h)).collect()
        };
        if !overdue.is_empty() {
            tokio::task::spawn_blocking(move || drop(overdue));
        }
    }

    /// Run `sql` over the tables it names, by `deadline`: its result, open, one row past the cap
    /// so that a result with more can be told from one with exactly as many.
    fn run(&self, sql: &str, deadline: Instant) -> Result<RowStream, EngineError> {
        // One read query, refused here before anything else reads the SQL; the engine checks
        // again, as it does for every caller.
        crate::engine::sql::ensure_read_only(sql)?;
        let ctx = RequestContext::detached().with_deadline(deadline);
        let tables = self.referenced(sql)?;
        let engine = self.engines.resolve(Need::Query(&tables))?;
        let cap = self.limits.max_rows.saturating_add(1);
        engine.query_stream(&ctx, sql, &tables, Some(cap))
    }

    /// The declared tables `sql` names. Only those are registered for it: an Iceberg, Delta or
    /// JSON table is loaded whole for each query that registers it. A name that is none of them
    /// is left for the engine to refuse, in its own words.
    fn referenced(&self, sql: &str) -> Result<Vec<NamedSource>, EngineError> {
        use datafusion::sql::parser::DFParser;
        let statements = DFParser::parse_sql(sql)
            .map_err(|e| EngineError::Query(format!("could not parse SQL: {e}")))?;
        let mut names = Vec::new();
        for statement in &statements {
            let (references, _ctes) =
                datafusion::sql::resolve::resolve_table_references(statement, true)
                    .map_err(|e| EngineError::Query(e.to_string()))?;
            for reference in references {
                let ours = reference.catalog().is_none_or(|c| c == CATALOG)
                    && reference.schema().is_none_or(|s| s == SCHEMA);
                if ours {
                    names.push(reference.table().to_string());
                }
            }
        }
        Ok(self
            .tables
            .iter()
            .filter(|t| names.contains(&t.name))
            .map(|t| NamedSource {
                name: t.name.clone(),
                source: t.source.clone(),
            })
            .collect())
    }

    /// The schema queries of `table` return. An Iceberg or Delta table, or a Parquet directory,
    /// is one the SQL engine registers as the local reader reads it, so its schema is the local
    /// reader's, from the table's metadata and no row; a query loads such a table whole, a
    /// listing never does. Any other is the SQL engine's own, from a plan that reads no row.
    fn table_schema(&self, table: &NamedSource) -> Result<Schema, EngineError> {
        use crate::source::Format;
        let ctx = RequestContext::detached().with_timeout(self.limits.timeout);
        let source = &table.source;
        let via_local = matches!(source.format, Format::Iceberg | Format::Delta)
            || (source.format == Format::Parquet && source.path.is_dir());
        if via_local {
            let engine = self.engines.resolve(Need::Read(source))?;
            let preview = engine.preview(&ctx, source, 0)?;
            return Ok(preview.schema.as_ref().clone());
        }
        let tables = std::slice::from_ref(table);
        let engine = self.engines.resolve(Need::Query(tables))?;
        let sql = format!("SELECT * FROM \"{}\"", table.name);
        let stream = engine.query_stream(&ctx, &sql, tables, Some(0))?;
        Ok(stream.schema().as_ref().clone())
    }
}

/// A `--table` name as SQL reaches it: an identifier (letters, digits and `_`, not starting with
/// a digit), lowercase, as the SQL engine registers it.
fn table_name(name: &str) -> crate::error::Result<String> {
    let mut chars = name.chars();
    let identifier = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !identifier {
        return Err(EngineError::Other(format!(
            "`{name}` is not a table name a Flight SQL client can query: use letters, digits and \
             `_`, starting with a letter or `_`"
        )));
    }
    Ok(name.to_ascii_lowercase())
}

/// Whether `name` matches an SQL `LIKE` pattern, as the Flight SQL filters mean one: `%` is any
/// run of characters, `_` any one, and `\\` takes the next character as itself.
///
/// One pass, going back only to the last `%`: at most the name's length times the pattern's, so
/// a pattern a client sends cannot make a listing run long.
fn like(name: &str, pattern: &str) -> bool {
    #[derive(PartialEq)]
    enum Part {
        Any,
        One,
        Char(char),
    }
    let mut parts = Vec::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '%' if parts.last() == Some(&Part::Any) => {}
            '%' => parts.push(Part::Any),
            '_' => parts.push(Part::One),
            '\\' => parts.push(Part::Char(chars.next().unwrap_or('\\'))),
            c => parts.push(Part::Char(c)),
        }
    }
    let name: Vec<char> = name.chars().collect();
    let (mut n, mut p) = (0, 0);
    // The part after the last `%` seen, and where in the name that `%` is taken to end.
    let mut last_any: Option<(usize, usize)> = None;
    while n < name.len() {
        match parts.get(p) {
            Some(Part::Any) => {
                last_any = Some((p + 1, n));
                p += 1;
            }
            Some(Part::One) => {
                n += 1;
                p += 1;
            }
            Some(Part::Char(c)) if *c == name[n] => {
                n += 1;
                p += 1;
            }
            _ => match last_any {
                Some((after, from)) => {
                    last_any = Some((after, from + 1));
                    p = after;
                    n = from + 1;
                }
                None => return false,
            },
        }
    }
    parts[p..].iter().all(|part| *part == Part::Any)
}

/// A table listing's `LIKE` pattern, if it is one this server takes.
fn filter_pattern(which: &str, pattern: Option<&str>) -> Result<(), Status> {
    match pattern {
        Some(p) if p.chars().count() > MAX_PATTERN => Err(Status::invalid_argument(format!(
            "the {which} pattern is longer than {MAX_PATTERN} characters"
        ))),
        _ => Ok(()),
    }
}

/// What a server answers `GetSqlInfo` with: who it is, and that it only reads.
fn sql_info() -> &'static SqlInfoData {
    static INFO: OnceLock<SqlInfoData> = OnceLock::new();
    INFO.get_or_init(|| {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "Lakeleto");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        // The Arrow format version, as the Flight SQL spec asks: Schema.fbs's 1.3.
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
        builder.append(SqlInfo::FlightSqlServerReadOnly, true);
        builder.append(SqlInfo::FlightSqlServerSql, true);
        builder.append(SqlInfo::FlightSqlServerSubstrait, false);
        builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
        builder.append(SqlInfo::SqlDdlCatalog, false);
        builder.append(SqlInfo::SqlDdlSchema, false);
        builder.append(SqlInfo::SqlDdlTable, false);
        builder
            .build()
            .expect("a fixed set of SqlInfo values builds")
    })
}

/// An engine's error as the status a client acts on. UNIMPLEMENTED is left for what this server
/// does not do at all, since clients read it as "fall back".
fn status(e: EngineError) -> Status {
    match &e {
        EngineError::Query(_) => Status::invalid_argument(e.to_string()),
        EngineError::Forbidden(_) => Status::permission_denied(e.to_string()),
        EngineError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
            Status::not_found(e.to_string())
        }
        EngineError::Cancelled(CancelReason::Deadline) => Status::deadline_exceeded(e.to_string()),
        EngineError::Cancelled(CancelReason::Requested) => Status::cancelled(e.to_string()),
        EngineError::TooLarge(_) => Status::resource_exhausted(e.to_string()),
        EngineError::UnsupportedOperation { .. } | EngineError::UnsupportedFormat { .. } => {
            Status::failed_precondition(e.to_string())
        }
        _ => Status::internal(e.to_string()),
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

type DoGetStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>;

/// `batch` sent as a `DoGet` reply.
fn reply(batch: RecordBatch) -> Response<DoGetStream> {
    let schema = batch.schema();
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::once(async { Ok(batch) }))
        .map(|item| item.map_err(Status::from));
    Response::new(Box::pin(stream))
}

/// A `FlightInfo` whose one endpoint's ticket is `command` itself: what the catalog calls answer.
fn info_for(
    command: &impl ProstMessageExt,
    schema: &Schema,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let ticket = Ticket::new(command.as_any().encode_to_vec());
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(format!("encode the schema: {e}")))?
        .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
        .with_descriptor(descriptor);
    Ok(Response::new(info))
}

#[tonic::async_trait]
impl FlightSqlService for Flight {
    type FlightService = Flight;

    /// The handshake: the gate has checked the credentials, and the answer carries the bearer
    /// token, which the client sends from then on.
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let answer = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let stream = futures::stream::once(async { Ok(answer) });
        let mut response: Response<Pin<Box<dyn Stream<Item = _> + Send>>> =
            Response::new(Box::pin(stream));
        if let Some(token) = &self.inner.token {
            let bearer = format!("Bearer {token}")
                .parse()
                .map_err(|_| Status::internal("the token is not a header value"))?;
            response.metadata_mut().insert("authorization", bearer);
        }
        Ok(response)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let open = self.admit()?;
        let inner = self.inner.clone();
        let sql = query.query;
        // `Flight::new` checked that the timeout can be added to now.
        let deadline = Instant::now() + self.inner.limits.timeout;
        // The place goes with the work: a request the client cancels still holds it until its
        // query, which runs on regardless, gives it back.
        let (stream, open) = tokio::task::spawn_blocking(move || (inner.run(&sql, deadline), open))
            .await
            .map_err(|e| Status::internal(format!("the statement's thread failed: {e}")))?;
        let stream = stream.map_err(status)?;
        let schema = stream.schema().clone();
        let handle = self.hold(stream, open, deadline)?;
        let ticket = TicketStatementQuery {
            statement_handle: handle.to_vec().into(),
        };
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("encode the schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());
        Ok(Response::new(info))
    }

    /// The held result, streamed. Rows past the cap end the stream with an error: a result cut
    /// short there would look whole to the client, and its counts and sums would be wrong. So
    /// does the deadline, even while the client reads nothing: the read waits for room to send
    /// only until then, and gives its place back.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let held = self.take(&ticket.statement_handle)?;
        let schema = held.stream.schema().clone();
        let max_rows = self.inner.limits.max_rows;
        let deadline = tokio::time::Instant::from_std(held.deadline);
        let runtime = tokio::runtime::Handle::current();
        let late = Arc::new(AtomicBool::new(false));
        let gave_up = late.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, FlightError>>(2);
        tokio::task::spawn_blocking(move || {
            let Held { stream, _open, .. } = held;
            let mut rows = 0usize;
            for batch in stream {
                let item = match batch {
                    Ok(batch) => {
                        rows += batch.num_rows();
                        if rows > max_rows {
                            Err(Status::resource_exhausted(format!(
                                "the result has more than {max_rows} rows, the most this server \
                                 returns (--max-rows): aggregate or filter in the SQL, or add a \
                                 LIMIT"
                            )))
                        } else {
                            Ok(batch)
                        }
                    }
                    Err(e) => Err(status(e)),
                };
                let failed = item.is_err();
                let item = item.map_err(|s| FlightError::Tonic(Box::new(s)));
                // Stop reading, which drops the plan, once the error is sent, the client has hung
                // up (dropping the receiver), or the deadline passes with no room to send.
                match runtime.block_on(tokio::time::timeout_at(deadline, tx.send(item))) {
                    Ok(Ok(())) if !failed => {}
                    Ok(_) => break,
                    Err(_) => {
                        gave_up.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });
        let batches = futures::stream::unfold((rx, Some(late)), |(mut rx, late)| async move {
            if let Some(item) = rx.recv().await {
                return Some((item, (rx, late)));
            }
            // The read gave up at the deadline: what was sent is followed by the error, so the
            // client never takes the rows it has for the whole result.
            let late = late?;
            late.load(Ordering::SeqCst).then(|| {
                let status = Status::deadline_exceeded(
                    "the statement ran past its time (--timeout) before its result was read",
                );
                (Err(FlightError::Tonic(Box::new(status))), (rx, None))
            })
        });
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches)
            .map(|item| item.map_err(Status::from));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        info_for(&query, &schema, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(CATALOG);
        Ok(reply(builder.build().map_err(Status::from)?))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        info_for(&query, &schema, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        // The builder keeps only what the request's catalog and pattern match.
        let mut builder = query.into_builder();
        builder.append(CATALOG, SCHEMA);
        Ok(reply(builder.build().map_err(Status::from)?))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        info_for(&query, &schema, request.into_inner())
    }

    /// The tables the request's filters keep, and when it asks, each one's schema: the one its
    /// queries return ([`Inner::table_schema`]). A listing with schemas reads each table's
    /// metadata, so it counts as an open statement while it runs. A table whose schema cannot be
    /// read is left out, with a warning, rather than failing the listing of the rest.
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        filter_pattern("table name", query.table_name_filter_pattern.as_deref())?;
        filter_pattern("schema", query.db_schema_filter_pattern.as_deref())?;
        let open = match query.include_schema {
            true => Some(self.admit()?),
            false => None,
        };
        let inner = self.inner.clone();
        let batch = tokio::task::spawn_blocking(move || {
            let _open = open;
            let kept = |table: &NamedSource| {
                query.catalog.as_deref().is_none_or(|c| c == CATALOG)
                    && query
                        .db_schema_filter_pattern
                        .as_deref()
                        .is_none_or(|p| like(SCHEMA, p))
                    && query
                        .table_name_filter_pattern
                        .as_deref()
                        .is_none_or(|p| like(&table.name, p))
                    && (query.table_types.is_empty()
                        || query.table_types.iter().any(|t| t == TABLE))
            };
            let tables: Vec<&NamedSource> = inner.tables.iter().filter(|t| kept(t)).collect();
            let mut builder = query.into_builder();
            let with_schema = builder.include_schema();
            for table in tables {
                let schema = match with_schema {
                    false => Schema::empty(),
                    true => match inner.table_schema(table) {
                        Ok(schema) => schema,
                        Err(e) => {
                            eprintln!(
                                "lakeleto: warning: `{}` is left out of a table listing: {e}",
                                table.name
                            );
                            continue;
                        }
                    },
                };
                builder
                    .append(CATALOG, SCHEMA, &table.name, TABLE, &schema)
                    .map_err(Status::from)?;
            }
            builder.build().map_err(Status::from)
        })
        .await
        .map_err(|e| Status::internal(format!("the listing's thread failed: {e}")))??;
        Ok(reply(batch))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        info_for(&query, &schema, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(TABLE);
        Ok(reply(builder.build().map_err(Status::from)?))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder(sql_info()).schema();
        info_for(&query, &schema, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let batch = query
            .into_builder(sql_info())
            .build()
            .map_err(Status::from)?;
        Ok(reply(batch))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// The token check every call passes before its method runs.
#[derive(Clone)]
struct Gate {
    token: Option<Arc<str>>,
}

impl tonic::service::Interceptor for Gate {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let Some(token) = &self.token else {
            return Ok(request);
        };
        let carried = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| credentials_match(value, token));
        match carried {
            true => Ok(request),
            false => Err(Status::unauthenticated(
                "this server needs its token: send `authorization: Bearer <token>`, or log in \
                 with the token as the password",
            )),
        }
    }
}

/// Whether an `authorization` header carries `token`: as a bearer token, or as the password of
/// `Basic` credentials.
fn credentials_match(header: &str, token: &str) -> bool {
    use base64::Engine as _;
    if let Some(bearer) = header.strip_prefix("Bearer ") {
        return ct_eq(bearer.trim().as_bytes(), token.as_bytes());
    }
    if let Some(basic) = header.strip_prefix("Basic ") {
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(basic.trim()) else {
            return false;
        };
        let Some(at) = decoded.iter().position(|&b| b == b':') else {
            return false;
        };
        return ct_eq(&decoded[at + 1..], token.as_bytes());
    }
    false
}

/// Length-checked constant-time comparison, as `serve`'s token check makes it.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// Serve `flight` on `listener` until `shutdown` resolves: over TLS with `tls`, else plaintext.
/// A certificate or key that cannot be used is refused before anything is served.
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    flight: Flight,
    tls: Option<&Tls>,
    shutdown: impl std::future::Future<Output = ()>,
) -> crate::error::Result<()> {
    let gate = Gate {
        token: flight.inner.token.clone(),
    };
    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(PING_EVERY))
        .http2_keepalive_timeout(Some(PING_ANSWER_WITHIN));
    if let Some(tls) = tls {
        let identity = tonic::transport::Identity::from_pem(&tls.cert, &tls.key);
        server = server
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
            .map_err(|e| {
                EngineError::Other(format!("the TLS certificate and key cannot be used: {e}"))
            })?;
    }
    let incoming =
        tonic::transport::server::TcpIncoming::from(listener).with_keepalive(Some(TCP_KEEPALIVE));
    server
        .add_service(FlightServiceServer::with_interceptor(flight, gate))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
        .map_err(|e| EngineError::Other(format!("the Flight SQL server stopped: {e}")))
}

/// Serve `flight` on `addr` until Ctrl-C, over TLS with `tls`. Plaintext on an address other
/// than loopback is refused unless `plaintext` says a TLS proxy or tunnel is in front. A bind
/// other than loopback with no token is an open door to every table it names, and says so.
pub fn serve(
    addr: &str,
    flight: Flight,
    tls: Option<Tls>,
    plaintext: bool,
) -> crate::error::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(EngineError::Io)?;
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| EngineError::Other(format!("cannot listen on {addr}: {e}")))?;
        let bound = listener.local_addr().map_err(EngineError::Io)?;
        transport_allowed(&bound, tls.is_some(), plaintext)?;
        if flight.inner.token.is_none() && !is_loopback(&bound) {
            eprintln!(
                "lakeleto: WARNING binding {bound} (non-loopback) with no --token — every table \
                 named here is open to anyone who reaches it"
            );
        }
        if plaintext && !is_loopback(&bound) {
            eprintln!(
                "lakeleto: plaintext on {bound} (--plaintext): the token and the rows are in the \
                 clear up to whatever terminates TLS in front of this server"
            );
        }
        let scheme = if tls.is_some() { "grpc+tls" } else { "grpc" };
        eprintln!(
            "lakeleto: Flight SQL on {scheme}://{bound}, tables {} (catalog {CATALOG}, schema \
             {SCHEMA})",
            flight.table_names().join(", ")
        );
        serve_on(listener, flight, tls.as_ref(), async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    })
}

/// Whether a server bound to `bound` may speak as asked: TLS anywhere, plaintext on loopback, and
/// plaintext elsewhere only when `plaintext` says so.
fn transport_allowed(bound: &SocketAddr, tls: bool, plaintext: bool) -> crate::error::Result<()> {
    if tls && plaintext {
        return Err(EngineError::Other(
            "--plaintext and --tls-cert ask for opposite things: give one of them".to_string(),
        ));
    }
    if tls || plaintext || is_loopback(bound) {
        return Ok(());
    }
    Err(EngineError::Other(format!(
        "{bound} is not a loopback address, and the server would speak plaintext: the token and \
         the rows would cross the network in the clear. Give --tls-cert and --tls-key, or \
         --plaintext if a TLS proxy or tunnel is in front of it"
    )))
}

fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_name_is_an_identifier_kept_as_sql_registers_it() {
        assert_eq!(table_name("Orders").unwrap(), "orders");
        assert_eq!(table_name("_raw_2026").unwrap(), "_raw_2026");
        for bad in ["", "2026", "my-table", "a.b", "naïve", "o rders"] {
            assert!(table_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn like_patterns_match_as_sql_matches_them() {
        assert!(like("orders", "orders"));
        assert!(like("orders", "ord%"));
        assert!(like("orders", "%"));
        assert!(like("orders", "o_ders"));
        assert!(like("o_rders", "o\\_rders"));
        assert!(!like("oxrders", "o\\_rders"));
        assert!(!like("orders", "ord"));
        assert!(!like("orders", "x%"));
        assert!(!like("orders", "orders_"));
        assert!(like("orders", "%r%r%"));
        assert!(like("orders", "o%s"));
        assert!(!like("orders", "o%x%s"));
        assert!(like("orders", "%%%ders"));
        assert!(like("a%b", "a\\%b"));
        assert!(!like("axb", "a\\%b"));
        assert!(like("end\\", "end\\"));
        assert!(like("", "%"));
        assert!(!like("", "_"));
    }

    #[test]
    fn a_pattern_a_client_sends_cannot_make_a_listing_run_long() {
        // Exponential for a matcher that tries every split at each `%`.
        let name = "a".repeat(64);
        let slow = format!("{}b", "%a".repeat(200));
        let started = Instant::now();
        assert!(!like(&name, &slow));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{:?}",
            started.elapsed()
        );
        assert!(filter_pattern("table name", Some(&"%".repeat(MAX_PATTERN))).is_ok());
        let long = "%".repeat(MAX_PATTERN + 1);
        let refused = filter_pattern("table name", Some(&long)).unwrap_err();
        assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn plaintext_reaches_the_network_only_when_asked_to() {
        let loopback: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let loopback6: SocketAddr = "[::1]:50051".parse().unwrap();
        let anywhere: SocketAddr = "0.0.0.0:50051".parse().unwrap();
        let lan: SocketAddr = "192.168.1.20:50051".parse().unwrap();
        for bound in [&loopback, &loopback6] {
            assert!(transport_allowed(bound, false, false).is_ok());
        }
        for bound in [&anywhere, &lan] {
            let refused = transport_allowed(bound, false, false)
                .unwrap_err()
                .to_string();
            assert!(
                refused.contains("--tls-cert") && refused.contains("--plaintext"),
                "{refused}"
            );
            assert!(transport_allowed(bound, true, false).is_ok());
            assert!(transport_allowed(bound, false, true).is_ok());
        }
        assert!(transport_allowed(&anywhere, true, true).is_err());
    }

    #[tokio::test]
    async fn a_fetch_that_takes_a_result_ends_its_timer() {
        let limits = Limits {
            max_rows: 10,
            timeout: Duration::from_secs(30),
            held_for: Duration::from_secs(60),
        };
        let flight = Flight::new(EngineRegistry::local(), Vec::new(), limits, None).unwrap();
        let open = flight.admit().unwrap();
        let schema = Arc::new(Schema::empty());
        let stream = RowStream::new(
            schema,
            std::iter::empty::<crate::error::Result<RecordBatch>>(),
        );
        let handle = flight
            .hold(stream, open, Instant::now() + limits.timeout)
            .unwrap();
        let held = flight.take(&handle).unwrap();
        // Long before its 60 s are up, the timer is gone rather than left asleep.
        for _ in 0..50 {
            if held.timer.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(held.timer.is_finished());
    }

    #[tokio::test]
    async fn a_result_no_fetch_comes_for_is_dropped_on_time_with_no_request() {
        let limits = Limits {
            max_rows: 10,
            timeout: Duration::from_secs(30),
            held_for: Duration::from_millis(100),
        };
        let flight = Flight::new(EngineRegistry::local(), Vec::new(), limits, None).unwrap();
        let open = flight.admit().unwrap();
        let schema = Arc::new(Schema::empty());
        let stream = RowStream::new(
            schema,
            std::iter::empty::<crate::error::Result<RecordBatch>>(),
        );
        flight
            .hold(stream, open, Instant::now() + limits.timeout)
            .unwrap();
        assert_eq!(flight.inner.open.load(Ordering::SeqCst), 1);
        // No request comes: the timer alone drops it, and gives its place back.
        for _ in 0..50 {
            if flight.inner.open.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(flight.inner.open.load(Ordering::SeqCst), 0);
        assert!(lock(&flight.inner.held).is_empty());
    }

    #[test]
    fn credentials_carry_the_token_as_a_bearer_or_a_password() {
        use base64::Engine as _;
        let basic = |s: &str| {
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(s)
            )
        };
        assert!(credentials_match("Bearer t0k", "t0k"));
        assert!(credentials_match(&basic("anyone:t0k"), "t0k"));
        assert!(credentials_match(&basic(":t0k"), "t0k"));
        for wrong in [
            "Bearer t0",
            "Bearer t0kk",
            "bearer t0k",
            "t0k",
            &basic("t0k"),
            &basic("t0k:x"),
            "Basic !!!",
        ] {
            assert!(!credentials_match(wrong, "t0k"), "{wrong}");
        }
    }
}
