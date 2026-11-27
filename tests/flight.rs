//! `lakeleto flight`, served on a loopback port and driven with arrow-flight's own Flight SQL
//! client: statements over the declared tables, the catalog calls, the token on every call, the
//! refusals of a read-only server, and the bounds on what a statement may hold and return.
//!
//! Run with: `cargo test --features flight --test flight`. The same server under
//! Python's ADBC driver is `ops/flight_adbc_smoke.py`.
#![cfg(feature = "flight")]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables, SqlInfo};
use arrow_flight::{Action, Criteria, FlightDescriptor, FlightInfo, HandshakeRequest, Ticket};
use futures::TryStreamExt;
use lakeleto::engine::NamedSource;
use lakeleto::flight::{Flight, Limits, Tls, MAX_OPEN};
use lakeleto::{EngineRegistry, Source};
use tonic::transport::Channel;
use tonic::Code;

const TOKEN: &str = "fl1ght_t0ken";

fn limits() -> Limits {
    Limits {
        max_rows: 1_000,
        timeout: Duration::from_secs(30),
        held_for: Duration::from_secs(60),
    }
}

/// `orders.csv` (3 rows) and `Items.parquet` (4 rows) in `dir`.
fn fixtures(dir: &Path) -> Vec<(String, std::path::PathBuf)> {
    let orders = dir.join("orders.csv");
    std::fs::write(&orders, "id,amount,currency\n1,9.5,USD\n2,,EUR\n3,12,USD\n").unwrap();
    let items = dir.join("Items.parquet");
    let batch = RecordBatch::try_from_iter([
        (
            "k",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as Arc<dyn Array>,
        ),
        (
            "v",
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as Arc<dyn Array>,
        ),
    ])
    .unwrap();
    let file = std::fs::File::create(&items).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    vec![("orders".into(), orders), ("Items".into(), items)]
}

/// A server on a loopback port over `tables`, and what stops it.
struct Server {
    addr: String,
    _stop: tokio::sync::oneshot::Sender<()>,
}

async fn start(
    tables: &[(String, std::path::PathBuf)],
    limits: Limits,
    token: Option<&str>,
) -> Server {
    start_with(tables, limits, token, None).await
}

/// [`start`], over TLS with `tls`.
async fn start_with(
    tables: &[(String, std::path::PathBuf)],
    limits: Limits,
    token: Option<&str>,
    tls: Option<Tls>,
) -> Server {
    let named = tables
        .iter()
        .map(|(name, path)| NamedSource {
            name: name.clone(),
            source: Source::detect(path).unwrap(),
        })
        .collect();
    let flight = Flight::new(
        EngineRegistry::local(),
        named,
        limits,
        token.map(str::to_string),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        lakeleto::flight::serve_on(listener, flight, tls.as_ref(), async {
            let _ = stopped.await;
        })
        .await
    });
    Server { addr, _stop: stop }
}

async fn channel(addr: &str) -> Channel {
    Channel::from_shared(addr.to_string())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

async fn client(addr: &str, token: Option<&str>) -> FlightSqlServiceClient<Channel> {
    let mut client = FlightSqlServiceClient::new(channel(addr).await);
    if let Some(token) = token {
        client.set_token(token.to_string());
    }
    client
}

/// The status code an arrow-flight client error carries.
fn code(e: &arrow_flight::error::FlightError) -> Code {
    match e {
        arrow_flight::error::FlightError::Tonic(status) => status.code(),
        other => panic!("not a status: {other:?}"),
    }
}

fn ticket(info: &FlightInfo) -> Ticket {
    info.endpoint[0].ticket.clone().unwrap()
}

/// Everything `info`'s one endpoint streams.
async fn fetch(
    client: &mut FlightSqlServiceClient<Channel>,
    info: &FlightInfo,
) -> Result<Vec<RecordBatch>, arrow_flight::error::FlightError> {
    client.do_get(ticket(info)).await?.try_collect().await
}

async fn query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, arrow_flight::error::FlightError> {
    let info = client.execute(sql.to_string(), None).await?;
    fetch(client, &info).await
}

fn column<T: arrow_array::ArrowPrimitiveType>(
    batches: &[RecordBatch],
    name: &str,
) -> Vec<T::Native> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name(name).unwrap();
            col.as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<T>>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn strings(batch: &RecordBatch, name: &str) -> Vec<String> {
    let col = arrow_cast::cast(
        batch.column_by_name(name).unwrap(),
        &arrow_schema::DataType::Utf8,
    )
    .unwrap();
    let col = col.as_any().downcast_ref::<StringArray>().unwrap();
    (0..col.len()).map(|i| col.value(i).to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_runs_over_the_declared_tables_and_streams_its_result() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), Some(TOKEN)).await;
    let mut c = client(&server.addr, Some(TOKEN)).await;

    let batches = query(
        &mut c,
        "SELECT o.id, i.v FROM orders o JOIN items i ON o.id = i.k ORDER BY o.id",
    )
    .await
    .unwrap();
    assert_eq!(
        column::<arrow_array::types::Int64Type>(&batches, "id"),
        [1, 2, 3]
    );

    // A result with no row still says what its columns are.
    let info = c
        .execute("SELECT * FROM orders WHERE id > 100".into(), None)
        .await
        .unwrap();
    let schema = info.clone().try_decode_schema().unwrap();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(names, ["id", "amount", "currency"]);
    let rows: usize = fetch(&mut c, &info)
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 0);

    // A statement that names no table runs too.
    let one = query(&mut c, "SELECT 1 AS one").await.unwrap();
    assert_eq!(column::<arrow_array::types::Int64Type>(&one, "one"), [1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catalog_names_are_the_names_sql_resolves() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), Some(TOKEN)).await;
    let mut c = client(&server.addr, Some(TOKEN)).await;

    let info = c.get_catalogs().await.unwrap();
    let catalogs = fetch(&mut c, &info).await.unwrap();
    assert_eq!(strings(&catalogs[0], "catalog_name"), ["lakeleto_tables"]);

    let info = c
        .get_db_schemas(CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        })
        .await
        .unwrap();
    let schemas = fetch(&mut c, &info).await.unwrap();
    assert_eq!(strings(&schemas[0], "db_schema_name"), ["main"]);

    let info = c
        .get_tables(CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: true,
        })
        .await
        .unwrap();
    let tables = fetch(&mut c, &info).await.unwrap();
    let names = strings(&tables[0], "table_name");
    assert_eq!(
        names,
        ["items", "orders"],
        "as SQL registers them, `Items` as `items`"
    );

    // Each name, qualified and quoted as a client builds it from the listing, queries.
    for (table, rows) in [("orders", 3), ("items", 4)] {
        let sql = format!(r#"SELECT count(*) AS n FROM "lakeleto_tables"."main"."{table}""#);
        let n = query(&mut c, &sql).await.unwrap();
        assert_eq!(column::<arrow_array::types::Int64Type>(&n, "n"), [rows]);
    }
    // And unquoted, in any case, as SQL folds it.
    let n = query(&mut c, "SELECT count(*) AS n FROM ITEMS")
        .await
        .unwrap();
    assert_eq!(column::<arrow_array::types::Int64Type>(&n, "n"), [4]);

    // The schema GetTables gives is the one a query returns.
    let listed = tables[0]
        .column_by_name("table_schema")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap()
        .value(names.iter().position(|n| n == "items").unwrap())
        .to_vec();
    let listed = arrow_ipc::convert::try_schema_from_ipc_buffer(&listed).unwrap();
    let queried = c
        .execute("SELECT * FROM items".into(), None)
        .await
        .unwrap()
        .try_decode_schema()
        .unwrap();
    assert_eq!(listed.fields(), queried.fields());

    let info = c.get_table_types().await.unwrap();
    let types = fetch(&mut c, &info).await.unwrap();
    assert_eq!(strings(&types[0], "table_type"), ["TABLE"]);

    let info = c
        .get_sql_info(vec![
            SqlInfo::FlightSqlServerName,
            SqlInfo::FlightSqlServerReadOnly,
        ])
        .await
        .unwrap();
    let rows: usize = fetch(&mut c, &info)
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);
}

/// `body` as a request, carrying `header` as its `authorization` when there is one.
fn authed<T>(header: Option<&str>, body: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(body);
    if let Some(h) = header {
        req.metadata_mut()
            .insert("authorization", h.parse().unwrap());
    }
    req
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_call_needs_the_token() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), Some(TOKEN)).await;
    // A ticket a client with the token was given, to try fetching without it.
    let mut good = client(&server.addr, Some(TOKEN)).await;
    let info = good
        .execute("SELECT * FROM orders".into(), None)
        .await
        .unwrap();

    for header in [None, Some("Bearer wrong"), Some("t0ken")] {
        let mut raw = FlightServiceClient::new(channel(&server.addr).await);
        let descriptor = FlightDescriptor::new_cmd(b"anything".to_vec());
        let codes = [
            raw.handshake(authed(
                header,
                futures::stream::iter(vec![HandshakeRequest::default()]),
            ))
            .await
            .map(|_| ())
            .unwrap_err()
            .code(),
            raw.list_flights(authed(header, Criteria::default()))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.get_flight_info(authed(header, descriptor.clone()))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.poll_flight_info(authed(header, descriptor.clone()))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.get_schema(authed(header, descriptor))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.do_get(authed(header, ticket(&info)))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.do_put(authed(header, futures::stream::iter(vec![])))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.do_action(authed(header, Action::new("CancelQuery", "")))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.list_actions(authed(header, arrow_flight::Empty {}))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
            raw.do_exchange(authed(header, futures::stream::iter(vec![])))
                .await
                .map(|_| ())
                .unwrap_err()
                .code(),
        ];
        assert!(
            codes.iter().all(|c| *c == Code::Unauthenticated),
            "{header:?}: {codes:?}"
        );
    }

    // The refused fetch did not use the ticket up.
    let rows: usize = fetch(&mut good, &info)
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 3);

    // A login with the token as the password is answered with it as a bearer token.
    let mut login = client(&server.addr, None).await;
    let refused = login.handshake("anyone", "wrong").await.unwrap_err();
    assert!(refused.to_string().contains("needs its token"), "{refused}");
    login.handshake("anyone", TOKEN).await.unwrap();
    let n = query(&mut login, "SELECT count(*) AS n FROM orders")
        .await
        .unwrap();
    assert_eq!(column::<arrow_array::types::Int64Type>(&n, "n"), [3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_with_no_token_answers_anyone() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), None).await;
    let mut c = client(&server.addr, None).await;
    let n = query(&mut c, "SELECT count(*) AS n FROM orders")
        .await
        .unwrap();
    assert_eq!(column::<arrow_array::types::Int64Type>(&n, "n"), [3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_server_refuses_what_writes_and_leaves_the_rest_to_fall_back() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), Some(TOKEN)).await;
    let mut c = client(&server.addr, Some(TOKEN)).await;
    for sql in [
        "CREATE TABLE x AS SELECT 1",
        "INSERT INTO orders VALUES (4, 1.0, 'USD')",
        "COPY orders TO '/tmp/x.csv'",
        "SELECT 1; SELECT 2",
        "EXPLAIN ANALYZE SELECT * FROM orders",
    ] {
        let e = c.execute(sql.to_string(), None).await.unwrap_err();
        assert_eq!(code(&e), Code::InvalidArgument, "{sql}: {e}");
    }
    // What the server does not do at all is UNIMPLEMENTED, which a client reads as "fall back":
    // ADBC prepares a plain `execute` first, and then sends it as a statement.
    let e = c
        .prepare("SELECT * FROM orders".to_string(), None)
        .await
        .unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented, "{e}");
    let e = c
        .execute_update("DELETE FROM orders".to_string(), None)
        .await
        .unwrap_err();
    assert_eq!(code(&e), Code::Unimplemented, "{e}");
    let e = query(&mut c, "SELECT * FROM nowhere").await.unwrap_err();
    assert_eq!(code(&e), Code::InvalidArgument, "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_over_the_cap_is_an_error_not_a_short_result() {
    let dir = tempfile::tempdir().unwrap();
    let tables = fixtures(dir.path());
    let server = start(
        &tables,
        Limits {
            max_rows: 3,
            ..limits()
        },
        Some(TOKEN),
    )
    .await;
    let mut c = client(&server.addr, Some(TOKEN)).await;
    // Exactly the cap is whole.
    let three = query(&mut c, "SELECT * FROM orders").await.unwrap();
    assert_eq!(three.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    // One more is refused, whatever arrived before the refusal.
    let e = query(&mut c, "SELECT * FROM items").await.unwrap_err();
    assert_eq!(code(&e), Code::ResourceExhausted, "{e}");
    assert!(e.to_string().contains("more than 3 rows"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_past_its_deadline_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(
        &fixtures(dir.path()),
        Limits {
            timeout: Duration::from_nanos(1),
            ..limits()
        },
        Some(TOKEN),
    )
    .await;
    let mut c = client(&server.addr, Some(TOKEN)).await;
    let e = query(&mut c, "SELECT * FROM orders").await.unwrap_err();
    assert_eq!(code(&e), Code::DeadlineExceeded, "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_result_is_fetched_once_and_waits_only_so_long() {
    let dir = tempfile::tempdir().unwrap();
    let tables = fixtures(dir.path());
    let server = start(&tables, limits(), Some(TOKEN)).await;
    let mut c = client(&server.addr, Some(TOKEN)).await;

    let info = c
        .execute("SELECT * FROM orders".into(), None)
        .await
        .unwrap();
    fetch(&mut c, &info).await.unwrap();
    let e = fetch(&mut c, &info).await.unwrap_err();
    assert_eq!(code(&e), Code::NotFound, "used up: {e}");

    // A ticket no statement was given.
    let forged = arrow_flight::sql::TicketStatementQuery {
        statement_handle: vec![0u8; 16].into(),
    };
    use arrow_flight::sql::ProstMessageExt;
    use prost::Message;
    let e = c
        .do_get(Ticket::new(forged.as_any().encode_to_vec()))
        .await
        .expect_err("refused");
    assert_eq!(code(&e), Code::NotFound, "{e}");

    // At most MAX_OPEN are held; the next is refused as busy, and a fetch frees a place.
    let mut held = Vec::new();
    for _ in 0..MAX_OPEN {
        held.push(
            c.execute("SELECT * FROM orders".into(), None)
                .await
                .unwrap(),
        );
    }
    let e = c
        .execute("SELECT * FROM orders".into(), None)
        .await
        .unwrap_err();
    assert_eq!(code(&e), Code::ResourceExhausted, "{e}");
    fetch(&mut c, &held.pop().unwrap()).await.unwrap();
    let again = c
        .execute("SELECT * FROM orders".into(), None)
        .await
        .unwrap();
    fetch(&mut c, &again).await.unwrap();

    // A result nobody fetches in time is dropped, and its place freed.
    let short = start(
        &tables,
        Limits {
            held_for: Duration::from_millis(200),
            ..limits()
        },
        Some(TOKEN),
    )
    .await;
    let mut s = client(&short.addr, Some(TOKEN)).await;
    let mut stale = Vec::new();
    for _ in 0..MAX_OPEN {
        stale.push(
            s.execute("SELECT * FROM orders".into(), None)
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    let e = fetch(&mut s, &stale[0]).await.unwrap_err();
    assert_eq!(code(&e), Code::NotFound, "expired: {e}");
    let fresh = s
        .execute("SELECT * FROM orders".into(), None)
        .await
        .unwrap();
    fetch(&mut s, &fresh).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_hangs_up_mid_fetch_frees_its_statement() {
    let dir = tempfile::tempdir().unwrap();
    let mut tables = fixtures(dir.path());
    // Many batches, so a fetch that stops after the first leaves the read mid-way.
    let big = dir.path().join("big.csv");
    let mut body = String::from("n\n");
    for i in 0..200_000 {
        body.push_str(&format!("{i}\n"));
    }
    std::fs::write(&big, body).unwrap();
    tables.push(("big".into(), big));
    let server = start(
        &tables,
        Limits {
            max_rows: 1_000_000,
            ..limits()
        },
        Some(TOKEN),
    )
    .await;
    let mut c = client(&server.addr, Some(TOKEN)).await;

    for _ in 0..MAX_OPEN {
        let info = c.execute("SELECT n FROM big".into(), None).await.unwrap();
        let mut stream = c.do_get(ticket(&info)).await.unwrap();
        assert!(stream.try_next().await.unwrap().is_some(), "a first batch");
        drop(stream);
    }
    // Each read stops once its client is gone, and gives its place back.
    let mut admitted = false;
    for _ in 0..50 {
        match c.execute("SELECT 1 AS one".into(), None).await {
            Ok(info) => {
                fetch(&mut c, &info).await.unwrap();
                admitted = true;
                break;
            }
            Err(e) => {
                assert_eq!(code(&e), Code::ResourceExhausted, "{e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    assert!(admitted, "the abandoned fetches still hold every place");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_stops_reading_keeps_its_place_only_until_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut tables = fixtures(dir.path());
    // Parquet, so each statement opens it from its footer: all sixteen are open well within the
    // deadline.
    let big = dir.path().join("big.parquet");
    let n = Int64Array::from_iter_values(0..200_000);
    let batch = RecordBatch::try_from_iter([("n", Arc::new(n) as Arc<dyn Array>)]).unwrap();
    let file = std::fs::File::create(&big).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    tables.push(("big".into(), big));
    let timeout = Duration::from_secs(3);
    let server = start(
        &tables,
        Limits {
            max_rows: 1_000_000,
            timeout,
            ..limits()
        },
        Some(TOKEN),
    )
    .await;
    // Wide rows: each result is more than HTTP/2 lets the server send while nobody reads.
    let columns: Vec<String> = (0..8).map(|i| format!("n + {i} AS c{i}")).collect();
    let sql = format!("SELECT {} FROM big", columns.join(", "));
    // Each fetch on a connection of its own, with HTTP/2's smallest windows: the client takes
    // in 64 KiB it does not read, and the server then has no room to send the rest.
    // All at once, so that every one is open well within the deadline.
    let mut stalled = futures::future::join_all((0..MAX_OPEN).map(|_| {
        let (addr, sql) = (server.addr.clone(), sql.clone());
        async move {
            let channel = Channel::from_shared(addr)
                .unwrap()
                .initial_stream_window_size(65_535)
                .initial_connection_window_size(65_535)
                .connect()
                .await
                .unwrap();
            let mut c = FlightSqlServiceClient::new(channel);
            c.set_token(TOKEN.to_string());
            let info = c.execute(sql, None).await.unwrap();
            let mut stream = c.do_get(ticket(&info)).await.unwrap();
            assert!(stream.try_next().await.unwrap().is_some(), "a first batch");
            // Kept open, and read no further.
            (c, stream)
        }
    }))
    .await;
    // Every place is held by a fetch nobody reads, until the deadline gives them back.
    let mut other = client(&server.addr, Some(TOKEN)).await;
    let busy = other
        .execute("SELECT 1 AS one".into(), None)
        .await
        .unwrap_err();
    assert_eq!(code(&busy), Code::ResourceExhausted, "{busy}");
    let started = std::time::Instant::now();
    let mut admitted = false;
    while started.elapsed() < timeout * 5 {
        match other.execute("SELECT 1 AS one".into(), None).await {
            Ok(info) => {
                fetch(&mut other, &info).await.unwrap();
                admitted = true;
                break;
            }
            Err(e) => {
                assert_eq!(code(&e), Code::ResourceExhausted, "{e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    assert!(
        admitted,
        "fetches nobody reads hold every place past their deadline"
    );
    // A stalled fetch read again ends in an error, never as a result cut short.
    let (_c, mut stream) = stalled.pop().unwrap();
    let end = loop {
        match stream.try_next().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("a fetch stopped at its deadline ended as if it were whole"),
            Err(e) => break e,
        }
    };
    assert_eq!(code(&end), Code::DeadlineExceeded, "{end}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_registers_only_the_tables_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut tables = fixtures(dir.path());
    let gone = dir.path().join("gone.csv");
    std::fs::write(&gone, "x\n1\n").unwrap();
    tables.push(("gone".into(), gone.clone()));
    let server = start(&tables, limits(), Some(TOKEN)).await;
    // A declared table whose file is gone fails only the statements that name it.
    std::fs::remove_file(&gone).unwrap();
    let mut c = client(&server.addr, Some(TOKEN)).await;
    let n = query(&mut c, "SELECT count(*) AS n FROM orders")
        .await
        .unwrap();
    assert_eq!(column::<arrow_array::types::Int64Type>(&n, "n"), [3]);
    assert!(query(&mut c, "SELECT * FROM gone").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_shape_of_read_finds_the_tables_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), Some(TOKEN)).await;
    let mut c = client(&server.addr, Some(TOKEN)).await;
    for (sql, n) in [
        (
            "WITH o AS (SELECT * FROM orders) SELECT count(*) AS n FROM o",
            3,
        ),
        (
            "SELECT count(*) AS n FROM items WHERE k IN (SELECT id FROM orders)",
            3,
        ),
        (
            "SELECT count(*) AS n FROM (SELECT id FROM orders UNION ALL SELECT k FROM items) u",
            7,
        ),
        ("SELECT count(*) AS n FROM (SELECT * FROM main.orders) o", 3),
    ] {
        let rows = query(&mut c, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(
            column::<arrow_array::types::Int64Type>(&rows, "n"),
            [n],
            "{sql}"
        );
    }
    let plan = query(&mut c, "EXPLAIN SELECT * FROM orders JOIN items ON id = k")
        .await
        .unwrap();
    assert!(plan.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_declared_tables_are_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), Some(TOKEN)).await;
    let undeclared = dir.path().join("secret.csv");
    std::fs::write(&undeclared, "s\nhidden\n").unwrap();
    let mut c = client(&server.addr, Some(TOKEN)).await;
    for sql in [
        format!("SELECT * FROM '{}'", undeclared.display()),
        format!("SELECT * FROM \"{}\"", undeclared.display()),
        "SELECT * FROM datafusion.public.orders".to_string(),
        "SELECT * FROM information_schema.tables".to_string(),
    ] {
        let e = query(&mut c, &sql).await.unwrap_err();
        assert!(
            matches!(code(&e), Code::InvalidArgument | Code::NotFound),
            "{sql}: {e}"
        );
        assert!(!e.to_string().contains("hidden"), "{sql}: {e}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_listing_keeps_to_its_filters() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(&fixtures(dir.path()), limits(), Some(TOKEN)).await;
    let mut c = client(&server.addr, Some(TOKEN)).await;
    let listed =
        |catalog: Option<&str>, schema: Option<&str>, table: Option<&str>| CommandGetTables {
            catalog: catalog.map(str::to_string),
            db_schema_filter_pattern: schema.map(str::to_string),
            table_name_filter_pattern: table.map(str::to_string),
            table_types: vec![],
            include_schema: true,
        };
    for (request, expected) in [
        (listed(None, None, Some("ord%")), vec!["orders"]),
        (
            listed(Some("lakeleto_tables"), Some("m_in"), None),
            vec!["items", "orders"],
        ),
        (listed(Some("elsewhere"), None, None), vec![]),
        (listed(None, Some("other"), None), vec![]),
        (listed(None, None, Some("nothing%")), vec![]),
    ] {
        let info = c.get_tables(request).await.unwrap();
        let batches = fetch(&mut c, &info).await.unwrap();
        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| strings(b, "table_name"))
            .collect();
        assert_eq!(names, expected);
    }
}

#[test]
fn a_database_table_is_refused_at_start() {
    let named = NamedSource {
        name: "accounts".to_string(),
        source: Source::detect("sqlite:///tmp/app.db?table=accounts").unwrap(),
    };
    let e = Flight::new(EngineRegistry::local(), vec![named], limits(), None)
        .err()
        .expect("refused");
    assert!(e.to_string().contains("is a database table"), "{e}");
}

/// A self-signed certificate for `localhost` and 127.0.0.1, and its key, PEM.
fn certificate() -> (String, String) {
    let made =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    (made.cert.pem(), made.signing_key.serialize_pem())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn over_tls_a_client_that_trusts_the_certificate_queries_and_a_plaintext_one_cannot() {
    let dir = tempfile::tempdir().unwrap();
    let tables = fixtures(dir.path());
    let (cert, key) = certificate();
    let server = start_with(
        &tables,
        limits(),
        Some(TOKEN),
        Some(Tls::from_pem(cert.clone(), key)),
    )
    .await;
    let https = server.addr.replacen("http://", "https://", 1);
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&cert))
        .domain_name("localhost");
    let channel = Channel::from_shared(https.clone())
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut c = FlightSqlServiceClient::new(channel);
    c.set_token(TOKEN.to_string());
    let n = query(&mut c, "SELECT count(*) AS n FROM orders")
        .await
        .unwrap();
    assert_eq!(column::<arrow_array::types::Int64Type>(&n, "n"), [3]);

    // A client that does not trust the certificate gets no answer.
    let untrusting = Channel::from_shared(https)
        .unwrap()
        .tls_config(tonic::transport::ClientTlsConfig::new().domain_name("localhost"))
        .unwrap()
        .connect()
        .await;
    assert!(
        untrusting.is_err(),
        "a certificate nobody vouches for was accepted"
    );

    // Nor does one that speaks plaintext to it, within 10 s rather than never.
    let probe = async {
        let Ok(plain) = Channel::from_shared(server.addr.clone())
            .unwrap()
            .connect()
            .await
        else {
            return true;
        };
        let mut c = FlightSqlServiceClient::new(plain);
        c.set_token(TOKEN.to_string());
        query(&mut c, "SELECT 1 AS one").await.is_err()
    };
    let refused = tokio::time::timeout(Duration::from_secs(10), probe)
        .await
        .expect("a plaintext client to a TLS server got neither an answer nor an error");
    assert!(refused, "a plaintext client was answered by a TLS server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_certificate_that_cannot_be_used_is_refused_before_serving() {
    let (cert, _) = certificate();
    let (_, other_key) = certificate();
    for tls in [
        Tls::from_pem("not a certificate", "not a key"),
        Tls::from_pem(cert.clone(), "not a key"),
        Tls::from_pem(cert, other_key),
    ] {
        let flight = Flight::new(EngineRegistry::local(), Vec::new(), limits(), None).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let served = tokio::time::timeout(
            Duration::from_secs(10),
            lakeleto::flight::serve_on(listener, flight, Some(&tls), std::future::pending()),
        )
        .await
        .expect("a certificate that cannot be used was served with");
        let e = served.expect_err("refused");
        assert!(e.to_string().contains("TLS certificate and key"), "{e}");
    }
}

/// Run `command` and wait for it to exit, for at most 30 s: what it exited with, and its stderr. A
/// command that is still running then is killed and the test fails, never hangs.
fn exits_within_30s(
    command: &mut std::process::Command,
    what: &str,
) -> (std::process::ExitStatus, String) {
    let mut child = command
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut said = String::new();
        let _ = std::io::Read::read_to_string(&mut std::io::BufReader::new(stderr), &mut said);
        said
    });
    let waited = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return (status, reader.join().unwrap());
        }
        if waited.elapsed() > Duration::from_secs(30) {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("{what}: still running after 30 s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `lakeleto` with none of the caller's `LAKELETO_*` settings: a `LAKELETO_FLIGHT_TLS_CERT` and
/// `_KEY`, or a `LAKELETO_TOKEN`, in the shell that runs the tests would change what it does.
fn lakeleto_bin() -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_lakeleto"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("LAKELETO_") {
            command.env_remove(name);
        }
    }
    command
}

/// The binary, run as an operator would: plaintext on an address other than loopback is refused
/// unless `--plaintext` says something in front terminates TLS.
#[test]
fn plaintext_off_loopback_needs_tls_or_plaintext_said_out_loud() {
    let dir = tempfile::tempdir().unwrap();
    let tables = fixtures(dir.path());
    let orders = format!("orders={}", tables[0].1.display());
    let (status, said) = exits_within_30s(
        lakeleto_bin().args(["flight", "--addr", "0.0.0.0:0", "--table", &orders]),
        "plaintext on 0.0.0.0 with no --plaintext started a server",
    );
    assert!(!status.success());
    assert!(
        said.contains("--tls-cert") && said.contains("--plaintext"),
        "{said}"
    );

    // --plaintext starts it, and says what that means.
    let mut started = lakeleto_bin()
        .args([
            "flight",
            "--addr",
            "0.0.0.0:0",
            "--plaintext",
            "--token",
            TOKEN,
            "--table",
            &orders,
        ])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // Its stderr is read on a thread, so a server that never says where it listens fails the test
    // after 30 s instead of holding it.
    let stderr = started.stderr.take().unwrap();
    let (said, heard) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut seen = String::new();
        for line in std::io::BufRead::lines(std::io::BufReader::new(stderr)) {
            let Ok(line) = line else { break };
            seen.push_str(&line);
            seen.push('\n');
            if line.contains("Flight SQL on") {
                break;
            }
        }
        let _ = said.send(seen);
    });
    let seen = heard.recv_timeout(Duration::from_secs(30));
    started.kill().unwrap();
    let _ = started.wait();
    let seen = seen.expect("--plaintext on 0.0.0.0 did not say where it listens within 30 s");
    assert!(
        seen.contains("in the clear") && seen.contains("Flight SQL on grpc://0.0.0.0:"),
        "{seen}"
    );

    // Half a certificate is refused by the command line itself.
    let (status, said) = exits_within_30s(
        lakeleto_bin().args(["flight", "--tls-cert", "cert.pem", "--table", &orders]),
        "--tls-cert without --tls-key started a server",
    );
    assert!(!status.success());
    assert!(said.contains("--tls-key"), "{said}");
}

#[test]
fn an_empty_token_is_refused_at_start_not_taken_for_none() {
    let e = Flight::new(
        EngineRegistry::local(),
        Vec::new(),
        limits(),
        Some(String::new()),
    )
    .err()
    .expect("refused");
    assert!(e.to_string().contains("the token is empty"), "{e}");
}

/// A Delta table is listed with the schema its queries return, read from its log alone: a listing
/// never loads the table, as a query of it does.
#[cfg(feature = "delta")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_tables_schema_is_listed_from_its_metadata_as_queries_see_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut tables = fixtures(dir.path());
    tables.push((
        "events".into(),
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/delta_checkpointed"),
    ));
    let server = start(&tables, limits(), Some(TOKEN)).await;
    let mut c = client(&server.addr, Some(TOKEN)).await;
    let info = c
        .get_tables(CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: Some("events".into()),
            table_types: vec![],
            include_schema: true,
        })
        .await
        .unwrap();
    let tables = fetch(&mut c, &info).await.unwrap();
    assert_eq!(strings(&tables[0], "table_name"), ["events"]);
    let listed = tables[0]
        .column_by_name("table_schema")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap()
        .value(0)
        .to_vec();
    let listed = arrow_ipc::convert::try_schema_from_ipc_buffer(&listed).unwrap();
    let queried = c
        .execute("SELECT * FROM events".into(), None)
        .await
        .unwrap()
        .try_decode_schema()
        .unwrap();
    assert_eq!(listed.fields(), queried.fields());
    assert!(!listed.fields().is_empty());
}

#[test]
fn table_names_are_identifiers_and_distinct_as_sql_reads_them() {
    let dir = tempfile::tempdir().unwrap();
    let tables = fixtures(dir.path());
    let named = |name: &str| NamedSource {
        name: name.to_string(),
        source: Source::detect(&tables[0].1).unwrap(),
    };
    let e = Flight::new(
        EngineRegistry::local(),
        vec![named("Orders"), named("ORDERS")],
        limits(),
        None,
    )
    .err()
    .expect("refused");
    assert!(
        e.to_string().contains("two tables are named `orders`"),
        "{e}"
    );
    let e = Flight::new(
        EngineRegistry::local(),
        vec![named("my-orders")],
        limits(),
        None,
    )
    .err()
    .expect("refused");
    assert!(e.to_string().contains("not a table name"), "{e}");
}
