//! `RemoteEngine` **row** round-trip against a live in-process `lakeleto serve` — the remote
//! read path end to end over real HTTP. The metadata calls (`schema`/`profile`) were already
//! wired; these cover the part that makes the seam usable: `preview` and `query` returning
//! actual rows, encoded as an Arrow IPC stream.
//!
//! What is being proven is *fidelity*, not just plumbing. The same endpoints answer JSON to a
//! browser, and a JSON rendering is lossy in ways that matter to an engine: types collapse to
//! whatever a JSON scalar can carry, and an empty window loses its columns. So each test
//! asserts on the Arrow types and values the client gets back, not on row counts alone.
//!
//! Run with: `cargo test --features serve,remote --test remote_engine`.
#![cfg(all(feature = "serve", feature = "remote"))]

use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use lakeleto::api::router;
use lakeleto::engine::remote::RemoteEngine;
use lakeleto::engine::Engine;
use lakeleto::workspace::{LocalStore, WorkspaceStore};
use lakeleto::RequestContext;
use lakeleto::{EngineRegistry, LocalReaderEngine, NamedSource, Source};

/// 2^53 + 1 — the smallest integer an IEEE-754 double cannot represent. Any client that routes
/// this column through a JSON number rounds it to 9007199254740992; the Arrow arm must not.
const BIG_INT: i64 = 9_007_199_254_740_993;

fn fixture_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        // A timestamp is the clearest "not stringified" witness: the JSON rendering of this
        // column is a string, so a client reading JSON cannot get the Arrow type back.
        Field::new(
            "seen",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

/// Write a five-column, three-row Parquet fixture covering int / string / float / bool /
/// timestamp, with the out-of-double-range integer in row 1.
fn write_fixture(path: &Path) {
    let schema = fixture_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, BIG_INT, -7])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                Some("beta"),
                Some("gamma"),
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.25)])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_700_000_000_000_000,
                1_700_000_000_000_001,
                1_700_000_000_000_002,
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A throwaway workspace store — these tests never touch workspace endpoints.
fn store() -> Arc<dyn WorkspaceStore> {
    let dir = std::env::temp_dir().join("lakeleto-test-remote-engine-store");
    Arc::new(LocalStore::at(dir).unwrap())
}

/// Start an in-process `lakeleto serve` on an ephemeral port; returns the address to point a
/// [`RemoteEngine`] at. `sql` is the engine backing `POST /v1/query` (`None` → the endpoint
/// reports the missing feature, exactly as a server built without `sql` does).
async fn serve(sql: Option<Arc<dyn Engine>>) -> std::net::SocketAddr {
    serve_with_token(sql, None).await
}

/// [`serve`], plus a bearer token the server will require on every `/v1/*` request.
async fn serve_with_token(
    sql: Option<Arc<dyn Engine>>,
    token: Option<String>,
) -> std::net::SocketAddr {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let engines = match sql {
        Some(sql) => EngineRegistry::new(read).with_sql(sql),
        None => EngineRegistry::new(read),
    };
    let app = router(engines, 10_000, token, None, true, store());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preview_round_trips_arrow_types_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let addr = serve(None).await;

    // The client side is blocking, like every Engine.
    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let source = Source::resolve(&path, None).unwrap();
        let rb = engine
            .preview(&RequestContext::detached(), &source, 10)
            .unwrap();

        // The schema survives the wire whole — same fields, same types, same order. A JSON
        // round-trip would hand back five strings-and-numbers with types re-inferred.
        assert_eq!(rb.schema.as_ref(), fixture_schema().as_ref());
        assert_eq!(rb.num_rows(), 3);
        let batch = &rb.batches[0];
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
        assert_eq!(batch.column(3).data_type(), &DataType::Boolean);
        assert_eq!(
            batch.column(4).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        // Values, not just types.
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alpha");
        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(scores.is_null(1), "the null score stays null, not 0.0");
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preview_preserves_an_integer_a_json_number_would_round() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let addr = serve(None).await;

    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let source = Source::resolve(&path, None).unwrap();
        let rb = engine
            .preview(&RequestContext::detached(), &source, 10)
            .unwrap();
        let ids = rb.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id comes back as an Int64 array");
        assert_eq!(ids.value(1), BIG_INT);
        // Belt and braces: the value the double-rounding failure mode would produce.
        assert_ne!(ids.value(1), 9_007_199_254_740_992);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_row_window_still_carries_its_columns() {
    // The fidelity case that is easiest to get wrong: with no batches to read a schema off,
    // an implementation that infers the schema from the first batch returns "no columns".
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let addr = serve(None).await;

    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let source = Source::resolve(&path, None).unwrap();
        let rb = engine
            .preview(&RequestContext::detached(), &source, 0)
            .unwrap();
        assert_eq!(rb.num_rows(), 0);
        assert!(rb.is_empty());
        assert_eq!(rb.schema.fields().len(), 5);
        assert_eq!(rb.schema.field(0).name(), "id");
        assert_eq!(
            rb.schema.field(4).data_type(),
            &fixture_schema().field(4).data_type().clone()
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_against_a_server_without_sql_reports_the_missing_feature() {
    // The server answers 501 with a body explaining which feature is missing. That message has
    // to reach the user — a bare "HTTP status client error (501 Not Implemented)" tells an
    // operator nothing about what to rebuild.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let addr = serve(None).await;

    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let source = Source::resolve(&path, None).unwrap();
        let tables = vec![NamedSource {
            name: "t".to_string(),
            source,
        }];
        let msg = match engine.query(&RequestContext::detached(), "SELECT 1", &tables) {
            // `RowBatch` is not `Debug` (it is raw Arrow), so unwrap the error by hand.
            Err(e) => e.to_string(),
            Ok(rb) => panic!(
                "a server with no SQL engine answered {} rows",
                rb.num_rows()
            ),
        };
        assert!(msg.contains("sql"), "unreadable remote error: {msg}");
        assert!(
            msg.contains("/v1/query"),
            "error should name the endpoint: {msg}"
        );
    })
    .await
    .unwrap();
}

#[cfg(feature = "sql")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_and_query_capped_return_typed_rows() {
    use lakeleto::engine::sql::DataFusionEngine;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let sql: Arc<dyn Engine> = Arc::new(DataFusionEngine::new());
    let addr = serve(Some(sql)).await;

    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let source = Source::resolve(&path, None).unwrap();
        let tables = vec![NamedSource {
            name: "t".to_string(),
            source,
        }];

        let rb = engine
            .query(
                &RequestContext::detached(),
                "SELECT id, name FROM t ORDER BY id DESC",
                &tables,
            )
            .unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.schema.fields().len(), 2);
        let ids = rb.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id stays an Int64 through SQL and the wire");
        assert_eq!(ids.value(0), BIG_INT);

        // The cap is pushed into the request, so the server plans with it.
        let capped = engine
            .query_capped(
                &RequestContext::detached(),
                "SELECT id FROM t ORDER BY id DESC",
                &tables,
                1,
            )
            .unwrap();
        assert_eq!(capped.num_rows(), 1);
        // ...and the column shape is unchanged by capping.
        assert_eq!(capped.schema.fields().len(), 1);
    })
    .await
    .unwrap();
}

/// A source's read options travel with it — on the metadata and row calls as query parameters,
/// and inside `POST /v1/query`'s table list — so the peer reads it as the caller asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flattened_source_is_read_flattened_by_the_peer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("people.ndjson");
    std::fs::write(
        &path,
        "{\"id\":1,\"user\":{\"name\":\"Grace\"}}\n{\"id\":2,\"user\":{\"name\":\"Ada\"}}\n",
    )
    .unwrap();
    let addr = serve(None).await;

    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let source = Source::resolve(&path, None)
            .unwrap()
            .with_flatten(Some(lakeleto::Flatten::All));
        let ctx = RequestContext::detached();
        let schema = engine.schema(&ctx, &source).unwrap();
        let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(cols, ["id", "user.name"]);
        let rb = engine.preview(&ctx, &source, 10).unwrap();
        let names = rb.batches[0]
            .column_by_name("user.name")
            .expect("the peer flattened the rows too")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        assert_eq!(names.value(1), "Ada");
    })
    .await
    .unwrap();
}

#[cfg(feature = "sql")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_tables_read_options_reach_the_peer() {
    use arrow_array::StructArray;
    use lakeleto::engine::sql::DataFusionEngine;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("users.parquet");
    let user = StructArray::from(vec![(
        Arc::new(Field::new("name", DataType::Utf8, true)),
        Arc::new(StringArray::from(vec!["Grace", "Ada"])) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("user", user.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(user)],
    )
    .unwrap();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let sql: Arc<dyn Engine> = Arc::new(DataFusionEngine::new());
    let addr = serve(Some(sql)).await;

    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let source = Source::resolve(&path, None)
            .unwrap()
            .with_flatten(Some(lakeleto::Flatten::All));
        // `"user.name"` only exists if the peer registered the table flattened.
        let rb = engine
            .query(
                &RequestContext::detached(),
                r#"SELECT "user.name" FROM t ORDER BY id DESC"#,
                &[NamedSource {
                    name: "t".to_string(),
                    source,
                }],
            )
            .unwrap();
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.schema.field(0).name(), "user.name");
    })
    .await
    .unwrap();
}

// ---- error surfacing: every call, not half of them --------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_token_surfaces_the_servers_own_message_on_metadata_calls_too() {
    // `send` exists because `error_for_status` throws the server's `{"error": …}` body away.
    // `schema` and `profile` used to call `error_for_status` anyway, so exactly the calls a
    // misconfigured client makes first reported "HTTP status client error (401 Unauthorized)"
    // and nothing about the token. Both must now carry the server's own sentence.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let addr = serve_with_token(None, Some("the-right-token".to_string())).await;

    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), Some("the-wrong-token".into()));
        let source = Source::resolve(&path, None).unwrap();

        let err = engine
            .schema(&RequestContext::detached(), &source)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unauthorized"),
            "the server's message, not a bare status: {err}"
        );
        assert!(
            err.contains("Authorization: Bearer"),
            "the server explains how to send the token: {err}"
        );
        assert!(
            err.contains("/v1/schema"),
            "the error names the call: {err}"
        );

        // `profile` is the other `get_json` caller — it must not have its own error path.
        let err = engine
            .profile(&RequestContext::detached(), &source, 100)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unauthorized"), "{err}");
        assert!(err.contains("/v1/profile"), "{err}");
    })
    .await
    .unwrap();
}

// ---- the response byte cap ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_response_that_declares_more_than_the_cap_is_refused_before_it_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let addr = serve(None).await;

    tokio::task::spawn_blocking(move || {
        // 64 bytes is far under any real Arrow stream; the point is the cap, not the number.
        let engine = RemoteEngine::new(format!("http://{addr}"), None).with_max_response_bytes(64);
        let source = Source::resolve(&path, None).unwrap();
        let err = match engine.preview(&RequestContext::detached(), &source, 10) {
            Err(e) => e.to_string(),
            Ok(rb) => panic!("buffered an over-cap body ({} rows)", rb.num_rows()),
        };
        assert!(err.contains("too large"), "{err}");
        assert!(err.contains("64-byte cap"), "{err}");
        assert!(
            err.contains("declares"),
            "a response with a Content-Length is refused on the declaration, before the body \
             is read at all: {err}"
        );
    })
    .await
    .unwrap();
}

/// A hand-rolled HTTP/1.1 server that answers any request with a **chunked** body of at least
/// `total` bytes and no `Content-Length`.
///
/// `axum` always sets a `Content-Length`, so the only way to exercise the second half of the
/// guard — the bound on the read itself, which is what catches a peer that declares no length
/// (or lies about one) — is to speak HTTP by hand.
fn chunked_server(total: usize) -> std::net::SocketAddr {
    use std::io::{Read, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let Some(Ok(mut sock)) = listener.incoming().next() else {
            return;
        };
        let mut scratch = [0u8; 2048];
        let _ = sock.read(&mut scratch); // the request; its contents do not matter here
        if sock
            .write_all(
                b"HTTP/1.1 200 OK\r\n\
                  Content-Type: application/vnd.apache.arrow.stream\r\n\
                  Transfer-Encoding: chunked\r\n\r\n",
            )
            .is_err()
        {
            return;
        }
        let chunk = vec![b'x'; 4096];
        let mut sent = 0usize;
        // Every write is allowed to fail: the client hangs up the moment it hits its cap, which
        // is the behaviour under test.
        while sent < total {
            if sock
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .and_then(|()| sock.write_all(&chunk))
                .and_then(|()| sock.write_all(b"\r\n"))
                .is_err()
            {
                return;
            }
            sent += chunk.len();
        }
        let _ = sock.write_all(b"0\r\n\r\n");
    });
    addr
}

#[test]
fn a_chunked_response_cannot_run_past_the_byte_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.parquet");
    write_fixture(&path);
    let addr = chunked_server(1024 * 1024);

    let engine = RemoteEngine::new(format!("http://{addr}"), None).with_max_response_bytes(2048);
    let source = Source::resolve(&path, None).unwrap();
    let err = match engine.preview(&RequestContext::detached(), &source, 10) {
        Err(e) => e.to_string(),
        Ok(rb) => panic!(
            "buffered an unbounded chunked body ({} rows)",
            rb.num_rows()
        ),
    };
    assert!(err.contains("too large"), "{err}");
    assert!(err.contains("2048-byte cap"), "{err}");
    assert!(
        !err.contains("declares"),
        "with no Content-Length the read bound is what must fire: {err}"
    );
}

/// A database source must be refused before any of it reaches the wire.
///
/// `RemoteEngine` sends `source.uri()` verbatim, because the peer is what resolves the string. For
/// a database source that URI *is* the connection string, password included, so transmitting it
/// would hand the credential to another server and write it into that server's request log. The
/// engine used to only document that; now it refuses.
///
/// The endpoint here is deliberately a closed port. If the guard did not fire, the call would fail
/// trying to connect — so `Forbidden` rather than a transport error is what proves nothing was
/// sent, which is the property worth testing. Asserting on the message alone could not distinguish
/// "refused before sending" from "sent, then rejected".
#[test]
fn a_database_source_is_refused_before_it_can_reach_a_peer() {
    // Port 1 on loopback: reserved, and nothing this test controls is listening.
    let engine = RemoteEngine::new("http://127.0.0.1:1", None);
    const URI: &str = "postgres://app:hunter2@db.internal:5432/sales?table=orders";

    let explicit = Source::with_format(URI, lakeleto::Format::Database);
    // The same URI with no format at all: still unresolved, so a format-only check would let this
    // one through and the scheme test is what catches it.
    let inferred = Source::unresolved(URI, None).unwrap();

    for source in [&explicit, &inferred] {
        let mut failures = vec![engine
            .schema(&RequestContext::detached(), source)
            .expect_err("schema must refuse a database source")];
        // `RowBatch` has no `Debug`, so unlike `schema` above these two cannot use `expect_err`
        // (it has to format the `Ok` value) and go through `.err().expect(..)` instead.
        failures.push(
            engine
                .preview(&RequestContext::detached(), source, 10)
                .err()
                .expect("preview must refuse a database source"),
        );
        failures.push(
            engine
                .query(
                    &RequestContext::detached(),
                    "select 1",
                    &[NamedSource {
                        name: "t".into(),
                        source: source.clone(),
                    }],
                )
                .err()
                .expect("query must refuse a database source"),
        );

        for err in failures {
            let msg = err.to_string();
            assert!(
                msg.contains("will not read a database source"),
                "expected the guard to refuse before connecting, got: {msg}"
            );
            assert!(
                !msg.contains("hunter2"),
                "the refusal must not quote the credential it is protecting: {msg}"
            );
        }
    }
}

/// Serve `app` on an ephemeral port, for a stand-in that is not a `lakeleto serve`.
async fn serve_app(app: axum::Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// The client's capabilities are the server's, as `GET /v1/engines` reports them, less what the
/// client doesn't implement. None of it is made up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capabilities_are_the_servers_as_v1_engines_reports_them() {
    let addr = serve(None).await;
    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        assert_eq!(engine.endpoint(), format!("http://{addr}"));
        let caps = engine.capabilities();
        assert_eq!(caps.engine, format!("remote (HTTP @ http://{addr})"));
        assert_eq!(caps.formats, lakeleto::engine::readable_formats());
        assert!(!caps.sql, "the server has no SQL engine");
        assert!(caps.profile && caps.remote);
        assert!(
            !caps.scan && !caps.filtered_stats,
            "this client implements neither, whatever the server does"
        );

        let server = engine.server(&RequestContext::detached()).unwrap();
        assert_eq!(
            server.protocol.as_deref(),
            Some(lakeleto::protocol::PROTOCOL_VERSION)
        );
        assert_eq!(server.version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        assert_eq!(server.engines.len(), 1);
        assert!(server.engines[0].scan, "the server's own engine scans");
    })
    .await
    .unwrap();
}

/// `lakeleto engines --remote-url` against a live server lists it, and succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engines_lists_a_live_server() {
    let addr = serve(None).await;
    tokio::task::spawn_blocking(move || {
        let cli = lakeleto::cli::Cli {
            output: Some(lakeleto::render::Output::Table),
            out: None,
            engine: lakeleto::cli::EngineChoice::Auto,
            remote_url: Some(format!("http://{addr}")),
            remote_token: None,
            format: None,
            json_path: None,
            flatten: None,
            max_decompressed: lakeleto::source::DEFAULT_MAX_DECOMPRESSED,
            cmd: lakeleto::cli::Cmd::Engines,
        };
        assert_eq!(lakeleto::cli::run(cli).unwrap(), 0);
    })
    .await
    .unwrap();
}

#[cfg(feature = "sql")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_that_runs_sql_is_reported_as_running_it() {
    use lakeleto::engine::sql::DataFusionEngine;

    let addr = serve(Some(Arc::new(DataFusionEngine::new()))).await;
    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        assert!(engine.capabilities().sql);
        let server = engine.server(&RequestContext::detached()).unwrap();
        let names: Vec<&str> = server.engines.iter().map(|e| e.engine.as_str()).collect();
        assert_eq!(names, ["local (built-in reader)", "sql (DataFusion)"]);
    })
    .await
    .unwrap();
}

/// A server that serves part of `/v1` and no `/v1/engines`, as a hosted plane does: the client
/// reports its capabilities as unknown rather than guessing them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_without_v1_engines_has_unknown_capabilities() {
    let app = axum::Router::new().route("/v1/preview", axum::routing::get(|| async { "" }));
    let addr = serve_app(app).await;
    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let caps = engine.capabilities();
        assert_eq!(
            caps.engine,
            format!("remote (HTTP @ http://{addr}), capabilities unknown")
        );
        assert!(caps.formats.is_empty(), "{:?}", caps.formats);
        assert!(!caps.sql && !caps.profile && !caps.scan && !caps.filtered_stats);
        assert!(caps.remote);
        let err = engine
            .server(&RequestContext::detached())
            .expect_err("there is no /v1/engines to answer");
        assert!(err.to_string().contains("/v1/engines"), "{err}");
    })
    .await
    .unwrap();
}

/// The server is asked once and its answer kept; an ask that failed is made again, since a server
/// that was down may be up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_servers_answer_is_kept_and_a_failed_ask_is_made_again() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    // Fails the first ask and answers the rest, counting them.
    let asked = Arc::new(AtomicUsize::new(0));
    let counter = asked.clone();
    let app = axum::Router::new().route(
        "/v1/engines",
        axum::routing::get(move || {
            let counter = counter.clone();
            async move {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        axum::Json(serde_json::json!({ "error": "warming up" })),
                    )
                        .into_response();
                }
                axum::Json(serde_json::json!({
                    "version": "9.9.9",
                    "protocol": "1.3",
                    "engine": {
                        "engine": "reader",
                        "formats": ["parquet"],
                        "sql": false,
                        "profile": true,
                        "remote": false,
                    },
                    "sql_available": true,
                    "a field this client has never heard of": [1, 2, 3],
                }))
                .into_response()
            }
        }),
    );
    let addr = serve_app(app).await;
    tokio::task::spawn_blocking(move || {
        let engine = RemoteEngine::new(format!("http://{addr}"), None);
        let ctx = RequestContext::detached();
        let err = engine.server(&ctx).expect_err("the first ask fails");
        assert!(err.to_string().contains("warming up"), "{err}");

        let caps = engine.capabilities();
        assert_eq!(caps.formats, ["parquet"]);
        assert!(caps.sql && caps.profile);
        let server = engine.server(&ctx).unwrap();
        engine.capabilities();
        assert_eq!(server.protocol.as_deref(), Some("1.3"));
        assert_eq!(server.version.as_deref(), Some("9.9.9"));
        assert!(server.engines.is_empty(), "this server predates the list");
        assert_eq!(
            asked.load(Ordering::SeqCst),
            2,
            "asked again after the failure, then never again"
        );
    })
    .await
    .unwrap();
}

/// A server can read through another one. A `lakeleto serve` whose read engine is remote reports
/// the far server's formats at `/v1/engines`, asking for them off its async threads, where the
/// blocking client may wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_reading_through_another_reports_the_far_servers_formats() {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let far = serve(None).await;
    // Built, and below dropped, where the blocking client may build and drop its runtime.
    let remote = tokio::task::spawn_blocking(move || {
        Arc::new(RemoteEngine::new(format!("http://{far}"), None)) as Arc<dyn Engine>
    })
    .await
    .unwrap();
    let near = router(
        EngineRegistry::new(remote.clone()),
        10_000,
        None,
        None,
        true,
        store(),
    );
    let r = near
        .oneshot(
            Request::builder()
                .uri("/v1/engines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        json["engine"]["engine"],
        format!("remote (HTTP @ http://{far})")
    );
    assert_eq!(
        json["engine"]["formats"],
        serde_json::json!(lakeleto::engine::readable_formats())
    );
    tokio::task::spawn_blocking(move || drop(remote))
        .await
        .unwrap();
}
