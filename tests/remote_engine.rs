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
use lakeleto::{LocalReaderEngine, NamedSource, Source};

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
    let app = router(read, sql, None, 10_000, token, None, true, store());
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
        let rb = engine.preview(&source, 10).unwrap();

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
        let rb = engine.preview(&source, 10).unwrap();
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
        let rb = engine.preview(&source, 0).unwrap();
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
        let msg = match engine.query("SELECT 1", &tables) {
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
            .query("SELECT id, name FROM t ORDER BY id DESC", &tables)
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
            .query_capped("SELECT id FROM t ORDER BY id DESC", &tables, 1)
            .unwrap();
        assert_eq!(capped.num_rows(), 1);
        // ...and the column shape is unchanged by capping.
        assert_eq!(capped.schema.fields().len(), 1);
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

        let err = engine.schema(&source).unwrap_err().to_string();
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
        let err = engine.profile(&source, 100).unwrap_err().to_string();
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
        let err = match engine.preview(&source, 10) {
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
    let err = match engine.preview(&source, 10) {
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
