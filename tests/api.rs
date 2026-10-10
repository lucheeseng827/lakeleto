//! HTTP API tests for `lakeleto serve` (`--features serve`). Drives the router with
//! `tower::oneshot` — no socket. Run with: `cargo test --features serve`.
#![cfg(feature = "serve")]

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt; // oneshot

use lakeleto::api::router;
use lakeleto::engine::Engine;
use lakeleto::workspace::{LocalStore, WorkspaceStore};
use lakeleto::EngineRegistry;
use lakeleto::LocalReaderEngine;
use lakeleto::RequestContext;

const CSV: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/people.csv");

/// A throwaway workspace store for tests that don't exercise workspaces — a fixed, reused temp
/// subdir (never written to by these routers, so no isolation concern).
fn generic_store() -> Arc<dyn WorkspaceStore> {
    let dir = std::env::temp_dir().join("lakeleto-test-generic-store");
    Arc::new(LocalStore::at(dir).unwrap())
}

fn app() -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    router(
        EngineRegistry::new(read),
        10_000,
        None,
        None,
        true,
        generic_store(),
    )
}

fn app_auth(token: &str) -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    // loopback = true, so the `?token=` query form is accepted (the local browser flow).
    router(
        EngineRegistry::new(read),
        10_000,
        Some(token.to_string()),
        None,
        true,
        generic_store(),
    )
}

/// A token-gated router bound to a *non-loopback* address (loopback = false), where the
/// `?token=` query credential must be refused and only the header accepted.
fn app_auth_net(token: &str) -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    router(
        EngineRegistry::new(read),
        10_000,
        Some(token.to_string()),
        None,
        false,
        generic_store(),
    )
}

/// A router confined to `root` via `--root` (loopback, no token). Root is canonicalized, as the
/// CLI does, so the confinement's `starts_with` check is robust to symlinked path prefixes.
fn app_root(root: std::path::PathBuf) -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let root = std::fs::canonicalize(&root).unwrap();
    router(
        EngineRegistry::new(read),
        10_000,
        None,
        Some(root),
        true,
        generic_store(),
    )
}

/// A router over an explicit workspace store (for the workspace-endpoint tests). loopback, no
/// token, no root — pass a store shared across requests within a test to observe persistence.
fn app_store(store: Arc<dyn WorkspaceStore>) -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    router(EngineRegistry::new(read), 10_000, None, None, true, store)
}

async fn get_json(uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn healthz_ok() {
    let resp = app()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn engines_lists_endpoints() {
    let (status, json) = get_json("/v1/engines").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["sql_available"], serde_json::json!(false));
    assert!(!json["endpoints"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn schema_endpoint() {
    let (status, json) = get_json(&format!("/v1/schema?path={CSV}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["columns"].as_array().unwrap().len(), 5);
}

#[tokio::test]
async fn preview_endpoint_respects_limit() {
    let (status, json) = get_json(&format!("/v1/preview?path={CSV}&limit=3")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["num_rows"].as_u64().unwrap(), 3);
    assert_eq!(json["rows"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn profile_endpoint() {
    let (status, json) = get_json(&format!("/v1/profile?path={CSV}")).await;
    assert_eq!(status, StatusCode::OK);
    let score = json["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "score")
        .unwrap();
    assert_eq!(score["null_count"].as_u64().unwrap(), 2);
}

#[tokio::test]
async fn query_without_sql_engine_is_501() {
    // Router built with sql = None → the query endpoint reports the missing feature.
    let body = serde_json::json!({ "sql": "SELECT 1", "file": CSV }).to_string();
    let resp = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn missing_file_is_not_found() {
    let (status, json) = get_json("/v1/schema?path=/no/such/file.parquet").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(json["error"].is_string());
}

async fn get_raw(uri: &str) -> (StatusCode, String, Vec<u8>) {
    let resp = app()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, ct, bytes)
}

#[tokio::test]
async fn root_serves_the_embedded_spa() {
    let (status, ct, body) = get_raw("/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/html"), "content-type was {ct}");
    assert!(String::from_utf8_lossy(&body).contains("Lakeleto"));
}

#[tokio::test]
async fn spa_fallback_for_client_routes() {
    // A non-API path that isn't a real asset → index.html, so client-side routing works.
    let (status, ct, _) = get_raw("/some/client/route").await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("text/html"));
}

#[tokio::test]
async fn unknown_api_path_is_404_not_the_spa() {
    // The `/v1/*` namespace must 404 as JSON, never fall through to the SPA.
    let (status, ct, _) = get_raw("/v1/bogus").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(ct.starts_with("application/json"), "content-type was {ct}");
}

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples");

#[tokio::test]
async fn rows_endpoint_windows_offset_limit() {
    let (status, json) = get_json(&format!("/v1/rows?path={CSV}&offset=1&limit=2")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["num_rows"].as_u64().unwrap(), 2);
    assert_eq!(json["offset"].as_u64().unwrap(), 1);
    assert_eq!(
        json["rows"].as_array().unwrap()[0]["id"].as_i64().unwrap(),
        2
    );
}

#[tokio::test]
async fn rows_endpoint_sorts_descending() {
    let (status, json) = get_json(&format!("/v1/rows?path={CSV}&sort=id&desc=1&limit=3")).await;
    assert_eq!(status, StatusCode::OK);
    // people.csv has ids 1..8 → first row after DESC sort is 8.
    assert_eq!(
        json["rows"].as_array().unwrap()[0]["id"].as_i64().unwrap(),
        8
    );
}

#[tokio::test]
async fn rows_endpoint_filters() {
    // London appears twice (Ada, Alan).
    let (status, json) =
        get_json(&format!("/v1/rows?path={CSV}&filter=city:contains:London")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["matched_rows"].as_u64().unwrap(), 2);
}

/// `/v1/rows` shows a timestamp labelled `UTC`, as pandas and pyarrow label tz-aware UTC data, and
/// filters on the text it shows, in a build with no time zone database as in one with it.
#[tokio::test]
async fn rows_endpoint_shows_and_filters_a_utc_timestamp() {
    use arrow_array::{ArrayRef, RecordBatch, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");
    let at = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![Field::new("at", at, true)]));
    let values =
        TimestampMicrosecondArray::from(vec![1_704_164_645_123_456, 1_719_791_999_000_000])
            .with_timezone("UTC");
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as ArrayRef]).unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let path = path.display();

    let (status, json) = get_json(&format!("/v1/rows?path={path}")).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        json["rows"][0]["at"], "2024-01-02T03:04:05.123456Z",
        "{json}"
    );
    assert_eq!(json["rows"][1]["at"], "2024-06-30T23:59:59Z", "{json}");

    let (status, json) = get_json(&format!("/v1/rows?path={path}&filter=at:contains:59Z")).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["matched_rows"], 1, "{json}");
}

#[tokio::test]
async fn list_endpoint_lists_data_files() {
    let (status, json) = get_json(&format!("/v1/list?dir={DIR}")).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = json["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"people.csv"), "entries were {names:?}");
}

#[tokio::test]
async fn rows_endpoint_projects_columns() {
    let (status, json) = get_json(&format!("/v1/rows?path={CSV}&cols=score,id&limit=2")).await;
    assert_eq!(status, StatusCode::OK);
    let cols: Vec<&str> = json["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(cols, vec!["score", "id"]);
}

#[tokio::test]
async fn stats_endpoint_over_filtered_view() {
    let (status, json) =
        get_json(&format!("/v1/stats?path={CSV}&filter=city:contains:London")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["row_count"].as_u64().unwrap(),
        2,
        "London appears twice"
    );
}

#[tokio::test]
async fn auth_gates_v1_when_token_set() {
    let uri = format!("/v1/schema?path={CSV}");
    // No token → 401.
    let r = app_auth("s3cret")
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    // Bearer header → 200.
    let r = app_auth("s3cret")
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header("authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    // ?token= query param → 200 (used by export downloads / deep-links).
    let r = app_auth("s3cret")
        .oneshot(
            Request::builder()
                .uri(format!("{uri}&token=s3cret"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    // Wrong token → 401.
    let r = app_auth("s3cret")
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header("authorization", "Bearer nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn query_token_rejected_on_non_loopback_bind() {
    let uri = format!("/v1/schema?path={CSV}");
    // Over a non-loopback bind, the `?token=` query form must NOT authenticate…
    let r = app_auth_net("s3cret")
        .oneshot(
            Request::builder()
                .uri(format!("{uri}&token=s3cret"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::UNAUTHORIZED,
        "query token over the network"
    );
    // …but the Authorization header still does.
    let r = app_auth_net("s3cret")
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header("authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "header token over the network");
}

#[tokio::test]
async fn root_confines_v1_file_access() {
    // A confinement root with a valid CSV inside it.
    let root = tempfile::tempdir().unwrap();
    let inside = root.path().join("in.csv");
    std::fs::write(&inside, "a,b\n1,2\n").unwrap();
    // A *separate* dir with an equally valid CSV → genuinely outside the root.
    let other = tempfile::tempdir().unwrap();
    let outside = other.path().join("out.csv");
    std::fs::write(&outside, "a,b\n3,4\n").unwrap();

    // A file inside the root reads fine.
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/schema?path={}", inside.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "in-root read allowed");

    // A readable, valid data file outside the root is forbidden (403) — confinement fires after
    // the source resolves, so this is a real out-of-root block, not a format rejection.
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/schema?path={}", outside.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "out-of-root read blocked"
    );
}

/// Write a one-column, two-row Parquet file. Used by the footer-profile test, and by the Delta
/// fixture so its confinement guard (which canonicalizes) points at a file that really exists.
fn write_tiny_parquet(path: &std::path::Path) {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
    )
    .unwrap();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Write a `_delta_log/` under `dir` whose single `add` names `add_path` verbatim.
#[cfg(feature = "delta")]
fn write_delta_log(dir: &std::path::Path, add_path: &str) {
    let log = dir.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let schema_string = serde_json::json!({
        "type": "struct",
        "fields": [{"name": "id", "type": "long", "nullable": true, "metadata": {}}]
    })
    .to_string();
    let meta = serde_json::json!({"metaData": {
        "id": "t", "format": {"provider": "parquet", "options": {}},
        "schemaString": schema_string, "partitionColumns": [],
        "configuration": {}, "createdTime": 0
    }});
    let add = serde_json::json!({"add": {
        "path": add_path, "partitionValues": {},
        "size": 1, "modificationTime": 0, "dataChange": true
    }});
    std::fs::write(
        log.join("00000000000000000000.json"),
        format!("{meta}\n{add}\n"),
    )
    .unwrap();
}

#[tokio::test]
#[cfg(feature = "delta")]
async fn root_confines_delta_data_files_named_by_the_log() {
    // The entry path is inside the root, so `confine_entry` passes — the table dir really is where
    // the user says it is. The escape is in the log: a Delta `add.path` may be absolute, and the
    // reader resolves it verbatim. Without a `Format::Delta` arm in `confine_members` this served
    // a file from outside the root with a 200.
    let other = tempfile::tempdir().unwrap();
    let outside = other.path().join("secret.parquet");
    write_tiny_parquet(&outside);

    let root = tempfile::tempdir().unwrap();
    let escaping = root.path().join("escaping");
    write_delta_log(&escaping, &outside.display().to_string());

    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/schema?path={}", escaping.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "a Delta log naming a file outside --root must be blocked"
    );

    // Control: the same shape entirely inside the root still reads, so the guard is confining
    // rather than simply rejecting Delta.
    let ok = root.path().join("ok");
    write_delta_log(&ok, "part-0.parquet");
    write_tiny_parquet(&ok.join("part-0.parquet"));
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/schema?path={}", ok.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "in-root delta table still reads"
    );
}

#[tokio::test]
async fn root_confines_v1_list() {
    // Browsing a directory outside the confinement root is forbidden (a separate call site from
    // /v1/schema — guards against a route that forgets to confine).
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/list?dir={}", other.path().display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "out-of-root browse blocked"
    );

    // Browsing the root itself (the default dir when confined) is allowed.
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri("/v1/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "in-root browse allowed");
}

#[tokio::test]
async fn root_confinement_is_not_a_filesystem_oracle() {
    // With --root set, an out-of-root path must be refused BEFORE the source is resolved, so the
    // response can't distinguish exists/absent/readable/type for files outside the root.
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    // An existing, readable, non-data file (no data extension → would otherwise hit a magic sniff).
    let secret = other.path().join("secret");
    std::fs::write(&secret, b"topsecret").unwrap();

    // Existing out-of-root file → 403 (not a 400/500 from sniffing its bytes).
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/schema?path={}", secret.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "existing out-of-root file"
    );

    // Non-existent out-of-root path → the SAME 403 (not 404), so the two are indistinguishable.
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/schema?path={}/nope", other.path().display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "missing out-of-root path"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn root_confines_dataset_symlink_member() {
    // A dataset directory INSIDE the root whose member `.parquet` is a symlink pointing OUTSIDE
    // the root must be refused — confinement validates the files actually read, not just the dir.
    let root = tempfile::tempdir().unwrap();
    let ds = root.path().join("ds");
    std::fs::create_dir_all(&ds).unwrap();
    let other = tempfile::tempdir().unwrap();
    // The symlink target only needs to exist (its content is never read — we refuse first).
    let target = other.path().join("secret.parquet");
    std::fs::write(&target, b"not-really-parquet").unwrap();
    std::os::unix::fs::symlink(&target, ds.join("x.parquet")).unwrap();

    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .uri(format!("/v1/schema?path={}", ds.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "symlinked out-of-root dataset member"
    );
}

#[tokio::test]
async fn root_confines_v1_query() {
    // A query naming a table file outside the root is forbidden — confinement runs before the
    // SQL-engine requirement, so this holds even in a build without the sql feature (sql = None).
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let outside = other.path().join("out.csv");
    std::fs::write(&outside, "a,b\n1,2\n").unwrap();

    let body =
        serde_json::json!({ "sql": "SELECT * FROM t", "file": outside.display().to_string() })
            .to_string();
    let r = app_root(root.path().to_path_buf())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::FORBIDDEN,
        "out-of-root query table blocked"
    );
}

#[tokio::test]
async fn spa_and_healthz_exempt_from_auth() {
    // The page must load (and health checks pass) without a token.
    let r = app_auth("s3cret")
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = app_auth("s3cret")
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
}

#[tokio::test]
async fn export_endpoint_returns_csv_attachment() {
    let resp = app()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/export?path={CSV}&fmt=csv"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let dispo = resp
        .headers()
        .get(axum::http::header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(dispo.contains("attachment"), "disposition was {dispo}");
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes).starts_with("id,name,city,score,active"));
}

// ---- result encoding: JSON by default, Arrow IPC on request ---------------------------

const ARROW_MIME: &str = "application/vnd.apache.arrow.stream";

/// Like [`get_raw`] but with an explicit `Accept` (or none at all). Returns status,
/// content-type, the response headers, and the body bytes.
async fn get_accept(
    uri: &str,
    accept: Option<&str>,
) -> (StatusCode, String, axum::http::HeaderMap, Vec<u8>) {
    let mut builder = Request::builder().uri(uri);
    if let Some(a) = accept {
        builder = builder.header(axum::http::header::ACCEPT, a);
    }
    let resp = app()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let ct = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, ct, headers, bytes)
}

fn header(headers: &axum::http::HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

#[tokio::test]
async fn rows_without_accept_stay_json() {
    // No `Accept` at all — a plain `curl`, and the shape every existing client depends on.
    for uri in [
        format!("/v1/preview?path={CSV}&limit=2"),
        format!("/v1/rows?path={CSV}&limit=2"),
    ] {
        let (status, ct, _, body) = get_accept(&uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert!(
            ct.starts_with("application/json"),
            "{uri} content-type: {ct}"
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["num_rows"].as_u64().unwrap(), 2, "{uri}");
    }
}

#[tokio::test]
async fn rows_with_wildcard_accept_stay_json() {
    // THE SPA REGRESSION GUARD. `frontend/src/api.ts` sends no `Accept` of its own, so the
    // browser supplies `*/*` and the code unconditionally calls `r.json()`. If `*/*` ever
    // negotiates to Arrow, every grid, preview and query in the UI breaks at once.
    for accept in ["*/*", "application/json", "text/html,*/*;q=0.8"] {
        let (status, ct, _, body) =
            get_accept(&format!("/v1/preview?path={CSV}&limit=2"), Some(accept)).await;
        assert_eq!(status, StatusCode::OK, "Accept: {accept}");
        assert!(
            ct.starts_with("application/json"),
            "Accept: {accept} got content-type {ct}"
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["rows"].as_array().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn preview_with_the_arrow_token_returns_an_ipc_stream() {
    let (status, ct, headers, body) =
        get_accept(&format!("/v1/preview?path={CSV}&limit=3"), Some(ARROW_MIME)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct, ARROW_MIME, "content-type was {ct}");
    // The body is a real Arrow IPC stream, not JSON that happens to be labelled binary.
    let rb = lakeleto::render::from_arrow_ipc(&body).expect("body parses as an IPC stream");
    assert_eq!(rb.num_rows(), 3);
    assert_eq!(rb.schema.fields().len(), 5);
    assert_eq!(rb.schema.field(0).name(), "id");
    // `capped` has no JSON body to ride on here, so it travels as a header.
    assert_eq!(header(&headers, "x-lakeleto-capped"), "false");
}

#[tokio::test]
async fn an_empty_arrow_window_keeps_its_columns() {
    // The JSON arm reports `columns` explicitly; the Arrow arm has to carry them in the
    // stream's schema message, with no batch to infer them from.
    let (status, ct, _, body) =
        get_accept(&format!("/v1/preview?path={CSV}&limit=0"), Some(ARROW_MIME)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct, ARROW_MIME);
    let rb = lakeleto::render::from_arrow_ipc(&body).unwrap();
    assert_eq!(rb.num_rows(), 0);
    assert_eq!(rb.schema.fields().len(), 5);
}

#[tokio::test]
async fn rows_arrow_arm_carries_the_window_counts_in_headers() {
    // Everything `RowsWindow` states inline in JSON — the numbers the virtual scrollbar is
    // sized from — must still reach a client that asked for Arrow.
    let uri = format!("/v1/rows?path={CSV}&offset=1&limit=2");
    let (_, _, json_headers, json_body) = get_accept(&uri, None).await;
    assert!(
        header(&json_headers, "x-lakeleto-offset").is_empty(),
        "JSON arm adds no sidecars"
    );
    let json: serde_json::Value = serde_json::from_slice(&json_body).unwrap();

    let (status, ct, headers, body) = get_accept(&uri, Some(ARROW_MIME)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct, ARROW_MIME);
    let rb = lakeleto::render::from_arrow_ipc(&body).unwrap();
    assert_eq!(rb.num_rows(), 2);

    // The headers say exactly what the JSON body says, field for field.
    assert_eq!(
        header(&headers, "x-lakeleto-offset"),
        json["offset"].to_string()
    );
    assert_eq!(
        header(&headers, "x-lakeleto-num-rows"),
        json["num_rows"].to_string()
    );
    assert_eq!(
        header(&headers, "x-lakeleto-matched-rows"),
        json["matched_rows"].to_string()
    );
    assert_eq!(
        header(&headers, "x-lakeleto-total-known"),
        json["total_known"].to_string()
    );
    assert_eq!(
        header(&headers, "x-lakeleto-scanned-rows"),
        json["scanned_rows"].to_string()
    );
    assert_eq!(
        header(&headers, "x-lakeleto-bounded"),
        json["bounded"].to_string()
    );
    // Booleans are spelled "true"/"false", not 1/0.
    assert!(matches!(
        header(&headers, "x-lakeleto-bounded").as_str(),
        "true" | "false"
    ));
}

#[tokio::test]
async fn an_unknown_accept_is_answered_with_json_never_406() {
    // Negotiation cannot fail: the IPC codec is always compiled, and JSON always answers.
    let (status, ct, _, _) = get_accept(
        &format!("/v1/preview?path={CSV}&limit=1"),
        Some("application/x-nonsense"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(ct.starts_with("application/json"), "content-type was {ct}");
}

// ---- workspaces -----------------------------------------------------------------------

async fn send(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let builder = Request::builder().method(method).uri(uri);
    let req = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn workspace_crud_run_history_and_cached_result() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(dir.path()).unwrap());

    // create
    let (st, ws) = send(
        app_store(store.clone()),
        "POST",
        "/v1/workspaces",
        Some(serde_json::json!({ "name": "demo" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let id = ws["id"].as_str().unwrap().to_string();
    assert_eq!(ws["name"], "demo");

    // save a connection onto it
    let mut doc = ws.clone();
    doc["connections"] = serde_json::json!([{ "id": "c1", "label": "people", "path": CSV }]);
    let (st, saved) = send(
        app_store(store.clone()),
        "PUT",
        &format!("/v1/workspaces/{id}"),
        Some(doc),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(saved["connections"].as_array().unwrap().len(), 1);

    // run a scan of the CSV → recorded, and cached because this run asks to be
    let (st, run) = send(
        app_store(store.clone()),
        "POST",
        &format!("/v1/workspaces/{id}/runs"),
        Some(serde_json::json!({ "path": CSV, "limit": 100, "preview": 3, "cache": true })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "run resp: {run}");
    assert_eq!(run["run"]["status"], "ok");
    assert_eq!(run["num_rows"].as_u64().unwrap(), 3, "preview window");
    assert_eq!(run["run"]["cached"], true, "opted in, so it is cached");
    let run_id = run["run"]["id"].as_str().unwrap().to_string();

    // history carries the run
    let (st, hist) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}/history"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hist["history"].as_array().unwrap().len(), 1);

    // the cached result re-opens without re-running (full scan, 8 rows in people.csv)
    let (st, res) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}/runs/{run_id}?limit=100"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(res["num_rows"].as_u64().unwrap(), 8);

    // delete → gone
    let (st, _) = send(
        app_store(store.clone()),
        "DELETE",
        &format!("/v1/workspaces/{id}"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

/// A saved query and a run keep the flattening their SQL was written against, so reopening either
/// runs it again over the columns it named — `"user.name"` does not exist unflattened.
#[tokio::test]
async fn saved_queries_and_runs_keep_their_flattening() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(dir.path()).unwrap());
    let (_, ws) = send(
        app_store(store.clone()),
        "POST",
        "/v1/workspaces",
        Some(serde_json::json!({ "name": "flat" })),
    )
    .await;
    let id = ws["id"].as_str().unwrap().to_string();

    let mut doc = ws.clone();
    doc["saved_queries"] = serde_json::json!([{
        "id": "q1", "name": "names", "sql": "SELECT \"user.name\" FROM t", "flatten": "all"
    }]);
    let (st, _) = send(
        app_store(store.clone()),
        "PUT",
        &format!("/v1/workspaces/{id}"),
        Some(doc),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (_, reread) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}"),
        None,
    )
    .await;
    assert_eq!(reread["saved_queries"][0]["flatten"], "all", "{reread}");

    let data = write_nested_people(dir.path());
    let runs = format!("/v1/workspaces/{id}/runs");
    let (st, run) = send(
        app_store(store.clone()),
        "POST",
        &runs,
        Some(serde_json::json!({ "path": data, "flatten": "all", "preview": 5 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{run}");
    assert_eq!(run["run"]["flatten"], "all");
    assert_eq!(column_names(&run), ["id", "user.name", "user.geo.lat"]);
    let (_, plain) = send(
        app_store(store.clone()),
        "POST",
        &runs,
        Some(serde_json::json!({ "path": data, "preview": 1 })),
    )
    .await;
    assert!(plain["run"].get("flatten").is_none(), "{plain}");

    let (_, hist) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}/history"),
        None,
    )
    .await;
    let recorded: Vec<_> = hist["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["flatten"].clone())
        .collect();
    assert!(recorded.contains(&serde_json::json!("all")), "{hist}");
}

/// A run read at an explicit records path says so in its record, as it does for flattening:
/// the path decides which rows and columns its SQL sees.
#[tokio::test]
async fn runs_record_the_records_path_they_read_at() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(dir.path()).unwrap());
    let (_, ws) = send(
        app_store(store.clone()),
        "POST",
        "/v1/workspaces",
        Some(serde_json::json!({ "name": "paths" })),
    )
    .await;
    let runs = format!("/v1/workspaces/{}/runs", ws["id"].as_str().unwrap());
    let data = dir.path().join("two.json");
    std::fs::write(
        &data,
        r#"{"users": [{"a": 1}], "groups": [{"b": 2}, {"b": 3}]}"#,
    )
    .unwrap();
    let data = data.to_str().unwrap();

    let (st, run) = send(
        app_store(store.clone()),
        "POST",
        &runs,
        Some(serde_json::json!({ "path": data, "json_path": "groups", "preview": 5 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{run}");
    assert_eq!(
        run["run"]["json_path"], "/groups",
        "recorded as the pointer it read"
    );
    assert_eq!(column_names(&run), ["b"]);
    // `""` is a path too — the whole document, unwrapped by nothing — so it is recorded, empty,
    // not dropped as if the run had none: the app refuses to reopen either kind as detection.
    let (st, whole) = send(
        app_store(store.clone()),
        "POST",
        &runs,
        Some(serde_json::json!({ "path": data, "json_path": "", "preview": 1 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{whole}");
    assert_eq!(whole["run"]["json_path"], "", "{whole}");
    assert_eq!(column_names(&whole), ["users", "groups"]);
    let (_, plain) = send(
        app_store(store.clone()),
        "POST",
        &runs,
        Some(serde_json::json!({ "path": data, "preview": 1 })),
    )
    .await;
    assert!(plain["run"].get("json_path").is_none(), "{plain}");

    // A saved query keeps the path its SQL was written against — the empty one too, which is a
    // path (the whole document), not the absence of one.
    let id = ws["id"].as_str().unwrap();
    let mut doc = ws.clone();
    doc["saved_queries"] = serde_json::json!([
        { "id": "q1", "name": "groups", "sql": "SELECT b FROM t", "json_path": "/groups" },
        { "id": "q2", "name": "whole", "sql": "SELECT users FROM t", "json_path": "" },
        { "id": "q3", "name": "detected", "sql": "SELECT * FROM t" }
    ]);
    let (st, _) = send(
        app_store(store.clone()),
        "PUT",
        &format!("/v1/workspaces/{id}"),
        Some(doc),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (_, reread) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}"),
        None,
    )
    .await;
    let saved = &reread["saved_queries"];
    assert_eq!(saved[0]["json_path"], "/groups", "{reread}");
    assert_eq!(saved[1]["json_path"], "", "{reread}");
    assert!(saved[2].get("json_path").is_none(), "{reread}");
}

#[tokio::test]
async fn workspace_export_import_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(dir.path()).unwrap());
    let (_s, ws) = send(
        app_store(store.clone()),
        "POST",
        "/v1/workspaces",
        Some(serde_json::json!({ "name": "exp" })),
    )
    .await;
    let id = ws["id"].as_str().unwrap().to_string();
    send(
        app_store(store.clone()),
        "POST",
        &format!("/v1/workspaces/{id}/runs"),
        Some(serde_json::json!({ "path": CSV, "limit": 10 })),
    )
    .await;

    let (st, bundle) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}/export"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(bundle["bundle_version"], 1);
    assert_eq!(bundle["history"].as_array().unwrap().len(), 1);

    let (st, imported) = send(
        app_store(store.clone()),
        "POST",
        "/v1/workspaces/import",
        Some(bundle.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_ne!(
        imported["id"],
        serde_json::json!(id),
        "import mints a fresh id"
    );
    assert_eq!(imported["name"], "exp");
}

#[tokio::test]
async fn workspace_run_against_missing_workspace_is_not_found() {
    // A run against a workspace id that doesn't exist must 404 BEFORE any engine work — the
    // query is never executed (its record could not be stored anywhere).
    let (st, _) = send(
        app(),
        "POST",
        "/v1/workspaces/ws-does-not-exist/runs",
        Some(serde_json::json!({ "path": CSV, "limit": 10 })),
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn workspace_run_is_confined_by_root() {
    let ws_dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(ws_dir.path()).unwrap());
    // A root that does NOT contain the CSV.
    let root = tempfile::tempdir().unwrap();
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let confined = router(
        EngineRegistry::new(read),
        10_000,
        None,
        Some(std::fs::canonicalize(root.path()).unwrap()),
        true,
        store,
    );
    // A run pointing at the out-of-root CSV is refused (confine_entry, before any store access).
    let (st, _) = send(
        confined,
        "POST",
        "/v1/workspaces/ws-x/runs",
        Some(serde_json::json!({ "path": CSV, "limit": 10 })),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn workspace_history_sync_and_raw_result_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(dir.path()).unwrap());

    let (_, ws) = send(
        app_store(store.clone()),
        "POST",
        "/v1/workspaces",
        Some(serde_json::json!({ "name": "sync" })),
    )
    .await;
    let id = ws["id"].as_str().unwrap().to_string();

    // Sync-append a run record produced elsewhere (no engine work on this server).
    let rec = serde_json::json!({
        "id": "run-ext-1", "at_ms": 7, "sql": "SELECT * FROM t",
        "source_path": "/elsewhere/t.csv", "status": "ok", "row_count": 8,
        "duration_ms": 3, "cached": true
    });
    let (st, _) = send(
        app_store(store.clone()),
        "POST",
        &format!("/v1/workspaces/{id}/history"),
        Some(rec),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Upload the run's raw Parquet result (a real file, made through the OSS pipeline)…
    let source = lakeleto::Source::resolve(CSV, None).unwrap();
    let rb = LocalReaderEngine::default()
        .preview(&RequestContext::detached(), &source, 10)
        .unwrap();
    let parquet = lakeleto::render::to_parquet(&rb).unwrap();
    let put = Request::builder()
        .method("PUT")
        .uri(format!("/v1/workspaces/{id}/runs/run-ext-1/result"))
        .header("content-type", "application/vnd.apache.parquet")
        .body(Body::from(parquet.clone()))
        .unwrap();
    let resp = app_store(store.clone()).oneshot(put).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // …garbage bytes are parse-rejected, never cached…
    let bad = Request::builder()
        .method("PUT")
        .uri(format!("/v1/workspaces/{id}/runs/run-ext-1/result"))
        .body(Body::from("not parquet"))
        .unwrap();
    let resp = app_store(store.clone()).oneshot(bad).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // …the raw download round-trips byte-for-byte…
    let get = Request::builder()
        .method("GET")
        .uri(format!("/v1/workspaces/{id}/runs/run-ext-1/result"))
        .body(Body::empty())
        .unwrap();
    let resp = app_store(store.clone()).oneshot(get).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let dl = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(dl.as_ref(), parquet.as_slice());

    // …and the windowed JSON view reads the synced cache like a locally-run result.
    let (st, rows) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}/runs/run-ext-1?limit=3"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rows["num_rows"], 3);
    assert_eq!(rows["rows"][0]["name"], "Ada");
}

#[tokio::test]
async fn a_run_does_not_cache_its_result_unless_asked() {
    // A query result is dataset content, and the store is a trait — with a RemoteStore configured
    // this handler would upload those rows off the machine. So caching is stated per run, and the
    // history record reports what was actually written rather than what the caller hoped for.
    // Keep the `TempDir` alive for the test body: `tempdir().path()` alone would delete the
    // directory the moment the temporary drops, leaving `LocalStore` pointed at nothing.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalStore::at(dir.path()).unwrap()) as Arc<dyn WorkspaceStore>;
    let (_, ws) = send(
        app_store(store.clone()),
        "POST",
        "/v1/workspaces",
        Some(serde_json::json!({ "name": "no-cache" })),
    )
    .await;
    let id = ws["id"].as_str().unwrap().to_string();

    let (st, run) = send(
        app_store(store.clone()),
        "POST",
        &format!("/v1/workspaces/{id}/runs"),
        Some(serde_json::json!({ "path": CSV, "limit": 100, "preview": 3 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "the run itself still succeeds: {run}");
    assert_eq!(run["num_rows"].as_u64().unwrap(), 3, "rows still come back");
    assert_eq!(
        run["run"]["cached"], false,
        "the record must not claim a result the store was never given"
    );
    let run_id = run["run"]["id"].as_str().unwrap().to_string();

    // The run is in history — not caching a result is not the same as not recording the run.
    let (st, hist) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}/history"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hist["history"].as_array().unwrap().len(), 1);

    // But there is nothing to re-open, because nothing was written.
    let (st, _) = send(
        app_store(store.clone()),
        "GET",
        &format!("/v1/workspaces/{id}/runs/{run_id}?limit=100"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "no result bytes were stored");
}

#[tokio::test]
async fn rows_endpoint_supports_the_full_filter_vocabulary() {
    // people.csv: 8 rows; `score` is null for Linus and Barbara; two Londoners; two New Yorkers.
    for (filter, want, why) in [
        (
            "city:notcontains:London",
            6,
            "the six who are not in London",
        ),
        ("name:startswith:A", 2, "Ada, Alan"),
        ("name:endswith:a", 2, "Ada, Barbara"),
        ("name:endswith:e", 2, "Grace, Katherine"),
        ("city:in:London,Helsinki", 3, "two Londoners plus Linus"),
        ("city:in:London,%20Helsinki", 3, "list members are trimmed"),
        ("score:isnull:", 2, "Linus and Barbara have no score"),
        ("score:notnull:", 6, "the complement of the nulls"),
        ("score:isnull", 2, "a unary filter needs no trailing colon"),
        (
            "city:in:",
            0,
            "an empty list matches nothing rather than everything",
        ),
    ] {
        let (status, json) = get_json(&format!("/v1/rows?path={CSV}&filter={filter}")).await;
        assert_eq!(status, StatusCode::OK, "{filter}");
        assert_eq!(
            json["matched_rows"].as_u64().unwrap(),
            want,
            "{filter} should match {why}"
        );
    }
}

#[tokio::test]
async fn a_negative_text_filter_does_not_match_null_cells() {
    // The subtle one. `city` is never null here, but `score` is — and "does not contain 9" must
    // not quietly report the rows whose score is unknown as satisfying it. Both `contains` and
    // `notcontains` are false on a null, so the two partition the NON-null rows, not all rows.
    let (_, has) = get_json(&format!("/v1/rows?path={CSV}&filter=score:contains:9")).await;
    let (_, has_not) = get_json(&format!("/v1/rows?path={CSV}&filter=score:notcontains:9")).await;
    let (_, not_null) = get_json(&format!("/v1/rows?path={CSV}&filter=score:notnull:")).await;
    let n = |v: &serde_json::Value| v["matched_rows"].as_u64().unwrap();
    assert_eq!(
        n(&has) + n(&has_not),
        n(&not_null),
        "contains + notcontains must cover exactly the non-null rows"
    );
}

#[tokio::test]
async fn export_renders_every_advertised_format() {
    for (fmt, ext, mime, probe) in [
        ("csv", "csv", "text/csv", "id,name,city"),
        ("tsv", "tsv", "text/tab-separated-values", "id\tname\tcity"),
        ("ndjson", "ndjson", "application/x-ndjson", "{\"id\":1"),
        ("json", "json", "application/json", "[{"),
    ] {
        let r = app()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/export?path={CSV}&fmt={fmt}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "fmt={fmt}");
        let headers = r.headers().clone();
        assert_eq!(headers["content-type"], mime, "fmt={fmt}");
        assert!(
            headers["content-disposition"]
                .to_str()
                .unwrap()
                .ends_with(&format!(".{ext}\"")),
            "fmt={fmt}: filename should carry the right extension"
        );
        let body =
            String::from_utf8(to_bytes(r.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap();
        assert!(
            body.contains(probe),
            "fmt={fmt} body was: {}",
            &body[..80.min(body.len())]
        );
    }

    // NDJSON is one object per line, which is the entire reason to offer it over `json`.
    let r = app()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/export?path={CSV}&fmt=ndjson"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body =
        String::from_utf8(to_bytes(r.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap();
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 8, "one line per row");
    for l in lines {
        serde_json::from_str::<serde_json::Value>(l).expect("each line parses on its own");
    }
}

/// A JSON document read whole is one row whose columns are lists of records. Arrow's CSV writer
/// refuses a nested column, so that view could be read but not exported as CSV or TSV; each nested
/// cell is written as the JSON the grid shows for it.
#[tokio::test]
async fn export_writes_a_nested_column_as_json_text() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two.json");
    std::fs::write(
        &path,
        r#"{"users":[{"a":1}],"groups":[{"b":2},{"b":3}],"note":"x"}"#,
    )
    .unwrap();
    let p = path.display();
    for (fmt, want) in [
        (
            "csv",
            "users,groups,note\n\"[{\"\"a\"\":1}]\",\"[{\"\"b\"\":2},{\"\"b\"\":3}]\",x\n",
        ),
        (
            "tsv",
            "users\tgroups\tnote\n\"[{\"\"a\"\":1}]\"\t\"[{\"\"b\"\":2},{\"\"b\"\":3}]\"\tx\n",
        ),
    ] {
        let r = app()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/export?path={p}&json_path=&fmt={fmt}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = r.status();
        let body =
            String::from_utf8(to_bytes(r.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap();
        assert_eq!(status, StatusCode::OK, "fmt={fmt}: {body}");
        assert_eq!(body, want, "fmt={fmt}");
    }
}

/// A router with the SQL engine wired, which is what a real `serve --features sql` is. It matters
/// for filters specifically: the engine registry routes any non-plain window over a format
/// DataFusion can read — i.e. every filtered scan of a CSV/Parquet source — to DataFusion, so the
/// SQL `WHERE` builder, not the Arrow kernel path, answers them.
#[cfg(feature = "sql")]
fn app_sql() -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let sql: Arc<dyn Engine> = Arc::new(lakeleto::engine::sql::DataFusionEngine::new());
    router(
        EngineRegistry::new(read).with_sql(sql),
        10_000,
        None,
        None,
        true,
        generic_store(),
    )
}

#[tokio::test]
#[cfg(feature = "sql")]
async fn the_two_filter_implementations_agree() {
    // There are two implementations of every operator — Arrow compute kernels in `column_mask`
    // and generated SQL in `where_clause` — and which one runs depends on how the binary was
    // built. If they disagree, the same filter shows different rows to different users, which is
    // worse than either being wrong on its own.
    for filter in [
        "city:contains:London",
        "city:notcontains:London",
        "name:startswith:A",
        "name:endswith:a",
        "city:in:London,Helsinki",
        "city:in:",
        "score:isnull:",
        "score:notnull:",
        "score:gt:88",
        "name:eq:Ada",
        "name:ne:Ada",
        // LIKE metacharacters, matched literally. No fixture value contains `%` or `_`, so each of
        // these matches ZERO rows on the Arrow path — and an unescaped SQL translation would turn
        // them into wildcards matching everything, which is exactly the divergence this test
        // exists to catch. (`%25` is a literal `%` after URI decoding.)
        "name:contains:_",
        "city:contains:%25",
        "name:startswith:_",
        "name:endswith:%25",
        "city:notcontains:%25", // …and its negation must then match all 8, not zero
    ] {
        let uri = format!("/v1/rows?path={CSV}&filter={filter}&limit=100");
        let (s_local, local) = get_json(&uri).await;
        let r = app_sql()
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let s_sql = r.status();
        let sql: serde_json::Value =
            serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap();
        assert_eq!(s_local, StatusCode::OK, "{filter} (local): {local}");
        assert_eq!(s_sql, StatusCode::OK, "{filter} (sql): {sql}");
        // The full row-id sets, not just the counts — two implementations can disagree while
        // matching the same NUMBER of rows, and equal counts would mask exactly that.
        let ids = |v: &serde_json::Value| -> Vec<i64> {
            let mut ids: Vec<i64> = v["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["id"].as_i64().unwrap())
                .collect();
            ids.sort_unstable();
            ids
        };
        assert_eq!(
            ids(&local),
            ids(&sql),
            "{filter}: the two engines selected different rows"
        );
        assert_eq!(
            local["matched_rows"], sql["matched_rows"],
            "{filter}: local matched {} but sql matched {}",
            local["matched_rows"], sql["matched_rows"]
        );
    }
}

/// `GET` `uri` against `app` — [`get_json`] for a router other than the default one.
#[cfg(feature = "sql")]
async fn get_json_from(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// A JSON grid in a `sql` build — what every release binary and the Docker image are.
///
/// The API used to send every sorted or filtered window to DataFusion, which could not
/// register JSON then, so sorting or filtering *any* column of a `.json`/`.ndjson` file failed
/// with "the `sql` engine cannot read json sources yet" — in the builds users download, while the
/// lean build (no SQL engine, so the local reader answered) worked. The default `app()` has no SQL
/// engine, which is why no test saw it. DataFusion now reads JSON through the local reader, so the
/// sorted and filtered windows here go through SQL — and both builds must give the same answer.
#[tokio::test]
#[cfg(feature = "sql")]
async fn json_grid_sorts_filters_and_exports_with_sql_compiled_in() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<serde_json::Value> = serde_json::from_str(
        r#"[{"id":1,"name":"Ada","city":"London"},
            {"id":2,"name":"Grace","city":"New York"},
            {"id":3,"name":"Alan","city":"London"},
            {"id":4,"name":"Edsger","city":"Amsterdam"}]"#,
    )
    .unwrap();
    let array = dir.path().join("people.json");
    std::fs::write(&array, serde_json::to_string(&rows).unwrap()).unwrap();
    let ndjson = dir.path().join("people.ndjson");
    let lines: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
    std::fs::write(&ndjson, lines.join("\n")).unwrap();

    for path in [&array, &ndjson] {
        let p = path.display();
        let sort = format!("/v1/rows?path={p}&sort=id&desc=1&limit=2");
        let filter = format!("/v1/rows?path={p}&filter=city:eq:London");

        let (status, sorted) = get_json_from(app_sql(), &sort).await;
        assert_eq!(status, StatusCode::OK, "sort {p}: {sorted}");
        assert_eq!(sorted["rows"][0]["id"], 4, "sort {p}: {sorted}");

        let (status, filtered) = get_json_from(app_sql(), &filter).await;
        assert_eq!(status, StatusCode::OK, "filter {p}: {filtered}");
        assert_eq!(filtered["matched_rows"], 2, "filter {p}: {filtered}");

        // A tie (two London rows) comes in file order in both builds.
        let tied = format!("/v1/rows?path={p}&sort=city");
        let (status, tied_rows) = get_json_from(app_sql(), &tied).await;
        assert_eq!(status, StatusCode::OK, "tied {p}: {tied_rows}");
        let ids: Vec<_> = tied_rows["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, [4, 1, 3, 2], "tied {p}: London's rows in file order");

        // Same rows as the build without a SQL engine.
        for (uri, sql) in [(&sort, &sorted), (&filter, &filtered), (&tied, &tied_rows)] {
            let (_, local) = get_json(uri).await;
            assert_eq!(local["rows"], sql["rows"], "{uri}: the two builds disagree");
        }

        // `/v1/export` of a filtered view routes the same way.
        let resp = app_sql()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/export?path={p}&filter=city:eq:London&fmt=csv"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "export {p}");
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert_eq!(
            body.lines().filter(|l| !l.trim().is_empty()).count(),
            3,
            "export {p}: header + 2 rows, got {body}"
        );
    }
}

/// An unwrapped JSON document says where its rows came from, and nothing else grows the field.
#[tokio::test]
async fn schema_reports_the_records_path_it_unwrapped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api.json");
    std::fs::write(&path, r#"{"meta":{"n":2},"data":[{"id":1},{"id":2}]}"#).unwrap();
    let (status, json) = get_json(&format!("/v1/schema?path={}", path.display())).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["records_path"], "/data");

    let (_, csv) = get_json(&format!("/v1/schema?path={CSV}")).await;
    assert!(csv.get("records_path").is_none(), "{csv}");
}

/// `?json_path=` picks the records detection would not, on every read route that takes a path.
#[tokio::test]
async fn json_path_selects_records_on_schema_and_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two.json");
    std::fs::write(&path, r#"{"users":[{"a":1}],"groups":[{"b":2},{"b":3}]}"#).unwrap();
    let p = path.display();

    let (status, schema) = get_json(&format!("/v1/schema?path={p}&json_path=groups")).await;
    assert_eq!(status, StatusCode::OK, "{schema}");
    assert_eq!(schema["records_path"], "/groups");

    let (status, rows) =
        get_json(&format!("/v1/rows?path={p}&json_path=groups&sort=b&desc=1")).await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows["rows"][0]["b"], 3);

    // An empty path is a path — the whole document, unwrapped by nothing — on every route that
    // reads, not a missing one: the app sends it as `json_path=`.
    let (status, schema) = get_json(&format!("/v1/schema?path={p}&json_path=")).await;
    assert_eq!(status, StatusCode::OK, "{schema}");
    assert!(schema.get("records_path").is_none(), "{schema}");
    assert_eq!(schema["columns"][0]["name"], "users", "{schema}");
    for route in ["rows", "stats"] {
        let (status, body) = get_json(&format!("/v1/{route}?path={p}&json_path=")).await;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        assert_eq!(body["columns"][1]["name"], "groups", "{route}: {body}");
    }

    // A path that names nothing is the caller's mistake, said as a 400.
    let (status, err) = get_json(&format!("/v1/schema?path={p}&json_path=/nope")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
}

/// NDJSON with a nested `user`; row 2 has none.
fn write_nested_people(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("people.ndjson");
    std::fs::write(
        &path,
        concat!(
            r#"{"id":1,"user":{"name":"Grace","geo":{"lat":40.7}}}"#,
            "\n",
            r#"{"id":2,"user":null}"#,
            "\n",
            r#"{"id":3,"user":{"name":"Ada","geo":{"lat":51.5}}}"#,
            "\n",
        ),
    )
    .unwrap();
    path
}

fn column_names(schema: &serde_json::Value) -> Vec<String> {
    schema["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn flatten_spreads_struct_columns_on_every_read_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_nested_people(dir.path());
    let p = path.display();

    // A bare `?flatten` is every level.
    let (status, schema) = get_json(&format!("/v1/schema?path={p}&flatten")).await;
    assert_eq!(status, StatusCode::OK, "{schema}");
    assert_eq!(column_names(&schema), ["id", "user.name", "user.geo.lat"]);

    let (status, rows) = get_json(&format!(
        "/v1/rows?path={p}&flatten=all&sort=user.geo.lat&desc=1"
    ))
    .await;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows["rows"][0]["user.name"], "Ada");

    let (status, preview) = get_json(&format!("/v1/preview?path={p}&flatten=1&limit=5")).await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(column_names(&preview), ["id", "user.name", "user.geo"]);

    let (status, stats) = get_json(&format!(
        "/v1/stats?path={p}&flatten&filter=user.name:eq:Grace"
    ))
    .await;
    assert_eq!(status, StatusCode::OK, "{stats}");
    assert_eq!(stats["row_count"], 1);

    let (status, profile) = get_json(&format!("/v1/profile?path={p}&flatten")).await;
    assert_eq!(status, StatusCode::OK, "{profile}");
    assert_eq!(profile["columns"].as_array().unwrap().len(), 3);

    // `none` turns it off, and a value that is neither is the caller's mistake.
    let (_, schema) = get_json(&format!("/v1/schema?path={p}&flatten=none")).await;
    assert_eq!(column_names(&schema), ["id", "user"]);
    let (status, err) = get_json(&format!("/v1/schema?path={p}&flatten=deep")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
}

/// `id` and a `user: {name, geo: {lat}}` struct, as Parquet — a format the SQL engine reads, so a
/// flattened grid window goes to DataFusion in a `sql` build.
#[cfg(feature = "sql")]
fn write_nested_parquet(path: &std::path::Path) {
    use arrow_array::{
        Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    let geo = StructArray::from(vec![(
        Arc::new(Field::new("lat", DataType::Float64, true)),
        Arc::new(Float64Array::from(vec![40.7, 51.5, 60.2])) as ArrayRef,
    )]);
    let user = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["Grace", "Ada", "Linus"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("geo", geo.data_type().clone(), true)),
            Arc::new(geo) as ArrayRef,
        ),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("user", user.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(user)],
    )
    .unwrap();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A flattened grid over Parquet in a `sql` build sorts and filters through DataFusion — over the
/// whole file — and must show the rows the local engine would; a query names the fields too.
#[tokio::test]
#[cfg(feature = "sql")]
async fn a_flattened_grid_and_query_agree_with_sql_compiled_in() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("users.parquet");
    write_nested_parquet(&path);
    let p = path.display();

    let uri = format!("/v1/rows?path={p}&flatten&sort=user.name&desc=1&filter=user.geo.lat:gt:45");
    let (status, by_sql) = get_json_from(app_sql(), &uri).await;
    assert_eq!(status, StatusCode::OK, "{by_sql}");
    let (_, by_local) = get_json(&uri).await;
    assert_eq!(by_sql["rows"], by_local["rows"]);
    let ids: Vec<_> = by_sql["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, [3, 2], "Linus, then Ada");

    let body = serde_json::json!({
        "sql": r#"SELECT "user.name" AS name FROM u WHERE "user.geo.lat" < 55 ORDER BY id"#,
        "tables": [{"name": "u", "path": path, "flatten": "all"}],
    })
    .to_string();
    let resp = app_sql()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    let names: Vec<_> = json["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, ["Grace", "Ada"]);
}

/// SQL over a JSON document DataFusion's own reader cannot open — pretty-printed, wrapped in an
/// envelope with two candidate arrays — registered through the local reader, records path and all.
#[tokio::test]
#[cfg(feature = "sql")]
async fn sql_queries_json_the_way_the_grid_reads_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("api.json");
    std::fs::write(
        &path,
        r#"{
  "meta": {"page": 1},
  "users": [
    {"id": 1, "city": "London"},
    {"id": 2, "city": "Oslo"},
    {"id": 3, "city": "London"}
  ],
  "groups": [{"g": "a"}]
}"#,
    )
    .unwrap();
    let body = serde_json::json!({
        "sql": "SELECT city, count(*) AS n FROM t GROUP BY city ORDER BY n DESC, city",
        "tables": [{"name": "t", "path": path, "json_path": "users"}],
    })
    .to_string();
    let resp = app_sql()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        json["rows"],
        serde_json::json!([{"city": "London", "n": 2}, {"city": "Oslo", "n": 1}])
    );
}

#[tokio::test]
async fn engines_reports_the_limits_it_will_enforce() {
    // A client that knows the caps can show them before a user hits one, instead of explaining a
    // rejection afterwards — and the DB-connection cap in particular used to be a literal in the
    // frontend bundle, which made an edition's terms a frontend rebuild.
    let (status, json) = get_json("/v1/engines").await;
    assert_eq!(status, StatusCode::OK);
    let limits = &json["limits"];
    assert_eq!(limits["max_query_rows"].as_u64().unwrap(), 100_000);
    assert_eq!(limits["default_query_rows"].as_u64().unwrap(), 10_000);
    assert_eq!(limits["max_run_rows"].as_u64().unwrap(), 100_000);
    assert_eq!(limits["max_export_rows"].as_u64().unwrap(), 1_000_000);
    assert!(limits["max_export_bytes"].as_u64().unwrap() > 0);
    // `null` means unlimited, which is what an `ee` build reports; the OSS build names a number.
    if cfg!(feature = "ee") {
        assert!(limits["max_db_connections"].is_null());
    } else {
        assert_eq!(limits["max_db_connections"].as_u64().unwrap(), 2);
    }
}

/// `GET /v1/engines` from `app`, as JSON.
async fn engines_of(app: axum::Router) -> serde_json::Value {
    let r = app
        .oneshot(
            Request::builder()
                .uri("/v1/engines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap()
}

/// The `engine` name of every entry in a `/v1/engines` answer's `engines` list.
#[cfg(any(feature = "sql", feature = "sqlite"))]
fn engine_names(engines: &serde_json::Value) -> Vec<String> {
    engines["engines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["engine"].as_str().unwrap().to_string())
        .collect()
}

/// The endpoints a `/v1/engines` answer advertises.
fn advertised(engines: &serde_json::Value) -> Vec<String> {
    engines["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e.as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn engines_names_the_protocol_and_every_engine() {
    let engines = engines_of(app()).await;
    assert_eq!(engines["protocol"], lakeleto::protocol::PROTOCOL_VERSION);
    assert_eq!(engines["protocol"], "1.0");
    // A server that only reads has one engine, and it is the one `engine` names.
    assert_eq!(engines["engines"].as_array().unwrap().len(), 1);
    assert_eq!(engines["engines"][0], engines["engine"]);
    // No engine here runs SQL, so the query endpoint, which would answer every request with a
    // 501, isn't offered. Everything else is.
    let endpoints = advertised(&engines);
    assert!(
        !endpoints.iter().any(|e| e == "POST /v1/query"),
        "{endpoints:?}"
    );
    assert!(endpoints.iter().any(|e| e == "GET /v1/engines"));
    assert!(endpoints.iter().any(|e| e.starts_with("GET /v1/rows")));
}

#[tokio::test]
#[cfg(feature = "sql")]
async fn engines_lists_the_sql_engine_after_the_read_engine() {
    let engines = engines_of(app_sql()).await;
    assert_eq!(
        engine_names(&engines),
        ["local (built-in reader)", "sql (DataFusion)"]
    );
    assert_eq!(engines["engines"][1]["sql"], true);
    assert_eq!(engines["sql_available"], true);
    assert!(advertised(&engines).iter().any(|e| e == "POST /v1/query"));
}

/// A server with a database engine and no SQL engine runs SQL on databases only. It offers the
/// query endpoint, and says it has no SQL over files.
#[tokio::test]
#[cfg(feature = "sqlite")]
async fn a_server_with_only_a_database_engine_offers_queries_but_no_sql_over_files() {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let dbe: Arc<dyn Engine> = Arc::new(lakeleto::engine::database::DatabaseEngine::new());
    let engines = engines_of(router(
        EngineRegistry::new(read).with_database(dbe),
        10_000,
        None,
        None,
        true,
        generic_store(),
    ))
    .await;
    let names = engine_names(&engines);
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(names[1].starts_with("database ("), "{names:?}");
    assert_eq!(engines["sql_available"], false);
    assert!(advertised(&engines).iter().any(|e| e == "POST /v1/query"));
}

/// Every response says which protocol the server speaks, so a client learns it from whatever it
/// asked first: an answer, the health check, the page, an error, a route that doesn't exist, and
/// a request the token gate refused.
#[tokio::test]
async fn every_response_names_the_protocol() {
    for (app, uri, status) in [
        (app(), "/v1/engines".to_string(), StatusCode::OK),
        (app(), "/healthz".to_string(), StatusCode::OK),
        (app(), "/".to_string(), StatusCode::OK),
        (
            app(),
            "/v1/schema?path=/no/such/file.parquet".to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            app(),
            "/v1/no-such-route".to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            app_auth("s3cret"),
            format!("/v1/schema?path={CSV}"),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let r = app
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), status, "{uri}");
        assert_eq!(
            r.headers()
                .get(lakeleto::protocol::PROTOCOL_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some(lakeleto::protocol::PROTOCOL_VERSION),
            "{uri}"
        );
    }
}

#[tokio::test]
async fn preview_and_profile_clamp_a_caller_limit_to_the_query_cap() {
    // `POST /v1/query` was bounded, but `GET /v1/preview?limit=` and `GET /v1/profile?scan=` took
    // the caller's number verbatim — so `?limit=999999999` materialised the whole table. Both now
    // clamp to the same ceiling. A fixture BIGGER than the cap makes the clamp observable; with a
    // small fixture an oversized ask returns every row and the test would pass even unclamped.
    let cap = 100_000u64; // QUERY_CAP, also advertised as /v1/engines limits.max_query_rows
                          // Twice the cap, so a clamped scan (~cap) is unmistakably smaller than the whole file. A
                          // fixture only one row over the cap can't tell the two apart — batch reads round to a boundary.
    let rows = 2 * cap;
    let dir = tempfile::tempdir().unwrap();
    let big = dir.path().join("big.csv");
    {
        use std::io::Write;
        let mut f = std::io::BufWriter::new(std::fs::File::create(&big).unwrap());
        writeln!(f, "id").unwrap();
        for i in 0..rows {
            writeln!(f, "{i}").unwrap();
        }
    }
    let path = big.display().to_string();

    // preview: an oversized limit returns exactly the cap, not the whole (2*cap) table.
    let (st, v) = get_json(&format!("/v1/preview?path={path}&limit=999999999")).await;
    assert_eq!(st, StatusCode::OK, "preview: {v}");
    assert_eq!(v["rows"].as_array().unwrap().len() as u64, cap);

    // profile: an oversized scan walks ~cap rows (bounded by the clamp), never the whole 2*cap file.
    let (st, p) = get_json(&format!("/v1/profile?path={path}&scan=999999999")).await;
    assert_eq!(st, StatusCode::OK, "profile: {p}");
    let scanned = p["scanned_rows"].as_u64().unwrap();
    assert!(
        (cap..2 * cap).contains(&scanned),
        "scan clamped to ~{cap}, got {scanned} (unclamped would be {rows})"
    );

    // The clamp bounds only the upper end: `limit=0` (an explicit "no rows" probe) still returns
    // zero rows rather than being rewritten to 1.
    let (st, z) = get_json(&format!("/v1/preview?path={path}&limit=0")).await;
    assert_eq!(st, StatusCode::OK, "preview limit=0: {z}");
    assert_eq!(z["rows"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn profile_scan_zero_reaches_the_footer_fast_path() {
    // `?scan=0` selects the near-instant footer-statistics profile (Parquet only), signalled by
    // `scanned_rows == 0`. The caller-limit clamp must bound only the upper end — a lower clamp of
    // 1 would rewrite 0 → 1 and silently scan a single row instead, disabling the fast path.
    let dir = tempfile::tempdir().unwrap();
    let pq = dir.path().join("t.parquet");
    write_tiny_parquet(&pq); // 2 rows
    let path = pq.display().to_string();

    let (st, p) = get_json(&format!("/v1/profile?path={path}&scan=0")).await;
    assert_eq!(st, StatusCode::OK, "profile scan=0: {p}");
    assert_eq!(
        p["scanned_rows"].as_u64().unwrap(),
        0,
        "scan=0 → footer path is marked by scanned_rows==0"
    );
    assert_eq!(
        p["row_count"].as_u64().unwrap(),
        2,
        "the footer reports the exact row count without scanning"
    );

    // A positive scan actually walks rows, confirming 0 is the special-cased fast path.
    let (st, p2) = get_json(&format!("/v1/profile?path={path}&scan=10")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(p2["scanned_rows"].as_u64().unwrap(), 2);
}

#[tokio::test]
#[cfg(feature = "sql")]
async fn the_advertised_query_cap_is_the_one_enforced() {
    // An advertised limit that the server does not actually apply is worse than no limit at all,
    // so this asks for more than the cap and checks the answer is clamped rather than obeyed.
    let (_, engines) = {
        let r = app_sql()
            .oneshot(
                Request::builder()
                    .uri("/v1/engines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let st = r.status();
        let v: serde_json::Value =
            serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap();
        (st, v)
    };
    let cap = engines["limits"]["max_query_rows"].as_u64().unwrap();

    // A fixture BIGGER than the cap, or the clamp is invisible and this test passes even if the
    // handler honours the oversized ask — an advertised limit tested with eight rows tests nothing.
    let dir = tempfile::tempdir().unwrap();
    let big = dir.path().join("big.csv");
    {
        use std::io::Write;
        let mut f = std::io::BufWriter::new(std::fs::File::create(&big).unwrap());
        writeln!(f, "id").unwrap();
        for i in 0..=cap {
            writeln!(f, "{i}").unwrap(); // cap + 1 data rows
        }
    }

    let query = |limit: Option<u64>| {
        let mut body =
            serde_json::json!({ "sql": "select * from t", "file": big.display().to_string() });
        if let Some(l) = limit {
            body["limit"] = serde_json::json!(l);
        }
        async move {
            let r = app_sql()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/query")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(r.into_body(), usize::MAX).await.unwrap(),
            )
            .unwrap()
        }
    };

    // Asking for MORE than the advertised cap is clamped to it, and the truncation is declared.
    let out = query(Some(cap + 1)).await;
    assert_eq!(
        out["num_rows"].as_u64().unwrap(),
        cap,
        "clamped to the advertised cap"
    );
    assert_eq!(out["capped"], true, "a truncated result must say so");

    // Naming no limit gets the advertised default, not the cap and not everything.
    let deflt = engines["limits"]["default_query_rows"].as_u64().unwrap();
    let out = query(None).await;
    assert_eq!(
        out["num_rows"].as_u64().unwrap(),
        deflt,
        "the advertised default applies"
    );
    assert_eq!(
        out["capped"], true,
        "the default filled → declared as possibly-truncated"
    );
}

#[tokio::test]
async fn info_reports_a_size_for_a_local_file() {
    // The size lookup grew a branch for remote objects; this pins that the local one still
    // answers from the filesystem, which is the case every test-suite run actually exercises.
    let (status, json) = get_json(&format!("/v1/info?path={CSV}")).await;
    assert_eq!(status, StatusCode::OK);
    let on_disk = std::fs::metadata(CSV).unwrap().len();
    assert_eq!(json["size_bytes"].as_u64().unwrap(), on_disk);
    assert_eq!(json["format"], "csv");
}

/// `GET /v1/info` answers in the shape `lakeleto info -o json` prints (`render::SourceInfo`): these
/// keys, in this order, with `credentials` only for a catalog table.
#[tokio::test]
async fn info_answers_in_the_shape_lakeleto_info_prints() {
    let (status, json) = get_json(&format!("/v1/info?path={CSV}")).await;
    assert_eq!(status, StatusCode::OK);
    let keys: Vec<&str> = json
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "path",
            "format",
            "engine",
            "size_bytes",
            "row_count",
            "columns"
        ]
    );
}

/// A [`WorkspaceStore`] double that records whether any call ever HANDED IT result rows. This is
/// the strongest statement a router-level test can make about egress: the store trait is the only
/// door result bytes leave through (`RemoteStore` is just this trait over HTTP), so "the store was
/// never given a result" is "nothing could have been uploaded", without needing a live server.
struct RecordingStore {
    inner: LocalStore,
    // Held so the backing directory outlives the store; a bare `tempdir().path()` would drop the
    // `TempDir` at once and delete the directory out from under `LocalStore`.
    _dir: tempfile::TempDir,
    result_writes: std::sync::atomic::AtomicUsize,
}

impl RecordingStore {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        Self {
            inner: LocalStore::at(dir.path()).unwrap(),
            _dir: dir,
            result_writes: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn result_writes(&self) -> usize {
        self.result_writes.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl WorkspaceStore for RecordingStore {
    fn list(&self) -> lakeleto::error::Result<Vec<lakeleto::workspace::WorkspaceMeta>> {
        self.inner.list()
    }
    fn create(&self, name: &str) -> lakeleto::error::Result<lakeleto::workspace::Workspace> {
        self.inner.create(name)
    }
    fn get(&self, id: &str) -> lakeleto::error::Result<lakeleto::workspace::Workspace> {
        self.inner.get(id)
    }
    fn save(
        &self,
        id: &str,
        ws: &lakeleto::workspace::Workspace,
    ) -> lakeleto::error::Result<lakeleto::workspace::Workspace> {
        self.inner.save(id, ws)
    }
    fn delete(&self, id: &str) -> lakeleto::error::Result<()> {
        self.inner.delete(id)
    }
    fn history(&self, id: &str) -> lakeleto::error::Result<Vec<lakeleto::workspace::RunRecord>> {
        self.inner.history(id)
    }
    fn append_run(
        &self,
        id: &str,
        rec: &lakeleto::workspace::RunRecord,
        result: Option<&lakeleto::engine::RowBatch>,
    ) -> lakeleto::error::Result<()> {
        if result.is_some() {
            self.result_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.append_run(id, rec, result)
    }
    fn run_result(
        &self,
        id: &str,
        run_id: &str,
        offset: usize,
        limit: usize,
    ) -> lakeleto::error::Result<lakeleto::engine::RowBatch> {
        self.inner.run_result(id, run_id, offset, limit)
    }
    fn put_result_bytes(
        &self,
        id: &str,
        run_id: &str,
        parquet: &[u8],
    ) -> lakeleto::error::Result<()> {
        self.result_writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.put_result_bytes(id, run_id, parquet)
    }
    fn run_result_bytes(&self, id: &str, run_id: &str) -> lakeleto::error::Result<Vec<u8>> {
        self.inner.run_result_bytes(id, run_id)
    }
    fn export(&self, id: &str) -> lakeleto::error::Result<lakeleto::workspace::WorkspaceBundle> {
        self.inner.export(id)
    }
    fn import(
        &self,
        bundle: &lakeleto::workspace::WorkspaceBundle,
    ) -> lakeleto::error::Result<lakeleto::workspace::Workspace> {
        self.inner.import(bundle)
    }
}

#[tokio::test]
async fn an_uncached_run_never_hands_result_rows_to_the_store() {
    // The LocalStore test above proves an uncached result cannot be RE-OPENED; this one proves
    // the stronger no-egress statement — with a store that could be remote, the router never even
    // OFFERED it the rows. A store that is never given a result has nothing to upload.
    let store = std::sync::Arc::new(RecordingStore::new());
    let dyn_store: std::sync::Arc<dyn WorkspaceStore> = store.clone();
    let router = || {
        let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
        lakeleto::api::router(
            EngineRegistry::new(read),
            10_000,
            None,
            None,
            true,
            dyn_store.clone(),
        )
    };

    let (_, ws) = send(
        router(),
        "POST",
        "/v1/workspaces",
        Some(serde_json::json!({ "name": "egress" })),
    )
    .await;
    let id = ws["id"].as_str().unwrap().to_string();

    // Default: no `cache` field → the store must never see a byte of the result.
    let (st, run) = send(
        router(),
        "POST",
        &format!("/v1/workspaces/{id}/runs"),
        Some(serde_json::json!({ "path": CSV, "preview": 3 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{run}");
    assert_eq!(
        store.result_writes(),
        0,
        "no result was offered to the store"
    );

    // Opt-in: the same run WITH the flag hands the store exactly one result.
    let (st, _) = send(
        router(),
        "POST",
        &format!("/v1/workspaces/{id}/runs"),
        Some(serde_json::json!({ "path": CSV, "preview": 3, "cache": true })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        store.result_writes(),
        1,
        "opting in is what writes, and only once"
    );
}

#[tokio::test]
#[cfg(feature = "sqlite")]
async fn the_query_cap_binds_the_database_engine_too() {
    // The cap's other implementation. DataFusion pushes a plan-level LIMIT; the database engine
    // cannot rewrite the user's SQL, so it streams and STOPS at the cap — and this test feeds it
    // a query whose full result is far larger than what it asks for, because a cap tested with
    // fewer rows than the cap tests nothing. A recursive CTE generates the rows server-side, so
    // no fixture data is written: an empty file is a valid, empty SQLite database, and the pool
    // opens it read-only.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("empty.db");
    std::fs::File::create(&db).unwrap();
    let uri = format!("sqlite://{}", db.display());

    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let dbe: Arc<dyn Engine> = Arc::new(lakeleto::engine::database::DatabaseEngine::new());
    let app = router(
        EngineRegistry::new(read).with_database(dbe),
        10_000,
        None,
        None,
        true,
        generic_store(),
    );

    let asked: usize = 500;
    let body = serde_json::json!({
        // 100k rows if left unbounded — the stream must stop at `asked`, not fetch and trim.
        "sql": "WITH RECURSIVE t(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM t WHERE i < 100000) SELECT i FROM t",
        "tables": [{ "name": "t", "path": uri }],
        "limit": asked,
    });
    let r = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let out: serde_json::Value =
        serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        out["num_rows"].as_u64().unwrap() as usize,
        asked,
        "clamped to the ask"
    );
    assert_eq!(out["capped"], true, "a filled cap is declared");
    // And the rows really are the first `asked` of the sequence, not an arbitrary window.
    // Compared as rendered text: SQLite types a computed CTE column dynamically, so the batch
    // mapper may land it as Utf8 rather than Int64 — the sequence check is what matters here.
    let cell = |idx: usize| -> String {
        let v = &out["rows"][idx]["i"];
        v.as_i64()
            .map(|n| n.to_string())
            .unwrap_or_else(|| v.as_str().unwrap_or("").to_string())
    };
    assert_eq!(cell(0), "1", "row 0 was {}", out["rows"][0]);
    assert_eq!(cell(asked - 1), asked.to_string());
}

/// The Arrow arm over **DataFusion** results, not just the local reader's.
///
/// `to_arrow_ipc` now refuses a `RowBatch` whose batches disagree with its declared schema, and
/// the SQL engine is the one producer where the declared schema and the batches come from two
/// different places: `RowBatch::schema` is taken from `df.schema()`, the batches from the
/// execution plan. If those two ever drifted in a way the check treats as a mismatch, every
/// Arrow response from a `--features sql` server would turn into a 400 — a worse bug than the
/// one the check exists to prevent. Both DataFusion-backed Arrow paths are pinned here: the
/// filtered `/v1/rows` scan (which the engine registry routes to DataFusion) and `POST /v1/query`.
#[tokio::test]
#[cfg(feature = "sql")]
async fn datafusion_results_still_encode_on_the_arrow_arm() {
    // A filtered window: the engine registry sends this to DataFusion rather than the Arrow
    // kernels.
    let r = app_sql()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/rows?path={CSV}&limit=5&filter=city:contains:a"
                ))
                .header(axum::http::header::ACCEPT, ARROW_MIME)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "the DataFusion scan encodes");
    let body = to_bytes(r.into_body(), usize::MAX).await.unwrap().to_vec();
    let rb = lakeleto::render::from_arrow_ipc(&body).expect("a real IPC stream");
    assert!(!rb.schema.fields().is_empty());

    // ...and a projection + aggregate, whose output schema DataFusion synthesises rather than
    // inheriting from the source.
    let sql = serde_json::json!({
        "sql": "SELECT city, count(*) AS n FROM t GROUP BY city ORDER BY city",
        "file": CSV,
    })
    .to_string();
    let r = app_sql()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/query")
                .header("content-type", "application/json")
                .header(axum::http::header::ACCEPT, ARROW_MIME)
                .body(Body::from(sql))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK, "the DataFusion query encodes");
    let body = to_bytes(r.into_body(), usize::MAX).await.unwrap().to_vec();
    let rb = lakeleto::render::from_arrow_ipc(&body).expect("a real IPC stream");
    assert_eq!(rb.schema.fields().len(), 2);
    assert_eq!(rb.schema.field(1).name(), "n");
    assert!(rb.num_rows() > 0);
}

/// REGRESSION. The batch-vs-schema guard on the Arrow arm must not compare *nullability*.
///
/// A `UNION ALL` whose two arms disagree about nullability is the standard case where
/// DataFusion's `df.schema()` and the batches its execution plan emits legitimately differ: the
/// literal arm is `NOT NULL`, the column arm is nullable, and the declared union schema does not
/// have to match either batch field for field. An earlier revision of
/// `render::ensure_batches_match_schema` compared `is_nullable()` and turned this correct query
/// into a `400` on the Arrow arm — while the JSON arm, which does no such check, still answered
/// `200`. A negotiated encoding that rejects what the default encoding accepts is a bug in the
/// encoding, not in the query.
#[tokio::test]
#[cfg(feature = "sql")]
async fn a_union_with_mixed_nullability_still_encodes_on_the_arrow_arm() {
    let sql = serde_json::json!({
        "sql": "SELECT city FROM t UNION ALL SELECT NULL UNION ALL SELECT 'zzz'",
        "file": CSV,
    })
    .to_string();

    for (accept, label) in [
        (Some(ARROW_MIME), "arrow arm"),
        (None, "json arm — the control"),
    ] {
        let mut req = Request::builder().method("POST").uri("/v1/query");
        req = req.header("content-type", "application/json");
        if let Some(a) = accept {
            req = req.header(axum::http::header::ACCEPT, a);
        }
        let r = app_sql()
            .oneshot(req.body(Body::from(sql.clone())).unwrap())
            .await
            .unwrap();
        assert_eq!(
            r.status(),
            StatusCode::OK,
            "{label}: a mixed-nullability UNION ALL must encode"
        );

        if accept.is_some() {
            let body = to_bytes(r.into_body(), usize::MAX).await.unwrap().to_vec();
            let rb = lakeleto::render::from_arrow_ipc(&body).expect("a real IPC stream");
            assert_eq!(rb.schema.fields().len(), 1);
            // The NULL and the literal both survive: more rows than the source alone.
            assert!(rb.num_rows() >= 2);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// `POST /v1/query` with `stream: true` — the Arrow IPC body written as the engine produces it
// ---------------------------------------------------------------------------------------------

/// A CSV big enough that DataFusion produces several batches, so the response is a stream of more
/// than one IPC message rather than a buffer with a stream's content-type.
#[cfg(feature = "sql")]
fn big_csv(dir: &std::path::Path) -> String {
    let path = dir.join("big.csv");
    let mut body = String::from("id,name\n");
    for i in 0..30_000 {
        body.push_str(&format!("{i},row-{i}\n"));
    }
    std::fs::write(&path, body).unwrap();
    path.to_string_lossy().into_owned()
}

#[cfg(feature = "sql")]
async fn post_query_stream(
    body: serde_json::Value,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query")
        .header("content-type", "application/json")
        .header("accept", ARROW_MIME)
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app_sql().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, headers, bytes.to_vec())
}

/// The streamed body decodes to the same rows the buffered one returns, and it is a *complete* IPC
/// stream — the end-of-stream marker is what distinguishes a finished result from an abandoned one,
/// so `from_arrow_ipc` succeeding is the assertion that matters most here.
#[tokio::test]
#[cfg(feature = "sql")]
async fn a_streamed_query_decodes_to_the_same_rows_as_a_buffered_one() {
    let dir = tempfile::tempdir().unwrap();
    let csv = big_csv(dir.path());

    let (status, headers, streamed) = post_query_stream(serde_json::json!({
        "sql": "SELECT id, name FROM t", "file": csv, "limit": 25_000, "stream": true
    }))
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").unwrap(),
        ARROW_MIME,
        "a streamed result is still an Arrow IPC stream"
    );

    let rb = lakeleto::render::from_arrow_ipc(&streamed).expect("a complete IPC stream");
    assert_eq!(rb.num_rows(), 25_000);
    assert!(
        rb.batches.len() > 1,
        "the point is that it arrives in pieces; got {} batch(es)",
        rb.batches.len()
    );

    // Byte-identical to the buffered encoding of the same query, so the flag changes when bytes
    // arrive and not what they say.
    let (status, _, buffered) = post_query_stream(serde_json::json!({
        "sql": "SELECT id, name FROM t", "file": csv, "limit": 25_000
    }))
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        streamed, buffered,
        "streaming must not change the encoding, only its timing"
    );
}

/// `capped` cannot be a header on a streaming response — it is not knowable before the first byte —
/// so the cap itself is sent instead and the client derives the same fact. This pins that the
/// information is preserved rather than dropped.
#[tokio::test]
#[cfg(feature = "sql")]
async fn a_streamed_response_reports_the_cap_in_place_of_capped() {
    let dir = tempfile::tempdir().unwrap();
    let csv = big_csv(dir.path());
    let (status, headers, body) = post_query_stream(serde_json::json!({
        "sql": "SELECT id FROM t", "file": csv, "limit": 100, "stream": true
    }))
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("x-lakeleto-row-cap").unwrap(),
        "100",
        "the client needs the cap to compute `capped` itself"
    );
    assert!(
        headers.get("x-lakeleto-capped").is_none(),
        "a streaming response must not claim to know something it cannot"
    );

    // And the cap really bounds the result, not just the header.
    let rb = lakeleto::render::from_arrow_ipc(&body).unwrap();
    assert_eq!(rb.num_rows(), 100);
    // capped, derived the way a client would: rows received == the cap it was told about.
    assert_eq!(rb.num_rows(), 100);
}

/// Streaming is refused over JSON rather than silently buffered. The JSON body carries aggregate
/// counts that do not exist until the last row is read, so honouring the flag there would mean
/// returning a different document — a wire change wearing a flag's clothing.
#[tokio::test]
#[cfg(feature = "sql")]
async fn streaming_is_refused_over_json_rather_than_ignored() {
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query")
        .header("content-type", "application/json")
        // No `accept: arrow`.
        .body(Body::from(
            serde_json::json!({ "sql": "SELECT 1 AS a", "file": CSV, "stream": true }).to_string(),
        ))
        .unwrap();
    let resp = app_sql().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("stream"),
        "the refusal should name the flag: {body}"
    );
}

/// A query that cannot be planned is still a 4xx with a message, because the plan is opened before
/// the response exists. Once a body has started the status is spent, so this is the boundary worth
/// pinning: everything knowable up front stays knowable.
#[tokio::test]
#[cfg(feature = "sql")]
async fn a_planning_failure_is_still_a_status_code_not_a_truncated_stream() {
    let (status, _, body) = post_query_stream(serde_json::json!({
        "sql": "SELECT no_such_column FROM t", "file": CSV, "stream": true
    }))
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "expected an error status, got {status}"
    );
    assert!(
        lakeleto::render::from_arrow_ipc(&body).is_err(),
        "an error response must not decode as a result"
    );
}

/// An empty result still produces a well-formed stream — schema message, no batches, end-of-stream
/// marker — rather than an empty body a reader cannot interpret.
#[tokio::test]
#[cfg(feature = "sql")]
async fn an_empty_streamed_result_is_still_a_valid_ipc_stream() {
    let (status, _, body) = post_query_stream(serde_json::json!({
        "sql": "SELECT * FROM t WHERE 1 = 0", "file": CSV, "stream": true
    }))
    .await;
    assert_eq!(status, StatusCode::OK);
    let rb = lakeleto::render::from_arrow_ipc(&body).expect("a valid, empty IPC stream");
    assert_eq!(rb.num_rows(), 0);
    assert!(
        !rb.schema.fields().is_empty(),
        "the schema travels even with no rows — a client still needs the columns"
    );
}

/// A catalog reference is not a path under `--root`, so it is refused before anything resolves it,
/// whether or not the build reads catalogs: as a source, and as a directory to browse.
#[tokio::test]
async fn a_catalog_reference_is_refused_under_root() {
    let root = tempfile::tempdir().unwrap();
    for uri in [
        "/v1/schema?path=catalog%3A%2F%2Fprod%2Fsales%2Forders",
        "/v1/preview?path=catalog%3A%2F%2Fprod%2Fsales%2Forders",
        "/v1/list?dir=catalog%3A%2F%2F",
        "/v1/list?dir=catalog%3A%2F%2Fprod%2Fsales%2F",
    ] {
        let r = app_root(root.path().to_path_buf())
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN, "{uri}");
        let body = to_bytes(r.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("outside the server root"),
            "{uri}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}
