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
use lakeleto::LocalReaderEngine;

const CSV: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/people.csv");

/// A throwaway workspace store for tests that don't exercise workspaces — a fixed, reused temp
/// subdir (never written to by these routers, so no isolation concern).
fn generic_store() -> Arc<dyn WorkspaceStore> {
    let dir = std::env::temp_dir().join("lakeleto-test-generic-store");
    Arc::new(LocalStore::at(dir).unwrap())
}

fn app() -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    router(read, None, None, 10_000, None, None, true, generic_store())
}

fn app_auth(token: &str) -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    // loopback = true, so the `?token=` query form is accepted (the local browser flow).
    router(
        read,
        None,
        None,
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
        read,
        None,
        None,
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
        read,
        None,
        None,
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
    router(read, None, None, 10_000, None, None, true, store)
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
        read,
        None,
        None,
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
    let rb = LocalReaderEngine::default().preview(&source, 10).unwrap();
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

/// A router with the SQL engine wired, which is what a real `serve --features sql` is. It matters
/// for filters specifically: `scan_engine` routes any non-plain window — i.e. every filtered scan
/// — to DataFusion, so the SQL `WHERE` builder, not the Arrow kernel path, answers them.
#[cfg(feature = "sql")]
fn app_sql() -> axum::Router {
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let sql: Arc<dyn Engine> = Arc::new(lakeleto::engine::sql::DataFusionEngine::new());
    router(
        read,
        Some(sql),
        None,
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
            read,
            None,
            None,
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
        read,
        None,
        Some(dbe),
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
/// filtered `/v1/rows` scan (which `scan_engine` routes to DataFusion) and `POST /v1/query`.
#[tokio::test]
#[cfg(feature = "sql")]
async fn datafusion_results_still_encode_on_the_arrow_arm() {
    // A filtered window: `scan_engine` sends this to DataFusion rather than the Arrow kernels.
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
