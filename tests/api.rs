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

    // run a scan of the CSV → recorded + cached
    let (st, run) = send(
        app_store(store.clone()),
        "POST",
        &format!("/v1/workspaces/{id}/runs"),
        Some(serde_json::json!({ "path": CSV, "limit": 100, "preview": 3 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "run resp: {run}");
    assert_eq!(run["run"]["status"], "ok");
    assert_eq!(run["num_rows"].as_u64().unwrap(), 3, "preview window");
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
