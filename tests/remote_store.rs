//! `RemoteStore` round-trip against a live in-process `lakeleto serve` — proving the
//! store-swap seam end-to-end over real HTTP: another server that speaks
//! `/v1/workspaces/*` (here: our own router, backed by a `LocalStore`; in production:
//! a second `lakeleto serve` or the Lakeleto Cloud plane) is a drop-in workspace backend.
//! Run with: `cargo test --features serve,remote --test remote_store`.
#![cfg(all(feature = "serve", feature = "remote"))]

use std::sync::Arc;

use lakeleto::api::router;
use lakeleto::engine::Engine;
use lakeleto::workspace::{new_run_id, now_ms, LocalStore, RunRecord, RunStatus, WorkspaceStore};
use lakeleto::workspace_remote::RemoteStore;
use lakeleto::{LocalReaderEngine, Source};

const CSV: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/people.csv");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_store_round_trips_against_a_live_server() {
    // The "plane": an in-process lakeleto serve router over its own LocalStore.
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(dir.path()).unwrap());
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let app = router(read, None, 10_000, None, None, true, backend);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // The client side runs on a blocking thread (RemoteStore is sync, like every store).
    tokio::task::spawn_blocking(move || {
        let rs = RemoteStore::new(format!("http://{addr}"), None);

        // CRUD.
        let ws = rs.create("synced from the laptop").unwrap();
        assert_eq!(rs.list().unwrap().len(), 1);
        let mut fetched = rs.get(&ws.id).unwrap();
        fetched.name = "renamed remotely".into();
        assert_eq!(rs.save(&ws.id, &fetched).unwrap().name, "renamed remotely");

        // Local execution → synced record + cached result bytes.
        let source = Source::resolve(CSV, None).unwrap();
        let rb = LocalReaderEngine::default().preview(&source, 10).unwrap();
        let rec = RunRecord {
            id: new_run_id(),
            at_ms: now_ms(),
            sql: Some("SELECT * FROM t".into()),
            source_path: CSV.into(),
            format: Some("csv".into()),
            status: RunStatus::Ok,
            error: None,
            row_count: Some(rb.num_rows() as u64),
            duration_ms: 1,
            cached: true,
        };
        rs.append_run(&ws.id, &rec, Some(&rb)).unwrap();

        // The synced history + a windowed read of the remote cache.
        let hist = rs.history(&ws.id).unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].id, rec.id);
        let win = rs.run_result(&ws.id, &rec.id, 0, 3).unwrap();
        assert_eq!(win.num_rows(), 3);

        // Portable bundle round-trip mints a fresh id server-side.
        let bundle = rs.export(&ws.id).unwrap();
        assert_eq!(bundle.bundle_version, 1);
        let imported = rs.import(&bundle).unwrap();
        assert_ne!(imported.id, ws.id);

        // Remote errors keep their class: a missing workspace is NotFound here too.
        assert!(rs.get("ws-does-not-exist").is_err());

        rs.delete(&ws.id).unwrap();
        assert!(rs.get(&ws.id).is_err());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_result_upload_never_records_history() {
    // append_run must upload the result BEFORE posting the history record (same ordering as
    // LocalStore: cache first, record second). Force the upload to fail — a run id the
    // server's path-safety check rejects — and the history must stay empty: no record may
    // ever claim `cached: true` for bytes that never landed.
    let dir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn WorkspaceStore> = Arc::new(LocalStore::at(dir.path()).unwrap());
    let read: Arc<dyn Engine> = Arc::new(LocalReaderEngine::default());
    let app = router(read, None, 10_000, None, None, true, backend);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::task::spawn_blocking(move || {
        let rs = RemoteStore::new(format!("http://{addr}"), None);
        let ws = rs.create("ordering").unwrap();
        let source = Source::resolve(CSV, None).unwrap();
        let rb = LocalReaderEngine::default().preview(&source, 5).unwrap();
        let rec = RunRecord {
            id: "run..not-path-safe".into(), // rejected by the server's safe_id on the PUT
            at_ms: now_ms(),
            sql: Some("SELECT * FROM t".into()),
            source_path: CSV.into(),
            format: Some("csv".into()),
            status: RunStatus::Ok,
            error: None,
            row_count: Some(rb.num_rows() as u64),
            duration_ms: 1,
            cached: true,
        };
        assert!(rs.append_run(&ws.id, &rec, Some(&rb)).is_err());
        // The upload failed first, so the lying history record was never posted.
        assert!(rs.history(&ws.id).unwrap().is_empty());
    })
    .await
    .unwrap();
}
