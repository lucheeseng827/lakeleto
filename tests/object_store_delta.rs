//! Delta tables in an object store, read from a stand-in for S3 on localhost. The log is listed
//! from the checkpoint `_last_checkpoint` names; the checkpoint and the commits after it are read;
//! the data files are read by ranged requests, only as far as a read goes. Nothing is copied to
//! disk, so these tests check the requests made.
//!
//! The table is `tests/data/delta_checkpointed`, which delta-rs wrote (see
//! `tests/data/delta_checkpointed.py`): a checkpoint at version 2, commit 3 after it, and commits
//! 0–2 cleaned up. One partition's directory, and so its keys, hold a `%`.
//!
//! Run with: `cargo test --features delta,object-store --test object_store_delta`.
#![cfg(all(feature = "delta", feature = "object-store"))]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};

use lakeleto::engine::{Engine, ScanSpec};
use lakeleto::{EngineError, Format, LocalReaderEngine, RemoteProbe, RequestContext, Source};

#[path = "support/fake_s3.rs"]
mod fake_s3;
use fake_s3::FakeS3;

const ORDERS: &str = "warehouse/delta/orders";
const LOG: &str = "warehouse/delta/orders/_delta_log";

fn uri(key: &str) -> String {
    format!("s3://bucket/{key}")
}

fn orders() -> Source {
    Source::with_format(uri(ORDERS), Format::Delta)
}

/// The live files' keys: the eu file the delete rewrote, the "us west:%x" one, and commit 3's.
const LIVE: [&str; 3] = [
    "region=eu/part-00000-aa8efbac-5875-4706-a888-b50559065548-c000.snappy.parquet",
    "region=us%20west%3A%25x/part-00000-a64158a1-efde-4f72-9b4a-ddc86150b2c3-c000.snappy.parquet",
    "region=eu/part-00000-54e61bda-184b-40e6-b7f4-37c11d5c3015-c000.snappy.parquet",
];
/// The eu file version 2's delete removed, still in the bucket as it is until a `VACUUM`.
const REMOVED: &str = "6a8c34f0";

/// Put the fixture's files under `prefix`, each at its path in the table. Returns each key's size.
fn put_table(s3: &FakeS3, prefix: &str) -> HashMap<String, usize> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/delta_checkpointed");
    let mut stored = HashMap::new();
    let mut dirs = vec![root.clone()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
                continue;
            }
            let relative = path.strip_prefix(&root).unwrap().to_string_lossy();
            let key = format!("{prefix}/{}", relative.replace('\\', "/"));
            let body = std::fs::read(&path).unwrap();
            stored.insert(key.clone(), body.len());
            s3.put(&key, body);
        }
    }
    stored
}

/// Every row as `(id, region)`, by id.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let regions = batch
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.push((ids.value(i), regions.value(i).to_string()));
        }
    }
    out.sort();
    out
}

fn live_rows() -> Vec<(i64, String)> {
    [
        (1, "eu"),
        (3, "eu"),
        (4, "us west:%x"),
        (5, "us west:%x"),
        (6, "eu"),
    ]
    .into_iter()
    .map(|(id, region)| (id, region.to_string()))
    .collect()
}

/// The table's commits 0–2 are gone; its checkpoint holds their state. The log is listed from the
/// version `_last_checkpoint` names, the checkpoint is read by ranges, then commit 3, then the
/// live files by ranges: the file the delete removed, still in the bucket, is never asked for.
#[test]
fn a_cleaned_up_table_reads_from_its_checkpoint_listing_the_log_from_the_hint() {
    let s3 = FakeS3::start();
    let stored: usize = put_table(&s3, ORDERS).values().sum();
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let preview = engine
        .preview(&RequestContext::detached(), &orders(), 10)
        .unwrap();
    assert_eq!(rows(&preview.batches), live_rows());

    let requests = s3.requests();
    assert_eq!(requests[0], format!("GET {LOG}/_last_checkpoint"));
    assert_eq!(
        requests[1],
        format!("LIST {LOG}/ after {LOG}/00000000000000000002"),
        "{requests:#?}"
    );
    let checkpoint = format!("{LOG}/00000000000000000002.checkpoint.parquet");
    let commit = format!("GET {LOG}/00000000000000000003.json");
    let at_commit = requests.iter().position(|r| *r == commit).unwrap();
    for request in &requests[2..at_commit] {
        assert!(
            request.starts_with(&format!("HEAD {checkpoint}"))
                || request.starts_with(&format!("GET {checkpoint} bytes=")),
            "the checkpoint, by ranges, before the commit after it: {requests:#?}"
        );
    }
    for request in &requests[at_commit + 1..] {
        assert!(
            LIVE.iter()
                .any(|key| request.starts_with(&format!("GET {ORDERS}/{key} bytes="))),
            "then only the live files, by ranges and with no HEAD (the log records their \
             sizes): {requests:#?}"
        );
    }
    assert!(
        !requests.iter().any(|r| r.contains(REMOVED)),
        "{requests:#?}"
    );
    // One listing, from the hint: not the whole log.
    assert_eq!(
        requests.iter().filter(|r| r.starts_with("LIST")).count(),
        1,
        "{requests:#?}"
    );
    assert!(s3.sent() < stored as u64, "sent {} of {stored}", s3.sent());
}

/// The log records each file's rows, so the table is counted, and its schema known, without
/// asking for a data file.
#[test]
fn the_schema_and_count_are_the_logs() {
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let schema = engine
        .schema(&RequestContext::detached(), &orders())
        .unwrap();
    assert_eq!(schema.row_count, Some(5));
    let columns: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(columns, ["id", "name", "region"]);
    let requests = s3.requests();
    assert!(
        !requests.iter().any(|r| r.contains("/region=")),
        "no data file is opened: {requests:#?}"
    );
}

/// A window past the first files skips them by the rows the log records, without opening them.
#[test]
fn a_deep_window_skips_whole_files_by_their_logged_rows() {
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let all = engine
        .preview(&RequestContext::detached(), &orders(), 10)
        .unwrap();
    // The files in the plan's order; the window's offset is the first two files' rows.
    let in_order: Vec<i64> = all
        .batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..ids.len()).map(move |i| ids.value(i))
        })
        .collect();
    s3.reset();
    let res = engine
        .scan(
            &RequestContext::detached(),
            &orders(),
            &ScanSpec {
                offset: 4,
                limit: 10,
                sort: None,
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    assert_eq!(res.matched_rows, 5);
    let ids: Vec<i64> = rows(&res.batch.batches)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, in_order[4..]);
    let opened: Vec<_> = LIVE
        .iter()
        .filter(|key| s3.requests().iter().any(|r| r.contains(*key)))
        .collect();
    assert_eq!(
        opened.len(),
        1,
        "only the file the window is in: {opened:?}"
    );
}

/// `_last_checkpoint` is a hint. One naming a checkpoint that is not there leads to a listing
/// of the whole log, which finds the one that is.
#[test]
fn a_stale_hint_falls_back_to_listing_the_whole_log() {
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    s3.put(
        &format!("{LOG}/_last_checkpoint"),
        br#"{"version":7,"size":5}"#.to_vec(),
    );
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let preview = engine
        .preview(&RequestContext::detached(), &orders(), 10)
        .unwrap();
    assert_eq!(rows(&preview.batches), live_rows());
    let listings: Vec<_> = s3
        .requests()
        .into_iter()
        .filter(|r| r.starts_with("LIST"))
        .collect();
    assert_eq!(
        listings,
        [
            format!("LIST {LOG}/ after {LOG}/00000000000000000007"),
            format!("LIST {LOG}/"),
        ]
    );
}

#[test]
fn a_commit_missing_after_the_checkpoint_is_an_error_not_a_short_read() {
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    s3.put(&format!("{LOG}/00000000000000000005.json"), b"".to_vec());
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let Err(err) = engine.preview(&RequestContext::detached(), &orders(), 10) else {
        panic!("commit 4 is missing");
    };
    assert!(err.to_string().contains("no commit 4"), "{err}");
}

/// A table in an object store is data someone else may have written. A log there naming a file
/// on this machine is refused, and the file is never opened.
#[test]
fn a_table_in_a_store_cannot_name_a_file_on_this_machine() {
    let local = tempfile::tempdir().unwrap();
    let secret = local.path().join("secret.parquet");
    std::fs::write(&secret, b"PAR1").unwrap();
    for named in [
        secret.display().to_string(),
        format!("file://{}", secret.display()),
    ] {
        let s3 = FakeS3::start();
        put_table(&s3, ORDERS);
        let add = serde_json::json!({"add": {
            "path": named, "partitionValues": {"region": "eu"}, "size": 4,
            "modificationTime": 0, "dataChange": true
        }});
        s3.put(
            &format!("{LOG}/00000000000000000004.json"),
            format!("{add}\n").into_bytes(),
        );
        let engine = LocalReaderEngine::default().with_store_options(s3.options());
        let Err(err) = engine.preview(&RequestContext::detached(), &orders(), 10) else {
            panic!("{named}: a store table naming a local file must be refused");
        };
        assert!(matches!(err, EngineError::Forbidden(_)), "{named}: {err}");
        assert!(
            err.to_string().contains("is not in an object store"),
            "{named}: {err}"
        );
        assert!(
            !err.to_string().contains("secret"),
            "the refusal names no local path: {err}"
        );
    }
}

/// The log is read as the identity the call brought, not the process's. Observable with no
/// network: `objstore` refuses a provider for the wrong family before building a store, so an
/// `s3://` table whose read fails mentioning GCS can only have been read as the GCS identity.
#[test]
fn a_table_in_a_store_is_read_as_the_calls_identity() {
    let provider: object_store::gcp::GcpCredentialProvider = Arc::new(
        object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
            bearer: "not-a-real-token".to_string(),
        }),
    );
    let ctx = RequestContext::detached().with_store_options(
        lakeleto::objstore::StoreOptions::empty()
            .with_credentials(lakeleto::objstore::StoreCredentials::Gcs(provider)),
    );
    let err = LocalReaderEngine::default()
        .schema(&ctx, &orders())
        .expect_err("a GCS identity cannot address an s3:// table");
    assert!(err.to_string().contains("GCS"), "{err}");
}

/// What `check` counts of a Delta table in a store: its live files, their bytes as the log
/// records them, and the latest time the log records.
#[test]
fn a_tables_files_in_a_store_are_its_live_files_and_its_time_is_the_logs() {
    let s3 = FakeS3::start();
    let stored = put_table(&s3, ORDERS);
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let seen = engine
        .files(&RequestContext::detached(), &orders())
        .unwrap();
    assert_eq!(seen.files, Some(3));
    let live_bytes: usize = LIVE
        .iter()
        .map(|key| stored[&format!("{ORDERS}/{key}")])
        .sum();
    assert_eq!(seen.bytes, Some(live_bytes as u64));
    assert_eq!(seen.updated_ms, Some(1_795_703_408_135), "commit 3's time");
}

/// A bare prefix is told to be a Delta table by its `_delta_log/`, as a directory is.
#[test]
fn a_prefix_with_a_delta_log_is_detected_as_delta() {
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    let ctx = RequestContext::detached().with_store_options(s3.options());
    let source = Source::detect_in(uri(ORDERS), &ctx, RemoteProbe::VendedOnly).unwrap();
    assert_eq!(source.format, Format::Delta);
    assert_eq!(
        s3.requests(),
        [format!("LIST {LOG}/")],
        "one probe, the Delta one"
    );
}

#[cfg(feature = "sql")]
#[test]
fn sql_reads_a_delta_table_in_a_store() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::NamedSource;

    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    let engine = DataFusionEngine::with_store_options(s3.options());
    let rows = engine
        .query(
            &RequestContext::detached(),
            "SELECT count(*) AS n, sum(id) AS ids, count(DISTINCT region) AS regions FROM t",
            &[NamedSource {
                name: "t".to_string(),
                source: orders(),
            }],
        )
        .unwrap();
    let column = |i: usize| {
        rows.batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!((column(0), column(1), column(2)), (5, 19, 2));
    assert!(
        !s3.requests().iter().any(|r| r.contains(REMOVED)),
        "{:#?}",
        s3.requests()
    );
}

/// A version of a table in a store is compared by its log: `_last_checkpoint`, a listing from it,
/// the checkpoint, and the commits up to the version. No data file is asked for.
#[test]
fn a_version_in_a_store_is_described_by_its_log_alone() {
    use lakeleto::diff::{diff_snapshots, Change, DiffOptions};
    use lakeleto::snapshot::SnapshotRef;
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    let registry = lakeleto::EngineRegistry::new(Arc::new(
        LocalReaderEngine::default().with_store_options(s3.options()),
    ));
    let report = diff_snapshots(
        &RequestContext::detached(),
        &registry,
        (&orders(), Some(&SnapshotRef::Id(2))),
        (&orders(), None),
        &DiffOptions::default(),
    )
    .unwrap();
    assert_eq!((report.rows.left, report.rows.right), (Some(4), Some(5)));
    assert_eq!(report.left.snapshot.as_deref(), Some("2"));
    assert_eq!(report.right.snapshot.as_deref(), Some("3"));
    assert!(
        report.found.contains(&Change::RowCount),
        "{:?}",
        report.found
    );
    let requests = s3.requests();
    assert!(
        !requests.iter().any(|r| r.contains("/region=")),
        "no data file: {requests:#?}"
    );
    // Version 2 is the checkpoint itself: listed from the hint, and replayed from it alone. Its
    // commit, cleaned up, is asked for only to date the version.
    assert!(
        requests.contains(&format!("LIST {LOG}/ after {LOG}/00000000000000000002")),
        "{requests:#?}"
    );
}

/// What the log says of the table now is what every row says: each statistic the metadata gives
/// equals the one an exact count over the rows gives. The independent check on `describe`.
#[cfg(feature = "sql")]
#[test]
fn the_log_at_the_head_agrees_with_every_row() {
    use lakeleto::diff::{diff, diff_snapshots, DiffOptions, StatsMethod};
    use lakeleto::snapshot::SnapshotRef;
    let table = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/delta_checkpointed");
    let source = Source::with_format(&table, Format::Delta);
    let registry = lakeleto::EngineRegistry::local();
    let ctx = RequestContext::detached();
    let exact = diff(&ctx, &registry, &source, &source, &DiffOptions::default()).unwrap();
    assert_eq!(exact.stats.method, StatsMethod::Exact);
    let by_metadata = diff_snapshots(
        &ctx,
        &registry,
        (&source, Some(&SnapshotRef::Id(3))),
        (&source, None),
        &DiffOptions::default(),
    )
    .unwrap();
    assert_eq!(by_metadata.rows.left, exact.rows.left);
    let mut compared = 0;
    for (meta, rows) in by_metadata.stats.columns.iter().zip(&exact.stats.columns) {
        assert_eq!(meta.name, rows.name);
        let (m, r) = (&meta.left, &rows.left);
        for (given, every) in [
            (m.min.as_ref(), r.min.as_ref()),
            (m.max.as_ref(), r.max.as_ref()),
        ] {
            if let Some(given) = given {
                assert_eq!(Some(given), every, "{}", meta.name);
                compared += 1;
            }
        }
        if let Some(nulls) = m.nulls {
            assert_eq!(Some(nulls), r.nulls, "{}", meta.name);
            compared += 1;
        }
    }
    assert!(compared >= 7, "the statistics given: {compared}");
}

/// A name that exists whole is a path (ADR-0002), in a store too: a table under a prefix that
/// ends in `@7` is that table. Only a name with nothing under it is a snapshot address, which
/// anything but `diff` refuses, pointing at it.
#[test]
fn a_prefix_ending_in_at_digits_is_a_table_when_one_is_there() {
    let s3 = FakeS3::start();
    put_table(&s3, "warehouse/delta/orders@7");
    let ctx = RequestContext::detached().with_store_options(s3.options());
    let source = Source::detect_in(
        uri("warehouse/delta/orders@7"),
        &ctx,
        RemoteProbe::VendedOnly,
    )
    .unwrap();
    assert_eq!(source.format, Format::Delta);
    assert_eq!(
        source.path.to_string_lossy(),
        uri("warehouse/delta/orders@7")
    );

    let err = Source::detect_in(
        uri("warehouse/delta/orders@3"),
        &ctx,
        RemoteProbe::VendedOnly,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("lakeleto diff"), "{err}");
}

/// A name ending in what reads as a snapshot address is told by its whole name strictly: a listing
/// the store refuses is an error, since "nothing here" would read it as a snapshot of another
/// table. A name with no such ending keeps the usual answer, which asks for a format.
#[test]
fn a_refused_probe_of_a_name_ending_at_digits_is_an_error_not_a_snapshot() {
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    s3.refuse("warehouse/delta/orders@2");
    s3.refuse("warehouse/delta/denied");
    let ctx = RequestContext::detached().with_store_options(s3.options());
    let detect = |key: &str| {
        Source::detect_in(uri(key), &ctx, RemoteProbe::VendedOnly)
            .unwrap_err()
            .to_string()
    };
    let err = detect("warehouse/delta/orders@2");
    assert!(
        err.contains("could not tell whether") && !err.contains("lakeleto diff"),
        "{err}"
    );
    let err = detect("warehouse/delta/denied");
    assert!(err.contains("cannot infer the format"), "{err}");
}

/// `lakeleto diff` on store names, through the binary: `orders@2` is version 2 of `orders`, with
/// `--format delta` or without; a prefix named `kept@7` holding a table is that table; and a probe
/// the store refuses is the error, not a read of `orders` at 2.
#[test]
fn a_diff_side_in_a_store_names_a_snapshot_only_where_its_whole_name_is_nothing() {
    let s3 = FakeS3::start();
    put_table(&s3, ORDERS);
    put_table(&s3, "warehouse/delta/kept@7");
    let lakeleto = |args: &[&str]| {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_lakeleto"))
            .args(args)
            .env("AWS_ENDPOINT", s3.endpoint())
            .env("AWS_ALLOW_HTTP", "true")
            .env("AWS_SKIP_SIGNATURE", "true")
            .env("AWS_REGION", "us-east-1")
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env_remove("AWS_SESSION_TOKEN")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        (out.status.code(), format!("{stdout}{stderr}"))
    };
    let (at2, now) = (uri("warehouse/delta/orders@2"), uri(ORDERS));
    let kept = uri("warehouse/delta/kept@7");
    for format in [&[][..], &["--format", "delta"][..]] {
        let mut args = vec!["diff", &at2, &now, "--fail-on", "row-count"];
        args.extend_from_slice(format);
        let (code, out) = lakeleto(&args);
        assert_eq!(code, Some(3), "{format:?}: {out}");
        assert!(
            out.contains(&format!("{now}@2")) && out.contains(&format!("{now}@3")),
            "{format:?}: {out}"
        );

        let mut args = vec!["diff", &kept, &kept];
        args.extend_from_slice(format);
        let (code, out) = lakeleto(&args);
        assert_eq!(code, Some(0), "{format:?}: {out}");
        assert!(!out.contains("metadata"), "{format:?}: {out}");
    }

    s3.refuse("warehouse/delta/orders@2");
    let (code, out) = lakeleto(&["diff", &at2, &now]);
    assert_eq!(code, Some(1), "{out}");
    assert!(out.contains("could not tell whether"), "{out}");
}
