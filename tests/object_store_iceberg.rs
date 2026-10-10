//! Iceberg tables in an object store, read from a stand-in for S3 on localhost. The planner fetches
//! the current snapshot's metadata, manifest list and manifests, and nothing else the table has
//! ever written; data files are read by ranged requests, only as far as a read goes. Nothing is
//! copied to disk, so these tests check the requests made rather than any file left behind.
//!
//! Run with: `cargo test --features iceberg,object-store --test object_store_iceberg`.
#![cfg(all(feature = "iceberg", feature = "object-store"))]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;

use lakeleto::engine::{Engine, ScanSpec};
use lakeleto::{EngineError, Format, LocalReaderEngine, RequestContext, Source};

#[path = "support/fake_s3.rs"]
mod fake_s3;
#[path = "support/iceberg_fixtures.rs"]
mod iceberg_fixtures;
use fake_s3::FakeS3;
use iceberg_fixtures::*;

fn orders() -> Source {
    Source::with_format(uri(ORDERS), Format::Iceberg)
}

/// The requests the current snapshot's metadata takes: the hint, the metadata it names, the
/// manifest list, and the manifest.
fn metadata_requests() -> [String; 4] {
    [
        format!("GET {ORDERS}/metadata/version-hint.text"),
        format!("GET {ORDERS}/metadata/v2.metadata.json"),
        format!("GET {ORDERS}/metadata/snap-2.avro"),
        format!("GET {ORDERS}/metadata/manifest-2.avro"),
    ]
}

#[test]
fn a_window_is_the_current_snapshots_metadata_and_a_ranged_read_of_one_file() {
    let s3 = FakeS3::start();
    let stored: usize = put_orders(&s3).values().sum();
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let rows = engine
        .preview(&RequestContext::detached(), &orders(), 10)
        .unwrap();
    assert_eq!(ids(&rows.batches), (0..10).collect::<Vec<_>>());

    // Before this, a table in a store was copied to disk whole: every object under the prefix,
    // the superseded snapshot and the orphan included, on the first read.
    let requests = s3.requests();
    assert_eq!(requests[..4], metadata_requests());
    let data = &requests[4..];
    assert!(!data.is_empty());
    for request in data {
        assert!(
            request.starts_with(&format!("GET {ORDERS}/data/a.parquet bytes=")),
            "only the first file is read, and only by ranges: {requests:#?}"
        );
    }
    // The manifest records each file's size, so no `HEAD` asks for it.
    assert!(
        !requests.iter().any(|r| r.starts_with("HEAD")),
        "{requests:#?}"
    );
    // A footer and one row group of one file: a fraction of what the bucket holds.
    assert!(
        s3.sent() < stored as u64 / 8,
        "sent {} of {stored} bytes",
        s3.sent()
    );
}

/// The manifest records each file's rows, so the table is counted without opening a file: the
/// schema takes the first file's footer, and the count no request at all.
#[test]
fn the_schema_is_one_footer_and_the_count_is_the_manifests() {
    let s3 = FakeS3::start();
    let stored: usize = put_orders(&s3).values().sum();
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let schema = engine
        .schema(&RequestContext::detached(), &orders())
        .unwrap();
    assert_eq!(schema.row_count, Some(90_000));
    let columns: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(columns, ["id", "pad"]);

    let requests = s3.requests();
    assert_eq!(requests[..4], metadata_requests());
    for request in &requests[4..] {
        assert!(
            request.starts_with(&format!("GET {ORDERS}/data/a.parquet bytes=")),
            "only the first file's footer: {requests:#?}"
        );
    }
    assert!(
        s3.sent() < stored as u64 / 100,
        "sent {} of {stored} bytes",
        s3.sent()
    );
}

/// A manifest that leaves out a file's rows leaves its footer to count them.
#[test]
fn a_file_its_manifest_does_not_count_is_counted_by_its_footer() {
    let s3 = FakeS3::start();
    const UNCOUNTED: &str = "warehouse/db/uncounted";
    let mut entries = Vec::new();
    for (i, name) in ["x.parquet", "y.parquet"].iter().enumerate() {
        let key = format!("{UNCOUNTED}/data/{name}");
        let body = parquet(i as i64 * 50..i as i64 * 50 + 50, 50);
        // `record_count` and `file_size_in_bytes` 0: what a reader sees when a writer left them
        // out. The size is then found by a `HEAD`.
        let (rows, size) = if i == 0 { (50, body.len()) } else { (0, 0) };
        entries.push(entry(0, &uri(&key), rows, size));
        s3.put(&key, body);
    }
    let manifest = format!("{UNCOUNTED}/metadata/m.avro");
    let list = format!("{UNCOUNTED}/metadata/snap.avro");
    s3.put(&manifest, avro(MANIFEST_SCHEMA, entries));
    s3.put(&list, manifest_list(&[(&uri(&manifest), 0)]));
    s3.put(
        &format!("{UNCOUNTED}/metadata/v1.metadata.json"),
        metadata(UNCOUNTED, &[(1, &uri(&list))]),
    );
    s3.put(
        &format!("{UNCOUNTED}/metadata/version-hint.text"),
        b"1".to_vec(),
    );
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let schema = engine
        .schema(
            &RequestContext::detached(),
            &Source::with_format(uri(UNCOUNTED), Format::Iceberg),
        )
        .unwrap();
    assert_eq!(schema.row_count, Some(100));
    let requests = s3.requests();
    assert!(
        requests.iter().any(|r| r.contains("y.parquet bytes=")),
        "y.parquet's footer counts its rows: {requests:#?}"
    );
    assert!(
        requests.contains(&format!("HEAD {UNCOUNTED}/data/y.parquet")),
        "{requests:#?}"
    );
}

/// Where a ranged `GET` starts: `GET key bytes=start-end`.
fn range_start(request: &str) -> usize {
    let range = request.split_once(" bytes=").expect("a ranged request").1;
    range.split_once('-').unwrap().0.parse().unwrap()
}

/// The ranges of data-file requests that read more than a footer: row groups.
fn row_group_reads<'a>(requests: &'a [String], sizes: &HashMap<String, usize>) -> Vec<&'a str> {
    let tail = 64 * 1024;
    requests
        .iter()
        .filter(|r| r.contains("/data/"))
        .filter(|r| range_start(r) + tail < sizes[r.split(' ').nth(1).unwrap()])
        .map(String::as_str)
        .collect()
}

#[test]
fn a_deep_window_skips_whole_files_by_their_manifest_counts() {
    let s3 = FakeS3::start();
    let sizes = put_orders(&s3);
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let res = engine
        .scan(
            &RequestContext::detached(),
            &orders(),
            &ScanSpec {
                offset: 75_000,
                limit: 10,
                sort: None,
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    assert_eq!(
        ids(&res.batch.batches),
        (75_000..75_010).collect::<Vec<_>>()
    );
    assert_eq!(res.matched_rows, 90_000);
    // The window starts 15,000 rows into the third file. The first file's footer gives the schema
    // and skips its 30,000 rows; the second is skipped by its manifest's count without being
    // opened; the third is read from the row group the window starts in. The table's row count
    // is the manifests' too, and the metadata is read once for the window and the count.
    let requests = s3.requests();
    assert_eq!(requests[..4], metadata_requests());
    assert!(
        !requests[4..].iter().any(|r| r.contains("/metadata/")),
        "{requests:#?}"
    );
    assert!(
        !requests.iter().any(|r| r.contains("b.parquet")),
        "{requests:#?}"
    );
    let row_groups = row_group_reads(&requests, &sizes);
    assert_eq!(row_groups.len(), 1, "{requests:#?}");
    assert!(row_groups[0].contains("c.parquet"), "{requests:#?}");
}

/// A read that is cancelled stops at its next batch, not at the end of the file it is in: from an
/// object store each row group is a request, so finishing the file would keep downloading for a
/// caller who has gone. The token is cancelled as the first row group is requested; the read
/// takes that row group's first batch and stops, without asking for the file's other two.
#[test]
fn a_cancelled_read_stops_requesting_row_groups() {
    let s3 = FakeS3::start();
    let sizes = put_orders(&s3);
    let token = lakeleto::CancelToken::new();
    let cancel = token.clone();
    let (first, size) = {
        let a = format!("{ORDERS}/data/a.parquet");
        (a.clone(), sizes[&a])
    };
    s3.on_request(move |request| {
        let reads_a_row_group = request.starts_with(&format!("GET {first} bytes="))
            && range_start(request) + 64 * 1024 < size;
        if reads_a_row_group {
            cancel.cancel();
        }
    });
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let ctx = RequestContext::detached().with_cancel(token);

    let Err(err) = engine.preview(&ctx, &orders(), 30_000) else {
        panic!("the read was cancelled");
    };
    assert!(
        matches!(
            err,
            EngineError::Cancelled(lakeleto::CancelReason::Requested)
        ),
        "{err}"
    );
    let requests = s3.requests();
    assert_eq!(row_group_reads(&requests, &sizes).len(), 1, "{requests:#?}");
}

/// A window across a row-group boundary arrives as a batch from each row group, and takes both.
#[test]
fn a_window_across_row_groups_reads_each() {
    let s3 = FakeS3::start();
    let sizes = put_orders(&s3);
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let res = engine
        .scan(
            &RequestContext::detached(),
            &orders(),
            &ScanSpec {
                offset: 9_995,
                limit: 10,
                sort: None,
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    assert_eq!(ids(&res.batch.batches), (9_995..10_005).collect::<Vec<_>>());
    let requests = s3.requests();
    let row_groups = row_group_reads(&requests, &sizes);
    assert_eq!(row_groups.len(), 2, "{requests:#?}");
    assert!(
        row_groups.iter().all(|r| r.contains("a.parquet")),
        "{requests:#?}"
    );
}

/// A prefix with no metadata is refused with where it was looked for.
#[test]
fn a_prefix_with_no_metadata_names_where_it_looked() {
    let s3 = FakeS3::start();
    const EMPTY: &str = "warehouse/db/empty";
    s3.put(&format!("{EMPTY}/data/x.parquet"), parquet(0..10, 10));
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let Err(err) = engine.preview(
        &RequestContext::detached(),
        &Source::with_format(uri(EMPTY), Format::Iceberg),
        10,
    ) else {
        panic!("a prefix with no metadata is not a table");
    };
    assert!(
        err.to_string()
            .contains(&format!("no *.metadata.json under {}/metadata", uri(EMPTY))),
        "{err}"
    );
}

#[test]
fn a_table_with_no_version_hint_is_found_by_listing_its_metadata() {
    let s3 = FakeS3::start();
    const EVENTS: &str = "warehouse/db/events";
    let data = format!("{EVENTS}/data/e.parquet");
    let body = parquet(0..100, 100);
    let manifest = format!("{EVENTS}/metadata/m.avro");
    let list = format!("{EVENTS}/metadata/snap.avro");
    s3.put(
        &manifest,
        avro(
            MANIFEST_SCHEMA,
            vec![entry(0, &uri(&data), 100, body.len())],
        ),
    );
    s3.put(&data, body);
    s3.put(&list, manifest_list(&[(&uri(&manifest), 0)]));
    // `<version>-<uuid>` names, as a catalog writes them, and no hint: the highest version is
    // current, and only listing the directory finds it.
    s3.put(
        &format!("{EVENTS}/metadata/00001-aaaa.metadata.json"),
        metadata(EVENTS, &[]),
    );
    s3.put(
        &format!("{EVENTS}/metadata/00002-bbbb.metadata.json"),
        metadata(EVENTS, &[(7, &uri(&list))]),
    );
    let engine = LocalReaderEngine::default().with_store_options(s3.options());

    let rows = engine
        .preview(
            &RequestContext::detached(),
            &Source::with_format(uri(EVENTS), Format::Iceberg),
            3,
        )
        .unwrap();
    assert_eq!(ids(&rows.batches), [0, 1, 2]);
    let requests = s3.requests();
    assert_eq!(
        requests[..3],
        [
            format!("GET {EVENTS}/metadata/version-hint.text"),
            format!("LIST {EVENTS}/metadata/"),
            format!("GET {EVENTS}/metadata/00002-bbbb.metadata.json"),
        ]
    );
}

#[test]
fn positional_deletes_in_a_store_apply_to_the_files_they_name() {
    let s3 = FakeS3::start();
    const DELETED: &str = "warehouse/db/deleted";
    // The first data file is 20,000 rows in row groups of 5,000, so it is read as four batches,
    // and its deletes fall in more than one of them. The second follows it.
    let data = format!("{DELETED}/data/d.parquet");
    let body = parquet(0..20_000, 5_000);
    let after = format!("{DELETED}/data/e.parquet");
    let after_body = parquet(20_000..20_100, 100);
    let deleted = [0, 1, 2, 5_000, 12_345, 19_999];
    let deletes = {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![uri(&data); deleted.len()])) as ArrayRef,
                Arc::new(Int64Array::from(deleted.to_vec())) as ArrayRef,
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        out
    };
    let delete_file = format!("{DELETED}/data/pos-deletes.parquet");
    let data_manifest = format!("{DELETED}/metadata/data.avro");
    let delete_manifest = format!("{DELETED}/metadata/deletes.avro");
    let list = format!("{DELETED}/metadata/snap.avro");
    s3.put(
        &data_manifest,
        avro(
            MANIFEST_SCHEMA,
            vec![
                entry(0, &uri(&data), 20_000, body.len()),
                entry(0, &uri(&after), 100, after_body.len()),
            ],
        ),
    );
    s3.put(
        &delete_manifest,
        avro(
            MANIFEST_SCHEMA,
            vec![entry(1, &uri(&delete_file), 6, deletes.len())],
        ),
    );
    s3.put(&data, body);
    s3.put(&after, after_body);
    s3.put(&delete_file, deletes);
    s3.put(
        &list,
        manifest_list(&[(&uri(&data_manifest), 0), (&uri(&delete_manifest), 1)]),
    );
    s3.put(
        &format!("{DELETED}/metadata/v1.metadata.json"),
        metadata(DELETED, &[(1, &uri(&list))]),
    );
    s3.put(
        &format!("{DELETED}/metadata/version-hint.text"),
        b"1".to_vec(),
    );
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let source = Source::with_format(uri(DELETED), Format::Iceberg);
    let ctx = RequestContext::detached();

    let rows = engine.preview(&ctx, &source, 5).unwrap();
    assert_eq!(ids(&rows.batches), [3, 4, 5, 6, 7]);
    // A delete file is read whole, since every position in it matters; the data file still by
    // ranges. The window is covered by the first file, so the second is never opened.
    let requests = s3.requests();
    assert!(
        requests.contains(&format!("GET {delete_file}")),
        "{requests:#?}"
    );
    for request in requests.iter().filter(|r| r.contains("d.parquet")) {
        assert!(request.contains(" bytes="), "{request} is not ranged");
    }
    assert!(
        !requests.iter().any(|r| r.contains("e.parquet")),
        "{requests:#?}"
    );

    // Every batch of the first file loses the rows at its own deleted positions.
    let all = ids(&engine.preview(&ctx, &source, 30_000).unwrap().batches);
    let expected: Vec<i64> = (0..20_100).filter(|id| !deleted.contains(id)).collect();
    assert_eq!(all.len(), expected.len());
    assert_eq!(all, expected);

    let schema = engine.schema(&ctx, &source).unwrap();
    assert_eq!(schema.row_count, Some(20_094));
}

/// A table in an object store is data someone else may have written. When the table was copied to
/// disk to be read, a manifest naming a local path made Lakeleto read that path: the planner
/// resolved it on this machine like any local table's. Now such a table is refused, and the file is
/// never opened.
#[test]
fn a_table_in_a_store_cannot_name_a_file_on_this_machine() {
    let local = tempfile::tempdir().unwrap();
    let secret = local.path().join("secret.parquet");
    std::fs::write(&secret, parquet(0..10, 10)).unwrap();

    for named in [
        secret.display().to_string(),
        format!("file://{}", secret.display()),
    ] {
        let s3 = FakeS3::start();
        const SNEAKY: &str = "warehouse/db/sneaky";
        let manifest = format!("{SNEAKY}/metadata/m.avro");
        let list = format!("{SNEAKY}/metadata/snap.avro");
        s3.put(
            &manifest,
            avro(MANIFEST_SCHEMA, vec![entry(0, &named, 10, 0)]),
        );
        s3.put(&list, manifest_list(&[(&uri(&manifest), 0)]));
        s3.put(
            &format!("{SNEAKY}/metadata/v1.metadata.json"),
            metadata(SNEAKY, &[(1, &uri(&list))]),
        );
        s3.put(
            &format!("{SNEAKY}/metadata/version-hint.text"),
            b"1".to_vec(),
        );
        let engine = LocalReaderEngine::default().with_store_options(s3.options());

        let Err(err) = engine.preview(
            &RequestContext::detached(),
            &Source::with_format(uri(SNEAKY), Format::Iceberg),
            10,
        ) else {
            panic!("{named}: a store table naming a local file must be refused");
        };
        assert!(matches!(err, EngineError::Forbidden(_)), "{named}: {err}");
        assert!(
            err.to_string().contains("is not in an object store"),
            "{named}: {err}"
        );
    }
}

/// The plan is read as the identity the call brought, not the process's. Observable with no
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

#[cfg(feature = "sql")]
#[test]
fn sql_reads_a_table_in_a_store_without_copying_it() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::NamedSource;

    let s3 = FakeS3::start();
    let stored: usize = put_orders(&s3).values().sum();
    let engine = DataFusionEngine::with_store_options(s3.options());
    let rows = engine
        .query(
            &RequestContext::detached(),
            "SELECT count(*) AS n, min(id) AS lo, max(id) AS hi FROM t",
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
    assert_eq!((column(0), column(1), column(2)), (90_000, 0, 89_999));
    let requests = s3.requests();
    for never in [
        "v1.metadata.json",
        "snap-1.avro",
        "manifest-1.avro",
        "old.parquet",
        "orphan",
    ] {
        assert!(
            !requests.iter().any(|r| r.contains(never)),
            "{never} is not in the current snapshot: {requests:#?}"
        );
    }
    // The query reads every row of the current snapshot, and nothing else.
    assert!(s3.sent() < stored as u64, "sent {} of {stored}", s3.sent());
}
