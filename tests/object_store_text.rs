//! JSON and CSV in an object store, read from a stand-in for S3 on localhost: the grid streams the
//! object — a request per read, read only as far as the read goes, a located JSON records member
//! by its byte range — and SQL streams JSON a pass per query, rather than either fetching it whole
//! for every read. A sorted SQL window over either counts what matches in the passes that sort it,
//! rather than in a download of its own.
//!
//! Run with: `cargo test --features object-store,sql --test object_store_text`.
#![cfg(feature = "object-store")]

use arrow_array::Int64Array;
use lakeleto::engine::Engine;
use lakeleto::{LocalReaderEngine, RequestContext, Source};

#[path = "support/fake_s3.rs"]
mod fake_s3;
use fake_s3::FakeS3;

/// `rows` NDJSON records of about sixty bytes each.
fn records(rows: usize) -> String {
    let pad = "x".repeat(40);
    (0..rows)
        .map(|i| format!("{{\"id\": {i}, \"pad\": \"{pad}\"}}\n"))
        .collect()
}

#[test]
fn the_grid_reads_a_json_object_only_as_far_as_its_window() {
    let s3 = FakeS3::start();
    // Far larger than any socket buffers what a reader leaves unread, so what is sent shows it.
    let body = records(800_000);
    s3.put("big.ndjson", body.clone());
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let source = Source::resolve("s3://bucket/big.ndjson", None).unwrap();
    let ctx = RequestContext::detached();
    let schema = engine.schema(&ctx, &source).unwrap();
    let columns: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(columns, ["id", "pad"]);

    // Its layout and schema known, a window is a HEAD for the version and a GET stopped with it.
    s3.reset();
    let head = engine.preview(&ctx, &source, 10).unwrap();
    assert_eq!(head.num_rows(), 10);
    assert_eq!(s3.requests(), ["HEAD big.ndjson", "GET big.ndjson"]);
    let half = body.len() as u64 / 2;
    assert!(
        s3.sent() < half,
        "sent {} of {} bytes",
        s3.sent(),
        body.len()
    );
}

#[test]
fn a_json_objects_records_are_fetched_by_their_range_once_located() {
    let s3 = FakeS3::start();
    let body = "{\"meta\": {\"n\": 2}, \"data\": [{\"a\": 1}, {\"a\": 2}]}";
    s3.put("member.json", body);
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let source = Source::resolve("s3://bucket/member.json", None).unwrap();
    let ctx = RequestContext::detached();
    assert_eq!(engine.preview(&ctx, &source, 10).unwrap().num_rows(), 2);

    s3.reset();
    assert_eq!(engine.preview(&ctx, &source, 10).unwrap().num_rows(), 2);
    let (first, last) = (body.find('[').unwrap(), body.rfind(']').unwrap());
    assert_eq!(
        s3.requests(),
        [
            "HEAD member.json".to_string(),
            format!("GET member.json bytes={first}-{last}")
        ]
    );
}

/// `rows` CSV records of about fifty bytes each, under a header.
fn csv_records(rows: usize) -> String {
    let pad = "x".repeat(40);
    let mut body = String::from("id,pad\n");
    for i in 0..rows {
        body.push_str(&format!("{i},{pad}\n"));
    }
    body
}

/// A CSV object's schema, a preview and a window past the inference sample are each one `HEAD`
/// and one `GET`, stopped before half the object is sent.
#[test]
fn the_grid_reads_a_csv_object_only_as_far_as_its_window() {
    use lakeleto::engine::ScanSpec;
    let s3 = FakeS3::start();
    let body = csv_records(800_000);
    s3.put("big.csv", body.clone());
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let source = Source::resolve("s3://bucket/big.csv", None).unwrap();
    let ctx = RequestContext::detached();
    let half = body.len() as u64 / 2;
    let one_request = |what: &str| {
        assert_eq!(s3.requests(), ["HEAD big.csv", "GET big.csv"], "{what}");
        assert!(
            s3.sent() < half,
            "{what}: sent {} of {}",
            s3.sent(),
            body.len()
        );
        s3.reset();
    };

    // The schema, inferred from the sample at the start of the object.
    let schema = engine.schema(&ctx, &source).unwrap();
    let columns: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(columns, ["id", "pad"]);
    one_request("schema");

    // A preview, and a window well past the sample: its schema inferred, and its rows read, from
    // the one response.
    assert_eq!(engine.preview(&ctx, &source, 10).unwrap().num_rows(), 10);
    one_request("preview");
    let spec = ScanSpec {
        offset: 100_000,
        limit: 10,
        ..Default::default()
    };
    let window = engine.scan(&ctx, &source, &spec).unwrap().batch;
    let ids = window.batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(ids, 100_000);
    one_request("window");
}

#[cfg(feature = "sql")]
mod sql {
    use super::*;
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::{NamedSource, RowBatch};

    /// The object at `key`, as table `t`.
    fn table(key: &str) -> [NamedSource; 1] {
        [NamedSource {
            name: "t".to_string(),
            source: Source::resolve(format!("s3://bucket/{key}"), None).unwrap(),
        }]
    }

    /// The one integer a `count(*)` answers.
    fn count(rows: &RowBatch) -> i64 {
        rows.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[test]
    fn sql_streams_a_json_object_a_pass_per_query() {
        let s3 = FakeS3::start();
        let body = records(800_000);
        s3.put("big.ndjson", body.clone());
        let engine = DataFusionEngine::with_store_options(s3.options());
        let ctx = RequestContext::detached();
        let tables = table("big.ndjson");
        let n = engine
            .query(&ctx, "SELECT count(*) AS n FROM t", &tables)
            .unwrap();
        assert_eq!(count(&n), 800_000);

        // A query with its rows stops its pass, and the transfer with it.
        s3.reset();
        let few = engine
            .query(&ctx, "SELECT id FROM t LIMIT 5", &tables)
            .unwrap();
        assert_eq!(few.num_rows(), 5);
        let half = body.len() as u64 / 2;
        assert!(
            s3.sent() < half,
            "sent {} of {} bytes",
            s3.sent(),
            body.len()
        );
    }

    /// A sorted grid window counts what matches in the pass that sorts it: for an object, one
    /// download a page rather than a second one for the count. Tied or not, as a streamed table
    /// skips the parallel probe that a tie would follow with a second read.
    #[test]
    fn a_sorted_window_over_a_json_object_counts_its_matches_in_the_same_pass() {
        use lakeleto::engine::{FilterOp, FilterSpec, ScanSpec, SortSpec};
        let s3 = FakeS3::start();
        s3.put("sorted.ndjson", records(10_000));
        let engine = DataFusionEngine::with_store_options(s3.options());
        let source = Source::resolve("s3://bucket/sorted.ndjson", None).unwrap();
        let ctx = RequestContext::detached();
        let window = |column: &str, filters: Vec<FilterSpec>| ScanSpec {
            limit: 10,
            sort: Some(SortSpec {
                column: column.into(),
                descending: true,
            }),
            filters,
            ..Default::default()
        };
        let first_id = |rows: &RowBatch| {
            rows.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        // Its layout and schema learned, so what follows is the window's own requests.
        engine
            .scan(&ctx, &source, &window("id", Vec::new()))
            .unwrap();

        // Every record holds the same `pad`, so a sort by it is all ties, and the object's first
        // record comes first.
        for (column, filters, matched, first) in [
            ("id", Vec::new(), 10_000, 9_999),
            (
                "id",
                vec![FilterSpec {
                    column: "id".into(),
                    op: FilterOp::Lt,
                    value: "250".into(),
                }],
                250,
                249,
            ),
            ("pad", Vec::new(), 10_000, 0),
        ] {
            s3.reset();
            let res = engine
                .scan(&ctx, &source, &window(column, filters))
                .unwrap();
            assert_eq!(res.matched_rows, matched, "sorted by {column}");
            assert_eq!(first_id(&res.batch), first, "sorted by {column}");
            assert_eq!(
                s3.requests(),
                ["HEAD sorted.ndjson", "GET sorted.ndjson"],
                "sorted by {column}"
            );
        }
    }

    /// A sorted grid window over a CSV object is one download of it, tied or not: its rows are
    /// sorted by key and then by their place in the object, and counted by the TopK that keeps the
    /// best of them — not downloaded again for a tie, or for the count.
    #[test]
    fn a_sorted_window_over_a_csv_object_is_one_download_tied_or_not() {
        use lakeleto::engine::{FilterOp, FilterSpec, ScanSpec, SortSpec};
        let s3 = FakeS3::start();
        // `grp` holds seven values, `id` one per row. Under the 10 MiB at which DataFusion splits
        // a file across partitions, so each pass over it is one GET of the object.
        let rows = 20_000;
        let mut body = String::from("id,grp,pad\n");
        for i in 0..rows {
            body.push_str(&format!("{i},{},{}\n", i % 7, "x".repeat(40)));
        }
        s3.put("sorted.csv", body);
        let engine = DataFusionEngine::with_store_options(s3.options());
        let source = Source::resolve("s3://bucket/sorted.csv", None).unwrap();
        let ctx = RequestContext::detached();
        let window = |column: &str, filters: Vec<FilterSpec>| ScanSpec {
            limit: 10,
            sort: Some(SortSpec {
                column: column.into(),
                descending: true,
            }),
            filters,
            ..Default::default()
        };
        let threes = vec![FilterSpec {
            column: "grp".into(),
            op: FilterOp::Eq,
            value: "3".into(),
        }];
        let matching_three = (0..rows).filter(|i| i % 7 == 3).count();
        let first_id = |rows: &RowBatch| {
            rows.batches[0]
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };

        // The first of the tied rows is the earliest in the object.
        for (column, filters, matched, first) in [
            ("id", Vec::new(), rows, 19_999),
            ("grp", Vec::new(), rows, 6),
            ("id", threes, matching_three, 19_995),
        ] {
            s3.reset();
            let res = engine
                .scan(&ctx, &source, &window(column, filters))
                .unwrap();
            assert_eq!(res.matched_rows, matched, "sorted by {column}");
            assert_eq!(first_id(&res.batch), first, "sorted by {column}");
            // Registering the table infers its schema from the object's first rows, in a GET of
            // its own; the window's one pass is the other.
            let gets = s3
                .requests()
                .iter()
                .filter(|r| r.starts_with("GET "))
                .count();
            assert_eq!(gets, 2, "sorted by {column}: {:?}", s3.requests());
        }
    }

    /// A sorted window deep into a CSV object, past the 50,000 rows a one-pass read would keep, is
    /// one download of it: the pass that samples where the window falls keeps the rows it reads
    /// on disk, and the one that keeps the rows around the window reads them back. The count and
    /// the window are a sort of every row's.
    #[test]
    fn a_deep_sorted_window_over_a_csv_object_is_one_download() {
        use lakeleto::engine::{ScanSpec, SortSpec};
        let s3 = FakeS3::start();
        // Under the 10 MiB at which DataFusion splits a file across partitions, so each pass over
        // it is one GET of the object.
        let rows = 60_000;
        let mut body = String::from("id,grp\n");
        for i in 0..rows {
            body.push_str(&format!("{i},{}\n", i % 7));
        }
        s3.put("deep.csv", body);
        let engine = DataFusionEngine::with_store_options(s3.options());
        let source = Source::resolve("s3://bucket/deep.csv", None).unwrap();
        let ctx = RequestContext::detached();
        let mut want: Vec<i64> = (0..rows).collect();
        want.sort_by_key(|&i| (i % 7, i));
        let offset = 55_000;
        let spec = ScanSpec {
            offset,
            limit: 10,
            sort: Some(SortSpec {
                column: "grp".into(),
                descending: false,
            }),
            ..Default::default()
        };
        let res = engine.scan(&ctx, &source, &spec).unwrap();
        assert_eq!(res.matched_rows, rows as usize);
        let ids = res.batch.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, want[offset..offset + 10]);
        // Registering the table infers its schema from the object's first rows, in a GET of its
        // own; the window's first pass is the other.
        let gets = s3
            .requests()
            .iter()
            .filter(|r| r.starts_with("GET "))
            .count();
        assert_eq!(gets, 2, "{:?}", s3.requests());
    }

    /// A sorted window deep into a JSON object, past the 50,000 rows a one-pass read would keep,
    /// is one download of it: the pass that samples where the window falls keeps the rows it reads
    /// on disk, and the one that keeps the rows around the window reads them back. The count and
    /// the window are a sort of every row's.
    #[test]
    fn a_deep_sorted_window_over_a_json_object_is_one_download() {
        use lakeleto::engine::{ScanSpec, SortSpec};
        let s3 = FakeS3::start();
        let rows = 60_000;
        s3.put("deep.ndjson", records(rows));
        let engine = DataFusionEngine::with_store_options(s3.options());
        let source = Source::resolve("s3://bucket/deep.ndjson", None).unwrap();
        let ctx = RequestContext::detached();
        // Its layout and schema learned, so what follows is the window's own requests.
        let first = ScanSpec {
            limit: 1,
            ..Default::default()
        };
        engine.scan(&ctx, &source, &first).unwrap();
        s3.reset();
        // Every record holds the same `pad`, so the window is the records at its offset, in order.
        let offset = 55_000;
        let spec = ScanSpec {
            offset,
            limit: 10,
            sort: Some(SortSpec {
                column: "pad".into(),
                descending: false,
            }),
            ..Default::default()
        };
        let res = engine.scan(&ctx, &source, &spec).unwrap();
        assert_eq!(res.matched_rows, rows);
        let ids = res.batch.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, (55_000..55_010).collect::<Vec<i64>>());
        assert_eq!(s3.requests(), ["HEAD deep.ndjson", "GET deep.ndjson"]);
    }

    #[test]
    fn sql_over_a_json_object_reads_a_value_its_sample_did_not_see() {
        let s3 = FakeS3::start();
        let mut body: String = (0..20_001).map(|i| format!("{{\"v\": {i}}}\n")).collect();
        body.push_str("{\"v\": \"late text\"}\n");
        s3.put("drift.ndjson", body);
        let engine = DataFusionEngine::with_store_options(s3.options());
        let ctx = RequestContext::detached();
        // Planned over the sampled integers this fails, until the object is found to hold text.
        let late = engine
            .query(
                &ctx,
                "SELECT count(*) AS n FROM t WHERE v = 'late text'",
                &table("drift.ndjson"),
            )
            .unwrap();
        assert_eq!(count(&late), 1);
    }
}
