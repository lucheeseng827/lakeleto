//! Compressed text — `t.csv.gz`, `t.ndjson.zst`, `t.tsv.bz2`, `t.json.xz` — read as what it holds:
//! the same schema, rows and windows as the file uncompressed, from a file or an object in a store,
//! in the grid and in SQL. A read that inflates past `--max-decompressed` stops with an error that
//! names it, and a build without a codec's decoder refuses the file up front, naming the feature.
//!
//! gzip reads in every build; zstd, bzip2 and xz with `--features compression`.

use std::io::Write;

use arrow_array::RecordBatch;
use lakeleto::engine::{Engine, ScanSpec, SortSpec, TableSchema};
use lakeleto::{Codec, LocalReaderEngine, RequestContext, Source};

/// The codecs this build decodes.
fn codecs() -> Vec<Codec> {
    [Codec::Gzip, Codec::Zstd, Codec::Bzip2, Codec::Xz]
        .into_iter()
        .filter(Codec::decodable)
        .collect()
}

/// The extension that names `codec`.
fn extension(codec: Codec) -> &'static str {
    match codec {
        Codec::Gzip => "gz",
        Codec::Zstd => "zst",
        Codec::Bzip2 => "bz2",
        Codec::Xz => "xz",
    }
}

/// `body` compressed with `codec`.
fn compress(codec: Codec, body: &[u8]) -> Vec<u8> {
    match codec {
        Codec::Gzip => {
            let mut w = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            w.write_all(body).unwrap();
            w.finish().unwrap()
        }
        #[cfg(feature = "compression")]
        Codec::Zstd => zstd::encode_all(body, 1).unwrap(),
        #[cfg(feature = "compression")]
        Codec::Bzip2 => {
            let mut w = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
            w.write_all(body).unwrap();
            w.finish().unwrap()
        }
        #[cfg(feature = "compression")]
        Codec::Xz => {
            let mut w = liblzma::write::XzEncoder::new(Vec::new(), 1);
            w.write_all(body).unwrap();
            w.finish().unwrap()
        }
        #[cfg(not(feature = "compression"))]
        other => panic!("this build cannot write {other}"),
    }
}

/// Each text layout, as a file name and a body of `rows` records with an `id` and a `name`: CSV,
/// TSV, NDJSON, a JSON array, and a JSON document whose records are its `data` member.
fn layouts(rows: usize) -> Vec<(&'static str, String)> {
    let csv: String = std::iter::once("id,name\n".to_string())
        .chain((0..rows).map(|i| format!("{i},name {i}\n")))
        .collect();
    let objects: Vec<String> = (0..rows)
        .map(|i| format!("{{\"id\": {i}, \"name\": \"name {i}\"}}"))
        .collect();
    vec![
        ("t.csv", csv.clone()),
        ("t.tsv", csv.replace(',', "\t")),
        ("t.ndjson", objects.join("\n")),
        ("array.json", format!("[{}]", objects.join(","))),
        (
            "member.json",
            format!(
                "{{\"meta\": {{\"rows\": {rows}}}, \"data\": [{}]}}",
                objects.join(",")
            ),
        ),
    ]
}

/// A schema's columns, names and types, and the rest of what a read reports about the file.
fn shape(schema: &TableSchema) -> (Vec<(String, String)>, Option<u64>, Option<String>) {
    let columns = schema
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.data_type.clone()))
        .collect();
    (columns, schema.row_count, schema.records_path.clone())
}

/// The rows of `batches` as one batch.
fn concat(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches[0].schema();
    arrow_select::concat::concat_batches(&schema, batches).unwrap()
}

/// Every layout, compressed with each codec this build decodes, reads as it does plain: its
/// schema, the first rows, a window past the first batch, and a window sorted over the file.
#[test]
fn every_layout_reads_compressed_as_it_does_plain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let deep = ScanSpec {
        offset: 1500,
        limit: 20,
        ..ScanSpec::default()
    };
    let sorted = ScanSpec {
        offset: 0,
        limit: 5,
        sort: Some(SortSpec {
            column: "id".to_string(),
            descending: true,
        }),
        ..ScanSpec::default()
    };
    for (name, body) in layouts(3000) {
        let plain_path = dir.path().join(name);
        std::fs::write(&plain_path, &body).unwrap();
        let plain = Source::detect(&plain_path).unwrap();
        let want_schema = shape(&engine.schema(&ctx, &plain).unwrap());
        let want_head = concat(&engine.preview(&ctx, &plain, 10).unwrap().batches);
        let want_deep = concat(&engine.scan(&ctx, &plain, &deep).unwrap().batch.batches);
        let want_sorted = concat(&engine.scan(&ctx, &plain, &sorted).unwrap().batch.batches);
        assert_eq!(want_deep.num_rows(), 20, "{name}");
        for codec in codecs() {
            let what = format!("{name}.{}", extension(codec));
            let path = dir.path().join(&what);
            std::fs::write(&path, compress(codec, body.as_bytes())).unwrap();
            let source = Source::detect(&path).unwrap();
            assert_eq!(source.codec, Some(codec), "{what}");
            assert_eq!(
                shape(&engine.schema(&ctx, &source).unwrap()),
                want_schema,
                "{what}"
            );
            let head = engine.preview(&ctx, &source, 10).unwrap();
            assert_eq!(concat(&head.batches), want_head, "{what}");
            let window = engine.scan(&ctx, &source, &deep).unwrap();
            assert_eq!(concat(&window.batch.batches), want_deep, "{what}");
            let top = engine.scan(&ctx, &source, &sorted).unwrap();
            assert_eq!(concat(&top.batch.batches), want_sorted, "{what}");
        }
    }
}

/// The file browser lists a compressed file as the format it holds, and hides one that holds
/// nothing it reads: a binary format compresses inside its own container, so none takes a codec.
#[test]
fn the_browser_lists_compressed_files_as_what_they_hold() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "a.csv.gz",
        "b.ndjson.zst",
        "c.json.xz",
        "d.parquet.gz",
        "e.gz",
        "f.arrow.gz",
    ] {
        std::fs::write(dir.path().join(name), b"").unwrap();
    }
    let listing = lakeleto::source::list_dir(&dir.path().to_string_lossy()).unwrap();
    let listed: Vec<(String, Option<String>)> = listing
        .entries
        .into_iter()
        .map(|e| (e.name, e.format))
        .collect();
    assert_eq!(
        listed,
        [
            ("a.csv.gz".to_string(), Some("csv".to_string())),
            ("b.ndjson.zst".to_string(), Some("json".to_string())),
            ("c.json.xz".to_string(), Some("json".to_string())),
        ]
    );
}

/// The built binary, run with `args`, its output captured.
fn lakeleto(args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_lakeleto"))
        .args(args)
        .output()
        .expect("lakeleto runs")
}

/// A read that needs more than `--max-decompressed` bytes stops with an error that names the
/// limit; one that needs fewer, or a bigger limit, reads.
#[test]
fn a_read_past_max_decompressed_stops_with_an_error_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.csv.gz");
    let (_, body) = layouts(50_000).remove(0);
    std::fs::write(&path, compress(Codec::Gzip, body.as_bytes())).unwrap();
    let file = path.to_str().unwrap();

    let run = lakeleto(&["--max-decompressed", "64K", "head", file, "-n", "20000"]);
    assert!(!run.status.success());
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(
        err.contains("too large") && err.contains("--max-decompressed") && err.contains("65536"),
        "{err}"
    );

    // The first rows fit under the same limit, and every row under a bigger one.
    let few = lakeleto(&[
        "--max-decompressed",
        "64K",
        "head",
        file,
        "-n",
        "5",
        "-o",
        "csv",
    ]);
    assert!(
        few.status.success(),
        "{}",
        String::from_utf8_lossy(&few.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&few.stdout).lines().count(), 6);
    let all = lakeleto(&[
        "--max-decompressed",
        "1M",
        "head",
        file,
        "-n",
        "20000",
        "-o",
        "csv",
    ]);
    assert!(
        all.status.success(),
        "{}",
        String::from_utf8_lossy(&all.stderr)
    );

    let bad = lakeleto(&["--max-decompressed", "lots", "head", file]);
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("not a size"));
}

/// A query reads a compressed file under the grid's limit: one that decompresses more than
/// `--max-decompressed` stops with the same error, naming it, CSV and TSV as well as JSON.
#[cfg(feature = "sql")]
#[test]
fn a_query_past_max_decompressed_stops_with_an_error_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    let sql = "SELECT count(*) AS n FROM t";
    for (name, body) in layouts(50_000).into_iter().take(3) {
        let path = dir.path().join(format!("{name}.gz"));
        std::fs::write(&path, compress(Codec::Gzip, body.as_bytes())).unwrap();
        let file = path.to_str().unwrap();

        let over = lakeleto(&["--max-decompressed", "64K", "query", sql, "--file", file]);
        assert!(!over.status.success(), "{name}");
        let err = String::from_utf8_lossy(&over.stderr);
        assert!(
            err.contains("too large")
                && err.contains("--max-decompressed")
                && err.contains("65536"),
            "{name}: {err}"
        );

        let under = lakeleto(&[
            "--max-decompressed",
            "64M",
            "query",
            sql,
            "--file",
            file,
            "-o",
            "csv",
        ]);
        assert!(
            under.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&under.stderr)
        );
        let out = String::from_utf8_lossy(&under.stdout);
        assert_eq!(out.lines().collect::<Vec<_>>(), ["n", "50000"], "{name}");
    }
}

/// A build without `compression` refuses zstd, bzip2 and xz up front, naming the feature, where
/// the grid and SQL would otherwise read compressed bytes as text.
#[cfg(not(feature = "compression"))]
#[test]
fn a_build_without_compression_refuses_its_codecs_naming_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["t.csv.zst", "t.tsv.bz2", "t.json.xz"] {
        let path = dir.path().join(name);
        std::fs::write(&path, b"not read").unwrap();
        let source = Source::detect(&path).unwrap();
        let err = LocalReaderEngine::default()
            .preview(&RequestContext::detached(), &source, 5)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("--features compression"), "{name}: {err}");
    }
}

/// SQL reads compressed CSV and JSON as it reads them plain, through the reader the grid uses: a
/// pass at a time, each held to `--max-decompressed`.
#[cfg(feature = "sql")]
#[test]
fn sql_reads_compressed_text_as_it_reads_it_plain() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;

    let dir = tempfile::tempdir().unwrap();
    let sql = "SELECT count(*) AS n, sum(id) AS ids, max(name) AS last FROM t WHERE id % 3 = 0";
    let query = |path: &std::path::Path| {
        let named = [NamedSource {
            name: "t".to_string(),
            source: Source::detect(path).unwrap(),
        }];
        let rb = DataFusionEngine::new()
            .query(&RequestContext::detached(), sql, &named)
            .unwrap();
        concat(&rb.batches)
    };
    for (name, body) in layouts(3000) {
        let plain = dir.path().join(name);
        std::fs::write(&plain, &body).unwrap();
        let want = query(&plain);
        assert_eq!(want.num_rows(), 1);
        for codec in codecs() {
            let what = format!("{name}.{}", extension(codec));
            let path = dir.path().join(&what);
            std::fs::write(&path, compress(codec, body.as_bytes())).unwrap();
            assert_eq!(query(&path), want, "{what}");
        }
    }
}

#[cfg(feature = "object-store")]
#[path = "support/fake_s3.rs"]
mod fake_s3;

#[cfg(feature = "object-store")]
mod objects {
    use super::fake_s3::FakeS3;
    use super::*;

    /// A compressed object is decompressed as it streams: the first rows are a request stopped
    /// with the read, not a download of the object.
    #[test]
    fn a_compressed_object_is_read_only_as_far_as_its_window() {
        let s3 = FakeS3::start();
        // Gzip that stores its rows rather than compressing them, so the object is as big as they
        // are: far larger than the socket buffers that take in what a reader leaves unread (a few
        // megabytes on loopback, however much the reader took), so what is sent shows how far the
        // read went.
        let body: String = (0..800_000u64)
            .map(|i| {
                format!(
                    "{{\"id\": {i}, \"pad\": \"{:016x}\"}}\n",
                    i.wrapping_mul(0x9e37_79b9_7f4a_7c15)
                )
            })
            .collect();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::none());
        gz.write_all(body.as_bytes()).unwrap();
        let gz = gz.finish().unwrap();
        s3.put("big.ndjson.gz", gz.clone());
        let engine = LocalReaderEngine::default().with_store_options(s3.options());
        let source = Source::resolve("s3://bucket/big.ndjson.gz", None).unwrap();
        let ctx = RequestContext::detached();
        assert_eq!(engine.schema(&ctx, &source).unwrap().columns.len(), 2);

        s3.reset();
        let head = engine.preview(&ctx, &source, 10).unwrap();
        assert_eq!(head.num_rows(), 10);
        assert_eq!(s3.requests(), ["HEAD big.ndjson.gz", "GET big.ndjson.gz"]);
        assert!(
            s3.sent() < gz.len() as u64 / 2,
            "sent {} of {} bytes",
            s3.sent(),
            gz.len()
        );
    }

    /// A compressed JSON document's records member cannot be asked for by its range — the range is
    /// the decompressed bytes' — so each read takes the object from the start.
    #[test]
    fn a_compressed_records_member_is_read_from_the_start() {
        let s3 = FakeS3::start();
        let (_, body) = layouts(100).remove(4);
        s3.put("member.json.gz", compress(Codec::Gzip, body.as_bytes()));
        let engine = LocalReaderEngine::default().with_store_options(s3.options());
        let source = Source::resolve("s3://bucket/member.json.gz", None).unwrap();
        let ctx = RequestContext::detached();
        let schema = engine.schema(&ctx, &source).unwrap();
        assert_eq!(schema.records_path.as_deref(), Some("/data"));
        assert_eq!(engine.preview(&ctx, &source, 500).unwrap().num_rows(), 100);

        s3.reset();
        assert_eq!(engine.preview(&ctx, &source, 500).unwrap().num_rows(), 100);
        assert_eq!(s3.requests(), ["HEAD member.json.gz", "GET member.json.gz"]);
    }

    /// SQL reads a compressed object in a store as it reads the file: CSV through DataFusion, JSON
    /// through the grid's reader, each decompressed as it streams.
    #[cfg(feature = "sql")]
    #[test]
    fn sql_reads_compressed_objects() {
        use lakeleto::engine::sql::DataFusionEngine;
        use lakeleto::engine::NamedSource;

        let s3 = FakeS3::start();
        let layouts = layouts(2000);
        for (name, body) in [&layouts[0], &layouts[2]] {
            s3.put(
                &format!("{name}.gz"),
                compress(Codec::Gzip, body.as_bytes()),
            );
        }
        let engine = DataFusionEngine::with_store_options(s3.options());
        for name in ["t.csv.gz", "t.ndjson.gz"] {
            let named = [NamedSource {
                name: "t".to_string(),
                source: Source::resolve(format!("s3://bucket/{name}"), None).unwrap(),
            }];
            let rb = engine
                .query(
                    &RequestContext::detached(),
                    "SELECT count(*) AS n, sum(id) AS ids FROM t",
                    &named,
                )
                .unwrap();
            let got = concat(&rb.batches);
            let n = got
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0);
            let ids = got
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!((n, ids), (2000, (0..2000).sum::<i64>()), "{name}");
        }
    }
}
