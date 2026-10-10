//! Arrow IPC read as a table: the file format (`.arrow`, `.feather`, `.ipc`) by its footer, with
//! its row count and windows that read only their batches, and the stream format (`.arrows`) front
//! to back. What `lakeleto -o arrow|arrows` writes reads back as the rows it wrote, and SQL reads
//! both framings through DataFusion.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use lakeleto::engine::{Engine, ScanSpec, SortSpec};
use lakeleto::{Format, LocalReaderEngine, RequestContext, Source};

/// `batches` batches of `rows` rows: an id counting up from 0 and a name.
fn batches(batches: usize, rows: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    (0..batches)
        .map(|b| {
            let ids: Vec<i64> = (0..rows).map(|r| (b * rows + r) as i64).collect();
            let names: Vec<String> = ids.iter().map(|i| format!("name {i}")).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// `batches` in the IPC file format.
fn file(batches: &[RecordBatch]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut w = FileWriter::try_new(&mut out, &batches[0].schema()).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
    drop(w);
    out
}

/// `batches` in the IPC stream format.
fn stream(batches: &[RecordBatch]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut w = StreamWriter::try_new(&mut out, &batches[0].schema()).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
    drop(w);
    out
}

/// The ids of `batches`, in order.
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn write(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

/// A file reports its row count from its footer, so the grid's scroll knows its end; a stream
/// cannot, and reads as far as a window goes. Windows, sorts and previews read the same rows from
/// either framing, under any of the names the format goes by.
#[test]
fn files_and_streams_read_as_tables_under_every_name() {
    let dir = tempfile::tempdir().unwrap();
    let written = batches(5, 400);
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let window = ScanSpec {
        offset: 790,
        limit: 20,
        ..ScanSpec::default()
    };
    let sorted = ScanSpec {
        limit: 3,
        sort: Some(SortSpec {
            column: "id".to_string(),
            descending: true,
        }),
        ..ScanSpec::default()
    };
    let named = [
        ("t.arrow", file(&written), Some(2000)),
        ("t.feather", file(&written), Some(2000)),
        ("t.ipc", file(&written), Some(2000)),
        ("t.arrows", stream(&written), None),
        // The first bytes say which framing a file has, whatever it is called.
        ("stream-named.arrow", stream(&written), None),
    ];
    for (name, bytes, count) in named {
        let source = Source::detect(write(dir.path(), name, &bytes)).unwrap();
        assert_eq!(source.format, Format::Arrow, "{name}");
        let schema = engine.schema(&ctx, &source).unwrap();
        assert_eq!(schema.format, "arrow");
        assert_eq!(schema.row_count, count, "{name}");
        let columns: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(columns, ["id", "name"], "{name}");

        let got = engine.scan(&ctx, &source, &window).unwrap();
        assert_eq!(
            ids(&got.batch.batches),
            (790..810).collect::<Vec<_>>(),
            "{name}"
        );
        assert_eq!(got.total_known, count.is_some(), "{name}");
        if let Some(count) = count {
            assert_eq!(got.matched_rows as u64, count, "{name}");
        }
        let top = engine.scan(&ctx, &source, &sorted).unwrap();
        assert_eq!(ids(&top.batch.batches), [1999, 1998, 1997], "{name}");
        assert_eq!(engine.preview(&ctx, &source, 5).unwrap().num_rows(), 5);
    }
    // No extension at all: the file format's magic number names it.
    let source = Source::detect(write(dir.path(), "export", &file(&written))).unwrap();
    assert_eq!(source.format, Format::Arrow);
}

/// The built binary, run with `args`, its stdout captured; it must succeed.
fn lakeleto(args: &[&str]) -> Vec<u8> {
    let run = std::process::Command::new(env!("CARGO_BIN_EXE_lakeleto"))
        .args(args)
        .output()
        .expect("lakeleto runs");
    assert!(
        run.status.success(),
        "lakeleto {args:?}: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    run.stdout
}

/// What `-o arrow` and `-o arrows` write, `lakeleto` reads back as the rows it wrote, and
/// `schema` counts a file's rows.
#[test]
fn what_lakeleto_writes_as_arrow_it_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("people.csv");
    std::fs::write(
        &csv,
        "id,name,score\n1,ada,9.5\n2,grace,\n3,linus,7.25\n4,ken,8\n",
    )
    .unwrap();
    let csv = csv.to_str().unwrap();
    let want = lakeleto(&["head", csv, "-o", "csv"]);
    for out in ["back.arrow", "back.arrows"] {
        let path = dir.path().join(out);
        let path = path.to_str().unwrap();
        lakeleto(&["head", csv, "--out", path]);
        assert_eq!(lakeleto(&["head", path, "-o", "csv"]), want, "{out}");
    }
    let schema: serde_json::Value = serde_json::from_slice(&lakeleto(&[
        "schema",
        dir.path().join("back.arrow").to_str().unwrap(),
        "-o",
        "json",
    ]))
    .unwrap();
    assert_eq!(schema["row_count"], 4, "{schema}");
}

/// SQL reads both framings through DataFusion, as the grid reads them.
#[cfg(feature = "sql")]
#[test]
fn sql_reads_arrow_files_and_streams() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;

    let dir = tempfile::tempdir().unwrap();
    let written = batches(4, 250);
    for (name, bytes) in [("t.arrow", file(&written)), ("t.arrows", stream(&written))] {
        let named = [NamedSource {
            name: "t".to_string(),
            source: Source::detect(write(dir.path(), name, &bytes)).unwrap(),
        }];
        let rb = DataFusionEngine::new()
            .query(
                &RequestContext::detached(),
                "SELECT count(*) AS n, sum(id) AS ids FROM t WHERE id % 2 = 0",
                &named,
            )
            .unwrap();
        let row = &rb.batches[0];
        let value = |c: usize| {
            row.column(c)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(
            (value(0), value(1)),
            (500, (0..1000).filter(|i| i % 2 == 0).sum()),
            "{name}"
        );
    }
}

#[cfg(feature = "object-store")]
#[path = "support/fake_s3.rs"]
mod fake_s3;

/// In a store, a file is read where it lies: its layout once per version, then for a window only
/// the batches it covers, by ranged requests.
#[cfg(feature = "object-store")]
#[test]
fn a_file_in_a_store_is_read_by_ranges() {
    let s3 = fake_s3::FakeS3::start();
    let written = batches(10, 50_000);
    let bytes = file(&written);
    s3.put("big.arrow", bytes.clone());
    let engine = LocalReaderEngine::default().with_store_options(s3.options());
    let source = Source::resolve("s3://bucket/big.arrow", None).unwrap();
    let ctx = RequestContext::detached();
    assert_eq!(
        engine.schema(&ctx, &source).unwrap().row_count,
        Some(500_000)
    );

    s3.reset();
    let window = ScanSpec {
        offset: 260_000,
        limit: 10,
        ..ScanSpec::default()
    };
    let got = engine.scan(&ctx, &source, &window).unwrap();
    assert_eq!(
        ids(&got.batch.batches),
        (260_000..260_010).collect::<Vec<_>>()
    );
    assert_eq!(got.matched_rows, 500_000);
    let gets: Vec<_> = s3
        .requests()
        .into_iter()
        .filter(|r| r.starts_with("GET"))
        .collect();
    assert_eq!(gets.len(), 1, "one ranged GET for the one batch: {gets:?}");
    assert!(gets[0].contains("bytes="), "{gets:?}");
    assert!(
        s3.sent() < bytes.len() as u64 / 5,
        "sent {} of {} bytes",
        s3.sent(),
        bytes.len()
    );
}
