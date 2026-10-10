//! A timestamp column whose zone is a name, read through the `lakeleto` binary. `UTC` is what
//! pandas, pyarrow and Polars write for tz-aware UTC data (`timestamp[us, tz=UTC]`), and what the
//! Parquet reader calls any column a file marks as adjusted to UTC. It prints in every build as the
//! release binaries print it. A zone the build cannot print is refused, naming it, where it used to
//! print blank or fail half-way through the output.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};

/// What `head` prints for [`rows`] in `UTC`, as the release binaries print it.
const TABLE: &str = "| id | ts                          | \n\
                     |----|-----------------------------|\n\
                     | 1  | 2024-01-02T03:04:05.123456Z | \n\
                     | 2  | 2024-06-30T23:59:59Z        | \n\
                     | 3  | ·                           | \n\
                     \n3 row(s)\n";

/// What `head -o csv` prints for [`rows`] in `UTC`.
const CSV: &str = "id,ts\n1,2024-01-02T03:04:05.123456Z\n2,2024-06-30T23:59:59Z\n3,\n";

/// `id`, and `ts` in `zone`: 2024-01-02T03:04:05.123456Z, 2024-06-30T23:59:59Z and a null.
fn rows(zone: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone.into())),
            true,
        ),
    ]));
    let ts = TimestampMicrosecondArray::from(vec![
        Some(1_704_164_645_123_456),
        Some(1_719_791_999_000_000),
        None,
    ])
    .with_timezone(zone);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(ts),
        ],
    )
    .unwrap()
}

/// [`rows`] in `zone` as Parquet into `dir`, with its Arrow schema, as pyarrow, pandas and Polars
/// write it, or without one, as DuckDB, Spark and Trino do. Without it the reader takes the zone
/// from the file's own flag, adjusted to UTC, and calls it `UTC` whatever it was.
fn parquet(dir: &Path, zone: &str, arrow_schema: bool) -> PathBuf {
    let batch = rows(zone);
    let path = dir.join(format!("arrow_schema_{arrow_schema}.parquet"));
    let options = ArrowWriterOptions::new().with_skip_arrow_metadata(!arrow_schema);
    let file = std::fs::File::create(&path).unwrap();
    let mut w = ArrowWriter::try_new_with_options(file, batch.schema(), options).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    path
}

/// [`rows`] in `zone` as an Arrow IPC file in `dir`, as `DataFrame.to_feather` writes it.
fn feather(dir: &Path, zone: &str) -> PathBuf {
    let batch = rows(zone);
    let path = dir.join("rows.arrow");
    let file = std::fs::File::create(&path).unwrap();
    let mut w = arrow_ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    path
}

/// The built binary, run with `args`.
fn lakeleto(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lakeleto"))
        .args(args)
        .output()
        .expect("lakeleto runs")
}

/// [`lakeleto`]'s stdout; it must succeed.
fn ok(args: &[&str]) -> String {
    let run = lakeleto(args);
    assert!(
        run.status.success(),
        "lakeleto {args:?}: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    String::from_utf8(run.stdout).unwrap()
}

/// `profile -o json`'s `ts` column, with `--fast` when `fast`.
fn profiled(path: &str, fast: bool) -> serde_json::Value {
    let mut args = vec!["profile", path, "-o", "json"];
    if fast {
        args.push("--fast");
    }
    let profile: serde_json::Value = serde_json::from_str(&ok(&args)).unwrap();
    profile["columns"][1].clone()
}

#[test]
fn utc_prints_in_head_schema_and_profile() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [
        parquet(dir.path(), "UTC", true),
        parquet(dir.path(), "UTC", false),
        feather(dir.path(), "UTC"),
    ];
    for path in &paths {
        let path = path.to_str().unwrap();
        assert_eq!(ok(&["head", path]), TABLE, "{path}");
        assert_eq!(ok(&["head", path, "-o", "csv"]), CSV, "{path}");
        // The schema says what the file says.
        let schema = ok(&["schema", path]);
        assert!(schema.contains(r#"Timestamp(µs, "UTC")"#), "{schema}");
        let ts = profiled(path, false);
        assert_eq!(ts["distinct"], 2, "{ts}");
        assert_eq!(ts["min"], "2024-01-02T03:04:05.123456Z", "{ts}");
        assert_eq!(ts["max"], "2024-06-30T23:59:59Z", "{ts}");
    }
    // A Parquet footer's statistics print as the scanned values do.
    for path in &paths[..2] {
        let ts = profiled(path.to_str().unwrap(), true);
        assert_eq!(ts["min"], "2024-01-02T03:04:05.123456Z", "{ts}");
        assert_eq!(ts["max"], "2024-06-30T23:59:59Z", "{ts}");
    }
}

/// A build with no time zone database refuses a zone it cannot print before it writes a byte,
/// naming the zone and the builds that print it; one with a database (`sql`) prints the zone's
/// own time. Either way, the schema names the zone.
#[test]
fn a_zone_this_build_cannot_print_is_refused_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    for path in [
        parquet(dir.path(), "Europe/Paris", true),
        feather(dir.path(), "Europe/Paris"),
    ] {
        let path = path.to_str().unwrap();
        let schema = ok(&["schema", path]);
        assert!(
            schema.contains(r#"Timestamp(µs, "Europe/Paris")"#),
            "{schema}"
        );
        for output in ["table", "csv", "json"] {
            let run = lakeleto(&["head", path, "-o", output]);
            let (stdout, stderr) = (
                String::from_utf8_lossy(&run.stdout),
                String::from_utf8_lossy(&run.stderr),
            );
            if cfg!(feature = "sql") {
                assert!(run.status.success(), "{stderr}");
                assert!(
                    stdout.contains("2024-01-02T04:04:05.123456+01:00"),
                    "{stdout}"
                );
            } else {
                assert!(!run.status.success(), "{path} -o {output}: {stdout}");
                assert_eq!(stdout, "", "{path} -o {output}");
                assert!(stderr.contains("`Europe/Paris`"), "{stderr}");
                assert!(stderr.contains("--features sql"), "{stderr}");
            }
        }
    }
}
