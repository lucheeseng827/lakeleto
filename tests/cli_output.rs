//! `-o parquet|arrow|arrows` and `--out`, driven through the `lakeleto` binary the way a pipeline
//! runs it. What the binary writes must read back, with each format's own reader, as the rows the
//! library returns for the same command, and `--out` must leave the whole file or none of it.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow_array::{RecordBatch, RecordBatchReader};
use lakeleto::engine::Engine;
use lakeleto::{LocalReaderEngine, RequestContext, Source};

/// Rows with a null, floats and text, as a pipeline's CSV has them.
const CSV: &str = "id,name,score\n1,ada,9.5\n2,grace,\n3,linus,7.25\n4,ken,8\n";

/// The binary formats this build writes: Parquet only with the `parquet-out` feature.
fn binary() -> Vec<&'static str> {
    let mut formats = vec!["arrow", "arrows"];
    if cfg!(feature = "parquet-out") {
        formats.push("parquet");
    }
    formats
}

/// [`CSV`] written into `dir` as `people.csv`.
fn fixture(dir: &Path) -> PathBuf {
    let path = dir.join("people.csv");
    std::fs::write(&path, CSV).unwrap();
    path
}

/// Run the built `lakeleto` with `args`, its stdout and stderr captured, so stdout is a pipe.
fn lakeleto(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lakeleto"))
        .args(args)
        .output()
        .expect("lakeleto runs")
}

/// Run `lakeleto`, require success, and return its stdout.
fn ok(args: &[&str]) -> Vec<u8> {
    let run = lakeleto(args);
    assert!(
        run.status.success(),
        "lakeleto {args:?} failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    run.stdout
}

/// A path as a command-line argument.
fn arg(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// `bytes` in `format`, read back with that format's own reader, as one batch.
fn read(bytes: Vec<u8>, format: &str) -> RecordBatch {
    let (schema, batches) = match format {
        "parquet" => {
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                bytes::Bytes::from(bytes),
            )
            .unwrap()
            .build()
            .unwrap();
            (
                reader.schema(),
                reader.collect::<Result<Vec<_>, _>>().unwrap(),
            )
        }
        "arrow" => {
            let reader = arrow_ipc::reader::FileReader::try_new(Cursor::new(bytes), None).unwrap();
            (
                reader.schema(),
                reader.collect::<Result<Vec<_>, _>>().unwrap(),
            )
        }
        "arrows" => {
            let reader =
                arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None).unwrap();
            (
                reader.schema(),
                reader.collect::<Result<Vec<_>, _>>().unwrap(),
            )
        }
        other => panic!("{other} is not a binary format"),
    };
    arrow_select::concat::concat_batches(&schema, &batches).unwrap()
}

/// What the library's reader returns for `head -n rows`: the rows the binary must have written.
fn preview(path: &Path, rows: usize) -> RecordBatch {
    let source = Source::detect(path).unwrap();
    let rb = LocalReaderEngine::default()
        .preview(&RequestContext::detached(), &source, rows)
        .unwrap();
    arrow_select::concat::concat_batches(&rb.schema, &rb.batches).unwrap()
}

/// The same fields and the same columns: the rows, with their types.
fn assert_same_rows(got: &RecordBatch, want: &RecordBatch, what: &str) {
    assert_eq!(
        got.schema().fields(),
        want.schema().fields(),
        "{what}: schema"
    );
    assert_eq!(got.columns(), want.columns(), "{what}: rows");
}

/// `head -o <format> --out <file>` reads back as the library's preview, in each binary format.
#[test]
fn head_writes_each_binary_format_as_the_rows_it_reads() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let want = preview(&csv, 3);
    assert_eq!(want.num_rows(), 3);
    for format in binary() {
        let out = dir.path().join(format!("head.{format}"));
        ok(&[
            "head",
            arg(&csv),
            "-n",
            "3",
            "-o",
            format,
            "--out",
            arg(&out),
        ]);
        let got = read(std::fs::read(&out).unwrap(), format);
        assert_same_rows(&got, &want, format);
    }
}

/// stdout is a pipe here, as in `lakeleto … | python`, so the bytes go through.
#[test]
fn a_binary_format_goes_down_a_pipe_as_it_goes_to_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let want = preview(&csv, 10);
    for format in binary() {
        let got = read(ok(&["head", arg(&csv), "-o", format]), format);
        assert_same_rows(&got, &want, format);
    }
    // `--out -` is stdout too.
    let got = read(
        ok(&["head", arg(&csv), "-o", "arrows", "--out", "-"]),
        "arrows",
    );
    assert_same_rows(&got, &want, "--out -");
}

/// With no `-o`, `--out`'s extension decides the format, and `-o` overrides it.
#[test]
fn the_out_extension_picks_the_format_when_o_is_not_given() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let path = |name: &str| dir.path().join(name);
    let mut named = vec![("x.arrow", &b"ARROW1"[..]), ("x.feather", b"ARROW1")];
    if cfg!(feature = "parquet-out") {
        named.push(("x.parquet", b"PAR1"));
    }
    for (name, magic) in named {
        ok(&["head", arg(&csv), "--out", arg(&path(name))]);
        let bytes = std::fs::read(path(name)).unwrap();
        assert!(
            bytes.starts_with(magic),
            "{name} is not what its extension names"
        );
    }
    let got = read(std::fs::read(path("x.arrow")).unwrap(), "arrow");
    assert_same_rows(&got, &preview(&csv, 10), "x.arrow");

    ok(&["head", arg(&csv), "--out", arg(&path("x.csv"))]);
    let text = std::fs::read_to_string(path("x.csv")).unwrap();
    assert!(text.starts_with("id,name,score\n"), "{text}");
    // `-o` wins over the extension.
    ok(&[
        "head",
        arg(&csv),
        "-o",
        "ndjson",
        "--out",
        arg(&path("y.parquet")),
    ]);
    let text = std::fs::read_to_string(path("y.parquet")).unwrap();
    assert!(text.starts_with("{\"id\":1"), "{text}");
}

/// A run that fails after `--out` is opened leaves the destination as it was, and nothing else.
#[test]
fn a_failed_run_keeps_the_file_that_was_there_and_leaves_no_temporary() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("report.arrow");
    std::fs::write(&out, b"yesterday's report").unwrap();
    let missing = dir.path().join("no-such-table.csv");
    let run = lakeleto(&["head", arg(&missing), "--out", arg(&out)]);
    assert!(!run.status.success(), "reading a missing file must fail");
    assert_eq!(std::fs::read(&out).unwrap(), b"yesterday's report");
    let left: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(left, ["report.arrow"], "a temporary file was left behind");
}

/// A run that succeeds renames its file over the destination, and leaves no temporary file.
#[test]
fn a_successful_run_replaces_the_file_that_was_there() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let out = dir.path().join("report.arrow");
    std::fs::write(&out, b"yesterday's report").unwrap();
    ok(&["head", arg(&csv), "--out", arg(&out)]);
    let got = read(std::fs::read(&out).unwrap(), "arrow");
    assert_same_rows(&got, &preview(&csv, 10), "report.arrow");
    let left: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(left.len(), 2, "people.csv and report.arrow only: {left:?}");
}

/// `--out` with a bare file name, as it is mostly typed, writes in the working directory and
/// leaves nothing else there.
#[test]
fn a_bare_out_name_writes_in_the_working_directory() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let run = Command::new(env!("CARGO_BIN_EXE_lakeleto"))
        .args(["head", arg(&csv), "--out", "sample.arrow"])
        .current_dir(dir.path())
        .output()
        .expect("lakeleto runs");
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let got = read(
        std::fs::read(dir.path().join("sample.arrow")).unwrap(),
        "arrow",
    );
    assert_same_rows(&got, &preview(&csv, 10), "sample.arrow");
    let mut left: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(left, ["people.csv", "sample.arrow"]);
}

/// A binary format on a command that prints a description is refused before `--out` exists.
#[test]
fn a_refused_output_is_refused_before_any_file_is_made() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let out = dir.path().join("schema.parquet");
    let run = lakeleto(&["schema", arg(&csv), "--out", arg(&out)]);
    assert!(!run.status.success());
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(err.contains("writes rows"), "{err}");
    assert!(!out.exists());
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        1,
        "only people.csv"
    );
}

/// Without `parquet-out`, `-o parquet` and a `.parquet` `--out` are refused before anything is
/// read or made, with the feature named.
#[cfg(not(feature = "parquet-out"))]
#[test]
fn a_build_without_parquet_out_refuses_parquet_before_making_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let out = dir.path().join("x.parquet");
    for args in [
        vec!["head", arg(&csv), "--out", arg(&out)],
        vec!["head", arg(&csv), "-o", "parquet"],
    ] {
        let run = lakeleto(&args);
        assert!(!run.status.success(), "{args:?}");
        let err = String::from_utf8_lossy(&run.stderr);
        assert!(err.contains("--features parquet-out"), "{err}");
        assert!(run.stdout.is_empty(), "{args:?} wrote to stdout");
    }
    assert!(!out.exists());
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        1,
        "only people.csv"
    );
}

/// `query` in each binary format, to a file and down a pipe, reads back as the SQL engine's answer.
#[cfg(feature = "sql")]
#[test]
fn query_writes_each_binary_format_as_the_engine_answers() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;

    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let sql = "SELECT name, score * 2 AS doubled FROM t WHERE id > 1 ORDER BY id";
    let named = [NamedSource {
        name: "t".to_string(),
        source: Source::detect(&csv).unwrap(),
    }];
    let rb = DataFusionEngine::new()
        .query(&RequestContext::detached(), sql, &named)
        .unwrap();
    let want = arrow_select::concat::concat_batches(&rb.schema, &rb.batches).unwrap();
    assert_eq!(want.num_rows(), 3);
    for format in binary() {
        let out = dir.path().join(format!("query.{format}"));
        ok(&["query", sql, "--file", arg(&csv), "--out", arg(&out)]);
        let got = read(std::fs::read(&out).unwrap(), format);
        assert_same_rows(&got, &want, format);
        // And down a pipe, as `query` streams it.
        let got = read(
            ok(&["query", sql, "--file", arg(&csv), "-o", format]),
            format,
        );
        assert_same_rows(&got, &want, &format!("{format} on stdout"));
    }
}

/// `lakeleto info` prints what `-o` asks for. It used to print its `name : value` lines whatever
/// `-o` said, so a script that asked for JSON got text it could not parse.
#[test]
fn info_prints_what_o_asks_for() {
    let dir = tempfile::tempdir().unwrap();
    let csv = fixture(dir.path());
    let text = |args: &[&str]| String::from_utf8(ok(args)).unwrap();

    let json: serde_json::Value =
        serde_json::from_str(&text(&["info", arg(&csv), "-o", "json"])).unwrap();
    assert_eq!(json["path"], arg(&csv), "{json}");
    assert_eq!(json["format"], "csv", "{json}");
    assert_eq!(json["engine"], "local", "{json}");
    assert_eq!(json["size_bytes"], CSV.len(), "{json}");
    assert_eq!(json["columns"], 3, "{json}");
    // A CSV file records no row count, and a file is not a catalog table.
    assert!(json["row_count"].is_null(), "{json}");
    assert!(json.get("credentials").is_none(), "{json}");

    let line = text(&["info", arg(&csv), "-o", "ndjson"]);
    assert_eq!(line.lines().count(), 1, "{line}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap(),
        json
    );

    let header = "path,format,engine,size_bytes,row_count,columns,credentials";
    let row = format!("{},csv,local,{},,3,", arg(&csv), CSV.len());
    assert_eq!(
        text(&["info", arg(&csv), "-o", "csv"]),
        format!("{header}\n{row}\n")
    );
    assert_eq!(
        text(&["info", arg(&csv), "-o", "tsv"]),
        format!(
            "{}\n{}\n",
            header.replace(',', "\t"),
            row.replace(',', "\t")
        )
    );

    assert_eq!(
        text(&["info", arg(&csv)]),
        format!(
            "path   : {}\nformat : csv\nengine : local\nsize   : {} B\nrows   : unknown\n\
             columns: 3\n",
            arg(&csv),
            CSV.len()
        )
    );
}
