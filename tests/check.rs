//! `lakeleto check`, driven through the binary the way a CI job runs it: a baseline written, a
//! table checked against it, each gate failing at its limit and not before, and the exit code and
//! the report — text, JSON or JUnit — a pipeline reads.

use std::path::{Path, PathBuf};
use std::process::Command;

fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

fn lakeleto(args: &[&str]) -> std::process::Output {
    // A server named in the shell would take every read; these read their own fixtures.
    Command::new(env!("CARGO_BIN_EXE_lakeleto"))
        .env_remove("LAKELETO_REMOTE_URL")
        .args(args)
        .output()
        .expect("lakeleto runs")
}

fn arg(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// Run `lakeleto`, and return its exit code, stdout and stderr.
fn run(args: &[&str]) -> (i32, String, String) {
    let out = lakeleto(args);
    (
        out.status.code().expect("an exit code"),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn a_check_needs_a_baseline_or_an_age_and_update_needs_a_baseline() {
    let (code, _, stderr) = run(&["check", "t.csv"]);
    assert_eq!(code, 2, "a usage error: {stderr}");
    let (code, _, stderr) = run(&["check", "t.csv", "--max-age", "1d", "--update"]);
    assert_eq!(code, 2, "a usage error: {stderr}");
    for bad in [
        ["--max-age", "24"],
        ["--max-file-jump", "0.5"],
        ["--max-row-drop", "101"],
    ] {
        let (code, _, stderr) = run(&["check", "t.csv", "--baseline", "b.json", bad[0], bad[1]]);
        assert_eq!(code, 2, "{bad:?}: {stderr}");
    }
}

#[test]
fn a_contract_through_a_server_is_refused_before_anything_is_read() {
    // Port 9 on loopback: nothing listens, so a refusal that names the reason was made before any
    // request — a contract would stream every row, and a server answers with only some.
    let (code, _, stderr) = run(&[
        "--remote-url",
        "http://127.0.0.1:9",
        "check",
        "a.parquet",
        "--contract",
        "c.yaml",
    ]);
    assert_eq!(code, 1);
    assert!(stderr.contains("where the table is"), "{stderr}");
    assert!(stderr.contains("caps the rows"), "{stderr}");
}

#[test]
fn check_has_three_forms_and_junit_is_only_checks() {
    let dir = tempfile::tempdir().unwrap();
    let t = write(dir.path(), "t.csv", "id\n1\n");
    for refused in ["csv", "markdown", "ndjson"] {
        let (code, _, stderr) = run(&["check", arg(&t), "--max-age", "1d", "-o", refused]);
        assert_eq!(code, 1, "-o {refused}");
        assert!(stderr.contains("`check` prints a report"), "{stderr}");
    }
    let (code, _, stderr) = run(&["schema", arg(&t), "-o", "junit"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("no JUnit form"), "{stderr}");
    // `.xml` asks for JUnit of `check` alone: any other command writes its text there, as before.
    let out = dir.path().join("schema.xml");
    let (code, _, stderr) = run(&["schema", arg(&t), "--out", arg(&out)]);
    assert_eq!(code, 0, "{stderr}");
    assert!(std::fs::read_to_string(&out)
        .unwrap()
        .starts_with("source : "));
}

/// Orders as one Parquet file: an id, a name with a null in it, and a list of tags — a column
/// whose nulls a Parquet footer does not record at the top level.
fn orders_parquet(path: &Path) {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use std::sync::Arc;
    let mut tags = ListBuilder::new(StringBuilder::new());
    for row in [&["a"][..], &["b", "c"], &[]] {
        for t in row {
            tags.values().append_value(t);
        }
        tags.append(true);
    }
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "name",
            Arc::new(StringArray::from(vec![Some("ada"), None, Some("linus")])) as ArrayRef,
        ),
        ("tags", Arc::new(tags.finish()) as ArrayRef),
    ])
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn nulls_of(report: &serde_json::Value, column: &str) -> serde_json::Value {
    report["measured"]["nulls"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == column)
        .unwrap_or_else(|| panic!("no {column}: {report}"))["nulls"]
        .clone()
}

/// `--fast` counts a Parquet file from its footer: the rows and each column's nulls the footer
/// records, and no row read. A column whose nulls it did not record is not counted — never
/// counted as none — so a nulls gate over it cannot pass.
#[test]
fn a_parquet_file_is_counted_from_its_footer_and_what_it_did_not_record_is_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("orders.parquet");
    orders_parquet(&table);
    let (code, stdout, stderr) = run(&[
        "check",
        arg(&table),
        "--fast",
        "--max-age",
        "1d",
        "-o",
        "json",
    ]);
    assert_eq!(code, 0, "{stderr}");
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["measured"]["method"], "footer");
    assert_eq!(report["measured"]["rows"], 3);
    assert_eq!(nulls_of(&report, "id"), 0);
    assert_eq!(nulls_of(&report, "name"), 1);
    assert_eq!(nulls_of(&report, "tags"), serde_json::Value::Null);

    let baseline = dir.path().join("orders.check.json");
    let (code, _, stderr) = run(&[
        "check",
        arg(&table),
        "--fast",
        "--baseline",
        arg(&baseline),
        "--update",
    ]);
    assert_eq!(code, 0, "{stderr}");
    let (code, stdout, _) = run(&["check", arg(&table), "--fast", "--baseline", arg(&baseline)]);
    assert_eq!(code, 1, "a gate that cannot be told is no pass: {stdout}");
    assert!(
        stdout.contains("UNKNOWN  nulls      1 column not counted"),
        "{stdout}"
    );
    assert!(
        stdout.contains("tags: its nulls are not counted — a Parquet footer"),
        "{stdout}"
    );
    assert!(stdout.contains("counted from its footer by"), "{stdout}");

    // A footer is a single Parquet file's: anything else is refused, never sampled.
    let csv = write(dir.path(), "t.csv", "id\n1\n");
    let (code, _, stderr) = run(&["check", arg(&csv), "--fast", "--max-age", "1d"]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("`--fast` counts from a single Parquet file's footer"),
        "{stderr}"
    );
}

/// The footer and every row read by SQL count a file alike: what licenses comparing a baseline
/// counted one way with a check counted the other.
/// A reader whose listing of a table's files fails, as a store's `HEAD` can when it times out;
/// everything else is the local reader's.
struct Unlisted(lakeleto::LocalReaderEngine);

impl lakeleto::Engine for Unlisted {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn capabilities(&self) -> lakeleto::Capabilities {
        self.0.capabilities()
    }
    fn schema(
        &self,
        ctx: &lakeleto::RequestContext,
        source: &lakeleto::Source,
    ) -> lakeleto::Result<lakeleto::TableSchema> {
        self.0.schema(ctx, source)
    }
    fn preview(
        &self,
        ctx: &lakeleto::RequestContext,
        source: &lakeleto::Source,
        limit: usize,
    ) -> lakeleto::Result<lakeleto::RowBatch> {
        self.0.preview(ctx, source, limit)
    }
    fn profile(
        &self,
        ctx: &lakeleto::RequestContext,
        source: &lakeleto::Source,
        scan_limit: usize,
    ) -> lakeleto::Result<lakeleto::TableProfile> {
        self.0.profile(ctx, source, scan_limit)
    }
    fn files(
        &self,
        _ctx: &lakeleto::RequestContext,
        _source: &lakeleto::Source,
    ) -> lakeleto::Result<lakeleto::engine::TableFiles> {
        Err(lakeleto::EngineError::Other(
            "HEAD timed out after 10s".to_string(),
        ))
    }
}

/// A listing that fails keeps the rows already counted and says why the files are missing, and
/// the measurement says it was not listed: `--update` writes no baseline from it, which would
/// leave the files gate nothing to compare from then on.
#[test]
fn a_listing_that_fails_keeps_the_count_and_is_no_baseline() {
    use lakeleto::check::{measure, Counting};
    use lakeleto::{EngineRegistry, LocalReaderEngine, RequestContext, Source};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("orders.parquet");
    orders_parquet(&table);
    let source = Source::resolve(&table, None).unwrap();
    let ctx = RequestContext::detached();

    let unlisted = EngineRegistry::new(Arc::new(Unlisted(LocalReaderEngine::default())));
    let seen = measure(&ctx, &unlisted, &source, Counting::Footer, 0).unwrap();
    assert_eq!(seen.fingerprint.rows, 3, "the rows are still counted");
    assert!(!seen.listed);
    assert_eq!(
        (
            seen.fingerprint.files,
            seen.fingerprint.bytes,
            seen.fingerprint.updated_ms
        ),
        (None, None, None)
    );
    assert_eq!(seen.notes.len(), 1, "{:?}", seen.notes);
    assert!(
        seen.notes[0].contains("could not be listed") && seen.notes[0].contains("HEAD timed out"),
        "{}",
        seen.notes[0]
    );

    let listed = EngineRegistry::new(Arc::new(LocalReaderEngine::default()));
    let seen = measure(&ctx, &listed, &source, Counting::Footer, 0).unwrap();
    assert!(seen.listed && seen.notes.is_empty(), "{:?}", seen.notes);
    assert_eq!(seen.fingerprint.files, Some(1));
}

#[cfg(feature = "sql")]
#[test]
fn a_footer_and_sql_count_a_parquet_file_alike() {
    let dir = tempfile::tempdir().unwrap();
    let table = dir.path().join("orders.parquet");
    orders_parquet(&table);
    let count = |extra: &[&str]| {
        let mut args = vec!["check", arg(&table), "--max-age", "1d", "-o", "json"];
        args.extend_from_slice(extra);
        let (code, stdout, stderr) = run(&args);
        assert_eq!(code, 0, "{stderr}");
        serde_json::from_str::<serde_json::Value>(&stdout).unwrap()
    };
    let (footer, sql) = (count(&["--fast"]), count(&[]));
    assert_eq!(sql["measured"]["method"], "exact");
    assert_eq!(footer["measured"]["rows"], sql["measured"]["rows"]);
    for column in ["id", "name"] {
        assert_eq!(
            nulls_of(&footer, column),
            nulls_of(&sql, column),
            "{column}"
        );
    }
    assert_eq!(
        nulls_of(&sql, "tags"),
        0,
        "SQL counts what the footer does not"
    );
}

#[cfg(not(feature = "sql"))]
#[test]
fn without_sql_a_check_is_refused_naming_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    let t = write(dir.path(), "t.csv", "id\n1\n");
    let (code, _, stderr) = run(&["check", arg(&t), "--max-age", "1d"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("`sql` feature"), "{stderr}");
}

#[cfg(feature = "sql")]
mod with_sql {
    use super::*;

    /// The reference table: an id, a name with a null, a city.
    const BEFORE: &str = "id,name,city\n1,ada,Oslo\n2,,Lima\n3,linus,Oslo\n4,ken,Pune\n";

    fn json(args: &[&str]) -> (i32, serde_json::Value) {
        let mut args = args.to_vec();
        args.extend_from_slice(&["-o", "json"]);
        let (code, stdout, stderr) = run(&args);
        let report =
            serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("{e}: {stdout}{stderr}"));
        (code, report)
    }

    fn gate<'a>(report: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
        report["gates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["name"] == name)
            .unwrap_or_else(|| panic!("no gate {name}: {report}"))
    }

    /// A baseline of `text` as a CSV, written by a check with `--update`.
    fn baseline_of(dir: &Path, text: &str) -> (PathBuf, PathBuf) {
        let table = write(dir, "t.csv", text);
        let baseline = dir.join("t.check.json");
        let (code, _, stderr) = run(&[
            "check",
            arg(&table),
            "--baseline",
            arg(&baseline),
            "--update",
        ]);
        assert_eq!(code, 0, "{stderr}");
        (table, baseline)
    }

    fn check(table: &Path, baseline: &Path, extra: &[&str]) -> (i32, serde_json::Value) {
        let mut args = vec!["check", arg(table), "--baseline", arg(baseline)];
        args.extend_from_slice(extra);
        json(&args)
    }

    #[test]
    fn a_first_check_writes_the_baseline_and_the_next_compares_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path(), "t.csv", BEFORE);
        let baseline = dir.path().join("t.check.json");
        // No baseline, and no `--update` to write one: an error, never a pass.
        let (code, _, stderr) = run(&["check", arg(&table), "--baseline", arg(&baseline)]);
        assert_eq!(code, 1);
        assert!(
            stderr.contains("run the check once with `--update`"),
            "{stderr}"
        );

        let (code, report) = json(&[
            "check",
            arg(&table),
            "--baseline",
            arg(&baseline),
            "--update",
        ]);
        assert_eq!(code, 0, "{report}");
        assert_eq!(report["baseline_written"], true);
        assert_eq!(gate(&report, "schema")["status"], "skipped");
        let written = std::fs::read_to_string(&baseline).unwrap();
        let fingerprint: serde_json::Value = serde_json::from_str(&written).unwrap();
        assert_eq!(fingerprint["version"], 1);
        assert_eq!(fingerprint["rows"], 4);
        assert_eq!(fingerprint["files"], 1);
        assert_eq!(fingerprint["method"], "exact");
        let name_nulls = fingerprint["nulls"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "name")
            .unwrap()["nulls"]
            .clone();
        assert_eq!(name_nulls, 1);
        // What the table holds stays out of a file that is meant to be committed.
        for value in ["Oslo", "ada", "linus"] {
            assert!(!written.contains(value), "{value} in {written}");
        }

        let (code, stdout, stderr) = run(&["check", arg(&table), "--baseline", arg(&baseline)]);
        assert_eq!(code, 0, "{stderr}");
        for line in [
            "PASS     schema",
            "PASS     row-count",
            "PASS     nulls",
            "PASS     files",
        ] {
            assert!(stdout.contains(line), "{line}: {stdout}");
        }
        assert!(stdout.contains("SKIPPED  freshness"), "{stdout}");
        assert!(stdout.contains("result    PASS"), "{stdout}");
    }

    #[test]
    fn a_column_removed_retyped_or_tightened_fails_and_one_added_passes() {
        let dir = tempfile::tempdir().unwrap();
        let (table, baseline) = baseline_of(dir.path(), BEFORE);

        write(
            dir.path(),
            "t.csv",
            "id,name,city,team\n1,ada,Oslo,a\n2,,Lima,b\n3,linus,Oslo,a\n4,ken,Pune,b\n",
        );
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 0, "a column added is a table that grew: {report}");
        assert_eq!(
            gate(&report, "schema")["reason"],
            "no column removed or changed"
        );

        write(dir.path(), "t.csv", "id,name\n1,ada\n2,\n3,linus\n4,ken\n");
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 3, "{report}");
        let schema = gate(&report, "schema");
        assert_eq!(schema["status"], "fail");
        assert!(
            schema["details"][0]
                .as_str()
                .unwrap()
                .starts_with("removed  city"),
            "{schema}"
        );

        write(
            dir.path(),
            "t.csv",
            "id,name,city\n1,ada,1\n2,,2\n3,linus,3\n4,ken,4\n",
        );
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 3, "{report}");
        assert!(gate(&report, "schema")["details"][0]
            .as_str()
            .unwrap()
            .contains("changed  city  Utf8 → Int64"));
    }

    #[test]
    fn rows_may_drop_by_the_share_allowed_and_no_more() {
        let rows = |n: usize| {
            let mut t = "id\n".to_string();
            for i in 0..n {
                t.push_str(&format!("{i}\n"));
            }
            t
        };
        let dir = tempfile::tempdir().unwrap();
        let (table, baseline) = baseline_of(dir.path(), &rows(10));
        write(dir.path(), "t.csv", &rows(9));
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 0, "exactly 10% is allowed: {report}");
        write(dir.path(), "t.csv", &rows(8));
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 3, "{report}");
        let gate = gate(&report, "row-count");
        assert_eq!(gate["status"], "fail");
        assert_eq!(
            gate["reason"],
            "10 → 8 rows: 2 fewer (20.00%), more than the 10% allowed"
        );
        let (code, _) = check(&table, &baseline, &["--max-row-drop", "20"]);
        assert_eq!(code, 0);
    }

    #[test]
    fn a_share_of_nulls_rising_past_the_points_allowed_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (table, baseline) = baseline_of(
            dir.path(),
            "id,v\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n7,g\n8,h\n9,i\n10,j\n",
        );
        // One null in ten: 0% → 10%, past the 5 points allowed.
        write(
            dir.path(),
            "t.csv",
            "id,v\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n7,g\n8,h\n9,i\n10,\n",
        );
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 3, "{report}");
        let nulls = gate(&report, "nulls");
        assert_eq!(nulls["status"], "fail");
        assert_eq!(nulls["details"][0], "v: 0.00% → 10.00% null");
        let (code, _) = check(&table, &baseline, &["--max-null-rise", "10"]);
        assert_eq!(code, 0, "exactly 10 points is allowed");
    }

    /// `batch` written to `path` as a Parquet file, uncompressed, so every build reads it.
    fn parquet(path: &Path, ids: &[i64]) {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch};
        use std::sync::Arc;
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
        )])
        .unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn a_parquet_directory_counts_its_files_and_a_jump_in_them_fails() {
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("orders");
        parquet(&table.join("part-0.parquet"), &[1, 2]);
        std::fs::write(table.join("_SUCCESS"), "").unwrap();
        let baseline = dir.path().join("orders.check.json");
        let (code, report) = json(&[
            "check",
            arg(&table),
            "--baseline",
            arg(&baseline),
            "--update",
        ]);
        assert_eq!(code, 0, "{report}");
        assert_eq!(
            report["measured"]["files"], 1,
            "a marker file is no data file"
        );

        parquet(&table.join("part-1.parquet"), &[3, 4]);
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 0, "twice the files is within 2×: {report}");
        parquet(&table.join("part-2.parquet"), &[5, 6]);
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 3, "{report}");
        let files = gate(&report, "files");
        assert_eq!(files["status"], "fail");
        assert_eq!(files["details"][0], "files  1 → 3");
        let (code, _) = check(&table, &baseline, &["--max-file-jump", "3"]);
        assert_eq!(code, 0);
    }

    #[test]
    fn a_file_changed_longer_ago_than_max_age_fails_with_no_baseline_needed() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path(), "t.csv", BEFORE);
        let two_days = std::time::Duration::from_secs(2 * 86_400);
        std::fs::File::options()
            .write(true)
            .open(&table)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - two_days)
            .unwrap();
        let (code, stdout, stderr) = run(&["check", arg(&table), "--max-age", "1d"]);
        assert_eq!(code, 3, "{stderr}");
        assert!(
            stdout.contains("FAIL     freshness  last changed 2d ago, more than the 1d allowed"),
            "{stdout}"
        );
        assert!(
            stdout.contains("SKIPPED  schema     no `--baseline`"),
            "{stdout}"
        );
        let (code, _, stderr) = run(&["check", arg(&table), "--max-age", "3d"]);
        assert_eq!(code, 0, "{stderr}");
    }

    #[test]
    fn a_failing_check_leaves_the_baseline_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let (table, baseline) = baseline_of(dir.path(), BEFORE);
        let before = std::fs::read(&baseline).unwrap();
        write(dir.path(), "t.csv", "id\n1\n");
        let (code, report) = check(&table, &baseline, &["--update"]);
        assert_eq!(code, 3, "{report}");
        assert_eq!(report["baseline_written"], false);
        assert_eq!(std::fs::read(&baseline).unwrap(), before);
        // No stray file beside it either.
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names.len(), 2, "{names:?}");
    }

    /// `--update` makes the directories a baseline goes in; a baseline it cannot write is said in
    /// the report, which is still printed, and the check errs rather than passing.
    #[test]
    fn a_baseline_goes_where_it_is_asked_and_one_that_cannot_be_written_keeps_the_report() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path(), "t.csv", BEFORE);
        let nested = dir.path().join("checks").join("daily").join("t.json");
        let (code, _, stderr) =
            run(&["check", arg(&table), "--baseline", arg(&nested), "--update"]);
        assert_eq!(code, 0, "{stderr}");
        assert!(nested.is_file());

        // Where nobody can make a file, root included — and where reading one finds none, so
        // the check runs to the end before it tries.
        if cfg!(target_os = "linux") {
            let unwritable = format!("/proc/lakeleto-check-{}.json", std::process::id());
            let (code, stdout, _) =
                run(&["check", arg(&table), "--baseline", &unwritable, "--update"]);
            assert_eq!(code, 1, "{stdout}");
            assert!(
                stdout.contains("result    PASS"),
                "the report is still printed: {stdout}"
            );
            assert!(
                stdout.contains("note      the baseline could not be written"),
                "{stdout}"
            );
        }
    }

    #[test]
    fn a_passing_update_rolls_the_baseline_forward() {
        let dir = tempfile::tempdir().unwrap();
        let (table, baseline) = baseline_of(dir.path(), "id\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
        write(dir.path(), "t.csv", "id\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
        let (code, report) = check(&table, &baseline, &["--update"]);
        assert_eq!(code, 0, "{report}");
        assert_eq!(report["baseline_written"], true);
        // Each run compares with the last that passed: 9 → 8 is within 10% of 9? No: 11.11%.
        write(dir.path(), "t.csv", "id\n1\n2\n3\n4\n5\n6\n7\n8\n");
        let (code, report) = check(&table, &baseline, &[]);
        assert_eq!(code, 3, "{report}");
        assert_eq!(
            gate(&report, "row-count")["reason"],
            "9 → 8 rows: 1 fewer (11.11%), more than the 10% allowed"
        );
    }

    #[test]
    fn a_baseline_that_is_not_one_this_lakeleto_reads_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path(), "t.csv", BEFORE);
        let baseline = write(dir.path(), "b.json", r#"{"version": 2, "rows": 4}"#);
        let (code, _, stderr) = run(&["check", arg(&table), "--baseline", arg(&baseline)]);
        assert_eq!(code, 1);
        assert!(
            stderr.contains("of version 2, and this lakeleto reads version 1"),
            "{stderr}"
        );
        // Even with `--update`: a baseline that does not read is never written over.
        let (code, _, _) = run(&[
            "check",
            arg(&table),
            "--baseline",
            arg(&baseline),
            "--update",
        ]);
        assert_eq!(code, 1);
        assert_eq!(
            std::fs::read_to_string(&baseline).unwrap(),
            r#"{"version": 2, "rows": 4}"#
        );
        write(dir.path(), "b.json", "not json");
        let (code, _, stderr) = run(&["check", arg(&table), "--baseline", arg(&baseline)]);
        assert_eq!(code, 1);
        assert!(
            stderr.contains("is not a baseline `check` wrote"),
            "{stderr}"
        );
    }

    /// JUnit is read by CI parsers that refuse a control character outright: whatever the table
    /// supplies is escaped, and the test cases are the gates.
    #[test]
    fn junit_names_each_gate_and_carries_nothing_a_parser_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let (table, baseline) = baseline_of(dir.path(), "id,\"a<b&\u{1b}[2J\"\n1,x\n");
        write(dir.path(), "t.csv", "id\n1\n");
        let out = dir.path().join("report.xml");
        // A file a fifth the size is past 2×: allowed here, so the one failure is the schema's.
        let (code, _, stderr) = run(&[
            "check",
            arg(&table),
            "--baseline",
            arg(&baseline),
            "--max-file-jump",
            "10",
            "--out",
            arg(&out),
        ]);
        assert_eq!(code, 3, "{stderr}");
        let xml = std::fs::read_to_string(&out).unwrap();
        assert!(
            xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"),
            "`.xml` is JUnit: {xml}"
        );
        assert!(
            xml.contains("tests=\"5\" failures=\"1\" errors=\"0\" skipped=\"1\""),
            "{xml}"
        );
        for name in ["schema", "row-count", "nulls", "files", "freshness"] {
            assert!(xml.contains(&format!("name=\"{name}\"")), "{name}: {xml}");
        }
        assert!(
            xml.contains("<failure message=\"1 column removed, 0 columns changed\">"),
            "{xml}"
        );
        assert!(xml.contains("removed  a&lt;b&amp;\\x1b[2J  Utf8"), "{xml}");
        assert!(!xml.contains('\u{1b}'), "{xml:?}");
    }

    /// A Delta table changed when its log says, whatever its files' modification times — which a
    /// copy or a checkout resets — say.
    #[cfg(feature = "delta")]
    #[test]
    fn a_delta_tables_age_and_files_come_from_its_log() {
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("events");
        let log = table.join("_delta_log");
        std::fs::create_dir_all(&log).unwrap();
        parquet(&table.join("part-0.parquet"), &[1, 2, 3]);
        let size = std::fs::metadata(table.join("part-0.parquet"))
            .unwrap()
            .len();
        let ten_days_ago = lakeleto::check::now_ms() - 10 * 86_400_000;
        let schema = serde_json::json!({
            "type": "struct",
            "fields": [{"name": "id", "type": "long", "nullable": true, "metadata": {}}]
        })
        .to_string();
        let commit = [
            serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            serde_json::json!({"metaData": {
                "id": "t", "format": {"provider": "parquet", "options": {}},
                "schemaString": schema, "partitionColumns": [], "configuration": {}
            }}),
            serde_json::json!({"add": {
                "path": "part-0.parquet", "partitionValues": {}, "size": size,
                "modificationTime": ten_days_ago - 1000, "dataChange": true
            }}),
            serde_json::json!({"commitInfo": {"timestamp": ten_days_ago, "operation": "WRITE"}}),
        ]
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        std::fs::write(log.join("00000000000000000000.json"), commit).unwrap();

        let (code, report) = json(&["check", arg(&table), "--max-age", "1d"]);
        assert_eq!(
            code, 3,
            "the files are new, and the table ten days old: {report}"
        );
        assert_eq!(report["measured"]["updated_ms"], ten_days_ago);
        assert_eq!(report["measured"]["files"], 1);
        assert_eq!(report["measured"]["bytes"], size);
        assert_eq!(report["measured"]["rows"], 3);
        let (code, report) = json(&["check", arg(&table), "--max-age", "30d"]);
        assert_eq!(code, 0, "{report}");
    }
}

#[cfg(not(feature = "contracts"))]
#[test]
fn without_the_validator_a_contract_is_refused_naming_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    let t = write(dir.path(), "t.csv", "id\n1\n");
    let c = write(dir.path(), "c.yaml", "covenant: 1\n");
    let (code, _, stderr) = run(&["check", arg(&t), "--contract", arg(&c)]);
    assert_eq!(code, 1);
    assert!(stderr.contains("`contracts` feature"), "{stderr}");
}

#[cfg(feature = "contracts")]
mod contracts {
    use super::*;

    /// Orders: a unique id, an amount of at least 0, and a currency of two.
    const CONTRACT: &str =
        "covenant: 1\nid: orders\nversion: 1.0.0\nmodels:\n  orders:\n    fields:\n      \
        id:\n        type: integer\n        required: true\n        unique: true\n      \
        amount:\n        type: float\n        required: true\n        min: 0\n      \
        currency:\n        type: string\n        required: true\n        allowed: [USD, EUR]\n";

    /// An ODCS contract with one rule the validator cannot enforce: a SQL quality check.
    const ODCS_PARTIAL: &str = "apiVersion: v3.2.0\nkind: DataContract\nid: orders\nversion: 1.0.0\n\
        schema:\n  - name: orders\n    properties:\n      - name: id\n        logicalType: integer\n        \
        required: true\n\n    quality:\n      - type: sql\n        query: SELECT 1\n        mustBe: 0\n";

    fn json(args: &[&str]) -> (i32, serde_json::Value, String) {
        let mut args = args.to_vec();
        args.extend_from_slice(&["-o", "json"]);
        let (code, stdout, stderr) = run(&args);
        let report =
            serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("{e}: {stdout}{stderr}"));
        (code, report, stderr)
    }

    #[test]
    fn rows_that_keep_the_contract_pass_and_rows_that_break_it_fail_without_their_values() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.yaml", CONTRACT);
        let table = write(
            dir.path(),
            "t.csv",
            "id,amount,currency\n1,9.5,USD\n2,0,EUR\n3,12,USD\n",
        );
        let (code, report, stderr) = json(&["check", arg(&table), "--contract", arg(&contract)]);
        assert_eq!(code, 0, "a contract alone is a check: {stderr}{report}");
        let result = &report["contract"];
        assert_eq!(result["contract"], "orders");
        assert_eq!(result["model"], "orders");
        assert_eq!(result["rows"], 3);
        assert_eq!(result["violations"], 0);

        write(
            dir.path(),
            "t.csv",
            "id,amount,currency\n1,9.5,USD\n2,-5,GBP\n2,12,CHF\n",
        );
        let (code, report, _) = json(&["check", arg(&table), "--contract", arg(&contract)]);
        assert_eq!(code, 3, "{report}");
        let result = &report["contract"];
        assert_eq!(result["violations"], 4, "{result}");
        let rule = |field: &str, rule: &str| {
            result["rules"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["field"] == field && r["rule"] == rule)
                .unwrap_or_else(|| panic!("no {field} {rule}: {result}"))
                .clone()
        };
        assert_eq!(rule("amount", "min")["first_row"], 1);
        assert_eq!(rule("currency", "allowed")["count"], 2);
        assert_eq!(rule("id", "unique")["first_row"], 2);

        let (code, stdout, _) = run(&["check", arg(&table), "--contract", arg(&contract)]);
        assert_eq!(code, 3);
        assert!(
            stdout.contains("FAIL     contract   4 violations in 3 rows, 0 allowed"),
            "{stdout}"
        );
        assert!(
            stdout.contains("currency  allowed: 2, first at data row 2"),
            "{stdout}"
        );
        assert!(
            stdout.contains("contract  orders 1.0.0  (model orders)"),
            "{stdout}"
        );
        // What the rows held stays out of a report a CI log keeps.
        for value in ["GBP", "CHF", "-5"] {
            assert!(!stdout.contains(value), "{value}: {stdout}");
        }
    }

    /// `unique` holds every distinct value it has seen, so it is given a budget
    /// (`--unique-memory`). Past it, uniqueness is not checked for the rest of the table, and
    /// a check that could not finish is unknown (exit 1), not a pass, unless
    /// `--allow-unenforced` accepts it as partial. Duplicates found before the stop still count,
    /// and every other rule is still checked on every row.
    #[test]
    fn unique_values_past_their_memory_stop_being_checked_and_the_gate_says_from_where() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.yaml", CONTRACT);
        // 20,000 rows: rows 1 and 2 share an id; the last row breaks `min`.
        let mut csv = String::from("id,amount,currency\n1,1,USD\n1,1,USD\n");
        for id in 3..20_000 {
            csv.push_str(&format!("{id},1,USD\n"));
        }
        csv.push_str("20000,-1,USD\n");
        let table = write(dir.path(), "t.csv", &csv);
        let gate = |report: &serde_json::Value| {
            report["gates"]
                .as_array()
                .unwrap()
                .iter()
                .find(|g| g["name"] == "contract")
                .unwrap_or_else(|| panic!("no contract gate: {report}"))
                .clone()
        };
        let rule = |report: &serde_json::Value, field: &str, rule: &str| {
            report["contract"]["rules"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["field"] == field && r["rule"] == rule)
                .cloned()
        };

        // The whole table fits the default: both rules are checked on every row.
        let (code, report, _) = json(&["check", arg(&table), "--contract", arg(&contract)]);
        assert_eq!(code, 3, "{report}");
        // The budgets below are sized by the estimate for an integer id — 132 bytes a value,
        // so one batch of up to 8,192 rows fits 2 MiB and 20,000 rows do not. Read as text,
        // the sizes would be off: say so here rather than as a puzzling stop row.
        assert_eq!(
            report["measured"]["columns"][0]["data_type"], "Int64",
            "the id column must read as an integer for these budgets: {report}"
        );
        assert!(
            report["contract"].get("unique_stopped").is_none(),
            "{report}"
        );
        assert_eq!(rule(&report, "id", "unique").unwrap()["first_row"], 1);

        // 2 MiB holds the first batch's ids and not the whole table's: the duplicate before the
        // stop is still caught, and the rows after it are still checked for `min`.
        let (code, report, _) = json(&[
            "check",
            arg(&table),
            "--contract",
            arg(&contract),
            "--unique-memory",
            "2M",
        ]);
        assert_eq!(code, 3, "the violations still fail it: {report}");
        let stopped = &report["contract"]["unique_stopped"];
        let from = stopped["from_row"].as_u64().unwrap();
        assert!(from > 1 && from < 20_000, "{stopped}");
        assert_eq!(stopped["fields"], serde_json::json!(["id"]));
        assert_eq!(stopped["limit"], 2 << 20);
        assert_eq!(rule(&report, "id", "unique").unwrap()["count"], 1);
        assert_eq!(rule(&report, "amount", "min").unwrap()["first_row"], 19_999);

        // A table that breaks nothing: the stop alone makes the gate unknown.
        let clean = write(
            dir.path(),
            "clean.csv",
            &csv.replace("1,1,USD\n1,1,USD\n", "1,1,USD\n2,1,USD\n")
                .replace("20000,-1,USD", "20000,1,USD"),
        );
        let (code, report, _) = json(&[
            "check",
            arg(&clean),
            "--contract",
            arg(&contract),
            "--unique-memory",
            "2M",
        ]);
        assert_eq!(
            code, 1,
            "a check that could not finish is not a pass: {report}"
        );
        let contract_gate = gate(&report);
        assert_eq!(contract_gate["status"], "unknown", "{contract_gate}");
        assert_eq!(contract_gate["partial"], true);
        let reason = contract_gate["reason"].as_str().unwrap();
        assert!(
            reason.contains("--unique-memory") && reason.contains("--allow-unenforced"),
            "{reason}"
        );

        // Accepted as partial, it passes, and says so.
        let (code, report, _) = json(&[
            "check",
            arg(&clean),
            "--contract",
            arg(&contract),
            "--unique-memory",
            "2M",
            "--allow-unenforced",
        ]);
        assert_eq!(code, 0, "{report}");
        let contract_gate = gate(&report);
        assert_eq!(contract_gate["status"], "pass");
        assert_eq!(contract_gate["partial"], true);
        assert_eq!(report["contract"]["unique_stopped"]["accepted"], true);

        // Too little for even one batch: nothing is held, and the duplicate goes unseen.
        let duplicated = write(
            dir.path(),
            "duplicated.csv",
            &csv.replace("20000,-1,USD", "20000,1,USD"),
        );
        let (code, stdout, _) = run(&[
            "check",
            arg(&duplicated),
            "--contract",
            arg(&contract),
            "--unique-memory",
            "1K",
        ]);
        assert_eq!(code, 1, "{stdout}");
        assert!(
            stdout.contains("not checked from data row 1  unique: id"),
            "{stdout}"
        );
        assert!(stdout.contains("no violation in 20000 rows"), "{stdout}");
    }

    #[test]
    fn unique_memory_is_a_contracts_option() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path(), "t.csv", "id\n1\n");
        let (code, _, stderr) = run(&[
            "check",
            arg(&table),
            "--max-age",
            "1d",
            "--unique-memory",
            "1M",
        ]);
        assert_eq!(code, 2, "{stderr}");
        assert!(stderr.contains("--contract"), "{stderr}");
    }

    /// The contract is read before the table: a model it does not have is the error, not
    /// whatever reading a table that does not read would have said.
    #[test]
    fn a_contract_that_cannot_be_used_stops_the_check_before_the_table_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.yaml", CONTRACT);
        let broken = write(dir.path(), "broken.parquet", "not a parquet file");
        let (code, _, stderr) = run(&[
            "check",
            arg(&broken),
            "--contract",
            arg(&contract),
            "--model",
            "nope",
        ]);
        assert_eq!(code, 1);
        assert!(
            stderr.contains("nope"),
            "the model, not the table: {stderr}"
        );
    }

    /// A database table's contract is refused before the database is opened.
    #[cfg(feature = "sqlite")]
    #[test]
    fn a_database_tables_contract_is_refused_before_it_is_opened() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.yaml", CONTRACT);
        let missing = dir.path().join("no-such.db");
        let table = format!("sqlite://{}?table=orders", missing.display());
        let (code, _, stderr) = run(&["check", &table, "--contract", arg(&contract)]);
        assert_eq!(code, 1);
        assert!(
            stderr.contains("does not query a database's tables"),
            "{stderr}"
        );
        assert!(!missing.exists());
    }

    #[test]
    fn a_contract_failure_keeps_the_baseline_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.yaml", CONTRACT);
        let table = write(dir.path(), "t.csv", "id,amount,currency\n1,-1,USD\n");
        let baseline = dir.path().join("t.check.json");
        let (code, report, _) = json(&[
            "check",
            arg(&table),
            "--baseline",
            arg(&baseline),
            "--update",
            "--contract",
            arg(&contract),
        ]);
        assert_eq!(code, 3, "{report}");
        assert_eq!(report["baseline_written"], false);
        assert!(!baseline.exists());
    }

    /// A table of no rows still has columns, and a column the contract requires that it lacks is
    /// still a violation.
    #[test]
    fn a_table_of_no_rows_still_misses_the_columns_it_lacks() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.yaml", CONTRACT);
        let table = write(dir.path(), "t.csv", "id\n");
        let (code, report, _) = json(&["check", arg(&table), "--contract", arg(&contract)]);
        assert_eq!(code, 3, "{report}");
        assert_eq!(report["contract"]["rows"], 0);
        let missing: Vec<_> = report["contract"]["rules"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["rule"] == "schema_missing_field")
            .map(|r| r["field"].as_str().unwrap().to_string())
            .collect();
        assert!(missing.contains(&"amount".to_string()), "{report}");
    }

    #[test]
    fn a_contract_with_rules_it_cannot_enforce_is_refused_or_checked_as_partial() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.odcs.yaml", ODCS_PARTIAL);
        let table = write(dir.path(), "t.csv", "id\n1\n2\n");
        let (code, _, stderr) = run(&["check", arg(&table), "--contract", arg(&contract)]);
        assert_eq!(code, 1);
        assert!(stderr.contains("cannot enforce"), "{stderr}");
        assert!(stderr.contains("--allow-unenforced"), "{stderr}");

        let (code, report, stderr) = json(&[
            "check",
            arg(&table),
            "--contract",
            arg(&contract),
            "--allow-unenforced",
        ]);
        assert_eq!(code, 0, "{stderr}");
        let gate = report["gates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["name"] == "contract")
            .unwrap()
            .clone();
        assert_eq!(gate["status"], "pass");
        assert_eq!(gate["partial"], true, "never a plain pass: {gate}");
        assert_eq!(
            report["contract"]["unenforced"].as_array().unwrap().len(),
            1
        );

        let (_, stdout, _) = run(&[
            "check",
            arg(&table),
            "--contract",
            arg(&contract),
            "--allow-unenforced",
        ]);
        assert!(
            stdout.contains("partial: 1 contract rule not enforced"),
            "{stdout}"
        );
        assert!(stdout.contains("PASS, partially"), "{stdout}");
        let out = dir.path().join("check.xml");
        let (code, _, _) = run(&[
            "check",
            arg(&table),
            "--contract",
            arg(&contract),
            "--allow-unenforced",
            "--out",
            arg(&out),
        ]);
        assert_eq!(code, 0);
        let xml = std::fs::read_to_string(&out).unwrap();
        assert!(
            xml.contains("<system-out>no violation in 2 rows — partial"),
            "{xml}"
        );

        let (code, _, stderr) = run(&[
            "check",
            arg(&table),
            "--contract",
            arg(&contract),
            "--allow-unenforced",
            "--model",
            "nope",
        ]);
        assert_eq!(code, 1, "{stderr}");
    }
}

/// A database table is counted in its own database, in its own dialect's quoting: it has no files
/// here and no recorded change, so those gates skip — unless asked for, when they are unknown.
#[cfg(feature = "sqlite")]
mod databases {
    use super::*;

    /// `rows` as the table `orders` (an id, and a `unit price` that may be null) of a SQLite file.
    fn sqlite_orders(path: &Path, rows: &[(i64, Option<f64>)]) {
        use sqlx::Connection;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let url = format!("sqlite://{}?mode=rwc", path.display());
            let mut db = sqlx::SqliteConnection::connect(&url).await.unwrap();
            sqlx::query("DROP TABLE IF EXISTS orders")
                .execute(&mut db)
                .await
                .unwrap();
            sqlx::query("CREATE TABLE orders (id INTEGER, \"unit price\" REAL)")
                .execute(&mut db)
                .await
                .unwrap();
            for (id, price) in rows {
                sqlx::query("INSERT INTO orders VALUES (?, ?)")
                    .bind(id)
                    .bind(price)
                    .execute(&mut db)
                    .await
                    .unwrap();
            }
            db.close().await.unwrap();
        });
    }

    #[test]
    fn a_sqlite_table_is_counted_in_the_database_and_gated_like_any_table() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("shop.db");
        sqlite_orders(
            &db,
            &[(1, Some(9.5)), (2, None), (3, Some(1.0)), (4, Some(2.0))],
        );
        let table = format!("sqlite://{}?table=orders", db.display());
        let baseline = dir.path().join("orders.check.json");
        let (code, stdout, stderr) = run(&[
            "check",
            &table,
            "--baseline",
            arg(&baseline),
            "--update",
            "-o",
            "json",
        ]);
        assert_eq!(code, 0, "{stderr}");
        let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(report["measured"]["rows"], 4);
        assert_eq!(report["measured"]["method"], "exact");
        assert_eq!(nulls_of(&report, "unit price"), 1, "a quoted column name");
        assert_eq!(report["measured"]["files"], serde_json::Value::Null);

        sqlite_orders(&db, &[(1, Some(9.5)), (2, None)]);
        let (code, stdout, _) = run(&["check", &table, "--baseline", arg(&baseline)]);
        assert_eq!(code, 3, "{stdout}");
        assert!(
            stdout.contains("FAIL     row-count  4 → 2 rows"),
            "{stdout}"
        );
        assert!(stdout.contains("SKIPPED  files"), "{stdout}");

        // What a database table does not have here cannot be asked of it and pass.
        let (code, stdout, _) = run(&["check", &table, "--max-age", "1d"]);
        assert_eq!(code, 1, "{stdout}");
        assert!(stdout.contains("UNKNOWN  freshness"), "{stdout}");
        let (code, _, stderr) = run(&["check", &table, "--fast", "--max-age", "1d"]);
        assert_eq!(code, 1);
        assert!(stderr.contains("this is a database table"), "{stderr}");
    }

    /// The baseline is committed, so a database password must reach none of it: not the
    /// fingerprint, not the report, not an error. Runs against the Postgres in
    /// `LAKELETO_TEST_PG_URL` (with a password in it), and is skipped without one.
    #[cfg(feature = "postgres")]
    #[test]
    fn a_postgres_table_is_counted_and_its_password_is_nowhere_in_what_check_writes() {
        use sqlx::Connection;
        let Ok(url) = std::env::var("LAKELETO_TEST_PG_URL") else {
            eprintln!("skipped: no LAKELETO_TEST_PG_URL");
            return;
        };
        // `postgres://user:password@host…`: what is between the first `:` after the scheme and
        // the `@`.
        let password = url
            .split_once("://")
            .and_then(|(_, rest)| rest.split_once('@'))
            .and_then(|(userinfo, _)| userinfo.split_once(':'))
            .map(|(_, password)| password.to_string())
            .filter(|p| !p.is_empty())
            .expect("LAKELETO_TEST_PG_URL carries a password");
        assert_eq!(
            url.matches(password.as_str()).count(),
            1,
            "LAKELETO_TEST_PG_URL needs a password that appears nowhere else in it, or finding it \
             proves nothing"
        );
        let table = format!("lakeleto_check_{}", std::process::id());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let exec = |sql: String| {
            runtime.block_on(async {
                let mut db = sqlx::PgConnection::connect(&url).await.unwrap();
                sqlx::query(&sql).execute(&mut db).await.unwrap();
                db.close().await.unwrap();
            })
        };
        exec(format!(
            "CREATE TABLE {table} (id bigint, \"Unit Price\" double precision)"
        ));
        exec(format!(
            "INSERT INTO {table} VALUES (1, 9.5), (2, NULL), (3, 1.0)"
        ));

        let dir = tempfile::tempdir().unwrap();
        let baseline = dir.path().join("pg.check.json");
        let source = format!("{url}?table={table}");
        let (code, stdout, stderr) = run(&[
            "check",
            &source,
            "--baseline",
            arg(&baseline),
            "--update",
            "-o",
            "json",
        ]);
        let written = std::fs::read_to_string(&baseline).unwrap_or_default();
        exec(format!("DROP TABLE {table}"));
        assert_eq!(code, 0, "{stderr}");
        let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(report["measured"]["rows"], 3);
        assert_eq!(
            nulls_of(&report, "Unit Price"),
            1,
            "a mixed-case quoted column"
        );
        for (what, text) in [
            ("report", &stdout),
            ("errors", &stderr),
            ("baseline", &written),
        ] {
            assert!(
                !text.contains(&password),
                "the password is in the {what}: {text}"
            );
        }
        assert!(!written.is_empty());

        // A refused login's error names the URI without the password it was refused with.
        let wrong = "Wr0ng-pass-x7";
        let refused = format!("{}?table={table}", url.replacen(&password, wrong, 1));
        let (code, stdout, stderr) = run(&["check", &refused, "--max-age", "1d"]);
        assert_eq!(code, 1, "{stdout}{stderr}");
        assert!(!stderr.contains(wrong), "{stderr}");
        assert!(!stdout.contains(wrong), "{stdout}");
    }
}
