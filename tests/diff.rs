//! `lakeleto diff`, driven through the binary the way a CI job runs it: what it reports, in each of
//! its three forms, and the exit code `--fail-on` turns a finding into.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The reference table: an id, a name with a null, a score with a null, a city.
const BEFORE: &str = "id,name,score,city\n\
                      1,ada,9.5,Oslo\n\
                      2,grace,,Lima\n\
                      3,linus,7.25,Oslo\n\
                      4,,8,Pune\n";

/// [`BEFORE`] a run later: row 3 gone, row 5 new with a higher score, `city` dropped, `team`
/// added, and the null name filled in.
const AFTER: &str = "id,name,score,team\n\
                     1,ada,9.5,a\n\
                     2,grace,,b\n\
                     4,ken,8,a\n\
                     5,mary,9.75,b\n";

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

/// The JSON report for `left` against `right`, with `extra` arguments.
fn json(left: &Path, right: &Path, extra: &[&str]) -> serde_json::Value {
    let mut args = vec!["diff", arg(left), arg(right), "-o", "json"];
    args.extend_from_slice(extra);
    let (code, stdout, stderr) = run(&args);
    assert_eq!(code, 0, "{stderr}");
    serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("{e}: {stdout}"))
}

fn names(value: &serde_json::Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn a_table_against_itself_differs_in_nothing_and_exits_zero_whatever_fail_on_names() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let (code, stdout, stderr) = run(&["diff", arg(&before), arg(&before), "--fail-on", "any"]);
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.contains("found  no difference"), "{stdout}");
    assert!(stdout.contains("the same columns"), "{stdout}");
    let report = json(&before, &before, &[]);
    assert_eq!(report["version"], 1);
    assert_eq!(report["found"], serde_json::json!([]));
    assert_eq!(report["rows"]["left"], 4);
    assert_eq!(report["rows"]["right"], 4);
}

#[test]
fn columns_rows_and_statistics_are_each_reported_and_found() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let after = write(dir.path(), "after.csv", AFTER);
    let report = json(&before, &after, &[]);
    assert_eq!(names(&report["schema"]["added"]), ["team"]);
    assert_eq!(names(&report["schema"]["removed"]), ["city"]);
    assert_eq!(report["schema"]["changed"], serde_json::json!([]));
    assert_eq!(report["schema"]["reordered"], false);
    // Four rows on each side: one gone, one new.
    assert_eq!(report["rows"]["left"], 4);
    assert_eq!(report["rows"]["right"], 4);
    // Every row of each side is read: either by SQL or by a sample larger than the table.
    assert_eq!(report["stats"]["method"], "exact");
    let column = |name: &str| {
        report["stats"]["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("no {name} in {report}"))
            .clone()
    };
    let id = column("id");
    assert_eq!(id["changed"], true);
    assert_eq!(id["left"]["max"], "4");
    assert_eq!(id["right"]["max"], "5");
    let name = column("name");
    assert_eq!(name["changed"], true, "its null was filled in");
    assert_eq!(name["left"]["nulls"], 1);
    assert_eq!(name["right"]["nulls"], 0);
    assert_eq!(column("score")["changed"], true, "a new maximum");
    assert_eq!(
        report["found"],
        serde_json::json!(["added-column", "removed-column", "stats"])
    );
}

#[test]
fn fail_on_exits_three_for_what_it_names_and_zero_for_what_it_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let after = write(dir.path(), "after.csv", AFTER);
    let code = |fail_on: &str| run(&["diff", arg(&before), arg(&after), "--fail-on", fail_on]).0;
    assert_eq!(code("removed-column"), 3);
    assert_eq!(code("schema"), 3);
    assert_eq!(code("type-change,row-count"), 0, "neither is found");
    assert_eq!(code("type-change,stats"), 3, "a list fails on any it names");
    // Repeated, as clap takes it, as well as comma-separated.
    let (repeated, _, _) = run(&[
        "diff",
        arg(&before),
        arg(&after),
        "--fail-on",
        "nullability",
        "--fail-on",
        "added-column",
    ]);
    assert_eq!(repeated, 3);
    // Without --fail-on a difference is a report, not a failure.
    assert_eq!(run(&["diff", arg(&before), arg(&after)]).0, 0);
    // A kind it does not know is a usage error, before anything is read.
    let (unknown, _, stderr) = run(&["diff", arg(&before), arg(&after), "--fail-on", "colour"]);
    assert_eq!(unknown, 2, "{stderr}");
}

#[test]
fn a_failing_diff_still_writes_its_whole_report_to_out() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let after = write(dir.path(), "after.csv", AFTER);
    let out = dir.path().join("diff.md");
    let (code, stdout, stderr) = run(&[
        "diff",
        arg(&before),
        arg(&after),
        "--out",
        arg(&out),
        "--fail-on",
        "any",
    ]);
    assert_eq!(code, 3, "{stderr}");
    assert!(stdout.is_empty());
    // `.md` asks for Markdown, as `.json` asks for JSON.
    let md = std::fs::read_to_string(&out).unwrap();
    assert!(
        md.starts_with("### lakeleto diff: added-column, removed-column, stats"),
        "{md}"
    );
    assert!(md.contains("| added | `team` |"), "{md}");
    assert!(md.contains("| removed | `city` |"), "{md}");
    assert!(md.contains("| `id` | 0 | `1` | `4` → `5` |"), "{md}");
}

#[test]
fn the_text_report_shows_what_differs_and_counts_what_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let after = write(dir.path(), "after.csv", AFTER);
    let (code, stdout, stderr) = run(&["diff", arg(&before), arg(&after)]);
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.contains("  + team  Utf8 (nullable)"), "{stdout}");
    assert!(stdout.contains("  - city  Utf8 (nullable)"), "{stdout}");
    assert!(stdout.contains("  4 → 4  (no change)"), "{stdout}");
    assert!(stdout.contains("4 → 5"), "{stdout}");
}

#[test]
fn a_retyped_column_and_a_moved_one_are_told_apart() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", "a,b,c\n1,x,2\n2,y,3\n");
    let after = write(dir.path(), "after.csv", "b,a,c\nx,1,2.5\ny,2,3\n");
    let report = json(&before, &after, &[]);
    assert_eq!(report["schema"]["reordered"], true);
    let changed = &report["schema"]["changed"][0];
    assert_eq!(changed["name"], "c");
    assert_eq!(changed["left_type"], "Int64");
    assert_eq!(changed["right_type"], "Float64");
    assert_eq!(
        report["found"],
        serde_json::json!(["type-change", "column-order", "stats"])
    );
}

#[test]
fn a_sample_says_it_is_one_and_footer_statistics_need_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let after = write(dir.path(), "after.csv", AFTER);
    let report = json(&before, &after, &["--scan", "2"]);
    assert_eq!(report["stats"]["method"], "sample");
    assert_eq!(report["stats"]["left_rows_read"], 2);
    let (_, stdout, _) = run(&["diff", arg(&before), arg(&after), "--scan", "2"]);
    assert!(
        stdout.contains("may not be one between the tables"),
        "{stdout}"
    );
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&after), "--fast"]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("`--fast` reads Parquet footers, and the left side is csv"),
        "{stderr}"
    );
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&after), "--fast", "--scan", "2"]);
    assert_eq!(
        code, 2,
        "--fast and --scan are two ways of taking them: {stderr}"
    );
}

#[test]
fn diff_has_three_forms_and_markdown_is_only_diffs() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    for refused in ["csv", "tsv", "ndjson", "arrow"] {
        let (code, _, stderr) = run(&["diff", arg(&before), arg(&before), "-o", refused]);
        assert_eq!(code, 1, "-o {refused}");
        assert!(stderr.contains("`diff` prints a report"), "{stderr}");
    }
    let (code, _, stderr) = run(&["schema", arg(&before), "-o", "md"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("no Markdown form"), "{stderr}");
}

#[test]
fn a_diff_through_a_server_is_refused_before_anything_is_read() {
    let (code, _, stderr) = run(&[
        "--remote-url",
        "http://127.0.0.1:9",
        "diff",
        "a.parquet",
        "b.parquet",
    ]);
    assert_eq!(code, 1);
    assert!(stderr.contains("where the tables are"), "{stderr}");
}

/// Every row is read by SQL, which needs every column name quoted as it is.
#[cfg(feature = "sql")]
#[test]
fn exact_statistics_quote_every_column_name_and_say_which_engine_took_them() {
    let dir = tempfile::tempdir().unwrap();
    let csv = "\"Order ID\",\"say \"\"hi\"\"\",lower\n1,x,a\n2,,b\n";
    let before = write(dir.path(), "before.csv", csv);
    let after = write(
        dir.path(),
        "after.csv",
        "\"Order ID\",\"say \"\"hi\"\"\",lower\n1,x,a\n3,y,b\n",
    );
    let report = json(&before, &after, &[]);
    assert_eq!(report["stats"]["method"], "exact");
    assert_eq!(report["stats"]["engine"], "sql");
    let columns = report["stats"]["columns"].as_array().unwrap();
    let id = columns.iter().find(|c| c["name"] == "Order ID").unwrap();
    assert_eq!(id["right"]["max"], "3");
    let quoted = columns.iter().find(|c| c["name"] == "say \"hi\"").unwrap();
    assert_eq!(quoted["left"]["nulls"], 1);
    assert_eq!(quoted["right"]["nulls"], 0);
}

/// A build with no SQL reads a sample, and says so when the sample is not the whole table.
#[cfg(not(feature = "sql"))]
#[test]
fn without_sql_a_table_past_the_sample_is_reported_as_a_sample() {
    let dir = tempfile::tempdir().unwrap();
    let mut csv = String::from("n\n");
    for i in 0..10_001 {
        csv.push_str(&format!("{i}\n"));
    }
    let big = write(dir.path(), "big.csv", &csv);
    let report = json(&big, &big, &[]);
    assert_eq!(report["stats"]["method"], "sample");
    let notes = report["notes"].as_array().unwrap();
    assert!(
        notes[0].as_str().unwrap().contains("--features sql"),
        "{notes:?}"
    );
}

// ---- rows by key ----------------------------------------------------------------------------

/// The keyed section of `left` against `right`, by `key`.
#[cfg(feature = "sql")]
fn keyed(left: &Path, right: &Path, key: &str, extra: &[&str]) -> serde_json::Value {
    let mut args = vec!["--key", key];
    args.extend_from_slice(extra);
    json(left, right, &args)["keyed"].clone()
}

/// [`BEFORE`] against [`AFTER`] by `id`: row 3 removed, row 5 added, row 4's null name filled,
/// rows 1 and 2 as they were — row 2's null score matching the other side's null score. `city`
/// and `team` are on one side each, so neither is compared.
#[cfg(feature = "sql")]
#[test]
fn rows_matched_by_key_are_counted_added_removed_changed_and_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let after = write(dir.path(), "after.csv", AFTER);
    let k = keyed(&before, &after, "id", &[]);
    assert_eq!(k["key"], serde_json::json!(["id"]));
    assert_eq!(k["compared"], serde_json::json!(["name", "score"]));
    assert_eq!(k["not_compared"], serde_json::json!([]));
    assert_eq!(
        (&k["added"], &k["removed"], &k["changed"], &k["unchanged"]),
        (
            &serde_json::json!(1),
            &serde_json::json!(1),
            &serde_json::json!(1),
            &serde_json::json!(2)
        )
    );
    assert_eq!(
        k["changed_by_column"],
        serde_json::json!([{"name": "name", "changed": 1}, {"name": "score", "changed": 0}])
    );
    assert_eq!(
        k["samples"]["added"],
        serde_json::json!([{"key": ["5"], "values": ["mary", "9.75"]}])
    );
    assert_eq!(
        k["samples"]["removed"],
        serde_json::json!([{"key": ["3"], "values": ["linus", "7.25"]}])
    );
    assert_eq!(
        k["samples"]["changed"],
        serde_json::json!([{"key": ["4"], "changes": [{"column": "name", "left": null, "right": "ken"}]}])
    );
    let report = json(&before, &after, &["--key", "id"]);
    let found = report["found"].as_array().unwrap();
    for kind in ["added-rows", "removed-rows", "changed-rows"] {
        assert!(found.contains(&serde_json::json!(kind)), "{found:?}");
    }
}

#[cfg(feature = "sql")]
#[test]
fn fail_on_rows_exits_three_only_for_the_rows_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let only_changed = write(
        dir.path(),
        "changed.csv",
        "id,name,score,city\n1,ada,9.5,Oslo\n2,grace,,Lima\n3,linus,7.5,Oslo\n4,,8,Pune\n",
    );
    let code = |fail_on: &str| {
        run(&[
            "diff",
            arg(&before),
            arg(&only_changed),
            "--key",
            "id",
            "--fail-on",
            fail_on,
        ])
        .0
    };
    assert_eq!(code("added-rows,removed-rows"), 0);
    assert_eq!(code("changed-rows"), 3);
    assert_eq!(code("rows"), 3);
    let (text_code, stdout, _) = run(&["diff", arg(&before), arg(&only_changed), "--key", "id"]);
    assert_eq!(text_code, 0);
    assert!(
        stdout.contains("added 0 · removed 0 · changed 1 · unchanged 3"),
        "{stdout}"
    );
    assert!(stdout.contains("  ~ id=3  score: 7.25 → 7.5"), "{stdout}");
}

/// A key that names two rows would match rows many to many and miscount: refused, naming it.
#[cfg(feature = "sql")]
#[test]
fn a_key_that_repeats_on_either_side_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let repeated = write(
        dir.path(),
        "repeated.csv",
        "id,name,score,city\n1,ada,9.5,Oslo\n2,grace,,Lima\n2,again,1,Lima\n2,thrice,2,Lima\n",
    );
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&repeated), "--key", "id"]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("names more than one row on the right side: 1 key repeats, id=2 3 times"),
        "{stderr}"
    );
    // Made unique by a second column, it is a key.
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&repeated), "--key", "id,name"]);
    assert_eq!(code, 0, "{stderr}");
}

#[cfg(feature = "sql")]
#[test]
fn a_key_both_sides_cannot_compare_is_refused_before_any_row_is_matched() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let after = write(dir.path(), "after.csv", AFTER);
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&after), "--key", "city"]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("`city` is not on the right side"),
        "{stderr}"
    );
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&after), "--key", "id,id"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("in the key twice"), "{stderr}");
    let texts = write(dir.path(), "texts.csv", "id,name\nx1,ada\nx2,grace\n");
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&texts), "--key", "id"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("types differ: Int64 and Utf8"), "{stderr}");
}

/// Columns whose values are of one kind compare whatever their width: an integer column against
/// a float column holding the same numbers is unchanged. One whose kinds differ is listed, never
/// counted as changed.
#[cfg(feature = "sql")]
#[test]
fn columns_compare_by_kind_and_a_column_that_cannot_is_listed_not_changed() {
    let dir = tempfile::tempdir().unwrap();
    let left = write(dir.path(), "left.csv", "id,n,when\n1,2,a\n2,3,b\n");
    let right = write(dir.path(), "right.csv", "id,n,when\n1,2.0,10\n2,3.0,20\n");
    let k = keyed(&left, &right, "id", &[]);
    assert_eq!(k["compared"], serde_json::json!(["n"]));
    assert_eq!(
        k["not_compared"],
        serde_json::json!([{"name": "when", "reason": "types differ: Utf8 and Int64"}])
    );
    assert_eq!(k["changed"], 0);
    assert_eq!(k["unchanged"], 2);
}

/// What equal means, pinned: a null key matches a null key, `NaN` matches `NaN`, and `0.0` and
/// `-0.0` differ, as Arrow's total order has them.
#[cfg(feature = "sql")]
#[test]
fn null_keys_nan_and_negative_zero_compare_as_arrow_orders_them() {
    let dir = tempfile::tempdir().unwrap();
    let left = write(dir.path(), "left.csv", "id,x\n1,NaN\n2,0.0\n3,1.5\n,7\n");
    let right = write(dir.path(), "right.csv", "id,x\n1,NaN\n2,-0.0\n3,1.5\n,7\n");
    let k = keyed(&left, &right, "id", &[]);
    assert_eq!(
        (&k["added"], &k["removed"]),
        (&serde_json::json!(0), &serde_json::json!(0))
    );
    assert_eq!(k["changed"], 1);
    assert_eq!(k["unchanged"], 3);
    assert_eq!(
        k["samples"]["changed"],
        serde_json::json!([{"key": ["2"], "changes": [{"column": "x", "left": "0.0", "right": "-0.0"}]}])
    );
}

/// Every name is quoted: a key with a space, a compared column with a quote and capitals.
#[cfg(feature = "sql")]
#[test]
fn keys_and_columns_with_any_name_compare() {
    let dir = tempfile::tempdir().unwrap();
    let left = write(
        dir.path(),
        "left.csv",
        "\"Order ID\",\"Say \"\"Hi\"\"\"\n1,a\n2,b\n",
    );
    let right = write(
        dir.path(),
        "right.csv",
        "\"Order ID\",\"Say \"\"Hi\"\"\"\n1,a\n2,c\n",
    );
    let k = keyed(&left, &right, "Order ID", &[]);
    assert_eq!(k["compared"], serde_json::json!(["Say \"Hi\""]));
    assert_eq!(k["changed"], 1);
    assert_eq!(k["samples"]["changed"][0]["key"], serde_json::json!(["2"]));
}

#[cfg(feature = "sql")]
#[test]
fn sample_sets_how_many_rows_are_shown_and_markdown_shows_them() {
    let dir = tempfile::tempdir().unwrap();
    let mut left = String::from("id,v\n");
    let mut right = String::from("id,v\n");
    for i in 0..20 {
        left.push_str(&format!("{i},{i}\n"));
        right.push_str(&format!("{i},{}\n", i + 1));
    }
    let left = write(dir.path(), "left.csv", &left);
    let right = write(dir.path(), "right.csv", &right);
    let k = keyed(&left, &right, "id", &["--sample", "3"]);
    assert_eq!(k["changed"], 20);
    let keys: Vec<&str> = k["samples"]["changed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["key"][0].as_str().unwrap())
        .collect();
    assert_eq!(
        keys,
        ["0", "1", "2"],
        "least key first, by the key's own order"
    );
    let none = keyed(&left, &right, "id", &["--sample", "0"]);
    assert_eq!(none["samples"]["changed"], serde_json::json!([]));
    let (code, md, stderr) = run(&[
        "diff",
        arg(&left),
        arg(&right),
        "--key",
        "id",
        "--sample",
        "1",
        "-o",
        "markdown",
    ]);
    assert_eq!(code, 0, "{stderr}");
    assert!(
        md.contains("**Rows by key** (`id`): added 0 · removed 0 · changed 20 · unchanged 0"),
        "{md}"
    );
    assert!(
        md.contains("| changed | `id` = `0` | `v`: `0` → `1` |"),
        "{md}"
    );
}

#[test]
fn sample_needs_a_key() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&before), "--sample", "3"]);
    assert_eq!(code, 2, "{stderr}");
}

/// A build with no SQL compares no rows, and says which feature would.
#[cfg(not(feature = "sql"))]
#[test]
fn without_sql_a_key_is_refused_naming_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&before), "--key", "id"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("--features sql"), "{stderr}");
}

// ---- Parquet, as a table written twice is ---------------------------------------------------

/// `batch` written to `dir/name` as a Parquet file, uncompressed, so every build reads it.
fn parquet(dir: &Path, name: &str, batch: &arrow_array::RecordBatch) -> PathBuf {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    path
}

/// Orders with an id, an amount and a list of tags: the second write widens the id to 64 bits,
/// raises one amount, drops order 3 and adds order 4.
fn orders_twice(dir: &Path) -> (PathBuf, PathBuf) {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch};
    use std::sync::Arc;
    let tags = |rows: &[&[&str]]| -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for row in rows {
            for t in *row {
                b.values().append_value(t);
            }
            b.append(true);
        }
        Arc::new(b.finish())
    };
    let before = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "amount",
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])) as ArrayRef,
        ),
        ("tags", tags(&[&["a"], &["b", "c"], &[]])),
    ])
    .unwrap();
    let after = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 4])) as ArrayRef),
        (
            "amount",
            Arc::new(Float64Array::from(vec![10.0, 25.0, 40.0])) as ArrayRef,
        ),
        ("tags", tags(&[&["a"], &["b", "c"], &["d"]])),
    ])
    .unwrap();
    (
        parquet(dir, "before.parquet", &before),
        parquet(dir, "after.parquet", &after),
    )
}

#[test]
fn two_parquet_files_diff_from_their_footers_with_no_row_read() {
    let dir = tempfile::tempdir().unwrap();
    let (before, after) = orders_twice(dir.path());
    let report = json(&before, &after, &["--fast"]);
    assert_eq!(report["stats"]["method"], "footer");
    assert_eq!(report["stats"]["left_rows_read"], 0);
    assert_eq!(report["rows"]["left"], 3);
    assert_eq!(report["rows"]["right"], 3);
    let changed = &report["schema"]["changed"][0];
    assert_eq!(
        (
            &changed["name"],
            &changed["left_type"],
            &changed["right_type"]
        ),
        (
            &serde_json::json!("id"),
            &serde_json::json!("Int32"),
            &serde_json::json!("Int64")
        )
    );
    let amount = report["stats"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "amount")
        .unwrap()
        .clone();
    assert_eq!(amount["changed"], true, "{amount}");
    assert_eq!(amount["right"]["max"], "40.0");
    // A footer records a list column's nulls at no top level: not counted, so not 0 = 0.
    let tags = report["stats"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "tags")
        .unwrap()
        .clone();
    assert_eq!(tags["left"]["nulls"], serde_json::Value::Null, "{tags}");
    assert_eq!(amount["left"]["nulls"], 0, "{amount}");
}

/// A nested column reads, gets its null count, and is never compared row by row.
#[cfg(feature = "sql")]
#[test]
fn a_parquet_table_written_twice_compares_by_key_across_widths_and_skips_nested_columns() {
    let dir = tempfile::tempdir().unwrap();
    let (before, after) = orders_twice(dir.path());
    let report = json(&before, &after, &["--key", "id"]);
    assert_eq!(report["stats"]["method"], "exact");
    let id = report["stats"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "id")
        .unwrap()
        .clone();
    assert!(
        id.get("not_compared").is_none(),
        "Int32 and Int64 are both numbers: {id}"
    );
    let k = &report["keyed"];
    assert_eq!(k["compared"], serde_json::json!(["amount"]));
    let reason = k["not_compared"][0]["reason"].as_str().unwrap();
    assert_eq!(k["not_compared"][0]["name"], "tags");
    assert!(reason.starts_with("a nested type"), "{reason}");
    assert_eq!(
        (&k["added"], &k["removed"], &k["changed"], &k["unchanged"]),
        (
            &serde_json::json!(1),
            &serde_json::json!(1),
            &serde_json::json!(1),
            &serde_json::json!(1)
        )
    );
    assert_eq!(
        k["samples"]["changed"],
        serde_json::json!([{"key": ["2"], "changes": [{"column": "amount", "left": "20.0", "right": "25.0"}]}])
    );
}

/// The sample orders as CSV and as Parquet hold the same rows: an `Int64` id against an `Int32`
/// one and timestamps in seconds against nanoseconds compare equal, and nothing has changed.
#[cfg(all(feature = "sql", feature = "compression"))]
#[test]
fn the_sample_orders_as_csv_and_as_parquet_are_the_same_rows() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/sample-data");
    let report = json(
        &data.join("orders.csv"),
        &data.join("orders.parquet"),
        &["--key", "order_id"],
    );
    let k = &report["keyed"];
    assert_eq!(k["not_compared"], serde_json::json!([]));
    assert_eq!(
        (&k["added"], &k["removed"], &k["changed"], &k["unchanged"]),
        (
            &serde_json::json!(0),
            &serde_json::json!(0),
            &serde_json::json!(0),
            &serde_json::json!(5000)
        )
    );
    assert_eq!(report["found"], serde_json::json!(["type-change"]));
}

/// Statistics of a column whose two types hold different kinds of value are shown, not compared;
/// numbers compare as numbers.
#[test]
fn statistics_compare_numbers_as_numbers_and_never_text_against_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let left = write(dir.path(), "left.csv", "id,n,when\n1,2,a\n2,3,b\n");
    let right = write(dir.path(), "right.csv", "id,n,when\n1,2.0,10\n2,3.0,20\n");
    let report = json(&left, &right, &[]);
    let column = |name: &str| {
        report["stats"]["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap()
            .clone()
    };
    assert_eq!(column("n")["changed"], false, "2 and 2.0 are one value");
    assert_eq!(column("when")["changed"], false);
    assert_eq!(
        column("when")["not_compared"],
        "types differ: Utf8 and Int64"
    );
    assert_eq!(report["found"], serde_json::json!(["type-change"]));
}

#[test]
fn a_row_kind_in_fail_on_needs_a_key() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let (code, _, stderr) = run(&[
        "diff",
        arg(&before),
        arg(&before),
        "--fail-on",
        "schema,changed-rows",
    ]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("`--fail-on changed-rows` needs `--key`"),
        "{stderr}"
    );
    let (code, _, stderr) = run(&[
        "--engine",
        "local",
        "diff",
        arg(&before),
        arg(&before),
        "--key",
        "id",
    ]);
    assert_eq!(code, 1);
    assert!(stderr.contains("drop `--engine local`"), "{stderr}");
}

/// A key column may be called what an alias in the query would be.
#[cfg(feature = "sql")]
#[test]
fn a_key_named_like_an_alias_still_names_its_repeats() {
    let dir = tempfile::tempdir().unwrap();
    let repeated = write(dir.path(), "n.csv", "n,v\n1,a\n1,b\n");
    let (code, _, stderr) = run(&["diff", arg(&repeated), arg(&repeated), "--key", "n"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("1 key repeats, n=1 2 times"), "{stderr}");
    let unique = write(dir.path(), "u.csv", "n,present,lp\n1,a,x\n2,b,y\n");
    let (code, _, stderr) = run(&["diff", arg(&unique), arg(&unique), "--key", "n"]);
    assert_eq!(code, 0, "{stderr}");
}

// ---- every type a column may have -----------------------------------------------------------

/// `column` as `v` beside an `id`, written to `dir/name` as an Arrow IPC file.
fn arrow_file(dir: &Path, name: &str, column: arrow_array::ArrayRef) -> PathBuf {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use std::sync::Arc;
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("v", column),
    ])
    .unwrap();
    let path = dir.join(name);
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = arrow_ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    path
}

/// The types whose `min` and `max` an engine may not take: each diffs whole — its null count at
/// least — rather than failing the report.
#[test]
fn a_column_of_any_type_diffs_without_failing_the_report() {
    use arrow_array::types::{IntervalDayTimeType, IntervalMonthDayNanoType};
    use arrow_array::{
        ArrayRef, Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array,
        DurationSecondArray, Float32Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
        Time64NanosecondArray,
    };
    use arrow_schema::DataType;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let half = arrow_cast::cast(&Float32Array::from(vec![1.0, 2.0]), &DataType::Float16).unwrap();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("float16", half),
        ("duration", Arc::new(DurationSecondArray::from(vec![1, 2]))),
        (
            "interval_day_time",
            Arc::new(IntervalDayTimeArray::from(vec![
                IntervalDayTimeType::make_value(1, 0),
                IntervalDayTimeType::make_value(2, 0),
            ])),
        ),
        (
            "interval_month_day_nano",
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNanoType::make_value(1, 0, 0),
                IntervalMonthDayNanoType::make_value(2, 0, 0),
            ])),
        ),
        (
            "decimal32",
            Arc::new(
                Decimal32Array::from(vec![1, 2])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
        ),
        (
            "decimal64",
            Arc::new(
                Decimal64Array::from(vec![1, 2])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
        ),
        (
            "decimal128",
            Arc::new(
                Decimal128Array::from(vec![1, 2])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ),
        (
            "decimal256",
            Arc::new(
                Decimal256Array::from(vec![
                    arrow_buffer::i256::from(1),
                    arrow_buffer::i256::from(2),
                ])
                .with_precision_and_scale(76, 2)
                .unwrap(),
            ),
        ),
        ("time64", Arc::new(Time64NanosecondArray::from(vec![1, 2]))),
    ];
    let mut failed = Vec::new();
    for (name, column) in columns {
        let file = arrow_file(dir.path(), &format!("{name}.arrow"), column);
        let (code, stdout, stderr) = run(&["diff", arg(&file), arg(&file), "-o", "json"]);
        if code != 0 {
            failed.push(format!("{name}: {stderr}"));
            continue;
        }
        let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(report["found"], serde_json::json!([]), "{name}: {report}");
    }
    assert!(failed.is_empty(), "{failed:#?}");
}

/// A report posted as a pull request comment shows the data, and does nothing because of it: a
/// mention, an image or a link a table holds is inline code, and so is a column name with a
/// backtick in it.
#[test]
fn markdown_shows_what_a_table_holds_and_renders_none_of_it() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", "id,\"na`me\"\n1,a\n2,b\n");
    let after = write(
        dir.path(),
        "after.csv",
        "id,\"na`me\"\n1,a\n2,\"@acme/x ![x](https://h.invalid/p.png) [go](https://h.i)\"\n",
    );
    let (code, md, stderr) = run(&["diff", arg(&before), arg(&after), "-o", "markdown"]);
    assert_eq!(code, 0, "{stderr}");
    // The column name in a fence longer than its backtick, and the value whole inside one.
    assert!(
        md.contains(
            "| ``na`me`` | 0 | `a` → `@acme/x ![x](https://h.invalid/p.png) [go](https://h.i)` |"
        ),
        "{md}"
    );
    assert_eq!(md.matches("@acme").count(), 1, "{md}");
}

// ---- from the reviews of #1996 ---------------------------------------------------------------

/// Columns in a new order and nothing else is a difference `any` names, and `schema` does not:
/// a reader that finds columns by name never sees it.
#[test]
fn a_new_column_order_alone_is_found_and_failed_on_by_any_or_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", "a,b\n1,2\n");
    let after = write(dir.path(), "after.csv", "b,a\n2,1\n");
    let code = |fail_on: &str| run(&["diff", arg(&before), arg(&after), "--fail-on", fail_on]).0;
    assert_eq!(code("any"), 3);
    assert_eq!(code("column-order"), 3);
    assert_eq!(code("schema"), 0);
    let (_, stdout, _) = run(&["diff", arg(&before), arg(&before), "--fail-on", "any"]);
    assert!(stdout.contains("found  no difference"), "{stdout}");
}

#[test]
fn scan_takes_one_row_or_more_and_fast_takes_parquet_only() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&before), "--scan", "0"]);
    assert_eq!(code, 2, "a usage error: {stderr}");
    let (code, _, stderr) = run(&["diff", arg(&before), arg(&before), "--fast"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("drop `--fast`"), "{stderr}");
}

/// `.md` asks for Markdown of `diff` alone; every other command writes its text to it, as before.
#[test]
fn an_md_file_is_markdown_only_for_diff() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", BEFORE);
    let schema = dir.path().join("schema.md");
    let (code, _, stderr) = run(&["schema", arg(&before), "--out", arg(&schema)]);
    assert_eq!(code, 0, "{stderr}");
    let text = std::fs::read_to_string(&schema).unwrap();
    assert!(text.starts_with("source : "), "{text}");
    let (code, _, _) = run(&["schema", arg(&before), "-o", "markdown"]);
    assert_eq!(code, 1, "asked for by name, it is still refused");
}

/// Keys are matched on equal values, and a float holds every integer only up to 2^53: two integer
/// keys would match one float key, and a row would go uncounted. Refused.
#[cfg(feature = "sql")]
#[test]
fn an_integer_key_is_never_matched_against_a_float_key() {
    let dir = tempfile::tempdir().unwrap();
    let left = write(
        dir.path(),
        "left.csv",
        "id,v\n9007199254740992,a\n9007199254740993,b\n",
    );
    let right = write(dir.path(), "right.csv", "id,v\n9007199254740992.0,a\n");
    let (code, _, stderr) = run(&["diff", arg(&left), arg(&right), "--key", "id"]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("the key column `id` cannot be matched: Int64 and Float64"),
        "{stderr}"
    );
}

/// A dictionary of floats is a column of floats: as a key it is not matched against integers either.
#[cfg(feature = "sql")]
#[test]
fn a_dictionary_of_floats_is_a_float_key() {
    use arrow_array::types::Int32Type;
    use arrow_array::{DictionaryArray, Float64Array, Int32Array, Int64Array};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let floats = DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![0, 1]),
        Arc::new(Float64Array::from(vec![9007199254740992.0, 1.0])),
    );
    let integers = Int64Array::from(vec![9007199254740992, 9007199254740993]);
    let left = arrow_file(dir.path(), "left.arrow", Arc::new(floats));
    let right = arrow_file(dir.path(), "right.arrow", Arc::new(integers));
    let (code, _, stderr) = run(&["diff", arg(&left), arg(&right), "--key", "v"]);
    assert_eq!(code, 1, "{stderr}");
    assert!(
        stderr
            .contains("the key column `v` cannot be matched: Dictionary(Int32, Float64) and Int64"),
        "{stderr}"
    );
}

/// Rows matched by key are rows counted: a sample that counted nothing still fails `row-count`.
#[cfg(feature = "sql")]
#[test]
fn rows_matched_by_key_count_the_rows_a_sample_did_not() {
    let dir = tempfile::tempdir().unwrap();
    let one = write(dir.path(), "one.csv", "id\n1\n");
    let two = write(dir.path(), "two.csv", "id\n1\n2\n");
    let (code, stdout, stderr) = run(&[
        "diff",
        arg(&one),
        arg(&two),
        "--scan",
        "1",
        "--key",
        "id",
        "--fail-on",
        "row-count",
    ]);
    assert_eq!(code, 3, "{stdout}{stderr}");
    assert!(stdout.contains("  1 → 2  (+1, +100.00%)"), "{stdout}");
}

/// Two fixed-size binaries of different widths have no common type in the SQL engine: listed as
/// not compared, and the rest of the rows are still compared.
#[cfg(feature = "sql")]
#[test]
fn fixed_size_binaries_of_two_widths_are_listed_not_compared() {
    use arrow_array::FixedSizeBinaryArray;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let narrow =
        FixedSizeBinaryArray::try_from_iter(vec![b"ab".to_vec(), b"cd".to_vec()].into_iter())
            .unwrap();
    let wide =
        FixedSizeBinaryArray::try_from_iter(vec![b"abc".to_vec(), b"cde".to_vec()].into_iter())
            .unwrap();
    let left = arrow_file(dir.path(), "left.arrow", Arc::new(narrow));
    let right = arrow_file(dir.path(), "right.arrow", Arc::new(wide));
    let report = json(&left, &right, &["--key", "id"]);
    let k = &report["keyed"];
    assert_eq!(k["not_compared"][0]["name"], "v", "{k}");
    assert_eq!(k["unchanged"], 2);
}

/// A value a table holds cannot drive the terminal the text report is read in: an escape that
/// would clear the screen is written as text. JSON carries the value whole.
#[test]
fn the_text_report_writes_control_characters_as_escapes_and_json_keeps_them() {
    let dir = tempfile::tempdir().unwrap();
    let before = write(dir.path(), "before.csv", "id,v\na,x\n");
    let after = write(dir.path(), "after.csv", "id,v\na,\"\u{1b}[2Jgone\"\n");
    let (code, text, stderr) = run(&["diff", arg(&before), arg(&after)]);
    assert_eq!(code, 0, "{stderr}");
    assert!(!text.contains('\u{1b}'), "{text:?}");
    assert!(text.contains("\\x1b[2Jgone"), "{text}");
    let report = json(&before, &after, &[]);
    let v = report["stats"]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "v")
        .unwrap()
        .clone();
    assert_eq!(v["right"]["max"], "\u{1b}[2Jgone");
}

/// The Delta table delta-rs wrote, read from its checkpoint: versions 2 (4 rows) and 3 (5 rows).
#[cfg(feature = "delta")]
fn checkpointed() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/delta_checkpointed")
}

/// A copy of the fixture at `to`.
#[cfg(feature = "delta")]
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// `<table>@<snapshot>` through the binary: a version against the table now, by metadata, each
/// side showing the version it was compared at, and `--fail-on` turning what it found into exit 3.
#[cfg(feature = "delta")]
#[test]
fn a_version_against_the_table_now_through_the_binary() {
    let table = checkpointed();
    let at_two = format!("{}@2", table.display());
    let (code, stdout, stderr) = run(&["diff", &at_two, arg(&table), "--fail-on", "row-count"]);
    assert_eq!(code, 3, "{stdout}{stderr}");
    assert!(stdout.contains("delta_checkpointed@2"), "{stdout}");
    assert!(stdout.contains("delta_checkpointed@3"), "{stdout}");
    assert!(stdout.contains("4 → 5"), "{stdout}");
    assert!(stdout.contains("table metadata"), "{stdout}");

    // Anything but `diff` refuses a snapshot, pointing at it.
    let (code, _, stderr) = run(&["schema", &at_two]);
    assert_eq!(code, 1);
    assert!(stderr.contains("lakeleto diff"), "{stderr}");
    // As do `diff`'s row-reading flags.
    let (code, _, stderr) = run(&["diff", &at_two, arg(&table), "--scan", "10"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("`--scan`"), "{stderr}");
}

/// A directory whose name ends in `@2` is that directory: a name that exists whole is a path.
#[cfg(feature = "delta")]
#[test]
fn a_directory_named_with_an_at_is_a_path_not_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let named = dir.path().join("t@2");
    copy_dir(&checkpointed(), &named);
    let report = json(&named, &named, &[]);
    assert_ne!(report["stats"]["method"], "metadata", "{report}");
    assert!(report["left"].get("snapshot").is_none(), "{report}");
    assert_eq!(report["rows"]["left"], 5, "{report}");
}
