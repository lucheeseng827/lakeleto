//! OpenLineage events, driven through the binary the way a pipeline runs it: `check`, `diff` and
//! an export sent as START and then COMPLETE or FAIL, held to the published schemas (copies in
//! `tests/openlineage/`) and sent to an endpoint on this machine. Nothing is sent unless an
//! endpoint is given, and nothing that goes wrong sending changes what the command did.
//!
//! Run with: `cargo test --features openlineage --test openlineage`.
// Without the feature only the refusal is tested, and most of the helpers have nothing to do.
#![cfg_attr(not(feature = "openlineage"), allow(dead_code))]

use std::path::{Path, PathBuf};
use std::process::Command;

#[path = "support/openlineage.rs"]
mod openlineage;
#[allow(unused_imports)]
use openlineage::*;
#[allow(unused_imports)]
use serde_json::{json, Value};

/// The key the runs authenticate with: never in what a command prints.
const KEY: &str = "ol_key_3f9a71c2";

fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

fn arg(path: &Path) -> &str {
    path.to_str().unwrap()
}

/// Run `lakeleto` with `env` and the key, and none of the shell's OpenLineage settings: its exit
/// code, stdout and stderr.
fn run(args: &[&str], env: &[(&str, &str)]) -> (i32, String, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lakeleto"));
    command
        .env_remove("LAKELETO_REMOTE_URL")
        .env_remove("OPENLINEAGE_URL")
        .env_remove("OPENLINEAGE_NAMESPACE")
        .env("OPENLINEAGE_API_KEY", KEY)
        .args(args);
    for (k, v) in env {
        command.env(k, v);
    }
    let out = command.output().expect("lakeleto runs");
    let (stdout, stderr) = (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    );
    for printed in [&stdout, &stderr] {
        assert!(!printed.contains(KEY), "the key was printed: {printed}");
    }
    (out.status.code().expect("an exit code"), stdout, stderr)
}

fn orders(dir: &Path) -> PathBuf {
    write(dir, "orders.csv", "id,amount\n1,9.5\n2,\n3,12\n")
}

/// `orders.parquet`, an id and an amount for each of `amounts`: a check counts it from its footer
/// in any build, where a CSV takes the SQL engine.
fn orders_parquet(dir: &Path, amounts: &[Option<f64>]) -> PathBuf {
    use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
    use std::sync::Arc;
    let path = dir.join("orders.parquet");
    let ids: Vec<i64> = (1..=amounts.len() as i64).collect();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
        (
            "amount",
            Arc::new(Float64Array::from(amounts.to_vec())) as ArrayRef,
        ),
    ])
    .unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

const AMOUNTS: &[Option<f64>] = &[Some(9.5), None, Some(12.0)];

#[cfg(feature = "openlineage")]
fn bodies(received: &[Received]) -> Vec<Value> {
    received.iter().map(Received::json).collect()
}

/// `{namespace, name}` of a file on this machine.
#[cfg(feature = "openlineage")]
fn file(path: &Path) -> (String, String) {
    ("file".to_string(), path.display().to_string())
}

#[cfg(feature = "openlineage")]
#[test]
fn a_check_is_a_start_and_a_complete_that_say_what_it_found() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    let (url, rx) = endpoint(201);
    let to = format!("{url}/api/v1/lineage");
    let (code, _, stderr) = run(
        &["--openlineage", &to, "check", arg(&t), "--max-age", "1d"],
        &[],
    );
    assert_eq!(code, 0, "{stderr}");
    let received = sent(&rx, 2);
    for r in &received {
        assert_eq!(r.request_line, "POST /api/v1/lineage HTTP/1.1");
        assert_eq!(
            r.header("authorization"),
            Some(format!("Bearer {KEY}").as_str())
        );
        assert_eq!(r.header("content-type"), Some("application/json"));
    }
    let events = bodies(&received);
    assert_run(&events, "COMPLETE");
    assert_eq!(
        events[0]["job"],
        json!({ "namespace": "lakeleto", "name": format!("check file:{}", t.display()) })
    );
    assert_eq!(names(&events[0], "inputs"), [file(&t)]);
    assert_eq!(names(&events[1], "inputs"), [file(&t)]);
    assert_eq!(events[1]["outputs"], json!([]));
    assert_eq!(
        events[0]["producer"],
        format!(
            "https://github.com/lucheeseng827/lakeleto/tree/v{}",
            env!("CARGO_PKG_VERSION")
        ),
        "the release, where it is tagged"
    );

    let facets = &events[1]["inputs"][0]["inputFacets"];
    // Freshness judged and passed; the gates with no baseline to judge by are no assertion.
    assert_eq!(
        facets["dataQualityAssertions"]["assertions"],
        json!([{ "assertion": "freshness", "success": true, "severity": "error" }])
    );
    let metrics = &facets["dataQualityMetrics"];
    assert_eq!(metrics["rowCount"], 3);
    assert_eq!(
        metrics["columnMetrics"],
        json!({ "id": { "nullCount": 0 }, "amount": { "nullCount": 1 } })
    );
    assert_eq!(metrics["fileCount"], 1);
    assert_eq!(metrics["bytes"], std::fs::metadata(&t).unwrap().len());
    assert!(metrics["lastUpdated"].as_str().unwrap().ends_with('Z'));
    assert!(
        stderr.contains(&format!(
            "lakeleto: openlineage: START and COMPLETE sent → {to}"
        )),
        "{stderr}"
    );
}

#[cfg(feature = "openlineage")]
#[test]
fn a_failed_gate_completes_and_a_check_that_errs_fails() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    let baseline = dir.path().join("baseline.json");
    let (url, rx) = endpoint(200);
    let to = format!("{url}/lineage");
    let (code, _, stderr) = run(
        &["check", arg(&t), "--baseline", arg(&baseline), "--update"],
        &[],
    );
    assert_eq!(code, 0, "{stderr}");
    nothing_sent(&rx);

    // A third of the rows gone, past a 10% limit: the gate fails, and the run completed.
    orders_parquet(dir.path(), &AMOUNTS[..2]);
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &to,
            "check",
            arg(&t),
            "--baseline",
            arg(&baseline),
            "--max-row-drop",
            "10",
        ],
        &[],
    );
    assert_eq!(code, 3, "{stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "COMPLETE");
    let assertions = &events[1]["inputs"][0]["inputFacets"]["dataQualityAssertions"]["assertions"];
    let verdict = |gate: &str| {
        assertions
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["assertion"] == gate)
            .map(|a| a["success"].clone())
    };
    assert_eq!(verdict("row-count"), Some(json!(false)), "{assertions}");
    assert_eq!(verdict("schema"), Some(json!(true)), "{assertions}");
    assert_eq!(verdict("freshness"), None, "not asked: {assertions}");

    // A check that cannot read its table is a run that failed, naming what it was to read, with
    // nothing found of it.
    let missing = dir.path().join("missing.parquet");
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &to,
            "check",
            arg(&missing),
            "--max-age",
            "1d",
        ],
        &[],
    );
    assert_eq!(code, 1, "{stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "FAIL");
    assert_eq!(names(&events[1], "inputs"), [file(&missing)]);
    assert!(events[1]["inputs"][0].get("inputFacets").is_none());
    assert!(stderr.contains("START and FAIL sent"), "{stderr}");
}

#[cfg(feature = "openlineage")]
#[test]
fn a_diff_names_both_sides_and_one_that_found_a_change_completed() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(dir.path(), "a.csv", "id\n1\n2\n");
    let b = write(dir.path(), "b.csv", "id,note\n1,x\n");
    let (url, rx) = endpoint(200);
    let to = format!("{url}/lineage");
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &to,
            "diff",
            arg(&a),
            arg(&b),
            "--fail-on",
            "schema",
        ],
        &[],
    );
    assert_eq!(code, 3, "the schemas differ: {stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "COMPLETE");
    assert_eq!(names(&events[0], "inputs"), [file(&a), file(&b)]);
    assert_eq!(
        events[0]["job"]["name"],
        format!("diff file:{} file:{}", a.display(), b.display())
    );
    assert!(events[1]["inputs"][0].get("inputFacets").is_none());

    // Refused before either side is read, and still named: the run was asked of them.
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &to,
            "diff",
            arg(&a),
            arg(&b),
            "--fail-on",
            "rows",
        ],
        &[],
    );
    assert_eq!(code, 1, "{stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "FAIL");
    assert_eq!(names(&events[1], "inputs"), [file(&a), file(&b)]);
}

#[cfg(feature = "openlineage")]
#[test]
fn a_diff_side_at_a_snapshot_is_its_table() {
    let dir = tempfile::tempdir().unwrap();
    let t = write(dir.path(), "t.csv", "id\n1\n");
    let (url, rx) = endpoint(200);
    let to = format!("{url}/lineage");
    let at = format!("{}@2", t.display());
    // A CSV has no snapshot, so the diff is refused; the side it names is the file, which is the
    // other side too: one input, read twice.
    let (code, _, stderr) = run(&["--openlineage", &to, "diff", &at, arg(&t)], &[]);
    assert_eq!(code, 1, "{stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "FAIL");
    assert_eq!(names(&events[1], "inputs"), [file(&t)]);
    assert_eq!(
        events[1]["job"]["name"],
        format!("diff file:{}", t.display())
    );
}

#[cfg(feature = "openlineage")]
#[test]
fn an_export_names_its_output_once_it_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders(dir.path());
    let out = dir.path().join("head.csv");
    let (url, rx) = endpoint(200);
    let to = format!("{url}/lineage");
    let (code, _, stderr) = run(
        &["--openlineage", &to, "--out", arg(&out), "head", arg(&t)],
        &[],
    );
    assert_eq!(code, 0, "{stderr}");
    assert!(out.is_file());
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "COMPLETE");
    assert_eq!(names(&events[1], "inputs"), [file(&t)]);
    assert_eq!(names(&events[1], "outputs"), [file(&out)]);
    assert_eq!(names(&events[0], "outputs"), [file(&out)]);
    assert_eq!(
        events[0]["job"]["name"],
        format!("export file:{}", out.display())
    );

    // A failed export wrote no file, so it names none as an output; its job is the export to that
    // file all the same, so its failures and successes are one job's runs.
    let missing = dir.path().join("missing.csv");
    let never = dir.path().join("never.csv");
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &to,
            "--out",
            arg(&never),
            "head",
            arg(&missing),
        ],
        &[],
    );
    assert_eq!(code, 1, "{stderr}");
    assert!(!never.exists());
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "FAIL");
    assert_eq!(names(&events[1], "outputs"), []);
    assert_eq!(names(&events[1], "inputs"), [file(&missing)]);
    assert_eq!(
        events[1]["job"]["name"],
        format!("export file:{}", never.display())
    );

    // Printing rows is no export, and sends nothing.
    let (code, _, stderr) = run(&["head", arg(&t)], &[("OPENLINEAGE_URL", &url)]);
    assert_eq!(code, 0, "{stderr}");
    nothing_sent(&rx);
}

#[cfg(feature = "openlineage")]
#[test]
fn a_run_refused_before_it_reads_names_what_it_was_asked_to() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    let (url, rx) = endpoint(200);
    let to = format!("{url}/lineage");
    // A check prints no CSV: refused before the table is opened.
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &to,
            "-o",
            "csv",
            "check",
            arg(&t),
            "--max-age",
            "1d",
        ],
        &[],
    );
    assert_eq!(code, 1, "{stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "FAIL");
    assert_eq!(names(&events[1], "inputs"), [file(&t)]);
    assert_eq!(
        events[1]["job"]["name"],
        format!("check file:{}", t.display())
    );
    // An `--out` that cannot be written: the export to it failed.
    let out = dir.path().join("no-such-dir").join("head.csv");
    let (code, _, stderr) = run(
        &["--openlineage", &to, "--out", arg(&out), "head", arg(&t)],
        &[],
    );
    assert_eq!(code, 1, "{stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "FAIL");
    assert_eq!(names(&events[1], "inputs"), [file(&t)]);
    assert_eq!(names(&events[1], "outputs"), []);
    assert_eq!(
        events[1]["job"]["name"],
        format!("export file:{}", out.display())
    );
}

#[cfg(all(feature = "openlineage", feature = "sql", feature = "parquet-out"))]
#[test]
fn a_query_written_to_parquet_names_each_table_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders(dir.path());
    let more = write(dir.path(), "more.csv", "id\n4\n");
    let out = dir.path().join("q.parquet");
    let (url, rx) = endpoint(200);
    let to = format!("{url}/lineage");
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &to,
            "-o",
            "parquet",
            "--out",
            arg(&out),
            "query",
            "SELECT id FROM o UNION ALL SELECT id FROM m",
            "--table",
            &format!("o={}", t.display()),
            "--table",
            &format!("m={}", more.display()),
        ],
        &[],
    );
    assert_eq!(code, 0, "{stderr}");
    let events = bodies(&sent(&rx, 2));
    assert_run(&events, "COMPLETE");
    assert_eq!(names(&events[1], "inputs"), [file(&t), file(&more)]);
    assert_eq!(names(&events[1], "outputs"), [file(&out)]);
}

#[cfg(feature = "openlineage")]
#[test]
fn the_environment_names_a_base_and_the_flag_names_the_endpoint() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    let (url, rx) = endpoint(200);
    // OPENLINEAGE_URL is the server, as dagron and the OpenLineage clients read it.
    let (code, _, stderr) = run(
        &["check", arg(&t), "--max-age", "1d"],
        &[
            ("OPENLINEAGE_URL", &format!("{url}/")),
            ("OPENLINEAGE_NAMESPACE", "nightly"),
        ],
    );
    assert_eq!(code, 0, "{stderr}");
    let received = sent(&rx, 2);
    assert_eq!(received[0].request_line, "POST /api/v1/lineage HTTP/1.1");
    let events = bodies(&received);
    assert_run(&events, "COMPLETE");
    assert_eq!(events[0]["job"]["namespace"], "nightly");

    // The flag wins, and is the endpoint itself.
    let (other, other_rx) = endpoint(200);
    let (code, _, stderr) = run(
        &[
            "--openlineage",
            &format!("{other}/events"),
            "check",
            arg(&t),
            "--max-age",
            "1d",
        ],
        &[("OPENLINEAGE_URL", &url)],
    );
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(sent(&other_rx, 2)[0].request_line, "POST /events HTTP/1.1");
    nothing_sent(&rx);
}

#[cfg(feature = "openlineage")]
#[test]
fn an_endpoint_the_rule_refuses_is_refused_by_the_flag_and_warned_of_from_the_environment() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    for (to, why) in [
        (
            "http://lineage.example.com/api/v1/lineage",
            "plain http only to this machine",
        ),
        (
            "http://user:s3cret@127.0.0.1:9/x",
            "credentials do not go in a URL",
        ),
        ("ftp://127.0.0.1/x", "not an http(s) URL"),
    ] {
        // The flag is refused before anything is read.
        let (code, stdout, stderr) = run(
            &["--openlineage", to, "check", arg(&t), "--max-age", "1d"],
            &[],
        );
        assert_eq!(code, 1, "{to}: {stderr}");
        assert!(stdout.is_empty(), "{stdout}");
        assert!(stderr.contains("cannot send to --openlineage"), "{stderr}");
        assert!(stderr.contains(why), "{to}: {stderr}");
        assert!(!stderr.contains("s3cret"), "{stderr}");
        // The environment's is a warning: a scheduler set it for every command, and the check
        // stands.
        let (code, _, stderr) = run(
            &["check", arg(&t), "--max-age", "1d"],
            &[("OPENLINEAGE_URL", to)],
        );
        assert_eq!(code, 0, "{to}: {stderr}");
        assert!(
            stderr.contains("OPENLINEAGE_URL is set, but no OpenLineage event is sent"),
            "{stderr}"
        );
        assert!(stderr.contains(why), "{to}: {stderr}");
        assert!(!stderr.contains("s3cret"), "{stderr}");
    }
}

#[cfg(feature = "openlineage")]
#[test]
fn nothing_is_sent_by_default_or_by_an_empty_setting() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    for env in [vec![], vec![("OPENLINEAGE_URL", "")]] {
        let (code, _, stderr) = run(&["check", arg(&t), "--max-age", "1d"], &env);
        assert_eq!(code, 0, "{stderr}");
        assert!(!stderr.to_lowercase().contains("openlineage"), "{stderr}");
    }
}

#[cfg(feature = "openlineage")]
#[test]
fn an_endpoint_that_fails_changes_nothing_but_a_warning() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    let (_, plain, _) = run(&["check", arg(&t), "--max-age", "1d"], &[]);
    let (refusing, refusing_rx) = endpoint(500);
    let (moving, moving_rx) = endpoint(302);
    for (to, why) in [
        (format!("{refusing}/lineage"), "HTTP 500".to_string()),
        (
            format!("{moving}/lineage"),
            "a redirect is not followed".to_string(),
        ),
        // Port 9 on loopback: nothing listens.
        ("http://127.0.0.1:9/lineage".to_string(), String::new()),
    ] {
        let (code, stdout, stderr) = run(
            &["--openlineage", &to, "check", arg(&t), "--max-age", "1d"],
            &[],
        );
        assert_eq!(code, 0, "{to}: {stderr}");
        // The report is the check's, whatever became of its events, but for the time it read.
        assert_eq!(
            stdout.lines().count(),
            plain.lines().count(),
            "{stdout}\n{plain}"
        );
        assert!(
            stderr.contains(&format!(
                "lakeleto: warning: OpenLineage events not sent to {to}"
            )),
            "{stderr}"
        );
        assert!(stderr.contains(&why), "{to}: {stderr}");
    }
    // A refused START is the end of it: no COMPLETE follows a run the endpoint never began.
    assert_eq!(sent(&refusing_rx, 1).len(), 1);
    assert_eq!(sent(&moving_rx, 1).len(), 1);
}

#[cfg(feature = "openlineage")]
#[test]
fn the_flag_on_a_command_that_sends_nothing_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders(dir.path());
    let (url, rx) = endpoint(200);
    let (code, _, stderr) = run(&["--openlineage", &url, "schema", arg(&t)], &[]);
    assert_eq!(code, 1, "{stderr}");
    assert!(stderr.contains("`schema` is none of them"), "{stderr}");
    // The environment's applies only where a run is sent.
    let (code, _, stderr) = run(&["schema", arg(&t)], &[("OPENLINEAGE_URL", &url)]);
    assert_eq!(code, 0, "{stderr}");
    assert!(!stderr.contains("OpenLineage"), "{stderr}");
    nothing_sent(&rx);
}

#[cfg(not(feature = "openlineage"))]
#[test]
fn without_the_feature_the_flag_is_refused_and_the_environment_warned_of() {
    let dir = tempfile::tempdir().unwrap();
    let t = orders_parquet(dir.path(), AMOUNTS);
    let (code, stdout, stderr) = run(
        &[
            "--openlineage",
            "http://127.0.0.1:9/x",
            "check",
            arg(&t),
            "--max-age",
            "1d",
        ],
        &[],
    );
    assert_eq!(code, 1, "{stderr}");
    assert!(stdout.is_empty());
    assert!(stderr.contains("`openlineage` feature"), "{stderr}");
    let (code, _, stderr) = run(
        &["check", arg(&t), "--max-age", "1d"],
        &[("OPENLINEAGE_URL", "http://127.0.0.1:9")],
    );
    assert_eq!(code, 0, "{stderr}");
    assert!(
        stderr.contains("this build has no `openlineage` feature"),
        "{stderr}"
    );
}

#[cfg(all(feature = "openlineage", feature = "contracts"))]
mod contracts {
    use super::*;

    /// Orders: a unique id, an amount of at least 0, and a currency of two.
    const CONTRACT: &str =
        "covenant: 1\nid: orders\nversion: 1.0.0\nmodels:\n  orders:\n    fields:\n      \
        id:\n        type: integer\n        required: true\n        unique: true\n      \
        amount:\n        type: float\n        required: true\n        min: 0\n      \
        currency:\n        type: string\n        required: true\n        allowed: [USD, EUR]\n";

    fn assertion(event: &Value, rule: &str, column: Option<&str>) -> Value {
        event["inputs"][0]["inputFacets"]["dataQualityAssertions"]["assertions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["assertion"] == rule && a.get("column").and_then(Value::as_str) == column)
            .unwrap_or_else(|| panic!("no {rule} assertion on {column:?}: {event:#}"))
            .clone()
    }

    #[test]
    fn a_contract_asserts_each_rule_it_declares_as_covenant_does() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(dir.path(), "orders.yaml", CONTRACT);
        let t = write(
            dir.path(),
            "t.csv",
            "id,amount,currency\n1,9.5,USD\n1,-2,BTC\n3,12,EUR\n",
        );
        let (url, rx) = endpoint(200);
        let to = format!("{url}/lineage");
        let (code, _, stderr) = run(
            &[
                "--openlineage",
                &to,
                "check",
                arg(&t),
                "--contract",
                arg(&contract),
            ],
            &[],
        );
        assert_eq!(code, 3, "{stderr}");
        let events = bodies(&sent(&rx, 2));
        assert_run(&events, "COMPLETE");
        let end = &events[1];
        assert_eq!(assertion(end, "contract", None)["success"], false);
        assert_eq!(assertion(end, "unique", Some("id"))["success"], false);
        assert_eq!(assertion(end, "min", Some("amount"))["success"], false);
        assert_eq!(
            assertion(end, "allowed", Some("currency"))["success"],
            false
        );
        assert_eq!(
            assertion(end, "required_missing", Some("id"))["success"],
            true
        );
        assert_eq!(
            assertion(end, "type_mismatch", Some("amount"))["success"],
            true
        );
        assert_eq!(
            end["inputs"][0]["inputFacets"]["dataQualityMetrics"]["rowCount"],
            3
        );
        assert_eq!(assertion(end, "unique", Some("id"))["severity"], "error");
    }

    #[test]
    fn a_warn_contracts_rules_are_asserted_at_warn_as_covenant_asserts_them() {
        let dir = tempfile::tempdir().unwrap();
        let contract = write(
            dir.path(),
            "orders.yaml",
            &format!("{CONTRACT}policy:\n  on_violation: warn\n"),
        );
        let t = write(
            dir.path(),
            "t.csv",
            "id,amount,currency\n1,9.5,USD\n2,-2,EUR\n",
        );
        let (url, rx) = endpoint(200);
        let to = format!("{url}/lineage");
        let (_, _, stderr) = run(
            &[
                "--openlineage",
                &to,
                "check",
                arg(&t),
                "--contract",
                arg(&contract),
            ],
            &[],
        );
        let events = bodies(&sent(&rx, 2));
        assert_run(&events, "COMPLETE");
        let end = &events[1];
        let min = assertion(end, "min", Some("amount"));
        assert_eq!(min["success"], false, "{stderr}");
        assert_eq!(min["severity"], "warn");
        assert_eq!(assertion(end, "unique", Some("id"))["severity"], "warn");
        // The gate is Lakeleto's, and it failed the check.
        assert_eq!(assertion(end, "contract", None)["severity"], "error");
    }
}

#[cfg(all(feature = "openlineage", feature = "serve"))]
mod serve {
    use super::*;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn router(home: &Path, to: Option<&str>) -> axum::Router {
        confined(home, None, to)
    }

    /// A server whose file access is confined to `root`, when it is given.
    fn confined(home: &Path, root: Option<&Path>, to: Option<&str>) -> axum::Router {
        lakeleto::api::router_with_lineage(
            lakeleto::EngineRegistry::local(),
            1000,
            None,
            root.map(|r| r.canonicalize().unwrap()),
            true,
            Arc::new(lakeleto::workspace::LocalStore::at(home).unwrap()),
            None,
            to.map(|to| lakeleto::lineage::Destination::parse(to).unwrap()),
        )
    }

    async fn get(router: axum::Router, uri: &str) -> axum::http::StatusCode {
        router
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    fn export(path: &Path) -> String {
        format!(
            "/v1/export?path={}&fmt=csv",
            lakeleto::api::encode_query(arg(path))
        )
    }

    #[tokio::test]
    async fn an_export_from_the_server_is_a_run_that_names_what_it_read() {
        let dir = tempfile::tempdir().unwrap();
        let t = orders(dir.path());
        let (url, rx) = endpoint(200);
        let to = format!("{url}/lineage");
        let status = get(router(dir.path(), Some(&to)), &export(&t)).await;
        assert_eq!(status, 200);
        let events = tokio::task::spawn_blocking(move || bodies(&sent(&rx, 2)))
            .await
            .unwrap();
        assert_run(&events, "COMPLETE");
        assert_eq!(names(&events[1], "inputs"), [file(&t)]);
        // The file is wherever the browser saved it, which the server never learns.
        assert_eq!(names(&events[1], "outputs"), []);
        assert_eq!(
            events[0]["job"]["name"],
            format!("export file:{}", t.display())
        );
    }

    #[tokio::test]
    async fn an_export_that_cannot_read_its_table_is_a_run_that_failed() {
        let dir = tempfile::tempdir().unwrap();
        let (url, rx) = endpoint(200);
        let to = format!("{url}/lineage");
        let missing = dir.path().join("missing.csv");
        let status = get(router(dir.path(), Some(&to)), &export(&missing)).await;
        assert_ne!(status, 200);
        let events = tokio::task::spawn_blocking(move || bodies(&sent(&rx, 2)))
            .await
            .unwrap();
        assert_run(&events, "FAIL");
        assert_eq!(names(&events[1], "inputs"), [file(&missing)]);
    }

    #[tokio::test]
    async fn a_server_sends_nothing_unless_it_was_given_an_endpoint_or_for_what_it_refused() {
        let dir = tempfile::tempdir().unwrap();
        let t = orders(dir.path());
        let (url, rx) = endpoint(200);
        let to = format!("{url}/lineage");
        assert_eq!(get(router(dir.path(), None), &export(&t)).await, 200);
        // Outside `--root`: refused before anything is read, so no run, and the name the caller
        // tried is not passed on.
        let root = dir.path().join("served");
        std::fs::create_dir(&root).unwrap();
        let status = get(confined(dir.path(), Some(&root), Some(&to)), &export(&t)).await;
        assert_eq!(status, 403);
        tokio::task::spawn_blocking(move || nothing_sent(&rx))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn an_endpoint_that_fails_leaves_the_export_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let t = orders(dir.path());
        let status = get(
            router(dir.path(), Some("http://127.0.0.1:9/lineage")),
            &export(&t),
        )
        .await;
        assert_eq!(status, 200);
    }
}
