//! The MCP server driven in memory: whole sessions through [`serve`], as a client would run them.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{json, Value};

use super::tools::SLEEP;
use super::*;
use crate::engine::registry::EngineRegistry;

/// The limits the tests run with, unless a test says otherwise.
fn limits() -> Limits {
    Limits {
        default_scan: 1_000,
        max_rows: 100,
        max_bytes: 32 * 1024,
        timeout: Duration::from_secs(30),
    }
}

/// A temporary root holding `people.parquet` (300 rows: `id`, `name`, `score`, with `score` null
/// on every tenth row) and `people.csv` (the same 300 rows).
struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
}

/// Write the fixture's two tables into a fresh temporary root.
fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let ids: Vec<i64> = (0..300).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("person-{i:03}")).collect();
    let scores: Vec<Option<f64>> = ids
        .iter()
        .map(|i| (i % 10 != 0).then_some(*i as f64 / 2.0))
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
            Arc::new(StringArray::from(names.clone())) as ArrayRef,
            Arc::new(Float64Array::from(scores.clone())) as ArrayRef,
        ],
    )
    .unwrap();
    let file = std::fs::File::create(root.join("people.parquet")).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut csv = String::from("id,name,score\n");
    for i in 0..300 {
        let score = scores[i].map(|s| s.to_string()).unwrap_or_default();
        csv.push_str(&format!("{},{},{}\n", ids[i], names[i], score));
    }
    std::fs::write(root.join("people.csv"), csv).unwrap();
    Fixture { _dir: dir, root }
}

/// This build's engines, confined to `root` if given.
fn tools(root: Option<&Path>, limits: Limits) -> Tools {
    Tools::new(EngineRegistry::local(), root.map(Path::to_path_buf), limits)
}

/// Run a session that sends `lines`, then closes its end; return every message the server wrote.
fn session_raw(tools: Tools, lines: &[String]) -> Vec<Value> {
    let input: String = lines.iter().map(|l| format!("{l}\n")).collect();
    let mut out = Vec::new();
    serve(Cursor::new(input.into_bytes()), &mut out, tools).unwrap();
    String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("every line the server writes is JSON"))
        .collect()
}

/// [`session_raw`] with JSON messages rather than raw lines.
fn session(tools: Tools, messages: &[Value]) -> Vec<Value> {
    let lines: Vec<String> = messages.iter().map(Value::to_string).collect();
    session_raw(tools, &lines)
}

/// A JSON-RPC request.
fn request(id: i64, method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

/// A `tools/call` request for tool `name`.
fn call(id: i64, name: &str, args: Value) -> Value {
    request(id, "tools/call", json!({ "name": name, "arguments": args }))
}

/// The reply to request `id`, which must be there.
fn reply(messages: &[Value], id: i64) -> &Value {
    messages
        .iter()
        .find(|m| m["id"] == json!(id))
        .unwrap_or_else(|| panic!("no reply to request {id} in {messages:?}"))
}

/// A tool call's result: whether it is an error, and its text parsed as JSON.
fn outcome(messages: &[Value], id: i64) -> (bool, Value) {
    let result = &reply(messages, id)["result"];
    let text = result["content"][0]["text"].as_str().expect("text content");
    (
        result["isError"].as_bool().unwrap(),
        serde_json::from_str(text).expect("the text is JSON"),
    )
}

/// Call one tool in a fresh session and return its outcome.
fn one_call(tools: Tools, name: &str, args: Value) -> (bool, Value) {
    let messages = session(tools, &[call(1, name, args)]);
    outcome(&messages, 1)
}

// ---- the lifecycle ---------------------------------------------------------------------------

#[test]
fn initialize_agrees_on_the_clients_revision_or_offers_the_newest() {
    for (asked, agreed) in [
        (json!("2025-11-25"), "2025-11-25"),
        (json!("2025-06-18"), "2025-06-18"),
        (json!("2025-03-26"), "2025-03-26"),
        (json!("2024-11-05"), "2024-11-05"),
        (json!("2026-07-28"), "2025-11-25"),
        (json!("1.0"), "2025-11-25"),
        (Value::Null, "2025-11-25"),
    ] {
        let messages = session(
            tools(None, limits()),
            &[request(
                1,
                "initialize",
                json!({ "protocolVersion": asked }),
            )],
        );
        let result = &reply(&messages, 1)["result"];
        assert_eq!(
            result["protocolVersion"],
            json!(agreed),
            "asked for {asked}"
        );
        assert_eq!(result["serverInfo"]["name"], json!("lakeleto"));
        assert_eq!(result["capabilities"]["tools"]["listChanged"], json!(false));
        assert!(result["instructions"]
            .as_str()
            .unwrap()
            .contains("describe"));
    }
}

#[test]
fn a_later_revisions_discovery_probe_gets_method_not_found_so_the_client_falls_back() {
    // A 2026-07-28 client asks `server/discover` first. Any error but its two modern rejection
    // codes marks a server of an earlier revision, and the client falls back to `initialize`.
    let messages = session(
        tools(None, limits()),
        &[request(1, "server/discover", json!({}))],
    );
    assert_eq!(
        reply(&messages, 1)["error"]["code"],
        json!(METHOD_NOT_FOUND)
    );
}

#[test]
fn ping_is_answered_and_notifications_are_not() {
    let messages = session(
        tools(None, limits()),
        &[
            json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
            json!({ "jsonrpc": "2.0", "method": "notifications/cancelled",
                    "params": { "requestId": 99 } }),
            // A response to a request of the server's (it sends none) is not answered either.
            json!({ "jsonrpc": "2.0", "id": 5, "result": {} }),
            request(1, "ping", json!({})),
        ],
    );
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert_eq!(reply(&messages, 1)["result"], json!({}));
}

#[test]
fn malformed_messages_get_json_rpc_errors() {
    let lines = [
        "this is not json".to_string(),
        json!([request(1, "ping", json!({}))]).to_string(),
        "42".to_string(),
        json!({ "jsonrpc": "1.0", "id": 2, "method": "ping" }).to_string(),
        json!({ "jsonrpc": "2.0", "id": null, "method": "ping" }).to_string(),
        json!({ "jsonrpc": "2.0", "id": 3 }).to_string(),
        String::new(),
    ];
    let messages = session_raw(tools(None, limits()), &lines);
    let codes: Vec<(Value, Value)> = messages
        .iter()
        .map(|m| (m["id"].clone(), m["error"]["code"].clone()))
        .collect();
    assert_eq!(
        codes,
        vec![
            (Value::Null, json!(PARSE_ERROR)),
            (Value::Null, json!(INVALID_REQUEST)),
            (Value::Null, json!(INVALID_REQUEST)),
            (json!(2), json!(INVALID_REQUEST)),
            (Value::Null, json!(INVALID_REQUEST)),
            (json!(3), json!(INVALID_REQUEST)),
        ],
        "a blank line gets no reply; every other line one error"
    );
}

#[test]
fn a_malformed_tool_call_is_a_protocol_error() {
    let messages = session(
        tools(None, limits()),
        &[
            request(1, "tools/call", json!({ "arguments": {} })),
            call(2, "drop_table", json!({})),
            request(3, "tools/call", json!({ "name": "list", "arguments": [1] })),
        ],
    );
    for id in 1..=3 {
        assert_eq!(reply(&messages, id)["error"]["code"], json!(INVALID_PARAMS));
    }
}

#[test]
fn tools_list_offers_what_this_build_and_root_can_run() {
    let names = |root: Option<&Path>| -> Vec<String> {
        let messages = session(
            tools(root, limits()),
            &[request(1, "tools/list", json!({}))],
        );
        let tools = reply(&messages, 1)["result"]["tools"]
            .as_array()
            .unwrap()
            .clone();
        for tool in &tools {
            assert_eq!(tool["inputSchema"]["type"], json!("object"));
            assert_eq!(tool["annotations"]["readOnlyHint"], json!(true));
        }
        tools
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    };
    let f = fixture();
    let open = names(None);
    let confined = names(Some(&f.root));
    for core in ["list", "describe", "preview", "profile"] {
        assert!(open.contains(&core.to_string()) && confined.contains(&core.to_string()));
    }
    // SQL runs on the SQL engine over files, or on a database's own engine over its tables.
    let queryable = cfg!(any(
        feature = "sql",
        feature = "sqlite",
        feature = "postgres",
        feature = "mysql"
    ));
    assert_eq!(open.contains(&"query".to_string()), queryable);
    assert_eq!(
        open.contains(&"catalog_ls".to_string()),
        cfg!(feature = "catalog")
    );
    // A root refuses every catalog reference, so the catalog tool isn't offered under one.
    assert!(!confined.contains(&"catalog_ls".to_string()));
    // The tests' own tool is callable, never listed.
    assert!(!open.contains(&SLEEP.to_string()));
}

// ---- the tools -------------------------------------------------------------------------------

#[test]
fn describe_answers_from_a_parquet_footer_without_reading_rows() {
    let f = fixture();
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "describe",
        json!({ "path": "people.parquet" }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["format"], json!("parquet"));
    assert_eq!(out["row_count"], json!(300));
    assert!(out["size_bytes"].as_u64().unwrap() > 0);
    assert!(out["statistics"].as_str().unwrap().contains("footer"));
    let columns = out["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(
        columns[0],
        json!({ "name": "id", "data_type": "Int64", "nullable": false,
                "null_count": 0, "min": "0", "max": "299" })
    );
    assert_eq!(columns[2]["null_count"], json!(30));
    assert_eq!(columns[2]["max"], json!("149.5"));
}

#[test]
fn describe_a_csv_infers_its_types_and_says_it_stores_no_statistics() {
    let f = fixture();
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "describe",
        json!({ "path": "people.csv" }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["format"], json!("csv"));
    assert_eq!(out["row_count"], Value::Null);
    assert!(out["statistics"]
        .as_str()
        .unwrap()
        .contains("profile scans rows"));
    let columns = out["columns"].as_array().unwrap();
    assert_eq!(columns[0]["data_type"], json!("Int64"));
    assert!(columns.iter().all(|c| c.get("min").is_none()));
}

#[test]
fn preview_returns_rows_as_arrays_in_column_order() {
    let f = fixture();
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "preview",
        json!({ "path": "people.parquet", "rows": 3, "offset": 10 }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["offset"], json!(10));
    assert_eq!(out["row_count"], json!(300));
    assert_eq!(
        out["columns"],
        json!([{ "name": "id", "data_type": "Int64" }, { "name": "name", "data_type": "Utf8" },
               { "name": "score", "data_type": "Float64" }])
    );
    // A null cell is a null in its place, not a missing key.
    assert_eq!(
        out["rows"],
        json!([
            [10, "person-010", null],
            [11, "person-011", 5.5],
            [12, "person-012", 6.0]
        ])
    );
    assert_eq!(out["truncated"], json!(true));
    assert!(out["note"].as_str().unwrap().contains("offset"));
}

#[test]
fn preview_of_the_last_rows_is_not_truncated_and_can_pick_columns() {
    let f = fixture();
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "preview",
        json!({ "path": "people.csv", "rows": "5", "offset": 298, "columns": ["name"] }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["rows"], json!([["person-298"], ["person-299"]]));
    assert!(out.get("truncated").is_none(), "{out}");
}

#[test]
fn rows_are_capped_by_count_and_by_bytes() {
    let f = fixture();
    // `--max-rows` caps what a call may ask for.
    let few = Limits {
        max_rows: 4,
        ..limits()
    };
    let (_, out) = one_call(
        tools(Some(&f.root), few),
        "preview",
        json!({ "path": "people.csv", "rows": 1000 }),
    );
    assert_eq!(out["rows"].as_array().unwrap().len(), 4);
    assert_eq!(out["truncated"], json!(true));
    // `--max-bytes` cuts the rows that don't fit, and the result says how many it left out.
    let small = Limits {
        max_bytes: 1024,
        ..limits()
    };
    let messages = session(
        tools(Some(&f.root), small),
        &[call(
            1,
            "preview",
            json!({ "path": "people.csv", "rows": 100 }),
        )],
    );
    let text = reply(&messages, 1)["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(text.len() <= 1024, "{} bytes", text.len());
    let (is_error, out) = outcome(&messages, 1);
    assert!(!is_error);
    let kept = out["rows"].as_array().unwrap().len();
    assert!(kept > 0 && kept < 100, "{kept} rows kept");
    assert_eq!(out["truncated"], json!(true));
    let note = out["note"].as_str().unwrap();
    assert!(
        note.starts_with(&format!("{} more rows", 100 - kept)),
        "{note}"
    );
}

#[test]
fn a_result_is_never_sent_over_the_byte_cap_even_when_its_columns_alone_would_be() {
    // Three hundred long column names: their header alone is about 20 KB.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let names: Vec<String> = (0..300)
        .map(|i| format!("measurement_with_a_rather_long_name_{i:03}"))
        .collect();
    let mut csv = names.join(",");
    csv.push('\n');
    for r in 0..3 {
        let cells: Vec<String> = (0..300).map(|c| (r * 300 + c).to_string()).collect();
        csv.push_str(&cells.join(","));
        csv.push('\n');
    }
    std::fs::write(root.join("wide.csv"), csv).unwrap();
    let small = Limits {
        max_bytes: 4096,
        ..limits()
    };
    // `preview` can't fit its columns, let alone a row: refused, not sent over the cap.
    let (is_error, out) = one_call(
        tools(Some(&root), small.clone()),
        "preview",
        json!({ "path": "wide.csv" }),
    );
    assert!(is_error, "{out}");
    assert_eq!(out["error"], json!("too_large"));
    assert!(out["message"].as_str().unwrap().contains("fewer columns"));
    // Asked for fewer columns, it fits.
    let (is_error, out) = one_call(
        tools(Some(&root), small.clone()),
        "preview",
        json!({ "path": "wide.csv", "columns": [names[0], names[299]] }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["rows"], json!([[0, 299], [300, 599], [600, 899]]));
    // `describe` cuts its columns to the cap instead, and says so.
    let messages = session(
        tools(Some(&root), small),
        &[call(1, "describe", json!({ "path": "wide.csv" }))],
    );
    let text = reply(&messages, 1)["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(text.len() <= 4096, "{} bytes", text.len());
    let (is_error, out) = outcome(&messages, 1);
    assert!(!is_error);
    assert_eq!(out["truncated"], json!(true));
}

#[test]
fn profile_scans_and_scan_zero_reads_the_footer() {
    let f = fixture();
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "profile",
        json!({ "path": "people.csv", "scan": 50 }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["scanned_rows"], json!(50));
    assert_eq!(out["columns"][0]["name"], json!("id"));
    let (_, out) = one_call(
        tools(Some(&f.root), limits()),
        "profile",
        json!({ "path": "people.parquet", "scan": 0 }),
    );
    assert_eq!(out["scanned_rows"], json!(0));
    assert_eq!(out["row_count"], json!(300));
}

#[test]
fn list_shows_the_roots_tables_without_empty_fields() {
    let f = fixture();
    std::fs::create_dir(f.root.join("archive")).unwrap();
    std::fs::write(f.root.join("notes.txt"), "not a table").unwrap();
    let (is_error, out) = one_call(tools(Some(&f.root), limits()), "list", json!({}));
    assert!(!is_error, "{out}");
    let names: Vec<&str> = out["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["archive", "people.csv", "people.parquet"]);
    let archive = &out["entries"][0];
    assert_eq!(archive["kind"], json!("dir"));
    assert!(archive.get("size").is_none() && archive.get("format").is_none());
}

#[test]
fn refusals_say_what_kind_they_are() {
    let f = fixture();
    let outside = tempfile::tempdir().unwrap();
    let elsewhere = outside.path().join("secret.csv");
    std::fs::write(&elsewhere, "a\n1\n").unwrap();
    let cases = [
        ("describe", json!({ "path": elsewhere }), "forbidden"),
        (
            "describe",
            json!({ "path": "/etc/passwd", "format": "csv" }),
            "forbidden",
        ),
        ("list", json!({ "path": outside.path() }), "forbidden"),
        (
            "preview",
            json!({ "path": "s3://bucket/t.parquet" }),
            "forbidden",
        ),
        (
            "describe",
            json!({ "path": "missing.parquet" }),
            "not_found",
        ),
        ("describe", json!({}), "invalid_arguments"),
        ("describe", json!({ "path": 7 }), "invalid_arguments"),
        (
            "preview",
            json!({ "path": "people.csv", "rows": -1 }),
            "invalid_arguments",
        ),
        (
            "preview",
            json!({ "path": "people.csv", "columns": "id" }),
            "invalid_arguments",
        ),
    ];
    for (tool, args, kind) in cases {
        let (is_error, out) = one_call(tools(Some(&f.root), limits()), tool, args.clone());
        assert!(is_error, "{tool} {args} was not refused: {out}");
        assert_eq!(out["error"], json!(kind), "{tool} {args}: {out}");
        assert!(out["message"].as_str().is_some_and(|m| !m.is_empty()));
    }
}

#[test]
fn a_deadline_the_engine_notices_is_a_deadline_refusal() {
    let f = fixture();
    let now = Limits {
        timeout: Duration::ZERO,
        ..limits()
    };
    let (is_error, out) = one_call(
        tools(Some(&f.root), now),
        "describe",
        json!({ "path": "people.parquet" }),
    );
    assert!(is_error);
    assert_eq!(out["error"], json!("deadline"));
}

#[cfg(feature = "sql")]
#[test]
fn query_runs_read_only_sql_over_the_tables_it_names() {
    let f = fixture();
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "query",
        json!({ "sql": "SELECT count(*) AS n, max(score) AS top FROM t WHERE score IS NOT NULL",
                "path": "people.parquet" }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["rows"], json!([[270, 149.5]]));
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "query",
        json!({ "sql": "SELECT a.id FROM a JOIN b ON a.id = b.id ORDER BY a.id LIMIT 2",
                "tables": [{ "name": "a", "path": "people.csv" },
                           { "name": "b", "path": "people.parquet" }] }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(out["rows"], json!([[0], [1]]));
    // One row past `rows` is what tells a cut result from a complete one.
    let (_, out) = one_call(
        tools(Some(&f.root), limits()),
        "query",
        json!({ "sql": "SELECT id FROM t", "path": "people.csv", "rows": 2 }),
    );
    assert_eq!(out["rows"], json!([[0], [1]]));
    assert_eq!(out["truncated"], json!(true));
    for (sql, kind) in [
        ("DROP TABLE t", "query"),
        ("SELECT 1; DELETE FROM t", "query"),
        ("INSERT INTO t VALUES (1, 'x', 1.0)", "query"),
    ] {
        let (is_error, out) = one_call(
            tools(Some(&f.root), limits()),
            "query",
            json!({ "sql": sql, "path": "people.csv" }),
        );
        assert!(is_error, "{sql}");
        assert_eq!(out["error"], json!(kind), "{sql}: {out}");
    }
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "query",
        json!({ "sql": "SELECT 1" }),
    );
    assert!(is_error);
    assert_eq!(out["error"], json!("invalid_arguments"));
}

#[cfg(feature = "sqlite")]
#[test]
fn a_databases_sql_runs_on_the_database_and_its_tables_are_listed() {
    // An empty file is a valid SQLite database, and a recursive CTE makes rows without a table, so
    // no fixture has to be written through a pool that opens the file read-only.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("empty.db");
    std::fs::File::create(&db).unwrap();
    let uri = format!("sqlite://{}", db.display());
    let numbers = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1000) \
                   SELECT i FROM n";
    let (is_error, out) = one_call(
        tools(None, limits()),
        "query",
        json!({ "sql": numbers, "path": uri, "rows": 3 }),
    );
    assert!(!is_error, "{out}");
    // A CTE's column has no declared type, which the database engine reads as text.
    assert_eq!(out["rows"], json!([["1"], ["2"], ["3"]]));
    assert_eq!(out["truncated"], json!(true));
    let (is_error, out) = one_call(
        tools(None, limits()),
        "query",
        json!({ "sql": "DELETE FROM n", "path": uri }),
    );
    assert!(is_error);
    assert_eq!(out["error"], json!("query"));
    let (is_error, out) = one_call(tools(None, limits()), "list", json!({ "path": uri }));
    assert!(!is_error, "{out}");
    assert_eq!(out["entries"], json!([]));
    // A root is local files only.
    let f = fixture();
    let (is_error, out) = one_call(
        tools(Some(&f.root), limits()),
        "list",
        json!({ "path": uri }),
    );
    assert!(is_error);
    assert_eq!(out["error"], json!("forbidden"));
}

// ---- concurrency, cancellation and deadlines ---------------------------------------------------

#[test]
fn a_slow_call_holds_up_neither_a_ping_nor_the_end_of_the_session() {
    let started = Instant::now();
    let messages = session(
        tools(None, limits()),
        &[
            call(1, SLEEP, json!({ "ms": 300 })),
            request(2, "ping", json!({})),
        ],
    );
    // The ping is answered first; the session still waits for the call before it ends.
    assert_eq!(messages[0]["id"], json!(2));
    assert_eq!(outcome(&messages, 1), (false, json!({ "slept": 300 })));
    assert!(started.elapsed() >= Duration::from_millis(300));
}

#[test]
fn a_cancelled_call_is_not_answered() {
    let messages = session(
        tools(None, limits()),
        &[
            call(7, SLEEP, json!({ "ms": 500 })),
            json!({ "jsonrpc": "2.0", "method": "notifications/cancelled",
                    "params": { "requestId": 7, "reason": "the user stopped it" } }),
            request(8, "ping", json!({})),
        ],
    );
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert_eq!(messages[0]["id"], json!(8));
}

#[test]
fn a_call_whose_engine_ignores_its_deadline_is_answered_when_it_passes() {
    let quick = Limits {
        timeout: Duration::from_millis(50),
        ..limits()
    };
    let started = Instant::now();
    let messages = session(
        tools(None, quick),
        &[call(1, SLEEP, json!({ "ms": 5_000 }))],
    );
    let elapsed = started.elapsed();
    let (is_error, out) = outcome(&messages, 1);
    assert!(is_error);
    assert_eq!(out["error"], json!("deadline"));
    // Answered at the deadline and its grace, not when the sleep would have ended.
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    assert!(elapsed >= Duration::from_millis(50) + GRACE, "{elapsed:?}");
}

#[test]
fn a_call_past_the_running_limit_is_refused_as_busy() {
    let mut messages: Vec<Value> = (1..=MAX_RUNNING as i64)
        .map(|id| call(id, SLEEP, json!({ "ms": 300 })))
        .collect();
    let over = MAX_RUNNING as i64 + 1;
    messages.push(call(over, SLEEP, json!({ "ms": 0 })));
    let replies = session(tools(None, limits()), &messages);
    assert_eq!(replies.len(), MAX_RUNNING + 1);
    let (is_error, out) = outcome(&replies, over);
    assert!(is_error);
    assert_eq!(out["error"], json!("busy"));
    for id in 1..=MAX_RUNNING as i64 {
        assert_eq!(outcome(&replies, id), (false, json!({ "slept": 300 })));
    }
}

#[test]
fn an_id_already_running_is_refused() {
    let messages = session(
        tools(None, limits()),
        &[
            call(1, SLEEP, json!({ "ms": 200 })),
            call(1, SLEEP, json!({ "ms": 0 })),
        ],
    );
    assert_eq!(messages.len(), 2, "{messages:?}");
    assert_eq!(messages[0]["error"]["code"], json!(INVALID_REQUEST));
    assert_eq!(messages[1]["result"]["isError"], json!(false));
}
