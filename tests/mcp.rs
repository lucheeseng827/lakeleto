//! `lakeleto mcp` as an MCP client runs it: the binary started as a child process, JSON-RPC
//! written to its stdin and read back from its stdout a line at a time.
//!
//! `cargo test --features mcp` (add `sql` for the query test).
#![cfg(feature = "mcp")]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{json, Value};

#[cfg(feature = "catalog")]
#[path = "support/rest_catalog.rs"]
mod rest_catalog;

/// A table directory holding `orders.parquet` (1,000 rows) and `orders.csv` (the same rows), and
/// the `LAKELETO_HOME` the server runs with, so no catalog configuration of the machine's leaks in.
struct Fixture {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    home: std::path::PathBuf,
}

/// Write the fixture's tables, and make its home, in a fresh temporary directory.
fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let base = std::fs::canonicalize(dir.path()).unwrap();
    let (root, home) = (base.join("tables"), base.join("home"));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let ids: Vec<i64> = (0..1_000).collect();
    let cities: Vec<&str> = ids
        .iter()
        .map(|i| ["Oslo", "Lima", "Pune"][*i as usize % 3])
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
            Arc::new(StringArray::from(cities.clone())) as ArrayRef,
        ],
    )
    .unwrap();
    let file = std::fs::File::create(root.join("orders.parquet")).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut csv = String::from("id,city\n");
    for (id, city) in ids.iter().zip(&cities) {
        csv.push_str(&format!("{id},{city}\n"));
    }
    std::fs::write(root.join("orders.csv"), csv).unwrap();
    Fixture {
        _dir: dir,
        root,
        home,
    }
}

/// A running `lakeleto mcp`.
struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: i64,
}

impl Server {
    /// `lakeleto mcp` with `args`, its `LAKELETO_HOME` at `home`.
    fn start(home: &Path, args: &[&str]) -> Server {
        Server::start_with(home, args, &[])
    }

    /// [`Server::start`] with more environment variables.
    fn start_with(home: &Path, args: &[&str], env: &[(&str, &str)]) -> Server {
        let mut child = Command::new(env!("CARGO_BIN_EXE_lakeleto"))
            .arg("mcp")
            .args(args)
            .env("LAKELETO_HOME", home)
            .envs(env.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start lakeleto mcp");
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Server {
            child,
            stdin,
            stdout,
            next_id: 1,
        }
    }

    /// Write one message to the server's stdin.
    fn send(&mut self, msg: &Value) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        writeln!(stdin, "{msg}").unwrap();
        stdin.flush().unwrap();
    }

    /// The next message on stdout. Every line the server writes must be one JSON-RPC message.
    fn receive(&mut self) -> Value {
        let mut line = String::new();
        let n = self.stdout.read_line(&mut line).unwrap();
        assert!(n > 0, "the server closed stdout");
        let msg: Value = serde_json::from_str(&line)
            .unwrap_or_else(|e| panic!("stdout carried a line that isn't JSON ({e}): {line}"));
        assert_eq!(msg["jsonrpc"], json!("2.0"), "{msg}");
        msg
    }

    /// Send a request and return its reply.
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        let reply = self.receive();
        assert_eq!(reply["id"], json!(id), "{reply}");
        reply
    }

    /// Call a tool: whether it was refused, and its result's text as JSON.
    fn call(&mut self, name: &str, arguments: Value) -> (bool, Value) {
        let reply = self.request(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        );
        let result = &reply["result"];
        let text = result["content"][0]["text"].as_str().expect("text content");
        (
            result["isError"].as_bool().unwrap(),
            serde_json::from_str(text).unwrap(),
        )
    }

    /// Close stdin, as a client shutting down does, and wait for the server to exit.
    fn close(mut self) -> ExitStatus {
        drop(self.stdin.take());
        self.child.wait().unwrap()
    }
}

#[test]
fn a_client_session_over_stdio() {
    let f = fixture();
    let root = f.root.to_str().unwrap();
    let mut server = Server::start(&f.home, &["--root", root, "--max-rows", "50"]);

    let init = server.request(
        "initialize",
        json!({ "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": { "name": "test", "version": "0" } }),
    );
    assert_eq!(init["result"]["protocolVersion"], json!("2025-11-25"));
    assert_eq!(
        init["result"]["serverInfo"]["version"],
        json!(env!("CARGO_PKG_VERSION"))
    );
    server.send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));

    let tools = server.request("tools/list", json!({}));
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(
        names.starts_with(&["list", "describe", "preview", "profile"]),
        "{names:?}"
    );
    // SQL runs on the SQL engine over files, or on a database's own engine over its tables.
    let queryable = cfg!(any(
        feature = "sql",
        feature = "sqlite",
        feature = "postgres",
        feature = "mysql"
    ));
    assert_eq!(names.contains(&"query"), queryable);
    assert!(
        !names.contains(&"catalog_ls"),
        "a root refuses catalogs: {names:?}"
    );

    // What is in the table, from its footer alone.
    let (is_error, described) = server.call("describe", json!({ "path": "orders.parquet" }));
    assert!(!is_error, "{described}");
    assert_eq!(described["row_count"], json!(1000));
    assert_eq!(described["columns"][0]["max"], json!("999"));
    assert_eq!(described["columns"][1]["min"], json!("Lima"));

    let (_, listed) = server.call("list", json!({}));
    assert_eq!(listed["dir"], json!(root));
    assert_eq!(listed["entries"].as_array().unwrap().len(), 2);

    // `--max-rows` caps what a call may ask for.
    let (_, rows) = server.call("preview", json!({ "path": "orders.csv", "rows": 500 }));
    assert_eq!(rows["rows"].as_array().unwrap().len(), 50);
    assert_eq!(rows["rows"][0], json!([0, "Oslo"]));
    assert_eq!(rows["truncated"], json!(true));

    let (_, profile) = server.call("profile", json!({ "path": "orders.csv" }));
    assert_eq!(profile["scanned_rows"], json!(1000));
    assert_eq!(profile["columns"][1]["distinct"], json!(3));

    // Outside the root is a structured refusal, not a protocol error.
    let (is_error, refused) =
        server.call("describe", json!({ "path": "/etc/hosts", "format": "csv" }));
    assert!(is_error);
    assert_eq!(refused["error"], json!("forbidden"));

    assert!(server.close().success());
}

#[cfg(feature = "sql")]
#[test]
fn query_over_stdio_is_read_only() {
    let f = fixture();
    let mut server = Server::start(&f.home, &["--root", f.root.to_str().unwrap()]);
    let (is_error, out) = server.call(
        "query",
        json!({ "sql": "SELECT city, count(*) AS n FROM t GROUP BY city ORDER BY city",
                "path": "orders.parquet" }),
    );
    assert!(!is_error, "{out}");
    assert_eq!(
        out["rows"],
        json!([["Lima", 333], ["Oslo", 334], ["Pune", 333]])
    );
    for sql in ["DROP TABLE t", "DELETE FROM t", "SELECT 1; DROP TABLE t"] {
        let (is_error, out) = server.call("query", json!({ "sql": sql, "path": "orders.csv" }));
        assert!(is_error, "{sql}");
        assert_eq!(out["error"], json!("query"), "{sql}: {out}");
    }
    assert!(server.close().success());
}

#[test]
fn a_bad_root_fails_at_startup() {
    let f = fixture();
    let missing = f.root.join("nope");
    let out = Command::new(env!("CARGO_BIN_EXE_lakeleto"))
        .args(["mcp", "--root", missing.to_str().unwrap()])
        .env("LAKELETO_HOME", &f.home)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "stdout is the protocol's alone");
    assert!(String::from_utf8_lossy(&out.stderr).contains("--root"));
}

#[cfg(feature = "catalog")]
#[test]
fn catalog_ls_walks_a_rest_catalog_and_never_shows_a_credential() {
    let catalog = rest_catalog::FakeCatalog::start();
    catalog.set_login(rest_catalog::Login::Token("sesame".to_string()));
    catalog.add_table(
        &["db"],
        "orders",
        rest_catalog::Table::new(
            "s3://bucket/db/orders/metadata/v1.metadata.json",
            Value::Null,
        ),
    );
    let f = fixture();
    std::fs::write(
        f.home.join("catalogs.toml"),
        format!(
            "[catalog.lab]\ntype = \"rest\"\nuri = \"{}\"\n",
            catalog.uri()
        ),
    )
    .unwrap();
    // No `--root`: a root refuses every catalog reference.
    let mut server =
        Server::start_with(&f.home, &[], &[("LAKELETO_CATALOG__LAB__TOKEN", "sesame")]);

    let (is_error, catalogs) = server.call("catalog_ls", json!({}));
    assert!(!is_error, "{catalogs}");
    assert_eq!(
        catalogs["catalogs"],
        json!([{ "name": "lab", "type": "rest", "uri": catalog.uri(),
                 "reference": "catalog://lab/" }])
    );
    let (_, namespaces) = server.call("catalog_ls", json!({ "reference": "catalog://lab/" }));
    assert_eq!(
        namespaces["entries"],
        json!([{ "name": "db", "kind": "namespace", "reference": "catalog://lab/db/" }])
    );
    let (_, tables) = server.call("catalog_ls", json!({ "reference": "catalog://lab/db/" }));
    assert_eq!(
        tables["entries"],
        json!([{ "name": "orders", "kind": "table", "reference": "catalog://lab/db/orders" }])
    );
    // `list` walks a catalog the same way, as the file browser does.
    let (_, listed) = server.call("list", json!({ "path": "catalog://lab/db/" }));
    assert_eq!(
        listed["entries"][0]["path"],
        json!("catalog://lab/db/orders")
    );
    for out in [&catalogs, &namespaces, &tables, &listed] {
        assert!(
            !out.to_string().contains("sesame"),
            "a credential leaked: {out}"
        );
    }
    assert!(server.close().success());
}
