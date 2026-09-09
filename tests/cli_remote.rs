//! The CLI against `--remote-url`: a ref the *server* resolves must reach the server.
//!
//! Every read command used to call `Source::detect(path)` before it picked an engine, and
//! `detect` is local — it stats the path, walks directories and sniffs magic bytes. So a ref
//! that only the far side can interpret (a catalog name, a table id, any opaque string a server
//! understands and this process does not) was resolved against the laptop's own filesystem,
//! failed there, and never reached the network:
//!
//! ```text
//! $ lakeleto head "cat://acme/geo/cities" --remote-url http://127.0.0.1:9999
//! lakeleto: io error: No such file or directory (os error 2)
//! ```
//!
//! These tests pin the rule that replaced it — **a remote engine resolves its own sources** —
//! by running the real `cli::run` against a one-shot socket and inspecting the request that
//! arrives. Nothing here knows what any particular ref *means*; the point is precisely that the
//! client does not have to.
//!
//! Run with: `cargo test --features remote --test cli_remote`.
#![cfg(feature = "remote")]

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc;

use lakeleto::cli::{Cli, Cmd, EngineChoice};
use lakeleto::render::Output;

/// A ref this machine cannot resolve and a server can. Any opaque string would do — `cat://`
/// is used because it is the shape the hosted plane happens to accept, not because the OSS
/// crate knows anything about it.
const OPAQUE_REF: &str = "cat://acme/geo/cities";

/// A server that accepts exactly one request, reports its request line back through `rx`, and
/// answers with the `/v1/*` error shape (`{"error": …}`) so the client's own error surfaces the
/// server's message rather than a status line.
///
/// Deliberately *not* a `lakeleto serve`: what is under test is that a request is made at all,
/// and with which parameters. A real server would resolve the ref and answer rows, which proves
/// less about this seam and needs a filesystem the ref does not name.
fn one_shot_server() -> (SocketAddr, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader.read_line(&mut request_line).expect("request line");
        // Drain the headers so the client is not left writing into a full buffer.
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).unwrap_or(0) == 0 || header.trim().is_empty() {
                break;
            }
        }
        let body = br#"{"error":"no such endpoint: /v1/preview"}"#;
        let mut stream = reader.into_inner();
        let _ = write!(
            stream,
            "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(body);
        let _ = stream.flush();
        let _ = tx.send(request_line);
    });
    (addr, rx)
}

fn head_cli(path: &str, remote_url: Option<String>, format: Option<&str>) -> Cli {
    Cli {
        output: Output::Table,
        engine: EngineChoice::Auto,
        remote_url,
        remote_token: None,
        format: format.map(str::to_string),
        cmd: Cmd::Head {
            path: path.into(),
            rows: 3,
        },
    }
}

#[test]
fn a_ref_this_machine_cannot_resolve_reaches_the_server_when_remote_url_is_set() {
    let (addr, rx) = one_shot_server();
    let err = lakeleto::cli::run(head_cli(OPAQUE_REF, Some(format!("http://{addr}")), None))
        .expect_err("the stub server answers 404, so the command fails — over HTTP");

    // The failure is the SERVER's, carried back verbatim. The bug this pins made it a local
    // `io error: No such file or directory` instead, from `Source::detect` on the laptop.
    let msg = err.to_string();
    assert!(
        msg.contains("no such endpoint: /v1/preview"),
        "expected the server's own message, got: {msg}"
    );
    assert!(
        !msg.contains("No such file"),
        "the ref must not be resolved locally: {msg}"
    );

    let request = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the client must actually have sent a request");
    assert!(
        request.starts_with("GET /v1/preview?"),
        "unexpected request: {request}"
    );
    // The ref travels opaquely (percent-encoded, unchanged in meaning).
    assert!(
        request.contains("path=cat%3A%2F%2Facme%2Fgeo%2Fcities"),
        "the ref must be forwarded verbatim: {request}"
    );
    // And no format is invented on the client's behalf: `?format=` is an explicit override on
    // this contract, so a guess here would become an instruction there.
    assert!(
        !request.contains("format="),
        "an unresolved source must send no format at all: {request}"
    );
    assert!(request.contains("limit=3"), "{request}");
}

#[test]
fn an_explicit_format_still_travels_as_the_override_it_is() {
    let (addr, rx) = one_shot_server();
    let _ = lakeleto::cli::run(head_cli(
        OPAQUE_REF,
        Some(format!("http://{addr}")),
        Some("csv"),
    ));
    let request = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("request");
    assert!(
        request.contains("format=csv"),
        "an explicit --format is the one thing the client does know: {request}"
    );
}

#[test]
fn an_unknown_format_name_is_refused_before_anything_is_sent() {
    let err = lakeleto::cli::run(head_cli(
        OPAQUE_REF,
        Some("http://127.0.0.1:9".to_string()),
        Some("parquay"),
    ))
    .expect_err("a typo'd --format is a client-side error");
    assert!(err.to_string().contains("unknown format"), "{err}");
}

#[test]
fn the_rule_follows_the_engine_not_the_url() {
    // `--engine local` reads locally even with a URL set, so the ref is resolved locally and
    // fails there — which is correct, and is why the passthrough keys off the selected engine
    // rather than off `--remote-url` being present.
    let mut cli = head_cli(OPAQUE_REF, Some("http://127.0.0.1:9".to_string()), None);
    cli.engine = EngineChoice::Local;
    let err = lakeleto::cli::run(cli).expect_err("local read of a non-local ref fails locally");
    let msg = err.to_string();
    assert!(
        msg.contains("io error") || msg.contains("unsupported format"),
        "expected a local resolution failure, got: {msg}"
    );
}
