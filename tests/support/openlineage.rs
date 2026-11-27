//! An OpenLineage endpoint on this machine that records what it is sent, and the published
//! schemas (copies in `tests/openlineage/`) the events it records are held to.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use jsonschema::Registry;
use serde_json::{json, Value};

/// One request as the endpoint read it.
pub struct Received {
    pub request_line: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Received {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// An endpoint on a loopback port that answers every request with `status` and hands what it
/// read to the returned channel. Its URL is the server's root: a path, or none, is the caller's.
/// A 3xx names a `Location` off this machine.
pub fn endpoint(status: u16) -> (String, mpsc::Receiver<Received>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut headers = Vec::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let line = line.trim_end();
                if line.is_empty() {
                    break;
                }
                let (k, v) = line.split_once(':').unwrap();
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
            let length: usize = headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .map(|(_, v)| v.parse().unwrap())
                .unwrap_or(0);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let location = match status {
                300..=399 => "Location: http://lineage.example.com/api/v1/lineage\r\n",
                _ => "",
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status} Status\r\n{location}Content-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = tx.send(Received {
                request_line: request_line.trim_end().to_string(),
                headers,
                body,
            });
        }
    });
    (url, rx)
}

/// The `n` requests a run sent, waiting a while for each; a short wait after the last finds
/// none more.
pub fn sent(rx: &mpsc::Receiver<Received>, n: usize) -> Vec<Received> {
    let got: Vec<Received> = (0..n)
        .map(|i| {
            rx.recv_timeout(Duration::from_secs(20))
                .unwrap_or_else(|_| panic!("request {} of {n} never came", i + 1))
        })
        .collect();
    assert!(
        rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "more than {n} requests"
    );
    got
}

/// That nothing was sent within a short wait.
pub fn nothing_sent(rx: &mpsc::Receiver<Received>) {
    if let Ok(r) = rx.recv_timeout(Duration::from_millis(500)) {
        panic!(
            "sent {}: {}",
            r.request_line,
            String::from_utf8_lossy(&r.body)
        );
    }
}

/// Errors from validating `value` against `reference`, a URI into one of the published schemas.
pub fn openlineage_errors(value: &Value, reference: &str) -> Vec<String> {
    let load = |name: &str| -> Value {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/openlineage")
            .join(name);
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    };
    let spec = "https://openlineage.io/spec";
    let registry = Registry::new()
        .add(
            format!("{spec}/2-0-2/OpenLineage.json"),
            load("OpenLineage-2-0-2.json"),
        )
        .unwrap()
        .add(
            format!("{spec}/facets/1-0-2/DataQualityAssertionsDatasetFacet.json"),
            load("DataQualityAssertionsDatasetFacet-1-0-2.json"),
        )
        .unwrap()
        .add(
            format!("{spec}/facets/1-0-3/DataQualityMetricsInputDatasetFacet.json"),
            load("DataQualityMetricsInputDatasetFacet-1-0-3.json"),
        )
        .unwrap()
        .add(
            format!("{spec}/facets/1-0-1/SymlinksDatasetFacet.json"),
            load("SymlinksDatasetFacet-1-0-1.json"),
        )
        .unwrap()
        .add(
            format!("{spec}/facets/1-0-0/CatalogDatasetFacet.json"),
            load("CatalogDatasetFacet-1-0-0.json"),
        )
        .unwrap()
        .prepare()
        .unwrap();
    let validator = jsonschema::options()
        .with_registry(&registry)
        .should_validate_formats(true)
        .build(&json!({ "$ref": reference }))
        .unwrap();
    validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect()
}

/// That `event` is an OpenLineage 2-0-2 run event, and each facet its datasets carry is what the
/// schema it names says.
pub fn assert_openlineage(event: &Value) {
    let errors = openlineage_errors(
        event,
        "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
    );
    assert!(errors.is_empty(), "{errors:?}\n{event:#}");
    for kind in ["inputs", "outputs"] {
        for dataset in event[kind].as_array().unwrap() {
            for key in ["facets", "inputFacets"] {
                let Some(facets) = dataset.get(key) else {
                    continue;
                };
                for (name, facet) in facets.as_object().unwrap() {
                    let schema = facet["_schemaURL"].as_str().unwrap();
                    let errors = openlineage_errors(facet, schema);
                    assert!(errors.is_empty(), "{name}: {errors:?}\n{facet:#}");
                }
            }
        }
    }
}

/// The two events of one run, START then `end`, each held to the schemas, sharing a run id that
/// is a UUIDv7 and naming the same job.
pub fn assert_run(events: &[Value], end: &str) {
    assert_eq!(events.len(), 2);
    for event in events {
        assert_openlineage(event);
    }
    assert_eq!(events[0]["eventType"], "START");
    assert_eq!(events[1]["eventType"], end);
    let id = events[0]["run"]["runId"].as_str().unwrap();
    assert_eq!(events[1]["run"]["runId"], id, "one run");
    assert_eq!(id.len(), 36, "{id}");
    assert_eq!(&id[14..15], "7", "a UUIDv7: {id}");
    assert_eq!(events[0]["job"], events[1]["job"]);
    assert!(
        events[0]["eventTime"].as_str() <= events[1]["eventTime"].as_str(),
        "START first"
    );
    for input in events[0]["inputs"].as_array().unwrap() {
        assert!(
            input.get("inputFacets").is_none(),
            "START only names: {input}"
        );
    }
}

/// `{namespace, name}` of each of `event`'s `kind` (`inputs` or `outputs`).
pub fn names(event: &Value, kind: &str) -> Vec<(String, String)> {
    event[kind]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["namespace"].as_str().unwrap().to_string(),
                d["name"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}
