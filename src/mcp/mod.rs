//! `lakeleto mcp`: the tables, read-only, to an AI agent over the Model Context Protocol.
//!
//! The transport is stdio: one JSON-RPC 2.0 message per line on stdin, one per line on stdout,
//! and nothing else on stdout. Warnings go to stderr, as the engines already send theirs.
//!
//! It is a loop over `serde_json` rather than an SDK. The `rmcp` crate measured 0.9 MB more in a
//! release build, and thirteen more crates, for a protocol this server uses five methods of. It
//! speaks the `initialize` lifecycle of revisions 2024-11-05 to 2025-11-25. A client of a later
//! revision probes with `server/discover` first; the "method not found" it gets back is how a
//! server of an earlier revision answers, and the client falls back to `initialize`.
//!
//! Each tool call runs on a thread of its own, so a slow call holds up neither a `ping` nor
//! another call, and so its deadline holds even when an engine doesn't check its context: the
//! call is answered with an error when the deadline passes, and whatever its thread sends later
//! is dropped.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::context::{CancelToken, RequestContext};

mod tools;
pub use tools::{Limits, Tools};

/// The protocol revisions this server speaks, newest first. A client asking for one of them gets
/// it; any other request gets the newest, as the lifecycle's version negotiation says.
pub const PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// How long past a call's deadline its thread has to answer before the loop answers for it.
/// Engines that check their context stop at the deadline and say so themselves; this only covers
/// one that doesn't.
const GRACE: Duration = Duration::from_secs(1);

/// Calls that may run at once; one more is refused as `busy`, for the client to try again. A call
/// answered at its deadline can leave its thread running until its engine returns, so this bounds
/// the threads, not only the calls still waited on.
const MAX_RUNNING: usize = 16;

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// Serve `tools` on this process's stdin and stdout until the client closes stdin, and every
/// call it made has been answered.
pub fn serve_stdio(tools: Tools) -> crate::error::Result<()> {
    // The reader is a thread of its own, so it takes stdin itself rather than a lock on it.
    let input = std::io::BufReader::new(std::io::stdin());
    serve(input, std::io::stdout().lock(), tools)?;
    Ok(())
}

enum Event {
    /// One line from the client.
    Line(String),
    /// The client closed its end: no more requests are coming.
    Closed,
    /// Call number `seq`'s thread finished, with the `tools/call` result to send.
    Done { seq: u64, result: Value },
}

/// A tool call that has not been answered yet.
struct Pending {
    id: Value,
    /// `id` as JSON text: what a cancellation names the call by.
    key: String,
    deadline: Instant,
    cancel: CancelToken,
}

/// Serve `tools` over `input` and `output`: [`serve_stdio`] with the streams passed in, so tests
/// can drive it in memory.
pub fn serve<R, W>(input: R, mut output: W, tools: Tools) -> std::io::Result<()>
where
    R: BufRead + Send + 'static,
    W: Write,
{
    let tools = Arc::new(tools);
    let (tx, rx) = mpsc::channel();
    let reader = tx.clone();
    std::thread::spawn(move || {
        for line in input.lines() {
            // A read error (a broken pipe, bytes that aren't UTF-8) ends the session as EOF does.
            let Ok(line) = line else { break };
            if reader.send(Event::Line(line)).is_err() {
                return;
            }
        }
        let _ = reader.send(Event::Closed);
    });

    let mut session = Session {
        tools,
        tx,
        pending: HashMap::new(),
        calls: 0,
        running: Arc::new(AtomicUsize::new(0)),
    };
    let mut open = true;
    loop {
        // Calls past their deadline are answered first, so a deadline holds however busy the
        // client keeps the loop.
        for call in session.overdue(Instant::now()) {
            call.cancel.cancel();
            send(&mut output, &success(call.id, session.tools.overdue()))?;
        }
        if !open && session.pending.is_empty() {
            break;
        }
        let next = session.pending.values().map(|p| p.deadline).min();
        let event = match next {
            Some(at) => match rx.recv_timeout(at.saturating_duration_since(Instant::now())) {
                Ok(event) => event,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            },
            None => match rx.recv() {
                Ok(event) => event,
                Err(_) => break,
            },
        };
        match event {
            Event::Line(line) => {
                if let Some(reply) = session.receive(&line) {
                    send(&mut output, &reply)?;
                }
            }
            Event::Closed => open = false,
            Event::Done { seq, result } => {
                // Gone when the call was cancelled, or already answered at its deadline.
                if let Some(call) = session.pending.remove(&seq) {
                    send(&mut output, &success(call.id, result))?;
                }
            }
        }
    }
    Ok(())
}

struct Session {
    tools: Arc<Tools>,
    tx: mpsc::Sender<Event>,
    /// Unanswered calls by number. Numbered here rather than keyed by the client's `id`, so the
    /// late result of a cancelled call can't answer a later request that reuses its `id`.
    pending: HashMap<u64, Pending>,
    /// Calls started so far: the next call's number.
    calls: u64,
    /// Call threads still running, answered or not.
    running: Arc<AtomicUsize>,
}

impl Session {
    /// Handle one line from the client, and return the reply to send now, if there is one. A tool
    /// call's reply comes later, from its thread.
    fn receive(&mut self, line: &str) -> Option<Value> {
        if line.trim().is_empty() {
            return None;
        }
        let msg: Value = match serde_json::from_str(line) {
            Ok(msg) => msg,
            Err(e) => return Some(failure(Value::Null, PARSE_ERROR, &format!("not JSON: {e}"))),
        };
        if msg.is_array() {
            // Batches were in revision 2025-03-26 only, and no client is known to send them.
            return Some(failure(
                Value::Null,
                INVALID_REQUEST,
                "batched requests are not supported; send one request per line",
            ));
        }
        if !msg.is_object() {
            return Some(failure(
                Value::Null,
                INVALID_REQUEST,
                "a message must be a JSON object",
            ));
        }
        let id = msg.get("id").cloned();
        let Some(method) = msg.get("method").and_then(Value::as_str) else {
            // A response to a request of ours (this server sends none), or not a request at all.
            let is_response = msg.get("result").is_some() || msg.get("error").is_some();
            return match id {
                Some(id) if !is_response => Some(failure(id, INVALID_REQUEST, "no `method`")),
                _ => None,
            };
        };
        if msg.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return id.map(|id| failure(id, INVALID_REQUEST, "`jsonrpc` must be \"2.0\""));
        }
        let params = msg.get("params").cloned().unwrap_or(Value::Null);
        let Some(id) = id else {
            self.notify(method, &params);
            return None;
        };
        if !(id.is_string() || id.is_number()) {
            return Some(failure(
                Value::Null,
                INVALID_REQUEST,
                "a request's `id` must be a string or a number",
            ));
        }
        match method {
            "initialize" => Some(success(id, self.initialize(&params))),
            "ping" => Some(success(id, json!({}))),
            "tools/list" => Some(success(id, json!({ "tools": self.tools.definitions() }))),
            "tools/call" => self.call(id, &params),
            _ => Some(failure(
                id,
                METHOD_NOT_FOUND,
                &format!("method not found: {method}"),
            )),
        }
    }

    /// Agree on a protocol revision and say what this server offers.
    fn initialize(&self, params: &Value) -> Value {
        let asked = params.get("protocolVersion").and_then(Value::as_str);
        let version = PROTOCOL_VERSIONS
            .iter()
            .find(|v| Some(**v) == asked)
            .unwrap_or(&PROTOCOL_VERSIONS[0]);
        json!({
            "protocolVersion": version,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": {
                "name": "lakeleto",
                "title": "Lakeleto",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "instructions": self.tools.instructions(),
        })
    }

    /// A notification: nothing is sent back. Only a cancellation does anything.
    fn notify(&mut self, method: &str, params: &Value) {
        if method == "notifications/cancelled" {
            if let Some(request) = params.get("requestId") {
                // The cancelled call is not answered, so its entry goes; its thread stops at the
                // next check of its context, and what it sends after is dropped.
                let key = request.to_string();
                let seq = self.pending.iter().find(|(_, call)| call.key == key);
                if let Some(seq) = seq.map(|(seq, _)| *seq) {
                    if let Some(call) = self.pending.remove(&seq) {
                        call.cancel.cancel();
                    }
                }
            }
        }
    }

    /// Start a tool call on a thread of its own. Only a malformed call is answered here.
    fn call(&mut self, id: Value, params: &Value) -> Option<Value> {
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return Some(failure(
                id,
                INVALID_PARAMS,
                "`tools/call` needs a tool `name`",
            ));
        };
        if !self.tools.offers(name) {
            return Some(failure(
                id,
                INVALID_PARAMS,
                &format!("unknown tool: {name}"),
            ));
        }
        let args = match params.get("arguments") {
            None | Some(Value::Null) => json!({}),
            Some(args @ Value::Object(_)) => args.clone(),
            Some(_) => {
                return Some(failure(id, INVALID_PARAMS, "`arguments` must be an object"));
            }
        };
        let key = id.to_string();
        if self.pending.values().any(|call| call.key == key) {
            return Some(failure(
                id,
                INVALID_REQUEST,
                "a call with this `id` is still running",
            ));
        }
        if self.running.load(Ordering::SeqCst) >= MAX_RUNNING {
            return Some(success(id, self.tools.busy(MAX_RUNNING)));
        }
        let timeout = self.tools.timeout();
        let cancel = CancelToken::new();
        let ctx = RequestContext::detached()
            .with_timeout(timeout)
            .with_cancel(cancel.clone());
        let seq = self.calls;
        self.calls += 1;
        let (tools, tx, name) = (self.tools.clone(), self.tx.clone(), name.to_string());
        let running = self.running.clone();
        running.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(move || {
            // A panic in an engine answers this call with an error instead of leaving it to the
            // deadline. The panic message itself still goes to stderr.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tools.call(&name, &args, &ctx)
            }))
            .unwrap_or_else(|_| tools.crashed());
            running.fetch_sub(1, Ordering::SeqCst);
            let _ = tx.send(Event::Done { seq, result });
        });
        self.pending.insert(
            seq,
            Pending {
                id,
                key,
                deadline: Instant::now() + timeout + GRACE,
                cancel,
            },
        );
        None
    }

    /// The calls whose deadline has passed, taken out of `pending`.
    fn overdue(&mut self, now: Instant) -> Vec<Pending> {
        let late: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, call)| call.deadline <= now)
            .map(|(seq, _)| *seq)
            .collect();
        late.iter()
            .filter_map(|seq| self.pending.remove(seq))
            .collect()
    }
}

/// The JSON-RPC response to request `id` that carries `result`.
fn success(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// The JSON-RPC error response to request `id`.
fn failure(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// Write one message and its newline, and flush: the client reads line by line.
fn send(output: &mut impl Write, msg: &Value) -> std::io::Result<()> {
    serde_json::to_writer(&mut *output, msg)?;
    output.write_all(b"\n")?;
    output.flush()
}

#[cfg(test)]
mod tests;
