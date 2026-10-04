//! Cancellation and deadlines reaching the engines through [`RequestContext`].
//!
//! The point of these tests is the distinction between a context that is *carried* and one that
//! is *honoured*. A parameter nothing reads is decoration; what has to be shown is that a real
//! read of a real file stops. `dataset_deadline_fires_inside_the_read_loop` is the load-bearing
//! one: it establishes that the failure came from a check *inside* the scan rather than the
//! pre-flight check at the top of the method, which is the difference between an engine that can
//! be interrupted and one that merely refuses to start.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use lakeleto::engine::Engine;
use lakeleto::{CancelToken, EngineError, LocalReaderEngine, RequestContext, Source};

use lakeleto::CancelReason;

fn batch(start: i64, rows: i64) -> RecordBatch {
    let id = Int64Array::from((start..start + rows).collect::<Vec<_>>());
    let name = StringArray::from(
        (start..start + rows)
            .map(|i| format!("row-{i}"))
            .collect::<Vec<_>>(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(id) as ArrayRef, Arc::new(name) as ArrayRef],
    )
    .unwrap()
}

fn write_one(path: &Path, start: i64, rows: i64) {
    let b = batch(start, rows);
    let file = File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, b.schema(), None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
}

/// A single-file Parquet source.
fn one_file(dir: &Path) -> Source {
    let p = dir.join("people.parquet");
    write_one(&p, 0, 1_000);
    Source::detect(&p).unwrap()
}

/// A multi-file Parquet *dataset* — the shape whose read loop is worth interrupting, because a
/// real one is thousands of files and each is a separate open.
fn dataset(dir: &Path, files: usize) -> Source {
    let root = dir.join("events.parquet");
    std::fs::create_dir_all(&root).unwrap();
    for i in 0..files {
        write_one(
            &root.join(format!("part-{i:04}.parquet")),
            i as i64 * 500,
            500,
        );
    }
    Source::detect(&root).unwrap()
}

/// `RowBatch` and `ScanResult` deliberately do not implement `Debug` (they hold Arrow arrays), so
/// `unwrap_err`/`expect_err` are unavailable on a read's result. This says the same thing without
/// requiring it.
#[track_caller]
fn err_of<T>(r: Result<T, EngineError>, what: &str) -> EngineError {
    match r {
        Ok(_) => panic!("expected {what} to stop, but it completed"),
        Err(e) => e,
    }
}

fn cancelled_reason(e: &EngineError) -> Option<CancelReason> {
    match e {
        EngineError::Cancelled(r) => Some(*r),
        _ => None,
    }
}

#[test]
fn a_detached_context_reads_normally() {
    let dir = tempfile::tempdir().unwrap();
    let src = one_file(dir.path());
    let engine = LocalReaderEngine::default();

    // The control: without this, a cancellation test proves only that the read was broken.
    let rb = engine
        .preview(&RequestContext::detached(), &src, 100)
        .expect("a detached context must never stop a read");
    assert_eq!(rb.num_rows(), 100);
}

#[test]
fn a_cancelled_token_stops_a_parquet_read() {
    let dir = tempfile::tempdir().unwrap();
    let src = one_file(dir.path());
    let engine = LocalReaderEngine::default();

    let token = CancelToken::new();
    token.cancel();
    let ctx = RequestContext::detached().with_cancel(token);

    let err = err_of(engine.preview(&ctx, &src, 100), "a cancelled read");
    assert_eq!(cancelled_reason(&err), Some(CancelReason::Requested));
}

#[test]
fn an_expired_deadline_stops_a_parquet_read() {
    let dir = tempfile::tempdir().unwrap();
    let src = one_file(dir.path());
    let engine = LocalReaderEngine::default();

    let ctx = RequestContext::detached().with_deadline(Instant::now() - Duration::from_secs(1));

    let err = err_of(engine.preview(&ctx, &src, 100), "a read past its deadline");
    assert_eq!(cancelled_reason(&err), Some(CancelReason::Deadline));
}

/// Every `Engine` read method honours the context, not just `preview`.
#[test]
fn schema_profile_and_scan_all_honour_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let src = one_file(dir.path());
    let engine = LocalReaderEngine::default();

    let token = CancelToken::new();
    token.cancel();
    let ctx = RequestContext::detached().with_cancel(token);

    assert_eq!(
        cancelled_reason(&err_of(engine.schema(&ctx, &src), "schema")),
        Some(CancelReason::Requested),
        "schema"
    );
    assert_eq!(
        cancelled_reason(&err_of(engine.profile(&ctx, &src, 1_000), "profile")),
        Some(CancelReason::Requested),
        "profile"
    );
    assert_eq!(
        cancelled_reason(&err_of(
            engine.scan(&ctx, &src, &lakeleto::engine::ScanSpec::default()),
            "scan"
        )),
        Some(CancelReason::Requested),
        "scan"
    );
    assert_eq!(
        cancelled_reason(&err_of(engine.stats(&ctx, &src, &[], 1_000), "stats")),
        Some(CancelReason::Requested),
        "stats"
    );
}

/// **The load-bearing test.** A deadline that is still in the future when the read *starts* must
/// still stop it partway through — which is only possible if the check lives inside the per-file
/// loop rather than only at the top of the method.
///
/// The argument is made airtight by measuring first: an unbounded read of this dataset is timed,
/// and the deadline is then set to a quarter of that. At entry the deadline is by construction
/// still in the future, so the pre-flight `ctx.check()` provably passes; the read nevertheless
/// fails, so some later check fired. The test asserts its own premise (that the read is slow
/// enough to bisect) rather than assuming it, so on a machine fast enough to break the setup it
/// fails loudly instead of passing vacuously.
#[test]
fn dataset_deadline_fires_inside_the_read_loop() {
    let dir = tempfile::tempdir().unwrap();
    let src = dataset(dir.path(), 120);
    let engine = LocalReaderEngine::default();

    // Warm the dataset before timing it. The budget below is a *fraction* of the measured read,
    // so the measurement and the read it bounds have to run under the same conditions: time a cold
    // read, and a warmer second read can finish inside `elapsed / 4`, return `Ok`, and make
    // `err_of` panic on a test that was never wrong about the behaviour — only about the clock.
    // The `elapsed >= 8ms` assertion below does not catch that, because it constrains the first
    // read's duration, not the ratio between the two.
    engine
        .preview(&RequestContext::detached(), &src, usize::MAX)
        .expect("the warm-up read must succeed");

    let started = Instant::now();
    let full = engine
        .preview(&RequestContext::detached(), &src, usize::MAX)
        .expect("the uncancelled read must succeed");
    let elapsed = started.elapsed();
    assert_eq!(full.num_rows(), 120 * 500);

    assert!(
        elapsed >= Duration::from_millis(8),
        "this test needs a read slow enough to interrupt partway; reading 120 parquet files \
         took only {elapsed:?}. Raise the file count rather than deleting the assertion — \
         without it the test below could pass for the wrong reason."
    );

    // Still in the future at entry, comfortably in the past long before the read could finish.
    let budget = elapsed / 4;
    let ctx = RequestContext::detached().with_timeout(budget);
    let err = err_of(
        engine.preview(&ctx, &src, usize::MAX),
        "a read with a deadline shorter than itself",
    );
    assert_eq!(
        cancelled_reason(&err),
        Some(CancelReason::Deadline),
        "expected the in-loop deadline check to fire, got: {err}"
    );
}

/// A cancellation raised *between* two reads is observed by the second — the token is shared
/// state, not a snapshot taken when the context was built.
#[test]
fn a_token_cancelled_between_reads_stops_only_the_second() {
    let dir = tempfile::tempdir().unwrap();
    let src = one_file(dir.path());
    let engine = LocalReaderEngine::default();

    let token = CancelToken::new();
    let ctx = RequestContext::detached().with_cancel(token.clone());

    assert_eq!(engine.preview(&ctx, &src, 10).unwrap().num_rows(), 10);
    token.cancel();
    assert_eq!(
        cancelled_reason(&err_of(engine.preview(&ctx, &src, 10), "the second read")),
        Some(CancelReason::Requested),
    );
}

#[cfg(feature = "sql")]
mod sql {
    use super::*;
    use lakeleto::engine::NamedSource;

    /// The SQL engine reaches DataFusion through the watchdog in `engine::sql::guarded`, so a
    /// context that is already cancelled must stop the query rather than run it to completion
    /// and discard the answer.
    #[test]
    fn a_cancelled_context_stops_a_datafusion_query() {
        let dir = tempfile::tempdir().unwrap();
        let src = one_file(dir.path());
        let engine = lakeleto::engine::sql::DataFusionEngine::new();

        let token = CancelToken::new();
        token.cancel();
        let ctx = RequestContext::detached().with_cancel(token);

        let named = vec![NamedSource {
            name: "t".to_string(),
            source: src,
        }];
        let err = err_of(
            engine.query(&ctx, "SELECT count(*) FROM t", &named),
            "a cancelled query",
        );
        assert_eq!(cancelled_reason(&err), Some(CancelReason::Requested));
    }

    /// And a detached one still works — the guard must not cost correctness on the common path.
    #[test]
    fn a_detached_context_runs_a_datafusion_query() {
        let dir = tempfile::tempdir().unwrap();
        let src = one_file(dir.path());
        let engine = lakeleto::engine::sql::DataFusionEngine::new();
        let named = vec![NamedSource {
            name: "t".to_string(),
            source: src,
        }];
        let rb = engine
            .query(
                &RequestContext::detached(),
                "SELECT count(*) FROM t",
                &named,
            )
            .expect("a detached context must not interfere");
        assert_eq!(rb.num_rows(), 1);
    }
}
