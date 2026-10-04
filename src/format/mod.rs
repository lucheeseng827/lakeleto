//! File formats read into Arrow: every single-file row format the local engine opens that is not
//! Parquet, behind one [`FormatReader`] trait.
//!
//! Parquet, Iceberg and Delta keep their own paths in the local engine, because row groups,
//! footers, datasets and snapshots are more than "bytes in, rows out". Everything else is a reader
//! here, found by [`reader`], so the engine has one arm for all of them and a new format is a module
//! in this directory, a line in [`READERS`] and its tests. The capability lists the API routes on
//! ([`crate::engine::readable_formats`], [`crate::engine::sql_registers`]) are read off the same
//! registry, so they cannot drift from what is actually readable.

pub(crate) mod csv;
pub(crate) mod json;

use std::io::BufRead;
use std::ops::Range;
use std::path::Path;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::Format;

/// Where a reader's bytes are: a local file or an object in a store, each read afresh by every
/// pass, or bytes an engine already holds.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Input<'a> {
    File(&'a Path),
    Bytes(&'a [u8]),
    /// Read as its bytes arrive, never held whole — for a reader that
    /// [streams objects](FormatReader::streams_objects).
    Object(&'a dyn RemoteObject),
}

/// An object in a store, as of one version of it, that a reader reads where it lies: each pass is
/// a request of its own, its bytes decoded as they arrive.
pub(crate) trait RemoteObject: std::fmt::Debug + Send + Sync {
    /// Its URI (`s3://bucket/key`), which names it in messages and in a reader's cache.
    fn uri(&self) -> &str;

    /// The credential identity it was looked up as. A reader's cache keys what it learns of an
    /// object by this too: the same URI can name another object for another identity (`az://`
    /// does not say which account), and what one identity's read learned never answers another's.
    fn identity(&self) -> &str;

    /// Its size in bytes.
    fn size(&self) -> u64;

    /// The version this is (the store's ETag where it gives one). A request for its bytes asks for
    /// this version, and fails rather than read another, so what a reader learned about one
    /// version — its schema, where its records are — is never applied to the next.
    fn version(&self) -> &str;

    /// Its bytes, or those in `range`, as they arrive.
    fn open(&self, range: Option<Range<u64>>) -> Result<Box<dyn BufRead + Send + '_>>;
}

/// How to read, beyond where from. One set for every reader — each takes the fields that mean
/// something to its format.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadOptions<'a> {
    /// Rows per Arrow batch.
    pub batch_size: usize,
    /// Rows a CSV/TSV schema is inferred over, at least; a read infers over its whole window.
    pub csv_infer_rows: usize,
    /// The caller's JSON records path ([`crate::source::Source::json_path`]).
    pub json_path: Option<&'a str>,
}

/// What a reader says about a file before any of its rows are read.
#[derive(Debug)]
pub(crate) struct FileSchema {
    pub schema: SchemaRef,
    /// Where inside the file the rows come from, when the reader chose it rather than the caller:
    /// a JSON records member (`/data`). Reported so an unwrap is never silent.
    pub records_path: Option<String>,
}

/// How the SQL engine reads a format. There is no "cannot": whatever a reader here opens, SQL can
/// read through it.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(feature = "sql"), allow(dead_code))] // read by the SQL engine only
pub(crate) enum SqlSupport {
    /// DataFusion has its own reader for it and registers the file natively — streamed, with
    /// projections and filters pushed into the scan. `DataFusionEngine::register` needs an arm
    /// for each such format.
    Native,
    /// Read by this crate's reader, in one pass over the file per query: for a format DataFusion
    /// has no reader for, or none that reads it the way this crate does. Every read option applies
    /// in SQL exactly as in the grid, and a query holds the batches in flight rather than the file.
    /// The table's schema is the reader's, fixed when the query is planned — so a pass can find a
    /// value it cannot hold, which [`Pass::Widened`] reports.
    ///
    /// The reader must [stream objects](FormatReader::streams_objects): a file in an object store
    /// is passed over as one, a request per pass.
    Streamed(PassFn),
}

/// One pass over every row of the input given — a local file, or an object in a store — read with
/// the options given and decoded as the schema given (the one SQL planned its table with), each
/// batch handed to the callback until it returns `false`.
#[cfg_attr(not(feature = "sql"), allow(dead_code))]
pub(crate) type PassFn = fn(
    Input<'_>,
    &ReadOptions<'_>,
    &SchemaRef,
    &mut dyn FnMut(RecordBatch) -> bool,
) -> Result<Pass>;

/// How a [`PassFn`] ended, short of failing.
#[derive(Debug)]
#[cfg_attr(not(feature = "sql"), allow(dead_code))]
pub(crate) enum Pass {
    /// Every row was handed over, or the callback stopped taking them.
    Done,
    /// A row did not fit the schema, and the file's own schema is wider: the reader has made the
    /// wider one the file's, so a query planned again reads that row. The pass stopped there.
    /// Carries what did not fit.
    Widened(String),
}

/// One single-file row format: bytes in, Arrow out.
pub(crate) trait FormatReader: Send + Sync {
    /// The format this reads.
    fn format(&self) -> Format;

    /// The lower-case file extensions that mean this format.
    fn extensions(&self) -> &'static [&'static str];

    /// How the SQL engine reads this format — so a reader cannot be added without deciding it.
    #[cfg_attr(not(feature = "sql"), allow(dead_code))]
    fn sql(&self) -> SqlSupport;

    /// Whether this reader reads an object in a store as it arrives ([`Input::Object`]). One that
    /// does not is handed the object's bytes, fetched whole for every read.
    #[cfg_attr(not(feature = "object-store"), allow(dead_code))]
    fn streams_objects(&self) -> bool {
        false
    }

    /// The schema a read of `input` will have.
    fn schema(&self, input: Input<'_>, opts: &ReadOptions<'_>) -> Result<FileSchema>;

    /// Up to `row_limit` rows from the start (all of them when `None`), with their schema.
    fn read(
        &self,
        ctx: &RequestContext,
        input: Input<'_>,
        opts: &ReadOptions<'_>,
        row_limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)>;
}

/// Every [`FormatReader`], in the order capability lists report them.
static READERS: [&dyn FormatReader; 3] = [&csv::CSV, &csv::TSV, &json::JSON];

/// The reader for `format`, if it is one of the registry's.
pub(crate) fn reader(format: Format) -> Option<&'static dyn FormatReader> {
    READERS.iter().copied().find(|r| r.format() == format)
}

/// The format a (lower-case) file extension names, among the registry's.
pub(crate) fn format_for_extension(ext: &str) -> Option<Format> {
    READERS
        .iter()
        .find(|r| r.extensions().contains(&ext))
        .map(|r| r.format())
}

/// The registry's formats, in order.
pub(crate) fn formats() -> impl Iterator<Item = Format> {
    READERS.iter().map(|r| r.format())
}

/// Drain a batch iterator into a list, checking `ctx` between batches and stopping once
/// `row_limit` rows are in.
fn collect(
    ctx: &RequestContext,
    batches: impl IntoIterator<Item = std::result::Result<RecordBatch, ArrowError>>,
    row_limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    let mut rows = 0usize;
    for batch in batches {
        ctx.check()?;
        let batch = batch.map_err(read_error)?;
        rows += batch.num_rows();
        out.push(batch);
        if row_limit.is_some_and(|n| rows >= n) {
            break;
        }
    }
    Ok(out)
}

/// A decoder's error, with one that came from reading its bytes kept as the I/O error it is. Only
/// a decode error says anything about the rows, and a reader that would look again on one — a
/// schema to widen — has no reason to read the input again for a connection that dropped.
fn read_error(e: ArrowError) -> EngineError {
    match e {
        ArrowError::IoError(_, e) => EngineError::Io(e),
        e => EngineError::arrow(e),
    }
}

/// The batch size for a read of `row_limit` rows: no bigger than the read, nor than `batch_size`.
fn batch_size_for(opts: &ReadOptions<'_>, row_limit: Option<usize>) -> usize {
    row_limit.map_or(opts.batch_size, |n| n.clamp(1, opts.batch_size))
}

/// What the readers' tests read an object through.
#[cfg(test)]
pub(crate) mod testing {
    use std::io::{BufRead, Read};
    use std::ops::Range;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    use super::RemoteObject;
    use crate::error::Result;

    /// An object held in memory that keeps count of what is asked of it — the requests, with their
    /// ranges, and the bytes a reader took — and hands its bytes out a chunk at a time, as a
    /// store's response arrives.
    #[derive(Debug)]
    pub(crate) struct MemObject {
        uri: String,
        identity: String,
        version: String,
        bytes: Vec<u8>,
        /// After this many bytes are taken, a read fails as a dropped connection would.
        fail_after: Option<u64>,
        /// The size it reports, if not its bytes'.
        claimed: Option<u64>,
        requests: Mutex<Vec<Option<Range<u64>>>>,
        taken: AtomicU64,
    }

    /// The most a response hands over at once.
    const CHUNK: usize = 8 * 1024;

    impl MemObject {
        /// `bytes` at `uri`, as version `version`, looked up as the identity `tests`.
        pub(crate) fn new(uri: &str, version: &str, bytes: impl Into<Vec<u8>>) -> Self {
            MemObject {
                uri: uri.to_string(),
                identity: "tests".to_string(),
                version: version.to_string(),
                bytes: bytes.into(),
                fail_after: None,
                claimed: None,
                requests: Mutex::default(),
                taken: AtomicU64::new(0),
            }
        }

        /// The same object, looked up as another credential identity.
        pub(crate) fn looked_up_as(mut self, identity: &str) -> Self {
            self.identity = identity.to_string();
            self
        }

        /// One that reports a size of `n` bytes, larger than what it holds: what a reader decides
        /// by size alone, without the bytes to back it.
        pub(crate) fn claiming(mut self, n: u64) -> Self {
            self.claimed = Some(n);
            self
        }

        /// One whose reads fail once `n` bytes have been taken from it.
        pub(crate) fn failing_after(mut self, n: u64) -> Self {
            self.fail_after = Some(n);
            self
        }

        /// The requests made so far, oldest first: `None` for the whole object.
        pub(crate) fn requests(&self) -> Vec<Option<Range<u64>>> {
            self.requests.lock().unwrap().clone()
        }

        /// The bytes readers have taken so far.
        pub(crate) fn taken(&self) -> u64 {
            self.taken.load(Ordering::SeqCst)
        }

        /// Forget the requests and bytes counted so far.
        pub(crate) fn reset(&self) {
            self.requests.lock().unwrap().clear();
            self.taken.store(0, Ordering::SeqCst);
        }
    }

    impl RemoteObject for MemObject {
        fn uri(&self) -> &str {
            &self.uri
        }

        fn identity(&self) -> &str {
            &self.identity
        }

        fn size(&self) -> u64 {
            self.claimed.unwrap_or(self.bytes.len() as u64)
        }

        fn version(&self) -> &str {
            &self.version
        }

        fn open(&self, range: Option<Range<u64>>) -> Result<Box<dyn BufRead + Send + '_>> {
            self.requests.lock().unwrap().push(range.clone());
            let (start, end) = range.map_or((0, self.bytes.len()), |r| {
                (r.start as usize, (r.end as usize).min(self.bytes.len()))
            });
            Ok(Box::new(Response {
                object: self,
                rest: &self.bytes[start..end],
            }))
        }
    }

    /// One request's bytes, handed out a chunk at a time and counted as they are taken.
    struct Response<'a> {
        object: &'a MemObject,
        rest: &'a [u8],
    }

    impl Read for Response<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = {
                let available = self.fill_buf()?;
                let n = available.len().min(buf.len());
                buf[..n].copy_from_slice(&available[..n]);
                n
            };
            self.consume(n);
            Ok(n)
        }
    }

    impl BufRead for Response<'_> {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            if self
                .object
                .fail_after
                .is_some_and(|n| self.object.taken() >= n)
            {
                return Err(std::io::Error::other("connection reset by peer"));
            }
            Ok(&self.rest[..self.rest.len().min(CHUNK)])
        }

        fn consume(&mut self, n: usize) {
            self.rest = &self.rest[n..];
            self.object.taken.fetch_add(n as u64, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_format_has_one_reader_and_its_extensions_lead_back_to_it() {
        let mut seen = Vec::new();
        for r in READERS {
            assert!(!seen.contains(&r.format()), "{} twice", r.format());
            seen.push(r.format());
            assert!(std::ptr::addr_eq(reader(r.format()).unwrap(), r));
            for ext in r.extensions() {
                assert_eq!(ext.to_ascii_lowercase(), *ext, "extensions are lower-case");
                assert_eq!(format_for_extension(ext), Some(r.format()));
            }
        }
    }

    /// The text readers — CSV, TSV and JSON — and no others read an object in a store as its
    /// bytes arrive.
    #[test]
    fn every_text_reader_is_handed_an_object_to_read_as_it_arrives() {
        // A reader that could not — one that seeks around its input — would be handed a file in
        // an object store fetched whole instead.
        let streaming: Vec<Format> = READERS
            .iter()
            .filter(|r| r.streams_objects())
            .map(|r| r.format())
            .collect();
        assert_eq!(streaming, [Format::Csv, Format::Tsv, Format::Json]);
    }
}
