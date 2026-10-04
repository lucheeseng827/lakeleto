//! JSON, NDJSON and JSON Lines — every shape a JSON file arrives in, decoded into Arrow.
//!
//! The shape is read from the bytes, not the extension, because the extension does not say it:
//! a `.json` file is as likely to be one record per line as one big array.
//!
//! - **Values** — JSON values separated by whitespace: NDJSON / JSON Lines, `jq` output (pretty
//!   values back to back), or a single object. arrow-json's streaming decoder reads these as they
//!   are. It never needed one value per line; only the line-based schema inference did.
//! - **Array** — a top-level `[...]`, streamed element by element into the same decoder by
//!   [`ArrayToNdjson`]. Memory is bounded by the row window, as for NDJSON, not by the file size.
//! - **Records** — one top-level object whose rows live in a member: `{"meta": {…}, "data": […]}`,
//!   a GeoJSON `features` list. Chosen when the object has exactly one member holding a non-empty
//!   array of objects, and reported as a JSON Pointer (`/data`) so it is never silent. The member
//!   is *located*, not parsed — the byte range of its array, found once per file version without
//!   building a value — and then streamed like a top-level array. Locating it holds the document's
//!   bytes in memory, so that shape is capped at [`DOCUMENT_MAX_BYTES`].
//!
//! Inference reads *values*, not lines, so pretty-printed input infers like NDJSON does. Decoding
//! coerces scalars to text where inference widened a column to `Utf8` because its rows disagree
//! (`{"v": 1}` then `{"v": "a"}`), instead of failing on the first row that differs. Columns come in
//! the order their keys first appear, because `serde_json`'s `preserve_order` is on.
//!
//! An object in a store reads as a local file does, its bytes decoded as they arrive
//! ([`Input::Object`]): each pass is a request, a located records member a ranged one, and what is
//! learned about it is kept per version of the object, as a file's is per version of the file.

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use serde::de::{Deserialize, Deserializer, SeqAccess, Visitor};
use serde_json::value::RawValue;
use serde_json::Value;

use super::{
    batch_size_for, collect, read_error, FileSchema, FormatReader, Input, Pass, ReadOptions,
    SqlSupport,
};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::Format;

/// The largest single JSON **document** held in memory to find its records in — as bytes, once
/// per file version; the records then stream. Only that shape holds a document whole — NDJSON and
/// top-level arrays stream from the start — so this bounds one request's memory for exactly that
/// case. A larger document is still read, as the one row it literally is, rather than refused.
pub(crate) const DOCUMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Values sampled to infer a stream's schema: the same first values on every read, cached per
/// file version (see [`cache_key`]), so the schema, the first window and a window far down all
/// agree. It is DuckDB's default for the same job, and costs tens of milliseconds; inferring a
/// whole file cost over a second per 60 MiB, which a preview cannot afford. A read that reaches a
/// value the sample did not anticipate widens the schema rather than failing — see [`read_rows`].
pub(crate) const SAMPLE_VALUES: usize = 20_000;

/// A UTF-8 byte-order mark. Editors on Windows still write one; the decoder rejects it.
const BOM: &[u8] = b"\xEF\xBB\xBF";

/// The JSON reader — see the module docs.
pub(crate) struct Json;

pub(crate) static JSON: Json = Json;

impl FormatReader for Json {
    fn format(&self) -> Format {
        Format::Json
    }

    fn extensions(&self) -> &'static [&'static str] {
        // GeoJSON is JSON: a FeatureCollection unwraps to one row per `features` entry.
        &["json", "ndjson", "jsonl", "geojson"]
    }

    fn sql(&self) -> SqlSupport {
        // Not DataFusion's JSON reader: it infers line by line — the failure this reader exists to
        // fix — and knows no records path, flattening or widening. Through this one, every layout
        // reads in SQL as it does in the grid, streamed a pass per query.
        SqlSupport::Streamed(pass)
    }

    /// An object is read as its bytes arrive: each read a request, a located records member a
    /// ranged one.
    fn streams_objects(&self) -> bool {
        true
    }

    fn schema(&self, input: Input<'_>, opts: &ReadOptions<'_>) -> Result<FileSchema> {
        let (schema, shape) = schema_and_shape(input, opts)?;
        Ok(FileSchema {
            schema,
            records_path: shape.records_path().map(str::to_string),
        })
    }

    fn read(
        &self,
        ctx: &RequestContext,
        input: Input<'_>,
        opts: &ReadOptions<'_>,
        row_limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        read_rows(ctx, input, opts, row_limit)
    }
}

/// How a JSON source lays out its records — see the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Shape {
    Values,
    Array,
    /// The records are the elements of the array at this JSON Pointer (e.g. `/data`).
    Records(String),
}

impl Shape {
    /// The JSON Pointer the rows were read from, when they came from inside the document.
    pub(crate) fn records_path(&self) -> Option<&str> {
        match self {
            Shape::Records(path) => Some(path),
            Shape::Values | Shape::Array => None,
        }
    }
}

/// The input classified.
enum Plan {
    /// Streamed from the input, value by value.
    Values,
    /// Streamed from the input through [`ArrayToNdjson`].
    Array,
    /// The records array inside one document, streamed through [`ArrayToNdjson`] from where it
    /// lies.
    Member(Member),
    /// One document, parsed whole into the one row it is: the document itself, or — with `path`
    /// set — the object an explicit records path points at.
    Parsed { path: Option<String>, row: Value },
}

impl Plan {
    fn shape(&self) -> Shape {
        match self {
            Plan::Values | Plan::Parsed { path: None, .. } => Shape::Values,
            Plan::Array => Shape::Array,
            Plan::Member(member) => Shape::Records(member.path.clone()),
            Plan::Parsed {
                path: Some(path), ..
            } => Shape::Records(path.clone()),
        }
    }
}

/// Where a document's records are: the JSON Pointer they were found at, and the bytes of their
/// array, `[` to `]`, counted from the start of the input.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    path: String,
    start: usize,
    len: usize,
}

/// The schema of `input` and the shape it was read as. The caller's records path
/// ([`ReadOptions::json_path`]) overrides detection, with `""` meaning the whole document,
/// unwrapped by nothing.
fn schema_and_shape(input: Input<'_>, opts: &ReadOptions<'_>) -> Result<(SchemaRef, Shape)> {
    let key = cache_key(input, opts.json_path);
    let plan = plan(input, opts.json_path, key.as_ref())?;
    let schema = infer(input, &plan, key.as_ref())?;
    Ok((schema, plan.shape()))
}

/// Read up to `row_limit` rows (all of them when `None`), with the schema [`schema_and_shape`]
/// reports.
///
/// Unless a value past the sample disagrees with it — an integer column that turns to text at row
/// 50,000. Then the rows this read covers are inferred in full and, when that says something wider,
/// read again with it; the wider schema is remembered for the file, so its schema only ever
/// widens, and a read never fails on a drift the sample could not see.
fn read_rows(
    ctx: &RequestContext,
    input: Input<'_>,
    opts: &ReadOptions<'_>,
    row_limit: Option<usize>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let key = cache_key(input, opts.json_path);
    let plan = plan(input, opts.json_path, key.as_ref())?;
    let schema = infer(input, &plan, key.as_ref())?;
    let batch_size = batch_size_for(opts, row_limit);
    let decode_with = |schema: &SchemaRef| match &plan {
        Plan::Parsed { row, .. } => serialize(
            ctx,
            schema,
            batch_size,
            row_limit,
            std::slice::from_ref(row),
        ),
        streamed => decode(ctx, schema, batch_size, row_limit, stream(input, streamed)?),
    };
    match decode_with(&schema) {
        Ok(batches) => Ok((schema, batches)),
        // Only a stream can disagree with its sample; a parsed document was inferred whole.
        Err(EngineError::Arrow(msg)) if !matches!(plan, Plan::Parsed { .. }) => {
            let covered = row_limit.unwrap_or(usize::MAX).max(SAMPLE_VALUES);
            let wider = infer_stream(stream(input, &plan)?, covered)?;
            if wider == schema {
                return Err(EngineError::Arrow(msg));
            }
            if let Some(key) = key {
                SCHEMAS.put(key, wider.clone());
            }
            Ok((wider.clone(), decode_with(&wider)?))
        }
        Err(e) => Err(e),
    }
}

/// Every record of `input` — a local file, or an object in a store — read as [`read_rows`] reads
/// it, decoded as batches of `schema` and handed to `each` until it returns `false`: SQL's
/// [`super::PassFn`] for JSON. `schema` is what the query was planned with: this reader's schema of
/// the input, sampled or widened since.
///
/// A planned query cannot change its schema partway, so unlike a grid read a record that does not
/// fit `schema` ends the pass. If the whole input infers wider — a value past the sample that it
/// did not anticipate — the wider schema becomes its own, as a grid read's would, and the pass
/// returns [`Pass::Widened`] for the query to be planned again. Otherwise the record is malformed,
/// and that is the error.
pub(crate) fn pass(
    input: Input<'_>,
    opts: &ReadOptions<'_>,
    schema: &SchemaRef,
    each: &mut dyn FnMut(RecordBatch) -> bool,
) -> Result<Pass> {
    let key = cache_key(input, opts.json_path);
    let plan = plan(input, opts.json_path, key.as_ref())?;
    let decoded = match &plan {
        // One row, and its schema was inferred from it: the two disagree only if the file changed
        // since the query was planned.
        Plan::Parsed { row, .. } => serialize(
            &RequestContext::detached(),
            schema,
            opts.batch_size,
            None,
            std::slice::from_ref(row),
        )
        .and_then(|batches| hand_over(batches.into_iter().map(Ok), each)),
        streamed => decode_each(schema, opts.batch_size, stream(input, streamed)?, each),
    };
    match decoded {
        Ok(()) => Ok(Pass::Done),
        // Not for a parsed row: its file is not its records (the row can sit below a records
        // path), so inferring the file would cache a schema that is not the row's.
        Err(EngineError::Arrow(msg)) if !matches!(plan, Plan::Parsed { .. }) => {
            let wider = infer_stream(stream(input, &plan)?, usize::MAX)?;
            if wider == *schema {
                return Err(EngineError::Arrow(msg));
            }
            if let Some(key) = key {
                SCHEMAS.put(key, wider);
            }
            Ok(Pass::Widened(msg))
        }
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------------------------
// Classifying the input
// ---------------------------------------------------------------------------------------------

/// How to read `input`: its layout detected, or the caller's records path applied. A layout that
/// streams is remembered for the version `key` names, so the next read — the grid's next window, a
/// query's next pass — goes straight to its records: to a member's bytes, located once, and for an
/// object, without a request spent on where its rows begin.
fn plan(input: Input<'_>, explicit: Option<&str>, key: Option<&CacheKey>) -> Result<Plan> {
    if let Some(layout) = key.and_then(|key| LAYOUTS.get(key)) {
        return Ok(layout.plan());
    }
    let plan = classify(input, explicit)?;
    if let (Some(key), Some(layout)) = (key, Layout::of(&plan)) {
        LAYOUTS.put(key.clone(), layout);
    }
    Ok(plan)
}

/// A [`Plan`] that streams, as remembered per version. A document read whole as its one row is
/// not: the row is the document, and is read again each time.
#[derive(Debug, Clone)]
enum Layout {
    Values,
    Array,
    Member(Member),
}

impl Layout {
    /// The layout `plan` reads, if it is one to remember.
    fn of(plan: &Plan) -> Option<Layout> {
        match plan {
            Plan::Values => Some(Layout::Values),
            Plan::Array => Some(Layout::Array),
            Plan::Member(member) => Some(Layout::Member(member.clone())),
            Plan::Parsed { .. } => None,
        }
    }

    /// The plan that reads this layout.
    fn plan(self) -> Plan {
        match self {
            Layout::Values => Plan::Values,
            Layout::Array => Plan::Array,
            Layout::Member(member) => Plan::Member(member),
        }
    }
}

/// [`plan`] the long way: the input read to learn its layout, or to apply the caller's path.
fn classify(input: Input<'_>, explicit: Option<&str>) -> Result<Plan> {
    let mut r = open(input)?;
    let first = first_byte(&mut *r)?;
    if let Some(path) = explicit {
        return explicit_plan(input, &mut *r, first, path);
    }
    match first {
        Some(b'[') => Ok(Plan::Array),
        Some(b'{') => {
            // One document, or the first of many? NDJSON answers on its first line. A single
            // document is scanned to its end — no parse, no allocation — before anything is spent
            // on it, and one too large to hold is read as the single row it is.
            if !is_single_value(&mut *r)? || len(input)? > DOCUMENT_MAX_BYTES {
                return Ok(Plan::Values);
            }
            let doc = Document::read(input)?;
            Ok(match doc.records_member()? {
                Some(member) => Plan::Member(member),
                None => Plan::Parsed {
                    path: None,
                    row: doc.parse()?,
                },
            })
        }
        // Empty input is zero rows; anything else the decoder reports in its own words.
        _ => Ok(Plan::Values),
    }
}

/// The caller named the records path, so nothing is detected: `path` is read from inside the
/// one document the input must be, or — for `""` — the input is read as it is, unwrapped by
/// nothing.
fn explicit_plan(
    input: Input<'_>,
    r: &mut dyn BufRead,
    first: Option<u8>,
    path: &str,
) -> Result<Plan> {
    if path.is_empty() {
        return Ok(match first {
            Some(b'[') => Plan::Array,
            Some(b'{') if is_single_value(r)? && len(input)? <= DOCUMENT_MAX_BYTES => {
                Plan::Parsed {
                    path: None,
                    row: Document::read(input)?.parse()?,
                }
            }
            _ => Plan::Values,
        });
    }
    if first.is_none() {
        return Err(EngineError::Query(format!(
            "the JSON input is empty, so there is nothing at `{path}`"
        )));
    }
    if !matches!(first, Some(b'{') | Some(b'[')) || !is_single_value(r)? {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "a JSON path (`{path}`) selects records inside one JSON document, but this input \
                 is a sequence of values (NDJSON / JSON Lines)"
            ),
        });
    }
    let size = len(input)?;
    if size > DOCUMENT_MAX_BYTES {
        return Err(EngineError::TooLarge(format!(
            "the JSON document is {size} bytes; finding the records at `{path}` holds it in \
             memory, which is limited to {DOCUMENT_MAX_BYTES} bytes"
        )));
    }
    let doc = Document::read(input)?;
    let nothing = || EngineError::Query(format!("nothing at `{path}` in this JSON document"));
    let value = doc.locate(path)?.ok_or_else(nothing)?;
    match value.get().as_bytes().first() {
        Some(b'[') => Ok(Plan::Member(doc.member(path.to_string(), value))),
        Some(b'{') => Ok(Plan::Parsed {
            path: Some(path.to_string()),
            row: parse(value.get())?,
        }),
        Some(b'n') | None => Err(nothing()),
        Some(_) => Err(EngineError::Query(format!(
            "`{path}` holds a single {}, not records",
            kind(value)
        ))),
    }
}

/// What a JSON value is, which its first byte says.
fn kind(value: &RawValue) -> &'static str {
    match value.get().as_bytes().first() {
        Some(b'"') => "string",
        Some(b't' | b'f') => "boolean",
        Some(b'[') => "array",
        Some(b'{') => "object",
        Some(b'n') | None => "null",
        Some(_) => "number",
    }
}

/// Open `input` as a buffered reader positioned after any byte-order mark.
fn open<'a>(input: Input<'a>) -> Result<Box<dyn BufRead + Send + 'a>> {
    let mut r: Box<dyn BufRead + Send + 'a> = match input {
        Input::File(path) => Box::new(BufReader::with_capacity(64 * 1024, File::open(path)?)),
        Input::Bytes(bytes) => Box::new(bytes),
        Input::Object(object) => object.open(None)?,
    };
    if r.fill_buf()?.starts_with(BOM) {
        r.consume(BOM.len());
    }
    Ok(r)
}

/// Open a records member's array — its bytes only — as a buffered reader.
fn open_member<'a>(input: Input<'a>, member: &Member) -> Result<Box<dyn BufRead + Send + 'a>> {
    Ok(match input {
        Input::File(path) => {
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(member.start as u64))?;
            Box::new(BufReader::with_capacity(
                64 * 1024,
                file.take(member.len as u64),
            ))
        }
        // Located in these very bytes, so in range. Were it not, the read would be empty, which
        // the array reader reports as a truncated array — an error, not a panic.
        Input::Bytes(bytes) => Box::new(
            bytes
                .get(member.start..member.start.saturating_add(member.len))
                .unwrap_or_default(),
        ),
        // A request for the member's bytes alone. Located in this very version, which every
        // request is pinned to, so in range.
        Input::Object(object) => {
            let start = member.start as u64;
            object.open(Some(start..start + member.len as u64))?
        }
    })
}

fn len(input: Input<'_>) -> Result<u64> {
    Ok(match input {
        Input::File(path) => std::fs::metadata(path)?.len(),
        Input::Bytes(bytes) => bytes.len() as u64,
        Input::Object(object) => object.size(),
    })
}

/// The first byte that is not whitespace, left unconsumed. `None` for empty input.
fn first_byte(r: &mut dyn BufRead) -> std::io::Result<Option<u8>> {
    loop {
        let buf = r.fill_buf()?;
        if buf.is_empty() {
            return Ok(None);
        }
        match buf.iter().position(|b| !b.is_ascii_whitespace()) {
            Some(i) => {
                let b = buf[i];
                r.consume(i);
                return Ok(Some(b));
            }
            None => {
                let n = buf.len();
                r.consume(n);
            }
        }
    }
}

/// Is the value starting here the only one in the input? Scans bytes — strings, escapes and
/// nesting only — so a multi-gigabyte document costs a pass at memory speed and no allocation,
/// and NDJSON costs its first line. Truncated input answers `false`: the decoder then reports it.
fn is_single_value(r: &mut dyn BufRead) -> std::io::Result<bool> {
    let (mut depth, mut in_string, mut escape, mut closed) = (0i64, false, false, false);
    loop {
        let buf = r.fill_buf()?;
        if buf.is_empty() {
            return Ok(closed);
        }
        for &b in buf {
            if closed {
                if !b.is_ascii_whitespace() {
                    return Ok(false);
                }
            } else if in_string {
                if escape {
                    escape = false;
                } else if b == b'\\' {
                    escape = true;
                } else if b == b'"' {
                    in_string = false;
                }
            } else {
                match b {
                    b'"' => in_string = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        closed = depth == 0;
                    }
                    _ => {}
                }
            }
        }
        let n = buf.len();
        r.consume(n);
    }
}

// ---------------------------------------------------------------------------------------------
// Finding records inside one document
// ---------------------------------------------------------------------------------------------

/// One JSON document's text, held whole to find its records in. Values in it are *located* — as
/// the slices serde_json's [`RawValue`] borrows — not built, so finding a member costs a pass over
/// the text and no memory beyond it; building the values would cost seconds on a large document,
/// and several times its size in memory.
struct Document<'a> {
    text: Cow<'a, str>,
    /// Where the document starts in `text`: past a byte-order mark, if there is one.
    base: usize,
}

impl<'a> Document<'a> {
    /// Read the whole of `input`. The caller has already checked its size.
    fn read(input: Input<'a>) -> Result<Self> {
        let owned = |bytes: Vec<u8>| -> Result<Cow<'a, str>> {
            Ok(Cow::Owned(
                String::from_utf8(bytes).map_err(|e| invalid(e.utf8_error()))?,
            ))
        };
        let text = match input {
            Input::File(path) => owned(std::fs::read(path)?)?,
            Input::Bytes(bytes) => Cow::Borrowed(std::str::from_utf8(bytes).map_err(invalid)?),
            Input::Object(object) => {
                let mut bytes = Vec::with_capacity(object.size() as usize);
                object.open(None)?.read_to_end(&mut bytes)?;
                owned(bytes)?
            }
        };
        let base = if text.as_bytes().starts_with(BOM) {
            BOM.len()
        } else {
            0
        };
        Ok(Document { text, base })
    }

    fn body(&self) -> &str {
        &self.text[self.base..]
    }

    /// The document as the one row it is.
    fn parse(&self) -> Result<Value> {
        parse(self.body())
    }

    /// The array `value` — borrowed from this document — as a records member at `path`.
    fn member(&self, path: String, value: &RawValue) -> Member {
        Member {
            path,
            // Both point into `text`, so this is the value's offset in the input, mark included.
            start: value.get().as_ptr() as usize - self.text.as_ptr() as usize,
            len: value.get().len(),
        }
    }

    /// The member of this document — a single top-level object — that holds its records: the
    /// *only* one whose value is a non-empty array of objects. `None` when there is none, or more
    /// than one to choose between: the document is then read as one row, which is what it is.
    fn records_member(&self) -> Result<Option<Member>> {
        let members: HashMap<String, &RawValue> =
            serde_json::from_str(self.body()).map_err(invalid)?;
        let mut found = None;
        for (key, value) in members {
            if holds_records(value)? {
                if found.is_some() {
                    return Ok(None);
                }
                found = Some((key, value));
            }
        }
        Ok(found.map(|(key, value)| self.member(pointer(&key), value)))
    }

    /// The value at a JSON Pointer, found as [`Value::pointer`] finds one (RFC 6901: `~1` is `/`,
    /// `~0` is `~`, an array index has no sign and no leading zero) but located, not built. `None`
    /// when nothing is there.
    fn locate(&self, pointer: &str) -> Result<Option<&RawValue>> {
        let mut at: &RawValue = serde_json::from_str(self.body()).map_err(invalid)?;
        let Some(tokens) = pointer.strip_prefix('/') else {
            return Ok(pointer.is_empty().then_some(at));
        };
        for token in tokens.split('/') {
            let token = token.replace("~1", "/").replace("~0", "~");
            let next = match at.get().as_bytes().first() {
                Some(b'{') => serde_json::from_str::<HashMap<String, &RawValue>>(at.get())
                    .map_err(invalid)?
                    .remove(&token),
                Some(b'[') => match index(&token) {
                    Some(i) => serde_json::from_str::<Vec<&RawValue>>(at.get())
                        .map_err(invalid)?
                        .get(i)
                        .copied(),
                    None => None,
                },
                _ => None,
            };
            let Some(next) = next else {
                return Ok(None);
            };
            at = next;
        }
        Ok(Some(at))
    }
}

/// Is `value` a non-empty array of objects? Its elements are stepped over, not built: this runs
/// over every array member of a document, which can be most of the file.
fn holds_records(value: &RawValue) -> Result<bool> {
    if !value.get().starts_with('[') {
        return Ok(false);
    }
    Ok(serde_json::from_str::<AllObjects>(value.get())
        .map_err(invalid)?
        .0)
}

/// A JSON array read for one fact: whether it has elements, and every one is an object.
struct AllObjects(bool);

impl<'de> Deserialize<'de> for AllObjects {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Elements;
        impl<'de> Visitor<'de> for Elements {
            type Value = AllObjects;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON array")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut elements: A,
            ) -> std::result::Result<AllObjects, A::Error> {
                let (mut any, mut objects) = (false, true);
                while let Some(element) = elements.next_element::<&'de RawValue>()? {
                    any = true;
                    objects &= element.get().starts_with('{');
                }
                Ok(AllObjects(any && objects))
            }
        }
        d.deserialize_seq(Elements)
    }
}

/// An array index in a JSON Pointer, read as [`Value::pointer`] reads one.
fn index(token: &str) -> Option<usize> {
    if token.starts_with('+') || (token.starts_with('0') && token.len() != 1) {
        return None;
    }
    token.parse().ok()
}

fn parse(text: &str) -> Result<Value> {
    serde_json::from_str(text).map_err(invalid)
}

fn invalid(e: impl std::fmt::Display) -> EngineError {
    EngineError::Query(format!("invalid JSON: {e}"))
}

/// A top-level member name as a JSON Pointer (RFC 6901: `~` → `~0`, `/` → `~1`).
fn pointer(key: &str) -> String {
    format!("/{}", key.replace('~', "~0").replace('/', "~1"))
}

// ---------------------------------------------------------------------------------------------
// Inference and decoding
// ---------------------------------------------------------------------------------------------

/// The schema to read `input` with: a parsed document's from its one row, a stream's from its
/// first [`SAMPLE_VALUES`] values — once per file version (`key`), or as widened since by
/// [`read_rows`].
fn infer(input: Input<'_>, plan: &Plan, key: Option<&CacheKey>) -> Result<SchemaRef> {
    if let Plan::Parsed { row, .. } = plan {
        return schema_of(std::iter::once(Ok(row)));
    }
    if let Some(schema) = key.and_then(|key| SCHEMAS.get(key)) {
        return Ok(schema);
    }
    let schema = infer_stream(stream(input, plan)?, SAMPLE_VALUES)?;
    if let Some(key) = key {
        SCHEMAS.put(key.clone(), schema.clone());
    }
    Ok(schema)
}

/// The input as a stream of whitespace-separated values — arrays re-emitted element by element.
fn stream<'a>(input: Input<'a>, plan: &Plan) -> Result<Box<dyn BufRead + Send + 'a>> {
    Ok(match plan {
        Plan::Array => Box::new(ArrayToNdjson::new(open(input)?)),
        Plan::Member(member) => Box::new(ArrayToNdjson::new(open_member(input, member)?)),
        Plan::Values | Plan::Parsed { .. } => open(input)?,
    })
}

/// Infer over the first `n` values of a stream, holding one value at a time.
fn infer_stream(r: impl Read, n: usize) -> Result<SchemaRef> {
    schema_of(
        serde_json::Deserializer::from_reader(r)
            .into_iter::<Value>()
            .take(n)
            .map(|v| {
                v.map_err(|e| {
                    if e.is_io() {
                        // The bytes could not be read: not the JSON's fault (see `read_error`).
                        ArrowError::IoError(e.to_string(), e.into())
                    } else {
                        ArrowError::JsonError(format!("invalid JSON: {e}"))
                    }
                })
            }),
    )
}

fn schema_of<V: Borrow<Value>>(
    values: impl Iterator<Item = std::result::Result<V, ArrowError>>,
) -> Result<SchemaRef> {
    let schema = arrow_json::reader::infer_json_schema_from_iterator(values).map_err(read_error)?;
    Ok(Arc::new(schema))
}

// ---------------------------------------------------------------------------------------------
// What is known about a file, per version
// ---------------------------------------------------------------------------------------------

/// A file or an object, as of one version of its bytes, read through one records path. `serve`
/// asks for the same file's schema, and reads its records, on every grid scroll; this is what
/// makes finding both a lookup instead of a pass.
///
/// Callers take the key before reading the input, so a write that races a read files what that
/// read learned under a version that no longer exists, never under the new one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    version: Version,
    records_path: Option<String>,
}

/// One version of an input's bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Version {
    /// A local file, as of its length and modification time.
    File {
        path: PathBuf,
        len: u64,
        modified: SystemTime,
    },
    /// An object, as one credential identity sees it, as of the version its store reports, which
    /// its reads are pinned to.
    Object {
        uri: String,
        identity: String,
        size: u64,
        version: String,
    },
}

/// Files and objects: bytes an engine already holds have no version to key them by, and are read
/// afresh.
fn cache_key(input: Input<'_>, records_path: Option<&str>) -> Option<CacheKey> {
    let version = match input {
        Input::File(path) => {
            let meta = std::fs::metadata(path).ok()?;
            Version::File {
                path: path.to_path_buf(),
                len: meta.len(),
                modified: meta.modified().ok()?,
            }
        }
        Input::Object(object) => Version::Object {
            uri: object.uri().to_string(),
            identity: object.identity().to_string(),
            size: object.size(),
            version: object.version().to_string(),
        },
        Input::Bytes(_) => return None,
    };
    Some(CacheKey {
        version,
        records_path: records_path.map(str::to_string),
    })
}

/// One fact per file version. Enough entries for the files one person has open; clearing when
/// full keeps it bounded without the bookkeeping an LRU would add for a map that is cheap to
/// refill.
struct Cache<V>(OnceLock<Mutex<HashMap<CacheKey, V>>>);

const CACHE_ENTRIES: usize = 64;

impl<V: Clone> Cache<V> {
    const fn new() -> Self {
        Cache(OnceLock::new())
    }

    fn map(&self) -> MutexGuard<'_, HashMap<CacheKey, V>> {
        self.0
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn get(&self, key: &CacheKey) -> Option<V> {
        self.map().get(key).cloned()
    }

    fn put(&self, key: CacheKey, value: V) {
        let mut map = self.map();
        if map.len() >= CACHE_ENTRIES {
            map.clear();
        }
        map.insert(key, value);
    }
}

/// Stream schemas: sampled, or widened since by [`read_rows`].
static SCHEMAS: Cache<SchemaRef> = Cache::new();

/// Layouts: found once, since finding one reads the start of the input, and locating a records
/// member reads its document whole.
static LAYOUTS: Cache<Layout> = Cache::new();

fn builder(schema: &SchemaRef, batch_size: usize) -> arrow_json::ReaderBuilder {
    arrow_json::ReaderBuilder::new(schema.clone())
        .with_batch_size(batch_size)
        .with_coerce_primitive(true)
}

fn decode(
    ctx: &RequestContext,
    schema: &SchemaRef,
    batch_size: usize,
    row_limit: Option<usize>,
    r: impl BufRead,
) -> Result<Vec<RecordBatch>> {
    let reader = builder(schema, batch_size)
        .build(r)
        .map_err(EngineError::arrow)?;
    collect(ctx, reader, row_limit)
}

/// [`decode`] a batch at a time: each handed to `each` as it is decoded, until it returns `false`.
fn decode_each(
    schema: &SchemaRef,
    batch_size: usize,
    r: impl BufRead,
    each: &mut dyn FnMut(RecordBatch) -> bool,
) -> Result<()> {
    let reader = builder(schema, batch_size)
        .build(r)
        .map_err(EngineError::arrow)?;
    hand_over(reader.map(|batch| batch.map_err(read_error)), each)
}

/// Hand `batches` to `each` in order until it returns `false`, which leaves the rest undecoded.
fn hand_over(
    batches: impl Iterator<Item = Result<RecordBatch>>,
    each: &mut dyn FnMut(RecordBatch) -> bool,
) -> Result<()> {
    for batch in batches {
        if !each(batch?) {
            break;
        }
    }
    Ok(())
}

/// Decode already-parsed rows, `batch_size` at a time, stopping at `row_limit`.
fn serialize(
    ctx: &RequestContext,
    schema: &SchemaRef,
    batch_size: usize,
    row_limit: Option<usize>,
    rows: &[Value],
) -> Result<Vec<RecordBatch>> {
    let rows = &rows[..row_limit.map_or(rows.len(), |n| n.min(rows.len()))];
    let mut decoder = builder(schema, batch_size)
        .build_decoder()
        .map_err(EngineError::arrow)?;
    let mut batches = Vec::new();
    for chunk in rows.chunks(batch_size.max(1)) {
        ctx.check()?;
        decoder.serialize(chunk).map_err(EngineError::arrow)?;
        if let Some(batch) = decoder.flush().map_err(EngineError::arrow)? {
            batches.push(batch);
        }
    }
    Ok(batches)
}

// ---------------------------------------------------------------------------------------------
// Streaming a top-level array
// ---------------------------------------------------------------------------------------------

/// Re-emits a top-level JSON array as newline-delimited JSON as it is read: the `[` is dropped,
/// commas between elements become newlines, and reading stops at the closing `]`. Strings are
/// tracked so brackets and commas inside them are left alone.
///
/// A port of DataFusion's `JsonArrayToNdjsonReader` (`datafusion-datasource-json` 54, Apache-2.0),
/// trimmed to a `BufRead` source — the lean build has no DataFusion to borrow it from. One change:
/// input that ends before the array closes is an error, not a quietly shorter table.
pub(crate) struct ArrayToNdjson<R> {
    inner: R,
    scan: ArrayScan,
    out: Vec<u8>,
    pos: usize,
}

impl<R: BufRead> ArrayToNdjson<R> {
    pub(crate) fn new(inner: R) -> Self {
        ArrayToNdjson {
            inner,
            scan: ArrayScan::default(),
            out: Vec::with_capacity(64 * 1024),
            pos: 0,
        }
    }

    /// Transform the next chunk of input into `out`. `false` at a clean end of input.
    fn refill(&mut self) -> std::io::Result<bool> {
        self.out.clear();
        self.pos = 0;
        while self.out.is_empty() {
            let buf = self.inner.fill_buf()?;
            if buf.is_empty() {
                self.scan.finish()?;
                return Ok(false);
            }
            let n = buf.len();
            for &b in buf {
                if let Some(o) = self.scan.step(b)? {
                    self.out.push(o);
                }
            }
            self.inner.consume(n);
        }
        Ok(true)
    }
}

impl<R: BufRead> Read for ArrayToNdjson<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let available = self.fill_buf()?;
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl<R: BufRead> BufRead for ArrayToNdjson<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.pos >= self.out.len() && !self.refill()? {
            return Ok(&[]);
        }
        Ok(&self.out[self.pos..])
    }

    fn consume(&mut self, amt: usize) {
        self.pos = (self.pos + amt).min(self.out.len());
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum ArrayState {
    #[default]
    BeforeOpen,
    InArray,
    AfterClose,
}

/// The byte-level state [`ArrayToNdjson`] carries between chunks.
#[derive(Debug, Default)]
struct ArrayScan {
    state: ArrayState,
    depth: i64,
    in_string: bool,
    escape: bool,
}

impl ArrayScan {
    /// The byte to emit for `b`, if any.
    fn step(&mut self, b: u8) -> std::io::Result<Option<u8>> {
        match self.state {
            ArrayState::BeforeOpen => match b {
                b'[' => {
                    self.state = ArrayState::InArray;
                    Ok(None)
                }
                b if b.is_ascii_whitespace() => Ok(None),
                _ => Err(malformed("expected `[` to open a JSON array")),
            },
            ArrayState::InArray if self.in_string => {
                if self.escape {
                    self.escape = false;
                } else if b == b'\\' {
                    self.escape = true;
                } else if b == b'"' {
                    self.in_string = false;
                }
                Ok(Some(b))
            }
            ArrayState::InArray => Ok(match b {
                b'"' => {
                    self.in_string = true;
                    Some(b)
                }
                b'{' | b'[' => {
                    self.depth += 1;
                    Some(b)
                }
                b'}' => {
                    self.depth -= 1;
                    Some(b)
                }
                b']' if self.depth == 0 => {
                    self.state = ArrayState::AfterClose;
                    None
                }
                b']' => {
                    self.depth -= 1;
                    Some(b)
                }
                b',' if self.depth == 0 => Some(b'\n'),
                b if self.depth == 0 && b.is_ascii_whitespace() => None,
                b => Some(b),
            }),
            ArrayState::AfterClose if b.is_ascii_whitespace() => Ok(None),
            ArrayState::AfterClose => Err(malformed("unexpected content after the closing `]`")),
        }
    }

    /// End of input: only a closed array is complete.
    fn finish(&self) -> std::io::Result<()> {
        match self.state {
            ArrayState::AfterClose => Ok(()),
            ArrayState::BeforeOpen | ArrayState::InArray => Err(malformed(
                "the JSON array ends before its closing `]` — the file may be truncated",
            )),
        }
    }
}

fn malformed(what: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("malformed JSON: {what}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::testing::MemObject;
    use crate::format::RemoteObject;

    fn ndjson(input: &str) -> std::io::Result<String> {
        let mut out = String::new();
        ArrayToNdjson::new(input.as_bytes()).read_to_string(&mut out)?;
        Ok(out)
    }

    #[test]
    fn an_array_streams_as_one_element_per_line() {
        assert_eq!(
            ndjson(" [ {\"a\": 1} ,\n {\"b\": [1, 2]}, {\"c\": \"x,y]\"} ] \n").unwrap(),
            "{\"a\": 1}\n{\"b\": [1, 2]}\n{\"c\": \"x,y]\"}"
        );
    }

    #[test]
    fn brackets_quotes_and_escapes_inside_strings_are_data() {
        let input = r#"[{"s": "a \"quoted\" ], [ , value \\"}, {"t": "\\\\"}]"#;
        assert_eq!(
            ndjson(input).unwrap(),
            "{\"s\": \"a \\\"quoted\\\" ], [ , value \\\\\"}\n{\"t\": \"\\\\\\\\\"}"
        );
    }

    #[test]
    fn a_truncated_or_padded_array_is_an_error_not_a_shorter_table() {
        let err = ndjson("[{\"a\": 1}, {\"a\": 2").unwrap_err();
        assert!(err.to_string().contains("truncated"), "{err}");
        let err = ndjson("[{\"a\": 1}] {\"a\": 2}").unwrap_err();
        assert!(err.to_string().contains("after the closing"), "{err}");
    }

    #[test]
    fn an_empty_array_is_empty() {
        assert_eq!(ndjson("[]").unwrap(), "");
        assert_eq!(ndjson("  [ \n ]  ").unwrap(), "");
    }

    fn shape_of(input: &str) -> Shape {
        plan(Input::Bytes(input.as_bytes()), None, None)
            .unwrap()
            .shape()
    }

    #[test]
    fn the_shape_comes_from_the_bytes() {
        assert_eq!(shape_of("{\"a\":1}\n{\"a\":2}\n"), Shape::Values);
        assert_eq!(
            shape_of("{\n  \"a\": 1\n}\n{\n  \"a\": 2\n}"),
            Shape::Values
        );
        assert_eq!(shape_of("  [{\"a\":1}]"), Shape::Array);
        assert_eq!(shape_of(""), Shape::Values);
        assert_eq!(
            shape_of("{\"meta\": {\"n\": 2}, \"data\": [{\"a\": 1}, {\"a\": 2}]}"),
            Shape::Records("/data".to_string())
        );
        // With a BOM in front, all the same.
        assert_eq!(shape_of("\u{feff}[{\"a\":1}]"), Shape::Array);
    }

    #[test]
    fn only_an_unambiguous_member_is_unwrapped() {
        // Two candidate members: nothing to choose between, so the object is one row.
        assert_eq!(
            shape_of("{\"users\": [{\"a\": 1}], \"groups\": [{\"b\": 2}]}"),
            Shape::Values
        );
        // An array of scalars, or an empty one, holds no records.
        assert_eq!(shape_of("{\"ids\": [1, 2, 3]}"), Shape::Values);
        assert_eq!(shape_of("{\"data\": []}"), Shape::Values);
        // A mixed array is not an array of records either.
        assert_eq!(shape_of("{\"data\": [{\"a\": 1}, 2]}"), Shape::Values);
    }

    #[test]
    fn a_member_name_is_escaped_as_a_json_pointer() {
        assert_eq!(pointer("data"), "/data");
        assert_eq!(pointer("a/b~c"), "/a~1b~0c");
    }

    /// The records member `plan` locates in `input`: its pointer, and the text of the bytes it
    /// says the array spans.
    fn located<'a>(input: &'a str, json_path: Option<&str>) -> (String, &'a str) {
        match plan(Input::Bytes(input.as_bytes()), json_path, None).unwrap() {
            Plan::Member(m) => (m.path, &input[m.start..m.start + m.len]),
            _ => panic!("no records member located in {input}"),
        }
    }

    #[test]
    fn a_records_member_is_located_as_the_bytes_of_its_array() {
        // A byte-order mark, whitespace everywhere, and brackets inside strings, before and in it.
        let doc = "\u{feff} {\"meta\": {\"note\": \"[not, this]\"},\n \"data\" :\t[{\"a\": \"]\"},\n {\"a\": 2}]\n}\n";
        assert_eq!(
            located(doc, None),
            ("/data".to_string(), "[{\"a\": \"]\"},\n {\"a\": 2}]")
        );
        // Named as a pointer, escapes and all; and reached below the top level by one.
        let doc = r#"{"a/b~c": [{"x": 1}], "n": {"items": [{"y": 2}, {"y": 3}]}}"#;
        assert_eq!(
            located(doc, None),
            ("/a~1b~0c".to_string(), r#"[{"x": 1}]"#)
        );
        assert_eq!(
            located(doc, Some("/n/items")),
            ("/n/items".to_string(), r#"[{"y": 2}, {"y": 3}]"#)
        );
    }

    #[test]
    fn a_pointer_finds_what_serde_json_finds() {
        let text = r#"{"a/b": {"~k": [10, {"deep": [{"z": 1}]}, 30]}, "": {"": [1]},
                      "arr": [[{"q": 1}]], "0": "zero", "dup": 1, "dup": [{"d": 2}]}"#;
        let doc: Value = serde_json::from_str(text).unwrap();
        let located = Document::read(Input::Bytes(text.as_bytes())).unwrap();
        for pointer in [
            "",
            "/",
            "//",
            "/a~1b",
            "/a~1b/~0k",
            "/a~1b/~0k/0",
            "/a~1b/~0k/1/deep/0",
            "/a~1b/~0k/2",
            "/a~1b/~0k/3",
            "/a~1b/~0k/01",
            "/a~1b/~0k/+1",
            "/a~1b/~0k/-",
            "/arr/0/0/q",
            "/0",
            "/0/x",
            "/dup",
            "/missing",
            "no-slash",
        ] {
            let found = located
                .locate(pointer)
                .unwrap()
                .map(|v| serde_json::from_str::<Value>(v.get()).unwrap());
            assert_eq!(found.as_ref(), doc.pointer(pointer), "{pointer}");
        }
    }

    #[test]
    fn a_member_in_fetched_bytes_reads_like_one_in_a_file() {
        let doc =
            "\u{feff}{\"meta\": {\"n\": 3}, \"data\": [{\"a\": 1}, {\"a\": \"x\"}, {\"b\": true}]}";
        let opts = ReadOptions {
            batch_size: 2,
            csv_infer_rows: 100,
            json_path: None,
        };
        let input = Input::Bytes(doc.as_bytes());
        let (schema, shape) = schema_and_shape(input, &opts).unwrap();
        assert_eq!(shape, Shape::Records("/data".to_string()));
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        let (_, batches) = read_rows(&RequestContext::detached(), input, &opts, None).unwrap();
        let mut writer = arrow_json::ArrayWriter::new(Vec::new());
        writer
            .write_batches(&batches.iter().collect::<Vec<_>>())
            .unwrap();
        writer.finish().unwrap();
        let rows: Value = serde_json::from_slice(&writer.into_inner()).unwrap();
        assert_eq!(
            rows,
            serde_json::json!([{"a": "1"}, {"a": "x"}, {"b": true}])
        );
    }

    /// The schema a read of `input` is planned with — the one SQL registers it under.
    fn planned(input: Input<'_>, json_path: Option<&str>) -> SchemaRef {
        let opts = ReadOptions {
            batch_size: 2,
            csv_infer_rows: 100,
            json_path,
        };
        JSON.schema(input, &opts).unwrap().schema
    }

    /// The rows of `batches`, as JSON.
    fn json_rows(batches: &[RecordBatch]) -> Vec<Value> {
        let mut writer = arrow_json::ArrayWriter::new(Vec::new());
        writer
            .write_batches(&batches.iter().collect::<Vec<_>>())
            .unwrap();
        writer.finish().unwrap();
        serde_json::from_slice(&writer.into_inner()).unwrap_or_default()
    }

    /// The rows a grid read of `input` returns, up to `row_limit` of them, as JSON.
    fn rows_read(
        input: Input<'_>,
        json_path: Option<&str>,
        row_limit: Option<usize>,
    ) -> Vec<Value> {
        let opts = ReadOptions {
            batch_size: 2,
            csv_infer_rows: 100,
            json_path,
        };
        let (_, batches) = JSON
            .read(&RequestContext::detached(), input, &opts, row_limit)
            .unwrap();
        json_rows(&batches)
    }

    /// What a [`pass`] over `input` hands over — each batch's rows as JSON — when told to stop
    /// after `stop_after` batches, and how it ended.
    fn passed(
        input: Input<'_>,
        json_path: Option<&str>,
        schema: &SchemaRef,
        batch_size: usize,
        stop_after: usize,
    ) -> (Vec<Vec<Value>>, Result<Pass>) {
        let opts = ReadOptions {
            batch_size,
            csv_infer_rows: 100,
            json_path,
        };
        let mut batches = Vec::new();
        let ended = pass(input, &opts, schema, &mut |batch| {
            let mut writer = arrow_json::ArrayWriter::new(Vec::new());
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            batches.push(serde_json::from_slice(&writer.into_inner()).unwrap());
            batches.len() < stop_after
        });
        (batches, ended)
    }

    #[test]
    fn a_pass_hands_over_every_record_a_batch_at_a_time_until_told_to_stop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.ndjson");
        std::fs::write(&path, "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n").unwrap();
        let schema = planned(Input::File(&path), None);

        let (batches, ended) = passed(Input::File(&path), None, &schema, 2, usize::MAX);
        assert!(matches!(ended, Ok(Pass::Done)), "{ended:?}");
        assert_eq!(
            batches,
            [
                vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})],
                vec![serde_json::json!({"id": 3})]
            ]
        );

        // A query that has its rows stops the pass, so the rest of the file is not decoded.
        let (batches, ended) = passed(Input::File(&path), None, &schema, 2, 1);
        assert!(matches!(ended, Ok(Pass::Done)), "{ended:?}");
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn a_pass_reads_every_layout_as_a_grid_read_does() {
        let dir = tempfile::tempdir().unwrap();
        for (name, body, json_path, want) in [
            (
                "array.json",
                "[{\"a\": 1}, {\"a\": 2}]",
                None,
                serde_json::json!([{"a": 1}, {"a": 2}]),
            ),
            (
                "member.json",
                "{\"meta\": {}, \"data\": [{\"a\": 1}, {\"a\": 2}]}",
                None,
                serde_json::json!([{"a": 1}, {"a": 2}]),
            ),
            // The whole document, and an object a records path points at, are one row each.
            (
                "whole.json",
                "{\"meta\": {\"n\": 1}, \"data\": [{\"a\": 1}]}",
                Some(""),
                serde_json::json!([{"meta": {"n": 1}, "data": [{"a": 1}]}]),
            ),
            (
                "object.json",
                "{\"page\": {\"n\": 1, \"next\": \"x\"}}",
                Some("/page"),
                serde_json::json!([{"n": 1, "next": "x"}]),
            ),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            let schema = planned(Input::File(&path), json_path);
            let (batches, ended) = passed(Input::File(&path), json_path, &schema, 8, usize::MAX);
            assert!(matches!(ended, Ok(Pass::Done)), "{name}: {ended:?}");
            let rows: Vec<Value> = batches.into_iter().flatten().collect();
            assert_eq!(Value::Array(rows), want, "{name}");
        }
    }

    #[test]
    fn a_value_past_the_sample_that_does_not_fit_widens_the_file_and_ends_the_pass() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drift.ndjson");
        let mut body: String = (0..SAMPLE_VALUES)
            .map(|i| format!("{{\"v\": {i}}}\n"))
            .collect();
        body.push_str("{\"v\": \"late\"}\n");
        std::fs::write(&path, body).unwrap();
        let sampled = planned(Input::File(&path), None);
        assert_eq!(sampled.field(0).data_type(), &arrow_schema::DataType::Int64);

        let (_, ended) = passed(Input::File(&path), None, &sampled, 4096, usize::MAX);
        assert!(matches!(ended, Ok(Pass::Widened(_))), "{ended:?}");
        // The file's schema is the wider one now, so the query planned again reads every value.
        let wider = planned(Input::File(&path), None);
        assert_eq!(wider.field(0).data_type(), &arrow_schema::DataType::Utf8);
        let (batches, ended) = passed(Input::File(&path), None, &wider, 4096, usize::MAX);
        assert!(matches!(ended, Ok(Pass::Done)), "{ended:?}");
        let rows: Vec<Value> = batches.into_iter().flatten().collect();
        assert_eq!(rows.len(), SAMPLE_VALUES + 1);
        assert_eq!(rows[SAMPLE_VALUES], serde_json::json!({"v": "late"}));
    }

    #[test]
    fn a_malformed_record_past_the_sample_fails_the_pass_rather_than_widening_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.ndjson");
        let mut body: String = (0..SAMPLE_VALUES)
            .map(|i| format!("{{\"v\": {i}}}\n"))
            .collect();
        body.push_str("{\"v\": oops}\n");
        std::fs::write(&path, body).unwrap();
        let schema = planned(Input::File(&path), None);
        let (_, ended) = passed(Input::File(&path), None, &schema, 4096, usize::MAX);
        assert!(ended.is_err(), "{ended:?}");
        assert_eq!(
            planned(Input::File(&path), None),
            schema,
            "nothing to widen to"
        );
    }

    #[test]
    fn a_row_parsed_whole_that_changed_since_planning_fails_the_pass_rather_than_widening_it() {
        // A row read whole is its own sample, so it disagrees with the planned schema only when
        // the file changed in between. Widening would infer the file, which below a records path
        // is the whole document, not the row.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("page.json");
        std::fs::write(&path, "{\"page\": {\"n\": 1}}").unwrap();
        let schema = planned(Input::File(&path), Some("/page"));
        std::fs::write(&path, "{\"page\": {\"n\": \"one\"}}").unwrap();

        let (_, ended) = passed(Input::File(&path), Some("/page"), &schema, 8, usize::MAX);
        assert!(ended.is_err(), "{ended:?}");
        // The row's own schema, not the document's.
        let now = planned(Input::File(&path), Some("/page"));
        assert_eq!(now.field(0).name(), "n");
        assert_eq!(now.field(0).data_type(), &arrow_schema::DataType::Utf8);
    }

    /// `rows` NDJSON records, padded so that a few hundred fill a response's chunk.
    fn records(rows: usize) -> String {
        let pad = "x".repeat(40);
        (0..rows)
            .map(|i| format!("{{\"id\": {i}, \"pad\": \"{pad}\"}}\n"))
            .collect()
    }

    #[test]
    fn an_object_reads_as_its_file_does() {
        let dir = tempfile::tempdir().unwrap();
        for (name, body, json_path) in [
            (
                "values.ndjson",
                "{\"a\": 1}\n{\"a\": \"x\", \"b\": true}\n",
                None,
            ),
            ("array.json", "[{\"a\": 1}, {\"b\": 2}]", None),
            // Its records' offsets count the byte-order mark, as a ranged request must.
            (
                "member.json",
                "\u{feff}{\"meta\": {}, \"data\": [{\"a\": 1}, {\"a\": 2}]}",
                None,
            ),
            (
                "whole.json",
                "{\"meta\": {\"n\": 1}, \"data\": [{\"a\": 1}]}",
                Some(""),
            ),
            (
                "object.json",
                "{\"page\": {\"n\": 1, \"next\": \"x\"}}",
                Some("/page"),
            ),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            let object = MemObject::new(&format!("s3://bucket/{name}"), "v1", body);
            let opts = ReadOptions {
                batch_size: 2,
                csv_infer_rows: 100,
                json_path,
            };
            let file = JSON.schema(Input::File(&path), &opts).unwrap();
            let read = JSON.schema(Input::Object(&object), &opts).unwrap();
            assert_eq!(read.schema, file.schema, "{name}");
            assert_eq!(read.records_path, file.records_path, "{name}");

            let rows = rows_read(Input::File(&path), json_path, None);
            assert!(!rows.is_empty(), "{name}");
            assert_eq!(
                rows_read(Input::Object(&object), json_path, None),
                rows,
                "{name}"
            );
            let (batches, ended) = passed(
                Input::Object(&object),
                json_path,
                &read.schema,
                8,
                usize::MAX,
            );
            assert!(matches!(ended, Ok(Pass::Done)), "{name}: {ended:?}");
            assert_eq!(batches.concat(), rows, "{name}");
        }
    }

    #[test]
    fn a_read_of_an_object_takes_its_bytes_only_as_far_as_it_reads() {
        let body = records(50_000);
        let object = MemObject::new("s3://bucket/big.ndjson", "v1", body.clone());
        let schema = planned(Input::Object(&object), None);

        // A query that has its rows: one request, stopped with the pass.
        object.reset();
        let (batches, ended) = passed(Input::Object(&object), None, &schema, 1024, 1);
        assert!(matches!(ended, Ok(Pass::Done)), "{ended:?}");
        assert_eq!(batches.len(), 1);
        assert_eq!(object.requests(), [None]);
        let tenth = body.len() as u64 / 10;
        assert!(object.taken() < tenth, "took {} bytes", object.taken());

        // A grid window: its layout and schema known, one request for its rows and no more.
        object.reset();
        assert_eq!(rows_read(Input::Object(&object), None, Some(10)).len(), 10);
        assert_eq!(object.requests(), [None]);
        assert!(object.taken() < tenth, "took {} bytes", object.taken());
    }

    #[test]
    fn an_objects_records_are_requested_by_their_range_once_located() {
        let body = "{\"meta\": {\"n\": 2}, \"data\": [{\"a\": 1}, {\"a\": 2}]}";
        let object = MemObject::new("s3://bucket/member.json", "v1", body);
        let rows = [serde_json::json!({"a": 1}), serde_json::json!({"a": 2})];
        assert_eq!(rows_read(Input::Object(&object), None, None), rows);

        // Locating them read the document whole; every read since asks for their bytes alone.
        let member = body.find('[').unwrap() as u64..body.rfind(']').unwrap() as u64 + 1;
        object.reset();
        assert_eq!(rows_read(Input::Object(&object), None, None), rows);
        assert_eq!(object.requests(), [Some(member)]);
    }

    #[test]
    fn what_is_learned_of_an_object_is_kept_for_its_version() {
        let object = MemObject::new("s3://bucket/v.ndjson", "v1", "{\"a\": 12}\n{\"a\": 34}\n");
        let schema = planned(Input::Object(&object), None);
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
        object.reset();
        assert_eq!(planned(Input::Object(&object), None), schema);
        assert!(object.requests().is_empty(), "known, so not read again");

        // Its next version, the same size and at the same URI, is read afresh.
        let replaced = MemObject::new(
            "s3://bucket/v.ndjson",
            "v2",
            "{\"a\":\"x\"}\n{\"a\":\"y\"}\n",
        );
        assert_eq!(replaced.size(), object.size());
        let schema = planned(Input::Object(&replaced), None);
        assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Utf8);
    }

    #[test]
    fn what_one_identity_learned_of_an_object_never_answers_another() {
        // One URI, version and size, seen by two credential identities as two objects — `az://`
        // does not say which account it names. Their records sit at different offsets and hold
        // different types, so a layout or a schema carried from one to the other fails the read.
        let body = "{\"meta\": {\"n\": 2}, \"data\": [{\"a\": 12}, {\"a\": 34}]}";
        let theirs = MemObject::new("az://data/i.json", "v1", body).looked_up_as("tenant-a");
        let ours = MemObject::new(
            "az://data/i.json",
            "v1",
            "{\"data\": [{\"a\":\"x\"}, {\"a\":\"y\"}], \"meta\": {\"n\": 2}}",
        )
        .looked_up_as("tenant-b");
        assert_eq!(ours.size(), theirs.size());

        let their_rows = [serde_json::json!({"a": 12}), serde_json::json!({"a": 34})];
        assert_eq!(rows_read(Input::Object(&theirs), None, None), their_rows);
        let our_rows = [serde_json::json!({"a": "x"}), serde_json::json!({"a": "y"})];
        assert_eq!(rows_read(Input::Object(&ours), None, None), our_rows);

        // ...and what each learned still answers its own next read.
        let member = body.find('[').unwrap() as u64..body.rfind(']').unwrap() as u64 + 1;
        theirs.reset();
        assert_eq!(rows_read(Input::Object(&theirs), None, None), their_rows);
        assert_eq!(theirs.requests(), [Some(member)]);
    }

    #[test]
    fn a_document_too_large_to_hold_is_read_as_its_one_row() {
        // Its records are not looked for inside it, since finding them holds the document whole.
        let body = "{\"data\": [{\"a\": 1}, {\"a\": 2}]}";
        let object =
            MemObject::new("s3://bucket/huge.json", "v1", body).claiming(DOCUMENT_MAX_BYTES + 1);
        assert_eq!(
            rows_read(Input::Object(&object), None, None),
            [serde_json::json!({"data": [{"a": 1}, {"a": 2}]})]
        );

        // Asked for them by path, it is refused rather than held.
        let opts = ReadOptions {
            batch_size: 2,
            csv_infer_rows: 100,
            json_path: Some("/data"),
        };
        let refused = JSON.schema(Input::Object(&object), &opts);
        assert!(
            matches!(refused, Err(EngineError::TooLarge(_))),
            "{:?}",
            refused.err()
        );
    }

    #[test]
    fn a_dropped_connection_is_an_io_error_not_a_reason_to_read_again() {
        let body = records(50_000);
        // Well past the sample, which `planned` reads first.
        let object = MemObject::new("s3://bucket/drop.ndjson", "v1", body.clone())
            .failing_after(body.len() as u64 * 3 / 4);
        let schema = planned(Input::Object(&object), None);

        object.reset();
        let (_, ended) = passed(Input::Object(&object), None, &schema, 1024, usize::MAX);
        assert!(matches!(ended, Err(EngineError::Io(_))), "{ended:?}");
        assert_eq!(
            object.requests(),
            [None],
            "no pass to look for a wider schema"
        );

        object.reset();
        let opts = ReadOptions {
            batch_size: 1024,
            csv_infer_rows: 100,
            json_path: None,
        };
        let read = JSON.read(
            &RequestContext::detached(),
            Input::Object(&object),
            &opts,
            None,
        );
        assert!(matches!(read, Err(EngineError::Io(_))), "{:?}", read.err());
        assert_eq!(object.requests(), [None], "nor a read to infer a wider one");
    }

    #[test]
    fn the_cache_keeps_every_file_until_it_is_full() {
        // What a file's widened schema and located records rely on: another file's entry does not
        // evict them, until the cache is full and starts again.
        let key = |i: usize| CacheKey {
            version: Version::File {
                path: PathBuf::from(format!("/data/{i}.json")),
                len: 1,
                modified: SystemTime::UNIX_EPOCH,
            },
            records_path: None,
        };
        let cache: Cache<usize> = Cache::new();
        for i in 0..CACHE_ENTRIES {
            cache.put(key(i), i);
        }
        assert!((0..CACHE_ENTRIES).all(|i| cache.get(&key(i)) == Some(i)));
        cache.put(key(CACHE_ENTRIES), CACHE_ENTRIES);
        assert_eq!(cache.get(&key(0)), None, "full, so cleared");
        assert_eq!(cache.get(&key(CACHE_ENTRIES)), Some(CACHE_ENTRIES));
    }
}
