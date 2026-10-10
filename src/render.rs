//! Rendering — turn engine results into what a human (or a pipe) actually reads.
//!
//! This is where "pure UX" lives for the headless MVP: a clean aligned table by default,
//! plus `--output json|ndjson|csv` for piping into other tools. The desktop UI (Phase 2)
//! will replace `to_table` with a real grid, but everything else (schema/profile shaping)
//! is reused.

use arrow_cast::display::{ArrayFormatter, FormatOptions};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::engine::{RowBatch, TableProfile, TableSchema};
use crate::error::{EngineError, Result};

const MAX_CELL: usize = 40;

/// The output format for CLI results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Output {
    Table,
    Json,
    Ndjson,
    Csv,
    /// Tab-separated. The same writer as [`Output::Csv`] with a tab delimiter — worth having
    /// because a TSV pastes into a spreadsheet without the delimiter guessing that trips CSV up
    /// on any column holding a comma, and because Lakeleto already *reads* TSV.
    Tsv,
    /// Parquet, Snappy-compressed, for DuckDB, Polars, pandas or Spark (feature `parquet-out`).
    Parquet,
    /// Arrow IPC file (`.arrow`, Feather v2): `pandas.read_feather`, `polars.read_ipc`.
    Arrow,
    /// Arrow IPC stream (`.arrows`), the one to pipe: `polars.read_ipc_stream(sys.stdin.buffer)`.
    Arrows,
}

/// Whether this build writes Parquet: the `parquet-out` feature, which the release builds have.
/// The writer is about half a megabyte of the default build, so `-o parquet` parses in every build
/// and one without the feature refuses with [`no_parquet_writer`], before reading anything.
pub const WRITES_PARQUET: bool = cfg!(feature = "parquet-out");

/// The refusal for `-o parquet` in a build without `parquet-out`, naming the way to get it.
pub fn no_parquet_writer() -> EngineError {
    EngineError::Other(
        "this build writes no Parquet: `-o parquet` needs the `parquet-out` feature \
         (`cargo install lakeleto --features parquet-out`), which the release binaries and the \
         image have. `-o arrow` writes a file pandas, Polars and pyarrow read as well"
            .to_string(),
    )
}

impl Output {
    /// Bytes rather than text, so written to a file or a pipe and never to a terminal, and only
    /// for rows: a schema or a profile is a description, which these formats have no shape for.
    pub fn is_binary(self) -> bool {
        matches!(self, Output::Parquet | Output::Arrow | Output::Arrows)
    }

    /// The format a file name asks for, by its extension, which is what `--out` falls back on when
    /// no `-o` is given. `None` for an extension that names none of them.
    ///
    /// `.arrow`, `.feather` and `.ipc` are the IPC *file* format and `.arrows` the *stream* format:
    /// the Arrow project's own convention, and the one a reader of those names expects.
    pub fn for_path(path: &std::path::Path) -> Option<Output> {
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        Some(match ext.as_str() {
            "parquet" => Output::Parquet,
            "arrow" | "feather" | "ipc" => Output::Arrow,
            "arrows" => Output::Arrows,
            "csv" => Output::Csv,
            "tsv" => Output::Tsv,
            "json" => Output::Json,
            "ndjson" | "jsonl" => Output::Ndjson,
            _ => return None,
        })
    }

    /// The name `-o` takes, for messages.
    pub fn name(self) -> &'static str {
        match self {
            Output::Table => "table",
            Output::Json => "json",
            Output::Ndjson => "ndjson",
            Output::Csv => "csv",
            Output::Tsv => "tsv",
            Output::Parquet => "parquet",
            Output::Arrow => "arrow",
            Output::Arrows => "arrows",
        }
    }
}

// ---- row batches ----------------------------------------------------------------------

/// Render a [`RowBatch`] in the requested output format, as text. The binary formats are bytes,
/// not a `String`: [`stream_rows`] writes them.
pub fn rows(rb: &RowBatch, output: Output) -> Result<String> {
    match output {
        Output::Table => rows_table(rb),
        Output::Json => rows_json(rb, false),
        Output::Ndjson => rows_json(rb, true),
        Output::Csv => rows_delimited(rb, b','),
        Output::Tsv => rows_delimited(rb, b'\t'),
        Output::Parquet | Output::Arrow | Output::Arrows => Err(EngineError::Other(format!(
            "`{}` output is binary and is written with `stream_rows`, not rendered as text",
            output.name()
        ))),
    }
}

/// Write a [`RowStream`] in `output`'s format, one batch at a time, without ever holding the whole
/// result. Returns the number of rows written.
///
/// **Every format but `Table` streams, and the exception is the format's, not the plumbing's.**
/// CSV, TSV and NDJSON are line-oriented — a row's bytes depend on nothing after it — and a JSON
/// array is `[`, comma-separated values, `]`, which an incremental writer handles. The Arrow IPC
/// formats write each batch as a message. Parquet holds the row group in progress, which is the
/// format's unit, and writes it out when it fills (the writer's default, 1 Mi rows). `Table`
/// aligns columns, and a column's width is a property of every row in the result, so the last row
/// can widen the first; rendering it means having them all. That is not a limitation worth hiding
/// behind a streaming signature, so `Table` collects and says so here.
///
/// Empty results match the buffered [`rows`] renderer byte for byte: a stream that yields no batches
/// at all emits `[]` for JSON and nothing for the rest, rather than a bare `[` or a lone header. The
/// binary formats have no buffered twin; an empty one is still a whole file, with the schema in it.
///
/// `Send` because the Parquet writer requires it of what it writes to.
pub fn stream_rows<W: std::io::Write + Send>(
    stream: crate::engine::RowStream,
    output: Output,
    out: &mut W,
) -> Result<usize> {
    match output {
        #[cfg(feature = "parquet-out")]
        Output::Parquet => return stream_parquet(stream, out),
        #[cfg(not(feature = "parquet-out"))]
        Output::Parquet => return Err(no_parquet_writer()),
        Output::Arrow => return stream_ipc(stream, IpcFormat::File, out),
        Output::Arrows => return stream_ipc(stream, IpcFormat::Stream, out),
        _ => {}
    }
    let delimiter = match output {
        Output::Csv => Some(b','),
        Output::Tsv => Some(b'\t'),
        _ => None,
    };
    // Aligned output needs every row before it can place the first one.
    if matches!(output, Output::Table) {
        let rb = stream.collect_batch()?;
        let rows = rb.num_rows();
        out.write_all(rows_table(&rb)?.as_bytes())
            .map_err(|e| EngineError::Other(e.to_string()))?;
        return Ok(rows);
    }

    // Text from here on, so each batch is relabelled as it prints (see `crate::zone`).
    let mut iter = stream.map(|b| b.and_then(|b| crate::zone::printable_batch(&b)));
    let Some(first) = iter.next() else {
        // No batches at all. The buffered renderer emits `[]` here and nothing for the others;
        // matching it exactly is what lets a caller switch paths without changing its output.
        if matches!(output, Output::Json) {
            out.write_all(b"[]")
                .map_err(|e| EngineError::Other(e.to_string()))?;
        }
        return Ok(0);
    };
    let first = first?;
    let mut rows = first.num_rows();

    if let Some(delimiter) = delimiter {
        let mut w = arrow_csv::writer::WriterBuilder::new()
            .with_header(true)
            .with_delimiter(delimiter)
            .build(out);
        w.write(&nested_as_json(&first)?)
            .map_err(EngineError::arrow)?;
        for b in iter {
            let b = b?;
            rows += b.num_rows();
            w.write(&nested_as_json(&b)?).map_err(EngineError::arrow)?;
        }
    } else if matches!(output, Output::Ndjson) {
        let mut w = arrow_json::LineDelimitedWriter::new(out);
        w.write(&first).map_err(EngineError::arrow)?;
        for b in iter {
            let b = b?;
            rows += b.num_rows();
            w.write(&b).map_err(EngineError::arrow)?;
        }
        w.finish().map_err(EngineError::arrow)?;
    } else {
        let mut w = arrow_json::ArrayWriter::new(out);
        w.write(&first).map_err(EngineError::arrow)?;
        for b in iter {
            let b = b?;
            rows += b.num_rows();
            w.write(&b).map_err(EngineError::arrow)?;
        }
        w.finish().map_err(EngineError::arrow)?;
    }
    Ok(rows)
}

fn rows_table(rb: &RowBatch) -> Result<String> {
    let opts = FormatOptions::default().with_null("·");
    let headers: Vec<String> = rb
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let ncols = headers.len();

    let mut cells: Vec<Vec<String>> = Vec::new();
    for batch in &rb.batches {
        let batch = crate::zone::printable_batch(batch)?;
        let fmts: Vec<Option<ArrayFormatter>> = (0..ncols)
            .map(|c| ArrayFormatter::try_new(batch.column(c).as_ref(), &opts).ok())
            .collect();
        for r in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(ncols);
            for fmt in &fmts {
                let s = match fmt {
                    Some(f) => f.value(r).try_to_string().unwrap_or_default(),
                    None => String::new(),
                };
                row.push(truncate(&s, MAX_CELL));
            }
            cells.push(row);
        }
    }

    let mut widths: Vec<usize> = headers.iter().map(|h| dw(h)).collect();
    for row in &cells {
        for (c, cell) in row.iter().enumerate() {
            widths[c] = widths[c].max(dw(cell));
        }
    }

    let mut out = String::new();
    render_row(&mut out, &headers, &widths);
    render_sep(&mut out, &widths);
    for row in &cells {
        render_row(&mut out, row, &widths);
    }
    out.push_str(&format!("\n{} row(s)\n", rb.num_rows()));
    Ok(out)
}

/// The properties Lakeleto writes Parquet with: Snappy, which every Parquet reader decodes and is
/// the one codec the default build links.
fn parquet_props() -> parquet::file::properties::WriterProperties {
    parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build()
}

/// Row batch → Parquet bytes (Snappy), for `export-current-view`.
pub fn to_parquet(rb: &RowBatch) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, rb.schema.clone(), Some(parquet_props()))
            .map_err(EngineError::parquet)?;
        for b in &rb.batches {
            writer.write(b).map_err(EngineError::parquet)?;
        }
        writer.close().map_err(EngineError::parquet)?;
    }
    Ok(buf)
}

/// Row batch → Arrow IPC **stream** bytes — the wire codec for row results.
///
/// This is what the HTTP API writes when a caller negotiates
/// `Accept: application/vnd.apache.arrow.stream`, and what the `remote` engine decodes back
/// into a [`RowBatch`]. It is the only path that keeps a result's Arrow types intact end to
/// end: the JSON body ([`row_values`]) is a rendering, and it flattens types on the way out.
///
/// **Decision — IPC on the wire, Parquet at rest.** Reusing [`to_parquet`] plus
/// `workspace::parquet_window_from_bytes` was considered and rejected. Those two stay the
/// *at-rest* result-cache codec: a seekable, column-compressed file with a footer, written
/// once and windowed many times. A result travelling over one HTTP response wants the
/// opposite — a framed stream a reader consumes batch by batch, with no footer to seek back
/// to. Keeping the two codecs distinct means neither has to compromise for the other; this is
/// a decision, not an oversight.
///
/// **Decision — no compression.** The stream is written with default (uncompressed) write
/// options on purpose. An LZ4/ZSTD-framed IPC body only decodes in a client that compiled the
/// matching codec, so leaving it off pins interop for every future client, in any language;
/// and the windows that reach this function are already row-capped by the endpoints that
/// produce them, so there is little left to save. The *read* side enforces the same rule from
/// its own end — see [`from_arrow_ipc`] — because a decision about what we write says nothing
/// about what a peer frames.
///
/// Mismatched batches are refused rather than encoded: see [`ensure_batches_match_schema`].
pub fn to_arrow_ipc(rb: &RowBatch) -> Result<Vec<u8>> {
    ensure_batches_match_schema(rb)?;
    let mut buf = Vec::new();
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, rb.schema.as_ref())
            .map_err(EngineError::arrow)?;
        for b in &rb.batches {
            w.write(b).map_err(EngineError::arrow)?;
        }
        w.finish().map_err(EngineError::arrow)?;
    }
    Ok(buf)
}

/// Which of Arrow IPC's two framings [`stream_ipc`] writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpcFormat {
    /// The file format: `ARROW1` magic and a footer indexing the batches, which is what `.arrow`
    /// and Feather v2 name. Written front to back, so it goes to a pipe too, but its readers seek
    /// to the footer.
    File,
    /// The stream format: messages one after another, which a reader takes from a pipe as they
    /// arrive. The same bytes `/v1/rows` serves as `application/vnd.apache.arrow.stream`.
    Stream,
}

/// Write a stream as Parquet. The writer holds the row group in progress and nothing else.
#[cfg(feature = "parquet-out")]
fn stream_parquet<W: std::io::Write + Send>(
    stream: crate::engine::RowStream,
    out: &mut W,
) -> Result<usize> {
    let schema = stream.schema().clone();
    let mut w = parquet::arrow::ArrowWriter::try_new(out, schema, Some(parquet_props()))
        .map_err(EngineError::parquet)?;
    let rows = drain_checked(stream, "a Parquet file", |b| {
        w.write(b).map_err(EngineError::parquet)
    })?;
    w.close().map_err(EngineError::parquet)?;
    Ok(rows)
}

/// Write a stream in one of the Arrow IPC framings, uncompressed for the reason [`to_arrow_ipc`]
/// gives: a codec in the bytes is one every reader then has to have compiled in.
fn stream_ipc<W: std::io::Write>(
    stream: crate::engine::RowStream,
    format: IpcFormat,
    out: &mut W,
) -> Result<usize> {
    let schema = stream.schema().clone();
    match format {
        IpcFormat::File => {
            // The file format holds one dictionary per column for the whole file, and the writer
            // refuses a second, while a result read from Parquet (a pandas or Polars categorical)
            // can change its dictionary at every row group. So dictionary columns are written as
            // their values here: a type every reader takes the same way, where keeping the
            // dictionary would fail the write at the second row group. The stream format and
            // Parquet both take a new dictionary, and keep theirs.
            let plain = without_dictionaries(&schema);
            let mut w = arrow_ipc::writer::FileWriter::try_new(out, plain.as_ref())
                .map_err(EngineError::arrow)?;
            let rows = drain_checked(stream, "an Arrow IPC file", |b| {
                if plain == schema {
                    w.write(b).map_err(EngineError::arrow)
                } else {
                    w.write(&cast_batch(b, &plain)?).map_err(EngineError::arrow)
                }
            })?;
            w.finish().map_err(EngineError::arrow)?;
            Ok(rows)
        }
        IpcFormat::Stream => {
            let mut w = arrow_ipc::writer::StreamWriter::try_new(out, schema.as_ref())
                .map_err(EngineError::arrow)?;
            let rows = drain_checked(stream, "an Arrow IPC stream", |b| {
                w.write(b).map_err(EngineError::arrow)
            })?;
            w.finish().map_err(EngineError::arrow)?;
            Ok(rows)
        }
    }
}

/// `schema` with every dictionary type replaced by its value type, at any depth; the same schema
/// when it has none.
fn without_dictionaries(schema: &arrow_schema::SchemaRef) -> arrow_schema::SchemaRef {
    use arrow_schema::{DataType, FieldRef};
    fn plain_type(dt: &DataType) -> DataType {
        match dt {
            DataType::Dictionary(_, values) => plain_type(values),
            DataType::List(f) => DataType::List(plain_field(f)),
            DataType::LargeList(f) => DataType::LargeList(plain_field(f)),
            DataType::FixedSizeList(f, n) => DataType::FixedSizeList(plain_field(f), *n),
            DataType::Struct(fields) => DataType::Struct(fields.iter().map(plain_field).collect()),
            DataType::Map(f, sorted) => DataType::Map(plain_field(f), *sorted),
            other => other.clone(),
        }
    }
    fn plain_field(f: &FieldRef) -> FieldRef {
        std::sync::Arc::new(f.as_ref().clone().with_data_type(plain_type(f.data_type())))
    }
    let fields: Vec<FieldRef> = schema.fields().iter().map(plain_field).collect();
    if fields.iter().zip(schema.fields()).all(|(a, b)| a == b) {
        return schema.clone();
    }
    std::sync::Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// `batch` with each column cast to `schema`'s type for it.
fn cast_batch(
    batch: &arrow_array::RecordBatch,
    schema: &arrow_schema::SchemaRef,
) -> Result<arrow_array::RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                arrow_cast::cast(column, field.data_type()).map_err(EngineError::arrow)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    arrow_array::RecordBatch::try_new_with_options(schema.clone(), columns, &options)
        .map_err(EngineError::arrow)
}

/// Hand each batch of `stream` to `write` once it has passed [`ensure_batch_matches`] against the
/// stream's declared schema, and count the rows. `what` names the output for the refusal.
fn drain_checked(
    stream: crate::engine::RowStream,
    what: &str,
    mut write: impl FnMut(&arrow_array::RecordBatch) -> Result<()>,
) -> Result<usize> {
    let schema = stream.schema().clone();
    let mut rows = 0;
    for (i, batch) in stream.enumerate() {
        let batch = batch?;
        ensure_batch_matches(&schema, i, &batch, what)?;
        rows += batch.num_rows();
        write(&batch)?;
    }
    Ok(rows)
}

/// One field, rendered for an error message: `name: DataType (nullable|not null)`.
fn describe_field(f: &arrow_schema::Field) -> String {
    format!(
        "{}: {} ({})",
        f.name(),
        f.data_type(),
        if f.is_nullable() {
            "nullable"
        } else {
            "not null"
        }
    )
}

/// Every batch in `rb` must actually carry the columns `rb.schema` declares, in order.
///
/// **Why this is checked at all.** `StreamWriter::write` performs *no* schema check of its own —
/// unlike the `parquet::ArrowWriter` behind [`to_parquet`], whose own docs say it "will fail if
/// the `batch`'s schema does not match the writer's schema". (arrow-ipc's `FileWriter::write` is
/// **not** a counter-example, though an earlier revision of this comment claimed it was: read at
/// arrow-ipc 58.4.0 it checks only whether the writer is already finished, exactly like
/// `StreamWriter`.) It writes each batch's buffers **positionally** against the schema message
/// it already emitted.
/// So a [`RowBatch`] whose `schema` and `batches` disagree encodes "successfully", the server
/// answers `200`, and the client decodes confidently mislabelled data: an extra column silently
/// vanishes, a renamed column silently relabels someone else's values, a retyped column is
/// reinterpreted. A caller cannot un-see wrong rows, so the only safe answer is to refuse to
/// write them — naming the batch and the exact difference, so the bug is findable.
///
/// **Decision — compare `(name, data_type)` only.** The check is per field, and deliberately
/// ignores both *metadata* (the schema-level map and each field's) and *nullability*. The rule
/// is: compare exactly what decides how a byte is interpreted, and nothing else.
///
/// - *Metadata* is annotation, not shape. It changes nothing about the encoding, and producers
///   legitimately disagree about it. A full `Schema` equality check — or even `Fields` equality,
///   which compares field metadata — would reject correct results in order to guard nothing.
/// - *Nullability* is not a positional hazard either. A reader that is handed the declared
///   schema decodes the same bytes whether the field says nullable or not; no value is
///   mislabelled and no column shifts. It is also the attribute producers disagree about most:
///   the SQL engine takes `RowBatch::schema` from `df.schema()` while the batches come out of
///   the execution plan (`engine/sql.rs`), and DataFusion's planner and its plans legitimately
///   differ here — a `UNION ALL` mixing a `NOT NULL` source with a nullable one is the standard
///   case. An earlier revision of this function compared nullability and turned exactly that
///   query into a `400` on the Arrow arm while the JSON arm still answered `200`. Guarding it
///   cost correct results and bought no safety.
///
/// The one genuinely unsafe case nullability *could* flag — a field declared `NOT NULL` whose
/// array actually contains nulls — is a property of the data, not of the schema, so this check
/// could not see it without scanning every value on every response. It is deliberately out of
/// scope.
///
/// The CLI's binary outputs ([`stream_rows`] with `-o parquet|arrow|arrows`) run the same check on
/// every batch as it streams past. Parquet's writer would refuse most mismatches itself, but not
/// in these words, and one rule for all four writers is easier to trust than four.
fn ensure_batches_match_schema(rb: &RowBatch) -> Result<()> {
    for (i, batch) in rb.batches.iter().enumerate() {
        ensure_batch_matches(&rb.schema, i, batch, "an Arrow IPC stream")?;
    }
    Ok(())
}

/// [`ensure_batches_match_schema`]'s check for one batch, `i`, about to be written as `what`.
fn ensure_batch_matches(
    schema: &arrow_schema::Schema,
    i: usize,
    batch: &arrow_array::RecordBatch,
    what: &str,
) -> Result<()> {
    let want = schema.fields();
    let got = batch.schema_ref().fields();
    if want.len() != got.len() {
        return Err(EngineError::Arrow(format!(
            "batch {i} has {} column(s) but the result declares {}: [{}] vs [{}] — \
             refusing to write {what}, which would encode the batch positionally against \
             the declared schema and hand the reader wrong data",
            got.len(),
            want.len(),
            got.iter()
                .map(|f| describe_field(f))
                .collect::<Vec<_>>()
                .join(", "),
            want.iter()
                .map(|f| describe_field(f))
                .collect::<Vec<_>>()
                .join(", "),
        )));
    }
    for (c, (w, g)) in want.iter().zip(got.iter()).enumerate() {
        // Name and type only — see the decision note above. Nullability and metadata are
        // deliberately not compared: neither changes how a byte is interpreted, and both
        // drift legitimately between a declared schema and the plan that produced it.
        if w.name() != g.name() || w.data_type() != g.data_type() {
            return Err(EngineError::Arrow(format!(
                "batch {i}, column {c}: the batch has `{}` but the result declares `{}` — \
                 refusing to write {what}, which would encode the batch positionally against \
                 the declared schema and hand the reader wrong data",
                describe_field(g),
                describe_field(w),
            )));
        }
    }
    Ok(())
}

/// Refuse an IPC stream whose record- or dictionary-batch messages declare a compression codec.
///
/// **Why the read side needs its own answer.** [`to_arrow_ipc`] writes uncompressed, but that is
/// a decision about what *we* send; `StreamReader` decodes whatever a peer framed, and
/// `arrow-ipc`'s `lz4`/`zstd` features are enabled in any build that also links DataFusion
/// (`cargo tree -e features -i arrow-ipc` shows them arriving through `datafusion`, while
/// `--features remote` alone leaves them off). A compressed buffer carries its *uncompressed*
/// length as an attacker-controlled 64-bit prefix, and that prefix drives a pre-allocation — so
/// a small hostile body can ask a client for a very large one.
///
/// **What arrow-ipc 58 offers, and what this therefore does.** There is no reader-side switch:
/// `StreamReader::try_new` takes a projection and nothing else, and the codec is resolved
/// internally from each message's `BodyCompression`. But the crate does re-export the generated
/// FlatBuffers accessors (`root_as_message`, `Message::header_as_record_batch`,
/// `RecordBatch::compression`), so the codec *is* readable from the message metadata before any
/// buffer is touched. This walks the stream's framing — continuation marker, metadata length,
/// metadata, body — and rejects the first message that names one. Nothing is decompressed and
/// nothing is allocated in proportion to a declared length; the walk only ever indexes into the
/// caller's slice.
fn ensure_uncompressed(bytes: &[u8]) -> Result<()> {
    let malformed = |what: &str| {
        EngineError::Arrow(format!(
            "not a well-formed Arrow IPC stream: {what} (the stream is scanned for a \
             compression codec before it is decoded)"
        ))
    };
    let mut pos = 0usize;
    loop {
        // An IPC stream may end either with the `0xFFFFFFFF 0x00000000` end-of-stream marker or
        // simply at EOF; both are legal, so running out of bytes here is not an error.
        if pos == bytes.len() {
            return Ok(());
        }
        let mut raw: [u8; 4] = bytes
            .get(pos..pos + 4)
            .ok_or_else(|| malformed("truncated message length"))?
            .try_into()
            .expect("4-byte slice");
        pos += 4;
        if raw == [0xff; 4] {
            // The v4+ continuation marker: the real length is the next four bytes.
            raw = bytes
                .get(pos..pos + 4)
                .ok_or_else(|| malformed("truncated message length after a continuation marker"))?
                .try_into()
                .expect("4-byte slice");
            pos += 4;
        }
        let meta_len = i32::from_le_bytes(raw);
        if meta_len == 0 {
            return Ok(()); // end-of-stream marker
        }
        let meta_len =
            usize::try_from(meta_len).map_err(|_| malformed("negative metadata length"))?;
        let end = pos
            .checked_add(meta_len)
            .ok_or_else(|| malformed("metadata length overflows the buffer"))?;
        let meta = bytes
            .get(pos..end)
            .ok_or_else(|| malformed("truncated message metadata"))?;
        pos = end;
        let msg = arrow_ipc::root_as_message(meta)
            .map_err(|e| malformed(&format!("unreadable message metadata ({e})")))?;
        let compression = match msg.header_type() {
            arrow_ipc::MessageHeader::RecordBatch => {
                msg.header_as_record_batch().and_then(|b| b.compression())
            }
            arrow_ipc::MessageHeader::DictionaryBatch => msg
                .header_as_dictionary_batch()
                .and_then(|d| d.data())
                .and_then(|b| b.compression()),
            _ => None,
        };
        if let Some(c) = compression {
            let codec = c.codec().variant_name().unwrap_or("unknown");
            return Err(EngineError::Arrow(format!(
                "refusing a compressed Arrow IPC stream (codec {codec}): Lakeleto writes and \
                 reads uncompressed IPC only. A compressed body declares its decompressed size, \
                 and that declaration drives the allocation — a peer must not be able to size \
                 this client's memory"
            )));
        }
        let body_len =
            usize::try_from(msg.bodyLength()).map_err(|_| malformed("negative body length"))?;
        pos = pos
            .checked_add(body_len)
            .ok_or_else(|| malformed("body length overflows the buffer"))?;
        if pos > bytes.len() {
            return Err(malformed("truncated message body"));
        }
    }
}

/// Arrow IPC stream bytes → [`RowBatch`].
///
/// The reverse direction of [`to_arrow_ipc`] for everything a reader can check: the stream's
/// schema message and every record batch it framed are decoded back into the `RowBatch` they
/// were written from. What it cannot check is that the *whole* stream arrived. A body cut
/// exactly on an IPC message boundary is indistinguishable from a shorter result — it decodes
/// as `Ok` with fewer rows, and `StreamReader::is_finished()` is no help (it reads `true` for a
/// cut stream and a complete one alike, since both simply run out of messages). Truncation
/// *inside* a message is caught, by the framing walk in [`ensure_uncompressed`] or by the
/// decoder itself. Callers that need "all of it, or an error" must bound the transfer at the
/// transport layer, which is what [`crate::engine::remote::RemoteEngine`] does.
///
/// Compressed streams are refused; see [`ensure_uncompressed`] for why the read side decides
/// this for itself rather than trusting the writer's decision.
pub fn from_arrow_ipc(bytes: &[u8]) -> Result<RowBatch> {
    ensure_uncompressed(bytes)?;
    // **Why the decode runs inside `catch_unwind`.** These bytes came off a socket from a server
    // this process does not run, and `arrow-ipc`'s decoder *panics* rather than erroring on a
    // range of malformed input: the buffer offsets and lengths inside a record-batch message are
    // read from the message's own FlatBuffers metadata and then used to slice the body, and a
    // value that does not address real bytes reaches an `unwrap` rather than a `Result`.
    // [`ensure_uncompressed`] walks the stream's *framing*, so it rejects a truncated or
    // mis-declared message, but it deliberately does not re-implement arrow's buffer accounting
    // — validating a foreign format's internals by hand is how you get a second parser with its
    // own bugs.
    //
    // Measured, not assumed: mutating single bytes of a valid 776-byte stream panicked on 326 of
    // 3,198 mutations (~10%), the first at byte 28 with `Option::unwrap()` on a `None` value.
    // Without this guard any peer — hostile, buggy, or simply on a bad link — can take the
    // client process down, which for a server embedding this engine is a denial of service.
    //
    // `AssertUnwindSafe` is sound here: the closure borrows `bytes` immutably, touches no shared
    // mutable state, and builds a fresh `RowBatch` it either returns whole or drops. A panic
    // leaves nothing half-updated for a later observer.
    //
    // The panic is still reported by whatever hook is installed, so a corrupt peer stays visible
    // in the logs rather than being silently swallowed. This crate deliberately does not install
    // a hook of its own — that is a process-wide, racy decision for an application to make.
    //
    // The workspace sets `panic = "unwind"` explicitly on the release profile, so this holds in
    // a release build. Under `panic = "abort"` it would not, and nothing here could help.
    let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(EngineError::arrow)?;
        // The schema comes from the stream's schema message, NEVER from the first batch. An IPC
        // stream always carries its schema up front, and a zero-row window carries *no* batches
        // at all — reading the schema off `batches.first()` would decode "0 rows, 5 columns" as
        // "0 rows, no columns", losing exactly the fidelity this codec exists to preserve.
        let schema = reader.schema();
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(EngineError::arrow)?);
        }
        Ok(RowBatch { schema, batches })
    }));

    match decoded {
        Ok(result) => result,
        Err(_) => Err(EngineError::Arrow(
            "malformed Arrow IPC stream: the decoder panicked part-way through, which means the \
             message metadata does not describe the bytes that follow it. Treat the response as \
             corrupt — a truncated transfer, or a server that is not speaking this codec."
                .to_string(),
        )),
    }
}

/// Row batch → a `Vec` of JSON row objects (`{column: value}`), for the HTTP API.
pub fn row_values(rb: &RowBatch) -> Result<Vec<serde_json::Value>> {
    let mut buf = Vec::new();
    {
        let mut w = arrow_json::ArrayWriter::new(&mut buf);
        for b in &rb.batches {
            w.write(&crate::zone::printable_batch(b)?)
                .map_err(EngineError::arrow)?;
        }
        w.finish().map_err(EngineError::arrow)?;
    }
    if buf.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_slice(&buf).map_err(|e| EngineError::Other(e.to_string()))
}

fn rows_json(rb: &RowBatch, line_delimited: bool) -> Result<String> {
    let mut buf = Vec::new();
    if line_delimited {
        let mut w = arrow_json::LineDelimitedWriter::new(&mut buf);
        for b in &rb.batches {
            w.write(&crate::zone::printable_batch(b)?)
                .map_err(EngineError::arrow)?;
        }
        w.finish().map_err(EngineError::arrow)?;
    } else {
        let mut w = arrow_json::ArrayWriter::new(&mut buf);
        for b in &rb.batches {
            w.write(&crate::zone::printable_batch(b)?)
                .map_err(EngineError::arrow)?;
        }
        w.finish().map_err(EngineError::arrow)?;
    }
    if buf.is_empty() {
        return Ok(if line_delimited {
            String::new()
        } else {
            "[]".to_string()
        });
    }
    String::from_utf8(buf).map_err(|e| EngineError::Other(e.to_string()))
}

fn rows_delimited(rb: &RowBatch, delimiter: u8) -> Result<String> {
    let mut buf = Vec::new();
    {
        let mut w = arrow_csv::writer::WriterBuilder::new()
            .with_header(true)
            .with_delimiter(delimiter)
            .build(&mut buf);
        for b in &rb.batches {
            w.write(&nested_as_json(&crate::zone::printable_batch(b)?)?)
                .map_err(EngineError::arrow)?;
        }
    }
    String::from_utf8(buf).map_err(|e| EngineError::Other(e.to_string()))
}

/// `batch` with each nested column — a list, a struct, a map — replaced by its values as JSON
/// text, so the CSV writer can take it.
///
/// A CSV cell holds one value, and Arrow's CSV writer refuses a whole batch over one nested column
/// ("Nested type List(…) is not supported in CSV"). Each nested cell is written as the compact JSON
/// the grid shows for it ([`crate::engine::json_text`]) — the form the web app's in-browser CSV
/// already used — and a null as an empty cell. Columns are picked by Arrow's own
/// [`DataType::is_nested`](arrow_schema::DataType::is_nested), the test the writer refuses by, so
/// nothing left in the batch can be refused. Scalar columns pass through and keep the writer's
/// formatting.
fn nested_as_json(batch: &arrow_array::RecordBatch) -> Result<arrow_array::RecordBatch> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if column.data_type().is_nested() {
            fields.push(std::sync::Arc::new(arrow_schema::Field::new(
                field.name(),
                arrow_schema::DataType::Utf8,
                true,
            )));
            columns.push(
                std::sync::Arc::new(crate::engine::json_text(column)?) as arrow_array::ArrayRef
            );
        } else {
            fields.push(field.clone());
            columns.push(column.clone());
        }
    }
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    arrow_array::RecordBatch::try_new_with_options(
        std::sync::Arc::new(arrow_schema::Schema::new(fields)),
        columns,
        &options,
    )
    .map_err(EngineError::arrow)
}

// ---- schema ---------------------------------------------------------------------------

/// Render a [`TableSchema`] (table view) or hand back JSON.
pub fn schema(s: &TableSchema, output: Output) -> Result<String> {
    if matches!(output, Output::Json | Output::Ndjson) {
        return serde_json::to_string_pretty(s).map_err(|e| EngineError::Other(e.to_string()));
    }
    let rows_count = s
        .row_count
        .map(|n| n.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let mut out = format!("source : {}\nformat : {}\n", s.source, s.format);
    // Only when the rows came from inside the document (`{"data": [...]}`) — say where.
    if let Some(path) = &s.records_path {
        out.push_str(&format!("records: {path}\n"));
    }
    out.push_str(&format!(
        "engine : {}\nrows   : {}\ncolumns: {}\n\n",
        s.engine,
        rows_count,
        s.columns.len()
    ));
    let name_w = s
        .columns
        .iter()
        .map(|c| c.name.chars().count())
        .max()
        .unwrap_or(4)
        .max(6);
    let type_w = s
        .columns
        .iter()
        .map(|c| c.data_type.chars().count())
        .max()
        .unwrap_or(4)
        .max(4);
    out.push_str(&format!(
        "{:<name_w$}  {:<type_w$}  {}\n",
        "column", "type", "null?"
    ));
    out.push_str(&format!(
        "{}  {}  {}\n",
        "-".repeat(name_w),
        "-".repeat(type_w),
        "-----"
    ));
    for c in &s.columns {
        out.push_str(&format!(
            "{:<name_w$}  {:<type_w$}  {}\n",
            c.name,
            c.data_type,
            if c.nullable { "yes" } else { "no" }
        ));
    }
    Ok(out)
}

// ---- profile --------------------------------------------------------------------------

/// Render a [`TableProfile`] (table view) or hand back JSON.
pub fn profile(p: &TableProfile, output: Output) -> Result<String> {
    if matches!(output, Output::Json | Output::Ndjson) {
        return serde_json::to_string_pretty(p).map_err(|e| EngineError::Other(e.to_string()));
    }
    let rows_count = p
        .row_count
        .map(|n| n.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    // `scanned == 0` marks a footer-statistics profile: min/max/nulls are exact whole-file, but
    // distinct + samples weren't computed (no scan).
    let footer = p.scanned_rows == 0;
    let scanned = if footer {
        "footer stats — no scan".to_string()
    } else {
        format!("scanned {}", p.scanned_rows)
    };
    let mut out = format!(
        "source : {}\nengine : {}\nrows   : {} ({})\n\n",
        p.source, p.engine, rows_count, scanned
    );

    let headers = ["column", "type", "nulls", "null%", "distinct", "min", "max"];
    let mut table: Vec<Vec<String>> = vec![headers.iter().map(|h| h.to_string()).collect()];
    for c in &p.columns {
        let distinct = if footer {
            "—".to_string() // not computed from footer stats
        } else if c.distinct_capped {
            format!("{}+", c.distinct)
        } else {
            c.distinct.to_string()
        };
        table.push(vec![
            truncate(&c.name, MAX_CELL),
            truncate(&c.data_type, MAX_CELL),
            c.null_count.to_string(),
            format!("{:.1}%", c.null_fraction * 100.0),
            distinct,
            truncate(c.min.as_deref().unwrap_or("·"), 24),
            truncate(c.max.as_deref().unwrap_or("·"), 24),
        ]);
    }
    let ncols = headers.len();
    let mut widths = vec![0usize; ncols];
    for row in &table {
        for (c, cell) in row.iter().enumerate() {
            widths[c] = widths[c].max(dw(cell));
        }
    }
    render_row(&mut out, &table[0], &widths);
    render_sep(&mut out, &widths);
    for row in &table[1..] {
        render_row(&mut out, row, &widths);
    }
    Ok(out)
}

// ---- info -----------------------------------------------------------------------------

/// What `lakeleto info` and `GET /v1/info` say about a source. One type for both, so the CLI's
/// `-o json` and the API answer in the same shape.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SourceInfo {
    /// The source as it was named.
    pub path: String,
    /// The detected format. `None` when the source is resolved elsewhere: with `--remote-url`,
    /// the server detects the format and reads the bytes.
    pub format: Option<String>,
    /// The engine that read the schema.
    pub engine: String,
    /// The file's size, from the filesystem; `GET /v1/info` also asks an object store for an
    /// object's. `None` when neither knows it, as for a catalog table.
    pub size_bytes: Option<u64>,
    /// The row count, where the format records one, as a Parquet footer or an Iceberg snapshot
    /// does.
    pub row_count: Option<u64>,
    /// The number of columns.
    pub columns: usize,
    /// Whose credentials read a catalog table's files: `vended`, `catalog` or `ambient`. Omitted
    /// for every other source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<String>,
}

/// Render a [`SourceInfo`] as `output` asks:
/// - **`table`** (the default): the `name : value` lines `lakeleto info` has always printed;
/// - **`json`**: the object `GET /v1/info` returns, and **`ndjson`** the same on one line;
/// - **`csv`** and **`tsv`**: a header and one row, with the same columns whatever the source,
///   so the rows for many files line up under one header.
///
/// The binary formats are rows, and a description has no shape in them; the CLI refuses them
/// before it reads anything.
pub fn info(i: &SourceInfo, output: Output) -> Result<String> {
    let json = |text: serde_json::Result<String>| {
        text.map(|t| t + "\n")
            .map_err(|e| EngineError::Other(e.to_string()))
    };
    match output {
        Output::Table => Ok(info_lines(i)),
        Output::Json => json(serde_json::to_string_pretty(i)),
        Output::Ndjson => json(serde_json::to_string(i)),
        Output::Csv | Output::Tsv => rows(&info_row(i)?, output),
        Output::Parquet | Output::Arrow | Output::Arrows => Err(EngineError::Other(format!(
            "`-o {}` writes rows, and `info` prints a description: use `-o json`",
            output.name()
        ))),
    }
}

/// The `name : value` lines.
fn info_lines(i: &SourceInfo) -> String {
    let mut out = format!("path   : {}\n", i.path);
    // An unresolved source has no format *here*: the server resolved the reference and read the
    // bytes. Printing the placeholder's name ("unknown") would read like a failed detection
    // rather than a deliberate absence.
    let format = i.format.as_deref().unwrap_or("(resolved by the server)");
    out.push_str(&format!("format : {format}\n"));
    out.push_str(&format!("engine : {}\n", i.engine));
    let size = i.size_bytes.map(human_bytes);
    out.push_str(&format!("size   : {}\n", size.as_deref().unwrap_or("?")));
    let rows = i.row_count.map(|n| n.to_string());
    out.push_str(&format!(
        "rows   : {}\n",
        rows.as_deref().unwrap_or("unknown")
    ));
    out.push_str(&format!("columns: {}\n", i.columns));
    if let Some(credentials) = &i.credentials {
        out.push_str(&format!("creds  : {}\n", describe_credentials(credentials)));
    }
    out
}

/// What the `creds` line says for the credentials a catalog table was read with.
fn describe_credentials(source: &str) -> &'static str {
    match source {
        "vended" => "vended by the catalog for this table",
        "catalog" => "the storage keys configured for the catalog",
        _ => "this machine's own (the catalog vended none)",
    }
}

/// A size in bytes, in the largest power-of-1024 unit it reaches.
fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

/// A [`SourceInfo`] as one row, for the CSV and TSV writers.
fn info_row(i: &SourceInfo) -> Result<RowBatch> {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};

    let text = |v: Option<&str>| Arc::new(StringArray::from(vec![v])) as ArrayRef;
    let count = |v: Option<u64>| Arc::new(UInt64Array::from(vec![v])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("format", DataType::Utf8, true),
        Field::new("engine", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, true),
        Field::new("row_count", DataType::UInt64, true),
        Field::new("columns", DataType::UInt64, false),
        Field::new("credentials", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            text(Some(&i.path)),
            text(i.format.as_deref()),
            text(Some(&i.engine)),
            count(i.size_bytes),
            count(i.row_count),
            count(Some(i.columns as u64)),
            text(i.credentials.as_deref()),
        ],
    )
    .map_err(EngineError::arrow)?;
    Ok(RowBatch {
        schema,
        batches: vec![batch],
    })
}

#[cfg(test)]
mod info_tests {
    use super::*;

    /// A local Parquet file, with a comma in its name for the CSV writer to quote.
    fn parquet() -> SourceInfo {
        SourceInfo {
            path: "data/events, 2026.parquet".to_string(),
            format: Some("parquet".to_string()),
            engine: "local".to_string(),
            size_bytes: Some(1024 * 1024 * 3 / 2),
            row_count: Some(1000),
            columns: 4,
            credentials: None,
        }
    }

    /// A catalog table read through a server: no format here, no size and no row count.
    fn served() -> SourceInfo {
        SourceInfo {
            path: "catalog://prod/sales/orders".to_string(),
            format: None,
            engine: "remote".to_string(),
            size_bytes: None,
            row_count: None,
            columns: 2,
            credentials: Some("catalog".to_string()),
        }
    }

    /// The default output is the lines `lakeleto info` has always printed.
    #[test]
    fn the_default_is_the_lines_info_always_printed() {
        assert_eq!(
            info(&parquet(), Output::Table).unwrap(),
            "path   : data/events, 2026.parquet\nformat : parquet\nengine : local\n\
             size   : 1.5 MiB\nrows   : 1000\ncolumns: 4\n"
        );
        assert_eq!(
            info(&served(), Output::Table).unwrap(),
            "path   : catalog://prod/sales/orders\nformat : (resolved by the server)\n\
             engine : remote\nsize   : ?\nrows   : unknown\ncolumns: 2\n\
             creds  : the storage keys configured for the catalog\n"
        );
    }

    /// `info` tells the three sources of a catalog table's credentials apart, so keys configured
    /// for the catalog are never reported as this machine's own.
    #[test]
    fn info_says_whose_credentials_read_a_catalog_table() {
        let creds = |source: &str| {
            let mut i = served();
            i.credentials = Some(source.to_string());
            info(&i, Output::Table).unwrap()
        };
        let line = |text: &str| format!("\ncreds  : {text}\n");
        assert!(creds("vended").ends_with(&line("vended by the catalog for this table")));
        assert!(creds("catalog").ends_with(&line("the storage keys configured for the catalog")));
        assert!(creds("ambient").ends_with(&line("this machine's own (the catalog vended none)")));
    }

    /// `-o json` is the object `GET /v1/info` returns and `-o ndjson` the same on one line. A
    /// source that is not a catalog table has no `credentials` key, and what is not known is null.
    #[test]
    fn json_is_the_object_v1_info_returns() {
        let pretty = info(&parquet(), Output::Json).unwrap();
        assert!(pretty.ends_with("}\n"), "{pretty}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&pretty).unwrap(),
            serde_json::json!({
                "path": "data/events, 2026.parquet",
                "format": "parquet",
                "engine": "local",
                "size_bytes": 1_572_864,
                "row_count": 1000,
                "columns": 4,
            })
        );
        let line = info(&served(), Output::Ndjson).unwrap();
        assert_eq!(line.matches('\n').count(), 1, "{line}");
        assert!(line.ends_with('\n'), "{line}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).unwrap(),
            serde_json::json!({
                "path": "catalog://prod/sales/orders",
                "format": null,
                "engine": "remote",
                "size_bytes": null,
                "row_count": null,
                "columns": 2,
                "credentials": "catalog",
            })
        );
    }

    /// `-o csv` and `-o tsv` are a header and one row, with the same columns for every source:
    /// what is not known is an empty cell.
    #[test]
    fn csv_and_tsv_are_a_header_and_one_row() {
        assert_eq!(
            info(&parquet(), Output::Csv).unwrap(),
            "path,format,engine,size_bytes,row_count,columns,credentials\n\
             \"data/events, 2026.parquet\",parquet,local,1572864,1000,4,\n"
        );
        assert_eq!(
            info(&served(), Output::Tsv).unwrap(),
            "path\tformat\tengine\tsize_bytes\trow_count\tcolumns\tcredentials\n\
             catalog://prod/sales/orders\t\tremote\t\t\t2\tcatalog\n"
        );
    }

    /// A size is in the largest power-of-1024 unit it reaches, up to TiB.
    #[test]
    fn a_size_is_in_the_largest_unit_it_reaches() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1024 * 1024 * 3 / 2), "1.5 MiB");
        assert_eq!(human_bytes(1 << 40), "1.0 TiB");
        assert_eq!(human_bytes(u64::MAX), "16777216.0 TiB");
    }

    /// The binary formats are rows, and a description has no shape in them.
    #[test]
    fn the_binary_formats_are_refused() {
        for output in [Output::Parquet, Output::Arrow, Output::Arrows] {
            let err = info(&parquet(), output).unwrap_err().to_string();
            assert!(err.contains("use `-o json`"), "{output:?}: {err}");
        }
    }
}

// ---- small table primitives -----------------------------------------------------------

fn render_row(out: &mut String, cells: &[String], widths: &[usize]) {
    out.push_str("| ");
    for (c, cell) in cells.iter().enumerate() {
        let pad = widths[c].saturating_sub(dw(cell));
        out.push_str(cell);
        out.push_str(&" ".repeat(pad));
        out.push_str(" | ");
    }
    out.push('\n');
}

fn render_sep(out: &mut String, widths: &[usize]) {
    out.push('|');
    for w in widths {
        out.push_str(&"-".repeat(w + 2));
        out.push('|');
    }
    out.push('\n');
}

/// Terminal display width (CJK/emoji count as 2, combining marks as 0) — not scalar count.
fn dw(s: &str) -> usize {
    UnicodeWidthStr::width(s)
}

/// Truncate to a display width of `max` (last column reserved for the `…` ellipsis).
fn truncate(s: &str, max: usize) -> String {
    if dw(s) <= max {
        return s.to_string();
    }
    let budget = max.saturating_sub(1);
    let mut out = String::new();
    let mut width = 0;
    for ch in s.chars() {
        let cw = UnicodeWidthChar::width(ch).unwrap_or(0);
        if width + cw > budget {
            break;
        }
        out.push(ch);
        width += cw;
    }
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    fn batch(ids: Vec<i64>) -> RecordBatch {
        let n = ids.len();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(
                    (0..n).map(|i| format!("row{i}")).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    (0..n).map(|i| i % 2 == 0).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// Lakeleto's Parquet is Snappy-compressed, as its docs say: the codec every reader decodes,
    /// and the one the default build links. The writer's own default is no compression.
    #[test]
    fn parquet_is_written_with_snappy() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 2, 3])],
        };
        let bytes = bytes::Bytes::from(to_parquet(&rb).unwrap());
        let reader = SerializedFileReader::new(bytes).unwrap();
        let columns = reader.metadata().row_group(0).columns();
        assert_eq!(columns.len(), 3);
        for column in columns {
            assert_eq!(
                column.compression(),
                parquet::basic::Compression::SNAPPY,
                "{}",
                column.column_path()
            );
        }
    }

    #[test]
    fn arrow_ipc_round_trips_types_and_values() {
        // 9_007_199_254_740_993 = 2^53 + 1: the first integer an IEEE-754 double cannot
        // represent, i.e. the first one a JSON-number client silently rounds. It must come
        // back bit-for-bit here, as an Int64 — that is the whole point of the wire codec.
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 9_007_199_254_740_993])],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.schema, rb.schema);
        assert_eq!(back.num_rows(), 2);
        let ids = back.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id stays an Int64 array, not a string");
        assert_eq!(ids.value(1), 9_007_199_254_740_993);
    }

    #[test]
    fn arrow_ipc_round_trips_multiple_batches() {
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 2]), batch(vec![3])],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.num_rows(), 3);
        assert_eq!(back.batches.len(), 2);
    }

    #[test]
    fn arrow_ipc_round_trips_an_empty_window_with_its_columns() {
        // A zero-row window still has a shape. The reader must take the schema from the
        // stream's schema message, not from a first batch that does not exist.
        let rb = RowBatch {
            schema: schema(),
            batches: vec![],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert!(back.is_empty());
        assert_eq!(back.schema.fields().len(), 3);
        assert_eq!(back.schema.field(0).name(), "id");
        assert_eq!(back.schema, rb.schema);
    }

    #[test]
    fn arrow_ipc_rejects_bytes_that_are_not_a_stream() {
        assert!(from_arrow_ipc(b"not an arrow stream").is_err());
    }

    // ---- the writer refuses batches that do not match the declared schema ----------------
    //
    // `StreamWriter::write` does no schema check of its own: it encodes each batch's buffers
    // positionally against the schema message it already emitted. Every case below used to
    // encode "successfully" and decode into wrong data, so each is pinned separately.

    /// `to_arrow_ipc` must fail, and the message must name the batch and the difference.
    fn refuses(rb: &RowBatch, needles: &[&str]) {
        let err = match to_arrow_ipc(rb) {
            Err(e) => e.to_string(),
            Ok(bytes) => panic!("encoded {} bytes instead of refusing", bytes.len()),
        };
        for needle in needles {
            assert!(
                err.contains(needle),
                "error should mention `{needle}`: {err}"
            );
        }
    }

    #[test]
    fn arrow_ipc_refuses_a_batch_with_an_extra_column() {
        // Declared: [id]. Actual: [id, name, active]. Encoded positionally, the extra columns
        // are silently dropped and the caller never learns they existed.
        let declared = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let rb = RowBatch {
            schema: declared,
            batches: vec![batch(vec![1, 2])],
        };
        refuses(&rb, &["batch 0", "3 column(s)", "declares 1"]);
    }

    #[test]
    fn arrow_ipc_refuses_a_batch_missing_a_column() {
        let narrow = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let one_col = RecordBatch::try_new(
            narrow.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let rb = RowBatch {
            // Declared: the full three-column schema. Actual: one column.
            schema: schema(),
            batches: vec![one_col],
        };
        refuses(&rb, &["batch 0", "1 column(s)", "declares 3"]);
    }

    #[test]
    fn arrow_ipc_refuses_a_renamed_column() {
        // Same arity, same types — only the name moved. This is the nastiest case: it encodes
        // and decodes without complaint, and every value ends up under the wrong header.
        let renamed = Arc::new(Schema::new(vec![
            Field::new("identifier", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let rb = RowBatch {
            schema: renamed,
            batches: vec![batch(vec![1])],
        };
        refuses(&rb, &["batch 0, column 0", "id:", "identifier:"]);
    }

    #[test]
    fn arrow_ipc_refuses_a_retyped_column() {
        let retyped = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let rb = RowBatch {
            schema: retyped,
            batches: vec![batch(vec![1])],
        };
        refuses(&rb, &["batch 0, column 0", "Int64", "Utf8"]);
    }

    #[test]
    fn arrow_ipc_refuses_a_mismatch_in_a_later_batch() {
        // The loop must not stop at the first batch: a divergent batch anywhere is corruption.
        let narrow = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let one_col = RecordBatch::try_new(
            narrow,
            vec![Arc::new(Int64Array::from(vec![9])) as ArrayRef],
        )
        .unwrap();
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 2]), one_col],
        };
        refuses(&rb, &["batch 1"]);
    }

    #[test]
    fn arrow_ipc_still_writes_a_multi_batch_result_whose_batches_all_match() {
        // The guard must not cost a legitimate result anything: three batches of different
        // lengths, all matching the declared schema, still round-trip whole.
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 2, 3]), batch(vec![]), batch(vec![4])],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.schema, rb.schema);
        assert_eq!(back.num_rows(), 4);
        assert_eq!(back.batches.len(), 3);
        let last = back.batches[2]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(last.value(0), 4);
    }

    #[test]
    fn from_arrow_ipc_never_panics_on_a_corrupted_stream() {
        // REGRESSION + property test. `arrow-ipc`'s decoder panics rather than erroring on
        // malformed buffer offsets, and these bytes come from a peer, so a panic here is a
        // denial of service against the client process. Before the `catch_unwind` guard, 326 of
        // 3,198 single-byte mutations of this stream panicked (~10%), the first at byte 28.
        //
        // Every mutation must now come back as Ok or Err — never a panic. The assertion is on
        // the panic count, not on the error/ok split, because which corruptions are *detectable*
        // is arrow's business and may shift between versions; that none of them may take the
        // process down is ours.
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 2, 3])],
        };
        let good = to_arrow_ipc(&rb).expect("the clean stream encodes");
        assert!(from_arrow_ipc(&good).is_ok(), "and decodes");

        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let mut panics = 0usize;
        let mut checked = 0usize;
        for i in 0..good.len() {
            for v in [0x00u8, 0x01, 0x7f, 0xff, 0xfe] {
                if good[i] == v {
                    continue;
                }
                let mut bad = good.clone();
                bad[i] = v;
                checked += 1;
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // Must not panic. Result value is irrelevant.
                    let _ = from_arrow_ipc(&bad);
                }));
                if r.is_err() {
                    panics += 1;
                }
            }
        }
        std::panic::set_hook(prev);
        assert!(checked > 1_000, "the probe actually ran ({checked} cases)");
        assert_eq!(
            panics, 0,
            "{panics} of {checked} corrupted streams panicked"
        );
    }

    #[test]
    fn arrow_ipc_ignores_nullability_when_matching_batches_to_the_schema() {
        // REGRESSION. An earlier revision of ensure_batches_match_schema compared
        // is_nullable(), which rejected legitimate DataFusion results: df.schema() and the
        // execution plan's batches disagree on nullability for, among others, a UNION ALL
        // mixing a NOT NULL source with a nullable one. That turned a correct query into a 400
        // on the Arrow arm while the JSON arm answered 200. Nullability is not a positional
        // hazard — the same bytes decode identically either way — so it is not compared.
        //
        // Both directions, because a producer can drift either way.
        let declared_not_null = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]));
        let rb = RowBatch {
            // `batch()` builds its arrays as nullable; the declaration above says NOT NULL.
            schema: declared_not_null,
            batches: vec![batch(vec![1, 2])],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).expect("nullability drift must encode"))
            .expect("and decode");
        assert_eq!(back.num_rows(), 2);
        assert_eq!(back.schema.fields().len(), 3);

        // The opposite direction: declared nullable, batch fields NOT NULL.
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![7_i64, 8]));
        let strict = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![ids],
        )
        .unwrap();
        let rb2 = RowBatch {
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            batches: vec![strict],
        };
        let back2 = from_arrow_ipc(&to_arrow_ipc(&rb2).expect("the other direction too")).unwrap();
        assert_eq!(back2.num_rows(), 2);
    }

    #[test]
    fn arrow_ipc_ignores_metadata_when_matching_batches_to_the_schema() {
        // The documented half of the decision: the check compares (name, data_type) and NOT
        // metadata, because metadata changes nothing about the encoding and producers
        // legitimately differ on it. A result whose declared schema carries annotations its
        // batches do not must still be encodable.
        let annotated = Arc::new(
            Schema::new(vec![
                Field::new("id", DataType::Int64, false)
                    .with_metadata([("comment".to_string(), "primary key".to_string())].into()),
                Field::new("name", DataType::Utf8, true),
                Field::new("active", DataType::Boolean, true),
            ])
            .with_metadata([("origin".to_string(), "test".to_string())].into()),
        );
        let rb = RowBatch {
            schema: annotated,
            batches: vec![batch(vec![1, 2])],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.num_rows(), 2);
    }

    // ---- codec round-trips worth pinning -------------------------------------------------

    #[test]
    fn arrow_ipc_round_trips_a_dictionary_across_batches() {
        // Dictionary-encoded columns are the one Arrow shape whose IPC encoding is stateful:
        // the values live in separate dictionary messages, and a second batch with different
        // values forces a *replacement* message. `StreamWriter` allows replacement; a reader
        // that mishandled it would hand back the first batch's strings for the second batch.
        use arrow_array::types::Int32Type;
        use arrow_array::DictionaryArray;

        let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let sch = Arc::new(Schema::new(vec![Field::new("city", dt, true)]));
        let first: DictionaryArray<Int32Type> =
            vec!["oslo", "bergen", "oslo"].into_iter().collect();
        let second: DictionaryArray<Int32Type> = vec!["tromso", "oslo"].into_iter().collect();
        let rb = RowBatch {
            schema: sch.clone(),
            batches: vec![
                RecordBatch::try_new(sch.clone(), vec![Arc::new(first) as ArrayRef]).unwrap(),
                RecordBatch::try_new(sch.clone(), vec![Arc::new(second) as ArrayRef]).unwrap(),
            ],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.num_rows(), 5);
        assert_eq!(back.schema, sch);
        let rendered = rows(&back, Output::Csv).unwrap();
        assert!(
            rendered.contains("tromso"),
            "the replacement dictionary must survive: {rendered}"
        );
        assert_eq!(
            rendered.matches("oslo").count(),
            3,
            "three oslos across the two batches: {rendered}"
        );
    }

    #[test]
    fn arrow_ipc_round_trips_nested_list_and_struct_columns() {
        use arrow_array::{ListArray, StructArray};

        let list = ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![]),
        ]);
        let inner = StructArray::from(vec![
            (
                Arc::new(Field::new("n", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("s", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
            ),
        ]);
        let sch = Arc::new(Schema::new(vec![
            Field::new("tags", list.data_type().clone(), true),
            Field::new("nested", inner.data_type().clone(), true),
        ]));
        let rb = RowBatch {
            schema: sch.clone(),
            batches: vec![
                RecordBatch::try_new(sch.clone(), vec![Arc::new(list), Arc::new(inner)]).unwrap(),
            ],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.schema, sch, "nested types survive the wire unchanged");
        assert_eq!(back.num_rows(), 3);
        let json = rows_json(&back, false).unwrap();
        assert!(json.contains("\"tags\":[1,2]"), "{json}");
        assert!(json.contains("\"n\":30"), "{json}");
    }

    /// Three rows over a list, a struct and a map: values, nulls, empties, and a string that needs
    /// escaping both as JSON (`"`) and as CSV (`,`).
    pub(super) fn nested_rows() -> RowBatch {
        use arrow_array::builder::{Int64Builder, ListBuilder, MapBuilder, StringBuilder};
        use arrow_array::StructArray;

        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("x");
        tags.values().append_value("y");
        tags.append(true);
        tags.append(false);
        tags.append(true);
        let tags = tags.finish();

        // Row 2's struct is null over non-null fields — it must read as null, not as the fields.
        // (The null buffer is borrowed from a `BooleanArray`, so the test need not name its type.)
        let addr = StructArray::try_new(
            vec![
                Field::new("city", DataType::Utf8, true),
                Field::new("zip", DataType::Utf8, true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec![Some("KL"), Some("x"), Some("S\"G")])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("y"), Some("018")])) as ArrayRef,
            ],
            BooleanArray::from(vec![Some(true), None, Some(true)])
                .nulls()
                .cloned(),
        )
        .unwrap();

        let mut m = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        m.keys().append_value("k");
        m.values().append_value(1);
        m.append(true).unwrap();
        m.append(true).unwrap();
        m.append(false).unwrap();
        let m = m.finish();

        let sch = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("tags", tags.data_type().clone(), true),
            Field::new("addr", addr.data_type().clone(), true),
            Field::new("m", m.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            sch.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("ada"), None, Some("b,c")])),
                Arc::new(tags),
                Arc::new(addr),
                Arc::new(m),
            ],
        )
        .unwrap();
        RowBatch {
            schema: sch,
            batches: vec![batch],
        }
    }

    #[test]
    fn csv_and_tsv_write_a_nested_cell_as_the_json_the_grid_shows() {
        let rb = nested_rows();
        assert_eq!(
            rows(&rb, Output::Csv).unwrap(),
            concat!(
                "id,name,tags,addr,m\n",
                r#"1,ada,"[""x"",""y""]","{""city"":""KL""}","{""k"":1}""#,
                "\n",
                "2,,,,{}\n",
                r#"3,"b,c",[],"{""city"":""S\""G"",""zip"":""018""}","#,
                "\n",
            )
        );
        // The same cells, tab-separated: `|` stands for the tab, which no value here contains.
        // With tabs between cells the comma in `b,c` is plain text, so that cell is not quoted.
        let tsv = concat!(
            "id|name|tags|addr|m\n",
            r#"1|ada|"[""x"",""y""]"|"{""city"":""KL""}"|"{""k"":1}""#,
            "\n",
            "2||||{}\n",
            r#"3|b,c|[]|"{""city"":""S\""G"",""zip"":""018""}"|"#,
            "\n",
        );
        assert_eq!(rows(&rb, Output::Tsv).unwrap(), tsv.replace('|', "\t"));
    }

    /// Arrow's CSV writer refuses a dictionary of nested values as well, so it is written too — by
    /// the value each key points at, and empty where that value is null, not just where the key is.
    #[test]
    fn csv_writes_a_dictionary_of_structs_by_value() {
        use arrow_array::types::Int8Type;
        use arrow_array::{DictionaryArray, Int8Array, StructArray};

        let values = StructArray::try_new(
            vec![Field::new("a", DataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
            BooleanArray::from(vec![Some(true), None]).nulls().cloned(),
        )
        .unwrap();
        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(0)]);
        let d = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap();
        let sch = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("d", d.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            sch.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])), Arc::new(d)],
        )
        .unwrap();
        let rb = RowBatch {
            schema: sch,
            batches: vec![batch],
        };
        assert_eq!(
            rows(&rb, Output::Csv).unwrap(),
            "id,d\n1,\"{\"\"a\"\":1}\"\n2,\n3,\n4,\"{\"\"a\"\":1}\"\n"
        );
    }

    #[test]
    fn arrow_ipc_round_trips_all_null_columns() {
        // An all-null column is where a "clever" encoder is most tempted to drop a buffer.
        let sch = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("nothing", DataType::Null, true),
        ]));
        let rb = RowBatch {
            schema: sch.clone(),
            batches: vec![RecordBatch::try_new(
                sch.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![None, None, None])) as ArrayRef,
                    Arc::new(StringArray::from(vec![None::<&str>, None, None])) as ArrayRef,
                    Arc::new(arrow_array::NullArray::new(3)) as ArrayRef,
                ],
            )
            .unwrap()],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.schema, sch);
        assert_eq!(back.num_rows(), 3);
        assert_eq!(back.batches[0].column(0).null_count(), 3);
        assert_eq!(back.batches[0].column(1).null_count(), 3);
    }

    #[test]
    fn arrow_ipc_round_trips_sliced_arrays_without_leaking_neighbours() {
        // `RowBatch::first` caps a result with zero-copy `RecordBatch::slice`s, so every capped
        // window reaching the wire is made of sliced arrays: arrays whose buffers still hold
        // the rows on either side. The encoder must write the slice, not the buffer.
        let full = batch(vec![10, 11, 12, 13, 14]);
        let rb = RowBatch {
            schema: schema(),
            batches: vec![full.slice(1, 3)],
        };
        let back = from_arrow_ipc(&to_arrow_ipc(&rb).unwrap()).unwrap();
        assert_eq!(back.num_rows(), 3);
        let ids = back.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[11, 12, 13]);
        let names = back.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "row1", "the slice offset survives the wire");
    }

    // ---- the reader refuses a compressed stream -------------------------------------------

    /// Writing a compressed stream needs `arrow-ipc`'s `lz4`/`zstd` codecs, which are only
    /// compiled when something else in the build turns them on — in this crate that is
    /// DataFusion, behind the `sql` feature. That is also exactly the build where the risk is
    /// real, so this test lives where the hazard does.
    #[cfg(feature = "sql")]
    #[test]
    fn from_arrow_ipc_refuses_a_compressed_stream() {
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 2, 3])],
        };
        let opts = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = arrow_ipc::writer::StreamWriter::try_new_with_options(
                &mut buf,
                rb.schema.as_ref(),
                opts,
            )
            .unwrap();
            w.write(&rb.batches[0]).unwrap();
            w.finish().unwrap();
        }
        // Sanity: without the guard this body decodes fine, which is the whole problem.
        assert!(arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&buf), None).is_ok());
        let err = match from_arrow_ipc(&buf) {
            Err(e) => e.to_string(),
            Ok(rb) => panic!("decoded a compressed stream ({} rows)", rb.num_rows()),
        };
        assert!(err.contains("compressed"), "{err}");
        assert!(err.contains("LZ4_FRAME"), "{err}");
    }

    #[test]
    fn from_arrow_ipc_refuses_a_stream_truncated_inside_a_message() {
        // Truncation exactly on a message boundary is undetectable (documented on
        // `from_arrow_ipc`); truncation *inside* one is not, and must not be mistaken for a
        // short result.
        let rb = RowBatch {
            schema: schema(),
            batches: vec![batch(vec![1, 2, 3])],
        };
        let bytes = to_arrow_ipc(&rb).unwrap();
        let cut = &bytes[..bytes.len() - 16];
        assert!(from_arrow_ipc(cut).is_err());
    }
}

#[cfg(test)]
mod stream_tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::{rows, stream_rows, Output};
    use crate::engine::{RowBatch, RowStream};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// Several batches, so a streaming writer has to get the joins between them right — the comma
    /// between two JSON array elements, and the header appearing exactly once in a CSV.
    fn batches(n: usize, per: usize) -> RowBatch {
        let mut batches = Vec::new();
        let mut next = 0i64;
        for _ in 0..n {
            let ids: ArrayRef = Arc::new(Int64Array::from(
                (0..per).map(|i| next + i as i64).collect::<Vec<_>>(),
            ));
            let names: ArrayRef = Arc::new(StringArray::from(
                (0..per)
                    .map(|i| format!("row-{}", next + i as i64))
                    .collect::<Vec<_>>(),
            ));
            batches.push(RecordBatch::try_new(schema(), vec![ids, names]).unwrap());
            next += per as i64;
        }
        RowBatch {
            schema: schema(),
            batches,
        }
    }

    fn streamed(rb: &RowBatch, output: Output) -> (String, usize) {
        let stream = RowStream::from_batch(RowBatch {
            schema: rb.schema.clone(),
            batches: rb.batches.clone(),
        });
        let mut out = Vec::new();
        let rows_written = stream_rows(stream, output, &mut out).unwrap();
        (String::from_utf8(out).unwrap(), rows_written)
    }

    /// The property that lets a caller switch to the streaming path without its output changing:
    /// byte-for-byte equality with the buffered renderer, in every format.
    #[test]
    fn streamed_output_is_byte_identical_to_the_buffered_renderer() {
        let rb = batches(4, 3);
        for output in [
            Output::Csv,
            Output::Tsv,
            Output::Ndjson,
            Output::Json,
            Output::Table,
        ] {
            let (streamed, rows_written) = streamed(&rb, output);
            assert_eq!(
                streamed,
                rows(&rb, output).unwrap(),
                "{output:?} differs between the streaming and buffered renderers"
            );
            assert_eq!(rows_written, 12, "{output:?} miscounted rows");
        }
    }

    /// The joins between batches, asserted directly rather than only via equality — so a change
    /// that breaks both renderers the same way still fails here.
    #[test]
    fn batch_boundaries_do_not_leak_into_the_output() {
        let rb = batches(3, 2);

        let (csv, _) = streamed(&rb, Output::Csv);
        assert_eq!(
            csv.lines().filter(|l| l.starts_with("id,")).count(),
            1,
            "the CSV header must appear once, not once per batch: {csv:?}"
        );
        assert_eq!(csv.lines().count(), 7, "1 header + 6 rows: {csv:?}");

        let (json, _) = streamed(&rb, Output::Json);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).expect("valid JSON array");
        assert_eq!(parsed.len(), 6, "{json}");
        assert_eq!(parsed[0]["id"], 0);
        assert_eq!(parsed[5]["id"], 5);

        let (ndjson, _) = streamed(&rb, Output::Ndjson);
        assert_eq!(ndjson.lines().count(), 6);
        for line in ndjson.lines() {
            serde_json::from_str::<serde_json::Value>(line).expect("each line is a JSON object");
        }
    }

    /// An empty result is the case where "write as you go" is most tempting to get wrong — a bare
    /// `[` with no closing bracket, or a header for a table with no rows.
    #[test]
    fn an_empty_stream_matches_the_buffered_renderer_too() {
        let empty = RowBatch {
            schema: schema(),
            batches: Vec::new(),
        };
        for output in [Output::Csv, Output::Tsv, Output::Ndjson, Output::Json] {
            let (streamed, rows_written) = streamed(&empty, output);
            assert_eq!(rows_written, 0);
            assert_eq!(
                streamed,
                rows(&empty, output).unwrap(),
                "{output:?} differs on an empty result"
            );
        }
        // And the JSON is still a parseable document rather than a truncated one.
        let (json, _) = streamed(&empty, Output::Json);
        assert_eq!(
            serde_json::from_str::<Vec<serde_json::Value>>(&json).unwrap(),
            Vec::<serde_json::Value>::new()
        );
    }

    /// Nested columns stream as they render: each batch is written the same way, and the header
    /// still appears once.
    #[test]
    fn nested_columns_stream_as_they_render() {
        let one = super::tests::nested_rows();
        let rb = RowBatch {
            schema: one.schema.clone(),
            batches: vec![one.batches[0].slice(0, 2), one.batches[0].slice(2, 1)],
        };
        for output in [Output::Csv, Output::Tsv] {
            let (streamed, rows_written) = streamed(&rb, output);
            assert_eq!(rows_written, 3, "{output:?} miscounted rows");
            assert_eq!(
                streamed,
                rows(&one, output).unwrap(),
                "{output:?}: two batches stream as the one they were cut from renders"
            );
        }
    }

    // ---- the binary formats -----------------------------------------------------------------

    /// The binary formats this build writes: Parquet only with `parquet-out`.
    fn binary() -> Vec<Output> {
        let mut formats = vec![Output::Arrow, Output::Arrows];
        if super::WRITES_PARQUET {
            formats.push(Output::Parquet);
        }
        formats
    }

    /// `rb` written in `output` by [`stream_rows`], and the rows it counted.
    fn written(rb: &RowBatch, output: Output) -> (Vec<u8>, usize) {
        let stream = RowStream::from_batch(RowBatch {
            schema: rb.schema.clone(),
            batches: rb.batches.clone(),
        });
        let mut out = Vec::new();
        let rows_written = stream_rows(stream, output, &mut out).unwrap();
        (out, rows_written)
    }

    /// What a reader of the format gets back from its bytes, using each format's own reader.
    fn read_back(bytes: Vec<u8>, output: Output) -> RowBatch {
        use arrow_array::RecordBatchReader;
        match output {
            Output::Parquet => {
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        bytes::Bytes::from(bytes),
                    )
                    .unwrap()
                    .build()
                    .unwrap();
                RowBatch {
                    schema: reader.schema(),
                    batches: reader.collect::<Result<Vec<_>, _>>().unwrap(),
                }
            }
            Output::Arrow => {
                let reader =
                    arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
                        .unwrap();
                RowBatch {
                    schema: reader.schema(),
                    batches: reader.collect::<Result<Vec<_>, _>>().unwrap(),
                }
            }
            Output::Arrows => super::from_arrow_ipc(&bytes).unwrap(),
            other => unreachable!("{other:?} is text"),
        }
    }

    /// Every row in one batch, so a comparison does not depend on where a writer cut them.
    fn concat(rb: &RowBatch) -> RecordBatch {
        arrow_select::concat::concat_batches(&rb.schema, &rb.batches).unwrap()
    }

    /// Several batches, in each binary format, read back as the rows and types written.
    #[test]
    fn binary_outputs_read_back_as_the_rows_and_types_written() {
        let rb = batches(4, 3);
        for output in binary() {
            let (bytes, rows_written) = written(&rb, output);
            assert_eq!(rows_written, 12, "{output:?} miscounted rows");
            let back = read_back(bytes, output);
            assert_eq!(
                back.schema.fields(),
                rb.schema.fields(),
                "{output:?} schema"
            );
            assert_eq!(
                concat(&back).columns(),
                concat(&rb).columns(),
                "{output:?} rows"
            );
        }
    }

    /// An empty result is still a whole file with the schema in it, not zero bytes a reader
    /// rejects.
    #[test]
    fn an_empty_binary_output_is_a_whole_file_with_the_schema() {
        let empty = RowBatch {
            schema: schema(),
            batches: Vec::new(),
        };
        for output in binary() {
            let (bytes, rows_written) = written(&empty, output);
            assert_eq!(rows_written, 0);
            let back = read_back(bytes, output);
            assert_eq!(back.schema.fields(), empty.schema.fields(), "{output:?}");
            assert_eq!(back.num_rows(), 0, "{output:?}");
        }
    }

    /// Lists, structs and maps are the binary formats' own, so they come back as they went in,
    /// where CSV has to write them as JSON text.
    #[test]
    fn nested_columns_survive_the_binary_formats_whole() {
        let one = super::tests::nested_rows();
        let rb = RowBatch {
            schema: one.schema.clone(),
            batches: vec![one.batches[0].slice(0, 2), one.batches[0].slice(2, 1)],
        };
        for output in binary() {
            let (bytes, rows_written) = written(&rb, output);
            assert_eq!(rows_written, 3, "{output:?} miscounted rows");
            let back = read_back(bytes, output);
            assert_eq!(
                back.schema.fields(),
                one.schema.fields(),
                "{output:?} schema"
            );
            assert_eq!(
                concat(&back).columns(),
                one.batches[0].columns(),
                "{output:?} rows"
            );
        }
    }

    /// Each `-o` value is pinned to the framing it names: `arrow` is the IPC file format a
    /// `.arrow` reader seeks a footer in, `arrows` the stream a pipe reader takes as it comes.
    #[test]
    fn each_binary_format_has_its_own_framing() {
        let rb = batches(1, 2);
        if super::WRITES_PARQUET {
            let (parquet, _) = written(&rb, Output::Parquet);
            assert!(parquet.starts_with(b"PAR1") && parquet.ends_with(b"PAR1"));
        }
        let (file, _) = written(&rb, Output::Arrow);
        assert!(file.starts_with(b"ARROW1") && file.ends_with(b"ARROW1"));
        let (stream, _) = written(&rb, Output::Arrows);
        assert_eq!(
            &stream[..4],
            &[0xff; 4],
            "a stream opens with a continuation marker"
        );
        assert!(
            stream.windows(6).all(|w| w != b"ARROW1"),
            "no file magic in a stream"
        );
    }

    /// A dictionary that changes between batches, as one read from a Parquet file's row groups
    /// can, is written by every binary format. The IPC file format allows one dictionary per
    /// column, so it writes the values instead, at any depth; the others keep the dictionary.
    #[test]
    fn a_dictionary_that_changes_between_batches_still_writes() {
        use arrow_array::types::Int32Type;
        use arrow_array::{DictionaryArray, StructArray};

        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let inner = Field::new("city", dict.clone(), true);
        let sch: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("city", dict.clone(), true),
            Field::new("addr", DataType::Struct(vec![inner.clone()].into()), true),
        ]));
        let batch = |cities: Vec<&str>| {
            let top: DictionaryArray<Int32Type> = cities.clone().into_iter().collect();
            let nested: DictionaryArray<Int32Type> = cities.into_iter().collect();
            let addr = StructArray::try_new(
                vec![inner.clone()].into(),
                vec![Arc::new(nested) as ArrayRef],
                None,
            )
            .unwrap();
            RecordBatch::try_new(sch.clone(), vec![Arc::new(top) as ArrayRef, Arc::new(addr)])
                .unwrap()
        };
        let rb = RowBatch {
            schema: sch.clone(),
            batches: vec![batch(vec!["KL", "SG", "KL"]), batch(vec!["Tokyo", "Seoul"])],
        };
        // Each column's values as text, whatever its encoding.
        let values = |rb: &RowBatch| -> Vec<Vec<Option<String>>> {
            let all = concat(rb);
            let addr = all
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            [all.column(0), addr.column(0)]
                .into_iter()
                .map(|c| {
                    let s = arrow_cast::cast(c.as_ref(), &DataType::Utf8).unwrap();
                    let s = s.as_any().downcast_ref::<StringArray>().unwrap();
                    s.iter().map(|v| v.map(str::to_string)).collect()
                })
                .collect()
        };
        for output in binary() {
            let (bytes, rows_written) = written(&rb, output);
            assert_eq!(rows_written, 5, "{output:?}");
            let back = read_back(bytes, output);
            assert_eq!(values(&back), values(&rb), "{output:?}");
            let top = back.schema.field(0).data_type().clone();
            if output == Output::Arrow {
                assert_eq!(top, DataType::Utf8, "the file format writes the values");
                assert_eq!(
                    back.schema.field(1).data_type(),
                    &DataType::Struct(vec![Field::new("city", DataType::Utf8, true)].into()),
                    "and does so inside a struct too"
                );
            } else {
                assert_eq!(top, dict, "{output:?} keeps the dictionary");
            }
        }
    }

    /// A batch wider than the declared schema is refused by every binary writer, named.
    #[test]
    fn binary_outputs_refuse_a_batch_that_does_not_match_the_schema() {
        let declared = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for (output, what) in [
            (Output::Parquet, "a Parquet file"),
            (Output::Arrow, "an Arrow IPC file"),
            (Output::Arrows, "an Arrow IPC stream"),
        ] {
            if !binary().contains(&output) {
                continue;
            }
            let stream =
                RowStream::new(declared.clone(), batches(1, 2).batches.into_iter().map(Ok));
            let mut out = Vec::new();
            let err = stream_rows(stream, output, &mut out)
                .expect_err("a batch wider than the schema must be refused")
                .to_string();
            assert!(
                err.contains("batch 0") && err.contains(what),
                "{output:?}: {err}"
            );
        }
    }

    /// Without `parquet-out` there is no writer to reach: the refusal names the feature instead.
    #[cfg(not(feature = "parquet-out"))]
    #[test]
    fn a_build_without_parquet_out_refuses_parquet_and_says_how() {
        let stream = RowStream::from_batch(batches(1, 2));
        let mut out = Vec::new();
        let err = stream_rows(stream, Output::Parquet, &mut out)
            .expect_err("no Parquet writer in this build")
            .to_string();
        assert!(err.contains("--features parquet-out"), "{err}");
        assert!(out.is_empty(), "nothing is written before the refusal");
    }

    /// [`rows`] renders text, and says so for the binary formats.
    #[test]
    fn the_text_renderer_refuses_the_binary_formats() {
        for output in [Output::Parquet, Output::Arrow, Output::Arrows] {
            assert!(rows(&batches(1, 1), output).is_err(), "{output:?}");
        }
    }

    /// [`Output::for_path`]: case-insensitive, and `None` for what names no format.
    #[test]
    fn a_file_name_picks_the_format_by_its_extension() {
        use std::path::Path;
        for (name, want) in [
            ("out.parquet", Some(Output::Parquet)),
            ("OUT.PARQUET", Some(Output::Parquet)),
            ("out.arrow", Some(Output::Arrow)),
            ("out.feather", Some(Output::Arrow)),
            ("out.ipc", Some(Output::Arrow)),
            ("out.arrows", Some(Output::Arrows)),
            ("out.csv", Some(Output::Csv)),
            ("out.tsv", Some(Output::Tsv)),
            ("out.json", Some(Output::Json)),
            ("out.ndjson", Some(Output::Ndjson)),
            ("out.jsonl", Some(Output::Ndjson)),
            ("out.txt", None),
            ("out", None),
            ("tables.parquet/out", None),
        ] {
            assert_eq!(Output::for_path(Path::new(name)), want, "{name}");
        }
    }

    /// An error partway through a stream reaches the caller instead of being written as rows.
    #[test]
    fn an_error_mid_stream_propagates_rather_than_truncating_silently() {
        let schema = schema();
        let good = batches(1, 2).batches.remove(0);
        let stream = RowStream::new(
            schema,
            vec![
                Ok(good),
                Err(crate::error::EngineError::Query("boom".into())),
            ]
            .into_iter(),
        );
        let mut out = Vec::new();
        let err = stream_rows(stream, Output::Csv, &mut out)
            .expect_err("the error must surface, not be swallowed");
        assert!(err.to_string().contains("boom"), "{err}");
    }
}

/// Zoned timestamps in text: a column labelled `UTC`, as pandas, pyarrow and Polars label tz-aware
/// UTC data, prints in every build exactly as the release binaries print it, and a zone the build
/// cannot print is refused rather than left blank. See `crate::zone`.
#[cfg(test)]
mod time_zone_tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::{from_arrow_ipc, row_values, rows, stream_rows, Output};
    use crate::engine::{RowBatch, RowStream};
    use crate::zone::has_tz_database;

    const TEXT: [Output; 5] = [
        Output::Table,
        Output::Csv,
        Output::Tsv,
        Output::Json,
        Output::Ndjson,
    ];

    fn ts_type(zone: &str) -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone.into()))
    }

    /// Three rows: 2024-01-02T03:04:05.123456Z, 2024-06-30T23:59:59Z and a null, in `zone`.
    fn zoned(zone: &str) -> RowBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", ts_type(zone), true),
        ]));
        let ts = TimestampMicrosecondArray::from(vec![
            Some(1_704_164_645_123_456),
            Some(1_719_791_999_000_000),
            None,
        ])
        .with_timezone(zone);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(ts),
            ],
        )
        .unwrap();
        RowBatch {
            schema,
            batches: vec![batch],
        }
    }

    fn streamed(rb: &RowBatch, output: Output) -> crate::error::Result<String> {
        let mut out = Vec::new();
        let rb = RowBatch {
            schema: rb.schema.clone(),
            batches: rb.batches.clone(),
        };
        stream_rows(RowStream::from_batch(rb), output, &mut out)?;
        Ok(String::from_utf8(out).unwrap())
    }

    /// `head`'s table and CSV, as the release binaries print a `UTC` column.
    #[test]
    fn utc_prints_as_the_release_binaries_print_it() {
        let rb = zoned("UTC");
        assert_eq!(
            rows(&rb, Output::Table).unwrap(),
            "| id | ts                          | \n\
             |----|-----------------------------|\n\
             | 1  | 2024-01-02T03:04:05.123456Z | \n\
             | 2  | 2024-06-30T23:59:59Z        | \n\
             | 3  | ·                           | \n\
             \n3 row(s)\n"
        );
        assert_eq!(
            rows(&rb, Output::Csv).unwrap(),
            "id,ts\n1,2024-01-02T03:04:05.123456Z\n2,2024-06-30T23:59:59Z\n3,\n"
        );
    }

    /// Every text format, buffered and streamed, and the JSON values `/v1/rows` answers with,
    /// print `UTC` as they print `+00:00`, which arrow-cast resolves in every build.
    #[test]
    fn utc_prints_as_the_zero_offset_in_every_text_format() {
        let (utc, offset) = (zoned("UTC"), zoned("+00:00"));
        for output in TEXT {
            let want = rows(&offset, output).unwrap();
            assert!(want.contains("2024-06-30T23:59:59Z"), "{want}");
            assert_eq!(rows(&utc, output).unwrap(), want, "{}", output.name());
            assert_eq!(streamed(&utc, output).unwrap(), want, "{}", output.name());
        }
        let values = row_values(&utc).unwrap();
        assert_eq!(values, row_values(&offset).unwrap());
        assert_eq!(values[0]["ts"], "2024-01-02T03:04:05.123456Z");
    }

    /// A zone this build cannot print fails every text format, naming it, where the table used to
    /// print the column blank; a build that can prints the zone's own time.
    #[test]
    fn a_zone_this_build_cannot_print_is_refused_not_left_blank() {
        let rb = zoned("Europe/Paris");
        let mut printed = TEXT
            .iter()
            .map(|&output| rows(&rb, output))
            .chain(TEXT.iter().map(|&output| streamed(&rb, output)))
            .collect::<Vec<_>>();
        printed.push(row_values(&rb).map(|v| serde_json::Value::Array(v).to_string()));
        for text in printed {
            match text {
                Ok(text) => {
                    assert!(has_tz_database(), "{text}");
                    assert!(text.contains("2024-01-02T04:04:05.123456+01:00"), "{text}");
                }
                Err(e) => {
                    assert!(!has_tz_database(), "{e}");
                    assert!(e.to_string().contains("`Europe/Paris`"), "{e}");
                }
            }
        }
    }

    /// Only text is relabelled: Arrow output keeps the zone the rows came with, and needs no
    /// database to, so it is also how a build without one writes a column it cannot print.
    #[test]
    fn arrow_output_keeps_the_zone() {
        for zone in ["UTC", "Europe/Paris"] {
            for output in [Output::Arrow, Output::Arrows] {
                let mut out = Vec::new();
                stream_rows(RowStream::from_batch(zoned(zone)), output, &mut out).unwrap();
                let back = if output == Output::Arrows {
                    from_arrow_ipc(&out).unwrap().schema
                } else {
                    arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(out), None)
                        .unwrap()
                        .schema()
                };
                assert_eq!(back.field(1).data_type(), &ts_type(zone), "{zone}");
            }
        }
    }
}
