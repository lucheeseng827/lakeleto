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
}

// ---- row batches ----------------------------------------------------------------------

/// Render a [`RowBatch`] in the requested output format.
pub fn rows(rb: &RowBatch, output: Output) -> Result<String> {
    match output {
        Output::Table => Ok(rows_table(rb)),
        Output::Json => rows_json(rb, false),
        Output::Ndjson => rows_json(rb, true),
        Output::Csv => rows_delimited(rb, b','),
        Output::Tsv => rows_delimited(rb, b'\t'),
    }
}

fn rows_table(rb: &RowBatch) -> String {
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
    out
}

/// Row batch → Parquet bytes (Snappy), for `export-current-view`.
pub fn to_parquet(rb: &RowBatch) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, rb.schema.clone(), Some(props))
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
fn ensure_batches_match_schema(rb: &RowBatch) -> Result<()> {
    let want = rb.schema.fields();
    for (i, batch) in rb.batches.iter().enumerate() {
        let got = batch.schema_ref().fields();
        if want.len() != got.len() {
            return Err(EngineError::Arrow(format!(
                "batch {i} has {} column(s) but the row batch declares {}: [{}] vs [{}] — \
                 refusing to write an Arrow IPC stream, which would encode the batch \
                 positionally against the declared schema and hand the reader wrong data",
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
                    "batch {i}, column {c}: the batch has `{}` but the row batch declares `{}` — \
                     refusing to write an Arrow IPC stream, which would encode the batch \
                     positionally against the declared schema and hand the reader wrong data",
                    describe_field(g),
                    describe_field(w),
                )));
            }
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
            w.write(b).map_err(EngineError::arrow)?;
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
            w.write(b).map_err(EngineError::arrow)?;
        }
        w.finish().map_err(EngineError::arrow)?;
    } else {
        let mut w = arrow_json::ArrayWriter::new(&mut buf);
        for b in &rb.batches {
            w.write(b).map_err(EngineError::arrow)?;
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
            w.write(b).map_err(EngineError::arrow)?;
        }
    }
    String::from_utf8(buf).map_err(|e| EngineError::Other(e.to_string()))
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
    let mut out = format!(
        "source : {}\nformat : {}\nengine : {}\nrows   : {}\ncolumns: {}\n\n",
        s.source,
        s.format,
        s.engine,
        rows_count,
        s.columns.len()
    );
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
