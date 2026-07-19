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
}

// ---- row batches ----------------------------------------------------------------------

/// Render a [`RowBatch`] in the requested output format.
pub fn rows(rb: &RowBatch, output: Output) -> Result<String> {
    match output {
        Output::Table => Ok(rows_table(rb)),
        Output::Json => rows_json(rb, false),
        Output::Ndjson => rows_json(rb, true),
        Output::Csv => rows_csv(rb),
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

fn rows_csv(rb: &RowBatch) -> Result<String> {
    let mut buf = Vec::new();
    {
        let mut w = arrow_csv::writer::WriterBuilder::new()
            .with_header(true)
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
