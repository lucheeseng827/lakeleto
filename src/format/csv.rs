//! CSV and TSV: delimited text with a header row, read by arrow-csv. One reader, two registry
//! entries — the delimiter is the format's ([`Format::delimiter`]), so an explicit `--format tsv`
//! splits on tabs whatever the file is called.
//!
//! An object in a store is read as its bytes arrive ([`Input::Object`]), in one request per read:
//! the rows a schema is inferred from are the first the read decodes, so they are taken once.

use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use super::{
    batch_size_for, collect, FileSchema, FormatReader, Input, Pass, PassFn, ReadOptions, SqlSupport,
};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::{Codec, Format};

pub(crate) struct Delimited {
    format: Format,
    extensions: &'static [&'static str],
    /// [`Delimited::pass`] for this format, as SQL takes it.
    pass: PassFn,
}

pub(crate) static CSV: Delimited = Delimited {
    format: Format::Csv,
    extensions: &["csv"],
    pass: csv_pass,
};

pub(crate) static TSV: Delimited = Delimited {
    format: Format::Tsv,
    extensions: &["tsv"],
    pass: tsv_pass,
};

/// SQL's pass over a compressed CSV file.
fn csv_pass(
    input: Input<'_>,
    opts: &ReadOptions<'_>,
    schema: &SchemaRef,
    each: &mut dyn FnMut(RecordBatch) -> bool,
) -> Result<Pass> {
    CSV.pass(input, opts, schema, each)
}

/// SQL's pass over a compressed TSV file.
fn tsv_pass(
    input: Input<'_>,
    opts: &ReadOptions<'_>,
    schema: &SchemaRef,
    each: &mut dyn FnMut(RecordBatch) -> bool,
) -> Result<Pass> {
    TSV.pass(input, opts, schema, each)
}

impl Delimited {
    /// How arrow-csv reads this format: a header row, then rows split on its delimiter.
    fn dialect(&self) -> arrow_csv::reader::Format {
        arrow_csv::reader::Format::default()
            .with_header(true)
            .with_delimiter(self.format.delimiter())
    }

    /// The schema of the first `rows` rows of `input`.
    fn infer(&self, input: Input<'_>, rows: usize) -> Result<SchemaRef> {
        match input {
            Input::File(path) => self.infer_from(BufReader::new(File::open(path)?), rows),
            Input::Bytes(bytes) => self.infer_from(Cursor::new(bytes), rows),
            Input::Object(object) => self.infer_from(object.open(None)?, rows),
        }
    }

    /// The schema of the first `rows` rows read from `r`.
    fn infer_from(&self, r: impl Read, rows: usize) -> Result<SchemaRef> {
        let (schema, _) = self
            .dialect()
            .infer_schema(r, Some(rows))
            .map_err(EngineError::arrow)?;
        Ok(Arc::new(schema))
    }

    /// The rows read from `r`, decoded as `schema`, up to `row_limit` of them.
    fn decode(
        &self,
        ctx: &RequestContext,
        schema: &SchemaRef,
        r: impl Read,
        opts: &ReadOptions<'_>,
        row_limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let reader = self.reader(schema, r, batch_size_for(opts, row_limit))?;
        collect(ctx, reader, row_limit)
    }

    /// The rows read from `r`, decoded as `schema`, `batch_size` to a batch.
    fn reader<R: Read>(
        &self,
        schema: &SchemaRef,
        r: R,
        batch_size: usize,
    ) -> Result<arrow_csv::Reader<R>> {
        arrow_csv::reader::ReaderBuilder::new(schema.clone())
            .with_header(true)
            .with_delimiter(self.format.delimiter())
            .with_batch_size(batch_size)
            .build(r)
            .map_err(EngineError::arrow)
    }

    /// Every row of `input`, decoded as `schema` and handed to `each` until it returns `false`:
    /// SQL's [`PassFn`] for a compressed file of this format. A value that does not fit `schema`,
    /// one past the rows it was inferred from, fails the pass, as it fails DataFusion's own scan
    /// of a file that is not compressed. Read as [`Delimited::read`] reads, so arrow-csv is built
    /// for no reader type it was not built for already.
    fn pass(
        &self,
        input: Input<'_>,
        opts: &ReadOptions<'_>,
        schema: &SchemaRef,
        each: &mut dyn FnMut(RecordBatch) -> bool,
    ) -> Result<Pass> {
        let size = opts.batch_size;
        match input {
            Input::File(path) => hand_over(self.reader(schema, File::open(path)?, size)?, each),
            Input::Bytes(bytes) => hand_over(self.reader(schema, Cursor::new(bytes), size)?, each),
            Input::Object(object) => {
                hand_over(self.reader(schema, object.open(None)?, size)?, each)
            }
        }
    }
}

/// Every batch of `batches`, handed to `each` until it returns `false`.
fn hand_over(
    batches: impl Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
    each: &mut dyn FnMut(RecordBatch) -> bool,
) -> Result<Pass> {
    for batch in batches {
        if !each(batch.map_err(EngineError::arrow)?) {
            break;
        }
    }
    Ok(Pass::Done)
}

impl FormatReader for Delimited {
    fn format(&self) -> Format {
        self.format
    }

    fn extensions(&self) -> &'static [&'static str] {
        self.extensions
    }

    /// DataFusion's own scan, which splits a file across every core — unless it is compressed:
    /// then a pass at a time through this reader, as DataFusion would decompress it with no limit
    /// (and on one core: a compressed file cannot be split).
    fn sql(&self, codec: Option<Codec>) -> SqlSupport {
        match codec {
            Some(_) => SqlSupport::Streamed(self.pass),
            None => SqlSupport::Native,
        }
    }

    /// An object is read as its bytes arrive, one request a read.
    fn streams_objects(&self) -> bool {
        true
    }

    fn compressible(&self) -> bool {
        true
    }

    fn schema(&self, input: Input<'_>, opts: &ReadOptions<'_>) -> Result<FileSchema> {
        Ok(FileSchema {
            schema: self.infer(input, opts.csv_infer_rows)?,
            records_path: None,
        })
    }

    /// Up to `row_limit` rows of `input`, all of them for `None`, and the schema they are decoded
    /// as: inferred over the first `csv_infer_rows` rows, or over every row read when a limit asks
    /// for more.
    fn read(
        &self,
        ctx: &RequestContext,
        input: Input<'_>,
        opts: &ReadOptions<'_>,
        row_limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        // Infer over at least the rows read: a column that only widens after `csv_infer_rows`
        // rows would otherwise make the fixed-schema reader error.
        let infer_rows = row_limit.map_or(opts.csv_infer_rows, |n| n.max(opts.csv_infer_rows));
        match input {
            Input::File(path) => {
                let schema = self.infer(input, infer_rows)?;
                let batches = self.decode(ctx, &schema, File::open(path)?, opts, row_limit)?;
                Ok((schema, batches))
            }
            Input::Bytes(bytes) => {
                let schema = self.infer(input, infer_rows)?;
                let batches = self.decode(ctx, &schema, Cursor::new(bytes), opts, row_limit)?;
                Ok((schema, batches))
            }
            // The rows inference reads are the first the read decodes, so the bytes it takes are
            // kept as they go by and decoded from memory, ahead of the rest of the same response.
            // A second request would download them again: for a window deep in the object, most
            // of it. Both sides are the boxed reader an object's bytes always come as, so arrow-csv
            // is built for no reader type it was not built for already: new ones slowed
            // DataFusion's own CSV scan in the release build (docs/FORMATS-PLAN.md, item 8).
            Input::Object(object) => {
                let mut response = object.open(None)?;
                let mut taken = Vec::new();
                let sampled: Box<dyn BufRead + Send + '_> = Box::new(BufReader::new(Tee {
                    inner: &mut response,
                    taken: &mut taken,
                }));
                let schema = self.infer_from(sampled, infer_rows)?;
                let rows: Box<dyn BufRead + Send + '_> =
                    Box::new(Cursor::new(taken).chain(response));
                let batches = self.decode(ctx, &schema, rows, opts, row_limit)?;
                Ok((schema, batches))
            }
        }
    }
}

/// A reader that keeps a copy of every byte read through it.
struct Tee<'a, R> {
    inner: R,
    taken: &'a mut Vec<u8>,
}

impl<R: Read> Read for Tee<'_, R> {
    /// Read into `buf` from the inner reader, keeping a copy of what it read.
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.taken.extend_from_slice(&buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::testing::MemObject;
    use crate::format::RemoteObject;

    /// A file, the same bytes fetched from a store, and the object read as it arrives all read
    /// identically — the first two used to be two copies of this code, one in each of the local
    /// engine's local and remote paths.
    #[test]
    fn a_file_and_its_bytes_read_alike() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tsv");
        let body = b"a\tb\n1\tx\n2\ty\n3\tz\n";
        std::fs::write(&path, body).unwrap();
        let opts = ReadOptions {
            batch_size: 2,
            csv_infer_rows: 10,
            json_path: None,
        };
        let ctx = RequestContext::detached();
        let (file_schema, from_file) = TSV.read(&ctx, Input::File(&path), &opts, None).unwrap();
        let (byte_schema, from_bytes) = TSV.read(&ctx, Input::Bytes(body), &opts, None).unwrap();
        assert_eq!(file_schema, byte_schema);
        assert_eq!(file_schema.fields().len(), 2, "split on tabs");
        assert_eq!(from_file, from_bytes);
        assert_eq!(from_file.len(), 2, "batches of two");
        let object = MemObject::new("s3://bucket/t.tsv", "v1", body.to_vec());
        let (object_schema, from_object) =
            TSV.read(&ctx, Input::Object(&object), &opts, None).unwrap();
        assert_eq!(object_schema, file_schema);
        assert_eq!(from_object, from_file);
        let (_, limited) = TSV.read(&ctx, Input::Bytes(body), &opts, Some(1)).unwrap();
        assert_eq!(limited.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        // The delimiter is the format's: the same bytes as CSV are one column.
        let as_csv = CSV.schema(Input::Bytes(body), &opts).unwrap().schema;
        assert_eq!(as_csv.fields().len(), 1);
    }

    /// Compressed, a CSV or TSV file is read by SQL a pass at a time through this reader: every
    /// row, a batch at a time, from a file, its bytes or an object alike, and no further than the
    /// query takes.
    #[test]
    fn a_pass_hands_over_every_row_until_told_to_stop() {
        let dir = tempfile::tempdir().unwrap();
        let opts = ReadOptions {
            batch_size: 2,
            csv_infer_rows: 10,
            json_path: None,
        };
        for (reader, body) in [
            (&CSV, "a,b\n1,x\n2,y\n3,z\n"),
            (&TSV, "a\tb\n1\tx\n2\ty\n3\tz\n"),
        ] {
            let SqlSupport::Streamed(pass) = reader.sql(Some(Codec::Gzip)) else {
                panic!("{} is read natively, compressed", reader.format());
            };
            let schema = reader
                .schema(Input::Bytes(body.as_bytes()), &opts)
                .unwrap()
                .schema;
            assert_eq!(schema.fields().len(), 2, "{}", reader.format());
            let path = dir.path().join(reader.extensions()[0]);
            std::fs::write(&path, body).unwrap();
            let object = MemObject::new("s3://bucket/t", "v1", body);
            for input in [
                Input::File(&path),
                Input::Bytes(body.as_bytes()),
                Input::Object(&object),
            ] {
                let mut rows = Vec::new();
                let mut every = |batch: RecordBatch| {
                    rows.push(batch.num_rows());
                    true
                };
                let done = pass(input, &opts, &schema, &mut every).unwrap();
                assert!(matches!(done, Pass::Done));
                assert_eq!(rows, [2, 1], "{} {input:?}", reader.format());
                let mut batches = 0;
                let mut first = |_: RecordBatch| {
                    batches += 1;
                    false
                };
                pass(input, &opts, &schema, &mut first).unwrap();
                assert_eq!(batches, 1, "{} {input:?}: stops when told", reader.format());
            }
        }
    }

    /// `rows` CSV records under a header, every seventh with a quoted line break and padded so a
    /// few hundred fill a response's chunk.
    fn records(rows: usize) -> String {
        let pad = "x".repeat(40);
        let mut body = String::from("id,note,pad\n");
        for i in 0..rows {
            let note = if i % 7 == 0 { "\"two\nlines\"" } else { "one" };
            body.push_str(&format!("{i},{note},{pad}\n"));
        }
        body
    }

    /// Each read of a CSV object — its schema, a window inside the inference sample, one past it,
    /// every row — gives what a read of the same file does, in one request that stops with it.
    #[test]
    fn a_read_of_an_object_is_one_request_that_stops_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.csv");
        let body = records(50_000);
        std::fs::write(&path, &body).unwrap();
        let object = MemObject::new("s3://bucket/big.csv", "v1", body.clone());
        let opts = ReadOptions {
            batch_size: 1024,
            csv_infer_rows: 1000,
            json_path: None,
        };
        let ctx = RequestContext::detached();
        let tenth = body.len() as u64 / 10;

        // A schema, and a window inside the sample and past it: each read one request, stopped
        // where it stops, and the rows a file read gives.
        let schema = CSV.schema(Input::Object(&object), &opts).unwrap().schema;
        assert_eq!(
            schema,
            CSV.schema(Input::File(&path), &opts).unwrap().schema
        );
        assert_eq!(object.requests(), [None]);
        assert!(object.taken() < tenth, "took {} bytes", object.taken());
        for row_limit in [10, 3000] {
            object.reset();
            let read = CSV.read(&ctx, Input::Object(&object), &opts, Some(row_limit));
            let file = CSV.read(&ctx, Input::File(&path), &opts, Some(row_limit));
            assert_eq!(read.unwrap(), file.unwrap(), "{row_limit} rows");
            assert_eq!(object.requests(), [None], "{row_limit} rows");
            assert!(object.taken() < tenth, "took {} bytes", object.taken());
        }

        // Every row, past the bytes inference kept, in the same one request.
        object.reset();
        let (_, every) = CSV.read(&ctx, Input::Object(&object), &opts, None).unwrap();
        assert_eq!(every.iter().map(|b| b.num_rows()).sum::<usize>(), 50_000);
        assert_eq!(object.requests(), [None]);
        assert_eq!(object.taken(), object.size());
    }
}
