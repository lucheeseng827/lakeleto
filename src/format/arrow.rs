//! Arrow IPC: the file format (`.arrow`, `.feather`, `.ipc`; Feather v2 is this format) and the
//! stream format (`.arrows`), as pyarrow, Polars, DuckDB and `lakeleto -o arrow|arrows` write them.
//! Which of the two a file is, its first bytes say, whatever it is called.
//!
//! A file is read by its footer, which says where each record batch lies, and by each batch's
//! header, a few hundred bytes that say how many rows it holds. So a file's row count is known
//! without reading its rows, and a window reads only the batches it covers: from a local file by
//! seeking, from an object in a store by ranged requests. What the footer and headers say, and the
//! file's dictionaries, are kept per version of the file, so a grid scroll reads only its batches.
//!
//! A stream has no footer. It is read front to back, as the text formats are, and an object in a
//! store as its bytes arrive.
//!
//! Buffers compressed inside the file are decoded by arrow-ipc: LZ4, which pyarrow's Feather writer
//! uses by default, in every build, and zstd with the `compression` feature.

use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::{read_footer_length, FileDecoder, StreamReader};
use arrow_ipc::Block;
use arrow_schema::{ArrowError, SchemaRef};

use super::{
    collect, read_error, Cache, FileSchema, FormatReader, Input, ReadOptions, RemoteObject,
    SqlSupport, Version,
};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::{Codec, Format};

/// The Arrow IPC reader — see the module docs.
pub(crate) struct ArrowIpc;

pub(crate) static ARROW: ArrowIpc = ArrowIpc;

/// The bytes at the end of a file read to find its footer: enough to hold the footer of any file
/// with fewer than about a thousand batches, so one read finds it.
const TAIL: u64 = 64 * 1024;

/// An object in a store this small is read whole to learn its layout, rather than by a request
/// per batch header.
const SMALL_OBJECT: u64 = 4 * 1024 * 1024;

/// What a stream starts with: the continuation marker of its first message.
const CONTINUATION: [u8; 4] = [0xff; 4];

impl FormatReader for ArrowIpc {
    fn format(&self) -> Format {
        Format::Arrow
    }

    fn extensions(&self) -> &'static [&'static str] {
        &["arrow", "feather", "ipc", "arrows"]
    }

    /// DataFusion reads both framings, files and streams, from a file or an object store. Never
    /// compressed as a whole: its buffers are, inside the file.
    fn sql(&self, _codec: Option<Codec>) -> SqlSupport {
        SqlSupport::Native
    }

    /// An object is read where it lies: a file by ranged requests for its footer and the batches a
    /// read needs, a stream as its bytes arrive.
    fn streams_objects(&self) -> bool {
        true
    }

    fn schema(&self, input: Input<'_>, _opts: &ReadOptions<'_>) -> Result<FileSchema> {
        let schema = match kind(input)? {
            Kind::File(layout) => layout.schema.clone(),
            Kind::Stream => stream(input)?.schema(),
        };
        Ok(FileSchema {
            schema,
            records_path: None,
        })
    }

    fn read(
        &self,
        ctx: &RequestContext,
        input: Input<'_>,
        opts: &ReadOptions<'_>,
        row_limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        self.window(ctx, input, opts, 0, row_limit.unwrap_or(usize::MAX))
    }

    fn counts_rows(&self) -> bool {
        true
    }

    /// A file's, from its batches' headers; a stream says only by being read.
    fn row_count(&self, input: Input<'_>) -> Result<Option<u64>> {
        Ok(match kind(input)? {
            Kind::File(layout) => Some(layout.rows()),
            Kind::Stream => None,
        })
    }

    fn window(
        &self,
        ctx: &RequestContext,
        input: Input<'_>,
        _opts: &ReadOptions<'_>,
        offset: usize,
        limit: usize,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        match kind(input)? {
            Kind::File(layout) => read_file(ctx, input, &layout, offset, limit),
            Kind::Stream => {
                let reader = stream(input)?;
                let schema = reader.schema();
                let batches = collect(ctx, reader, Some(offset.saturating_add(limit)))?;
                Ok((
                    schema,
                    crate::engine::window_batches(batches, offset, limit),
                ))
            }
        }
    }
}

/// Which framing a file has, and for the file format what its footer and headers say.
#[derive(Clone)]
enum Kind {
    File(Arc<Layout>),
    Stream,
}

/// What is known of each file version, learned once.
static KINDS: Cache<Version, Kind> = Cache::new();

/// What `input` is, read from its first bytes and, for a file, its footer: once per version.
fn kind(input: Input<'_>) -> Result<Kind> {
    let version = Version::of(input);
    if let Some(kind) = version.as_ref().and_then(|v| KINDS.get(v)) {
        return Ok(kind);
    }
    let at = At::open(input)?;
    let head = at.read(0..at.len().min(8))?;
    let kind = if head.starts_with(b"ARROW1") {
        Kind::File(Arc::new(Layout::read(&at)?))
    } else if head.starts_with(&CONTINUATION) {
        Kind::Stream
    } else if head.starts_with(b"FEA1") {
        return Err(invalid(
            "it is a Feather v1 file, which pyarrow wrote before 0.17; Lakeleto reads Feather v2 — \
             rewrite it with `pyarrow.feather.write_feather` or `pandas.DataFrame.to_feather`",
        ));
    } else {
        return Err(invalid(if head.is_empty() {
            "it is empty"
        } else {
            "it starts with neither `ARROW1`, as an Arrow IPC file does, nor the continuation \
             marker a stream does"
        }));
    };
    if let Some(version) = version {
        KINDS.put(version, kind.clone());
    }
    Ok(kind)
}

/// What a file's footer and its batches' headers say, and a decoder holding its dictionaries.
struct Layout {
    schema: SchemaRef,
    /// Each record batch, with the rows it holds.
    batches: Vec<(Block, u64)>,
    /// The rows of every batch: totalled once, so no count of them can overflow later.
    rows: u64,
    decoder: FileDecoder,
}

impl Layout {
    /// The layout of the file `at`: its footer's schema and blocks, and each batch's row count from
    /// its header.
    fn read(at: &At<'_>) -> Result<Layout> {
        // A small object is read once, whole, rather than by a request for each header.
        if let At::Object(object) = at {
            if object.size() <= SMALL_OBJECT {
                return Layout::read(&At::Bytes(Cow::Owned(at.read(0..at.len())?)));
            }
        }
        let len = at.len();
        let tail_start = len.saturating_sub(TAIL);
        let tail = at.read(tail_start..len)?;
        let trailer: [u8; 10] = tail
            .get(tail.len().saturating_sub(10)..)
            .and_then(|t| t.try_into().ok())
            .ok_or_else(|| invalid("it ends before its footer"))?;
        let footer_len = read_footer_length(trailer).map_err(|e| invalid(&e.to_string()))? as u64;
        let footer_start = (len - 10)
            .checked_sub(footer_len)
            .ok_or_else(|| invalid("its footer is longer than the file"))?;
        let footer_bytes = if footer_start >= tail_start {
            Cow::Borrowed(&tail[(footer_start - tail_start) as usize..tail.len() - 10])
        } else {
            Cow::Owned(at.read(footer_start..len - 10)?)
        };
        let footer = arrow_ipc::root_as_footer(&footer_bytes)
            .map_err(|e| invalid(&format!("its footer cannot be read: {e}")))?;
        let schema = Arc::new(arrow_ipc::convert::fb_to_schema(
            footer
                .schema()
                .ok_or_else(|| invalid("its footer has no schema"))?,
        ));
        let mut decoder = FileDecoder::new(schema.clone(), footer.version());
        for block in footer.dictionaries().iter().flatten() {
            decoder
                .read_dictionary(block, &block_bytes(at, block)?)
                .map_err(decode_error)?;
        }
        let batches = footer
            .recordBatches()
            .iter()
            .flatten()
            .map(|block| Ok((*block, rows_in(at, block)?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Layout {
            schema,
            rows: total_rows(&batches)?,
            batches,
            decoder,
        })
    }

    /// How many rows the file holds.
    fn rows(&self) -> u64 {
        self.rows
    }
}

/// The rows of every batch in `batches`, or the error for a file whose headers claim more than a
/// count can hold: no window's running count of them can overflow once this one has not.
fn total_rows(batches: &[(Block, u64)]) -> Result<u64> {
    batches
        .iter()
        .try_fold(0u64, |total, (_, rows)| total.checked_add(*rows))
        .ok_or_else(|| invalid("its batches claim more rows than can be counted"))
}

/// The rows `offset..offset + limit` of a file: the batches that hold them read and decoded, and
/// cut to the window.
fn read_file(
    ctx: &RequestContext,
    input: Input<'_>,
    layout: &Layout,
    offset: usize,
    limit: usize,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let at = At::open(input)?;
    let (offset, end) = (offset as u64, (offset as u64).saturating_add(limit as u64));
    let mut batches = Vec::new();
    let mut start = 0u64;
    for (block, rows) in &layout.batches {
        if start >= end {
            break;
        }
        // Within the layout's total, which fits ([`total_rows`]).
        let stop = start + rows;
        if stop > offset {
            ctx.check()?;
            if let Some(batch) = layout
                .decoder
                .read_record_batch(block, &block_bytes(&at, block)?)
                .map_err(decode_error)?
            {
                let from = offset.saturating_sub(start);
                let to = (end - start).min(*rows);
                batches.push(batch.slice(from as usize, (to - from) as usize));
            }
        }
        start = stop;
    }
    Ok((layout.schema.clone(), batches))
}

/// How many rows the batch at `block` holds, read from its header alone.
fn rows_in(at: &At<'_>, block: &Block) -> Result<u64> {
    let start = offset_of(block)?;
    // An `i64` offset and an `i32` length: their sum fits in a `u64`.
    let header = at.read(start..start + header_len(block)?)?;
    // An encapsulated message: the continuation marker and the header's length, then the header
    // (the marker is missing from files written before Arrow 0.15).
    let message = match header.get(..4) {
        Some(marker) if marker == CONTINUATION => header.get(8..),
        _ => header.get(4..),
    }
    .ok_or_else(|| invalid("a record batch's header is cut short"))?;
    let rows = arrow_ipc::root_as_message(message)
        .ok()
        .and_then(|m| m.header_as_record_batch())
        .map(|batch| batch.length())
        .ok_or_else(|| invalid("a block its footer lists as a record batch is not one"))?;
    u64::try_from(rows).map_err(|_| invalid("a record batch says it holds fewer than no rows"))
}

/// The bytes of the message at `block`: its header and its body.
fn block_bytes(at: &At<'_>, block: &Block) -> Result<Buffer> {
    Ok(Buffer::from_vec(at.read(extent(block)?)?))
}

/// Where the message at `block` lies, as its footer says: an offset and a body length near
/// `i64::MAX` add up past any file, and past a `u64`.
fn extent(block: &Block) -> Result<Range<u64>> {
    let start = offset_of(block)?;
    let body =
        u64::try_from(block.bodyLength()).map_err(|_| invalid("a block has a negative length"))?;
    let end = start
        .checked_add(header_len(block)?)
        .and_then(|end| end.checked_add(body))
        .ok_or_else(|| invalid("a block it lists lies past its end"))?;
    Ok(start..end)
}

/// Where the message at `block` starts.
fn offset_of(block: &Block) -> Result<u64> {
    u64::try_from(block.offset()).map_err(|_| invalid("a block lies before the file's start"))
}

/// How long the header of the message at `block` is.
fn header_len(block: &Block) -> Result<u64> {
    u64::try_from(block.metaDataLength()).map_err(|_| invalid("a block has a negative length"))
}

/// A stream, read from the start.
fn stream<'a>(input: Input<'a>) -> Result<StreamReader<Box<dyn BufRead + Send + 'a>>> {
    let r: Box<dyn BufRead + Send + 'a> = match input {
        Input::File(path) => Box::new(BufReader::with_capacity(64 * 1024, File::open(path)?)),
        Input::Bytes(bytes) => Box::new(bytes),
        Input::Object(object) => object.open(None)?,
    };
    StreamReader::try_new(r, None).map_err(decode_error)
}

/// An input read by position.
enum At<'a> {
    File(File, u64),
    Bytes(Cow<'a, [u8]>),
    Object(&'a dyn RemoteObject),
}

impl<'a> At<'a> {
    /// `input`, to be read by position: a file opened, bytes and an object as they are.
    fn open(input: Input<'a>) -> Result<At<'a>> {
        Ok(match input {
            Input::File(path) => {
                let file = File::open(path)?;
                let len = file.metadata()?.len();
                At::File(file, len)
            }
            Input::Bytes(bytes) => At::Bytes(Cow::Borrowed(bytes)),
            Input::Object(object) => At::Object(object),
        })
    }

    /// How many bytes it holds.
    fn len(&self) -> u64 {
        match self {
            At::File(_, len) => *len,
            At::Bytes(bytes) => bytes.len() as u64,
            At::Object(object) => object.size(),
        }
    }

    /// The bytes in `range`, all of them.
    fn read(&self, range: Range<u64>) -> Result<Vec<u8>> {
        if range.end > self.len() || range.start > range.end {
            return Err(invalid("a block it lists lies past its end"));
        }
        let n = (range.end - range.start) as usize;
        match self {
            At::File(file, _) => {
                let mut file = file;
                file.seek(SeekFrom::Start(range.start))?;
                let mut buf = vec![0; n];
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
            At::Bytes(bytes) => Ok(bytes[range.start as usize..range.end as usize].to_vec()),
            At::Object(object) => {
                let mut buf = Vec::with_capacity(n);
                object.open(Some(range))?.read_to_end(&mut buf)?;
                if buf.len() != n {
                    return Err(invalid("the store sent fewer bytes than were asked for"));
                }
                Ok(buf)
            }
        }
    }
}

/// An input that is not the Arrow IPC it is read as.
fn invalid(why: &str) -> EngineError {
    EngineError::UnsupportedFormat {
        detail: format!("not an Arrow IPC file or stream: {why}"),
    }
}

/// A decoder's error, with a buffer compressed by a codec this build lacks named for the feature
/// that decodes it.
fn decode_error(e: ArrowError) -> EngineError {
    let message = e.to_string();
    // arrow-ipc's words for a buffer compressed with a codec it was built without.
    if message.contains("zstd IPC decompression requires the zstd feature") {
        return EngineError::UnsupportedFormat {
            detail: format!(
                "this Arrow file's buffers are zstd-compressed, and this build decompresses LZ4 \
                 only: zstd needs the `compression` feature (`cargo install lakeleto --features \
                 compression`), which the release binaries and the image have ({message})"
            ),
        };
    }
    read_error(e)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, DictionaryArray, Int32Array, Int64Array, StringArray};
    use arrow_ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
    use arrow_ipc::CompressionType;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::format::testing::MemObject;

    /// `batches` batches of `rows` rows each: an id counting up from 0, a padded name, and a
    /// dictionary-encoded colour.
    fn batches(batches: usize, rows: usize) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "colour",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));
        (0..batches)
            .map(|b| {
                let ids: Vec<i64> = (0..rows).map(|r| (b * rows + r) as i64).collect();
                let names: Vec<String> = ids.iter().map(|i| format!("name {i:>24}")).collect();
                // One dictionary for every batch: the file format holds one per column.
                let colours = DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from_iter_values(ids.iter().map(|i| (*i % 3) as i32)),
                    Arc::new(StringArray::from(vec!["red", "green", "blue"])),
                )
                .unwrap();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ids)) as ArrayRef,
                        Arc::new(StringArray::from(names)),
                        Arc::new(colours),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    fn file_bytes(batches: &[RecordBatch], compression: Option<CompressionType>) -> Vec<u8> {
        let options = IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap();
        let mut out = Vec::new();
        let mut w =
            FileWriter::try_new_with_options(&mut out, &batches[0].schema(), options).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
        drop(w);
        out
    }

    fn stream_bytes(batches: &[RecordBatch]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut w = StreamWriter::try_new(&mut out, &batches[0].schema()).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
        drop(w);
        out
    }

    fn opts() -> ReadOptions<'static> {
        ReadOptions {
            batch_size: 1024,
            csv_infer_rows: 100,
            json_path: None,
        }
    }

    /// The ids in `batches`, in order.
    fn ids(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn window(input: Input<'_>, offset: usize, limit: usize) -> (SchemaRef, Vec<RecordBatch>) {
        ARROW
            .window(&RequestContext::detached(), input, &opts(), offset, limit)
            .unwrap()
    }

    /// A file, its bytes and the object holding them all read alike: the schema and row count from
    /// the footer and headers, any window, and every row, dictionaries decoded.
    #[test]
    fn a_file_reads_by_its_footer_alike_from_disk_bytes_and_a_store() {
        let written = batches(6, 700);
        let bytes = file_bytes(&written, None);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        std::fs::write(&path, &bytes).unwrap();
        let object = MemObject::new("s3://b/t.arrow", "v1", bytes.clone());
        for input in [
            Input::File(&path),
            Input::Bytes(&bytes),
            Input::Object(&object),
        ] {
            let schema = ARROW.schema(input, &opts()).unwrap().schema;
            assert_eq!(schema, written[0].schema());
            assert_eq!(ARROW.row_count(input).unwrap(), Some(4200));
            // Across a batch boundary, and past the end.
            let (_, rows) = window(input, 690, 20);
            assert_eq!(ids(&rows), (690..710).collect::<Vec<_>>());
            let (_, rows) = window(input, 4190, 100);
            assert_eq!(ids(&rows), (4190..4200).collect::<Vec<_>>());
            let (_, every) = ARROW
                .read(&RequestContext::detached(), input, &opts(), None)
                .unwrap();
            assert_eq!(
                arrow_select::concat::concat_batches(&schema, &every).unwrap(),
                arrow_select::concat::concat_batches(&schema, &written).unwrap()
            );
        }
    }

    /// In a store, a file's layout is learned once per version, and after that a window is one
    /// ranged request for each batch it covers, and a schema or a count none at all.
    #[test]
    fn a_window_of_a_file_in_a_store_asks_only_for_its_batches() {
        // Larger than an object read whole to learn its layout.
        let written = batches(8, 40_000);
        let bytes = file_bytes(&written, None);
        assert!(bytes.len() as u64 > SMALL_OBJECT);
        let object = MemObject::new("s3://b/big.arrow", "v1", bytes.clone());
        assert_eq!(
            ARROW.row_count(Input::Object(&object)).unwrap(),
            Some(320_000)
        );
        assert!(
            object.taken() < bytes.len() as u64 / 50,
            "learning the layout took {} bytes",
            object.taken()
        );

        object.reset();
        ARROW.schema(Input::Object(&object), &opts()).unwrap();
        assert_eq!(
            ARROW.row_count(Input::Object(&object)).unwrap(),
            Some(320_000)
        );
        assert_eq!(object.requests(), [], "known for this version");

        let (_, rows) = window(Input::Object(&object), 120_010, 5);
        assert_eq!(ids(&rows), (120_010..120_015).collect::<Vec<_>>());
        assert_eq!(object.requests().len(), 1, "the one batch the window is in");
        assert!(object.taken() < bytes.len() as u64 / 4);

        // Another version is learned afresh.
        let changed = MemObject::new("s3://b/big.arrow", "v2", file_bytes(&written[..2], None));
        assert_eq!(
            ARROW.row_count(Input::Object(&changed)).unwrap(),
            Some(80_000)
        );
    }

    /// Learning a file's layout in a store costs a request for its first bytes, which say it is a
    /// file, one for its tail, which holds the footer of a file of many batches, one for its
    /// dictionary, and one per batch header. An object small enough to fetch whole costs one
    /// request after its first bytes, however many batches it has.
    #[test]
    fn a_layout_costs_the_tail_and_each_header_or_one_read_when_small() {
        let written = batches(64, 5_000);
        let bytes = file_bytes(&written, None);
        assert!(bytes.len() as u64 > SMALL_OBJECT, "{} bytes", bytes.len());
        let object = MemObject::new("s3://b/many.arrow", "v1", bytes);
        let count = ARROW.row_count(Input::Object(&object)).unwrap();
        assert_eq!(count, Some(320_000));
        assert_eq!(
            object.requests().len(),
            1 + 1 + 1 + 64,
            "its first bytes, the tail, the dictionary, each header"
        );

        let written = batches(2, 20_000);
        let bytes = file_bytes(&written, None);
        let size = bytes.len() as u64;
        assert!(size > 1 << 20 && size <= SMALL_OBJECT, "{size} bytes");
        let object = MemObject::new("s3://b/small.arrow", "v1", bytes);
        let count = ARROW.row_count(Input::Object(&object)).unwrap();
        assert_eq!(count, Some(40_000));
        assert_eq!(
            object.requests().len(),
            1 + 1,
            "its first bytes, then all of it"
        );
    }

    /// A stream is read front to back: its schema from its first message, and its rows as far as
    /// a read goes. It has no footer, so no count.
    #[test]
    fn a_stream_reads_front_to_back() {
        let written = batches(3, 500);
        let bytes = stream_bytes(&written);
        let object = MemObject::new("s3://b/t.arrows", "v1", bytes.clone());
        for input in [Input::Bytes(&bytes), Input::Object(&object)] {
            assert_eq!(
                ARROW.schema(input, &opts()).unwrap().schema,
                written[0].schema()
            );
            assert_eq!(ARROW.row_count(input).unwrap(), None);
            let (_, rows) = window(input, 480, 40);
            assert_eq!(ids(&rows), (480..520).collect::<Vec<_>>());
        }
        object.reset();
        window(Input::Object(&object), 0, 10);
        assert_eq!(object.requests(), [None], "one request, read as it arrives");
        assert!(object.taken() < bytes.len() as u64);
    }

    /// Buffers compressed with LZ4, as pyarrow's Feather writer does by default, read in every
    /// build; zstd ones with `compression`, and without it are refused naming the feature.
    #[test]
    fn compressed_buffers_read_with_the_codecs_this_build_has() {
        let written = batches(2, 300);
        let lz4 = file_bytes(&written, Some(CompressionType::LZ4_FRAME));
        let (_, rows) = window(Input::Bytes(&lz4), 0, 600);
        assert_eq!(ids(&rows), (0..600).collect::<Vec<_>>());

        // A writer without zstd cannot write zstd either, so the bytes are made where it can be.
        #[cfg(feature = "compression")]
        {
            let zstd = file_bytes(&written, Some(CompressionType::ZSTD));
            let (_, rows) = window(Input::Bytes(&zstd), 250, 100);
            assert_eq!(ids(&rows), (250..350).collect::<Vec<_>>());
        }
    }

    /// arrow-ipc's error for a zstd buffer in a build without zstd is answered with the feature
    /// that decodes it.
    #[test]
    fn a_zstd_buffer_in_a_build_without_zstd_names_the_feature() {
        let err = decode_error(ArrowError::InvalidArgumentError(
            "zstd IPC decompression requires the zstd feature".to_string(),
        ))
        .to_string();
        assert!(err.contains("--features compression"), "{err}");
        assert!(matches!(
            decode_error(ArrowError::ParseError("x".into())),
            EngineError::Arrow(_)
        ));
    }

    /// What is not an Arrow file or stream is refused, saying what it is instead.
    #[test]
    fn what_is_not_arrow_is_refused_saying_what_it_is() {
        for (bytes, says) in [
            (&b""[..], "it is empty"),
            (b"FEA1\x00\x00\x00\x00rest", "Feather v1"),
            (b"id,name\n1,ada\n", "starts with neither"),
            (b"ARROW1\x00\x00 cut short", "not an Arrow IPC file"),
        ] {
            let err = ARROW
                .schema(Input::Bytes(bytes), &opts())
                .unwrap_err()
                .to_string();
            assert!(err.contains("not an Arrow IPC file or stream"), "{err}");
            assert!(err.contains(says), "{says}: {err}");
        }
    }

    /// A footer's block is where it says, unless its offset and lengths add up past a `u64`: then
    /// it is refused, not added around to a range inside the file.
    #[test]
    fn a_block_past_any_file_is_refused_not_wrapped_around() {
        assert_eq!(extent(&Block::new(8, 120, 1000)).unwrap(), 8..1128);
        assert_eq!(
            extent(&Block::new(i64::MAX, 0, i64::MAX)).unwrap(),
            i64::MAX as u64..u64::MAX - 1
        );
        for block in [
            Block::new(i64::MAX, 2, i64::MAX),
            Block::new(i64::MAX, i32::MAX, i64::MAX),
        ] {
            let err = extent(&block).unwrap_err().to_string();
            assert!(err.contains("lies past its end"), "{block:?}: {err}");
        }
    }

    /// Batches whose headers claim more rows between them than a `u64` counts are refused when
    /// the layout is read, so no count of them overflows later.
    #[test]
    fn batches_claiming_more_rows_than_can_be_counted_are_refused() {
        let block = Block::new(0, 0, 0);
        assert_eq!(total_rows(&[]).unwrap(), 0);
        assert_eq!(total_rows(&[(block, 40_000), (block, 2)]).unwrap(), 40_002);
        assert_eq!(
            total_rows(&[(block, u64::MAX - 1), (block, 1)]).unwrap(),
            u64::MAX
        );
        let err = total_rows(&[(block, u64::MAX - 1), (block, 1), (block, 1)])
            .unwrap_err()
            .to_string();
        assert!(err.contains("more rows than can be counted"), "{err}");
    }
}
