//! Parquet's compression codecs: which builds read which. Every build reads pages compressed with
//! Snappy, gzip or LZ4, through libraries it links anyway; zstd, Polars' default, and Brotli need
//! `compression`, which the release binaries have, and `iceberg` reads zstd too. A build without a
//! codec refuses its file with the feature that reads it, where parquet's own error named a
//! feature of parquet's.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

use lakeleto::engine::Engine;
use lakeleto::render::{rows, Output};
use lakeleto::{LocalReaderEngine, RequestContext, Source};

/// Four rows: an id, and a name with a null.
fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("Ada"),
                Some("Grace"),
                None,
                Some("Alan"),
            ])),
        ],
    )
    .unwrap()
}

/// [`batch`] as Parquet compressed with `codec`.
fn written(codec: Compression) -> Vec<u8> {
    let props = WriterProperties::builder().set_compression(codec).build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, batch().schema(), Some(props)).unwrap();
    writer.write(&batch()).unwrap();
    writer.close().unwrap();
    out
}

/// The first rows of the Parquet file `bytes`, read by the local engine, as CSV.
fn read(dir: &std::path::Path, name: &str, bytes: &[u8]) -> lakeleto::Result<String> {
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    let source = Source::detect(&path).unwrap();
    let rb = LocalReaderEngine::default().preview(&RequestContext::detached(), &source, 10)?;
    rows(&rb, Output::Csv)
}

/// Parquet reads in every codec this build decompresses, the Hadoop framing of LZ4 as well as the
/// raw one: the rows come back as from an uncompressed file. The footer shows the pages are in
/// the codec, not left uncompressed.
#[test]
fn every_codec_this_build_has_reads() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let mut codecs = vec![
        Compression::SNAPPY,
        Compression::GZIP(GzipLevel::default()),
        Compression::LZ4,
        Compression::LZ4_RAW,
    ];
    // `sql` has all five, as DataFusion takes parquet with its default features.
    if cfg!(any(
        feature = "compression",
        feature = "iceberg",
        feature = "sql"
    )) {
        codecs.push(Compression::ZSTD(ZstdLevel::default()));
    }
    if cfg!(any(feature = "compression", feature = "sql")) {
        codecs.push(Compression::BROTLI(BrotliLevel::default()));
    }
    let dir = tempfile::tempdir().unwrap();
    let want = read(
        dir.path(),
        "plain.parquet",
        &written(Compression::UNCOMPRESSED),
    )
    .unwrap();
    assert!(want.starts_with("id,name\n1,Ada\n"), "{want}");
    for (i, codec) in codecs.into_iter().enumerate() {
        let bytes = written(codec);
        let footer = SerializedFileReader::new(bytes::Bytes::from(bytes.clone())).unwrap();
        assert_eq!(
            footer.metadata().row_group(0).column(0).compression(),
            codec
        );
        let got = read(dir.path(), &format!("codec{i}.parquet"), &bytes);
        assert_eq!(got.unwrap_or_else(|e| panic!("{codec:?}: {e}")), want);
    }
}

/// [`batch`] as Parquet whose footer says its pages are compressed with `codec`, though they are
/// not. That is enough for a build without the codec: its reader refuses on the footer, before
/// it reads a page.
#[cfg(not(any(feature = "compression", feature = "sql")))]
fn claiming(codec: Compression) -> Vec<u8> {
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter};

    let bytes = bytes::Bytes::from(written(Compression::UNCOMPRESSED));
    let footer = ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .unwrap();
    let row_groups = footer
        .row_groups()
        .iter()
        .map(|rg| {
            let columns = rg
                .columns()
                .iter()
                .map(|c| {
                    c.clone()
                        .into_builder()
                        .set_compression(codec)
                        .build()
                        .unwrap()
                })
                .collect();
            rg.clone()
                .into_builder()
                .set_column_metadata(columns)
                .build()
                .unwrap()
        })
        .collect();
    let claimed = ParquetMetaData::new(footer.file_metadata().clone(), row_groups);
    // The old footer is its metadata, the metadata's length in four bytes, and `PAR1`.
    let tail: [u8; 4] = bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap();
    let mut out = bytes[..bytes.len() - 8 - u32::from_le_bytes(tail) as usize].to_vec();
    ParquetMetaDataWriter::new(&mut out, &claimed)
        .finish()
        .unwrap();
    out
}

/// A build without zstd or Brotli refuses a file in one, naming the feature that reads it and the
/// codecs the build has, where it used to fail in parquet's words. The footer still reads:
/// `schema` needs no page.
#[cfg(not(any(feature = "compression", feature = "sql")))]
#[test]
fn a_codec_this_build_lacks_is_refused_naming_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    let mut codecs = vec![(Compression::BROTLI(BrotliLevel::default()), "Brotli")];
    let mut has = "Snappy, gzip, LZ4 and zstd";
    if !cfg!(feature = "iceberg") {
        codecs.push((Compression::ZSTD(ZstdLevel::default()), "zstd"));
        has = "Snappy, gzip and LZ4";
    }
    for (codec, name) in codecs {
        let path = dir.path().join(format!("{name}.parquet"));
        std::fs::write(&path, claiming(codec)).unwrap();
        let source = Source::detect(&path).unwrap();
        let schema = LocalReaderEngine::default()
            .schema(&RequestContext::detached(), &source)
            .unwrap();
        assert_eq!(schema.row_count, Some(4), "{name}");

        let refusal = read(dir.path(), &format!("{name}.parquet"), &claiming(codec))
            .unwrap_err()
            .to_string();
        assert!(refusal.contains(&format!("{name}-compressed")), "{refusal}");
        assert!(
            refusal.contains(&format!("decompresses {has} only")),
            "{refusal}"
        );
        assert!(refusal.contains("--features compression"), "{refusal}");
    }
}
