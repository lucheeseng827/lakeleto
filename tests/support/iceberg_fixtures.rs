//! Iceberg tables in the S3 stand-in, for the tests that read them: Avro manifests and manifest
//! lists, Parquet data files, metadata, and `warehouse/db/orders`, a table written twice.
//!
//! Included with `#[path = "support/iceberg_fixtures.rs"] mod iceberg_fixtures;`, next to
//! `fake_s3`, which it fills.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::{Schema, Writer};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::fake_s3::FakeS3;

pub const MANIFEST_LIST_SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[
  {"name":"manifest_path","type":"string"},
  {"name":"content","type":"int"},
  {"name":"sequence_number","type":"long"}]}"#;

/// The fields of a manifest entry this reader uses, as a writer records them: the file's row count
/// and size included.
pub const MANIFEST_SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"sequence_number","type":["null","long"]},
  {"name":"data_file","type":{"type":"record","name":"r2","fields":[
    {"name":"content","type":"int"},
    {"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"},
    {"name":"record_count","type":"long"},
    {"name":"file_size_in_bytes","type":"long"}]}}]}"#;

/// An Avro container of `records`.
pub fn avro(schema: &str, records: Vec<Value>) -> Vec<u8> {
    let schema = Schema::parse_str(schema).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    for record in records {
        writer.append(record).unwrap();
    }
    writer.into_inner().unwrap()
}

/// A Parquet file of rows `ids`: an `id`, and a `pad` of 64 characters that differ row to row, so
/// a row group is mostly bytes that would have to be fetched. Row groups of `group` rows.
pub fn parquet(ids: std::ops::Range<i64>, group: usize) -> Vec<u8> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let pad: Vec<String> = ids
        .clone()
        .map(|id| {
            let mut x = (id as u64)
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1);
            (0..4)
                .map(|_| {
                    x = x
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    format!("{x:016x}")
                })
                .collect()
        })
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids)) as ArrayRef,
            Arc::new(StringArray::from(pad)) as ArrayRef,
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group))
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    out
}

pub fn uri(key: &str) -> String {
    format!("s3://bucket/{key}")
}

/// A manifest entry for the file at `path`: data (`content` 0) or positional deletes (1).
pub fn entry(content: i32, path: &str, rows: i64, size: usize) -> Value {
    Value::Record(vec![
        ("status".into(), Value::Int(1)),
        (
            "sequence_number".into(),
            Value::Union(1, Box::new(Value::Long(1))),
        ),
        (
            "data_file".into(),
            Value::Record(vec![
                ("content".into(), Value::Int(content)),
                ("file_path".into(), Value::String(path.to_string())),
                ("file_format".into(), Value::String("PARQUET".into())),
                ("record_count".into(), Value::Long(rows)),
                ("file_size_in_bytes".into(), Value::Long(size as i64)),
            ]),
        ),
    ])
}

pub fn manifest_list(manifests: &[(&str, i32)]) -> Vec<u8> {
    avro(
        MANIFEST_LIST_SCHEMA,
        manifests
            .iter()
            .map(|(path, content)| {
                Value::Record(vec![
                    ("manifest_path".into(), Value::String(path.to_string())),
                    ("content".into(), Value::Int(*content)),
                    ("sequence_number".into(), Value::Long(1)),
                ])
            })
            .collect(),
    )
}

/// Metadata naming `snapshots` (id, manifest list), the last one current.
pub fn metadata(table: &str, snapshots: &[(i64, &str)]) -> Vec<u8> {
    let current = snapshots.last().map(|(id, _)| *id).unwrap_or(-1);
    let snapshots: Vec<_> = snapshots
        .iter()
        .map(|(id, list)| serde_json::json!({ "snapshot-id": id, "manifest-list": list }))
        .collect();
    serde_json::to_vec(&serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000001",
        "location": uri(table),
        "current-snapshot-id": current,
        "snapshots": snapshots,
    }))
    .unwrap()
}

pub const ORDERS: &str = "warehouse/db/orders";

/// `warehouse/db/orders`, as a table that has been written twice. The current snapshot is three
/// data files of 30,000 rows each, in row groups of 10,000. The first snapshot's metadata,
/// manifest list, manifest and data file are still in the bucket, and so is a data file no
/// snapshot names. Returns every key stored, with its size.
pub fn put_orders(s3: &FakeS3) -> HashMap<String, usize> {
    let mut stored = HashMap::new();
    let mut put = |key: String, body: Vec<u8>| {
        stored.insert(key.clone(), body.len());
        s3.put(&key, body);
    };
    let data = |name: &str| format!("{ORDERS}/data/{name}");
    let meta = |name: &str| format!("{ORDERS}/metadata/{name}");

    // The first snapshot, superseded: one data file.
    let old = parquet(1_000_000..1_000_010, 10);
    let manifest_1 = avro(
        MANIFEST_SCHEMA,
        vec![entry(0, &uri(&data("old.parquet")), 10, old.len())],
    );
    put(data("old.parquet"), old);
    put(meta("manifest-1.avro"), manifest_1);
    put(
        meta("snap-1.avro"),
        manifest_list(&[(&uri(&meta("manifest-1.avro")), 0)]),
    );
    put(
        meta("v1.metadata.json"),
        metadata(ORDERS, &[(1, &uri(&meta("snap-1.avro")))]),
    );

    // The current snapshot: three files.
    let mut entries = Vec::new();
    for (i, name) in ["a.parquet", "b.parquet", "c.parquet"].iter().enumerate() {
        let first = i as i64 * 30_000;
        let body = parquet(first..first + 30_000, 10_000);
        entries.push(entry(0, &uri(&data(name)), 30_000, body.len()));
        put(data(name), body);
    }
    put(meta("manifest-2.avro"), avro(MANIFEST_SCHEMA, entries));
    put(
        meta("snap-2.avro"),
        manifest_list(&[(&uri(&meta("manifest-2.avro")), 0)]),
    );
    put(
        meta("v2.metadata.json"),
        metadata(
            ORDERS,
            &[
                (1, &uri(&meta("snap-1.avro"))),
                (2, &uri(&meta("snap-2.avro"))),
            ],
        ),
    );
    put(meta("version-hint.text"), b"2".to_vec());

    // In the bucket, in no snapshot.
    put(data("orphan.parquet"), parquet(5_000_000..5_001_000, 1_000));
    stored
}

pub fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
