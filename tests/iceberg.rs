//! End-to-end test for the self-contained Iceberg reader (`--features iceberg`).
//!
//! Synthesizes a minimal v2 Iceberg table on disk — a Parquet data file, an Avro manifest +
//! manifest-list pointing at it, and a `metadata.json` / `version-hint.text` — then reads it
//! back through the engine (detect → schema → preview → windowed scan). No external tools.
#![cfg(feature = "iceberg")]

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::{Codec, Schema, Writer};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;

use lakeleto::engine::{Engine, FilterOp, FilterSpec, ScanSpec};
use lakeleto::{Format, LocalReaderEngine, Source};

const MANIFEST_LIST_SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[
  {"name":"manifest_path","type":"string"},{"name":"content","type":"int"}]}"#;

const MANIFEST_SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"data_file","type":{"type":"record","name":"r129","fields":[
    {"name":"content","type":"int"},
    {"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"}]}}]}"#;

/// A manifest schema carrying the entry `sequence_number` and the data_file `equality_ids`
/// (used by the equality-delete test to exercise sequence-number semantics).
const MANIFEST_EQ_SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"sequence_number","type":["null","long"]},
  {"name":"data_file","type":{"type":"record","name":"req","fields":[
    {"name":"content","type":"int"},
    {"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"},
    {"name":"equality_ids","type":["null",{"type":"array","items":"int"}]}]}}]}"#;

fn write_avro(path: &Path, schema_json: &str, records: Vec<Value>) {
    write_avro_codec(path, schema_json, records, Codec::Null);
}

/// Write an Avro container file with an explicit codec (Snappy/Zstandard exercise the
/// compressed-manifest reader path).
fn write_avro_codec(path: &Path, schema_json: &str, records: Vec<Value>, codec: Codec) {
    let schema = Schema::parse_str(schema_json).unwrap();
    let mut w = Writer::with_codec(&schema, Vec::new(), codec);
    for r in records {
        w.append(r).unwrap();
    }
    fs::write(path, w.into_inner().unwrap()).unwrap();
}

/// Write a Parquet file with the two-column (`id` Int64, `name` Utf8) schema.
fn write_data_parquet(path: &Path, ids: Vec<i64>, names: Vec<&str>) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// An Arrow field carrying an Iceberg `PARQUET:field_id` (so ArrowWriter stamps the id and the
/// reader can match columns by id across schema evolution).
fn id_field(name: &str, dt: DataType, nullable: bool, id: i32) -> Field {
    Field::new(name, dt, nullable).with_metadata(HashMap::from([(
        "PARQUET:field_id".to_string(),
        id.to_string(),
    )]))
}

/// Write a Parquet file from an explicit (field-id'd) schema + columns.
fn write_parquet(path: &Path, fields: Vec<Field>, cols: Vec<ArrayRef>) {
    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let mut w = ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Write an Iceberg positional-delete Parquet (`file_path` Utf8, `pos` Int64).
fn write_positional_delete(path: &Path, referenced: &Path, positions: Vec<i64>) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("pos", DataType::Int64, false),
    ]));
    let n = positions.len();
    let paths = StringArray::from(vec![referenced.display().to_string(); n]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(paths) as ArrayRef,
            Arc::new(Int64Array::from(positions)) as ArrayRef,
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Build a minimal Iceberg table under `dir` with 3 rows; return the table dir.
fn build_table(dir: &Path) -> std::path::PathBuf {
    let tbl = dir.join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // data-1.parquet: id Int64, name Utf8
    let data_parquet = data.join("data-1.parquet");
    let id = Int64Array::from(vec![1_i64, 2, 3]);
    let name = StringArray::from(vec!["ada", "grace", "linus"]);
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id) as ArrayRef, Arc::new(name) as ArrayRef],
    )
    .unwrap();
    let f = fs::File::create(&data_parquet).unwrap();
    let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    // manifest-1.avro → the data file
    let manifest = meta.join("manifest-1.avro");
    write_avro(
        &manifest,
        MANIFEST_SCHEMA,
        vec![Value::Record(vec![
            ("status".into(), Value::Int(1)),
            (
                "data_file".into(),
                Value::Record(vec![
                    ("content".into(), Value::Int(0)),
                    (
                        "file_path".into(),
                        Value::String(data_parquet.display().to_string()),
                    ),
                    ("file_format".into(), Value::String("PARQUET".into())),
                ]),
            ),
        ])],
    );

    // snap-1.avro (manifest-list) → the manifest
    let snap = meta.join("snap-1.avro");
    write_avro(
        &snap,
        MANIFEST_LIST_SCHEMA,
        vec![Value::Record(vec![
            (
                "manifest_path".into(),
                Value::String(manifest.display().to_string()),
            ),
            ("content".into(), Value::Int(0)),
        ])],
    );

    // metadata.json + version-hint.text
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": tbl.display().to_string(),
        "current-snapshot-id": 1,
        "snapshots": [ { "snapshot-id": 1, "manifest-list": snap.display().to_string() } ]
    });
    fs::write(
        meta.join("v1.metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    fs::write(meta.join("version-hint.text"), "1").unwrap();

    tbl
}

#[test]
fn plan_with_root_confines_files_to_root() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_table(dir.path());
    let root = std::fs::canonicalize(&tbl).unwrap();

    // Root = the table dir → every file it reads (metadata, manifests, data) is inside → Ok.
    assert!(lakeleto::iceberg::plan_with_root(&tbl, Some(root.as_path())).is_ok());
    // No root → unconfined (the engine's own call path) → Ok.
    assert!(lakeleto::iceberg::plan_with_root(&tbl, None).is_ok());

    // Root = an unrelated dir → the table's files all sit outside it → refused before any read.
    let other = tempfile::tempdir().unwrap();
    let other_root = std::fs::canonicalize(other.path()).unwrap();
    let err = lakeleto::iceberg::plan_with_root(&tbl, Some(other_root.as_path())).unwrap_err();
    assert!(
        matches!(err, lakeleto::error::EngineError::Forbidden(_)),
        "expected Forbidden, got: {err:?}"
    );
}

#[test]
fn plan_with_root_preserves_positional_delete_matching() {
    // A table WITH positional deletes: under confinement the guard canonicalizes both the data
    // files and the delete-referenced paths, so deletes must still associate with their file (this
    // also exercises the positional-delete guard path, not just data files).
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_table_with_deletes(dir.path());
    let root = std::fs::canonicalize(&tbl).unwrap();

    // Per-file delete sets keyed by the *canonicalized* path, so the confined plan's canonical
    // keys line up with the unconfined plan's raw keys. Comparing the full maps (not just the
    // summed count) proves deletes land on the *right* file — a swap between files that kept the
    // same grand total would still fail here.
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    let deletes_by_path = |p: &lakeleto::iceberg::TablePlan| -> BTreeMap<PathBuf, BTreeSet<i64>> {
        p.files
            .iter()
            .map(|f| {
                let key = std::fs::canonicalize(&f.path).unwrap_or_else(|_| f.path.clone());
                (key, f.deletes.clone())
            })
            .collect()
    };
    let unconfined = deletes_by_path(&lakeleto::iceberg::plan(&tbl).unwrap());
    let confined =
        deletes_by_path(&lakeleto::iceberg::plan_with_root(&tbl, Some(root.as_path())).unwrap());

    assert!(
        unconfined.values().any(|d| !d.is_empty()),
        "fixture must carry positional deletes"
    );
    assert_eq!(
        confined, unconfined,
        "confinement (canonical keys) must not drop or misroute positional deletes"
    );
}

#[test]
fn reads_a_minimal_iceberg_table() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_table(dir.path());

    let source = Source::detect(&tbl).unwrap();
    assert_eq!(
        source.format,
        Format::Iceberg,
        "dir with metadata/ → Iceberg"
    );

    let engine = LocalReaderEngine::default();

    let schema = engine.schema(&source).unwrap();
    assert_eq!(schema.row_count, Some(3), "sum of data-file footers");
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["id", "name"]);

    let preview = engine.preview(&source, 2).unwrap();
    assert_eq!(preview.num_rows(), 2);
    let ids = preview.batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
}

#[test]
fn iceberg_windowed_scan_offsets() {
    use lakeleto::engine::ScanSpec;
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_table(dir.path());
    let source = Source::detect(&tbl).unwrap();

    let res = LocalReaderEngine::default()
        .scan(
            &source,
            &ScanSpec {
                offset: 1,
                limit: 5,
                sort: None,
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    assert_eq!(res.batch.num_rows(), 2, "3 rows, offset 1 → 2 remain");
    let ids = res.batch.batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 2, "offset 1 skips id=1");
}

/// Build a v2 table with a merge-on-read **positional delete** file, with all Avro manifests
/// **snappy-compressed** (so this also covers the compressed-manifest reader path). 4 rows;
/// physical position 1 (id=2, "grace") is deleted → 3 live rows [1, 3, 4].
fn build_table_with_deletes(dir: &Path) -> std::path::PathBuf {
    let tbl = dir.join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    let data_parquet = data.join("data-1.parquet");
    write_data_parquet(
        &data_parquet,
        vec![1, 2, 3, 4],
        vec!["ada", "grace", "linus", "ken"],
    );

    // positional delete file: delete physical position 1 of data-1.parquet.
    let delete_parquet = data.join("delete-1.parquet");
    write_positional_delete(&delete_parquet, &data_parquet, vec![1]);

    // data manifest → the data file (content 0); snappy-compressed.
    let manifest = meta.join("manifest-1.avro");
    write_avro_codec(
        &manifest,
        MANIFEST_SCHEMA,
        vec![Value::Record(vec![
            ("status".into(), Value::Int(1)),
            (
                "data_file".into(),
                Value::Record(vec![
                    ("content".into(), Value::Int(0)),
                    (
                        "file_path".into(),
                        Value::String(data_parquet.display().to_string()),
                    ),
                    ("file_format".into(), Value::String("PARQUET".into())),
                ]),
            ),
        ])],
        Codec::Snappy,
    );

    // delete manifest → the positional-delete file (content 1); snappy-compressed.
    let delete_manifest = meta.join("delete-manifest-1.avro");
    write_avro_codec(
        &delete_manifest,
        MANIFEST_SCHEMA,
        vec![Value::Record(vec![
            ("status".into(), Value::Int(1)),
            (
                "data_file".into(),
                Value::Record(vec![
                    ("content".into(), Value::Int(1)),
                    (
                        "file_path".into(),
                        Value::String(delete_parquet.display().to_string()),
                    ),
                    ("file_format".into(), Value::String("PARQUET".into())),
                ]),
            ),
        ])],
        Codec::Snappy,
    );

    // manifest-list → both manifests, tagged data(0) / deletes(1); snappy-compressed.
    let snap = meta.join("snap-1.avro");
    write_avro_codec(
        &snap,
        MANIFEST_LIST_SCHEMA,
        vec![
            Value::Record(vec![
                (
                    "manifest_path".into(),
                    Value::String(manifest.display().to_string()),
                ),
                ("content".into(), Value::Int(0)),
            ]),
            Value::Record(vec![
                (
                    "manifest_path".into(),
                    Value::String(delete_manifest.display().to_string()),
                ),
                ("content".into(), Value::Int(1)),
            ]),
        ],
        Codec::Snappy,
    );

    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": tbl.display().to_string(),
        "current-snapshot-id": 1,
        "snapshots": [ { "snapshot-id": 1, "manifest-list": snap.display().to_string() } ]
    });
    fs::write(
        meta.join("v1.metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    fs::write(meta.join("version-hint.text"), "1").unwrap();
    tbl
}

#[test]
fn applies_positional_deletes_over_compressed_manifests() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_table_with_deletes(dir.path());
    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();

    // row count reflects the delete: 4 physical − 1 deleted = 3 live.
    let schema = engine.schema(&source).unwrap();
    assert_eq!(schema.row_count, Some(3), "4 rows − 1 positional delete");

    // preview skips the deleted physical position 1 (id=2 "grace").
    let preview = engine.preview(&source, 10).unwrap();
    let ids: Vec<i64> = preview
        .batches
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
        .collect();
    assert_eq!(ids, vec![1, 3, 4], "grace (pos 1) is deleted");
}

/// Build a table whose two data files were written under **different schemas**, matched by
/// field-id to the current schema `{1:id long, 2:name string, 3:score int}`. File A (older) has
/// field 1 physically `Int32` named `xid` and field 2 named `full_name`, with no `score` column
/// (added later); file B (current) has `id` Int64, `name` Utf8, `score` Int32. Exercises type
/// promotion (int→long), rename (by id), and add-column (null backfill).
fn build_evolved_table(dir: &Path) -> std::path::PathBuf {
    let tbl = dir.join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // file A — old schema: xid Int32 (id 1), full_name Utf8 (id 2). 2 rows.
    let file_a = data.join("data-a.parquet");
    write_parquet(
        &file_a,
        vec![
            id_field("xid", DataType::Int32, false, 1),
            id_field("full_name", DataType::Utf8, false, 2),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ada", "grace"])) as ArrayRef,
        ],
    );

    // file B — current schema: id Int64 (1), name Utf8 (2), score Int32 (3). 1 row.
    let file_b = data.join("data-b.parquet");
    write_parquet(
        &file_b,
        vec![
            id_field("id", DataType::Int64, false, 1),
            id_field("name", DataType::Utf8, false, 2),
            id_field("score", DataType::Int32, true, 3),
        ],
        vec![
            Arc::new(Int64Array::from(vec![3_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["linus"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        ],
    );

    // one data manifest with both files.
    let manifest = meta.join("manifest-1.avro");
    let entry = |p: &Path| {
        Value::Record(vec![
            ("status".into(), Value::Int(1)),
            (
                "data_file".into(),
                Value::Record(vec![
                    ("content".into(), Value::Int(0)),
                    ("file_path".into(), Value::String(p.display().to_string())),
                    ("file_format".into(), Value::String("PARQUET".into())),
                ]),
            ),
        ])
    };
    write_avro(
        &manifest,
        MANIFEST_SCHEMA,
        vec![entry(&file_a), entry(&file_b)],
    );

    let snap = meta.join("snap-1.avro");
    write_avro(
        &snap,
        MANIFEST_LIST_SCHEMA,
        vec![Value::Record(vec![
            (
                "manifest_path".into(),
                Value::String(manifest.display().to_string()),
            ),
            ("content".into(), Value::Int(0)),
        ])],
    );

    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": tbl.display().to_string(),
        "current-snapshot-id": 1,
        "current-schema-id": 0,
        "schemas": [ { "schema-id": 0, "type": "struct", "fields": [
            {"id": 1, "name": "id",    "required": true,  "type": "long"},
            {"id": 2, "name": "name",  "required": false, "type": "string"},
            {"id": 3, "name": "score", "required": false, "type": "int"}
        ] } ],
        "snapshots": [ { "snapshot-id": 1, "manifest-list": snap.display().to_string() } ]
    });
    fs::write(
        meta.join("v1.metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    fs::write(meta.join("version-hint.text"), "1").unwrap();
    tbl
}

/// A manifest entry with an explicit sequence number and optional `equality_ids`.
fn eq_entry(path: &Path, content: i32, seq: i64, equality_ids: Option<Vec<i32>>) -> Value {
    let eq_val = match equality_ids {
        Some(ids) => Value::Union(
            1,
            Box::new(Value::Array(ids.into_iter().map(Value::Int).collect())),
        ),
        None => Value::Union(0, Box::new(Value::Null)),
    };
    Value::Record(vec![
        ("status".into(), Value::Int(1)),
        (
            "sequence_number".into(),
            Value::Union(1, Box::new(Value::Long(seq))),
        ),
        (
            "data_file".into(),
            Value::Record(vec![
                ("content".into(), Value::Int(content)),
                (
                    "file_path".into(),
                    Value::String(path.display().to_string()),
                ),
                ("file_format".into(), Value::String("PARQUET".into())),
                ("equality_ids".into(), eq_val),
            ]),
        ),
    ])
}

/// Build a v2 table exercising **equality deletes** with sequence-number semantics: file A
/// (seq 1) has ids [1,2,3]; an equality-delete file (seq 2) deletes id=2; file B (seq 3)
/// re-inserts id=2. The delete applies to A (seq 1 < 2) but NOT to B (seq 3 > 2), so the
/// re-inserted row survives → live ids [1, 3, 2].
fn build_equality_delete_table(dir: &Path) -> std::path::PathBuf {
    let tbl = dir.join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    let file_a = data.join("data-a.parquet");
    write_parquet(
        &file_a,
        vec![
            id_field("id", DataType::Int64, false, 1),
            id_field("name", DataType::Utf8, false, 2),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    );
    let file_b = data.join("data-b.parquet");
    write_parquet(
        &file_b,
        vec![
            id_field("id", DataType::Int64, false, 1),
            id_field("name", DataType::Utf8, false, 2),
        ],
        vec![
            Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["b2"])) as ArrayRef,
        ],
    );
    // equality-delete file: delete rows where id (field-id 1) == 2.
    let eq_delete = data.join("eq-delete-1.parquet");
    write_parquet(
        &eq_delete,
        vec![id_field("id", DataType::Int64, false, 1)],
        vec![Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef],
    );

    // data manifest → files A (seq 1) and B (seq 3).
    let manifest = meta.join("manifest-1.avro");
    write_avro(
        &manifest,
        MANIFEST_EQ_SCHEMA,
        vec![eq_entry(&file_a, 0, 1, None), eq_entry(&file_b, 0, 3, None)],
    );
    // delete manifest → the equality-delete file (seq 2, equality_ids [1]).
    let delete_manifest = meta.join("delete-manifest-1.avro");
    write_avro(
        &delete_manifest,
        MANIFEST_EQ_SCHEMA,
        vec![eq_entry(&eq_delete, 2, 2, Some(vec![1]))],
    );
    let snap = meta.join("snap-1.avro");
    write_avro(
        &snap,
        MANIFEST_LIST_SCHEMA,
        vec![
            Value::Record(vec![
                (
                    "manifest_path".into(),
                    Value::String(manifest.display().to_string()),
                ),
                ("content".into(), Value::Int(0)),
            ]),
            Value::Record(vec![
                (
                    "manifest_path".into(),
                    Value::String(delete_manifest.display().to_string()),
                ),
                ("content".into(), Value::Int(1)),
            ]),
        ],
    );

    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": tbl.display().to_string(),
        "current-snapshot-id": 1,
        "current-schema-id": 0,
        "schemas": [ { "schema-id": 0, "type": "struct", "fields": [
            {"id": 1, "name": "id",   "required": true,  "type": "long"},
            {"id": 2, "name": "name", "required": false, "type": "string"}
        ] } ],
        "snapshots": [ { "snapshot-id": 1, "manifest-list": snap.display().to_string() } ]
    });
    fs::write(
        meta.join("v1.metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    fs::write(meta.join("version-hint.text"), "1").unwrap();
    tbl
}

#[test]
fn applies_equality_deletes_respecting_sequence_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_equality_delete_table(dir.path());
    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();

    // count is unknown (equality deletes remove by value, not cheaply countable).
    let schema = engine.schema(&source).unwrap();
    assert_eq!(schema.row_count, None);

    let preview = engine.preview(&source, 10).unwrap();
    let ids: Vec<i64> = preview
        .batches
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
        .collect();
    // id=2 deleted from file A (seq 1 < 2) but the re-inserted id=2 in file B (seq 3 > 2) lives.
    assert_eq!(
        ids,
        vec![1, 3, 2],
        "sequence-number semantics: re-insert survives"
    );
}

#[test]
fn unifies_evolved_schemas_by_field_id() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_evolved_table(dir.path());
    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();

    // schema reflects the current table schema (id long, name string, score int).
    let schema = engine.schema(&source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["id", "name", "score"]);
    assert_eq!(schema.row_count, Some(3));

    let preview = engine.preview(&source, 10).unwrap();
    // id: file A promoted Int32→Int64, file B native Int64.
    let ids: Vec<i64> = preview
        .batches
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
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "field-id 1 promoted + concatenated");

    // score: null for the older file (added later), 42 for the current file.
    let scores: Vec<Option<i32>> = preview
        .batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..a.len())
                .map(|i| a.is_valid(i).then(|| a.value(i)))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        scores,
        vec![None, None, Some(42)],
        "added column null-filled"
    );

    // name: renamed from `full_name` in file A, matched by field-id 2.
    let names: Vec<String> = preview
        .batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(names, vec!["ada", "grace", "linus"], "rename matched by id");
}

#[test]
fn positional_delete_windowed_scan_maps_logical_rows() {
    use lakeleto::engine::ScanSpec;
    let dir = tempfile::tempdir().unwrap();
    let tbl = build_table_with_deletes(dir.path());
    let source = Source::detect(&tbl).unwrap();

    // Logical rows after delete are [1, 3, 4]; offset 1 → [3, 4].
    let res = LocalReaderEngine::default()
        .scan(
            &source,
            &ScanSpec {
                offset: 1,
                limit: 5,
                sort: None,
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    let ids: Vec<i64> = res
        .batch
        .batches
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
        .collect();
    assert_eq!(ids, vec![3, 4], "offset 1 over logical rows [1,3,4]");
}

// ---- statistics / partition pruning ---------------------------------------------------

/// A manifest schema carrying per-file statistics (record_count + int-keyed maps for null/nan
/// counts and lower/upper bounds), used to test data-file skipping.
const MANIFEST_STATS_SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"data_file","type":{"type":"record","name":"dfs","fields":[
    {"name":"content","type":"int"},
    {"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"},
    {"name":"record_count","type":"long"},
    {"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"nvc","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}]},
    {"name":"nan_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"nnc","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}]},
    {"name":"lower_bounds","type":["null",{"type":"array","items":{"type":"record","name":"lbr","fields":[{"name":"key","type":"int"},{"name":"value","type":"bytes"}]}}]},
    {"name":"upper_bounds","type":["null",{"type":"array","items":{"type":"record","name":"ubr","fields":[{"name":"key","type":"int"},{"name":"value","type":"bytes"}]}}]}
  ]}}]}"#;

fn le64(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

/// An Iceberg int-keyed long map (`null_value_counts` / `nan_value_counts`) as an Avro union value.
fn long_map(entries: &[(i32, i64)]) -> Value {
    Value::Union(
        1,
        Box::new(Value::Array(
            entries
                .iter()
                .map(|(k, v)| {
                    Value::Record(vec![
                        ("key".into(), Value::Int(*k)),
                        ("value".into(), Value::Long(*v)),
                    ])
                })
                .collect(),
        )),
    )
}

/// An Iceberg int-keyed bytes map (`lower_bounds` / `upper_bounds`) as an Avro union value.
fn bytes_map(entries: &[(i32, Vec<u8>)]) -> Value {
    Value::Union(
        1,
        Box::new(Value::Array(
            entries
                .iter()
                .map(|(k, v)| {
                    Value::Record(vec![
                        ("key".into(), Value::Int(*k)),
                        ("value".into(), Value::Bytes(v.clone())),
                    ])
                })
                .collect(),
        )),
    )
}

/// A data-file manifest entry carrying full statistics.
#[allow(clippy::too_many_arguments)]
fn stats_entry(
    path: &Path,
    record_count: i64,
    nulls: &[(i32, i64)],
    nans: &[(i32, i64)],
    lowers: &[(i32, Vec<u8>)],
    uppers: &[(i32, Vec<u8>)],
) -> Value {
    Value::Record(vec![
        ("status".into(), Value::Int(1)),
        (
            "data_file".into(),
            Value::Record(vec![
                ("content".into(), Value::Int(0)),
                (
                    "file_path".into(),
                    Value::String(path.display().to_string()),
                ),
                ("file_format".into(), Value::String("PARQUET".into())),
                ("record_count".into(), Value::Long(record_count)),
                ("null_value_counts".into(), long_map(nulls)),
                ("nan_value_counts".into(), long_map(nans)),
                ("lower_bounds".into(), bytes_map(lowers)),
                ("upper_bounds".into(), bytes_map(uppers)),
            ]),
        ),
    ])
}

/// Write the manifest/manifest-list/metadata for a single-manifest table with the given entries
/// and current schema fields.
fn finalize_table(tbl: &Path, meta: &Path, entries: Vec<Value>, schema_fields: serde_json::Value) {
    let manifest = meta.join("manifest-1.avro");
    write_avro(&manifest, MANIFEST_STATS_SCHEMA, entries);
    let snap = meta.join("snap-1.avro");
    write_avro(
        &snap,
        MANIFEST_LIST_SCHEMA,
        vec![Value::Record(vec![
            (
                "manifest_path".into(),
                Value::String(manifest.display().to_string()),
            ),
            ("content".into(), Value::Int(0)),
        ])],
    );
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": tbl.display().to_string(),
        "current-snapshot-id": 1,
        "current-schema-id": 0,
        "schemas": [ { "schema-id": 0, "type": "struct", "fields": schema_fields } ],
        "snapshots": [ { "snapshot-id": 1, "manifest-list": snap.display().to_string() } ]
    });
    fs::write(
        meta.join("v1.metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    fs::write(meta.join("version-hint.text"), "1").unwrap();
}

fn filter(col: &str, op: FilterOp, value: &str) -> ScanSpec {
    ScanSpec {
        offset: 0,
        limit: 1000,
        sort: None,
        filters: vec![FilterSpec {
            column: col.to_string(),
            op,
            value: value.to_string(),
        }],
        projection: None,
    }
}

#[test]
fn prunes_data_files_by_column_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = dir.path().join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // Three files over `x long` (field-id 1): [0..10], [100..110], [200..210].
    let mk = |name: &str, lo: i64, hi: i64| -> (std::path::PathBuf, i64, i64) {
        let p = data.join(name);
        let ids: Vec<i64> = (lo..=hi).collect();
        write_parquet(
            &p,
            vec![id_field("x", DataType::Int64, false, 1)],
            vec![Arc::new(Int64Array::from(ids)) as ArrayRef],
        );
        (p, lo, hi)
    };
    let a = mk("a.parquet", 0, 10);
    let b = mk("b.parquet", 100, 110);
    let c = mk("c.parquet", 200, 210);
    let entry = |f: &(std::path::PathBuf, i64, i64)| {
        stats_entry(
            &f.0,
            f.2 - f.1 + 1,
            &[(1, 0)],
            &[],
            &[(1, le64(f.1))],
            &[(1, le64(f.2))],
        )
    };
    finalize_table(
        &tbl,
        &meta,
        vec![entry(&a), entry(&b), entry(&c)],
        serde_json::json!([{"id":1,"name":"x","required":true,"type":"long"}]),
    );

    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();

    // x > 150 → only file C (200..210) can match; A and B are skipped by their upper bounds.
    let res = engine
        .scan(&source, &filter("x", FilterOp::Gt, "150"))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 11,
        "only file C read; A,B pruned by bounds"
    );
    assert_eq!(res.matched_rows, 11);
    assert!(res.total_known, "read all survivors → exact");

    // x = 105 → only file B ([100..110]) survives.
    let res = engine
        .scan(&source, &filter("x", FilterOp::Eq, "105"))
        .unwrap();
    assert_eq!(res.scanned_rows, 11, "only file B read");
    assert_eq!(res.matched_rows, 1);

    // x > 1000 → every file pruned → empty, exact, no error.
    let res = engine
        .scan(&source, &filter("x", FilterOp::Gt, "1000"))
        .unwrap();
    assert_eq!(res.scanned_rows, 0);
    assert_eq!(res.matched_rows, 0);
    assert_eq!(res.batch.num_rows(), 0);
    assert!(res.total_known);
}

#[test]
fn nan_disables_unsafe_float_pruning_but_allows_it_when_nan_free() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = dir.path().join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // File N: y = [5.0, NaN] (field-id 1). Iceberg excludes NaN from bounds → L=U=5.0, nan=1.
    let fnan = data.join("n.parquet");
    write_parquet(
        &fnan,
        vec![id_field("y", DataType::Float64, false, 1)],
        vec![Arc::new(Float64Array::from(vec![5.0_f64, f64::NAN])) as ArrayRef],
    );
    // File Z: y = [1.0, 2.0], nan-free.
    let fz = data.join("z.parquet");
    write_parquet(
        &fz,
        vec![id_field("y", DataType::Float64, false, 1)],
        vec![Arc::new(Float64Array::from(vec![1.0_f64, 2.0])) as ArrayRef],
    );

    // Two separate single-file tables so each filter targets one file's stats cleanly.
    let build = |tbl: &Path, meta: &Path, entry: Value| {
        finalize_table(
            tbl,
            meta,
            vec![entry],
            serde_json::json!([{"id":1,"name":"y","required":false,"type":"double"}]),
        );
    };
    build(
        &tbl,
        &meta,
        stats_entry(
            &fnan,
            2,
            &[(1, 0)],
            &[(1, 1)], // one NaN
            &[(1, 5.0_f64.to_le_bytes().to_vec())],
            &[(1, 5.0_f64.to_le_bytes().to_vec())],
        ),
    );
    let src_nan = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();

    // y != 5 : the NaN row matches (NaN != 5 is true) — the file must NOT be pruned.
    let res = engine
        .scan(&src_nan, &filter("y", FilterOp::Ne, "5"))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 2,
        "NaN present → Ne pruning disabled, file read"
    );
    assert_eq!(res.matched_rows, 1, "NaN row matches != 5");

    // y > 100 : under Arrow total order NaN is the max, so NaN matches — must NOT be pruned.
    let res = engine
        .scan(&src_nan, &filter("y", FilterOp::Gt, "100"))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 2,
        "NaN present → Gt pruning disabled, file read"
    );
    assert_eq!(res.matched_rows, 1, "NaN matches > 100 under total order");

    // Nan-free file: the same Gt filter now prunes cleanly.
    let tblz = dir.path().join("tblz");
    let metaz = tblz.join("metadata");
    fs::create_dir_all(&metaz).unwrap();
    build(
        &tblz,
        &metaz,
        stats_entry(
            &fz,
            2,
            &[(1, 0)],
            &[(1, 0)], // nan-free → pruning enabled
            &[(1, 1.0_f64.to_le_bytes().to_vec())],
            &[(1, 2.0_f64.to_le_bytes().to_vec())],
        ),
    );
    let src_z = Source::detect(&tblz).unwrap();
    let res = engine
        .scan(&src_z, &filter("y", FilterOp::Gt, "100"))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 0,
        "nan-free + upper bound 2.0 < 100 → pruned"
    );
    assert_eq!(res.matched_rows, 0);
}

#[test]
fn prunes_all_null_files() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = dir.path().join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // File all-null on x, and a file with a real value.
    let fnull = data.join("null.parquet");
    write_parquet(
        &fnull,
        vec![id_field("x", DataType::Int64, true, 1)],
        vec![Arc::new(Int64Array::from(vec![None::<i64>, None, None])) as ArrayRef],
    );
    let fval = data.join("val.parquet");
    write_parquet(
        &fval,
        vec![id_field("x", DataType::Int64, true, 1)],
        vec![Arc::new(Int64Array::from(vec![Some(5_i64)])) as ArrayRef],
    );
    finalize_table(
        &tbl,
        &meta,
        vec![
            // all-null file: null_count == record_count, no bounds
            stats_entry(&fnull, 3, &[(1, 3)], &[], &[], &[]),
            stats_entry(&fval, 1, &[(1, 0)], &[], &[(1, le64(5))], &[(1, le64(5))]),
        ],
        serde_json::json!([{"id":1,"name":"x","required":false,"type":"long"}]),
    );

    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();
    // x = 5 : the all-null file can't match any op → skipped; only the value file is read.
    let res = engine
        .scan(&source, &filter("x", FilterOp::Eq, "5"))
        .unwrap();
    assert_eq!(res.scanned_rows, 1, "all-null file skipped");
    assert_eq!(res.matched_rows, 1);
}

/// Iceberg decimal bound: unscaled value as two's-complement big-endian (full 16 bytes here; the
/// reader also accepts the minimal-length form real writers emit).
fn dec_be(unscaled: i128) -> Vec<u8> {
    unscaled.to_be_bytes().to_vec()
}

#[test]
fn prunes_decimal_columns() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = dir.path().join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // price decimal(10,2), field-id 1. Unscaled: file A = [1.00, 50.00], file B = [100.00, 200.00].
    let mk = |name: &str, vals: Vec<i128>| -> std::path::PathBuf {
        let p = data.join(name);
        let arr = arrow_array::Decimal128Array::from(vals)
            .with_precision_and_scale(10, 2)
            .unwrap();
        write_parquet(
            &p,
            vec![id_field("price", DataType::Decimal128(10, 2), false, 1)],
            vec![Arc::new(arr) as ArrayRef],
        );
        p
    };
    let fa = mk("a.parquet", vec![100, 5000]);
    let fb = mk("b.parquet", vec![10000, 20000]);
    finalize_table(
        &tbl,
        &meta,
        vec![
            stats_entry(
                &fa,
                2,
                &[(1, 0)],
                &[],
                &[(1, dec_be(100))],
                &[(1, dec_be(5000))],
            ),
            stats_entry(
                &fb,
                2,
                &[(1, 0)],
                &[],
                &[(1, dec_be(10000))],
                &[(1, dec_be(20000))],
            ),
        ],
        serde_json::json!([{"id":1,"name":"price","required":true,"type":"decimal(10, 2)"}]),
    );

    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();
    // price > 75 → file A (max 50.00) pruned, file B kept.
    let res = engine
        .scan(&source, &filter("price", FilterOp::Gt, "75"))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 2,
        "only file B read; A pruned by decimal bound"
    );
    assert_eq!(res.matched_rows, 2);
}

#[test]
fn prunes_date_columns() {
    // Compute an ISO date string from a day number exactly as the reader will (same arrow cast).
    let day_str = |d: i32| -> String {
        let a = arrow_array::Date32Array::from(vec![d]);
        let s = arrow_cast::cast(&a, &DataType::Utf8).unwrap();
        s.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };

    let dir = tempfile::tempdir().unwrap();
    let tbl = dir.path().join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // d date, field-id 1. File A = days [100, 200], file B = days [1000, 1100].
    let mk = |name: &str, days: Vec<i32>| -> std::path::PathBuf {
        let p = data.join(name);
        write_parquet(
            &p,
            vec![id_field("d", DataType::Date32, false, 1)],
            vec![Arc::new(arrow_array::Date32Array::from(days)) as ArrayRef],
        );
        p
    };
    let fa = mk("a.parquet", vec![100, 200]);
    let fb = mk("b.parquet", vec![1000, 1100]);
    let le32 = |d: i32| d.to_le_bytes().to_vec();
    finalize_table(
        &tbl,
        &meta,
        vec![
            stats_entry(&fa, 2, &[(1, 0)], &[], &[(1, le32(100))], &[(1, le32(200))]),
            stats_entry(
                &fb,
                2,
                &[(1, 0)],
                &[],
                &[(1, le32(1000))],
                &[(1, le32(1100))],
            ),
        ],
        serde_json::json!([{"id":1,"name":"d","required":true,"type":"date"}]),
    );

    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();
    // d >= <day 500> → file A (max day 200) pruned lexically, file B kept.
    let res = engine
        .scan(&source, &filter("d", FilterOp::Ge, &day_str(500)))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 2,
        "only file B read; A pruned by date bound"
    );
    assert_eq!(res.matched_rows, 2);
}

/// Write a Parquet file split into row groups of `rg_size` rows (so row-group skipping has
/// something to skip). ArrowWriter writes per-row-group column statistics by default.
fn write_parquet_rg(path: &Path, fields: Vec<Field>, cols: Vec<ArrayRef>, rg_size: usize) {
    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(rg_size))
        .build();
    let mut w = ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[test]
fn skips_row_groups_within_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = dir.path().join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // One file, three row groups of 100 rows: x in [0..99], [1000..1099], [2000..2099].
    let p = data.join("multi.parquet");
    let ids: Vec<i64> = (0..100).chain(1000..1100).chain(2000..2100).collect();
    write_parquet_rg(
        &p,
        vec![id_field("x", DataType::Int64, false, 1)],
        vec![Arc::new(Int64Array::from(ids)) as ArrayRef],
        100,
    );
    // No manifest bounds → file-level pruning keeps the file; row-group skipping does the work.
    finalize_table(
        &tbl,
        &meta,
        vec![stats_entry(&p, 300, &[(1, 0)], &[], &[], &[])],
        serde_json::json!([{"id":1,"name":"x","required":true,"type":"long"}]),
    );

    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();
    // x > 1500 → only the third row group ([2000..2099]) can match.
    let res = engine
        .scan(&source, &filter("x", FilterOp::Gt, "1500"))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 100,
        "read only the one matching row group of 300"
    );
    assert_eq!(res.matched_rows, 100);

    // A filter matching everything reads all three row groups (no skipping).
    let res = engine
        .scan(&source, &filter("x", FilterOp::Ge, "0"))
        .unwrap();
    assert_eq!(res.scanned_rows, 300, "no row group excluded");
}

/// A manifest schema carrying the data_file `partition` struct (a single `id_bucket` int), for
/// the transform-partition pruning test.
const MANIFEST_PART_SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
  {"name":"status","type":"int"},
  {"name":"data_file","type":{"type":"record","name":"dfp","fields":[
    {"name":"content","type":"int"},
    {"name":"file_path","type":"string"},
    {"name":"file_format","type":"string"},
    {"name":"partition","type":{"type":"record","name":"pt","fields":[{"name":"id_bucket","type":"int"}]}}]}}]}"#;

#[test]
fn prunes_by_bucket_partition() {
    let dir = tempfile::tempdir().unwrap();
    let tbl = dir.path().join("tbl");
    let meta = tbl.join("metadata");
    let data = tbl.join("data");
    fs::create_dir_all(&meta).unwrap();
    fs::create_dir_all(&data).unwrap();

    // Table partitioned by bucket[16] on `id`. File A declares bucket 3 (34 hashes to bucket 3);
    // file B declares bucket 5. No column bounds → only partition pruning can skip a file.
    let mk = |name: &str, ids: Vec<i64>| -> std::path::PathBuf {
        let p = data.join(name);
        write_parquet(
            &p,
            vec![id_field("id", DataType::Int64, false, 1)],
            vec![Arc::new(Int64Array::from(ids)) as ArrayRef],
        );
        p
    };
    let fa = mk("a.parquet", vec![34]);
    let fb = mk("b.parquet", vec![99, 100]);
    let part_entry = |p: &std::path::Path, bucket: i32| {
        Value::Record(vec![
            ("status".into(), Value::Int(1)),
            (
                "data_file".into(),
                Value::Record(vec![
                    ("content".into(), Value::Int(0)),
                    ("file_path".into(), Value::String(p.display().to_string())),
                    ("file_format".into(), Value::String("PARQUET".into())),
                    (
                        "partition".into(),
                        Value::Record(vec![("id_bucket".into(), Value::Int(bucket))]),
                    ),
                ]),
            ),
        ])
    };
    let manifest = meta.join("manifest-1.avro");
    write_avro(
        &manifest,
        MANIFEST_PART_SCHEMA,
        vec![part_entry(&fa, 3), part_entry(&fb, 5)],
    );
    let snap = meta.join("snap-1.avro");
    write_avro(
        &snap,
        MANIFEST_LIST_SCHEMA,
        vec![Value::Record(vec![
            (
                "manifest_path".into(),
                Value::String(manifest.display().to_string()),
            ),
            ("content".into(), Value::Int(0)),
        ])],
    );
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": tbl.display().to_string(),
        "current-snapshot-id": 1,
        "current-schema-id": 0,
        "schemas": [ { "schema-id": 0, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"}
        ] } ],
        "default-spec-id": 0,
        "partition-specs": [ { "spec-id": 0, "fields": [
            {"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[16]"}
        ] } ],
        "snapshots": [ { "snapshot-id": 1, "manifest-list": snap.display().to_string() } ]
    });
    fs::write(
        meta.join("v1.metadata.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
    fs::write(meta.join("version-hint.text"), "1").unwrap();

    let source = Source::detect(&tbl).unwrap();
    let engine = LocalReaderEngine::default();

    // id = 34 → bucket 3 → only file A (bucket 3) can match; file B (bucket 5) is pruned.
    let res = engine
        .scan(&source, &filter("id", FilterOp::Eq, "34"))
        .unwrap();
    assert_eq!(
        res.scanned_rows, 1,
        "only file A read; file B pruned by bucket"
    );
    assert_eq!(res.matched_rows, 1);

    // A range filter can't prune across a bucket transform → both files read (3 rows).
    let res = engine
        .scan(&source, &filter("id", FilterOp::Gt, "0"))
        .unwrap();
    assert_eq!(res.scanned_rows, 3, "range op: no bucket pruning");
}
