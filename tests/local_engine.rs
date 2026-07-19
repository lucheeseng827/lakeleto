//! End-to-end tests for the default `local` engine: detect → schema → preview → profile,
//! plus the JSON output path. Fixtures are synthesized into a tempdir so the tests are
//! hermetic (no committed binary parquet).

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use lakeleto::engine::Engine;
use lakeleto::render::{rows, Output};
use lakeleto::{Format, LocalReaderEngine, Source};

fn sample_batch() -> RecordBatch {
    let id = Int64Array::from(vec![1, 2, 3, 4]);
    let name = StringArray::from(vec![Some("Ada"), Some("Grace"), None, Some("Alan")]);
    let score = Float64Array::from(vec![Some(91.5), Some(88.0), None, Some(79.25)]);
    let active = BooleanArray::from(vec![true, false, true, true]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id) as ArrayRef,
            Arc::new(name) as ArrayRef,
            Arc::new(score) as ArrayRef,
            Arc::new(active) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write_parquet(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("people.parquet");
    let batch = sample_batch();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

#[test]
fn parquet_schema_preview_profile() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path());

    let source = Source::detect(&path).unwrap();
    assert_eq!(source.format, Format::Parquet);

    let engine = LocalReaderEngine::default();

    let schema = engine.schema(&source).unwrap();
    assert_eq!(schema.row_count, Some(4));
    assert_eq!(schema.columns.len(), 4);
    assert_eq!(schema.columns[0].name, "id");
    assert!(!schema.columns[0].nullable);
    assert!(schema.columns[1].nullable);

    let preview = engine.preview(&source, 2).unwrap();
    assert_eq!(preview.num_rows(), 2);

    let profile = engine.profile(&source, 10_000).unwrap();
    assert_eq!(profile.scanned_rows, 4);
    let name = profile.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name.null_count, 1);
    let id = profile.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id.min.as_deref(), Some("1"));
    assert_eq!(id.max.as_deref(), Some("4"));
    assert_eq!(id.distinct, 4);
}

#[test]
fn footer_profile_matches_scan_without_scanning() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path());
    let source = Source::detect(&path).unwrap();
    let engine = LocalReaderEngine::default();

    // Footer path (scan_limit 0): no rows scanned, but exact whole-file stats.
    let footer = engine.profile(&source, 0).unwrap();
    assert_eq!(footer.scanned_rows, 0, "footer-derived: no scan");
    assert_eq!(footer.row_count, Some(4));

    let scan = engine.profile(&source, 10_000).unwrap();
    for col in ["id", "name", "score"] {
        let f = footer.columns.iter().find(|c| c.name == col).unwrap();
        let s = scan.columns.iter().find(|c| c.name == col).unwrap();
        // Null counts and min/max are the exact whole-file values — identical to the scan.
        assert_eq!(f.null_count, s.null_count, "{col} null_count");
        assert_eq!(f.min, s.min, "{col} min");
        assert_eq!(f.max, s.max, "{col} max");
        // Distinct + samples aren't computed from the footer.
        assert_eq!(f.distinct, 0, "{col} distinct not computed");
        assert!(f.sample.is_empty(), "{col} no samples");
    }
    // Spot-check the exact values.
    let id = footer.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(
        (id.min.as_deref(), id.max.as_deref()),
        (Some("1"), Some("4"))
    );
    let name = footer.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name.null_count, 1);
}

#[test]
fn csv_detect_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    std::fs::write(&path, "id,name,score\n1,Ada,91.5\n2,Grace,\n3,Linus,88.0\n").unwrap();

    let source = Source::detect(&path).unwrap();
    assert_eq!(source.format, Format::Csv);

    let engine = LocalReaderEngine::default();
    let schema = engine.schema(&source).unwrap();
    assert_eq!(schema.columns.len(), 3);

    let preview = engine.preview(&source, 10).unwrap();
    assert_eq!(preview.num_rows(), 3);

    let profile = engine.profile(&source, 10_000).unwrap();
    let score = profile.columns.iter().find(|c| c.name == "score").unwrap();
    assert_eq!(score.null_count, 1);
}

#[test]
fn tsv_detect_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.tsv");
    // Tab-delimited: must split into three columns, not one comma-delimited blob.
    std::fs::write(&path, "id\tname\tscore\n1\tAda\t91.5\n2\tGrace\t\n").unwrap();

    let source = Source::detect(&path).unwrap();
    assert_eq!(source.format, Format::Tsv, ".tsv extension → Tsv");

    let engine = LocalReaderEngine::default();
    let schema = engine.schema(&source).unwrap();
    assert_eq!(
        schema.columns.len(),
        3,
        "tab-delimited columns must split correctly"
    );

    let preview = engine.preview(&source, 10).unwrap();
    assert_eq!(preview.num_rows(), 2);
    let names: Vec<Option<String>> = strs(&preview, "name");
    assert_eq!(names, vec![Some("Ada".into()), Some("Grace".into())]);

    // profile/stats/scan reach the windowed read path (`read_window`), which must also treat Tsv
    // as delimited — else a `.tsv` source errors as an unsupported format there.
    let profile = engine.profile(&source, 10_000).unwrap();
    assert_eq!(profile.columns.len(), 3, "tab columns via read_window");
}

#[test]
fn format_tsv_override_selects_tab_for_any_filename() {
    // A tab-separated file whose name doesn't imply TSV (here `.csv`): an explicit `--format tsv`
    // must select the tab delimiter regardless of the extension (the delimiter used to be keyed
    // off the extension, so the override was silently lost).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.csv");
    std::fs::write(&path, "id\tname\n1\tAda\n2\tGrace\n").unwrap();

    // Detected as CSV → comma → the whole tab-delimited header collapses into one column.
    let detected = Source::detect(&path).unwrap();
    assert_eq!(detected.format, Format::Csv);
    assert_eq!(
        LocalReaderEngine::default()
            .schema(&detected)
            .unwrap()
            .columns
            .len(),
        1,
        "as CSV the tabbed header is a single column"
    );

    // Explicit `--format tsv` override → tab → two columns.
    let source = Source::resolve(&path, Some("tsv")).unwrap();
    assert_eq!(source.format, Format::Tsv);
    assert_eq!(
        LocalReaderEngine::default()
            .schema(&source)
            .unwrap()
            .columns
            .len(),
        2,
        "override must split on tabs"
    );
}

#[test]
fn json_output_is_an_array_of_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path());
    let source = Source::detect(&path).unwrap();
    let engine = LocalReaderEngine::default();

    let preview = engine.preview(&source, 10).unwrap();
    let json = rows(&preview, Output::Json).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(value.is_array());
    assert_eq!(value.as_array().unwrap().len(), 4);
}

#[test]
fn large_int_minmax_is_exact() {
    // Values above 2^53 must not be rounded through an f64 accumulator.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ids.parquet");
    let ids = Int64Array::from(vec![1_i64, i64::MAX, i64::MAX - 1]);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ids) as ArrayRef]).unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let source = Source::detect(&path).unwrap();
    let profile = LocalReaderEngine::default()
        .profile(&source, 10_000)
        .unwrap();
    let id = &profile.columns[0];
    assert_eq!(id.min.as_deref(), Some("1"));
    assert_eq!(id.max.as_deref(), Some(i64::MAX.to_string().as_str()));
}

#[test]
fn all_nan_float_reports_no_minmax() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nans.parquet");
    let vals = Float64Array::from(vec![f64::NAN, f64::NAN]);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(vals) as ArrayRef]).unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let source = Source::detect(&path).unwrap();
    let profile = LocalReaderEngine::default()
        .profile(&source, 10_000)
        .unwrap();
    let v = &profile.columns[0];
    assert_eq!(v.min, None, "all-NaN column must not report a finite min");
    assert_eq!(v.max, None, "all-NaN column must not report a finite max");
}

#[test]
fn scan_sorts_and_filters_via_arrow_kernels() {
    use lakeleto::engine::{FilterOp, FilterSpec, ScanSpec, SortSpec};

    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path()); // id 1..4, score 91.5/88.0/null/79.25
    let source = Source::detect(&path).unwrap();
    let engine = LocalReaderEngine::default();

    // Sort by id descending → first row is id 4.
    let sorted = engine
        .scan(
            &source,
            &ScanSpec {
                offset: 0,
                limit: 10,
                sort: Some(SortSpec {
                    column: "id".into(),
                    descending: true,
                }),
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    let first = &sorted.batch.batches[0];
    let ids = first
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 4);
    assert_eq!(sorted.matched_rows, 4);

    // Filter id > 2 → 2 rows (3 and 4).
    let filtered = engine
        .scan(
            &source,
            &ScanSpec {
                offset: 0,
                limit: 10,
                sort: None,
                filters: vec![FilterSpec {
                    column: "id".into(),
                    op: FilterOp::Gt,
                    value: "2".into(),
                }],
                projection: None,
            },
        )
        .unwrap();
    assert_eq!(filtered.matched_rows, 2);
    assert_eq!(filtered.batch.num_rows(), 2);
}

#[test]
fn scan_projects_and_orders_columns() {
    use lakeleto::engine::ScanSpec;
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path());
    let source = Source::detect(&path).unwrap();
    let res = LocalReaderEngine::default()
        .scan(
            &source,
            &ScanSpec {
                offset: 0,
                limit: 10,
                sort: None,
                filters: vec![],
                projection: Some(vec!["score".into(), "id".into()]),
            },
        )
        .unwrap();
    let cols: Vec<&str> = res
        .batch
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        cols,
        vec!["score", "id"],
        "projection selects + orders columns"
    );
}

#[test]
fn stats_over_filtered_view() {
    use lakeleto::engine::{FilterOp, FilterSpec};
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path()); // id 1..4, name[Ada,Grace,null,Alan]
    let source = Source::detect(&path).unwrap();
    let prof = LocalReaderEngine::default()
        .stats(
            &source,
            &[FilterSpec {
                column: "id".into(),
                op: FilterOp::Gt,
                value: "2".into(),
            }],
            10_000,
        )
        .unwrap();
    assert_eq!(prof.row_count, Some(2), "filtered to ids 3,4");
    let name = prof.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name.null_count, 1, "id 3 has a null name");
}

#[cfg(feature = "sql")]
#[test]
fn sql_scan_external_sort_is_exact_and_unbounded() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path());
    let source = Source::detect(&path).unwrap();
    let res = DataFusionEngine::new()
        .scan(
            &source,
            &ScanSpec {
                offset: 0,
                limit: 10,
                sort: Some(SortSpec {
                    column: "id".into(),
                    descending: true,
                }),
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    assert_eq!(res.matched_rows, 4);
    assert!(
        res.total_known && !res.bounded,
        "DataFusion scan is exact + unbounded"
    );
    let ids = res.batch.batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 4, "ORDER BY id DESC");
}

#[test]
fn scan_plain_window_offsets() {
    use lakeleto::engine::ScanSpec;
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path());
    let source = Source::detect(&path).unwrap();
    let engine = LocalReaderEngine::default();

    let win = engine
        .scan(
            &source,
            &ScanSpec {
                offset: 1,
                limit: 2,
                sort: None,
                filters: vec![],
                projection: None,
            },
        )
        .unwrap();
    assert_eq!(win.batch.num_rows(), 2);
    assert_eq!(win.matched_rows, 4, "parquet total from the footer");
    assert!(win.total_known);
    let ids = win.batch.batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 2, "offset=1 skips the first row");
}

#[test]
fn magic_byte_sniff_detects_extensionless_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let src = write_parquet(dir.path());
    // Copy to an extension-less path so detection must fall back to the PAR1 magic sniff.
    let noext = dir.path().join("people_noext");
    std::fs::copy(&src, &noext).unwrap();
    let source = Source::detect(&noext).unwrap();
    assert_eq!(source.format, Format::Parquet);
}

// ---- multi-file parquet dataset -------------------------------------------------------

fn write_pq(path: &Path, fields: Vec<Field>, cols: Vec<ArrayRef>) {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn i64s(rb: &lakeleto::RowBatch, col: &str) -> Vec<Option<i64>> {
    rb.batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..a.len())
                .map(|i| a.is_valid(i).then(|| a.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn strs(rb: &lakeleto::RowBatch, col: &str) -> Vec<Option<String>> {
    rb.batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..a.len())
                .map(|i| a.is_valid(i).then(|| a.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn reads_multi_file_parquet_dataset() {
    let dir = tempfile::tempdir().unwrap();
    // A `foo.parquet/` directory with a nested (Hive-ish) subdir and a marker file.
    let root = dir.path().join("events.parquet");
    std::fs::create_dir_all(root.join("part=a")).unwrap();
    let idf = || Field::new("id", DataType::Int64, false);
    let namef = || Field::new("name", DataType::Utf8, false);
    write_pq(
        &root.join("part-0.parquet"),
        vec![idf(), namef()],
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    );
    write_pq(
        &root.join("part=a").join("part-1.parquet"),
        vec![idf(), namef()],
        vec![
            Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef,
        ],
    );
    std::fs::write(root.join("_SUCCESS"), b"").unwrap(); // must be ignored

    let source = Source::detect(&root).unwrap();
    assert_eq!(source.format, Format::Parquet, "dir of parquet → dataset");
    let engine = LocalReaderEngine::default();

    let schema = engine.schema(&source).unwrap();
    assert_eq!(schema.row_count, Some(4), "summed across files");
    // Two data columns plus the `part` Hive partition column from `part=a/`.
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["id", "name", "part"]);

    let preview = engine.preview(&source, 100).unwrap();
    let ids: Vec<i64> = i64s(&preview, "id").into_iter().flatten().collect();
    assert_eq!(ids, vec![1, 2, 3, 4], "part-0 then part=a/part-1, in order");
    // The partition value is surfaced as a column: null for the root file, "a" under part=a/.
    assert_eq!(
        strs(&preview, "part"),
        vec![None, None, Some("a".into()), Some("a".into())],
    );
}

#[test]
fn dataset_unions_schemas_and_null_fills() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("ds.parquet");
    std::fs::create_dir_all(&root).unwrap();
    // File a has {id, name}; file b adds a `score` column.
    write_pq(
        &root.join("a.parquet"),
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        ],
    );
    write_pq(
        &root.join("b.parquet"),
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["y"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![9.5])) as ArrayRef,
        ],
    );

    let source = Source::detect(&root).unwrap();
    let engine = LocalReaderEngine::default();
    let schema = engine.schema(&source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["id", "name", "score"],
        "union of both files' columns"
    );

    let preview = engine.preview(&source, 100).unwrap();
    let score = preview.batches.iter().flat_map(|b| {
        let a = b
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..a.len())
            .map(|i| a.is_valid(i).then(|| a.value(i)))
            .collect::<Vec<_>>()
    });
    // File a (no score) → null; file b → 9.5.
    assert_eq!(score.collect::<Vec<_>>(), vec![None, Some(9.5)]);
}

#[test]
fn dataset_type_conflict_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("bad.parquet");
    std::fs::create_dir_all(&root).unwrap();
    // Same column `id`, conflicting types across files.
    write_pq(
        &root.join("a.parquet"),
        vec![Field::new("id", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    );
    write_pq(
        &root.join("b.parquet"),
        vec![Field::new("id", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["2"])) as ArrayRef],
    );
    let source = Source::detect(&root).unwrap();
    let err = LocalReaderEngine::default().schema(&source).unwrap_err();
    assert!(err.to_string().contains("schema mismatch"), "got: {err}");
}

#[test]
fn dataset_surfaces_multi_key_hive_partition_columns() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("events.parquet");
    // year=2024/month=01/... and year=2024/month=02/... — a two-level Hive layout.
    for (month, ids) in [("01", [1i64, 2]), ("02", [3, 4])] {
        let leaf = root.join("year=2024").join(format!("month={month}"));
        std::fs::create_dir_all(&leaf).unwrap();
        write_pq(
            &leaf.join("data.parquet"),
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
        );
    }

    let source = Source::detect(&root).unwrap();
    let engine = LocalReaderEngine::default();
    let schema = engine.schema(&source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["id", "year", "month"],
        "data + partition columns"
    );

    let preview = engine.preview(&source, 100).unwrap();
    let ids: Vec<i64> = i64s(&preview, "id").into_iter().flatten().collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    assert_eq!(strs(&preview, "year"), vec![Some("2024".into()); 4]);
    assert_eq!(
        strs(&preview, "month"),
        vec![
            Some("01".into()),
            Some("01".into()),
            Some("02".into()),
            Some("02".into()),
        ],
    );
}

#[test]
fn dataset_partition_key_does_not_shadow_a_real_column() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("t.parquet");
    // `id` is both a partition dir name and a real data column — the data column must win, and
    // the partition key is not appended a second time.
    let leaf = root.join("id=99");
    std::fs::create_dir_all(&leaf).unwrap();
    write_pq(
        &leaf.join("data.parquet"),
        vec![Field::new("id", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    );

    let source = Source::detect(&root).unwrap();
    let engine = LocalReaderEngine::default();
    let schema = engine.schema(&source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["id"],
        "real `id` column wins; no shadow partition"
    );

    let preview = engine.preview(&source, 100).unwrap();
    let ids: Vec<i64> = i64s(&preview, "id").into_iter().flatten().collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "values come from the file, not the dir name"
    );
}
