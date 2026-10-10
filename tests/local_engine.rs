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
use lakeleto::RequestContext;
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

    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    assert_eq!(schema.row_count, Some(4));
    assert_eq!(schema.columns.len(), 4);
    assert_eq!(schema.columns[0].name, "id");
    assert!(!schema.columns[0].nullable);
    assert!(schema.columns[1].nullable);

    let preview = engine
        .preview(&RequestContext::detached(), &source, 2)
        .unwrap();
    assert_eq!(preview.num_rows(), 2);

    let profile = engine
        .profile(&RequestContext::detached(), &source, 10_000)
        .unwrap();
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
    let footer = engine
        .profile(&RequestContext::detached(), &source, 0)
        .unwrap();
    assert_eq!(footer.scanned_rows, 0, "footer-derived: no scan");
    assert_eq!(footer.row_count, Some(4));

    let scan = engine
        .profile(&RequestContext::detached(), &source, 10_000)
        .unwrap();
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

/// A column with no values has no min or max from the footer, as it has none from a scan: its
/// statistics are all null, and a null is no value to print.
#[test]
fn footer_profile_of_an_all_null_column_has_no_min_or_max() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sparse.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nothing", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None, None])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let source = Source::detect(&path).unwrap();
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    for scan_limit in [0, 10_000] {
        let profile = engine.profile(&ctx, &source, scan_limit).unwrap();
        let nothing = profile
            .columns
            .iter()
            .find(|c| c.name == "nothing")
            .unwrap();
        assert_eq!(nothing.null_count, 2, "scan {scan_limit}");
        assert_eq!(
            (nothing.min.as_deref(), nothing.max.as_deref()),
            (None, None),
            "scan {scan_limit}"
        );
    }
}

#[test]
fn csv_detect_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    std::fs::write(&path, "id,name,score\n1,Ada,91.5\n2,Grace,\n3,Linus,88.0\n").unwrap();

    let source = Source::detect(&path).unwrap();
    assert_eq!(source.format, Format::Csv);

    let engine = LocalReaderEngine::default();
    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    assert_eq!(schema.columns.len(), 3);

    let preview = engine
        .preview(&RequestContext::detached(), &source, 10)
        .unwrap();
    assert_eq!(preview.num_rows(), 3);

    let profile = engine
        .profile(&RequestContext::detached(), &source, 10_000)
        .unwrap();
    let score = profile.columns.iter().find(|c| c.name == "score").unwrap();
    assert_eq!(score.null_count, 1);
}

#[test]
fn ndjson_detect_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.ndjson");
    std::fs::write(
        &path,
        "{\"id\":1,\"name\":\"Ada\",\"score\":91.5}\n\
         {\"id\":2,\"name\":\"Grace\",\"score\":null}\n\
         {\"id\":3,\"name\":\"Linus\",\"score\":88.0}\n",
    )
    .unwrap();

    let source = Source::detect(&path).unwrap();
    assert_eq!(source.format, Format::Json, ".ndjson → Json");

    let engine = LocalReaderEngine::default();
    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    assert_eq!(schema.columns.len(), 3);

    let preview = engine
        .preview(&RequestContext::detached(), &source, 10)
        .unwrap();
    assert_eq!(preview.num_rows(), 3);

    // The row limit is honoured, not ignored.
    assert_eq!(
        engine
            .preview(&RequestContext::detached(), &source, 2)
            .unwrap()
            .num_rows(),
        2
    );

    // `null` is a null cell, not the string "null".
    let profile = engine
        .profile(&RequestContext::detached(), &source, 10_000)
        .unwrap();
    let score = profile.columns.iter().find(|c| c.name == "score").unwrap();
    assert_eq!(score.null_count, 1);
}

#[test]
fn json_array_detect_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.json");
    // A top-level array — leading whitespace before `[` — must read the same as newline-delimited.
    std::fs::write(
        &path,
        "\n  [\n    {\"id\": 1, \"name\": \"Ada\"},\n    {\"id\": 2, \"name\": \"Grace\"}\n  ]\n",
    )
    .unwrap();

    let source = Source::detect(&path).unwrap();
    assert_eq!(source.format, Format::Json, ".json → Json");

    let engine = LocalReaderEngine::default();
    assert_eq!(
        engine
            .schema(&RequestContext::detached(), &source)
            .unwrap()
            .columns
            .len(),
        2
    );
    assert_eq!(
        engine
            .preview(&RequestContext::detached(), &source, 10)
            .unwrap()
            .num_rows(),
        2
    );
}

// ---- JSON shapes that used to fail to open (docs/FORMATS-PLAN.md §1.2) ----------------------

/// Write `body` to a temp file named `name` and detect it. The tempdir rides along so the file
/// outlives the reads.
fn json_fixture(name: &str, body: &[u8]) -> (tempfile::TempDir, Source) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    std::fs::write(&path, body).unwrap();
    let source = Source::detect(&path).unwrap();
    assert_eq!(source.format, Format::Json, "{name} should detect as JSON");
    (dir, source)
}

fn column_names(source: &Source) -> Vec<String> {
    LocalReaderEngine::default()
        .schema(&RequestContext::detached(), source)
        .unwrap()
        .columns
        .into_iter()
        .map(|c| c.name)
        .collect()
}

/// The first `limit` rows as JSON objects, the way `--output json` prints them.
fn json_rows(source: &Source, limit: usize) -> Vec<serde_json::Value> {
    let preview = LocalReaderEngine::default()
        .preview(&RequestContext::detached(), source, limit)
        .unwrap();
    serde_json::from_str(&rows(&preview, Output::Json).unwrap()).unwrap()
}

#[test]
fn a_column_of_mixed_scalars_reads_as_text() {
    // Used to fail on the first row: "whilst decoding field 'v': expected string got 1".
    let (_dir, source) = json_fixture("t.ndjson", b"{\"v\":1}\n{\"v\":\"a\"}\n{\"v\":true}\n");
    let schema = LocalReaderEngine::default()
        .schema(&RequestContext::detached(), &source)
        .unwrap();
    assert_eq!(schema.columns[0].data_type, "Utf8");
    let v: Vec<_> = json_rows(&source, 10)
        .iter()
        .map(|r| r["v"].clone())
        .collect();
    assert_eq!(v, ["1", "a", "true"]);
}

#[test]
fn a_type_that_drifts_after_the_first_thousand_rows_still_profiles() {
    let mut body = String::new();
    for i in 0..1500 {
        if i < 1200 {
            body.push_str(&format!("{{\"id\":{i},\"v\":{i}}}\n"));
        } else {
            body.push_str(&format!("{{\"id\":{i},\"v\":\"s{i}\"}}\n"));
        }
    }
    let (_dir, source) = json_fixture("drift.ndjson", body.as_bytes());
    let profile = LocalReaderEngine::default()
        .profile(&RequestContext::detached(), &source, 10_000)
        .unwrap();
    assert_eq!(profile.scanned_rows, 1500);
    let v = profile.columns.iter().find(|c| c.name == "v").unwrap();
    assert_eq!(v.data_type, "Utf8");
    assert_eq!(v.null_count, 0, "every value survives, numbers as text");
}

#[test]
fn a_documents_records_member_is_unwrapped_and_reported() {
    let doc = serde_json::json!({
        "meta": {"page": 1, "total": 3},
        "data": [
            {"id": 1, "city": "London"},
            {"id": 2, "city": "Paris"},
            {"id": 3, "city": "Oslo"}
        ]
    });
    // Pretty-printed used to fail outright; one line used to be one row with a list column.
    for body in [
        serde_json::to_string_pretty(&doc).unwrap(),
        serde_json::to_string(&doc).unwrap(),
    ] {
        let (_dir, source) = json_fixture("api.json", body.as_bytes());
        let schema = LocalReaderEngine::default()
            .schema(&RequestContext::detached(), &source)
            .unwrap();
        assert_eq!(schema.records_path.as_deref(), Some("/data"));
        assert_eq!(column_names(&source), ["id", "city"]);
        let rows = json_rows(&source, 10);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2]["city"], "Oslo");
    }
}

#[test]
fn an_object_without_one_clear_records_member_is_one_row() {
    for body in [
        // Two candidates: nothing to choose between.
        r#"{"users": [{"a": 1}], "groups": [{"b": 2}]}"#,
        // No array of objects at all: a configuration-shaped document.
        r#"{"name": "cfg", "ids": [1, 2, 3]}"#,
    ] {
        let (_dir, source) = json_fixture("doc.json", body.as_bytes());
        let schema = LocalReaderEngine::default()
            .schema(&RequestContext::detached(), &source)
            .unwrap();
        assert_eq!(schema.records_path, None, "{body}");
        assert_eq!(json_rows(&source, 10).len(), 1, "{body}");
    }
}

#[test]
fn back_to_back_pretty_objects_read_like_ndjson() {
    // `jq` output: used to fail with "EOF while parsing an object".
    let (_dir, source) = json_fixture("jq.json", b"{\n  \"a\": 1\n}\n{\n  \"a\": 2\n}\n");
    let rows = json_rows(&source, 10);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["a"], 2);
}

#[test]
fn a_byte_order_mark_is_skipped() {
    for (name, body) in [
        ("bom.ndjson", "\u{feff}{\"a\":1}\n{\"a\":2}\n"),
        ("bom.json", "\u{feff}[{\"a\":1},{\"a\":2}]"),
        ("bom-doc.json", "\u{feff}{\"data\":[{\"a\":1},{\"a\":2}]}"),
    ] {
        let (_dir, source) = json_fixture(name, body.as_bytes());
        assert_eq!(json_rows(&source, 10).len(), 2, "{name}");
    }
}

#[test]
fn columns_come_in_the_order_their_keys_first_appear() {
    let (_dir, source) = json_fixture(
        "t.ndjson",
        b"{\"zeta\":1,\"alpha\":{\"name\":\"x\",\"geo\":{\"lat\":1}}}\n{\"mid\":3,\"zeta\":4}\n",
    );
    assert_eq!(column_names(&source), ["zeta", "alpha", "mid"]);
    // Nested fields too.
    let schema = LocalReaderEngine::default()
        .schema(&RequestContext::detached(), &source)
        .unwrap();
    let alpha = &schema.columns[1].data_type;
    assert!(
        alpha.find("\"name\"").unwrap() < alpha.find("\"geo\"").unwrap(),
        "{alpha}"
    );
}

#[test]
fn a_geojson_feature_collection_reads_one_row_per_feature() {
    let body = r#"{"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"name": "a"}, "geometry": {"type": "Point", "coordinates": [1.0, 2.0]}},
        {"type": "Feature", "properties": {"name": "b"}, "geometry": {"type": "Point", "coordinates": [3.0, 4.0]}}
    ]}"#;
    let (_dir, source) = json_fixture("cities.geojson", body.as_bytes());
    let schema = LocalReaderEngine::default()
        .schema(&RequestContext::detached(), &source)
        .unwrap();
    assert_eq!(schema.records_path.as_deref(), Some("/features"));
    assert_eq!(column_names(&source), ["type", "properties", "geometry"]);
    let rows = json_rows(&source, 10);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["properties"]["name"], "b");
}

#[test]
fn a_large_array_streams_and_windows_across_chunk_boundaries() {
    // ~1 MB, well past the reader's 64 KiB chunks, with the characters the array scanner keys on
    // (commas, brackets, escaped quotes) inside every string, so a scanner that lost its string
    // state at a chunk edge would split a record.
    let mut body = String::from("[");
    for i in 0..20_000 {
        if i > 0 {
            body.push_str(",\n  ");
        }
        body.push_str(&format!(r#"{{"id": {i}, "s": "x,]\"[{i}"}}"#));
    }
    body.push(']');
    let (_dir, source) = json_fixture("big.json", body.as_bytes());
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let all = engine.preview(&ctx, &source, 50_000).unwrap();
    assert_eq!(all.num_rows(), 20_000);

    let window = engine
        .scan(
            &ctx,
            &source,
            &lakeleto::engine::ScanSpec {
                offset: 19_990,
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&rows(&window.batch, Output::Json).unwrap()).unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0]["id"], 19_990);
    assert_eq!(rows[9]["s"], "x,]\"[19999");
}

#[test]
fn a_documents_records_stream_window_and_widen_like_an_array() {
    // The records of a wrapped document are streamed from where they lie, not parsed with the
    // document on every read: a deep window reads, and a drift past the 20,000-value sample
    // widens the schema, exactly as for a top-level array. Strings carry the characters the
    // array scanner keys on, and a member follows the array, so its bytes must end at its `]`.
    let mut body = String::from(r#"{"meta": {"note": "x,]\"[", "total": 25000}, "data": ["#);
    for i in 0..25_000 {
        if i > 0 {
            body.push_str(",\n  ");
        }
        let v = if i < 21_000 {
            i.to_string()
        } else {
            format!("\"s{i}\"")
        };
        body.push_str(&format!(r#"{{"id": {i}, "s": "x,]\"[{i}", "v": {v}}}"#));
    }
    body.push_str(r#"], "next": null}"#);
    let (_dir, source) = json_fixture("wrapped.json", body.as_bytes());
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let schema = engine.schema(&ctx, &source).unwrap();
    assert_eq!(schema.records_path.as_deref(), Some("/data"));
    assert_eq!(schema.columns[2].data_type, "Int64", "all the sample saw");

    let window = engine
        .scan(
            &ctx,
            &source,
            &lakeleto::engine::ScanSpec {
                offset: 24_990,
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&rows(&window.batch, Output::Json).unwrap()).unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0]["id"], 24_990);
    assert_eq!(rows[0]["v"], "s24990");
    assert_eq!(rows[9]["s"], "x,]\"[24999");
    let schema = engine.schema(&ctx, &source).unwrap();
    assert_eq!(schema.records_path.as_deref(), Some("/data"));
    assert_eq!(schema.columns[2].data_type, "Utf8", "widened, and kept");
}

#[test]
fn a_rewritten_document_has_its_records_found_again() {
    // Where the records lie is remembered per file version; a new version is looked at afresh.
    let (dir, source) = json_fixture("api.json", br#"{"data": [{"a": 1}, {"a": 2}]}"#);
    assert_eq!(json_rows(&source, 10).len(), 2);
    std::fs::write(
        dir.path().join("api.json"),
        r#"{"meta": {"note": "the records moved"}, "data": [{"a": 3}]}"#,
    )
    .unwrap();
    assert_eq!(json_rows(&source, 10), [serde_json::json!({"a": 3})]);
}

/// `json_fixture` with an explicit records path, as `--json-path` / `?json_path=` set it.
fn json_fixture_at(name: &str, body: &str, json_path: &str) -> (tempfile::TempDir, Source) {
    let (dir, source) = json_fixture(name, body.as_bytes());
    (dir, source.with_json_path(Some(json_path)).unwrap())
}

#[test]
fn an_explicit_json_path_picks_the_records_detection_would_not() {
    let two = r#"{"users": [{"a": 1}], "groups": [{"b": 2}, {"b": 3}]}"#;
    // Two candidates are ambiguous to detection; a bare member name settles it.
    let (_dir, source) = json_fixture_at("two.json", two, "groups");
    let schema = LocalReaderEngine::default()
        .schema(&RequestContext::detached(), &source)
        .unwrap();
    assert_eq!(schema.records_path.as_deref(), Some("/groups"));
    assert_eq!(json_rows(&source, 10).len(), 2);

    // Detection only looks one level down; a pointer reaches further.
    let nested = r#"{"response": {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}}"#;
    let (_dir, source) = json_fixture_at("nested.json", nested, "/response/items");
    assert_eq!(column_names(&source), ["id"]);
    assert_eq!(json_rows(&source, 10).len(), 3);

    // An object is one record.
    let (_dir, source) = json_fixture_at(
        "meta.json",
        r#"{"meta": {"page": 1}, "data": [{"a": 1}]}"#,
        "/meta",
    );
    assert_eq!(json_rows(&source, 10), [serde_json::json!({"page": 1})]);
}

#[test]
fn the_whole_document_path_turns_unwrapping_off() {
    let (_dir, source) = json_fixture_at("api.json", r#"{"data": [{"a": 1}, {"a": 2}]}"#, "");
    let schema = LocalReaderEngine::default()
        .schema(&RequestContext::detached(), &source)
        .unwrap();
    assert_eq!(schema.records_path, None);
    assert_eq!(json_rows(&source, 10).len(), 1, "the document is one row");

    // ...and changes nothing for input that was never unwrapped: an array, or NDJSON — which
    // starts like a document, and is not one.
    let (_dir, source) = json_fixture_at("array.json", r#"[{"a": 1}, {"a": 2}]"#, "");
    assert_eq!(json_rows(&source, 10).len(), 2);
    let (_dir, source) = json_fixture_at("lines.ndjson", "{\"a\": 1}\n{\"a\": 2}\n", "");
    assert_eq!(json_rows(&source, 10).len(), 2);
}

#[test]
fn an_explicit_json_path_that_cannot_apply_says_why() {
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    for (name, body, path, expect) in [
        (
            "missing.json",
            r#"{"data": [{"a": 1}]}"#,
            "/rows",
            "nothing at `/rows`",
        ),
        ("scalar.json", r#"{"n": 3}"#, "/n", "a single number"),
        (
            "lines.ndjson",
            "{\"a\":1}\n{\"a\":2}\n",
            "/a",
            "sequence of values",
        ),
        ("empty.json", "", "/data", "is empty"),
    ] {
        let (_dir, source) = json_fixture_at(name, body, path);
        let err = engine.schema(&ctx, &source).unwrap_err().to_string();
        assert!(err.contains(expect), "{name}: {err}");
    }
}

#[test]
fn a_json_path_on_a_source_that_is_not_json_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    std::fs::write(&path, "a,b\n1,2\n").unwrap();
    let err = Source::detect(&path)
        .unwrap()
        .with_json_path(Some("/data"))
        .unwrap_err();
    assert!(err.to_string().contains("but"), "{err}");
    assert!(err.to_string().contains("csv"), "{err}");
}

/// Column names and types of a read, for comparing one read's schema with another's.
fn shape_of(rb: &lakeleto::RowBatch) -> Vec<(String, String)> {
    rb.schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().to_string()))
        .collect()
}

#[test]
fn a_key_that_first_appears_late_is_in_the_schema_and_every_window() {
    // Inference used to stop at row 1,000, so `schema` never showed `late`, while a window that
    // reached row 1,200 did — the columns depended on how far you had scrolled.
    let mut body = String::new();
    for i in 0..1500 {
        if i == 1200 {
            body.push_str(&format!("{{\"id\":{i},\"late\":\"surprise\"}}\n"));
        } else {
            body.push_str(&format!("{{\"id\":{i}}}\n"));
        }
    }
    let (_dir, source) = json_fixture("late.ndjson", body.as_bytes());
    assert_eq!(column_names(&source), ["id", "late"]);

    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let first = engine.preview(&ctx, &source, 5).unwrap();
    let far = engine
        .scan(
            &ctx,
            &source,
            &lakeleto::engine::ScanSpec {
                offset: 1195,
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        shape_of(&first),
        shape_of(&far.batch),
        "same columns at any offset"
    );
}

#[test]
fn the_first_window_is_typed_like_the_whole_file() {
    // `v` is an integer for 1,200 rows and text after. A five-row preview used to type it Int64 —
    // the whole file is text, and now every read says so.
    let mut body = String::new();
    for i in 0..1500 {
        if i < 1200 {
            body.push_str(&format!("{{\"v\":{i}}}\n"));
        } else {
            body.push_str(&format!("{{\"v\":\"s{i}\"}}\n"));
        }
    }
    let (_dir, source) = json_fixture("drift.ndjson", body.as_bytes());
    let schema = LocalReaderEngine::default()
        .schema(&RequestContext::detached(), &source)
        .unwrap();
    assert_eq!(schema.columns[0].data_type, "Utf8");
    let first = LocalReaderEngine::default()
        .preview(&RequestContext::detached(), &source, 5)
        .unwrap();
    assert_eq!(first.schema.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn a_drift_past_the_sample_widens_the_schema_instead_of_failing() {
    // `v` turns to text at row 21,000 — past the 20,000 values inference samples.
    let mut body = String::new();
    for i in 0..25_000 {
        if i < 21_000 {
            body.push_str(&format!("{{\"v\":{i}}}\n"));
        } else {
            body.push_str(&format!("{{\"v\":\"s{i}\"}}\n"));
        }
    }
    let (_dir, source) = json_fixture("late-drift.ndjson", body.as_bytes());
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let v_type = |engine: &LocalReaderEngine| {
        engine.schema(&ctx, &source).unwrap().columns[0]
            .data_type
            .clone()
    };
    assert_eq!(v_type(&engine), "Int64", "all the sample saw");

    let window = engine
        .scan(
            &ctx,
            &source,
            &lakeleto::engine::ScanSpec {
                offset: 22_000,
                limit: 3,
                ..Default::default()
            },
        )
        .expect("a window past the sample reads, it does not fail");
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&rows(&window.batch, Output::Json).unwrap()).unwrap();
    assert_eq!(rows[0]["v"], "s22000");
    assert_eq!(
        v_type(&engine),
        "Utf8",
        "and the file's schema stays widened"
    );
}

#[test]
fn malformed_input_past_the_sample_is_still_an_error() {
    let mut body = String::new();
    for i in 0..20_500 {
        body.push_str(&format!("{{\"v\":{i}}}\n"));
    }
    body.push_str("{\"v\": oops}\n");
    let (_dir, source) = json_fixture("broken.ndjson", body.as_bytes());
    let Err(err) =
        LocalReaderEngine::default().preview(&RequestContext::detached(), &source, 30_000)
    else {
        panic!("a malformed value must not read as a shorter table");
    };
    assert!(err.to_string().to_lowercase().contains("json"), "{err}");
}

#[test]
fn a_struct_column_sorts_and_filters_by_the_json_the_grid_shows() {
    // Arrow can neither sort a struct nor cast one to text, so both used to fail the grid.
    let (_dir, source) = json_fixture(
        "people.ndjson",
        br#"{"id":1,"user":{"name":"Grace","geo":{"lat":40.7}},"tags":["b"]}
{"id":2,"user":{"name":"Ada","geo":{"lat":51.5}},"tags":["a","b"]}
{"id":3,"user":{"name":"Linus","geo":{"lat":60.2}},"tags":[]}
"#,
    );
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let scan = |spec: lakeleto::engine::ScanSpec| {
        let res = engine.scan(&ctx, &source, &spec).unwrap();
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(&rows(&res.batch, Output::Json).unwrap()).unwrap();
        (res.matched_rows, rows)
    };
    let filter =
        |column: &str, op: lakeleto::engine::FilterOp, value: &str| lakeleto::engine::ScanSpec {
            limit: 10,
            filters: vec![lakeleto::engine::FilterSpec {
                column: column.to_string(),
                op,
                value: value.to_string(),
            }],
            ..Default::default()
        };

    // Sorted by the struct's JSON text: {"name":"Ada"…} < {"name":"Grace"…} < {"name":"Linus"…}.
    let (_, sorted) = scan(lakeleto::engine::ScanSpec {
        limit: 10,
        sort: Some(lakeleto::engine::SortSpec {
            column: "user".to_string(),
            descending: false,
        }),
        ..Default::default()
    });
    let ids: Vec<_> = sorted.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, [2, 1, 3]);

    // A text filter matches the JSON, exactly as displayed.
    use lakeleto::engine::FilterOp;
    let (n, found) = scan(filter("user", FilterOp::Contains, "Grace"));
    assert_eq!((n, found[0]["id"].as_i64()), (1, Some(1)));
    let (n, _) = scan(filter(
        "user",
        FilterOp::Eq,
        r#"{"name":"Ada","geo":{"lat":51.5}}"#,
    ));
    assert_eq!(n, 1);
    // Lists too: the grid shows `["a","b"]`, so that is what `contains "a"` searches.
    let (n, _) = scan(filter("tags", FilterOp::Contains, "\"a\""));
    assert_eq!(n, 1);
}

#[test]
fn a_rewritten_file_is_inferred_again() {
    let (dir, source) = json_fixture("t.ndjson", b"{\"a\":1}\n");
    assert_eq!(column_names(&source), ["a"]);
    // A new version of the file (a different length, and a later mtime) must not reuse the
    // schema cached for the old one.
    std::fs::write(
        dir.path().join("t.ndjson"),
        b"{\"a\":1,\"b\":2}\n{\"a\":3}\n",
    )
    .unwrap();
    assert_eq!(column_names(&source), ["a", "b"]);
}

#[test]
fn a_truncated_array_is_an_error_not_a_shorter_table() {
    let (_dir, source) = json_fixture("cut.json", b"[{\"a\":1},{\"a\":2},{\"a\":");
    let Err(err) = LocalReaderEngine::default().preview(&RequestContext::detached(), &source, 10)
    else {
        panic!("a truncated array must not read as a shorter one");
    };
    assert!(err.to_string().contains("truncated"), "{err}");
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
    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    assert_eq!(
        schema.columns.len(),
        3,
        "tab-delimited columns must split correctly"
    );

    let preview = engine
        .preview(&RequestContext::detached(), &source, 10)
        .unwrap();
    assert_eq!(preview.num_rows(), 2);
    let names: Vec<Option<String>> = strs(&preview, "name");
    assert_eq!(names, vec![Some("Ada".into()), Some("Grace".into())]);

    // profile/stats/scan reach the windowed read path (`read_window`), which must also treat Tsv
    // as delimited — else a `.tsv` source errors as an unsupported format there.
    let profile = engine
        .profile(&RequestContext::detached(), &source, 10_000)
        .unwrap();
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
            .schema(&RequestContext::detached(), &detected)
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
            .schema(&RequestContext::detached(), &source)
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

    let preview = engine
        .preview(&RequestContext::detached(), &source, 10)
        .unwrap();
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
        .profile(&RequestContext::detached(), &source, 10_000)
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
        .profile(&RequestContext::detached(), &source, 10_000)
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
            &RequestContext::detached(),
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
            &RequestContext::detached(),
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
            &RequestContext::detached(),
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
            &RequestContext::detached(),
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
            &RequestContext::detached(),
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
            &RequestContext::detached(),
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

    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    assert_eq!(schema.row_count, Some(4), "summed across files");
    // Two data columns plus the `part` Hive partition column from `part=a/`.
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["id", "name", "part"]);

    let preview = engine
        .preview(&RequestContext::detached(), &source, 100)
        .unwrap();
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
    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["id", "name", "score"],
        "union of both files' columns"
    );

    let preview = engine
        .preview(&RequestContext::detached(), &source, 100)
        .unwrap();
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
    let err = LocalReaderEngine::default()
        .schema(&RequestContext::detached(), &source)
        .unwrap_err();
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
    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["id", "year", "month"],
        "data + partition columns"
    );

    let preview = engine
        .preview(&RequestContext::detached(), &source, 100)
        .unwrap();
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

/// SQL reads a Hive-partitioned Parquet directory through the local engine, as the grid does, so
/// it keeps the `key=value` partition columns that `register_parquet` would drop. A plain Parquet
/// file DataFusion scans itself, rather than holding it as a table in memory.
#[cfg(feature = "sql")]
#[test]
fn sql_keeps_hive_partition_columns_and_scans_a_parquet_file_itself() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("events.parquet");
    for (month, ids) in [("01", [1i64, 2]), ("02", [3, 4])] {
        let leaf = root.join("year=2024").join(format!("month={month}"));
        std::fs::create_dir_all(&leaf).unwrap();
        write_pq(
            &leaf.join("data.parquet"),
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
        );
    }
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let table = |path: &Path| {
        [NamedSource {
            name: "t".to_string(),
            source: Source::detect(path).unwrap(),
        }]
    };

    let rows = sql
        .query(&ctx, "SELECT id, month FROM t ORDER BY id", &table(&root))
        .unwrap();
    assert_eq!(i64s(&rows, "id"), [1, 2, 3, 4].map(Some));
    assert_eq!(
        strs(&rows, "month"),
        ["01", "01", "02", "02"].map(|m| Some(m.to_string()))
    );

    let file = root.join("year=2024").join("month=01").join("data.parquet");
    let explained = sql
        .query(&ctx, "EXPLAIN SELECT id FROM t", &table(&file))
        .unwrap();
    let plan = strs(&explained, "plan")
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("file_type=parquet"), "{plan}");
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
    let schema = engine.schema(&RequestContext::detached(), &source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["id"],
        "real `id` column wins; no shadow partition"
    );

    let preview = engine
        .preview(&RequestContext::detached(), &source, 100)
        .unwrap();
    let ids: Vec<i64> = i64s(&preview, "id").into_iter().flatten().collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "values come from the file, not the dir name"
    );
}

// ---- `--flatten` -------------------------------------------------------------------------------

fn every_level(source: Source) -> Source {
    source.with_flatten(Some(lakeleto::Flatten::All))
}

fn names_of(rb: &lakeleto::RowBatch) -> Vec<String> {
    rb.schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// `id` and `user: {name, geo: {lat}}` over four rows. Row 3 has no user and row 4 no geo — each
/// with a value left in the children beneath, which Arrow allows and a flattened read must not show.
fn write_users_parquet(dir: &Path) -> std::path::PathBuf {
    use arrow_array::builder::NullBufferBuilder;
    use arrow_array::StructArray;
    use arrow_schema::Fields;
    let valid = |flags: &[bool]| {
        let mut nulls = NullBufferBuilder::new(flags.len());
        for f in flags {
            nulls.append(*f);
        }
        nulls.finish()
    };
    let geo_fields = Fields::from(vec![Field::new("lat", DataType::Float64, true)]);
    let geo = StructArray::new(
        geo_fields.clone(),
        vec![Arc::new(Float64Array::from(vec![40.7, 51.5, 0.0, 60.2])) as ArrayRef],
        valid(&[true, true, true, false]),
    );
    let user_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("geo", DataType::Struct(geo_fields), true),
    ]);
    let user = StructArray::new(
        user_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Grace", "Ada", "Ghost", "Linus"])) as ArrayRef,
            Arc::new(geo),
        ],
        valid(&[true, true, false, true]),
    );
    let path = dir.join("users.parquet");
    write_pq(
        &path,
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user", DataType::Struct(user_fields), true),
        ],
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])), Arc::new(user)],
    );
    path
}

#[test]
fn a_flattened_parquet_struct_reads_as_its_fields() {
    let dir = tempfile::tempdir().unwrap();
    let source = every_level(Source::detect(write_users_parquet(dir.path())).unwrap());
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();

    let schema = engine.schema(&ctx, &source).unwrap();
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, ["id", "user.name", "user.geo.lat"]);
    assert_eq!(
        schema.row_count,
        Some(4),
        "the footer count survives flattening"
    );

    let preview = engine.preview(&ctx, &source, 10).unwrap();
    assert_eq!(names_of(&preview), ["id", "user.name", "user.geo.lat"]);
    assert_eq!(
        strs(&preview, "user.name"),
        [
            Some("Grace".into()),
            Some("Ada".into()),
            None,
            Some("Linus".into())
        ]
    );
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&rows(&preview, Output::Json).unwrap()).unwrap();
    let lats: Vec<_> = rows.iter().map(|r| r["user.geo.lat"].as_f64()).collect();
    assert_eq!(lats, [Some(40.7), Some(51.5), None, None]);
}

#[test]
fn a_flattened_window_still_pushes_its_columns_into_the_parquet_reader() {
    use lakeleto::engine::ScanSpec;
    let dir = tempfile::tempdir().unwrap();
    let source = every_level(Source::detect(write_users_parquet(dir.path())).unwrap());
    // A flattened name is its Parquet leaf path, so the projection selects just that leaf — and
    // the window comes back in the order asked for.
    let res = LocalReaderEngine::default()
        .scan(
            &RequestContext::detached(),
            &source,
            &ScanSpec {
                offset: 1,
                limit: 2,
                projection: Some(vec!["user.geo.lat".into(), "id".into()]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(names_of(&res.batch), ["user.geo.lat", "id"]);
    assert_eq!(i64s(&res.batch, "id"), [Some(2), Some(3)]);
    assert!(res.total_known && res.matched_rows == 4);
}

#[test]
fn a_flattened_source_sorts_filters_and_profiles_by_its_fields() {
    use lakeleto::engine::{FilterOp, FilterSpec, ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    let source = every_level(Source::detect(write_users_parquet(dir.path())).unwrap());
    let engine = LocalReaderEngine::default();
    let ctx = RequestContext::detached();
    let res = engine
        .scan(
            &ctx,
            &source,
            &ScanSpec {
                limit: 10,
                sort: Some(SortSpec {
                    column: "user.name".into(),
                    descending: true,
                }),
                filters: vec![FilterSpec {
                    column: "user.geo.lat".into(),
                    op: FilterOp::Gt,
                    value: "0".into(),
                }],
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        i64s(&res.batch, "id"),
        [Some(1), Some(2)],
        "Grace, then Ada"
    );
    assert_eq!(res.matched_rows, 2);

    let stats = engine
        .stats(
            &ctx,
            &source,
            &[FilterSpec {
                column: "user.name".into(),
                op: FilterOp::Eq,
                value: "Linus".into(),
            }],
            100,
        )
        .unwrap();
    assert_eq!(stats.row_count, Some(1));
    let names: Vec<&str> = stats.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "user.name", "user.geo.lat"]);

    // The footer profile lists the fields too. Parquet's statistics reach top-level columns only,
    // so `id` has them and the struct's fields have none — as the struct column never had any.
    let footer = engine.profile(&ctx, &source, 0).unwrap();
    let by_name = |n: &str| footer.columns.iter().find(|c| c.name == n).unwrap();
    assert_eq!(footer.columns.len(), 3);
    assert_eq!(by_name("id").max.as_deref(), Some("4"));
    assert_eq!(by_name("user.name").max, None);
}

#[test]
fn a_flattened_json_source_spreads_to_the_depth_asked() {
    let (_dir, source) = json_fixture(
        "people.ndjson",
        br#"{"id":1,"user":{"name":"Grace","geo":{"lat":40.7}},"tags":["b"]}
{"id":2,"user":null,"tags":[]}
{"id":3,"user":{"name":"Linus","geo":{"lat":60.2}},"tags":["a"]}
"#,
    );
    let all = every_level(source.clone());
    assert_eq!(
        column_names(&all),
        ["id", "user.name", "user.geo.lat", "tags"],
        "lists stay whole"
    );
    let one = source.clone().with_flatten(Some(lakeleto::Flatten::Levels(
        std::num::NonZeroUsize::new(1).unwrap(),
    )));
    assert_eq!(column_names(&one), ["id", "user.name", "user.geo", "tags"]);
    assert_eq!(
        column_names(&source.with_flatten(None)),
        ["id", "user", "tags"]
    );

    let rows = json_rows(&all, 10);
    assert_eq!(rows[0]["user.name"], "Grace");
    assert!(rows[1]["user.name"].is_null(), "a null user has no name");
    assert_eq!(rows[2]["user.geo.lat"], 60.2);
}

#[cfg(feature = "sql")]
#[test]
fn sql_flattens_a_parquet_struct_as_the_local_engine_does() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{FilterOp, FilterSpec, NamedSource, ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    let source = every_level(Source::detect(write_users_parquet(dir.path())).unwrap());
    let ctx = RequestContext::detached();
    let local = LocalReaderEngine::default();
    let sql = DataFusionEngine::new();

    let names = |s: lakeleto::TableSchema| -> Vec<String> {
        s.columns.into_iter().map(|c| c.name).collect()
    };
    assert_eq!(
        names(sql.schema(&ctx, &source).unwrap()),
        names(local.schema(&ctx, &source).unwrap())
    );
    let json = |rb: lakeleto::RowBatch| rows(&rb, Output::Json).unwrap();
    assert_eq!(
        json(sql.preview(&ctx, &source, 10).unwrap()),
        json(local.preview(&ctx, &source, 10).unwrap()),
        "same rows, same nulls"
    );

    // The grid's sort and filter by field name, which DataFusion runs over the whole file.
    let spec = ScanSpec {
        limit: 10,
        sort: Some(SortSpec {
            column: "user.name".into(),
            descending: true,
        }),
        filters: vec![FilterSpec {
            column: "user.geo.lat".into(),
            op: FilterOp::Gt,
            value: "0".into(),
        }],
        ..Default::default()
    };
    let (by_sql, by_local) = (
        sql.scan(&ctx, &source, &spec).unwrap(),
        local.scan(&ctx, &source, &spec).unwrap(),
    );
    assert_eq!(i64s(&by_sql.batch, "id"), [Some(1), Some(2)]);
    assert_eq!(i64s(&by_sql.batch, "id"), i64s(&by_local.batch, "id"));

    // And in a query, quoted like any name with a dot in it.
    let rb = sql
        .query(
            &ctx,
            r#"SELECT "user.name" FROM people WHERE "user.geo.lat" > 45 ORDER BY id"#,
            &[NamedSource {
                name: "people".to_string(),
                source,
            }],
        )
        .unwrap();
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&rows(&rb, Output::Json).unwrap()).unwrap();
    assert_eq!(rows, [serde_json::json!({"user.name": "Ada"})]);
}

// ---- compressed files -------------------------------------------------------------------------

/// A `.csv.gz` used to fail detection as an unknown extension, and with `--format csv` it read as a
/// table of compressed bytes. It reads as the CSV it holds, detected or named — in every read, and
/// in SQL — and one whose bytes are not the gzip its name says fails as such, naming the file,
/// rather than being read as text.
#[test]
fn a_compressed_file_reads_as_what_it_holds_and_a_corrupt_one_says_so() {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv.gz");
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(b"id,name\n1,ada\n2,grace\n").unwrap();
    std::fs::write(&path, gz.finish().unwrap()).unwrap();
    let ctx = RequestContext::detached();
    let engine = LocalReaderEngine::default();
    for source in [
        Source::detect(&path).unwrap(),
        Source::resolve(&path, Some("csv")).unwrap(),
    ] {
        assert_eq!(source.codec, Some(lakeleto::Codec::Gzip));
        let schema = engine.schema(&ctx, &source).unwrap();
        let names: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
        assert_eq!(engine.preview(&ctx, &source, 5).unwrap().num_rows(), 2);
        #[cfg(feature = "sql")]
        {
            let sql = lakeleto::engine::sql::DataFusionEngine::new()
                .schema(&ctx, &source)
                .unwrap();
            assert_eq!(sql.columns.len(), 2);
        }
    }

    // gzip's magic bytes, then noise: what a real .csv.gz starts like, and is not.
    let corrupt = dir.path().join("bad.csv.gz");
    std::fs::write(&corrupt, b"\x1f\x8b\x08\x00 compressed rows").unwrap();
    let err = engine
        .preview(&ctx, &Source::detect(&corrupt).unwrap(), 5)
        .err()
        .expect("not gzip")
        .to_string();
    assert!(err.contains("bad.csv.gz is not valid gzip"), "{err}");
}

// ---- SQL over JSON ------------------------------------------------------------------------------

/// Every JSON layout reads the same through SQL as through the local engine — schema and rows —
/// because SQL registers JSON by reading it through the local reader. DataFusion's own JSON reader
/// infers line by line, and would fail the pretty, wrapped and BOM-prefixed inputs here outright.
#[cfg(feature = "sql")]
#[test]
fn sql_reads_every_json_layout_as_the_local_engine_does() {
    use lakeleto::engine::sql::DataFusionEngine;
    let cases: &[(&str, &str, Option<&str>, bool)] = &[
        ("lines.ndjson", "{\"id\":1,\"v\":\"a\"}\n{\"id\":2,\"v\":\"b\"}\n", None, false),
        ("pretty.json", "[\n  {\"id\": 1, \"v\": \"a\"},\n  {\"id\": 2}\n]", None, false),
        ("jq.json", "{\n  \"id\": 1\n}\n{\n  \"id\": 2\n}\n", None, false),
        (
            "wrapped.json",
            r#"{"meta": {"n": 2}, "data": [{"id": 1}, {"id": 2}]}"#,
            None,
            false,
        ),
        (
            "places.geojson",
            r#"{"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"name": "x"}, "geometry": null}]}"#,
            None,
            false,
        ),
        ("bom.ndjson", "\u{feff}{\"id\":1}\n{\"id\":2}\n", None, false),
        ("mixed.ndjson", "{\"v\": 1}\n{\"v\": \"a\"}\n", None, false),
        (
            "nested.ndjson",
            "{\"id\":1,\"user\":{\"name\":\"Ada\",\"geo\":{\"lat\":51.5}},\"tags\":[\"a\"]}\n",
            None,
            false,
        ),
        (
            "whole.json",
            r#"{"meta": {"n": 2}, "data": [{"id": 1}, {"id": 2}]}"#,
            Some(""),
            false,
        ),
        (
            "flat.ndjson",
            "{\"id\":1,\"user\":{\"name\":\"Ada\",\"geo\":{\"lat\":51.5}}}\n{\"id\":2,\"user\":null}\n",
            None,
            true,
        ),
    ];
    let ctx = RequestContext::detached();
    let local = LocalReaderEngine::default();
    let sql = DataFusionEngine::new();
    let columns = |s: lakeleto::TableSchema| -> Vec<(String, String, bool)> {
        s.columns
            .into_iter()
            .map(|c| (c.name, c.data_type, c.nullable))
            .collect()
    };
    for (name, body, json_path, flatten) in cases {
        let (_dir, source) = json_fixture(name, body.as_bytes());
        let source = source
            .with_json_path(*json_path)
            .unwrap()
            .with_flatten(flatten.then_some(lakeleto::Flatten::All));
        assert_eq!(
            columns(sql.schema(&ctx, &source).unwrap()),
            columns(local.schema(&ctx, &source).unwrap()),
            "{name}: schema"
        );
        assert_eq!(
            rows(&sql.preview(&ctx, &source, 100).unwrap(), Output::Json).unwrap(),
            rows(&local.preview(&ctx, &source, 100).unwrap(), Output::Json).unwrap(),
            "{name}: rows"
        );
    }
}

/// A value past the 20,000-value sample that disagrees with it widens the schema in SQL as it does
/// in the grid: the pass that meets it finds the file's wider schema, and the query is planned
/// again with it rather than failing.
#[cfg(feature = "sql")]
#[test]
fn a_sql_query_over_json_survives_a_drift_past_the_sample() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;
    let mut body = String::new();
    for i in 0..20_001 {
        body.push_str(&format!("{{\"v\": {i}}}\n"));
    }
    body.push_str("{\"v\": \"late text\"}\n");
    let (_dir, source) = json_fixture("drift.ndjson", body.as_bytes());
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let tables = [NamedSource {
        name: "t".to_string(),
        source,
    }];
    let count = sql
        .query(&ctx, "SELECT count(*) AS n FROM t", &tables)
        .unwrap();
    assert_eq!(i64s(&count, "n"), [Some(20_002)]);
    let late = sql
        .query(&ctx, "SELECT v FROM t WHERE v = 'late text'", &tables)
        .unwrap();
    assert_eq!(strs(&late, "v"), [Some("late text".to_string())]);
}

/// `count` integers, each its own NDJSON record `{"v": n}`, with `tail` appended.
#[cfg(feature = "sql")]
fn ints_then(count: usize, tail: &str) -> String {
    let mut body: String = (0..count).map(|i| format!("{{\"v\": {i}}}\n")).collect();
    body.push_str(tail);
    body
}

/// SQL streams JSON: a query reads the file only as far as it needs to. A file read whole into
/// memory, as JSON used to be, fails on its malformed last line before a `LIMIT 10` can answer.
#[cfg(feature = "sql")]
#[test]
fn a_sql_query_over_json_reads_the_file_only_as_far_as_it_needs() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;
    let (_dir, source) = json_fixture(
        "long.ndjson",
        ints_then(100_000, "{\"v\": oops}\n").as_bytes(),
    );
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let head = sql.preview(&ctx, &source, 10).unwrap();
    assert_eq!(head.num_rows(), 10);
    let tables = [NamedSource {
        name: "t".to_string(),
        source,
    }];
    let Err(err) = sql.query(&ctx, "SELECT v FROM t WHERE v < 3", &tables) else {
        panic!("a query that reads every line reaches the malformed one");
    };
    assert!(err.to_string().contains("invalid JSON"), "{err}");
}

/// A query can fail to plan over the sampled types before any row is read: a string compared with a
/// column the sample saw only integers in. The file is checked then, and the query planned over the
/// type it holds. A query that fails over every type the file holds still fails, with its own error.
#[cfg(feature = "sql")]
#[test]
fn a_query_that_cannot_plan_over_the_sampled_types_is_planned_over_the_files() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let t = |source: Source| {
        [NamedSource {
            name: "t".to_string(),
            source,
        }]
    };
    let (_late_dir, late) = json_fixture(
        "late.ndjson",
        ints_then(20_001, "{\"v\": \"late text\"}\n").as_bytes(),
    );
    let found = sql
        .query(
            &ctx,
            "SELECT count(*) AS n FROM t WHERE v = 'late text'",
            &t(late),
        )
        .unwrap();
    assert_eq!(i64s(&found, "n"), [Some(1)]);

    let (_ints_dir, ints) = json_fixture("ints.ndjson", ints_then(10, "").as_bytes());
    let Err(err) = sql.query(
        &ctx,
        "SELECT count(*) AS n FROM t WHERE v = 'text'",
        &t(ints),
    ) else {
        panic!("a string is not an integer, whatever the file holds");
    };
    assert!(err.to_string().contains("Cast error"), "{err}");
}

/// A streamed result can be planned again only until a batch of it has reached the caller. A query
/// that reads the whole table before answering — a sort, an aggregate — meets the late value first,
/// and is planned again unseen. One already handing rows over stops, says the file's schema is
/// wider now, and reads every row when run again.
#[cfg(feature = "sql")]
#[test]
fn a_streamed_sql_result_widens_unseen_until_rows_have_left() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::NamedSource;
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let named = |source: Source| {
        [NamedSource {
            name: "t".to_string(),
            source,
        }]
    };
    let drain = |stream: lakeleto::engine::RowStream| {
        stream
            .map(|b| b.map(|b| b.num_rows()))
            .collect::<Result<Vec<_>, _>>()
            .map(|rows| rows.iter().sum::<usize>())
    };

    // Sorted: every row is read before the first leaves.
    let (_sorted_dir, sorted) = json_fixture(
        "sorted.ndjson",
        ints_then(20_001, "{\"v\": \"late text\"}\n").as_bytes(),
    );
    let mut stream = sql
        .query_stream(
            &ctx,
            "SELECT v FROM t ORDER BY v DESC LIMIT 1",
            &named(sorted),
            None,
        )
        .unwrap();
    let top = stream.next().unwrap().unwrap();
    let top = top
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(top.value(0), "late text");

    // Unsorted: the first batch has left long before the pass reaches row 100,000.
    let body = ints_then(100_000, "{\"v\": \"late text\"}\n");
    let (_plain_dir, plain) = json_fixture("plain.ndjson", body.as_bytes());
    let stream = sql
        .query_stream(&ctx, "SELECT v FROM t", &named(plain.clone()), None)
        .unwrap();
    let err = drain(stream).unwrap_err().to_string();
    assert!(err.contains("run again"), "{err}");
    let again = sql
        .query_stream(&ctx, "SELECT v FROM t", &named(plain), None)
        .unwrap();
    assert_eq!(drain(again).unwrap(), 100_001);
}

/// A filtered window through SQL comes back in the file's order, the same on every run, so the
/// next page continues where this one stopped. DataFusion's parallel scan used to deliver the
/// rows of an unsorted query in whatever order its partitions finished: the same request gave
/// different rows on a re-run, and paging repeated or skipped some.
#[cfg(feature = "sql")]
#[test]
fn a_filtered_sql_window_keeps_the_files_order_across_pages() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{FilterOp, FilterSpec, ScanSpec};
    let mut body = String::new();
    for i in 0..60_000 {
        body.push_str(&format!("{{\"id\": {i}, \"k\": {}}}\n", i % 3));
    }
    let (_dir, source) = json_fixture("many.ndjson", body.as_bytes());
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let page = |offset: usize| {
        let spec = ScanSpec {
            offset,
            limit: 1_000,
            filters: vec![FilterSpec {
                column: "k".into(),
                op: FilterOp::Eq,
                value: "0".into(),
            }],
            ..Default::default()
        };
        let res = sql.scan(&ctx, &source, &spec).unwrap();
        assert_eq!(res.matched_rows, 20_000);
        i64s(&res.batch, "id")
    };
    let expect = |from: i64| (0..1_000).map(|n| Some(from + 3 * n)).collect::<Vec<_>>();
    for _ in 0..5 {
        assert_eq!(page(0), expect(0), "first page, in file order");
    }
    assert_eq!(page(1_000), expect(3_000), "second page follows the first");
}

/// A sorted window through SQL is the same on every run, and the next page continues it, even when
/// the sort key has ties. DataFusion's parallel plan left ties to timing — its partitions' TopKs
/// share a strict threshold, so which equal keys made the page depended on which partition got there
/// first: over a CSV big enough to be scanned in parallel, sorting by a three-value column gave a
/// different page on most runs, drawn from all over the file.
#[cfg(feature = "sql")]
#[test]
fn a_sorted_sql_window_with_ties_is_the_same_on_every_run() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ties.csv");
    // Just over the 10 MiB at which DataFusion splits one file across partitions.
    let cities = ["KL", "SG", "JK"];
    let mut body = String::from("id,city,pad\n");
    for i in 0..600_000 {
        body.push_str(&format!("{i},{},{i:08}\n", cities[i % 3]));
    }
    assert!(body.len() > 10 * 1024 * 1024);
    std::fs::write(&path, body).unwrap();
    let source = Source::detect(&path).unwrap();
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let page = |offset: usize| {
        let spec = ScanSpec {
            offset,
            limit: 100,
            sort: Some(SortSpec {
                column: "city".into(),
                descending: false,
            }),
            ..Default::default()
        };
        i64s(&sql.scan(&ctx, &source, &spec).unwrap().batch, "id")
    };
    let first = page(0);
    assert_eq!(page(0), first, "the same window on a second run");
    // `JK` sorts first, and the two pages are its first 200 rows in the file: one partition keeps
    // the earliest of equal keys.
    let mut both: Vec<i64> = first
        .iter()
        .chain(&page(100))
        .map(|id| id.unwrap())
        .collect();
    both.sort_unstable();
    assert_eq!(both, (0..200).map(|n| 2 + 3 * n).collect::<Vec<i64>>());
}

/// A sorted window deep into a table, past the 50,000 rows a one-pass read would keep in every
/// partition, is the one a sort of every row gives: by key, nulls after every value and first when
/// descending, ties in the order of the source. So it is however the table is read: a CSV, and a
/// Parquet file in row groups, over the 10 MiB at which DataFusion splits a file across partitions
/// and numbered as their scans read them; an NDJSON file, streamed, alone and flattened into a
/// view; and a directory of Parquet files, read into memory — the last three numbered on one
/// partition. A numbering on top of the last two would see their rows out of order: the view's
/// struct is sometimes null, which DataFusion deals out over partitions to flatten, and the files'
/// batches differ in size, which DataFusion spreads over partitions by. Its count is every row that
/// matches, a window past the last row is empty, and pages either side of the depth at which the
/// reading changes meet exactly.
#[cfg(feature = "sql")]
#[test]
fn a_deep_sorted_sql_window_is_the_one_a_sort_of_every_row_gives() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{FilterOp, FilterSpec, ScanSpec, SortSpec};
    use parquet::file::properties::WriterProperties;
    use std::cmp::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let rows = 60_000usize;
    let grp = |i: usize| i % 7;
    let tag = |i: usize| (!i.is_multiple_of(3)).then(|| format!("t{:02}", i % 17));
    // Text no Parquet encoding shrinks: a file of it is as large as the CSV.
    let noise = |i: usize| {
        let mut x = (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (0..12)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                format!("{x:016x}")
            })
            .collect::<String>()
    };
    // Rows `range` in row groups of 4,000, with a column of noise if `padded`.
    let parquet = |path: &Path, range: std::ops::Range<usize>, padded: bool| {
        let ids = range.clone().map(|i| i as i64);
        let mut columns: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(Int64Array::from_iter_values(ids))),
            (
                "grp",
                Arc::new(Int64Array::from_iter_values(
                    range.clone().map(|i| grp(i) as i64),
                )),
            ),
            (
                "tag",
                Arc::new(StringArray::from_iter(range.clone().map(tag))),
            ),
        ];
        if padded {
            let pad = StringArray::from_iter_values(range.map(noise));
            columns.push(("pad", Arc::new(pad)));
        }
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4_000))
            .build();
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    };
    let csv = dir.path().join("deep.csv");
    let mut body = String::from("id,grp,tag,pad\n");
    for i in 0..rows {
        let pad = "x".repeat(180);
        body.push_str(&format!(
            "{i},{},{},{pad}\n",
            grp(i),
            tag(i).unwrap_or_default()
        ));
    }
    assert!(body.len() > 10 * 1024 * 1024);
    std::fs::write(&csv, body).unwrap();
    let file = dir.path().join("deep.parquet");
    parquet(&file, 0..rows, true);
    assert!(std::fs::metadata(&file).unwrap().len() > 10 * 1024 * 1024);
    let json = |nested: bool| {
        let mut body = String::new();
        for i in 0..rows {
            let tag = tag(i).map_or("null".into(), |t| format!("\"{t}\""));
            let x = match (nested, i % 5) {
                (false, _) => String::new(),
                (true, 0) => ", \"x\": null".into(),
                (true, _) => format!(", \"x\": {{\"n\": {i}}}"),
            };
            body.push_str(&format!(
                "{{\"id\": {i}, \"grp\": {}, \"tag\": {tag}{x}}}\n",
                grp(i)
            ));
        }
        body
    };
    let ndjson = dir.path().join("deep.ndjson");
    std::fs::write(&ndjson, json(false)).unwrap();
    let nested = dir.path().join("nested.ndjson");
    std::fs::write(&nested, json(true)).unwrap();
    let parts = dir.path().join("parts.parquet");
    std::fs::create_dir(&parts).unwrap();
    let mut start = 0;
    for (part, len) in [12_000, 8_000, 10_000, 6_000, 14_000, 10_000]
        .iter()
        .enumerate()
    {
        let path = parts.join(format!("part-{part}.parquet"));
        parquet(&path, start..start + len, false);
        start += len;
    }
    let flattened = Source::detect(&nested)
        .unwrap()
        .with_flatten(Some(lakeleto::Flatten::All));
    let sources = [
        ("CSV", Source::detect(&csv).unwrap()),
        ("Parquet", Source::detect(&file).unwrap()),
        ("NDJSON", Source::detect(&ndjson).unwrap()),
        ("flattened NDJSON", flattened),
        ("Parquet directory", Source::detect(&parts).unwrap()),
    ];
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let not_two = || FilterSpec {
        column: "grp".into(),
        op: FilterOp::Ne,
        value: "2".into(),
    };
    for (read, source) in &sources {
        for (column, descending, filtered) in [
            ("grp", false, false),
            ("grp", true, true),
            ("tag", false, false),
            ("tag", true, true),
            ("id", true, false),
        ] {
            // Nulls compare above every value; a stable sort keeps tied rows in the file's order.
            let key_order = |a: usize, b: usize| match column {
                "grp" => grp(a).cmp(&grp(b)),
                "tag" => match (tag(a), tag(b)) {
                    (Some(x), Some(y)) => x.cmp(&y),
                    (x, y) => x.is_none().cmp(&y.is_none()),
                },
                _ => a.cmp(&b),
            };
            let mut want: Vec<usize> = (0..rows).filter(|&i| !filtered || grp(i) != 2).collect();
            want.sort_by(|&a, &b| {
                let order = key_order(a, b);
                if descending && order != Ordering::Equal {
                    order.reverse()
                } else {
                    order
                }
            });
            let matched = want.len();
            // 49,850 reaches 49,950 and 49,950 reaches 50,050: read one way and the other, they
            // meet.
            for offset in [49_850, 49_950, matched - 60, matched + 5] {
                let spec = ScanSpec {
                    offset,
                    limit: 100,
                    sort: Some(SortSpec {
                        column: column.into(),
                        descending,
                    }),
                    filters: if filtered {
                        vec![not_two()]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                };
                let res = sql.scan(&ctx, source, &spec).unwrap();
                let got: Vec<usize> = i64s(&res.batch, "id")
                    .into_iter()
                    .map(|id| id.unwrap() as usize)
                    .collect();
                let page = &want[offset.min(matched)..(offset + 100).min(matched)];
                let what = format!(
                    "{read}: {column}, descending {descending}, filtered {filtered}, @{offset}"
                );
                assert_eq!(got, page, "{what}");
                assert_eq!(res.matched_rows, matched, "{what}");
            }
        }
    }
}

/// The local engine orders tied keys as they were read, as the SQL engine does — Arrow's sort is
/// unstable, so ties used to come out in an order of its own, and a lean build and a `sql` build
/// showed the same sorted grid differently.
#[test]
fn the_local_engine_keeps_ties_in_read_order() {
    use lakeleto::engine::{ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ties.csv");
    let mut body = String::from("id,city\n");
    for i in 0..3_000 {
        body.push_str(&format!("{i},{}\n", ["KL", "SG", "JK"][i % 3]));
    }
    std::fs::write(&path, body).unwrap();
    let source = Source::detect(&path).unwrap();
    for descending in [false, true] {
        let res = LocalReaderEngine::default()
            .scan(
                &RequestContext::detached(),
                &source,
                &ScanSpec {
                    limit: 3_000,
                    sort: Some(SortSpec {
                        column: "city".into(),
                        descending,
                    }),
                    ..Default::default()
                },
            )
            .unwrap();
        let ids: Vec<i64> = i64s(&res.batch, "id").into_iter().flatten().collect();
        // Within each city, ids ascend: the order the rows were read.
        let cities = strs(&res.batch, "city");
        for pair in ids.windows(2).zip(cities.windows(2)) {
            let ((a, b), (ca, cb)) = ((pair.0[0], pair.0[1]), (&pair.1[0], &pair.1[1]));
            if ca == cb {
                assert!(
                    a < b,
                    "descending={descending}: {a} before {b} within {ca:?}"
                );
            }
        }
    }
}

/// Pages of a sorted SQL window meet exactly, wherever a boundary falls. A window no tie touches is
/// taken from the parallel plan; one a tie touches is read again with ties broken by position in
/// the file. Both must agree with the one right order: by key, then by position.
#[cfg(feature = "sql")]
#[test]
fn sorted_sql_pages_meet_exactly_across_tied_and_untied_boundaries() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pairs.csv");
    // `pair` holds every value twice (rows 2j and 2j+1); `id` holds every value once.
    let mut body = String::from("id,pair\n");
    for i in 0..600 {
        body.push_str(&format!("{i},{}\n", i / 2));
    }
    std::fs::write(&path, body).unwrap();
    let source = Source::detect(&path).unwrap();
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let page = |column: &str, descending: bool, offset: usize| -> Vec<i64> {
        let spec = ScanSpec {
            offset,
            limit: 101,
            sort: Some(SortSpec {
                column: column.into(),
                descending,
            }),
            ..Default::default()
        };
        i64s(&sql.scan(&ctx, &source, &spec).unwrap().batch, "id")
            .into_iter()
            .map(Option::unwrap)
            .collect()
    };
    for descending in [false, true] {
        for column in ["pair", "id"] {
            // 101 rows a page, so boundaries fall both inside a tied pair and between two.
            let got: Vec<i64> = (0..600)
                .step_by(101)
                .flat_map(|offset| page(column, descending, offset))
                .collect();
            let mut want: Vec<i64> = (0..600).collect();
            let key = |i: i64| if column == "pair" { i / 2 } else { i };
            want.sort_by_key(|&i| (if descending { -key(i) } else { key(i) }, i));
            assert_eq!(got, want, "{column}, descending={descending}");
        }
    }
}

/// A sorted SQL window gives the exact count of what matches, however it is read. Over a CSV, from
/// the one pass that sorts it in file order, whose TopK takes in every match — tied or not,
/// filtered or not, and even with a filter that holds the key to one value, as the row's place in
/// the file still has to be sorted by. Over a JSON file, from the read on one partition whose TopK
/// takes in every row it numbered; or, when a filter holds the key to one value and the sort is
/// planned away — so the read stops at its window — from a count of its own.
#[cfg(feature = "sql")]
#[test]
fn a_sorted_sql_window_counts_what_matches_however_it_is_read() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{FilterOp, FilterSpec, ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    // `grp` holds seven values, `id` one per row. The CSV is over the 10 MiB at which DataFusion
    // splits one file across partitions; the JSON is streamed, a pass on one thread.
    let pad = "x".repeat(40);
    let csv = dir.path().join("groups.csv");
    let mut body = String::from("id,grp,pad\n");
    for i in 0..240_000 {
        body.push_str(&format!("{i},{},{pad}\n", i % 7));
    }
    assert!(body.len() > 10 * 1024 * 1024);
    std::fs::write(&csv, body).unwrap();
    let json = dir.path().join("groups.ndjson");
    let mut body = String::new();
    for i in 0..20_000 {
        body.push_str(&format!(
            "{{\"id\": {i}, \"grp\": {}, \"pad\": \"{pad}\"}}\n",
            i % 7
        ));
    }
    std::fs::write(&json, body).unwrap();
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    let eq = |column: &str, value: &str| {
        vec![FilterSpec {
            column: column.into(),
            op: FilterOp::Eq,
            value: value.into(),
        }]
    };
    for (path, rows) in [(csv, 240_000), (json, 20_000)] {
        let source = Source::detect(&path).unwrap();
        let threes = (0..rows).filter(|i| i % 7 == 3).count();
        for (what, column, filters, matched) in [
            ("untied", "id", Vec::new(), rows),
            ("tied", "grp", Vec::new(), rows),
            ("filtered", "id", eq("grp", "3"), threes),
            ("key held to one value", "grp", eq("grp", "3"), threes),
            ("one row", "id", eq("id", "5"), 1),
        ] {
            let spec = ScanSpec {
                limit: 10,
                sort: Some(SortSpec {
                    column: column.into(),
                    descending: true,
                }),
                filters,
                ..Default::default()
            };
            let res = sql.scan(&ctx, &source, &spec).unwrap();
            assert_eq!(res.matched_rows, matched, "{}: {what}", path.display());
        }
    }
}

/// Rows with equal sort keys come back in the order the source holds them, however DataFusion
/// spreads the source across partitions: a file of many batches read on one partition — where a
/// round-robin repartition under the numbering would deal its batches out of order — and a
/// directory of files, read in order of their paths whatever order they were written in.
#[cfg(feature = "sql")]
#[test]
fn a_sorted_sql_window_keeps_ties_in_file_order_across_batches_and_files() {
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::engine::{ScanSpec, SortSpec};
    let dir = tempfile::tempdir().unwrap();
    let csv = |ids: std::ops::Range<i64>| {
        let mut body = String::from("id,grp\n");
        for i in ids {
            body.push_str(&format!("{i},{}\n", i % 3));
        }
        body
    };
    // Some 25 batches of DataFusion's 8,192 rows, but well under the 10 MiB at which it splits a
    // file across partitions.
    let many = dir.path().join("many.csv");
    std::fs::write(&many, csv(0..200_000)).unwrap();
    // Written out of order: `b.csv` holds the middle ids, `a.csv` the first.
    let parts = dir.path().join("parts");
    std::fs::create_dir(&parts).unwrap();
    for (name, ids) in [
        ("b.csv", 1_000..2_000),
        ("a.csv", 0..1_000),
        ("c.csv", 2_000..3_000),
    ] {
        std::fs::write(parts.join(name), csv(ids)).unwrap();
    }
    let sql = DataFusionEngine::new();
    let ctx = RequestContext::detached();
    for (what, source, rows) in [
        ("many batches", Source::detect(&many).unwrap(), 200_000),
        (
            "three files",
            Source::with_format(&parts, Format::Csv),
            3_000,
        ),
    ] {
        // By `grp`, and within a `grp` by `id`: the order the rows were written in, with the files
        // read in order of their paths.
        let mut want: Vec<i64> = (0..rows).collect();
        want.sort_by_key(|&i| (i % 3, i));
        // Pages inside the first run of equal keys, and one that crosses into the next run.
        for offset in [0, rows as usize / 7, rows as usize / 3 - 50] {
            let spec = ScanSpec {
                offset,
                limit: 100,
                sort: Some(SortSpec {
                    column: "grp".into(),
                    descending: false,
                }),
                ..Default::default()
            };
            let res = sql.scan(&ctx, &source, &spec).unwrap();
            let got: Vec<i64> = i64s(&res.batch, "id")
                .into_iter()
                .map(Option::unwrap)
                .collect();
            assert_eq!(got, want[offset..offset + 100], "{what}, from row {offset}");
            assert_eq!(res.matched_rows, rows as usize, "{what}");
        }
    }
}
