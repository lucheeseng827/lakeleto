//! The default engine: a pure-Rust reader over Parquet and CSV (arrow + parquet).
//!
//! No C++ toolchain, no async runtime, no server — this is what makes `cargo build`
//! lean and what a first-run user hits when they point Lakeleto at a file. It answers
//! `schema` / `head` / `profile` directly from Arrow. It has no query planner, so
//! `query()` falls through to the trait default (a helpful "use --features sql" error).

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, SortOptions};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use super::{
    apply_scan, build_table_schema, filter_batches, profile_columns, project_rows,
    truncate_batches, window_batches, Capabilities, ColumnProfile, Engine, FilterSpec, RowBatch,
    ScanResult, ScanSpec, TableProfile, TableSchema,
};
use crate::error::{EngineError, Result};
use crate::source::{Format, Source};

/// Read Parquet/CSV locally with the Arrow reader stack.
pub struct LocalReaderEngine {
    /// Rows sampled to infer a CSV schema.
    pub csv_infer_max: usize,
    /// Batch size for streaming reads.
    pub batch_size: usize,
    /// Max rows read into memory for a sort/filter grid scan (bounded working set).
    pub scan_cap: usize,
}

impl Default for LocalReaderEngine {
    fn default() -> Self {
        Self {
            csv_infer_max: 1000,
            batch_size: 8192,
            scan_cap: 200_000,
        }
    }
}

impl LocalReaderEngine {
    /// Resolve the Iceberg read plan for `source`. A local table plans directly; an object-store
    /// table (`s3://…`) is first mirrored to a local temp dir (once per process) and planned
    /// against the mirror, with the absolute object URIs in its metadata remapped to the mirror.
    #[cfg(feature = "iceberg")]
    fn iceberg_plan(&self, source: &Source) -> Result<crate::iceberg::TablePlan> {
        #[cfg(feature = "object-store")]
        if source.is_remote() {
            let uri = source.path.to_string_lossy();
            let local = crate::objstore::materialize_prefix(uri.as_ref())?;
            return crate::iceberg::plan_object(&local, uri.as_ref());
        }
        crate::iceberg::plan(&source.path)
    }

    /// Open just the schema (+ a cheap row count when the format carries one).
    fn open_schema(&self, source: &Source) -> Result<(SchemaRef, Option<u64>)> {
        if source.is_remote() && !matches!(source.format, Format::Iceberg) {
            return self.remote_schema(source);
        }
        if is_parquet_dataset(source) {
            return self.dataset_schema(source);
        }
        match source.format {
            Format::Parquet => {
                let file = File::open(&source.path)?;
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(file).map_err(EngineError::parquet)?;
                let schema = builder.schema().clone();
                let rows = builder.metadata().file_metadata().num_rows();
                let row_count = if rows >= 0 { Some(rows as u64) } else { None };
                Ok((schema, row_count))
            }
            Format::Csv | Format::Tsv => Ok((
                self.infer_csv(&source.path, self.csv_infer_max, source.format.delimiter())?,
                None,
            )),
            #[cfg(feature = "iceberg")]
            Format::Iceberg => {
                let plan = self.iceberg_plan(source)?;
                let first = plan
                    .files
                    .first()
                    .ok_or_else(|| EngineError::UnsupportedFormat {
                        detail: format!("iceberg table {} has no data files", source.display()),
                    })?;
                let base = ParquetRecordBatchReaderBuilder::try_new(File::open(&first.path)?)
                    .map_err(EngineError::parquet)?
                    .schema()
                    .clone();
                // Report the current (evolved) schema when the metadata declares one.
                let schema = match &plan.schema {
                    Some(is) => crate::iceberg::target_schema(is, &base)?,
                    None => base,
                };
                // Equality deletes remove rows by value — their exact count isn't known without
                // scanning the data, so report the count as unknown when any are present.
                if !plan.equality_deletes.is_empty() {
                    return Ok((schema, None));
                }
                // Live row count = sum of file footers minus the positions each file deletes.
                let mut total: i64 = 0;
                for f in &plan.files {
                    let phys = ParquetRecordBatchReaderBuilder::try_new(File::open(&f.path)?)
                        .map_err(EngineError::parquet)?
                        .metadata()
                        .file_metadata()
                        .num_rows();
                    total += (phys - f.deletes.len() as i64).max(0);
                }
                Ok((schema, (total >= 0).then_some(total as u64)))
            }
            #[cfg(feature = "delta")]
            Format::Delta => {
                let plan = crate::engine::delta::plan(&source.path)?;
                Ok((
                    crate::engine::delta::schema(&plan)?,
                    Some(crate::engine::delta::row_count(&plan)?),
                ))
            }
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    /// Infer a CSV/TSV schema over up to `max_rows` rows (`delimiter` from the source format).
    fn infer_csv(
        &self,
        path: &std::path::Path,
        max_rows: usize,
        delimiter: u8,
    ) -> Result<SchemaRef> {
        let mut rdr = BufReader::new(File::open(path)?);
        let format = arrow_csv::reader::Format::default()
            .with_header(true)
            .with_delimiter(delimiter);
        let (schema, _) = format
            .infer_schema(&mut rdr, Some(max_rows))
            .map_err(EngineError::arrow)?;
        Ok(Arc::new(schema))
    }

    /// Read up to `row_limit` rows (all rows when `None`).
    fn read_batches(
        &self,
        source: &Source,
        row_limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        if source.is_remote() && !matches!(source.format, Format::Iceberg) {
            return self.remote_window(source, 0, row_limit.unwrap_or(usize::MAX));
        }
        if is_parquet_dataset(source) {
            return self.read_dataset_window(source, 0, row_limit.unwrap_or(usize::MAX));
        }
        match source.format {
            Format::Parquet => {
                let file = File::open(&source.path)?;
                let mut builder =
                    ParquetRecordBatchReaderBuilder::try_new(file).map_err(EngineError::parquet)?;
                let schema = builder.schema().clone();
                let bs = row_limit
                    .map(|n| n.clamp(1, self.batch_size))
                    .unwrap_or(self.batch_size);
                builder = builder.with_batch_size(bs);
                if let Some(n) = row_limit {
                    builder = builder.with_limit(n);
                }
                let reader = builder.build().map_err(EngineError::parquet)?;
                let mut batches = Vec::new();
                let mut rows = 0usize;
                for b in reader {
                    let b = b.map_err(EngineError::arrow)?;
                    rows += b.num_rows();
                    batches.push(b);
                    if row_limit.is_some_and(|n| rows >= n) {
                        break;
                    }
                }
                Ok((schema, batches))
            }
            Format::Csv | Format::Tsv => {
                let delim = source.format.delimiter();
                // Infer over at least the scan window: a column that only widens after
                // `csv_infer_max` rows would otherwise make the fixed-schema reader error.
                let infer_rows = row_limit
                    .map(|n| n.max(self.csv_infer_max))
                    .unwrap_or(self.csv_infer_max);
                let schema = self.infer_csv(&source.path, infer_rows, delim)?;
                let file = File::open(&source.path)?;
                let bs = row_limit
                    .map(|n| n.clamp(1, self.batch_size))
                    .unwrap_or(self.batch_size);
                let reader = arrow_csv::reader::ReaderBuilder::new(schema.clone())
                    .with_header(true)
                    .with_delimiter(delim)
                    .with_batch_size(bs)
                    .build(file)
                    .map_err(EngineError::arrow)?;
                let mut batches = Vec::new();
                let mut rows = 0usize;
                for batch in reader {
                    let batch = batch.map_err(EngineError::arrow)?;
                    rows += batch.num_rows();
                    batches.push(batch);
                    if row_limit.is_some_and(|n| rows >= n) {
                        break;
                    }
                }
                Ok((schema, batches))
            }
            #[cfg(feature = "iceberg")]
            Format::Iceberg => self.read_window(source, 0, row_limit.unwrap_or(usize::MAX), None),
            #[cfg(feature = "delta")]
            Format::Delta => self.read_window(source, 0, row_limit.unwrap_or(usize::MAX), None),
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    /// Read a specific `offset..offset+limit` row window. Parquet pushes the offset/limit
    /// into the reader (row-group skipping); CSV reads sequentially and slices; Iceberg walks
    /// the current snapshot's data files, skipping whole files by their footer row counts.
    /// `projection` (column names) is pushed into the Parquet reader so only those columns are
    /// decoded (the caller still reorders to the requested order); other formats read all columns.
    fn read_window(
        &self,
        source: &Source,
        offset: usize,
        limit: usize,
        projection: Option<&[String]>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        if source.is_remote() && !matches!(source.format, Format::Iceberg) {
            return self.remote_window(source, offset, limit);
        }
        if is_parquet_dataset(source) {
            return self.read_dataset_window(source, offset, limit);
        }
        match source.format {
            Format::Parquet => {
                let file = File::open(&source.path)?;
                let mut builder =
                    ParquetRecordBatchReaderBuilder::try_new(file).map_err(EngineError::parquet)?;
                // Column-projection push-down: decode only the requested columns.
                if let Some(cols) = projection.filter(|c| !c.is_empty()) {
                    let mask = ProjectionMask::columns(
                        builder.metadata().file_metadata().schema_descr(),
                        cols.iter().map(String::as_str),
                    );
                    builder = builder.with_projection(mask);
                }
                let bs = limit.clamp(1, self.batch_size);
                builder = builder.with_batch_size(bs);
                if offset > 0 {
                    builder = builder.with_offset(offset);
                }
                builder = builder.with_limit(limit);
                let reader = builder.build().map_err(EngineError::parquet)?;
                // Take the *projected* schema from the reader so it matches the decoded batches.
                let schema = reader.schema();
                let mut batches = Vec::new();
                let mut rows = 0usize;
                for b in reader {
                    let b = b.map_err(EngineError::arrow)?;
                    rows += b.num_rows();
                    batches.push(b);
                    if rows >= limit {
                        break;
                    }
                }
                Ok((schema, batches))
            }
            Format::Csv | Format::Tsv => {
                let (schema, batches) = self.read_batches(source, Some(offset + limit))?;
                Ok((schema, window_batches(batches, offset, limit)))
            }
            #[cfg(feature = "iceberg")]
            Format::Iceberg => {
                let plan = self.iceberg_plan(source)?;
                self.read_iceberg(source, &plan, offset, limit, None)
            }
            #[cfg(feature = "delta")]
            Format::Delta => {
                let plan = crate::engine::delta::plan(&source.path)?;
                crate::engine::delta::read_window(&plan, offset, limit)
            }
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    /// The unified schema of a multi-file Parquet dataset (a directory of `.parquet` files): the
    /// union of every file's columns in first-seen order (all nullable, since a file may omit a
    /// column), plus the summed row count. A column that appears with **conflicting types** across
    /// files is a schema mismatch and errors — the cross-file schema diff.
    fn dataset_schema(&self, source: &Source) -> Result<(SchemaRef, Option<u64>)> {
        let layout = self.dataset_layout(source)?;
        Ok((layout.full, layout.rows))
    }

    /// The physical layout of a multi-file Parquet dataset: the union of every file's data columns
    /// (first-seen order, all nullable) followed by the Hive-style partition columns parsed from
    /// `key=value` directory names (as `Utf8`, appended after the data columns). Conflicting types
    /// for a data column across files is an error. A partition key that collides with a real data
    /// column is dropped (the file's own column wins), so partition names never shadow data.
    fn dataset_layout(&self, source: &Source) -> Result<DatasetLayout> {
        let files = crate::source::list_parquet_files(&source.path);
        if files.is_empty() {
            return Err(EngineError::UnsupportedFormat {
                detail: format!("{} contains no .parquet files", source.display()),
            });
        }
        let mut fields: Vec<Field> = Vec::new();
        let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut part_keys: Vec<String> = Vec::new();
        let mut total: i64 = 0;
        for f in &files {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(f)?)
                .map_err(EngineError::parquet)?;
            total += builder.metadata().file_metadata().num_rows().max(0);
            for field in builder.schema().fields() {
                match index.get(field.name()) {
                    None => {
                        index.insert(field.name().clone(), fields.len());
                        fields.push(field.as_ref().clone().with_nullable(true));
                    }
                    Some(&i) if fields[i].data_type() != field.data_type() => {
                        return Err(EngineError::UnsupportedFormat {
                            detail: format!(
                                "parquet dataset schema mismatch: column `{}` is `{}` in one file \
                                 but `{}` in {}",
                                field.name(),
                                fields[i].data_type(),
                                field.data_type(),
                                f.display()
                            ),
                        });
                    }
                    Some(_) => {}
                }
            }
            // Discover partition keys from this file's `key=value` dirs (first-seen order).
            for (k, _) in crate::source::hive_partitions(f, &source.path) {
                if !index.contains_key(&k) && !part_keys.iter().any(|p| p == &k) {
                    part_keys.push(k);
                }
            }
        }
        let data = Arc::new(ArrowSchema::new(fields.clone()));
        for k in &part_keys {
            fields.push(Field::new(k, DataType::Utf8, true));
        }
        Ok(DatasetLayout {
            full: Arc::new(ArrowSchema::new(fields)),
            data,
            part_keys,
            rows: (total >= 0).then_some(total as u64),
        })
    }

    /// Read a `offset..offset+limit` window from a multi-file Parquet dataset: walk the files in
    /// order (skipping whole files by their footer row counts), unify each file's batches to the
    /// dataset's union schema (reorder columns, null-fill any the file omits), and accumulate.
    fn read_dataset_window(
        &self,
        source: &Source,
        offset: usize,
        limit: usize,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let layout = self.dataset_layout(source)?;
        let files = crate::source::list_parquet_files(&source.path);
        let mut batches = Vec::new();
        let mut to_skip = offset;
        let mut remaining = limit;
        for f in &files {
            if remaining == 0 {
                break;
            }
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(File::open(f)?)
                .map_err(EngineError::parquet)?;
            let frows = builder.metadata().file_metadata().num_rows().max(0) as usize;
            if to_skip >= frows {
                to_skip -= frows; // whole file precedes the window
                continue;
            }
            let bs = remaining.clamp(1, self.batch_size);
            builder = builder.with_batch_size(bs);
            if to_skip > 0 {
                builder = builder.with_offset(to_skip);
            }
            builder = builder.with_limit(remaining);
            let reader = builder.build().map_err(EngineError::parquet)?;
            // This file's Hive partition values — constant across all its rows.
            let parts = crate::source::hive_partitions(f, &source.path);
            let mut got = 0usize;
            for b in reader {
                let b = b.map_err(EngineError::arrow)?;
                got += b.num_rows();
                let unified = unify_batch(&b, &layout.data)?;
                batches.push(append_partition_cols(unified, &layout, &parts)?);
                if got >= remaining {
                    break;
                }
            }
            remaining = remaining.saturating_sub(got);
            to_skip = 0;
        }
        Ok((layout.full, batches))
    }

    /// Read a `offset..offset+limit` window from a (possibly pruned) Iceberg [`TablePlan`]:
    /// raw physical batches (delete-free fast path or the deletes filter path) unified to the
    /// current schema by evolution-aware projection. When `filters` is set (a filtered scan over a
    /// delete-free table), non-matching Parquet row groups are skipped *within* each file.
    #[cfg(feature = "iceberg")]
    fn read_iceberg(
        &self,
        source: &Source,
        plan: &crate::iceberg::TablePlan,
        offset: usize,
        limit: usize,
        filters: Option<&[FilterSpec]>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        // Deletes shift positions so they take the slower read-from-start path; delete-free
        // tables keep the footer-skip fast path (with row-group skipping when filtered).
        let (base_schema, batches) = if plan.has_deletes() {
            self.read_iceberg_with_deletes(source, plan, offset, limit)?
        } else {
            self.read_iceberg_plain(source, plan, offset, limit, filters)?
        };
        // Schema evolution: when the metadata declares a current schema, unify every file to it
        // (match by field-id, cast promoted types, null-fill added columns).
        match &plan.schema {
            Some(is) => {
                let target = crate::iceberg::target_schema(is, &base_schema)?;
                let projected = batches
                    .iter()
                    .map(|b| crate::iceberg::project_batch(b, &target))
                    .collect::<Result<Vec<_>>>()?;
                Ok((target, projected))
            }
            None => Ok((base_schema, batches)),
        }
    }

    /// Profile a Parquet file from its **footer statistics** — no row scan. Row count, per-column
    /// null count, and min/max come straight from the column chunk statistics (aggregated across
    /// all row groups), so it is near-instant even on huge files. Distinct counts and samples need
    /// the data, so they are left unset (`scanned_rows == 0` marks a footer-derived profile).
    fn profile_from_footer(&self, source: &Source) -> Result<TableProfile> {
        if source.format != Format::Parquet || is_parquet_dataset(source) {
            return Err(EngineError::Other(format!(
                "footer-statistics profiling needs a single Parquet file (got {}); \
                 run `profile` without `--fast` to scan",
                if is_parquet_dataset(source) {
                    "a parquet dataset directory"
                } else {
                    source.format.as_str()
                }
            )));
        }
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&source.path)?)
            .map_err(EngineError::parquet)?;
        let meta = builder.metadata().clone();
        let arrow_schema = builder.schema().clone();
        let parquet_schema = meta.file_metadata().schema_descr();
        let row_count = meta.file_metadata().num_rows().max(0) as u64;
        let row_groups: Vec<_> = meta.row_groups().iter().collect();

        let mut columns = Vec::with_capacity(arrow_schema.fields().len());
        let mut any_stats = false;
        for field in arrow_schema.fields() {
            let conv = StatisticsConverter::try_new(field.name(), &arrow_schema, parquet_schema)
                .map_err(EngineError::parquet)?;
            let nulls = conv
                .row_group_null_counts(row_groups.iter().copied())
                .map_err(EngineError::parquet)?;
            let mins = conv
                .row_group_mins(row_groups.iter().copied())
                .map_err(EngineError::parquet)?;
            let maxes = conv
                .row_group_maxes(row_groups.iter().copied())
                .map_err(EngineError::parquet)?;

            // Null count is exact only when every row group reported it.
            let null_complete = !nulls.is_empty() && nulls.null_count() == 0;
            let null_count: u64 = nulls.iter().flatten().sum();
            let min = array_extreme_str(&mins, false);
            let max = array_extreme_str(&maxes, true);
            if null_complete || min.is_some() || max.is_some() {
                any_stats = true;
            }
            columns.push(ColumnProfile {
                name: field.name().clone(),
                data_type: format!("{}", field.data_type()),
                null_count: if null_complete { null_count } else { 0 },
                null_fraction: if null_complete && row_count > 0 {
                    null_count as f64 / row_count as f64
                } else {
                    0.0
                },
                distinct: 0,
                distinct_capped: false,
                min,
                max,
                sample: Vec::new(),
            });
        }
        if !any_stats {
            return Err(EngineError::Other(format!(
                "{} has no column statistics in its Parquet footer — \
                 run `profile` without `--fast` to scan",
                source.display()
            )));
        }
        Ok(TableProfile {
            source: source.display(),
            engine: self.name().to_string(),
            row_count: Some(row_count),
            scanned_rows: 0, // footer-derived: no rows scanned
            columns,
        })
    }

    /// Read a bounded working set (up to `cap` rows) for a filter/sort scan or `stats`. For an
    /// Iceberg source with filters, prune data files the filters cannot match (statistics
    /// skipping) and read only the survivors — so the bounded window covers the *relevant* files,
    /// not just the first `cap` rows of the whole table. Everything else reads the plain window.
    /// Returns `(schema, batches, rows_read)`.
    fn read_working_set(
        &self,
        source: &Source,
        filters: &[FilterSpec],
        cap: usize,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>, usize)> {
        #[cfg(feature = "iceberg")]
        if source.format == Format::Iceberg && !filters.is_empty() {
            let plan = self.iceberg_plan(source)?;
            let (pruned, skipped) = crate::iceberg::prune(&plan, filters);
            if skipped > 0 && pruned.files.is_empty() {
                // Every file pruned out → an empty result under the table's schema (not an error).
                let schema = self.iceberg_schema_only(&plan)?;
                return Ok((schema, Vec::new(), 0));
            }
            let (schema, batches) = self.read_iceberg(source, &pruned, 0, cap, Some(filters))?;
            let scanned = batches.iter().map(|b| b.num_rows()).sum();
            return Ok((schema, batches, scanned));
        }
        let _ = filters;
        let (schema, batches) = self.read_window(source, 0, cap, None)?;
        let scanned = batches.iter().map(|b| b.num_rows()).sum();
        Ok((schema, batches, scanned))
    }

    /// The current (evolved) Arrow schema of an Iceberg plan, read from the first file's footer.
    #[cfg(feature = "iceberg")]
    fn iceberg_schema_only(&self, plan: &crate::iceberg::TablePlan) -> Result<SchemaRef> {
        let first = plan
            .files
            .first()
            .ok_or_else(|| EngineError::UnsupportedFormat {
                detail: "iceberg table has no data files".to_string(),
            })?;
        let base = ParquetRecordBatchReaderBuilder::try_new(File::open(&first.path)?)
            .map_err(EngineError::parquet)?
            .schema()
            .clone();
        match &plan.schema {
            Some(is) => crate::iceberg::target_schema(is, &base),
            None => Ok(base),
        }
    }

    /// Read a delete-free Iceberg table: walk the current snapshot's data files, skipping whole
    /// files by their footer row counts and pushing the residual offset/limit into the Parquet
    /// reader (row-group skipping). Returns raw (physical, pre-projection) batches.
    #[cfg(feature = "iceberg")]
    fn read_iceberg_plain(
        &self,
        source: &Source,
        plan: &crate::iceberg::TablePlan,
        offset: usize,
        limit: usize,
        filters: Option<&[FilterSpec]>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        let mut schema: Option<SchemaRef> = None;
        let mut batches = Vec::new();
        let mut to_skip = offset;
        let mut remaining = limit;
        for f in &plan.files {
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&f.path)?)
                .map_err(EngineError::parquet)?;
            if schema.is_none() {
                schema = Some(builder.schema().clone());
            }
            if remaining == 0 {
                break;
            }
            let frows = builder.metadata().file_metadata().num_rows().max(0) as usize;
            if to_skip >= frows {
                to_skip -= frows; // whole file is before the window — skip it
                continue;
            }
            // In-file row-group skipping (filtered scans only; offset is 0 there, so the
            // footer-skip path above never fires and positions aren't disturbed).
            let selected = filters.and_then(|f| {
                crate::iceberg::select_row_groups(
                    builder.metadata(),
                    builder.schema(),
                    plan.schema.as_ref(),
                    f,
                )
            });
            let bs = remaining.clamp(1, self.batch_size);
            builder = builder.with_batch_size(bs);
            if to_skip > 0 {
                builder = builder.with_offset(to_skip);
            }
            builder = builder.with_limit(remaining);
            if let Some(selected) = selected {
                builder = builder.with_row_groups(selected);
            }
            let reader = builder.build().map_err(EngineError::parquet)?;
            let mut got = 0usize;
            for b in reader {
                let b = b.map_err(EngineError::arrow)?;
                got += b.num_rows();
                batches.push(b);
                if got >= remaining {
                    break;
                }
            }
            remaining = remaining.saturating_sub(got);
            to_skip = 0;
        }
        let schema = schema.ok_or_else(|| EngineError::UnsupportedFormat {
            detail: format!("iceberg table {} has no data files", source.display()),
        })?;
        Ok((schema, batches))
    }

    /// Read an Iceberg table that carries merge-on-read deletes. Each data file is read from the
    /// start; rows at deleted **physical positions** are dropped, then rows whose **equality**
    /// keys match an applicable equality-delete (one with a higher sequence number) are dropped;
    /// the surviving (logical) rows across files are accumulated until the `offset..offset+limit`
    /// window is covered, then sliced. Bounded by the window, like the CSV path.
    #[cfg(feature = "iceberg")]
    fn read_iceberg_with_deletes(
        &self,
        source: &Source,
        plan: &crate::iceberg::TablePlan,
        offset: usize,
        limit: usize,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        let want = offset.saturating_add(limit);
        let mut schema: Option<SchemaRef> = None;
        let mut logical: Vec<arrow_array::RecordBatch> = Vec::new();
        let mut got = 0usize;
        for entry in &plan.files {
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&entry.path)?)
                .map_err(EngineError::parquet)?;
            if schema.is_none() {
                schema = Some(builder.schema().clone());
            }
            if got >= want {
                continue; // schema captured from the first file; window already covered
            }
            // Equality deletes apply only to files with a strictly lower sequence number.
            let eq: Vec<&crate::iceberg::EqualityDelete> = plan
                .equality_deletes
                .iter()
                .filter(|d| d.seq > entry.seq)
                .collect();
            builder = builder.with_batch_size(self.batch_size);
            let reader = builder.build().map_err(EngineError::parquet)?;
            let mut phys = 0i64;
            for b in reader {
                let b = b.map_err(EngineError::arrow)?;
                let rows = b.num_rows();
                let live = drop_positions(b, phys, &entry.deletes)?;
                phys += rows as i64;
                let live = apply_equality_deletes(live, &eq)?;
                got += live.num_rows();
                if live.num_rows() > 0 {
                    logical.push(live);
                }
                if got >= want {
                    break;
                }
            }
        }
        let schema = schema.ok_or_else(|| EngineError::UnsupportedFormat {
            detail: format!("iceberg table {} has no data files", source.display()),
        })?;
        Ok((schema, window_batches(logical, offset, limit)))
    }

    // --- object-store (s3://, gs://, az://) reads ------------------------------------------
    // BYO-credential reads over the same code paths as local files. Parquet is read with
    // ranged requests (larger-than-memory preserved); CSV is fetched whole. When the binary
    // lacks the `object-store` feature these return a targeted "rebuild with the feature"
    // error, so a URI never fails with an opaque filesystem message.

    #[cfg(feature = "object-store")]
    fn remote_schema(&self, source: &Source) -> Result<(SchemaRef, Option<u64>)> {
        let uri = source.path.to_string_lossy();
        match source.format {
            Format::Parquet => crate::objstore::parquet_schema(&uri),
            Format::Csv | Format::Tsv => {
                let bytes = crate::objstore::fetch_all(&uri)?;
                let delim = source.format.delimiter();
                Ok((
                    self.infer_csv_bytes(&bytes, self.csv_infer_max, delim)?,
                    None,
                ))
            }
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    #[cfg(feature = "object-store")]
    fn remote_window(
        &self,
        source: &Source,
        offset: usize,
        limit: usize,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        let uri = source.path.to_string_lossy();
        match source.format {
            Format::Parquet => {
                crate::objstore::parquet_window(&uri, offset, limit, self.batch_size)
            }
            Format::Csv | Format::Tsv => {
                // CSV can't be windowed by row remotely: fetch once, then infer + slice locally.
                let bytes = crate::objstore::fetch_all(&uri)?;
                let want = offset.saturating_add(limit);
                let delim = source.format.delimiter();
                let schema = self.infer_csv_bytes(&bytes, want.max(self.csv_infer_max), delim)?;
                let bs = limit.clamp(1, self.batch_size);
                let reader = arrow_csv::reader::ReaderBuilder::new(schema.clone())
                    .with_header(true)
                    .with_delimiter(delim)
                    .with_batch_size(bs)
                    .build(std::io::Cursor::new(&bytes))
                    .map_err(EngineError::arrow)?;
                let mut batches = Vec::new();
                let mut rows = 0usize;
                for batch in reader {
                    let batch = batch.map_err(EngineError::arrow)?;
                    rows += batch.num_rows();
                    batches.push(batch);
                    if rows >= want {
                        break;
                    }
                }
                Ok((schema, window_batches(batches, offset, limit)))
            }
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    /// Infer a CSV/TSV schema over up to `max_rows` rows of an in-memory buffer (remote CSV).
    /// `delimiter` keys off the object's extension (tab for `.tsv`) so remote `.tsv` splits the
    /// same as local `.tsv`.
    #[cfg(feature = "object-store")]
    fn infer_csv_bytes(&self, bytes: &[u8], max_rows: usize, delimiter: u8) -> Result<SchemaRef> {
        let mut rdr = std::io::Cursor::new(bytes);
        let format = arrow_csv::reader::Format::default()
            .with_header(true)
            .with_delimiter(delimiter);
        let (schema, _) = format
            .infer_schema(&mut rdr, Some(max_rows))
            .map_err(EngineError::arrow)?;
        Ok(Arc::new(schema))
    }

    #[cfg(not(feature = "object-store"))]
    fn remote_schema(&self, source: &Source) -> Result<(SchemaRef, Option<u64>)> {
        Err(remote_unavailable(source))
    }

    #[cfg(not(feature = "object-store"))]
    fn remote_window(
        &self,
        source: &Source,
        _offset: usize,
        _limit: usize,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        Err(remote_unavailable(source))
    }
}

/// Is this source a directory of Parquet files (a multi-file dataset) rather than a single file?
fn is_parquet_dataset(source: &Source) -> bool {
    source.format == Format::Parquet && !source.is_remote() && source.path.is_dir()
}

/// The physical layout of a multi-file Parquet dataset: `data` columns (the union of file schemas)
/// followed by `part_keys` Hive partition columns. `full` = `data` ++ partition columns (`Utf8`),
/// and is what dataset reads return; `data` alone is the [`unify_batch`] target per file.
struct DatasetLayout {
    /// Data columns + partition columns — the full dataset schema.
    full: SchemaRef,
    /// Data columns only (the union of file schemas); the per-file unify target.
    data: SchemaRef,
    /// Hive partition column names, appended after the data columns in `full`.
    part_keys: Vec<String>,
    /// Summed row count across files, when known.
    rows: Option<u64>,
}

/// Append the constant Hive partition columns to a data batch already unified to `layout.data`,
/// producing a batch matching `layout.full`. Each partition column carries this file's value for
/// that key (repeated for every row), or nulls when the file's path lacks the key (a mixed-depth
/// layout). Identity when the dataset has no partition columns.
fn append_partition_cols(
    batch: RecordBatch,
    layout: &DatasetLayout,
    parts: &[(String, String)],
) -> Result<RecordBatch> {
    if layout.part_keys.is_empty() {
        return Ok(batch);
    }
    let n = batch.num_rows();
    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    for key in &layout.part_keys {
        match parts.iter().find(|(k, _)| k == key) {
            Some((_, v)) => cols.push(Arc::new(StringArray::from(vec![v.clone(); n])) as ArrayRef),
            None => cols.push(arrow_array::new_null_array(&DataType::Utf8, n)),
        }
    }
    RecordBatch::try_new(layout.full.clone(), cols).map_err(EngineError::arrow)
}

/// Reorder/pad a data-file batch to the dataset's union `target` schema: each target column is
/// taken from the batch by name, or null-filled when the file omits it. Types already match by
/// construction (`dataset_schema` rejects conflicts), so no casting is needed.
fn unify_batch(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let src = batch.schema();
    let n = batch.num_rows();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for tf in target.fields() {
        match src.index_of(tf.name()) {
            Ok(i) => cols.push(batch.column(i).clone()),
            Err(_) => cols.push(arrow_array::new_null_array(tf.data_type(), n)),
        }
    }
    RecordBatch::try_new(target.clone(), cols).map_err(EngineError::arrow)
}

/// The min (`want_max=false`) or max (`want_max=true`) of a per-row-group statistics array,
/// formatted exactly as [`profile_columns`] formats scanned values (same `ArrayFormatter`), so a
/// footer-derived profile reads identically to a scanned one. `None` when the array is all-null.
fn array_extreme_str(arr: &ArrayRef, want_max: bool) -> Option<String> {
    if arr.is_empty() || arr.null_count() == arr.len() {
        return None;
    }
    // Sort with nulls last so the first index is the extreme non-null value.
    let opts = SortOptions {
        descending: want_max,
        nulls_first: false,
    };
    let idx = sort_to_indices(arr, Some(opts), None).ok()?;
    let pos = *idx.values().first()? as usize;
    let fmt = ArrayFormatter::try_new(arr.as_ref(), &FormatOptions::default()).ok()?;
    Some(fmt.value(pos).to_string())
}

/// Error for an object-store URI in a binary built without `--features object-store`.
#[cfg(not(feature = "object-store"))]
fn remote_unavailable(source: &Source) -> EngineError {
    EngineError::UnsupportedFormat {
        detail: format!(
            "{} is an object-store URI, but this binary was built without object-store support — \
             rebuild with `cargo build --features object-store`",
            source.display()
        ),
    }
}

/// Drop the rows of `batch` whose **physical** position (`phys_start + row_index`) is in `del`
/// — merge-on-read positional deletes. Identity when nothing in this batch's range is deleted.
#[cfg(feature = "iceberg")]
fn drop_positions(
    batch: arrow_array::RecordBatch,
    phys_start: i64,
    del: &std::collections::BTreeSet<i64>,
) -> Result<arrow_array::RecordBatch> {
    if del.is_empty() {
        return Ok(batch);
    }
    let n = batch.num_rows();
    let mut any = false;
    let keep: Vec<bool> = (0..n)
        .map(|i| {
            let deleted = del.contains(&(phys_start + i as i64));
            any |= deleted;
            !deleted
        })
        .collect();
    if !any {
        return Ok(batch);
    }
    let mask = arrow_array::BooleanArray::from(keep);
    arrow_select::filter::filter_record_batch(&batch, &mask).map_err(EngineError::arrow)
}

/// Drop the rows of `batch` matched by any applicable merge-on-read **equality** delete — a row
/// is deleted when, on a delete's equality field-ids, its encoded key is in that delete's key
/// set. Identity when no delete matches this batch.
#[cfg(feature = "iceberg")]
fn apply_equality_deletes(
    batch: arrow_array::RecordBatch,
    deletes: &[&crate::iceberg::EqualityDelete],
) -> Result<arrow_array::RecordBatch> {
    if deletes.is_empty() {
        return Ok(batch);
    }
    let n = batch.num_rows();
    let mut keep = vec![true; n];
    let mut any = false;
    for d in deletes {
        let keys = crate::iceberg::row_keys(&batch, &d.field_ids)?;
        for (r, k) in keys.iter().enumerate() {
            if keep[r] && d.keys.contains(k) {
                keep[r] = false;
                any = true;
            }
        }
    }
    if !any {
        return Ok(batch);
    }
    let mask = arrow_array::BooleanArray::from(keep);
    arrow_select::filter::filter_record_batch(&batch, &mask).map_err(EngineError::arrow)
}

impl Engine for LocalReaderEngine {
    fn name(&self) -> &str {
        "local"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            engine: "local (arrow/parquet/csv reader)".to_string(),
            formats: vec!["parquet".to_string(), "csv".to_string()],
            sql: false,
            profile: true,
            remote: false,
        }
    }

    fn schema(&self, source: &Source) -> Result<TableSchema> {
        let (schema, row_count) = self.open_schema(source)?;
        Ok(build_table_schema(source, self.name(), row_count, &schema))
    }

    fn preview(&self, source: &Source, limit: usize) -> Result<RowBatch> {
        let (schema, batches) = self.read_batches(source, Some(limit))?;
        Ok(RowBatch {
            schema,
            batches: truncate_batches(batches, limit),
        })
    }

    fn profile(&self, source: &Source, scan_limit: usize) -> Result<TableProfile> {
        // `scan_limit == 0` selects the near-instant footer-statistics path (Parquet only):
        // exact whole-file row count / null counts / min / max, no row scan (distinct + samples
        // are then not computed).
        if scan_limit == 0 {
            return self.profile_from_footer(source);
        }
        let row_count = self.open_schema(source)?.1;
        let (schema, batches) = self.read_batches(source, Some(scan_limit))?;
        let scanned_rows = batches.iter().map(|b| b.num_rows() as u64).sum();
        let columns = profile_columns(&schema, &batches);
        Ok(TableProfile {
            source: source.display(),
            engine: self.name().to_string(),
            row_count,
            scanned_rows,
            columns,
        })
    }

    fn scan(&self, source: &Source, spec: &ScanSpec) -> Result<ScanResult> {
        let proj = spec.projection.as_deref();
        if spec.is_plain_window() {
            // Fast path: read exactly the requested row window (offset + column projection
            // pushed into the Parquet reader), then reorder to the requested column order.
            let (schema, batches) = self.read_window(source, spec.offset, spec.limit, proj)?;
            let returned: usize = batches.iter().map(|b| b.num_rows()).sum();
            // Parquet carries an exact total in its footer; CSV does not (cheaply).
            let total = self.open_schema(source)?.1.map(|n| n as usize);
            Ok(ScanResult {
                batch: project_rows(RowBatch { schema, batches }, proj)?,
                matched_rows: total.unwrap_or(spec.offset + returned),
                total_known: total.is_some(),
                scanned_rows: returned,
                bounded: false,
                offset: spec.offset,
            })
        } else {
            // Sort/filter: read a bounded working set (Iceberg prunes non-matching files first),
            // then run Arrow kernels over it.
            let (schema, batches, scanned) =
                self.read_working_set(source, &spec.filters, self.scan_cap)?;
            let bounded = scanned >= self.scan_cap;
            let (window, matched) = apply_scan(&schema, &batches, spec)?;
            Ok(ScanResult {
                batch: project_rows(window, proj)?,
                matched_rows: matched,
                total_known: !bounded,
                scanned_rows: scanned,
                bounded,
                offset: spec.offset,
            })
        }
    }

    fn stats(
        &self,
        source: &Source,
        filters: &[FilterSpec],
        scan_limit: usize,
    ) -> Result<TableProfile> {
        // Profile the *filtered* view over a bounded working set (Iceberg prunes files first).
        let (schema, batches, scanned) = self.read_working_set(source, filters, scan_limit)?;
        let scanned_rows = scanned as u64;
        let (fschema, fbatches) = filter_batches(&schema, &batches, filters)?;
        let columns = profile_columns(&fschema, &fbatches);
        let matched: u64 = fbatches.iter().map(|b| b.num_rows() as u64).sum();
        Ok(TableProfile {
            source: source.display(),
            engine: self.name().to_string(),
            row_count: Some(matched),
            scanned_rows,
            columns,
        })
    }
}

#[cfg(all(test, feature = "object-store"))]
mod objstore_csv_tests {
    use super::LocalReaderEngine;

    #[test]
    fn infer_csv_bytes_honors_tab_delimiter() {
        let e = LocalReaderEngine::default();
        let tsv = b"a\tb\tc\n1\t2\t3\n";
        // Tab delimiter → three columns; the same bytes as comma collapse to one.
        assert_eq!(e.infer_csv_bytes(tsv, 10, b'\t').unwrap().fields().len(), 3);
        assert_eq!(e.infer_csv_bytes(tsv, 10, b',').unwrap().fields().len(), 1);
    }
}
