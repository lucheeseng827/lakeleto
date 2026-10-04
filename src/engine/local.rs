//! The default engine: a pure-Rust reader — Parquet (files, datasets, Iceberg, Delta) itself, and
//! every other format through the `crate::format` registry (arrow + parquet).
//!
//! No C++ toolchain, no async runtime, no server — this is what makes `cargo build`
//! lean and what a first-run user hits when they point Lakeleto at a file. It answers
//! `schema` / `head` / `profile` directly from Arrow. It has no query planner, so
//! `query()` falls through to the trait default (a helpful "use --features sql" error).

use std::fs::File;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, SortOptions};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use super::flatten::{flatten_rows, flatten_schema};
use super::{
    apply_scan, build_table_schema, filter_batches, profile_columns, project_rows,
    truncate_batches, window_batches, Capabilities, ColumnProfile, Engine, FilterSpec, RowBatch,
    ScanResult, ScanSpec, TableProfile, TableSchema,
};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::format::{self, FileSchema, FormatReader, Input, ReadOptions, RemoteObject};
use crate::source::{Format, Source};

/// Read Parquet/CSV locally with the Arrow reader stack.
pub struct LocalReaderEngine {
    /// Rows sampled to infer a CSV schema.
    pub csv_infer_max: usize,
    /// Batch size for streaming reads.
    pub batch_size: usize,
    /// Max rows read into memory for a sort/filter grid scan (bounded working set).
    pub scan_cap: usize,
    /// The identity this engine's object-store reads fall back to when the *call* names none.
    ///
    /// Identity is per-caller, so it travels on the [`RequestContext`] and not here — see
    /// [`crate::context`] for the rule. What survives on the engine is a **default**, for the
    /// single-identity case that has no caller to ask: a CLI invocation reads as the process, and
    /// [`Default`] therefore sets `Some(from_env())`, which is what every existing caller gets.
    ///
    /// `None` means *no ambient default*, and is not the same as "the environment". It makes an
    /// object-store read with no identity in the context a [`EngineError::Forbidden`] rather than
    /// a read performed as the host — the posture a multi-tenant plane needs, where losing a
    /// vended credential must fail the read instead of quietly succeeding as the plane. See
    /// [`Self::without_ambient_identity`].
    #[cfg(feature = "object-store")]
    default_store_options: Option<crate::objstore::StoreOptions>,
}

impl Default for LocalReaderEngine {
    fn default() -> Self {
        Self {
            csv_infer_max: 1000,
            batch_size: 8192,
            scan_cap: 200_000,
            #[cfg(feature = "object-store")]
            default_store_options: Some(crate::objstore::StoreOptions::from_env()),
        }
    }
}

impl LocalReaderEngine {
    /// This engine, defaulting to `opts` rather than to the environment when a call names no
    /// identity of its own.
    ///
    /// Prefer [`RequestContext::with_store_options`] for a per-caller identity: that is what lets
    /// one engine serve many tenants. This sets the *fallback*, and is still the right call for a
    /// process that reads as exactly one principal for its whole life.
    #[cfg(feature = "object-store")]
    pub fn with_store_options(mut self, opts: crate::objstore::StoreOptions) -> Self {
        self.default_store_options = Some(opts);
        self
    }

    /// This engine with **no** ambient identity: an object-store read is served only if the
    /// [`RequestContext`] carries one, and refused with [`EngineError::Forbidden`] otherwise.
    ///
    /// What a shared, multi-tenant engine wants. The alternative — defaulting to the environment —
    /// turns any bug that drops a tenant's vended credential into a successful read performed as
    /// the host, which is the one outcome a credential seam exists to prevent. A loud 403 on a
    /// path that should never be taken is cheap; a silent read as the wrong principal is not.
    #[cfg(feature = "object-store")]
    pub fn without_ambient_identity(mut self) -> Self {
        self.default_store_options = None;
        self
    }

    /// The identity a remote read of `uri` runs under: the call's, else this engine's default,
    /// else a refusal.
    ///
    /// One function so the precedence is stated once. Called at the point of a remote read rather
    /// than up front, so a local read through an engine with no default is unaffected — a plane
    /// with a `--compute-local-root` still works with no credential vendor configured at all.
    #[cfg(feature = "object-store")]
    fn identity<'a>(
        &'a self,
        ctx: &'a RequestContext,
        uri: &str,
    ) -> Result<&'a crate::objstore::StoreOptions> {
        if let Some(opts) = ctx.store_options() {
            return Ok(opts);
        }
        self.default_store_options.as_ref().ok_or_else(|| {
            EngineError::Forbidden(format!(
                "no credential identity for object-store read of `{uri}`: the request context \
                 carried none and this engine has no ambient default. Refusing rather than \
                 reading as the host process"
            ))
        })
    }
}

impl LocalReaderEngine {
    /// Resolve the Iceberg read plan for `source`. A local table plans directly; an object-store
    /// table (`s3://…`) is first mirrored to a local temp dir (once per process) and planned
    /// against the mirror, with the absolute object URIs in its metadata remapped to the mirror.
    #[cfg(feature = "iceberg")]
    fn iceberg_plan(
        &self,
        #[cfg_attr(not(feature = "object-store"), allow(unused_variables))] ctx: &RequestContext,
        source: &Source,
    ) -> Result<crate::iceberg::TablePlan> {
        #[cfg(feature = "object-store")]
        if source.is_remote() {
            let uri = source.path.to_string_lossy();
            let local = crate::objstore::materialize_prefix_with(
                uri.as_ref(),
                self.identity(ctx, uri.as_ref())?,
            )?;
            return crate::iceberg::plan_object(&local, uri.as_ref());
        }
        crate::iceberg::plan(&source.path)
    }

    /// The exact row count when the format carries one cheaply (a Parquet footer, Iceberg/Delta
    /// metadata). The registry's formats are text and never do, so they are answered without
    /// opening the file: on every grid scroll, there is no count to learn from one.
    fn row_count(&self, ctx: &RequestContext, source: &Source) -> Result<Option<u64>> {
        if format::reader(source.format).is_some() {
            return Ok(None);
        }
        Ok(self.open_schema(ctx, source)?.1)
    }

    /// How this engine reads a registry format's file, for `source` — and so how SQL reads one it
    /// streams, so the two agree on its schema.
    pub(crate) fn read_options<'a>(&self, source: &'a Source) -> ReadOptions<'a> {
        ReadOptions {
            batch_size: self.batch_size,
            csv_infer_rows: self.csv_infer_max,
            json_path: source.json_path.as_deref(),
        }
    }

    /// Hand a registry format's input to `f`: the file itself, or for an object-store source the
    /// object, under the call's identity — read as it arrives by a reader that
    /// [streams objects](FormatReader::streams_objects), so a read stops fetching where it stops
    /// reading, and otherwise fetched whole. A compressed file is refused here, before a byte is
    /// read — which is where its decoder will go.
    fn with_input<T>(
        &self,
        ctx: &RequestContext,
        source: &Source,
        reader: &dyn FormatReader,
        f: impl FnOnce(Input<'_>) -> Result<T>,
    ) -> Result<T> {
        source.require_uncompressed()?;
        if source.is_remote() {
            if reader.streams_objects() {
                let object = self.remote_object(ctx, source)?;
                return f(Input::Object(object.as_ref()));
            }
            let bytes = self.fetch_remote(ctx, source)?;
            return f(Input::Bytes(&bytes));
        }
        f(Input::File(&source.path))
    }

    /// A registry format's schema, local or remote.
    fn file_schema(
        &self,
        ctx: &RequestContext,
        source: &Source,
        reader: &dyn FormatReader,
    ) -> Result<FileSchema> {
        self.with_input(ctx, source, reader, |input| {
            reader.schema(input, &self.read_options(source))
        })
    }

    /// Up to `row_limit` rows of a registry format, local or remote.
    fn file_rows(
        &self,
        ctx: &RequestContext,
        source: &Source,
        reader: &dyn FormatReader,
        row_limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        self.with_input(ctx, source, reader, |input| {
            reader.read(ctx, input, &self.read_options(source), row_limit)
        })
    }

    /// Open just the schema (+ a cheap row count when the format carries one).
    fn open_schema(
        &self,
        ctx: &RequestContext,
        source: &Source,
    ) -> Result<(SchemaRef, Option<u64>)> {
        if let Some(reader) = format::reader(source.format) {
            return Ok((self.file_schema(ctx, source, reader)?.schema, None));
        }
        if source.is_remote() && !matches!(source.format, Format::Iceberg) {
            return self.remote_schema(ctx, source);
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
            #[cfg(feature = "iceberg")]
            Format::Iceberg => {
                let plan = self.iceberg_plan(ctx, source)?;
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

    /// Read up to `row_limit` rows (all rows when `None`).
    fn read_batches(
        &self,
        ctx: &RequestContext,
        source: &Source,
        row_limit: Option<usize>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        if let Some(reader) = format::reader(source.format) {
            return self.file_rows(ctx, source, reader, row_limit);
        }
        if source.is_remote() && !matches!(source.format, Format::Iceberg) {
            return self.remote_window(ctx, source, 0, row_limit.unwrap_or(usize::MAX));
        }
        if is_parquet_dataset(source) {
            return self.read_dataset_window(ctx, source, 0, row_limit.unwrap_or(usize::MAX));
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
                    ctx.check()?;
                    let b = b.map_err(EngineError::arrow)?;
                    rows += b.num_rows();
                    batches.push(b);
                    if row_limit.is_some_and(|n| rows >= n) {
                        break;
                    }
                }
                Ok((schema, batches))
            }
            #[cfg(feature = "iceberg")]
            Format::Iceberg => {
                self.read_window(ctx, source, 0, row_limit.unwrap_or(usize::MAX), None)
            }
            #[cfg(feature = "delta")]
            Format::Delta => {
                self.read_window(ctx, source, 0, row_limit.unwrap_or(usize::MAX), None)
            }
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    /// Read a specific `offset..offset+limit` row window. Parquet pushes the offset/limit
    /// into the reader (row-group skipping); a registry format reads sequentially and slices;
    /// Iceberg walks the current snapshot's data files, skipping whole files by their footer row
    /// counts. `projection` (column names) is pushed into the Parquet reader so only those columns
    /// are decoded (the caller still reorders to the requested order); other formats read all
    /// columns.
    fn read_window(
        &self,
        ctx: &RequestContext,
        source: &Source,
        offset: usize,
        limit: usize,
        projection: Option<&[String]>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        if let Some(reader) = format::reader(source.format) {
            let through = Some(offset.saturating_add(limit));
            let (schema, batches) = self.file_rows(ctx, source, reader, through)?;
            return Ok((schema, window_batches(batches, offset, limit)));
        }
        if source.is_remote() && !matches!(source.format, Format::Iceberg) {
            return self.remote_window(ctx, source, offset, limit);
        }
        if is_parquet_dataset(source) {
            return self.read_dataset_window(ctx, source, offset, limit);
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
                    ctx.check()?;
                    let b = b.map_err(EngineError::arrow)?;
                    rows += b.num_rows();
                    batches.push(b);
                    if rows >= limit {
                        break;
                    }
                }
                Ok((schema, batches))
            }
            #[cfg(feature = "iceberg")]
            Format::Iceberg => {
                let plan = self.iceberg_plan(ctx, source)?;
                self.read_iceberg(ctx, source, &plan, offset, limit, None)
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
        ctx: &RequestContext,
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
            // Per file, not per row: a dataset can be thousands of files, and this is the
            // boundary at which abandoning the read costs nothing already read.
            ctx.check()?;
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
                ctx.check()?;
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
        ctx: &RequestContext,
        source: &Source,
        plan: &crate::iceberg::TablePlan,
        offset: usize,
        limit: usize,
        filters: Option<&[FilterSpec]>,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        // Deletes shift positions so they take the slower read-from-start path; delete-free
        // tables keep the footer-skip fast path (with row-group skipping when filtered).
        let (base_schema, batches) = if plan.has_deletes() {
            self.read_iceberg_with_deletes(ctx, source, plan, offset, limit)?
        } else {
            self.read_iceberg_plain(ctx, source, plan, offset, limit, filters)?
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

        let shown = flatten_schema(&arrow_schema, source.flatten)?;
        let mut columns = Vec::with_capacity(shown.fields().len());
        let mut any_stats = false;
        for field in shown.fields() {
            // Parquet's statistics API reaches top-level columns only, so a flattened struct's
            // fields get no footer stats — just as the struct column itself never had any.
            if arrow_schema.column_with_name(field.name()).is_none() {
                columns.push(ColumnProfile {
                    name: field.name().clone(),
                    data_type: format!("{}", field.data_type()),
                    null_count: 0,
                    null_fraction: 0.0,
                    distinct: 0,
                    distinct_capped: false,
                    min: None,
                    max: None,
                    sample: Vec::new(),
                });
                continue;
            }
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
        ctx: &RequestContext,
        source: &Source,
        filters: &[FilterSpec],
        cap: usize,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>, usize)> {
        #[cfg(feature = "iceberg")]
        if source.format == Format::Iceberg && !filters.is_empty() {
            let plan = self.iceberg_plan(ctx, source)?;
            let (pruned, skipped) = crate::iceberg::prune(&plan, filters);
            if skipped > 0 && pruned.files.is_empty() {
                // Every file pruned out → an empty result under the table's schema (not an error).
                let schema = self.iceberg_schema_only(&plan)?;
                return Ok((schema, Vec::new(), 0));
            }
            let (schema, batches) =
                self.read_iceberg(ctx, source, &pruned, 0, cap, Some(filters))?;
            let scanned = batches.iter().map(|b| b.num_rows()).sum();
            return Ok((schema, batches, scanned));
        }
        let _ = filters;
        let (schema, batches) = self.read_window(ctx, source, 0, cap, None)?;
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
        ctx: &RequestContext,
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
            // One check per data file — an Iceberg snapshot can name thousands.
            ctx.check()?;
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
        ctx: &RequestContext,
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
            // One check per data file — an Iceberg snapshot can name thousands.
            ctx.check()?;
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&entry.path)?)
                .map_err(EngineError::parquet)?;
            if schema.is_none() {
                schema = Some(builder.schema().clone());
            }
            if got >= want {
                continue; // schema captured from the first file; window already covered
            }
            // An equality delete applies to this file only when BOTH halves of the spec's rule
            // hold: a strictly higher sequence number (so rows re-inserted after a delete
            // survive), and a matching partition (so a delete written for one partition does not
            // reach into another). The second half is `applies_to_partition`, which only skips a
            // delete when the two partitions are provably different.
            let eq: Vec<&crate::iceberg::EqualityDelete> = plan
                .equality_deletes
                .iter()
                .filter(|d| {
                    if d.seq <= entry.seq {
                        return false;
                    }
                    // A partition tuple records no spec id, so two of them are only comparable when
                    // the table has exactly one partition spec. Under spec evolution, same-arity
                    // tuples may be over different source columns, and "provably different" would
                    // then skip a delete that should apply — resurfacing deleted rows, which is the
                    // one direction this reader refuses. An evolved table therefore applies the
                    // delete, erring the same way `applies_to_partition` already does when it
                    // cannot prove a difference.
                    if !plan.single_partition_spec {
                        return true;
                    }
                    d.applies_to_partition(&entry.partition)
                })
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
    // ranged requests (larger-than-memory preserved); CSV and JSON stream. When the binary
    // lacks the `object-store` feature these return a targeted "rebuild with the feature"
    // error, so a URI never fails with an opaque filesystem message.

    /// Every read here goes through the `_with` wrapper under the identity
    /// [`Self::identity`] resolved, never the bare environment-reading one. That is not a
    /// stylistic preference: the bare wrappers are what this function used to call, so an engine
    /// built for one identity read plain remote Parquet, CSV and JSON as whatever principal the
    /// process carried, while only the Iceberg mirror honoured the caller.
    #[cfg(feature = "object-store")]
    fn remote_schema(
        &self,
        ctx: &RequestContext,
        source: &Source,
    ) -> Result<(SchemaRef, Option<u64>)> {
        let uri = source.path.to_string_lossy();
        let opts = self.identity(ctx, uri.as_ref())?;
        match source.format {
            Format::Parquet => crate::objstore::parquet_schema_with(&uri, opts),
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    /// Fetch a whole object under the call's identity — what a registry format's reader that does
    /// not stream objects reads a remote file from.
    #[cfg(feature = "object-store")]
    fn fetch_remote(&self, ctx: &RequestContext, source: &Source) -> Result<Vec<u8>> {
        let uri = source.path.to_string_lossy();
        let opts = self.identity(ctx, uri.as_ref())?;
        crate::objstore::fetch_all_with(&uri, opts)
    }

    /// Look an object up under the call's identity, for a reader that streams objects to read
    /// as its bytes arrive.
    #[cfg(feature = "object-store")]
    fn remote_object(
        &self,
        ctx: &RequestContext,
        source: &Source,
    ) -> Result<Box<dyn RemoteObject>> {
        let uri = source.path.to_string_lossy();
        let opts = self.identity(ctx, uri.as_ref())?;
        Ok(Box::new(crate::objstore::object_with(&uri, opts)?))
    }

    #[cfg(feature = "object-store")]
    fn remote_window(
        &self,
        ctx: &RequestContext,
        source: &Source,
        offset: usize,
        limit: usize,
    ) -> Result<(SchemaRef, Vec<arrow_array::RecordBatch>)> {
        let uri = source.path.to_string_lossy();
        // Same identity for every format, resolved before the first byte — see `remote_schema`.
        let opts = self.identity(ctx, uri.as_ref())?;
        match source.format {
            Format::Parquet => {
                crate::objstore::parquet_window_with(&uri, offset, limit, self.batch_size, opts)
            }
            other => Err(EngineError::unsupported_format(other, self.name())),
        }
    }

    #[cfg(not(feature = "object-store"))]
    fn remote_schema(
        &self,
        _ctx: &RequestContext,
        source: &Source,
    ) -> Result<(SchemaRef, Option<u64>)> {
        Err(remote_unavailable(source))
    }

    #[cfg(not(feature = "object-store"))]
    fn fetch_remote(&self, _ctx: &RequestContext, source: &Source) -> Result<Vec<u8>> {
        Err(remote_unavailable(source))
    }

    /// Without the `object-store` feature there is no store to look an object up in.
    #[cfg(not(feature = "object-store"))]
    fn remote_object(
        &self,
        _ctx: &RequestContext,
        source: &Source,
    ) -> Result<Box<dyn RemoteObject>> {
        Err(remote_unavailable(source))
    }

    #[cfg(not(feature = "object-store"))]
    fn remote_window(
        &self,
        _ctx: &RequestContext,
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
            formats: crate::engine::readable_formats(),
            sql: false,
            profile: true,
            remote: false,
            // Overrides both: `scan` at the Arrow-kernel windowed read below, and `stats`,
            // which filters before profiling rather than taking the filter-dropping default.
            scan: true,
            filtered_stats: true,
        }
    }

    fn schema(&self, ctx: &RequestContext, source: &Source) -> Result<TableSchema> {
        ctx.check()?;
        if let Some(reader) = format::reader(source.format) {
            // Say where the rows came from when the reader chose it, so an unwrapped
            // `{"data": [...]}` is never silent.
            let file = self.file_schema(ctx, source, reader)?;
            let schema = flatten_schema(&file.schema, source.flatten)?;
            let mut ts = build_table_schema(source, self.name(), None, &schema);
            ts.records_path = file.records_path;
            return Ok(ts);
        }
        let (schema, row_count) = self.open_schema(ctx, source)?;
        let schema = flatten_schema(&schema, source.flatten)?;
        Ok(build_table_schema(source, self.name(), row_count, &schema))
    }

    fn preview(&self, ctx: &RequestContext, source: &Source, limit: usize) -> Result<RowBatch> {
        let (schema, batches) = self.read_batches(ctx, source, Some(limit))?;
        let (schema, batches) =
            flatten_rows(schema, truncate_batches(batches, limit), source.flatten)?;
        Ok(RowBatch { schema, batches })
    }

    fn profile(
        &self,
        ctx: &RequestContext,
        source: &Source,
        scan_limit: usize,
    ) -> Result<TableProfile> {
        ctx.check()?;
        // `scan_limit == 0` selects the near-instant footer-statistics path (Parquet only):
        // exact whole-file row count / null counts / min / max, no row scan (distinct + samples
        // are then not computed).
        if scan_limit == 0 {
            return self.profile_from_footer(source);
        }
        let row_count = self.row_count(ctx, source)?;
        let (schema, batches) = self.read_batches(ctx, source, Some(scan_limit))?;
        let (schema, batches) = flatten_rows(schema, batches, source.flatten)?;
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

    fn scan(&self, ctx: &RequestContext, source: &Source, spec: &ScanSpec) -> Result<ScanResult> {
        ctx.check()?;
        let proj = spec.projection.as_deref();
        if spec.is_plain_window() {
            // Fast path: read exactly the requested row window (offset + column projection
            // pushed into the Parquet reader), then reorder to the requested column order.
            // A flattened column's name is its Parquet leaf path (`user.geo.lat`), which is what
            // the reader's projection matches on, so pushing it down still selects that column.
            let (schema, batches) = self.read_window(ctx, source, spec.offset, spec.limit, proj)?;
            let (schema, batches) = flatten_rows(schema, batches, source.flatten)?;
            let returned: usize = batches.iter().map(|b| b.num_rows()).sum();
            // Parquet carries an exact total in its footer; CSV does not (cheaply).
            let total = self.row_count(ctx, source)?.map(|n| n as usize);
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
                self.read_working_set(ctx, source, &spec.filters, self.scan_cap)?;
            let (schema, batches) = flatten_rows(schema, batches, source.flatten)?;
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
        ctx: &RequestContext,
        source: &Source,
        filters: &[FilterSpec],
        scan_limit: usize,
    ) -> Result<TableProfile> {
        ctx.check()?;
        // Profile the *filtered* view over a bounded working set (Iceberg prunes files first).
        let (schema, batches, scanned) = self.read_working_set(ctx, source, filters, scan_limit)?;
        let (schema, batches) = flatten_rows(schema, batches, source.flatten)?;
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

/// The credential seam, from the engine's side.
///
/// Every assertion here is offline and credential-free. `objstore` refuses a provider whose family
/// does not match the URI's scheme *before* it builds a store — a deliberate choice, because
/// silently dropping the mismatched provider would resolve the read against ambient credentials
/// and succeed as the wrong principal. That refusal is what makes "which identity did this read
/// actually use?" observable with no network and no AWS account: an `s3://` read that comes back
/// mentioning GCS can only have gone through options carrying a GCS provider.
#[cfg(all(test, feature = "object-store"))]
mod identity_tests {
    use super::*;
    use crate::objstore::{StoreCredentials, StoreOptions};

    fn gcs_creds() -> StoreCredentials {
        let provider: object_store::gcp::GcpCredentialProvider = std::sync::Arc::new(
            object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
                bearer: "not-a-real-token".to_string(),
            }),
        );
        StoreCredentials::Gcs(provider)
    }

    /// Options that are unmistakable in an error and usable nowhere: a GCS provider aimed at an
    /// `s3://` URI.
    fn tagged_identity() -> StoreOptions {
        StoreOptions::empty()
            .with_scope("tenant-a")
            .with_credentials(gcs_creds())
    }

    fn s3(format: Format) -> Source {
        let name = match format {
            Format::Parquet => "t.parquet",
            Format::Csv => "t.csv",
            Format::Json => "t.json",
            _ => unreachable!("only the three remote-readable formats are exercised here"),
        };
        Source::with_format(format!("s3://bucket/{name}"), format)
    }

    /// The bypass this change closed, asserted at every call site that had it.
    ///
    /// `LocalReaderEngine` held store options but handed them only to the Iceberg mirror; plain
    /// remote Parquet, CSV and JSON went through the environment-reading wrappers, so an engine
    /// built for one identity read them as whatever principal the process carried. Nothing live
    /// reached it — the only caller that set non-ambient options routed Iceberg — which is what
    /// made it a trap rather than an incident, and what would have made it silent when a caller
    /// finally arrived.
    ///
    /// Each assertion below fails if its call site goes back to the bare wrapper: the engine's own
    /// default here is the environment, so ignoring the context would attempt a real S3 request
    /// instead of refusing with a family mismatch.
    #[test]
    fn a_calls_identity_reaches_every_plain_remote_read() {
        let engine = LocalReaderEngine::default(); // default identity: the environment
        let ctx = RequestContext::detached().with_store_options(tagged_identity());

        for format in [Format::Parquet, Format::Csv, Format::Json] {
            let source = s3(format);

            // `remote_schema` — parquet_schema_with / fetch_all_with
            let err = engine.schema(&ctx, &source).unwrap_err();
            assert!(
                err.to_string().contains("GCS"),
                "schema({format:?}) did not read under the call's identity: {err}"
            );

            // `remote_window` — parquet_window_with / fetch_all_with
            let err = engine
                .preview(&ctx, &source, 10)
                .err()
                .expect("a preview under a mismatched identity must not succeed");
            assert!(
                err.to_string().contains("GCS"),
                "preview({format:?}) did not read under the call's identity: {err}"
            );
        }
    }

    /// The precedence rule, stated once in `identity` and asserted here.
    #[test]
    fn the_calls_identity_wins_over_the_engines_default() {
        let engine = LocalReaderEngine::default()
            .with_store_options(StoreOptions::empty().with_scope("engine-default"));
        assert_eq!(
            engine
                .identity(&RequestContext::detached(), "s3://b/t")
                .unwrap()
                .scope_id(),
            "engine-default"
        );

        let ctx = RequestContext::detached()
            .with_store_options(StoreOptions::empty().with_scope("tenant-a"));
        assert_eq!(
            engine.identity(&ctx, "s3://b/t").unwrap().scope_id(),
            "tenant-a"
        );
    }

    /// An engine built for many tenants refuses a remote read it has no identity for, rather than
    /// performing it as the host. A dropped credential has to fail loudly — succeeding as the
    /// plane is the single outcome the seam exists to prevent — so this is a `Forbidden` (403),
    /// the same answer the plane's own credential gate gives when it cannot vend.
    #[test]
    fn without_an_ambient_identity_an_unattributed_remote_read_is_refused() {
        let engine = LocalReaderEngine::default().without_ambient_identity();
        let source = s3(Format::Parquet);

        let err = engine
            .schema(&RequestContext::detached(), &source)
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Forbidden(_)),
            "expected a refusal, got {err:?}"
        );
        assert!(
            err.to_string().contains("s3://bucket/t.parquet"),
            "the refusal should name the read it refused: {err}"
        );

        // The same engine and the same URI, once the call brings an identity: now it gets as far
        // as resolving the store, which is how we know the refusal was about the identity and not
        // about the engine being unable to read s3:// at all.
        let ctx = RequestContext::detached().with_store_options(tagged_identity());
        let err = engine.schema(&ctx, &source).unwrap_err();
        assert!(err.to_string().contains("GCS"), "{err}");
    }

    /// A local path needs no identity, so an engine with no ambient default still reads one.
    /// Otherwise a plane configured with `--compute-local-root` and no credential vendor would
    /// refuse work that never touches an object store.
    #[test]
    fn a_local_read_needs_no_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.csv");
        std::fs::write(&path, b"a,b\n1,2\n").unwrap();
        let source = Source::with_format(path, Format::Csv);
        let engine = LocalReaderEngine::default().without_ambient_identity();
        let schema = engine
            .schema(&RequestContext::detached(), &source)
            .expect("a local read must not need a credential identity");
        assert_eq!(schema.columns.len(), 2);
    }
}
