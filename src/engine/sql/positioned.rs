//! A listing table — a CSV, TSV or Parquet file, or a directory of them — whose rows carry their
//! place in the source, numbered as the scan reads them, so that a sorted grid window breaks ties
//! in the source's order on the parallel plan.
//!
//! DataFusion reads a listing table one group of byte ranges per partition, and lays the groups
//! out in order: its files sorted by path, each cut into consecutive ranges, so partition `p`'s
//! ranges all come before partition `p + 1`'s. (A Parquet range reads the row groups that start in
//! it, in order.) A partition reads its ranges in turn, each row once, and no others, as the scan
//! is marked order-sensitive: a partition that ran out of work would otherwise take the next range
//! queued for any of them. So the `n`th row a partition reads, numbered `p << 40 | n`, sorts where
//! it stands in the source — the order a read on one partition gives `row_number()`, without a
//! pass of its own to give it.

use std::fmt;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::filter_pushdown::{
    ChildFilterDescription, FilterDescription, FilterPushdownPhase,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;

/// `table`, with one more column after its own: each row's place in the source.
#[derive(Debug)]
pub(super) struct Positioned {
    table: Arc<dyn TableProvider>,
    schema: SchemaRef,
}

impl Positioned {
    /// `table` with its rows' places in a column called `name`, which none of its own may be.
    pub(super) fn new(table: Arc<dyn TableProvider>, name: &str) -> Self {
        let own = table.schema();
        let mut fields = own.fields().to_vec();
        fields.push(Arc::new(Field::new(name, DataType::UInt64, false)));
        let schema = Arc::new(Schema::new_with_metadata(fields, own.metadata().clone()));
        Positioned { table, schema }
    }
}

#[async_trait]
impl TableProvider for Positioned {
    /// The table's columns, then the places.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// A table of its own, as the one it wraps is.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// The table's own scan, its rows numbered as they are read. Every filter is applied above it,
    /// and reaches the scan only as the plan lets it past the numbering ([`PositionExec`]); no
    /// limit is passed down: a read that stopped early would leave the rows after it unnumbered,
    /// and the count they make untaken.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let place = self.schema.fields().len() - 1;
        let wanted = match projection {
            Some(columns) => columns.clone(),
            None => (0..=place).collect(),
        };
        let own: Vec<usize> = wanted.iter().copied().filter(|&i| i != place).collect();
        let scan = self.table.scan(state, Some(&own), &[], None).await?;
        Ok(match wanted.iter().position(|&i| i == place) {
            Some(at) => Arc::new(PositionExec::new(
                scan,
                self.schema.fields()[place].clone(),
                at,
            )),
            None => scan,
        })
    }
}

/// Its input's rows, each with its place in the source inserted at column `at`: partition `p`'s
/// `n`th row is `p << 40 | n`.
///
/// It marks the scan order-sensitive, which keeps each partition to the ranges it was given. A file
/// scan otherwise queues every partition's ranges together, and a partition that starts late or
/// runs out takes the next one in the queue, wherever it lies in the source, and numbers it as its
/// own.
///
/// Whatever is under it stays there, so nothing comes between the scan and the numbering: it does
/// not benefit from more partitions, so no round-robin repartition is put under it to deal one
/// partition's rows out across others; it does not claim to keep its input's order, so no sort is
/// pushed under it; and it lets no limit past, nor any filter but the query's own. Each partition
/// comes out in order of its places, but it does not say so either: a sort by them alone could then
/// be planned away — as one by a key a filter holds to one value is — and the TopK that counts the
/// rows with it.
#[derive(Debug)]
struct PositionExec {
    input: Arc<dyn ExecutionPlan>,
    field: FieldRef,
    at: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl PositionExec {
    /// `input`, numbered into a column `field` at `at`, with the scan kept to each partition's own
    /// ranges.
    fn new(input: Arc<dyn ExecutionPlan>, field: FieldRef, at: usize) -> Self {
        let input = input.with_preserve_order(true).unwrap_or(input);
        let schema = input.schema();
        let mut fields = schema.fields().to_vec();
        fields.insert(at, field.clone());
        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let under = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            under.partitioning.clone(),
            under.emission_type,
            under.boundedness,
        ));
        PositionExec {
            input,
            field,
            at,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for PositionExec {
    /// `PositionExec: <column>` in an `EXPLAIN`, and the column alone in its tree form, which
    /// titles each operator by its name.
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PositionExec: {}", self.field.name())
            }
            DisplayFormatType::TreeRender => write!(f, "{}", self.field.name()),
        }
    }
}

impl ExecutionPlan for PositionExec {
    /// Its name in a plan.
    fn name(&self) -> &str {
        "PositionExec"
    }

    /// Its input's partitions, with no ordering declared.
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// No: a repartition under it would number rows out of the source's order.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    /// The scan it numbers.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// The query's own filters, down to the scan: a Parquet scan skips the row groups and pages
    /// they rule out, the same ones on every pass, and numbers what is left the same way each
    /// time. Not one on the places, which the scan has not got, and nothing in the planner's last
    /// round, which hands a TopK's threshold down: that tightens as the TopK reads, so the rows
    /// it dropped before they were numbered would depend on timing — and go uncounted.
    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        let child = if phase == FilterPushdownPhase::Pre {
            let own = (0..self.schema().fields().len())
                .filter(|&i| i != self.at)
                .collect();
            ChildFilterDescription::from_child_with_allowed_indices(
                &parent_filters,
                own,
                &self.input,
            )?
        } else {
            ChildFilterDescription::all_unsupported(&parent_filters)
        };
        Ok(FilterDescription::new().with_child(child))
    }

    /// The same numbering over a new input.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "PositionExec numbers one input, not {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(PositionExec::new(
            input,
            self.field.clone(),
            self.at,
        )))
    }

    /// Partition `partition` of the input, numbered from `partition << 40` as its rows arrive.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let at = self.at;
        let output = BaselineMetrics::new(&self.metrics, partition);
        let mut next = (partition as u64) << 40;
        let rows = self.input.execute(partition, context)?.map(move |batch| {
            let batch = batch?;
            let n = batch.num_rows() as u64;
            let mut columns = batch.columns().to_vec();
            columns.insert(at, Arc::new(UInt64Array::from_iter_values(next..next + n)));
            next += n;
            output.record_output(batch.num_rows());
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), rows)))
    }

    /// How many rows it numbered, which is how many it sent.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int64Type, UInt64Type};
    use arrow_array::{ArrayRef, Int64Array};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// The ids `0..rows` as table `t` — a CSV, or a Parquet file in row groups of 300 — and as `p`,
    /// each row's place in its column `position`, in a session that cuts the file into two ranges.
    /// The directory holding the file goes with it.
    fn numbered(rows: i64, parquet: bool) -> (tempfile::TempDir, SessionContext) {
        let dir = tempfile::tempdir().unwrap();
        let path = if parquet {
            let path = dir.path().join("t.parquet");
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
            let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(300))
                .build();
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            path
        } else {
            let path = dir.path().join("t.csv");
            let mut body = String::from("id\n");
            for i in 0..rows {
                body.push_str(&format!("{i}\n"));
            }
            std::fs::write(&path, body).unwrap();
            path
        };
        let config = SessionConfig::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(0)
            .with_batch_size(100);
        let ctx = SessionContext::new_with_config(config);
        super::super::runtime()
            .block_on(async {
                let at = path.to_str().unwrap();
                if parquet {
                    ctx.register_parquet("t", at, ParquetReadOptions::default())
                        .await?;
                } else {
                    ctx.register_csv("t", at, CsvReadOptions::new()).await?;
                }
                let table = ctx.table_provider("t").await?;
                ctx.register_table("p", Arc::new(Positioned::new(table, "position")))?;
                Ok::<_, DataFusionError>(())
            })
            .unwrap();
        (dir, ctx)
    }

    /// The physical plan of `sql` in `ctx`.
    fn plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        super::super::runtime()
            .block_on(async { ctx.sql(sql).await?.create_physical_plan().await })
            .unwrap()
    }

    /// Each partition numbers its own ranges, whichever partition is read first — a CSV's, and a
    /// Parquet file's row groups. DataFusion lets a file scan's partitions share one queue of
    /// ranges, and a partition takes the next range when it starts or runs out: here partition 1
    /// starts first, and unless the scan is kept to its own ranges it takes the file's first range
    /// and numbers it after the second, which partition 0 takes when it starts.
    #[test]
    fn a_partition_numbers_its_own_ranges_whichever_is_read_first() {
        let rows = 3_000;
        for parquet in [false, true] {
            let (_dir, ctx) = numbered(rows, parquet);
            let plan = plan(&ctx, "SELECT id, position FROM p");
            let parts = plan.properties().partitioning.partition_count();
            assert_eq!(parts, 2, "the file is cut in two (Parquet: {parquet})");
            let batches = super::super::runtime()
                .block_on(async {
                    let task = ctx.task_ctx();
                    let mut batches = Vec::new();
                    let mut streams = [plan.execute(1, task.clone())?, plan.execute(0, task)?];
                    // Partition 1 starts, and partition 0 starts before partition 1 is through.
                    for stream in &mut streams {
                        batches.push(stream.next().await.expect("a first batch")?);
                    }
                    for stream in &mut streams {
                        while let Some(batch) = stream.next().await {
                            batches.push(batch?);
                        }
                    }
                    Ok::<_, DataFusionError>(batches)
                })
                .unwrap();
            let mut placed = Vec::new();
            for batch in &batches {
                let ids = batch.column(0).as_primitive::<Int64Type>().values();
                let places = batch.column(1).as_primitive::<UInt64Type>().values();
                placed.extend(places.iter().copied().zip(ids.iter().copied()));
            }
            placed.sort_unstable();
            let ids: Vec<i64> = placed.into_iter().map(|(_, id)| id).collect();
            assert_eq!(ids, (0..rows).collect::<Vec<i64>>(), "Parquet: {parquet}");
        }
    }

    /// A query's own filter reaches a Parquet scan under the numbering, to skip the row groups it
    /// rules out, but a TopK's threshold does not: it would drop rows before they were numbered,
    /// and which ones would depend on when it tightened. Without the numbering, the scan takes
    /// both.
    #[test]
    fn a_parquet_scan_takes_the_querys_filter_but_not_a_topks_threshold() {
        let (_dir, ctx) = numbered(3_000, true);
        let scan = |columns: &str, table: &str| {
            let sql =
                format!("SELECT {columns} FROM {table} WHERE id >= 2500 ORDER BY id DESC LIMIT 5");
            let shown = displayable(plan(&ctx, &sql).as_ref())
                .indent(true)
                .to_string();
            let line = shown
                .lines()
                .map(str::trim_start)
                .find(|line| line.starts_with("DataSourceExec:"));
            line.unwrap_or_else(|| panic!("{shown}")).to_string()
        };
        let numbered = scan("id, position", "p");
        assert!(numbered.contains("predicate=id@0 >= 2500"), "{numbered}");
        assert!(!numbered.contains("DynamicFilter"), "{numbered}");
        let plain = scan("id", "t");
        assert!(plain.contains("DynamicFilter"), "{plain}");
    }

    /// An `EXPLAIN` shows the numbering, by name, straight on the scan it numbers: under a sort
    /// by it, nothing comes between them to move a row from where the scan read it.
    #[test]
    fn an_explain_shows_the_numbering_straight_on_the_scan() {
        let (_dir, ctx) = numbered(3_000, false);
        let plan = plan(&ctx, "SELECT id FROM p ORDER BY position DESC LIMIT 5");
        let shown = displayable(plan.as_ref()).indent(true).to_string();
        let lines: Vec<&str> = shown.lines().map(str::trim_start).collect();
        let at = lines
            .iter()
            .position(|line| *line == "PositionExec: position");
        let under = at.and_then(|at| lines.get(at + 1)).copied();
        assert!(
            under.is_some_and(|line| line.starts_with("DataSourceExec:")),
            "{shown}"
        );
        let tree = displayable(plan.as_ref()).tree_render().to_string();
        assert!(tree.contains("PositionExec"), "{tree}");
    }
}
