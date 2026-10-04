//! A sorted grid window deep into a table, read in two passes whose memory does not grow with the
//! window's depth.
//!
//! A window is `limit` rows from `offset`, in order of key and then of place in the source, as a
//! listing table's scan numbers its rows ([`super::Positioned`]) or `row_number()` does on one
//! partition. Read in one pass, its TopK keeps the best `offset + limit` rows in every partition,
//! and every batch any of them came in: at row 1,000,000 of a 2 million row file, most of it. Here
//! instead:
//!
//! 1. **Sample.** A pass over each row's key and place counts the rows that match, and keeps the
//!    [`SAMPLE`] whose places hash lowest — a uniform sample, and the same one on every run.
//!    Sorted, it says where the window falls in the order, give or take: a lower bound a margin
//!    before the window's first row, and an upper bound a margin after its last.
//! 2. **Band.** A pass over the window's columns counts the rows before the lower bound, and keeps
//!    those from it up to the upper: about 2% of the rows that match, plus the window. Sorted, the
//!    window is the slice of them from its offset less the rows before.
//!
//! Each row is compared by its key and place encoded as the sort orders them ([`RowConverter`]),
//! nulls and ties included, so the window is the one the one-pass read returns. The bounds are
//! estimates but the counts are exact: should the band miss the window, at odds of about one in a
//! billion, [`window`] says so and the caller reads it the one-pass way.
//!
//! A table whose every read costs more than reading its rows back from disk — a file decoded on
//! one thread, an object downloaded — can have its first pass read the window's columns, and keep
//! every row it reads on disk for the second to read back instead.

use std::collections::BinaryHeap;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{BooleanArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, SortOptions};
use arrow_select::coalesce::BatchCoalescer;
use arrow_select::concat::concat_batches;
use arrow_select::take::{take, take_record_batch};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::runtime::SpawnedTask;
use datafusion::config::SpillCompression;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::{ExecutionPlan, SpillManager};
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use crate::engine::RowBatch;
use crate::error::{EngineError, Result};

/// Rows a sorted window reaches into, its offset and its limit together, beyond which it is read
/// in two passes rather than by one TopK that keeps them all.
pub(super) const DEEP: usize = 50_000;

/// Whether the window of `limit` rows from `offset` reaches past [`DEEP`] rows, so is read in two
/// passes.
pub(super) fn is_deep(offset: usize, limit: usize) -> bool {
    offset.saturating_add(limit) > DEEP
}

/// Rows to a batch of the band's ([`band_rows`]).
const KEPT_BATCH: usize = 8_192;

/// How many rows the first pass samples. The band the second keeps is about `6 / √SAMPLE` of the
/// rows that match, 2.3% at this size, plus the window.
pub(super) const SAMPLE: usize = 1 << 16;

/// How a window's key is ordered: descending if asked, and nulls after every value — so first
/// when descending — as DataFusion orders them by default. The one-pass read says the same in SQL
/// ([`order_sql`]), so the two cannot disagree.
pub(super) fn order(descending: bool) -> SortOptions {
    SortOptions {
        descending,
        nulls_first: descending,
    }
}

/// [`order`] as SQL, after the key in an `ORDER BY`.
pub(super) fn order_sql(descending: bool) -> String {
    let order = order(descending);
    let direction = if order.descending { "DESC" } else { "ASC" };
    let nulls = if order.nulls_first { "FIRST" } else { "LAST" };
    format!("{direction} NULLS {nulls}")
}

/// The window of `limit` rows from `offset`, and how many rows match — `None` if the band missed
/// it.
///
/// `sample_sql` selects each matching row's key and place; `band_sql` the window's `columns`
/// columns, then the same key and place. The first pass samples `sample` rows.
///
/// If `spill`, the first pass reads `band_sql` instead, and writes every row it reads to a file in
/// the session's temporary directory, for the second pass to read back rather than read the table
/// again: for a table each read of which decodes its file on one thread, or downloads it. Should a
/// partition fail to keep its rows — no temporary directory, a full disk — the second pass reads
/// the table again.
#[allow(clippy::too_many_arguments)]
pub(super) async fn window(
    session: &SessionContext,
    sample_sql: &str,
    band_sql: &str,
    columns: usize,
    descending: bool,
    offset: usize,
    limit: usize,
    sample: usize,
    spill: bool,
) -> Result<Option<(RowBatch, usize)>> {
    // Where each row's key is in the first pass's rows, its place after it.
    let (first_sql, at) = if spill {
        (band_sql, columns)
    } else {
        (sample_sql, 0)
    };
    let (scan, task) = plan(session, first_sql).await?;
    let fields = vec![
        SortField::new_with_options(
            scan.schema().field(at).data_type().clone(),
            order(descending),
        ),
        SortField::new(DataType::UInt64),
    ];
    let disk = spill.then(|| kept_on_disk(session, scan.schema()));
    let passes = (0..scan.properties().partitioning.partition_count())
        .map(|p| {
            let mut rows = scan.execute(p, task.clone())?;
            let mut sampler = Sampler::new(fields.clone(), at, sample)?;
            // Without a file to write to, the rows are not kept: the second pass reads them again.
            let mut file = disk
                .as_ref()
                .and_then(|disk| disk.create_in_progress_file("a deep window's band").ok());
            Ok(SpawnedTask::spawn(async move {
                while let Some(batch) = rows.next().await {
                    let batch = batch?;
                    if file
                        .as_mut()
                        .is_some_and(|file| file.append_batch(&batch).is_err())
                    {
                        file = None;
                    }
                    sampler.push(&batch)?;
                }
                let kept = file.and_then(|mut file| file.finish().ok().flatten());
                Ok::<_, DataFusionError>((sampler, kept))
            }))
        })
        .collect::<DfResult<Vec<_>>>()
        .map_err(query)?;
    let (mut matched, mut sampled) = (0, Vec::new());
    let mut files = disk.is_some().then(Vec::new);
    for pass in passes {
        let (sampler, kept) = joined(pass).await?;
        // A partition that read rows and could not keep them leaves the second pass to read the
        // table again, and a file another one kept to be removed.
        if kept.is_none() && sampler.read > 0 {
            files = None;
        }
        if let (Some(files), Some(file)) = (files.as_mut(), kept) {
            files.push(file);
        }
        matched += sampler.read;
        sampled.extend(sampler.lowest);
    }
    if sampled.len() > sample {
        sampled.select_nth_unstable_by_key(sample - 1, |(hash, _)| *hash);
        sampled.truncate(sample);
    }
    let mut sampled: Vec<OwnedRow> = sampled.into_iter().map(|(_, row)| row).collect();
    sampled.sort_unstable();

    let second = match (disk, files) {
        (Some(disk), Some(files)) => Second::Kept(disk, files),
        _ => {
            let (scan, task) = plan(session, band_sql).await?;
            Second::Again(scan, task)
        }
    };
    let schema = match &second {
        Second::Kept(disk, _) => disk.schema().clone(),
        Second::Again(scan, _) => scan.schema(),
    };
    let shown: Vec<usize> = (0..columns).collect();
    let shown_schema = Arc::new(schema.project(&shown).map_err(query)?);
    if offset >= matched {
        let empty = RowBatch {
            schema: shown_schema,
            batches: Vec::new(),
        };
        return Ok(Some((empty, matched)));
    }
    let (lower, upper) = bounds(offset, limit, matched, sampled.len());
    let lower = lower.map(|i| sampled[i].clone());
    let upper = upper.map(|i| sampled[i].clone());
    let bands = match second {
        Second::Kept(disk, files) => files
            .into_iter()
            .map(|file| disk.read_spill_as_stream(file, None))
            .collect::<DfResult<Vec<_>>>(),
        Second::Again(scan, task) => (0..scan.properties().partitioning.partition_count())
            .map(|p| scan.execute(p, task.clone()))
            .collect::<DfResult<Vec<_>>>(),
    }
    .map_err(query)?;
    let passes: Vec<_> = bands
        .into_iter()
        .map(|rows| {
            let (lower, upper) = (lower.clone(), upper.clone());
            SpawnedTask::spawn(band_rows(rows, fields.clone(), [lower, upper], columns))
        })
        .collect();
    let (mut read, mut before, mut kept) = (0, 0, Vec::new());
    for pass in passes {
        let band = joined(pass).await?;
        read += band.read;
        before += band.before;
        kept.extend(band.kept);
    }
    // The file changed between the passes.
    if read != matched {
        return Ok(None);
    }
    let band = concat_batches(&schema, &kept).map_err(query)?;
    let Some(range) = slice(offset, limit, matched, before, band.num_rows()) else {
        return Ok(None);
    };
    let encoded = RowConverter::new(fields)
        .and_then(|rows| rows.convert_columns(&band.columns()[columns..]))
        .map_err(query)?;
    let mut sorted: Vec<u32> = (0..band.num_rows() as u32).collect();
    sorted.sort_unstable_by(|&a, &b| encoded.row(a as usize).cmp(&encoded.row(b as usize)));
    let picked = UInt32Array::from(sorted[range].to_vec());
    let rows = take_record_batch(&band, &picked)
        .and_then(|rows| rows.project(&shown))
        .map_err(query)?;
    let window = RowBatch {
        schema: shown_schema,
        batches: vec![rows],
    };
    Ok(Some((window, matched)))
}

/// What a deep window's second pass reads ([`window`]).
enum Second {
    /// The rows the first pass kept on disk: a file for each partition that read any.
    Kept(SpillManager, Vec<RefCountedTempFile>),
    /// The table, again.
    Again(Arc<dyn ExecutionPlan>, Arc<TaskContext>),
}

/// Where a first pass keeps the rows it reads, of `schema`, for the second: Arrow IPC files,
/// compressed with LZ4, in the session's temporary directory — the system's, unless its disk
/// manager says otherwise — each removed once the second pass is through with it.
fn kept_on_disk(session: &SessionContext, schema: arrow_schema::SchemaRef) -> SpillManager {
    let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    SpillManager::new(session.runtime_env(), metrics, schema)
        .with_compression_type(SpillCompression::Lz4Frame)
}

/// Where the band for the window of `limit` rows from `offset` begins and ends, of `matched` rows
/// sampled `sampled` times: the index in the sorted sample of its lower bound, `None` for the first
/// row, and of its upper bound, `None` for after the last.
///
/// Of the rows ahead of a given one, a uniform sample holds a share that varies from the rows'
/// share by a standard deviation of at most `√sampled / 2` rows. Each bound is three times
/// `√sampled` beyond its row, six deviations, so it falls on the wrong side of the window about
/// once in a billion times.
fn bounds(
    offset: usize,
    limit: usize,
    matched: usize,
    sampled: usize,
) -> (Option<usize>, Option<usize>) {
    let margin = (3.0 * (sampled as f64).sqrt()).ceil() as usize;
    let at = |rank: usize| (rank as u128 * sampled as u128 / matched as u128) as usize;
    let lower = at(offset).checked_sub(margin);
    let upper = at(offset.saturating_add(limit).min(matched)) + 1 + margin;
    (lower, (upper < sampled).then_some(upper))
}

/// The window's place among the `kept` rows of the band, sorted: from its offset less the `before`
/// rows ahead of the band, to its end or the last of `matched` rows — `None` if the band does not
/// hold all of it.
fn slice(
    offset: usize,
    limit: usize,
    matched: usize,
    before: usize,
    kept: usize,
) -> Option<Range<usize>> {
    let start = offset.checked_sub(before)?;
    let end = offset
        .saturating_add(limit)
        .min(matched)
        .checked_sub(before)?;
    (end <= kept).then_some(start..end)
}

/// `sql` planned on `session` as far as a physical plan, with the context to run it in.
async fn plan(
    session: &SessionContext,
    sql: &str,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<TaskContext>)> {
    let (_, plan, task) = super::physical(session, sql, None).await?;
    Ok((plan, task))
}

/// A pass over one partition, once it is through.
async fn joined<T: Send + 'static>(pass: SpawnedTask<DfResult<T>>) -> Result<T> {
    pass.join()
        .await
        .map_err(|e| EngineError::Query(e.to_string()))?
        .map_err(query)
}

/// One partition's part of the sample ([`window`]).
struct Sampler {
    /// Each row's key and place, encoded as the sort orders them.
    converter: RowConverter,
    /// Where each row's key is, its place after it.
    at: usize,
    /// How many rows to keep.
    sample: usize,
    /// The rows it read.
    read: usize,
    /// The `sample` of them whose places hash lowest, each as that hash and the row's key and
    /// place encoded.
    lowest: BinaryHeap<(u64, OwnedRow)>,
}

impl Sampler {
    /// A sample of `sample` rows whose key, of `fields[0]`, is at `at`, and their place after it.
    fn new(fields: Vec<SortField>, at: usize, sample: usize) -> DfResult<Sampler> {
        Ok(Sampler {
            converter: RowConverter::new(fields)?,
            at,
            sample,
            read: 0,
            lowest: BinaryHeap::with_capacity(sample),
        })
    }

    /// `batch`'s rows read, and any whose places hash low enough kept.
    fn push(&mut self, batch: &RecordBatch) -> DfResult<()> {
        self.read += batch.num_rows();
        // Only a row that hashes below the highest kept can join them.
        let ceiling = match self.lowest.peek() {
            Some((highest, _)) if self.lowest.len() == self.sample => Some(*highest),
            _ => None,
        };
        let (key, place) = (batch.column(self.at), batch.column(self.at + 1));
        let places = place.as_primitive::<UInt64Type>().values();
        let (picked, hashes): (Vec<u32>, Vec<u64>) = places
            .iter()
            .enumerate()
            .map(|(i, &place)| (i as u32, mix(place)))
            .filter(|&(_, hash)| ceiling.is_none_or(|ceiling| hash < ceiling))
            .unzip();
        if picked.is_empty() {
            return Ok(());
        }
        let picked = UInt32Array::from(picked);
        let columns = [take(key, &picked, None)?, take(place, &picked, None)?];
        let encoded = self.converter.convert_columns(&columns)?;
        for (i, hash) in hashes.into_iter().enumerate() {
            if self.lowest.len() < self.sample {
                self.lowest.push((hash, encoded.row(i).owned()));
            } else if self
                .lowest
                .peek()
                .is_some_and(|(highest, _)| hash < *highest)
            {
                self.lowest.pop();
                self.lowest.push((hash, encoded.row(i).owned()));
            }
        }
        Ok(())
    }
}

/// One partition's part of the band ([`band_rows`]).
struct Band {
    /// The rows it read.
    read: usize,
    /// How many of them come before the band.
    before: usize,
    /// The rest of them up to the band's end, copied out of the batches they came in.
    kept: Vec<RecordBatch>,
}

/// One partition's part of the band from `bounds[0]` up to `bounds[1]`, either open, of rows whose
/// key and place follow their `columns` shown columns.
///
/// The rows it keeps are copied out of the batches they came in. Filtered, a batch of strings read
/// as views (as DataFusion reads Parquet's) would keep every string it held, and a band spread
/// across the file would keep most of it.
async fn band_rows(
    mut rows: SendableRecordBatchStream,
    fields: Vec<SortField>,
    bounds: [Option<OwnedRow>; 2],
    columns: usize,
) -> DfResult<Band> {
    let converter = RowConverter::new(fields)?;
    let [lower, upper] = bounds;
    let mut kept = BatchCoalescer::new(rows.schema(), KEPT_BATCH);
    let (mut read, mut before) = (0, 0);
    while let Some(batch) = rows.next().await {
        let batch = batch?;
        read += batch.num_rows();
        let encoded = converter.convert_columns(&batch.columns()[columns..])?;
        let mut keep = Vec::with_capacity(batch.num_rows());
        for row in encoded.iter() {
            let ahead = lower.as_ref().is_some_and(|lower| row < lower.row());
            before += ahead as usize;
            keep.push(!ahead && upper.as_ref().is_none_or(|upper| row < upper.row()));
        }
        if keep.contains(&true) {
            kept.push_batch_with_filter(batch, &BooleanArray::from(keep))?;
        }
    }
    kept.finish_buffered_batch()?;
    let kept = std::iter::from_fn(|| kept.next_completed_batch()).collect();
    Ok(Band { read, before, kept })
}

/// A row's place, hashed for sampling: the SplitMix64 finalizer, which maps distinct places to
/// distinct hashes spread evenly over every bit.
fn mix(place: u64) -> u64 {
    let mut z = place.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// An Arrow or DataFusion error, as this engine's.
fn query(e: impl std::fmt::Display) -> EngineError {
    EngineError::Query(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sql::positioned::Positioned;
    use arrow_array::types::Int64Type;
    use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow_schema::SchemaRef;
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::PartitionStream;
    use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// The band reaches a margin either side of the window, and is open at an end the margin
    /// passes.
    #[test]
    fn the_band_reaches_a_margin_either_side_of_the_window() {
        // 1,000 of 100,000 rows sampled: a margin of 95 sampled rows.
        assert_eq!(bounds(50_000, 100, 100_000, 1_000), (Some(405), Some(597)));
        assert_eq!(bounds(1_000, 100, 100_000, 1_000), (None, Some(107)));
        assert_eq!(bounds(99_900, 100, 100_000, 1_000), (Some(904), None));
        // Every row sampled: each sampled row is the row of its rank.
        assert_eq!(bounds(500, 10, 1_000, 1_000), (Some(405), Some(606)));
    }

    /// The window is the band's rows from its offset less those before, when the band holds it.
    #[test]
    fn the_window_is_cut_from_the_band_only_when_the_band_holds_it() {
        assert_eq!(slice(1_000, 100, 5_000, 900, 300), Some(100..200));
        // The last page stops at the last row.
        assert_eq!(slice(4_950, 100, 5_000, 4_800, 200), Some(150..200));
        // The band starts after the window does, or ends before it does.
        assert_eq!(slice(1_000, 100, 5_000, 1_001, 300), None);
        assert_eq!(slice(1_000, 100, 5_000, 900, 199), None);
    }

    /// The sample's hash spreads consecutive places over the whole range, so a sample of the
    /// lowest is spread over the whole file.
    #[test]
    fn consecutive_places_hash_far_apart() {
        let lowest = (0..1_000_000u64)
            .filter(|&p| mix(p) < u64::MAX / 1_000)
            .count();
        assert!((900..1_100).contains(&lowest), "{lowest}");
        let early = (0..1_000_000u64)
            .filter(|&p| mix(p) < u64::MAX / 1_000 && p < 500_000)
            .count();
        assert!((400..600).contains(&early), "{early}");
    }

    /// The sample's hash is SplitMix64, a place taken as its state: the outputs published for seed
    /// 1,234,567, and the first for seed 0.
    #[test]
    fn the_sample_hash_is_splitmix64() {
        const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
        assert_eq!(mix(0), 0xE220_A839_7B1D_CDAF);
        let outputs: Vec<u64> = (0..5)
            .map(|k| mix(1_234_567u64.wrapping_add(GAMMA.wrapping_mul(k))))
            .collect();
        let published = [
            6_457_827_717_110_365_317,
            3_203_168_211_198_807_973,
            9_817_491_932_198_370_423,
            4_593_380_528_125_082_431,
            16_408_922_859_458_223_821,
        ];
        assert_eq!(outputs, published);
    }

    /// Rows with ties, nulls, a NaN and both zeros, as table `t` — a CSV or a Parquet file, in row
    /// groups of 1,000 — and as table `t_positioned`, each row's place in column `position`, read
    /// in two partitions.
    fn session(parquet: bool) -> (tempfile::TempDir, SessionContext) {
        session_on(parquet, DiskManagerBuilder::default())
    }

    /// [`session`], its temporary files kept by `disk`.
    fn session_on(parquet: bool, disk: DiskManagerBuilder) -> (tempfile::TempDir, SessionContext) {
        let dir = tempfile::tempdir().unwrap();
        let rows = 12_000;
        let val = |i: usize| match i % 11 {
            0 => None,
            1 => Some(f64::NAN),
            2 => Some(-0.0),
            3 => Some(0.0),
            _ => Some(((i * 7919) % 1_000) as f64 + 0.5),
        };
        let tag = |i: usize| (!i.is_multiple_of(4)).then(|| format!("t{}", i % 13));
        let path = if parquet {
            let path = dir.path().join("t.parquet");
            let columns: [(&str, ArrayRef); 4] = [
                ("id", Arc::new(Int64Array::from_iter_values(0..rows as i64))),
                (
                    "grp",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows as i64).map(|i| i % 5),
                    )),
                ),
                ("val", Arc::new(Float64Array::from_iter((0..rows).map(val)))),
                ("tag", Arc::new(StringArray::from_iter((0..rows).map(tag)))),
            ];
            let batch = RecordBatch::try_from_iter(columns).unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(1_000))
                .build();
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            path
        } else {
            let path = dir.path().join("t.csv");
            let mut body = String::from("id,grp,val,tag\n");
            for i in 0..rows {
                let val = val(i).map_or(String::new(), |v| format!("{v:?}"));
                let tag = tag(i).unwrap_or_default();
                body.push_str(&format!("{i},{},{val},{tag}\n", i % 5));
            }
            std::fs::write(&path, body).unwrap();
            path
        };
        let config = SessionConfig::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(0);
        let ctx = SessionContext::new_with_config_rt(config, runtime_on(disk));
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
                let positioned = Positioned::new(table, "position");
                ctx.register_table("t_positioned", Arc::new(positioned))?;
                Ok::<_, datafusion::error::DataFusionError>(())
            })
            .unwrap();
        (dir, ctx)
    }

    /// The first column of what `sql` returns in `ctx`, as integers.
    fn column(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = super::super::runtime()
            .block_on(async { ctx.sql(sql).await?.collect().await })
            .unwrap();
        batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect()
    }

    /// Every deep window of the rows `numbered` gives for each of `filters` — with their places
    /// in column `position` — is the one a one-pass read of them returns, whatever the key, its
    /// direction and the offset: ties in file order, nulls after every value and first when
    /// descending, NaN above every number and -0 below 0 — DataFusion's own order, which the
    /// one-pass read here leaves to its default. Sampled 512 times, so the band is cut from a
    /// sample, not from every row; its rows kept on disk between the passes if `spill`.
    fn deep_windows_match(
        ctx: &SessionContext,
        numbered: impl Fn(&str) -> String,
        filters: &[&str],
        spill: bool,
    ) {
        for filter in filters {
            let rows = numbered(filter);
            let matched = column(ctx, &format!("SELECT count(*) FROM {rows}"))[0] as usize;
            let offsets = [0, 777, matched / 2, matched - 150, matched - 1, matched + 9];
            for key in ["grp", "val", "tag", "id"] {
                for descending in [false, true] {
                    let dir = if descending { "DESC" } else { "ASC" };
                    let sample_sql = format!("SELECT {key} AS k, position FROM {rows}");
                    let band_sql = format!("SELECT id, {key} AS k, position FROM {rows}");
                    for offset in offsets {
                        let what = format!("{key} {dir}{filter} @{offset}");
                        let one_pass = column(
                            ctx,
                            &format!(
                                "SELECT id FROM {rows} ORDER BY {key} {dir}, position LIMIT 100 \
                                 OFFSET {offset}"
                            ),
                        );
                        let read = window(
                            ctx,
                            &sample_sql,
                            &band_sql,
                            1,
                            descending,
                            offset,
                            100,
                            512,
                            spill,
                        );
                        let (rows, counted) = super::super::runtime()
                            .block_on(read)
                            .unwrap()
                            .unwrap_or_else(|| panic!("{what}: the band missed the window"));
                        let ids: Vec<i64> = rows
                            .batches
                            .iter()
                            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
                            .collect();
                        assert_eq!(ids, one_pass, "{what}");
                        assert_eq!(counted, matched, "{what}");
                    }
                }
            }
        }
    }

    /// Over a CSV numbered as its two partitions read it.
    #[test]
    fn a_deep_window_is_the_one_a_one_pass_read_returns() {
        let (_dir, ctx) = session(false);
        deep_windows_match(
            &ctx,
            |filter| format!("t_positioned{filter}"),
            &["", " WHERE grp <> 3", " WHERE tag IS NULL OR val > 400"],
            false,
        );
    }

    /// Over a Parquet file numbered as its two partitions read its row groups, with a filter by
    /// which the scan skips some of them.
    #[test]
    fn a_deep_window_over_parquet_is_the_one_a_one_pass_read_returns() {
        let (_dir, ctx) = session(true);
        deep_windows_match(
            &ctx,
            |filter| format!("t_positioned{filter}"),
            &[
                "",
                " WHERE grp <> 3",
                " WHERE id >= 4500 AND (tag IS NULL OR val > 400)",
            ],
            false,
        );
    }

    /// Over rows `row_number()` numbers on one partition, as a table that is not a file's is
    /// numbered.
    #[test]
    fn a_deep_window_numbered_on_one_partition_is_the_one_a_one_pass_read_returns() {
        let (_dir, ctx) = session(true);
        column(&ctx, "SET datafusion.execution.target_partitions = 1");
        deep_windows_match(
            &ctx,
            |filter| format!("(SELECT *, row_number() OVER () AS position FROM t{filter})"),
            &["", " WHERE tag IS NULL OR val > 400"],
            false,
        );
    }

    /// A runtime whose temporary files `disk` keeps.
    fn runtime_on(disk: DiskManagerBuilder) -> Arc<RuntimeEnv> {
        RuntimeEnvBuilder::new()
            .with_disk_manager_builder(disk)
            .build_arc()
            .unwrap()
    }

    /// A table that holds its rows for its first read only, as a file replaced by an empty one
    /// would: a second pass over it reads nothing.
    #[derive(Debug)]
    struct ReadOnce {
        rows: RecordBatch,
        read: AtomicBool,
    }

    impl PartitionStream for ReadOnce {
        fn schema(&self) -> &SchemaRef {
            self.rows.schema_ref()
        }

        fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
            let rows = match self.read.swap(true, Ordering::SeqCst) {
                false => vec![Ok(self.rows.clone())],
                true => Vec::new(),
            };
            let rows = futures::stream::iter(rows);
            Box::pin(RecordBatchStreamAdapter::new(self.rows.schema(), rows))
        }
    }

    /// A session on one partition, its temporary files kept by `disk`, with table `t` the ids
    /// `0..20_000` and their `grp`, `id % 7` — readable once.
    fn read_once(disk: DiskManagerBuilder) -> SessionContext {
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..20_000));
        let grps: ArrayRef = Arc::new(Int64Array::from_iter_values((0..20_000).map(|i| i % 7)));
        let rows = RecordBatch::try_from_iter([("id", ids), ("grp", grps)]).unwrap();
        let schema = rows.schema();
        let table = ReadOnce {
            rows,
            read: AtomicBool::new(false),
        };
        let table = StreamingTable::try_new(schema, vec![Arc::new(table)]).unwrap();
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config_rt(config, runtime_on(disk));
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    /// The deep window at row 10,000 of [`read_once`]'s table by `grp`, kept on disk between the
    /// passes if `spill`: `None` if the band missed it, which the second pass reading the table
    /// again, empty by then, makes it.
    fn read_once_window(ctx: &SessionContext, spill: bool) -> Option<(Vec<i64>, usize)> {
        let rows = "(SELECT *, row_number() OVER () AS position FROM t)";
        let sample_sql = format!("SELECT grp AS k, position FROM {rows}");
        let band_sql = format!("SELECT id, grp AS k, position FROM {rows}");
        let read = window(
            ctx,
            &sample_sql,
            &band_sql,
            1,
            false,
            10_000,
            100,
            512,
            spill,
        );
        let (rows, counted) = super::super::runtime().block_on(read).unwrap()?;
        let ids = rows
            .batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect();
        Some((ids, counted))
    }

    /// Kept on disk, a deep window's rows are read back for the second pass, not read again: a
    /// table read once gives the window a sort of its rows does, and the file is gone after. Not
    /// kept, the second pass reads the table again, and finds it empty.
    #[test]
    fn a_deep_window_kept_on_disk_is_read_back_not_read_again() {
        let mut want: Vec<i64> = (0..20_000).collect();
        want.sort_by_key(|&i| (i % 7, i));
        let ctx = read_once(DiskManagerBuilder::default());
        let kept = read_once_window(&ctx, true);
        assert_eq!(kept, Some((want[10_000..10_100].to_vec(), 20_000)));
        assert_eq!(ctx.runtime_env().disk_manager.used_disk_space(), 0);
        let ctx = read_once(DiskManagerBuilder::default());
        assert_eq!(read_once_window(&ctx, false), None);
    }

    /// Rows that cannot be kept on disk — no temporary directory, or one too small for them — are
    /// read again by the second pass: the table read once is empty by then, and any other table
    /// gives the window a one-pass read returns.
    #[test]
    fn a_deep_window_that_cannot_keep_its_rows_reads_them_again() {
        let disks = || {
            [
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
                DiskManagerBuilder::default().with_max_temp_directory_size(64),
            ]
        };
        for disk in disks() {
            assert_eq!(read_once_window(&read_once(disk), true), None);
        }
        for disk in disks() {
            let (_dir, ctx) = session_on(true, disk);
            column(&ctx, "SET datafusion.execution.target_partitions = 1");
            deep_windows_match(
                &ctx,
                |filter| format!("(SELECT *, row_number() OVER () AS position FROM t{filter})"),
                &[""],
                true,
            );
        }
    }

    /// Kept on disk between the passes — a file for each of a CSV's two partitions, numbered as
    /// they read it, and one for rows `row_number()` numbers on one partition — and the files gone
    /// after.
    #[test]
    fn a_deep_window_kept_on_disk_is_the_one_a_one_pass_read_returns() {
        let (_dir, ctx) = session(false);
        deep_windows_match(
            &ctx,
            |filter| format!("t_positioned{filter}"),
            &["", " WHERE grp <> 3"],
            true,
        );
        assert_eq!(ctx.runtime_env().disk_manager.used_disk_space(), 0);
        let (_dir, ctx) = session(true);
        column(&ctx, "SET datafusion.execution.target_partitions = 1");
        deep_windows_match(
            &ctx,
            |filter| format!("(SELECT *, row_number() OVER () AS position FROM t{filter})"),
            &["", " WHERE tag IS NULL OR val > 400"],
            true,
        );
        assert_eq!(ctx.runtime_env().disk_manager.used_disk_space(), 0);
    }
}
