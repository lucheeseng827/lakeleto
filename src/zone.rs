//! Time zones, for a build that has no time zone database.
//!
//! Arrow keeps a zoned timestamp as a UTC instant, and its zone as a string on the type: an
//! offset (`+02:00`) or a name (`UTC`, `Europe/Paris`). To print a value, arrow-cast resolves the
//! zone: an offset in every build, a name only with arrow's `chrono-tz` feature, which links the
//! tz database. DataFusion turns that feature on, so a build with `sql` (the release binaries, the
//! image, the installers) prints every zone. The default build does not link it: chrono-tz measured
//! 1.9 MB and four crates there, far over the default build's size budget. Left to arrow-cast, the
//! default build would refuse every named zone, `UTC` among them, and `UTC` is what pandas, pyarrow
//! and Polars write for tz-aware UTC data, and what the Parquet reader calls any column a file
//! marks as adjusted to UTC, as DuckDB, Spark, Trino and Iceberg write them.
//!
//! [`printable`] relabels a zone that names UTC ([`UTC_NAMES`]) as the offset `+00:00` before a
//! value is printed. The values are UTC instants already, so only the label changes, and they
//! print exactly as a build with the database prints them: `2024-01-02T03:04:05Z`. Every build
//! does this, so the default build's tests run the code the release binaries run.
//!
//! Any other name needs the database. A build that has it prints the name as it is; one without it
//! refuses ([`no_time_zone_database`]). Printing the instant in UTC instead would show a wall-clock
//! time the column does not hold, and a value labelled with its zone (`…Z[Europe/Paris]`) is one
//! that CSV and JSON readers do not parse as a time. Refusing keeps the rule that a build prints a
//! timestamp as the release binaries do, or says why it cannot.
//!
//! Only text is relabelled. A schema, and the Arrow and Parquet a result is written as, keep the
//! zone the file names.

use std::sync::{Arc, OnceLock};

use arrow_array::timezone::Tz;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, FieldRef, Schema};

use crate::error::{EngineError, Result};

/// The names the tz database gives `Etc/UTC` and `Etc/GMT`, the two zones whose offset is zero at
/// every instant, and `Z`, which ISO 8601 writes for UTC. Matched as written, as chrono-tz matches
/// them.
const UTC_NAMES: [&str; 19] = [
    "UTC",
    "Etc/UTC",
    "Z",
    "UCT",
    "Etc/UCT",
    "Universal",
    "Etc/Universal",
    "Zulu",
    "Etc/Zulu",
    "GMT",
    "Etc/GMT",
    "GMT0",
    "Etc/GMT0",
    "GMT+0",
    "Etc/GMT+0",
    "GMT-0",
    "Etc/GMT-0",
    "Greenwich",
    "Etc/Greenwich",
];

/// The offset a name of UTC is relabelled as.
const UTC_OFFSET: &str = "+00:00";

/// Whether this build links the tz database. Asked at run time because the feature is arrow's
/// (`chrono-tz`), turned on by a dependency rather than by a feature of this crate.
pub(crate) fn has_tz_database() -> bool {
    static HAS: OnceLock<bool> = OnceLock::new();
    *HAS.get_or_init(|| "Etc/UTC".parse::<Tz>().is_ok())
}

/// The refusal for a named zone other than UTC, in a build with no tz database to resolve it.
fn no_time_zone_database(zone: &str) -> EngineError {
    EngineError::Other(format!(
        "this build cannot print timestamps in the time zone `{zone}`: it has no time zone \
         database, so it prints UTC and offsets such as `+02:00` only. The release binaries and \
         the image have one, as does a build with the `sql` feature \
         (`cargo install lakeleto --features sql`)"
    ))
}

/// The zone `zone` is printed in: `None` for itself, else the offset it is relabelled as. An error
/// when this build cannot print it; `database` is whether the build has the tz database.
fn printed_as(zone: &str, database: bool) -> Result<Option<&'static str>> {
    if UTC_NAMES.contains(&zone) {
        return Ok(Some(UTC_OFFSET));
    }
    // A zone with a sign is an offset (`+02:00`, `-0530`), which arrow-cast reads, or rejects, alike
    // in every build; no name in the database starts with one. A name needs the database, and a
    // build with it reads the name, or says it is no zone, as it prints it.
    if zone.starts_with(['+', '-']) || database {
        return Ok(None);
    }
    Err(no_time_zone_database(zone))
}

/// `dt` with each zone in it replaced by the one it is printed in, through lists, structs, maps and
/// dictionaries at any depth; `None` when none changes.
fn relabel(dt: &DataType, database: bool) -> Result<Option<DataType>> {
    Ok(match dt {
        DataType::Timestamp(unit, Some(zone)) => {
            printed_as(zone, database)?.map(|to| DataType::Timestamp(*unit, Some(to.into())))
        }
        DataType::Dictionary(key, value) => relabel(value, database)?
            .map(|value| DataType::Dictionary(key.clone(), Box::new(value))),
        DataType::List(f) => relabel_field(f, database)?.map(DataType::List),
        DataType::LargeList(f) => relabel_field(f, database)?.map(DataType::LargeList),
        DataType::FixedSizeList(f, n) => {
            relabel_field(f, database)?.map(|f| DataType::FixedSizeList(f, *n))
        }
        DataType::Map(f, sorted) => relabel_field(f, database)?.map(|f| DataType::Map(f, *sorted)),
        DataType::Struct(fields) => {
            let relabelled = fields
                .iter()
                .map(|f| relabel_field(f, database))
                .collect::<Result<Vec<_>>>()?;
            if relabelled.iter().all(Option::is_none) {
                None
            } else {
                Some(DataType::Struct(
                    fields
                        .iter()
                        .zip(relabelled)
                        .map(|(f, r)| r.unwrap_or_else(|| f.clone()))
                        .collect(),
                ))
            }
        }
        _ => None,
    })
}

/// [`relabel`] for a field: the same field with the relabelled type, or `None`.
fn relabel_field(f: &FieldRef, database: bool) -> Result<Option<FieldRef>> {
    Ok(relabel(f.data_type(), database)?.map(|dt| Arc::new(f.as_ref().clone().with_data_type(dt))))
}

/// `column` as it is printed: each zone that names UTC relabelled `+00:00`, wherever [`relabel`]
/// reaches it. The same column when it holds none, and an error when it holds a zone this build
/// cannot print.
///
/// The relabelling is a cast, which for a timestamp changes its type and keeps its values.
pub(crate) fn printable(column: &ArrayRef) -> Result<ArrayRef> {
    match relabel(column.data_type(), has_tz_database())? {
        None => Ok(column.clone()),
        Some(to) => arrow_cast::cast(column, &to).map_err(EngineError::arrow),
    }
}

/// `batch` as it is printed: [`printable`] for each column, and its schema to match. Names,
/// nullability and metadata are kept, and a batch with nothing to relabel is handed back.
pub(crate) fn printable_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(printable)
        .collect::<Result<Vec<_>>>()?;
    if columns
        .iter()
        .zip(batch.columns())
        .all(|(printed, column)| Arc::ptr_eq(printed, column))
    {
        return Ok(batch.clone());
    }
    let schema = batch.schema();
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .zip(&columns)
        .map(|(f, c)| Arc::new(f.as_ref().clone().with_data_type(c.data_type().clone())))
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(schema, columns).map_err(EngineError::arrow)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::types::Int32Type;
    use arrow_array::{
        Array, DictionaryArray, FixedSizeListArray, Int32Array, Int64Array, LargeListArray,
        ListArray, MapArray, RecordBatchOptions, StringArray, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::{Field, TimeUnit};

    use super::*;

    fn ts_type(zone: &str) -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone.into()))
    }

    /// 2024-01-02T03:04:05.123456Z, 2024-06-30T23:59:59Z and a null, in `zone`.
    fn ts(zone: &str) -> ArrayRef {
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(1_704_164_645_123_456),
                Some(1_719_791_999_000_000),
                None,
            ])
            .with_timezone(zone),
        )
    }

    /// [`ts`] as a column, and inside each type it can nest in: a struct beside a field with no
    /// zone, a list of each kind, a map's values and a dictionary's.
    fn columns(zone: &str) -> Vec<ArrayRef> {
        let item = || Arc::new(Field::new_list_field(ts_type(zone), true));
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
            (Arc::new(Field::new("value", ts_type(zone), true)), ts(zone)),
        ]);
        vec![
            ts(zone),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("n", DataType::Int64, false)),
                    Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                ),
                (Arc::new(Field::new("at", ts_type(zone), true)), ts(zone)),
            ])),
            Arc::new(
                ListArray::try_new(item(), OffsetBuffer::from_lengths([1, 2]), ts(zone), None)
                    .unwrap(),
            ),
            Arc::new(
                LargeListArray::try_new(item(), OffsetBuffer::from_lengths([2, 1]), ts(zone), None)
                    .unwrap(),
            ),
            Arc::new(FixedSizeListArray::try_new(item(), 1, ts(zone), None).unwrap()),
            Arc::new(
                MapArray::try_new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    OffsetBuffer::from_lengths([1, 2]),
                    entries,
                    None,
                    false,
                )
                .unwrap(),
            ),
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![1, 0, 2]), ts(zone))
                    .unwrap(),
            ),
        ]
    }

    /// Wherever a timestamp in `UTC` sits, it comes back as the same values labelled `+00:00`,
    /// which arrow-cast prints in every build.
    #[test]
    fn utc_is_relabelled_wherever_it_sits() {
        for (column, want) in columns("UTC").iter().zip(columns(UTC_OFFSET)) {
            let printed = printable(column).unwrap();
            assert_eq!(printed.to_data(), want.to_data(), "{}", column.data_type());
        }
    }

    /// A column with nothing to relabel is handed back, not cast: an offset, at any depth, and a
    /// timestamp with no zone.
    #[test]
    fn a_column_with_nothing_to_relabel_is_handed_back() {
        let naive: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1, 2]));
        for column in columns("+02:00").into_iter().chain([naive]) {
            let printed = printable(&column).unwrap();
            assert!(Arc::ptr_eq(&printed, &column), "{}", column.data_type());
        }
    }

    #[test]
    fn every_name_of_utc_prints_as_the_zero_offset_in_every_build() {
        for database in [false, true] {
            for name in UTC_NAMES {
                assert_eq!(
                    printed_as(name, database).unwrap(),
                    Some(UTC_OFFSET),
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn an_offset_prints_as_itself_in_every_build() {
        for database in [false, true] {
            for offset in ["+00:00", "-00:00", "+02:00", "-05:30", "+0530", "+05"] {
                assert_eq!(printed_as(offset, database).unwrap(), None, "{offset}");
            }
        }
    }

    /// Any other name is printed as it is where there is a database to resolve it in, and refused
    /// where there is none, naming the zone and the builds that have one. Names are matched as
    /// written, as the database matches them, so `utc` is not UTC.
    #[test]
    fn another_name_needs_the_tz_database() {
        for name in ["Europe/Paris", "America/New_York", "utc", "Mars/Olympus"] {
            assert_eq!(printed_as(name, true).unwrap(), None, "{name}");
            let refusal = printed_as(name, false).unwrap_err().to_string();
            assert!(refusal.contains(&format!("`{name}`")), "{refusal}");
            assert!(refusal.contains("no time zone database"), "{refusal}");
            assert!(refusal.contains("release binaries"), "{refusal}");
            assert!(refusal.contains("--features sql"), "{refusal}");
        }
    }

    /// DataFusion is what links the database, so a build has it exactly when it has `sql`. Should
    /// another feature bring it in, this fails, and the refusal's advice wants another look.
    #[test]
    fn a_build_has_the_tz_database_when_it_has_sql() {
        assert_eq!(has_tz_database(), cfg!(feature = "sql"));
    }

    /// A zone this build cannot print is refused wherever it sits; where the build can, the column
    /// is handed back.
    #[test]
    fn a_zone_this_build_cannot_print_is_refused_wherever_it_sits() {
        for column in columns("Europe/Paris") {
            match printable(&column) {
                Ok(printed) => {
                    assert!(has_tz_database(), "{}", column.data_type());
                    assert!(Arc::ptr_eq(&printed, &column));
                }
                Err(e) => {
                    assert!(!has_tz_database(), "{e}");
                    assert!(e.to_string().contains("`Europe/Paris`"), "{e}");
                }
            }
        }
    }

    /// A batch keeps its names, nullability and metadata. One with nothing to relabel is handed
    /// back as it is, and one with no columns keeps its rows.
    #[test]
    fn a_batch_keeps_everything_but_the_zone() {
        let metadata = HashMap::from([("origin".to_string(), "pandas".to_string())]);
        let schema = |zone: &str| {
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("at", ts_type(zone), true).with_metadata(metadata.clone()),
                ],
                metadata.clone(),
            ))
        };
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema("UTC"), vec![ids.clone(), ts("UTC")]).unwrap();
        let printed = printable_batch(&batch).unwrap();
        assert_eq!(printed.schema(), schema(UTC_OFFSET));
        assert_eq!(printed.column(0).to_data(), ids.to_data());
        assert_eq!(printed.column(1).to_data(), ts(UTC_OFFSET).to_data());

        let offset = RecordBatch::try_new(schema("+02:00"), vec![ids, ts("+02:00")]).unwrap();
        let printed = printable_batch(&offset).unwrap();
        assert!(Arc::ptr_eq(&printed.schema(), &offset.schema()));

        let empty = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(4)),
        )
        .unwrap();
        assert_eq!(printable_batch(&empty).unwrap().num_rows(), 4);
    }

    /// Every name on the list but `Z` is the tz database's for a zone at zero offset: printed where
    /// arrow-cast resolves names, 1900, 1970, 2024 and 2100 read as they do at `+00:00`. A zone
    /// with a summer time, or an offset in its past, differs at one of them.
    #[cfg(feature = "sql")]
    #[test]
    fn each_name_of_utc_is_the_tz_databases_for_the_zero_offset() {
        use arrow_cast::display::{ArrayFormatter, FormatOptions};
        let print = |zone: &str| {
            let at = arrow_array::TimestampSecondArray::from(vec![
                -2_208_988_800,
                0,
                1_719_791_999,
                4_102_444_800,
            ])
            .with_timezone(zone);
            let f = ArrayFormatter::try_new(&at, &FormatOptions::default()).unwrap();
            (0..at.len())
                .map(|i| f.value(i).to_string())
                .collect::<Vec<_>>()
        };
        for name in UTC_NAMES.into_iter().filter(|name| *name != "Z") {
            assert_eq!(print(name), print(UTC_OFFSET), "{name}");
        }
    }
}
