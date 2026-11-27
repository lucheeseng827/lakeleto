//! Snapshot addresses, `<table>@<snapshot>`, and a table described by its metadata.
//!
//! A snapshot address names a table as it was at one of its snapshots (ADR-0002 §1; the release
//! plan's §8 decision 5):
//!
//! - `@<digits>` is the table's own identifier: an Iceberg snapshot id, a Delta version.
//! - `@<RFC 3339 instant>`, such as `@2026-10-01T00:00:00Z`, is the snapshot current at that
//!   instant.
//!
//! It is read for `diff`, and by the snapshot's **metadata** alone ([`TableMetadata`]): its schema,
//! its row count and its columns' statistics as the table's manifests or log record them. No data
//! file is read, so a snapshot whose data files were since removed is still compared. Reading a
//! snapshot's rows is out of scope.
//!
//! A statistic is given only where the metadata makes it exact, and is `None` otherwise, with a
//! note saying why: a difference `diff` finds then means a difference in the data, never one in how
//! a writer rounded or cut a bound. See each format's reader for what that leaves out.

#[cfg(any(feature = "iceberg", feature = "delta"))]
use arrow_array::{Array, ArrayRef};

use crate::diff::ColumnStats;
use crate::engine::ColumnSchema;
use crate::error::EngineError;
#[cfg(any(feature = "iceberg", feature = "delta"))]
use crate::error::Result;

/// Which snapshot of a table an address names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotRef {
    /// The table's own identifier: an Iceberg snapshot id, a Delta version.
    Id(i64),
    /// The snapshot current at this instant, in milliseconds since the epoch, and as written.
    At { ms: i64, text: String },
}

impl SnapshotRef {
    /// Parse what follows an `@`: digits, or an RFC 3339 instant with its zone. `None` for anything
    /// else, which is then no snapshot at all: the `@` is part of the name.
    pub fn parse(s: &str) -> Option<SnapshotRef> {
        if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
            return s.parse().ok().map(SnapshotRef::Id);
        }
        rfc3339_ms(s).map(|ms| SnapshotRef::At {
            ms,
            text: s.to_string(),
        })
    }
}

impl std::fmt::Display for SnapshotRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotRef::Id(id) => write!(f, "{id}"),
            SnapshotRef::At { text, .. } => f.write_str(text),
        }
    }
}

/// An RFC 3339 instant, `YYYY-MM-DDTHH:MM:SS[.fff]` and a zone (`Z` or `±HH:MM`), in milliseconds
/// since the epoch, rounded down. A date alone, a time with no zone, or any space in it, is not
/// one: an instant is unambiguous, and its shape is checked whole before it is read.
fn rfc3339_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let digits =
        |r: std::ops::Range<usize>| b.get(r).is_some_and(|d| d.iter().all(u8::is_ascii_digit));
    let date_time = b.len() >= 20
        && digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && matches!(b[10], b'T' | b't')
        && digits(11..13)
        && b[13] == b':'
        && digits(14..16)
        && b[16] == b':'
        && digits(17..19);
    if !date_time {
        return None;
    }
    let mut rest = &b[19..];
    if let Some(fraction) = rest.strip_prefix(b".") {
        let n = fraction.iter().take_while(|d| d.is_ascii_digit()).count();
        if n == 0 {
            return None;
        }
        rest = &fraction[n..];
    }
    let zoned = matches!(rest, [b'Z' | b'z'])
        || matches!(rest, [b'+' | b'-', h1, h2, b':', m1, m2]
            if [h1, h2, m1, m2].iter().all(|d| d.is_ascii_digit()));
    if !zoned {
        return None;
    }
    // Read as a date-time, not as nanoseconds, which end in 2262.
    let utc: arrow_array::timezone::Tz = "+00:00".parse().ok()?;
    let at = arrow_cast::parse::string_to_datetime(&utc, s).ok()?;
    Some(at.timestamp_millis())
}

/// The table a reference names and the snapshot its trailing `@` asks for, when it asks for one.
///
/// The `@` is looked for in the reference's last segment only: one before it belongs to the path
/// or to a host, as in `abfss://container@account.dfs.core.windows.net/t`. What follows it must be
/// a snapshot ([`SnapshotRef::parse`]); otherwise the reference asks for none. Whether a reference
/// that exists whole as a file is a path is the caller's to decide first (ADR-0002: it is).
pub fn split(reference: &str) -> Option<(&str, SnapshotRef)> {
    let last = reference.rfind(['/', '\\']).map_or(0, |i| i + 1);
    let at = last + reference[last..].rfind('@')?;
    let (table, snapshot) = (&reference[..at], &reference[at + 1..]);
    if table.is_empty() || table.ends_with(['/', '\\']) {
        return None;
    }
    Some((table, SnapshotRef::parse(snapshot)?))
}

/// How [`only_diff_reads`]'s hint starts, which is how [`is_only_diff_reads`] tells it.
const ONLY_DIFF_READS: &str =
    "a snapshot is compared by `lakeleto diff`, by its metadata; its rows are not read.";

/// The refusal for a snapshot address given to anything but `diff`, which alone reads one.
///
/// It is also how resolving a name says that nothing is there by the whole of it, so that its
/// trailing `@` is a snapshot address: `diff` reads it so ([`is_only_diff_reads`]).
pub fn only_diff_reads(reference: &str, table: &str, at: &SnapshotRef) -> EngineError {
    EngineError::UnsupportedOperation {
        engine: "local".to_string(),
        op: format!("read `{reference}`, which names table `{table}` at snapshot `{at}`"),
        hint: format!("{ONLY_DIFF_READS} Drop `@{at}` to read the table as it is now"),
    }
}

/// Whether `err` is [`only_diff_reads`]'s refusal: the name resolved was nothing by its whole
/// name, and ends in a snapshot address. Any other error, a failed request among them, is not.
pub fn is_only_diff_reads(err: &EngineError) -> bool {
    matches!(err, EngineError::UnsupportedOperation { hint, .. } if hint.starts_with(ONLY_DIFF_READS))
}

/// An instant, in milliseconds since the epoch, as RFC 3339 in UTC, for a message.
pub fn instant(ms: i64) -> String {
    let at = arrow_array::TimestampMillisecondArray::from(vec![ms]).with_timezone("+00:00");
    crate::diff::cell_at(&at, 0).unwrap_or_else(|| format!("{ms} ms since the epoch"))
}

/// A table described by its metadata at one snapshot, for `diff`: nothing in it was read from a
/// data file.
#[derive(Debug, Clone, Default)]
pub struct TableMetadata {
    /// The table, as messages name it.
    pub source: String,
    pub format: String,
    pub engine: String,
    /// The snapshot described: an Iceberg snapshot id or a Delta version. `None` for an Iceberg
    /// table with no snapshot yet.
    pub snapshot: Option<String>,
    /// When that snapshot was committed, in milliseconds since the epoch, when its metadata says.
    pub committed_ms: Option<i64>,
    pub columns: Vec<ColumnSchema>,
    /// The live rows, when the metadata counts them exactly.
    pub rows: Option<u64>,
    /// The data files the snapshot holds.
    pub files: u64,
    /// Each column's statistics, every one `None` that the metadata does not make exact.
    pub stats: Vec<(String, ColumnStats)>,
    /// What a reader should know: the statistics left out, and why.
    pub notes: Vec<String>,
}

impl TableMetadata {
    /// Add `note`, once.
    #[cfg(any(feature = "iceberg", feature = "delta"))]
    pub(crate) fn note(&mut self, note: impl Into<String>) {
        let note = note.into();
        if !self.notes.contains(&note) {
            self.notes.push(note);
        }
    }
}

/// The least and the greatest of a column's `values`, each array one or more values of the
/// column's type, as `diff` prints a value. Nulls are passed over; `None` when there is no other.
#[cfg(any(feature = "iceberg", feature = "delta"))]
pub(crate) fn extremes(values: &[ArrayRef]) -> Result<(Option<String>, Option<String>)> {
    if values.is_empty() {
        return Ok((None, None));
    }
    let refs: Vec<&dyn Array> = values.iter().map(|a| a.as_ref()).collect();
    let all = arrow_select::concat::concat(&refs).map_err(EngineError::arrow)?;
    let first = |descending: bool| -> Result<Option<String>> {
        let order = arrow_schema::SortOptions {
            descending,
            nulls_first: false,
        };
        let at = arrow_ord::sort::sort_to_indices(&all, Some(order), Some(1))
            .map_err(EngineError::arrow)?;
        Ok(match at.len() {
            0 => None,
            _ => crate::diff::cell_at(&all, at.value(0) as usize),
        })
    };
    Ok((first(false)?, first(true)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_is_digits_or_an_instant_with_its_zone() {
        assert_eq!(SnapshotRef::parse("42"), Some(SnapshotRef::Id(42)));
        assert_eq!(
            SnapshotRef::parse("4182309823479"),
            Some(SnapshotRef::Id(4_182_309_823_479))
        );
        assert_eq!(
            SnapshotRef::parse("2026-10-01T00:00:00Z"),
            Some(SnapshotRef::At {
                ms: 1_790_812_800_000,
                text: "2026-10-01T00:00:00Z".to_string()
            })
        );
        assert_eq!(
            SnapshotRef::parse("2026-10-01T02:00:00.5+02:00").map(|s| match s {
                SnapshotRef::At { ms, .. } => ms,
                SnapshotRef::Id(_) => 0,
            }),
            Some(1_790_812_800_500)
        );
        // Past 2262, where nanoseconds since the epoch end, and before the epoch, rounded down.
        let ms = |s: &str| match SnapshotRef::parse(s) {
            Some(SnapshotRef::At { ms, .. }) => Some(ms),
            _ => None,
        };
        assert_eq!(ms("2300-01-01T00:00:00Z"), Some(10_413_792_000_000));
        assert_eq!(ms("1969-12-31T23:59:59.9995Z"), Some(-1));
        for not in [
            "",
            "v3",
            "-1",
            "2026-10-01",
            "2026-10-01T00:00:00",
            "2026-10-01 00:00:00Z",
            "2026-10-01T00:00:00 +00:00",
            "2026-10-01T00:00:00.Z",
            "2026-10-01T00:00:00+0000",
            "99999999999999999999",
            "latest",
        ] {
            assert_eq!(SnapshotRef::parse(not), None, "{not}");
        }
    }

    #[test]
    fn the_snapshot_is_after_the_last_segments_at() {
        let at = |s: &str| split(s).map(|(t, a)| (t.to_string(), a.to_string()));
        assert_eq!(at("orders@3"), Some(("orders".into(), "3".into())));
        assert_eq!(
            at("s3://b/warehouse/orders@2026-10-01T00:00:00Z"),
            Some((
                "s3://b/warehouse/orders".into(),
                "2026-10-01T00:00:00Z".into()
            ))
        );
        assert_eq!(
            at("catalog://prod/sales/orders@4182309823479"),
            Some(("catalog://prod/sales/orders".into(), "4182309823479".into()))
        );
        assert_eq!(
            at("abfss://c@acct.dfs.core.windows.net/t@7"),
            Some(("abfss://c@acct.dfs.core.windows.net/t".into(), "7".into()))
        );
        for none in [
            "orders",
            "abfss://c@acct.dfs.core.windows.net/t",
            "me@host/orders",
            "orders@latest",
            "@3",
            "dir/@3",
            "orders@3/",
        ] {
            assert_eq!(at(none), None, "{none}");
        }
    }

    #[test]
    fn only_the_snapshot_refusal_reads_as_one() {
        let at = SnapshotRef::Id(7);
        assert!(is_only_diff_reads(&only_diff_reads("t@7", "t", &at)));
        // `Engine::metadata`'s own refusal, of the same variant, is tested where it is defined.
        assert!(!is_only_diff_reads(&EngineError::Other(
            "could not tell whether `s3://b/t@7` is a table".to_string()
        )));
    }

    #[cfg(any(feature = "iceberg", feature = "delta"))]
    #[test]
    fn extremes_order_values_as_their_type_does() {
        use std::sync::Arc;
        let ints: Vec<ArrayRef> = [10_i64, -2, 300]
            .iter()
            .map(|v| Arc::new(arrow_array::Int64Array::from(vec![*v])) as ArrayRef)
            .collect();
        assert_eq!(
            extremes(&ints).unwrap(),
            (Some("-2".into()), Some("300".into()))
        );
        let none: Vec<ArrayRef> = vec![Arc::new(arrow_array::Int64Array::from(vec![None::<i64>]))];
        assert_eq!(extremes(&none).unwrap(), (None, None));
        assert_eq!(extremes(&[]).unwrap(), (None, None));
    }
}
