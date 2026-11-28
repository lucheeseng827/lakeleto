//! Write a result as a **new** table: the one way Lakeleto creates a table, and the rules that let
//! it make one without giving up "read-only by construction" for the data you already have.
//!
//! An analyst's loop is query → keep the answer → query the answer. Until this module the only
//! ways to keep an answer were an export download, `--out` on the CLI, or a run's cached result,
//! which lives in the workspace store under an opaque id and cannot be opened as a table. A
//! *new table* closes that loop: a named Parquet file under a directory the operator chose, which
//! the file browser lists, every engine reads, and DuckDB, Polars, pandas or Spark open as well.
//!
//! What makes it safe, each rule enforced here rather than by the callers:
//!
//! 1. **Opt-in.** Nothing writes without a [`WriteDir`]: the server takes one only from
//!    `--write-dir`, the CLI only for `create-table`. A server started without it answers the
//!    write endpoints with 403 and does not advertise them.
//! 2. **New, never replace.** A name that already exists is refused ([`EngineError::Conflict`]),
//!    and the check *is* the publish: the finished file is hard-linked to its name, which the file
//!    system refuses atomically when the name is taken. Two writers racing for one name cannot
//!    both win, and no existing file — a source table included — is overwritten or truncated. A
//!    file system without hard links is refused rather than published by a rename, which is not
//!    create-only.
//! 3. **Confined.** A name is one to three path-safe parts (`marts/revenue_by_city`), never a
//!    path: no `..`, no separators of the platform's own, no extension of the caller's choosing,
//!    no Windows device names. The directory it lands in is canonicalized after it is created and
//!    must still be inside the write directory, so a symlinked subdirectory cannot carry a write
//!    out of it.
//! 4. **Whole or absent.** Rows go to a hidden temporary file beside the destination and become a
//!    table only once the Parquet footer is written and synced. A failed or interrupted write
//!    leaves no table, and at worst a `.lakeleto.*.tmp` file nothing lists.
//! 5. **Complete or refused.** This module writes every row it is given; the callers refuse a
//!    result that hit a row cap rather than save its first N rows under a name that claims to be
//!    the answer.
//! 6. **Self-describing.** The table carries its own provenance — what produced it, from which
//!    tables, when, by which version — as Parquet key-value metadata under [`PROVENANCE_KEY`], so
//!    it survives being copied anywhere and any Parquet reader can show it. No sidecar file, which
//!    the browser would list as a JSON table beside it.

use std::io::BufWriter;
use std::path::{Path, PathBuf};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use serde::{Deserialize, Serialize};

use crate::engine::RowStream;
use crate::error::{EngineError, Result};

/// The Parquet key-value metadata key a new table's [`Provenance`] is stored under.
pub const PROVENANCE_KEY: &str = "lakeleto.provenance";

/// The version of the [`Provenance`] document, so a reader can tell a later shape from this one.
pub const PROVENANCE_VERSION: u32 = 1;

/// The most `/`-separated parts a table name may have: enough for `area/schema/table`.
pub const MAX_NAME_PARTS: usize = 3;

/// The longest one part of a table name may be.
pub const MAX_PART_LEN: usize = 64;

/// The directory new tables are written under, canonical, and how big one may be.
#[derive(Debug, Clone)]
pub struct WriteDir {
    root: PathBuf,
    max_bytes: Option<u64>,
}

/// Where a new table came from, as its [`Provenance`] records it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Origin {
    /// SQL run over named tables, or — with no `sql` — a copy of the one table named.
    Query {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sql: Option<String>,
        tables: Vec<OriginTable>,
    },
    /// A workspace run's cached result, saved as a table.
    Run {
        workspace: String,
        run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sql: Option<String>,
        source: String,
    },
}

/// One table a query read, by the name the SQL used for it and the path it resolved to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OriginTable {
    pub name: String,
    pub path: String,
}

/// What a new table says about itself, stored in its Parquet footer under [`PROVENANCE_KEY`].
///
/// Other programs write the same record into the files they make, and say who they are in
/// `writer` in place of `lakeleto_version`. A record names one or the other.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Provenance {
    pub provenance_version: u32,
    /// The name it was created as (`marts/revenue_by_city`), which a copy elsewhere keeps.
    pub name: String,
    pub created_at_ms: u64,
    /// The Lakeleto that wrote it. `None` for a file another program wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lakeleto_version: Option<String>,
    /// The program that wrote it and its version (`example-writer 1.0.0`), when that is not
    /// Lakeleto.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<String>,
    /// The rows in the file the record is in. For a table Lakeleto wrote, the table's rows; a
    /// program that writes a table as several files gives each file its own count.
    pub rows: u64,
    pub from: Origin,
}

impl Provenance {
    /// The value of [`crate::protocol::PROVENANCE_HEADER`] for this record, or `None` when a
    /// field holds anything a header cannot carry as an RFC 8941 string.
    ///
    /// A name Lakeleto wrote is always safe (path-safe parts only), but a footer can come from a
    /// file somebody else edited, and a header that smuggled a quote or a line break would be worse
    /// than no header. So anything outside printable ASCII, a `"` or a backslash drops the header
    /// rather than being escaped.
    pub fn header_value(&self) -> Option<String> {
        let plain = |s: &str| {
            !s.is_empty()
                && s.bytes()
                    .all(|b| (0x20..0x7f).contains(&b) && b != b'"' && b != b'\\')
        };
        // RFC 8941 integers have at most 15 digits. A real table's row count and a millisecond
        // timestamp are far inside that; a footer that says otherwise is dropped, not truncated.
        const MAX_SF_INTEGER: u64 = 999_999_999_999_999;
        if !plain(&self.name) || self.rows > MAX_SF_INTEGER || self.created_at_ms > MAX_SF_INTEGER {
            return None;
        }
        let mut out = format!(
            "name=\"{}\", created_at_ms={}, rows={}, provenance_version={}",
            self.name, self.created_at_ms, self.rows, self.provenance_version
        );
        for (key, value) in [
            ("lakeleto_version", &self.lakeleto_version),
            ("writer", &self.writer),
        ] {
            if let Some(value) = value {
                if !plain(value) {
                    return None;
                }
                out.push_str(&format!(", {key}=\"{value}\""));
            }
        }
        Some(out)
    }
}

/// A table that was just written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTable {
    pub name: String,
    /// The Parquet file, absolute: what `/v1/*` and the CLI open it by.
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    pub columns: Vec<String>,
    pub provenance: Provenance,
}

impl WriteDir {
    /// The write directory at `dir`, which must already exist and be a directory: a typo then
    /// fails when the server starts rather than creating a directory nobody meant.
    pub fn open(dir: &Path) -> Result<WriteDir> {
        let named =
            |what: String| EngineError::Other(format!("--write-dir {}: {what}", dir.display()));
        let root = std::fs::canonicalize(dir).map_err(|e| named(e.to_string()))?;
        if !root.is_dir() {
            return Err(named("is not a directory".to_string()));
        }
        Ok(WriteDir {
            root,
            max_bytes: None,
        })
    }

    /// Refuse a table bigger than `max` bytes on disk. The server sets one; the CLI, writing to
    /// the user's own disk at the user's own request, does not.
    pub fn with_max_bytes(mut self, max: u64) -> WriteDir {
        self.max_bytes = Some(max);
        self
    }

    /// The canonical directory.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Where the table `name` would be written, after checking the name. Nothing is created.
    pub fn target(&self, name: &str) -> Result<PathBuf> {
        let parts = parts(name)?;
        let (last, dirs) = parts
            .split_last()
            .expect("parts() returns at least one part");
        let mut path = self.root.clone();
        for d in dirs {
            path.push(d);
        }
        path.push(format!("{last}.parquet"));
        Ok(path)
    }

    /// Write `stream` as the new table `name`, recording `from` as its origin.
    ///
    /// The rows are written as they arrive, so memory is one batch however large the result.
    pub fn write(&self, name: &str, stream: RowStream, from: Origin) -> Result<NewTable> {
        let dest = self.target(name)?;
        if dest.symlink_metadata().is_ok() {
            // Checked again, atomically, at the publish; this is only the early, cheap answer that
            // saves running a query whose result could never be kept.
            return Err(exists(name, &dest));
        }
        let parent = dest
            .parent()
            .expect("a target is always inside the write dir");
        // Checked before anything is created: `create_dir_all` follows a symlink it meets, so a
        // part of the name that is already a symlink to elsewhere would have it build directories
        // outside the root before any later check could refuse. The deepest part that exists, a
        // symlink included, must resolve inside the root. The root itself always exists, so the
        // walk ends there at the latest.
        let mut existing = parent;
        while existing.symlink_metadata().is_err() {
            existing = existing
                .parent()
                .expect("a target's ancestors lead to the write dir, which exists");
        }
        self.confine(name, existing)?;
        std::fs::create_dir_all(parent)
            .map_err(|e| EngineError::Other(format!("create the directory for `{name}`: {e}")))?;
        // And again after, on the directory the table actually lands in, for a symlink put in
        // place between the check above and the creation.
        let real_parent = self.confine(name, parent)?;
        let dest = real_parent.join(dest.file_name().expect("a target names a file"));

        let mut tmp = TempFile::create(&real_parent)?;
        let schema = stream.schema().clone();
        let columns = schema.fields().iter().map(|f| f.name().clone()).collect();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let file = tmp.file.take().expect("a fresh temp file is open");
        let mut writer = ArrowWriter::try_new(BufWriter::new(file), schema.clone(), Some(props))
            .map_err(EngineError::parquet)?;
        let mut rows: u64 = 0;
        for (i, batch) in stream.enumerate() {
            let batch = conform(name, &schema, i, batch?)?;
            rows += batch.num_rows() as u64;
            writer.write(&batch).map_err(EngineError::parquet)?;
        }
        let provenance = Provenance {
            provenance_version: PROVENANCE_VERSION,
            name: name.to_string(),
            created_at_ms: now_ms(),
            lakeleto_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            writer: None,
            rows,
            from,
        };
        let json = serde_json::to_string(&provenance)
            .map_err(|e| EngineError::Other(format!("encode provenance: {e}")))?;
        // Appended after the rows because the row count is only known then; the footer is
        // written at `into_inner`, so the provenance lands in it.
        writer.append_key_value_metadata(KeyValue::new(PROVENANCE_KEY.to_string(), json));
        let buffered = writer.into_inner().map_err(EngineError::parquet)?;
        let file = buffered
            .into_inner()
            .map_err(|e| EngineError::Io(e.into_error()))?;
        file.sync_all()?;
        let bytes = file.metadata()?.len();
        drop(file);
        if let Some(max) = self.max_bytes {
            if bytes > max {
                return Err(EngineError::TooLarge(format!(
                    "`{name}` would be {bytes} bytes, over this server's {max}-byte cap for a \
                     new table — narrow the query, or write it with `lakeleto create-table`"
                )));
            }
        }
        tmp.publish_new(&dest).map_err(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => exists(name, &dest),
            _ => EngineError::Io(e),
        })?;
        Ok(NewTable {
            name: name.to_string(),
            path: dest.display().to_string(),
            rows,
            bytes,
            columns,
            provenance,
        })
    }
}

/// Batch `i` of the result on the result's own schema, or a refusal.
///
/// Parquet encodes each batch against the schema the file was opened with, by position, so every
/// batch must hold the result's columns: the same names, in the same order, of the same types.
/// That is the rule the CLI's other writers keep ([`crate::render::ensure_batch_matches`]), and
/// for the same reason it leaves nullability and metadata alone: a plan's batches legitimately
/// differ from its schema there. A `UNION ALL` with a literal row is the common case: the
/// literal's column cannot be null and the union's can, and each side's batches say what their
/// side says. The batch is rebuilt on the result's schema, which also refuses a null in a column
/// the result says has none.
///
/// Only a column's own nullability is left alone. A list's items and a struct's fields are part of
/// its type, compared whole; the physical plan gives each side of a union the same nested types
/// (as it does for `make_array` and `named_struct` columns), so a difference there is refused.
fn conform(name: &str, schema: &SchemaRef, i: usize, batch: RecordBatch) -> Result<RecordBatch> {
    if batch.schema_ref().fields() == schema.fields() {
        return Ok(batch);
    }
    crate::render::ensure_batch_matches(schema, i, &batch, &format!("the new table `{name}`"))?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), batch.columns().to_vec(), &options).map_err(
        |e| {
            EngineError::Arrow(format!(
                "batch {i}: {e} — refusing to write the new table `{name}`"
            ))
        },
    )
}

impl WriteDir {
    /// `path`, resolved, when it is inside the root; a refusal naming `name` when it is not, or
    /// cannot be resolved (a dangling symlink).
    fn confine(&self, name: &str, path: &Path) -> Result<PathBuf> {
        let outside = || {
            EngineError::Forbidden(format!(
                "`{name}` resolves outside the write directory {} (a symlinked directory?)",
                self.root.display()
            ))
        };
        let real = std::fs::canonicalize(path).map_err(|_| outside())?;
        if real.starts_with(&self.root) {
            Ok(real)
        } else {
            Err(outside())
        }
    }
}

/// The [`Provenance`] a table written by [`WriteDir::write`] carries, or `None` for a Parquet file
/// with none (anything Lakeleto did not create).
pub fn read_provenance(path: &Path) -> Result<Option<Provenance>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file).map_err(EngineError::parquet)?;
    let kv = reader.metadata().file_metadata().key_value_metadata();
    let Some(json) = kv
        .into_iter()
        .flatten()
        .find(|kv| kv.key == PROVENANCE_KEY)
        .and_then(|kv| kv.value.as_deref())
    else {
        return Ok(None);
    };
    serde_json::from_str(json)
        .map(Some)
        .map_err(|e| EngineError::Other(format!("{PROVENANCE_KEY} in {}: {e}", path.display())))
}

/// The parts of a table name, each checked.
fn parts(name: &str) -> Result<Vec<&str>> {
    let bad = |why: &str| {
        Err(EngineError::Other(format!(
            "`{name}` is not a table name: {why}. A name is 1 to {MAX_NAME_PARTS} parts \
             separated by `/`, each starting with a letter or digit and holding only letters, \
             digits, `_` and `-` (e.g. `marts/revenue_by_city`)"
        )))
    };
    if name.is_empty() {
        return bad("it is empty");
    }
    let parts: Vec<&str> = name.split('/').collect();
    if parts.len() > MAX_NAME_PARTS {
        return bad(&format!("it has {} parts", parts.len()));
    }
    for p in &parts {
        if p.is_empty() {
            return bad("it has an empty part");
        }
        if p.len() > MAX_PART_LEN {
            return bad(&format!("a part is longer than {MAX_PART_LEN} characters"));
        }
        let mut chars = p.chars();
        let first = chars.next().expect("not empty");
        if !first.is_ascii_alphanumeric() {
            return bad(&format!("`{p}` does not start with a letter or digit"));
        }
        if let Some(c) = chars.find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-')) {
            return bad(&format!("`{p}` holds `{c}`"));
        }
        if is_windows_device(p) {
            return bad(&format!("`{p}` is a device name on Windows"));
        }
    }
    Ok(parts)
}

/// `CON`, `NUL`, `COM1`…: names Windows opens as a device whatever the extension, so a table by
/// one would be written to no file at all.
fn is_windows_device(part: &str) -> bool {
    let upper = part.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper.len() == 4
            && upper.as_bytes()[3].is_ascii_digit())
}

fn exists(name: &str, dest: &Path) -> EngineError {
    EngineError::Conflict(format!(
        "a table named `{name}` already exists at {} — pick another name; Lakeleto never \
         replaces a table",
        dest.display()
    ))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A different 64-bit value on every call, from the random keys std seeds a `HashMap` with.
fn random_suffix() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

/// The hidden file a table is written to before it has a name, removed unless published.
struct TempFile {
    path: PathBuf,
    file: Option<std::fs::File>,
    published: bool,
}

impl TempFile {
    /// Create one in `dir`, exclusively, under a name no other run is using.
    fn create(dir: &Path) -> Result<TempFile> {
        for _ in 0..8 {
            let path = dir.join(format!(
                ".lakeleto.{}-{:016x}.tmp",
                std::process::id(),
                random_suffix()
            ));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(TempFile {
                        path,
                        file: Some(file),
                        published: false,
                    })
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(EngineError::Io(e)),
            }
        }
        Err(EngineError::Other(format!(
            "eight temporary names in {} were all taken",
            dir.display()
        )))
    }

    /// Give the finished file the name `dest`, failing with `AlreadyExists` if `dest` is taken.
    ///
    /// A hard link is the no-clobber publish: `link(2)` refuses an existing name atomically, and
    /// the table appears whole, since the file was complete before it had the name.
    ///
    /// A file system without hard links (FAT, some network and FUSE mounts) is refused rather than
    /// served by a fallback. The obvious one, claiming the name with an exclusive create and then
    /// renaming the finished file over the claim, is not create-only: another process can replace
    /// the claim between the two, and the rename would then replace that process's file.
    fn publish_new(self, dest: &Path) -> std::io::Result<()> {
        self.publish_with(dest, |from, to| std::fs::hard_link(from, to))
    }

    /// [`TempFile::publish_new`], linking with `link`, which a test chooses.
    fn publish_with(
        mut self,
        dest: &Path,
        link: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        self.file.take();
        match link(&self.path, dest) {
            Ok(()) => {
                self.published = true;
                let _ = std::fs::remove_file(&self.path);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(e),
            Err(e) => Err(std::io::Error::new(
                e.kind(),
                format!(
                    "this file system cannot publish a new table without risking replacing one \
                     (it gave no hard link: {e}) — use a write directory on a local disk"
                ),
            )),
        }
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if !self.published {
            self.file.take();
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RowBatch;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn rows(n: i64) -> RowStream {
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("orders", DataType::Int64, false),
        ]));
        let cities: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(cities)),
                Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        RowStream::from_batch(RowBatch {
            schema,
            batches: vec![batch],
        })
    }

    fn origin() -> Origin {
        Origin::Query {
            sql: Some("SELECT city, count(*) AS orders FROM t GROUP BY city".to_string()),
            tables: vec![OriginTable {
                name: "t".to_string(),
                path: "/data/orders.parquet".to_string(),
            }],
        }
    }

    fn dir() -> (tempfile::TempDir, WriteDir) {
        let tmp = tempfile::tempdir().unwrap();
        let wd = WriteDir::open(tmp.path()).unwrap();
        (tmp, wd)
    }

    fn leftovers(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        for entry in walk(dir) {
            let name = entry.file_name().unwrap().to_string_lossy().to_string();
            if name.ends_with(".tmp") {
                out.push(name);
            }
        }
        out
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
        out
    }

    #[test]
    fn writes_a_table_that_reads_back_with_its_provenance() {
        let (_tmp, wd) = dir();
        let t = wd.write("marts/by_city", rows(5), origin()).unwrap();
        assert_eq!(t.rows, 5);
        assert_eq!(t.columns, vec!["city", "orders"]);
        let path = Path::new(&t.path);
        assert!(path.ends_with("marts/by_city.parquet"), "{}", t.path);
        assert!(path.starts_with(wd.path()));
        assert_eq!(std::fs::metadata(path).unwrap().len(), t.bytes);

        let prov = read_provenance(path)
            .unwrap()
            .expect("provenance in the footer");
        assert_eq!(prov, t.provenance);
        assert_eq!(prov.name, "marts/by_city");
        assert_eq!(prov.rows, 5);
        assert_eq!(prov.from, origin());

        // And it is an ordinary Parquet file to any reader.
        let file = std::fs::File::open(path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let n: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(n, 5);
        assert!(leftovers(wd.path()).is_empty());
    }

    #[test]
    fn never_replaces_an_existing_table_or_file() {
        let (_tmp, wd) = dir();
        let first = wd.write("t", rows(3), origin()).unwrap();
        let before = std::fs::read(&first.path).unwrap();
        let err = wd.write("t", rows(7), origin()).unwrap_err();
        assert!(matches!(err, EngineError::Conflict(_)), "{err}");
        assert_eq!(
            std::fs::read(&first.path).unwrap(),
            before,
            "the table was touched"
        );

        // A file Lakeleto did not write — a source table — is just as safe.
        let source = wd.path().join("orders.parquet");
        std::fs::write(&source, b"not ours").unwrap();
        let err = wd.write("orders", rows(1), origin()).unwrap_err();
        assert!(matches!(err, EngineError::Conflict(_)), "{err}");
        assert_eq!(std::fs::read(&source).unwrap(), b"not ours");
        assert!(leftovers(wd.path()).is_empty());
    }

    #[test]
    fn the_publish_itself_refuses_a_name_taken_after_the_early_check() {
        // A writer that loses the race to a name finds it at the publish, not only up front.
        let (_tmp, wd) = dir();
        let dest = wd.target("race").unwrap();
        let tmp = TempFile::create(wd.path()).unwrap();
        std::fs::write(&dest, b"the other writer").unwrap();
        let err = tmp.publish_new(&dest).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&dest).unwrap(), b"the other writer");
        assert!(
            leftovers(wd.path()).is_empty(),
            "the loser's temp file is removed"
        );
    }

    /// A file system without hard links is refused, not published by rename: nothing appears at
    /// the name, a file that is there is untouched, and the temporary file is removed.
    #[test]
    fn without_hard_links_a_table_is_refused_not_renamed_into_place() {
        let (_tmp, wd) = dir();
        let unsupported = |_: &Path, _: &Path| -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "operation not supported",
            ))
        };
        let dest = wd.target("nolinks").unwrap();
        let err = TempFile::create(wd.path())
            .unwrap()
            .publish_with(&dest, unsupported)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        assert!(
            err.to_string().contains("without risking replacing"),
            "{err}"
        );
        assert!(!dest.exists(), "something was published at the name");
        assert!(
            leftovers(wd.path()).is_empty(),
            "the temporary file was left behind"
        );

        // And with a file already at the name, it stays exactly as it was.
        std::fs::write(&dest, b"someone else's").unwrap();
        TempFile::create(wd.path())
            .unwrap()
            .publish_with(&dest, unsupported)
            .unwrap_err();
        assert_eq!(std::fs::read(&dest).unwrap(), b"someone else's");
    }

    #[test]
    fn a_failed_write_leaves_no_table_and_no_temp_file() {
        let (_tmp, wd) = dir();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let failing = RowStream::new(
            schema,
            vec![Err(EngineError::Query(
                "the engine failed mid-result".into(),
            ))]
            .into_iter(),
        );
        let err = wd.write("half", failing, origin()).unwrap_err();
        assert!(matches!(err, EngineError::Query(_)), "{err}");
        assert!(!wd.target("half").unwrap().exists());
        assert!(leftovers(wd.path()).is_empty());
    }

    /// A stream whose result says `city` may be null, with a batch of rows from a table, where
    /// it may, and a batch from a literal row, where it cannot: a `UNION ALL` with a literal row.
    fn union_with_a_literal_row() -> RowStream {
        let result = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("orders", DataType::Int64, true),
        ]));
        let table = RecordBatch::try_new(
            result.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("Oslo"), None])),
                Arc::new(Int64Array::from(vec![Some(3), Some(4)])),
            ],
        )
        .unwrap();
        // The literal side's own schema, as the plan gives it: the union's names, and nothing in
        // it can be null.
        let literal = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("city", DataType::Utf8, false),
                Field::new("orders", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["Lima"])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap();
        RowStream::new(result, vec![Ok(table), Ok(literal)].into_iter())
    }

    fn read_back(path: &str) -> RecordBatch {
        let file = std::fs::File::open(path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    #[test]
    fn a_union_all_with_a_literal_row_is_written_with_the_results_columns() {
        let (_tmp, wd) = dir();
        let t = wd
            .write("with_lima", union_with_a_literal_row(), origin())
            .unwrap();
        assert_eq!(t.rows, 3);
        assert_eq!(t.columns, vec!["city", "orders"]);

        let back = read_back(&t.path);
        let schema = back.schema();
        assert_eq!(schema.field(0).name(), "city");
        assert!(
            schema.field(0).is_nullable(),
            "the result's column, not the literal's"
        );
        let cities = back
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            cities.iter().collect::<Vec<_>>(),
            vec![Some("Oslo"), None, Some("Lima")]
        );
        let orders = back
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(orders.values().to_vec(), vec![3, 4, 7]);
        assert!(leftovers(wd.path()).is_empty());
    }

    #[test]
    fn a_batch_that_does_not_hold_the_results_columns_is_refused() {
        use arrow_array::Int32Array;

        let result = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("orders", DataType::Int64, true),
        ]));
        let refused = |result: Arc<Schema>, batch: RecordBatch| -> String {
            let (_tmp, wd) = dir();
            let stream = RowStream::new(result, vec![Ok(batch)].into_iter());
            let err = wd.write("bad", stream, origin()).unwrap_err().to_string();
            assert!(!wd.target("bad").unwrap().exists(), "{err}");
            assert!(leftovers(wd.path()).is_empty(), "{err}");
            assert!(
                err.contains("refusing to write the new table `bad`"),
                "{err}"
            );
            err
        };
        let batch = |fields: Vec<Field>, columns: Vec<arrow_array::ArrayRef>| {
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };

        // Another type, even one a cast could reach.
        let err = refused(
            result.clone(),
            batch(
                vec![
                    Field::new("city", DataType::Utf8, false),
                    Field::new("orders", DataType::Int32, true),
                ],
                vec![
                    Arc::new(StringArray::from(vec!["Oslo"])),
                    Arc::new(Int32Array::from(vec![1])),
                ],
            ),
        );
        assert!(err.contains("the batch has `orders: Int32"), "{err}");

        // Another number of columns.
        let err = refused(
            result.clone(),
            batch(
                vec![Field::new("city", DataType::Utf8, false)],
                vec![Arc::new(StringArray::from(vec!["Oslo"]))],
            ),
        );
        assert!(
            err.contains("has 1 column(s) but the result declares 2"),
            "{err}"
        );

        // A null where the result says there is none.
        let err = refused(
            result.clone(),
            batch(
                vec![
                    Field::new("city", DataType::Utf8, true),
                    Field::new("orders", DataType::Int64, true),
                ],
                vec![
                    Arc::new(StringArray::from(vec![None::<&str>])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
            ),
        );
        assert!(err.contains("non-nullable"), "{err}");

        // Columns of one type in another order: by position they would land under each other's
        // names.
        let pair = Arc::new(Schema::new(vec![
            Field::new("shipped", DataType::Int64, true),
            Field::new("returned", DataType::Int64, true),
        ]));
        let err = refused(
            pair,
            batch(
                vec![
                    Field::new("returned", DataType::Int64, true),
                    Field::new("shipped", DataType::Int64, true),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![2])),
                ],
            ),
        );
        assert!(err.contains("the batch has `returned: Int64"), "{err}");
    }

    #[test]
    fn a_byte_cap_refuses_and_cleans_up() {
        let (_tmp, wd) = dir();
        let wd = wd.with_max_bytes(10);
        let err = wd.write("big", rows(100), origin()).unwrap_err();
        assert!(matches!(err, EngineError::TooLarge(_)), "{err}");
        assert!(!wd.target("big").unwrap().exists());
        assert!(leftovers(wd.path()).is_empty());
    }

    #[test]
    fn names_are_path_safe_parts_never_paths() {
        let (_tmp, wd) = dir();
        for ok in [
            "t",
            "revenue_by_city",
            "marts/by_city",
            "a/b/c",
            "2026-q3",
            "A-b_9",
        ] {
            assert!(wd.target(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "/abs",
            "trailing/",
            "a//b",
            "..",
            "../escape",
            "a/../b",
            ".hidden",
            "_lead",
            "-lead",
            "has space",
            "dot.ted",
            "t.parquet",
            "back\\slash",
            "c:drive",
            "a/b/c/d",
            "NUL",
            "con",
            "marts/com1",
            "ünïcode",
        ] {
            assert!(wd.target(bad).is_err(), "`{bad}` was accepted");
        }
        let long = "x".repeat(MAX_PART_LEN + 1);
        assert!(wd.target(&long).is_err());
        assert!(wd.target(&"x".repeat(MAX_PART_LEN)).is_ok());
        // COM10 is not a device; COM1 is.
        assert!(wd.target("com10").is_ok());
    }

    #[test]
    fn a_provenance_header_carries_only_what_a_header_can() {
        let mut p = Provenance {
            provenance_version: 1,
            name: "marts/revenue".to_string(),
            created_at_ms: 1_762_700_000_000,
            lakeleto_version: Some("0.4.1".to_string()),
            writer: None,
            rows: 24,
            from: Origin::Query {
                sql: Some("select 1\n-- a comment, with \"quotes\"".to_string()),
                tables: Vec::new(),
            },
        };
        assert_eq!(
            p.header_value().as_deref(),
            Some(
                "name=\"marts/revenue\", created_at_ms=1762700000000, rows=24, \
                 provenance_version=1, lakeleto_version=\"0.4.1\""
            ),
            "the SQL, with its line breaks and quotes, stays in the footer"
        );
        // A footer somebody else wrote can hold anything; a name a header cannot carry drops the
        // header rather than being escaped into one.
        for bad in ["a\"b", "a\\b", "line\nbreak", "caf\u{e9}", ""] {
            p.name = bad.to_string();
            assert_eq!(p.header_value(), None, "{bad:?}");
        }
        // Integers past RFC 8941's 15 digits drop the header too.
        p.name = "marts/revenue".to_string();
        p.rows = 999_999_999_999_999;
        assert!(
            p.header_value().is_some(),
            "15 digits is the largest a header carries"
        );
        p.rows = 1_000_000_000_000_000;
        assert_eq!(
            p.header_value(),
            None,
            "16 digits is not an RFC 8941 integer"
        );
        p.rows = 24;
        p.created_at_ms = u64::MAX;
        assert_eq!(p.header_value(), None);
    }

    /// The record another program writes into its own files: the same shape, naming itself in
    /// `writer`, with no `lakeleto_version`. Lakeleto reads it and passes it on, and writes its
    /// own records as before.
    #[test]
    fn a_record_another_program_wrote_reads_and_heads_an_export() {
        let json = r#"{"provenance_version":1,"name":"daily_orders","created_at_ms":1794225600000,"writer":"example-writer 1.0.0","rows":7,"from":{"kind":"query","sql":"SELECT 1","tables":[]}}"#;
        let p: Provenance = serde_json::from_str(json).unwrap();
        assert_eq!(p.lakeleto_version, None);
        assert_eq!(p.writer.as_deref(), Some("example-writer 1.0.0"));
        assert_eq!(p.rows, 7);
        assert_eq!(
            p.header_value().as_deref(),
            Some(
                "name=\"daily_orders\", created_at_ms=1794225600000, rows=7, \
                 provenance_version=1, writer=\"example-writer 1.0.0\""
            )
        );
        assert_eq!(
            serde_json::to_string(&p).unwrap(),
            json,
            "round-trips as written"
        );

        // A writer name a header cannot carry drops the header, as a bad name does.
        let mut bad = p.clone();
        bad.writer = Some("line\nbreak".to_string());
        assert_eq!(bad.header_value(), None);

        // Lakeleto's own record still names Lakeleto and nothing else.
        let (_tmp, wd) = dir();
        let t = wd.write("t", rows(2), origin()).unwrap();
        let raw = {
            use parquet::file::reader::{FileReader, SerializedFileReader};
            let reader = SerializedFileReader::new(std::fs::File::open(&t.path).unwrap()).unwrap();
            let kv = reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .cloned();
            kv.into_iter()
                .flatten()
                .find(|kv| kv.key == PROVENANCE_KEY)
                .and_then(|kv| kv.value)
                .unwrap()
        };
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["lakeleto_version"], env!("CARGO_PKG_VERSION"));
        assert!(v.get("writer").is_none(), "{raw}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_directory_cannot_carry_a_write_outside() {
        let (_tmp, wd) = dir();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), wd.path().join("out")).unwrap();
        let err = wd.write("out/t", rows(1), origin()).unwrap_err();
        assert!(matches!(err, EngineError::Forbidden(_)), "{err}");
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    }

    /// The deeper case: `out` is a symlink to elsewhere and `out/newdir` does not exist yet.
    /// `create_dir_all` would follow `out` and make `newdir` outside the root, so the check has
    /// to come before it, and nothing may appear outside.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_directory_cannot_have_directories_made_beyond_it() {
        let (_tmp, wd) = dir();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), wd.path().join("out")).unwrap();
        let err = wd.write("out/newdir/t", rows(1), origin()).unwrap_err();
        assert!(matches!(err, EngineError::Forbidden(_)), "{err}");
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            0,
            "a directory was created outside the write dir"
        );
        // A dangling symlink in the way is refused the same way, not followed.
        std::os::unix::fs::symlink("/nonexistent/elsewhere", wd.path().join("gone")).unwrap();
        let err = wd.write("gone/newdir/t", rows(1), origin()).unwrap_err();
        assert!(matches!(err, EngineError::Forbidden(_)), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn a_dangling_symlink_at_the_name_is_a_name_taken() {
        let (_tmp, wd) = dir();
        let dest = wd.target("t").unwrap();
        std::os::unix::fs::symlink("/nonexistent/elsewhere.parquet", &dest).unwrap();
        let err = wd.write("t", rows(1), origin()).unwrap_err();
        assert!(matches!(err, EngineError::Conflict(_)), "{err}");
    }

    #[test]
    fn the_write_dir_must_exist() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(WriteDir::open(&tmp.path().join("missing")).is_err());
        let file = tmp.path().join("f");
        std::fs::write(&file, b"").unwrap();
        assert!(WriteDir::open(&file).is_err());
    }

    #[test]
    fn a_file_lakeleto_did_not_write_has_no_provenance() {
        let (_tmp, wd) = dir();
        let rb = rows(2).collect_batch().unwrap();
        let path = wd.path().join("plain.parquet");
        std::fs::write(&path, crate::render::to_parquet(&rb).unwrap()).unwrap();
        assert_eq!(read_provenance(&path).unwrap(), None);
    }
}
