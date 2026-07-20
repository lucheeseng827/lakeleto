//! Source detection — figure out *what* a path is before an engine reads it.
//!
//! Detection order is **directory shape → extension → magic bytes**: a directory is treated
//! as an Iceberg table when it has a `metadata/` subdir; a directory of `.parquet` files (a
//! multi-file dataset, with optional Hive `key=value` partition subdirs) is treated as one
//! Parquet table; otherwise a file is classified by extension, falling back to a `PAR1`
//! magic-byte sniff.
//! Format is decoupled from Engine on purpose: the same `Source` is handed to whichever
//! engine the user picked (`local`, `sql`, `remote`), so format sniffing lives in one place.

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::error::{EngineError, Result};

/// A table format Lakeleto knows how to talk about. Whether a given *engine* can read it is a
/// separate question (see [`crate::engine::Capabilities`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Parquet,
    Csv,
    /// Tab-separated values — read exactly like [`Format::Csv`] but with a tab delimiter. A
    /// distinct variant (rather than keying the delimiter off the file extension) so an explicit
    /// `--format tsv` / `?format=tsv` override selects tab even for a differently-named file.
    Tsv,
    Json,
    Iceberg,
    /// A Delta Lake table — a directory with a `_delta_log/` transaction log over Parquet data.
    Delta,
    /// A database table reached over a connection URI (`sqlite://` / `postgres://` / `mysql://`).
    /// The dialect + table live in the URI (parsed by the `database` engine), so this is just the
    /// "this source is a live DB, not a file" marker.
    Database,
}

impl Format {
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Csv => "csv",
            Format::Tsv => "tsv",
            Format::Json => "json",
            Format::Iceberg => "iceberg",
            Format::Delta => "delta",
            Format::Database => "database",
        }
    }

    /// Parse a format name (case-insensitive) — used by the `--format` override and the API.
    pub fn parse(s: &str) -> Option<Format> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" | "pq" => Some(Format::Parquet),
            "csv" => Some(Format::Csv),
            "tsv" => Some(Format::Tsv),
            "json" | "ndjson" | "jsonl" => Some(Format::Json),
            "iceberg" => Some(Format::Iceberg),
            "delta" | "deltalake" => Some(Format::Delta),
            "database" | "db" | "sqlite" | "postgres" | "postgresql" | "mysql" => {
                Some(Format::Database)
            }
            _ => None,
        }
    }

    /// The field delimiter for the delimited-text formats: tab for [`Format::Tsv`], comma
    /// otherwise. Only meaningful for `Csv`/`Tsv`.
    pub fn delimiter(&self) -> u8 {
        match self {
            Format::Tsv => b'\t',
            _ => b',',
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// URI schemes Lakeleto treats as an object store (BYO-credential `s3://` / `gs://` / `az://`).
/// Recognised in every build — even without `--features object-store` — so a URI gets a
/// helpful "rebuild with the feature" message instead of a confusing filesystem error.
pub const REMOTE_SCHEMES: &[&str] = &[
    "s3", "s3a", "gs", "gcs", "az", "azure", "abfs", "abfss", "adl",
];

/// Does `s` look like an object-store URI we recognise (e.g. `s3://bucket/key.parquet`)?
pub fn is_object_uri(s: &str) -> bool {
    match s.split_once("://") {
        Some((scheme, rest)) if !rest.is_empty() => {
            REMOTE_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str())
        }
        _ => false,
    }
}

/// Connection-URI schemes handled by the `database` engine. Recognised in every build so a DB URI
/// gets a "rebuild with the feature" message instead of being mistaken for a file path.
pub const DATABASE_SCHEMES: &[&str] = &["sqlite", "postgres", "postgresql", "mysql"];

/// Does `s` look like a database connection URI (e.g. `sqlite:///data.db?table=orders`)?
pub fn is_database_uri(s: &str) -> bool {
    match s.split_once("://") {
        Some((scheme, _)) => DATABASE_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str()),
        _ => false,
    }
}

/// A resolved data source: a path plus the format Lakeleto detected for it.
#[derive(Debug, Clone)]
pub struct Source {
    pub path: PathBuf,
    pub format: Format,
}

impl Source {
    /// Detect the format of `path` (extension -> magic bytes -> directory shape).
    pub fn detect(path: impl AsRef<Path>) -> Result<Source> {
        let path = path.as_ref().to_path_buf();

        // Database connection URI (sqlite://… / postgres://… / mysql://…): a live DB, never a file.
        // Classify without touching the filesystem — the `database` engine parses the URI.
        if path.to_str().is_some_and(is_database_uri) {
            return Ok(Source {
                path,
                format: Format::Database,
            });
        }

        // Object-store URI (s3://…): classify by the key's extension without touching the
        // filesystem. Magic-byte sniffing would require fetching, so an unknown extension
        // needs an explicit `--format`.
        if path.to_str().is_some_and(is_object_uri) {
            if let Some(format) = format_from_extension(&path) {
                return Ok(Source { path, format });
            }
            // No data-file extension: a bare prefix is likely an Iceberg table — one cheap probe
            // for a `metadata/` child. (Only object stores; a network round-trip, so gated behind
            // the feature and reached only when the name gives nothing away.)
            #[cfg(feature = "object-store")]
            if path
                .to_str()
                .is_some_and(crate::objstore::looks_like_iceberg)
            {
                return Ok(Source {
                    path,
                    format: Format::Iceberg,
                });
            }
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "cannot infer the format of {} from its name — pass \
                     `--format parquet|csv|json|iceberg`",
                    path.display()
                ),
            });
        }

        if path.is_dir() {
            // A Delta Lake table has a `_delta_log/` transaction log. Check this BEFORE the plain
            // parquet-dir fallback — a Delta table's data files are Parquet, so without this it
            // would be misread as a raw parquet dataset (ignoring the log: stale/removed rows).
            if path.join("_delta_log").is_dir() {
                return Ok(Source {
                    path,
                    format: Format::Delta,
                });
            }
            // An Iceberg table is a directory containing a `metadata/` catalog dir.
            if path.join("metadata").is_dir() {
                return Ok(Source {
                    path,
                    format: Format::Iceberg,
                });
            }
            // Otherwise a directory of `.parquet` files (incl. Hive-partitioned subdirs, and the
            // `foo.parquet/part-*.parquet` split-file shape) is read as one multi-file dataset.
            if !list_parquet_files(&path).is_empty() {
                return Ok(Source {
                    path,
                    format: Format::Parquet,
                });
            }
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "{} is a directory but not an Iceberg table (no metadata/ subdir) and \
                     contains no .parquet files",
                    path.display()
                ),
            });
        }

        if let Some(format) = format_from_extension(&path) {
            return Ok(Source { path, format });
        }

        // No/unknown extension: sniff the magic bytes.
        let format = sniff_magic(&path)?;
        Ok(Source { path, format })
    }

    /// Build a source with an explicit format (used by `--format` overrides and tests).
    pub fn with_format(path: impl AsRef<Path>, format: Format) -> Source {
        Source {
            path: path.as_ref().to_path_buf(),
            format,
        }
    }

    /// Resolve a source from a path and an optional explicit format name (detect when `None`).
    pub fn resolve(path: impl AsRef<Path>, format: Option<&str>) -> Result<Source> {
        match format {
            Some(f) => Format::parse(f)
                .map(|fmt| Source::with_format(path, fmt))
                .ok_or_else(|| EngineError::UnsupportedFormat {
                    detail: format!("unknown format `{f}` (expected parquet/csv/tsv/json/iceberg)"),
                }),
            None => Source::detect(path),
        }
    }

    pub fn display(&self) -> String {
        self.path.display().to_string()
    }

    /// True when this source lives in an object store (`s3://` / `gs://` / `az://`) rather
    /// than on the local filesystem. The reading itself needs `--features object-store`.
    pub fn is_remote(&self) -> bool {
        self.path.to_str().is_some_and(is_object_uri)
    }
}

/// One entry in a [`DirListing`] — a subdirectory or a readable data file.
#[derive(Debug, Clone, Serialize)]
pub struct DirEntry {
    pub name: String,
    pub path: String,
    /// `"dir"` or `"file"`.
    pub kind: &'static str,
    /// Detected format for files (parquet/csv/json/iceberg), `None` for plain dirs.
    pub format: Option<String>,
    pub size: Option<u64>,
}

/// A directory's browsable contents: subdirectories + readable data files (other files hidden).
#[derive(Debug, Clone, Serialize)]
pub struct DirListing {
    pub dir: String,
    pub parent: Option<String>,
    pub entries: Vec<DirEntry>,
}

/// Collect the `.parquet` files under `dir` (recursively, so Hive-partitioned subdirs work),
/// sorted for a deterministic read order. Sidecar/marker entries whose name starts with `.` or
/// `_` (`_SUCCESS`, `_common_metadata`, `.crc`, …) are skipped.
pub fn list_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_parquet_files(dir, &mut out);
    out.sort();
    out
}

fn collect_parquet_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || name.starts_with('_') {
            continue; // skip _SUCCESS / _common_metadata / .crc / hidden
        }
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out);
        } else if path
            .extension()
            .and_then(|x| x.to_str())
            .is_some_and(|x| x.eq_ignore_ascii_case("parquet"))
        {
            out.push(path);
        }
    }
}

/// Parse Hive-style `key=value` partition columns from the directory names on the path from
/// `root` (exclusive) down to `file`'s parent, outermost first. A file at
/// `root/year=2024/month=03/part-0.parquet` yields `[("year","2024"), ("month","03")]`. Path
/// segments that are not `key=value` (or have an empty key) are ignored, so a mixed layout with
/// some non-partition subdirs is tolerated. Returns empty when `file` is not under `root`.
pub fn hive_partitions(file: &Path, root: &Path) -> Vec<(String, String)> {
    let Ok(rel) = file.strip_prefix(root) else {
        return Vec::new();
    };
    let comps: Vec<_> = rel.components().collect();
    let mut out = Vec::new();
    // Every component except the final file name is a candidate partition dir.
    for comp in comps.iter().take(comps.len().saturating_sub(1)) {
        if let std::path::Component::Normal(os) = comp {
            let seg = os.to_string_lossy();
            if let Some((k, v)) = seg.split_once('=') {
                if !k.is_empty() {
                    out.push((k.to_string(), v.to_string()));
                }
            }
        }
    }
    out
}

/// List a directory for the file browser: subdirectories and Parquet/CSV/JSON files (plus
/// Iceberg-table dirs), dirs first, then files, each alphabetical. Non-data files are hidden.
pub fn list_dir(dir: &str) -> Result<DirListing> {
    // Object-store prefixes are browsed through the object-store backend, not the filesystem.
    if is_object_uri(dir) {
        #[cfg(feature = "object-store")]
        return crate::objstore::list_prefix(dir);
        #[cfg(not(feature = "object-store"))]
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "{dir} is an object-store URI — rebuild with `--features object-store` to browse it"
            ),
        });
    }
    let base = Path::new(dir);
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(base)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue; // hide dotfiles/dirs
        }
        let meta = entry.metadata().ok();
        if meta.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
            let format = if path.join("metadata").is_dir() {
                Some(Format::Iceberg.as_str().to_string())
            } else {
                None
            };
            entries.push(DirEntry {
                name,
                path: path.display().to_string(),
                kind: "dir",
                format,
                size: None,
            });
        } else if let Some(fmt) = format_from_extension(&path) {
            entries.push(DirEntry {
                name,
                path: path.display().to_string(),
                kind: "file",
                format: Some(fmt.as_str().to_string()),
                size: meta.map(|m| m.len()),
            });
        }
    }
    // Dirs first, then files; alphabetical within each group.
    entries.sort_by(|a, b| {
        (a.kind == "file").cmp(&(b.kind == "file")).then_with(|| {
            a.name
                .to_ascii_lowercase()
                .cmp(&b.name.to_ascii_lowercase())
        })
    });
    Ok(DirListing {
        dir: base.display().to_string(),
        parent: base.parent().map(|p| p.display().to_string()),
        entries,
    })
}

pub(crate) fn format_from_extension(path: &Path) -> Option<Format> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("parquet") | Some("pq") => Some(Format::Parquet),
        Some("csv") => Some(Format::Csv),
        Some("tsv") => Some(Format::Tsv),
        Some("json") | Some("ndjson") | Some("jsonl") => Some(Format::Json),
        _ => None,
    }
}

fn sniff_magic(path: &Path) -> Result<Format> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut head = [0u8; 4];
    match file.read_exact(&mut head) {
        Ok(()) if &head == b"PAR1" => return Ok(Format::Parquet),
        Ok(()) => {}
        // Fewer than 4 bytes: too short to be Parquet — fall through to the error below.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
        // A real IO error (permissions, etc.) should surface, not be misread as "not parquet".
        Err(e) => return Err(EngineError::Io(e)),
    }
    Err(EngineError::UnsupportedFormat {
        detail: format!(
            "cannot infer the format of {} (unknown extension and not a Parquet file); \
             pass a .parquet/.csv/.json path",
            path.display()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_detection() {
        assert_eq!(
            Source::detect_ext_only("t.parquet").unwrap(),
            Format::Parquet
        );
        assert_eq!(Source::detect_ext_only("t.csv").unwrap(), Format::Csv);
        assert_eq!(Source::detect_ext_only("t.jsonl").unwrap(), Format::Json);
    }

    impl Source {
        // Test helper: extension-only detection (no filesystem access).
        fn detect_ext_only(p: &str) -> Option<Format> {
            format_from_extension(Path::new(p))
        }
    }
}
