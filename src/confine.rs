//! `--root`: confining what a server reads to one directory.
//!
//! `lakeleto serve` and `lakeleto mcp` both take `--root`, and both refuse the same things with
//! the same checks, so the rule lives here once. A path is checked twice: [`entry`] before it is
//! resolved, so an out-of-root path never reaches the filesystem probes that classify it, and
//! [`members`] after, for the files a table reads besides the one it was named by.
//!
//! The root is expected canonical (the CLI canonicalizes `--root` at startup), since every check
//! compares it with canonicalized paths.

use std::path::Path;

use crate::error::{EngineError, Result};
use crate::source::{Format, Source};

/// The uniform "outside `--root`" refusal. The message is deliberately the same whether the path
/// is out-of-root, non-existent, or unreadable, so a caller can't use the server as an
/// existence/type oracle over the filesystem outside the root.
pub fn out_of_root() -> EngineError {
    EngineError::Forbidden("path is outside the server root (--root)".to_string())
}

/// Is `path` a reference rather than a local path: an object-store, database or catalog URI?
/// `--root` is local-filesystem only, so [`entry`] refuses every one of them under a root.
pub fn is_reference(path: &str) -> bool {
    crate::source::is_object_uri(path)
        || crate::source::is_database_uri(path)
        || crate::catalog::is_catalog_uri(path)
}

/// **Pre-resolve** confinement of a requested path to `root`: refuses object-store URIs and
/// anything anchored outside the root *before* [`Source::resolve`]/`detect` touches the
/// filesystem — so an out-of-root path can't be used as an existence/type/readability oracle (via
/// `detect`'s `is_dir`/`read_dir`/`sniff_magic`), nor trigger a recursive-`read_dir` DoS. A
/// missing leaf *inside* the root is allowed through so the reader still returns a normal 404.
/// No-op when no root is configured (the default "point at any file" behaviour).
pub fn entry(root: Option<&Path>, path: &str) -> Result<()> {
    let Some(root) = root else { return Ok(()) };
    // --root is local-filesystem only: no object-store, database or catalog references, refused
    // here before anything reaches the network. (A URI read as a path would almost always be
    // refused by the walk below too, having no ancestor that exists; this doesn't rely on it.)
    if is_reference(path) {
        return Err(out_of_root());
    }
    // Walk up to the nearest existing ancestor and canonicalize it (symlinks resolved). If that
    // lies under the root the request is in-root (a missing leaf 404s later); otherwise — or when
    // nothing along the path exists — it's refused with the same error, leaking nothing.
    let mut cur = Path::new(path);
    loop {
        if let Ok(canon) = std::fs::canonicalize(cur) {
            return if canon.starts_with(root) {
                Ok(())
            } else {
                Err(out_of_root())
            };
        }
        match cur.parent() {
            Some(p) if !p.as_os_str().is_empty() => cur = p,
            _ => return Err(out_of_root()),
        }
    }
}

/// Confine every file the engine will actually **read** for `source` to `root`. The entry path
/// is already gated by [`entry`], but a directory dataset reads every member `.parquet`
/// (a symlink escaping the root is caught here by canonicalizing each), and a table format reads
/// whatever paths its own metadata names — Iceberg's manifest list, manifests, delete files and
/// absolute data-file paths, or Delta's `add.path` entries — any of which can point outside the
/// table dir. No-op without a root.
///
/// Note the shape of this guard: it is a per-format traversal whitelist, not a generic filesystem
/// sandbox. Each arm has to know, format by format, every file its reader will subsequently open —
/// so a new readable format needs an arm here, and its absence is a confinement hole rather than a
/// missing feature.
pub fn members(root: Option<&Path>, source: &Source) -> Result<()> {
    let Some(root) = root else { return Ok(()) };
    match source.format {
        Format::Parquet if source.path.is_dir() => {
            for f in crate::source::list_parquet_files(&source.path) {
                canonical(root, &f)?;
            }
        }
        #[cfg(feature = "iceberg")]
        Format::Iceberg => {
            // Re-plan with the root so every path the reader will open (manifests + delete files
            // + data files) is validated *before* it is read — gating metadata too, not just data.
            crate::iceberg::plan_with_root(&source.path, Some(root))?;
        }
        #[cfg(feature = "delta")]
        Format::Delta => {
            // Same reasoning as Iceberg, and just as necessary: a Delta `add.path` may be absolute
            // or relative-with-`..`, so the log can name data files anywhere on the filesystem.
            // The planner memoizes, so this replay and the engine's own share one pass over the log.
            crate::engine::delta::plan_with_root(&source.path, Some(root))?;
        }
        _ => {}
    }
    Ok(())
}

/// Canonicalize `path` and require it under `root`; refuse (uniformly) on escape or any failure.
fn canonical(root: &Path, path: &Path) -> Result<()> {
    match std::fs::canonicalize(path) {
        Ok(canon) if canon.starts_with(root) => Ok(()),
        _ => Err(out_of_root()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Is `r` the uniform root refusal?
    fn is_out_of_root(r: Result<()>) -> bool {
        matches!(r, Err(EngineError::Forbidden(m)) if m == "path is outside the server root (--root)")
    }

    /// A temporary directory, and its canonical path for use as a root.
    fn scratch() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        (dir, root)
    }

    #[test]
    fn references_are_told_from_local_paths() {
        for reference in [
            "s3://bucket/t.parquet",
            "gs://bucket/t.csv",
            "az://container/t.json",
            "sqlite:///data/app.db?table=orders",
            "postgres://u@h/db",
            "mysql://u@h/db",
            "catalog://prod/sales/orders",
        ] {
            assert!(is_reference(reference), "{reference}");
        }
        for path in [
            "/data/t.parquet",
            "t.csv",
            "./exports",
            "s3_exports/t.csv",
            "catalog",
        ] {
            assert!(!is_reference(path), "{path}");
        }
    }

    #[test]
    fn no_root_allows_anything() {
        assert!(entry(None, "/etc/passwd").is_ok());
        assert!(entry(None, "s3://bucket/key.parquet").is_ok());
        let source = Source::with_format("/anywhere", Format::Parquet);
        assert!(members(None, &source).is_ok());
    }

    #[test]
    fn a_root_refuses_references_that_are_not_local_paths() {
        let (_dir, root) = scratch();
        for path in [
            "s3://bucket/t.parquet",
            "gs://bucket/t.csv",
            "sqlite:///tmp/app.db",
            "postgres://u@h/db",
            "catalog://prod/sales/orders",
            "CATALOG://prod/",
        ] {
            assert!(
                is_out_of_root(entry(Some(&root), path)),
                "{path} was let through"
            );
        }
    }

    #[test]
    fn a_root_allows_its_own_files_and_missing_leaves_inside_it() {
        let (_dir, root) = scratch();
        let file = root.join("t.csv");
        std::fs::write(&file, "a\n1\n").unwrap();
        assert!(entry(Some(&root), file.to_str().unwrap()).is_ok());
        assert!(entry(Some(&root), root.to_str().unwrap()).is_ok());
        // A missing file under the root resolves to its existing parent, so the read gets a
        // normal "not found" rather than the root refusal.
        let missing = root.join("sub").join("none.parquet");
        assert!(entry(Some(&root), missing.to_str().unwrap()).is_ok());
    }

    #[test]
    fn a_root_refuses_paths_outside_it_whether_or_not_they_exist() {
        let (_dir, root) = scratch();
        let (_other, elsewhere) = scratch();
        let file = elsewhere.join("t.csv");
        std::fs::write(&file, "a\n1\n").unwrap();
        assert!(is_out_of_root(entry(Some(&root), file.to_str().unwrap())));
        assert!(is_out_of_root(entry(
            Some(&root),
            elsewhere.join("none.csv").to_str().unwrap()
        )));
        // `..` is resolved before the comparison, so it can't climb out.
        let climb = root
            .join("..")
            .join(elsewhere.file_name().unwrap())
            .join("t.csv");
        assert!(is_out_of_root(entry(Some(&root), climb.to_str().unwrap())));
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_out_of_the_root_is_refused_at_entry_and_as_a_dataset_member() {
        let (_dir, root) = scratch();
        let (_other, elsewhere) = scratch();
        let outside = elsewhere.join("part-0.parquet");
        std::fs::write(&outside, b"PAR1").unwrap();
        // Named directly.
        let link = root.join("link.parquet");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        assert!(is_out_of_root(entry(Some(&root), link.to_str().unwrap())));
        // Or as one member of a dataset directory inside the root.
        let dataset = root.join("ds");
        std::fs::create_dir(&dataset).unwrap();
        std::fs::write(dataset.join("part-1.parquet"), b"PAR1").unwrap();
        std::os::unix::fs::symlink(&outside, dataset.join("part-2.parquet")).unwrap();
        assert!(entry(Some(&root), dataset.to_str().unwrap()).is_ok());
        let source = Source::with_format(&dataset, Format::Parquet);
        assert!(is_out_of_root(members(Some(&root), &source)));
        // Without the escaping member, the dataset is fine.
        std::fs::remove_file(dataset.join("part-2.parquet")).unwrap();
        assert!(members(Some(&root), &source).is_ok());
    }
}
