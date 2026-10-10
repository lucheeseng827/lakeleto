//! [`PathCatalog`]: the catalog of names that are locations.

use std::path::Path;

#[cfg(feature = "catalog")]
use super::CatalogRef;
use super::{is_catalog_uri, Catalog, TableHandle};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::{
    codec_of, format_from_name, is_database_uri, is_object_uri, list_parquet_files, sniff_magic,
    DirEntry, DirListing, Format, RemoteProbe, Source,
};

/// The catalog of names that are locations: a local file or directory, an object-store URI, or a
/// database URI.
///
/// The name says where the table is, so loading one means classifying it, by its scheme, its
/// extension, its directory's shape or its first bytes. Listing one means listing the directory or
/// prefix it names. [`Source::detect_in`](crate::Source::detect_in) and
/// [`list_dir`](crate::source::list_dir) are this catalog under their old names, and behave
/// exactly as they did.
///
/// The one thing a path catalog decides beyond the name is whose credentials an object-store read
/// spends when the call vended none. That is its [`RemoteProbe`], and there is no default: a
/// caller names the posture it resolves under, as it names a detached context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathCatalog {
    probe: RemoteProbe,
}

impl PathCatalog {
    /// A path catalog that reads object stores under `probe`'s posture.
    pub fn new(probe: RemoteProbe) -> PathCatalog {
        PathCatalog { probe }
    }

    /// Resolve `path` as `format` names, or detect its format when `format` is `None`.
    ///
    /// An explicit format short-circuits before any probe, which is what makes
    /// [`RemoteProbe::VendedOnly`] a cost a caller can always avoid: naming the format is the
    /// answer to being refused a probe.
    ///
    /// Not part of [`Catalog`]: reading a location as whatever format the caller names is
    /// something a path offers. A catalog that serves tables by name says what each one is.
    pub fn resolve(
        &self,
        ctx: &RequestContext,
        path: &Path,
        format: Option<&str>,
    ) -> Result<Source> {
        // A catalog says what its tables are, so a format that disagrees is a mistake to report,
        // not an override to apply.
        if path.to_str().is_some_and(is_catalog_uri) {
            let source = self.load_table(ctx, path)?.into_source();
            return match format.map(|f| (f, Format::parse(f))) {
                Some((_, Some(f))) if f != source.format => Err(EngineError::UnsupportedFormat {
                    detail: format!(
                        "{} is a catalog table, which its catalog serves as {}; drop `--format {f}`",
                        path.display(),
                        source.format
                    ),
                }),
                Some((f, None)) => Err(EngineError::UnsupportedFormat {
                    detail: format!("unknown format `{f}` (expected parquet/csv/tsv/json/iceberg)"),
                }),
                _ => Ok(source),
            };
        }
        match format {
            // The name still says how the bytes are compressed, whatever format they hold.
            Some(f) => Format::parse(f)
                .map(|fmt| Source::with_format(path, fmt).with_codec(codec_of(path)))
                .ok_or_else(|| EngineError::UnsupportedFormat {
                    detail: format!("unknown format `{f}` (expected parquet/csv/tsv/json/iceberg)"),
                }),
            None => self.load_table(ctx, path).map(TableHandle::into_source),
        }
    }

    /// List an object-store prefix as the call's identity, else as this catalog's posture allows.
    ///
    /// A listing is not a probe, because the caller asked for it, so [`RemoteProbe::Never`] does
    /// not refuse one. The posture still decides what it exists to decide: whether a call that
    /// vended no identity may read as the process. [`RemoteProbe::VendedOnly`] says no, here as in
    /// [`load_table`](Catalog::load_table).
    #[cfg(feature = "object-store")]
    fn list_prefix(&self, ctx: &RequestContext, uri: &str) -> Result<DirListing> {
        match (ctx.store_options(), self.probe) {
            (Some(vended), _) => crate::objstore::list_prefix_with(uri, vended),
            (None, RemoteProbe::VendedOnly) => Err(EngineError::Forbidden(format!(
                "cannot list {uri}: no credentials were vended for this call, and listing it \
                 would read as the server rather than as you"
            ))),
            (None, RemoteProbe::Ambient | RemoteProbe::Never) => crate::objstore::list_prefix(uri),
        }
    }

    #[cfg(not(feature = "object-store"))]
    fn list_prefix(&self, _ctx: &RequestContext, uri: &str) -> Result<DirListing> {
        Err(EngineError::UnsupportedFormat {
            detail: format!(
                "{uri} is an object-store URI — rebuild with `--features object-store` to browse it"
            ),
        })
    }
}

impl Catalog for PathCatalog {
    /// List a directory for the file browser: subdirectories and Parquet/CSV/JSON files (plus
    /// Iceberg-table dirs), dirs first, then files, each alphabetical. Non-data files are hidden.
    fn list(&self, ctx: &RequestContext, namespace: &Path) -> Result<DirListing> {
        // A catalog's namespaces are not a directory on this machine.
        if namespace.to_str().is_some_and(is_catalog_uri) {
            return Err(catalog_listing(namespace));
        }
        // Object-store prefixes are browsed through the object-store backend, not the filesystem.
        if let Some(uri) = namespace.to_str().filter(|s| is_object_uri(s)) {
            return self.list_prefix(ctx, uri);
        }
        let base = namespace;
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
                // Same order as `load_table`, and for the same reason: a Delta table's data files
                // are Parquet and it may also carry a `metadata/` dir, so checking `_delta_log`
                // second would label a table in the browser as something the reader then opens as
                // Delta.
                let format = if path.join("_delta_log").is_dir() {
                    Some(Format::Delta.as_str().to_string())
                } else if path.join("metadata").is_dir() {
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
            } else if let Some((fmt, _)) = format_from_name(&path) {
                // By the whole name, so a compressed file (`t.csv.gz`) is listed as what it holds.
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

    /// Classify `table` (directory shape -> extension -> magic bytes).
    ///
    /// Only the object-store branch consults the context or the posture: a local path is
    /// classified by stat'ing it and reading its first bytes, which needs no credentials and
    /// reaches nobody else's data.
    #[cfg_attr(not(feature = "object-store"), allow(unused_variables))]
    fn load_table(&self, ctx: &RequestContext, table: &Path) -> Result<TableHandle> {
        let path = table;

        // A table a catalog serves: classified without a request, since every catalog this build
        // reads serves Iceberg. Reading it asks the catalog, under the read's own context.
        if path.to_str().is_some_and(is_catalog_uri) {
            return catalog_table(path);
        }

        // Database connection URI (sqlite://… / postgres://… / mysql://…): a live DB, never a file.
        // Classify without touching the filesystem — the `database` engine parses the URI.
        if path.to_str().is_some_and(is_database_uri) {
            return Ok(TableHandle::new(path, Format::Database));
        }

        // Object-store URI (s3://…): classify by the key's extension without touching the
        // filesystem. Magic-byte sniffing would require fetching, so an unknown extension
        // needs an explicit `--format`.
        if path.to_str().is_some_and(is_object_uri) {
            if let Some((format, codec)) = format_from_name(path) {
                return Ok(TableHandle::new(path, format).with_codec(codec));
            }
            // No data-file extension: a bare prefix is likely an Iceberg table — one cheap probe
            // for a `metadata/` child. (Only object stores; a network round-trip, so gated behind
            // the feature and reached only when the name gives nothing away.)
            //
            // Whose round-trip it is, is the posture's to decide — see [`RemoteProbe`]. The
            // identity resolves the same way an engine's does: the call's, else this catalog's
            // fallback, else no probe.
            #[cfg(feature = "object-store")]
            {
                let identity: Option<std::borrow::Cow<'_, crate::objstore::StoreOptions>> =
                    match (self.probe, ctx.store_options()) {
                        (RemoteProbe::Never, _) => None,
                        (_, Some(vended)) => Some(std::borrow::Cow::Borrowed(vended)),
                        (RemoteProbe::Ambient, None) => Some(std::borrow::Cow::Owned(
                            crate::objstore::StoreOptions::from_env(),
                        )),
                        // Refused, and said so rather than falling through to the generic message
                        // below. The two are a different problem with a different fix: that one
                        // means the name gave nothing away, this one means nobody would tell us
                        // whose credentials to find out with. Only one of them is the operator's.
                        (RemoteProbe::VendedOnly, None) => {
                            return Err(EngineError::UnsupportedFormat {
                                detail: format!(
                                    "cannot infer the format of {} from its name, and no \
                                     credentials were vended for this call — probing it would \
                                     read as the server rather than as you. Declare the format \
                                     (`parquet`/`csv`/`json`/`iceberg`) on this location.",
                                    path.display()
                                ),
                            });
                        }
                    };
                if let (Some(opts), Some(uri)) = (identity, path.to_str()) {
                    // `?`, not a swallowed `false`: an identity that cannot address this URI is a
                    // fact about the caller, and reporting it as "not an Iceberg table" would send
                    // them to fix their `--format` instead of their credentials.
                    if crate::objstore::looks_like_iceberg_as(uri, &opts)? {
                        return Ok(TableHandle::new(path, Format::Iceberg));
                    }
                }
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
                return Ok(TableHandle::new(path, Format::Delta));
            }
            // An Iceberg table is a directory containing a `metadata/` catalog dir.
            if path.join("metadata").is_dir() {
                return Ok(TableHandle::new(path, Format::Iceberg));
            }
            // Otherwise a directory of `.parquet` files (incl. Hive-partitioned subdirs, and the
            // `foo.parquet/part-*.parquet` split-file shape) is read as one multi-file dataset.
            if !list_parquet_files(path).is_empty() {
                return Ok(TableHandle::new(path, Format::Parquet));
            }
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "{} is a directory but not an Iceberg table (no metadata/ subdir) and \
                     contains no .parquet files",
                    path.display()
                ),
            });
        }

        if let Some((format, codec)) = format_from_name(path) {
            return Ok(TableHandle::new(path, format).with_codec(codec));
        }

        // No/unknown extension: sniff the magic bytes.
        let format = sniff_magic(path)?;
        Ok(TableHandle::new(path, format))
    }
}

/// Classify a `catalog://` name: a table reference is an Iceberg table; a namespace, or a build
/// without catalog support, is refused with what to do instead.
#[cfg(feature = "catalog")]
fn catalog_table(path: &Path) -> Result<TableHandle> {
    let reference = CatalogRef::parse(&path.to_string_lossy())?;
    if reference.table().is_none() {
        return Err(EngineError::UnsupportedFormat {
            detail: format!(
                "{reference} names a catalog or a namespace, not a table. List it with \
                 `lakeleto catalog ls {reference}`"
            ),
        });
    }
    Ok(TableHandle::new(path, Format::Iceberg))
}

#[cfg(not(feature = "catalog"))]
fn catalog_table(path: &Path) -> Result<TableHandle> {
    Err(EngineError::UnsupportedFormat {
        detail: format!(
            "{} is a catalog reference — rebuild with `--features catalog` to read catalog tables",
            path.display()
        ),
    })
}

/// The refusal for listing a catalog reference as a path: its catalog lists it, in a build that
/// has them.
fn catalog_listing(namespace: &Path) -> EngineError {
    if cfg!(feature = "catalog") {
        EngineError::Other(format!(
            "{} is listed by its catalog, not as a path: use `Catalogs::list`",
            namespace.display()
        ))
    } else {
        EngineError::UnsupportedFormat {
            detail: format!(
                "{} is a catalog reference — rebuild with `--features catalog` to browse catalogs",
                namespace.display()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::source::Codec;

    /// Every shape a location can take, loaded through a `dyn Catalog`: the type a second catalog
    /// will sit beside this one as. A local file is named by its extension, a compressed one by
    /// both of its extensions, and an unnamed one by its first bytes. A table directory is named
    /// by its log or metadata, and a directory of Parquet files is one table. A database URI and
    /// an object named by its extension are classified without a read.
    #[test]
    fn a_location_loads_as_the_handle_its_shape_names() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("t.csv"), "a\n1\n").unwrap();
        std::fs::write(root.join("t.csv.gz"), b"\x1f\x8b not really gzip").unwrap();
        std::fs::write(root.join("unnamed"), b"PAR1 and the rest").unwrap();
        std::fs::create_dir_all(root.join("delta/_delta_log")).unwrap();
        std::fs::create_dir_all(root.join("iceberg/metadata")).unwrap();
        std::fs::create_dir_all(root.join("parts/year=2026")).unwrap();
        std::fs::write(root.join("parts/year=2026/part-0.parquet"), b"PAR1").unwrap();

        let catalog: &dyn Catalog = &PathCatalog::new(RemoteProbe::Ambient);
        let ctx = RequestContext::detached();
        for (location, format, codec) in [
            (root.join("t.csv"), Format::Csv, None),
            (root.join("t.csv.gz"), Format::Csv, Some(Codec::Gzip)),
            (root.join("unnamed"), Format::Parquet, None),
            (root.join("delta"), Format::Delta, None),
            (root.join("iceberg"), Format::Iceberg, None),
            (root.join("parts"), Format::Parquet, None),
            (
                PathBuf::from("sqlite:///data.db?table=orders"),
                Format::Database,
                None,
            ),
            (
                PathBuf::from("s3://bucket/logs/day.ndjson.zst"),
                Format::Json,
                Some(Codec::Zstd),
            ),
        ] {
            let handle = catalog
                .load_table(&ctx, &location)
                .unwrap_or_else(|e| panic!("{}: {e}", location.display()));
            assert_eq!(
                (handle.location.as_path(), handle.format, handle.codec),
                (location.as_path(), format, codec)
            );
        }
    }

    /// The refusals keep their wording, which tells the caller what to do next: a directory with
    /// nothing readable in it, and an object whose name gives nothing away. Under
    /// [`RemoteProbe::Never`] the object is never probed, so this holds in every build.
    #[test]
    fn a_location_with_nothing_to_go_on_is_refused_with_the_next_step() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("empty")).unwrap();
        let ctx = RequestContext::detached();

        let err = PathCatalog::new(RemoteProbe::Ambient)
            .load_table(&ctx, &dir.path().join("empty"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("is a directory but not an Iceberg table") && err.contains(".parquet"),
            "{err}"
        );

        let err = PathCatalog::new(RemoteProbe::Never)
            .load_table(&ctx, Path::new("s3://bucket/table"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("cannot infer the format of s3://bucket/table from its name")
                && err.contains("--format"),
            "{err}"
        );
    }

    /// The browse contract [`Catalog::list`] states, pinned for the catalog that has always kept
    /// it: directories first, then tables, each sorted by name ignoring case. Dotfiles, and files
    /// no reader takes, are hidden. A file carries its size, a table directory its format, and
    /// every entry a path the catalog takes back.
    #[test]
    fn a_directory_lists_as_the_browser_shows_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("B.csv"), "a\n1\n").unwrap();
        std::fs::write(root.join("a.parquet"), b"PAR1").unwrap();
        std::fs::write(root.join("notes.txt"), "not a table").unwrap();
        std::fs::write(root.join(".hidden.csv"), "a\n1\n").unwrap();
        std::fs::create_dir_all(root.join("Zeta")).unwrap();
        std::fs::create_dir_all(root.join("alpha/_delta_log")).unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();

        let catalog: &dyn Catalog = &PathCatalog::new(RemoteProbe::Ambient);
        let listing = catalog.list(&RequestContext::detached(), root).unwrap();
        let shown: Vec<_> = listing
            .entries
            .iter()
            .map(|e| (e.name.as_str(), e.kind, e.format.as_deref(), e.size))
            .collect();
        assert_eq!(
            shown,
            [
                ("alpha", "dir", Some("delta"), None),
                ("Zeta", "dir", None, None),
                ("a.parquet", "file", Some("parquet"), Some(4)),
                ("B.csv", "file", Some("csv"), Some(4)),
            ]
        );
        for entry in &listing.entries {
            assert_eq!(entry.path, root.join(&entry.name).display().to_string());
        }
        assert_eq!(listing.dir, root.display().to_string());
        assert_eq!(
            listing.parent,
            root.parent().map(|p| p.display().to_string())
        );
    }

    /// A local directory lists the same under every posture: no identity is consulted.
    #[test]
    fn a_local_directory_lists_the_same_under_every_posture() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("t.csv"), "a\n1\n").unwrap();
        for probe in [
            RemoteProbe::Ambient,
            RemoteProbe::VendedOnly,
            RemoteProbe::Never,
        ] {
            let listing = PathCatalog::new(probe)
                .list(&RequestContext::detached(), dir.path())
                .unwrap_or_else(|e| panic!("{probe:?}: {e}"));
            assert_eq!(listing.entries.len(), 1, "{probe:?}");
        }
    }

    // ---------------------------------------------------------------------------------------
    // Whose credentials a listing spends: the same question the probe answers
    // ---------------------------------------------------------------------------------------

    /// A context carrying a GCS identity. `objstore` refuses a provider whose family doesn't match
    /// the URI's scheme before it builds a store, so an `s3://` read that fails mentioning GCS can
    /// only have used this identity. No network, and no credentials, are needed to tell.
    #[cfg(feature = "object-store")]
    fn gcs_identity() -> RequestContext {
        let provider: object_store::gcp::GcpCredentialProvider = std::sync::Arc::new(
            object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
                bearer: "not-a-real-token".to_string(),
            }),
        );
        RequestContext::detached().with_store_options(
            crate::objstore::StoreOptions::empty()
                .with_credentials(crate::objstore::StoreCredentials::Gcs(provider)),
        )
    }

    /// A prefix is listed as the identity the call vended, under every posture. The environment
    /// is a fallback, not a preference: had any posture read it first, the error would not
    /// mention GCS.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_prefix_lists_as_the_context_says_rather_than_as_the_process() {
        let ctx = gcs_identity();
        for probe in [
            RemoteProbe::Ambient,
            RemoteProbe::VendedOnly,
            RemoteProbe::Never,
        ] {
            let err = PathCatalog::new(probe)
                .list(&ctx, Path::new("s3://bucket/prefix/"))
                .expect_err("a GCS identity cannot address an s3:// URI");
            assert!(err.to_string().contains("GCS"), "{probe:?}: {err}");
        }
    }

    /// The listing half of the confused deputy. Listing a prefix someone else named, as the server,
    /// spends a credential nobody authorized, and answers whether the server's role can see that
    /// bucket. [`RemoteProbe::VendedOnly`] refuses it, as it refuses the probe.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_server_posture_refuses_to_list_a_prefix_it_has_no_vended_identity_for() {
        let err = PathCatalog::new(RemoteProbe::VendedOnly)
            .list(
                &RequestContext::detached(),
                Path::new("s3://someone-elses-bucket/"),
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::Forbidden(_)), "{err}");
        let msg = err.to_string();
        assert!(msg.contains("no credentials were vended"), "{msg}");
        assert!(msg.contains("read as the server"), "{msg}");
    }

    /// Without the feature, a prefix is refused with the way to get it, not misread as a missing
    /// directory.
    #[cfg(not(feature = "object-store"))]
    #[test]
    fn a_prefix_without_the_feature_says_to_rebuild() {
        let err = PathCatalog::new(RemoteProbe::Ambient)
            .list(
                &RequestContext::detached(),
                Path::new("s3://bucket/prefix/"),
            )
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("s3://bucket/prefix/ is an object-store URI")
                && err.contains("--features object-store"),
            "{err}"
        );
    }
}
