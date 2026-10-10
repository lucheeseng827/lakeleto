//! [`Catalogs`]: the catalogs configured on this machine, by name.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use super::config::{self, CatalogConfig};
use super::reference::{is_catalog_uri, CatalogRef};
use super::rest::RestCatalog;
use super::vended::{storage_for, Renewal};
use super::{Catalog, TableHandle};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};
use crate::source::{is_object_uri, DirEntry, DirListing, Format};

/// The catalogs `catalog://` references name, each by its configured name.
///
/// [`Catalogs::configured`] is the process's: what `$LAKELETO_HOME/catalogs.toml` and the
/// `LAKELETO_CATALOG__…` variables configure, read the first time a reference is used, so a
/// malformed file fails catalog references and nothing else. An engine or a server that is handed
/// its catalogs some other way builds them with [`Catalogs::from_configs`].
///
/// A client for each catalog is built once and kept, so its login token and what its
/// `GET /v1/config` said are fetched once per process rather than once per read.
pub struct Catalogs {
    state: OnceLock<std::result::Result<BTreeMap<String, Arc<RestCatalog>>, String>>,
    load: Box<dyn Fn() -> Result<config::Loaded> + Send + Sync>,
}

/// A listing of a catalog's namespaces and tables, and whether it was cut short.
pub struct CatalogListing {
    pub listing: DirListing,
    /// The catalog had more entries than one listing returns.
    pub truncated: bool,
}

impl Catalogs {
    /// The catalogs configured for this process: `$LAKELETO_HOME/catalogs.toml`, then the
    /// environment. Read once, on first use; a warning loading them raises is printed then.
    pub fn configured() -> Arc<Catalogs> {
        static PROCESS: OnceLock<Arc<Catalogs>> = OnceLock::new();
        PROCESS
            .get_or_init(|| {
                Arc::new(Catalogs::lazy(|| {
                    let file = crate::workspace::default_home().join(config::FILE_NAME);
                    let (env, skipped) = config::utf8_env(std::env::vars_os());
                    let mut loaded = config::load(&file, env)?;
                    loaded.warnings.extend(
                        skipped
                            .into_iter()
                            .map(|name| format!("{name} is not UTF-8, so Lakeleto ignores it")),
                    );
                    for warning in &loaded.warnings {
                        eprintln!("lakeleto: {warning}");
                    }
                    Ok(loaded)
                }))
            })
            .clone()
    }

    /// The catalogs configured in `file` (when it exists) and `env`, loaded now.
    pub fn load(file: &Path, env: impl IntoIterator<Item = (String, String)>) -> Result<Catalogs> {
        Catalogs::load_from(config::load(file, env)?.catalogs)
    }

    /// These catalogs, checked now.
    pub fn from_configs(configs: impl IntoIterator<Item = CatalogConfig>) -> Result<Catalogs> {
        let mut catalogs = BTreeMap::new();
        for config in configs {
            config.validate()?;
            catalogs.insert(config.name().to_string(), config);
        }
        Catalogs::load_from(catalogs)
    }

    fn load_from(catalogs: BTreeMap<String, CatalogConfig>) -> Result<Catalogs> {
        let registry = Catalogs::lazy(move || {
            Ok(config::Loaded {
                catalogs: catalogs.clone(),
                warnings: Vec::new(),
            })
        });
        registry.clients()?;
        Ok(registry)
    }

    fn lazy(load: impl Fn() -> Result<config::Loaded> + Send + Sync + 'static) -> Catalogs {
        Catalogs {
            state: OnceLock::new(),
            load: Box::new(load),
        }
    }

    /// A client per catalog, built the first time they are asked for.
    fn clients(&self) -> Result<&BTreeMap<String, Arc<RestCatalog>>> {
        self.state
            .get_or_init(|| {
                let loaded = (self.load)().map_err(|e| e.to_string())?;
                loaded
                    .catalogs
                    .into_iter()
                    .map(|(name, config)| {
                        let client = RestCatalog::new(config).map_err(|e| e.to_string())?;
                        Ok((name, Arc::new(client)))
                    })
                    .collect()
            })
            .as_ref()
            .map_err(|e| EngineError::Other(e.clone()))
    }

    /// Every configured catalog's configuration, by name.
    pub fn configs(&self) -> Result<Vec<&CatalogConfig>> {
        Ok(self.clients()?.values().map(|c| c.config()).collect())
    }

    fn client(&self, name: &str) -> Result<&Arc<RestCatalog>> {
        let clients = self.clients()?;
        clients.get(name).ok_or_else(|| {
            let configured = if clients.is_empty() {
                format!(
                    "no catalogs are configured. Add `[catalog.{name}]` to {}, or set \
                     {}",
                    crate::workspace::default_home()
                        .join(config::FILE_NAME)
                        .display(),
                    config::env_var(name, "uri"),
                )
            } else {
                format!(
                    "the configured catalogs are {}",
                    clients
                        .keys()
                        .map(|n| format!("`{n}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no catalog named `{name}` is configured: {configured}"),
            ))
        })
    }

    /// What lives under `reference`: the configured catalogs under `catalog://`, a catalog's
    /// namespaces under it, a namespace's child namespaces and tables under the namespace. A
    /// reference to a table lists the namespace of that name, as `ls` lists a directory named
    /// without its trailing `/`.
    pub fn list(&self, ctx: &RequestContext, reference: &CatalogRef) -> Result<CatalogListing> {
        ctx.check()?;
        let Some(name) = reference.catalog() else {
            let entries = self
                .clients()?
                .keys()
                .map(|name| DirEntry {
                    name: name.clone(),
                    path: format!("catalog://{name}/"),
                    kind: "dir",
                    format: None,
                    size: None,
                })
                .collect();
            return Ok(CatalogListing {
                listing: DirListing {
                    dir: CatalogRef::root().to_string(),
                    parent: None,
                    entries,
                },
                truncated: false,
            });
        };
        let client = self.client(name)?;
        let namespace = reference.as_namespace();
        let levels = namespace.namespace();
        let namespaces = client.namespaces(ctx, levels)?;
        let mut entries: Vec<DirEntry> = namespaces
            .names
            .iter()
            .map(|level| DirEntry {
                name: level.clone(),
                path: namespace.child_namespace(level).to_string(),
                kind: "dir",
                format: None,
                size: None,
            })
            .collect();
        let mut truncated = namespaces.truncated;
        // Iceberg keeps tables in namespaces, so a catalog itself holds none.
        if !levels.is_empty() {
            let tables = client.tables(ctx, levels)?;
            truncated |= tables.truncated;
            entries.extend(tables.names.iter().map(|table| DirEntry {
                name: table.clone(),
                path: namespace.child_table(table).to_string(),
                kind: "file",
                format: Some(Format::Iceberg.as_str().to_string()),
                size: None,
            }));
        }
        entries.sort_by(|a, b| {
            (a.kind == "file").cmp(&(b.kind == "file")).then_with(|| {
                a.name
                    .to_ascii_lowercase()
                    .cmp(&b.name.to_ascii_lowercase())
            })
        });
        Ok(CatalogListing {
            listing: DirListing {
                dir: namespace.to_string(),
                parent: namespace.parent().map(|p| p.to_string()),
                entries,
            },
            truncated,
        })
    }

    /// What the table `reference` names is: its current metadata, where that is, and whose
    /// credentials read its files.
    pub fn load_table(&self, ctx: &RequestContext, reference: &CatalogRef) -> Result<TableHandle> {
        ctx.check()?;
        let (Some(name), Some(table)) = (reference.catalog(), reference.table()) else {
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "{reference} names a catalog or a namespace, not a table. List it with \
                     `lakeleto catalog ls {reference}`"
                ),
            });
        };
        let namespace = reference.namespace();
        if namespace.is_empty() {
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "{reference} names a table directly under catalog `{name}`, and an Iceberg \
                     catalog keeps its tables in namespaces: catalog://{name}/<namespace>/{table}"
                ),
            });
        }
        let client = self.client(name)?;
        let Some(loaded) = client.load_table(ctx, namespace, table)? else {
            let as_namespace = reference.as_namespace();
            if client.namespace_exists(ctx, as_namespace.namespace())? {
                return Err(EngineError::UnsupportedFormat {
                    detail: format!(
                        "{reference} is a namespace, not a table. List it with `lakeleto catalog \
                         ls {as_namespace}`"
                    ),
                });
            }
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("catalog `{name}` has no table `{}`", reference.dotted()),
            )));
        };
        if loaded.config.get("scan-planning-mode").map(String::as_str) == Some("server") {
            return Err(EngineError::UnsupportedOperation {
                engine: "catalog".to_string(),
                op: format!("read {reference}"),
                hint: format!(
                    "catalog `{name}` requires server-side scan planning for this table, which \
                     Lakeleto does not do yet"
                ),
            });
        }
        if restricts(loaded.read_restrictions.as_ref()) {
            return Err(EngineError::Forbidden(format!(
                "catalog `{name}` requires row filters or column masks on {reference}, and \
                 Lakeleto cannot apply them yet, so it does not read the table rather than show \
                 what the catalog withholds"
            )));
        }
        let metadata = serde_json::to_vec(&loaded.metadata)
            .map_err(|e| EngineError::Other(format!("catalog `{name}`: {e}")))?;
        let properties = super::rest::string_map(loaded.metadata.get("properties"));
        let mut handle = TableHandle::new(&loaded.metadata_location, Format::Iceberg)
            .with_metadata(metadata.into())
            .with_properties(properties);
        // The table's own location decides whose credentials its files need; its metadata file is
        // under it.
        let location = loaded
            .metadata
            .get("location")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&loaded.metadata_location)
            .to_string();
        if is_object_uri(&location) {
            let server = client.server(ctx)?;
            let renewal = Renewal {
                catalog: client.clone(),
                namespace: namespace.to_vec(),
                table: table.to_string(),
                location,
            };
            handle = handle.with_storage(storage_for(
                &server,
                &loaded,
                renewal,
                &reference.to_string(),
            )?);
        }
        Ok(handle)
    }
}

/// Does a `read-restrictions` object require anything? A missing or empty one does not.
fn restricts(restrictions: Option<&serde_json::Value>) -> bool {
    let Some(restrictions) = restrictions else {
        return false;
    };
    let projections = restrictions
        .get("required-column-projections")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|p| !p.is_empty());
    let filter = restrictions
        .get("required-row-filter")
        .is_some_and(|f| !f.is_null());
    projections || filter
}

/// Names that start with `catalog://`, as the file browser and the CLI pass them.
impl Catalog for Catalogs {
    fn list(&self, ctx: &RequestContext, namespace: &Path) -> Result<DirListing> {
        let reference = parse(namespace)?;
        Catalogs::list(self, ctx, &reference).map(|l| l.listing)
    }

    fn load_table(&self, ctx: &RequestContext, table: &Path) -> Result<TableHandle> {
        let reference = parse(table)?;
        Catalogs::load_table(self, ctx, &reference)
    }
}

fn parse(path: &Path) -> Result<CatalogRef> {
    let text = path.to_string_lossy();
    if !is_catalog_uri(&text) {
        return Err(EngineError::Other(format!(
            "{text} is not a catalog reference: one starts with `catalog://`"
        )));
    }
    CatalogRef::parse(&text)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The catalogs answer, as a [`Catalog`], for `catalog://` names only.
    #[test]
    fn only_a_catalog_reference_names_a_catalog_table() {
        assert_eq!(
            parse(Path::new("catalog://lab/db/orders")).unwrap(),
            CatalogRef::parse("catalog://lab/db/orders").unwrap()
        );
        let err = parse(Path::new("/data/orders.parquet")).unwrap_err();
        assert!(
            err.to_string().contains("is not a catalog reference"),
            "{err}"
        );
    }

    #[test]
    fn read_restrictions_count_only_when_they_restrict() {
        assert!(!restricts(None));
        assert!(!restricts(Some(&serde_json::json!({}))));
        assert!(!restricts(Some(&serde_json::json!({
            "required-column-projections": [], "required-row-filter": null
        }))));
        assert!(restricts(Some(&serde_json::json!({
            "required-column-projections": [{"field-id": 4, "action": "show-last-4"}]
        }))));
        assert!(restricts(Some(&serde_json::json!({
            "required-row-filter": {"type": "eq", "left": {"type": "reference", "id": 1}, "right": {"type": "literal", "value": "US"}}
        }))));
    }

    /// A reference to an unknown catalog names the ones that are configured, and with none
    /// configured says where to configure one.
    #[test]
    fn an_unknown_catalog_names_the_configured_ones() {
        let catalogs = Catalogs::from_configs([
            CatalogConfig::new("dev")
                .unwrap()
                .with("uri", "http://localhost:8181"),
            CatalogConfig::new("prod")
                .unwrap()
                .with("uri", "https://c.example.com"),
        ])
        .unwrap();
        let ctx = RequestContext::detached();
        let err = catalogs
            .load_table(
                &ctx,
                &CatalogRef::parse("catalog://stage/sales/orders").unwrap(),
            )
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no catalog named `stage`") && err.contains("`dev`, `prod`"),
            "{err}"
        );

        let none = Catalogs::from_configs([]).unwrap();
        let err = none
            .list(&ctx, &CatalogRef::parse("catalog://prod/").unwrap())
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("no catalogs are configured")
                && err.contains("LAKELETO_CATALOG__PROD__URI"),
            "{err}"
        );
    }

    /// `catalog://` lists the configured catalogs, with no network, each as a directory.
    #[test]
    fn the_root_lists_the_configured_catalogs() {
        let catalogs = Catalogs::from_configs([
            CatalogConfig::new("prod")
                .unwrap()
                .with("uri", "https://c.example.com"),
            CatalogConfig::new("dev")
                .unwrap()
                .with("uri", "http://localhost:8181"),
        ])
        .unwrap();
        let listing = catalogs
            .list(&RequestContext::detached(), &CatalogRef::root())
            .unwrap()
            .listing;
        let shown: Vec<_> = listing
            .entries
            .iter()
            .map(|e| (e.name.as_str(), e.path.as_str(), e.kind))
            .collect();
        assert_eq!(
            shown,
            [
                ("dev", "catalog://dev/", "dir"),
                ("prod", "catalog://prod/", "dir")
            ]
        );
        assert_eq!((listing.dir.as_str(), listing.parent), ("catalog://", None));
    }

    /// References that cannot name a table are refused before any request, each with the next step.
    #[test]
    fn a_reference_that_is_not_a_table_is_refused_before_any_request() {
        let catalogs = Catalogs::from_configs([CatalogConfig::new("prod")
            .unwrap()
            .with("uri", "https://unreachable.invalid")])
        .unwrap();
        let ctx = RequestContext::detached();
        for (s, says) in [
            ("catalog://prod/sales/", "names a catalog or a namespace"),
            ("catalog://prod/", "names a catalog or a namespace"),
            ("catalog://prod/orders", "directly under catalog `prod`"),
        ] {
            let err = catalogs
                .load_table(&ctx, &CatalogRef::parse(s).unwrap())
                .unwrap_err()
                .to_string();
            assert!(err.contains(says), "{s}: {err}");
        }
    }

    /// A malformed configuration fails catalog references when they are used, and says why.
    #[test]
    fn a_bad_configuration_fails_on_first_use() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("catalogs.toml");
        std::fs::write(&file, "[catalog.prod]\nuri = \"http://c.example.com\"\n").unwrap();
        let err = Catalogs::load(&file, Vec::new()).err().unwrap().to_string();
        assert!(err.contains("plain `http` to another machine"), "{err}");
    }
}
