//! Catalogs: where a table's name becomes something an engine can read.
//!
//! Every surface names a table with one string. Before an engine can read it, something has to
//! say what that string refers to, a format and where the bytes are, and the file browser needs
//! the same something to say what lives under a name. That is a [`Catalog`]:
//!
//! - [`Catalog::load_table`] says what a name refers to, as a [`TableHandle`].
//! - [`Catalog::list`] says what lives under a name, in the shape the browser shows.
//!
//! [`PathCatalog`] is the catalog of names that are locations: file paths, object-store URIs and
//! database URIs. It is how Lakeleto has always resolved them, moved behind the trait with no
//! change in behaviour. [`Source::detect_in`] is its `load_table`, and
//! [`list_dir`](crate::source::list_dir) is its `list`.
//!
//! A catalog that serves tables by name is another implementation of the same two methods. With
//! the `catalog` feature, [`Catalogs`] serves the Iceberg REST catalogs configured on this machine:
//! `catalog://prod/sales/orders` resolves to the metadata file the catalog says is current, and
//! to the credentials it vends for the table's files. See [`CatalogRef`] for how those names are
//! written.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::context::RequestContext;
use crate::error::Result;
use crate::source::{Codec, DirListing, Format, Source};

#[cfg(feature = "catalog")]
pub mod config;
mod path;
mod reference;
#[cfg(feature = "catalog")]
mod registry;
#[cfg(feature = "catalog")]
mod rest;
#[cfg(feature = "catalog")]
mod vended;

#[cfg(feature = "catalog")]
pub use config::CatalogConfig;
pub use path::PathCatalog;
pub use reference::{is_catalog_name, is_catalog_uri, CatalogRef, CATALOG_SCHEME};
#[cfg(feature = "catalog")]
pub use registry::{CatalogListing, Catalogs};

/// Something that can say what a table's name refers to, and what lives under a name.
///
/// Synchronous, like [`Engine`](crate::Engine): a caller runs it on whatever thread it is on, and
/// an implementation that needs async I/O bridges to it, as the object-store reads already do.
///
/// Both methods take the call's [`RequestContext`]. Asking a catalog is a read, performed as
/// somebody and within somebody's deadline, and the context is where both come from.
pub trait Catalog: Send + Sync {
    /// What lives under `namespace`, as the file browser shows it.
    ///
    /// Child namespaces are `kind: "dir"` entries. Tables are `kind: "file"` entries carrying
    /// their format. Namespaces come first, and each group is sorted by name. Every entry's `path`
    /// is a name this catalog accepts back: the browser walks down by passing it to `list` again,
    /// and opens a table by passing it to [`load_table`](Self::load_table).
    fn list(&self, ctx: &RequestContext, namespace: &Path) -> Result<DirListing>;

    /// What `table` refers to: the format to read it as, and where its bytes are.
    fn load_table(&self, ctx: &RequestContext, table: &Path) -> Result<TableHandle>;
}

/// What a catalog says a table is: enough for an engine to read it.
///
/// Distinct from [`Source`], because a source also carries how the caller wants the table read
/// (a JSON records path, flattening). That is the caller's choice, and no catalog's business.
/// [`into_source`](Self::into_source) joins the two halves.
///
/// `#[non_exhaustive]` because a catalog that serves tables by name has more to say about one than
/// a path does, such as the table's properties, and saying it must not break code that builds or
/// matches a handle today.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableHandle {
    /// Where the table is: a file, a directory, an object-store URI or a database URI. For a table
    /// a catalog serves, its current metadata file.
    pub location: PathBuf,
    /// The format to read it as.
    pub format: Format,
    /// How its bytes are compressed, for a text file whose name says so. See [`Source::codec`].
    pub codec: Option<Codec>,
    /// The table's current metadata, when its catalog handed it over with the location, so an
    /// engine plans from it rather than reading the file again.
    pub metadata: Option<Bytes>,
    /// The table's properties, as its catalog reports them. Empty for a path.
    pub properties: BTreeMap<String, String>,
    /// How the table's files are read, when its catalog decided. `None` reads them as any
    /// location is read.
    #[cfg(feature = "object-store")]
    pub storage: Option<TableStorage>,
}

/// Whose credentials read a catalog table's files.
#[cfg(feature = "object-store")]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TableStorage {
    /// Credentials the catalog vended for this table, renewed through it before they expire.
    Vended(crate::objstore::StoreOptions),
    /// Storage keys configured for the catalog on this machine.
    Catalog(crate::objstore::StoreOptions),
    /// Neither: the files are read as the engine reads any location, with these object-store
    /// settings added (an endpoint, a region), which say where the table is and not who reads it.
    Ambient(Vec<(String, String)>),
}

#[cfg(feature = "object-store")]
impl TableStorage {
    /// Which of the three this is, as a read reports it: `vended`, `catalog` or `ambient`.
    pub fn source(&self) -> &'static str {
        match self {
            TableStorage::Vended(_) => "vended",
            TableStorage::Catalog(_) => "catalog",
            TableStorage::Ambient(_) => "ambient",
        }
    }
}

impl TableHandle {
    /// A table at `location`, read as `format`, with no compression.
    pub fn new(location: impl Into<PathBuf>, format: Format) -> TableHandle {
        TableHandle {
            location: location.into(),
            format,
            codec: None,
            metadata: None,
            properties: BTreeMap::new(),
            #[cfg(feature = "object-store")]
            storage: None,
        }
    }

    /// This table, compressed with `codec`.
    pub fn with_codec(mut self, codec: Option<Codec>) -> TableHandle {
        self.codec = codec;
        self
    }

    /// This table, with the current metadata its catalog handed over.
    pub fn with_metadata(mut self, metadata: Bytes) -> TableHandle {
        self.metadata = Some(metadata);
        self
    }

    /// This table, with the properties its catalog reports.
    pub fn with_properties(mut self, properties: BTreeMap<String, String>) -> TableHandle {
        self.properties = properties;
        self
    }

    /// This table, its files read as `storage` says.
    #[cfg(feature = "object-store")]
    pub fn with_storage(mut self, storage: TableStorage) -> TableHandle {
        self.storage = Some(storage);
        self
    }

    /// The source an engine reads for this table, with no reading options set. The caller adds
    /// those ([`Source::with_json_path`], [`Source::with_flatten`]).
    pub fn into_source(self) -> Source {
        Source {
            path: self.location,
            format: self.format,
            codec: self.codec,
            json_path: None,
            flatten: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A handle becomes a source carrying only what the catalog knew. How to read it (a JSON
    /// records path, flattening) stays the caller's to add.
    #[test]
    fn a_handle_becomes_a_source_with_no_reading_options() {
        let source = TableHandle::new("s3://bucket/day.csv.gz", Format::Csv)
            .with_codec(Some(Codec::Gzip))
            .into_source();
        assert_eq!(source.path, PathBuf::from("s3://bucket/day.csv.gz"));
        assert_eq!(
            (source.format, source.codec),
            (Format::Csv, Some(Codec::Gzip))
        );
        assert_eq!((source.json_path, source.flatten), (None, None));
    }
}
