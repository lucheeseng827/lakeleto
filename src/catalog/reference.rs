//! `catalog://` references: how a table that lives in a catalog is named.
//!
//! ```text
//! catalog://<catalog>/<level>/…/<level>/<table>
//!            └ name ┘ └──── namespace ────┘ └ table ┘
//! ```
//!
//! - `<catalog>` is a name configured on this machine, never a hostname: `[a-z0-9][a-z0-9-]*`, at
//!   most 63 characters, matched without regard to case.
//! - The last segment is the table and the ones before it are namespace levels. A trailing `/`
//!   names a namespace instead: `catalog://prod/` is catalog `prod`, `catalog://prod/sales/` is
//!   its namespace `sales`, and `catalog://` alone is every configured catalog.
//! - Segments are percent-encoded UTF-8, so any name can be written: a `/` in a name is `%2F`, an
//!   `@` is `%40` and a `%` is `%25`. An empty segment is refused.
//! - A reference never carries a credential.
//!
//! A reference is recognised in every build, as object-store and database URIs are, so a build
//! without catalog support can say what to rebuild with instead of looking for a file.

use std::fmt;

use crate::error::{EngineError, Result};

/// The scheme every catalog reference starts with.
pub const CATALOG_SCHEME: &str = "catalog";

/// The longest catalog name: one DNS label, so a name always fits wherever a label does.
const MAX_NAME: usize = 63;

/// Does `s` name a catalog, a namespace in one or a table in one (`catalog://…`)?
pub fn is_catalog_uri(s: &str) -> bool {
    s.split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case(CATALOG_SCHEME))
}

/// Is `name` a catalog name: `[a-z0-9][a-z0-9-]*`, at most 63 characters, any case?
///
/// There is no `_`, so a name maps onto an environment variable and back (`my-cat` is
/// `LAKELETO_CATALOG__MY_CAT__…`).
pub fn is_catalog_name(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= MAX_NAME
        && chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// A parsed `catalog://` reference: every configured catalog, one catalog, a namespace in one, or
/// a table in one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogRef {
    /// The catalog, lowercase. `None` for `catalog://`, which names every configured catalog.
    catalog: Option<String>,
    /// The namespace levels, decoded. Empty for a catalog, and for a table named directly under
    /// one, which no Iceberg catalog has.
    namespace: Vec<String>,
    /// The table, decoded. `None` when the reference names a namespace or a catalog.
    table: Option<String>,
}

impl CatalogRef {
    /// `catalog://`: every configured catalog.
    pub fn root() -> CatalogRef {
        CatalogRef {
            catalog: None,
            namespace: Vec::new(),
            table: None,
        }
    }

    /// Catalog `name` itself, which lists its top-level namespaces.
    pub fn catalog_named(name: &str) -> Result<CatalogRef> {
        if !is_catalog_name(name) {
            return Err(bad_name(name));
        }
        Ok(CatalogRef {
            catalog: Some(name.to_ascii_lowercase()),
            namespace: Vec::new(),
            table: None,
        })
    }

    /// Parse a `catalog://` reference.
    pub fn parse(s: &str) -> Result<CatalogRef> {
        let rest = match s.split_once("://") {
            Some((scheme, rest)) if scheme.eq_ignore_ascii_case(CATALOG_SCHEME) => rest,
            _ => {
                return Err(EngineError::Other(format!(
                    "`{s}` is not a catalog reference: one starts with `catalog://`"
                )))
            }
        };
        if rest.is_empty() {
            return Ok(CatalogRef::root());
        }
        let names_namespace = rest.ends_with('/');
        let mut segments: Vec<&str> = rest.split('/').collect();
        if names_namespace {
            segments.pop();
        }
        let name = segments.remove(0);
        let mut reference = CatalogRef::catalog_named(name)?;
        let mut decoded = Vec::with_capacity(segments.len());
        for (i, segment) in segments.iter().enumerate() {
            let last = i + 1 == segments.len();
            if segment.is_empty() {
                return Err(EngineError::Other(format!(
                    "`{s}` has an empty segment: a namespace level or a table name is never empty"
                )));
            }
            if let Some((_, snapshot)) = segment.split_once('@') {
                return Err(if last && !names_namespace {
                    EngineError::UnsupportedFormat {
                        detail: format!(
                            "`{s}` asks for snapshot `{snapshot}`. Reading a table as it was at \
                             a snapshot arrives in Lakeleto 0.5.0, so drop `@{snapshot}` to read \
                             the table as it is now"
                        ),
                    }
                } else {
                    EngineError::Other(format!(
                        "`{s}` has an `@` in a namespace level: write a literal `@` as `%40`"
                    ))
                });
            }
            decoded.push(decode(segment).ok_or_else(|| {
                EngineError::Other(format!(
                    "`{s}` has a segment that is not valid percent-encoded UTF-8: `{segment}`"
                ))
            })?);
        }
        if !names_namespace {
            reference.table = decoded.pop();
        }
        reference.namespace = decoded;
        Ok(reference)
    }

    /// The catalog's name, or `None` for every configured catalog.
    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    /// The namespace levels, decoded.
    pub fn namespace(&self) -> &[String] {
        &self.namespace
    }

    /// The table's name, decoded, when the reference names a table.
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    /// This reference read as a namespace: a table's name becomes the namespace's last level, so
    /// `catalog://prod/sales` lists `sales` as `catalog://prod/sales/` does.
    pub fn as_namespace(&self) -> CatalogRef {
        let mut namespace = self.namespace.clone();
        namespace.extend(self.table.clone());
        CatalogRef {
            catalog: self.catalog.clone(),
            namespace,
            table: None,
        }
    }

    /// The namespace `level` under this one.
    pub fn child_namespace(&self, level: &str) -> CatalogRef {
        let mut child = self.as_namespace();
        child.namespace.push(level.to_string());
        child
    }

    /// The table `name` in this namespace.
    pub fn child_table(&self, name: &str) -> CatalogRef {
        let mut child = self.as_namespace();
        child.table = Some(name.to_string());
        child
    }

    /// What this reference is listed under: a table's namespace, a namespace's parent, a catalog's
    /// `catalog://`, and nothing for `catalog://` itself.
    pub fn parent(&self) -> Option<CatalogRef> {
        if self.table.is_some() {
            return Some(CatalogRef {
                table: None,
                ..self.clone()
            });
        }
        self.catalog.as_ref()?;
        if self.namespace.is_empty() {
            return Some(CatalogRef::root());
        }
        let mut parent = self.clone();
        parent.namespace.pop();
        Some(parent)
    }

    /// The namespace and table as a person reads them: levels joined by `.`, as Iceberg prints an
    /// identifier (`sales.emea.orders`).
    pub fn dotted(&self) -> String {
        let mut parts: Vec<&str> = self.namespace.iter().map(String::as_str).collect();
        parts.extend(self.table.as_deref());
        parts.join(".")
    }
}

/// The reference as written: what [`CatalogRef::parse`] reads back.
impl fmt::Display for CatalogRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{CATALOG_SCHEME}://")?;
        let Some(catalog) = &self.catalog else {
            return Ok(());
        };
        write!(f, "{catalog}/")?;
        for level in &self.namespace {
            write!(f, "{}/", encode(level))?;
        }
        if let Some(table) = &self.table {
            write!(f, "{}", encode(table))?;
        }
        Ok(())
    }
}

fn bad_name(name: &str) -> EngineError {
    EngineError::Other(format!(
        "`{name}` is not a catalog name: a name is letters, digits and `-`, starts with a letter \
         or digit, and is at most {MAX_NAME} characters"
    ))
}

/// Percent-encode a segment: `%`, `/` and `@`, which mean something in a reference, and control
/// characters, which a reader could not see. Everything else is written as it is, so a reference
/// stays readable.
fn encode(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for c in segment.chars() {
        match c {
            '%' | '/' | '@' => out.push_str(&format!("%{:02X}", c as u32)),
            c if c.is_control() => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            c => out.push(c),
        }
    }
    out
}

/// Percent-decode a segment, or `None` when it is not valid percent-encoded UTF-8.
fn decode(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> CatalogRef {
        CatalogRef::parse(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    /// Every shape the grammar names, with what each one means.
    #[test]
    fn each_shape_parses_to_what_it_names() {
        let root = parse("catalog://");
        assert_eq!((root.catalog(), root.table()), (None, None));

        for s in ["catalog://prod/", "catalog://prod"] {
            let catalog = parse(s);
            assert_eq!(catalog.catalog(), Some("prod"), "{s}");
            assert!(catalog.namespace().is_empty(), "{s}");
            assert_eq!(catalog.table(), None, "{s}");
        }

        let namespace = parse("catalog://prod/sales/emea/");
        assert_eq!(namespace.namespace(), ["sales", "emea"]);
        assert_eq!(namespace.table(), None);

        let table = parse("catalog://prod/sales/emea/orders");
        assert_eq!(table.namespace(), ["sales", "emea"]);
        assert_eq!(table.table(), Some("orders"));
        assert_eq!(table.dotted(), "sales.emea.orders");
    }

    /// A name is matched without regard to case and kept lowercase; levels and tables keep theirs.
    #[test]
    fn the_catalog_name_is_lowercased_and_nothing_else_is() {
        let table = parse("CATALOG://Prod/Sales/Orders");
        assert_eq!(table.catalog(), Some("prod"));
        assert_eq!(table.namespace(), ["Sales"]);
        assert_eq!(table.table(), Some("Orders"));
    }

    /// Percent-encoding lets any name through, and writing a reference back encodes it again, so a
    /// reference survives a round trip through a string.
    #[test]
    fn percent_encoded_names_round_trip() {
        let table = parse("catalog://prod/raw%40v2/odd%2Fname%25");
        assert_eq!(table.namespace(), ["raw@v2"]);
        assert_eq!(table.table(), Some("odd/name%"));
        assert_eq!(
            table.to_string(),
            "catalog://prod/raw%40v2/odd%2Fname%25",
            "written back as it was read"
        );

        for s in [
            "catalog://",
            "catalog://prod/",
            "catalog://prod/sales/",
            "catalog://prod/sales/emea/orders",
            "catalog://prod/café/zürich",
        ] {
            assert_eq!(parse(s).to_string(), s);
        }
        // Encoding what needs none is accepted, and written back the plain way.
        let over = parse("catalog://prod/caf%C3%A9/z%C3%BCrich");
        assert_eq!(over, parse("catalog://prod/café/zürich"));
        assert_eq!(over.to_string(), "catalog://prod/café/zürich");
        let odd = CatalogRef::catalog_named("prod")
            .unwrap()
            .child_namespace("a/b")
            .child_table("tab\u{1f}le");
        assert_eq!(odd.to_string(), "catalog://prod/a%2Fb/tab%1Fle");
        assert_eq!(parse(&odd.to_string()), odd);
    }

    /// What the grammar refuses, each with the reason.
    #[test]
    fn malformed_references_are_refused_with_the_reason() {
        for (s, says) in [
            ("catalog://prod//orders", "empty segment"),
            ("catalog://pr_od/sales/orders", "not a catalog name"),
            ("catalog://-prod/", "not a catalog name"),
            (
                "catalog://prod/sales/%E9t%C3",
                "not valid percent-encoded UTF-8",
            ),
            ("catalog://prod/sales/%2", "not valid percent-encoded UTF-8"),
            ("catalog://prod/sa@les/orders", "`%40`"),
            ("catalog://prod/sa@les/", "`%40`"),
            ("s3://bucket/key", "not a catalog reference"),
        ] {
            let err = CatalogRef::parse(s).unwrap_err().to_string();
            assert!(err.contains(says), "{s}: {err}");
        }
        let long = format!("catalog://{}/", "a".repeat(64));
        assert!(CatalogRef::parse(&long).is_err());
        assert!(CatalogRef::parse(&format!("catalog://{}/", "a".repeat(63))).is_ok());
    }

    /// A snapshot suffix is part of the grammar, read from 0.5.0. Until then it is refused with
    /// the release that reads it, rather than taken as part of the table's name.
    #[test]
    fn a_snapshot_is_refused_with_the_release_that_reads_it() {
        let err = CatalogRef::parse("catalog://prod/sales/orders@4182309823479")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("0.5.0") && err.contains("@4182309823479"),
            "{err}"
        );
    }

    /// Walking a listing up and down: a table's parent is its namespace, a namespace's is the one
    /// above it, a catalog's is `catalog://`, and that one has none.
    #[test]
    fn parents_and_children_walk_the_tree() {
        let table = parse("catalog://prod/sales/emea/orders");
        let emea = table.parent().unwrap();
        assert_eq!(emea.to_string(), "catalog://prod/sales/emea/");
        let sales = emea.parent().unwrap();
        assert_eq!(sales.to_string(), "catalog://prod/sales/");
        let prod = sales.parent().unwrap();
        assert_eq!(prod.to_string(), "catalog://prod/");
        assert_eq!(prod.parent().unwrap(), CatalogRef::root());
        assert_eq!(CatalogRef::root().parent(), None);

        assert_eq!(sales.child_namespace("emea"), emea);
        assert_eq!(emea.child_table("orders"), table);
        assert_eq!(
            parse("catalog://prod/sales").as_namespace().to_string(),
            "catalog://prod/sales/"
        );
    }

    #[test]
    fn the_scheme_is_recognised_in_any_case_and_nothing_else_is() {
        for s in ["catalog://", "catalog://prod/x", "Catalog://prod/"] {
            assert!(is_catalog_uri(s), "{s}");
        }
        for s in [
            "cat://acme/geo/cities",
            "catalog:prod",
            "s3://catalog/x",
            "/catalog/x",
        ] {
            assert!(!is_catalog_uri(s), "{s}");
        }
    }
}
