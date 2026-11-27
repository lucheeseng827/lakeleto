//! The version of the HTTP contract `lakeleto serve` speaks, and how a client reads it.
//!
//! The contract is versioned in two parts, `major.minor`:
//!
//! - **The major is the path.** Every route is under `/v1`; a change that breaks a client would
//!   be served under `/v2`, beside `/v1`, rather than change what `/v1` means.
//! - **The minor counts additions.** It goes up when `/v1` gains an endpoint, a field or a
//!   parameter. Existing ones keep their meaning, so a client written against `1.0` works against
//!   any `1.x`, and a client ignores fields it doesn't know.
//!
//! A server says which version it speaks in two places: the `protocol` field of
//! `GET /v1/engines`, and the [`PROTOCOL_HEADER`] on every response, so a client learns it from
//! whatever request it made first. A server that sends neither predates versioning, and speaks a
//! subset of `1.0`.
//!
//! What each minor added:
//!
//! - `1.1` — new tables: `POST /v1/tables` and `POST /v1/workspaces/{id}/runs/{run_id}/table`
//!   (served only with `--write-dir`), the `writes` object on `GET /v1/engines`, and `truncated`
//!   on a run record.
//! - `1.2` — the [`PROVENANCE_HEADER`] on `GET /v1/export` of a table Lakeleto wrote, so a
//!   dashboard or a report reading the export learns which table, made when, without parsing
//!   Parquet. Sent only when the table carries provenance; a client must not require it.
//! - `1.3` — `GET /v1/diff`: what changed between two tables, as `lakeleto diff -o json` prints
//!   it. A client shows it only when `GET /v1/engines` lists it in `endpoints`.

/// The `/v1` contract this build speaks.
pub const PROTOCOL_VERSION: &str = "1.3";

/// The response header `GET /v1/export` adds when the exported view is one table Lakeleto wrote
/// (`Lakeleto-Provenance`): a summary of the provenance in the table's footer, as an
/// [RFC 8941](https://www.rfc-editor.org/rfc/rfc8941) dictionary —
///
/// ```text
/// lakeleto-provenance: name="marts/revenue_by_city", created_at_ms=1762700000000, rows=24,
///                      provenance_version=1, lakeleto_version="0.4.1"
/// ```
///
/// It describes the **table** the rows came from, whatever `sort`, `filter` or `cols` the
/// export applied. A file another program wrote with the same footer record names that program
/// in `writer="…"` in place of `lakeleto_version`. Members may be added; a client ignores
/// members it does not know. The full
/// record (the SQL and the tables it read) stays in the Parquet footer under
/// [`crate::table_out::PROVENANCE_KEY`].
pub const PROVENANCE_HEADER: &str = "lakeleto-provenance";

/// The response header that carries [`PROTOCOL_VERSION`] (`X-Lakeleto-Protocol`). Lowercase,
/// because HTTP field names are case-insensitive and `HeaderName::from_static` takes them so.
pub const PROTOCOL_HEADER: &str = "x-lakeleto-protocol";

/// The major and minor parts of a protocol version, or `None` for one that isn't `major.minor`.
pub fn parse(version: &str) -> Option<(u32, u32)> {
    let (major, minor) = version.trim().split_once('.')?;
    Some((major.parse().ok()?, minor.parse().ok()?))
}

/// Does a server speaking `server` serve this build's major version, so the `/v1` routes mean
/// what this build expects?
pub fn compatible(server: &str) -> bool {
    match (parse(server), parse(PROTOCOL_VERSION)) {
        (Some((theirs, _)), Some((ours, _))) => theirs == ours,
        _ => false,
    }
}

/// Rows `GET /v1/rows` returns when the request names no `limit`.
pub const ROWS_DEFAULT_LIMIT: usize = 100;

/// The most rows one `GET /v1/rows` window may hold; a larger `limit` is clamped to it.
pub const ROWS_MAX_LIMIT: usize = 10_000;

/// The query string `GET /v1/rows` (and `/v1/stats` and `/v1/export`) takes, decoded.
///
/// Lives here, outside the `serve` feature, because more than one server speaks `/v1/rows`: a
/// server that parsed the grid's parameters with a copy of this grammar would read the same URL as
/// a different window the day the two copies drifted.
#[derive(Debug, Clone)]
pub struct ScanQuery {
    /// The `path` parameter, verbatim. Interpreting it is the server's business.
    pub path: String,
    /// An explicit `format`; an empty value is the same as none.
    pub format: Option<String>,
    /// `json_path`. An empty value is kept: it names the whole document.
    pub json_path: Option<String>,
    /// `flatten`. An empty value is kept: it means `all`.
    pub flatten: Option<String>,
    /// The window, the sort, the filters and the projection.
    pub spec: crate::engine::ScanSpec,
    /// `fmt`, which only `/v1/export` reads.
    pub fmt: Option<String>,
}

/// Parse the shared `path/format/json_path/flatten/offset/limit/sort/desc/filter/cols/fmt` query
/// params (already URL-decoded into pairs, in order) into a [`ScanQuery`].
///
/// `filter` may repeat; each is `column:op:value`, with an op
/// [`FilterOp::parse`](crate::engine::FilterOp::parse) reads (`eq ne lt le gt ge contains
/// notcontains startswith endswith in isnull notnull`, or an alias of one, such as `>=` or
/// `not_contains`), and `isnull`/`notnull` take no value. `limit` is clamped to
/// `1..=`[`ROWS_MAX_LIMIT`] and defaults to [`ROWS_DEFAULT_LIMIT`]. A missing `path` is the only
/// error.
pub fn parse_scan_query(pairs: &[(String, String)]) -> crate::error::Result<ScanQuery> {
    use crate::engine::{FilterOp, FilterSpec, ScanSpec, SortSpec};

    let mut path = None;
    let mut format = None;
    let mut json_path = None;
    let mut flatten = None;
    let mut offset = 0usize;
    let mut limit = ROWS_DEFAULT_LIMIT;
    let mut sort = None;
    let mut desc = false;
    let mut fmt = None;
    let mut projection = None;
    let mut filters = Vec::new();

    for (k, v) in pairs {
        match k.as_str() {
            "path" => path = Some(v.clone()),
            "format" if !v.is_empty() => format = Some(v.clone()),
            // Empty is meaningful here — the whole document — so it is kept, unlike `format`.
            "json_path" => json_path = Some(v.clone()),
            // Empty is `all`, as for a bare `?flatten`.
            "flatten" => flatten = Some(v.clone()),
            "offset" => offset = v.parse().unwrap_or(0),
            "limit" => limit = v.parse().unwrap_or(ROWS_DEFAULT_LIMIT),
            "sort" if !v.is_empty() => sort = Some(v.clone()),
            "desc" => desc = v == "1" || v.eq_ignore_ascii_case("true"),
            "fmt" if !v.is_empty() => fmt = Some(v.clone()),
            "cols" if !v.is_empty() => {
                projection = Some(
                    v.split(',')
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                )
            }
            "filter" => {
                let mut it = v.splitn(3, ':');
                if let (Some(c), Some(o)) = (it.next(), it.next()) {
                    if let Some(op) = FilterOp::parse(o) {
                        // `is_null` / `not_null` take no operand, so `col:isnull` is accepted
                        // without the trailing colon that would otherwise be required to make the
                        // filter parse at all — this parser drops what it cannot read, and a
                        // silently missing filter shows wrong rows rather than an error.
                        let val = it.next();
                        if !c.is_empty() && (val.is_some() || op.is_unary()) {
                            filters.push(FilterSpec {
                                column: c.to_string(),
                                op,
                                value: val.unwrap_or_default().to_string(),
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let path =
        path.ok_or_else(|| crate::error::EngineError::Other("missing `path`".to_string()))?;
    let sort = sort.map(|column| SortSpec {
        column,
        descending: desc,
    });
    Ok(ScanQuery {
        path,
        format,
        json_path,
        flatten,
        spec: ScanSpec {
            offset,
            limit: limit.clamp(1, ROWS_MAX_LIMIT),
            sort,
            filters,
            projection,
        },
        fmt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn scan_query_reads_the_grid_grammar_and_clamps_the_window() {
        let q = parse_scan_query(&pairs(&[
            ("path", "cat://acme/geo/cities"),
            ("offset", "20"),
            ("limit", "999999"),
            ("sort", "n"),
            ("desc", "1"),
            ("filter", "city:startswith:Lo"),
            ("filter", "n:isnull"),
            ("filter", "n:bogus:1"),
            ("cols", "city,,n"),
            ("format", ""),
        ]))
        .unwrap();
        assert_eq!(q.path, "cat://acme/geo/cities");
        assert_eq!(q.format, None, "an empty format is no format");
        assert_eq!(q.spec.offset, 20);
        assert_eq!(q.spec.limit, ROWS_MAX_LIMIT);
        let sort = q.spec.sort.unwrap();
        assert_eq!((sort.column.as_str(), sort.descending), ("n", true));
        let filters: Vec<String> = q.spec.filters.iter().map(|f| f.to_wire()).collect();
        assert_eq!(filters, vec!["city:startswith:Lo", "n:isnull:"]);
        assert_eq!(q.spec.projection.unwrap(), vec!["city", "n"]);

        let q = parse_scan_query(&pairs(&[("path", "x"), ("limit", "0")])).unwrap();
        assert_eq!(q.spec.limit, 1, "a window holds at least one row");
        let q = parse_scan_query(&pairs(&[("path", "x")])).unwrap();
        assert_eq!(q.spec.limit, ROWS_DEFAULT_LIMIT);
        assert!(parse_scan_query(&pairs(&[("limit", "5")])).is_err());
    }

    #[test]
    fn versions_are_major_dot_minor_and_compatible_within_a_major() {
        assert_eq!(parse(PROTOCOL_VERSION), Some((1, 3)));
        assert_eq!(parse(" 1.7 "), Some((1, 7)));
        for bad in ["1", "1.x", "v1.0", "", "1.0.0"] {
            assert_eq!(parse(bad), None, "{bad}");
        }
        assert!(compatible("1.0"));
        assert!(compatible("1.9"), "a later minor only adds");
        assert!(!compatible("2.0"));
        assert!(!compatible("garbage"));
    }
}
