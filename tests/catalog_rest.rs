//! Iceberg tables served by a REST catalog, read through a stand-in for one on localhost and a
//! stand-in for S3 holding the tables' files. They pin what the client asks the catalog, how it
//! logs in, whose credentials read the files, and what the catalog's answers turn into.
//!
//! Run with: `cargo test --features catalog --test catalog_rest`.
#![cfg(feature = "catalog")]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value as Json;

use lakeleto::catalog::{CatalogConfig, CatalogRef, Catalogs};
use lakeleto::engine::Engine;
use lakeleto::{CancelReason, EngineError, Format, LocalReaderEngine, RequestContext, Source};

#[path = "support/fake_s3.rs"]
mod fake_s3;
#[path = "support/iceberg_fixtures.rs"]
mod iceberg_fixtures;
#[path = "support/rest_catalog.rs"]
mod rest_catalog;
use fake_s3::FakeS3;
use iceberg_fixtures::*;
use rest_catalog::{FakeCatalog, LastPage, Login, Table, Vending};

const CREDENTIALS: &str = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

/// `warehouse/db/orders` in the S3 stand-in, served by a catalog as `db.orders` under the prefix
/// `analytics`. The catalog's table is the orders table's current metadata.
fn serve_orders(s3: &FakeS3, catalog: &FakeCatalog) {
    put_orders(s3);
    let meta = |name: &str| uri(&format!("{ORDERS}/metadata/{name}"));
    let metadata: Json = serde_json::from_slice(&metadata(
        ORDERS,
        &[(1, &meta("snap-1.avro")), (2, &meta("snap-2.avro"))],
    ))
    .unwrap();
    catalog.set_prefix("analytics");
    catalog.add_table(
        &["db"],
        "orders",
        Table::new(&meta("v2.metadata.json"), metadata),
    );
}

/// Credentials vended for everything in the warehouse, for the S3 stand-in.
fn vending(s3: &FakeS3, expires_in_ms: Option<i64>) -> Vending {
    Vending {
        prefix: "s3://bucket/warehouse".to_string(),
        endpoint: s3.endpoint(),
        expires_in_ms,
    }
}

/// Catalog `lab` at `catalog`, with `extra` settings.
fn catalogs(catalog: &FakeCatalog, extra: &[(&str, &str)]) -> Arc<Catalogs> {
    let mut config = CatalogConfig::new("lab")
        .unwrap()
        .with("uri", catalog.uri())
        .with("warehouse", "analytics-wh");
    for (key, value) in extra {
        config = config.with(key, *value);
    }
    Arc::new(Catalogs::from_configs([config]).unwrap())
}

/// An engine that reads only as a call or a catalog says: with no ambient identity, a read the
/// catalog did not supply credentials for is refused rather than read as this machine.
fn engine(catalogs: &Arc<Catalogs>) -> LocalReaderEngine {
    LocalReaderEngine::default()
        .without_ambient_identity()
        .with_catalogs(catalogs.clone())
}

fn table(reference: &str) -> Source {
    Source::detect(reference).unwrap()
}

#[test]
fn a_catalog_reference_detects_as_iceberg_with_no_request() {
    let source = Source::detect("catalog://lab/db/orders").unwrap();
    assert_eq!(source.format, Format::Iceberg);
    assert!(source.is_catalog());
    for (reference, says) in [
        ("catalog://lab/db/", "names a catalog or a namespace"),
        ("catalog://lab/db/orders@42", "0.5.0"),
    ] {
        let err = Source::detect(reference).unwrap_err().to_string();
        assert!(err.contains(says), "{reference}: {err}");
    }
    // A catalog says what its tables are, so `--format` may only agree with it.
    assert!(Source::resolve("catalog://lab/db/orders", Some("iceberg")).is_ok());
    let err = Source::resolve("catalog://lab/db/orders", Some("csv"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("drop `--format csv`"), "{err}");
    let err = Source::resolve("catalog://lab/db/orders", Some("xyz"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown format `xyz`"), "{err}");
}

/// The whole path of a read: one `GET /v1/config`, one `loadTable` asking for credentials, then
/// the manifest list, the manifest and ranged reads of one data file, every one of them signed
/// with the key the catalog vended. The metadata file is not fetched, since `loadTable` handed it
/// over, and nothing outside the current snapshot is asked for.
#[test]
fn a_catalog_table_is_read_with_the_credentials_the_catalog_vends() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    catalog.vend(vending(&s3, None));
    let catalogs = catalogs(&catalog, &[]);
    let ctx = RequestContext::detached();

    let rows = engine(&catalogs)
        .preview(&ctx, &table("catalog://lab/db/orders"), 10)
        .unwrap();
    assert_eq!(ids(&rows.batches), (0..10).collect::<Vec<_>>());

    assert_eq!(
        catalog.calls(),
        [
            "GET /v1/config",
            "GET /v1/analytics/namespaces/db/tables/orders"
        ]
    );
    let requests = catalog.requests();
    assert_eq!(requests[0].query, "warehouse=analytics-wh");
    assert_eq!(
        requests[0].delegation, None,
        "only a table load asks for credentials"
    );
    assert_eq!(
        requests[1].delegation.as_deref(),
        Some("vended-credentials")
    );

    let reads = s3.requests();
    assert_eq!(reads[0], format!("GET {ORDERS}/metadata/snap-2.avro"));
    assert_eq!(reads[1], format!("GET {ORDERS}/metadata/manifest-2.avro"));
    for never in [
        "version-hint",
        "metadata.json",
        "snap-1",
        "manifest-1",
        "old.parquet",
        "orphan",
    ] {
        assert!(
            !reads.iter().any(|r| r.contains(never)),
            "{never}: {reads:#?}"
        );
    }
    assert!(
        s3.signers().iter().all(|key| key == "VENDED-1"),
        "{:?}",
        s3.signers()
    );

    let schema = engine(&catalogs)
        .schema(&ctx, &table("catalog://lab/db/orders"))
        .unwrap();
    assert_eq!(schema.row_count, Some(90_000));
    assert_eq!(schema.credentials.as_deref(), Some("vended"));
    assert_eq!(schema.format, "iceberg");
}

/// Credentials that have expired are not used again: the store asks the catalog for fresh ones,
/// through the credentials endpoint when the catalog lists it, and by loading the table again
/// when it does not.
#[test]
fn expired_vended_credentials_are_renewed_through_the_catalog() {
    for advertise in [true, false] {
        let s3 = FakeS3::start();
        let catalog = FakeCatalog::start();
        serve_orders(&s3, &catalog);
        if advertise {
            catalog.set_endpoints(&[CREDENTIALS]);
        }
        catalog.vend(vending(&s3, Some(-60_000)));
        let catalogs = catalogs(&catalog, &[]);

        let rows = engine(&catalogs)
            .preview(
                &RequestContext::detached(),
                &table("catalog://lab/db/orders"),
                10,
            )
            .unwrap();
        assert_eq!(ids(&rows.batches), (0..10).collect::<Vec<_>>());

        let signers = s3.signers();
        assert!(
            !signers.iter().any(|key| key == "VENDED-1"),
            "the key loadTable vended had expired: {signers:?}"
        );
        assert!(
            signers.iter().all(|key| key.starts_with("VENDED-")),
            "{signers:?}"
        );
        let calls = catalog.calls();
        let renewals = calls
            .iter()
            .filter(|c| {
                if advertise {
                    c.ends_with("/orders/credentials")
                } else {
                    c.ends_with("/tables/orders")
                }
            })
            .count();
        let at_least = if advertise { 1 } else { 2 };
        assert!(renewals >= at_least, "advertised {advertise}: {calls:#?}");
        if !advertise {
            assert!(
                !calls.iter().any(|c| c.ends_with("/credentials")),
                "{calls:#?}"
            );
        }
    }
}

/// OAuth2 client credentials: the login goes to the configured token endpoint as a form, every
/// request after it carries the token, and a token the catalog stops taking is replaced once and
/// the request sent again.
#[test]
fn a_catalog_logs_in_with_client_credentials_and_replaces_a_rejected_token() {
    let catalog = FakeCatalog::start();
    catalog.add_namespace(&["sales"]);
    catalog.set_login(Login::Client {
        id: "lakeleto".to_string(),
        secret: "s3cret".to_string(),
        expires_in: Some(3600),
    });
    let token_endpoint = catalog.token_endpoint();
    let catalogs = catalogs(
        &catalog,
        &[
            ("credential", "lakeleto:s3cret"),
            ("oauth2-server-uri", token_endpoint.as_str()),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ],
    );
    let ctx = RequestContext::detached();
    let root = CatalogRef::parse("catalog://lab/").unwrap();

    let names = |listing: lakeleto::catalog::CatalogListing| -> Vec<String> {
        listing
            .listing
            .entries
            .into_iter()
            .map(|e| e.name)
            .collect()
    };
    assert_eq!(names(catalogs.list(&ctx, &root).unwrap()), ["sales"]);
    let requests = catalog.requests();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/idp/token");
    let form = &requests[0].body;
    for field in [
        "grant_type=client_credentials",
        "client_id=lakeleto",
        "client_secret=s3cret",
        "scope=PRINCIPAL_ROLE%3AALL",
    ] {
        assert!(form.contains(field), "{form}");
    }
    assert!(requests[1..]
        .iter()
        .all(|r| r.bearer.as_deref() == Some("tok-1")));

    catalog.revoke_tokens();
    catalog.reset();
    assert_eq!(names(catalogs.list(&ctx, &root).unwrap()), ["sales"]);
    let replaced: Vec<(String, Option<String>)> = catalog
        .requests()
        .into_iter()
        .map(|r| (format!("{} {}", r.method, r.path), r.bearer))
        .collect();
    let base = rest_catalog::BASE;
    assert_eq!(
        replaced,
        [
            (
                format!("GET {base}/v1/namespaces"),
                Some("tok-1".to_string())
            ),
            ("POST /idp/token".to_string(), None),
            (
                format!("GET {base}/v1/namespaces"),
                Some("tok-2".to_string())
            ),
        ]
    );
}

/// A login the identity provider refuses says so, and never repeats the secret.
#[test]
fn a_refused_login_is_a_403_that_keeps_the_secret_to_itself() {
    let catalog = FakeCatalog::start();
    catalog.set_login(Login::Client {
        id: "lakeleto".to_string(),
        secret: "right".to_string(),
        expires_in: None,
    });
    let token_endpoint = catalog.token_endpoint();
    let catalogs = catalogs(
        &catalog,
        &[
            ("credential", "lakeleto:wr0ng-s3cret"),
            ("oauth2-server-uri", token_endpoint.as_str()),
        ],
    );
    let Err(err) = catalogs.list(
        &RequestContext::detached(),
        &CatalogRef::parse("catalog://lab/").unwrap(),
    ) else {
        panic!("a wrong secret logs nobody in");
    };
    assert!(matches!(err, EngineError::Forbidden(_)), "{err}");
    let msg = err.to_string();
    assert!(
        msg.contains("invalid_client: Bad client credentials"),
        "{msg}"
    );
    assert!(!msg.contains("wr0ng-s3cret"), "{msg}");
}

/// An identity provider that is down is a failure upstream (502), not a refusal of the credentials.
#[test]
fn an_identity_provider_outage_is_a_502_not_a_refusal() {
    let catalog = FakeCatalog::start();
    catalog.set_login(Login::Client {
        id: "lakeleto".to_string(),
        secret: "s3cret".to_string(),
        expires_in: None,
    });
    catalog.token_outage();
    let token_endpoint = catalog.token_endpoint();
    let catalogs = catalogs(
        &catalog,
        &[
            ("credential", "lakeleto:s3cret"),
            ("oauth2-server-uri", token_endpoint.as_str()),
        ],
    );
    let Err(err) = catalogs.list(
        &RequestContext::detached(),
        &CatalogRef::parse("catalog://lab/").unwrap(),
    ) else {
        panic!("nobody could log in");
    };
    assert!(matches!(err, EngineError::Remote(_)), "{err}");
    let msg = err.to_string();
    assert!(
        msg.contains("503") && msg.contains("temporarily_unavailable"),
        "{msg}"
    );
    assert!(!msg.contains("s3cret"), "{msg}");
}

/// A static token is sent as it is, and a catalog that does not take it refuses with a 403.
#[test]
fn a_static_token_is_sent_as_written_and_a_wrong_one_is_refused() {
    let catalog = FakeCatalog::start();
    catalog.add_namespace(&["sales"]);
    catalog.set_login(Login::Token("t0k-static".to_string()));
    let ctx = RequestContext::detached();
    let root = CatalogRef::parse("catalog://lab/").unwrap();

    let good = catalogs(&catalog, &[("token", "t0k-static")]);
    assert_eq!(good.list(&ctx, &root).unwrap().listing.entries.len(), 1);
    assert!(catalog
        .requests()
        .iter()
        .all(|r| r.bearer.as_deref() == Some("t0k-static")));

    let bad = catalogs(&catalog, &[("token", "not-it")]);
    let Err(err) = bad.list(&ctx, &root) else {
        panic!("a wrong token lists nothing");
    };
    assert!(matches!(err, EngineError::Forbidden(_)), "{err}");
    assert!(err.to_string().contains("401"), "{err}");
}

/// Listings follow pages, come back namespaces first then tables, and carry references the
/// catalog takes back, a name with a `/` in it included.
#[test]
fn listings_follow_pages_and_hand_back_references_that_load() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    catalog.set_page_size(2);
    for ns in [
        &["sales", "emea"][..],
        &["sales", "apac"],
        &["hr"],
        &["raw data"],
    ] {
        catalog.add_namespace(ns);
    }
    let orders = |name: &str| {
        let meta = uri(&format!("{ORDERS}/metadata/v2.metadata.json"));
        let snaps = [
            (1, uri(&format!("{ORDERS}/metadata/snap-1.avro"))),
            (2, uri(&format!("{ORDERS}/metadata/snap-2.avro"))),
        ];
        let snaps: Vec<(i64, &str)> = snaps.iter().map(|(i, s)| (*i, s.as_str())).collect();
        let _ = name;
        Table::new(
            &meta,
            serde_json::from_slice(&metadata(ORDERS, &snaps)).unwrap(),
        )
    };
    for name in ["c", "a", "b", "odd/name"] {
        catalog.add_table(&["sales"], name, orders(name));
    }
    catalog.vend(vending(&s3, None));
    let catalogs = catalogs(&catalog, &[]);
    let ctx = RequestContext::detached();

    let top = catalogs
        .list(&ctx, &CatalogRef::parse("catalog://lab/").unwrap())
        .unwrap();
    let shown: Vec<(String, String)> = top
        .listing
        .entries
        .iter()
        .map(|e| (e.name.clone(), e.path.clone()))
        .collect();
    assert_eq!(
        shown,
        [
            ("db".to_string(), "catalog://lab/db/".to_string()),
            ("hr".to_string(), "catalog://lab/hr/".to_string()),
            (
                "raw data".to_string(),
                "catalog://lab/raw data/".to_string()
            ),
            ("sales".to_string(), "catalog://lab/sales/".to_string()),
        ]
    );
    assert_eq!(top.listing.parent.as_deref(), Some("catalog://"));
    assert!(!top.truncated);

    catalog.reset();
    let sales = catalogs
        .list(&ctx, &CatalogRef::parse("catalog://lab/sales").unwrap())
        .unwrap()
        .listing;
    let shown: Vec<(&str, &str, Option<&str>)> = sales
        .entries
        .iter()
        .map(|e| (e.name.as_str(), e.kind, e.format.as_deref()))
        .collect();
    assert_eq!(
        shown,
        [
            ("apac", "dir", None),
            ("emea", "dir", None),
            ("a", "file", Some("iceberg")),
            ("b", "file", Some("iceberg")),
            ("c", "file", Some("iceberg")),
            ("odd/name", "file", Some("iceberg")),
        ]
    );
    assert_eq!(sales.dir, "catalog://lab/sales/");
    assert_eq!(sales.parent.as_deref(), Some("catalog://lab/"));
    let queries: Vec<String> = catalog.requests().iter().map(|r| r.query.clone()).collect();
    assert!(
        queries.iter().any(|q| q.contains("pageToken=p2")),
        "the table listing took a second page: {queries:#?}"
    );
    assert!(
        queries.iter().any(|q| q.contains("parent=sales")),
        "{queries:#?}"
    );

    // The listed reference loads: the `/` travels encoded inside the table's path segment.
    let odd = sales.entries.iter().find(|e| e.name == "odd/name").unwrap();
    assert_eq!(odd.path, "catalog://lab/sales/odd%2Fname");
    catalog.reset();
    let rows = engine(&catalogs)
        .preview(&ctx, &table(&odd.path), 3)
        .unwrap();
    assert_eq!(rows.num_rows(), 3);
    assert!(
        catalog
            .requests()
            .iter()
            .any(|r| r.path.ends_with("/namespaces/sales/tables/odd%2Fname")),
        "{:#?}",
        catalog.calls()
    );

    // A nested namespace's levels travel joined by the unit separator.
    catalog.reset();
    catalogs
        .list(
            &ctx,
            &CatalogRef::parse("catalog://lab/sales/emea/").unwrap(),
        )
        .unwrap();
    assert!(
        catalog
            .requests()
            .iter()
            .any(|r| r.path.ends_with("/namespaces/sales%1Femea/tables")),
        "{:#?}",
        catalog.calls()
    );
}

/// A listing stops where the catalog's pages end, however the catalog says so: with no token, with
/// an empty one, or with the one Lakeleto just sent. It also stops once it holds as many entries as
/// one listing returns, without asking for pages it would not show.
#[test]
fn a_listing_stops_where_the_pages_end_or_once_it_is_full() {
    let ctx = RequestContext::detached();
    let db = CatalogRef::parse("catalog://lab/db/").unwrap();
    let table_pages = |catalog: &FakeCatalog| {
        catalog
            .requests()
            .iter()
            .filter(|r| r.path.ends_with("/namespaces/db/tables"))
            .count()
    };
    let nowhere = || Table::new("file:///nowhere/metadata/v1.metadata.json", Json::Null);

    for last in [LastPage::Empty, LastPage::Repeat] {
        let catalog = FakeCatalog::start();
        catalog.set_page_size(2);
        catalog.end_listings_with(last);
        for name in ["a", "b", "c"] {
            catalog.add_table(&["db"], name, nowhere());
        }
        let listing = catalogs(&catalog, &[]).list(&ctx, &db).unwrap();
        let names: Vec<&str> = listing
            .listing
            .entries
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(names, ["a", "b", "c"], "{last:?}");
        assert!(!listing.truncated, "{last:?}");
        assert_eq!(table_pages(&catalog), 2, "{last:?}: {:#?}", catalog.calls());
    }

    // Two pages of 5,000 hold the most one listing returns, so the third is never asked for.
    let catalog = FakeCatalog::start();
    catalog.set_page_size(5_000);
    for i in 0..10_001 {
        catalog.add_table(&["db"], &format!("t{i:05}"), nowhere());
    }
    let listing = catalogs(&catalog, &[]).list(&ctx, &db).unwrap();
    assert!(listing.truncated);
    assert_eq!(listing.listing.entries.len(), 10_000);
    assert_eq!(table_pages(&catalog), 2, "{:#?}", catalog.calls());
}

/// A catalog that ignores `parent` answers a namespace's listing with its top-level namespaces.
/// Those are not under the namespace, so they are not listed as its children: listing `db` shows
/// its tables and no `db` inside `db`.
#[test]
fn a_catalog_that_ignores_parent_lists_no_namespace_twice() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    catalog.ignore_parent();
    let catalogs = catalogs(&catalog, &[]);
    let listing = catalogs
        .list(
            &RequestContext::detached(),
            &CatalogRef::parse("catalog://lab/db/").unwrap(),
        )
        .unwrap()
        .listing;
    let shown: Vec<(&str, &str)> = listing
        .entries
        .iter()
        .map(|e| (e.name.as_str(), e.kind))
        .collect();
    assert_eq!(shown, [("orders", "file")]);
}

/// What a catalog reports for a name that is not a table becomes the error a reader can act on:
/// a namespace says to list it, a missing table and a missing namespace are not found.
#[test]
fn a_name_that_is_not_a_table_says_what_it_is() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    catalog.add_namespace(&["db", "archive"]);
    let catalogs = catalogs(&catalog, &[]);
    let ctx = RequestContext::detached();
    let read = |reference: &str| {
        engine(&catalogs)
            .schema(&ctx, &table(reference))
            .map(|_| ())
            .unwrap_err()
    };

    let err = read("catalog://lab/db/archive").to_string();
    assert!(
        err.contains("is a namespace, not a table")
            && err.contains("lakeleto catalog ls catalog://lab/db/archive/"),
        "{err}"
    );
    let err = read("catalog://lab/db/missing");
    assert!(
        matches!(&err, EngineError::Io(e) if e.kind() == std::io::ErrorKind::NotFound),
        "{err}"
    );
    assert!(
        err.to_string().contains("has no table `db.missing`"),
        "{err}"
    );
    let err = read("catalog://lab/nowhere/orders");
    assert!(
        matches!(&err, EngineError::Io(e) if e.kind() == std::io::ErrorKind::NotFound),
        "{err}"
    );
    assert!(
        err.to_string().contains("NoSuchNamespaceException"),
        "{err}"
    );
    assert!(
        err.to_string()
            .contains("catalog `lab`: loading nowhere.orders:"),
        "{err}"
    );
    let err = read("catalog://elsewhere/db/orders").to_string();
    assert!(
        err.contains("no catalog named `elsewhere`") && err.contains("`lab`"),
        "{err}"
    );
}

/// Tables a reader must not read as they are: row filters or column masks it cannot apply,
/// planning only the server may do, and requests only the catalog may sign. Each is refused before
/// any of the table's files is read.
#[test]
fn a_table_the_catalog_restricts_is_refused_before_a_file_is_read() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    let meta = uri(&format!("{ORDERS}/metadata/v2.metadata.json"));
    let base: Json = serde_json::from_slice(&metadata(
        ORDERS,
        &[(2, &uri(&format!("{ORDERS}/metadata/snap-2.avro")))],
    ))
    .unwrap();

    let mut masked = Table::new(&meta, base.clone());
    masked.read_restrictions = Some(serde_json::json!({
        "required-column-projections": [{"field-id": 2, "action": "mask-alphanum"}]
    }));
    catalog.add_table(&["db"], "masked", masked);
    let mut planned = Table::new(&meta, base.clone());
    planned.config = BTreeMap::from([("scan-planning-mode".to_string(), "server".to_string())]);
    catalog.add_table(&["db"], "planned", planned);
    let mut signed = Table::new(&meta, base);
    signed.config = BTreeMap::from([("s3.remote-signing-enabled".to_string(), "true".to_string())]);
    catalog.add_table(&["db"], "signed", signed);
    let catalogs = catalogs(&catalog, &[]);
    let ctx = RequestContext::detached();
    s3.reset();

    for (name, says) in [
        ("masked", "row filters or column masks"),
        ("planned", "server-side scan planning"),
        ("signed", "s3.remote-signing-enabled"),
    ] {
        let err = engine(&catalogs)
            .preview(&ctx, &table(&format!("catalog://lab/db/{name}")), 10)
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(err.contains(says), "{name}: {err}");
    }
    assert!(s3.requests().is_empty(), "{:#?}", s3.requests());
}

/// With nothing vended, the catalog's own storage keys read the table; with none of those either,
/// the engine's identity does, unless the catalog refuses that fallback.
#[test]
fn without_vended_credentials_the_catalogs_keys_then_this_machines_read_the_table() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    let ctx = RequestContext::detached();
    let orders = table("catalog://lab/db/orders");
    let endpoint = s3.endpoint();

    // Storage keys configured for the catalog.
    let configured = catalogs(
        &catalog,
        &[
            ("s3.access-key-id", "CONFIGURED"),
            ("s3.secret-access-key", "configured-secret"),
            ("s3.endpoint", endpoint.as_str()),
            ("s3.region", "us-east-1"),
            ("s3.path-style-access", "true"),
        ],
    );
    let schema = engine(&configured).schema(&ctx, &orders).unwrap();
    assert_eq!(schema.credentials.as_deref(), Some("catalog"));
    assert!(
        s3.signers().iter().all(|k| k == "CONFIGURED"),
        "{:?}",
        s3.signers()
    );

    // Neither: the engine's own identity, with the catalog's endpoint applied.
    s3.reset();
    let open = catalogs(&catalog, &[]);
    let ambient = LocalReaderEngine::default()
        .with_store_options(s3.options())
        .with_catalogs(open.clone());
    let schema = ambient.schema(&ctx, &orders).unwrap();
    assert_eq!(schema.credentials.as_deref(), Some("ambient"));
    assert_eq!(schema.row_count, Some(90_000));

    // An engine with no identity of its own refuses rather than read as the host.
    let Err(err) = engine(&open).schema(&ctx, &orders) else {
        panic!("nobody vended or configured credentials");
    };
    assert!(matches!(err, EngineError::Forbidden(_)), "{err}");

    // `storage-fallback = "none"` refuses even an engine that has an identity.
    let strict = catalogs(&catalog, &[("storage-fallback", "none")]);
    let Err(err) = LocalReaderEngine::default()
        .with_store_options(s3.options())
        .with_catalogs(strict)
        .schema(&ctx, &orders)
    else {
        panic!("the catalog refused the fallback");
    };
    assert!(matches!(err, EngineError::Forbidden(_)), "{err}");
    assert!(
        err.to_string().contains("`storage-fallback` is `none`"),
        "{err}"
    );
}

/// A catalog that serves tables from its own disk hands out `file://` metadata with absolute
/// paths. The table is read from disk, and nothing reaches an object store.
#[test]
fn a_catalog_serving_its_own_disk_is_read_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().canonicalize().unwrap().join("tables/events");
    std::fs::create_dir_all(table_dir.join("data")).unwrap();
    std::fs::create_dir_all(table_dir.join("metadata")).unwrap();
    let data = table_dir.join("data/part-0.parquet");
    let body = parquet(0..50, 50);
    let manifest = table_dir.join("metadata/manifest.avro");
    std::fs::write(
        &manifest,
        avro(
            MANIFEST_SCHEMA,
            vec![entry(0, &data.display().to_string(), 50, body.len())],
        ),
    )
    .unwrap();
    std::fs::write(&data, body).unwrap();
    let list = table_dir.join("metadata/snap-1.avro");
    std::fs::write(
        &list,
        manifest_list(&[(&manifest.display().to_string(), 0)]),
    )
    .unwrap();
    let meta = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000002",
        "location": table_dir.display().to_string(),
        "current-snapshot-id": 1,
        "snapshots": [{"snapshot-id": 1, "manifest-list": list.display().to_string()}],
    });
    let meta_file = table_dir.join("metadata/v1.metadata.json");
    std::fs::write(&meta_file, serde_json::to_vec(&meta).unwrap()).unwrap();

    let catalog = FakeCatalog::start();
    catalog.add_table(
        &["default"],
        "events",
        Table::new(&format!("file://{}", meta_file.display()), meta),
    );
    let catalogs = catalogs(&catalog, &[]);
    let ctx = RequestContext::detached();
    let events = table("catalog://lab/default/events");
    let rows = engine(&catalogs).preview(&ctx, &events, 100).unwrap();
    assert_eq!(ids(&rows.batches), (0..50).collect::<Vec<_>>());
    let schema = engine(&catalogs).schema(&ctx, &events).unwrap();
    assert_eq!(
        schema.credentials, None,
        "a table on this machine needs none"
    );
}

/// SQL reads a catalog table the way the grid does, through the same catalog.
#[cfg(feature = "sql")]
#[test]
fn sql_reads_a_catalog_table() {
    use arrow_array::Int64Array;
    use lakeleto::engine::sql::DataFusionEngine;
    use lakeleto::NamedSource;

    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    catalog.vend(vending(&s3, None));
    let engine =
        DataFusionEngine::without_ambient_identity().with_catalogs(catalogs(&catalog, &[]));
    let rows = engine
        .query(
            &RequestContext::detached(),
            "SELECT count(*) AS n, max(id) AS hi FROM t",
            &[NamedSource {
                name: "t".to_string(),
                source: table("catalog://lab/db/orders"),
            }],
        )
        .unwrap();
    let column = |i: usize| {
        rows.batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!((column(0), column(1)), (90_000, 89_999));
    assert!(
        s3.signers().iter().all(|k| k.starts_with("VENDED-")),
        "{:?}",
        s3.signers()
    );
}

/// The binary, configured by `$LAKELETO_HOME/catalogs.toml` and the environment as a user would
/// configure it: `catalog ls` lists catalogs, namespaces and tables, and `info` says whose
/// credentials read the table.
#[test]
fn the_cli_reads_its_catalogs_from_lakeleto_home() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    catalog.vend(vending(&s3, None));
    catalog.set_login(Login::Token("from-the-environment".to_string()));
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("catalogs.toml"),
        format!(
            "# where the lab's tables are\n[catalog.lab]\ntype = \"rest\"\nuri = \"{}\"\n",
            catalog.uri()
        ),
    )
    .unwrap();
    let lakeleto = |args: &[&str]| {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_lakeleto"))
            .args(args)
            .env("LAKELETO_HOME", home.path())
            .env("LAKELETO_CATALOG__LAB__TOKEN", "from-the-environment")
            .env_remove("AWS_ACCESS_KEY_ID")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(out.status.success(), "{args:?}: {stderr}");
        stdout
    };

    let configured = lakeleto(&["catalog", "ls", "-o", "csv"]);
    assert_eq!(
        configured,
        format!("name,type,uri\nlab,rest,{}\n", catalog.uri())
    );
    assert!(
        !configured.contains("from-the-environment"),
        "never a credential"
    );

    let namespaces = lakeleto(&["catalog", "ls", "catalog://lab/", "-o", "csv"]);
    assert_eq!(
        namespaces,
        "name,kind,reference\ndb,namespace,catalog://lab/db/\n"
    );
    let tables = lakeleto(&["catalog", "ls", "catalog://lab/db/", "-o", "csv"]);
    assert_eq!(
        tables,
        "name,kind,reference\norders,table,catalog://lab/db/orders\n"
    );

    let info = lakeleto(&["info", "catalog://lab/db/orders"]);
    assert!(info.contains("format : iceberg"), "{info}");
    assert!(info.contains("rows   : 90000"), "{info}");
    assert!(
        info.contains("creds  : vended by the catalog for this table"),
        "{info}"
    );
}

/// A catalog nobody answers at is named, with the URL that was tried, less its query, and with
/// why the connection failed.
#[test]
fn an_unreachable_catalog_says_where_it_was_looked_for_and_why() {
    // Port 1 on loopback: reserved, and nothing listens there.
    let config = CatalogConfig::new("lab")
        .unwrap()
        .with("uri", "http://127.0.0.1:1/api/catalog")
        .with("warehouse", "analytics-wh");
    let catalogs = Catalogs::from_configs([config]).unwrap();
    let Err(err) = catalogs.list(
        &RequestContext::detached(),
        &CatalogRef::parse("catalog://lab/").unwrap(),
    ) else {
        panic!("nothing listens on port 1");
    };
    assert!(matches!(err, EngineError::Remote(_)), "{err}");
    let msg = err.to_string();
    assert!(
        msg.contains("catalog `lab`: cannot reach http://127.0.0.1:1/api/catalog/v1/config: "),
        "{msg}"
    );
    assert!(msg.to_lowercase().contains("refused"), "{msg}");
    assert!(!msg.contains("analytics-wh"), "the query stays out: {msg}");
}

/// A catalog's answer is read up to 64 MiB: a table whose metadata runs to megabytes loads, and
/// a response past the cap is refused rather than read.
#[test]
fn a_catalog_response_is_read_up_to_its_cap() {
    let s3 = FakeS3::start();
    let catalog = FakeCatalog::start();
    serve_orders(&s3, &catalog);
    catalog.vend(vending(&s3, None));
    let padded = |bytes: usize| {
        let meta = |name: &str| uri(&format!("{ORDERS}/metadata/{name}"));
        let mut metadata: Json = serde_json::from_slice(&metadata(
            ORDERS,
            &[(1, &meta("snap-1.avro")), (2, &meta("snap-2.avro"))],
        ))
        .unwrap();
        metadata["properties"] = serde_json::json!({ "padding": "x".repeat(bytes) });
        Table::new(&meta("v2.metadata.json"), metadata)
    };
    let catalogs = catalogs(&catalog, &[]);
    let ctx = RequestContext::detached();

    catalog.add_table(&["db"], "orders", padded(2 * 1024 * 1024));
    let schema = engine(&catalogs)
        .schema(&ctx, &table("catalog://lab/db/orders"))
        .expect("a 2 MiB answer is under the cap");
    assert!(!schema.columns.is_empty());

    catalog.add_table(&["db"], "orders", padded(65 * 1024 * 1024));
    let err = engine(&catalogs)
        .schema(&ctx, &table("catalog://lab/db/orders"))
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(err.contains("larger than 64 MiB"), "{err}");
}

/// A catalog that doesn't answer before the call's deadline ends the call as the deadline's, not
/// as a catalog that cannot be reached.
#[test]
fn a_catalog_that_hangs_past_the_deadline_cancels_the_call() {
    let catalog = FakeCatalog::start();
    catalog.stall(std::time::Duration::from_secs(5));
    let catalogs = catalogs(&catalog, &[]);
    let ctx = RequestContext::detached().with_timeout(std::time::Duration::from_millis(300));
    let started = std::time::Instant::now();
    let Err(err) = catalogs.list(&ctx, &CatalogRef::parse("catalog://lab/").unwrap()) else {
        panic!("the catalog answers after the deadline");
    };
    assert!(
        matches!(err, EngineError::Cancelled(CancelReason::Deadline)),
        "{err}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(4));
}
