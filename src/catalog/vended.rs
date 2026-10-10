//! Whose credentials read a catalog table's files.
//!
//! The first of these that applies:
//!
//! 1. **Vended** by the catalog for the table: the `storage-credentials` entry whose prefix is the
//!    longest match for the table's location, else credentials in `loadTable`'s `config`.
//! 2. **The catalog's** storage keys, as configured on this machine (`s3.*`, `gcs.*`, `adls.*`).
//! 3. **Ambient**: as the engine reads any object-store location, unless the catalog sets
//!    `storage-fallback = "none"`, which refuses instead.
//!
//! Location settings (an endpoint, a region, path-style addressing) apply whichever it is: they say
//! where the table is, not who reads it.
//!
//! Vended credentials are held in memory and nowhere else. When they carry an expiry, the store
//! asks for fresh ones shortly before it, through the catalog, so a long read outlives them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::aws::AwsCredential;
use object_store::azure::AzureCredential;
use object_store::gcp::GcpCredential;
use object_store::CredentialProvider;

use super::rest::{LoadedTable, RestCatalog, ServerConfig, StorageCredential};
use super::TableStorage;
use crate::error::{EngineError, Result};
use crate::objstore::{StoreCredentials, StoreOptions};

/// How long before vended credentials expire the store asks for new ones, at most. Credentials
/// that live for less than five times this are renewed when a fifth of their life is left.
const RENEW_BEFORE: Duration = Duration::from_secs(5 * 60);

/// The object-store family a location is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    S3,
    Gcs,
    Azure,
}

fn family(location: &str) -> Option<Family> {
    let scheme = location.split_once("://")?.0.to_ascii_lowercase();
    match scheme.as_str() {
        "s3" | "s3a" => Some(Family::S3),
        "gs" | "gcs" => Some(Family::Gcs),
        "az" | "azure" | "abfs" | "abfss" | "adl" => Some(Family::Azure),
        _ => None,
    }
}

/// Where a vended credential comes from again: the catalog, for this table, at this location.
#[derive(Clone)]
pub(crate) struct Renewal {
    pub(crate) catalog: Arc<RestCatalog>,
    pub(crate) namespace: Vec<String>,
    pub(crate) table: String,
    pub(crate) location: String,
}

impl Renewal {
    /// The table's storage properties as the catalog vends them now.
    async fn props(&self) -> Result<BTreeMap<String, String>> {
        let (creds, mut props) = self
            .catalog
            .refresh_credentials(&self.namespace, &self.table)
            .await?;
        if let Some(cred) = longest_prefix(&creds, &self.location) {
            props.extend(cred.config.clone());
        }
        Ok(props)
    }
}

/// Decide how the files of the table at `location` are read. `scope` names the identity, so what
/// one table's credentials read is never handed to another.
pub(crate) fn storage_for(
    server: &ServerConfig,
    loaded: &LoadedTable,
    renewal: Renewal,
    scope: &str,
) -> Result<TableStorage> {
    let location = renewal.location.clone();
    let Some(family) = family(&location) else {
        return Ok(TableStorage::Ambient(Vec::new()));
    };
    let catalog = renewal.catalog.clone();
    // Location settings, most specific last: the catalog's merged configuration, the table's.
    let mut props = server.props.clone();
    props.extend(loaded.config.clone());

    // Vended means the catalog sent it: each step asks whether its own answer carries
    // credentials, and only then reads them over the location settings. The merged configuration
    // also holds the keys configured on this machine, which are the next step's, not vended.
    if let Some(cred) = longest_prefix(&loaded.storage_credentials, &location) {
        if vends(family, &cred.config, &location) {
            let mut vended = props.clone();
            vended.extend(cred.config.clone());
            if let Some(options) = vended_options(family, &vended, renewal.clone(), scope) {
                return Ok(TableStorage::Vended(options));
            }
        }
    }
    if vends(family, &loaded.config, &location) {
        if let Some(options) = vended_options(family, &props, renewal, scope) {
            return Ok(TableStorage::Vended(options));
        }
    }
    let remote_signing = loaded
        .config
        .get("s3.remote-signing-enabled")
        .map(String::as_str)
        == Some("true");
    if remote_signing {
        return Err(EngineError::UnsupportedOperation {
            engine: "catalog".to_string(),
            op: format!("read {location}"),
            hint: format!(
                "catalog `{}` asked Lakeleto to sign each request through the catalog \
                 (s3.remote-signing-enabled) instead of vending credentials. Lakeleto does not \
                 sign remotely yet: configure the catalog to vend credentials, or set storage keys \
                 for it",
                catalog.name()
            ),
        });
    }
    // The catalog's own keys, as configured here, with the location settings around them.
    if let Some(options) = configured_options(family, catalog.config().props(), &location) {
        return Ok(TableStorage::Catalog(
            options
                .with_config_pairs(location_pairs(family, &props))
                .with_scope(format!("catalog://{}", catalog.name())),
        ));
    }
    if catalog.config().get("storage-fallback") == Some("none") {
        return Err(EngineError::Forbidden(format!(
            "catalog `{}` neither vended credentials for {location} nor has storage keys \
             configured, and its `storage-fallback` is `none`, so Lakeleto does not read the table \
             with this machine's credentials",
            catalog.name()
        )));
    }
    Ok(TableStorage::Ambient(location_pairs(family, &props)))
}

/// Do `props` carry credentials for `family`, by themselves?
fn vends(family: Family, props: &BTreeMap<String, String>, location: &str) -> bool {
    match family {
        Family::S3 => s3_credential(props, location).is_some(),
        Family::Gcs => gcs_credential(props, location).is_some(),
        Family::Azure => azure_credential(props, location).is_some(),
    }
}

/// The entry whose prefix is the longest one `location` starts with.
fn longest_prefix<'a>(
    creds: &'a [StorageCredential],
    location: &str,
) -> Option<&'a StorageCredential> {
    creds
        .iter()
        .filter(|c| location.starts_with(c.prefix.as_str()))
        .max_by_key(|c| c.prefix.len())
}

/// Options that read with credentials vended in `props`, renewed through `renewal` when they
/// carry an expiry, or `None` when `props` vends none for this family.
fn vended_options(
    family: Family,
    props: &BTreeMap<String, String>,
    renewal: Renewal,
    scope: &str,
) -> Option<StoreOptions> {
    let location = renewal.location.clone();
    let credentials = match family {
        Family::S3 => {
            let (credential, expires) = s3_credential(props, &location)?;
            StoreCredentials::S3(Arc::new(Vended::new(
                credential,
                expires,
                renewal,
                s3_credential,
            )))
        }
        Family::Gcs => {
            let (credential, expires) = gcs_credential(props, &location)?;
            StoreCredentials::Gcs(Arc::new(Vended::new(
                credential,
                expires,
                renewal,
                gcs_credential,
            )))
        }
        Family::Azure => {
            let (credential, expires) = azure_credential(props, &location)?;
            StoreCredentials::Azure(Arc::new(Vended::new(
                credential,
                expires,
                renewal,
                azure_credential,
            )))
        }
    };
    Some(
        StoreOptions::empty()
            .with_config_pairs(location_pairs(family, props))
            .with_credentials(credentials)
            .with_scope(scope),
    )
}

/// The object_store settings that say where a table is: an endpoint, a region, how buckets are
/// addressed. Never a credential.
fn location_pairs(family: Family, props: &BTreeMap<String, String>) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    if family == Family::S3 {
        if let Some(endpoint) = props.get("s3.endpoint") {
            pairs.push(("aws_endpoint".to_string(), endpoint.clone()));
            if endpoint.starts_with("http://") {
                pairs.push(("aws_allow_http".to_string(), "true".to_string()));
            }
        }
        if let Some(region) = props
            .get("s3.region")
            .or_else(|| props.get("client.region"))
        {
            pairs.push(("aws_region".to_string(), region.clone()));
        }
        match props.get("s3.path-style-access").map(String::as_str) {
            Some("true") => pairs.push(("aws_virtual_hosted_style_request".into(), "false".into())),
            Some("false") => pairs.push(("aws_virtual_hosted_style_request".into(), "true".into())),
            _ => {}
        }
    }
    pairs
}

/// Options that read with the keys configured for the catalog on this machine, or `None` when it
/// has none for this family. They do not expire, so nothing renews them.
fn configured_options(
    family: Family,
    props: &BTreeMap<String, String>,
    location: &str,
) -> Option<StoreOptions> {
    let options = StoreOptions::empty();
    Some(match family {
        Family::S3 => {
            let mut pairs = vec![
                (
                    "aws_access_key_id".to_string(),
                    props.get("s3.access-key-id")?.clone(),
                ),
                (
                    "aws_secret_access_key".to_string(),
                    props.get("s3.secret-access-key")?.clone(),
                ),
            ];
            if let Some(token) = props.get("s3.session-token") {
                pairs.push(("aws_session_token".to_string(), token.clone()));
            }
            options.with_config_pairs(pairs)
        }
        Family::Gcs => {
            let (credential, _) = gcs_credential(props, location)?;
            options.with_credentials(StoreCredentials::Gcs(Arc::new(
                object_store::StaticCredentialProvider::new(credential),
            )))
        }
        Family::Azure => match azure_sas(props, location) {
            Some((sas, _)) => options.with_config("azure_storage_sas_key", sas),
            None => options
                .with_config(
                    "azure_storage_account_name",
                    props.get("adls.account-name")?.clone(),
                )
                .with_config(
                    "azure_storage_account_key",
                    props.get("adls.account-key")?.clone(),
                ),
        },
    })
}

/// An expiry in milliseconds since the epoch, as Iceberg writes one.
fn expiry(props: &BTreeMap<String, String>, key: &str) -> Option<SystemTime> {
    let ms: u64 = props.get(key)?.trim().parse().ok()?;
    Some(UNIX_EPOCH + Duration::from_millis(ms))
}

fn s3_credential(
    props: &BTreeMap<String, String>,
    _location: &str,
) -> Option<(AwsCredential, Option<SystemTime>)> {
    let credential = AwsCredential {
        key_id: props.get("s3.access-key-id")?.clone(),
        secret_key: props.get("s3.secret-access-key")?.clone(),
        token: props.get("s3.session-token").cloned(),
    };
    Some((credential, expiry(props, "s3.session-token-expires-at-ms")))
}

fn gcs_credential(
    props: &BTreeMap<String, String>,
    _location: &str,
) -> Option<(GcpCredential, Option<SystemTime>)> {
    let credential = GcpCredential {
        bearer: props.get("gcs.oauth2.token")?.clone(),
    };
    Some((credential, expiry(props, "gcs.oauth2.token-expires-at")))
}

fn azure_credential(
    props: &BTreeMap<String, String>,
    location: &str,
) -> Option<(AzureCredential, Option<SystemTime>)> {
    let (sas, expires) = azure_sas(props, location)?;
    let pairs = url::form_urlencoded::parse(sas.trim_start_matches('?').as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    Some((AzureCredential::SASToken(pairs), expires))
}

/// The SAS token vended for the storage account `location` is in, keyed by the account's host
/// (`adls.sas-token.<account>.dfs.core.windows.net`) or by its name (`adls.sas-token.<account>`).
fn azure_sas(
    props: &BTreeMap<String, String>,
    location: &str,
) -> Option<(String, Option<SystemTime>)> {
    let rest = location.split_once("://")?.1;
    let authority = rest.split('/').next()?;
    let host = authority.rsplit('@').next()?;
    let account = host.split('.').next()?;
    for key in [host, account] {
        if let Some(sas) = props.get(&format!("adls.sas-token.{key}")) {
            let expires = expiry(props, &format!("adls.sas-token-expires-at-ms.{key}"));
            return Some((sas.clone(), expires));
        }
    }
    None
}

/// Reads a family's credential, and when it expires, out of a table's storage properties.
type Parse<C> = fn(&BTreeMap<String, String>, &str) -> Option<(C, Option<SystemTime>)>;

/// A credential the catalog vended, renewed through it before it expires.
struct Vended<C> {
    renewal: Renewal,
    parse: Parse<C>,
    current: Mutex<Current<C>>,
}

struct Current<C> {
    credential: Arc<C>,
    fetched: SystemTime,
    expires: Option<SystemTime>,
}

impl<C> Vended<C> {
    fn new(
        credential: C,
        expires: Option<SystemTime>,
        renewal: Renewal,
        parse: Parse<C>,
    ) -> Vended<C> {
        Vended {
            renewal,
            parse,
            current: Mutex::new(Current {
                credential: Arc::new(credential),
                fetched: SystemTime::now(),
                expires,
            }),
        }
    }

    /// The current credential, unless it is time to renew it.
    fn fresh(&self, now: SystemTime) -> Option<Arc<C>> {
        let current = self.current.lock().unwrap_or_else(|p| p.into_inner());
        let Some(expires) = current.expires else {
            return Some(current.credential.clone());
        };
        let life = expires.duration_since(current.fetched).unwrap_or_default();
        let margin = RENEW_BEFORE.min(life / 5);
        (now + margin < expires).then(|| current.credential.clone())
    }
}

/// Names the table and never prints the credential.
impl<C> std::fmt::Debug for Vended<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Vended(<redacted>) for catalog `{}` at {}",
            self.renewal.catalog.name(),
            self.renewal.location
        )
    }
}

#[async_trait::async_trait]
impl<C: Send + Sync + 'static> CredentialProvider for Vended<C> {
    type Credential = C;

    async fn get_credential(&self) -> object_store::Result<Arc<C>> {
        if let Some(credential) = self.fresh(SystemTime::now()) {
            return Ok(credential);
        }
        let generic = |source: EngineError| object_store::Error::Generic {
            store: "catalog",
            source: Box::new(source),
        };
        let props = self.renewal.props().await.map_err(generic)?;
        let (credential, expires) =
            (self.parse)(&props, &self.renewal.location).ok_or_else(|| {
                generic(EngineError::Forbidden(format!(
                    "catalog `{}` vended no credentials for {} when asked again",
                    self.renewal.catalog.name(),
                    self.renewal.location
                )))
            })?;
        let credential = Arc::new(credential);
        *self.current.lock().unwrap_or_else(|p| p.into_inner()) = Current {
            credential: credential.clone(),
            fetched: SystemTime::now(),
            expires,
        };
        Ok(credential)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_longest_matching_prefix_wins() {
        let cred = |prefix: &str| StorageCredential {
            prefix: prefix.to_string(),
            config: BTreeMap::new(),
        };
        let creds = [
            cred("s3://b"),
            cred("s3://b/warehouse/sales"),
            cred("s3://b/warehouse"),
            cred("s3://other"),
        ];
        let location = "s3://b/warehouse/sales/orders";
        assert_eq!(
            longest_prefix(&creds, location).unwrap().prefix,
            "s3://b/warehouse/sales"
        );
        assert!(longest_prefix(&creds, "gs://b/x").is_none());
    }

    /// Iceberg's property names become object_store's, and only location settings are written as
    /// settings: a credential never is.
    #[test]
    fn s3_properties_map_onto_object_store_settings() {
        let p = props(&[
            ("s3.endpoint", "http://localhost:9000"),
            ("client.region", "eu-west-1"),
            ("s3.path-style-access", "true"),
            ("s3.access-key-id", "AKIA"),
            ("s3.secret-access-key", "secret"),
        ]);
        assert_eq!(
            location_pairs(Family::S3, &p),
            [
                (
                    "aws_endpoint".to_string(),
                    "http://localhost:9000".to_string()
                ),
                ("aws_allow_http".to_string(), "true".to_string()),
                ("aws_region".to_string(), "eu-west-1".to_string()),
                (
                    "aws_virtual_hosted_style_request".to_string(),
                    "false".to_string()
                ),
            ]
        );
        let (credential, expires) = s3_credential(&p, "s3://b/t").unwrap();
        assert_eq!(
            (credential.key_id.as_str(), credential.token),
            ("AKIA", None)
        );
        assert_eq!(expires, None);
        let with_expiry = props(&[
            ("s3.access-key-id", "A"),
            ("s3.secret-access-key", "S"),
            ("s3.session-token", "T"),
            ("s3.session-token-expires-at-ms", "1760000000000"),
        ]);
        let (credential, expires) = s3_credential(&with_expiry, "s3://b/t").unwrap();
        assert_eq!(credential.token.as_deref(), Some("T"));
        assert_eq!(
            expires,
            Some(UNIX_EPOCH + Duration::from_millis(1_760_000_000_000))
        );
        assert!(s3_credential(&props(&[("s3.access-key-id", "A")]), "s3://b/t").is_none());
        // Virtual-hosted addressing, which is object_store's default, is written out when the
        // catalog turns path-style access off.
        assert_eq!(
            location_pairs(Family::S3, &props(&[("s3.path-style-access", "false")])),
            [(
                "aws_virtual_hosted_style_request".to_string(),
                "true".to_string()
            )]
        );
        assert!(location_pairs(Family::Gcs, &p).is_empty());
    }

    /// A GCS token, and when it expires.
    #[test]
    fn a_gcs_token_is_read_with_its_expiry() {
        let p = props(&[
            ("gcs.oauth2.token", "ya29.token"),
            ("gcs.oauth2.token-expires-at", "1760000000000"),
        ]);
        let (credential, expires) = gcs_credential(&p, "gs://b/t").unwrap();
        assert_eq!(credential.bearer, "ya29.token");
        assert_eq!(
            expires,
            Some(UNIX_EPOCH + Duration::from_millis(1_760_000_000_000))
        );
        assert!(gcs_credential(&props(&[]), "gs://b/t").is_none());
    }

    fn renewal() -> Renewal {
        let config = super::super::CatalogConfig::new("lab")
            .unwrap()
            .with("uri", "http://127.0.0.1:1/api/catalog");
        Renewal {
            catalog: Arc::new(RestCatalog::new(config).unwrap()),
            namespace: vec!["db".to_string()],
            table: "orders".to_string(),
            location: "s3://b/warehouse/db/orders".to_string(),
        }
    }

    /// A credential is renewed five minutes before it expires, or when a fifth of its life is left
    /// if that is sooner, so a short-lived one is not renewed as soon as it arrives. One that
    /// never expires is never renewed.
    #[test]
    fn a_vended_credential_is_renewed_before_it_expires() {
        let lasting = |life: Duration| {
            let vended = Vended::new((), None, renewal(), |_, _| None);
            let expires = {
                let mut current = vended.current.lock().unwrap();
                current.expires = Some(current.fetched + life);
                current.fetched + life
            };
            (vended, expires)
        };
        let secs = Duration::from_secs;

        let (hour, expires) = lasting(secs(3600));
        assert!(hour.fresh(expires - secs(301)).is_some());
        assert!(hour.fresh(expires - secs(300)).is_none());
        assert!(hour.fresh(expires - secs(299)).is_none());

        let (ten_minutes, expires) = lasting(secs(600));
        assert!(ten_minutes.fresh(expires - secs(121)).is_some());
        assert!(ten_minutes.fresh(expires - secs(120)).is_none());

        let forever = Vended::new((), None, renewal(), |_, _| None);
        assert!(forever.fresh(SystemTime::now() + secs(1 << 30)).is_some());
    }

    /// A vended credential prints as the table it reads, never as itself.
    #[test]
    fn a_vended_credential_never_prints_itself() {
        let credential = AwsCredential {
            key_id: "AKIAVENDED".to_string(),
            secret_key: "vended-secret".to_string(),
            token: Some("vended-token".to_string()),
        };
        let shown = format!(
            "{:?}",
            Vended::new(credential, None, renewal(), s3_credential)
        );
        assert_eq!(
            shown,
            "Vended(<redacted>) for catalog `lab` at s3://b/warehouse/db/orders"
        );
    }

    /// A SAS token is found by the account's host or its name, whichever the catalog used.
    #[test]
    fn an_azure_sas_token_is_found_by_account() {
        let location = "abfss://container@acct.dfs.core.windows.net/warehouse/t";
        for key in [
            "adls.sas-token.acct.dfs.core.windows.net",
            "adls.sas-token.acct",
        ] {
            let p = props(&[(key, "?sv=2024&sig=abc%2B")]);
            let (credential, _) = azure_credential(&p, location).unwrap();
            match credential {
                AzureCredential::SASToken(pairs) => assert_eq!(
                    pairs,
                    [
                        ("sv".to_string(), "2024".to_string()),
                        ("sig".to_string(), "abc+".to_string())
                    ]
                ),
                _ => panic!("a SAS token expected"),
            }
        }
        assert!(azure_credential(&props(&[("adls.sas-token.other", "x")]), location).is_none());
    }

    #[test]
    fn families_follow_the_scheme() {
        assert_eq!(family("s3a://b/k"), Some(Family::S3));
        assert_eq!(family("gs://b/k"), Some(Family::Gcs));
        assert_eq!(
            family("abfss://c@a.dfs.core.windows.net/k"),
            Some(Family::Azure)
        );
        assert_eq!(family("file:///tmp/t"), None);
        assert_eq!(family("/tmp/t"), None);
    }
}
