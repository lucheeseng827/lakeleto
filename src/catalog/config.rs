//! Where catalogs are configured: `$LAKELETO_HOME/catalogs.toml`, and the environment.
//!
//! ```toml
//! [catalog.prod]
//! type = "rest"
//! uri = "https://polaris.example.com/api/catalog"
//! warehouse = "analytics"
//! oauth2-server-uri = "https://polaris.example.com/api/catalog/v1/oauth/tokens"
//! scope = "PRINCIPAL_ROLE:ALL"
//! # The secret comes from LAKELETO_CATALOG__PROD__CREDENTIAL, so it stays out of this file.
//! ```
//!
//! The keys are the ones the Iceberg REST clients use (Java, pyiceberg, iceberg-rust), so an
//! entry copies over from one of them. A catalog can also be configured entirely in the
//! environment, as `LAKELETO_CATALOG__<NAME>__<KEY>`: `__` separates the name from the key and the
//! parts of a nested key (joined with `.`), a single `_` stands for `-`, and both are lowercased.
//! So `LAKELETO_CATALOG__PROD__OAUTH2_SERVER_URI` sets `oauth2-server-uri` for `prod`, and
//! `LAKELETO_CATALOG__DEV__S3__ENDPOINT` sets `s3.endpoint` for `dev`. A value in the environment
//! wins over the file's value for the same key, and an empty one removes it.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;

use url::Url;

use super::reference::is_catalog_name;
use crate::error::{EngineError, Result};

/// The prefix of every environment variable that configures a catalog.
pub const ENV_PREFIX: &str = "LAKELETO_CATALOG__";

/// The file catalogs are configured in, under `$LAKELETO_HOME`.
pub const FILE_NAME: &str = "catalogs.toml";

/// Keys a catalog reads, beyond the namespaces below.
const KEYS: [&str; 8] = [
    "type",
    "uri",
    "warehouse",
    "credential",
    "token",
    "oauth2-server-uri",
    "scope",
    "storage-fallback",
];

/// Key namespaces passed through as they are: request headers, and storage properties for a
/// catalog that vends no credentials.
const NAMESPACES: [&str; 5] = ["header.", "s3.", "gcs.", "adls.", "client."];

/// One configured catalog: its name and its properties, with keys lowercased.
#[derive(Clone, PartialEq, Eq)]
pub struct CatalogConfig {
    name: String,
    props: BTreeMap<String, String>,
}

/// Prints the name and the keys, never a value: several are secrets.
impl std::fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogConfig")
            .field("name", &self.name)
            .field("keys", &self.props.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CatalogConfig {
    /// A catalog called `name`, with no properties yet. The name is lowercased.
    pub fn new(name: &str) -> Result<CatalogConfig> {
        if !is_catalog_name(name) {
            return Err(EngineError::Other(format!(
                "`{name}` is not a catalog name: a name is letters, digits and `-`, and starts \
                 with a letter or digit"
            )));
        }
        Ok(CatalogConfig {
            name: name.to_ascii_lowercase(),
            props: BTreeMap::new(),
        })
    }

    /// This catalog with `key` set to `value`. The key is lowercased.
    pub fn with(mut self, key: &str, value: impl Into<String>) -> CatalogConfig {
        self.props.insert(key.to_ascii_lowercase(), value.into());
        self
    }

    /// The catalog's name, lowercase.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The value of `key`, if it is set.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.props.get(key).map(String::as_str)
    }

    /// Every property, keys lowercased. Values include secrets: never print them.
    pub(crate) fn props(&self) -> &BTreeMap<String, String> {
        &self.props
    }

    /// The catalog's type. `rest` when the entry does not say.
    pub fn kind(&self) -> &str {
        self.get("type").unwrap_or("rest")
    }

    /// The catalog's base URI, as configured.
    pub fn uri(&self) -> Option<&str> {
        self.get("uri")
    }

    /// Check that the catalog can be used: a type this build reads, a URI it may send a login to,
    /// and settings that say one thing.
    pub fn validate(&self) -> Result<()> {
        let name = &self.name;
        let invalid = |detail: String| EngineError::Other(format!("catalog `{name}`: {detail}"));
        match self.kind() {
            "rest" => {}
            "ducklake" => {
                return Err(invalid(
                    "type `ducklake` arrives in Lakeleto 0.5.0; this release reads `rest` catalogs"
                        .into(),
                ))
            }
            other => {
                return Err(invalid(format!(
                    "unknown type `{other}`: this release reads `rest` catalogs"
                )))
            }
        }
        let uri = self
            .uri()
            .ok_or_else(|| invalid("no `uri`: set the catalog's base URI".into()))?;
        check_endpoint(uri).map_err(|e| invalid(format!("`uri` {e}")))?;
        if let Some(server) = self.get("oauth2-server-uri") {
            check_endpoint(server).map_err(|e| invalid(format!("`oauth2-server-uri` {e}")))?;
        }
        if self.get("token").is_some() && self.get("credential").is_some() {
            return Err(invalid(
                "both `token` and `credential` are set: a catalog logs in with a bearer token or \
                 with client credentials, so set one"
                    .into(),
            ));
        }
        match self.get("storage-fallback") {
            None | Some("ambient") | Some("none") => {}
            Some(other) => {
                return Err(invalid(format!(
                    "`storage-fallback` is `{other}`: it is `ambient` (the default) or `none`"
                )))
            }
        }
        Ok(())
    }

    /// Keys this catalog sets that Lakeleto does not read. They are kept, so an entry copied from
    /// another client works, and reported, so a misspelt key is not silently ignored.
    pub fn unknown_keys(&self) -> Vec<&str> {
        self.props
            .keys()
            .map(String::as_str)
            .filter(|k| !KEYS.contains(k) && !NAMESPACES.iter().any(|ns| k.starts_with(ns)))
            .collect()
    }

    /// The keys this catalog sets whose values are secrets.
    pub fn secret_keys(&self) -> Vec<&str> {
        self.props
            .keys()
            .map(String::as_str)
            .filter(|k| is_secret_key(k))
            .collect()
    }
}

/// Is `key`'s value a secret: a login, or a storage key?
pub(crate) fn is_secret_key(key: &str) -> bool {
    matches!(
        key,
        "credential" | "token" | "header.authorization" | "adls.account-key" | "gcs.oauth2.token"
    ) || key.ends_with("secret-access-key")
        || key.ends_with("session-token")
        || key.starts_with("adls.sas-token")
}

/// May a login be sent to `uri`? It must be `https`, or `http` to this machine: a token never
/// crosses a network in plaintext.
fn check_endpoint(uri: &str) -> std::result::Result<(), String> {
    let url = Url::parse(uri).map_err(|e| format!("`{uri}` is not a URL: {e}"))?;
    match url.scheme() {
        "https" => Ok(()),
        "http" if is_loopback(&url) => Ok(()),
        "http" => Err(format!(
            "`{uri}` is plain `http` to another machine. Lakeleto sends logins and receives \
             storage credentials from a catalog, so it reaches one over `https`, or over `http` \
             only on this machine (localhost, 127.0.0.1 or ::1)"
        )),
        other => Err(format!(
            "`{uri}` is `{other}`: a catalog is reached over `https`"
        )),
    }
}

/// Is `url`'s host this machine?
pub(crate) fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

/// Every catalog configured in `file` and in `env`, with the warnings loading them raised.
#[derive(Debug, Default)]
pub struct Loaded {
    pub catalogs: BTreeMap<String, CatalogConfig>,
    /// Things worth telling the operator that are not errors: unknown keys, a file others can read.
    pub warnings: Vec<String>,
}

/// Load the catalogs configured in `file` (when it exists) and in `env`, the environment's values
/// winning, and check each one.
pub fn load(file: &Path, env: impl IntoIterator<Item = (String, String)>) -> Result<Loaded> {
    let mut loaded = Loaded::default();
    match std::fs::read_to_string(file) {
        Ok(text) => {
            loaded.catalogs = parse_file(&text)
                .map_err(|e| EngineError::Other(format!("{}: {e}", file.display())))?;
            if let Some(warning) = permission_warning(file, &loaded.catalogs) {
                loaded.warnings.push(warning);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(EngineError::Other(format!("{}: {e}", file.display()))),
    }
    apply_env(&mut loaded.catalogs, env)?;
    for catalog in loaded.catalogs.values() {
        catalog.validate()?;
        let unknown = catalog.unknown_keys();
        if !unknown.is_empty() {
            loaded.warnings.push(format!(
                "catalog `{}`: Lakeleto does not read {} (it reads {}, and the {} keys)",
                catalog.name,
                unknown
                    .iter()
                    .map(|k| format!("`{k}`"))
                    .collect::<Vec<_>>()
                    .join(", "),
                KEYS.map(|k| format!("`{k}`")).join(", "),
                NAMESPACES.map(|ns| format!("`{ns}*`")).join(", "),
            ));
        }
    }
    Ok(loaded)
}

/// The catalogs a `catalogs.toml` configures: one `[catalog.<name>]` table each.
pub fn parse_file(text: &str) -> Result<BTreeMap<String, CatalogConfig>> {
    let doc: toml::Table = text
        .parse()
        .map_err(|e: toml::de::Error| EngineError::Other(e.to_string().trim_end().to_string()))?;
    let mut catalogs = BTreeMap::new();
    for (key, value) in doc {
        if key != "catalog" {
            return Err(EngineError::Other(format!(
                "unknown top-level key `{key}`: catalogs are configured as `[catalog.<name>]`"
            )));
        }
        let toml::Value::Table(entries) = value else {
            return Err(EngineError::Other(
                "`catalog` is not a table: configure each catalog as `[catalog.<name>]`".into(),
            ));
        };
        for (name, entry) in entries {
            let mut catalog = CatalogConfig::new(&name)?;
            let toml::Value::Table(entry) = entry else {
                return Err(EngineError::Other(format!(
                    "`catalog.{name}` is not a table: configure it as `[catalog.{name}]`"
                )));
            };
            flatten(&name, "", entry, &mut catalog.props)?;
            if catalogs.insert(catalog.name.clone(), catalog).is_some() {
                return Err(EngineError::Other(format!(
                    "catalog `{}` is configured twice, under names that differ only in case",
                    name.to_ascii_lowercase()
                )));
            }
        }
    }
    Ok(catalogs)
}

/// Copy a catalog's table into `props`, nested tables as dotted keys: `[catalog.dev.s3]` and
/// `s3.endpoint = …` both set `s3.endpoint`.
fn flatten(
    name: &str,
    prefix: &str,
    table: toml::Table,
    props: &mut BTreeMap<String, String>,
) -> Result<()> {
    for (key, value) in table {
        let key = format!("{prefix}{}", key.to_ascii_lowercase());
        let text = match value {
            toml::Value::Table(nested) => {
                flatten(name, &format!("{key}."), nested, props)?;
                continue;
            }
            toml::Value::String(s) => s,
            toml::Value::Integer(n) => n.to_string(),
            toml::Value::Float(n) => n.to_string(),
            toml::Value::Boolean(b) => b.to_string(),
            toml::Value::Array(_) | toml::Value::Datetime(_) => {
                return Err(EngineError::Other(format!(
                    "catalog `{name}`: `{key}` is not a string, number or boolean"
                )))
            }
        };
        props.insert(key, text);
    }
    Ok(())
}

/// Apply `LAKELETO_CATALOG__<NAME>__<KEY>` variables over the file's catalogs, defining a catalog
/// the file does not mention.
fn apply_env(
    catalogs: &mut BTreeMap<String, CatalogConfig>,
    env: impl IntoIterator<Item = (String, String)>,
) -> Result<()> {
    for (var, value) in env {
        let Some((name, key)) = env_key(&var) else {
            continue;
        };
        let catalog = match catalogs.entry(name) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let fresh = CatalogConfig::new(entry.key())
                    .map_err(|e| EngineError::Other(format!("{var}: {e}")))?;
                entry.insert(fresh)
            }
        };
        if value.is_empty() {
            catalog.props.remove(&key);
        } else {
            catalog.props.insert(key, value);
        }
    }
    // A variable that only removed keys from a catalog the file does not define leaves an empty
    // entry behind; it configures nothing.
    catalogs.retain(|_, c| !c.props.is_empty());
    Ok(())
}

/// The catalog name and key an environment variable sets, or `None` when it is not one of ours.
pub fn env_key(var: &str) -> Option<(String, String)> {
    let rest = var.strip_prefix(ENV_PREFIX)?;
    let mut parts = rest.split("__");
    let name = parts.next().filter(|n| !n.is_empty())?;
    let key: Vec<String> = parts
        .map(|p| p.to_ascii_lowercase().replace('_', "-"))
        .collect();
    if key.is_empty() || key.iter().any(String::is_empty) {
        return None;
    }
    Some((name.to_ascii_lowercase().replace('_', "-"), key.join(".")))
}

/// The variable that sets `key` for catalog `name`: [`env_key`] run backwards.
pub fn env_var(name: &str, key: &str) -> String {
    let parts: Vec<String> = key
        .split('.')
        .map(|p| p.replace('-', "_").to_ascii_uppercase())
        .collect();
    format!(
        "{ENV_PREFIX}{}__{}",
        name.replace('-', "_").to_ascii_uppercase(),
        parts.join("__")
    )
}

/// The environment as strings, for [`load`], and the catalog variables left out of it.
///
/// `std::env::vars` panics on a variable whose name or value is not UTF-8, and a variable
/// Lakeleto never reads must not stop it reading its own, so such variables are skipped. A
/// `LAKELETO_CATALOG__…` variable skipped that way is named, so its value is reported missing
/// rather than silently ignored.
pub fn utf8_env(
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut env = Vec::new();
    let mut skipped = Vec::new();
    for (name, value) in vars {
        match (name.into_string(), value.into_string()) {
            (Ok(name), Ok(value)) => env.push((name, value)),
            (Ok(name), Err(_)) if name.starts_with(ENV_PREFIX) => skipped.push(name),
            _ => {}
        }
    }
    (env, skipped)
}

/// On Unix, a warning when `file` holds a secret and someone other than its owner can read it, as
/// ssh warns about a private key.
#[cfg(unix)]
fn permission_warning(file: &Path, catalogs: &BTreeMap<String, CatalogConfig>) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(file).ok()?.permissions().mode();
    if mode & 0o077 == 0 {
        return None;
    }
    let holding: Vec<String> = catalogs
        .values()
        .flat_map(|c| {
            c.secret_keys()
                .into_iter()
                .map(move |k| format!("`{k}` for `{}`", c.name))
        })
        .collect();
    if holding.is_empty() {
        return None;
    }
    Some(format!(
        "{} holds {} and can be read by other users (mode {:o}). Run `chmod 600 {}`, or move the \
         secret to its LAKELETO_CATALOG__… variable",
        file.display(),
        holding.join(", "),
        mode & 0o777,
        file.display(),
    ))
}

#[cfg(not(unix))]
fn permission_warning(_file: &Path, _catalogs: &BTreeMap<String, CatalogConfig>) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The ADR's example parses as written, nested keys become dotted ones whichever way they are
    /// spelt, and non-string scalars become strings.
    #[test]
    fn a_file_configures_catalogs_with_dotted_keys() {
        let catalogs = parse_file(
            r#"
            # comments are why this file is TOML
            [catalog.prod]
            type = "rest"
            uri = "https://polaris.example.com/api/catalog"
            warehouse = "analytics"
            oauth2-server-uri = "https://polaris.example.com/api/catalog/v1/oauth/tokens"
            scope = "PRINCIPAL_ROLE:ALL"
            header.X-Tenant = "acme"

            [catalog.Dev]
            uri = "http://localhost:8181"
            s3.endpoint = "http://localhost:9000"
            s3.path-style-access = true

            [catalog.Dev.gcs]
            project-id = 42
            "#,
        )
        .unwrap();
        assert_eq!(catalogs.keys().collect::<Vec<_>>(), ["dev", "prod"]);
        let prod = &catalogs["prod"];
        assert_eq!(prod.get("warehouse"), Some("analytics"));
        assert_eq!(prod.get("header.x-tenant"), Some("acme"));
        let dev = &catalogs["dev"];
        assert_eq!(dev.kind(), "rest", "the type defaults to rest");
        assert_eq!(dev.get("s3.endpoint"), Some("http://localhost:9000"));
        assert_eq!(dev.get("s3.path-style-access"), Some("true"));
        assert_eq!(dev.get("gcs.project-id"), Some("42"));
    }

    #[test]
    fn a_malformed_file_is_refused_with_what_is_wrong() {
        for (text, says) in [
            (
                "[catalogs.prod]\nuri = \"https://x\"",
                "unknown top-level key `catalogs`",
            ),
            ("catalog = 1", "`catalog` is not a table"),
            ("[catalog]\nprod = \"x\"", "`catalog.prod` is not a table"),
            ("[catalog.pr_od]\nuri = \"https://x\"", "not a catalog name"),
            ("[catalog.prod]\nscope = [\"a\"]", "`scope` is not a string"),
            ("[catalog.prod]\nuri = ", "TOML parse error"),
            (
                "[catalog.Prod]\nuri = \"https://x\"\n[catalog.prod]\nuri = \"https://y\"",
                "configured twice",
            ),
        ] {
            let err = parse_file(text).unwrap_err().to_string();
            assert!(err.contains(says), "{text:?}: {err}");
        }
    }

    /// The mapping runs both ways: `_` for `-`, `__` for nesting, any case in, lowercase out.
    #[test]
    fn environment_variables_map_to_keys_and_back() {
        for (var, name, key) in [
            ("LAKELETO_CATALOG__PROD__URI", "prod", "uri"),
            (
                "LAKELETO_CATALOG__PROD__OAUTH2_SERVER_URI",
                "prod",
                "oauth2-server-uri",
            ),
            ("LAKELETO_CATALOG__DEV__S3__ENDPOINT", "dev", "s3.endpoint"),
            (
                "LAKELETO_CATALOG__MY_CAT__HEADER__X_TENANT",
                "my-cat",
                "header.x-tenant",
            ),
        ] {
            assert_eq!(
                env_key(var),
                Some((name.to_string(), key.to_string())),
                "{var}"
            );
            assert_eq!(env_var(name, key), var);
        }
        for var in [
            "LAKELETO_CATALOG__PROD",
            "LAKELETO_CATALOG____URI",
            "LAKELETO_CATALOG__PROD____URI",
            "LAKELETO_HOME",
            "AWS_REGION",
        ] {
            assert_eq!(env_key(var), None, "{var}");
        }
    }

    /// The environment wins over the file, an empty value removes the file's, and a catalog can be
    /// configured with no file at all.
    #[test]
    fn the_environment_overrides_the_file_and_can_stand_alone() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(FILE_NAME);
        std::fs::write(
            &file,
            "[catalog.prod]\nuri = \"https://file.example.com\"\nwarehouse = \"w\"\n",
        )
        .unwrap();
        let loaded = load(
            &file,
            env(&[
                ("LAKELETO_CATALOG__PROD__URI", "https://env.example.com"),
                ("LAKELETO_CATALOG__PROD__WAREHOUSE", ""),
                ("LAKELETO_CATALOG__ENVONLY__URI", "http://127.0.0.1:8181"),
                ("PATH", "/usr/bin"),
            ]),
        )
        .unwrap();
        let prod = &loaded.catalogs["prod"];
        assert_eq!(prod.uri(), Some("https://env.example.com"));
        assert_eq!(prod.get("warehouse"), None);
        assert_eq!(
            loaded.catalogs["envonly"].uri(),
            Some("http://127.0.0.1:8181")
        );

        let none = load(&dir.path().join("missing.toml"), env(&[])).unwrap();
        assert!(
            none.catalogs.is_empty(),
            "no file and no variables is no catalogs"
        );
    }

    /// What `validate` refuses, each with the setting to change.
    #[test]
    fn a_catalog_that_cannot_be_used_is_refused_at_load() {
        let base = || {
            CatalogConfig::new("prod")
                .unwrap()
                .with("uri", "https://c.example.com")
        };
        for (catalog, says) in [
            (CatalogConfig::new("prod").unwrap(), "no `uri`"),
            (base().with("type", "glue"), "unknown type `glue`"),
            (base().with("type", "ducklake"), "0.5.0"),
            (base().with("uri", "not a url"), "is not a URL"),
            (base().with("uri", "ftp://c.example.com"), "is `ftp`"),
            (
                base().with("uri", "http://c.example.com"),
                "plain `http` to another machine",
            ),
            (
                base().with("oauth2-server-uri", "http://idp.example.com/token"),
                "`oauth2-server-uri`",
            ),
            (
                base().with("token", "t").with("credential", "id:secret"),
                "set one",
            ),
            (
                base().with("storage-fallback", "env"),
                "`ambient` (the default) or `none`",
            ),
        ] {
            let err = catalog.validate().unwrap_err().to_string();
            assert!(err.contains(says), "{catalog:?}: {err}");
            assert!(err.starts_with("catalog `prod`"), "{err}");
        }
        for uri in [
            "http://localhost:8181",
            "http://127.0.0.1:8181/api",
            "http://[::1]:8181",
            "https://c.example.com",
        ] {
            assert!(base().with("uri", uri).validate().is_ok(), "{uri}");
        }
    }

    /// Unknown keys are kept and reported; keys under the pass-through namespaces are not unknown.
    #[test]
    fn unknown_keys_are_reported_rather_than_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(FILE_NAME);
        std::fs::write(
            &file,
            "[catalog.prod]\nuri = \"https://c\"\noauth2-server-url = \"https://typo\"\n\
             s3.endpoint = \"https://s3\"\nheader.x-a = \"b\"\n",
        )
        .unwrap();
        let loaded = load(&file, env(&[])).unwrap();
        assert_eq!(
            loaded.catalogs["prod"].unknown_keys(),
            ["oauth2-server-url"]
        );
        assert!(
            loaded
                .warnings
                .iter()
                .any(|w| w.contains("`oauth2-server-url`") && w.contains("`oauth2-server-uri`")),
            "{:?}",
            loaded.warnings
        );
    }

    /// A file holding a secret warns when others can read it, and only then.
    #[cfg(unix)]
    #[test]
    fn a_secret_in_a_file_others_can_read_is_warned_about() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(FILE_NAME);
        let set_mode =
            |mode| std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();

        std::fs::write(
            &file,
            "[catalog.prod]\nuri = \"https://c\"\ntoken = \"t0p\"\n",
        )
        .unwrap();
        set_mode(0o644);
        let warnings = load(&file, env(&[])).unwrap().warnings;
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("`token` for `prod`") && w.contains("chmod 600")),
            "{warnings:?}"
        );
        assert!(
            !warnings.iter().any(|w| w.contains("t0p")),
            "the secret is never printed"
        );

        set_mode(0o600);
        assert!(load(&file, env(&[])).unwrap().warnings.is_empty());

        std::fs::write(&file, "[catalog.prod]\nuri = \"https://c\"\n").unwrap();
        set_mode(0o644);
        assert!(
            load(&file, env(&[])).unwrap().warnings.is_empty(),
            "a file with no secret in it is nobody's business"
        );
    }

    /// `Debug` is what lands in a log line or a panic message, so it names keys and never values.
    #[test]
    fn debug_never_prints_a_value() {
        let catalog = CatalogConfig::new("prod")
            .unwrap()
            .with("credential", "client:hunter2");
        let shown = format!("{catalog:?}");
        assert!(
            shown.contains("credential") && !shown.contains("hunter2"),
            "{shown}"
        );
    }

    /// Only a missing `catalogs.toml` means no catalogs are configured. One that is there but
    /// cannot be read fails loading, rather than quietly configuring nothing.
    #[test]
    fn a_file_that_cannot_be_read_is_an_error_and_a_missing_one_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(FILE_NAME);
        assert!(load(&file, env(&[])).unwrap().catalogs.is_empty());

        std::fs::write(&file, b"[catalog.prod]\nuri = \"caf\xe9\"\n").unwrap();
        let err = load(&file, env(&[])).unwrap_err().to_string();
        assert!(err.contains(FILE_NAME), "not UTF-8: {err}");

        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        let err = load(&file, env(&[])).unwrap_err().to_string();
        assert!(err.contains(FILE_NAME), "a directory: {err}");
    }

    /// What counts as a secret decides when a readable file is warned about: every login and every
    /// storage key does, and nothing that only says where a catalog or its tables are.
    #[test]
    fn logins_and_storage_keys_are_secrets_and_locations_are_not() {
        for key in [
            "credential",
            "token",
            "header.authorization",
            "s3.secret-access-key",
            "s3.session-token",
            "adls.account-key",
            "adls.sas-token.acct.dfs.core.windows.net",
            "gcs.oauth2.token",
        ] {
            assert!(is_secret_key(key), "{key} is a secret");
        }
        for key in [
            "uri",
            "warehouse",
            "oauth2-server-uri",
            "scope",
            "header.x-tenant",
            "s3.access-key-id",
            "s3.endpoint",
            "s3.region",
            "adls.account-name",
            "gcs.project-id",
        ] {
            assert!(!is_secret_key(key), "{key} is not a secret");
        }
    }

    /// A variable that is not UTF-8 is skipped rather than panicking the read, and one of
    /// Lakeleto's own is named so its value is reported missing.
    #[cfg(unix)]
    #[test]
    fn variables_that_are_not_utf8_are_skipped_and_ours_are_named() {
        use std::os::unix::ffi::OsStringExt;
        let not_utf8 = || OsString::from_vec(b"caf\xe9".to_vec());
        let (env, skipped) = utf8_env([
            (
                OsString::from("LAKELETO_CATALOG__PROD__URI"),
                OsString::from("https://c.example.com"),
            ),
            (OsString::from("LAKELETO_CATALOG__PROD__TOKEN"), not_utf8()),
            (OsString::from("LEGACY_PATH"), not_utf8()),
            (not_utf8(), OsString::from("anything")),
        ]);
        assert_eq!(
            env,
            [(
                "LAKELETO_CATALOG__PROD__URI".to_string(),
                "https://c.example.com".to_string()
            )]
        );
        assert_eq!(skipped, ["LAKELETO_CATALOG__PROD__TOKEN"]);
    }
}
