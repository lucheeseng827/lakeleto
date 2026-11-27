//! OpenLineage: a run of `check`, `diff` or an export, as a lineage service reads it.
//!
//! With `--openlineage <url>` (an endpoint, such as Marquez's `/api/v1/lineage`), or
//! `OPENLINEAGE_URL` (a server, whose `/api/v1/lineage` takes them, as dagron and the OpenLineage
//! clients read it), a run is sent as two events, START and then COMPLETE or FAIL, so it shows next
//! to the datasets it was about. Nothing is sent unless one of them asks. `OPENLINEAGE_API_KEY`
//! authenticates, as an `Authorization: Bearer` header; it is never read from the command line.
//! The job is under `OPENLINEAGE_NAMESPACE`, else [`JOB_NAMESPACE`].
//!
//! Datasets are named as ADR-0002 §6 says:
//!
//! - a local file or directory: namespace `file` and its absolute path, as Covenant names one;
//! - an object: `<scheme>://<bucket>` (an ADLS one `abfss://<container>@<account>…`) and its key;
//! - a catalog table: its location, named the same way, with its catalog identity in the spec's
//!   `symlinks` and `catalog` facets;
//! - a database table, by the OpenLineage naming conventions: Postgres `postgres://<host>:<port>`
//!   and `<database>.<schema>.<table>`, MySQL `mysql://<host>:<port>` and `<database>.<table>`.
//!
//! What none of these names (a SQLite table, which the conventions list nothing for; a Postgres
//! table named without its schema, which the server's `search_path` resolves; a relative path a
//! server resolves) is not guessed at: the run is not sent, and one warning says why.
//!
//! A check's input carries two facets, as Covenant's does: `dataQualityAssertions`, one assertion
//! per gate it judged and per rule a contract held the rows to, and `dataQualityMetrics`, what it
//! measured. No value from the table, and no error message, leaves the machine.

use std::path::Path;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::error::{EngineError, Result};

/// The events' schema: OpenLineage 2-0-2.
pub const EVENT_SCHEMA: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
const ASSERTIONS_SCHEMA: &str = "https://openlineage.io/spec/facets/1-0-2/DataQualityAssertionsDatasetFacet.json#/$defs/DataQualityAssertionsDatasetFacet";
const METRICS_SCHEMA: &str = "https://openlineage.io/spec/facets/1-0-3/DataQualityMetricsInputDatasetFacet.json#/$defs/DataQualityMetricsInputDatasetFacet";
#[cfg(feature = "catalog")]
const SYMLINKS_SCHEMA: &str =
    "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet";
#[cfg(feature = "catalog")]
const CATALOG_SCHEMA: &str =
    "https://openlineage.io/spec/facets/1-0-0/CatalogDatasetFacet.json#/$defs/CatalogDatasetFacet";

/// The server events are sent to, when `--openlineage` names none: its `/api/v1/lineage`.
pub const URL_ENV: &str = "OPENLINEAGE_URL";
/// The key the events are sent with, as `Authorization: Bearer`.
pub const API_KEY_ENV: &str = "OPENLINEAGE_API_KEY";
/// The job namespace, when set.
pub const NAMESPACE_ENV: &str = "OPENLINEAGE_NAMESPACE";
/// The job namespace a run is under, unless [`NAMESPACE_ENV`] names another.
pub const JOB_NAMESPACE: &str = "lakeleto";
/// Where a server named by [`URL_ENV`] takes events.
const LINEAGE_PATH: &str = "/api/v1/lineage";

/// The code that sent an event: Lakeleto's release repository at this version's tag, as
/// Covenant names its own (releases are tagged on the mirror, not in the monorepo).
fn producer() -> String {
    format!(
        "https://github.com/lucheeseng827/lakeleto/tree/v{}",
        env!("CARGO_PKG_VERSION")
    )
}

/// An endpoint events may be sent to: `https` anywhere, plain `http` only to this machine (a
/// loopback address or `localhost`), and no credentials in it, as Covenant sends its own. The key
/// and the events would otherwise cross the network in the clear, and a URL is argv.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    url: String,
    /// Plain http to this machine: no proxy is ever in the way.
    loopback: bool,
}

impl Destination {
    /// `url`, held to the rule; `Err` says why not, without repeating the URL.
    pub fn parse(url: &str) -> std::result::Result<Destination, String> {
        let Some((scheme, rest)) = url.split_once("://") else {
            return Err("not an http(s) URL".to_string());
        };
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        if authority.contains('@') {
            return Err(format!(
                "credentials do not go in a URL, which is argv: set {API_KEY_ENV}"
            ));
        }
        let host = match authority.strip_prefix('[') {
            Some(v6) => v6.split(']').next().unwrap_or_default(),
            None => authority.split(':').next().unwrap_or_default(),
        };
        if host.is_empty() {
            return Err("it names no host".to_string());
        }
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        match scheme.to_ascii_lowercase().as_str() {
            "https" => {}
            "http" if loopback => {}
            "http" => {
                return Err(
                    "plain http only to this machine (localhost, 127.0.0.1, ::1): use https"
                        .to_string(),
                )
            }
            _ => return Err("not an http(s) URL".to_string()),
        }
        Ok(Destination {
            url: url.to_string(),
            loopback,
        })
    }

    /// The endpoint as messages name it: without its query or fragment, where a token put in the
    /// URL would be, so a log never keeps one.
    pub fn shown(&self) -> &str {
        self.url.split(['?', '#']).next().unwrap_or_default()
    }
}

/// Where this process sends events, if anywhere: `flag` (`--openlineage`, an endpoint), else
/// [`URL_ENV`] (a server, whose `/api/v1/lineage` takes them).
///
/// A flag that breaks [`Destination`]'s rule refuses the run, before anything is read: it was
/// asked for on this command line. The variable only warns, and nothing is sent, since a scheduler
/// sets it for every command it runs.
pub fn destination(flag: Option<&str>) -> Result<Option<Destination>> {
    if let Some(url) = flag {
        return Destination::parse(url)
            .map(Some)
            .map_err(|why| EngineError::Other(format!("cannot send to --openlineage: {why}")));
    }
    let Some(base) = std::env::var(URL_ENV).ok().filter(|v| !v.trim().is_empty()) else {
        return Ok(None);
    };
    let endpoint = format!("{}{LINEAGE_PATH}", base.trim().trim_end_matches('/'));
    match Destination::parse(&endpoint) {
        Ok(to) => Ok(Some(to)),
        Err(why) => {
            warn(&format!(
                "{URL_ENV} is set, but no OpenLineage event is sent: {why}"
            ));
            Ok(None)
        }
    }
}

/// One line on stderr, never in a command's result.
pub fn warn(message: &str) {
    eprintln!("lakeleto: warning: {message}");
}

/// A dataset as OpenLineage names it, with the facets that say more of it.
#[derive(Debug, Clone, PartialEq)]
pub struct Dataset {
    pub namespace: String,
    pub name: String,
    /// Dataset facets: a catalog table's `symlinks` and `catalog`.
    pub facets: Map<String, Value>,
}

impl Dataset {
    fn new(namespace: impl Into<String>, name: impl Into<String>) -> Dataset {
        Dataset {
            namespace: namespace.into(),
            name: name.into(),
            facets: Map::new(),
        }
    }

    /// The dataset as a job's name shows it: `file:/data/orders.csv`, `s3://lake/orders`.
    fn shown(&self) -> String {
        match self.namespace.contains("://") {
            true => format!("{}/{}", self.namespace, self.name),
            false => format!("{}:{}", self.namespace, self.name),
        }
    }

    /// The dataset in an event: its name, its facets, and the input facets given.
    fn json(&self, input_facets: Option<&Value>) -> Value {
        let mut dataset = json!({ "namespace": self.namespace, "name": self.name });
        if !self.facets.is_empty() {
            dataset["facets"] = Value::Object(self.facets.clone());
        }
        if let Some(facets) = input_facets {
            dataset["inputFacets"] = facets.clone();
        }
        dataset
    }
}

/// A local file or directory: namespace `file`, and its absolute path (not resolved through
/// symlinks, as Covenant names one).
pub fn local(path: &Path) -> std::result::Result<Dataset, String> {
    let absolute = std::path::absolute(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let name = absolute
        .to_str()
        .ok_or_else(|| format!("{} is not UTF-8, so it has no name", absolute.display()))?;
    Ok(Dataset::new("file", name))
}

/// An object: namespace `<scheme>://<authority>` (a bucket, or an ADLS container and account),
/// and its key.
fn object(uri: &str) -> std::result::Result<Dataset, String> {
    let account = std::env::var(AZURE_ACCOUNT_ENV).ok();
    object_in(uri, account.as_deref())
}

/// [`object`], with `account` the storage account an `az://` container is in.
fn object_in(uri: &str, account: Option<&str>) -> std::result::Result<Dataset, String> {
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| format!("{uri} is not an object URI"))?;
    // A query or fragment (a version id, a signature) is no part of the object's name, and could
    // be a credential: neither leaves.
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    let (authority, key) = rest.split_once('/').unwrap_or((rest, ""));
    let key = key.trim_end_matches('/');
    let scheme = scheme.to_ascii_lowercase();
    // Neither does a user or password. ADLS writes its container where a user would be
    // (`container@account.dfs.core.windows.net`), which is kept, unless it holds a password.
    let adls = matches!(scheme.as_str(), "abfs" | "abfss");
    let authority = match authority.rsplit_once('@') {
        Some((container, host)) if adls && container.contains(':') => {
            return Err(format!(
                "{scheme}://…@{host} carries a password, which no name is sent with"
            ))
        }
        Some((_, host)) if !adls => host,
        _ => authority,
    };
    if authority.is_empty() || key.is_empty() {
        return Err(format!(
            "{scheme}://{authority}/… names no object in a bucket"
        ));
    }
    let namespace = match scheme.as_str() {
        // The other spellings of one store are one namespace, the OpenLineage conventions' own.
        "s3" | "s3a" => format!("s3://{authority}"),
        "gs" | "gcs" => format!("gs://{authority}"),
        // ADLS by the conventions: the container at the account's DFS host. `az://container/…`
        // names no account, which the store takes from the environment, as this does.
        "abfs" | "abfss" if authority.contains('@') => format!("abfss://{authority}"),
        "az" | "azure" | "adl" | "abfs" | "abfss" => {
            let account = account.filter(|a| !a.trim().is_empty()).ok_or_else(|| {
                format!(
                    "{scheme}://{authority}/… names no storage account, which ADLS is named \
                         by: set {AZURE_ACCOUNT_ENV}, or name it \
                         abfss://<container>@<account>.dfs.core.windows.net/…"
                )
            })?;
            format!(
                "abfss://{authority}@{}.dfs.core.windows.net",
                account.trim()
            )
        }
        other => format!("{other}://{authority}"),
    };
    Ok(Dataset::new(namespace, key))
}

/// The storage account an `az://` URI's container is in, as the store reads it.
const AZURE_ACCOUNT_ENV: &str = "AZURE_STORAGE_ACCOUNT_NAME";

/// A database table, by the OpenLineage naming conventions. The URI's credentials and every
/// parameter but the table never reach the name.
fn database(uri: &str) -> std::result::Result<Dataset, String> {
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| "not a database URI".to_string())?;
    let (scheme, port) = match scheme.to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => ("postgres", 5432),
        "mysql" => ("mysql", 3306),
        _ => {
            return Err(format!(
                "a {scheme} table has no OpenLineage name: its naming conventions list none"
            ))
        }
    };
    let (location, query) = rest.split_once('?').unwrap_or((rest, ""));
    let table = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == "table")
        .map(|(_, value)| percent_decoded(value))
        .filter(|t| !t.is_empty())
        .ok_or_else(|| "a database URI names no table (`?table=`)".to_string())?;
    let (authority, path) = location.split_once('/').unwrap_or((location, ""));
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let (host, given_port) = match host_port.strip_prefix('[') {
        Some(v6) => {
            let (host, after) = v6.split_once(']').unwrap_or((v6, ""));
            (format!("[{host}]"), after.strip_prefix(':'))
        }
        None => match host_port.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), Some(port)),
            None => (host_port.to_string(), None),
        },
    };
    let database = percent_decoded(path.split('/').next().unwrap_or_default());
    if host.is_empty() || database.is_empty() {
        return Err("a database URI names no host or no database".to_string());
    }
    let port = match given_port {
        Some(p) => p
            .parse::<u16>()
            .map_err(|_| "a database URI's port is not a number".to_string())?,
        None => port,
    };
    let name = match scheme {
        "postgres" => match table.split_once('.') {
            Some((schema, table)) if !schema.is_empty() && !table.is_empty() => {
                format!("{database}.{schema}.{table}")
            }
            _ => {
                return Err(format!(
                    "the Postgres table `{table}` is named without its schema, which the \
                     server's search_path resolves: name it `?table=<schema>.{table}`"
                ))
            }
        },
        _ => format!("{database}.{table}"),
    };
    Ok(Dataset::new(format!("{scheme}://{host}:{port}"), name))
}

/// `%XX` escapes decoded; anything else as written.
fn percent_decoded(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        match (bytes[i], bytes.get(i + 1), bytes.get(i + 2)) {
            (b'%', Some(&h), Some(&l)) if hex(h).is_some() && hex(l).is_some() => {
                out.push((hex(h).unwrap_or(0) * 16 + hex(l).unwrap_or(0)) as u8);
                i += 3;
            }
            (b, ..) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// What `reference` (a path, an object, a catalog or database reference) names, as OpenLineage
/// names it; `Err` says why it cannot be named. `remote`: the reference is a server's to resolve,
/// so a relative path is not this machine's to make whole.
pub fn dataset(
    #[cfg_attr(not(feature = "catalog"), allow(unused_variables))] ctx: &crate::RequestContext,
    reference: &str,
    remote: bool,
) -> std::result::Result<Dataset, String> {
    if crate::catalog::is_catalog_uri(reference) {
        if remote {
            return Err(format!(
                "{reference} is a catalog table the server reads, which this machine cannot locate"
            ));
        }
        #[cfg(feature = "catalog")]
        return catalog_table(ctx, reference);
        #[cfg(not(feature = "catalog"))]
        return Err(format!(
            "{reference} is a catalog table, and this build reads no catalog"
        ));
    }
    if crate::source::is_database_uri(reference) {
        return database(reference)
            .map_err(|why| format!("{}: {why}", crate::source::redact_uri_password(reference)));
    }
    if crate::source::is_object_uri(reference) {
        return object(reference);
    }
    if remote {
        return match Path::new(reference).is_absolute() {
            true => Ok(Dataset::new("file", reference)),
            false => Err(format!(
                "{reference} is a path the server resolves, and only an absolute one names its file"
            )),
        };
    }
    local(Path::new(reference))
}

/// A catalog table: named by its location, with the catalog's name for it in `symlinks` and the
/// catalog itself in `catalog`.
#[cfg(feature = "catalog")]
fn catalog_table(
    ctx: &crate::RequestContext,
    reference: &str,
) -> std::result::Result<Dataset, String> {
    let parsed = crate::catalog::CatalogRef::parse(reference).map_err(|e| e.to_string())?;
    let catalogs = crate::catalog::Catalogs::configured();
    let handle = catalogs
        .load_table(ctx, &parsed)
        .map_err(|e| format!("{reference} could not be located: {e}"))?;
    let metadata = handle
        .metadata
        .as_deref()
        .ok_or_else(|| format!("{reference}: the catalog handed over no metadata"))?;
    let root = crate::engine::local::table_root(metadata, &handle.location.to_string_lossy());
    let mut dataset = match crate::source::is_object_uri(&root) {
        true => object(&root)?,
        false => local(Path::new(&root))?,
    };
    let name = parsed.catalog().unwrap_or_default();
    let configs = catalogs.configs().map_err(|e| e.to_string())?;
    let config = configs
        .iter()
        .find(|c| c.name() == name)
        .ok_or_else(|| format!("{reference}: catalog `{name}` is not configured"))?;
    let uri = config.uri().map(bare_uri).unwrap_or_default();
    let producer = producer();
    dataset.facets.insert(
        "symlinks".to_string(),
        json!({
            "_producer": producer,
            "_schemaURL": SYMLINKS_SCHEMA,
            "identifiers": [{ "namespace": uri, "name": parsed.dotted(), "type": "TABLE" }],
        }),
    );
    let mut catalog = json!({
        "_producer": producer,
        "_schemaURL": CATALOG_SCHEMA,
        "framework": "iceberg",
        "type": config.kind(),
        "name": name,
        "metadataUri": uri,
    });
    // A REST catalog's `warehouse` is often a name, which is no location of the data.
    if let Some(warehouse) = config.get("warehouse").filter(|w| w.contains("://")) {
        catalog["warehouseUri"] = json!(bare_uri(warehouse));
    }
    dataset.facets.insert("catalog".to_string(), catalog);
    Ok(dataset)
}

/// `uri` with no user, password, query or fragment: the part that names a place, and none of
/// what could be a credential. One with none of them is as configured, so it names the catalog the
/// way the other clients configured with it do.
#[cfg(feature = "catalog")]
fn bare_uri(uri: &str) -> String {
    match url::Url::parse(uri) {
        Ok(url)
            if url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none() =>
        {
            uri.to_string()
        }
        Ok(mut url) => {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        }
        Err(_) => uri.split(['?', '#']).next().unwrap_or_default().to_string(),
    }
}

/// How a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// It gave its result, whatever that said: a check whose gates failed completed.
    Complete,
    /// It ended in an error.
    Fail,
}

impl Outcome {
    pub fn event_type(self) -> &'static str {
        match self {
            Outcome::Complete => "COMPLETE",
            Outcome::Fail => "FAIL",
        }
    }
}

/// A run being lineaged: where it is sent, when it started, and its id.
#[derive(Debug, Clone)]
pub struct Run {
    to: Destination,
    started_ms: i64,
    run_id: String,
}

/// A dataset a run read, and the facets that say what it found of it.
#[derive(Debug, Clone)]
pub struct Input {
    pub dataset: Dataset,
    pub facets: Option<Value>,
}

impl Run {
    /// A run starting now, to be sent to `to`.
    pub fn start(to: Destination) -> Run {
        let started_ms = now_ms();
        Run {
            run_id: run_id(started_ms),
            started_ms,
            to,
        }
    }

    pub fn destination(&self) -> &Destination {
        &self.to
    }

    /// The run's two events, START and its `outcome` now. The job is `job`'s command, named by
    /// what it is about. START names the datasets; the end adds the facets. A dataset read twice
    /// (a table diffed against its own snapshot) is one input, its first.
    pub fn events(
        &self,
        job: &Job<'_>,
        inputs: &[Input],
        outputs: &[Dataset],
        outcome: Outcome,
    ) -> [Value; 2] {
        self.events_at(job, inputs, outputs, outcome, now_ms())
    }

    fn events_at(
        &self,
        job: &Job<'_>,
        inputs: &[Input],
        outputs: &[Dataset],
        outcome: Outcome,
        ended_ms: i64,
    ) -> [Value; 2] {
        let namespace = std::env::var(NAMESPACE_ENV)
            .ok()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| JOB_NAMESPACE.to_string());
        let mut seen = std::collections::HashSet::new();
        let inputs: Vec<&Input> = inputs
            .iter()
            .filter(|i| seen.insert((&i.dataset.namespace, &i.dataset.name)))
            .collect();
        let job = job_name(job, &inputs);
        let producer = producer();
        let outputs: Vec<Value> = outputs.iter().map(|d| d.json(None)).collect();
        let event = |event_type: &str, at: i64, inputs: Vec<Value>| {
            json!({
                "eventType": event_type,
                "eventTime": rfc3339_ms(at),
                "run": { "runId": self.run_id },
                "job": { "namespace": namespace, "name": job },
                "inputs": inputs,
                "outputs": outputs,
                "producer": producer,
                "schemaURL": EVENT_SCHEMA,
            })
        };
        let named = inputs.iter().map(|i| i.dataset.json(None)).collect();
        let found = inputs
            .iter()
            .map(|i| i.dataset.json(i.facets.as_ref()))
            .collect();
        [
            event("START", self.started_ms, named),
            event(outcome.event_type(), ended_ms.max(self.started_ms), found),
        ]
    }

    /// Send the run's events, in order. `Err` says why they were not delivered, in words that
    /// never include the key.
    pub fn send(&self, events: &[Value; 2]) -> std::result::Result<(), String> {
        // One client for both: one connection, and one TLS handshake.
        let client = client(&self.to)?;
        for event in events {
            let body = serde_json::to_vec(event).map_err(|e| e.to_string())?;
            post(&client, &self.to, body)?;
        }
        Ok(())
    }
}

/// What a run is a run of: its command, and the dataset it is named for when that is not its
/// inputs. An export to a file is named for the file, whether or not it was written, so its runs
/// that failed are runs of the same job as those that completed.
#[derive(Debug, Clone, Copy)]
pub struct Job<'a> {
    pub command: &'a str,
    pub named_for: Option<&'a Dataset>,
}

/// A job's name: the command, and what it is about — the dataset it is named for, or its inputs.
fn job_name(job: &Job<'_>, inputs: &[&Input]) -> String {
    let about: Vec<String> = match job.named_for {
        Some(dataset) => vec![dataset.shown()],
        None => inputs.iter().map(|i| i.dataset.shown()).collect(),
    };
    match about.is_empty() {
        true => job.command.to_string(),
        false => format!("{} {}", job.command, about.join(" ")),
    }
}

/// The rules a check's contract held the rows to: each by field, in the model's order, as
/// Covenant names it, and whether its policy only warns (`on_violation: warn`).
#[derive(Debug, Clone, Default)]
pub struct ContractRules {
    pub declared: Vec<(Option<String>, &'static str)>,
    pub warn_only: bool,
}

/// The facets a check's input carries: each gate it judged and each rule a contract held the rows
/// to, as an assertion, and what it measured.
///
/// A gate that judged nothing (skipped, or unknown) is no assertion. `declared` is every rule the
/// contract's model holds a row to, by field, in its order; a rule the run reported broken that
/// it does not declare (a column missing) is an assertion too, failed.
pub fn check_facets(report: &crate::check::CheckReport, contract_rules: &ContractRules) -> Value {
    use crate::check::Status;
    let producer = producer();
    let declared = contract_rules.declared.as_slice();
    // A gate fails the check. A contract's rule is as severe as its policy says, as Covenant's
    // events have it: `warn` under `on_violation: warn`.
    let rule_severity = match contract_rules.warn_only {
        true => "warn",
        false => "error",
    };
    let assertion = |name: &str, success: bool, column: Option<&str>, severity: &str| {
        let mut a = json!({ "assertion": name, "success": success, "severity": severity });
        if let Some(column) = column.filter(|c| !c.is_empty()) {
            a["column"] = json!(column);
        }
        a
    };
    let mut assertions: Vec<Value> = report
        .gates
        .iter()
        .filter(|g| matches!(g.status, Status::Pass | Status::Fail))
        .map(|g| assertion(g.name.name(), g.status == Status::Pass, None, "error"))
        .collect();
    if let Some(contract) = &report.contract {
        let broken = |field: Option<&str>, rule: &str| {
            contract.rules.iter().any(|t| {
                t.count > 0
                    && t.rule == rule
                    && (rule == "unexpected_field" || t.field == field.unwrap_or_default())
            })
        };
        for (field, rule) in declared {
            assertions.push(assertion(
                rule,
                !broken(field.as_deref(), rule),
                field.as_deref(),
                rule_severity,
            ));
        }
        for tally in contract.rules.iter().filter(|t| t.count > 0) {
            let field = (!tally.field.is_empty()).then_some(tally.field.as_str());
            let is_declared = declared.iter().any(|(f, r)| {
                *r == tally.rule && (tally.rule == "unexpected_field" || f.as_deref() == field)
            });
            if !is_declared {
                assertions.push(assertion(&tally.rule, false, field, rule_severity));
            }
        }
    }
    let measured = &report.measured;
    let columns: Map<String, Value> = measured
        .nulls
        .iter()
        .filter_map(|c| Some((c.name.clone(), json!({ "nullCount": c.nulls? }))))
        .collect();
    let mut metrics = json!({
        "_producer": producer,
        "_schemaURL": METRICS_SCHEMA,
        "rowCount": measured.rows,
        "columnMetrics": columns,
    });
    if let Some(bytes) = measured.bytes {
        metrics["bytes"] = json!(bytes);
    }
    if let Some(files) = measured.files {
        metrics["fileCount"] = json!(files);
    }
    if let Some(updated) = measured.updated_ms {
        metrics["lastUpdated"] = json!(rfc3339_ms(updated));
    }
    json!({
        "dataQualityAssertions": {
            "_producer": producer,
            "_schemaURL": ASSERTIONS_SCHEMA,
            "assertions": assertions,
        },
        "dataQualityMetrics": metrics,
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// A UUIDv7: the run's start in milliseconds, then random bits.
fn run_id(started_ms: i64) -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut random = [0u8; 16];
    for (i, chunk) in random.chunks_mut(8).enumerate() {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_usize(i);
        hasher.write_i64(started_ms);
        hasher.write_u32(std::process::id());
        chunk.copy_from_slice(&hasher.finish().to_be_bytes());
    }
    let ms = (started_ms.max(0) as u64).to_be_bytes();
    let mut b = [0u8; 16];
    b[..6].copy_from_slice(&ms[2..]);
    b[6] = 0x70 | (random[0] & 0x0f);
    b[7] = random[1];
    b[8] = 0x80 | (random[2] & 0x3f);
    b[9..].copy_from_slice(&random[3..10]);
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

/// `ms` since the epoch as RFC 3339 in UTC, to the millisecond: `2026-11-27T01:02:03.004Z`.
fn rfc3339_ms(ms: i64) -> String {
    let (secs, milli) = (ms.div_euclid(1000), ms.rem_euclid(1000));
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Days to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{milli:03}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

/// The client a run's events are posted to `to` with. No redirect is followed: it could take the
/// events where the URL rule would not. A proxy the environment names applies to the network,
/// never to this machine.
fn client(to: &Destination) -> std::result::Result<reqwest::blocking::Client, String> {
    let mut builder = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("lakeleto/", env!("CARGO_PKG_VERSION")));
    if to.loopback {
        builder = builder.no_proxy();
    }
    builder.build().map_err(|e| e.to_string())
}

/// POST one event to `to`, with the key from [`API_KEY_ENV`] when it is set.
fn post(
    client: &reqwest::blocking::Client,
    to: &Destination,
    body: Vec<u8>,
) -> std::result::Result<(), String> {
    let key = std::env::var(API_KEY_ENV).ok().filter(|k| !k.is_empty());
    let mut request = client
        .post(&to.url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }
    match request.send() {
        Ok(response) if response.status().is_success() => Ok(()),
        Ok(response) if response.status().is_redirection() => Err(format!(
            "HTTP {}: a redirect is not followed",
            response.status().as_u16()
        )),
        Ok(response) => Err(format!("HTTP {}", response.status().as_u16())),
        Err(e) => Err(e.without_url().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A local file is named as Covenant names it, so a check by either joins the other's runs.
    #[cfg(feature = "contracts")]
    #[test]
    fn a_local_file_is_named_as_covenant_names_it() {
        for path in [
            "orders.csv",
            "./data/../orders.csv",
            "data/2026/orders.parquet",
            "/srv/lake/orders/",
        ] {
            let ours = local(Path::new(path)).unwrap();
            let theirs = covenant::lineage::dataset(path).unwrap();
            assert_eq!((ours.namespace, ours.name), theirs, "{path}");
        }
    }

    #[cfg(feature = "catalog")]
    #[test]
    fn a_catalog_is_named_by_its_uri_without_what_could_be_a_credential() {
        assert_eq!(
            bare_uri("https://catalog.example.com"),
            "https://catalog.example.com",
            "as configured"
        );
        assert_eq!(
            bare_uri("https://ana:s3cret@catalog.example.com/api/catalog?token=t0k#x"),
            "https://catalog.example.com/api/catalog"
        );
    }

    #[test]
    fn an_endpoint_is_shown_without_its_query() {
        let to =
            Destination::parse("https://lineage.example.com/api/v1/lineage?api_key=t0k#f").unwrap();
        assert_eq!(to.shown(), "https://lineage.example.com/api/v1/lineage");
    }

    #[test]
    fn https_goes_anywhere_and_plain_http_only_to_this_machine() {
        for ok in [
            "https://lineage.example.com/api/v1/lineage",
            "HTTPS://lineage.example.com:8443/api/v1/lineage?x=1",
            "http://localhost:5000/api/v1/lineage",
            "http://127.0.0.1/api/v1/lineage",
            "http://[::1]:5000/api/v1/lineage",
        ] {
            assert!(Destination::parse(ok).is_ok(), "{ok}");
        }
        for (bad, why) in [
            ("http://marquez:5000/api/v1/lineage", "plain http only"),
            ("https://user:secret@lineage.example.com/", "credentials"),
            ("ftp://lineage.example.com/", "not an http(s) URL"),
            ("lineage.example.com/api", "not an http(s) URL"),
            ("https:///api", "no host"),
        ] {
            let err = Destination::parse(bad).unwrap_err();
            assert!(err.contains(why), "{bad}: {err}");
            assert!(!err.contains("secret"), "{err}");
        }
        assert!(Destination::parse("http://localhost/x").unwrap().loopback);
        assert!(!Destination::parse("https://example.com").unwrap().loopback);
    }

    #[test]
    fn a_dataset_is_named_as_adr_0002_names_it() {
        let ctx = crate::RequestContext::detached();
        let named = |r: &str| {
            let d = dataset(&ctx, r, false).unwrap();
            (d.namespace, d.name)
        };
        assert_eq!(
            named("s3://lake/warehouse/orders/"),
            ("s3://lake".into(), "warehouse/orders".into())
        );
        assert_eq!(
            named("gs://lake/orders.parquet"),
            ("gs://lake".into(), "orders.parquet".into())
        );
        assert_eq!(
            named("abfss://c@acct.dfs.core.windows.net/raw/orders"),
            (
                "abfss://c@acct.dfs.core.windows.net".into(),
                "raw/orders".into()
            )
        );
        // One store's other spellings are its one namespace; an ADLS container is named at its
        // account, which `az://` leaves to the environment.
        let named_in = |uri: &str, account| object_in(uri, account).map(|d| (d.namespace, d.name));
        assert_eq!(
            named_in("s3a://lake/orders", None),
            Ok(("s3://lake".into(), "orders".into()))
        );
        assert_eq!(
            named_in("gcs://lake/orders", None),
            Ok(("gs://lake".into(), "orders".into()))
        );
        // No user, password, query or fragment reaches a name, or a message.
        for uri in [
            "s3://AKIAEXAMPLE:s3cret@lake/orders?versionId=v1&X-Amz-Signature=sig",
            "s3://lake/orders#s3cret",
        ] {
            assert_eq!(
                named_in(uri, None),
                Ok(("s3://lake".into(), "orders".into())),
                "{uri}"
            );
        }
        let pw = named_in("abfss://c:s3cret@acct.dfs.core.windows.net/raw", None).unwrap_err();
        assert!(!pw.contains("s3cret"), "{pw}");
        let none = named_in("s3://u:s3cret@lake/?sig=s3cret", None).unwrap_err();
        assert!(!none.contains("s3cret"), "{none}");
        for az in [
            "az://c/raw/orders",
            "azure://c/raw/orders",
            "abfss://c/raw/orders",
        ] {
            assert_eq!(
                named_in(az, Some("acct")),
                Ok((
                    "abfss://c@acct.dfs.core.windows.net".into(),
                    "raw/orders".into()
                )),
                "{az}"
            );
            let unnamed = named_in(az, None).unwrap_err();
            assert!(unnamed.contains("AZURE_STORAGE_ACCOUNT_NAME"), "{unnamed}");
        }
        let (ns, name) = named("data/orders.csv");
        assert_eq!(ns, "file");
        assert!(Path::new(&name).is_absolute(), "{name}");
        assert!(name.ends_with("orders.csv"), "{name}");
        assert!(dataset(&ctx, "s3://lake", false).is_err());
    }

    #[test]
    fn a_database_table_is_named_by_the_conventions_and_never_with_its_credentials() {
        let ctx = crate::RequestContext::detached();
        let named = |r: &str| dataset(&ctx, r, false).map(|d| (d.namespace, d.name));
        assert_eq!(
            named("postgres://alice:hunter2@db.internal/shop?sslmode=require&table=sales.orders"),
            Ok((
                "postgres://db.internal:5432".into(),
                "shop.sales.orders".into()
            ))
        );
        assert_eq!(
            named("mysql://bob:pw@[::1]:3307/shop?table=orders"),
            Ok(("mysql://[::1]:3307".into(), "shop.orders".into()))
        );
        let unqualified = named("postgres://alice:hunter2@db/shop?table=orders").unwrap_err();
        assert!(unqualified.contains("search_path"), "{unqualified}");
        assert!(!unqualified.contains("hunter2"), "{unqualified}");
        let sqlite = named("sqlite:///tmp/app.db?table=orders").unwrap_err();
        assert!(sqlite.contains("naming conventions"), "{sqlite}");
    }

    #[test]
    fn a_path_a_server_resolves_is_named_only_when_whole() {
        let ctx = crate::RequestContext::detached();
        let d = dataset(&ctx, "/srv/data/orders.csv", true).unwrap();
        assert_eq!(
            (d.namespace.as_str(), d.name.as_str()),
            ("file", "/srv/data/orders.csv")
        );
        assert!(dataset(&ctx, "data/orders.csv", true)
            .unwrap_err()
            .contains("server resolves"));
    }

    #[test]
    fn a_run_id_is_a_uuid_v7_that_starts_with_its_time() {
        let id = run_id(1_790_812_800_123);
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "7", "{id}");
        assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"), "{id}");
        assert!(id.starts_with("01a0f4c2-c47b"), "{id}");
        assert_ne!(run_id(1), run_id(1), "random bits");
    }

    #[test]
    fn times_are_rfc_3339_in_utc_to_the_millisecond() {
        assert_eq!(rfc3339_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339_ms(1_790_812_800_123), "2026-10-01T00:00:00.123Z");
        assert_eq!(rfc3339_ms(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(rfc3339_ms(-1), "1969-12-31T23:59:59.999Z");
    }

    #[test]
    fn a_job_is_named_by_its_command_and_what_it_was_about() {
        let input = |ns: &str, name: &str| Input {
            dataset: Dataset::new(ns, name),
            facets: None,
        };
        let of = |command| Job {
            command,
            named_for: None,
        };
        let orders = input("file", "/d/orders.csv");
        assert_eq!(
            job_name(&of("check"), &[&orders]),
            "check file:/d/orders.csv"
        );
        let (a, b) = (input("s3://lake", "a"), input("s3://lake", "b"));
        assert_eq!(
            job_name(&of("diff"), &[&a, &b]),
            "diff s3://lake/a s3://lake/b"
        );
        let out = Dataset::new("file", "/d/out.parquet");
        let export = Job {
            command: "export",
            named_for: Some(&out),
        };
        assert_eq!(job_name(&export, &[&orders]), "export file:/d/out.parquet");
        assert_eq!(job_name(&of("check"), &[]), "check");
    }

    #[test]
    fn a_dataset_read_twice_is_one_input() {
        let to = Destination::parse("http://127.0.0.1:9/x").unwrap();
        let run = Run::start(to);
        let t = Input {
            dataset: Dataset::new("file", "/d/t"),
            facets: None,
        };
        let job = Job {
            command: "diff",
            named_for: None,
        };
        let [start, end] = run.events(&job, &[t.clone(), t], &[], Outcome::Complete);
        for event in [&start, &end] {
            assert_eq!(event["inputs"].as_array().unwrap().len(), 1, "{event}");
            assert_eq!(event["job"]["name"], "diff file:/d/t");
        }
    }
}
