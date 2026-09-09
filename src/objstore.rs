//! BYO-credential object-store reads — the "sits next to your cloud data" piece.
//!
//! Feature-gated (`--features object-store`). Opens tables that live in an object store —
//! `s3://` (and `s3a://`), `gs://`, `az://`/`azure://`/`abfs[s]://`/`adl://` — using **the
//! user's own credentials from the process environment** and **zero hosted compute**. Nothing
//! is uploaded and no Lakeleto server is involved: the bytes flow straight from the customer's
//! bucket to the customer's machine, read with the customer's keys.
//!
//! Parquet is read with **ranged requests** through [`ParquetObjectReader`] — only the footer
//! plus the row groups a window touches are fetched — so remote reads stay larger-than-memory
//! just like the local engine's. CSV, being line-oriented, is fetched whole.
//!
//! # Where credentials come from — the store seam
//!
//! Every read here resolves its store through a [`StoreOptions`], and **the caller decides what
//! goes in one**. That is a deliberate widening: this module used to have exactly one credential
//! path, `std::env::vars()`, baked into a private `store_for`. A single-user CLI is fine with
//! that; anything that reads on behalf of more than one principal is not, because "the process
//! environment" is one ambient identity for every read the process makes, and no caller could
//! supply a different one even if it had one.
//!
//! The shape is deliberately cloud-neutral. [`StoreOptions`] carries key/value configuration —
//! which is exactly what [`object_store::parse_url_opts`] consumes, so `s3://`, `gs://` and
//! `az://` are all served by the same struct with no backend privileged — plus an optional
//! pre-built [`StoreCredentials`] provider for the case where the caller mints short-lived
//! credentials itself. [`StoreOptions::from_env()`] reproduces the historical behaviour exactly
//! (every `std::env::var` is offered to the backend, which keeps the keys it knows —
//! `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` / `AWS_ENDPOINT` /
//! `AWS_SESSION_TOKEN`, `GOOGLE_APPLICATION_CREDENTIALS` / `GOOGLE_SERVICE_ACCOUNT`,
//! `AZURE_STORAGE_ACCOUNT_NAME` / `AZURE_STORAGE_ACCOUNT_KEY`, … and ignores the rest; no config
//! file is read implicitly), and it is what the signature-preserving wrappers pass, so the local
//! CLI is unchanged.
//!
//! Each public read has a `_with` variant taking `&StoreOptions`; the historical signature is a
//! thin wrapper over it.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredentialProvider};
use object_store::azure::{AzureConfigKey, AzureCredentialProvider, MicrosoftAzureBuilder};
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::path::Path as ObjPath;
// `ObjectStore` is the core trait (and the trait object type + `list_with_delimiter`);
// `ObjectStoreExt` provides the ergonomic `get` / `head` / `put` convenience methods.
use object_store::{ListResult, ObjectStore, ObjectStoreExt, ObjectStoreScheme};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use url::Url;

use crate::error::{EngineError, Result};
use crate::source::{format_from_extension, DirEntry, DirListing};

/// A shared multi-thread Tokio runtime for the (blocking) object-store calls. Lakeleto's
/// [`Engine`](crate::engine::Engine) API is synchronous, so remote I/O is driven under
/// `block_on`; the `serve` HTTP layer already runs engine calls on `spawn_blocking` threads,
/// so blocking here never stalls the async executor.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build tokio runtime for object-store reads")
    })
}

// ---------------------------------------------------------------------------------------------
// The credential seam
// ---------------------------------------------------------------------------------------------

/// A pre-built credential provider for one store family.
///
/// `object_store` gives each backend its own provider type — `AwsCredentialProvider`,
/// `GcpCredentialProvider` and `AzureCredentialProvider` are three *unrelated*
/// `Arc<dyn CredentialProvider<Credential = …>>` aliases over three different credential structs
/// — so a single "provider" that serves all three has to be a sum. No backend is privileged: the
/// caller supplies whichever variant matches the URI's scheme, and a mismatch is an error rather
/// than a silent fall back to ambient credentials, which is the failure a credential seam exists
/// to prevent.
#[derive(Clone)]
pub enum StoreCredentials {
    /// S3-family stores: `s3://`, `s3a://`, and S3-compatible endpoints.
    S3(AwsCredentialProvider),
    /// Google Cloud Storage: `gs://`.
    Gcs(GcpCredentialProvider),
    /// Azure Blob / ADLS: `az://`, `azure://`, `abfs[s]://`, `adl://`.
    Azure(AzureCredentialProvider),
}

impl StoreCredentials {
    /// The store family this provider signs for. Used in error messages, and safe to log.
    pub fn family(&self) -> &'static str {
        match self {
            StoreCredentials::S3 { .. } => "S3",
            StoreCredentials::Gcs { .. } => "GCS",
            StoreCredentials::Azure { .. } => "Azure",
        }
    }
}

/// Prints the family and nothing else, on purpose.
///
/// `object_store`'s `CredentialProvider` requires `Debug`, and its stock
/// `StaticCredentialProvider<AwsCredential>` *derives* it — so a derived `Debug` here would put a
/// live secret key into any log line that happens to format a [`StoreOptions`]. Redacting at the
/// type is the only version of this that stays correct as call sites are added.
impl std::fmt::Debug for StoreCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StoreCredentials::{}(<redacted>)", self.family())
    }
}

/// Scope id for options that carry no explicit credential material and inherit the process
/// environment. Every such caller reads as the *same* ambient identity, so they may legitimately
/// share a cache entry — which is exactly the pre-existing single-identity behaviour, preserved.
const AMBIENT_ENV_SCOPE: &str = "ambient-env";

/// As [`AMBIENT_ENV_SCOPE`], for options that offer the backend nothing at all (anonymous reads of
/// a public bucket). A separate scope because the two really are different identities: one signs
/// with whatever the process was launched with, the other signs with nothing.
const AMBIENT_NONE_SCOPE: &str = "ambient-none";

/// How the caller wants a store configured: key/value config, an optional credential provider,
/// and who is asking.
///
/// This is the type that makes reads attributable. `parse_url_opts` already takes an arbitrary
/// `(key, value)` iterator, so config pairs cover every backend the crate supports without the
/// seam knowing anything about a particular cloud; the optional [`StoreCredentials`] covers what
/// strings cannot express, a provider that mints a fresh credential per request.
///
/// **`with_credentials` overrides everything.** `object_store` documents each builder's
/// `with_credentials` as "set the credential provider overriding any other options", and
/// [`StoreOptions`] is built so that property is actually reachable: when a provider is present
/// the store is constructed through the concrete builder with `with_credentials` applied *last*,
/// so ambient keys that leaked in through the config fold cannot win over the identity the caller
/// asked for. A seam that could only forward strings could never make that guarantee, which is
/// why the provider hook is here rather than deferred.
///
/// The provider's `get_credential` is `async`. Nothing in this crate awaits it: the store keeps
/// the `Arc` and drives it inside its own request future, so the synchronous `Engine` API and the
/// `block_on` calls in this module accept one unchanged.
#[derive(Clone)]
pub struct StoreOptions {
    inherit_env: bool,
    config: Vec<(String, String)>,
    credentials: Option<StoreCredentials>,
    /// The caller's declared credential identity, if it declared one.
    scope: Option<Arc<str>>,
    /// Identity of last resort for explicit-but-undeclared credential material. See
    /// [`StoreOptions::scope_id`].
    private_scope: Arc<str>,
}

/// Prints config **keys** but never values: a config pair is how a static access key or an
/// account key is passed, so the values are secrets even though the type is a plain `String`.
impl std::fmt::Debug for StoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreOptions")
            .field("inherit_env", &self.inherit_env)
            .field(
                "config_keys",
                &self.config.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("credentials", &self.credentials)
            .field("scope_id", &self.scope_id())
            .finish()
    }
}

impl Default for StoreOptions {
    /// The historical behaviour: read with the process environment.
    fn default() -> Self {
        Self::from_env()
    }
}

impl StoreOptions {
    /// Read with the process environment — what every `store_for`-era call did, and what the
    /// signature-preserving wrappers in this module pass.
    pub fn from_env() -> Self {
        Self {
            inherit_env: true,
            config: Vec::new(),
            credentials: None,
            scope: None,
            private_scope: new_private_scope(),
        }
    }

    /// Read with *nothing* ambient: no environment, no config, no provider. The starting point
    /// for a caller that intends to supply the whole configuration explicitly, and the only
    /// starting point that cannot accidentally pick up the host's credentials.
    pub fn empty() -> Self {
        Self {
            inherit_env: false,
            config: Vec::new(),
            credentials: None,
            scope: None,
            private_scope: new_private_scope(),
        }
    }

    /// Read as a role assumed via **AssumeRoleWithWebIdentity**, rather than as whoever the host
    /// is.
    ///
    /// This is the seam a multi-tenant reader needs and the reason it is worth a named
    /// constructor rather than three [`Self::with_config`] calls. `role_arn` is per-*options*, so
    /// two readers built from one process can assume two different roles and neither can reach
    /// the other's data — while `token_file` (the projected identity token) stays the same for
    /// both, because it proves *who is asking*, not *what may be read*.
    ///
    /// The work is `object_store`'s, not this crate's: given both a token file and a role ARN its
    /// `AmazonS3Builder` selects its own `WebIdentityProvider`, wraps it in the caching
    /// `TokenCredentialProvider`, and refuses plaintext to the STS endpoint. Re-implementing the
    /// STS call here would mean a second, less-tested copy of request signing and token refresh.
    ///
    /// # Why this validates its own keys
    ///
    /// `object_store`'s option fold keeps the keys a backend recognises and **silently drops the
    /// rest**. A typo in one of these would therefore not fail — it would quietly leave the role
    /// unset, and the builder would fall through to the host's ambient identity. A credential
    /// seam whose misconfiguration reads someone else's data as the wrong principal is worse than
    /// no seam, so every key is parsed here, up front, and an unparseable one is an error.
    ///
    /// `inherit_env` is off and cannot be turned on by this path: the environment is exactly what
    /// this is refusing to read as.
    pub fn aws_assume_role_with_web_identity(
        token_file: impl AsRef<str>,
        role_arn: impl AsRef<str>,
        region: impl AsRef<str>,
        session_name: impl AsRef<str>,
    ) -> Result<Self> {
        let role_arn = role_arn.as_ref();
        let pairs = [
            ("aws_web_identity_token_file", token_file.as_ref()),
            ("aws_role_arn", role_arn),
            ("aws_region", region.as_ref()),
            ("aws_role_session_name", session_name.as_ref()),
        ];
        for (key, value) in pairs {
            if key.parse::<AmazonS3ConfigKey>().is_err() {
                return Err(EngineError::Other(format!(
                    "object-store: `{key}` is not a key AmazonS3Builder recognises, so it would be \
                     dropped and the read would fall back to ambient credentials"
                )));
            }
            if value.is_empty() {
                return Err(EngineError::Other(format!(
                    "object-store: `{key}` is empty — an empty role ARN or token path silently \
                     disables role assumption rather than failing the read"
                )));
            }
        }
        let mut opts = Self::empty();
        for (key, value) in pairs {
            opts = opts.with_config(key, value);
        }
        // Scope the mirror cache to the assumed role, not to the process. Two tenants reading one
        // URI under two roles must not share a materialized copy; two reads under the SAME role
        // legitimately may, which is what makes this a role ARN rather than a fresh private id.
        Ok(opts.with_scope(role_arn))
    }

    /// Fold the process environment into the configuration (or stop doing so).
    pub fn inherit_env(mut self, yes: bool) -> Self {
        self.inherit_env = yes;
        self
    }

    /// Add one configuration pair, in whatever spelling the backend accepts
    /// (`aws_region`, `google_service_account`, `azure_storage_account_name`, …).
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.push((key.into(), value.into()));
        self
    }

    /// Add many configuration pairs at once.
    pub fn with_config_pairs<I, K, V>(mut self, pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.config
            .extend(pairs.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Sign every request with `credentials`, overriding any config pair (and the environment)
    /// that would otherwise have supplied an identity. See the type-level note.
    pub fn with_credentials(mut self, credentials: StoreCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Declare *who* these options read as.
    ///
    /// The label is opaque to this crate — a tenant id, a principal id, a workspace id, whatever
    /// the caller uses to mean "a different reader". It is never sent anywhere; its only job is
    /// to separate cache entries (see [`materialize_prefix_with`]) so one identity's bytes are
    /// never handed to another. Declaring it also makes the mirror reusable across processes,
    /// which an undeclared identity deliberately is not.
    pub fn with_scope(mut self, scope: impl AsRef<str>) -> Self {
        self.scope = Some(Arc::from(scope.as_ref()));
        self
    }

    /// The stable id of the credential *identity* these options read as.
    ///
    /// Four cases, in order: a declared scope wins; otherwise explicit credential material with
    /// no declared identity gets a private, non-reusable id (see [`new_private_scope`]) because
    /// the honest answer to "who is this?" is "unknown, so share with nobody"; otherwise the
    /// ambient environment; otherwise nothing at all.
    ///
    /// It is never derived from the credentials themselves — this value reaches a filename, and
    /// a filename derived from a secret is an offline oracle for that secret.
    pub fn scope_id(&self) -> &str {
        if let Some(scope) = &self.scope {
            return scope;
        }
        if self.credentials.is_some() || !self.config.is_empty() {
            return &self.private_scope;
        }
        if self.inherit_env {
            AMBIENT_ENV_SCOPE
        } else {
            AMBIENT_NONE_SCOPE
        }
    }

    /// The credential provider, if the caller supplied one.
    pub fn credentials(&self) -> Option<&StoreCredentials> {
        self.credentials.as_ref()
    }

    /// The key/value configuration handed to the store builder, in application order: the process
    /// environment first (when inherited), then the caller's explicit pairs — later wins, so an
    /// explicit value always beats an ambient one of the same key. This is exactly the iterator
    /// [`object_store::parse_url_opts`] consumes, which is why it is also the assertable surface
    /// of this seam: a test can check what *would* be handed to a builder without a live request.
    pub fn config_pairs(&self) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = Vec::new();
        if self.inherit_env {
            pairs.extend(std::env::vars());
        }
        pairs.extend(self.config.iter().cloned());
        pairs
    }
}

/// A token unique to this process and, with overwhelming probability, across processes: an
/// OS-seeded per-process base plus a monotonic counter.
///
/// `RandomState` is seeded from the OS once per process, so the base is neither predictable by a
/// local onlooker nor repeated by the next run — the two properties the two callers need. It is
/// deliberately not a hash of anything the caller supplied.
fn unique_token() -> String {
    static BASE: OnceLock<u64> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let base = *BASE.get_or_init(|| {
        use std::hash::{BuildHasher, Hasher};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u32(std::process::id());
        h.finish()
    });
    format!("{base:016x}{:08x}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// A fresh scope id for options that carry explicit credential material but declare no identity.
///
/// Unique per `StoreOptions` *value* (a clone keeps its parent's id, because a clone is the same
/// reader) and never reused by another process. The cost is that an undeclared identity gets no
/// cache reuse at all; that is the right default, because the alternative — deriving the id from
/// the credential material — would put a function of a secret into a directory name.
fn new_private_scope() -> Arc<str> {
    Arc::from(format!("private-{}", unique_token()).as_str())
}

/// Resolve an object-store URI to a store configured by `opts` plus the object's key.
fn store_for_with(uri: &str, opts: &StoreOptions) -> Result<(Arc<dyn ObjectStore>, ObjPath)> {
    let url = parse_uri(uri)?;
    match opts.credentials() {
        None => {
            let (store, path) = object_store::parse_url_opts(&url, opts.config_pairs())
                .map_err(|e| EngineError::Other(format!("object-store `{uri}`: {e}")))?;
            Ok((Arc::from(store), path))
        }
        Some(credentials) => build_with_credentials(uri, &url, opts, credentials),
    }
}

/// Build a store whose signing identity is the caller's provider.
///
/// [`object_store::parse_url_opts`] cannot express this: it forwards only *string* options, and a
/// provider is a live object. So this walks the same road by hand — the identical
/// `<Builder>::new().with_url(url)` plus `with_config(key, value)` fold that `parse_url_opts`
/// performs internally — and then calls `with_credentials` **last**, which is what makes the
/// documented "overriding any other options" guarantee actually hold: whatever ambient keys came
/// in through the fold cannot displace the identity the caller asked for.
///
/// A provider for the wrong family is an error, never a fallback. Quietly dropping an S3 provider
/// handed to a `gs://` URI would resolve the read against the host's ambient identity instead —
/// the read would *succeed*, as the wrong principal, which is worse than failing.
fn build_with_credentials(
    uri: &str,
    url: &Url,
    opts: &StoreOptions,
    credentials: &StoreCredentials,
) -> Result<(Arc<dyn ObjectStore>, ObjPath)> {
    let (scheme, path) = ObjectStoreScheme::parse(url)
        .map_err(|e| EngineError::Other(format!("object-store `{uri}`: {e}")))?;
    let pairs = opts.config_pairs();
    let build = |e: object_store::Error| EngineError::Other(format!("object-store `{uri}`: {e}"));
    let store: Arc<dyn ObjectStore> = match (&scheme, credentials) {
        (ObjectStoreScheme::AmazonS3, StoreCredentials::S3(provider)) => {
            let mut builder = AmazonS3Builder::new().with_url(url.to_string());
            for (key, value) in pairs {
                if let Ok(key) = key.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
                    builder = builder.with_config(key, value);
                }
            }
            Arc::new(
                builder
                    .with_credentials(provider.clone())
                    .build()
                    .map_err(build)?,
            )
        }
        (ObjectStoreScheme::GoogleCloudStorage, StoreCredentials::Gcs(provider)) => {
            let mut builder = GoogleCloudStorageBuilder::new().with_url(url.to_string());
            for (key, value) in pairs {
                if let Ok(key) = key.to_ascii_lowercase().parse::<GoogleConfigKey>() {
                    builder = builder.with_config(key, value);
                }
            }
            Arc::new(
                builder
                    .with_credentials(provider.clone())
                    .build()
                    .map_err(build)?,
            )
        }
        (ObjectStoreScheme::MicrosoftAzure, StoreCredentials::Azure(provider)) => {
            let mut builder = MicrosoftAzureBuilder::new().with_url(url.to_string());
            for (key, value) in pairs {
                if let Ok(key) = key.to_ascii_lowercase().parse::<AzureConfigKey>() {
                    builder = builder.with_config(key, value);
                }
            }
            Arc::new(
                builder
                    .with_credentials(provider.clone())
                    .build()
                    .map_err(build)?,
            )
        }
        _ => {
            return Err(EngineError::Other(format!(
                "object-store `{uri}`: a {} credential provider cannot sign for this URI \
                 ({scheme:?}) — supply the provider that matches the scheme",
                credentials.family()
            )))
        }
    };
    Ok((store, path))
}

/// The [`ObjectStore`] serving `uri` under `opts` — the very store every read in this module goes
/// through, exposed so a query engine can register it on its own session (see
/// [`crate::engine::sql`]). Handing out the store rather than a second configuration path is what
/// keeps the SQL engine and this module on one identity.
pub fn store_for_url(uri: &str, opts: &StoreOptions) -> Result<Arc<dyn ObjectStore>> {
    Ok(store_for_with(uri, opts)?.0)
}

fn parse_uri(uri: &str) -> Result<Url> {
    Url::parse(uri).map_err(|e| EngineError::UnsupportedFormat {
        detail: format!("not a valid object-store URL `{uri}`: {e}"),
    })
}

/// Cheap probe: does the object-store prefix look like an Iceberg table? True when it has a
/// `metadata/` child object. Lets `Source::detect` classify an extensionless `s3://…/table` prefix
/// as Iceberg (one `list` call; only reached when the name has no data-file extension).
pub fn looks_like_iceberg(uri: &str) -> bool {
    looks_like_iceberg_with(uri, &StoreOptions::from_env())
}

/// [`looks_like_iceberg`] with an explicit store configuration.
pub fn looks_like_iceberg_with(uri: &str, opts: &StoreOptions) -> bool {
    // `Source::detect` runs on the serve async thread; calling `block_on` there would panic
    // ("Cannot start a runtime from within a runtime"). Run the probe on a scratch OS thread, which
    // is not a Tokio worker, so `block_on` on our own runtime is legal.
    let uri = uri.to_string();
    let opts = opts.clone();
    std::thread::spawn(move || {
        let Ok((store, prefix)) = store_for_with(&uri, &opts) else {
            return false;
        };
        let meta = ObjPath::from(format!(
            "{}/metadata",
            prefix.as_ref().trim_end_matches('/')
        ));
        runtime().block_on(async {
            let mut listing = store.list(Some(&meta));
            matches!(listing.next().await, Some(Ok(_)))
        })
    })
    .join()
    .unwrap_or(false)
}

/// Mirror an object-store prefix (an Iceberg table directory) to a local temp directory, **once
/// per process per reader** (memoized on credential identity *and* URI — see
/// [`materialize_prefix_with`]). Returns the local mirror root.
///
/// The Iceberg reader is filesystem-based; this shim lets it read a table that lives in a bucket:
/// download the whole prefix (metadata + Avro manifests + Parquet data), then plan against the
/// mirror with [`crate::iceberg::plan_object`], which remaps the absolute object URIs stored in the
/// metadata back to the mirror. Keys are laid out relative to the prefix (so `…/metadata/x.avro`
/// mirrors to `<dest>/metadata/x.avro`), matching how `plan_object` strips the origin URI. Trades
/// ranged reads for simplicity — appropriate for exploring a table, not a streaming path.
pub fn materialize_prefix(uri: &str) -> Result<PathBuf> {
    materialize_prefix_with(uri, &StoreOptions::from_env())
}

/// [`materialize_prefix`] with an explicit store configuration.
///
/// # Why the cache is keyed on identity, not on the URI
///
/// The memo used to be keyed on the URI string alone. With one ambient identity per process that
/// was merely a cache; the moment a process can read as more than one principal it is a
/// cross-principal read: the first caller to download `s3://acme/table` fixes the bytes every
/// later caller sees, whatever credentials *they* presented, and the second principal's request
/// is never signed at all. The per-caller credential would be configured, honoured on the miss,
/// and then bypassed on every hit — the authorization decision defeated by the fast path.
///
/// So the key is `(scope, uri)`: [`StoreOptions::scope_id`] names the identity, and an identity
/// that will not name itself gets a private scope shared with nobody. A different credential
/// context can therefore never land on another's entry, on disk or in the map.
pub fn materialize_prefix_with(uri: &str, opts: &StoreOptions) -> Result<PathBuf> {
    static CACHE: OnceLock<Mutex<HashMap<(String, String), PathBuf>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (opts.scope_id().to_string(), uri.to_string());
    if let Some(p) = cache.lock().unwrap().get(&key) {
        return Ok(p.clone());
    }
    let (store, prefix) = store_for_with(uri, opts)?;
    let dest = mirror_dir(&key.0, uri)?;
    private_dir_builder().recursive(true).create(&dest)?;
    let prefix_str = prefix.as_ref().to_string();
    runtime().block_on(async {
        let mut listing = store.list(Some(&prefix));
        let mut n = 0usize;
        while let Some(meta) = listing.next().await {
            let meta =
                meta.map_err(|e| EngineError::Other(format!("object-store list {uri}: {e}")))?;
            let key = meta.location.as_ref();
            let rel = key
                .strip_prefix(&prefix_str)
                .unwrap_or(key)
                .trim_start_matches('/');
            let out = dest.join(rel);
            if let Some(parent) = out.parent() {
                private_dir_builder().recursive(true).create(parent)?;
            }
            let bytes = store
                .get(&meta.location)
                .await
                .map_err(|e| {
                    EngineError::Other(format!("object-store get {}: {e}", meta.location))
                })?
                .bytes()
                .await
                .map_err(|e| {
                    EngineError::Other(format!("object-store read {}: {e}", meta.location))
                })?;
            std::fs::File::create(&out)?.write_all(&bytes)?;
            n += 1;
        }
        if n == 0 {
            return Err(EngineError::UnsupportedFormat {
                detail: format!("no objects under `{uri}` (empty prefix or wrong path)"),
            });
        }
        Ok::<(), EngineError>(())
    })?;
    cache.lock().unwrap().insert(key, dest.clone());
    Ok(dest)
}

/// A `DirBuilder` that creates directories readable only by the user running the process.
///
/// A mirror holds the customer's *table data*. The old layout wrote it straight into
/// `std::env::temp_dir()`, which on a shared host is world-readable (`/tmp`, mode 0777) — so any
/// local user could read another user's, or another tenant's, table. `0700` is the fix, and it is
/// applied to the mirror root, which gates the whole subtree: without execute permission on the
/// root, the mode of the files beneath it cannot be reached. On non-unix targets this is a plain
/// `DirBuilder`; Windows temp directories are already per-user.
fn private_dir_builder() -> std::fs::DirBuilder {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
}

/// The process-private root every mirror lives under.
///
/// Permissions alone would not be enough: the old directory name was a hash of the URI, so it was
/// *predictable*, and on a shared `/tmp` another local user can pre-create a predictable name as a
/// directory they own and world-writable — after which `create_dir_all` happily succeeds and the
/// table data is written into their directory. The root is therefore created with `create_dir`,
/// which fails rather than adopting an existing entry, under a name carrying a per-process OS-seeded
/// random component. We never write into a directory we did not make.
fn mirror_root() -> Result<&'static PathBuf> {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    if let Some(root) = ROOT.get() {
        return Ok(root);
    }
    let made = create_private_root()?;
    let winner = ROOT.get_or_init(|| made.clone());
    if winner != &made {
        // Another thread won the race; ours is still empty, so drop it rather than leak it.
        let _ = std::fs::remove_dir(&made);
    }
    Ok(winner)
}

fn create_private_root() -> Result<PathBuf> {
    /// A squatter can only lose this race by guessing an OS-seeded value, so a handful of
    /// attempts is already generous; the bound exists so a pathological temp dir (full, or
    /// read-only) reports an error instead of spinning forever.
    const ATTEMPTS: usize = 8;
    let base = std::env::temp_dir();
    let mut last: Option<std::io::Error> = None;
    for _ in 0..ATTEMPTS {
        let candidate = base.join(format!("lakeleto-obj-{}", unique_token()));
        match private_dir_builder().create(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => last = Some(e),
            Err(e) => return Err(EngineError::Io(e)),
        }
    }
    Err(EngineError::Other(format!(
        "object-store: could not create a private mirror directory under {} ({})",
        base.display(),
        last.map(|e| e.to_string()).unwrap_or_default()
    )))
}

/// Where the mirror of `uri`, read under credential scope `scope`, lives.
///
/// Both components are hashed, and hashed *separately* rather than over their concatenation, so
/// no `(scope, uri)` pair can be re-split into a different pair with the same name. The hash is
/// `DefaultHasher` — deterministic across processes, which is what makes a declared scope's mirror
/// reusable — and it is applied to an identity label and a URI, never to credential material.
fn mirror_dir(scope: &str, uri: &str) -> Result<PathBuf> {
    Ok(mirror_root()?.join(format!("{}-{}", digest(scope), digest(uri))))
}

fn digest(s: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(s, &mut hasher);
    format!("{:016x}", std::hash::Hasher::finish(&hasher))
}

/// Build a ranged Parquet reader, hinting the file size (from a cheap `head`) so the reader
/// uses bounded range requests instead of suffix requests some stores don't support.
async fn object_reader(store: Arc<dyn ObjectStore>, path: &ObjPath) -> Result<ParquetObjectReader> {
    let meta = store
        .head(path)
        .await
        .map_err(|e| EngineError::Other(format!("head {path}: {e}")))?;
    Ok(ParquetObjectReader::new(store, path.clone()).with_file_size(meta.size))
}

/// Schema + exact row count of a remote Parquet object (footer read only — a few KiB).
pub fn parquet_schema(uri: &str) -> Result<(SchemaRef, Option<u64>)> {
    parquet_schema_with(uri, &StoreOptions::from_env())
}

/// [`parquet_schema`] with an explicit store configuration.
pub fn parquet_schema_with(uri: &str, opts: &StoreOptions) -> Result<(SchemaRef, Option<u64>)> {
    let (store, path) = store_for_with(uri, opts)?;
    runtime().block_on(parquet_schema_async(store, path))
}

async fn parquet_schema_async(
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
) -> Result<(SchemaRef, Option<u64>)> {
    let reader = object_reader(store, &path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(EngineError::parquet)?;
    let schema = builder.schema().clone();
    let rows = builder.metadata().file_metadata().num_rows();
    Ok((schema, (rows >= 0).then_some(rows as u64)))
}

/// Read the `offset..offset+limit` row window of a remote Parquet object with ranged requests
/// (offset/limit pushed into the reader → only the touched row groups are fetched).
pub fn parquet_window(
    uri: &str,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    parquet_window_with(uri, offset, limit, batch_size, &StoreOptions::from_env())
}

/// [`parquet_window`] with an explicit store configuration.
pub fn parquet_window_with(
    uri: &str,
    offset: usize,
    limit: usize,
    batch_size: usize,
    opts: &StoreOptions,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let (store, path) = store_for_with(uri, opts)?;
    runtime().block_on(parquet_window_async(store, path, offset, limit, batch_size))
}

async fn parquet_window_async(
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    offset: usize,
    limit: usize,
    batch_size: usize,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let reader = object_reader(store, &path).await?;
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(EngineError::parquet)?;
    let schema = builder.schema().clone();
    builder = builder.with_batch_size(limit.clamp(1, batch_size));
    if offset > 0 {
        builder = builder.with_offset(offset);
    }
    builder = builder.with_limit(limit);
    let mut stream = builder.build().map_err(EngineError::parquet)?;
    let mut batches = Vec::new();
    let mut rows = 0usize;
    while let Some(b) = stream.next().await {
        let b = b.map_err(EngineError::parquet)?;
        rows += b.num_rows();
        batches.push(b);
        if rows >= limit {
            break;
        }
    }
    Ok((schema, batches))
}

/// Fetch a whole remote object into memory (used for CSV, which can't be windowed by row).
pub fn fetch_all(uri: &str) -> Result<Vec<u8>> {
    fetch_all_with(uri, &StoreOptions::from_env())
}

/// [`fetch_all`] with an explicit store configuration.
pub fn fetch_all_with(uri: &str, opts: &StoreOptions) -> Result<Vec<u8>> {
    let (store, path) = store_for_with(uri, opts)?;
    runtime().block_on(fetch_all_async(store, path))
}

async fn fetch_all_async(store: Arc<dyn ObjectStore>, path: ObjPath) -> Result<Vec<u8>> {
    let got = store
        .get(&path)
        .await
        .map_err(|e| EngineError::Other(format!("get {path}: {e}")))?;
    let bytes = got
        .bytes()
        .await
        .map_err(|e| EngineError::Other(format!("read {path}: {e}")))?;
    Ok(bytes.to_vec())
}

/// The size of a single remote object, via `HEAD` — one metadata round-trip, no body.
///
/// `GET /v1/info` reported a size for local files and nothing at all for `s3://`/`gs://`/`az://`,
/// which reads as "unknown" when the number is one cheap request away. `None` covers every reason
/// it might not be answerable — a prefix rather than an object, a store that does not report it,
/// credentials that allow `GET` but not `HEAD` — because a missing size is a cosmetic gap and
/// failing the whole `info` call over it would not be.
pub fn object_size(uri: &str) -> Option<u64> {
    object_size_with(uri, &StoreOptions::from_env())
}

/// [`object_size`] with an explicit store configuration.
pub fn object_size_with(uri: &str, opts: &StoreOptions) -> Option<u64> {
    /// One metadata round-trip should be fast or absent. Without a bound, a stalled store pins
    /// the `spawn_blocking` worker `/v1/info` called this from and hangs the whole request —
    /// for a number that is cosmetic. Timeout folds into the same `None` as every other miss.
    const HEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    let (store, path) = store_for_with(uri, opts).ok()?;
    runtime()
        .block_on(async move { tokio::time::timeout(HEAD_TIMEOUT, store.head(&path)).await })
        .ok()? // elapsed → None
        .ok() // request error → None
        .map(|meta| meta.size)
}

/// List an object-store prefix for the file browser: immediate "subdirectories" (common
/// prefixes) and readable data files, mirroring [`crate::source::list_dir`]'s local shape so
/// the same SPA browser walks buckets and local disk identically.
pub fn list_prefix(uri: &str) -> Result<DirListing> {
    list_prefix_with(uri, &StoreOptions::from_env())
}

/// [`list_prefix`] with an explicit store configuration.
pub fn list_prefix_with(uri: &str, opts: &StoreOptions) -> Result<DirListing> {
    let url = parse_uri(uri)?;
    let scheme = url.scheme().to_string();
    let bucket = url.host_str().unwrap_or_default().to_string();
    let base = format!("{scheme}://{bucket}/");
    let (store, prefix) = store_for_with(uri, opts)?;
    let listing = runtime().block_on(async move {
        store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|e| EngineError::Other(format!("list {uri}: {e}")))
    })?;
    Ok(build_listing(uri, &base, listing))
}

/// Turn an object-store `list_with_delimiter` result into a browser [`DirListing`]. `base` is
/// `scheme://bucket/`; every entry's `path` is a full URI so navigation stays in the store.
fn build_listing(dir: &str, base: &str, res: ListResult) -> DirListing {
    let mut entries = Vec::new();
    for p in res.common_prefixes {
        let key = p.as_ref();
        let name = key.rsplit('/').find(|s| !s.is_empty()).unwrap_or(key);
        entries.push(DirEntry {
            name: name.to_string(),
            path: format!("{base}{key}"),
            kind: "dir",
            format: None,
            size: None,
        });
    }
    for o in res.objects {
        let key = o.location.as_ref();
        let name = key.rsplit('/').next().unwrap_or(key);
        if name.starts_with('.') {
            continue; // hide dotfiles
        }
        if let Some(fmt) = format_from_extension(std::path::Path::new(name)) {
            entries.push(DirEntry {
                name: name.to_string(),
                path: format!("{base}{key}"),
                kind: "file",
                format: Some(fmt.as_str().to_string()),
                size: Some(o.size),
            });
        }
    }
    entries.sort_by(|a, b| {
        (a.kind == "file").cmp(&(b.kind == "file")).then_with(|| {
            a.name
                .to_ascii_lowercase()
                .cmp(&b.name.to_ascii_lowercase())
        })
    });
    DirListing {
        dir: dir.to_string(),
        parent: parent_uri(dir),
        entries,
    }
}

/// The parent prefix of an object-store URI (`s3://b/a/c` -> `s3://b/a/`), or `None` at the
/// bucket root.
fn parent_uri(dir: &str) -> Option<String> {
    let (scheme, rest) = dir.split_once("://")?;
    let rest = rest.trim_end_matches('/');
    let (bucket, key) = rest.split_once('/')?;
    if key.is_empty() {
        return None;
    }
    match key.rsplit_once('/') {
        Some((parent_key, _)) => Some(format!("{scheme}://{bucket}/{parent_key}/")),
        None => Some(format!("{scheme}://{bucket}/")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use object_store::memory::InMemory;

    /// A 5-row single-column (`id` = 0..5) Parquet file, in memory.
    fn parquet_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0i64, 1, 2, 3, 4]))],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf
    }

    fn put(store: &Arc<dyn ObjectStore>, key: &str, bytes: Vec<u8>) {
        runtime()
            .block_on(store.put(&ObjPath::from(key), bytes.into()))
            .unwrap();
    }

    #[test]
    fn reads_parquet_schema_and_window_over_ranged_requests() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&store, "data/t.parquet", parquet_bytes());
        let path = ObjPath::from("data/t.parquet");

        let (schema, count) = runtime()
            .block_on(parquet_schema_async(store.clone(), path.clone()))
            .unwrap();
        assert_eq!(count, Some(5));
        assert_eq!(schema.field(0).name(), "id");

        // Window rows 2..4 -> ids [2, 3].
        let (_s, batches) = runtime()
            .block_on(parquet_window_async(store.clone(), path, 2, 2, 8192))
            .unwrap();
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn fetch_all_returns_object_bytes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&store, "d/a.csv", b"id,name\n1,ada\n".to_vec());
        let got = runtime()
            .block_on(fetch_all_async(store, ObjPath::from("d/a.csv")))
            .unwrap();
        assert_eq!(got, b"id,name\n1,ada\n");
    }

    #[test]
    fn lists_prefix_as_dirs_and_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put(&store, "d/a.parquet", parquet_bytes());
        put(&store, "d/b.csv", b"x\n1\n".to_vec());
        put(&store, "d/notes.txt", b"hi".to_vec()); // non-data: hidden
        put(&store, "d/sub/c.parquet", parquet_bytes());

        let res = runtime()
            .block_on(store.list_with_delimiter(Some(&ObjPath::from("d"))))
            .unwrap();
        let listing = build_listing("s3://bucket/d", "s3://bucket/", res);

        // dirs first, then files (alpha); notes.txt is filtered out.
        let names: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["sub", "a.parquet", "b.csv"]);
        let sub = &listing.entries[0];
        assert_eq!(sub.kind, "dir");
        assert_eq!(sub.path, "s3://bucket/d/sub");
        let a = &listing.entries[1];
        assert_eq!(a.kind, "file");
        assert_eq!(a.path, "s3://bucket/d/a.parquet");
        assert_eq!(a.format.as_deref(), Some("parquet"));
        assert_eq!(listing.parent.as_deref(), Some("s3://bucket/"));
    }

    // --- the credential seam ---------------------------------------------------------------

    /// A provider that signs with a fixed credential. Nothing here makes a request; the point is
    /// that a *live* provider object reaches the builder, which is what a per-caller vendor needs.
    fn static_s3_provider() -> object_store::aws::AwsCredentialProvider {
        Arc::new(object_store::StaticCredentialProvider::new(
            object_store::aws::AwsCredential {
                key_id: "AKIAEXAMPLE".to_string(),
                secret_key: "TOP-SECRET-VALUE".to_string(),
                token: None,
            },
        ))
    }

    #[test]
    fn web_identity_options_carry_every_key_object_store_needs() {
        let opts = StoreOptions::aws_assume_role_with_web_identity(
            "/var/run/secrets/token",
            "arn:aws:iam::123456789012:role/tenant-acme",
            "ap-southeast-1",
            "lakeleto-acme",
        )
        .expect("valid inputs");
        let pairs = opts.config_pairs();
        let get = |k: &str| {
            pairs
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        };
        // The two that select the provider. Without BOTH, AmazonS3Builder::build falls through to
        // the next arm and reads as the host.
        assert_eq!(
            get("aws_web_identity_token_file").as_deref(),
            Some("/var/run/secrets/token")
        );
        assert_eq!(
            get("aws_role_arn").as_deref(),
            Some("arn:aws:iam::123456789012:role/tenant-acme")
        );
        assert_eq!(get("aws_region").as_deref(), Some("ap-southeast-1"));
        assert_eq!(
            get("aws_role_session_name").as_deref(),
            Some("lakeleto-acme")
        );
        // Every key must be one the builder actually keeps; an unrecognised key is dropped in
        // silence, which is the failure this whole constructor exists to make impossible.
        for (key, _) in &pairs {
            assert!(
                key.parse::<AmazonS3ConfigKey>().is_ok(),
                "`{key}` would be silently dropped by the option fold"
            );
        }
    }

    #[test]
    fn web_identity_options_refuse_the_environment() {
        // The point of assuming a role is not to read as the host. If these options inherited the
        // environment, an ambient AWS_ACCESS_KEY_ID would be folded in — and the builder prefers
        // the static-credential arm over the web-identity arm, so the role would be ignored
        // entirely and the read would succeed as the WRONG principal.
        let opts = StoreOptions::aws_assume_role_with_web_identity(
            "/t",
            "arn:aws:iam::1:role/r",
            "us-east-1",
            "s",
        )
        .unwrap();
        assert!(
            !opts.config_pairs().iter().any(|(k, _)| k == "PATH"),
            "process environment leaked into role-assumption options"
        );
        assert_eq!(opts.config_pairs().len(), 4, "only the four declared keys");
    }

    #[test]
    fn web_identity_refuses_an_empty_role_or_token() {
        // An empty value parses as a key but disables assumption, which is the fail-open case.
        for (token, role) in [("", "arn:aws:iam::1:role/r"), ("/t", "")] {
            assert!(
                StoreOptions::aws_assume_role_with_web_identity(token, role, "us-east-1", "s")
                    .is_err(),
                "empty token/role must be refused, not silently ignored"
            );
        }
    }

    #[test]
    fn two_roles_do_not_share_a_mirror_scope() {
        // The cross-tenant read this seam exists to prevent: same URI, two roles, one cache.
        let a = StoreOptions::aws_assume_role_with_web_identity(
            "/t",
            "arn:aws:iam::1:role/tenant-a",
            "us-east-1",
            "s",
        )
        .unwrap();
        let b = StoreOptions::aws_assume_role_with_web_identity(
            "/t",
            "arn:aws:iam::1:role/tenant-b",
            "us-east-1",
            "s",
        )
        .unwrap();
        assert_ne!(a.scope_id(), b.scope_id());
        // ...and the same role legitimately shares one, or every read re-downloads the table.
        let a2 = StoreOptions::aws_assume_role_with_web_identity(
            "/t",
            "arn:aws:iam::1:role/tenant-a",
            "us-east-1",
            "other-session",
        )
        .unwrap();
        assert_eq!(a.scope_id(), a2.scope_id());
    }

    #[test]
    fn ambient_options_share_a_scope_and_declared_ones_do_not() {
        // The pre-existing single-identity behaviour: two env-backed reads are the same reader,
        // so they may share a cache entry.
        assert_eq!(
            StoreOptions::from_env().scope_id(),
            StoreOptions::from_env().scope_id()
        );
        // "the environment" and "nothing at all" are different identities.
        assert_ne!(
            StoreOptions::from_env().scope_id(),
            StoreOptions::empty().scope_id()
        );
        // Declared identities are distinct, and stable across constructions + clones.
        let a = StoreOptions::empty().with_scope("tenant-a");
        let b = StoreOptions::empty().with_scope("tenant-b");
        assert_ne!(a.scope_id(), b.scope_id());
        assert_eq!(
            a.scope_id(),
            StoreOptions::empty().with_scope("tenant-a").scope_id()
        );
        assert_eq!(a.scope_id(), a.clone().scope_id());
    }

    #[test]
    fn undeclared_credential_material_gets_a_scope_shared_with_nobody() {
        // Two callers that both supply explicit credentials but neither says who they are must
        // not be treated as the same reader — the safe answer to an unknown identity.
        let one = StoreOptions::empty().with_config("aws_access_key_id", "A");
        let two = StoreOptions::empty().with_config("aws_access_key_id", "B");
        assert_ne!(one.scope_id(), two.scope_id());
        assert_ne!(one.scope_id(), StoreOptions::empty().scope_id());
        // ...and a clone is the same reader, not a new one.
        assert_eq!(one.scope_id(), one.clone().scope_id());
        // A declared scope still wins over the private fallback.
        let declared = StoreOptions::empty()
            .with_config("aws_access_key_id", "A")
            .with_scope("tenant-a");
        assert_eq!(declared.scope_id(), "tenant-a");
    }

    #[test]
    fn explicit_config_is_applied_after_the_inherited_environment() {
        // `parse_url_opts` folds the pairs in order and each `with_config` overwrites, so "last
        // wins" is what makes an explicit value beat an ambient one of the same key.
        let opts = StoreOptions::from_env().with_config("aws_region", "eu-west-1");
        let pairs = opts.config_pairs();
        assert_eq!(
            pairs.last().map(|(k, v)| (k.as_str(), v.as_str())),
            Some(("aws_region", "eu-west-1"))
        );
        assert!(
            pairs.len() > 1,
            "the environment should have been folded in first"
        );
        // `empty()` offers the backend nothing: no ambient credential can leak in this way.
        assert!(StoreOptions::empty().config_pairs().is_empty());
        assert!(StoreOptions::from_env()
            .inherit_env(false)
            .config_pairs()
            .is_empty());
    }

    #[test]
    fn debug_output_never_carries_credential_values() {
        let opts = StoreOptions::empty()
            .with_config("aws_secret_access_key", "TOP-SECRET-VALUE")
            .with_credentials(StoreCredentials::S3(static_s3_provider()))
            .with_scope("tenant-a");
        let rendered = format!("{opts:?}");
        assert!(!rendered.contains("TOP-SECRET-VALUE"), "{rendered}");
        assert!(rendered.contains("aws_secret_access_key"), "{rendered}");
        assert!(rendered.contains("tenant-a"), "{rendered}");
        let creds = format!("{:?}", StoreCredentials::S3(static_s3_provider()));
        assert!(!creds.contains("TOP-SECRET-VALUE"), "{creds}");
        assert!(creds.contains("redacted"), "{creds}");
    }

    #[test]
    fn a_credential_provider_reaches_the_store_builder() {
        // Builds the store (no request): proof the provider hook is actually wired to
        // `with_credentials`, not merely accepted and dropped.
        let opts = StoreOptions::empty()
            .with_config("aws_region", "eu-west-1")
            .with_credentials(StoreCredentials::S3(static_s3_provider()));
        assert!(store_for_url("s3://bucket/t.parquet", &opts).is_ok());
    }

    #[test]
    fn a_provider_for_the_wrong_family_is_an_error_not_a_fallback() {
        // Dropping the mismatched provider would resolve the read against the host's ambient
        // identity instead — a *successful* read as the wrong principal. Fail loudly.
        let opts =
            StoreOptions::empty().with_credentials(StoreCredentials::S3(static_s3_provider()));
        let err = store_for_url("gs://bucket/t.parquet", &opts).unwrap_err();
        assert!(err.to_string().contains("S3"), "{err}");
    }

    // --- the mirror cache ------------------------------------------------------------------

    #[test]
    fn mirror_dir_is_keyed_on_identity_as_well_as_uri() {
        let uri = "s3://acme/table";
        assert_ne!(
            mirror_dir("tenant-a", uri).unwrap(),
            mirror_dir("tenant-b", uri).unwrap()
        );
        assert_ne!(
            mirror_dir("tenant-a", uri).unwrap(),
            mirror_dir("tenant-a", "s3://acme/other").unwrap()
        );
        assert_eq!(
            mirror_dir("tenant-a", uri).unwrap(),
            mirror_dir("tenant-a", uri).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_mirror_root_is_readable_only_by_this_user() {
        use std::os::unix::fs::PermissionsExt;
        let root = mirror_root().unwrap();
        let mode = std::fs::metadata(root).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "mirror root {root:?} is not user-private");
        // It also lives *under* the shared temp dir rather than being it.
        assert!(root.starts_with(std::env::temp_dir()));
    }

    #[cfg(unix)]
    #[test]
    fn a_materialized_mirror_is_readable_only_by_this_user() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("m.txt"), b"table data").unwrap();
        let uri = format!("file://{}", dir.path().display());
        let mirror =
            materialize_prefix_with(&uri, &StoreOptions::empty().with_scope("perm-check")).unwrap();
        let mode = std::fs::metadata(&mirror).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "mirror {mirror:?} is world-readable");
    }

    #[test]
    fn materialize_prefix_never_serves_one_identity_from_another() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("data.txt");
        std::fs::write(&file, b"tenant-a bytes").unwrap();
        let uri = format!("file://{}", dir.path().display());

        let a = StoreOptions::empty().with_scope("mirror-tenant-a");
        let b = StoreOptions::empty().with_scope("mirror-tenant-b");

        let mirror_a = materialize_prefix_with(&uri, &a).unwrap();
        assert_eq!(
            std::fs::read(mirror_a.join("data.txt")).unwrap(),
            b"tenant-a bytes"
        );

        // Same URI, different credential identity. Change the source first so a stale hit is
        // detectable: under the old URI-only key, B would have been handed A's bytes without a
        // single request signed as B.
        std::fs::write(&file, b"tenant-b bytes").unwrap();
        let mirror_b = materialize_prefix_with(&uri, &b).unwrap();
        assert_ne!(mirror_a, mirror_b);
        assert_eq!(
            std::fs::read(mirror_b.join("data.txt")).unwrap(),
            b"tenant-b bytes"
        );

        // ...while the memo still memoizes *within* one identity (the point of having it).
        std::fs::write(&file, b"changed again").unwrap();
        let again = materialize_prefix_with(&uri, &a).unwrap();
        assert_eq!(again, mirror_a);
        assert_eq!(
            std::fs::read(again.join("data.txt")).unwrap(),
            b"tenant-a bytes"
        );
    }

    #[test]
    fn parent_uri_walks_up() {
        assert_eq!(parent_uri("s3://b/a/c"), Some("s3://b/a/".to_string()));
        assert_eq!(parent_uri("s3://b/a/c/"), Some("s3://b/a/".to_string()));
        assert_eq!(parent_uri("s3://b/a"), Some("s3://b/".to_string()));
        assert_eq!(parent_uri("s3://b/"), None);
        assert_eq!(parent_uri("s3://b"), None);
    }
}
