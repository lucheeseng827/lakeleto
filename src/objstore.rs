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
//! just like the local engine's. JSON and CSV are **streamed**: a request per read, its bytes
//! decoded as they arrive and the transfer stopped with the read, so they too read as a local file
//! does.
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

use std::io::{BufRead, Read};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::{Buf, Bytes};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredentialProvider};
use object_store::azure::{AzureConfigKey, AzureCredentialProvider, MicrosoftAzureBuilder};
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::path::Path as ObjPath;
// `ObjectStore` is the core trait (and the trait object type + `list_with_delimiter`);
// `ObjectStoreExt` provides the ergonomic `get` / `head` / `put` convenience methods.
use object_store::{
    GetOptions, GetRange, ListResult, ObjectMeta, ObjectStore, ObjectStoreExt, ObjectStoreScheme,
};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use url::Url;

use crate::error::{EngineError, Result};
use crate::format::RemoteObject;
use crate::source::{format_from_name, DirEntry, DirListing};

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
        // Scope the identity to the assumed role, not to the process. Two tenants reading one URI
        // under two roles must not share what a read learned about an object; two reads under the
        // SAME role legitimately may, which is what makes this a role ARN rather than a fresh
        // private id.
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
    /// to separate cache entries, such as where a JSON object's records lie, so what one identity
    /// read is never handed to another.
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
    /// It is never derived from the credentials themselves: it keys caches and is printed by
    /// `Debug`, and nothing that does either should be a function of a secret.
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
/// local onlooker nor repeated by the next run — the two properties a private scope needs. It is
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

/// [`looks_like_iceberg`] with an explicit store configuration. Any failure is `false`.
pub fn looks_like_iceberg_with(uri: &str, opts: &StoreOptions) -> bool {
    looks_like_iceberg_as(uri, opts).unwrap_or(false)
}

/// [`looks_like_iceberg_with`], separating "this is not an Iceberg table" from "this identity
/// cannot address this URI at all".
///
/// The distinction is the difference between a fact about the *data* and a fact about the
/// *caller*. A `list` that returns nothing, or is refused, or times out, leaves the format
/// genuinely unknown, and `Ok(false)` — "nothing here says Iceberg" — is the honest answer; the
/// caller then asks for an explicit format. But an identity for the wrong family (an S3 provider
/// handed a `gs://` URI) or a URL that does not parse is not an answer about the table at all. It
/// is the caller having handed over something unusable, and reporting it as "not an Iceberg table"
/// would attribute a credential mistake to the data — the same class of lie
/// [`build_with_credentials`] refuses to tell when it declines to fall back to ambient keys.
pub fn looks_like_iceberg_as(uri: &str, opts: &StoreOptions) -> Result<bool> {
    // `Source::detect` runs on the serve async thread; calling `block_on` there would panic
    // ("Cannot start a runtime from within a runtime"). Run the probe on a scratch OS thread, which
    // is not a Tokio worker, so `block_on` on our own runtime is legal.
    let uri_owned = uri.to_string();
    let opts = opts.clone();
    std::thread::spawn(move || {
        let (store, prefix) = store_for_with(&uri_owned, &opts)?;
        let meta = ObjPath::from(format!(
            "{}/metadata",
            prefix.as_ref().trim_end_matches('/')
        ));
        Ok(runtime().block_on(async {
            let mut listing = store.list(Some(&meta));
            matches!(listing.next().await, Some(Ok(_)))
        }))
    })
    .join()
    .unwrap_or_else(|_| {
        Err(EngineError::Other(format!(
            "the Iceberg probe for `{uri}` panicked"
        )))
    })
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

/// Drive `future` to completion on the object-store runtime, from a thread that is not running one
/// (an engine call on a `spawn_blocking` thread, or the CLI's).
#[cfg(feature = "iceberg")]
pub(crate) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    runtime().block_on(future)
}

/// The stores one read reaches, under one identity: built once per bucket, and kept only for the
/// read.
///
/// An Iceberg table is many objects (its metadata, manifest lists, manifests, and data and delete
/// files), and a store built per object would open a client per object, so no request would reuse
/// another's connection. A store per bucket, for the length of one read, keeps the connections
/// pooled without keeping a store, or the credential it was built with, past the read. A
/// process-wide cache would have to be keyed by identity, and an identity that declares no scope
/// gets a fresh one per `StoreOptions` value, so such a cache would only grow.
#[cfg(feature = "iceberg")]
pub(crate) struct Stores {
    opts: StoreOptions,
    built: std::sync::Mutex<std::collections::HashMap<String, Arc<dyn ObjectStore>>>,
}

#[cfg(feature = "iceberg")]
impl Stores {
    /// Stores that read as `opts`.
    pub(crate) fn new(opts: &StoreOptions) -> Stores {
        Stores {
            opts: opts.clone(),
            built: Default::default(),
        }
    }

    /// The store serving `uri`, and the object's path within it.
    fn locate(&self, uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjPath)> {
        let url = parse_uri(uri)?;
        // The scheme and authority name a bucket (or an Azure account's container) whatever the
        // backend; the store built for one serves every key in it.
        let bucket = url[..url::Position::BeforePath].to_string();
        let mut built = self.built.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(store) = built.get(&bucket) {
            let (_, path) = ObjectStoreScheme::parse(&url)
                .map_err(|e| EngineError::Other(format!("object-store `{uri}`: {e}")))?;
            return Ok((store.clone(), path));
        }
        let (store, path) = store_for_with(uri, &self.opts)?;
        built.insert(bucket, store.clone());
        Ok((store, path))
    }

    /// The whole of the object at `uri`.
    pub(crate) fn get(&self, uri: &str) -> Result<Bytes> {
        self.fetch(uri)?
            .map_err(|e| EngineError::Other(format!("object-store get {uri}: {e}")))
    }

    /// The whole of the object at `uri`, or `None` when there is no object there.
    pub(crate) fn find(&self, uri: &str) -> Result<Option<Bytes>> {
        match self.fetch(uri)? {
            Ok(bytes) => Ok(Some(bytes)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(EngineError::Other(format!("object-store get {uri}: {e}"))),
        }
    }

    /// One `GET` of the object at `uri`: an error locating its store is the outer one, and the
    /// store's own answer, a missing object included, the inner.
    fn fetch(&self, uri: &str) -> Result<std::result::Result<Bytes, object_store::Error>> {
        let (store, path) = self.locate(uri)?;
        Ok(runtime().block_on(async move { store.get(&path).await?.bytes().await }))
    }

    /// The names of the objects directly under prefix `uri`, as a directory lists its files.
    pub(crate) fn names(&self, uri: &str) -> Result<Vec<String>> {
        let (store, prefix) = self.locate(uri)?;
        let listing = runtime()
            .block_on(async move { store.list_with_delimiter(Some(&prefix)).await })
            .map_err(|e| EngineError::Other(format!("object-store list {uri}: {e}")))?;
        Ok(listing
            .objects
            .into_iter()
            .filter_map(|object| object.location.filename().map(str::to_string))
            .collect())
    }

    /// A Parquet reader for the object at `uri`, which reads by ranged requests: the footer now,
    /// then only the row groups and columns a read asks for.
    ///
    /// `size`, when the caller knows it (an Iceberg manifest records every data file's), saves the
    /// `HEAD` that would otherwise find it. A reader is never left to find the size with a suffix
    /// request, which some stores refuse.
    pub(crate) fn parquet(
        &self,
        uri: &str,
        size: Option<u64>,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>> {
        let (store, path) = self.locate(uri)?;
        runtime().block_on(async move {
            let reader = match size {
                Some(size) => ParquetObjectReader::new(store, path).with_file_size(size),
                None => object_reader(store, &path).await?,
            };
            ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(EngineError::parquet)
        })
    }
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

/// One object, as of the version a `HEAD` found: what a reader that streams objects reads
/// ([`RemoteObject`]), a request per pass, each one pinned to that version.
pub(crate) struct StoreObject {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    uri: String,
    /// [`StoreOptions::scope_id`] of the options it was looked up with.
    identity: Arc<str>,
    meta: ObjectMeta,
    version: String,
}

/// The object and its version, and not the store: a store's `Debug` can print the credentials it
/// signs with (see [`StoreCredentials`]'s), and a plan's debug output names its tables by this.
impl std::fmt::Debug for StoreObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreObject")
            .field("uri", &self.uri)
            .field("identity", &self.identity)
            .field("size", &self.meta.size)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// The object at `uri`, looked up under `opts`: one `HEAD`, for its size and version. Nothing of
/// its body is read until a reader opens it.
pub(crate) fn object_with(uri: &str, opts: &StoreOptions) -> Result<StoreObject> {
    let (store, path) = store_for_with(uri, opts)?;
    let meta = runtime()
        .block_on(store.head(&path))
        .map_err(|e| EngineError::Other(format!("head {uri}: {e}")))?;
    // The ETag where the store gives one, which every request then asks for by name, and
    // otherwise the modification time, which each request is held to instead.
    let version = meta
        .e_tag
        .clone()
        .unwrap_or_else(|| meta.last_modified.to_rfc3339());
    Ok(StoreObject {
        store,
        path,
        uri: uri.to_string(),
        identity: Arc::from(opts.scope_id()),
        meta,
        version,
    })
}

impl StoreObject {
    /// A request for this version's bytes, or those in `range`: it fails, rather than answer with
    /// another version's, if the object was replaced since it was looked up.
    fn request(&self, range: Option<Range<u64>>) -> Result<object_store::GetResult> {
        let options = GetOptions {
            range: range.map(GetRange::Bounded),
            if_match: self.meta.e_tag.clone(),
            if_unmodified_since: self.meta.e_tag.is_none().then_some(self.meta.last_modified),
            ..Default::default()
        };
        runtime()
            .block_on(self.store.get_opts(&self.path, options))
            .map_err(|e| self.failed(e))
    }

    /// `e` from a request for this object's bytes, in words a reader can act on when it means the
    /// object was replaced since it was looked up.
    fn failed(&self, e: object_store::Error) -> EngineError {
        match e {
            object_store::Error::Precondition { .. } => EngineError::Query(format!(
                "`{}` changed while it was being read; reading it again reads the new version",
                self.uri
            )),
            e => EngineError::Other(format!("get {}: {e}", self.uri)),
        }
    }
}

impl RemoteObject for StoreObject {
    fn uri(&self) -> &str {
        &self.uri
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn size(&self) -> u64 {
        self.meta.size
    }

    fn version(&self) -> &str {
        &self.version
    }

    fn open(&self, range: Option<Range<u64>>) -> Result<Box<dyn BufRead + Send + '_>> {
        let got = self.request(range)?;
        Ok(Box::new(Body {
            object: self,
            at: got.range.start,
            end: got.range.end,
            chunks: got.into_stream(),
            chunk: Bytes::new(),
            arrived: false,
        }))
    }
}

/// An object's bytes as they arrive: each chunk handed on as the store sends it, and the next one
/// awaited only once the last is used up, so a reader that stops early stops the transfer with it.
///
/// A request lasts as long as its reader takes — a pass decoding a large object, or one held up by
/// a slow consumer of its rows — but the store's client times a request out (`object_store`: after
/// 30 seconds) and resumes it from where it got to only for so long (3 minutes, 10 times). So a
/// request that fails once bytes have arrived is made again for what is left, of the same version;
/// one that fails before any arrive ends the read, since the store is not answering.
struct Body<'a> {
    object: &'a StoreObject,
    chunks: BoxStream<'static, object_store::Result<Bytes>>,
    chunk: Bytes,
    /// The offset of the next byte to arrive, and the end of those asked for.
    at: u64,
    end: u64,
    /// Whether a byte has arrived since the last request.
    arrived: bool,
}

impl Read for Body<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = {
            let available = self.fill_buf()?;
            let n = available.len().min(buf.len());
            buf[..n].copy_from_slice(&available[..n]);
            n
        };
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for Body<'_> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        while self.chunk.is_empty() {
            match runtime().block_on(self.chunks.next()) {
                Some(Ok(chunk)) => {
                    self.at += chunk.len() as u64;
                    self.arrived |= !chunk.is_empty();
                    self.chunk = chunk;
                }
                Some(Err(_)) if self.arrived && self.at < self.end => {
                    let rest = self
                        .object
                        .request(Some(self.at..self.end))
                        .map_err(std::io::Error::other)?;
                    self.chunks = rest.into_stream();
                    self.arrived = false;
                }
                Some(Err(e)) => {
                    return Err(std::io::Error::other(format!(
                        "read {}: {e}",
                        self.object.uri
                    )));
                }
                None => break,
            }
        }
        Ok(&self.chunk)
    }

    fn consume(&mut self, amt: usize) {
        self.chunk.advance(amt);
    }
}

/// Fetch a whole remote object into memory, for a reader that cannot read one as it arrives.
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
        // By the whole name, so a compressed file (`day.ndjson.zst`) is listed as what it holds.
        if let Some((fmt, _)) = format_from_name(std::path::Path::new(name)) {
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
    fn two_roles_do_not_share_a_scope() {
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
        // ...and the same role legitimately shares one, so its reads can share a cache.
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

    #[test]
    fn an_object_streams_its_bytes_or_a_range_of_them() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("o.json");
        std::fs::write(&file, b"0123456789").unwrap();
        let uri = format!("file://{}", file.display());
        let object = object_with(&uri, &StoreOptions::empty().with_scope("tenant-a")).unwrap();
        assert_eq!(object.uri(), uri);
        assert_eq!(object.size(), 10);
        // What a reader learns of it is kept under this, so another identity never reads it.
        assert_eq!(object.identity(), "tenant-a");

        let read = |range| {
            let mut text = String::new();
            object
                .open(range)
                .unwrap()
                .read_to_string(&mut text)
                .unwrap();
            text
        };
        assert_eq!(read(None), "0123456789");
        assert_eq!(read(Some(2..5)), "234");
    }

    #[test]
    fn an_object_is_read_as_the_version_it_was_looked_up_at() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("o.ndjson");
        std::fs::write(&file, b"{\"v\": 1}\n").unwrap();
        let uri = format!("file://{}", file.display());
        let object = object_with(&uri, &StoreOptions::empty()).unwrap();

        // Replaced since: what was learned of the old version — its schema, where its records
        // lie — would misread the new one, so a request for the old one fails instead.
        std::fs::write(&file, b"{\"v\": \"two\"}\n").unwrap();
        let err = object
            .open(None)
            .err()
            .expect("the version looked up is gone");
        assert!(
            err.to_string().contains("changed while it was being read"),
            "{err}"
        );

        // Looked up again, it is the new version, and reads.
        let again = object_with(&uri, &StoreOptions::empty()).unwrap();
        assert_ne!(again.version(), object.version());
        let mut text = String::new();
        again.open(None).unwrap().read_to_string(&mut text).unwrap();
        assert_eq!(text, "{\"v\": \"two\"}\n");
    }

    #[test]
    fn an_object_without_an_etag_is_held_to_its_modification_time() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("o.ndjson");
        std::fs::write(&file, b"{\"v\": 1}\n").unwrap();
        let mut object = object_with(
            &format!("file://{}", file.display()),
            &StoreOptions::empty(),
        )
        .unwrap();
        // As a store that names no versions describes it: no ETag to ask for by name.
        object.meta.e_tag = None;
        let mut text = String::new();
        object
            .open(None)
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        assert_eq!(text, "{\"v\": 1}\n");

        // Replaced since, and so modified since: the request is refused rather than answered.
        std::fs::write(&file, b"{\"v\": 2}\n").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(60))
            .unwrap();
        let err = object
            .open(None)
            .err()
            .expect("modified since it was looked up");
        assert!(
            err.to_string().contains("changed while it was being read"),
            "{err}"
        );
    }

    #[test]
    fn a_request_the_store_gave_up_on_is_made_again_for_what_is_left() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("o.json");
        std::fs::write(&file, b"0123456789").unwrap();
        let object = object_with(
            &format!("file://{}", file.display()),
            &StoreOptions::empty(),
        )
        .unwrap();
        // A response as the store's client ends one it timed out: what arrived, then an error.
        let body = |arrived: &'static [u8]| {
            let ended = object_store::Error::Generic {
                store: "test",
                source: "request timed out".into(),
            };
            let chunks = [Ok(Bytes::from_static(arrived)), Err(ended)];
            Body {
                object: &object,
                chunks: futures::stream::iter(chunks).boxed(),
                chunk: Bytes::new(),
                at: 0,
                end: 10,
                arrived: false,
            }
        };

        // Three bytes arrived, so the rest is asked for: from the fourth, not from the start.
        let mut text = String::new();
        body(b"012").read_to_string(&mut text).unwrap();
        assert_eq!(text, "0123456789");

        // None arrived: the store is not answering, and asking again would not change that.
        let err = body(b"").read_to_string(&mut String::new()).unwrap_err();
        assert!(err.to_string().contains("request timed out"), "{err}");

        // What is asked for again is the version the read began with, or nothing.
        std::fs::write(&file, b"0123456789 and more").unwrap();
        let err = body(b"012").read_to_string(&mut String::new()).unwrap_err();
        assert!(
            err.to_string().contains("changed while it was being read"),
            "{err}"
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
