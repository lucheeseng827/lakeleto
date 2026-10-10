//! Source detection — figure out *what* a path is before an engine reads it.
//!
//! Detection order is **directory shape → extension → magic bytes**: a directory is treated as a
//! Delta table when it has a `_delta_log/` subdir, then as an Iceberg table when it has a
//! `metadata/` subdir; a directory of `.parquet` files (a multi-file dataset, with optional Hive
//! `key=value` partition subdirs) is treated as one Parquet table; otherwise a file is classified
//! by extension, falling back to a `PAR1` magic-byte sniff.
//!
//! [`list_dir`] labels directories in the same order, so the file browser and the reader never
//! disagree about what a directory is.
//!
//! Both are [`PathCatalog`]'s, behind the [`Catalog`] trait: [`Source::detect_in`] and
//! [`list_dir`] hand their work to it, so a reference that is not a location can be resolved
//! through the same seam.
//!
//! Format is decoupled from Engine on purpose: the same `Source` is handed to whichever
//! engine the user picked (`local`, `sql`, `remote`), so format sniffing lives in one place.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::catalog::{Catalog, PathCatalog, TableHandle};
use crate::context::RequestContext;
use crate::error::{EngineError, Result};

/// A table format Lakeleto knows how to talk about. Whether a given *engine* can read it is a
/// separate question (see [`crate::engine::Capabilities`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Parquet,
    Csv,
    /// Tab-separated values — read exactly like [`Format::Csv`] but with a tab delimiter. A
    /// distinct variant (rather than keying the delimiter off the file extension) so an explicit
    /// `--format tsv` / `?format=tsv` override selects tab even for a differently-named file.
    Tsv,
    Json,
    /// Arrow IPC: the file format (`.arrow`, `.feather`, `.ipc`) and the stream format (`.arrows`),
    /// told apart by their first bytes.
    Arrow,
    Iceberg,
    /// A Delta Lake table — a directory with a `_delta_log/` transaction log over Parquet data.
    Delta,
    /// A database table reached over a connection URI (`sqlite://` / `postgres://` / `mysql://`).
    /// The dialect + table live in the URI (parsed by the `database` engine), so this is just the
    /// "this source is a live DB, not a file" marker.
    Database,
    /// **Not a format — the absence of one.** The marker for a source that was deliberately NOT
    /// resolved on this machine because the engine that will read it resolves its own refs (see
    /// [`Source::unresolved`]).
    ///
    /// Never produced by [`Source::detect`] and never parsed from user input ([`Format::parse`]
    /// does not accept `"unknown"`), so it cannot be asked for: it exists so a client can say
    /// "I don't know, and it is not my job to know" on the wire — omitting `?format=` entirely —
    /// instead of guessing a format the peer would then be instructed to use. Every local engine
    /// refuses it, because a local engine that has one has been handed a source it was never
    /// meant to see.
    Unknown,
}

impl Format {
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Csv => "csv",
            Format::Tsv => "tsv",
            Format::Json => "json",
            Format::Arrow => "arrow",
            Format::Iceberg => "iceberg",
            Format::Delta => "delta",
            Format::Database => "database",
            Format::Unknown => "unknown",
        }
    }

    /// Parse a format name (case-insensitive) — used by the `--format` override and the API.
    pub fn parse(s: &str) -> Option<Format> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" | "pq" => Some(Format::Parquet),
            "csv" => Some(Format::Csv),
            "tsv" => Some(Format::Tsv),
            "json" | "ndjson" | "jsonl" | "geojson" => Some(Format::Json),
            "arrow" | "arrows" | "feather" | "ipc" => Some(Format::Arrow),
            "iceberg" => Some(Format::Iceberg),
            "delta" | "deltalake" => Some(Format::Delta),
            "database" | "db" | "sqlite" | "postgres" | "postgresql" | "mysql" => {
                Some(Format::Database)
            }
            _ => None,
        }
    }

    /// The field delimiter for the delimited-text formats: tab for [`Format::Tsv`], comma
    /// otherwise. Only meaningful for `Csv`/`Tsv`.
    pub fn delimiter(&self) -> u8 {
        match self {
            Format::Tsv => b'\t',
            _ => b',',
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// URI schemes Lakeleto treats as an object store (BYO-credential `s3://` / `gs://` / `az://`).
/// Recognised in every build — even without `--features object-store` — so a URI gets a
/// helpful "rebuild with the feature" message instead of a confusing filesystem error.
pub const REMOTE_SCHEMES: &[&str] = &[
    "s3", "s3a", "gs", "gcs", "az", "azure", "abfs", "abfss", "adl",
];

/// Does `s` look like an object-store URI we recognise (e.g. `s3://bucket/key.parquet`)?
pub fn is_object_uri(s: &str) -> bool {
    match s.split_once("://") {
        Some((scheme, rest)) if !rest.is_empty() => {
            REMOTE_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str())
        }
        _ => false,
    }
}

/// Connection-URI schemes handled by the `database` engine. Recognised in every build so a DB URI
/// gets a "rebuild with the feature" message instead of being mistaken for a file path.
pub const DATABASE_SCHEMES: &[&str] = &["sqlite", "postgres", "postgresql", "mysql"];

/// Does `s` look like a database connection URI (e.g. `sqlite:///data.db?table=orders`)?
pub fn is_database_uri(s: &str) -> bool {
    match s.split_once("://") {
        Some((scheme, _)) => DATABASE_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str()),
        _ => false,
    }
}

/// Query parameters whose value is a secret. Lower-case; matching is case-insensitive.
const SECRET_QUERY_KEYS: [&str; 4] = ["password", "pwd", "secret", "token"];

/// Replace the password in a URI-shaped string with `***`, leaving everything else verbatim.
///
/// Exists because a database source's credentials have nowhere else to live. An object-store URI
/// names a location and its credentials arrive beside it
/// ([`StoreOptions`](crate::objstore::StoreOptions)); a database URI is the connection string, so
/// `postgres://alice:hunter2@db/orders?table=t` *is* the location, and there is no form of it that
/// both connects and keeps the secret out. So the secret is in the [`Source`] — the one type this
/// module's own documentation says must never reach a response body or a cache — and the split
/// between [`Source::display`] and [`Source::uri`] is how both things stay true: one renders, one
/// connects.
///
/// The username survives. It is useful in the error it appears in ("which account was refused?"),
/// it is not the secret, and a message with no principal in it at all sends people guessing.
///
/// Parses per RFC 3986: userinfo ends at the first `@` of the authority, and the authority ends at
/// the first `/`, `?` or `#`. A password containing an unencoded one of those is malformed and
/// could not connect, so it is not a case worth being wrong in either direction about.
pub fn redact_uri_password(s: &str) -> std::borrow::Cow<'_, str> {
    // Requires a scheme, and that is a deliberate limit rather than an oversight. Without one there
    // is no way to tell `alice:hunter2@db/orders` (a connection string with the `://` forgotten)
    // from `table:sales.orders@v3` (an opaque catalog ref a remote engine resolves) or from a file
    // named `a:b@c` — and this function's output is what the *unresolved* remote ref path renders,
    // where mangling an identifier is a bug of its own. A caller that already knows its string was
    // meant to be a connection URI can redact the schemeless form safely; `engine::database`'s
    // `safe` is that caller, and the only one.
    let Some(scheme_end) = s.find("://") else {
        return std::borrow::Cow::Borrowed(s);
    };
    let rest_at = scheme_end + 3;
    let rest = &s[rest_at..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let mut out: Option<String> = None;

    // 1. `scheme://user:password@host` -> `scheme://user:***@host`
    if let Some(at) = rest[..authority_end].find('@') {
        if let Some(colon) = rest[..at].find(':') {
            let mut redacted = String::with_capacity(s.len());
            redacted.push_str(&s[..rest_at + colon + 1]);
            redacted.push_str("***");
            redacted.push_str(&s[rest_at + at..]);
            out = Some(redacted);
        }
    }

    // 2. `?password=…` and friends, wherever they sit in the query.
    let current = out.as_deref().unwrap_or(s);
    if let Some(q) = current.find('?') {
        let (head, query) = current.split_at(q + 1);
        let mut rebuilt = String::with_capacity(current.len());
        rebuilt.push_str(head);
        for (i, pair) in query.split('&').enumerate() {
            if i > 0 {
                rebuilt.push('&');
            }
            match pair.split_once('=') {
                Some((k, _)) if SECRET_QUERY_KEYS.contains(&k.to_ascii_lowercase().as_str()) => {
                    rebuilt.push_str(k);
                    rebuilt.push_str("=***");
                }
                _ => rebuilt.push_str(pair),
            }
        }
        if rebuilt != current {
            out = Some(rebuilt);
        }
    }

    match out {
        Some(redacted) => std::borrow::Cow::Owned(redacted),
        None => std::borrow::Cow::Borrowed(s),
    }
}

/// A resolved data source: a path plus the format Lakeleto detected for it.
///
/// **Deliberately no credential slot.** Reads of an object-store URI are configured by
/// [`crate::objstore::StoreOptions`], which travels *alongside* a `Source`, not on it. `Source`
/// answers "what is this and where does it live" — it is cheap, `Clone`, cached, embedded in
/// `TableSchema`/`DirEntry` and rendered into API responses, and it is the same value whoever is
/// asking. A credential context is the opposite on every count: it answers "who is asking", it is
/// per-request, it must never be cloned into a cache or a response body, and it carries a live
/// provider that cannot be serialized. Fusing the two would put secrets on the type most likely to
/// be logged and would make one identity's cached `Source` reusable by another. So the credential
/// context stays a separate argument, which also keeps the default `Source` path (a local file)
/// free of any notion of credentials at all.
/// Whether [`Source::detect`] may spend a network round-trip to classify a remote prefix, and
/// whose credentials it may spend it as.
///
/// A [`PathCatalog`] carries one, and asks it the same question when it lists an object-store
/// prefix: the call's vended identity first, and under [`RemoteProbe::VendedOnly`] no listing as
/// the process. A listing is what the caller asked for, so [`RemoteProbe::Never`] allows one.
///
/// `detect` is the one identity-bearing operation in the crate that sits **outside** the
/// [`Engine`](crate::engine::Engine) seam. An extensionless `s3://…/table` gives its format away
/// only by being probed — one `list` for a `metadata/` child — and a probe is a read, so it is
/// performed as *somebody*. Every other remote read resolves identity per call from a
/// [`RequestContext`](crate::context::RequestContext); this one had no context to resolve from and
/// so read as the process, which is the right answer for exactly one caller and the wrong one for
/// the rest.
///
/// The identity still comes from the context. What this enum decides is the question a context
/// cannot answer: what the *absence* of a vended identity means. It is the caller's posture, not
/// the request's, which is why it is a separate argument rather than another context field — the
/// context carries what is true of this call, and "I am a single-user binary on my own machine" is
/// true of the program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RemoteProbe {
    /// Probe as the context's identity if it has one, else as the process environment.
    ///
    /// The offline binary's posture, and correct there: one user, their own machine, their own
    /// credentials, and no one else's data within reach.
    #[default]
    Ambient,
    /// Probe as the context's identity, and refuse to probe at all without one.
    ///
    /// A multi-tenant server's posture. Falling back to the environment here would make a
    /// tenant-supplied location into a probe performed as the *server* — a confused deputy, and an
    /// existence oracle for any bucket the server's own role can see. Refusing costs the tenant an
    /// explicit `format` on their catalog entry and costs the server nothing.
    VendedOnly,
    /// Never probe. Classification is by name or not at all.
    Never,
}

/// How far to spread struct columns into top-level ones — see [`Source::flatten`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flatten {
    /// Every level: `user: {name, geo: {lat}}` becomes `user.name` and `user.geo.lat`.
    All,
    /// The first `n` levels only: at 1 the same column becomes `user.name` and a `user.geo`
    /// struct.
    Levels(NonZeroUsize),
}

impl Flatten {
    /// Parse a `--flatten=` / `?flatten=` value. `all` (or an empty value, or `true`) is every
    /// level and a positive number is that many; `0`, `false` and `none` are no flattening at all,
    /// which is `Ok(None)` — so a caller can turn off a default without a second flag.
    pub fn parse(s: &str) -> Result<Option<Flatten>> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "all" | "true" => Ok(Some(Flatten::All)),
            "0" | "false" | "none" => Ok(None),
            n => n
                .parse::<usize>()
                .ok()
                .and_then(NonZeroUsize::new)
                .map(|n| Some(Flatten::Levels(n)))
                .ok_or_else(|| {
                    EngineError::Other(format!(
                        "flatten takes `all`, a number of levels, or `none` — got `{s}`"
                    ))
                }),
        }
    }

    /// The value [`Flatten::parse`] reads back: what a remote engine sends for it.
    pub fn as_param(&self) -> String {
        match self {
            Flatten::All => "all".to_string(),
            Flatten::Levels(n) => n.to_string(),
        }
    }

    /// Levels to descend: `None` for every one.
    pub fn max_levels(&self) -> Option<usize> {
        match self {
            Flatten::All => None,
            Flatten::Levels(n) => Some(n.get()),
        }
    }
}

/// How a file's bytes are compressed on top of its format: `t.ndjson.zst` is JSON, compressed with
/// zstd. Orthogonal to [`Format`], so every text format reads with every codec — and only a text
/// format can have one: Parquet and Arrow compress inside their own containers and are read by
/// seeking, which a compressed stream cannot do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Gzip,
    Zstd,
    Bzip2,
    Xz,
}

/// The most bytes one read decompresses a file to, unless [`set_max_decompressed`] says otherwise:
/// 4 GiB. It stops a small file that inflates without end, not a big file: a read that needs
/// fewer bytes, such as the grid's first window, stops long before it.
pub const DEFAULT_MAX_DECOMPRESSED: u64 = 4 * 1024 * 1024 * 1024;

static MAX_DECOMPRESSED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(DEFAULT_MAX_DECOMPRESSED);

/// Set the most bytes one read decompresses a file to, for this process: the CLI's
/// `--max-decompressed`. A read past it fails with [`EngineError::TooLarge`].
pub fn set_max_decompressed(bytes: u64) {
    MAX_DECOMPRESSED.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// The most bytes one read decompresses a file to — see [`set_max_decompressed`].
pub fn max_decompressed() -> u64 {
    MAX_DECOMPRESSED.load(std::sync::atomic::Ordering::Relaxed)
}

impl Codec {
    /// Whether this build decompresses it: gzip always, and zstd, bzip2 and xz with the
    /// `compression` feature.
    pub fn decodable(&self) -> bool {
        matches!(self, Codec::Gzip) || cfg!(feature = "compression")
    }

    /// The codec a file extension names (case-insensitive).
    pub fn from_extension(ext: &str) -> Option<Codec> {
        match ext.to_ascii_lowercase().as_str() {
            "gz" | "gzip" => Some(Codec::Gzip),
            "zst" | "zstd" => Some(Codec::Zstd),
            "bz2" => Some(Codec::Bzip2),
            "xz" => Some(Codec::Xz),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Codec::Gzip => "gzip",
            Codec::Zstd => "zstd",
            Codec::Bzip2 => "bzip2",
            Codec::Xz => "xz",
        }
    }
}

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct Source {
    pub path: PathBuf,
    pub format: Format,
    /// How the file is compressed, from its name's last extension (`t.csv.gz`); `None` for a
    /// file read as it is. A text reader reads a compressed file's decompressed bytes, and a build
    /// without a codec's decoder refuses the file rather than misread its compressed bytes.
    pub codec: Option<Codec>,
    /// Where inside a JSON document to read records from, as a JSON Pointer (`/data`,
    /// `/response/items`) — the caller's explicit choice, overriding the reader's own detection of
    /// a records member. `Some("")` names the whole document and so turns unwrapping off. `None`
    /// leaves it to the reader. Only JSON sources carry one; see [`Source::with_json_path`].
    pub json_path: Option<String>,
    /// Read struct columns as one top-level column per field, named by its path (`user.geo.lat`),
    /// so nested data sorts, filters and exports like any other column. `None` reads them as
    /// they are.
    ///
    /// A view of the source rather than a way of parsing it, so any format takes it: a source
    /// with no struct columns — every CSV, every database table — reads the same either way.
    /// Lists and maps stay whole, because spreading those would mean one row becoming many.
    pub flatten: Option<Flatten>,
}

impl Source {
    /// Detect the format of `path` (extension -> magic bytes -> directory shape), using the
    /// process environment for the one remote probe that needs an identity.
    ///
    /// Equivalent to [`detect_in`](Self::detect_in) with a detached context and
    /// [`RemoteProbe::Ambient`] — the offline binary's posture, spelled out there. A server that
    /// resolves a location someone else supplied wants `detect_in` instead.
    pub fn detect(path: impl AsRef<Path>) -> Result<Source> {
        Source::detect_in(path, &RequestContext::detached(), RemoteProbe::Ambient)
    }

    /// [`detect`](Self::detect) with an explicit identity and probe posture.
    ///
    /// This is [`PathCatalog`]'s [`load_table`](Catalog::load_table), as a [`Source`]. The
    /// classification lives on the catalog, so detection runs through the [`Catalog`] seam
    /// whichever caller asks. Only the object-store branch consults the context or the posture: a
    /// local path is classified by stat'ing it and reading its first bytes, which needs no
    /// credentials and reaches nobody else's data.
    pub fn detect_in(
        path: impl AsRef<Path>,
        ctx: &RequestContext,
        probe: RemoteProbe,
    ) -> Result<Source> {
        PathCatalog::new(probe)
            .load_table(ctx, path.as_ref())
            .map(TableHandle::into_source)
    }

    /// Build a source with an explicit format (used by `--format` overrides and tests).
    pub fn with_format(path: impl AsRef<Path>, format: Format) -> Source {
        Source {
            path: path.as_ref().to_path_buf(),
            format,
            codec: None,
            json_path: None,
            flatten: None,
        }
    }

    /// Refuse a source compressed with a codec this build does not decompress, naming the feature
    /// that does. Read anyway, a `.csv.zst` would be a table of binary garbage.
    pub fn require_decodable(&self) -> Result<()> {
        match self.codec {
            Some(codec) if !codec.decodable() => {
                Err(crate::format::codec::undecodable(&self.display(), codec))
            }
            _ => Ok(()),
        }
    }

    /// This source with its struct columns flattened as `flatten` says — see [`Source::flatten`].
    pub fn with_flatten(mut self, flatten: Option<Flatten>) -> Source {
        self.flatten = flatten;
        self
    }

    /// This source, compressed with `codec` — see [`Source::codec`].
    pub fn with_codec(mut self, codec: Option<Codec>) -> Source {
        self.codec = codec;
        self
    }

    /// This source, read from `json_path` inside the document — see [`Source::json_path`].
    ///
    /// A bare member name (`data`) means that top-level member and becomes the pointer `/data`;
    /// anything starting with `/` is taken as a JSON Pointer as written. Refused for a source
    /// that is not JSON, because it would otherwise be silently ignored; a source whose format a
    /// server will decide ([`Source::unresolved`]) passes it on for that server to judge.
    pub fn with_json_path(mut self, json_path: Option<&str>) -> Result<Source> {
        let Some(json_path) = json_path else {
            return Ok(self);
        };
        if !matches!(self.format, Format::Json | Format::Unknown) {
            return Err(EngineError::UnsupportedFormat {
                detail: format!(
                    "a JSON path selects records inside a JSON document, but {} is {}",
                    self.display(),
                    self.format
                ),
            });
        }
        self.json_path = Some(normalize_json_path(json_path));
        Ok(self)
    }

    /// Build a source from a raw string **without touching the filesystem** — for an engine
    /// that resolves its own refs.
    ///
    /// [`Source::detect`] is a *local* operation: it stats the path, walks directories and
    /// sniffs magic bytes. That is exactly right when the bytes are on this machine and exactly
    /// wrong when they are not — a ref that only the far side can interpret (a catalog name, a
    /// table id, anything a server understands and this process does not) would be resolved
    /// against the wrong filesystem, fail there, and never reach the network.
    ///
    /// So this constructor resolves nothing. The string travels **opaquely** to whatever server
    /// the caller pointed at, and that server decides what it means — the general rule is *a
    /// remote engine resolves its own sources*, not a special case for any one scheme or
    /// product. An explicit `format` is still honoured (the caller genuinely knows something the
    /// name does not say); with `None` the format is left as [`Format::Unknown`], which the
    /// remote engine sends as *no* `?format=` at all so the server infers it the way it would
    /// for any of its own paths.
    ///
    /// Only the `remote` engine can read one of these. Handing it to a local engine is a
    /// programming error and is refused with a message that says so.
    pub fn unresolved(path: impl AsRef<Path>, format: Option<&str>) -> Result<Source> {
        match format {
            Some(f) => Source::resolve(path, Some(f)),
            None => Ok(Source::with_format(path, Format::Unknown)),
        }
    }

    /// Resolve a source from a path and an optional explicit format name (detect when `None`).
    ///
    /// Uses the process environment for the one remote probe that needs an identity — see
    /// [`detect`](Self::detect). A server resolving someone else's location wants
    /// [`resolve_in`](Self::resolve_in).
    pub fn resolve(path: impl AsRef<Path>, format: Option<&str>) -> Result<Source> {
        Source::resolve_in(
            path,
            format,
            &RequestContext::detached(),
            RemoteProbe::Ambient,
        )
    }

    /// [`resolve`](Self::resolve) with an explicit identity and probe posture.
    ///
    /// This is [`PathCatalog::resolve`]. An explicit `format` short-circuits before any probe,
    /// which is what makes [`RemoteProbe::VendedOnly`] a cost a caller can always avoid: naming the
    /// format is the answer to being refused a probe.
    pub fn resolve_in(
        path: impl AsRef<Path>,
        format: Option<&str>,
        ctx: &RequestContext,
        probe: RemoteProbe,
    ) -> Result<Source> {
        PathCatalog::new(probe).resolve(ctx, path.as_ref(), format)
    }

    /// This source rendered for a **human**: an API response body, a run record, a log line, an
    /// error message, the CLI's `path :` row.
    ///
    /// A database password is replaced with `***` — see [`redact_uri_password`]. Everything that
    /// renders a source goes through here, which is what makes that one function enough: the
    /// alternative is remembering to redact at each of the dozen places a source is printed, and
    /// the one that gets forgotten is a credential in someone's saved run history.
    ///
    /// Never use this to *reach* the source. [`Source::uri`] is that.
    pub fn display(&self) -> String {
        redact_uri_password(&self.path.to_string_lossy()).into_owned()
    }

    /// This source rendered for a **machine**: the string that opens a connection, or that a peer
    /// engine is asked to resolve.
    ///
    /// Verbatim, secrets included, because a redacted connection string does not connect. Every
    /// caller of this is a caller that would break if it were redacted, which is the property that
    /// makes the pair reviewable — a new `uri()` in a `format!` bound for a response body is
    /// visibly the wrong one of the two.
    pub fn uri(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    /// True when nothing local resolved this source and its format is the peer's to determine
    /// (see [`Source::unresolved`]).
    pub fn is_unresolved(&self) -> bool {
        self.format == Format::Unknown
    }

    /// True when this source lives in an object store (`s3://` / `gs://` / `az://`) rather
    /// than on the local filesystem. The reading itself needs `--features object-store`.
    pub fn is_remote(&self) -> bool {
        self.path.to_str().is_some_and(is_object_uri)
    }

    /// Is this a table a catalog serves, named by a `catalog://` reference?
    pub fn is_catalog(&self) -> bool {
        self.path
            .to_str()
            .is_some_and(crate::catalog::is_catalog_uri)
    }
}

/// A records path as a JSON Pointer: `""` (the whole document) and `/…` pointers as written, a
/// bare member name as that top-level member, escaped per RFC 6901 (`~` → `~0`, `/` → `~1`).
fn normalize_json_path(path: &str) -> String {
    if path.is_empty() || path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path.replace('~', "~0").replace('/', "~1"))
    }
}

/// One entry in a [`DirListing`] — a subdirectory or a readable data file.
#[derive(Debug, Clone, Serialize)]
pub struct DirEntry {
    pub name: String,
    pub path: String,
    /// `"dir"` or `"file"`.
    pub kind: &'static str,
    /// Detected format for files (parquet/csv/json/iceberg), `None` for plain dirs.
    pub format: Option<String>,
    pub size: Option<u64>,
}

/// A directory's browsable contents: subdirectories + readable data files (other files hidden).
#[derive(Debug, Clone, Serialize)]
pub struct DirListing {
    pub dir: String,
    pub parent: Option<String>,
    pub entries: Vec<DirEntry>,
}

/// Collect the `.parquet` files under `dir` (recursively, so Hive-partitioned subdirs work),
/// sorted for a deterministic read order. Sidecar/marker entries whose name starts with `.` or
/// `_` (`_SUCCESS`, `_common_metadata`, `.crc`, …) are skipped.
pub fn list_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_parquet_files(dir, &mut out);
    out.sort();
    out
}

fn collect_parquet_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || name.starts_with('_') {
            continue; // skip _SUCCESS / _common_metadata / .crc / hidden
        }
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out);
        } else if path
            .extension()
            .and_then(|x| x.to_str())
            .is_some_and(|x| x.eq_ignore_ascii_case("parquet"))
        {
            out.push(path);
        }
    }
}

/// Parse Hive-style `key=value` partition columns from the directory names on the path from
/// `root` (exclusive) down to `file`'s parent, outermost first. A file at
/// `root/year=2024/month=03/part-0.parquet` yields `[("year","2024"), ("month","03")]`. Path
/// segments that are not `key=value` (or have an empty key) are ignored, so a mixed layout with
/// some non-partition subdirs is tolerated. Returns empty when `file` is not under `root`.
pub fn hive_partitions(file: &Path, root: &Path) -> Vec<(String, String)> {
    let Ok(rel) = file.strip_prefix(root) else {
        return Vec::new();
    };
    let comps: Vec<_> = rel.components().collect();
    let mut out = Vec::new();
    // Every component except the final file name is a candidate partition dir.
    for comp in comps.iter().take(comps.len().saturating_sub(1)) {
        if let std::path::Component::Normal(os) = comp {
            let seg = os.to_string_lossy();
            if let Some((k, v)) = seg.split_once('=') {
                if !k.is_empty() {
                    out.push((k.to_string(), v.to_string()));
                }
            }
        }
    }
    out
}

/// List a directory for the file browser: subdirectories and Parquet/CSV/JSON files (plus
/// Iceberg-table dirs), dirs first, then files, each alphabetical. Non-data files are hidden.
///
/// This is [`PathCatalog`]'s [`list`](Catalog::list) under the offline binary's posture, as
/// [`Source::detect`] is its `load_table`: an object-store prefix is listed as the process
/// environment.
pub fn list_dir(dir: &str) -> Result<DirListing> {
    PathCatalog::new(RemoteProbe::Ambient).list(&RequestContext::detached(), Path::new(dir))
}

/// The format and codec a file's name gives: `t.csv` is CSV, `t.csv.gz` gzip-compressed CSV. Only a
/// text format takes a codec (see [`Codec`]), so `t.parquet.gz` names nothing and is sniffed like
/// any unknown name.
pub(crate) fn format_from_name(path: &Path) -> Option<(Format, Option<Codec>)> {
    let Some(codec) = codec_of(path) else {
        return Some((format_from_extension(path)?, None));
    };
    let format = format_from_extension(Path::new(path.file_stem()?))?;
    crate::format::reader(format)?
        .compressible()
        .then_some((format, Some(codec)))
}

/// The codec a name's last extension gives, if any.
pub(crate) fn codec_of(path: &Path) -> Option<Codec> {
    Codec::from_extension(path.extension()?.to_str()?)
}

pub(crate) fn format_from_extension(path: &Path) -> Option<Format> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    match ext.as_str() {
        "parquet" | "pq" => Some(Format::Parquet),
        // Every other file format is a registry reader's, and names its own extensions.
        ext => crate::format::format_for_extension(ext),
    }
}

pub(crate) fn sniff_magic(path: &Path) -> Result<Format> {
    use std::io::Read;
    let file = std::fs::File::open(path)?;
    let mut head = Vec::with_capacity(6);
    // A real IO error (permissions, etc.) should surface, not be misread as "not parquet"; fewer
    // bytes than a magic number falls through to the error below.
    file.take(6).read_to_end(&mut head)?;
    if head.starts_with(b"PAR1") {
        return Ok(Format::Parquet);
    }
    if head.starts_with(b"ARROW1") {
        return Ok(Format::Arrow);
    }
    Err(EngineError::UnsupportedFormat {
        detail: format!(
            "cannot infer the format of {} (unknown extension, and neither a Parquet nor an \
             Arrow file); pass a .parquet/.csv/.json/.arrow path",
            path.display()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Source::unresolved` must touch nothing and interpret nothing: whatever the caller typed
    /// is what the server is asked about. The strings below are deliberately unrelated — this
    /// is a general rule about who resolves a ref, not a special case for any one scheme, and a
    /// test that only used the scheme some server happens to accept would not say so.
    #[test]
    fn an_unresolved_source_is_the_caller_string_verbatim_and_carries_no_format() {
        for raw in [
            "cat://acme/geo/cities",
            "table:sales.orders@v3",
            "/no/such/file/on/this/disk.parquet",
            "just-a-name",
        ] {
            let s = Source::unresolved(raw, None).unwrap();
            assert_eq!(s.display(), raw, "the ref travels verbatim");
            assert!(s.is_unresolved(), "{raw}: format is the server's to decide");
            assert_eq!(s.format, Format::Unknown);
        }
    }

    /// The one thing a caller *can* assert. `Format::parse` still gates it, so a typo fails on
    /// this machine rather than travelling to a server that would reject it less clearly.
    #[test]
    fn an_explicit_format_is_honoured_and_a_bad_one_is_refused() {
        let s = Source::unresolved("some://opaque/ref", Some("tsv")).unwrap();
        assert_eq!(s.format, Format::Tsv);
        assert!(!s.is_unresolved());
        let err = Source::unresolved("some://opaque/ref", Some("parquay")).unwrap_err();
        assert!(err.to_string().contains("unknown format"), "{err}");
    }

    /// `Format::Unknown` is reachable only through [`Source::unresolved`]: nothing detects it,
    /// nothing parses it, so it can never be smuggled in from a user or a wire.
    #[test]
    fn the_unknown_format_cannot_be_asked_for() {
        assert_eq!(Format::parse("unknown"), None);
        assert_eq!(Format::Unknown.as_str(), "unknown");
    }

    /// Arrow is asked for by any name its files go by, and calls itself `arrow`.
    #[test]
    fn arrow_is_asked_for_by_every_name_its_files_go_by() {
        for name in ["arrow", "arrows", "feather", "ipc"] {
            assert_eq!(Format::parse(name), Some(Format::Arrow), "{name}");
        }
        assert_eq!(Format::Arrow.as_str(), "arrow");
    }

    #[test]
    fn extension_detection() {
        assert_eq!(
            Source::detect_ext_only("t.parquet").unwrap(),
            Format::Parquet
        );
        assert_eq!(Source::detect_ext_only("t.csv").unwrap(), Format::Csv);
        assert_eq!(Source::detect_ext_only("t.jsonl").unwrap(), Format::Json);
        assert_eq!(Source::detect_ext_only("t.geojson").unwrap(), Format::Json);
    }

    impl Source {
        // Test helper: extension-only detection (no filesystem access).
        fn detect_ext_only(p: &str) -> Option<Format> {
            format_from_extension(Path::new(p))
        }
    }

    #[test]
    fn a_compressed_name_gives_its_format_and_codec() {
        let name = |p: &str| format_from_name(Path::new(p));
        assert_eq!(name("t.csv.gz"), Some((Format::Csv, Some(Codec::Gzip))));
        assert_eq!(
            name("t.ndjson.zst"),
            Some((Format::Json, Some(Codec::Zstd)))
        );
        assert_eq!(name("T.TSV.BZ2"), Some((Format::Tsv, Some(Codec::Bzip2))));
        assert_eq!(
            name("dir.v2/t.json.xz"),
            Some((Format::Json, Some(Codec::Xz)))
        );
        assert_eq!(name("t.csv"), Some((Format::Csv, None)));
        // Parquet compresses inside its container; a codec around one names nothing.
        assert_eq!(name("t.parquet.gz"), None);
        assert_eq!(name("t.gz"), None);
        // The last extension alone names no format: the whole name does.
        assert_eq!(format_from_extension(Path::new("t.csv.gz")), None);
    }

    /// A compressed file is detected by its name, read when this build decodes its codec, and
    /// refused up front, naming the feature, when it does not.
    #[test]
    fn a_compressed_file_is_detected_and_refused_only_without_its_decoder() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.csv.gz");
        std::fs::write(&path, b"\x1f\x8b not really gzip").unwrap();
        let source = Source::detect(&path).unwrap();
        assert_eq!(
            (source.format, source.codec),
            (Format::Csv, Some(Codec::Gzip))
        );
        assert!(
            source.require_decodable().is_ok(),
            "gzip decodes in every build"
        );
        let zstd = Source::with_format(dir.path().join("t.csv.zst"), Format::Csv)
            .with_codec(Some(Codec::Zstd));
        let refused = zstd.require_decodable();
        assert_eq!(refused.is_ok(), cfg!(feature = "compression"));
        if let Err(e) = refused {
            let e = e.to_string();
            assert!(
                e.contains("t.csv.zst is zstd-compressed") && e.contains("--features compression"),
                "{e}"
            );
        }
        // An explicit format keeps the codec the name gives.
        let explicit = Source::resolve(&path, Some("tsv")).unwrap();
        assert_eq!(explicit.codec, Some(Codec::Gzip));
        // Object-store names are classified the same way, without a request.
        let remote = Source::detect("s3://bucket/logs/day.ndjson.zst").unwrap();
        assert_eq!(
            (remote.format, remote.codec),
            (Format::Json, Some(Codec::Zstd))
        );
    }

    #[test]
    fn list_dir_labels_table_directories_the_way_detect_resolves_them() {
        // The browser and the reader have to agree. A directory the reader opens as Delta but the
        // browser labels as a plain folder is a table the user cannot find; one the browser labels
        // Iceberg and the reader opens as Delta is worse.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("delta_tbl/_delta_log")).unwrap();
        std::fs::create_dir_all(dir.path().join("iceberg_tbl/metadata")).unwrap();
        // A Delta table that also carries a `metadata/` dir: detect checks `_delta_log` first, so
        // the listing must too, or the two disagree on exactly the ambiguous case.
        std::fs::create_dir_all(dir.path().join("both/_delta_log")).unwrap();
        std::fs::create_dir_all(dir.path().join("both/metadata")).unwrap();
        std::fs::create_dir_all(dir.path().join("plain")).unwrap();

        let listing = list_dir(&dir.path().display().to_string()).unwrap();
        let format_of = |name: &str| {
            listing
                .entries
                .iter()
                .find(|e| e.name == name)
                .unwrap_or_else(|| panic!("{name} missing from the listing"))
                .format
                .clone()
        };
        assert_eq!(format_of("delta_tbl").as_deref(), Some("delta"));
        assert_eq!(format_of("iceberg_tbl").as_deref(), Some("iceberg"));
        assert_eq!(format_of("both").as_deref(), Some("delta"));
        assert_eq!(format_of("plain"), None);

        for (name, want) in [
            ("delta_tbl", Format::Delta),
            ("iceberg_tbl", Format::Iceberg),
            ("both", Format::Delta),
        ] {
            let resolved =
                Source::resolve(dir.path().join(name).display().to_string().as_str(), None)
                    .unwrap()
                    .format;
            assert_eq!(resolved, want, "{name}: listing and detect must agree");
        }
    }

    #[test]
    fn readable_formats_tracks_the_compiled_features() {
        // The list is what `GET /v1/engines` and `lakeleto engines` both report, so it has to be
        // derived from the build rather than hardcoded — that drift is the bug this replaced.
        let formats = crate::engine::readable_formats();
        for always in ["parquet", "csv", "tsv"] {
            assert!(formats.iter().any(|f| f == always), "missing {always}");
        }
        assert_eq!(
            formats.iter().any(|f| f == "iceberg"),
            cfg!(feature = "iceberg")
        );
        assert_eq!(
            formats.iter().any(|f| f == "delta"),
            cfg!(feature = "delta")
        );
        assert!(
            !formats.iter().any(|f| f == "object-store"),
            "object-store is a source capability, not a file format — a client iterating this \
             list to enumerate openable types must not meet it here"
        );
    }

    // ---------------------------------------------------------------------------------------
    // Whose credentials the remote probe spends — see [`RemoteProbe`]
    // ---------------------------------------------------------------------------------------

    /// The confused deputy, stated as a test.
    ///
    /// An extensionless `s3://…/prefix` is classified by probing it, and before this the probe
    /// always ran on `StoreOptions::from_env()` — the *process's* identity — over a location the
    /// caller supplied. On a multi-tenant server that is a read performed as the server against a
    /// bucket a tenant named, which is both a credential nobody authorized spending and an
    /// existence oracle for anything the server's own role can see. `VendedOnly` is the posture
    /// that says: my identity is not available for this.
    #[cfg(feature = "object-store")]
    #[test]
    fn a_server_posture_refuses_to_probe_a_prefix_it_has_no_vended_identity_for() {
        let err = Source::detect_in(
            "s3://someone-elses-bucket/table",
            &RequestContext::detached(),
            RemoteProbe::VendedOnly,
        )
        .unwrap_err();

        // Refused for the credential reason, not merely "unknown format" — the two have different
        // fixes and a caller reading this should be sent to the right one.
        let msg = err.to_string();
        assert!(msg.contains("no credentials were vended"), "{msg}");
        assert!(msg.contains("read as the server"), "{msg}");
    }

    /// `Never` is the same refusal without the explanation: classification by name or not at all.
    #[cfg(feature = "object-store")]
    #[test]
    fn the_never_posture_falls_through_to_the_plain_unknown_format_error() {
        let err = Source::detect_in(
            "s3://bucket/table",
            &RequestContext::detached(),
            RemoteProbe::Never,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot infer the format"), "{err}");
    }

    /// The other half: the probe uses the identity it was *handed*, not the environment.
    ///
    /// Observable with no network and no credentials, by the same trick the engine tests use —
    /// `objstore` refuses a provider whose family does not match the URI's scheme before it builds
    /// a store, so an `s3://` probe that comes back mentioning GCS can only have gone through the
    /// options carrying a GCS provider. Had it read `from_env()` the mismatch could not arise.
    #[cfg(feature = "object-store")]
    #[test]
    fn the_probe_reads_as_the_context_says_rather_than_as_the_process() {
        let provider: object_store::gcp::GcpCredentialProvider = std::sync::Arc::new(
            object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
                bearer: "not-a-real-token".to_string(),
            }),
        );
        let ctx = RequestContext::detached().with_store_options(
            crate::objstore::StoreOptions::empty()
                .with_credentials(crate::objstore::StoreCredentials::Gcs(provider)),
        );

        let err = Source::detect_in("s3://bucket/table", &ctx, RemoteProbe::VendedOnly)
            .expect_err("a GCS identity cannot address an s3:// URI");
        assert!(err.to_string().contains("GCS"), "{err}");

        // And the vended identity is used under `Ambient` too: the fallback is a fallback, not a
        // preference. Same assertion, opposite posture — if `Ambient` consulted the environment
        // first, this would be an unknown-format error with no mention of GCS.
        let err = Source::detect_in("s3://bucket/table", &ctx, RemoteProbe::Ambient)
            .expect_err("a GCS identity cannot address an s3:// URI");
        assert!(err.to_string().contains("GCS"), "{err}");
    }

    /// Naming the format is always the way out of a refused probe, whatever the posture — which is
    /// what makes `VendedOnly` a cost a caller can pay rather than a wall.
    #[test]
    fn an_explicit_format_short_circuits_every_probe_posture() {
        for probe in [
            RemoteProbe::Ambient,
            RemoteProbe::VendedOnly,
            RemoteProbe::Never,
        ] {
            let s = Source::resolve_in(
                "s3://bucket/table",
                Some("iceberg"),
                &RequestContext::detached(),
                probe,
            )
            .unwrap_or_else(|e| panic!("{probe:?} should not probe at all: {e}"));
            assert_eq!(s.format, Format::Iceberg);
        }
    }

    /// A local path reaches none of this: no identity is consulted, so the posture cannot change
    /// the answer. Guards against a future refactor that gates local detection on a credential.
    #[test]
    fn a_local_path_is_classified_the_same_under_every_posture() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t.csv");
        std::fs::write(&file, "a,b\n1,2\n").unwrap();
        for probe in [
            RemoteProbe::Ambient,
            RemoteProbe::VendedOnly,
            RemoteProbe::Never,
        ] {
            let s = Source::detect_in(&file, &RequestContext::detached(), probe).unwrap();
            assert_eq!(
                s.format,
                Format::Csv,
                "posture {probe:?} changed a local answer"
            );
        }
    }

    // ---------------------------------------------------------------------------------------
    // A database source's password is the one secret that has to ride on a `Source`
    // ---------------------------------------------------------------------------------------

    #[test]
    fn redaction_replaces_the_password_and_keeps_everything_useful() {
        // The username survives: it says which account was refused, and it is not the secret.
        assert_eq!(
            redact_uri_password("postgres://alice:hunter2@db.internal:5432/orders?table=t"),
            "postgres://alice:***@db.internal:5432/orders?table=t"
        );
        // A password in the query string, wherever it sits.
        assert_eq!(
            redact_uri_password("postgres://db/orders?table=t&password=hunter2&sslmode=require"),
            "postgres://db/orders?table=t&password=***&sslmode=require"
        );
        // Both at once.
        assert_eq!(
            redact_uri_password("mysql://root:a@h/d?password=b"),
            "mysql://root:***@h/d?password=***"
        );
    }

    #[test]
    fn redaction_leaves_alone_everything_that_carries_no_secret() {
        for untouched in [
            "/var/data/events.parquet",              // a plain filesystem path
            "C:\\data\\events.parquet",              // ...on Windows
            "s3://bucket/prefix/t.parquet",          // credentials travel beside this, not in it
            "sqlite:///var/data/app.db?table=t",     // no authority, so no userinfo
            "postgres://db.internal/orders?table=t", // a location and nothing else: the goal shape
            "postgres://alice@db/orders",            // a username with no password
            "data/2024/events.parquet",              // relative, no authority
            "./cache@v2/t.parquet",                  // an `@` in a directory name, no userinfo
            "runs/a@b/t.parquet",                    // ...not in the first segment either
            // An opaque ref a remote engine resolves. It has a colon and an `@` in the shape
            // of userinfo and is not a credential at all — the reason this function needs a scheme.
            "table:sales.orders@v3",
        ] {
            assert_eq!(
                redact_uri_password(untouched),
                untouched,
                "redaction should not have touched {untouched}"
            );
            // And it borrows rather than allocating when there is nothing to do.
            assert!(matches!(
                redact_uri_password(untouched),
                std::borrow::Cow::Borrowed(_)
            ));
        }
    }

    /// The split that keeps both invariants true at once: this module's own documentation says a
    /// `Source` must never reach a response body carrying a credential, and a database URI cannot
    /// name its location without one.
    #[test]
    fn display_is_redacted_and_uri_is_verbatim() {
        let raw = "postgres://alice:hunter2@db/orders?table=events";
        let src = Source::with_format(raw, Format::Database);

        // What a person sees — an API response, a run record, a log line, an error.
        assert_eq!(src.display(), "postgres://alice:***@db/orders?table=events");
        assert!(!src.display().contains("hunter2"));

        // What connects. Redacting here would mean not connecting at all.
        assert_eq!(src.uri(), raw);
    }

    /// A local path must round-trip through `display()` unchanged, because plenty of things print a
    /// path and then expect to be able to use what they printed.
    #[test]
    fn display_still_round_trips_for_everything_that_is_not_a_connection_string() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("t.parquet");
        let src = Source::with_format(&file, Format::Parquet);
        assert_eq!(src.display(), src.uri());
        assert_eq!(src.display(), file.to_string_lossy());
    }
}
