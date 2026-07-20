//! BYO-credential object-store reads — the "sits next to your cloud data" piece.
//!
//! Feature-gated (`--features object-store`). Opens tables that live in an object store —
//! `s3://` (and `s3a://`), `gs://`, `az://`/`azure://`/`abfs[s]://`/`adl://` — using **the
//! user's own credentials from the process environment** and **zero hosted compute**. Nothing
//! is uploaded and no Lakeleto server is involved: the bytes flow straight from the customer's
//! bucket to the customer's machine, read with the customer's keys.
//!
//! Credentials are taken from the environment exactly as the cloud SDKs expect them — every
//! `std::env::var` is offered to the `object_store` backend, which keeps the keys it knows
//! (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` / `AWS_ENDPOINT` / `AWS_SESSION_TOKEN`,
//! `GOOGLE_APPLICATION_CREDENTIALS` / `GOOGLE_SERVICE_ACCOUNT`, `AZURE_STORAGE_ACCOUNT_NAME` /
//! `AZURE_STORAGE_ACCOUNT_KEY`, …) and ignores the rest. No config file is read implicitly.
//!
//! Parquet is read with **ranged requests** through [`ParquetObjectReader`] — only the footer
//! plus the row groups a window touches are fetched — so remote reads stay larger-than-memory
//! just like the local engine's. CSV, being line-oriented, is fetched whole.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
// `ObjectStore` is the core trait (and the trait object type + `list_with_delimiter`);
// `ObjectStoreExt` provides the ergonomic `get` / `head` / `put` convenience methods.
use object_store::{ListResult, ObjectStore, ObjectStoreExt};
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

/// Resolve an object-store URI to a store built from the environment's credentials plus the
/// object's key. `std::env::vars()` is passed as options: each backend picks the config keys
/// it recognises (BYO credentials), so `s3://` / `gs://` / `az://` all work uniformly.
fn store_for(uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjPath)> {
    let url = parse_uri(uri)?;
    let (store, path) = object_store::parse_url_opts(&url, std::env::vars())
        .map_err(|e| EngineError::Other(format!("object-store `{uri}`: {e}")))?;
    Ok((Arc::from(store), path))
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
    // `Source::detect` runs on the serve async thread; calling `block_on` there would panic
    // ("Cannot start a runtime from within a runtime"). Run the probe on a scratch OS thread, which
    // is not a Tokio worker, so `block_on` on our own runtime is legal.
    let uri = uri.to_string();
    std::thread::spawn(move || {
        let Ok((store, prefix)) = store_for(&uri) else {
            return false;
        };
        let meta = ObjPath::from(format!("{}/metadata", prefix.as_ref().trim_end_matches('/')));
        runtime().block_on(async {
            let mut listing = store.list(Some(&meta));
            matches!(listing.next().await, Some(Ok(_)))
        })
    })
    .join()
    .unwrap_or(false)
}

/// Mirror an object-store prefix (an Iceberg table directory) to a local temp directory, **once
/// per process** (cached by URI). Returns the local mirror root.
///
/// The Iceberg reader is filesystem-based; this shim lets it read a table that lives in a bucket:
/// download the whole prefix (metadata + Avro manifests + Parquet data), then plan against the
/// mirror with [`crate::iceberg::plan_object`], which remaps the absolute object URIs stored in the
/// metadata back to the mirror. Keys are laid out relative to the prefix (so `…/metadata/x.avro`
/// mirrors to `<dest>/metadata/x.avro`), matching how `plan_object` strips the origin URI. Trades
/// ranged reads for simplicity — appropriate for exploring a table, not a streaming path.
pub fn materialize_prefix(uri: &str) -> Result<PathBuf> {
    static CACHE: OnceLock<Mutex<HashMap<String, PathBuf>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(p) = cache.lock().unwrap().get(uri) {
        return Ok(p.clone());
    }
    let (store, prefix) = store_for(uri)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&uri, &mut hasher);
    let dest = std::env::temp_dir().join(format!(
        "lakeleto-obj-{:016x}",
        std::hash::Hasher::finish(&hasher)
    ));
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
                std::fs::create_dir_all(parent)?;
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
    cache.lock().unwrap().insert(uri.to_string(), dest.clone());
    Ok(dest)
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
    let (store, path) = store_for(uri)?;
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
    let (store, path) = store_for(uri)?;
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
    let (store, path) = store_for(uri)?;
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

/// List an object-store prefix for the file browser: immediate "subdirectories" (common
/// prefixes) and readable data files, mirroring [`crate::source::list_dir`]'s local shape so
/// the same SPA browser walks buckets and local disk identically.
pub fn list_prefix(uri: &str) -> Result<DirListing> {
    let url = parse_uri(uri)?;
    let scheme = url.scheme().to_string();
    let bucket = url.host_str().unwrap_or_default().to_string();
    let base = format!("{scheme}://{bucket}/");
    let (store, prefix) = store_for(uri)?;
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

    #[test]
    fn parent_uri_walks_up() {
        assert_eq!(parent_uri("s3://b/a/c"), Some("s3://b/a/".to_string()));
        assert_eq!(parent_uri("s3://b/a/c/"), Some("s3://b/a/".to_string()));
        assert_eq!(parent_uri("s3://b/a"), Some("s3://b/".to_string()));
        assert_eq!(parent_uri("s3://b/"), None);
        assert_eq!(parent_uri("s3://b"), None);
    }
}
