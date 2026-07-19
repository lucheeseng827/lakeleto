# Lakeleto configuration reference

Every knob `lakeleto` reads, in one place. Lakeleto is configured entirely by **CLI
flags** (a handful also read an env var) — there is no config file. Source of truth:
the clap derive in [`src/cli.rs`](../src/cli.rs), the `serve` router in
[`src/api.rs`](../src/api.rs), format detection in [`src/source.rs`](../src/source.rs),
and the `[features]` in [`Cargo.toml`](../Cargo.toml). Regenerate this file when they
change.

The binary is `lakeleto`. The default build is the lean, pure-Rust local reader; every
heavier engine is an off-by-default cargo feature (see [Feature flags](#feature-flags)).
`lakeleto engines` prints which backends are compiled into *your* binary.

## Global flags

Available on every subcommand (clap `global = true`).

| Flag | Env | Default | What it does |
|---|---|---|---|
| `-o`, `--output <fmt>` | — | `table` | Output format: `table` \| `json` \| `ndjson` \| `csv`. |
| `--engine <choice>` | — | `auto` | Which engine reads: `auto` \| `local` \| `sql` \| `remote`. `auto` = local, unless `--remote-url` is set (then remote). |
| `--remote-url <url>` | `LAKELETO_REMOTE_URL` | unset | Lakeleto Cloud endpoint. Setting it implies `--engine remote`. Needs `--features remote`. |
| `--remote-token <tok>` | `LAKELETO_REMOTE_TOKEN` | unset | Bearer token for the Lakeleto Cloud endpoint. |

## Subcommands

| Command | Positional | Flags | What it does |
|---|---|---|---|
| `schema <path>` | `path` | — | Columns, types, nullability, row count (exact for Parquet, from the footer). |
| `head <path>` | `path` | `-n`, `--rows <N>` (default `10`) | Preview the first N rows. |
| `profile <path>` | `path` | `--scan <N>` (default `10000`), `--fast` | Per-column null %, distinct, min/max, samples from a bounded scan of `--scan` rows. `--fast` uses the Parquet footer statistics (no row scan; exact nulls/min/max over the whole file, distinct/samples not computed). |
| `info <path>` | `path` | — | Quick source info: path, format, engine, file size, row count, column count. |
| `engines` | — | — | List the engine backends compiled into this binary and their capabilities. |
| `query <sql>` | `sql` | `--file <path>`, `--table <NAME=PATH>` (repeatable) | Run read-only SQL over one or more tables. Needs `--engine sql` (`--features sql`) or `--engine remote`. `--file` registers the source as table `t`; `--table name=path` registers a named table. Rejects non-`SELECT`/`WITH`/`EXPLAIN`. |
| `serve` | — | see [`serve` flags](#serve-flags) | Serve the HTTP/JSON API + embedded SPA. **Needs `--features serve`.** |
| `open <path>` | `path` | `--addr`, `--default-scan`, `--token`, `--root` | Start the server and launch a browser tab deep-linked to `path` (`?path=`). **Needs `--features serve`.** |

`query` on `--engine local` is an error (the local reader has no SQL planner). Without
the `sql`/`remote` feature, `query` returns a "rebuild with `--features sql`" message.

### `serve` flags

The `serve` subcommand exists only when built with `--features serve`.

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--addr <host:port>` | `LAKELETO_ADDR` | `127.0.0.1:8080` | Bind address for the HTTP listener. Loopback by default. |
| `--default-scan <N>` | — | `10000` | Row cap for `/v1/profile` when the request omits `scan`. |
| `--token <TOKEN>` | `LAKELETO_TOKEN` | unset (**API open**) | Require this bearer token on every `/v1/*` route (`Authorization: Bearer <TOKEN>`, or `?token=` on a loopback bind). `/healthz` + the SPA stay open. Constant-time compared. |
| `--root <dir>` | — | unset (any path) | Confine `/v1/*` file access to this directory; reads/browse outside it (and all object-store URIs) are refused with a uniform 403. Canonicalized at startup — a missing/non-dir path fails fast. |
| `--workspace-remote <url>` | `LAKELETO_WORKSPACE_REMOTE` | unset (local store) | Sync the workspace data plane (`/v1/workspaces/*`) to another server instead of the on-disk store. Needs `--features remote`. |
| `--workspace-remote-token <tok>` | `LAKELETO_WORKSPACE_REMOTE_TOKEN` | unset | Bearer token for `--workspace-remote`. |

`open` takes the same `--addr`/`--default-scan`/`--token`/`--root` flags as `serve`
(no workspace-remote flags).

## Supported inputs

Format detection order (`src/source.rs`): **object-store URI → directory shape →
extension → magic bytes**. Override with `--format` / `?format=` (accepts
`parquet`/`pq`, `csv`, `tsv`, `json`/`ndjson`/`jsonl`, `iceberg`).

| Input | How it's recognized | Read by |
|---|---|---|
| `.parquet` / `.pq` file | extension, or `PAR1` magic bytes when the extension is unknown | local (default) |
| `.csv` file | extension | local (default) |
| `.tsv` file | extension (read tab-delimited); `--format tsv` forces tab for any name | local (default) |
| `.json` / `.ndjson` / `.jsonl` | extension | local (default) |
| Directory of `.parquet` files | a dir with `.parquet` files (recursive; `foo.parquet/part-*` splits and Hive `key=value` partition subdirs); `_`/`.` sidecars skipped, columns unioned, partition keys become columns | local (default) |
| Iceberg table | a directory containing a `metadata/` subdir | `--features iceberg` |
| `s3://` / `gs://` / `az://` URI | scheme (see below); classified by the key's extension (needs explicit `--format` if the name has no known extension) | `--features object-store` |

Recognized object-store schemes: `s3` (`s3a`), `gs` (`gcs`), `az` (`azure`, `abfs`,
`abfss`, `adl`). These are recognized in **every** build — without `--features
object-store` a URI gets a "rebuild with `--features object-store`" message instead of a
filesystem error. Object-store credentials come only from the environment (see
[OPERATIONS.md](./OPERATIONS.md#object-store-credentials-byo)).

## Feature flags

`Cargo.toml` `[features]`. `default = []` — the pure-Rust local reader
(`arrow`/`parquet`/`csv`), which always compiles fast with no C++ toolchain.

| Feature | Turns on | Adds |
|---|---|---|
| *(default)* | `LocalReaderEngine` | Parquet + CSV/TSV/JSON reads (schema/head/profile/info/grid). |
| `sql` | `DataFusionEngine` (+ tokio) | Read-only SQL: the `query` command with `--engine sql`, and `POST /v1/query`. Pushes sort/filter/count into DataFusion. |
| `iceberg` | self-contained Iceberg reader (apache-avro) | Read Iceberg tables: current-snapshot Parquet via metadata + Avro manifests, merge-on-read positional + equality deletes, schema evolution, statistics/partition pruning. Reads compressed manifests. |
| `object-store` | BYO-credential `s3://`/`gs://`/`az://` reads (object_store + url + futures + tokio) | Every read op over a remote URI with *your own* env credentials, zero hosted compute. Ranged Parquet reads (footer + touched row groups); CSV fetched whole. |
| `serve` | `lakeleto serve` / `lakeleto open` (axum + rust-embed + tokio) | The HTTP/JSON `/v1/*` API and the embedded SPA (bundled via rust-embed — air-gapped). Add `sql` too for a working `POST /v1/query`. |
| `remote` | `RemoteEngine` → Lakeleto Cloud seam (reqwest) | `--engine remote` / `--remote-url`; the hosted-plane client (optional). Also enables `--workspace-remote` sync in `serve`. |
| `duckdb` | *(stub — no code yet)* | Reserved Phase-2 DuckDB backend (C++ toolchain). Currently a no-op feature. |

The container image ([`Dockerfile`](../Dockerfile)) is built with
`--features serve,sql,iceberg,object-store`.

## `serve` HTTP/JSON endpoints

`lakeleto serve` (and `open`) bind `--addr` (default `127.0.0.1:8080`). Errors return
`{ "error": ... }` with a mapped status: `400` bad request/format, `403` outside
`--root`, `404` not found / unknown `/v1/*` endpoint, `413` export over the byte cap,
`501` a needed feature (e.g. `sql`) wasn't compiled in, `502` remote engine, `500` other
IO. Non-API paths fall back to the SPA's `index.html`.

### Read endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | Liveness (`ok`); always open, even with a token. |
| `GET` | `/v1/engines` | Serving engine capabilities, `sql_available`, and the endpoint list. |
| `GET` | `/v1/schema?path=&format=` | Columns, types, nullability, row count. |
| `GET` | `/v1/info?path=&format=` | Format, engine, size, rows, columns. |
| `GET` | `/v1/preview?path=&limit=&format=` | First N rows (default 50) as `{ columns, rows }`. |
| `GET` | `/v1/profile?path=&scan=&format=` | Per-column null %, distinct, min/max (`scan=0` = Parquet footer fast path). |
| `GET` | `/v1/rows?path=&offset=&limit=&sort=&desc=&filter=col:op:value&cols=a,b` | Grid window: filter → sort → page → project. `limit` clamped to 10000. |
| `GET` | `/v1/stats?path=&filter=col:op:value` | Column profile over the **filtered** view. |
| `GET` | `/v1/export?path=&fmt=csv\|json\|parquet&sort=&filter=&cols=` | Current view as a download (row cap 1,000,000; byte cap 512 MiB → 413). |
| `GET` | `/v1/list?dir=` | File browser: subdirs + readable data files (defaults to `--root`, else cwd). |
| `POST` | `/v1/query` | `{ sql, file?, tables[] }` → `{ columns, rows }`. Needs `sql`. |

Filter ops: `eq ne lt le gt ge contains` (aliases `= != < <= > >= ~`). With `sql`,
sort/filter/count are pushed into DataFusion (exact, unbounded); without it the local
reader works over a bounded set (`scan_cap`, default 200k) and the response's `bounded`
flag marks a partial view.

### Workspace data-plane endpoints ("Postman" workbench)

Persisted through a `WorkspaceStore` (local JSON + Parquet result cache by default; a
`--workspace-remote` server behind the same contract). All under `/v1/*`, so the same
`--token`/`--root` gates apply.

| Method | Path | Purpose |
|---|---|---|
| `GET` / `POST` | `/v1/workspaces` | List / create a workspace. |
| `GET` / `PUT` / `DELETE` | `/v1/workspaces/{id}` | Fetch / save / delete a workspace. |
| `GET` / `POST` | `/v1/workspaces/{id}/history` | Run history (newest first) / sync-append a record. |
| `POST` | `/v1/workspaces/{id}/runs` | Run SQL/scan (root-confined), record it + cache the result (row cap 100,000). |
| `GET` | `/v1/workspaces/{id}/runs/{run_id}?offset=&limit=` | A window over a cached run result. |
| `PUT` / `GET` | `/v1/workspaces/{id}/runs/{run_id}/result` | Raw Parquet result bytes (sync up/down; upload cap 128 MiB). |
| `GET` | `/v1/workspaces/{id}/export` | Download a portable workspace bundle. |
| `POST` | `/v1/workspaces/import` | Import a bundle (mints a fresh id). |

The local store lives under `$LAKELETO_HOME` (`~/.lakeleto/workspaces/<id>/` —
`workspace.json` · `history.jsonl` · `results/*.parquet`).
