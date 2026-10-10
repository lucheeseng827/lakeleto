# Lakeleto configuration reference

Every knob `lakeleto` reads, in one place. Lakeleto is configured by **CLI flags** (a
handful also read an env var). Its one config file is `$LAKELETO_HOME/catalogs.toml`, which
names the catalogs the `catalog` feature reads ([Catalogs](#catalogs-catalogstoml)). Source
of truth: the clap derive in [`src/cli.rs`](../src/cli.rs), the `serve` router in
[`src/api.rs`](../src/api.rs), format detection in [`src/source.rs`](../src/source.rs),
catalog configuration in [`src/catalog/config.rs`](../src/catalog/config.rs), and the
`[features]` in [`Cargo.toml`](../Cargo.toml). Regenerate this file when they change.

The binary is `lakeleto`. The default build is the lean, pure-Rust local reader; every
heavier engine is an off-by-default cargo feature (see [Feature flags](#feature-flags)).
`lakeleto engines` prints which backends are compiled into *your* binary.

## Global flags

Available on every subcommand (clap `global = true`).

| Flag | Env | Default | What it does |
|---|---|---|---|
| `-o`, `--output <fmt>` | — | `table`, or what `--out`'s extension names | Output format: `table` \| `json` \| `ndjson` \| `csv` \| `tsv`, and for rows (`head`, `query`, `catalog ls`) also `arrow` (Arrow IPC file, Feather v2), `arrows` (Arrow IPC stream) or `parquet` (Snappy; needs `--features parquet-out`, which the release binaries have). In `csv` and `tsv` a nested value (a list, struct or map) is written into its cell as compact JSON; the binary formats keep it nested. A binary format is refused when stdout is a terminal. See [Output for other tools](GUIDE.md#output-for-other-tools). |
| `--out <file>` | — | stdout | Write the output to this file (`-` is stdout). It appears only when the command succeeds, renamed into place from a hidden temporary file beside it, so a failed run leaves no half-written file and keeps the one that was there. A run killed outright can leave that file, `.lakeleto.<pid>-<hex>.tmp`, behind. On Unix the temporary file is readable only by you, and the result keeps the permission bits of the file it replaces (a new file gets what any new file gets); ownership and ACLs are not carried over. With no `-o`, the extension picks the format: `.parquet`; `.arrow`, `.feather` or `.ipc`; `.arrows`; `.csv`, `.tsv`, `.json`, `.ndjson` or `.jsonl`. |
| `--engine <choice>` | — | `auto` | Which engine reads: `auto` \| `local` \| `sql` \| `remote`. `auto` = local, unless `--remote-url` is set (then remote). |
| `--remote-url <url>` | `LAKELETO_REMOTE_URL` | unset | A Lakeleto server speaking the `/v1/*` contract — in practice a `lakeleto serve` you run. Setting it implies `--engine remote`. Needs `--features remote`, which the release binaries have. |
| `--remote-token <tok>` | `LAKELETO_REMOTE_TOKEN` | unset | Bearer token for the Lakeleto Cloud endpoint. |
| `--format <name>` | — | inferred | Read the source as this format instead of inferring it (see [Supported inputs](#supported-inputs)); `?format=` on the API. |
| `--json-path <pointer>` | — | detected | Read a JSON document's rows from this place: a JSON Pointer (`/response/items`) or a top-level member name (`data`); `""` unwraps nothing. `?json_path=` on the API. See [JSON layouts](#json-layouts). |
| `--flatten[=<levels>]` | — | off | Read struct columns as one column per field, named by its path (`user.geo.lat`); `--flatten` is every level, `--flatten=1` the first only. Any format. `?flatten=` on the API. See [Flattening nested columns](#flattening-nested-columns). |
| `--max-decompressed <bytes>` | `LAKELETO_MAX_DECOMPRESSED` | `4GiB` | The most bytes one read decompresses a compressed file (`t.csv.gz`) to; a read that needs more stops with a `too large` error (HTTP 413 from `serve`). A guard against a small file that inflates without end: the grid's first rows stop long before it. A number of bytes, or one with a suffix in powers of 1024 (`512M`, `8GiB`). Applies to SQL, `serve` and `mcp` too. |

## Subcommands

| Command | Positional | Flags | What it does |
|---|---|---|---|
| `schema <path>` | `path` | — | Columns, types, nullability, row count (exact for Parquet and Arrow files, from the footer). |
| `head <path>` | `path` | `-n`, `--rows <N>` (default `10`) | Preview the first N rows. |
| `profile <path>` | `path` | `--scan <N>` (default `10000`), `--fast` | Per-column null %, distinct, min/max, samples from a bounded scan of `--scan` rows. `--fast` uses the Parquet footer statistics (no row scan; exact nulls/min/max over the whole file, distinct/samples not computed). |
| `info <path>` | `path` | — | Quick source info: path, format, engine, file size, row count, column count. |
| `engines` | — | — | List the engine backends compiled into this binary and their capabilities. |
| `query <sql>` | `sql` | `--file <path>`, `--table <NAME=PATH>` (repeatable) | Run read-only SQL over one or more tables. Needs `--engine sql` (`--features sql`) or `--engine remote`. `--file` registers the source as table `t`; `--table name=path` registers a named table. Rejects non-`SELECT`/`WITH`/`EXPLAIN`. |
| `serve` | — | see [`serve` flags](#serve-flags) | Serve the HTTP/JSON API + embedded SPA. **Needs `--features serve`.** |
| `open <path>` | `path` | `--addr`, `--default-scan`, `--token`, `--root` | Start the server and launch a browser tab deep-linked to `path` (`?path=`). **Needs `--features serve`.** |
| `mcp` | — | see [`mcp` flags](#mcp-flags) | Serve the tables to an AI agent over the Model Context Protocol, on stdin and stdout: the read-only tools in [`mcp` tools](#mcp-tools). An MCP client starts it. **Needs `--features mcp`.** |

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
| `--workspace-remote <url>` | `LAKELETO_WORKSPACE_REMOTE` | unset (local store) | Sync the workspace data plane (`/v1/workspaces/*`) to another server instead of the on-disk store. Needs `--features remote`, which the release binaries have. |
| `--workspace-remote-token <tok>` | `LAKELETO_WORKSPACE_REMOTE_TOKEN` | unset | Bearer token for `--workspace-remote`. |

`open` takes the same `--addr`/`--default-scan`/`--token`/`--root` flags as `serve`
(no workspace-remote flags).

### `mcp` flags

The `mcp` subcommand exists only when built with `--features mcp`. It reads with the engines the
build has, as `serve` does; the global `--engine`, `--remote-url` and `-o` flags don't apply.

| Flag | Default | What it does |
|---|---|---|
| `--root <dir>` | unset (any path) | Read only under this directory: anything outside it, and every object-store, database and catalog reference, is refused with a `forbidden` error; relative paths are taken from it. Canonicalized at startup — a missing/non-dir path fails fast. The same check as `serve --root`. |
| `--default-scan <N>` | `10000` | Rows `profile` scans when the call doesn't say. |
| `--max-rows <N>` | `1000` | The most rows `preview` and `query` return in one call (at least 1). |
| `--max-bytes <N>` | `32768` | The most bytes of JSON one call returns (at least 1024); rows, entries or columns past it are left out, and the result says `truncated`, even if none are left (a single row bigger than the room). Any tool's result whose other fields alone come within 256 bytes of the cap, such as a `preview`'s `columns` for thousands of columns, is refused as `too_large` rather than sent over it. |
| `--timeout <secs>` | `30` | How long a call may run. The engines stop at the deadline; a call one doesn't stop is answered with a `deadline` error a second after it, and its late result is dropped. |

### `mcp` tools

JSON-RPC 2.0 over stdio, one message per line; nothing else is written to stdout. Protocol
revisions `2024-11-05` to `2025-11-25`, through the `initialize` handshake; a client of a later
revision falls back to it when its `server/discover` probe gets "method not found". Calls run
concurrently, up to 16 at once (one more is refused as `busy`); `notifications/cancelled` stops
one, and `ping` is answered during a long call.

`tools/list` offers only what the build and flags can run: `query` needs `sql` (or a database
driver, for a database's own SQL), and `catalog_ls`
needs `catalog` and no `--root`. A result is one text item holding compact JSON. Rows are arrays in
the order of `columns`, with `null` for a null cell. A result that was cut has `"truncated": true`
and a `note`.

| Tool | Arguments | Result |
|---|---|---|
| `list` | `path` (default: the root, else the working directory) | `dir`, `parent`, `entries` (`name`, `path`, `kind`: `dir`\|`file`, `format`, `size`). A database URI lists its tables; a `catalog://` reference, a namespace. |
| `describe` | `path`; `format`, `json_path` | `source`, `format`, `engine`, `row_count` (`null` when the table doesn't record one), `size_bytes`, `statistics`, `columns` (`name`, `data_type`, `nullable`, and from a local Parquet file's footer `null_count`, `min`, `max`). Reads no rows. |
| `preview` | `path`; `rows` (default 20), `offset`, `columns`; `format`, `json_path` | `offset`, `row_count` (when known), `columns`, `rows`. |
| `profile` | `path`; `scan` (default `--default-scan`, at most 100,000 unless that is larger; `0` reads a Parquet footer only); `format`, `json_path` | The `/v1/profile` body: `row_count`, `scanned_rows`, `columns` (`null_count`, `null_fraction`, `distinct`, `min`, `max`, `sample`, …). |
| `query` | `sql`; `path` (the table `t`) and/or `tables` (`[{name, path, format?, json_path?}]`); `rows` (default 100) | `columns`, `rows`. One `SELECT`, `WITH` or `EXPLAIN` statement; a database URI's SQL runs on the database. |
| `catalog_ls` | `reference` (default `catalog://`) | At `catalog://`, `catalogs` (`name`, `type`, `uri`, `reference`), from configuration with no network call; below it, `entries` (`name`, `kind`: `namespace`\|`table`, `reference`). |

A refused or failed call is a result with `"isError": true` and the text
`{"error": <kind>, "message": …}`. The kinds are `forbidden` (outside `--root`, or refused by a
catalog), `not_found`, `query` (the SQL failed, or wrote and was refused), `deadline`,
`cancelled`, `busy` (16 calls are running), `unsupported_format`, `unsupported` (the build lacks
a feature), `too_large`, `remote`, `invalid_arguments` and `failed`. A malformed request, an unknown tool or an unknown
method is a JSON-RPC error instead (`-32602`, `-32601`).

## Supported inputs

Format detection order (`src/source.rs`): **object-store URI → directory shape →
extension → magic bytes**. Override with `--format` / `?format=` (accepts
`parquet`/`pq`, `csv`, `tsv`, `json`/`ndjson`/`jsonl`/`geojson`, `arrow`/`arrows`/`feather`/`ipc`,
`iceberg`).

| Input | How it's recognized | Read by |
|---|---|---|
| `.parquet` / `.pq` file | extension, or `PAR1` magic bytes when the extension is unknown | local (default): pages compressed with Snappy, gzip or LZ4 in every build, zstd (Polars' default) and Brotli with `--features compression`, and zstd with `iceberg` too; a codec the build lacks is refused, naming the feature. `schema`, `info` and `profile --fast` read only the footer, so they read any codec. |
| `.csv` file | extension | local (default) |
| `.tsv` file | extension (read tab-delimited); `--format tsv` forces tab for any name | local (default) |
| `.json` / `.ndjson` / `.jsonl` / `.geojson` | extension; the layout comes from the bytes — see [JSON layouts](#json-layouts) | local (default); `sql` through the local reader, streamed — from an object store too |
| Arrow IPC: `.arrow` / `.feather` / `.ipc` file, `.arrows` stream | extension, or `ARROW1` magic bytes when the extension is unknown; which framing a file has, its first bytes say, whatever it is called | local (default): a file by its footer, so its row count is exact and a window reads only the record batches it covers, from an object store by ranged requests; a stream front to back. Buffers compressed with LZ4 (pyarrow's Feather default) read in every build, zstd with `--features compression`. Feather v1 is not read. `sql` natively. |
| Directory of `.parquet` files | a dir with `.parquet` files (recursive; `foo.parquet/part-*` splits and Hive `key=value` partition subdirs); `_`/`.` sidecars skipped, columns unioned, partition keys become columns | local (default) |
| Iceberg table | a directory containing a `metadata/` subdir | `--features iceberg` |
| `s3://` / `gs://` / `az://` URI | scheme (see below); classified by the key's extension (needs explicit `--format` if the name has no known extension) | `--features object-store` |
| `catalog://<catalog>/<namespace>/<table>` | scheme; a table an Iceberg REST catalog serves (see [Catalogs](#catalogs-catalogstoml)) | `--features catalog` |
| Compressed text: `.csv.gz`, `.ndjson.zst`, `.tsv.bz2`, `.json.xz` | the last extension is the codec (`gz`, `zst`, `bz2`, `xz`), the one before it the format | local (default), as the format it holds: gzip in every build, zstd, bzip2 and xz with `--features compression`; a codec the build lacks is refused, naming the feature. Decompressed as it is read, from an object store as it streams; a JSON records member is read from the start each time. Each read stops at `--max-decompressed`. `sql` too, a pass at a time through the same reader and under the same limit. |

### JSON layouts

A JSON file is read the same way whatever its extension says; the layout is detected from the
bytes:

| Layout | Example | Rows |
|---|---|---|
| Values separated by whitespace | NDJSON / JSON Lines, `jq` output (pretty objects back to back), one object | one per value |
| Top-level array | `[{…}, {…}]` | one per element — streamed, so no size limit |
| One document with a records member | `{"meta": {…}, "data": [{…}, …]}`, a GeoJSON `FeatureCollection` | one per element of that member |

The records member is used when a single top-level object has **exactly one** member holding a
non-empty array of objects; `lakeleto schema` then prints it (`records: /data`) and `/v1/schema`
returns it as `records_path`. With no such member, or several, the object is one row. The member
is found once per file version by reading the document's bytes into memory, without parsing them
into values — so a document is only unwrapped up to 256 MiB — and its records then stream like a
top-level array's: a grid window costs the rows up to it, not the document.

`--json-path` / `?json_path=` overrides the detection: `--json-path groups` picks between two
candidates, `--json-path /response/items` reaches below the top level, and `--json-path ''` reads
the document as one row. An explicit path applies to a single document only; NDJSON and a missing
member are errors that say so, and a path on a source that is not JSON is refused rather than
ignored.

In the web app, a JSON tab's **Records** button shows the same choice — `Records: /data · auto`
when the reader found the member, the path when one is set, `whole document` for `''` — and sets
it: type a pointer or a member name, or pick **Whole document** or **Auto**. The path belongs to
the tab and its SQL, is saved with a saved query, and comes back when the query or a run from
history is reopened.

Columns come in the order their keys first appear. A column whose values disagree in type
(`{"v": 1}` then `{"v": "a"}`) is read as text rather than failing. A UTF-8 byte-order mark is
ignored, and an array cut off before its closing `]` is an error, not a shorter table.

Nested values — objects and arrays, read as `Struct` and `List` columns — are shown in the grid
as compact JSON (pretty-printed in the row drawer), and sorting or filtering one works on that JSON
text, so a filter matches what you read. Lists that Arrow can order natively (element by element)
sort that way. A CSV or TSV export writes the same JSON into the cell.

The schema comes from the first 20,000 values — the same ones on every read, remembered per file
version — so `schema`, the first grid window and a window far down agree. A value past those that
disagrees with them (an integer column that turns to text at row 50,000) widens that file's schema
instead of failing the read; a key that first appears past them is not shown.

SQL (`--features sql`) reads JSON through this same reader rather than DataFusion's own, which
infers line by line, so every layout, `--json-path`, `--flatten` and the widening above apply in
SQL exactly as in the grid. A local file is streamed: each query makes its own pass over it and
holds only the batches in flight, so `LIMIT 10` stops reading early and a count or `GROUP BY` over
a large file runs in memory set by the query — at the cost of decoding the file again for each
query. The query's column types are the sampled ones; a value past the sample that does not fit
them widens the file's schema, and the query is planned again with it. A streamed result that has
already sent rows cannot be planned again without sending some twice, so it stops with an error
instead, and the next run reads the value. A sorted or filtered grid window over JSON goes through
SQL too, complete over the whole file rather than its first 200,000 rows. A JSON file in an object
store streams the same way, each pass a request of its own that stops where the pass stops.

### Flattening nested columns

`--flatten` / `?flatten=` reads each struct column as one column per field, named by its path:
`user: {name, geo: {lat}}` becomes `user.name` and `user.geo.lat`, in the struct's place. Those
columns sort, filter, profile and export like any other, and SQL names them quoted
(`SELECT "user.name" FROM t`). The grid's **Flatten structs** button (shown when a source has a
struct column) does the same for a tab, including that tab's SQL.

| Value | Meaning |
|---|---|
| `--flatten`, `?flatten`, `all` | every level |
| a number `n` | the first `n` levels: at `1`, `user.geo` stays a struct |
| `none`, `0` | off (the default) |

It is a view of the source, not a way of parsing it, so it works for every format with struct
columns — JSON objects, nested Parquet, Iceberg and Delta — and changes nothing for one without
(CSV, a database table). A field of a null struct is null. Lists and maps stay whole, because
spreading one would turn a row into many; an empty struct stays too. Two columns that would share
a name (a `user.name` column beside a `user` struct with a `name` field) are refused rather than
one shadowing the other.

The SQL engine flattens with a view over the table, so a sorted or filtered grid window still runs
over the whole file. A Parquet footer profile (`profile --fast`, `?scan=0`) lists the fields but
has statistics only for top-level columns — Parquet's statistics API does not reach inside a
struct, which is also why the struct column itself never had them.

Recognized object-store schemes: `s3` (`s3a`), `gs` (`gcs`), `az` (`azure`, `abfs`,
`abfss`, `adl`). These are recognized in **every** build — without `--features
object-store` a URI gets a "rebuild with `--features object-store`" message instead of a
filesystem error. Object-store credentials come only from the environment (see
[OPERATIONS.md](./OPERATIONS.md#object-store-credentials-byo)).

## Catalogs (`catalogs.toml`)

With `--features catalog`, a table in an Iceberg REST catalog is named by a `catalog://` reference
wherever a path goes: the CLI, `?path=`, `/v1/list?dir=`, `--table name=…` and a workspace's saved
connections. Polaris, Lakekeeper, Nessie and Unity Catalog serve the protocol, and Lakeleto is
tested against Polaris and Lakekeeper, with the credentials each vends. Glue and S3 Tables serve
it behind AWS request signing, which this release does not do yet.

```text
catalog://<catalog>/<level>/…/<level>/<table>
```

- **`<catalog>` is a name you configure**, never a hostname: letters, digits and `-`, at most 63
  characters, any case. So a saved reference keeps working when the catalog moves.
- **The last segment is the table, and the ones before it are namespace levels.** A trailing `/`
  names a namespace: `catalog://prod/` lists prod's namespaces, and `catalog://prod/sales/` lists
  what is in `sales`.
- **Segments are percent-encoded:** a `/` in a name is `%2F`, an `@` is `%40` and a `%` is `%25`.
- **A snapshot suffix (`@<id>` or `@<timestamp>`) is refused** until 0.5.0, which reads it.
- **A reference never carries a credential.**

### Where catalogs are configured

`$LAKELETO_HOME/catalogs.toml` (by default `~/.lakeleto/catalogs.toml`), and the environment. The
keys are the ones the Iceberg REST clients use, so an entry copies over from pyiceberg or Spark.

```toml
[catalog.prod]
type = "rest"                                     # the default
uri = "https://polaris.example.com/api/catalog"
warehouse = "analytics"
oauth2-server-uri = "https://polaris.example.com/api/catalog/v1/oauth/tokens"
scope = "PRINCIPAL_ROLE:ALL"
# The secret comes from LAKELETO_CATALOG__PROD__CREDENTIAL, so it stays out of this file.

[catalog.dev]
uri = "http://localhost:8181"
s3.endpoint = "http://localhost:9000"
s3.path-style-access = true
```

| Key | Meaning |
|---|---|
| `type` | `rest`, the default and the only type this release reads |
| `uri` | The catalog's base URI. Over `https`, or over plain `http` only to this machine (`localhost`, `127.0.0.1`, `::1`): a catalog sends logins and storage credentials. |
| `warehouse` | Sent to `GET /v1/config`, which answers with the catalog's defaults and route prefix |
| `credential` | `<client-id>:<client-secret>` (or just the secret) for OAuth2 client credentials |
| `token` | A static bearer token, instead of `credential` |
| `oauth2-server-uri` | The token endpoint for `credential`. Set it: without it the login goes to the catalog's own `/v1/oauth/tokens`, which the REST spec deprecates, and Lakeleto warns. |
| `scope` | The OAuth2 scope (default `catalog`) |
| `header.<name>` | A header sent with every request. An empty value turns a header off, including `X-Iceberg-Access-Delegation`, which Lakeleto sends as `vended-credentials`. |
| `s3.*`, `gcs.*`, `adls.*`, `client.region` | Storage settings, and storage keys for a catalog that vends none |
| `storage-fallback` | `ambient` (the default) or `none`: whether a table the catalog neither vends nor configures credentials for is read with this machine's own |

- **Environment variables:** `LAKELETO_CATALOG__<NAME>__<KEY>`. `__` separates the name, the key
  and a nested key's parts (joined with `.`), a single `_` stands for `-`, and both are lowercased.
  - `LAKELETO_CATALOG__PROD__OAUTH2_SERVER_URI` sets `oauth2-server-uri` for `prod`.
  - `LAKELETO_CATALOG__DEV__S3__ENDPOINT` sets `s3.endpoint` for `dev`.
  - A variable wins over the file's value, and an empty one removes it. A catalog can be
    configured with variables alone.
- **Unknown keys are kept and reported**, so a misspelt key is not silently ignored.
- **The file is read the first time a reference is used,** so a malformed file fails catalog
  references and nothing else.
- **On Unix, Lakeleto warns when the file holds a secret and others can read it.**

### Whose credentials read a table's files

The first of these that applies:

1. **Vended by the catalog for the table.** Lakeleto asks for them on every `loadTable`. It uses
   the `storage-credentials` entry whose prefix is the longest match for the table's location,
   else the credentials in `loadTable`'s `config`. When they carry an expiry, fresh ones are
   fetched from the catalog shortly before it, through its credentials endpoint if it lists one,
   else by loading the table again. They are held in memory, and never written to disk.
2. **The catalog's own storage keys**, from `catalogs.toml` or its variables.
3. **This machine's credentials**, as an `s3://` read uses them, unless the catalog sets
   `storage-fallback = "none"`, which refuses the read instead. The catalog still says where the
   table is, its endpoint included, so set `none` for a catalog you don't trust with them.

`lakeleto info` and `/v1/info` say which one a read used (`vended`, `catalog` or `ambient`), and
the `/v1/schema` response carries it as `credentials`.

**Refused before any file is read:**
- a table whose catalog requires row filters or column masks (`read-restrictions`), which Lakeleto
  cannot apply yet;
- a table that requires server-side scan planning;
- a table whose catalog signs requests itself (`s3.remote-signing-enabled`) instead of vending
  credentials.

## Feature flags

`Cargo.toml` `[features]`. `default = []` — the pure-Rust local reader (Parquet, CSV/TSV, JSON
and Arrow IPC), which always compiles fast with no C++ toolchain.

| Feature | Turns on | Adds |
|---|---|---|
| *(default)* | `LocalReaderEngine` | Parquet (Snappy, gzip and LZ4 pages), CSV/TSV/JSON, Arrow IPC (LZ4 included) and gzip-compressed text reads (schema/head/profile/info/grid). |
| `sql` | `DataFusionEngine` (+ tokio) | Read-only SQL: the `query` command with `--engine sql`, and `POST /v1/query`. Pushes sort/filter/count into DataFusion. Reads Parquet, CSV/TSV and Arrow IPC natively; JSON and compressed text through the local reader, streamed a pass per query; Iceberg, Delta and Parquet directories through the local reader, loaded into memory for the query. |
| `iceberg` | self-contained Iceberg reader (apache-avro) | Read Iceberg tables: current-snapshot Parquet via metadata + Avro manifests, merge-on-read positional + equality deletes (scoped by sequence number, and by partition where the reader can prove two partitions differ; on a table with more than one partition spec, or with null or undecodable partition values, a delete applies anyway and can over-delete), schema evolution, statistics/partition pruning. Reads compressed manifests, and zstd data files, Iceberg's default since 1.4, through the zstd that reads the manifests. |
| `catalog` | Iceberg REST catalogs (reqwest + toml; implies `iceberg` and `object-store`) | `catalog://` tables read through the catalogs in [`catalogs.toml`](#catalogs-catalogstoml), with the credentials a catalog vends; `lakeleto catalog ls`; catalogs in the file browser. |
| `object-store` | BYO-credential `s3://`/`gs://`/`az://` reads (object_store + url + futures + tokio) | Every read op over a remote URI with *your own* env credentials, zero hosted compute. Ranged Parquet reads (footer + touched row groups); JSON and CSV streamed, a request per read, and a located JSON records member by its range. With `iceberg`, a table in a store is read in place: the current snapshot's metadata and manifests are fetched, and its data files are read by ranged requests. Nothing is copied to disk. |
| `serve` | `lakeleto serve` / `lakeleto open` (axum + rust-embed + tokio) | The HTTP/JSON `/v1/*` API and the embedded SPA (bundled via rust-embed — air-gapped). Add `sql` too for a working `POST /v1/query`. |
| `remote` | `RemoteEngine` → the over-HTTP seam (reqwest) | `--engine remote` / `--remote-url`; a client for another `lakeleto serve` (optional). Also enables `--workspace-remote` sync in `serve`. |
| `mcp` | `lakeleto mcp` (no new crate) | The MCP server for agents: [`mcp` tools](#mcp-tools) over stdio, read-only. Its tools read what the build's other features read. |
| `parquet-out` | Parquet output from the CLI (no new crate) | `-o parquet`, and `--out` to a `.parquet` file. Every build writes the Arrow formats (`-o arrow`, `-o arrows`); Parquet is a feature because its writer is about 530 KB of the default build. |
| `compression` | zstd, bzip2, xz and Brotli (zstd, bzip2, liblzma, brotli; zstd and xz are C) | `.zst`, `.bz2` and `.xz` text, zstd-compressed buffers in Arrow files, and Parquet pages compressed with zstd (Polars' default) or Brotli. gzip text, and Snappy, gzip and LZ4 Parquet, read in every build. With `sql` it adds no crate, as DataFusion links the same four. |
| `duckdb` | *(inert — no code)* | Nothing, and nothing is planned: the `sql` (DataFusion) engine covers what a DuckDB backend would. A no-op feature. |

The release binaries and the published image are built with
`--features serve,sql,iceberg,object-store,catalog,sqlite,postgres,mysql,delta,mcp,remote,parquet-out,compression`, and
the Windows and macOS installers add `desktop`. The container image the
[`Dockerfile`](../Dockerfile) builds from source has
`--features serve,sql,iceberg,object-store,catalog,mcp,compression`.

**Time zones.** A zoned timestamp prints in its column's zone. Every build prints UTC (`UTC`,
`Etc/UTC`, `Z`, `GMT` and the tz database's other names for it) and offsets such as `+02:00`. Any
other zone (`Europe/Paris`) needs the tz database, which `sql` links through DataFusion, so the
release binaries and the image print every zone; a build without `sql` refuses such a column in
text output, naming the zone. `schema`, and `-o arrow|arrows|parquet`, keep the zone the file has.

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
| `GET` | `/v1/engines` | The protocol version, what each engine does, the server's limits, and the endpoints it answers (see [below](#the-protocol-version-and-v1engines)). |
| `GET` | `/v1/schema?path=&format=` | Columns, types, nullability, row count. |
| `GET` | `/v1/info?path=&format=` | Format, engine, size, rows, columns. |
| `GET` | `/v1/preview?path=&limit=&format=` | First N rows (default 50) as `{ columns, rows }`, or Arrow IPC (see below). |
| `GET` | `/v1/profile?path=&scan=&format=` | Per-column null %, distinct, min/max (`scan=0` = Parquet footer fast path). |
| `GET` | `/v1/rows?path=&offset=&limit=&sort=&desc=&filter=col:op:value&cols=a,b` | Grid window: filter → sort → page → project. `limit` clamped to 10000. Arrow IPC on request (see below). |
| `GET` | `/v1/stats?path=&filter=col:op:value` | Column profile over the **filtered** view. |
| `GET` | `/v1/export?path=&fmt=csv\|tsv\|json\|ndjson\|parquet&sort=&filter=&cols=` | Current view as a download (row cap 1,000,000; byte cap 512 MiB → 413). |
| `GET` | `/v1/list?dir=` | File browser: subdirs + readable data files (defaults to `--root`, else cwd). |
| `POST` | `/v1/query` | `{ sql, file?, tables[] }` → `{ columns, rows }`, or Arrow IPC (see below). Needs `sql`, or over database tables `sqlite`, `postgres` or `mysql`. |

Every endpoint that reads a source also takes `json_path=` and `flatten=` (as the `--json-path` and
`--flatten` flags), and so does each entry of `/v1/query`'s `tables[]`.

Filter ops: `eq ne lt le gt ge contains notcontains startswith endswith in isnull notnull`
(aliases `= != < <= > >= ~ !~ ^ $`); `in` takes a comma-separated list (`status:in:new,open`).
With `sql`, sort/filter/count are pushed into DataFusion and are exact over the whole table; an
Iceberg or Delta table or a Parquet directory is read into memory whole for it first. A database
table's are pushed into the database's own SQL. Without `sql` the local reader works over a
bounded set (`scan_cap`, default 200k) and the response's `bounded` flag marks a partial view. Either way a window is the same on every request: unsorted rows come
in file order, and rows with equal sort keys in file order too, so consecutive pages meet
exactly. (With `sql`, a CSV or TSV is sorted by key and then by each row's place in the file, in
one pass. Any other table's sorted window that a tie touches is read a second time to guarantee
that, which costs a sort by a many-duplicates column about as much as a plain scan of the file. A
window of any table that reaches past row 50,000 is read in two passes instead, which keep a
sample of the rows and a band around the window rather than every row before it, so its memory
stays flat however deep it is. A JSON file is still decoded once, and a JSON, CSV or TSV object in
a store downloaded once: the first pass keeps the rows it reads in a temporary file, LZ4-compressed
in the system's temporary directory (`TMPDIR` moves it), for the second to read back — 120 MiB for
a 256 MB CSV — and the source is read again if that file can't be written. A Parquet object's
first pass downloads only the columns it sorts and filters by.)

#### The protocol version, and `/v1/engines`

The HTTP contract is versioned as `major.minor`, today `1.0`. The major is the path: a change
that would break a client is served under `/v2`, beside `/v1`, and doesn't change what `/v1`
means. The minor counts additions: it goes up when `/v1` gains an endpoint, a field or a
parameter, so a client written against `1.0` works against any `1.x` and ignores fields it
doesn't know.

The server sends its version on **every response**, as `X-Lakeleto-Protocol: 1.0`: answers,
errors, `401`s, `/healthz` and the page alike, so a client learns it from whatever it asked
first. A server that sends no version predates versioning (Lakeleto 0.3 and earlier), and speaks
a subset of `1.0`.

`GET /v1/engines` says what the server is and does:

| Field | |
|---|---|
| `version` | The server's Lakeleto version. |
| `protocol` | The contract it speaks, as above. |
| `engines` | Every engine it reads with, each with its `engine` name, the `formats` it reads, and whether it runs `sql`, `profile`s, is `remote`, answers grid windows (`scan`) and filtered `stats` (`filtered_stats`). The read engine comes first, then the SQL engine and the database engine when the build has them. |
| `engine` | The read engine's entry, also first in `engines`. |
| `sql_available` | Whether it runs SQL over files. |
| `ee` | Whether it is a Lakeleto Cloud (`ee`) build. |
| `limits` | The caps it enforces: export rows and bytes, query rows (and the default), workspace run rows, database connections per workspace (`null` for unlimited). |
| `endpoints` | The endpoints it answers. `POST /v1/query` is left out when no engine runs SQL. |

One set of rules, which the CLI shares, decides which engine answers a request: a database table
goes to the database engine, SQL to the SQL engine, a sorted or filtered `/v1/rows` window to the
SQL engine when it reads the table's format, and every other read to the read engine.

#### Result encoding — `Accept: application/vnd.apache.arrow.stream`

`/v1/preview`, `/v1/rows` and `POST /v1/query` answer **JSON by default**. A request whose
`Accept` contains the exact token `application/vnd.apache.arrow.stream` gets the same rows as
an uncompressed **Arrow IPC stream** instead (`Content-Type` echoes that media type).

| `Accept` | Answer |
|---|---|
| *(absent)* | JSON |
| `*/*` | JSON — this is what browsers and `fetch()` send |
| `application/json` | JSON |
| anything unrecognised | JSON (never a `406`; negotiation cannot fail) |
| `application/vnd.apache.arrow.stream` | Arrow IPC stream |

The Arrow body carries rows only, so the counts the JSON body states inline come back as
response headers — `X-Lakeleto-Offset`, `X-Lakeleto-Num-Rows`, `X-Lakeleto-Matched-Rows`,
`X-Lakeleto-Total-Known`, `X-Lakeleto-Scanned-Rows`, `X-Lakeleto-Bounded` on `/v1/rows`, and
`X-Lakeleto-Capped` on `/v1/preview` and `POST /v1/query`. Booleans are spelled `true`/`false`.
(They are headers, not Arrow schema metadata, so the schema a client decodes is exactly the
schema the server read.)

This is the codec the `remote` engine speaks, and the reason it can return rows at all: unlike
a JSON rendering it preserves a result's real Arrow types — an `Int64` beyond 2⁵³, a decimal, a
timestamp — and the columns of a zero-row window. The stream is uncompressed on purpose, so any
Arrow implementation can read it without a matching codec; the client refuses a *compressed*
stream for its own reasons (a compressed body declares the size it decompresses to, and that
declaration would size the client's allocation).

#### What `--remote-url` calls, and what it does not

The table above is the **server's** surface. The `remote` engine is a **client** for a subset of
it, pointed at any server that speaks the contract:

| trait method | request it makes | encoding |
|---|---|---|
| `schema` | `GET /v1/schema?path=&format=` | JSON |
| `profile` | `GET /v1/profile?path=&format=&scan=` | JSON |
| `preview` | `GET /v1/preview?path=&format=&limit=` | Arrow IPC |
| `query` / `query_capped` | `POST /v1/query` | Arrow IPC |
| `capabilities` | `GET /v1/engines`, once | JSON |
| `scan` | *(nothing — not implemented)* | — |

So `GET /v1/rows` is offered by the server and **not used by this client**: grid windowing
(filter → sort → page → project) over a remote engine answers `501`, and the grid is served by
the local and `sql` engines.

`--remote-url` points at **whatever speaks this contract** — a `lakeleto serve` you host, or a
hosted plane that serves part of it. Three things follow, and each is discovered the ordinary
way rather than configured:

- **A server may answer only some of these routes.** An unserved one replies `404` or `501`
  carrying the server's own message, and `schema`/`profile` are separate endpoints from the row
  methods, so a server can answer one and not the other.
- **The server resolves its own refs.** With `--remote-url` set the CLI does **not** resolve the
  path locally: it forwards the string opaquely and lets the peer decide what it names — a file
  path there, a catalog name, a table id. `?format=` is sent only when you passed `--format`;
  otherwise the server infers it. `--json-path` and `--flatten` travel the same way, as query
  parameters and on each table of a `query`.
- **A server may impose limits this contract does not name** (how many tables one query may
  register, which paths it will accept at all). Those arrive as an ordinary `4xx` with the
  server's own message.

The client's capabilities are the server's: the formats its read engine reads, whether it runs
SQL over files and profiles, from `GET /v1/engines`, asked once and kept. `scan` and filtered
`stats` stay off whatever the server does, since the client doesn't implement them. A server that
doesn't answer `/v1/engines` within 5 seconds, because it is down or doesn't serve that route, is
reported as capabilities unknown, with no formats and no SQL, and asked again next time; its other
routes are still called. `lakeleto engines --remote-url …` prints what the server says: its
version, its protocol, and a row per engine. It fails when the server can't say.

A response body over **256 MiB** is refused by the client (`Content-Length` where the server
declares one, and the read itself where it does not), and every call — metadata and rows alike —
reports the server's own `{"error": …}` message rather than a bare status line.

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
