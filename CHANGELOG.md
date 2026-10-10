# Changelog

All notable changes to Lakeleto are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-10-10

### Added
- **Compressed text: `t.csv.gz`, `t.ndjson.zst`, `t.tsv.bz2`, `t.json.xz`.** CSV, TSV and every
  JSON layout read as the format they hold, in the grid, `head`, `schema`, `profile` and SQL.
  Before, such a file was refused.
  - **gzip in every build**; zstd, bzip2 and xz with the new `compression` feature, which the
    release binaries, the image and the installers have. A build without a codec's decoder refuses
    the file up front, naming the feature.
  - **Decompressed as it is read**, so the first rows of a file in an object store arrive without a
    download. A JSON document's records member is read from the start each time: its offsets are
    the decompressed bytes'.
  - **`--max-decompressed`** (4 GiB unless set; env `LAKELETO_MAX_DECOMPRESSED`) stops a read that
    inflates past it with a `too large` error, 413 from `serve`: the guard against a small file
    that inflates without end.
  - A file whose bytes are not what its name says fails as such: `… is not valid gzip: …`.
  - The file browser lists compressed files, as the format they hold.
  - SQL reads a compressed file through the reader the grid uses, a pass at a time and under the
    same `--max-decompressed`: DataFusion's own decompression knows no limit.
- **Arrow IPC input: `.arrow`, `.feather`, `.ipc` files and `.arrows` streams.** What pyarrow,
  pandas, Polars and `-o arrow|arrows` write opens as a table; which framing a file has, its first
  bytes say.
  - **A file is read by its footer.** Its row count is exact, in `schema`, `info` and the grid's
    scrollbar, and a window reads only the record batches it covers: from an object store by
    ranged requests. What the footer says is kept per version of the file.
  - **A stream** is read front to back.
  - **Compressed buffers:** LZ4, pyarrow's Feather default, in every build, and zstd with
    `compression`. Feather v1 files are refused, with how to rewrite them.
  - SQL reads both framings through DataFusion. A file with no extension is recognised by its
    `ARROW1` magic.
- For the library: `Format::Arrow` is a new variant. `Source::require_uncompressed` is now
  `Source::require_decodable`, which refuses only a codec this build lacks. New are
  `Codec::decodable` and `source::{max_decompressed, set_max_decompressed,
  DEFAULT_MAX_DECOMPRESSED}`, and `cli::Cli` has a `max_decompressed` field.
- **Parquet and Arrow output for pipelines: `-o parquet`, `-o arrow`, `-o arrows` and `--out`.**
  `head`, `query` and `catalog ls` write their rows in a format the next program reads with its
  types: DuckDB, Polars, pandas, pyarrow or Spark.
  - **`arrow`** is the Arrow IPC file format (`.arrow`, Feather v2), and **`arrows`** the stream
    format, which a reader takes from a pipe:
    `lakeleto query … -o arrows | python -c '… pl.read_ipc_stream(sys.stdin.buffer)'`.
    **`parquet`** is Snappy-compressed. Lists, structs and maps stay nested, and `query` writes all
    three as the engine answers.
  - **`--out <file>`** writes to a file, and with no `-o` takes the format from its extension. The
    file appears only when the command succeeds: it is written under a temporary name beside the
    destination and renamed over it, so a failed run leaves no half-written file and keeps the one
    that was there. On Unix the temporary file is private, and the result keeps the permission
    bits of the file it replaces. `--out -` is stdout.
  - **Refused where they make no sense:** on a terminal, and on `schema`, `profile`, `info` and
    `engines`, which print descriptions rather than rows. Both are refused before anything is read.
  - **`-o parquet` needs the new `parquet-out` feature**, which the release binaries, the image and
    the installers have. Its writer is about 530 KB of the default build, against about 110 KB for
    the Arrow writers, which every build has.
  - For the library: `cli::Cli`'s `output` is now an `Option` (resolved by `Cli::output()`) beside
    a new `out` field, and `render::stream_rows` takes a `Send` writer, as Parquet's writer needs.
- **`lakeleto mcp`: an AI agent reads your tables (`--features mcp`).** An MCP client — Claude
  Code, Claude Desktop, Cursor — starts `lakeleto mcp` and speaks the Model Context Protocol on its
  stdin and stdout. Everything it can do is read.
  - **The tools.** `list` (a directory's tables, an object-store prefix, a database's tables, a
    catalog namespace); `describe` (columns and types, the row count, and for a Parquet file each
    column's null count, min and max from its footer, all without reading a row); `preview` (rows,
    from any offset, optionally only some columns); `profile` (column statistics from a bounded
    scan); `query` (one read-only SQL statement over the tables it names, with `sql` or a
    database driver); and `catalog_ls` (the configured Iceberg REST catalogs, with `catalog`).
    `tools/list` offers only the ones the build and flags can run.
  - **Bounded.** A call returns at most `--max-rows` rows (1,000) and `--max-bytes` of JSON
    (32 KiB), as arrays in column order rather than objects that repeat every name, and says
    `truncated` when it was cut. It is answered within `--timeout` seconds (30): the engines that
    can stop at the deadline do, and a call one doesn't stop is answered with an error when it
    passes.
  - **`--root`** confines it as it confines `serve`, and relative paths are taken from it.
  - **Refusals say what they are.** A refused call is a result with `isError` and a kind:
    `forbidden`, `not_found`, `query` (including a write the read-only guard refused), `deadline`,
    `invalid_arguments`, `unsupported`, …
  - **No new crate.** The protocol is a loop over `serde_json`: the `rmcp` SDK measured 0.9 MB
    larger in a release build. It speaks revisions 2024-11-05 to 2025-11-25, and a client of a
    later revision falls back to them. Calls run concurrently, up to 16 at once, a cancellation
    stops one, and a `ping` is answered during a long call.
  - The release binaries and the Docker image include it; the guide has configurations for Claude
    Code, Claude Desktop, Cursor and Docker.
- **`lakeleto::confine`**, the `--root` check, shared by `serve` and `mcp` so that they refuse the
  same paths with the same code.
- **A protocol version for the HTTP contract.** `lakeleto serve` speaks protocol `1.0`, and says
  so on every response, errors and refusals included, in an `X-Lakeleto-Protocol` header, and in
  the `protocol` field of `GET /v1/engines`. The major is the `/v1` in the path; a minor version
  only adds endpoints, fields and parameters. `lakeleto::protocol` holds the version and
  `compatible()`, which says whether a server's version is one this build speaks.
- **`/v1/engines` lists every engine.** Its new `engines` field has the capabilities of each engine
  the server reads with: the read engine, then the SQL and database engines when the build has
  them. `engine` stays, for clients that read only the read engine.
- **`lakeleto engines --remote-url …`** also prints what the server says it runs: its version, its
  protocol, and a row per engine.
- **`EngineRegistry`, which decides which engine answers what,** for the CLI and the API alike. A
  database table goes to the database engine, SQL to the SQL engine, a sorted or filtered grid
  window to the SQL engine when it reads the table's format, and every other read to the read
  engine. `EngineRegistry::local()` is this build's engines. `RemoteEngine::server()` returns what
  a server says about itself at `/v1/engines`.
- **Iceberg REST catalogs (`--features catalog`).** A table a catalog serves is named by
  reference, wherever a path goes: `lakeleto head catalog://prod/sales/orders`,
  `--table t=catalog://prod/sales/orders`, `?path=`, and the file browser.
  - **Configuration.** Catalogs are configured by name in `$LAKELETO_HOME/catalogs.toml`, with the
    keys the Iceberg REST clients use (`uri`, `warehouse`, `credential`, `token`,
    `oauth2-server-uri`, `scope`, `header.*`, `s3.*`, …), or entirely through
    `LAKELETO_CATALOG__<NAME>__<KEY>` variables, which win over the file.
  - **The protocol.** Lakeleto speaks the read side of it: `GET /v1/config` (with the warehouse,
    the route prefix and the namespace separator it answers), namespace and table listings
    (followed across pages, up to 10,000 entries), and `loadTable`.
  - **Logging in.** It logs in with a bearer token or OAuth2 client credentials, keeps the token in
    memory, and fetches a new one before it expires, or once when the catalog stops taking it.
  - **Reading.** A table is planned from the metadata `loadTable` hands over, so its metadata file
    is not fetched again.
  - **Credentials for the table's files.** They come from the catalog, which Lakeleto asks to vend
    them, matched to the table by the longest `storage-credentials` prefix. They are renewed
    through the catalog before they expire and never written to disk. Without vended
    credentials, the catalog's configured storage keys are used, then this machine's, unless the
    catalog sets `storage-fallback = "none"`. `lakeleto info` and `/v1/info` say which one a read
    used.
  - **Browsing.** `lakeleto catalog ls [catalog://…]` lists the configured catalogs, a catalog's
    namespaces, or a namespace's namespaces and tables, in every `-o` format, never printing a
    credential. `/v1/list?dir=catalog://…` lists the same for the file browser.
  - **Tables on the catalog's own disk.** A catalog that serves tables from its own disk, with
    `file://` metadata, is read from disk.
  - **What is refused.** A namespace, a table that does not exist and a misnamed catalog are each
    refused with what they are, and with the next step.
  - **Tested against Apache Polaris 1.8.0 and Lakekeeper 0.14.0**, each vending credentials for
    an S3 store. pyiceberg writes a table into each, and Lakeleto lists it, counts and queries it,
    and serves it to the grid, with the vended credentials alone.
- **A `Catalog` trait, for what a table's name refers to.** `Catalog::load_table` says what a name
  is, as a `TableHandle` (its location, format and codec), and `Catalog::list` says what lives
  under a name, in the shape the file browser shows. `PathCatalog` is its one implementation:
  Lakeleto's format detection and directory listing, moved behind the trait unchanged.
  `Source::detect_in`, `Source::resolve_in` and `list_dir` hand their work to it, and `serve`
  resolves and browses through it, so every path, object-store URI and database URI resolves as it
  did. Like detection, it lists an object-store prefix as the request's identity when the request
  carries one, and under `RemoteProbe::VendedOnly` it refuses to list one as the server. A catalog
  that serves tables by name plugs in as another implementation.

### Changed
- **`lakeleto engines` says what each backend is.**
  - The `duckdb` flag is shown as **inert**, not planned: it wires nothing, and nothing is planned
    for it.
  - The local engine is the **built-in reader**, in `lakeleto engines` and in `GET /v1/engines`
    alike. Its old label named three crates, which read as the formats it opens.
  - The object-store row lists every format it reads remotely: Parquet, CSV, TSV, JSON and Arrow.
- **The release binaries and the Docker image include the `remote` engine:** `--remote-url`, a
  client for another Lakeleto server, and `serve --workspace-remote`. The guide already said the
  release binaries had every engine, and they lacked this one. It adds no crate (`reqwest` is
  already in them for `catalog`) and 170 KB to a 45 MB binary.
- **The web grid scrolls through the whole table.** It showed a table's first 200 rows, sorted
  and filtered, and nothing past them. It now reads 200-row windows from `/v1/rows` as it
  scrolls, two at a time, draws only the rows in view over a scroll area sized to the table, and
  keeps 8,000 rows at most:
  - **Very tall tables.** A table taller than a browser can draw maps onto its scrollbar in
    proportion, and the wheel and the keys still move by rows.
  - **Unknown totals.** A CSV, TSV or JSON file's row count isn't known until its last window is
    read, so its scrollbar grows as you scroll and its footer says `≥ N`.
  - **Rows.** They are numbered, and a row's number opens it in Row detail.
  - **Failures.** A window that fails is asked for again after 3 seconds, then less often.
- **The CLI reads database tables.** `lakeleto head`, `schema`, `profile` and `info` read a
  `sqlite://`, `postgres://` or `mysql://` table with the database engine, and `lakeleto query`
  runs SQL over one on it, as `serve` already did. They used to hand it to the file reader or to
  DataFusion, neither of which reads a database. `--engine local` still runs no SQL.
- **`RemoteEngine` reports the server's capabilities.** The formats it reads, and whether it runs
  SQL and profiles, come from the server's `GET /v1/engines`, asked once and kept. It used to
  report `server-defined` formats and SQL whatever the server did. A server that doesn't answer
  within 5 seconds, or serves no `/v1/engines`, is reported as capabilities unknown, with no
  formats and no SQL, and asked again the next time.
- **`/v1/engines` offers `POST /v1/query` only when the server can run SQL:** on files with the
  `sql` engine, or on databases with a database engine. Without either, every query answered
  `501`.
- **`api::router` and `api::serve` take an `EngineRegistry`** in place of a read engine and
  optional SQL and database engines: `router(EngineRegistry::new(read).with_sql(sql), …)`.
- **A cancelled Iceberg read stops at its next batch.** It used to finish the data file it was in
  before checking the request's deadline and cancellation. From an object store each row group is
  a request, so a read nobody wanted any longer kept downloading the rest of the file.
- **An Iceberg table in an object store is read in place.** It used to be copied whole to a private
  temporary directory on its first read in a process: every object under the table's prefix,
  earlier snapshots and files no snapshot names included. Now:
  - the current snapshot's metadata, manifest list and manifests are fetched, a `GET` each, and
    nothing else the table has written;
  - data files are read by ranged requests, as Parquet objects are, so a page of the grid reads
    one footer and one row group;
  - the table's row count is the sum of what its manifests record for each file (a footer counts
    any file a manifest leaves out), so counting it opens no file. Local tables are counted the
    same way;
  - nothing is written to disk.

  On a 299 MiB table (8 data files of 500,000 rows, plus an old snapshot and an orphaned file) at
  31 MB/s with 20 ms per request, the first page of a fresh server took 12.8 s and 299 MiB of
  downloads, and now takes 0.7 s and 6 MiB; its schema and row count went from 12.7–12.8 s to
  0.4 s.
  The cost: every read now goes to the store, where the copy served reads after the first from
  disk. A later page takes 0.4–0.8 s rather than 0.05 s, and SQL over the table reads the current
  snapshot's data files on every query (7 s at 125 MB/s, where the copy took 0.7 s once it was
  made).

### Removed
- `objstore::materialize_prefix`, `objstore::materialize_prefix_with` and `iceberg::plan_object`,
  which made and read that copy.

### Fixed
- **`lakeleto info` prints what `-o` asks for.** It printed its `name : value` lines whatever
  `-o` said, so `info -o json` gave a script text it could not parse.
  - **`-o json`** is the object `GET /v1/info` returns: `path`, `format`, `engine`, `size_bytes`,
    `row_count`, `columns` and, for a catalog table, `credentials`. What is not known is `null`.
    The CLI and the API now build it from one type, so the two cannot drift apart.
  - **`-o ndjson`** is the same object on one line.
  - **`-o csv` and `-o tsv`** are a header and one row, with the same columns for every
    source, so the rows for many files line up under one header.
  - The default output is unchanged, and the binary formats are still refused.
- **Polars' Parquet reads without `sql`.** Polars compresses Parquet with zstd unless told
  otherwise, and every build without `sql` failed such a file's rows with "Disabled feature at
  compile time: zstd": the default build, whose Parquet reader decompressed Snappy only, and a
  build with `compression`, which read zstd text but not zstd Parquet. The release binaries, which
  have `sql`, read it all along.
  - **Snappy, gzip and LZ4 pages read in every build**, LZ4 in Hadoop's framing and the raw one,
    through the gzip and LZ4 decoders every build links for compressed text and Arrow files: no new
    crate, and about 48 KB.
  - **zstd and Brotli come with `compression`**, which the release binaries, the image and the
    installers have. `iceberg` reads zstd too, with the zstd it links for Avro manifests: Iceberg
    writes its data files zstd-compressed by default since 1.4. In the default build, zstd measured
    eight crates, over its crate budget, and Brotli 1.1 MB, over its size budget.
  - **A build without one refuses the file naming the feature**, where it used to fail in parquet's
    words, which name a feature of parquet's: "this Parquet file's pages are zstd-compressed, and
    this build decompresses Snappy, gzip and LZ4 only: zstd needs the `compression` feature
    (`cargo install lakeleto --features compression`), which the release binaries and the image
    have". It is an `unsupported format` error: 400 from `serve`, as before, and
    `unsupported_format` to an MCP client. `schema`, `info` and `profile --fast` read only the
    footer, and read such a file as before.
- **A timestamp in `UTC` prints in every build.** `timestamp[us, tz=UTC]` is what pandas, pyarrow
  and Polars write for tz-aware UTC data, and what the Parquet reader calls any column a file marks
  as adjusted to UTC, as DuckDB, Spark, Trino and Iceberg write them. The default build, which has
  no time zone database, printed such a column blank in `head`'s table, failed `-o csv`, `-o tsv`
  (after the header), `-o json` and `-o ndjson` with "only offset based timezones supported without
  chrono-tz feature", and profiled it with no min or max and one distinct value. Every build
  without `sql` failed the same way in the grid's `/v1/rows`, in its filters, and on an Iceberg
  equality delete keyed on a `timestamptz` column.
  - **A name of UTC prints as the offset `+00:00`:** `UTC`, `Etc/UTC`, `Z`, and the tz database's
    other names for its two zero-offset zones (`GMT`, `Zulu`, `Universal`, …). The values are UTC
    instants already, so every build prints them exactly as the release binaries do:
    `2024-01-02T03:04:05.123456Z`. `Z`, which no build could print, prints in all of them.
  - **Another named zone, such as `Europe/Paris`, needs the database.** The release binaries and
    the image have it, through DataFusion, and print the zone's own time as before. A build without
    it now refuses the column before writing anything, naming the zone and the builds that print it.
    The database measured 1.9 MB and four crates in the default build, over its size budget, and
    printing the instant in UTC instead would show a time the column does not hold.
  - Only text changes: `schema`, and `-o arrow`, `-o arrows` and `-o parquet`, keep the zone the
    file names.
  - For the library: `engine::profile_columns` returns a `Result`.

### Security
- **Catalog logins stay off the network and out of logs.** A catalog is reached over `https`, or
  over plain `http` only to this machine, and a login never follows a redirect. Tokens and
  vended credentials live in memory only, and are never printed in an error, a `Debug` value or
  `lakeleto catalog ls`. On Unix, Lakeleto warns when `catalogs.toml` holds a secret other users can
  read.
- **A table a catalog restricts is refused, not read in full.** Lakeleto refuses, before reading a
  file, a table whose catalog requires row filters or column masks it cannot apply
  (`read-restrictions`), server-side scan planning, or remote request signing.
- **`--root` refuses catalog references**, as it refuses object-store URIs, before anything reaches
  a catalog.
- **A table in an object store can no longer read a file on this machine.** When the table was
  copied to disk to be read, a manifest naming a local path or a `file://` URI was followed onto
  the local disk. Such a table is now refused with a 403, before the file is opened.

## [0.3.0] - 2026-10-03

### Added
- **A JSON document's records member unwraps to its rows.** `{"meta": {…}, "data": [{…}, …]}` —
  an API response, a GeoJSON `FeatureCollection` — used to be one row with a list column (or,
  pretty-printed, to fail outright). When a single top-level object has exactly one member holding
  a non-empty array of objects, its elements are the rows; `lakeleto schema` prints the member
  (`records: /data`) and `/v1/schema` returns it as `records_path`, so the unwrap is never silent.
  With no such member, or several, the object is still one row. The member is found once per file
  version, as the byte range of its array, and its records then stream like a top-level array's —
  sampled schema, widening on drift — so a grid window does not parse the document again.
  `.geojson` is now read as JSON.
- **`--json-path` / `?json_path=` choose the records explicitly**: a JSON Pointer
  (`/response/items`) or a top-level member name (`groups`), for documents detection cannot
  settle — two candidate arrays, records below the top level — and `""` to unwrap nothing. It
  travels to a `--remote-url` server, is accepted on every `/v1/*` read route and on workspace
  runs, and is refused on a source that is not JSON instead of being silently ignored.
- **The web app reads JSON at a records path.** A JSON tab's toolbar says where its rows come
  from — the member the reader detected (`Records: /data · auto`), a path set for the tab, or the
  whole document — and sets it as `--json-path` does: a JSON Pointer or a member name, **Whole
  document**, or **Auto** to detect again. The path is kept with the tab, applied to its SQL,
  saved with a saved query (`json_path`, like `flatten`) and restored when that query or a run
  from history is reopened, so SQL written against the records finds them again.
- **`--flatten[=levels]` / `?flatten=` read struct columns as one column per field**, named by its
  path — `user: {name, geo: {lat}}` becomes `user.name` and `user.geo.lat` — so nested data sorts,
  filters, profiles and exports like flat data, and SQL can name a field (`"user.name"`). `--flatten`
  is every level, a number that many. It works for any format with structs (JSON, nested Parquet,
  Iceberg, Delta): the local engine flattens as it reads, and the SQL engine through a view, so the
  file still streams and a sorted grid window still covers all of it. A field of a null struct is
  null in both — DataFusion's `get_field` alone would show a value stored beneath one. Lists, maps
  and empty structs stay whole, and two columns that would share a name are refused. The grid
  gains a **Flatten structs** toggle, kept with the tab and applied to its SQL.
- **SQL reads JSON.** `lakeleto query`, `POST /v1/query` and workspace runs accept `.json` /
  `.ndjson` / `.jsonl` / `.geojson` tables. They are read by the local JSON reader, not DataFusion's
  — which infers line by line and fails on pretty, wrapped or BOM-prefixed files — so every layout,
  `--json-path`, `--flatten` and schema widening apply in SQL as in the grid; a parity test reads
  each layout through both engines and gets the same schema and rows. A file is streamed into the
  query a batch at a time, one pass per query — read from disk, or for a file in an object store,
  requested afresh by each pass — so a query holds the batches in flight rather than the file:
  `LIMIT 10` stops reading once it has its rows, and a `count(*)` or `GROUP BY` over a large file
  runs in memory set by the query, not by the file. A value past the sampled rows that does not fit
  their column types widens the file's schema, and the query is planned again with it — unless it
  was already streaming rows to its caller; then it stops with an error, and the next run reads the
  value. Sorted and filtered JSON grid windows now go through SQL as well, so they cover the whole
  file rather than its first 200,000 rows; a sorted one counts what matches in the pass that sorts
  it, so it reads the file, or downloads the object, once.

### Changed
- **JSON and CSV in an object store stream.** An `s3://` / `gs://` / `az://` JSON, CSV or TSV file
  was fetched whole for every read — each grid window, schema and preview downloaded all of it. It
  is now read as its bytes arrive, as a local file is: a window requests the object and stops the
  transfer at its last row. A CSV read is one request: the rows its schema is inferred from are the
  first it decodes, so their bytes are kept as they arrive rather than downloaded again. A JSON
  records member is fetched by its byte range once located, and a JSON object's layout and schema
  are kept per version (its ETag) and per credential identity, so the next read goes straight to its
  rows. Every request asks for the version its read looked up, so an object replaced mid-read fails
  that read, with a message to run it again, rather than mixing two versions. A read that outlasts
  the store client's request timeout — a large object, or a streamed result its consumer reads
  slowly — asks again for what is left, of the same version. A connection that drops mid-read and
  cannot be resumed fails the read as the I/O error it is, rather than being taken for a malformed
  record. Over 2 million rows served by a local S3 stand-in, a grid window went from 0.7–0.9 s to
  0.2 s as a 255 MB NDJSON, and from 0.6 s to 0.03 s as a 256 MB CSV, with the server's peak memory
  down from about 510 MiB to 25–26 MiB for both.
- **A sorted grid window over a CSV is one pass over the file, tied or not.** With `sql`, a sorted
  window over a CSV or TSV file was counted by a query of its own — a pass over the whole file, and
  for an object in a store a download of all of it — before its parallel probe read the file again
  to sort it, and a tie that touched the window sent it to a third read on one partition, to break
  ties in the file's order. Now the scan numbers each row as it reads it, by partition and by place
  within one — DataFusion lays a file's partitions out in order, and the scan is kept to each
  partition's own ranges — so the parallel sort breaks ties by that number itself, and the TopK that
  keeps the best rows takes in every one that matches, so how many it took in is the count. Parquet
  keeps its probe and its count: its scan skips row groups by the threshold the TopK sets, which
  numbering its rows would cost it on every sort. Over the 256 MB CSV in a local S3 stand-in, every
  sorted window is now one download of the object, where an untied one was two and a tied one three.
  Against the version that already counted as it sorted, a tied window went from 1.3–1.6 s to 0.4 s
  and a window at row 1,000,000 from 9 s to 2.1 s; on a local file, a tied window from 1.4 s to
  0.3 s.
- **A sorted grid window deep into a CSV no longer holds every row before it.** With `sql`, a sorted
  window over a CSV or TSV kept the best `offset + limit` rows in every partition until its one pass
  was through, and every batch they came in, so the deeper the window the more memory: at row
  1,000,000 of the 256 MB benchmark CSV the server peaked at 705–781 MiB, and a window in the middle
  of a bigger file kept more. A window that reaches past 50,000 rows is now read in two passes. The
  first counts the rows that match and samples 65,536 of them by a hash of their place in the file,
  which bounds where the window falls; the second keeps only the rows between the bounds, about 2.3%
  of them, and cuts the window from those. Its rows and count are the one-pass read's, and should
  the bounds miss the window, about one time in a billion, it is read the one-pass way. At row
  1,000,000 of a local file the server now peaks at 82 MiB, and the window takes 0.57 s, from
  1.7–1.8 s. An object in a store is still downloaded once: the first pass writes the rows it reads
  to a temporary file, as a JSON file's does (below), for the second to read back, and the object is
  downloaded again only if that file can't be written. From a local S3 stand-in the window takes
  1.3–1.6 s, from 2.4 s, and peaks at 118–135 MiB, from 684–717 MiB, with 116–120 MiB on disk.
  A window whose filter keeps few rows can take longer than it did: with a filter that keeps an
  eighth of the rows, at row 100,000, 0.46–0.48 s from 0.34–0.36 s locally, at 75 MiB from
  118–119, and 0.56–0.62 s from 0.48–0.50 s over the stand-in.
- **A sorted grid window deep into Parquet or JSON no longer holds every row before it either.**
  With `sql`, a window that reaches past 50,000 rows into any table now takes the two passes a
  CSV's does. A Parquet file's rows are numbered as its parallel scan reads its row groups, and a
  filter still skips the row groups it rules out; a JSON file's, a flattened view's and a table's
  read into memory are numbered on one partition. Over the 2 million benchmark rows as a 98 MB
  Parquet file, at row 1,000,000, the server peaks at 142–168 MiB, from 1.1 GiB, and a window
  sorted by a key with ties takes 0.4–0.5 s, from 8.6–9.6 s, as a tie no longer sends it to a
  second read on one partition. A JSON file is still decoded, and downloaded from a store, once:
  its first pass writes the rows it reads to a temporary file, LZ4-compressed Arrow in the system's
  temporary directory, removed once the window is read, and the second pass reads them back. Should
  that file fail to be written, the second pass decodes the file again. As a 255 MB NDJSON file, at
  row 1,000,000, the server peaks at 52–55 MiB, from 349–352 MiB, and the window takes 3.0–3.2 s,
  from 7.4–7.9 s, with 86–88 MiB on disk; sorted by `id`, 2.9–3.2 s from 3.1–3.4 s, and with a
  filter that keeps an eighth of the rows, at row 100,000, 2.4–2.6 s from 2.5–2.7 s.
- **A JSON file's schema no longer depends on how far you scrolled.** Inference sampled 1,000
  values for `schema` but the whole window for a read, so a key first seen at row 1,200 was
  missing from `schema` yet present in a window past it, and a column could change type between
  windows; an unbounded read (SQL registration, `query`) sampled only 1,000 rows and failed on a
  later drift. Every read now uses the same first 20,000 values, remembered per file version
  (about 0.1 s on a 60 MiB file, paid once per version in `serve`), and a value past them that
  disagrees widens the file's schema instead of failing the read. Sampling streams, so the
  200,000-row sort/filter working set no longer holds its sample in memory as parsed values.
- **Every non-Parquet format is read through one registry.** CSV, TSV and JSON are
  `FormatReader`s in `src/format/`; the local engine has one arm for them where it had a CSV and a
  JSON branch in five places, and the format lists `GET /v1/engines` reports, SQL routing and
  extension detection are read off the registry. No behaviour change — the seam new formats plug
  into.
- **A compressed file is named, not misread.** `t.csv.gz` failed detection as an unknown
  extension, and with `--format csv` it was read as a table of compressed bytes. The last extension
  is now recognised as a codec (`gz`, `zst`, `bz2`, `xz`) over the format before it, and every
  read — local, SQL — refuses it with an error that says it is compressed. The file browser keeps
  hiding such files until they can be read.
- **JSON columns come in the order their keys first appear**, not alphabetically, nested fields
  included (`serde_json`'s `preserve_order`; `indexmap` was already in the tree).
- **Top-level JSON arrays stream.** An array used to be parsed whole and was refused above
  256 MiB; it is now fed element by element into the decoder, so it is bounded by the row window
  like NDJSON. An array cut off before its closing `]` is an error rather than a shorter table.

### Fixed
- **Nested columns work in the grid.** A struct cell showed `[object Object]` (and a list its
  elements comma-joined); sorting a struct column failed with "Sort not supported for data type
  Struct", and filtering any struct column failed because Arrow cannot cast a struct to text. Cells
  now show compact JSON (pretty-printed in the row drawer, and in copies, CSV downloads, the quick
  search and generated `WHERE`/`INSERT` too), nested columns get a wider default width, and
  sort/filter work on the same JSON text — structs and maps sorted by it, lists still sorted natively.
- **JSON files that failed to open now read.** A column whose values disagree in type
  (`{"v": 1}` then `{"v": "a"}`) failed on the first row that differed; it now reads as text.
  Pretty-printed objects and `jq` output failed in schema inference, which read lines rather than
  values; it now reads values. A UTF-8 byte-order mark failed the decoder; it is skipped. The
  reader lives in the new `src/format/json.rs`.
- **Sorting or filtering a JSON file in the grid works in `sql` builds.** `/v1/rows` sent every
  sorted or filtered window to the SQL engine whenever one was compiled in — the release binaries
  and the Docker image — and that engine cannot register JSON, so sorting or filtering *any*
  column of a `.json`/`.ndjson`/`.jsonl` file failed with "the `sql` engine cannot read json
  sources yet"; `/v1/export` of a filtered view failed the same way. A window now goes to the SQL
  engine only when its capabilities list the source's format, and the local reader's Arrow-kernel
  sort/filter answers the rest. The SQL engine's capabilities — and its `lakeleto engines` row —
  are now derived from the same predicate `register` refuses by, so the list the API routes on
  cannot drift from what the engine accepts.
- **Paging a sorted or filtered grid through SQL no longer repeats or skips rows.** DataFusion's
  parallel plan guarantees no order beyond the `ORDER BY`: a filtered window came back in
  whatever order its partitions finished, and a sorted one left rows with equal keys to timing —
  and even on one partition each page's `LIMIT` ordered them its own way. So the same window
  differed between runs, and the next page, a fresh query at a new offset, could overlap the last
  (14 of 100 rows, sorting a 5-million-row CSV by a three-value column). A filter-only window (and
  a filtered export) now runs on one partition, in the file's order. A sorted window is taken
  from the parallel plan with one row either side, and kept when no tie touches it — then it is
  the only right answer, so a sort by distinct values (an id, a timestamp) costs what it did.
  When a tie does touch it, it is read again with ties broken by position in the file: on 5
  million rows and 4 cores, ~1.2 s for a 146 MB CSV and ~0.5 s for Parquet, where the old,
  unstable answer took 0.3 s and 0.05 s.
- **The local engine orders tied sort keys as they were read.** Arrow's sort is unstable, so ties
  came out in an order of its own — the same on every request, but not the SQL engine's, so a
  lean build and a `sql` build showed a tied sort differently. Both now use file order.
- **A remote query's tables keep their read options.** `RemoteEngine::query` sent each table's
  name, path and format only, so a table read with `--json-path` reached the server without it.
  Tables now carry the same keys a single-source call sends (`json_path`, `flatten`).
- **CSV and TSV exports take nested columns.** Arrow's CSV writer refuses a list, struct or map
  column outright, so a view holding one — a JSON document read whole, a list of tags, a query
  that returns an array — failed to download as CSV or TSV with "Nested type List(…) is not
  supported in CSV", from `/v1/export` (the web app's **Download view**) and from
  `-o csv` / `-o tsv`. Each nested cell is now the compact JSON the grid shows for it, the form
  the web app's in-browser CSV already used, and a null is an empty cell; other columns are
  written as before.

## [0.2.0] - 2026-09-10

### Added
- **`--remote-url` forwards the path instead of resolving it, and a global `--format` was added.**
  Every read command used to call `Source::detect` *before* it picked an engine, and `detect` is
  local: it stats the path, walks directories and sniffs magic bytes. So a ref only the far side
  could interpret was resolved against your own filesystem, failed there, and never reached the
  network — `lakeleto head "some://server-ref" --remote-url https://server` died with
  `io error: No such file or directory`.

  The rule now is **a remote engine resolves its own sources** (`Source::unresolved`): when the
  read is going out over HTTP, the string is forwarded verbatim and the server decides what it
  names. Nothing in this crate knows what any particular ref means, which is the point — a plain
  path, a catalog name and a table id all travel the same way.

  `--format` (global) is the one thing a caller can still assert, on either engine; without it a
  remote request now sends **no `?format=` at all**, so the server infers the format the same way
  it would for its own paths. Guessing one would have been worse than sending nothing: `format`
  is an explicit *override* in this contract, so a client's guess becomes a server's instruction.
  The rule follows the selected engine rather than the mere presence of a URL, so
  `--engine local --remote-url …` still reads locally. `tests/cli_remote.rs` pins it.
- **The remote engine returns rows.** `RemoteEngine::preview` and `query`/`query_capped` were
  stubs that answered with an error string, so `--remote-url` got schema and profile but never
  data — from *any* server. They are implemented now, over a new Arrow-over-HTTP result codec,
  so `--remote-url <a lakeleto serve>` is a working read path rather than a wired-up seam.

  Scope, stated plainly: the peer is **any server that speaks this contract** — a self-hosted
  `lakeleto serve` (your workstation talking to a box that can see a lake your laptop cannot),
  or a hosted plane serving part of it. Partial service is normal and needs no configuration: a
  route a server does not answer comes back as a `404`/`501` carrying that server's own message,
  and `schema`/`profile` are separate endpoints from the row methods, so a server can answer one
  and not the other.

  The three row-returning endpoints (`GET /v1/preview`, `GET /v1/rows`, `POST /v1/query`) gained
  content negotiation: they answer **JSON by default**, and an uncompressed **Arrow IPC stream**
  when the request's `Accept` names the exact token `application/vnd.apache.arrow.stream`. Every
  other `Accept` — absent, `*/*`, `application/json`, anything unrecognised — is JSON, which is
  why the SPA needed no change; and there is no `406`, since the codec is always compiled and
  JSON is always a valid answer. The Arrow body carries rows only, so the counts the JSON bodies
  state inline travel as `X-Lakeleto-*` response headers (`Offset`, `Num-Rows`, `Matched-Rows`,
  `Total-Known`, `Scanned-Rows`, `Bounded`, `Capped`) rather than as Arrow schema metadata, which
  would have altered the schema a client decodes.

  Arrow rather than JSON because a JSON body is a *rendering*: an `Int64` past 2⁵³ rounds, a
  decimal or timestamp arrives as something the client has to guess at, and a zero-row window
  loses its columns entirely. The remote engine now gets back the same `RowBatch` the local
  reader produces, so everything above the `Engine` trait behaves identically no matter who read
  the bytes. Parquet was considered for the wire and deliberately kept where it is: it remains
  the at-rest result-cache codec (seekable, compressed, written once, windowed many times),
  while the wire wants a framed stream with no footer to seek back to. The stream is written
  uncompressed so any Arrow implementation can read it without a matching codec — the windows
  are already row-capped, so there is little to save.

  `arrow-ipc` is now a named dependency. It was already compiled in the default build (`parquet`
  pulls it in for its `arrow` feature), so this is a nameability change, not weight.

  Both ends of the codec are guarded rather than trusted. `to_arrow_ipc` refuses a `RowBatch`
  whose batches do not match its declared schema: `StreamWriter::write` performs no schema check
  of its own (unlike the Parquet writer next door, which does), so a divergent batch would be
  encoded *positionally* and answered with a `200` carrying silently mislabelled columns. The
  check compares `(name, data_type)` per field and ignores both metadata and nullability: neither
  changes how a byte is interpreted, and both legitimately differ between a DataFusion result's
  declared schema and the batches its plan emits — a `UNION ALL` mixing a `NOT NULL` source with
  a nullable one is the standard case, and comparing nullability rejected it.
  `from_arrow_ipc` refuses a **compressed** stream: writing uncompressed
  is a decision about what we send, and says nothing about what a peer frames — a compressed body
  declares its own decompressed length, and that declaration would size this client's allocation.
  `arrow-ipc` 58 has no reader-side switch for this, so the stream's framing is walked and each
  message's `BodyCompression` read before anything is decoded.

  `RemoteEngine` also bounds what it will buffer: a response over 256 MiB is refused, on the
  declared `Content-Length` where there is one and on the read itself where there is not. The
  body is still read before the status is acted on — deliberately, because the server's error
  message lives *in* that body — but it can no longer be unbounded. Every call now goes
  through that one path, so `schema` and `profile` surface the server's `{"error": …}` body the
  way the row calls always did — a wrong `--remote-token` reports what the server said about the
  token, not a bare `401`.

  Still deferred: `RemoteEngine::scan` (the grid's filter → sort → window over `GET /v1/rows`).
  The server already speaks the Arrow arm there; what is missing is the request half, which needs
  an exact inverse of the filter parser — separate risk, separate change. Until it lands, a
  remote engine has no grid windowing: `scan` answers `UnsupportedOperation` (a `501` through the
  API layer).

- **JSON is now a readable format.** `.json`, `.ndjson`, and `.jsonl` were advertised as openable
  but no engine read them, so every click was a guaranteed error. The engine now reads them via
  `arrow-json` (already in-tree for the NDJSON export, so no new dependency): newline-delimited
  JSON streams row by row, and a top-level `[...]` array is normalised to newline-delimited first,
  so both shapes read the same — for local files and, under `object-store`, for remote (`s3://`
  etc.) objects, mirroring how remote CSV is fetched-then-read. `/v1/engines` advertises `json` in
  the readable-format list. A bracketed array cannot be line-streamed, so it is buffered in full
  before the row window applies; to keep a single `preview?limit=50` from exhausting memory on a
  huge array, arrays over 256 MiB are rejected — with an error pointing at the newline-delimited
  form — before any allocation (checked against the file size locally, and the fetched length
  remotely). Newline-delimited JSON has no such limit: it streams, bounded by the row window.

- **Double-click installers for Windows and macOS.** Getting started used to mean
  download a zip, extract it, open a terminal in the right folder, and type a
  command — a sequence the README had to preface with *"double-clicking the file
  won't work"*. It now means downloading `lakeleto-<version>-x64.msi` or
  `Lakeleto-<version>.dmg` and double-clicking it.

  The `.msi` installs per-user, which is what keeps it free of a UAC elevation
  prompt: Lakeleto is a personal read-only viewer, not a service, so it has no
  business writing to Program Files. It adds a Start Menu entry, and puts the CLI
  on PATH so the terminal audience loses nothing. The `.dmg` is a universal
  build — one download, no asking people whether their Mac is Intel or Apple
  Silicon — presented as the usual drag-to-Applications window.

- **`lakeleto-desktop`, the launcher behind those entries** (`--features desktop`).
  It is what makes the icon work, and it exists because a GUI launch cannot assume
  any of the things `serve` gets from a terminal. It has no console window, opens
  the browser itself, and lives in the tray / menu bar, because a process with no
  console and no window is one the user otherwise cannot quit. Fatal errors go to
  `$LAKELETO_HOME/launcher.log`, since a windowless process reporting to a stderr
  nobody is attached to makes "I clicked it and nothing happened" unanswerable.

  It takes **a port the OS says is free**, never a well-known one. `8080` is among
  the most contended ports on a working machine — a spare Tomcat, a
  `python -m http.server`, a colleague's dev server — so a launcher that wants it
  is one that regularly cannot have it, and there is no terminal in which to
  report the bind failure. Binding to port 0 asks the kernel for a port nothing is
  using, which is by construction the least contended choice available. The
  consequence is that the URL differs between runs, so the running instance
  publishes its port to `$LAKELETO_HOME/desktop.port` and a second double-click
  finds it there and reopens the tab, rather than starting a rival server that
  would silently split the user's workspaces across two stores. The record is
  treated as evidence rather than truth — a killed launcher leaves a stale one —
  so the port is confirmed with a `GET /v1/engines` probe before it is reused.
  `lakeleto serve` is unchanged and still defaults to `8080`.

  Shipped as a separate binary, and off by default: it links a tray icon and an
  event loop, and the CLI's pitch is a lean static binary that also runs in a
  locked-down CI runner. Nothing here reaches a default build.

- **The Strata mark as icon art, generated rather than committed.** `src/icon.rs`
  writes PNG and multi-size ICO with no image dependency — PNG mandates zlib but
  permits *stored* (uncompressed) deflate, which costs a few KB per icon and saves
  a compression crate. `cargo run --features serve --example gen_icons` emits the
  Windows `.ico` and the macOS `.iconset` from the same geometry the tray icon
  draws, so the mark has one definition. The output is ~7 MB and is regenerated in
  CI rather than committed.

- **The tray menu can put the CLI on your PATH, and can tell you where it is.**
  Both installers ship `lakeleto` beside the launcher, but nothing in the
  installed experience said so — the Start Menu entry and the `.app` open a
  browser tab, and that was the whole story a user got.

  **Copy CLI path** (both platforms) puts the full path on the clipboard, via
  `pbcopy` / `clip` / `wl-copy` / `xclip` rather than a clipboard dependency.

  **Install command line tool…** is macOS-only, because the `.msi` already adds
  its install directory to the user's PATH and a menu item that answers "already
  installed" forever is worse than no menu item. A `.dmg` has no install step to
  run, and `Lakeleto.app/Contents/MacOS/` is on nobody's PATH, so this links the
  bundled binary into `/usr/local/bin` — chosen because it is in `/etc/paths` and
  therefore works in a new shell with no profile editing — falling back to
  `~/.local/bin` when that is root-owned, with the `export PATH` line the user
  then needs. It resolves the CLI as the running launcher's sibling, so moving or
  renaming the `.app` does not break it, and it replaces a stale symlink from an
  older location but **refuses to overwrite a real file**: that is almost
  certainly Homebrew's `lakeleto`, and clobbering someone's package manager is
  not a menu item's business. From a menu rather than on first launch, because
  symlinking into a shared bin directory unasked is not something a table viewer
  should do merely because it started.

- **`--workspace-home`.** The flag was documented in a code comment and did not exist,
  so two servers on one machine always shared `$LAKELETO_HOME` — including the tray
  launcher and a `serve` you started yourself. It is mutually exclusive with
  `--workspace-remote` rather than layered: the store is either over there or in a
  local directory, and silently picking one would put workspaces somewhere nobody asked.

- **Export as NDJSON and TSV** (`?fmt=ndjson` / `?fmt=tsv`), alongside CSV, JSON and
  Parquet. NDJSON is one object per line, which is what log and ETL pipelines consume;
  TSV pastes into a spreadsheet without the delimiter guessing that trips CSV up on any
  column containing a comma. `--output tsv` works on the CLI for the same reason.
  (Arrow IPC was considered and deliberately left out: it needs a new dependency and
  would pre-commit the result wire codec that the remote engine still has to choose.)

- **`GET /v1/info` reports a size for `s3://` / `gs://` / `az://` objects**, via one `HEAD`.
  It answered from the filesystem and returned nothing for a remote URI, which reads as
  "unknown" when the number is one cheap metadata request away. Still `None` when the URI
  is a prefix rather than an object, or the store will not say — a missing size is a
  cosmetic gap and failing the whole `info` call over it would not be. HTTP surface only:
  the CLI `lakeleto info` still sizes via the filesystem (the rest of the remote dataset
  story is queued as `remote-dataset-io`).

- **`GET /v1/engines` reports the limits it enforces** — export row and byte caps, the
  query cap and its default, the workspace-run cap, and how many databases a workspace
  may connect to at once (`null` = unlimited, which is what an EE build reports). A
  client that knows a bound can show it before someone hits it, rather than explaining a
  rejection afterwards. The database cap in particular used to be a literal in the
  frontend bundle, so changing an edition's terms meant rebuilding the frontend; the
  SPA now reads the server's number and keeps the old one only as a fallback for an
  older server. A test asserts the advertised query cap is the one actually applied.

- **Copy a row as a SQL `WHERE` clause or an `INSERT`**, next to the existing copy-as-JSON
  in the row-detail drawer. Looking at one row is usually the step before mentioning it
  somewhere else — in a ticket, a query, a fixture. String literals have their quotes
  doubled and nulls become `IS NULL` / `NULL` rather than the string "null". The drawer
  now says which form reached the clipboard, because three buttons that all silently
  succeed look exactly like three that silently fail.

- **A usage summary above the run history**: runs (and how many failed), rows read, total
  time, and the slowest successful run. Computed from the history already on screen — no
  new endpoint, no new state. A list of runs cannot answer "is this getting slower, and
  which one is the slow one", which is the question people open history to ask. Failures
  are excluded from "slowest" because a failed run's duration measures how long it took to
  give up.

- **Six more grid filter operators** — `does not contain`, `starts with`, `ends with`,
  `is one of a list`, `is null`, `is not null`. In the filter box: `!~`, `^`, `$`,
  `in:a,b,c`, `null`, `!null`, next to the `>` `<` `>=` `<=` `=` `!=` `~` already there.

  Two details worth knowing. The null tests read the column as it was read from disk,
  before any cast, because a cast can manufacture nulls and a null filter that counted
  those would be answering about the cast rather than the data. And a negative text
  filter is not the complement of the positive one over null cells: both `contains` and
  `does not contain` are false for a null, so filtering for "not X" never quietly
  surfaces rows whose value is unknown. A test pins that the two partition exactly the
  non-null rows, and another pins that the Arrow-kernel path and the generated-SQL path
  select **identical rows** for every operator — they are two implementations of the same
  vocabulary, and which one runs depends on how the binary was built. The text operators
  match `%` and `_` **literally** on every path: the SQL translations escape them (and MySQL
  gets its own `ESCAPE` character, because a backslash is itself special inside a MySQL
  string literal), so filtering a discount column for `50%` finds `50%`, not everything.

### Changed
- **The Strata mark is now in the UI**, in the workspace header and as the favicon,
  as inline SVG drawn from the same geometry as the tray and installer icons — so
  the brand is one shape everywhere rather than three that drift. The header's
  `· the Postman of lakehouse tables` subtitle is gone, and the browser tab now
  reads `Lakeleto` rather than `Lakeleto — the Postman of lakehouse tables`. The
  phrase stays in the README and the docs, where it is positioning; in the chrome
  of a tool you use daily it was a tagline occupying the space a logo should.
- The release workflow gained `installer-windows` and `installer-macos` jobs, both
  cosign-signed and checksummed like every other artifact. Authenticode signing and
  Apple notarization are wired but **credential-gated**: with the secrets unset the
  installers still build, and the job logs a warning rather than failing the
  release. Windows packaging pins **WiX 5** — 6 and 7 refuse to run until the Open
  Source Maintenance Fee EULA is accepted (`error WIX7015`).

- **Iceberg and Delta tables with modern deletes are refused, not answered wrongly.**
  Iceberg v3 replaces positional delete files with Puffin **deletion vectors**, and
  Delta has its own. Neither reader parses them, and both dropped them silently — so
  the deleted rows came back as live rows while the row count was still reported as
  exact. A table like that now refuses to open, with a message that names the
  mechanism and says how to get a readable table (compaction / `REORG … PURGE`).

  The refusal is scoped to the deletes, not to the version: a v3 Iceberg table using
  none of v3's unmodelled features reads exactly as before, and its `format-version`
  is now recorded rather than ignored. Delta additionally refuses **column mapping**
  (`delta.columnMapping.mode` = `name`/`id`), where the physical Parquet columns are
  named by id and every logical column would otherwise read as null. Any other
  reader feature a Delta `protocol` action declares is noted on stderr and read.

- **`serve --root` now confines Delta tables.** A Delta transaction log names its own
  data files, and the path may be absolute or contain `..` — so a table sitting
  legitimately inside the root could point at any file on the machine and have it
  read. `--root` is a per-format traversal whitelist rather than a general sandbox,
  and Delta had no entry in it. Iceberg and Parquet directories were already covered.

  Confining means replaying the log twice per request, and a Delta replay costs a
  read and a JSON parse per commit — so the planner now memoizes per table, keyed on
  the log's highest version, commit count, and mtime. The confinement pass and the
  reader's own share one replay.

- **`POST /v1/query` is bounded.** It ran the uncapped query path, so `SELECT *` over
  a large table materialized the whole result and serialized every row into one JSON
  body. It now accepts an optional `limit` (default 10,000, clamped to 100,000),
  pushes that bound into the plan, and returns `capped: true` when the result filled
  it, so a truncated answer cannot be mistaken for a complete one. `GET /v1/export`
  is still the way to pull a large result.

- **Delta tables are visible in the file browser.** `GET /v1/list` marked a directory
  as a table only when it looked like Iceberg, so a Delta table appeared as an
  ordinary folder even though clicking through to it read fine. It now checks
  `_delta_log/` first — the same order the reader uses, so the browser and the reader
  agree on a directory that has both.

### Fixed
- **`GET /v1/preview` and `GET /v1/profile` now bound a caller-supplied size.** `POST /v1/query`
  already clamped its `limit`, but `?limit=` on preview and `?scan=` on profile were taken
  verbatim, so `?limit=999999999` still materialised the whole table. Both now clamp a
  caller-supplied value to the same query cap; the operator-configured profile default (trusted)
  is left untouched when no value is passed.
- **A windowed CSV/TSV/JSON read no longer overflows on a large `offset`.** `GET /v1/rows`
  takes a caller-supplied `offset`, and the local reader computed `offset + limit` to size the
  scan — which panics in debug and wraps in release for an `offset` near `usize::MAX`. It now
  uses a saturating add, so an out-of-range offset yields an empty window instead of a crash.
- **A run no longer caches its result rows unless asked.** `POST /v1/workspaces/{id}/runs`
  unconditionally wrote the full result to the workspace store — and the store is a
  trait, so with `--workspace-remote` configured that sent the rows off the machine
  without anyone choosing it. Caching is now per-run (`"cache": true`), the record's
  `cached` flag reports what was actually written rather than what was hoped for, and
  the workbench carries a labelled *Save result rows* switch that says what turning it
  on does. History still records every run either way; what changes is whether the rows
  are kept, and a run whose rows were not kept re-runs when you click it.

- **`lakeleto engines` and `GET /v1/engines` stop under-reporting the binary.** Both
  claimed `parquet, csv` no matter which features were compiled in, and the engine
  list omitted delta, sqlite, postgres and mysql entirely. The format list is now
  derived from the build's own feature flags, and every backend has a row. The
  inert `duckdb` flag is shown as *planned* — ROADMAP Phase 5 said it would be, and
  it never was, so `--features duckdb` compiled and wired nothing without saying so.

## [0.1.4] - 2026-07-21

### Added
- **Bring your own database** — read-only connectors for **SQLite, Postgres, and
  MySQL** behind the same `Engine` trait (via sqlx). Address a table with a
  connection URI (`sqlite:///…?table=…`, `postgres://…`, `mysql://…`); a URI with
  no `?table=` browses the database's tables. Credentials can use `{{VAR}}`
  placeholders so they aren't stored in the workspace. NUMERIC/DECIMAL render as
  numbers and dates as text (via bigdecimal/chrono). New sidebar **＋ Add
  connection** form (SQLite / Postgres / MySQL / File) with editable properties.
- **Delta Lake tables** — a correct, self-contained reader that replays the
  `_delta_log` transaction log (add/remove, latest schema, partition columns from
  add-actions), so overwrites/deletes read the right snapshot instead of stale
  files. (JSON commit log; checkpoints not consulted.)
- **Iceberg on object storage** (`s3://` / `gs://` / `az://`) — an object-store
  Iceberg table is mirrored to a local temp dir and read with your own env
  credentials; a bare prefix with a `metadata/` child auto-detects as Iceberg.

### Fixed
- **SQL over Iceberg and over a Hive-partitioned Parquet directory** — the SQL
  engine now reads both (partition columns included) by materializing them through
  the local reader; previously Iceberg was unsupported in SQL and a partitioned
  directory dropped its partition columns.

## [0.1.3] - 2026-07-20

### Fixed
- **Windows: `serve`/`open` no longer panic on Ctrl-C.** The DataFusion engine
  owned a Tokio runtime that was dropped inside the serve runtime's async context
  at graceful shutdown ("Cannot drop a runtime in a context where blocking is not
  allowed"). The runtime is now a process-wide static, so nothing is dropped in an
  async context.

### Added
- **Theme toggle** in the header — cycles Auto → Light → Dark and persists (Auto
  follows the OS `prefers-color-scheme`).
- **New-tab launcher** — the `+` button opens a start page to open a data source
  (path/URI) or start a query on a saved connection.
- **Drag to reorder** — tabs and the sidebar sections can be dragged to reorder;
  sidebar order + collapse state persist.
- **Collapsible sidebar sections**, with **Files** first and clearly navigable.
- **Copy path** — the toolbar path is click-to-copy (and the Windows `\\?\`
  verbatim prefix is stripped for display/copy).
- **Version in the header** — `GET /v1/engines` now reports the running binary's
  version, shown under the title.
- **Modern scrollbars** — thin, always-visible rounded-pill scrollbars.

### Changed
- **Friendlier workspace import** — feeding a data file to "Import" (which loads a
  workspace bundle) now explains the mistake instead of a raw JSON parse error.

### Docs
- New usage guide (`docs/GUIDE.md`) with a variables (`{{...}}`) section, a
  step-by-step "Running it" walkthrough in the README, and a Docker Hub
  getting-started section.

## [0.1.2] - 2026-07-20

### Fixed
- **SQL/grid over `.tsv` files.** DataFusion's `register_csv` gates on a `.csv`
  extension and rejected `.tsv` (or `--format tsv` over any name) with
  "File path '….tsv' does not match the expected extension '.csv'". The reader
  is now told the file's real extension.
- **Grid "contains" filter on non-text columns.** A substring filter on a
  numeric/bool/temporal column errored ("There isn't a common type to coerce
  Float64 and Utf8 in LIKE expression"); the column is now cast to text so
  contains works on any type.
- **Grid overlapping the side panels.** With the Row-detail and History panels
  open, the wide data grid painted over them — the grid now owns its horizontal
  scroll and clips to its box.

## [0.1.1] - 2026-07-20

### Fixed
- **Windows: SQL tab / filtered grid no longer panic.** The SQL engine passed
  canonicalized paths (from `--root` or `fs::canonicalize`, which carry the
  Windows extended-length `\\?\` verbatim prefix) straight to DataFusion, whose
  `ListingTableUrl` can't round-trip that prefix — surfacing as
  `to_file_path() failed to produce an absolute Path`. The path is now normalized
  before registration, covering every DataFusion-backed operation
  (`POST /v1/query` and any `/v1/rows` scan with a filter/sort).

### Added
- **Windows x64 release binary.** Every release now ships a signed
  `lakeleto-x86_64-pc-windows-msvc.zip` (cosign + SHA256 + SLSA provenance),
  alongside the existing Linux (musl x86_64/aarch64) and macOS (Intel/Apple
  Silicon) artifacts. `cargo binstall lakeleto` resolves it automatically.
- **README `Install` section** covering binstall, Homebrew, Docker, and
  from-source across all platforms.

## [0.1.0] - 2026-07-20

First public release — the MVP scaffold for idea #25 "Lakeleto": instant, offline,
no-account inspection of columnar data, with the engine kept a commodity behind
one pluggable trait.

### Added
- **Pluggable `Engine` trait** (`src/engine/mod.rs`). Everything above the seam —
  CLI, `serve` endpoints, the SPA — binds only to `Box<dyn Engine>`, so every
  backend below it is swappable. The default build wires the lean, pure-Rust
  **`LocalReaderEngine`** (`arrow` + `parquet` + `csv`), which always compiles
  in seconds with no C++ toolchain, no async runtime, and no server.
- **Local reader for Parquet + CSV/TSV.** `.tsv` is read tab-delimited;
  `--format tsv` forces it for any name. A **directory** of `.parquet` files
  (a `foo.parquet/part-*` split or Hive-partitioned subdirs) reads as one table —
  columns unioned across files, sidecars ignored, Hive `key=value` dir names
  become columns.
- **CLI commands** (`schema` / `head` / `profile` / `info` / `engines` / `query`):
  - `schema` — columns, types, nullability (+ exact row count for Parquet, from
    the footer);
  - `head -n N` — first-N-rows preview;
  - `profile` — per-column null %, distinct, min/max, samples over a bounded
    scan; `--fast` reads Parquet footer statistics with no row scan (exact
    nulls/min/max over the whole file);
  - `info` — format, engine, size, rows, columns;
  - `engines` — which engines this binary was compiled with.
- **table / JSON / NDJSON / CSV output** (`-o`), pipe-friendly for scripting.
- **Optional DataFusion SQL engine** (`--features sql`). `lakeleto query "<SELECT>"
  --file …` runs read-only SQL over the same trait; a guard rejects anything that
  isn't `SELECT` / `WITH` / `EXPLAIN`. Heavy DataFusion build stays out of the
  default binary.
- **`lakeleto serve` / `lakeleto open` HTTP-JSON API + embedded SPA**
  (`--features serve`). Exposes every `Engine` op over HTTP and serves a
  build-step-free, virtualized data-grid SPA embedded in the binary via
  `rust-embed` (works air-gapped). Endpoints: `/healthz`, `/v1/engines`,
  `/v1/schema`, `/v1/info`, `/v1/preview`, `/v1/profile`, `/v1/rows` (grid window:
  filter → sort → page → project), `/v1/stats`, `/v1/export`, `/v1/list`, and
  `POST /v1/query` (needs `sql`). A `--root` boundary confines every path;
  `--token` (or `LAKELETO_TOKEN`) gates `/v1/*` behind a constant-time-compared
  bearer token. With `sql`, sort/filter scans push into DataFusion; without it,
  Arrow kernels sort/filter over a bounded working set (`scan_cap`, default 200k
  rows) and mark a partial view with a `bounded` flag.
- **Self-contained Iceberg reader** (`--features iceberg`). Reads Apache Iceberg
  tables on the existing arrow-58 stack — parses `metadata.json` + the Avro
  manifest-list/manifests to find the current snapshot's Parquet data files —
  with merge-on-read positional + equality deletes (sequence-number aware),
  compressed manifests, schema evolution (field-id match/cast/null-fill), and
  statistics/partition pruning. No dependency on `iceberg-datafusion`.
- **BYO-credential object-store reads** (`--features object-store`). Reads
  `s3://` (`s3a://`), `gs://` (`gcs://`), and `az://` (`azure://`/`abfs[s]://`/
  `adl://`) tables with the user's **own** environment credentials and zero
  hosted compute — bytes go bucket→machine, nothing is uploaded. Remote Parquet
  uses ranged requests (footer + touched row groups) to stay
  larger-than-memory; every `Engine` op works over a remote URI.
- **Paid Lakeleto Cloud engine seam** (`--features remote`). `RemoteEngine` speaks
  the same HTTP/JSON contract as `serve`, reserving the hosted-plane seam behind
  the trait; the hosted plane itself is future work.
- **`cargo binstall` support** — the release fetches the prebuilt `lakeleto`
  binary from the GitHub release instead of compiling.
- Release scaffolding: `LICENSE` (Apache-2.0), `NOTICE`, `CONTRIBUTING.md` (DCO),
  `SECURITY.md`, `CODE_OF_CONDUCT.md`, and this changelog.

[Unreleased]: https://github.com/lucheeseng827/lakeleto/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/lucheeseng827/lakeleto/releases/tag/v0.4.0
[0.3.0]: https://github.com/lucheeseng827/lakeleto/releases/tag/v0.3.0
[0.2.0]: https://github.com/lucheeseng827/lakeleto/releases/tag/v0.2.0
[0.1.0]: https://github.com/lucheeseng827/lakeleto/releases/tag/v0.1.0
