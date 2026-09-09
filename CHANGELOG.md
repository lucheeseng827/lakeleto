# Changelog

All notable changes to Lakeleto are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/lucheeseng827/lakeleto/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/lucheeseng827/lakeleto/releases/tag/v0.2.0
[0.1.0]: https://github.com/lucheeseng827/lakeleto/releases/tag/v0.1.0
