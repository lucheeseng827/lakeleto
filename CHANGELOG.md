# Changelog

All notable changes to Lakeleto are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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

[Unreleased]: https://github.com/lucheeseng827/lakeleto/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/lucheeseng827/lakeleto/releases/tag/v0.1.0
