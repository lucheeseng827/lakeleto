# Changelog

All notable changes to Lakeleto are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_Nothing yet._

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
