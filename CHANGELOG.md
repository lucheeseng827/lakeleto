# Changelog

All notable changes to Lakeleto are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_Nothing yet._

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
