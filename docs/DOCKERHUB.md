# lakeleto

**The Postman of lakehouse tables — inspect Parquet, CSV, TSV, and Iceberg in your
browser, no signup, no upload, offline.**

`lakeleto serve` runs the Lakeleto explorer: a single static Rust binary that reads
columnar tables through one `Engine` trait and serves an embedded single-page UI
(bundled via `rust-embed`, so it works air-gapped). Point it at a file, an Iceberg
table, or an object-store URI and browse the schema, a virtualized row grid, and
per-column profiles — reads never leave your machine.

## Where it fits

Lakeleto is the **read/inspect surface** in front of columnar data — it opens a table
and serves the page that browses it. It owns the read-and-render hop and nothing else:
no cluster, no daemon, no coordinator, no write path. One process reads the bytes and
serves the UI.

```
   SOURCES                 LAKELETO serve                THE UI
   (tables & files)        (this image)                 (your browser)

 ┌──────────────┐
 │ Local files  │──┐   .parquet / .csv / .tsv
 │ (disk · CI)  │  │
 └──────────────┘  │
 ┌──────────────┐  │     ┌───────────────┐
 │ Iceberg      │──┼───▶ │ lakeleto serve │      ┌──────────────────┐
 │ tables       │  │     │ Engine trait  │      │ your browser:    │
 └──────────────┘  │     │ reads + serves│───▶  │ virtualized grid │
 ┌──────────────┐  │     │ embedded SPA  │      │ schema·profile·  │
 │ Object store │──┘     │ + /v1/* JSON  │      │ SQL tabs         │
 │ (BYO creds)  │ s3://  └───────┬───────┘      └──────────────────┘
 └──────────────┘ gs:// az://    │
                                 └──▶ export .csv / .json / .parquet
```

- **Upstream** — anything Lakeleto can open as a table: local `.parquet`/`.csv`/`.tsv`,
  an Iceberg table (metadata + Avro manifests, merge-on-read deletes), or an
  object-store URI (`s3://`/`gs://`/`az://`) read with *your own* credentials and zero
  hosted compute. Nothing is uploaded — the bytes go source → process.
- **lakeleto serve** — one `Engine` trait over the reader (lean local reader by default;
  DataFusion SQL and the Iceberg reader are opt-in features). It exposes every read as
  `/v1/*` JSON and embeds the SPA via `rust-embed`, so it serves the UI with no build
  step and no network. `--root` confinement plus an optional bearer token gate the
  surface before you expose it.
- **Downstream** — your browser. The SPA is a client of that same `/v1/*` contract: it
  probes the same-origin server first (the embedded case) before any configured host,
  then renders a virtualized grid — larger-than-memory Parquet, click-to-sort,
  per-column filters — with Schema / Profile / SQL tabs. The current view exports to
  CSV / JSON / Parquet.
