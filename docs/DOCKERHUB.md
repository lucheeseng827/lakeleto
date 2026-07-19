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

## Quick start — serve your local data

Mount a folder of data read-only and open the UI in your browser:

```bash
docker run --rm -p 8080:8080 -v "$PWD:/data:ro" \
  mancube/lakeleto serve --addr 0.0.0.0:8080 --root /data
# open http://localhost:8080  and browse anything under the folder you mounted
```

Three parts to remember:

- **`-v "$PWD:/data:ro"`** — mount your data into the container at `/data` (read-only).
  Swap `$PWD` for any folder, e.g. `-v /home/me/exports:/data:ro`.
- **`--addr 0.0.0.0:8080`** — bind *all* interfaces **inside** the container. The default
  `127.0.0.1` is only reachable from within the container, so the host couldn't see it.
  `-p 8080:8080` then maps it to your machine's port 8080.
- **`--root /data`** — confine every read/browse to the mounted folder (nothing outside it).

**Windows (PowerShell):**

```powershell
docker run --rm -p 8080:8080 -v "${PWD}:/data:ro" mancube/lakeleto serve --addr 0.0.0.0:8080 --root /data
# or a specific folder:
docker run --rm -p 8080:8080 -v "C:\Users\you\data:/data:ro" mancube/lakeleto serve --addr 0.0.0.0:8080 --root /data
```

## One-shot inspection (no server)

The entrypoint is the `lakeleto` binary, so any subcommand works — handy in CI or a
`Makefile`:

```bash
docker run --rm -v "$PWD:/data:ro" mancube/lakeleto schema  /data/orders.parquet
docker run --rm -v "$PWD:/data:ro" mancube/lakeleto profile /data/orders.csv
docker run --rm -v "$PWD:/data:ro" mancube/lakeleto head    /data/events.parquet -n 20
```

## Read S3 / GCS / Azure (your credentials, no mount)

Point at an object-store URI and pass credentials as env — no volume needed, nothing is
uploaded, the bytes stream straight from your bucket:

```bash
docker run --rm -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION \
  mancube/lakeleto serve --addr 0.0.0.0:8080
# then in the UI, open:  s3://my-bucket/warehouse/
```

Notes: omit `--root` when reading remote URIs (it confines to a *local* dir and refuses
object-store URIs). Also works for `gs://` (`GOOGLE_APPLICATION_CREDENTIALS`) and `az://`
(`AZURE_STORAGE_ACCOUNT_NAME` / `_KEY`); S3-compatible stores (MinIO, R2) via `AWS_ENDPOINT`.

## docker compose

```yaml
services:
  lakeleto:
    image: mancube/lakeleto:latest
    ports: ["8080:8080"]
    volumes: ["./data:/data:ro"]
    command: ["serve", "--addr", "0.0.0.0:8080", "--root", "/data"]
```

## Expose it safely

Behind a shared host or proxy, require a bearer token on every `/v1/*` call:

```bash
docker run --rm -p 8080:8080 -v /srv/data:/data:ro \
  -e LAKELETO_TOKEN=change-me \
  mancube/lakeleto serve --addr 0.0.0.0:8080 --root /data
# every API call now needs:  Authorization: Bearer change-me
```

Prefer a reverse proxy with TLS (or an SSH tunnel) over publishing the port directly.

## Good to know

- **Tags:** `:latest` and `:vX.Y.Z` (pin a version for reproducibility).
- **Multi-arch:** `linux/amd64` + `linux/arm64`. Each image is cosign-signed and carries
  SLSA build provenance.
- **Distroless:** no shell in the image (smaller, less attack surface), so
  `docker exec … sh` won't work by design — inspect from the host instead.
- Full command/flag reference and more worked examples: the
  [usage guide](https://github.com/lucheeseng827/lakeleto/blob/main/docs/GUIDE.md).
