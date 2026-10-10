# Lakeleto operations runbook

How to run `lakeleto serve` in practice. Everything here describes the shipped OSS
binary — grounded in [`src/cli.rs`](../src/cli.rs) and [`src/api.rs`](../src/api.rs).
Flags are in [CONFIG.md](./CONFIG.md); how to get the binary is in
[DEPLOY.md](./DEPLOY.md).

Lakeleto is a **laptop / CI-runner tool**, not a service: the primary surface is the
one-shot CLI (`schema`/`head`/`profile`/`info`/`query`). `serve` is the same `Engine`
trait exposed over HTTP plus an embedded browser UI — it is designed to run **local, for
one user**, and hardened only enough to sit behind your own boundary when you go wider.

## Running the server

```sh
# needs a binary built with --features serve (and sql for POST /v1/query)
lakeleto serve                       # http://127.0.0.1:8080  (Ctrl-C to stop)
lakeleto open data/events.parquet    # start server + launch a browser tab deep-linked to the file
```

- **Bind / port.** Default `127.0.0.1:8080` — **loopback**. Override with `--addr` or
  `LAKELETO_ADDR`. Binding a non-loopback address with no `--token` prints a loud warning;
  the API is then open to the network.
- **Embedded SPA.** The single-page UI is compiled into the binary via `rust-embed`
  (`frontend/dist/`), so `serve`/`open` need no separate web server, no node/npm at run
  time, and work **air-gapped**. Non-API paths fall back to `index.html`; `/v1/*` and
  `/healthz` never do (a miss there is JSON `404`).
- **Same-origin API.** The SPA is a client of the same `/v1/*` contract it is served
  from and probes the same-origin server first, so no CORS setup is needed for the
  embedded case.
- **Readiness signal.** One stderr line once the socket is bound:
  `lakeleto: listening on http://127.0.0.1:8080  (Ctrl-C to stop)` (plus
  `lakeleto: bearer token required on /v1/*` and a `/v1/* file access confined to --root …`
  line when those are set). `lakeleto open` also prints/launches the URL with the token
  attached. Startup fails fast if `--addr` can't bind or `--root` doesn't resolve to a
  directory.
- **Process model.** One foreground process; supervise with systemd / a container restart
  policy. Graceful shutdown on Ctrl-C (SIGINT).

## Security posture

- **What listens where.** Exactly one TCP socket — `--addr`, default **loopback**
  `127.0.0.1:8080`. Binding wider is a deliberate act (and warns without a token). Keep it
  local, or put it behind your own network boundary.
- **No in-process TLS.** The binary speaks plain HTTP. For any non-loopback exposure put
  a TLS-terminating reverse proxy in front.
- **Auth is optional and off by default.** With no `--token`, every `/v1/*` route is
  **open** — fine for a loopback, single-user session. `--token <TOKEN>` (or
  `LAKELETO_TOKEN`) gates all of `/v1/*` behind `Authorization: Bearer <TOKEN>`
  (constant-time compared); `/healthz` and the SPA stay open so the page can load. This is
  a single shared secret, **not** per-user identity or RBAC — treat it as a boundary
  control, not a full authz plane, and always pair a network-exposed bind with a token.
- **`?token=` leaks — prefer the header.** The `?token=` query form (for browser
  downloads / deep-links) is honoured **only on a loopback bind**; over the network the
  `Authorization` header is required, because a token in a URL leaks through browser
  history, shell history, and proxy/access logs.
- **Filesystem confinement.** By default `serve` will read any path the process can
  (the "point at any file" behaviour). `--root <dir>` confines all `/v1/*` file access to
  one directory: paths outside it — and *all* object-store URIs and `catalog://` references —
  are refused with a uniform 403 that leaks nothing (same message whether out-of-root, missing, or
  unreadable). Confinement is enforced *before* the filesystem is touched and re-checked
  for every member a directory dataset / Iceberg table actually reads. **Set `--root`
  whenever you expose the API beyond your own machine.**
- **Reads stay local.** Reads of local files and tables never touch the network. Object-store
  reads use *your own* credentials with zero hosted compute — bytes go bucket → machine,
  nothing is uploaded. The `remote` engine is opt-in and never a default.
- **A table in an object store stays in it.** An Iceberg table in a store may name only
  objects. A manifest that names a local path or a `file://` URI is refused rather than
  followed, so a table someone else wrote can't make the server read its disk.
- **A catalog is trusted with what it serves, and checked on what it asks.** A `catalog://` table
  is read where its catalog says its metadata is, with the credentials it vends. Lakeleto reaches
  a catalog over `https`, or plain `http` only to this machine, never follows a redirect with a
  login, and refuses a table whose catalog requires row filters or column masks it cannot apply.
- **Read-only.** `query` / `POST /v1/query` / the MCP `query` tool reject anything that isn't
  `SELECT`/`WITH`/`EXPLAIN`. Lakeleto never mutates your data.
- **`lakeleto mcp` opens no socket.** It speaks only to the client that started it, on its stdin
  and stdout, and reads with that user's permissions and credentials. `--root` confines it
  exactly as it confines `serve`, with the same checks, and every tool is read-only. What a tool
  returns goes to the agent, and from there to the model service it runs on: point `--root` at
  what you're willing to send there.

## Object-store credentials (BYO)

Object-store reads (`--features object-store`) take credentials **only from the
environment**, exactly as the cloud SDKs expect — nothing is read from a config file and
nothing is uploaded.

| Store | Env vars |
|---|---|
| S3 (+ MinIO / R2 via `AWS_ENDPOINT`) | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT`, `AWS_SESSION_TOKEN` |
| GCS | `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_SERVICE_ACCOUNT` |
| Azure | `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY` |

```sh
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=us-east-1
lakeleto schema s3://my-bucket/events.parquet
```

Note: `--root` confinement is local-filesystem only, so it always refuses object-store
URIs. A `serve` deployment that must stay on-disk only is naturally covered by `--root`.

## Catalog credentials (`--features catalog`)

A `catalog://` table involves two logins, kept apart:

- **The catalog's own.** `token` sends a bearer token. `credential` runs OAuth2 client
  credentials against `oauth2-server-uri`. Both come from `$LAKELETO_HOME/catalogs.toml` or, better
  for a secret, its `LAKELETO_CATALOG__<NAME>__<KEY>` variable. A token from the flow is kept in
  memory and fetched again before it expires, or when the catalog stops taking it.
- **The table's files.** Lakeleto reads them with:
  1. the credentials the catalog vends for the table;
  2. else the storage keys configured for the catalog;
  3. else this machine's, as any `s3://` read uses them.

  A catalog with `storage-fallback = "none"` refuses the third, so its tables are never read as
  this machine. Set it for a catalog you don't trust with this machine's credentials: on the third
  step, the catalog still says which bucket and endpoint they are used against. `lakeleto info`
  and `/v1/info` say which one a read used.

Neither login is written to disk, put in a log or an error message, or printed by `lakeleto
catalog ls`. On Unix, Lakeleto warns when `catalogs.toml` holds a secret and other users can read
it. `chmod 600` it, or move the secret to its variable.

## Resource notes

- **Streams; flat RAM.** The grid renders only the visible rows over a spacer sized to
  the total and fetches 200-row windows from `/v1/rows` on scroll, two at a time and
  8,000 rows at most in the browser, so it browses **larger-than-memory Parquet**. A
  window that fails is asked for again after 3 seconds, then less often, up to every
  30. Remote Parquet is read with ranged requests (footer +
  only the row groups a window touches); local Parquet reads are windowed the same way.
  A CSV window is read from the file's start as far as the window: from local disk, or as
  one streamed request to an object store. A JSON window is read the same way, except that
  one in a records member (`/data`) starts at the member, read by its byte range. A JSON
  file's first read also learns its layout and schema, in passes of their own, kept per file
  version; locating a records member reads the whole document.
- **Bounded scans.** Profiles scan up to `--scan` rows (CLI, default 10k) /
  `--default-scan` (server). Without the `sql` feature, grid sort/filter runs over a
  bounded working set (~200k rows) and the `/v1/rows` response's `bounded` flag marks a
  partial view; with `sql`, sort/filter/count are pushed into DataFusion and are exact over
  the whole table, an Iceberg or Delta table or a Parquet directory being read into memory
  whole for it first. Exports are capped at 1,000,000 rows **and** 512 MiB (413 past either) —
  narrow the view (filters / fewer columns) for large downloads.
- **MCP calls are small.** A `lakeleto mcp` call returns at most `--max-rows` rows (1,000) and
  `--max-bytes` of JSON (32 KiB), and is answered within `--timeout` seconds (30). Each call runs
  on a thread of its own, so an agent's parallel calls run at once, up to 16 (one more is
  refused as `busy`), and a `ping` is answered during a long one. A call whose engine doesn't
  stop at the deadline (a database query, once it has started) is answered with an error a
  second after it; its thread runs on until the engine returns, and its result is dropped.
- **Workspace store.** Persisted under `$LAKELETO_HOME` (`~/.lakeleto/workspaces/<id>/`):
  `workspace.json` + `history.jsonl` + `results/*.parquet` result cache. Runs are capped
  at 100k rows; result uploads (sync path) at 128 MiB. Back up `~/.lakeleto` if the saved
  queries / cached results matter.

## Troubleshooting (symptom first)

- **``no catalog named `prod` is configured``.** Lakeleto read `$LAKELETO_HOME/catalogs.toml` and
  the `LAKELETO_CATALOG__…` variables and found no `prod`. The message lists the catalogs it did
  find. A `serve` started before the file was written needs a restart.
- **``catalog `prod` refused the request: … 401``.** The token is wrong or expired, or the client
  credentials are. A refused client-credentials login names the identity provider's reason
  (`invalid_client: …`).
- **`… is a namespace, not a table`.** The reference names a namespace. List it with `lakeleto
  catalog ls <reference>/`.
- **`Forbidden` reading a catalog table's files.** The message ends with whose credentials the read
  used. If those were this machine's, the catalog vended none: check that it is configured to vend
  credentials, or set storage keys for it.

What the client sees → why → what to do.

| Symptom (status / message) | Cause | Fix |
|---|---|---|
| `501` `… rebuild with --features sql` on `POST /v1/query` (or `query`) | Binary built without the `sql` engine | Rebuild with `--features serve,sql` (or use `--engine remote`). |
| `403` `path is outside the server root (--root)` | `--root` is set and the path (or an object-store URI, or a dataset member) resolves outside it | Point at a path under `--root`, or drop/relocate `--root`. Object-store URIs are always refused under a root. |
| `401` `unauthorized — send the bearer token as Authorization: Bearer …` | `--token`/`LAKELETO_TOKEN` is set and the request lacks a valid bearer (or used `?token=` over a non-loopback bind) | Send `Authorization: Bearer <TOKEN>`. The `?token=` form works only on a loopback bind. |
| `400` `cannot infer the format of … pass --format …` | Unknown extension and not a Parquet file, or an object-store key with no known extension | Pass `--format parquet\|csv\|tsv\|json`, or use `?format=` on the request. |
| `400` `… is a directory but not an Iceberg table … contains no .parquet files` | Directory has no `metadata/` subdir and no `.parquet` files | Point at a real Iceberg table dir or a Parquet dataset; for Iceberg also build `--features iceberg`. |
| `400` `<uri> is an object-store URI — rebuild with --features object-store` | An `s3://`/`gs://`/`az://` path without the feature | Rebuild with `--features object-store` and set the store's env credentials. |
| `413` export is `N` bytes, over the cap | Export view exceeds 1,000,000 rows or 512 MiB | Narrow with filters / fewer columns, or export a smaller window. |
| `404` JSON `no such endpoint: /v1/…` | Typo'd `/v1/*` path (API misses never fall back to the SPA) | Check the path against [CONFIG.md](./CONFIG.md#serve-httpjson-endpoints) / `GET /v1/engines`. |
| `query a database` … `rebuild with … --features sqlite` on a `sqlite://`, `postgres://` or `mysql://` table | Binary built without a database engine | Rebuild with `--features sqlite` (or `postgres`, `mysql`). |
| `lakeleto engines --remote-url …` fails on `…/v1/engines` | The server serves only part of `/v1`, as a hosted plane does, and not `/v1/engines` | Its other routes still answer; the client reports its capabilities as unknown. Check which version a server speaks with `curl -sI <url>/healthz`: a versioned one sends `X-Lakeleto-Protocol`. |
| Startup: `WARNING binding <addr> (non-loopback) with no --token — the API is unauthenticated` | `--addr` is non-loopback and no token set | Add `--token`, and front it with a TLS proxy; or bind loopback. |
| Startup fails: `--root <dir> … / is not a directory` | `--root` path is missing or not a directory | Create the directory / fix the path. |
| Browser tab didn't open on `lakeleto open` (headless/CI) | No browser to launch | Expected — the URL is printed to stderr; open it yourself or use `serve` + `curl`. |
