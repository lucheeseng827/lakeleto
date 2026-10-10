# Lakeleto — usage guide

A task-oriented tour: start it, understand how it thinks, then worked examples —
**viewing daily data**, **exploring in the browser**, **batch-querying many files
at once**, **reusable `{{variables}}`**, **reading your S3 / GCS / Azure data
locally**, and **letting an AI agent read your tables**.

Everything runs on your own machine. Lakeleto never uploads your data and needs
no account or server.

> The prebuilt release binary already includes every optional engine (`serve`,
> `sql`, `iceberg`, `object-store`, `catalog`, `delta`, the `sqlite`/`postgres`/`mysql`
> database connectors, and `remote`, a client for another Lakeleto server) on top of the
> built-in `local` reader, the MCP server (`mcp`), Parquet output (`parquet-out`) and the zstd,
> bzip2, xz and Brotli decoders (`compression`), so the commands below are just `lakeleto …` — no
> `--features` flag needed. If you build from source, add
> `--features serve,sql,iceberg,object-store,catalog,delta,sqlite,postgres,mysql,mcp,remote,parquet-out,compression`.

---

## 1. Two ways to start

| You want to… | Command | What happens |
| --- | --- | --- |
| Open **one file** in the browser | `lakeleto open sales.parquet` | starts the local viewer **and opens your browser** at that file |
| Browse a **folder** of data | `lakeleto serve --root ./data` | serves the UI at <http://127.0.0.1:8080>; browse any file under `./data` |
| **Inspect from the terminal** (no UI) | `lakeleto schema sales.parquet` | prints schema/rows/profile to stdout — good for scripts & pipes |

Stop the server anytime with **Ctrl-C**. (New to the terminal? See the
"Running it — step by step" section in the [README](../README.md).)

---

## 2. How Lakeleto thinks (the 3 ideas)

1. **A source is a file or a folder.** Parquet, CSV/TSV, JSON or Arrow IPC on
   local disk (text gzip-, zstd-, bzip2- or xz-compressed too), an Iceberg or Delta
   table, a database table, or an object-store URI (`s3://…`). Point Lakeleto at it
   and it reads the bytes directly — no import step, no copy.
2. **In SQL, your source is the table `t`.** The SQL tab (and `lakeleto query`)
   register the current file as a table named `t`, so every query is
   `… FROM t`. Queries are **read-only** — Lakeleto is an explorer, not an editor.
3. **The engine is a swappable detail.** `local` (pure-Rust Arrow reader) is the
   default; `sql` (DataFusion) kicks in for SQL and for grid filter/sort;
   `iceberg` and `object-store` extend *where* it can read. You never pick one —
   Lakeleto routes automatically. `lakeleto engines` lists what your binary has.

**Workspaces & connections** (the browser workbench):

- **Save source** pins the current file to the left **CONNECTIONS** list so you
  can reopen it and target it from *Run across* (see Example 3).
- A **workspace** groups your tabs, saved sources, saved queries, and run
  history. It persists on disk under `~/.lakeleto/workspaces/<id>/`
  (override the location with `LAKELETO_HOME`), so your setup survives a restart.

---

## 3. Example — view today's data (and a folder of daily files)

**One dated file.** Just open it:

```bash
lakeleto open exports/orders-2026-07-18.parquet
```

You land on the **Grid**; flip to **Schema**, **Profile**, or **SQL** at the top.

**A whole folder as one table.** Point Lakeleto at a *directory* of Parquet
files and it reads them as a single table — columns are unioned across files:

```bash
lakeleto open exports/orders.parquet/          # a foo.parquet/part-*.parquet split
# or serve the parent and click into it:
lakeleto serve --root ./exports
```

**Partitioned (Hive-style) folders.** If your daily dumps live in
`date=YYYY-MM-DD/` subdirectories, those `key=value` directory names become real
columns you can filter and group on:

```text
warehouse/orders/
  date=2026-07-16/part-0.parquet
  date=2026-07-17/part-0.parquet
  date=2026-07-18/part-0.parquet
```

```bash
lakeleto open warehouse/orders/
```

Now `date` is a column. In the **SQL** tab:

```sql
SELECT date, count(*) AS orders, round(sum(amount), 2) AS revenue
FROM t
GROUP BY date
ORDER BY date DESC
```

> **Daily-review tip:** keep a terminal handy and alias your latest dump, e.g.
> `lakeleto open "exports/orders-$(date +%F).parquet"` on macOS/Linux — one
> command each morning opens today's file in the browser.

### Compressed files, and Arrow files from Python

A compressed text file opens as the format it holds, and an Arrow file as the table it is:

```bash
lakeleto open logs/2026-07-18.ndjson.gz        # gzip: decompressed as it is read
lakeleto head exports/orders.csv.zst -n 20     # zstd, bzip2 and xz too
lakeleto open features.feather                 # df.to_feather(…) in pandas, df.write_ipc(…) in Polars
lakeleto schema features.feather               # the exact row count, from the file's footer
```

- **Compressed text:** `.csv`, `.tsv`, `.json`, `.ndjson` and `.jsonl` with `.gz`, `.zst`, `.bz2`
  or `.xz` after them. A file in an object store is decompressed as it streams, so the first rows
  arrive without a download. Every build reads gzip; zstd, bzip2 and xz need
  `--features compression` when you build from source, and the release binaries have it. A read
  stops at 4 GiB of decompressed bytes, which `--max-decompressed` changes: the guard against a
  small file that inflates without end.
- **Parquet from Polars:** Polars compresses Parquet with zstd unless told otherwise, as Iceberg
  does its data files; pyarrow, pandas, DuckDB and Spark use Snappy. Every build reads Snappy, gzip
  and LZ4 Parquet; zstd and Brotli need `--features compression` when you build from source
  (`iceberg` reads zstd too), and the release binaries have it. A build without them refuses such
  a file's rows, naming the feature; `schema`, `info` and `profile --fast`, which read only its
  footer, work all the same.
- **Arrow:** `.arrow`, `.feather` and `.ipc` files, and `.arrows` streams, as pyarrow, pandas,
  Polars and `lakeleto -o arrow` write them. A file says how many rows it holds in its footer, so
  the grid knows its end, and a window reads only the batches it covers, from an object store too.
  Feather's default LZ4 compression reads in every build, zstd with `compression`. Feather v1 files,
  from pyarrow before 0.17, are not read.
- **Time zones:** a tz-aware UTC column, a pandas `datetime64[…, UTC]` or a pyarrow
  `timestamp[us, tz=UTC]`, shows as `2024-01-02T03:04:05Z` in every build, and so does a column
  in an offset such as `+02:00`. Another zone, such as `Europe/Paris`, is shown in its own time by
  the release binaries and the image. A build from source without `--features sql` has no time
  zone database, and refuses such a column in text output, naming its zone.

---

## 4. Example — explore a table in the browser

Once a file is open:

- **Grid** — scroll rows, from the first to the last: the grid reads them a window
  at a time as you scroll, so a two-million-row file stays smooth. A CSV, TSV or
  JSON file's total isn't known until its end is read, so the footer says
  `≥ N` and the scrollbar grows as you scroll. Type in a
  column's **filter** box: plain text is a *contains* match; prefix
  `>` `<` `>=` `<=` `=` `!=` for comparisons (e.g. `>= 100` on an amount column,
  or `Singapore` on a city column). Click a header to **sort**, a cell to copy
  it, a row's number for the full **Row detail** panel.
- **Schema** — every column, its type, nullability, and the exact row count.
- **Profile** — per-column null %, distinct count, min/max, and sample values —
  a fast data-quality read on any file.
- **SQL** — read-only `SELECT … FROM t`. Results render in the same grid.
- **Download view** — save the *current* (filtered + sorted) view as CSV, JSON,
  or Parquet.

Everything above is also a one-shot terminal command for scripting:

```bash
lakeleto schema  sales.parquet
lakeleto head    sales.parquet -n 20
lakeleto profile sales.parquet
lakeleto profile --fast sales.parquet          # instant, from Parquet footer stats
lakeleto query "SELECT city, count(*) n FROM t GROUP BY city ORDER BY n DESC" --file sales.csv
lakeleto head sales.parquet -o json | jq .     # pipe-friendly
```

### Output for other tools

`-o` picks the format (`table`, `json`, `ndjson`, `csv`, `tsv`), and for rows, from `head`, `query`
and `catalog ls`, three that keep their types for the next program: `parquet`, `arrow` and
`arrows`. `--out <file>` writes to a file instead of stdout and, with no `-o`, takes the format
from the file's extension:

```bash
# A Parquet file DuckDB, Polars, pandas or Spark reads as a table, with its types:
lakeleto query "SELECT * FROM t WHERE amount > 100" --file sales.csv --out big.parquet
duckdb -c "SELECT city, sum(amount) FROM 'big.parquet' GROUP BY city"

# An Arrow stream straight into Python, with no file in between:
lakeleto head sales.parquet -n 100000 -o arrows | python -c '
import sys, polars as pl
print(pl.read_ipc_stream(sys.stdin.buffer).describe())'

# An Arrow file (Feather v2) for pandas.read_feather or polars.read_ipc:
lakeleto head sales.parquet --out sample.arrow
```

- **`arrow` or `arrows`.** `arrow` is the Arrow IPC *file* format, what `.arrow` and `.feather`
  name, and its readers look for an index at the end of the file. `arrows` is the *stream* format,
  which a reader takes from a pipe as it arrives (`polars.read_ipc_stream`,
  `pyarrow.ipc.open_stream`). DuckDB reads the Parquet file, but takes neither format from a pipe.
- **Nested columns stay nested.** A list, struct or map is the formats' own, where `csv` writes
  it as JSON text. A dictionary-encoded column, such as a pandas or Polars categorical, keeps its
  dictionary in `parquet` and `arrows`; `arrow` writes its values, because that format holds one
  dictionary per column for the whole file.
- **`--out` is all or nothing.** The file appears only when the command succeeds. A failed run
  leaves no half-written file, and keeps the one that was there. On Unix it also keeps that
  file's permissions, so a report you made private stays private, and nobody else can read the
  output while it is being written. A run you interrupt keeps the old file too, but can leave
  its hidden temporary file beside it, named `.lakeleto.<pid>-<hex>.tmp`, for you to delete.
- **Not on a terminal.** These three are bytes, so Lakeleto refuses to print them to a terminal:
  give `--out`, redirect, or pipe.
- **Parquet needs `--features parquet-out`** when you build from source. The release binaries
  and the image have it; without it, `-o parquet` is refused before anything is read. Its writer
  is about 530 KB of the default build, where the Arrow writers are about 110 KB, so every build
  writes `arrow` and `arrows`.

---

## 5. Example — batch-query many files at once ("Run across")

**Run across** runs *one* SQL query against *several* files and shows the results
side by side. It's built for same-shape files — daily dumps, per-region exports,
partitions — where you want the same aggregation over each and a quick compare.

Because each source is registered as the table **`t`**, write the query against
`t` and it runs once per selected source.

1. Open each file you want to compare (one tab each), e.g.
   `orders-2026-07-16.parquet`, `…-07-17.parquet`, `…-07-18.parquet`.
2. On each tab, click **Save source** — they appear under **CONNECTIONS** on the
   left. *Run across targets these saved connections.*
3. On any tab, open the **SQL** sub-tab and type your query over `t`:
   ```sql
   SELECT count(*) AS rows, round(sum(amount), 2) AS revenue FROM t
   ```
4. Click **▶ Run across…** (it enables only on the SQL tab). A dialog lists every
   connection as a **target** with a checkbox and the shared, editable SQL.
5. Tick the targets → **Run**. Each file executes the same SQL; you get one
   result row per file. A file whose schema doesn't fit the query shows an error
   on *that* row only — the others still run.

> **CLI equivalent** for scripting a fan-out — loop the same query over files:
> ```bash
> for f in exports/orders-2026-07-*.parquet; do
>   echo "== $f =="
>   lakeleto query "SELECT count(*) rows, round(sum(amount),2) revenue FROM t" --file "$f" -o json
> done
> ```

**Related, but different:** the sidebar's *Run folder* runs each **saved query**
(each with its *own* SQL) once — a saved report pack. *Run across* is one SQL
over many sources.

---

## 6. Example — reusable values with variables (`{{...}}`)

Variables are **Postman-style `{{key}}` placeholders**. They're resolved in **both the
SQL and the path** right before a query runs — a literal text substitution
(`{{key}}` → its value). They live per-workspace and persist.

**Set one:** sidebar → **Variables** → **+ Variable** → a key and a value, e.g.
`city = Singapore`, `min_amt = 100`, `day = 2026-07-18`.

**Use in SQL** (the current source is the table `t`). Because it's a literal replace,
**you write the quotes** for string values and leave numbers bare:

```sql
-- {{city}} → Singapore   (you supply the quotes)
SELECT * FROM t WHERE city = '{{city}}'

-- numeric → no quotes
SELECT tier, count(*) AS n, round(avg(amount_usd), 2) AS avg_usd
FROM t
WHERE amount_usd > {{min_amt}}
GROUP BY tier
ORDER BY n DESC

-- date / timestamp
SELECT * FROM t WHERE order_ts >= TIMESTAMP '{{day}} 00:00:00'
```

**Use in the path box too** — swap the source without retyping it:

```
C:\exports\orders-{{day}}.parquet
s3://my-bucket/events/{{day}}.parquet
```

Change `day` once and every tab/query that references `{{day}}` re-points.

Notes:

- It's a **literal substitution**, not a bound parameter — quote strings yourself,
  leave numbers unquoted. (So don't paste untrusted text into a value.)
- An **unresolved** `{{x}}` shows an "unset" warning chip in the toolbar until you
  define it.
- Pairs well with **Run across** (§5): one `{{min_amt}}` query fanned over many files.

## 7. Example — read your S3 / GCS / Azure data locally

Point Lakeleto at an object-store URI and it reads the table **with your own
credentials and zero hosted compute** — the bytes go straight from your bucket to
your machine, nothing is uploaded, and no hosted or remote Lakeleto service is in
the path. The only Lakeleto process is the CLI (or a local `lakeleto serve`)
running on your own machine.

Credentials come from the environment, exactly as the cloud SDKs expect:

```bash
# AWS S3 (and S3-compatible: MinIO, Cloudflare R2, … via AWS_ENDPOINT)
export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…  AWS_REGION=us-east-1
lakeleto schema s3://my-bucket/events/2026-07-18.parquet
lakeleto head   s3://my-bucket/events/2026-07-18.parquet -n 20

# Google Cloud Storage
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
lakeleto profile gs://my-bucket/events.parquet

# Azure Blob
export AZURE_STORAGE_ACCOUNT_NAME=…  AZURE_STORAGE_ACCOUNT_KEY=…
lakeleto schema az://my-container/events.parquet
```

Browse a bucket prefix in the **UI**, same grid as local disk:

```bash
lakeleto serve                     # then in the browser, open a source and paste:
#   s3://my-bucket/warehouse/
```

Details worth knowing:

- **Schemes:** `s3://` (`s3a://`), `gs://` (`gcs://`), `az://`
  (`azure://` / `abfs[s]://` / `adl://`).
- **Ranged reads:** remote **Parquet** is read with range requests — only the
  footer plus the row groups a window touches — so it stays larger-than-memory
  just like local files.
- **Streamed JSON and CSV:** remote **JSON** and **CSV** are read as they
  arrive. A grid window or a SQL pass over JSON requests the object and stops
  the transfer where it stops reading; a JSON records member (`/data`) is
  fetched by its byte range once located; a CSV read is one request, its schema
  inferred from the rows it reads; and every request asks for the version (ETag)
  the read began with, so an object replaced mid-read fails that read rather
  than mixing two versions. A long read — a large object, or a query result read
  slowly — that outlasts the store's request timeout carries on from where it
  stopped.
- **S3-compatible stores:** set `AWS_ENDPOINT=https://…` for MinIO, R2, etc.
- **Env-only:** credentials are read from the environment and nowhere else; they
  are never written to disk or a config file.
- Every operation — schema / head / profile / grid / SQL / export / browse — works
  over a remote URI exactly as it does locally.

> **Note:** with `--root` set, object-store URIs are refused (root confines reads
> to a local directory). Run without `--root` — or on a trusted machine — when
> reading remote data.

---

## 8. Example — query a database (SQLite · Postgres · MySQL)

Lakeleto can point at a **live database** the same way it points at a file — your
own connection, **read-only**, nothing copied. SQLite, Postgres, and MySQL all
ship in the release binary.

A database is addressed by a **connection URI**:

```text
sqlite:///C:/data/app.db                       # SQLite file (Windows: forward slashes, triple slash)
sqlite:///home/me/app.db?table=orders          #   …one table
postgres://user:{{PGPASS}}@host:5432/shop      # Postgres
mysql://user:{{MYSQLPASS}}@host:3306/shop      # MySQL
```

A URI **without** `?table=` opens the whole database and **lists its tables**; add
`?table=<name>` to open one directly.

**Add it in the UI** — sidebar **CONNECTIONS → ＋** → pick **SQLite / Postgres /
MySQL** → paste the URI (+ an optional table) → **Add**. The connection is saved
and opened. Editing a connection (the ✎ on its row) reopens the same form.

> **Passwords:** use a `{{VAR}}` in the URI (e.g. `…:{{PGPASS}}@…`) and define
> `PGPASS` under **Variables** — the secret is substituted at query time and is
> **not** stored in the workspace file. (See §6.)

Once connected:

- **Browse tables** — a whole-database connection lists its tables in **Files**;
  click one to open it.
- **Explore** — the usual **Grid / Schema / Profile**, with column filters + sort
  pushed down to SQL against the database.
- **Run SQL** on the **SQL** tab — query the real database tables directly (not a
  single `t`), e.g.
  ```sql
  SELECT city, count(*) AS n, round(sum(amount), 2) AS total
  FROM orders GROUP BY city ORDER BY n DESC
  ```

Read-only by design (an explorer, not an editor) — write statements are refused
and the connection is opened read-only. NUMERIC/DECIMAL render as numbers and
dates/timestamps as text.

**From the terminal**, a database URI goes wherever a path does:

```bash
lakeleto head 'sqlite:///home/me/app.db?table=orders'
lakeleto schema 'sqlite:///home/me/app.db?table=orders'
lakeleto query --file 'sqlite:///home/me/app.db' \
  "SELECT city, count(*) AS n FROM orders GROUP BY city ORDER BY n DESC"
```

The SQL runs on the database itself, so it names the database's own tables.

## 9. Example — lakehouse tables (Iceberg · Delta · partitioned Parquet)

Point Lakeleto at a lakehouse table directory and it reads the **correct current
snapshot**, not the raw files. All auto-detected — no format flag needed.

**Iceberg** — a directory containing `metadata/`:

```bash
lakeleto open ./warehouse/db/orders            # local Iceberg table
```
Reads the current snapshot's Parquet data files (incl. merge-on-read positional and
equality deletes). Also works over object storage — an `s3://bucket/warehouse/db/orders`
prefix with a `metadata/` child auto-detects as Iceberg and is read with your own
env credentials (see §7 for the AWS/GCS/Azure vars):
```bash
lakeleto open s3://my-bucket/warehouse/db/orders
```
It is read in place. Lakeleto fetches the current snapshot's metadata and manifests,
then reads data files by ranged requests, only as far as a read goes: a page of the
grid reads part of one file. Earlier snapshots, and files the table no longer uses,
are never fetched, and nothing is copied to disk.

**Iceberg through a REST catalog** — name the table, not where it is
(`--features catalog`; Polaris, Lakekeeper, Nessie, Unity Catalog):

```bash
# ~/.lakeleto/catalogs.toml
#   [catalog.prod]
#   uri = "https://polaris.example.com/api/catalog"
#   warehouse = "analytics"
#   oauth2-server-uri = "https://polaris.example.com/api/catalog/v1/oauth/tokens"
export LAKELETO_CATALOG__PROD__CREDENTIAL='<client-id>:<client-secret>'

lakeleto catalog ls                         # the configured catalogs
lakeleto catalog ls catalog://prod/         # prod's namespaces
lakeleto catalog ls catalog://prod/sales/   # what is in sales
lakeleto info catalog://prod/sales/orders   # rows, columns, and whose credentials read it
lakeleto open catalog://prod/sales/orders   # in the browser
lakeleto query "SELECT count(*) FROM t" --table t=catalog://prod/sales/orders
```
The catalog says which metadata is current and vends credentials for the table's
files, which Lakeleto keeps in memory and asks for again before they expire. In the
browser, open a `catalog://` reference from **Open a source**: the sidebar then lists
its namespace, and its folders and tables open like any others. Keys, environment
variables and the credential order are in
[CONFIG.md](./CONFIG.md#catalogs-catalogstoml).

**Delta Lake** — a directory containing `_delta_log/`:

```bash
lakeleto open ./warehouse/delta_orders
```
Lakeleto replays the transaction log (`_delta_log`), so an overwritten or
row-deleted table reads the **right rows** — not the stale/removed Parquet files
still sitting on disk. Partition columns are filled from the log. (JSON commit
log; checkpoints aren't read, so the table's `*.json` commits must all still be
there. Once log cleanup has removed the early ones, the table either fails to open
or shows only the files added since, with a row count that falls short. `VACUUM`,
which deletes unused data files, is fine.)

**Hive-partitioned Parquet** — a directory of `date=…/region=…/*.parquet`:

```bash
lakeleto open ./warehouse/sales_lake
```
Read as one table with the `key=value` partition directories exposed as columns.

**SQL over all of these** works on the **SQL** tab (and `lakeleto query`) — the
current table is `t`, partition columns included:
```sql
SELECT region, count(*) AS n, round(sum(revenue), 2) AS rev
FROM t GROUP BY region ORDER BY n DESC
```

---

## 10. Example — let an AI agent read your tables (MCP)

`lakeleto mcp` lets an AI agent read your tables: Claude Code, Claude Desktop,
Cursor, or any other client of the [Model Context Protocol](https://modelcontextprotocol.io).
The client starts it and talks to it on its stdin and stdout, so there is nothing
for you to run. Everything it can do is read.

**Claude Code** — add it, confined to your project's `data/` folder:

```bash
claude mcp add lakeleto -- lakeleto mcp --root "$PWD/data"
```

or share it with everyone on the project, in `.mcp.json`:

```json
{
  "mcpServers": {
    "lakeleto": {
      "command": "lakeleto",
      "args": ["mcp", "--root", "${CLAUDE_PROJECT_DIR}/data"]
    }
  }
}
```

**Claude Desktop** — *Settings → Developer → Edit Config* opens
`claude_desktop_config.json` (macOS: `~/Library/Application Support/Claude/`,
Windows: `%APPDATA%\Claude\`). Desktop starts a server from no particular folder,
so give it absolute paths; `which lakeleto` (`where lakeleto` on Windows) prints
the binary's:

```json
{
  "mcpServers": {
    "lakeleto": {
      "command": "/usr/local/bin/lakeleto",
      "args": ["mcp", "--root", "/Users/me/data"]
    }
  }
}
```

**Cursor** — `.cursor/mcp.json` in the project, or `~/.cursor/mcp.json` for every
project:

```json
{
  "mcpServers": {
    "lakeleto": {
      "type": "stdio",
      "command": "lakeleto",
      "args": ["mcp", "--root", "${workspaceFolder}"]
    }
  }
}
```

**From the Docker image**, with the folder mounted read-only (`-i` keeps stdin
open), in any of the files above:

```json
{
  "mcpServers": {
    "lakeleto": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "-v", "/Users/me/data:/data:ro",
               "mancube/lakeleto", "mcp", "--root", "/data"]
    }
  }
}
```

Then ask in plain words — *"what's in orders.parquet?"*, *"which cities have the
most orders?"* — and the agent picks the tools:

| Tool | What it does |
| --- | --- |
| `list` | What's in a directory: its subdirectories and tables. Also an object-store prefix, a database's tables, or a catalog namespace. |
| `describe` | What's in a table, without reading its rows: columns and types, the row count, and for a Parquet file each column's null count, min and max from its footer. A CSV or JSON file stores neither, so its types are inferred from its first rows. |
| `preview` | Rows, from any `offset`, optionally only some `columns`. |
| `profile` | For each column: null count and fraction, distinct values, min, max and samples, from a scan of up to `scan` rows. |
| `query` | One read-only SQL statement over the tables it names (`path` is the table `t`). Anything that writes is refused. |
| `catalog_ls` | The configured Iceberg REST catalogs, their namespaces and their tables. |

What keeps it in bounds:

- `--root <dir>` — only paths under `<dir>` can be read, and relative paths are
  taken from it. Object-store, database and catalog references are refused, as
  with `serve` (§11). Without `--root`, the agent can read whatever your user can.
- `--max-rows <N>` (default `1000`) and `--max-bytes <N>` (default `32768`) — the
  most one call returns. A result that was cut says `truncated` and why, so the
  agent narrows its question rather than filling its context.
- `--timeout <secs>` (default `30`) — a call that runs longer is answered with
  an error. Reading files stops at the deadline; a database query that has
  started runs on to its end, and its result is dropped.
- A refused call says what kind of refusal it is (`forbidden`, `not_found`,
  `query`, `deadline`, `invalid_arguments`, …), so the agent can correct itself.

> The agent sees what the tools return, and so does the service that runs it.
> Point `--root` at data you're happy for both to read.

---

## 11. Sharing it safely (beyond your own machine)

By default `serve` binds to loopback (`127.0.0.1`) and the API is open — fine for
your own machine. If you expose it (a shared box, a container), lock it down:

```bash
lakeleto serve \
  --addr 0.0.0.0:8080 \
  --root /data \
  --token "$(openssl rand -hex 16)"
```

- `--root <dir>` — refuse any read or browse outside `<dir>` (and all
  object-store URIs). Canonicalized at startup.
- `--token <tok>` — require `Authorization: Bearer <tok>` on every `/v1/*` call
  (or `?token=` on a loopback bind). `/healthz` and the SPA stay open so the page
  loads.

Prefer an SSH tunnel or a reverse proxy with TLS over binding `0.0.0.0`
directly. See [OPERATIONS.md](OPERATIONS.md) and [DEPLOY.md](DEPLOY.md).

---

## 12. Where things live · stopping · resetting

- **Workspace state:** `~/.lakeleto/workspaces/<id>/` (`workspace.json`,
  `history.jsonl`, `results/*.parquet`). Override the base with `LAKELETO_HOME`.
- **Stop the server:** Ctrl-C in its terminal.
- **Reset a workspace:** delete its folder under `~/.lakeleto/workspaces/`, or use
  **Delete** in the workspace bar.

## 13. Troubleshooting

| Symptom | Fix |
| --- | --- |
| Double-clicking the binary flashes a window and closes | It's a command-line tool — run it from a terminal (see the README walkthrough). |
| Windows SmartScreen "unknown publisher" | **More info → Run anyway**; the download is cosign-signed with a `.sha256` you can verify. |
| macOS "cannot verify the developer" | Right-click the file → **Open** once, or `xattr -d com.apple.quarantine ./lakeleto`. |
| An object-store URI errors about a missing feature | Use the release binary (all engines built in), or rebuild with `--features object-store`. |
| Port 8080 already in use | `lakeleto serve --addr 127.0.0.1:8090` (any free port). |
| An MCP client shows no Lakeleto tools | Run its command in a terminal, e.g. `lakeleto mcp --root /Users/me/data`: it should wait quietly for input (Ctrl-C to quit). An error there — a `--root` that doesn't exist, a binary built without `mcp` — is what the client hit. Claude Desktop needs absolute paths. |
| A *Run across* / SQL query errors on one file only | That file's schema doesn't fit the query (a column it lacks); the other targets still run. |

---

See also: [CONFIG.md](CONFIG.md) (every flag & env var) ·
[OPERATIONS.md](OPERATIONS.md) · [DEPLOY.md](DEPLOY.md) · the
[README](../README.md) quick tour.
