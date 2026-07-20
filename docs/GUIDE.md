# Lakeleto — usage guide

A task-oriented tour: start it, understand how it thinks, then worked examples —
**viewing daily data**, **exploring in the browser**, **batch-querying many files
at once**, **reusable `{{variables}}`**, and **reading your S3 / GCS / Azure data
locally**.

Everything runs on your own machine. Lakeleto never uploads your data and needs
no account or server.

> The prebuilt release binary already includes every engine
> (`serve`, `sql`, `iceberg`, `object-store`), so the commands below are just
> `lakeleto …` — no `--features` flags. If you build from source, add
> `--features serve,sql,iceberg,object-store`.

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

1. **A source is a file or a folder.** Parquet, CSV, or TSV on local disk, an
   Iceberg table, or an object-store URI (`s3://…`). Point Lakeleto at it and it
   reads the bytes directly — no import step, no copy.
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

---

## 4. Example — explore a table in the browser

Once a file is open:

- **Grid** — scroll rows (windowed, so million-row files stay smooth). Type in a
  column's **filter** box: plain text is a *contains* match; prefix
  `>` `<` `>=` `<=` `=` `!=` for comparisons (e.g. `>= 100` on an amount column,
  or `Singapore` on a city column). Click a header to **sort**, a cell to copy
  it, a row for the full **Row detail** panel.
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
  just like local files. Remote **CSV** is fetched whole.
- **S3-compatible stores:** set `AWS_ENDPOINT=https://…` for MinIO, R2, etc.
- **Env-only:** credentials are read from the environment and nowhere else; they
  are never written to disk or a config file.
- Every operation — schema / head / profile / grid / SQL / export / browse — works
  over a remote URI exactly as it does locally.

> **Note:** with `--root` set, object-store URIs are refused (root confines reads
> to a local directory). Run without `--root` — or on a trusted machine — when
> reading remote data.

---

## 8. Example — query a database (bring your own SQLite)

Lakeleto can point at a **live database** the same way it points at a file — with
your own connection, read-only, nothing copied. SQLite ships today; Postgres and
MySQL are next.

A database is addressed by a **connection URI** in the path/open box:

```text
sqlite:///C:/data/app.db            # the whole database — lists its tables
sqlite:///C:/data/app.db?table=orders   # one table, opened as a grid
```

(On Windows use forward slashes and the triple slash: `sqlite:///C:/…`. On
macOS/Linux: `sqlite:///home/me/app.db?table=orders`.)

- **Browse tables:** open the bare `sqlite:///…app.db` URI (or paste it in the
  file box) and the sidebar lists every table — click one to open it.
- **Explore a table:** `?table=<name>` gives you the usual **Grid / Schema /
  Profile**, column filters, and sort — all pushed down to SQL against the DB.
- **Run SQL:** on the **SQL** tab, query the database directly — the tables are
  the real database tables (not a single `t`), e.g.
  ```sql
  SELECT city, count(*) AS n, round(sum(amount), 2) AS total
  FROM orders
  GROUP BY city
  ORDER BY n DESC
  ```

Read-only by design (an explorer, not an editor) — write statements are refused,
and the connection is opened read-only. Requires a build with `--features sqlite`
(the release binary includes it).

## 9. Example — an Iceberg table

Point at the table directory (the one containing `metadata/`):

```bash
lakeleto schema ./warehouse/db/orders        # local Iceberg table
lakeleto open   ./warehouse/db/orders         # browse it in the UI
```

Lakeleto reads the current snapshot's Parquet data files (including
merge-on-read positional deletes). Combine with the object-store scheme to read
an Iceberg table sitting in a bucket.

---

## 10. Sharing it safely (beyond your own machine)

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

## 11. Where things live · stopping · resetting

- **Workspace state:** `~/.lakeleto/workspaces/<id>/` (`workspace.json`,
  `history.jsonl`, `results/*.parquet`). Override the base with `LAKELETO_HOME`.
- **Stop the server:** Ctrl-C in its terminal.
- **Reset a workspace:** delete its folder under `~/.lakeleto/workspaces/`, or use
  **Delete** in the workspace bar.

## 12. Troubleshooting

| Symptom | Fix |
| --- | --- |
| Double-clicking the binary flashes a window and closes | It's a command-line tool — run it from a terminal (see the README walkthrough). |
| Windows SmartScreen "unknown publisher" | **More info → Run anyway**; the download is cosign-signed with a `.sha256` you can verify. |
| macOS "cannot verify the developer" | Right-click the file → **Open** once, or `xattr -d com.apple.quarantine ./lakeleto`. |
| An object-store URI errors about a missing feature | Use the release binary (all engines built in), or rebuild with `--features object-store`. |
| Port 8080 already in use | `lakeleto serve --addr 127.0.0.1:8090` (any free port). |
| A *Run across* / SQL query errors on one file only | That file's schema doesn't fit the query (a column it lacks); the other targets still run. |

---

See also: [CONFIG.md](CONFIG.md) (every flag & env var) ·
[OPERATIONS.md](OPERATIONS.md) · [DEPLOY.md](DEPLOY.md) · the
[README](../README.md) quick tour.
