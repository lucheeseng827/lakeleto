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

### What changed since yesterday (`diff`)

`lakeleto diff` compares two tables — any two Lakeleto reads, in any formats — and says what
changed, from the left one (the old, or the reference) to the right one:

```bash
lakeleto diff exports/orders-2026-07-17.parquet exports/orders-2026-07-18.parquet
```

```text
left   exports/orders-2026-07-17.parquet  (parquet, local engine)
right  exports/orders-2026-07-18.parquet  (parquet, local engine)
found  added-column, removed-column, row-count, stats

schema
  + channel  Utf8 (nullable)
  - rating   Float64 (nullable)

rows
  5000 → 4998  (-2, -0.04%)

statistics  (exact: every row of each side, sql engine)
  column      nulls      min        max
  city        0 → 1      Berlin     Toronto
  amount_usd  0          5.01       1172.35 → 99999.0
  (8 more shared columns unchanged)
```

- **Schema:** columns added (`+`), removed (`-`) and changed (`~`: type or nullability), matched by
  name, and whether the shared ones moved. A type change is the physical one: a CSV's `Int64`
  against a Parquet file's `Int32` is reported, though the values may be the same.
- **Statistics** read every row of each side when your build has SQL (the release binaries do).
  Numbers compare as numbers (`2` and `2.0` are one value); a column that holds text on one side
  and numbers on the other is shown and listed as *not compared*, never as changed.
  `--fast` takes them from Parquet footers instead — exact, and no row read — for two single
  Parquet files. `--scan N` reads the first N rows of each, which is quicker and is a sample: a
  difference it finds may not be one between the tables, and the report says so.
- **Rows, matched by a key** (needs SQL): `--key order_id` (or `--key id,day` for a key of two
  columns) counts the rows added, removed, changed and unchanged, which columns the changed ones
  changed in, and shows a few of each (`--sample N`, 5 by default):

  ```bash
  lakeleto diff yesterday.parquet today.parquet --key order_id
  ```

  ```text
  rows by key  (order_id)
    added 1 · removed 3 · changed 11 · unchanged 4986
    changed in: amount_usd 10, city 1
    + order_id=999999  customer=cust_0640  city=London  …
    - order_id=100010  customer=cust_0597  city=Nairobi  …
    ~ order_id=100000  amount_usd: 23.29 → 25.62
  ```

  The key must name one row on each side: a key that repeats is refused, rather than matching
  rows many to many and miscounting. A null key matches a null key. A key of integers is never
  matched against one of floats — a float holds every integer only up to 2^53, so two keys could
  match as one — and is refused. Columns compare when their values are of one kind, whatever the
  width — an `Int32` against an `Int64`, text against text, an integer against a float (as
  floats) — and any other shared column is listed as *not compared*, never as changed. `NaN`
  matches `NaN`; `0.0` and `-0.0` differ. The comparison runs in the SQL engine, which holds the
  left side's key and compared columns in memory, so put the smaller table on the left. `--key`
  is refused for a database table (read by its own engine, which cannot join it to a file) and
  with `--engine local` (which runs no SQL).

**In CI.** `--fail-on` names the kinds of difference that fail the job; any other is reported and
passes. It exits `3` when one is found, `0` when none is, and `1` on an error:

```bash
lakeleto diff baseline.parquet build/output.parquet --key id \
  --fail-on removed-column,type-change,removed-rows -o markdown --out diff.md
```

The kinds are `added-column`, `removed-column`, `type-change`, `nullability`, `column-order`,
`row-count`, `stats`, `added-rows`, `removed-rows` and `changed-rows`, and the groups `schema`
(the first four: not the order, which a reader that finds columns by name never sees), `rows` (the
three row kinds) and `any`; a row kind needs `--key`. `-o markdown` writes a
report for a pull request comment — every value, column name and path in it is inline code, so
whatever a table holds (an `@mention`, a link, an image) shows as text and never renders. In the
terminal and in Markdown, a control character a value holds is written as its escape (`\n`,
`\x1b`), so data cannot move the cursor or hide a line. `-o json` writes the whole report, values
whole, with a `version` and a `found` list for a script to read.

The source flags — `--format`, `--json-path`, `--flatten` — apply to both sides, so leave them out
when the two are different formats: each side's format is found on its own.

**A table against an earlier snapshot of itself.** An Iceberg or Delta table keeps its history, so
`<table>@<snapshot>` names it as it was — `@<id>` (an Iceberg snapshot id, a Delta version) or
`@<instant>` (RFC 3339, such as `@2026-10-01T00:00:00Z`: the snapshot current then):

```bash
lakeleto diff warehouse/orders@41 warehouse/orders          # version 41 against today
lakeleto diff s3://lake/orders@2026-10-01T00:00:00Z s3://lake/orders --fail-on schema
lakeleto diff catalog://prod/sales/orders@4182309823479 catalog://prod/sales/orders
```

```text
left   warehouse/orders@41  (delta, local engine, committed 2026-11-20T09:12:44.017Z)
right  warehouse/orders@44  (delta, local engine, committed 2026-11-26T14:30:08.135Z)
found  row-count, stats
```

Both sides are then compared by the table's **metadata** — an Iceberg table's manifests, a Delta
table's log — at the snapshot each names, the side without `@` at the newest. No row and no data
file is read: in a store, an Iceberg side costs a request for its metadata, its manifest list and
each manifest, and a Delta side the log's listing, its checkpoint and each commit after it (and for
`@<instant>`, a few more commits, each read whole). A snapshot whose files were since cleaned up
still compares. Each side shows the snapshot it was compared at, which is how to see which one an
`@<instant>` found, and when it was committed: a Delta version's in-commit timestamp when it records
one, otherwise the latest time its log records.

- **What is compared:** the schema the snapshot was written with (the side without `@`: the table's
  current schema, which a schema change sets without a new snapshot), its rows, and each column's
  nulls, minimum and maximum — only where the metadata makes them exact, so a difference found is
  one in the data. Text and binary bounds are left out (writers truncate them), as are a Delta
  table's timestamp bounds (cut to milliseconds) and float bounds (the protocol leaves NaN unsaid),
  and an Iceberg float's where a file counted no NaN. An Iceberg snapshot with delete files gives no
  rows or statistics, since only reading the deletes would count them, and one that records no
  `schema-id` (as early v1 writers did not) is refused, since its columns are not recorded. Each
  leaving-out, a file whose metadata gives nothing for a column among them, is a note in the report.
- **`@<instant>` on a Delta table** needs in-commit timestamps (`delta.enableInCommitTimestamps`).
  Without them the protocol dates a commit by its log file's modification time, which a copy or a
  checkout resets, so it is refused: name the version (`@<n>`) instead. An Iceberg table answers
  from its `snapshot-log`.
- **Not with a snapshot:** `--key`, `--fast` and `--scan` read rows or footers, and are refused.
  Only `diff` takes `@<snapshot>`. Anywhere else it is refused, pointing here, except for a store
  name read with `--format`, which skips the probe that tells: that is read whole, and fails as a
  table that is not there. A name that exists whole is still that name: a directory called
  `orders@2`, or a store prefix with a table under it (found by the same probes a bare prefix always
  takes, run even with `--format iceberg` or `delta`). If the store refuses that probe, or it fails,
  that is the error: the name is not read as a snapshot of another table on a guess. A database URI,
  a ref sent with `--remote-url`, and a store object named with another format (`--format csv`) are
  always taken whole.

### Is the table still what it was? (`check`)

`lakeleto check` is a gate for a CI job or a pipeline step: it measures a table and compares it
with a **baseline** — what the table was the last time the check passed — and fails when it moved
too far. The first run writes the baseline:

```bash
lakeleto check warehouse/orders --baseline checks/orders.json --update
```

Every later run compares with it, and exits `3` when a check fails, `0` when none does, and `1`
on an error:

```bash
lakeleto check warehouse/orders --baseline checks/orders.json --max-age 24h
```

```text
check     warehouse/orders  (delta, rows counted by sql)
now       8 rows · 3 files, 4.0 KB · changed 2h 5m ago
baseline  checks/orders.json  (10 rows · 3 files, 4.8 KB · changed 1d 2h ago, taken 1d ago)

  FAIL     schema     1 column removed, 0 columns changed
                        removed  city  Utf8
                        added    team  Utf8  (allowed)
  FAIL     row-count  10 → 8 rows: 2 fewer (20.00%), more than the 10% allowed
  FAIL     nulls      1 column rose more than the 5 points allowed
                        name: 10.00% → 37.50% null
  PASS     files      within 2×
                        files  3 → 3
                        bytes  5017 → 4198
  PASS     freshness  last changed 2h 5m ago, within the 1d allowed

result    FAIL
```

- **schema:** a column removed, retyped or changed in nullability fails. A column added, or the
  columns in a new order, is reported and passes: a table that grows is a healthy one.
- **row-count:** fails when the rows drop by more than `--max-row-drop` percent (10 by default;
  exactly 10% passes).
- **nulls:** fails when a column's share of nulls rises by more than `--max-null-rise` percentage
  points (5 by default) — 0.1% to 40% fails; twice the rows with twice the nulls is the same share.
- **files:** fails when the table's data files, or their bytes, grow or shrink by more than
  `--max-file-jump` (2× by default). They are the files the reader reads: a file's or an
  object's own, a Parquet directory's `.parquet` files, an Iceberg table's current snapshot —
  on this machine, in a store or behind a catalog, read as your identity for the store — and a
  Delta table's, from its log, on this machine or in a store. For anything else (a database
  table) the gate is skipped, unless `--max-file-jump` is given, which asks for it: then it is
  unknown.
- **freshness:** with `--max-age 30m`, `24h` or `7d`, fails when the table last changed longer ago
  than that — an Iceberg table's current snapshot, a Delta table's latest commit, or a file's
  modification time. It needs no baseline: `lakeleto check t.parquet --max-age 1d` alone is a
  check.

Each gate passes, fails, is **skipped** (nothing to compare: no baseline yet), or is **unknown** —
asked for, and it could not be told, such as `--max-age` on a table whose last change is not
recorded here. An unknown gate makes the check exit `1`, never `0`: a gate that could not judge
never reads as one that passed.

**Every row is counted.** The rows and each column's nulls are counted by the SQL engine, which
the release binaries have. For a single Parquet file, `--fast` reads them from its footer instead
— exact, and no row read — which is also how a build without SQL counts one; anything else
without SQL is refused, rather than gated on a sample. A column whose nulls the footer did not
record (a list column's never are) is not counted, so the nulls gate is unknown: drop `--fast` to
count every row.

**The baseline** is a JSON file with a `version`, the table's columns, its row count, each
column's null count, its files and bytes, and when it last changed — and no value the table holds,
so it can be committed beside the job that checks it. `--update` writes it when the check passes
or there is none yet, whole or not at all, making the directories it is in when they are missing;
a baseline that cannot be written is said in the report, which is still printed, and the check
exits `1`. When the table's files could not be listed — a store's `HEAD` that timed out — the
rows are still counted and a note says why, but `--update` writes no baseline from that run, and
exits `1`: a baseline without the files would leave the `files` gate nothing to compare from then
on. A check that fails leaves the baseline as it was, so the next run
still compares with the last good table. To accept a change that fails a gate — a column dropped
on purpose — remove the file and run `--update` again. A baseline of another version is refused,
never misread.

**In CI.** `-o junit` (or `--out report.xml`) writes the report as JUnit XML, which GitHub
Actions, GitLab and Jenkins show as tests: one test case a gate, a failed gate a `<failure>`, an
unknown one an `<error>`. `-o json` writes the whole report, with a `version`, both fingerprints
and each gate's status, for a script to read. In the terminal and in JUnit, a control character a
column name holds is written as its escape (`\x1b`), so it cannot drive a terminal or break a
CI's XML parser.

```yaml
# .github/workflows/data.yml
- run: lakeleto check warehouse/orders --baseline checks/orders.json --max-age 24h --out check.xml
```

An orchestrator's shell step runs the same command — an Airflow `BashOperator`, a Dagster
`op` that shells out, a dagron step — and fails the step on a non-zero exit.

**Through a server** (`--remote-url`), the server counts the rows and nulls: its answer is one
row, which no cap on a result cuts, so the counts are every row's. The server's files are not
seen from here, so `files` skips and `freshness` is unknown (`--max-age` asks for it); run `check`
where the table is for those. `--contract` is refused through a server — it streams every row,
and a server answers with only some — and so is a database table, as every remote read of one is.

**A database table** (`sqlite://…?table=orders`, `postgres://…?table=orders`) is counted in its
own database — one `count(*)` and a `count(<column>)` a column, in its own quoting — and gated
like any table. It has no files here and no recorded last change, so `files` and `freshness` skip,
or are unknown when `--max-file-jump` or `--max-age` asks for them; `--fast` and `--contract` are
refused for it. The baseline and the report carry the URI with its password masked. SQLite and
Postgres are tested; MySQL takes the same path untested.

**Against a data contract.** `--contract orders.odcs.yaml` checks every row against an ODCS v3
contract — or a Covenant one — with Covenant's validator, and fails when there are more
violations than the contract's policy allows (none, unless it says). A contract is a check on its
own, or beside a baseline:

```bash
lakeleto check warehouse/orders --contract contracts/orders.odcs.yaml
```

```text
contract  orders 1.0.0  (model orders)

  FAIL     contract   4 violations in 3 rows, 0 allowed
                        amount  min: 1, first at data row 2
                        currency  allowed: 2, first at data row 2
                        id  unique: 1, first at data row 3
```

The report counts each rule's violations and says where each first broke — never the value, so a
CI log does not keep what the table holds. The text counts data rows from 1, not counting a CSV's
header; JSON's `first_row` counts them from 0, as Covenant does. `--model <name>` picks the model when the contract has
several. A contract with rules the validator cannot enforce (a SQL quality check, say) is refused;
`--allow-unenforced` checks the rest, and the report says the check was **partial** — in the text
(`PASS, partially`), in JSON (`"partial": true` and the rules left unchecked), and in JUnit (the
test case's output). The contract is read and compiled before the table is: a contract that does
not read, a model it does not have, or rules it cannot enforce stop the check before any row is.
The rows are streamed through the SQL engine a batch at a time — a second read of the table, after
the one that counts it — and memory holds a batch, plus every distinct value seen so far of a
field the contract says is `unique`, up to `--unique-memory` (512 MiB unless given: `2G`, `256M`).
That is an estimate on the high side — 2 million 36-character ids take some 240 MiB. Past it, the
rest of the table is checked for every rule but uniqueness, duplicates already found still count,
and the gate says from which data row `unique` stopped: it is **unknown** (exit `1`), not a pass,
unless `--allow-unenforced` accepts it as partial. It needs the `contracts` feature, which builds
the SQL engine with it; the release binaries do not have it yet. A database table's contract is
refused for now.

### Into your lineage graph (`--openlineage`)

A check, a diff or an export that runs in a pipeline can say so to the lineage server the rest of
the pipeline reports to (Marquez, or any OpenLineage endpoint), with a build that has the
`openlineage` feature:

```bash
export OPENLINEAGE_URL=https://marquez.internal       # the server; dagron reads the same variable
export OPENLINEAGE_API_KEY=…                          # sent as a bearer token, never printed
lakeleto check s3://lake/orders --baseline orders.baseline.json
lakeleto --openlineage https://marquez.internal/api/v1/lineage diff orders@41 orders
```

Each run is a `START` and then a `COMPLETE` (a failed gate completed: the gate's verdict is the
result) or a `FAIL` (an error). It names the tables it read, and an export (`--out` to a file)
also names the file it wrote. A check also sends what it found: each gate and contract rule as an
assertion, and the row count and null counts as metrics. No row or value is sent. Nothing is sent
unless an endpoint is given, and an endpoint that is down costs a warning, never the command's
result. [OpenLineage events](CONFIG.md#openlineage-events) has what each event holds and how
tables are named.

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
- **Diff** — what changed between this table and another, or one of its own snapshots
  (type `@41` for version 41): the columns, the row count, each column's nulls, min and max, and
  with a **key** the rows added, removed and changed. The same report as
  [`lakeleto diff`](#what-changed-since-yesterday-diff). It reads nothing until you press
  **Compare**; exact statistics read both tables whole, so on a large table pick footers or a
  sample. Offered only by a server that answers it, as `lakeleto serve` does.
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

### Keep an answer as a table

A query you'll come back to can be kept as a table of its own, which the browser lists and every
tool reads:

```bash
lakeleto create-table marts/revenue_by_city --write-dir ./tables \
  --table orders=./orders.parquet \
  --sql "SELECT city, sum(amount) AS revenue FROM orders GROUP BY city"
```

That writes `./tables/marts/revenue_by_city.parquet`. Lakeleto never replaces a table: run it
again and it refuses, so pick a new name (`marts/revenue_by_city_2026_07`). `--sql-file
mart.sql` reads the SQL from a file. In the browser, start the server with
`lakeleto serve --write-dir ./tables` and a cached result gets a **Save as table…** button.

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
- **Iceberg and Delta tables:** a table's prefix opens it
  (`lakeleto open s3://my-bucket/warehouse/orders`), told apart by its `_delta_log/` (Delta) or
  `metadata/` (Iceberg). Its metadata, or its log and checkpoint, are read through the store, and
  its data files by ranged requests, as far as a read goes: nothing is copied to disk.
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
still sitting on disk. Partition columns are filled from the log. The replay
starts from the newest checkpoint, so a table whose old commits log cleanup has
removed reads correctly; a commit missing after the checkpoint is an error, never
a table read short. The same table in an object store opens the same way
(`lakeleto open s3://my-bucket/warehouse/delta_orders`), read as your identity,
with nothing copied to disk. (V2 checkpoints, named with a UUID or keeping their
files in `_sidecars/`, aren't read yet: a table only one can rebuild is refused,
naming it.)

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

### From a notebook or a JDBC tool (Flight SQL)

A build with the `flight` feature serves your tables to any Flight SQL client, read-only:

```bash
lakeleto flight --token s3cret --table orders=data/orders.parquet --table events=s3://lake/events/
```

From Python, with ADBC (`pip install adbc_driver_flightsql pyarrow pandas`; pandas only for the
last line):

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect(
    "grpc://127.0.0.1:50051",
    db_kwargs={"adbc.flight.sql.authorization_header": "Bearer s3cret"},
)
cur = conn.cursor()
cur.execute("SELECT region, sum(amount) AS total FROM orders GROUP BY region")
df = cur.fetch_arrow_table().to_pandas()
```

Off this machine, give it a certificate: `--tls-cert` and `--tls-key` (PEM) make it
`grpc+tls://host:50051`. Plaintext on any address but loopback is refused unless you pass
`--plaintext`, for a TLS proxy or tunnel in front. A client trusts a private certificate with
`"adbc.flight.sql.client_option.tls_root_certs": open("ca.pem").read()` in `db_kwargs`.

Each table is `lakeleto_tables.main.<name>`, which is how a tool that browses catalogs lists it.
Every statement is one read query. A result past `--max-rows` (a million rows unless set) is an
error, not a cut-off answer. [Flight SQL server](CONFIG.md#flight-sql-server) has the details.

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
