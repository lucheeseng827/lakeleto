# Lakeleto import/export scripts — from a data file to a BI-ready DuckDB table

Prebuilt Bash **and** PowerShell scripts that drive the [Lakeleto REST API](../../README.md) to
walk a newcomer through the whole loop:

```
 your data ──▶ import ──▶ export ──▶ DuckDB intermediate table ──▶ BI (Power BI / Tableau / Metabase / …)
              (workspace   (Parquet /  (materialized, fast,
               + source)    CSV / JSON)  file-backed)
```

Each stage is one small script that talks to a **running `lakeleto serve`** over HTTP — nothing is
uploaded, the server reads your bytes locally. The scripts are pure REST clients, so they don't
need the Rust build; they just need the server up.

| Stage | Bash | PowerShell | REST it calls |
| --- | --- | --- | --- |
| shared helpers | `common.sh` | `LakeletoCommon.ps1` | — |
| **import** — workspace + data source | `import.sh` | `Import-LakeletoData.ps1` | `POST /v1/workspaces`, `PUT /v1/workspaces/{id}`, `GET /v1/schema`, `POST …/runs` |
| **export** — as a different datasource | `export.sh` | `Export-LakeletoData.ps1` | `GET /v1/export`, or `POST …/runs` + `GET …/runs/{id}/result` |
| **to DuckDB** — intermediate table | `to_duckdb.sh` | `ConvertTo-DuckDbTable.ps1` | (local `duckdb` over the export) |
| **quickstart** — all of the above | `quickstart.sh` | `Start-LakeletoQuickstart.ps1` | orchestrates the three |

## Prerequisites

- A running server: `lakeleto serve --features serve,sql` (the release binary already has every
  engine — just `lakeleto serve`). The `sql` engine is only needed for the shaped-report export.
- **Bash:** `curl`, `jq` (+ `duckdb` for the BI step).
- **PowerShell:** 7+ (`pwsh`) (+ `duckdb` for the BI step).
- Config via environment: `LAKELETO_URL` (default `http://127.0.0.1:8080`) and, if the server was
  started with `--token`, `LAKELETO_TOKEN`. The token is always sent as an `Authorization: Bearer`
  header, never a `?token=` query param.

> **Paths are resolved by the server.** A `path` you pass is opened by the `lakeleto serve`
> process — relative to *its* working directory, or an absolute path, or an object-store URI
> (`s3://…`, `gs://…`, `az://…`). If the server was started with `--root DIR`, the path must live
> under `DIR`.

## 60-second quickstart

Start a server in one terminal (from the repo root, so it can see the sample data):

```bash
cargo run --features serve,sql --bin lakeleto -- serve
```

Then in another terminal:

```bash
cd ./examples/scripts
./quickstart.sh                 # bash
# pwsh ./Start-LakeletoQuickstart.ps1   # PowerShell
```

That imports the bundled `orders.csv`, exports it as Parquet, builds a *revenue-by-city* report,
and materializes both into `lakeleto-out/reports.duckdb`. Prefer it to start its own throwaway
server? Add `--serve` (bash) / `-Serve` (PowerShell).

Peek at the result:

```bash
duckdb lakeleto-out/reports.duckdb -c 'SELECT * FROM report_by_city LIMIT 10;'
duckdb -ui lakeleto-out/reports.duckdb        # DuckDB's browser UI
```

## Step by step (your own data)

```bash
# 1. Import: create a workspace and register your file as a saved data source.
#    Prints a JSON summary; capture the workspace id for the SQL-report export.
summary=$(./import.sh --path /data/sales.parquet --name "Sales")
WS=$(printf '%s' "$summary" | jq -r .workspace_id)

# 2a. Export the whole table as Parquet (or CSV/JSON) for BI.
./export.sh --path /data/sales.parquet --out out/sales.parquet

# 2b. Export a shaped SQL *report* (revenue by region) — result is Parquet.
./export.sh --path /data/sales.parquet --out out/by_region.parquet --workspace "$WS" \
  --sql "SELECT region, count(*) AS orders, round(sum(amount),2) AS revenue
         FROM t GROUP BY region ORDER BY revenue DESC"

# 3. Build DuckDB intermediate tables BI tools query directly.
./to_duckdb.sh --input out/sales.parquet    --db out/reports.duckdb --table sales
./to_duckdb.sh --input out/by_region.parquet --db out/reports.duckdb --table sales_by_region
```

PowerShell is identical in shape:

```powershell
$summary = ./Import-LakeletoData.ps1 -Path C:\data\sales.parquet -Name Sales
$ws = $summary.WorkspaceId
./Export-LakeletoData.ps1 -Path C:\data\sales.parquet -Out out\sales.parquet
./Export-LakeletoData.ps1 -Path C:\data\sales.parquet -Out out\by_region.parquet -Workspace $ws `
  -Sql "SELECT region, count(*) AS orders, round(sum(amount),2) AS revenue FROM t GROUP BY region ORDER BY revenue DESC"
./ConvertTo-DuckDbTable.ps1 -InputFile out\sales.parquet -Db out\reports.duckdb -Table sales
```

## Why DuckDB in the middle?

The Lakeleto export is a plain file (Parquet/CSV). DuckDB reads it in place and **materializes**
it into a compact, file-backed table (`CREATE OR REPLACE TABLE … AS SELECT …`) — an *intermediate*
that a BI tool hits without re-scanning the source each refresh. You can shape while you
materialize: the export is exposed as the view `src`, so `--transform-sql "… FROM src …"` builds an
aggregate/curated table (star-schema fact, daily rollup, etc.). One `.duckdb` file can hold many
such tables and is trivially portable.

BI tools connect to the `.duckdb` file via the DuckDB **ODBC/JDBC** driver (Power BI, Tableau,
Excel), a native connector (Metabase, Superset), or `duckdb -ui` for a quick look.

## Two kinds of "export"

- **Data export** (these scripts): the *rows* as Parquet/CSV/JSON, for BI — `GET /v1/export` or a
  cached SQL run result.
- **Workspace export** (`GET /v1/workspaces/{id}/export`): a portable *workspace bundle* (JSON:
  connections, saved queries, history) for moving your setup between machines — not covered here.

## Options

Run any script with `--help` (bash) or `Get-Help ./Name.ps1 -Full` (PowerShell) for the full flag
list — including plain-view `--filter col:op:val` / `--cols a,b` / `--sort`, and the DuckDB
`--transform-sql` / `--view` shaping options.
