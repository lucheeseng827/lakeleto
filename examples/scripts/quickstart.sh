#!/usr/bin/env bash
#
# quickstart.sh — the whole loop end to end, so a newcomer sees the value in one command:
#
#     import  →  export  →  DuckDB intermediate table  →  BI-ready
#
#   1. import.sh     load a sample file, create a workspace + a saved data source
#   2. export.sh     export it as Parquet (the "different datasource" a BI tool can read)
#   3. export.sh     also produce a shaped SQL *report* (revenue by city) as Parquet
#   4. to_duckdb.sh  materialize both into out/reports.duckdb as intermediate tables
#
# By default it runs against the bundled examples/sample-data/orders.csv and writes everything
# under ./lakeleto-out/. Point --path at your own file to do the real thing.
#
# The server must already be running (start it once: `lakeleto serve --features serve,sql`).
# Pass --serve to have this script start a throwaway server for the duration and stop it after.
#
# Usage:
#   ./quickstart.sh                          # sample data, server already running
#   ./quickstart.sh --serve                  # start a local server too (needs the lakeleto binary or cargo)
#   ./quickstart.sh --path /data/sales.parquet --out-dir ./bi
#
# Options:
#   --path PATH     data file to run through the loop  (default: bundled orders.csv, absolute)
#   --out-dir DIR   where exports + the .duckdb land    (default: ./lakeleto-out)
#   --serve         start a local `lakeleto serve` for this run and stop it at the end
#   --report-sql SQL  the SQL report (over table `t`)   (default: revenue by city)
#   --amount-col C  numeric column to sum in the default report   (default: amount_usd)
#   --group-col C   grouping column in the default report         (default: city)

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
. "${HERE}/common.sh"

SAMPLE_DEFAULT="$(cd "${HERE}/.." && pwd)/sample-data/orders.csv"
DATA_PATH="" OUT_DIR="./lakeleto-out" DO_SERVE="" REPORT_SQL="" AMOUNT_COL="amount_usd" GROUP_COL="city"
while [ $# -gt 0 ]; do
  case "$1" in
    --path)       DATA_PATH="$2"; shift 2 ;;
    --out-dir)    OUT_DIR="$2"; shift 2 ;;
    --serve)      DO_SERVE="1"; shift ;;
    --report-sql) REPORT_SQL="$2"; shift 2 ;;
    --amount-col) AMOUNT_COL="$2"; shift 2 ;;
    --group-col)  GROUP_COL="$2"; shift 2 ;;
    -h|--help)    awk 'NR==1{next} /^#/{sub(/^# ?/,"");print;next}{exit}' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[ -n "$DATA_PATH" ] || DATA_PATH="$SAMPLE_DEFAULT"
[ -n "$REPORT_SQL" ] || REPORT_SQL="SELECT ${GROUP_COL} AS ${GROUP_COL}, count(*) AS orders, round(sum(${AMOUNT_COL}),2) AS revenue FROM t GROUP BY ${GROUP_COL} ORDER BY revenue DESC"

require curl jq
mkdir -p "$OUT_DIR"

# Optionally stand up a throwaway server for the demo, torn down on exit ------------------
SERVER_PID=""
cleanup() { [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true; }
trap cleanup EXIT

if [ -n "$DO_SERVE" ] && ! lakeleto_health; then
  if command -v lakeleto >/dev/null 2>&1; then
    launcher=(lakeleto)
  elif command -v cargo >/dev/null 2>&1; then
    launcher=(cargo run -q --features serve,sql --bin lakeleto --)
    log "no 'lakeleto' binary — building via cargo (first run compiles, be patient)…"
  else
    echo "quickstart.sh: --serve needs the 'lakeleto' binary or 'cargo' on PATH" >&2; exit 1
  fi
  log "starting a local server on ${LAKELETO_URL}…"
  "${launcher[@]}" serve >/dev/null 2>&1 &
  SERVER_PID=$!
  lakeleto_wait_health 300 || { echo "quickstart.sh: server didn't become healthy" >&2; exit 1; }
fi

lakeleto_require_up

echo
log "STEP 1/4 — import ${DATA_PATH}"
summary="$("${HERE}/import.sh" --path "$DATA_PATH" --name "Quickstart")"
WS_ID="$(printf '%s' "$summary" | jq -r '.workspace_id')"
log "workspace = ${WS_ID}"

echo
log "STEP 2/4 — export the raw table as Parquet (a BI-ready datasource)"
RAW_PARQUET="${OUT_DIR}/raw.parquet"
"${HERE}/export.sh" --path "$DATA_PATH" --out "$RAW_PARQUET" --format parquet >/dev/null

echo
log "STEP 3/4 — export a shaped SQL report as Parquet"
log "  ${REPORT_SQL}"
REPORT_PARQUET="${OUT_DIR}/report_by_${GROUP_COL}.parquet"
"${HERE}/export.sh" --path "$DATA_PATH" --out "$REPORT_PARQUET" \
  --workspace "$WS_ID" --sql "$REPORT_SQL" >/dev/null

echo
log "STEP 4/4 — build DuckDB intermediate tables for BI"
DB="${OUT_DIR}/reports.duckdb"
"${HERE}/to_duckdb.sh" --input "$RAW_PARQUET"    --db "$DB" --table raw_orders    >/dev/null
"${HERE}/to_duckdb.sh" --input "$REPORT_PARQUET" --db "$DB" --table "report_by_${GROUP_COL}" >/dev/null

echo
log "ALL DONE ✓"
echo "    exports : ${RAW_PARQUET}, ${REPORT_PARQUET}" >&2
echo "    duckdb  : ${DB}  (tables: raw_orders, report_by_${GROUP_COL})" >&2
echo >&2
echo "  Peek at the report:" >&2
echo "    duckdb ${DB} -c 'SELECT * FROM report_by_${GROUP_COL} LIMIT 10;'" >&2
echo "  Open the DuckDB browser UI:" >&2
echo "    duckdb -ui ${DB}" >&2
echo "  Or connect Power BI / Tableau / Metabase / Superset to ${DB} via the DuckDB ODBC/JDBC driver." >&2
