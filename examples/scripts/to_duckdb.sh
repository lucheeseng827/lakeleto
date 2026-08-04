#!/usr/bin/env bash
#
# to_duckdb.sh — build a DuckDB *intermediate table* from a Lakeleto export, ready for BI.
#
# Lakeleto exports the raw/shaped data (see export.sh); DuckDB then materializes it into a small,
# fast, file-backed table that BI tools (Power BI, Tableau, Metabase, Superset, Excel via the
# DuckDB ODBC/JDBC driver, or `duckdb -ui`) can query directly — no server, no re-reading the
# source each time.
#
# It reads the export in place (read_parquet / read_csv_auto), so the export file is never copied
# into the DB unless you materialize it — which is exactly the point of an intermediate table.
#
# Usage:
#   ./to_duckdb.sh --input out/orders.parquet --db out/reports.duckdb --table orders
#
#   # shape while materializing — the export is exposed as the view `src`:
#   ./to_duckdb.sh --input out/orders.parquet --db out/reports.duckdb --table orders_by_city \
#       --transform-sql "SELECT city, count(*) AS orders, round(sum(amount_usd),2) AS revenue \
#                        FROM src GROUP BY city ORDER BY revenue DESC"
#
#   # add a curated BI view on top of the table:
#   ./to_duckdb.sh --input out/orders.parquet --db out/reports.duckdb --table orders \
#       --view top_cities --view-sql "SELECT city, sum(amount_usd) rev FROM orders \
#                                     GROUP BY city ORDER BY rev DESC LIMIT 10"
#
# Options:
#   --input FILE          exported .parquet / .csv / .tsv / .json (from export.sh)   (required)
#   --db FILE             DuckDB database file to create/append                      (required)
#   --table NAME          intermediate table to (re)create                          (required)
#   --transform-sql SQL   build the table from this SELECT (FROM `src`); default: SELECT * FROM src
#   --view NAME           also (re)create a named view for BI
#   --view-sql SQL        the view body (required with --view)
#   --format FMT          override reader: parquet|csv|tsv|json  (default: from --input extension)

set -euo pipefail
# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

INPUT="" DB="" TABLE="" TRANSFORM="" VIEW="" VIEW_SQL="" FMT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --input)         INPUT="$2"; shift 2 ;;
    --db)            DB="$2"; shift 2 ;;
    --table)         TABLE="$2"; shift 2 ;;
    --transform-sql) TRANSFORM="$2"; shift 2 ;;
    --view)          VIEW="$2"; shift 2 ;;
    --view-sql)      VIEW_SQL="$2"; shift 2 ;;
    --format)        FMT="$2"; shift 2 ;;
    -h|--help)       awk 'NR==1{next} /^#/{sub(/^# ?/,"");print;next}{exit}' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

[ -n "$INPUT" ] || { echo "to_duckdb.sh: --input is required (see --help)" >&2; exit 2; }
[ -n "$DB" ]    || { echo "to_duckdb.sh: --db is required (see --help)"    >&2; exit 2; }
[ -n "$TABLE" ] || { echo "to_duckdb.sh: --table is required (see --help)" >&2; exit 2; }
[ -f "$INPUT" ] || { echo "to_duckdb.sh: no such input file: $INPUT" >&2; exit 2; }
[ -n "$VIEW" ] && [ -z "$VIEW_SQL" ] && { echo "to_duckdb.sh: --view needs --view-sql" >&2; exit 2; }

require duckdb
mkdir -p "$(dirname "$DB")"

# Pick a DuckDB reader from the format (explicit --format wins, else the file extension).
ext="${FMT:-${INPUT##*.}}"
case "$(printf '%s' "$ext" | tr '[:upper:]' '[:lower:]')" in
  parquet|pq)  reader="read_parquet" ;;
  csv)         reader="read_csv_auto" ;;
  tsv)         reader="read_csv_auto" ;;  # DuckDB sniffs the tab delimiter
  json|ndjson|jsonl) reader="read_json_auto" ;;
  *) echo "to_duckdb.sh: don't know how to read '.${ext}' — pass --format" >&2; exit 2 ;;
esac

# Absolute path + single-quote escaping so DuckDB's string literal is safe.
abs_input="$(cd "$(dirname "$INPUT")" && pwd)/$(basename "$INPUT")"
esc_input="${abs_input//\'/\'\'}"

body="${TRANSFORM:-SELECT * FROM src}"

# Escape the table/view names for the two SQL contexts they land in: a "quoted identifier"
# (double any embedded ") and, for the row-count line, a 'string literal' (double any embedded ').
tbl_id="${TABLE//\"/\"\"}"
tbl_lit="${TABLE//\'/\'\'}"
view_id="${VIEW//\"/\"\"}"

log "materializing ${TABLE} in ${DB} from ${INPUT} (${reader})…"
# `src` is a scratch staging view — TEMP so it never clobbers (or leaves behind) a real `src`.
sql="CREATE OR REPLACE TEMP VIEW src AS SELECT * FROM ${reader}('${esc_input}');"
sql+="CREATE OR REPLACE TABLE \"${tbl_id}\" AS ${body};"
if [ -n "$VIEW" ]; then
  sql+="CREATE OR REPLACE VIEW \"${view_id}\" AS ${VIEW_SQL};"
fi
sql+="DROP VIEW src;"                                                       # keep the DB tidy
sql+="SELECT '${tbl_lit}' AS table_name, count(*) AS rows FROM \"${tbl_id}\";"

duckdb "$DB" -c "$sql"

log "done ✓  table '${TABLE}'${VIEW:+ (+ view '${VIEW}')} is in ${DB}"
log "BI next steps:"
echo "    duckdb ${DB} -c 'SELECT * FROM \"${TABLE}\" LIMIT 20;'" >&2
echo "    duckdb -ui ${DB}                        # open DuckDB's browser UI on this DB" >&2
echo "    # or point Power BI / Tableau / Metabase at ${DB} via the DuckDB ODBC/JDBC driver" >&2
printf '%s\n' "$DB"
