#!/usr/bin/env bash
#
# export.sh — export a Lakeleto source "as a different datasource" (Parquet / CSV / JSON) so a
#             BI tool (or the DuckDB step, to_duckdb.sh) can pick it up.
#
# Two modes:
#
#   plain view  (default) — GET /v1/export?path=&fmt=&filter=&sort=&cols=
#       A straight extract of the source in the format you ask for, optionally filtered / sorted /
#       projected. No workspace or SQL engine required. Great for "give BI the whole table as
#       Parquet" or "just the columns I care about, filtered".
#
#   SQL report  (--sql) — POST /v1/workspaces/{id}/runs then GET …/runs/{run_id}/result
#       Shapes the data with a read-only SELECT (the source is the table `t`), caches the result,
#       and downloads it. The cached result is always Parquet — perfect as the "intermediate" that
#       to_duckdb.sh materializes. Needs a workspace (--workspace) and a server built with `sql`.
#
# Usage:
#   ./export.sh --path examples/sample-data/orders.csv --out out/orders.parquet
#   ./export.sh --path orders.csv --out out/orders.csv  --format csv --cols order_id,city,amount_usd
#   ./export.sh --path orders.csv --out out/big.parquet --filter amount_usd:ge:100 --sort amount_usd --desc
#   ./export.sh --path orders.csv --out out/by_city.parquet --workspace $WS \
#       --sql "SELECT city, count(*) AS orders, round(sum(amount_usd),2) AS revenue \
#              FROM t GROUP BY city ORDER BY revenue DESC"
#
# Options:
#   --path PATH        source to export (file / dir / object-store URI)     (required)
#   --out FILE         output file (parent dirs are created)                (required)
#   --format FMT       plain-view format: parquet|csv|json                  (default: parquet)
#   --filter C:OP:V    plain-view filter, repeatable (op = eq|ne|lt|le|gt|ge|contains)
#   --cols A,B,C       plain-view column projection
#   --sort COL         plain-view sort column
#   --desc             sort descending
#   --sql "SELECT …"   SQL-report mode (result is Parquet); requires --workspace
#   --workspace ID     workspace to run the SQL in (from import.sh)
#   --limit N          SQL-report row cap (default: 100000, server max 100000)

set -euo pipefail
# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

PATH_ARG="" OUT="" FORMAT="parquet" SORT="" DESC="" SQL="" WS_ID="" LIMIT=100000
FILTERS=() COLS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --path)      PATH_ARG="$2"; shift 2 ;;
    --out)       OUT="$2"; shift 2 ;;
    --format)    FORMAT="$2"; shift 2 ;;
    --filter)    FILTERS+=("$2"); shift 2 ;;
    --cols)      COLS="$2"; shift 2 ;;
    --sort)      SORT="$2"; shift 2 ;;
    --desc)      DESC="1"; shift ;;
    --sql)       SQL="$2"; shift 2 ;;
    --workspace) WS_ID="$2"; shift 2 ;;
    --limit)     LIMIT="$2"; shift 2 ;;
    -h|--help)   awk 'NR==1{next} /^#/{sub(/^# ?/,"");print;next}{exit}' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

[ -n "$PATH_ARG" ] || { echo "export.sh: --path is required (see --help)" >&2; exit 2; }
[ -n "$OUT" ]      || { echo "export.sh: --out is required (see --help)"  >&2; exit 2; }

require curl
mkdir -p "$(dirname "$OUT")"
lakeleto_require_up

if [ -n "$SQL" ]; then
  # ---- SQL report mode: run in the workspace, download the cached Parquet result ---------
  require jq
  [ -n "$WS_ID" ] || { echo "export.sh: --sql needs --workspace ID (run import.sh first)" >&2; exit 2; }
  log "running report SQL in workspace ${WS_ID}…"
  run_json="$(api POST "/v1/workspaces/${WS_ID}/runs" \
    -H 'content-type: application/json' \
    --data "$(jq -nc --arg sql "$SQL" --arg p "$PATH_ARG" --argjson lim "$LIMIT" \
                '{sql:$sql, path:$p, limit:$lim, preview:1}')")" || exit 1

  status="$(printf '%s' "$run_json" | jq -r '.run.status')"
  run_id="$(printf '%s' "$run_json" | jq -r '.run.id')"
  rows="$(printf '%s' "$run_json" | jq -r '.run.row_count // "?"')"
  if [ "$status" != "ok" ]; then
    log "query failed: $(printf '%s' "$run_json" | jq -r '.run.error // "unknown error"')"
    exit 1
  fi
  log "report ok: ${rows} rows (run ${run_id}); downloading Parquet result…"
  api_download GET "/v1/workspaces/${WS_ID}/runs/${run_id}/result" "$OUT" || exit 1
  [ "$FORMAT" = "parquet" ] || log "note: SQL-report results are Parquet; ignoring --format ${FORMAT}"
else
  # ---- plain view mode: GET /v1/export with url-encoded params ----------------------------
  args=(-G --data-urlencode "path=${PATH_ARG}" --data-urlencode "fmt=${FORMAT}")
  [ -n "$SORT" ] && args+=(--data-urlencode "sort=${SORT}")
  [ -n "$DESC" ] && args+=(--data-urlencode "desc=1")
  [ -n "$COLS" ] && args+=(--data-urlencode "cols=${COLS}")
  for f in "${FILTERS[@]+"${FILTERS[@]}"}"; do
    args+=(--data-urlencode "filter=${f}")
  done
  log "exporting ${PATH_ARG} → ${OUT} (${FORMAT})"
  api_download GET /v1/export "$OUT" "${args[@]}" || exit 1
fi

bytes="$(wc -c < "$OUT" | tr -d ' ')"
log "wrote ${OUT} (${bytes} bytes) ✓"
printf '%s\n' "$OUT"
