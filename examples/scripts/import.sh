#!/usr/bin/env bash
#
# import.sh — the "get started" step: load a data file into Lakeleto and create a workspace
#             around it.
#
# What it does (all over the REST API, nothing is uploaded — the server reads the bytes locally):
#   1. POST /v1/workspaces                 → create a named workspace (or reuse one via --workspace)
#   2. PUT  /v1/workspaces/{id}            → register the file as a saved *Connection* (data source)
#   3. GET  /v1/schema                     → show columns / types / row count so you see it worked
#   4. POST /v1/workspaces/{id}/runs       → cache a preview so the source opens instantly later
#
# The path is resolved by the *server* (relative to where `lakeleto serve` runs, or absolute, or
# an object-store URI like s3://bucket/key.parquet). If the server was started with --root, the
# path must live under that root.
#
# Prints a JSON summary { workspace_id, connection_id, path, rows, columns } on stdout so the
# other scripts (export.sh, quickstart.sh) can chain off it; human progress goes to stderr.
#
# Usage:
#   ./import.sh --path examples/sample-data/orders.csv
#   ./import.sh --path s3://bucket/events.parquet --name "Events" --label "prod events"
#   LAKELETO_URL=http://box:8080 LAKELETO_TOKEN=… ./import.sh --path /data/sales.parquet
#
# Options:
#   --path PATH        data file / directory / object-store URI to import   (required)
#   --name NAME        workspace name to create        (default: "Imported data")
#   --workspace ID     add the source to an existing workspace instead of creating one
#   --label LABEL      connection label in the UI       (default: the file's basename)
#   --format FMT       force a format (parquet|csv|tsv|json|iceberg|delta)  (default: auto-detect)
#   --preview N        rows to cache for instant re-open (default: 50)

set -euo pipefail
# shellcheck source=common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

PATH_ARG="" WS_NAME="Imported data" WS_ID="" LABEL="" FORMAT="" PREVIEW=50
while [ $# -gt 0 ]; do
  case "$1" in
    --path)      PATH_ARG="$2"; shift 2 ;;
    --name)      WS_NAME="$2"; shift 2 ;;
    --workspace) WS_ID="$2"; shift 2 ;;
    --label)     LABEL="$2"; shift 2 ;;
    --format)    FORMAT="$2"; shift 2 ;;
    --preview)   PREVIEW="$2"; shift 2 ;;
    -h|--help)   awk 'NR==1{next} /^#/{sub(/^# ?/,"");print;next}{exit}' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

[ -n "$PATH_ARG" ] || { echo "import.sh: --path is required (see --help)" >&2; exit 2; }
[ -n "$LABEL" ] || LABEL="$(basename "$PATH_ARG")"

require curl jq
lakeleto_require_up

# 1. Create the workspace (or use the one passed in) --------------------------------------
if [ -z "$WS_ID" ]; then
  log "creating workspace '${WS_NAME}'…"
  ws_json="$(api POST /v1/workspaces \
    -H 'content-type: application/json' \
    --data "$(jq -nc --arg n "$WS_NAME" '{name:$n}')")" || exit 1
else
  log "using existing workspace ${WS_ID}…"
  ws_json="$(api GET "/v1/workspaces/${WS_ID}")" || exit 1
fi
WS_ID="$(printf '%s' "$ws_json" | jq -r '.id')"

# 2. Register the file as a saved Connection (the "data source") --------------------------
conn_id="conn-$(date +%s)-${RANDOM}"
log "registering data source '${LABEL}' → ${PATH_ARG}"
updated_ws="$(printf '%s' "$ws_json" | jq \
  --arg id "$conn_id" --arg label "$LABEL" --arg path "$PATH_ARG" --arg fmt "$FORMAT" '
    .connections += [
      ( { id:$id, label:$label, path:$path, pinned:true }
        + ( if $fmt == "" then {} else { format:$fmt } end ) )
    ]')"
api PUT "/v1/workspaces/${WS_ID}" \
  -H 'content-type: application/json' \
  --data "$updated_ws" >/dev/null || exit 1

# 3. Confirm the source reads — schema (columns / types / exact row count where known) ----
fmt_q=()
[ -n "$FORMAT" ] && fmt_q=(--data-urlencode "format=${FORMAT}")
schema_json="$(api GET /v1/schema -G --data-urlencode "path=${PATH_ARG}" \
  "${fmt_q[@]+"${fmt_q[@]}"}")" || exit 1
n_cols="$(printf '%s' "$schema_json" | jq '.columns | length')"
n_rows="$(printf '%s' "$schema_json" | jq -r '.row_count // "unknown"')"

log "schema: ${n_cols} columns, ${n_rows} rows"
printf '%s' "$schema_json" | jq -r '.columns[] | "    \(.name): \(.data_type)"' >&2

# 4. Cache a preview so the source re-opens instantly (a raw scan, no SQL needed) ---------
log "caching a ${PREVIEW}-row preview…"
api POST "/v1/workspaces/${WS_ID}/runs" \
  -H 'content-type: application/json' \
  --data "$(jq -nc --arg p "$PATH_ARG" --argjson prev "$PREVIEW" '{path:$p, preview:$prev}')" \
  >/dev/null || log "preview cache skipped (source scanned, but not cached)"

log "imported ✓  workspace=${WS_ID}  connection=${conn_id}"

# Machine-readable summary on stdout for chaining.
jq -nc \
  --arg ws "$WS_ID" --arg conn "$conn_id" --arg path "$PATH_ARG" \
  --argjson cols "$n_cols" --arg rows "$n_rows" \
  '{workspace_id:$ws, connection_id:$conn, path:$path, columns:$cols, rows:$rows}'
