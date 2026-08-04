#!/usr/bin/env bash
#
# common.sh — shared helpers for the Lakeleto REST scripts (source it, don't run it).
#
# Everything here talks to a *running* `lakeleto serve` over HTTP, so the only tools you
# need are `curl` and `jq` (and `duckdb` for the BI step). Nothing depends on the Rust build.
#
# Config (all overridable via the environment):
#   LAKELETO_URL     base URL of the server            (default http://127.0.0.1:8080)
#   LAKELETO_TOKEN   bearer token, if the server was started with --token   (default: none)
#
# Every request sends the token as an `Authorization: Bearer` header — never a `?token=`
# query param — because a token in a URL leaks through shell/proxy logs (see the README).

set -euo pipefail

LAKELETO_URL="${LAKELETO_URL:-http://127.0.0.1:8080}"
LAKELETO_TOKEN="${LAKELETO_TOKEN:-}"

# Bound every request so a hung/slow server can't block the pipeline forever. `--connect-timeout`
# caps establishing the TCP/TLS connection; `--max-time` caps the whole transfer — generous by
# default so a large export/report isn't cut off, and overridable (raise LAKELETO_MAX_TIME for
# very large downloads). These defaults sit *before* the caller's args in the curl line, so a
# per-call override still wins (curl honours the last value).
LAKELETO_CONNECT_TIMEOUT="${LAKELETO_CONNECT_TIMEOUT:-10}"
LAKELETO_MAX_TIME="${LAKELETO_MAX_TIME:-300}"

# Auth header as an array so it expands to *nothing* when no token is set (safe under `set -u`).
LAKELETO_CURL_AUTH=()
if [ -n "${LAKELETO_TOKEN}" ]; then
  LAKELETO_CURL_AUTH=(-H "Authorization: Bearer ${LAKELETO_TOKEN}")
fi

# require cmd...  — fail loudly if a prerequisite is missing.
require() {
  local missing=0 c
  for c in "$@"; do
    if ! command -v "$c" >/dev/null 2>&1; then
      echo "lakeleto: missing required command: $c" >&2
      missing=1
    fi
  done
  [ "$missing" -eq 0 ]
}

# log MSG...  — human-facing progress on stderr (stdout stays machine-readable for chaining).
log() { echo "lakeleto: $*" >&2; }

# api METHOD PATH [curl args...]  — JSON request. Prints the response body on success;
# on a transport error or non-2xx status prints the error to stderr and returns 1.
# Use as:  if resp=$(api GET /v1/schema ...); then ...; fi     (or)   resp=$(api ...) || exit 1
api() {
  local method="$1" path="$2"; shift 2
  local url="${LAKELETO_URL%/}${path}" tmp err code
  tmp="$(mktemp)"; err="${tmp}.err"
  if ! code="$(curl -sS -X "$method" \
      --connect-timeout "$LAKELETO_CONNECT_TIMEOUT" --max-time "$LAKELETO_MAX_TIME" \
      "${LAKELETO_CURL_AUTH[@]+"${LAKELETO_CURL_AUTH[@]}"}" \
      -o "$tmp" -w '%{http_code}' "$@" "$url" 2>"$err")"; then
    log "request failed: $method $url"
    sed 's/^/  /' "$err" >&2 2>/dev/null || true
    rm -f "$tmp" "$err"; return 1
  fi
  rm -f "$err"
  if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
    log "HTTP $code — $method $path"
    sed 's/^/  /' "$tmp" >&2 2>/dev/null || true
    rm -f "$tmp"; return 1
  fi
  cat "$tmp"; rm -f "$tmp"
}

# api_download METHOD PATH OUTFILE [curl args...]  — write the (possibly binary) body to OUTFILE.
# Downloads to a sibling temp file and only moves it into place on success, so a failed request
# (transport error or non-2xx) never overwrites or deletes an existing OUTFILE. On non-2xx the
# (JSON) error body is shown; the temp file is always cleaned up on failure.
api_download() {
  local method="$1" path="$2" out="$3"; shift 3
  local url="${LAKELETO_URL%/}${path}" code dir tmp
  dir="$(dirname "$out")"
  tmp="$(mktemp "${dir}/.$(basename "$out").part.XXXXXX")" || {
    log "cannot create a temp file in ${dir}"; return 1; }
  if ! code="$(curl -sS -X "$method" \
      --connect-timeout "$LAKELETO_CONNECT_TIMEOUT" --max-time "$LAKELETO_MAX_TIME" \
      "${LAKELETO_CURL_AUTH[@]+"${LAKELETO_CURL_AUTH[@]}"}" \
      -o "$tmp" -w '%{http_code}' "$@" "$url")"; then
    log "request failed: $method $url"; rm -f "$tmp"; return 1
  fi
  if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
    log "HTTP $code — $method $path"
    sed 's/^/  /' "$tmp" >&2 2>/dev/null || true
    rm -f "$tmp"; return 1
  fi
  # Same-directory rename: atomic on the same filesystem, leaving no partial OUTFILE.
  mv -f "$tmp" "$out"
}

# lakeleto_health  — 0 if /healthz answers ok (short timeouts: it's polled in a wait loop).
lakeleto_health() {
  curl -fsS --connect-timeout 3 --max-time 5 "${LAKELETO_URL%/}/healthz" >/dev/null 2>&1
}

# lakeleto_wait_health [tries]  — poll /healthz once a second up to `tries` times (default 30).
lakeleto_wait_health() {
  local tries="${1:-30}" i
  for ((i = 0; i < tries; i++)); do
    lakeleto_health && return 0
    sleep 1
  done
  return 1
}

# lakeleto_require_up  — friendly bail-out with the exact start command when the server is down.
lakeleto_require_up() {
  if ! lakeleto_health; then
    log "no server at ${LAKELETO_URL} — start one first, e.g.:"
    echo "    lakeleto serve --features serve,sql            # release binary" >&2
    echo "    cargo run --features serve,sql --bin lakeleto -- serve" >&2
    return 1
  fi
}
