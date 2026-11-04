# lakeleto: the commands for building, testing and using this crate, callable.
#
#   make                 # this list, grouped
#   make schema          # what is in examples/people.csv
#   make serve           # the HTTP API and the web app on 127.0.0.1:8080
#   make api-smoke       # start a server, call every read endpoint, stop it
#   make ci              # what CI runs, in its order
#
# A target here runs a command this crate already documents or runs in CI: README.md,
# CONTRIBUTING.md, docs/, the Dockerfiles, examples/scripts/ and the CI workflows. It is a
# shortcut, not a second way to run lakeleto: what `make` does is still true typed out by hand.
#
# Run it from this directory, the crate root: the paths below are relative to it.

.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash
# A failure anywhere in a pipeline fails the target, `curl ... | jq .` included. GNU make reads
# .SHELLFLAGS from 3.82 on; under an older make, such as macOS's 3.81, a failed request piped
# into jq can look like a success.
.SHELLFLAGS := -eo pipefail -c

# --- knobs: override any of them, as in `make head FILE=data.parquet N=5` --------------------

CARGO  ?= cargo
DOCKER ?= docker
CURL   ?= curl
PYTHON ?= python3

# The crate, for cargo's -p.
PKG ?= -p lakeleto

# Feature sets. FEATURES is what the run targets below build with. It defaults to the release
# set, so `make serve`, `make catalog-ls` and `make mcp` behave as the released binaries do.
# FEATURES= gives the lean default build, the one a plain `cargo install` makes.
#
# RELEASE_FEATURES is read from the release workflow, so it cannot drift from what ships: that
# is ops/release.yml in the source tree and .github/workflows/release.yml in the public repo.
# DB_FEATURES is CI's third set: the database drivers, object stores and MCP, without `sql`.
RELEASE_YML      := $(firstword $(wildcard ops/release.yml .github/workflows/release.yml))
RELEASE_FEATURES ?= $(if $(RELEASE_YML),$(shell sed -n 's/^  FEATURES: *//p' $(RELEASE_YML)))
DB_FEATURES      ?= serve,object-store,sqlite,postgres,mysql,mcp
FEATURES         ?= $(RELEASE_FEATURES)

# What the CLI targets read, and how they print. FMT is `-o` (table, json, ndjson, csv, tsv,
# and for rows also parquet, arrow, arrows); OUT is `--out`, a file instead of stdout.
FILE    ?= examples/people.csv
PARQUET ?= examples/sample-data/orders.parquet
N       ?= 10
SCAN    ?= 10000
SQL     ?= SELECT city, count(*) AS n, avg(score) AS avg_score FROM t GROUP BY city ORDER BY n DESC
FMT     ?=
OUT     ?=

# `serve` and `open`, and the server the API targets call.
ADDR         ?= 127.0.0.1:8080
ROOT         ?=
TOKEN        ?=
DEFAULT_SCAN ?= 10000
URL          ?= http://$(ADDR)

# The API targets' parameters: GET /v1/list?dir=, and a /v1/rows window.
DIR    ?= examples
OFFSET ?= 0
LIMIT  ?= 20
SORT   ?=
DESC   ?=
FILTER ?=
COLS   ?=
EXPORT ?= csv

# Catalogs, MCP and remote servers.
CATALOG    ?=
MCP_ROOT   ?= examples
MCP_FILE   ?= people.csv
REMOTE_URL ?= $(URL)

# Where `api-smoke` runs its own server.
SMOKE_ADDR ?= 127.0.0.1:18080

# Containers: the lakeleto image (./Dockerfile).
IMAGE           ?= lakeleto:dev
PUBLISHED_IMAGE ?= mancube/lakeleto
DOCKER_DATA_DIR ?= ./data
PORT            ?= 8080

# Pass-through arguments and packaging.
ARGS            ?=
QUICKSTART_ARGS ?=
VERSION         ?=

# Maintainers' targets, which need files only the private source tree has.
ARTIFACT    ?= release
SIZE_TARGET ?= x86_64-unknown-linux-gnu
STAGE       ?= $(or $(TMPDIR),/tmp)/lakeleto-oss-stage

# The values the recipes quote reach the shell through its environment, so a path with spaces,
# SQL with quotes or a token is passed as it is and not echoed with the command.
export FILE SQL TOKEN CATALOG DIR SORT FILTER COLS

# --- derived -------------------------------------------------------------------------------

# One build serves every run target: `cargo run` rebuilds only when the sources or FEATURES change.
RUN = $(CARGO) run $(PKG) --features "$(FEATURES)" --bin lakeleto --
OUTPUT_FLAGS = $(if $(FMT),-o $(FMT)) $(if $(OUT),--out "$(OUT)")
SERVER_FLAGS = $(if $(ROOT),--root "$(ROOT)") $(if $(TOKEN),--token "$$TOKEN")
REMOTE_FLAGS = --remote-url "$(REMOTE_URL)" $(if $(TOKEN),--remote-token "$$TOKEN")

# `out` writes FMT (parquet unless set) to OUT (out/result.<fmt> unless set).
OUT_FMT  = $(or $(FMT),parquet)
OUT_FILE = $(or $(OUT),out/result.$(OUT_FMT))

# curl for the API targets: fail on an HTTP error but still print the server's JSON error, and
# send the bearer token when there is one. JSON is pretty-printed when jq is installed.
API  = $(CURL) -sS --fail-with-body $(if $(TOKEN),-H "Authorization: Bearer $$TOKEN")
JSON = $(if $(shell command -v jq 2>/dev/null),| jq .)

# Where cargo builds, which is not ./target in a workspace.
TARGET_DIR = $(shell $(CARGO) metadata --format-version 1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')

comma := ,
has-feature = $(filter $(1),$(subst $(comma), ,$(FEATURES)))

define require-release-features
[ -n "$(RELEASE_FEATURES)" ] || { echo "RELEASE_FEATURES is empty: no ops/release.yml or .github/workflows/release.yml here. Set it: make $@ RELEASE_FEATURES=serve,sql,..." >&2; exit 2; }
endef

# $(call require-file,PATH): stop with a message when PATH is missing (the private-tree targets).
define require-file
[ -e "$(1)" ] || { echo "make $@ needs $(1), which only the private source tree has." >&2; exit 2; }
endef

.PHONY: help \
        build build-release build-db install install-release desktop \
        schema head profile profile-fast info engines \
        query out roundtrip \
        catalog-ls mcp mcp-smoke engines-remote remote-head cli \
        serve open \
        api-health api-engines api-list api-info api-schema api-preview api-profile api-rows \
        api-stats api-export api-query api-arrow api-smoke \
        fmt fmt-check clippy clippy-release clippy-db test test-release test-db check ci \
        frontend-install frontend-dev frontend-build frontend-preview frontend-check \
        docker-build docker-run docker-pull docker-serve docker-cli \
        import export-view to-duckdb quickstart \
        size ossync-check interop interop-deps \
        package-macos package-windows gen-icons docs docs-open clean

##@ Build and install

build: ## The lean default build: Parquet, CSV/TSV, JSON and Arrow IPC, no server
	$(CARGO) build $(PKG)

build-release: ## The release feature set (RELEASE_FEATURES, read from the release workflow)
	@$(require-release-features)
	$(CARGO) build $(PKG) --features "$(RELEASE_FEATURES)"

build-db: ## CI's third set (DB_FEATURES): database drivers, object stores and MCP, no sql
	$(CARGO) build $(PKG) --features "$(DB_FEATURES)"

install: ## cargo install the lean CLI onto PATH
	$(CARGO) install --path .

install-release: ## cargo install with the release feature set
	@$(require-release-features)
	$(CARGO) install --path . --features "$(RELEASE_FEATURES)"

desktop: ## The tray launcher (lakeleto-desktop), with the features the installers ship
	$(CARGO) run $(PKG) --features "desktop,$(RELEASE_FEATURES)" --bin lakeleto-desktop

##@ CLI: what is in a table (FILE=examples/people.csv, FMT=json|csv|..., OUT=file)

schema: ## Columns, types, nullability and row count
	$(RUN) schema "$$FILE" $(OUTPUT_FLAGS)

head: ## The first N rows (N=10)
	$(RUN) head "$$FILE" -n $(N) $(OUTPUT_FLAGS)

profile: ## Null %, distinct count, min/max and samples per column, over SCAN rows (SCAN=10000)
	$(RUN) profile "$$FILE" --scan $(SCAN) $(OUTPUT_FLAGS)

profile-fast: ## The same from a Parquet footer alone, no row scan (PARQUET=examples/sample-data/orders.parquet)
	$(RUN) profile "$(PARQUET)" --fast $(OUTPUT_FLAGS)

info: ## Format, engine, size, rows and columns; FMT=json prints what GET /v1/info returns
	$(RUN) info "$$FILE" $(OUTPUT_FLAGS)

engines: ## The engines this build has and what each reads
	$(RUN) engines $(OUTPUT_FLAGS)

##@ CLI: SQL, and files for the next tool

query: ## Read-only SQL over FILE as table t (SQL="SELECT ...")
	$(RUN) query "$$SQL" --file "$$FILE" $(OUTPUT_FLAGS)

out: ## Write the query's rows to a file: FMT=parquet|arrow|arrows|csv|tsv|json|ndjson, OUT=out/result.<fmt>
	@mkdir -p "$(dir $(OUT_FILE))"
	$(RUN) query "$$SQL" --file "$$FILE" -o $(OUT_FMT) --out "$(OUT_FILE)"
	@echo "wrote $(OUT_FILE)"

roundtrip: out ## `out`, then read the file it wrote: its schema and first rows
	$(RUN) schema "$(OUT_FILE)"
	$(RUN) head "$(OUT_FILE)" -n $(N)

##@ CLI: catalogs, agents and other servers

catalog-ls: ## Iceberg REST catalogs ($LAKELETO_HOME/catalogs.toml): the catalogs, or what is under CATALOG=catalog://prod/
	$(RUN) catalog ls $(if $(CATALOG),"$$CATALOG") $(OUTPUT_FLAGS)

mcp: ## The MCP server on stdin/stdout, read-only under MCP_ROOT=examples (an MCP client starts it)
	$(RUN) mcp --root "$(MCP_ROOT)"

mcp-smoke: ## Talk to the MCP server: initialize, list its tools, describe MCP_FILE=people.csv
	@set -euo pipefail; \
	printf '%s\n' \
	  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"make mcp-smoke","version":"0"}}}' \
	  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
	  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
	  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"describe","arguments":{"path":"$(MCP_FILE)"}}}' \
	| $(RUN) mcp --root "$(MCP_ROOT)" $(JSON)

engines-remote: ## What a running server runs: its version, protocol and engines (REMOTE_URL=http://ADDR)
	$(RUN) engines $(REMOTE_FLAGS) $(OUTPUT_FLAGS)

remote-head: ## Read FILE through a running server instead of this machine (REMOTE_URL, N)
	$(RUN) head "$$FILE" -n $(N) $(REMOTE_FLAGS) $(OUTPUT_FLAGS)

cli: ## Any lakeleto command: ARGS="schema examples/people.csv -o json"
	@[ -n "$(ARGS)" ] || { echo 'usage: make cli ARGS="<command> [flags]"' >&2; exit 2; }
	$(RUN) $(ARGS)

##@ Server

serve: ## The HTTP API and the web app (ADDR=127.0.0.1:8080, ROOT=dir to confine to, TOKEN=bearer token)
	$(RUN) serve --addr "$(ADDR)" --default-scan $(DEFAULT_SCAN) $(SERVER_FLAGS)

open: ## The same, with FILE open in a browser tab
	$(RUN) open "$$FILE" --addr "$(ADDR)" --default-scan $(DEFAULT_SCAN) $(SERVER_FLAGS)

##@ HTTP API, against a running `make serve` (URL=http://ADDR, TOKEN when it has one)

api-health: ## GET /healthz
	@$(API) "$(URL)/healthz"; echo

api-engines: ## GET /v1/engines: the protocol, every engine's capabilities, the endpoints
	@$(API) "$(URL)/v1/engines" $(JSON)

api-list: ## GET /v1/list: a directory's tables and subdirectories (DIR=examples)
	@$(API) --get --data-urlencode "dir=$$DIR" "$(URL)/v1/list" $(JSON)

api-info: ## GET /v1/info: format, engine, size, rows, columns (FILE)
	@$(API) --get --data-urlencode "path=$$FILE" "$(URL)/v1/info" $(JSON)

api-schema: ## GET /v1/schema (FILE)
	@$(API) --get --data-urlencode "path=$$FILE" "$(URL)/v1/schema" $(JSON)

api-preview: ## GET /v1/preview: the first N rows (FILE, N)
	@$(API) --get --data-urlencode "path=$$FILE" --data "limit=$(N)" "$(URL)/v1/preview" $(JSON)

api-profile: ## GET /v1/profile (FILE, SCAN; SCAN=0 reads a Parquet footer only)
	@$(API) --get --data-urlencode "path=$$FILE" --data "scan=$(SCAN)" "$(URL)/v1/profile" $(JSON)

api-rows: ## GET /v1/rows: a window, filtered, sorted, paged (OFFSET, LIMIT, SORT=col, DESC=1, FILTER=col:op:value, COLS=a,b)
	@$(API) --get --data-urlencode "path=$$FILE" --data "offset=$(OFFSET)" --data "limit=$(LIMIT)" \
	  $(if $(SORT),--data-urlencode "sort=$$SORT") $(if $(DESC),--data "desc=$(DESC)") \
	  $(if $(FILTER),--data-urlencode "filter=$$FILTER") $(if $(COLS),--data-urlencode "cols=$$COLS") \
	  "$(URL)/v1/rows" $(JSON)

api-stats: ## GET /v1/stats: the profile of the filtered view (FILE, FILTER)
	@$(API) --get --data-urlencode "path=$$FILE" $(if $(FILTER),--data-urlencode "filter=$$FILTER") \
	  "$(URL)/v1/stats" $(JSON)

api-export: ## GET /v1/export: the view as a file (EXPORT=csv|tsv|json|ndjson|parquet, SORT, FILTER, COLS, OUT)
	@out="$(or $(OUT),out/export.$(EXPORT))"; mkdir -p "$$(dirname "$$out")"; \
	$(API) --get --data-urlencode "path=$$FILE" --data "fmt=$(EXPORT)" \
	  $(if $(SORT),--data-urlencode "sort=$$SORT") $(if $(DESC),--data "desc=$(DESC)") \
	  $(if $(FILTER),--data-urlencode "filter=$$FILTER") $(if $(COLS),--data-urlencode "cols=$$COLS") \
	  -o "$$out" "$(URL)/v1/export"; \
	echo "wrote $$out"

api-query: ## POST /v1/query: SQL over FILE as table t (SQL; the server needs `sql`)
	@set -euo pipefail; \
	esc() { printf '%s' "$$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr '\n\t' '  '; }; \
	$(API) -H 'Content-Type: application/json' \
	  --data "{\"sql\":\"$$(esc "$$SQL")\",\"file\":\"$$(esc "$$FILE")\"}" "$(URL)/v1/query" $(JSON)

api-arrow: ## GET /v1/preview as an Arrow IPC stream into OUT (out/preview.arrows); `make head FILE=<it>` reads it
	@out="$(or $(OUT),out/preview.arrows)"; mkdir -p "$$(dirname "$$out")"; \
	$(API) -H 'Accept: application/vnd.apache.arrow.stream' --get --data-urlencode "path=$$FILE" \
	  --data "limit=$(N)" -o "$$out" "$(URL)/v1/preview"; \
	echo "wrote $$out"

##@ HTTP API, end to end

api-smoke: ## Start a server on SMOKE_ADDR confined to examples/, call every read endpoint, stop it
	@set -euo pipefail; \
	$(CARGO) build $(PKG) --features "$(FEATURES)" --bin lakeleto; \
	log="$$(mktemp)"; \
	$(RUN) serve --addr "$(SMOKE_ADDR)" --root examples >"$$log" 2>&1 & pid=$$!; \
	trap 'kill $$pid 2>/dev/null || true; wait $$pid 2>/dev/null || true; rm -f "$$log"' EXIT; \
	base="http://$(SMOKE_ADDR)"; \
	for _ in $$(seq 1 120); do $(CURL) -fsS "$$base/healthz" >/dev/null 2>&1 && break; sleep 0.5; done; \
	$(CURL) -fsS "$$base/healthz" >/dev/null || { echo "the server did not start:" >&2; cat "$$log" >&2; exit 1; }; \
	failed=0; \
	check() { \
	  label="$$1"; want="$$2"; shift 2; \
	  if body="$$($(CURL) -sS --fail-with-body "$$@" 2>&1)" && grep -q -- "$$want" <<<"$$body"; then \
	    printf 'ok    %s\n' "$$label"; \
	  else \
	    printf 'FAIL  %s\n%s\n' "$$label" "$$(head -c 400 <<<"$$body")"; failed=1; \
	  fi; \
	}; \
	people=examples/people.csv; orders=examples/sample-data/orders.csv; \
	check "GET /healthz" "" "$$base/healthz"; \
	check "GET /v1/engines" '"protocol"' "$$base/v1/engines"; \
	check "GET /v1/list" '"entries"' --get --data-urlencode "dir=examples" "$$base/v1/list"; \
	check "GET /v1/info" '"row_count"' --get --data-urlencode "path=$$people" "$$base/v1/info"; \
	check "GET /v1/schema" '"columns"' --get --data-urlencode "path=$$people" "$$base/v1/schema"; \
	check "GET /v1/preview" '"rows"' --get --data-urlencode "path=$$people" --data limit=3 "$$base/v1/preview"; \
	check "GET /v1/profile" '"columns"' --get --data-urlencode "path=$$people" "$$base/v1/profile"; \
	check "GET /v1/rows (filter, sort, page)" '"rows"' --get --data-urlencode "path=$$orders" \
	  --data-urlencode "filter=city:eq:Berlin" --data sort=amount_usd --data desc=1 --data limit=5 "$$base/v1/rows"; \
	check "GET /v1/stats (filtered)" '"columns"' --get --data-urlencode "path=$$orders" \
	  --data-urlencode "filter=city:eq:Berlin" "$$base/v1/stats"; \
	check "GET /v1/export (csv)" 'id,name' --get --data-urlencode "path=$$people" --data fmt=csv "$$base/v1/export"; \
	if [ -n "$(call has-feature,sql)" ]; then \
	  check "POST /v1/query" '"rows"' -H 'Content-Type: application/json' \
	    --data '{"sql":"SELECT count(*) AS n FROM t","file":"examples/people.csv"}' "$$base/v1/query"; \
	else echo "skip  POST /v1/query (this build has no sql)"; fi; \
	arrow="$$($(CURL) -fsS -H 'Accept: application/vnd.apache.arrow.stream' --get \
	  --data-urlencode "path=$$people" --data limit=3 "$$base/v1/preview" | wc -c)"; \
	if [ "$$arrow" -gt 0 ]; then echo "ok    GET /v1/preview as Arrow ($$arrow bytes)"; \
	else echo "FAIL  GET /v1/preview as Arrow"; failed=1; fi; \
	exit $$failed

##@ Test and lint: what CI runs

fmt: ## Reformat this crate
	$(CARGO) fmt $(PKG)

fmt-check: ## Check formatting, changing nothing
	$(CARGO) fmt $(PKG) -- --check

clippy: ## Lint the lean build, warnings denied
	$(CARGO) clippy $(PKG) --all-targets -- -D warnings

clippy-release: ## Lint the release feature set
	@$(require-release-features)
	$(CARGO) clippy $(PKG) --features "$(RELEASE_FEATURES)" --all-targets -- -D warnings

clippy-db: ## Lint CI's database set
	$(CARGO) clippy $(PKG) --features "$(DB_FEATURES)" --all-targets -- -D warnings

test: ## Test the lean build
	$(CARGO) test $(PKG)

test-release: ## Test the release feature set
	@$(require-release-features)
	$(CARGO) test $(PKG) --features "$(RELEASE_FEATURES)"

test-db: ## Test CI's database set
	$(CARGO) test $(PKG) --features "$(DB_FEATURES)"

check: fmt-check clippy test ## A quick pre-push check: format, lint and test the lean build

ci: fmt-check clippy clippy-release test test-release clippy-db test-db ## Everything CI's Rust job runs, in its order, on all three feature sets

##@ Web app (frontend/, embedded into the binary)

frontend-install: ## Install the web app's dependencies from its lockfile
	cd frontend && npm ci

frontend-dev: ## Vite's dev server with hot reload, against a running `make serve`
	cd frontend && npm run dev

frontend-build: ## Rebuild frontend/dist/, the bundle `serve` embeds
	cd frontend && npm run build

frontend-preview: ## Preview the production build
	cd frontend && npm run preview

frontend-check: frontend-install frontend-build ## Rebuild and fail if the committed frontend/dist/ differs, as CI does
	git diff --exit-code -- frontend/dist

##@ Containers: the lakeleto image (DOCKER=podman works too)

docker-build: ## Build the image from source, this directory as the build context
	$(DOCKER) build -t $(IMAGE) .

docker-run: ## Run that image's server, published to this host's loopback only (PORT=8080)
	$(DOCKER) run --rm -p 127.0.0.1:$(PORT):8080 $(IMAGE) serve --addr 0.0.0.0:8080

docker-pull: ## Pull the published image instead
	$(DOCKER) pull $(PUBLISHED_IMAGE)

docker-serve: ## Serve a read-only data directory with the published image (DOCKER_DATA_DIR=./data)
	$(DOCKER) run --rm -p 127.0.0.1:$(PORT):8080 -v "$(DOCKER_DATA_DIR):/data:ro" \
	  $(PUBLISHED_IMAGE) serve --addr 0.0.0.0:8080 --root /data

docker-cli: ## One command in the published image: ARGS="schema /data/events.parquet"
	@[ -n "$(ARGS)" ] || { echo 'usage: make docker-cli ARGS="schema /data/events.parquet"' >&2; exit 2; }
	$(DOCKER) run --rm -v "$(DOCKER_DATA_DIR):/data:ro" $(PUBLISHED_IMAGE) $(ARGS)

##@ Workspace scripts (examples/scripts/, against a running `make serve`)

import: ## Load a file into a workspace: ARGS="--path examples/sample-data/orders.csv"
	@[ -n "$(ARGS)" ] || { echo 'usage: make import ARGS="--path examples/sample-data/orders.csv"' >&2; exit 2; }
	bash examples/scripts/import.sh $(ARGS)

export-view: ## Export a view or a SQL report: ARGS="--path FILE --out out/orders.parquet"
	@[ -n "$(ARGS)" ] || { echo 'usage: make export-view ARGS="--path examples/sample-data/orders.csv --out out/orders.parquet"' >&2; exit 2; }
	bash examples/scripts/export.sh $(ARGS)

to-duckdb: ## Load an export into DuckDB: ARGS="--input out/orders.parquet --db out/reports.duckdb --table orders"
	@[ -n "$(ARGS)" ] || { echo 'usage: make to-duckdb ARGS="--input out/orders.parquet --db out/reports.duckdb --table orders"' >&2; exit 2; }
	bash examples/scripts/to_duckdb.sh $(ARGS)

quickstart: ## import, export, DuckDB, end to end (QUICKSTART_ARGS="--serve" when no server runs)
	bash examples/scripts/quickstart.sh $(QUICKSTART_ARGS)

##@ Packaging and docs

package-macos: ## Build Lakeleto.app and the .dmg (macOS; VERSION=x.y.z)
	@[ -n "$(VERSION)" ] || { echo "usage: make package-macos VERSION=0.4.0" >&2; exit 2; }
	bash packaging/macos/build-dmg.sh $(VERSION)

package-windows: ## Build the .msi with WiX (Windows, pwsh; VERSION=x.y.z)
	@[ -n "$(VERSION)" ] || { echo "usage: make package-windows VERSION=0.4.0" >&2; exit 2; }
	pwsh packaging/windows/build-msi.ps1 -Version $(VERSION)

gen-icons: ## Regenerate the installers' icon art into packaging/
	$(CARGO) run $(PKG) --features serve --example gen_icons

docs: ## Build this crate's rustdoc
	$(CARGO) doc $(PKG) --no-deps

docs-open: ## Build it and open it
	$(CARGO) doc $(PKG) --no-deps --open

clean: ## Remove this crate's build artifacts and what `out`, `api-export` and `api-arrow` wrote to out/
	$(CARGO) clean $(PKG)
	rm -rf out

##@ Maintainers: these need files only the private source tree has

size: ## Build ARTIFACT=lean|release with the release profile and hold it to its size budget
	@$(call require-file,ops/size_check.py)
	set -euo pipefail; \
	features="$$($(PYTHON) ops/size_check.py features $(ARTIFACT))"; \
	$(CARGO) build --release --locked $(PKG) --bin lakeleto $${features:+--features "$$features"}; \
	$(PYTHON) ops/size_check.py check $(ARTIFACT) --target $(SIZE_TARGET) --binary "$(TARGET_DIR)/release/lakeleto"

ossync-check: ## Stage the public repo's cut, apply its rewrites, scan it and check its links (needs mikefarah yq v4)
	@$(call require-file,scripts/ossync-check.sh)
	case "$(STAGE)" in */lakeleto-oss-stage) ;; *) echo "STAGE must end in /lakeleto-oss-stage" >&2; exit 2 ;; esac
	bash scripts/ossync-check.sh "$(STAGE)"

interop-deps: ## Install the catalog interop harness's Python packages (pyiceberg, pyarrow)
	@$(call require-file,ops/catalog-interop/requirements.txt)
	$(PYTHON) -m pip install -r ops/catalog-interop/requirements.txt

interop: ## Polaris and Lakekeeper in Docker, a table written into each, read back by lakeleto (needs interop-deps)
	@$(call require-file,ops/catalog-interop/interop.py)
	@$(require-release-features)
	set -euo pipefail; \
	compose="$(DOCKER) compose -f ops/catalog-interop/compose.yml"; \
	trap '$$compose down --volumes' EXIT; \
	$$compose up --detach --quiet-pull; \
	$(CARGO) build $(PKG) --bin lakeleto --features "$(RELEASE_FEATURES)"; \
	$(PYTHON) ops/catalog-interop/interop.py --lakeleto "$(TARGET_DIR)/debug/lakeleto"

##@ Hosted plane: ee/ is private and not in the public repo

ee-%: ## Run an ee/ target, as `make ee-check` or `make ee-cloud-run` (`make ee-help` lists them)
	@[ -f ee/Makefile ] || { echo "ee/, the hosted plane, is not in this tree." >&2; exit 2; }
	$(MAKE) -C ee $*

##@ Help

help: ## This list
	@awk 'BEGIN { FS = ":.*##" } \
	  /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next } \
	  /^[a-zA-Z0-9_%-]+:.*##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf '\n\033[1mVariables\033[0m (make TARGET VAR=value)\n'
	@printf '  %-16s %s\n' \
	  'FILE'             'the table the CLI and API targets read (default $(FILE))' \
	  'FMT, OUT'         '-o and --out: json, csv, ..., parquet, arrow, arrows; a file instead of stdout' \
	  'SQL'              'the query for query, out and api-query; the table is t' \
	  'FEATURES'         'what the run targets build with (default: the release set; FEATURES= is the lean build)' \
	  'ADDR, URL'        'where serve listens and the API targets call (default $(ADDR))' \
	  'TOKEN, ROOT'      'the bearer token, and the directory serve is confined to' \
	  'CATALOG'          'a catalog:// reference for catalog-ls' \
	  'REMOTE_URL'       'the server engines-remote and remote-head read through' \
	  'DOCKER'           'the container CLI (default $(DOCKER); podman works)'
	@printf '\nEvery target runs a command the README, CONTRIBUTING, docs/, examples/scripts/ or CI already use.\n\n'
