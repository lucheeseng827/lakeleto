<!-- SPDX-License-Identifier: Apache-2.0 -->
# Getting & running Lakeleto

Lakeleto is one static binary named `lakeleto` — primarily a **laptop / CI-runner CLI**.
Install it with Cargo, fetch a prebuilt binary, or run the container for the
`serve` UI. Flags are in [CONFIG.md](./CONFIG.md); running the server is in
[OPERATIONS.md](./OPERATIONS.md).

> **Security defaults.** `lakeleto serve` binds **loopback** `127.0.0.1:8080` and has
> **no in-process TLS**. Auth is **off by default** — with no `--token` every `/v1/*`
> route is open, which is fine for a local, single-user session and nothing else. Before
> exposing the server beyond your machine: bind behind a TLS-terminating reverse proxy,
> set `--token` (a shared bearer secret — not per-user RBAC), and set `--root <dir>` to
> confine file access. A non-loopback bind with no token warns loudly and stays open.

## Install from source

```sh
# from a checkout of this crate:
cargo install --path .                       # local reader only (lean default)
cargo install --path . --features serve,sql,iceberg,object-store   # full: UI + SQL + Iceberg + cloud reads

# or build in place:
cargo build --release --bin lakeleto --features serve,sql   # → target/release/lakeleto
```

Default builds pull only `arrow`/`parquet`/`csv` (no C++ toolchain, no async runtime, no
server). Add only the [features](./CONFIG.md#feature-flags) you need.

## Prebuilt binary (`cargo binstall`)

`Cargo.toml` carries `cargo-binstall` metadata, so binstall fetches the prebuilt
`lakeleto` from a GitHub release instead of compiling:

```sh
cargo binstall lakeleto
```

Release assets are named `lakeleto-<target><archive-suffix>` with the binary `lakeleto`
inside — `.tar.gz` on Unix, `.zip` on Windows. For example:

| Target | Asset |
|---|---|
| `x86_64-unknown-linux-musl` | `lakeleto-x86_64-unknown-linux-musl.tar.gz` |
| `aarch64-apple-darwin` | `lakeleto-aarch64-apple-darwin.tar.gz` |
| `x86_64-pc-windows-msvc` | `lakeleto-x86_64-pc-windows-msvc.zip` |

## Release tarballs (manual)

Download the tarball for your platform from the release page, unpack, and put `lakeleto`
on your `PATH`:

```sh
curl -sSL -o lakeleto.tar.gz \
  https://github.com/lucheeseng827/lakeleto/releases/download/v0.1.0/lakeleto-x86_64-unknown-linux-musl.tar.gz
tar xzf lakeleto.tar.gz
./lakeleto schema data/events.parquet
```

## Container (`docker run`)

The image ships the binary built with `--features serve,sql,iceberg,object-store` on a
distroless static base ([`Dockerfile`](../Dockerfile)). `serve` binds loopback *inside*
the container, so pass `--addr 0.0.0.0:8080` to make it reachable and publish the port to
**loopback on the host**:

```sh
# inspect a table (mount a data dir read-only)
docker run --rm -v "$PWD/data:/data:ro" mancube/lakeleto schema /data/events.parquet

# run the serve UI, published to host loopback only
docker run --rm \
  -p 127.0.0.1:8080:8080 \
  -v "$PWD/data:/data:ro" \
  mancube/lakeleto serve --addr 0.0.0.0:8080 --root /data
# → open http://127.0.0.1:8080
```

- **Publish to loopback** (`-p 127.0.0.1:8080:8080`), not `0.0.0.0`, unless you have put a
  TLS + auth proxy in front. Inside the container `--addr 0.0.0.0:8080` is required so the
  listener escapes the network namespace; the host `-p` mapping is what actually controls
  exposure.
- **`--root /data`** confines `/v1/*` to the mounted data dir — set it whenever the port
  is reachable by anything but you.
- **Bearer token:** add `-e LAKELETO_TOKEN=…` (or `serve --token …`) for a shared secret on
  `/v1/*`. Prefer the `Authorization` header; `?token=` is honoured only on a loopback bind.
- **Object-store creds:** pass them as env
  (`-e AWS_ACCESS_KEY_ID=… -e AWS_SECRET_ACCESS_KEY=… -e AWS_REGION=…`) — see
  [OPERATIONS.md](./OPERATIONS.md#object-store-credentials-byo).

To build the image locally instead of pulling:

```sh
docker build -t lakeleto:dev .
docker run --rm -p 127.0.0.1:8080:8080 lakeleto:dev serve --addr 0.0.0.0:8080
```

## Upgrade / rollback

Stateless swap: replace the binary (or bump the image tag) and restart. The only on-disk
state is the workspace store under `~/.lakeleto` (saved queries + cached results); back it
up if it matters. There is no on-disk migration in 0.1.x.
