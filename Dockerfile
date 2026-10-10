# Local from-source distroless build of the `lakeleto` binary (static musl). For CI releases the
# multi-arch image is assembled from prebuilt binaries via Dockerfile.release — this one is for
# `docker build` on a developer machine without the release matrix.
#
#   docker build -t lakeleto:dev .
#   docker run --rm -p 8080:8080 lakeleto:dev serve --addr 0.0.0.0:8080
#
# Lakeleto is a SINGLE crate (lakeleto). The `serve` feature embeds the committed SPA bundle
# under frontend/dist/ into the binary via rust-embed — so no node/npm step is needed here, but
# the build context MUST include frontend/dist/ (it does: `COPY . .`).
FROM rust:1-bookworm AS build
RUN rustup target add x86_64-unknown-linux-musl && \
    apt-get update && apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Server profile: the HTTP/JSON API + embedded SPA (serve), SQL engine (sql), Iceberg reader
# (iceberg), BYO-credential object-store reads (object-store), Iceberg REST catalogs (catalog), the
# MCP server for agents (mcp: `docker run -i --rm lakeleto:dev mcp`), and zstd, bzip2 and xz text
# (compression, whose codecs `sql` already links).
# The ee/ plane is separate and is not built here.
RUN cargo build --release --bin lakeleto \
        --features serve,sql,iceberg,object-store,catalog,mcp,compression \
        --target x86_64-unknown-linux-musl && \
    cp target/x86_64-unknown-linux-musl/release/lakeleto /lakeleto

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/lucheeseng827/lakeleto" \
      org.opencontainers.image.description="Lakeleto — read-only lakehouse-table explorer (Parquet, CSV, JSON, Arrow, Iceberg, object stores) in a single binary" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /lakeleto /usr/local/bin/lakeleto
# `lakeleto serve` binds 127.0.0.1:8080 by default (env LAKELETO_ADDR). In a container pass
# `serve --addr 0.0.0.0:8080` so the listener is reachable from outside the network namespace.
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/lakeleto"]
CMD ["--help"]
