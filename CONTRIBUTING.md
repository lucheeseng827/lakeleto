# Contributing to Lakeleto

Thanks for your interest in Lakeleto — an instant, offline, single-binary
**lakehouse-table explorer** ("the Postman of lakehouse tables"). The whole
thesis is that the engine is a commodity behind one [`Engine`](src/engine/mod.rs)
trait and the value is in UX and packaging, so contributions are held to a high
bar for clarity, test coverage, and keeping the default build lean.

## License-in = license-out (DCO, no CLA)

- Contributions to the **OSS engine** (the module root: `src/**`) are accepted
  under **Apache-2.0** inbound — the same license they ship under. We do **not**
  require a CLA.
- Instead we use the **Developer Certificate of Origin** ([DCO](https://developercertificate.org/)).
  Sign off every commit:

  ```sh
  git commit -s -m "engine: profile min/max from Parquet footer stats"
  ```

  The `-s` adds a `Signed-off-by: Your Name <you@example.com>` trailer
  certifying you wrote the patch or have the right to submit it under Apache-2.0.

- The **`ee/` plane is not open to outside contribution.** `ee/lakeleto-cloud` is
  source-available under the **Elastic License 2.0** for transparency and
  self-hosting, not community development. PRs touching `ee/**` are closed by
  policy — please file an issue instead.

## Before you open a PR

1. **Build & test the OSS engine standalone**, with `ee/` absent — this proves
   the reader + trait surface is self-contained and the default build stays lean:

   ```sh
   cargo build                          # lean default (arrow/parquet/csv only)
   cargo build --features serve,sql,iceberg   # the heavier opt-in surface
   cargo test                            # unit + serve-router + CLI tests
   cargo clippy --all-targets
   cargo fmt --all -- --check
   ```

2. **Add tests.** New behavior needs a test; bug fixes need a regression test.
   The `serve` router is driven in-process (`tower::ServiceExt::oneshot`, no
   socket); reader work should add a throwaway Parquet/CSV fixture (`tempfile`).

3. **Keep the default build lean.** Anything heavy — DataFusion (`sql`), the
   server (`serve`), Iceberg (`iceberg`), object stores (`object-store`), DuckDB
   (`duckdb`) — is an **off-by-default feature**. Never pull a C++ toolchain, an
   async runtime, or a server into the default `cargo build -p lakeleto`.

4. **Bind to the trait, never a concrete engine.** New surface (CLI, `serve`
   endpoints, UI) talks to `Box<dyn Engine>`, not a specific backend — that is
   the design invariant (see `docs/ADR-0001-local-engine-first.md`).

5. **Read-only stays read-only.** `lakeleto query` accepts only
   `SELECT`/`WITH`/`EXPLAIN`. An explorer never mutates the user's data.

## Security issues are private

Do **not** open a public issue for a vulnerability. Follow the private,
embargoed disclosure process in [SECURITY.md](./SECURITY.md).

## Code style

- Match the surrounding code: small, well-documented modules with doc comments
  that explain *why*, not just *what*.
- One logical change per PR. Keep diffs reviewable.
- Run `cargo fmt` and `cargo clippy` clean before pushing.
