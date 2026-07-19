# Releasing Lakeleto

Releases are cut by tag. `.github/workflows/release.yml` (synced from
`ops/release.yml`) does the rest. Every artifact is **signed, attested, and
SBOM'd**, so a user can verify the binary end-to-end without trusting the
publisher (see [Verify a release](#verify-a-release-no-trust-in-the-publisher-required)).

Lakeleto is a **single crate** producing the `lakeleto` binary (a `lib` + `bin`,
both named `lakeleto`). There are no internal workspace-dependency crates to
publish in order.

> **Crate name: `lakeleto`, not `lakeleto`.** In the monorepo the package is
> `name = "lakeleto"` (with `[lib]`/`[bin] name = "lakeleto"`, `publish = false`).
> The crates.io package name is whatever `[package] name` says. **`lakeleto` is
> already taken on crates.io** (a placeholder by an unrelated project), so the
> `.ossync` rewrite renames the mirror's `[package] name` to **`lakeleto`** and
> flips `publish = true`. The **binary stays `lakeleto`** — `cargo install lakeleto`
> installs a `lakeleto` binary — as do the GitHub repo and the Docker image. Verify
> the rewrite landed on the mirror before tagging; `cargo publish` is append-only.

## Cut a release

1. Bump `version` in `Cargo.toml`, update `CHANGELOG.md`, land on `main`.
2. Tag and push:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
   Exercise the pipeline first with a pre-release tag (`v0.1.0-rc.1`): it builds,
   signs, and attaches a GitHub **pre-release**, but skips crates.io, Homebrew,
   and `:latest` promotion.
3. On a real `v*` tag the `release` workflow:
   - **builds** static-musl (`x86_64`/`aarch64`) and macOS (`x86_64`/`arm64`)
     `lakeleto` binaries, each archived as `lakeleto-<target>.tar.gz`
     (`.zip` on `x86_64-pc-windows-msvc`) with a `.sha256`, a **cosign** keyless
     signature (`.sig` + `.pem`), and a **SLSA build-provenance** attestation;
   - generates a **CycloneDX SBOM** for the crate;
   - **publishes** the GitHub release with every artifact + an aggregated
     `SHA256SUMS`;
   - **publishes** the single OSS crate `lakeleto` to crates.io, idempotently
     (see the crate-name caveat above; `ee/lakeleto-cloud` is a separate ELv2
     workspace and is **never** published);
   - **bumps** `Formula/lakeleto.rb` and commits it to `main` (this repo is its
     own tap);
   - **pushes** a multi-arch (`linux/amd64`+`linux/arm64`) distroless image to
     `docker.io/mancube/lakeleto:{vX.Y.Z,latest}`, cosign-signed + SLSA-attested.

Which release-build features are compiled into the published binary/image
(default lean vs. `serve,sql,iceberg,object-store`) is set by the workflow's
build matrix — confirm it matches what this doc advertises before tagging.

`workflow_dispatch` re-runs the pipeline against an existing tag (input `tag`);
`promote` re-points Homebrew + `:latest` (use only for the newest tag).

## Distribution channels

| Channel | Source of truth |
|---|---|
| `cargo install lakeleto` / `cargo binstall lakeleto` | crates.io — the `lakeleto` crate installs a `lakeleto` binary |
| `brew install lakeleto` | `Formula/lakeleto.rb` (this repo is its own tap) |
| `docker run … mancube/lakeleto` | `Dockerfile.release` (distroless, from the prebuilt static binary) |
| release tarballs | the GitHub release (`lakeleto-<target>.tar.gz` + `.sha256` + `.sig` + `.pem`) |

## Verify a release (no trust in the publisher required)

```bash
# 1. signature — keyless cosign against this repo's release workflow identity
cosign verify-blob \
  --certificate lakeleto-x86_64-unknown-linux-musl.tar.gz.pem \
  --signature  lakeleto-x86_64-unknown-linux-musl.tar.gz.sig \
  --certificate-identity-regexp '^https://github\.com/lucheeseng827/lakeleto/\.github/workflows/release\.yml@refs/tags/v.+$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  lakeleto-x86_64-unknown-linux-musl.tar.gz

# 2. provenance — the SLSA attestation (built by the named workflow from the tagged commit)
gh attestation verify lakeleto-x86_64-unknown-linux-musl.tar.gz --repo lucheeseng827/lakeleto

# 3. checksums (SHA256SUMS) + 4. build from source standalone (ee/ absent).
```

## Verify a build locally

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --bin lakeleto --target x86_64-unknown-linux-musl
docker buildx build --platform linux/amd64,linux/arm64 -t lakeleto:dev .
```

## Mirror first-release setup (one-time)

The pipeline runs on the **public mirror** (`lucheeseng827/lakeleto`), populated by
the monorepo's OSS-sync workflow. The `release.yml` code is correct as authored —
a first release fails only when the mirror repo/registry aren't set up. Do all of
the following **before pushing the first `v*` tag**, or the `docker` / `cratesio`
/ `brew` jobs fail (some silently):

1. **Crate rename + `publish` flip landed on the mirror** (see the caveat above) —
   otherwise `cargo publish` uploads `lakeleto`, or refuses because
   `publish = false`.
2. **`cicd` environment + secrets.** Create a GitHub Environment named `cicd` on
   the mirror with `DOCKER_USERNAME`, `DOCKER_PASSWORD` (a Docker Hub **access
   token** with write scope, not the account password), and `CRATESIO_TOKEN`. The
   `docker` and `cratesio` jobs are `environment: cicd`; missing secrets →
   registry/publish login fails.
3. **`cicd` deployment-branch policy must allow the `v*` TAG ref.** The pipeline
   is tag-triggered. If the environment restricts deployments to `main` only, the
   tag-run `docker`/`cratesio` jobs are **blocked with no obvious error**. Leave
   the policy unrestricted, or add a tag pattern (`v*`) to the allowed refs.
   *(Easiest gotcha to miss.)*
4. **Create the Docker Hub repo `mancube/lakeleto` first.** Docker Hub rejects the
   first `push` (and the description update) if the repo doesn't exist.
   Pre-create it (public), and confirm the `DOCKER_PASSWORD` token has push
   rights to the `mancube` namespace.
5. **Enable Actions + "Read and write" workflow permissions.** Public mirrors
   often ship with Actions disabled. The `brew` job commits `Formula/lakeleto.rb`
   back to `main` via `github.token`, so Settings → Actions → General → Workflow
   permissions must be **Read and write**.
6. **Don't let branch protection on `main` block the actions bot.** The `brew`
   job pushes directly to `main`. A protection rule requiring a PR/review with no
   bypass for `github-actions[bot]` fails that push (the image + crate still
   publish; only the formula bump fails).
7. **Sync token needs the Workflows scope.** The OSS-sync maps `ops/release.yml`
   → `.github/workflows/release.yml` on the mirror; a Contents-only PAT `403`s on
   `.github/workflows` and the workflow file never lands.

The crate name `lakeleto` is confirmed free on crates.io (`lakeleto` was taken by an unrelated project) —
`cargo publish` is append-only and a name clash aborts the publish.

### First-release smoke test

Pull the freshly-pushed image and run it — the one interaction CI doesn't
exercise:

```bash
docker run --rm mancube/lakeleto:v0.1.0 --version     # binary starts on distroless/static
docker run --rm mancube/lakeleto:v0.1.0 engines       # confirm compiled-in features
```
