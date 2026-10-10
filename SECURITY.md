# Security Policy

Lakeleto reads tables — Parquet, CSV/TSV, JSON and Arrow IPC files (text compressed
too), Iceberg and Delta tables, and read-only SQLite/Postgres/MySQL tables — from local
disk and (with `--features object-store`) from object stores using **your own**
credentials, and can expose the `Engine` surface over HTTP (`lakeleto serve`) and to AI
agents over stdio (`lakeleto mcp`).
Parsers over untrusted files and a network listener are the sensitive surfaces,
so we treat security reports with priority and coordinate disclosure.

## Reporting a vulnerability

**Please do not open a public GitHub issue for a security vulnerability.**

Report privately through GitHub's **private vulnerability reporting** ("Report a
vulnerability" under the repository's *Security* tab).

*(A dedicated `security@` email address will be published at public launch; until
then, GitHub private vulnerability reporting is the supported channel.)*

Include, where you can:

- the affected component (the `lakeleto` binary, the local reader, or a feature:
  `sql`, `iceberg`, `delta`, `catalog`, `object-store`, `sqlite` / `postgres` /
  `mysql`, `compression`, `serve`, `mcp`, `remote`) and version / commit,
- a description of the issue and its impact (e.g. a crafted Parquet/Avro file
  causing memory unsafety or unbounded allocation, path traversal past the
  `serve --root` confinement, a `serve` auth-token bypass, credential leakage),
- reproduction steps or a proof of concept (a minimal fixture file is ideal),
- any suggested remediation.

## What to expect

- **Acknowledgement** within a few business days.
- A **coordinated, embargoed** fix: we will work with you on a timeline, prepare
  a patch, and credit you (opt-in) in the advisory and `CHANGELOG.md`.
- Public disclosure only **after** a fix is available, via a GitHub Security
  Advisory.

## Scope

In scope — the OSS engine in this module, specifically:

- **Malformed-input handling** — a crafted Parquet, CSV/TSV, JSON or Arrow IPC
  file, Avro/Iceberg metadata or manifest, Delta log, or compressed file (one that
  inflates past `--max-decompressed`) that causes a crash, memory unsafety, or
  unbounded resource consumption in the reader.
- **`serve` confinement bypass** — reaching a file outside the `--root` boundary
  (`confine_entry` / `confine_members`), or a path-traversal / SSRF via a `path=`
  or object-store URI parameter.
- **`serve` auth** — bypassing the `--token` bearer gate on a `/v1/*` route, or
  the constant-time token comparison.
- **Credential exposure** — leaking the object-store credentials read from the
  environment, or writing them into logs / cached results.

Out of scope — issues requiring an already-compromised host, and behavior of the
third-party engines Lakeleto merely embeds (e.g. DataFusion) beyond how Lakeleto
drives them.

## Supported versions

Lakeleto is pre-1.0 and fast-moving; security fixes target the latest `main` and
the most recent tagged release. Pin a release tag and watch `CHANGELOG.md` for
security-relevant entries.
