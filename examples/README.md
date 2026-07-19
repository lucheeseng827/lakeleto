# Lakeleto examples

A tiny CSV to try the first-run loop against. It deliberately has nulls (`score`,
`active`) so `profile` shows non-trivial null counts.

```bash
# from the repo root
cargo run --bin lakeleto -- schema  ./examples/people.csv
cargo run --bin lakeleto -- head    ./examples/people.csv -n 5
cargo run --bin lakeleto -- profile ./examples/people.csv
cargo run --bin lakeleto -- info    ./examples/people.csv
cargo run --bin lakeleto -- engines

# machine-readable output for piping
cargo run --bin lakeleto -- head ./examples/people.csv -o json
```

SQL (opt-in, needs the `sql` feature):

```bash
cargo run --features sql --bin lakeleto -- \
  query "SELECT city, count(*) AS n, avg(score) AS avg_score FROM t GROUP BY city ORDER BY n DESC" \
  --file ./examples/people.csv
```

To try Parquet, write one from this CSV with the `sql` feature (DuckDB/pandas also work):

```bash
cargo run --features sql --bin lakeleto -- \
  query "COPY (SELECT * FROM t) TO 'people.parquet'" --file .../people.csv  # (blocked: read-only guard)
```

> Note: `lakeleto query` is read-only by design, so it won't write Parquet. Use DuckDB
> (`duckdb -c "COPY (SELECT * FROM 'people.csv') TO 'people.parquet'"`) or pandas to
> produce a Parquet fixture, then `lakeleto schema people.parquet`.
