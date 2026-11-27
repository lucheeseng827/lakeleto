#!/usr/bin/env python3
"""Smoke-test `lakeleto flight` with Python's ADBC Flight SQL driver, as a notebook would use it.

    pip install adbc_driver_flightsql pyarrow
    python3 ops/flight_adbc_smoke.py target/debug/lakeleto

The binary must be built with `--features flight`. The script writes two small tables to a
temporary directory, serves them on a free loopback port with a token, and checks, through the
driver's DB-API:

- a plain `execute` runs (the driver asks to prepare it first, and falls back on UNIMPLEMENTED);
- a zero-row result keeps its columns;
- `adbc_get_info`, `adbc_get_objects`, `adbc_get_table_types` and `adbc_get_table_schema`;
- a name from the listing, quoted and qualified, queries;
- a log-in with the token as the password works, and none without the token;
- a write is refused;
- over TLS (a certificate `openssl` makes for the run), a client that trusts the certificate
  queries, and one that does not is refused.

The certificate needs OpenSSL 1.1.1 or later on PATH, for `-addext`; macOS's LibreSSL `openssl`
lacks it.

It exits 0 when every check passes and 1 at the first that fails, printing what each one saw.
"""

import os
import socket
import subprocess
import sys
import tempfile
import time

import adbc_driver_flightsql.dbapi as flight_sql
import adbc_driver_manager
import pyarrow as pa
import pyarrow.parquet as pq

TOKEN = "smoke_t0ken"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def check(name: str, ok: bool, saw) -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}: {saw}")
    if not ok:
        sys.exit(1)


def connect(uri: str, **kwargs):
    return flight_sql.connect(uri, autocommit=True, **kwargs)


def wait_for_start(server: subprocess.Popen, log_path: str, within: float = 30.0) -> str:
    """The server's line saying where it listens, once it has printed it; or, if it exits or
    `within` seconds pass first, what it printed. Its stderr goes to a file, not a pipe, so a
    server that warns a lot never blocks on a pipe nobody reads."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        with open(log_path) as f:
            text = f.read()
        for line in text.splitlines():
            if "Flight SQL on " in line:
                return line
        if server.poll() is not None:
            return f"exited with {server.returncode}: {text.strip()}"
        time.sleep(0.1)
    return f"nothing within {within:.0f} s"


def self_signed(tmp: str) -> tuple[str, str]:
    """A certificate for localhost and 127.0.0.1 that lives as long as the run, and its key."""
    cert, key = os.path.join(tmp, "cert.pem"), os.path.join(tmp, "key.pem")
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
         "-keyout", key, "-out", cert],
        check=True, capture_output=True,
    )
    return cert, key


def stop(server: subprocess.Popen) -> None:
    server.terminate()
    try:
        server.wait(timeout=10)
    except subprocess.TimeoutExpired:
        server.kill()


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    binary = os.path.abspath(sys.argv[1])
    with tempfile.TemporaryDirectory() as tmp:
        orders = os.path.join(tmp, "orders.csv")
        with open(orders, "w") as f:
            f.write("id,amount,currency\n1,9.5,USD\n2,,EUR\n3,12,USD\n")
        items = os.path.join(tmp, "Items.parquet")
        pq.write_table(pa.table({"k": [1, 2, 3, 4], "v": ["a", "b", "c", "d"]}), items)

        port = free_port()
        log_path = os.path.join(tmp, "server.log")
        log = open(log_path, "w")
        server = subprocess.Popen(
            [binary, "flight", "--addr", f"127.0.0.1:{port}", "--token", TOKEN,
             "--table", f"orders={orders}", "--table", f"Items={items}"],
            stderr=log,
        )
        try:
            line = wait_for_start(server, log_path)
            check("the server starts", "Flight SQL on grpc://" in line, line.strip())
            uri = f"grpc://127.0.0.1:{port}"
            auth = {"adbc.flight.sql.authorization_header": f"Bearer {TOKEN}"}

            with connect(uri, db_kwargs=auth) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT currency, sum(amount) AS total FROM orders "
                    "GROUP BY currency ORDER BY currency"
                )
                rows = cur.fetch_arrow_table().to_pylist()
                check("a statement runs", rows == [
                    {"currency": "EUR", "total": None},
                    {"currency": "USD", "total": 21.5},
                ], rows)

                cur.execute("SELECT * FROM orders WHERE id > 100")
                empty = cur.fetch_arrow_table()
                check("a zero-row result keeps its columns",
                      empty.num_rows == 0 and empty.schema.names == ["id", "amount", "currency"],
                      empty.schema.names)

                info = conn.adbc_get_info()
                check("get_info names the server", info.get("vendor_name") == "Lakeleto",
                      info.get("vendor_name"))

                listed = [
                    (c["catalog_name"], s["db_schema_name"], t["table_name"])
                    for c in conn.adbc_get_objects().read_all().to_pylist()
                    for s in c["catalog_db_schemas"]
                    for t in s["db_schema_tables"]
                ]
                check("get_objects lists the tables",
                      sorted(listed) == [("lakeleto_tables", "main", "items"),
                                         ("lakeleto_tables", "main", "orders")], listed)

                check("get_table_types", conn.adbc_get_table_types() == ["TABLE"],
                      conn.adbc_get_table_types())

                schema = conn.adbc_get_table_schema(
                    "items", catalog_filter="lakeleto_tables", db_schema_filter="main")
                check("get_table_schema", schema.names == ["k", "v"], schema.names)

                catalog, db_schema, table = listed[0]
                cur.execute(f'SELECT count(*) AS n FROM "{catalog}"."{db_schema}"."{table}"')
                n = cur.fetch_arrow_table().to_pylist()
                check("a listed name, quoted and qualified, queries", n == [{"n": 4}], n)

                try:
                    cur.execute("CREATE TABLE x AS SELECT 1")
                    cur.fetchall()
                    check("a write is refused", False, "it ran")
                except adbc_driver_manager.Error as e:
                    check("a write is refused", "only read queries" in str(e), str(e)[:120])

            with connect(uri, db_kwargs={"username": "anyone", "password": TOKEN}) as conn, \
                    conn.cursor() as cur:
                cur.execute("SELECT count(*) AS n FROM orders")
                n = cur.fetch_arrow_table().to_pylist()
                check("a log-in with the token as the password", n == [{"n": 3}], n)

            try:
                with connect(uri) as conn, conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchall()
                check("no token, no answer", False, "it answered")
            except adbc_driver_manager.Error as e:
                check("no token, no answer", "needs its token" in str(e), str(e)[:120])

            cert, key = self_signed(tmp)
            tls_port = free_port()
            tls_log_path = os.path.join(tmp, "tls-server.log")
            with open(tls_log_path, "w") as tls_log:
                tls_server = subprocess.Popen(
                    [binary, "flight", "--addr", f"127.0.0.1:{tls_port}", "--token", TOKEN,
                     "--tls-cert", cert, "--tls-key", key, "--table", f"orders={orders}"],
                    stderr=tls_log,
                )
                try:
                    line = wait_for_start(tls_server, tls_log_path)
                    check("the server starts with TLS", "grpc+tls://" in line, line.strip())
                    tls_uri = f"grpc+tls://127.0.0.1:{tls_port}"
                    with open(cert) as f:
                        trusted = {**auth, "adbc.flight.sql.client_option.tls_root_certs": f.read()}
                    with connect(tls_uri, db_kwargs=trusted) as conn, conn.cursor() as cur:
                        cur.execute("SELECT count(*) AS n FROM orders")
                        n = cur.fetch_arrow_table().to_pylist()
                        check("over TLS, a client that trusts the certificate", n == [{"n": 3}], n)
                    try:
                        with connect(tls_uri, db_kwargs=auth) as conn, conn.cursor() as cur:
                            cur.execute("SELECT 1")
                            cur.fetchall()
                        check("over TLS, a client that does not trust it", False, "it answered")
                    except adbc_driver_manager.Error as e:
                        check("over TLS, a client that does not trust it", True, str(e)[:120])
                finally:
                    stop(tls_server)
        finally:
            stop(server)
            log.close()
    print("all checks passed")


if __name__ == "__main__":
    main()
