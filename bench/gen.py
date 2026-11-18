"""Generate the benchmark tables: orders-shaped, deterministic (setseed)."""
import duckdb, os
con = duckdb.connect()
con.execute("SELECT setseed(0.42)")
q = """
SELECT
  i::BIGINT AS order_id,
  'cust_' || lpad(((random() * 90000)::INT)::VARCHAR, 5, '0') AS customer,
  (['Berlin','Nairobi','Toronto','New York','Mumbai','Sao Paulo','Singapore','Sydney','London','Tokyo'])[1 + (random() * 9.999)::INT] AS city,
  (['free','pro','enterprise'])[1 + (random() * 2.999)::INT] AS tier,
  round(5 + random() * 1200, 2) AS amount_usd,
  (1 + random() * 18)::INT AS quantity,
  round((random() * 5)::INT * 0.05, 2) AS discount_pct,
  random() < 0.04 AS is_refunded,
  CASE WHEN random() < 0.15 THEN NULL ELSE (1 + (random() * 4)::INT)::DOUBLE END AS rating,
  TIMESTAMP '2024-01-01' + to_seconds((random() * 15552000)::BIGINT) AS order_ts
FROM range({n}) t(i)
"""
os.makedirs("data", exist_ok=True)
con.execute(f"COPY ({q.format(n=20_000_000)}) TO 'data/orders_20m.parquet' (FORMAT parquet, COMPRESSION snappy)")
con.execute(f"COPY ({q.format(n=2_000_000)}) TO 'data/orders_2m.csv' (HEADER)")
for f in os.listdir("data"):
    print(f, round(os.path.getsize("data/" + f) / 2**20, 1), "MiB")
