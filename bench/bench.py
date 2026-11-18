"""Lakeleto vs DuckDB CLI vs Polars vs pandas: one command per task, a fresh process each run.

Wall time is measured from process start to exit (so a Python tool includes interpreter start and
imports, which is what a user running that one-liner pays). Peak memory is the peak working set
Windows records for the process, read from its handle after it exits. Windows only, as written.

    uv venv --python 3.12 .venv
    uv pip install --python .venv duckdb==1.4.1 polars==1.34.0 pandas==2.3.3 pyarrow==21.0.0
    .venv\\Scripts\\python -I gen.py                    # writes data/orders_20m.parquet, data/orders_2m.csv
    .venv\\Scripts\\python -I bench.py <lakeleto.exe> <duckdb.exe>   # writes results.json
"""
import json, os, platform, statistics, subprocess, sys, time
import psutil

HERE = os.path.dirname(os.path.abspath(__file__))
PQ = os.path.join(HERE, "data", "orders_20m.parquet")
CSV = os.path.join(HERE, "data", "orders_2m.csv")
LAKELETO = sys.argv[1]
DUCKDB = sys.argv[2]
# The real interpreter, not the venv's python.exe: on Windows that is a launcher that runs the
# interpreter as a child, which would put the work (and the memory) in a process we don't time.
# The venv's site-packages goes on sys.path inside the one-liner instead.
PY = sys._base_executable
SITE = next(p for p in sys.path if p.endswith("site-packages"))
RUNS = int(os.environ.get("RUNS", "5"))

GROUP = "SELECT city, count(*) AS n, round(sum(amount_usd), 2) AS revenue FROM {t} GROUP BY city ORDER BY revenue DESC"
TOPN = "SELECT order_id, city, amount_usd FROM {t} WHERE tier = 'enterprise' AND NOT is_refunded ORDER BY amount_usd DESC LIMIT 10"

def py(code):
    return [PY, "-I", "-X", "utf8", "-c", f"import sys; sys.path.insert(0, r'{SITE}'); " + code]

TASKS = {
    "schema": {
        "label": "Schema + row count (20M-row Parquet)",
        "lakeleto": [LAKELETO, "schema", PQ],
        "duckdb": [DUCKDB, "-c", f"DESCRIBE SELECT * FROM '{PQ}'; SELECT count(*) FROM '{PQ}';"],
        "polars": py(f"import polars as pl; print(pl.read_parquet_schema(r'{PQ}')); print(pl.scan_parquet(r'{PQ}').select(pl.len()).collect())"),
        "pandas": py(f"import pyarrow.parquet as pq; f = pq.ParquetFile(r'{PQ}'); print(f.schema_arrow); print(f.metadata.num_rows)"),
    },
    "head": {
        "label": "First 10 rows (20M-row Parquet)",
        "lakeleto": [LAKELETO, "head", PQ, "-n", "10"],
        "duckdb": [DUCKDB, "-c", f"SELECT * FROM '{PQ}' LIMIT 10"],
        "polars": py(f"import polars as pl; print(pl.scan_parquet(r'{PQ}').head(10).collect())"),
        "pandas": py(f"import pyarrow.parquet as pq; b = next(pq.ParquetFile(r'{PQ}').iter_batches(batch_size=10)); print(b.to_pandas())"),
    },
    "groupby": {
        "label": "GROUP BY city over all 20M rows (Parquet)",
        "lakeleto": [LAKELETO, "query", GROUP.format(t="t"), "--file", PQ],
        "duckdb": [DUCKDB, "-c", GROUP.format(t=f"'{PQ}'")],
        "polars": py(f"import polars as pl; print(pl.scan_parquet(r'{PQ}').group_by('city').agg(n=pl.len(), revenue=pl.col('amount_usd').sum().round(2)).sort('revenue', descending=True).collect())"),
        "pandas": py(f"import pandas as pd; d = pd.read_parquet(r'{PQ}', columns=['city','amount_usd']); print(d.groupby('city')['amount_usd'].agg(['count','sum']).round(2).sort_values('sum', ascending=False))"),
    },
    "topn": {
        "label": "Filter + sort, top 10 (20M-row Parquet)",
        "lakeleto": [LAKELETO, "query", TOPN.format(t="t"), "--file", PQ],
        "duckdb": [DUCKDB, "-c", TOPN.format(t=f"'{PQ}'")],
        "polars": py(f"import polars as pl; print(pl.scan_parquet(r'{PQ}').filter((pl.col('tier') == 'enterprise') & ~pl.col('is_refunded')).sort('amount_usd', descending=True).head(10).select('order_id','city','amount_usd').collect())"),
        "pandas": py(f"import pandas as pd; d = pd.read_parquet(r'{PQ}', columns=['order_id','city','tier','amount_usd','is_refunded']); d = d[(d.tier == 'enterprise') & ~d.is_refunded]; print(d.nlargest(10, 'amount_usd')[['order_id','city','amount_usd']])"),
    },
    "csv_groupby": {
        "label": "GROUP BY city over a 2M-row CSV",
        "lakeleto": [LAKELETO, "query", GROUP.format(t="t"), "--file", CSV],
        "duckdb": [DUCKDB, "-c", GROUP.format(t=f"'{CSV}'")],
        "polars": py(f"import polars as pl; print(pl.scan_csv(r'{CSV}').group_by('city').agg(n=pl.len(), revenue=pl.col('amount_usd').sum().round(2)).sort('revenue', descending=True).collect())"),
        "pandas": py(f"import pandas as pd; d = pd.read_csv(r'{CSV}', usecols=['city','amount_usd']); print(d.groupby('city')['amount_usd'].agg(['count','sum']).round(2).sort_values('sum', ascending=False))"),
    },
}

import ctypes
from ctypes import wintypes

class PMC(ctypes.Structure):
    _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]
_psapi = ctypes.WinDLL("psapi")

def peak_of(handle):
    """Peak working set of a process that has exited: its handle stays valid until closed."""
    c = PMC(); c.cb = ctypes.sizeof(PMC)
    if not _psapi.GetProcessMemoryInfo(wintypes.HANDLE(int(handle)), ctypes.byref(c), c.cb):
        raise OSError("GetProcessMemoryInfo failed")
    return c.PeakWorkingSetSize

def run_once(cmd):
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    dt = time.perf_counter() - t0
    peak = peak_of(p._handle)
    if p.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed: {err.decode(errors='replace')[:400]}")
    return dt, peak, out.decode(errors="replace")

results = {"machine": {
    "os": platform.platform(), "cpu": platform.processor(),
    "cores_logical": psutil.cpu_count(), "ram_gib": round(psutil.virtual_memory().total / 2**30, 1),
}, "runs": RUNS, "tasks": {}}
for key, t in TASKS.items():
    results["tasks"][key] = {"label": t["label"], "tools": {}}
    for tool in ("lakeleto", "duckdb", "polars", "pandas"):
        run_once(t[tool])  # warm-up: file in the OS cache, binaries loaded
        times, peaks = [], []
        for _ in range(RUNS):
            dt, peak, out = run_once(t[tool])
            times.append(dt); peaks.append(peak)
        results["tasks"][key]["tools"][tool] = {
            "median_s": round(statistics.median(times), 3),
            "min_s": round(min(times), 3), "max_s": round(max(times), 3),
            "peak_mib": round(max(peaks) / 2**20, 1),
            "sample_output": out[:900],
        }
        print(f"{key:12} {tool:9} median {statistics.median(times):7.3f}s  peak {max(peaks)/2**20:8.1f} MiB", flush=True)

json.dump(results, open(os.path.join(HERE, "results.json"), "w"), indent=2)
