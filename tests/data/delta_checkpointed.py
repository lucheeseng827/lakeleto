"""Writes tests/data/delta_checkpointed: a Delta table, written by delta-rs, read from a checkpoint.

    pip install deltalake==1.7.0 pyarrow
    python tests/data/delta_checkpointed.py tests/data/delta_checkpointed

Version 0 writes ids 1-3 in region "eu"; 1 appends 4-5 in region "us west:%x", whose partition
directory and add.path are percent-encoded; 2 deletes id 2, rewriting the eu file; a checkpoint is
taken at 2; 3 appends 6 in eu. Then commits 0-2 are deleted, as log cleanup does, so the table can
be read only from its checkpoint. The eu file version 2 removed stays on disk, as it does until a
VACUUM. Live rows: ids 1, 3, 4, 5 and 6.
"""

import os
import shutil
import sys

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

out = sys.argv[1]
shutil.rmtree(out, ignore_errors=True)
schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("region", pa.string())])


def rows(ids, names, region):
    return pa.table({"id": ids, "name": names, "region": [region] * len(ids)}, schema=schema)


write_deltalake(out, rows([1, 2, 3], ["ada", "bob", "cy"], "eu"), partition_by=["region"])
write_deltalake(out, rows([4, 5], ["dee", None], "us west:%x"), mode="append", partition_by=["region"])
DeltaTable(out).delete("id = 2")
DeltaTable(out).create_checkpoint()
write_deltalake(out, rows([6], ["eve"], "eu"), mode="append", partition_by=["region"])
log = os.path.join(out, "_delta_log")
for version in range(3):
    os.remove(os.path.join(log, f"{version:020}.json"))
print(sorted(os.listdir(log)))
