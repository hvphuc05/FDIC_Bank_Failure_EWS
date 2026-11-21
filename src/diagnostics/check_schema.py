from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

types = defaultdict(set)
for p in sorted(Path("data_raw/fdic/financials").glob("*.parquet")):
    for field in pq.read_schema(p):
        types[field.name].add(str(field.type))

for name, kinds in types.items():
    print(f"{name:10s} {sorted(kinds)}")