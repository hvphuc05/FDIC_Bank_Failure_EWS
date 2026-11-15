import glob
import numpy as np
import pandas as pd

RAW = "data_raw/fdic"
cols = ["CERT", "REPDTE", "NETINC", "INTINC", "NTLNLS", "ROA", "ROE", "NIMY", "ASSET"]
fin = pd.concat([pd.read_parquet(p, columns=cols)
                 for p in sorted(glob.glob(f"{RAW}/financials/**/*.parquet", recursive=True))],
                ignore_index=True)
fin["d"] = pd.to_datetime(fin["REPDTE"].astype(str), format="mixed")
fin["yr"], fin["q"] = fin["d"].dt.year, fin["d"].dt.quarter
for c in cols[2:]:
    fin[c] = pd.to_numeric(fin[c], errors="coerce")

print(f"{'col':8} {'n_years':>8} {'|Q2|/|Q1|':>10} {'|Q3|/|Q1|':>10} {'|Q4|/|Q1|':>10} {'share Q1==0':>12}")
for c in cols[2:]:
    w = fin.pivot_table(index=["CERT", "yr"], columns="q", values=c, aggfunc="first").dropna()
    a = w.abs()
    nz = a[1] > 0
    r = lambda k: (a.loc[nz, k] / a.loc[nz, 1]).median()
    print(f"{c:8} {len(w):8d} {r(2):10.2f} {r(3):10.2f} {r(4):10.2f} {(~nz).mean():12.3f}")