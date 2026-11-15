import pandas as pd

pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 38)
RAW = "data_raw/fdic"

f = pd.read_parquet(f"{RAW}/failures.parquet")
inst = pd.read_parquet(f"{RAW}/institutions.parquet")
f["fail_dt"] = pd.to_datetime(f["FAILDATE"], format="%m/%d/%Y", errors="coerce")
f["cert_num"] = pd.to_numeric(f["CERT"], errors="coerce")
f["cost_num"] = pd.to_numeric(f["COST"], errors="coerce")
scope = f[(f["fail_dt"] >= "1984-01-01") & (f["fail_dt"] <= "2026-06-30")]

print("\n=== 1. In-scope records by RESTYPE ===")
print(scope["RESTYPE"].value_counts(dropna=False).to_string())

print("\n=== 2. In-scope records per year and RESTYPE ===")
print(pd.crosstab(scope["fail_dt"].dt.year, scope["RESTYPE"]).to_string())

print("\n=== 3. COST by RESTYPE (in scope) ===")
print(scope.groupby("RESTYPE")["cost_num"].agg(
    n="size", n_null=lambda s: s.isna().sum(), n_zero=lambda s: (s == 0).sum(),
    median="median", total="sum").to_string())

print("\n=== 4. Event sequence for CERTs that appear more than once (in scope) ===")
rep = scope[scope["cert_num"].notna() & scope["cert_num"].duplicated(keep=False)]
seq = rep.sort_values(["cert_num", "fail_dt"]).groupby("cert_num")["RESTYPE"].agg(" > ".join)
print(seq.value_counts().to_string())

print("\n=== 5. Reports dated after the failure date, by RESTYPE ===")
detail = pd.read_csv("report/dq_failures_linked_detail.csv")
fx = f[["ID", "RESTYPE", "cert_num", "fail_dt"]].copy()
fx["ID"] = pd.to_numeric(fx["ID"], errors="coerce")  # ID is text in failures.parquet
d = detail.merge(fx, on="ID", how="left")
neg = d[d["gap_days"] < 0].copy()
neg["gap_band"] = pd.cut(neg["gap_days"], [-1e9, -1095, -366, -1],
                         labels=["> 3 years", "1-3 years", "< 1 year"])
print(pd.crosstab(neg["gap_band"], neg["RESTYPE"]).to_string())

print("\n=== 6. FAILURE records with later reports: institutions table vs failure date ===")
nf = neg[neg["RESTYPE"] == "FAILURE"]
meta = inst.assign(cert_num=pd.to_numeric(inst["CERT"], errors="coerce"))[
    ["cert_num", "NAME", "ESTYMD"]].rename(columns={"NAME": "INST_NAME"})
m = nf.merge(meta.drop_duplicates("cert_num"), on="cert_num", how="left")
m["est_dt"] = pd.to_datetime(m["ESTYMD"], format="mixed", errors="coerce")
print(f"rows: {len(m)} | established AFTER the failure date: {(m['est_dt'] > m['fail_dt']).sum()} "
      f"| ESTYMD unparsed: {m['est_dt'].isna().sum()}")
print(m.sort_values("gap_days")[["ID", "CERT", "NAME", "INST_NAME", "FAILDATE", "ESTYMD",
                                   "gap_days"]].head(12).to_string(index=False))

print("\n=== 7. What the institutions table holds (raw values) ===")
print("ACTIVE:", inst["ACTIVE"].value_counts(dropna=False).to_dict())
print("CHANGEC1 (top 12):", inst["CHANGEC1"].value_counts(dropna=False).head(12).to_dict())
print("ENDEFYMD (top 5):", inst["ENDEFYMD"].value_counts(dropna=False).head(5).to_dict())