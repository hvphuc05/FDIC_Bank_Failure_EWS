import pandas as pd

pd.set_option("display.width", 250)
RAW = "data_raw/fdic"

f = pd.read_parquet(f"{RAW}/failures.parquet")
inst = pd.read_parquet(f"{RAW}/institutions.parquet")
f["fail_dt"] = pd.to_datetime(f["FAILDATE"], format="%m/%d/%Y", errors="coerce")
f["cert_num"] = pd.to_numeric(f["CERT"], errors="coerce")
f["cost_num"] = pd.to_numeric(f["COST"], errors="coerce")
inst["cert_num"] = pd.to_numeric(inst["CERT"], errors="coerce")
scope = f[(f["fail_dt"] >= "1984-01-01") & (f["fail_dt"] <= "2026-06-30")]

print("\n=== A. CERT lặp có chuỗi KHÁC 'ASSISTANCE > FAILURE' ===")
rep = scope[scope["cert_num"].duplicated(keep=False) & scope["cert_num"].notna()]
rep = rep.sort_values(["cert_num", "fail_dt"])
seq = rep.groupby("cert_num")["RESTYPE"].agg(" > ".join)
odd = seq[seq != "ASSISTANCE > FAILURE"].index
print(rep[rep["cert_num"].isin(odd)][
    ["ID", "CERT", "NAME", "FAILDATE", "RESTYPE", "COST"]].to_string(index=False))

print("\n=== B. FAILURE có COST = 0: có ASSISTANCE trước đó (cùng CERT) không? ===")
fail = scope[scope["RESTYPE"] == "FAILURE"]
assist = scope[scope["RESTYPE"] == "ASSISTANCE"][["cert_num", "fail_dt", "cost_num"]]
assist = assist.rename(columns={"fail_dt": "assist_dt", "cost_num": "assist_cost"})
z = fail[fail["cost_num"] == 0].merge(assist, on="cert_num", how="left")
z["has_prior_assist"] = z["assist_dt"].notna() & (z["assist_dt"] < z["fail_dt"])
z1 = z.drop_duplicates("ID")
print("Tổng FAILURE COST=0:", len(z1),
      "| có ASSISTANCE trước đó:", int(z1.groupby("ID")["has_prior_assist"].max().sum()))
print(pd.crosstab(z1["fail_dt"].dt.year, z1["has_prior_assist"]).to_string())

print("\n=== C. ASSISTANCE: bao nhiêu CERT, bao nhiêu CERT sau đó FAILURE? ===")
a_certs = set(assist["cert_num"].dropna())
f_certs = set(fail["cert_num"].dropna())
print("CERT có ASSISTANCE:", len(a_certs),
      "| trong đó sau đó có FAILURE:", len(a_certs & f_certs))

print("\n=== D. ACTIVE x (ENDEFYMD == 12/31/9999) ===")
inst["end_open"] = inst["ENDEFYMD"].astype(str).eq("12/31/9999")
print(pd.crosstab(inst["ACTIVE"], inst["end_open"]).to_string())

print("\n=== E. CHANGEC1 của các CERT FAILURE / ASSISTANCE (bằng chứng thực nghiệm) ===")
for name, certs in [("FAILURE", f_certs), ("ASSISTANCE", a_certs)]:
    sub = inst[inst["cert_num"].isin(certs)]
    print(name, "n =", len(sub), "->", sub["CHANGEC1"].value_counts(dropna=False).head(8).to_dict())