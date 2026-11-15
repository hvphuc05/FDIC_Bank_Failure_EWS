import glob
import pandas as pd

pd.set_option("display.width", 250)
RAW = "data_raw/fdic"

f = pd.read_parquet(f"{RAW}/failures.parquet")
f["fail_dt"] = pd.to_datetime(f["FAILDATE"], format="%m/%d/%Y", errors="coerce")
f["cert_num"] = pd.to_numeric(f["CERT"], errors="coerce")
scope = f[(f["fail_dt"] >= "1984-01-01") & (f["fail_dt"] <= "2026-06-30")].copy()

parts = []
for p in sorted(glob.glob(f"{RAW}/financials/**/*.parquet", recursive=True)):
    parts.append(pd.read_parquet(p, columns=["CERT", "REPDTE", "ASSET"]))
fin = pd.concat(parts, ignore_index=True)
fin["cert_num"] = pd.to_numeric(fin["CERT"], errors="coerce")
fin["rep_dt"] = pd.to_datetime(fin["REPDTE"].astype(str), format="mixed", errors="coerce")
fin["ASSET"] = pd.to_numeric(fin["ASSET"], errors="coerce")
fin = fin[["cert_num", "rep_dt", "ASSET"]]
print("financial rows:", len(fin), "| rep_dt null:", fin["rep_dt"].isna().sum(),
      "| range:", fin["rep_dt"].min().date(), "->", fin["rep_dt"].max().date())

print("\n=== A. 4 CERT bất thường: báo cáo đầu/cuối và các sự kiện ===")
for c in [28448, 29400, 31927, 32059]:
    r = fin.loc[fin["cert_num"] == c, "rep_dt"]
    ev = scope.loc[scope["cert_num"] == c, ["RESTYPE", "FAILDATE"]].values.tolist()
    print(c, "| n =", len(r), "| first", r.min().date(), "| last", r.max().date(), "| events", ev)

m = scope.merge(fin, on="cert_num", how="left")
m["is_pre"] = m["rep_dt"] < m["fail_dt"]
m["is_post"] = m["rep_dt"] >= m["fail_dt"]
g = m.groupby("ID").agg(RESTYPE=("RESTYPE", "first"), fail_dt=("fail_dt", "first"),
                        cert_num=("cert_num", "first"),
                        n_pre=("is_pre", "sum"), n_post=("is_post", "sum"))
g["last_pre"] = g.index.map(m[m["is_pre"]].groupby("ID")["rep_dt"].max())

print("\n=== B. Số báo cáo sau ngày sự kiện, theo RESTYPE ===")
for rt, s in g.groupby("RESTYPE"):
    p = s[s["n_post"] > 0]
    print(rt, "| records:", len(s), "| no report before event:", int((s["n_pre"] == 0).sum()),
          "| with >=1 report after:", len(p), "| with >=4 after:", int((s["n_post"] >= 4).sum()),
          "| median n_post (among >0):", p["n_post"].median())
print("\nFAILURE: phân bố n_post (chỉ các record có n_post>0):")
print(g[(g["RESTYPE"] == "FAILURE") & (g["n_post"] > 0)]["n_post"].value_counts().sort_index().head(15).to_string())

print("\n=== C. Dòng sau FAILDATE của FAILURE: ASSET null hoặc 0? ===")
post = m[m["is_post"] & (m["RESTYPE"] == "FAILURE")]
print("rows:", len(post), "| ASSET null:", int(post["ASSET"].isna().sum()),
      "| ASSET == 0:", int((post["ASSET"] == 0).sum()))

print("\n=== D. FAILURE gán nhãn được (có báo cáo trong 366 ngày trước FAILDATE) ===")
fl = g[g["RESTYPE"] == "FAILURE"].copy()
fl["lag_days"] = (fl["fail_dt"] - fl["last_pre"]).dt.days
fl["labelable"] = fl["lag_days"] <= 366
periods = {"dev  (FAILDATE 1984-04-01 .. 2007-12-31)": ("1984-04-01", "2007-12-31"),
           "test (FAILDATE 2008-04-01 .. 2026-06-30)": ("2008-04-01", "2026-06-30")}
for name, (a, b) in periods.items():
    s = fl[(fl["fail_dt"] >= a) & (fl["fail_dt"] <= b)]
    print(name, "| records:", len(s), "| labelable records:", int(s["labelable"].sum()),
          "| distinct CERT labelable:", s.loc[s["labelable"], "cert_num"].nunique())