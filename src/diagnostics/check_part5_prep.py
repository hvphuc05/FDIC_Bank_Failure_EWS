import polars as pl
import datetime as dt

# ---- Part 1: NCLNLS == P9ASSET + NAASSET ? ----
p = pl.read_parquet("data_processed/panel_decumulated.parquet",
                    columns=["yr", "NCLNLS", "P9ASSET", "NAASSET"]).drop_nulls()
p = p.with_columns((pl.col("NCLNLS") - pl.col("P9ASSET") - pl.col("NAASSET")).abs().alias("d"))
print("rows with all three:", p.height,
      "| share |diff|==0:", round((p["d"] == 0).mean(), 4),
      "| share |diff|<=1:", round((p["d"] <= 1).mean(), 4),
      "| median diff:", p["d"].median())
print(p.group_by((pl.col("yr") // 5 * 5).alias("era"))
        .agg((pl.col("d") <= 1).mean().round(4).alias("share_diff_le_1"), pl.len().alias("rows"))
        .sort("era"))

# ---- Part 2: expanding-window folds with a 4-quarter purge ----
f = pl.read_parquet("data_processed/features.parquet",
                    columns=["CERT", "REPDTE", "split", "y_fail_4q"]).filter(pl.col("split") == "dev")
D = dt.date
folds = [  # (train_end T, val_start = T + 12 months, val_end)
    (D(1986, 12, 31), D(1987, 12, 31), D(1989, 12, 31)),
    (D(1989, 12, 31), D(1990, 12, 31), D(1992, 12, 31)),
    (D(1992, 12, 31), D(1993, 12, 31), D(1999, 12, 31)),
    (D(1999, 12, 31), D(2000, 12, 31), D(2006, 12, 31)),
]
rows = []
for i, (t, v0, v1) in enumerate(folds, 1):
    for name, s in (("train", f.filter(pl.col("REPDTE") <= t)),
                    ("val", f.filter((pl.col("REPDTE") >= v0) & (pl.col("REPDTE") <= v1)))):
        pos = s.filter(pl.col("y_fail_4q") == 1)
        rows.append((i, name, s.height, pos.height, pos["CERT"].n_unique(),
                     round(100 * pos.height / s.height, 3)))
print(pl.DataFrame(rows, schema=["fold", "set", "rows", "pos_rows", "pos_banks", "pos_rate_pct"],
                   orient="row"))