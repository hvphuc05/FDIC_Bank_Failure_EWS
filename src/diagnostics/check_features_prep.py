import polars as pl
import pandas as pd

pd.set_option("display.width", 250)
pd.set_option("display.max_rows", 200)
df = pl.scan_parquet("data_processed/panel/*/*.parquet", hive_partitioning=False)

# A. days from REPDTE to the failure that created each positive label
fe = (pl.read_parquet("data_processed/failure_events.parquet")
        .filter(pl.col("RESTYPE") == "FAILURE")
        .select(pl.col("cert_num").cast(pl.Int64).alias("CERT"), pl.col("fail_dt").cast(pl.Date))
        .drop_nulls())
pos = (df.filter(pl.col("y_fail_4q") == 1).select("CERT", "REPDTE", "split").collect()
         .join(fe, on="CERT")
         .filter((pl.col("fail_dt") > pl.col("REPDTE"))
                 & (pl.col("fail_dt") <= pl.col("REPDTE").dt.offset_by("12mo")))
         .group_by(["CERT", "REPDTE", "split"]).agg(pl.col("fail_dt").min()))
pos = pos.with_columns((pl.col("fail_dt") - pl.col("REPDTE")).dt.total_days().alias("days"))
band = (pl.when(pl.col("days") <= 45).then(pl.lit("1: <=45d"))
          .when(pl.col("days") <= 91).then(pl.lit("2: 46-91d"))
          .when(pl.col("days") <= 182).then(pl.lit("3: 92-182d"))
          .when(pl.col("days") <= 273).then(pl.lit("4: 183-273d"))
          .otherwise(pl.lit("5: >273d")))
print("=== A. Positive rows by days from REPDTE to FAILDATE (rows:", pos.height, ") ===")
print(pos.with_columns(band.alias("band")).to_pandas()
         .pipe(lambda p: pd.crosstab(p["band"], p["split"])).to_string())

cols = ["ASSET", "DEP", "DEPDOM", "EQ", "BRO", "CHBAL", "SC", "LNLSGR", "LNLSNET", "LNRE", "LNCI",
        "LNCON", "LNATRES", "NCLNLS", "P3ASSET", "P9ASSET", "NAASSET", "ORE", "ROA", "ROE", "NIMY",
        "RBC1AAJ", "RBCRWAJ", "NETINC_q", "INTINC_q", "EINTEXP_q", "NONII_q", "NONIX_q",
        "ELNATR_q", "NTLNLS_q"]

# B. null share by 5-year era
era = (pl.col("yr") // 5 * 5).alias("era")
nul = (df.group_by(era).agg([pl.col(c).is_null().mean().alias(c) for c in cols] + [pl.len().alias("rows")])
         .sort("era").collect().to_pandas().set_index("era"))
print("\n=== B. Null share by 5-year era (columns = variables) ===")
print(nul.drop(columns="rows").round(2).T.to_string())

# C. scale check: raw median vs median of value/ASSET
chk = ["P3ASSET", "P9ASSET", "NAASSET", "NCLNLS", "LNATRES", "ORE", "BRO", "NTLNLS_q", "LNLSGR", "EQ"]
sc = (df.group_by("split").agg(
        [pl.col(c).median().alias(f"{c}") for c in chk]
        + [(pl.col(c) / pl.col("ASSET")).median().alias(f"{c}/A") for c in chk])
        .sort("split").collect().to_pandas().set_index("split"))
print("\n=== C. Medians by split: raw value and value/ASSET ===")
print(sc.T.round(4).to_string())