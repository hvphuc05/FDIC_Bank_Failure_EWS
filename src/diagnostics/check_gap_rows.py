import polars as pl

df = pl.read_parquet("data_processed/panel_decumulated.parquet",
                     columns=["CERT", "REPDTE", "yr", "q", "NETINC"])
first = df.group_by("CERT").agg(pl.col("REPDTE").min().alias("first_dt"))
prev = (df.select("CERT", "yr", "q", pl.col("NETINC").alias("prev_val"))
          .with_columns((pl.col("q") + 1), pl.lit(True).alias("prev_row")))
g = (df.filter((pl.col("q") > 1) & pl.col("NETINC").is_not_null())
       .join(prev, on=["CERT", "yr", "q"], how="left")
       .filter(pl.col("prev_val").is_null())
       .join(first, on="CERT"))
cat = (pl.when(pl.col("prev_row").is_null() & (pl.col("REPDTE") == pl.col("first_dt")))
         .then(pl.lit("A: first-ever report is at Q2-Q4"))
         .when(pl.col("prev_row").is_null())
         .then(pl.lit("B: previous-quarter row missing (other)"))
         .otherwise(pl.lit("C: previous row exists, its NETINC is null")))
print(g.height, "(expected 5278)")
print(g.with_columns(cat.alias("cat")).group_by("cat").len().sort("cat"))