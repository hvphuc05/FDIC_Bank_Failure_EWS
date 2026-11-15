"""Step 07 - feature table (stateless transforms only). Fitted preprocessing lives in step 08."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as C


def ratio(num, den, name):
    n = pl.col(num) if isinstance(num, str) else num
    return pl.when(pl.col(den) > 0).then(n / pl.col(den)).otherwise(None).alias(name)


def build_features():
    lf = pl.scan_parquet(str(C.PROCESSED_PANEL_DIR / "*" / "*.parquet"))
    # value 12 calendar months earlier (calendar join, not row shift); only past information
    lag = lf.select("CERT", pl.col("REPDTE").dt.offset_by("12mo").alias("REPDTE"),
                    pl.col("ASSET").alias("ASSET_l4"), pl.col("LNLSGR").alias("LNLSGR_l4"))
    lf = lf.join(lag, on=["CERT", "REPDTE"], how="left")
    feats = [
        ratio("EQ", "ASSET", "eq_asset"),
        ratio("NCLNLS", "LNLSGR", "nclnls_loans"),
        ratio("LNATRES", "LNLSGR", "lnatres_loans"),
        ratio("ORE", "ASSET", "ore_asset"),
        ratio("NTLNLS_q", "LNLSGR", "ntlnls_q_loans"),
        ratio("P9ASSET", "ASSET", "p9asset_asset"),
        ratio("NAASSET", "ASSET", "naasset_asset"),
        ratio("NONIX_q", "ASSET", "nonix_q_asset"),
        ratio("NETINC_q", "ASSET", "netinc_q_asset"),
        ratio(pl.col("INTINC_q") - pl.col("EINTEXP_q"), "ASSET", "nim_q_asset"),
        ratio("CHBAL", "ASSET", "chbal_asset"),
        ratio("SC", "ASSET", "sc_asset"),
        ratio("BRO", "DEP", "bro_dep"),
        ratio("LNLSNET", "DEP", "lnlsnet_dep"),
        ratio("LNRE", "LNLSGR", "lnre_loans"),
        pl.when(pl.col("ASSET") > 0).then(pl.col("ASSET").log()).otherwise(None).alias("log_asset"),
        pl.when((pl.col("ASSET_l4") > 0) & (pl.col("ASSET") > 0))
          .then(pl.col("ASSET") / pl.col("ASSET_l4") - 1).otherwise(None).alias("asset_growth_yoy"),
        pl.when((pl.col("LNLSGR_l4") > 0) & (pl.col("LNLSGR") > 0))
          .then(pl.col("LNLSGR") / pl.col("LNLSGR_l4") - 1).otherwise(None).alias("loan_growth_yoy"),
    ]
    keys = ["CERT", "REPDTE", "split", "label_observable"] + C.LABEL_COLS
    out = (lf.with_columns(feats)
             .select(keys + C.FEATURE_COLS)
             .with_columns([pl.col(c).cast(pl.Float32) for c in C.FEATURE_COLS])
             .collect().sort(["CERT", "REPDTE"]))
    return out


def main():
    assert set(C.FEATURE_COLS).isdisjoint(C.FORBIDDEN_FEATURE_COLS), "forbidden column in FEATURE_COLS"
    out = build_features()
    out.write_parquet(C.FEATURE_PATH, compression="zstd")
    print("rows:", out.height, "| features:", len(C.FEATURE_COLS),
          "| size MB:", round(C.FEATURE_PATH.stat().st_size / 1e6, 1))
    assert out.height == 1_681_547, "row count must equal the labeled panel"

    nulls = (out.group_by("split")
                .agg([pl.col(c).is_null().mean().round(3).alias(c) for c in C.FEATURE_COLS])
                .sort("split").to_pandas().set_index("split").T)
    infs = {c: int(out[c].is_infinite().sum()) for c in C.FEATURE_COLS}
    print("\nNull share by split:\n", nulls.to_string())
    print("\nFeatures with +-inf:", {k: v for k, v in infs.items() if v})

    # independent pandas check of yoy growth on a random sample (past-only, calendar offset)
    p = pd.read_parquet(C.PROCESSED_PANEL_DIR, columns=["CERT", "REPDTE", "ASSET"])
    p["REPDTE"] = pd.to_datetime(p["REPDTE"])
    s = out.select("CERT", "REPDTE", "asset_growth_yoy").sample(5000, seed=C.RANDOM_SEED).to_pandas()
    s["REPDTE"] = pd.to_datetime(s["REPDTE"])
    s["prev"] = s["REPDTE"] - pd.DateOffset(years=1)
    s = (s.merge(p.rename(columns={"ASSET": "A0"}), on=["CERT", "REPDTE"])
          .merge(p.rename(columns={"REPDTE": "prev", "ASSET": "A1"}), on=["CERT", "prev"], how="left"))
    ref = np.where((s["A1"] > 0) & (s["A0"] > 0), s["A0"] / s["A1"] - 1, np.nan)
    ok = np.isclose(s["asset_growth_yoy"].astype(float), ref, rtol=1e-4, atol=1e-6, equal_nan=True)
    print(f"\nCHECK asset_growth_yoy vs pandas recomputation: mismatches = {int((~ok).sum())} / {len(s)}")
    assert ok.all()


if __name__ == "__main__":
    main()