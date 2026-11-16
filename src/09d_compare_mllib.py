"""Step 09d - local: (1) --prep builds the development-only input of the MLlib demo; (2) compare the MLlib results with
the scikit-learn ones (fold-3 out-of-fold probabilities and the final-model coefficients).
  python src/09d_compare_mllib.py --prep
  python src/09d_compare_mllib.py --compare mllib_out"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from sklearn.metrics import average_precision_score, roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

OUT = C.PROCESSED_DATA_DIR / "bench" / "mllib_dev_features.parquet"


def prep():
    from modeling import load_dev                       # development rows only (asserted)
    df = load_dev()
    df = df.select("CERT", "REPDTE", pl.col("y_fail_4q").cast(pl.Int32),
                   *[pl.col(c).cast(pl.Float64) for c in C.FEATURE_COLS])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT, compression="zstd")
    print("written", OUT, df.height, "rows;", int(df["y_fail_4q"].sum()), "positive rows;", round(OUT.stat().st_size / 1e6), "MB")


def compare(spark_dir):
    d = Path(spark_dir)
    res = json.loads((d / "mllib_result.json").read_text(encoding="utf-8"))
    sp = pl.read_parquet(str(d / "fold3_predictions" / "*.parquet"))
    oof = pl.read_parquet(C.PROCESSED_DATA_DIR / "oof_dev.parquet").filter(pl.col("fold") == 3).select(
        "CERT", "REPDTE", "y_fail_4q", "B1_logit")
    j = sp.join(oof, on=["CERT", "REPDTE"], how="inner")
    print("Spark rows:", sp.height, "| scikit-learn out-of-fold rows (fold 3):", oof.height, "| matched:", j.height)
    assert j.height == sp.height == oof.height, "row sets differ"
    assert (j["label"].cast(pl.Int64) == j["y_fail_4q"].cast(pl.Int64)).all()
    p_sp, p_sk, y = j["p"].to_numpy(), j["B1_logit"].to_numpy(), j["y_fail_4q"].to_numpy()
    diff = np.abs(p_sp - p_sk)
    print(f"\nprobabilities: max |diff| {diff.max():.3e} | mean |diff| {diff.mean():.3e} | "
          f"Pearson {np.corrcoef(p_sp, p_sk)[0, 1]:.6f} | Spearman {pd.Series(p_sp).corr(pd.Series(p_sk), method='spearman'):.6f}")
    ref = pd.read_csv(C.REPORT_DIR / "cv_bootstrap_ci.csv")
    ref = ref[(ref["fold"] == 3) & (ref["model"] == "B1_logit")].iloc[0]
    print(f"scikit-learn fold 3: ROC-AUC {ref['roc_auc']:.4f}, PR-AUC (average precision) {ref['pr_auc']:.4f}")
    print(f"Spark probabilities scored with scikit-learn: ROC-AUC {roc_auc_score(y, p_sp):.4f}, "
          f"PR-AUC (average precision) {average_precision_score(y, p_sp):.4f}")
    f3 = res["fold3"]
    print(f"Spark's own metrics: areaUnderROC {f3['areaUnderROC']:.4f}, areaUnderPR (trapezoid, not comparable to average precision) "
          f"{f3['areaUnderPR_spark_trapezoid']:.4f} | iterations {f3['iterations']} | fit {f3['seconds_total_fit']} s")

    sk = pd.read_csv(C.REPORT_DIR / "final_model_coefficients.csv")
    sk = sk[sk["feature"] != "(intercept)"].set_index("feature")["coef_standardised"]
    ml = pd.Series(res["final_fit"]["coefficients"])
    tab = pd.DataFrame({"sklearn": sk, "mllib": ml.reindex(sk.index)})
    tab["abs_diff"] = (tab["sklearn"] - tab["mllib"]).abs()
    print("\nfinal-model coefficients (standardised features), sorted by |scikit-learn|:")
    print(tab.reindex(tab["sklearn"].abs().sort_values(ascending=False).index).round(4).to_string())
    print(f"max |diff| {tab['abs_diff'].max():.4f} | same sign {(np.sign(tab['sklearn']) == np.sign(tab['mllib'])).sum()}/{len(tab)} | "
          f"Spearman {tab['sklearn'].corr(tab['mllib'], method='spearman'):.4f}")
    tab.to_csv(C.REPORT_DIR / "mllib_vs_sklearn_coefficients.csv")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prep", action="store_true")
    ap.add_argument("--compare", metavar="MLLIB_OUT_DIR")
    a = ap.parse_args()
    prep() if a.prep else compare(a.compare)