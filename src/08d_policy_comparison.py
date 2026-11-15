"""Step 08d - threshold POLICIES compared on the DEV out-of-fold predictions (robustness across regimes).
Reuses the cost functions of 08c. saving_vs_never_pct > 0  <=>  precision of the alert set > 1/(1+r)."""
import importlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

cc = importlib.import_module("08c_calibration_cost")


def main():
    oof = pl.read_parquet(C.PROCESSED_DATA_DIR / "oof_dev.parquet")
    folds = sorted(oof["fold"].unique().to_list())
    y = oof["y_fail_4q"].to_numpy()
    fold = oof["fold"].to_numpy()
    rows = []
    for m in cc.ALL_MODELS:
        s = oof[m].to_numpy()
        frac = oof.select((pl.col(m).rank("ordinal", descending=True).over("REPDTE")
                           / pl.len().over("REPDTE")).alias("f"))["f"].to_numpy()
        s_rank = -frac                       # alert if within-quarter rank fraction <= q
        for r in cc.R_GRID:
            def per_fold(score, thr_of):
                return [cc.at_threshold(y[fold == k], score[fold == k], thr_of(fold == k), r)
                        ["saving_vs_never_pct"] for k in folds]

            def add(policy, label, sav):
                rows.append({"policy": policy, "model": m, "r": r, "thr_or_alert_pct": label,
                             **{f"sav_f{k}": v for k, v in zip(folds, sav)},
                             "min_sav": min(sav), "mean_sav": float(np.mean(sav))})

            thr_p = cc.best_threshold(y, s, r)
            add("P1_pooled_prob_thr", thr_p, per_fold(s, lambda mk: thr_p))
            if m in cc.PROB_MODELS:
                add("P2_p_star", 1 / (1 + r), per_fold(s, lambda mk: 1 / (1 + r)))
            thr_q = cc.best_threshold(y, s_rank, r)
            q_pct = 100 * (-thr_q) if np.isfinite(thr_q) else 0.0
            add("P3_top_q_per_quarter", q_pct, per_fold(s_rank, lambda mk: thr_q))
            add("ORACLE_in_fold(ref)", np.nan,
                per_fold(s, lambda mk: cc.best_threshold(y[mk], s[mk], r)))
    t = pd.DataFrame(rows)
    t.to_csv(C.REPORT_DIR / "cv_policy_comparison.csv", index=False)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_rows", 500)
    print("=== saving vs never-alert (%), per fold, by policy ===")
    print(t.sort_values(["model", "r", "policy"]).round(2).to_string(index=False))
    print("\n=== average over r of the worst-fold saving and of the mean saving ===")
    print(t.groupby(["model", "policy"])[["min_sav", "mean_sav"]].mean().round(2).to_string())


if __name__ == "__main__":
    main()