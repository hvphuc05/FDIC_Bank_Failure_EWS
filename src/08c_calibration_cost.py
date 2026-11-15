"""Step 08c - calibration and cost-based thresholds on the DEV out-of-fold predictions.
Cost unit = C_FP. A missed failure costs r = C_FN / C_FP; costs are counted per bank-quarter row."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as C

R_GRID = [10, 20, 50, 100]
PROB_MODELS = ["B1_logit", "M1_hist_gbm"]
ALL_MODELS = ["B0_low_capital_rule"] + PROB_MODELS


def cost_descriptives():
    ev = pd.read_parquet(C.PROCESSED_DATA_DIR / "failure_events.parquet")
    c = ev.loc[ev["use_for_cost_dev"], "cost_effective"].astype(float)
    print("=== COST of dev failures usable for the cost estimate (thousand USD) ===")
    print(f"n={len(c)} | mean={c.mean():,.0f} | median={c.median():,.0f} | p90={c.quantile(.9):,.0f} | "
          f"max={c.max():,.0f} | mean/median={c.mean() / c.median():.1f}")
    print("Implied C_FP = mean COST / r:", {r: round(c.mean() / r) for r in R_GRID})
    return float(c.mean())


def calibration(oof):
    edges = [0, .5, .9, .95, .98, .99, 1.0001]
    rows = []
    for k in sorted(oof["fold"].unique().to_list()):
        f = oof.filter(pl.col("fold") == k)
        y = f["y_fail_4q"].to_numpy()
        for m in PROB_MODELS:
            p = f[m].to_numpy()
            rows.append({"fold": k, "model": m, "bin": "ALL", "n": len(y),
                         "mean_pred_pct": 100 * p.mean(), "obs_pct": 100 * y.mean()})
            pos = np.empty(len(p))
            pos[np.argsort(p, kind="mergesort")] = np.arange(len(p)) / len(p)
            for lo, hi in zip(edges[:-1], edges[1:]):
                mk = (pos >= lo) & (pos < hi)
                rows.append({"fold": k, "model": m, "bin": f"{lo:.0%}-{min(hi, 1):.0%}", "n": int(mk.sum()),
                             "mean_pred_pct": 100 * p[mk].mean(), "obs_pct": 100 * y[mk].mean()})
    return pd.DataFrame(rows)


def at_threshold(y, s, thr, r):
    alert = s >= thr
    P, N = int(y.sum()), len(y)
    tp = int((alert & (y == 1)).sum())
    fp = int(alert.sum()) - tp
    cost = r * (P - tp) + fp
    never = r * P
    return {"thr": thr, "alert_rate_pct": 100 * alert.mean(), "recall": tp / P,
            "precision_pct": 100 * tp / max(tp + fp, 1), "saving_vs_never_pct": 100 * (1 - cost / never),
            "beats_always_alert": bool(cost < (N - P))}


def best_threshold(y, s, r):
    order = np.argsort(-s, kind="mergesort")
    ys, ss = y[order].astype(np.int64), s[order]
    tp, fp = np.cumsum(ys), np.cumsum(1 - ys)
    idx = np.r_[np.flatnonzero(np.diff(ss) != 0), len(ss) - 1]   # evaluate only at tie-group ends
    cost = r * (int(ys.sum()) - tp[idx]) + fp[idx]
    j = int(np.argmin(cost))
    return ss[idx[j]] if cost[j] < r * int(ys.sum()) else np.inf  # inf = never alert


def cost_tables(oof):
    rows = []
    groups = {k: oof.filter(pl.col("fold") == k) for k in sorted(oof["fold"].unique().to_list())}
    groups["pooled"] = oof
    for m in ALL_MODELS:
        for r in R_GRID:
            thr_pooled = best_threshold(oof["y_fail_4q"].to_numpy(), oof[m].to_numpy(), r)
            for g, f in groups.items():
                y, s = f["y_fail_4q"].to_numpy(), f[m].to_numpy()
                rows.append({"rule": "opt_in_group", "fold": g, "model": m, "r": r,
                             **at_threshold(y, s, best_threshold(y, s, r), r)})
                if g != "pooled":
                    rows.append({"rule": "pooled_thr_applied", "fold": g, "model": m, "r": r,
                                 **at_threshold(y, s, thr_pooled, r)})
                if m in PROB_MODELS:
                    rows.append({"rule": "p_star=1/(1+r)", "fold": g, "model": m, "r": r,
                                 **at_threshold(y, s, 1 / (1 + r), r)})
    return pd.DataFrame(rows)


def main():
    cost_descriptives()
    oof = pl.read_parquet(C.PROCESSED_DATA_DIR / "oof_dev.parquet")
    pd.set_option("display.width", 250)
    pd.set_option("display.max_rows", 500)
    cal = calibration(oof)
    cal.to_csv(C.REPORT_DIR / "cv_calibration.csv", index=False)
    print("\n=== Calibration (mean predicted % vs observed %), by rank bin of predicted probability ===")
    print(cal.round(4).to_string(index=False))
    t = cost_tables(oof)
    t.to_csv(C.REPORT_DIR / "cv_cost_thresholds.csv", index=False)
    for rule in ["opt_in_group", "pooled_thr_applied", "p_star=1/(1+r)"]:
        print(f"\n=== rule: {rule} ===")
        print(t[t["rule"] == rule].drop(columns="rule").round(4).to_string(index=False))


if __name__ == "__main__":
    main()