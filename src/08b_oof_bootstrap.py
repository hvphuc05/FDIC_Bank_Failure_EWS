"""Step 08b - out-of-fold predictions on DEV + cluster bootstrap (banks are resampled, not rows)."""
import datetime as dt
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from sklearn.metrics import average_precision_score, roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C
from modeling import load_dev, make_models

N_BOOT = 200
MODELS = ["B0_low_capital_rule", "B1_logit", "M1_hist_gbm"]


def build_oof():
    df = load_dev()
    rep, cert = df["REPDTE"], df["CERT"].to_numpy()
    X = df.select(C.FEATURE_COLS).to_numpy().astype(np.float32)
    y = df["y_fail_4q"].to_numpy().astype(np.int8)
    D = dt.date.fromisoformat
    parts = []
    for k, (t_end, v_start, v_end) in enumerate(C.CV_FOLDS, 1):
        tr = (rep <= D(t_end)).to_numpy()
        va = ((rep >= D(v_start)) & (rep <= D(v_end))).to_numpy()
        assert not (tr & va).any()
        scores = {}
        for name, m in make_models(X[tr]).items():
            if callable(m):
                scores[name] = m(X[va]).astype(np.float64)
            else:
                m.fit(X[tr], y[tr])
                scores[name] = m.predict_proba(X[va])[:, 1]
        part = df.filter(pl.Series(va)).select("CERT", "REPDTE", "y_fail_4q").with_columns(
            pl.lit(k, dtype=pl.Int8).alias("fold"), *[pl.Series(n, s) for n, s in scores.items()])
        parts.append(part)
        print(f"fold {k}: OOF rows {part.height}")
    oof = pl.concat(parts)
    oof.write_parquet(C.PROCESSED_DATA_DIR / "oof_dev.parquet", compression="zstd")
    return oof


def ci(a):
    a = np.asarray(a)
    return np.percentile(a, 2.5), np.percentile(a, 97.5)


def bootstrap(oof):
    rng = np.random.default_rng(C.RANDOM_SEED)
    single, pairs = [], []
    for k in sorted(oof["fold"].unique().to_list()):
        f = oof.filter(pl.col("fold") == k)
        y = f["y_fail_4q"].to_numpy()
        banks, bidx = np.unique(f["CERT"].to_numpy(), return_inverse=True)
        S = {m: f[m].to_numpy() for m in MODELS}
        point = {m: (average_precision_score(y, S[m]), roc_auc_score(y, S[m])) for m in MODELS}
        draws = {m: {"ap": [], "auc": []} for m in MODELS}
        for _ in range(N_BOOT):
            cnt = np.bincount(rng.integers(0, len(banks), len(banks)), minlength=len(banks))
            w = cnt[bidx]
            keep = w > 0
            yy, ww = y[keep], w[keep]
            for m in MODELS:
                draws[m]["ap"].append(average_precision_score(yy, S[m][keep], sample_weight=ww))
                draws[m]["auc"].append(roc_auc_score(yy, S[m][keep], sample_weight=ww))
        for m in MODELS:
            single.append({"fold": k, "model": m, "pos_banks": int(np.unique(f["CERT"].to_numpy()[y == 1]).size),
                           "pr_auc": point[m][0], "pr_auc_lo": ci(draws[m]["ap"])[0], "pr_auc_hi": ci(draws[m]["ap"])[1],
                           "roc_auc": point[m][1], "roc_auc_lo": ci(draws[m]["auc"])[0], "roc_auc_hi": ci(draws[m]["auc"])[1]})
        for a, b in itertools.combinations(reversed(MODELS), 2):   # M1-B1, M1-B0, B1-B0
            for met in ("ap", "auc"):
                d = np.array(draws[a][met]) - np.array(draws[b][met])
                lo, hi = ci(d)
                pairs.append({"fold": k, "metric": met, "diff": f"{a} - {b}", "mean_diff": d.mean(),
                              "lo": lo, "hi": hi, "share_positive": float((d > 0).mean())})
    return pd.DataFrame(single), pd.DataFrame(pairs)


def main():
    oof = build_oof()
    single, pairs = bootstrap(oof)
    single.to_csv(C.REPORT_DIR / "cv_bootstrap_ci.csv", index=False)
    pairs.to_csv(C.REPORT_DIR / "cv_bootstrap_pairs.csv", index=False)
    pd.set_option("display.width", 250)
    print(f"\n=== point estimate and 95% bank-cluster bootstrap CI ({N_BOOT} resamples) ===")
    print(single.round(4).to_string(index=False))
    print("\n=== paired differences (same resamples) ===")
    print(pairs.round(4).to_string(index=False))


if __name__ == "__main__":
    main()