"""Step 08 (stage A) - time-ordered validation on DEV only. The test period is never loaded here."""
import datetime as dt
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C
from preprocess import make_preprocessor

ALERT_RATE = 0.05  # descriptive only: recall when the top 5% of rows by score raise an alert


def load_dev():
    df = (pl.scan_parquet(C.FEATURE_PATH).filter(pl.col("split") == "dev")
            .select(["CERT", "REPDTE", "y_fail_4q"] + C.FEATURE_COLS).collect())
    assert df["y_fail_4q"].null_count() == 0
    assert df["REPDTE"].max() <= dt.date.fromisoformat(C.DEV_END), "non-dev rows loaded"
    return df


def metrics(y, s):
    base = y.mean()
    ap = average_precision_score(y, s)
    thr = np.quantile(s, 1 - ALERT_RATE)
    return {"pos_rows": int(y.sum()), "base_rate_pct": 100 * base,
            "roc_auc": roc_auc_score(y, s), "pr_auc": ap, "pr_auc_lift": ap / base,
            f"recall_top{int(ALERT_RATE * 100)}pct": y[s >= thr].sum() / y.sum()}


def make_models(Xtr):
    i = C.FEATURE_COLS.index("eq_asset")
    med = np.nanmedian(-Xtr[:, i])
    rule = lambda X: np.where(np.isnan(X[:, i]), med, -X[:, i])  # lower capital ratio = riskier
    logit = Pipeline([("prep", make_preprocessor()),
                      ("clf", LogisticRegression(C=1.0, max_iter=300))])
    hgb = Pipeline([("prep", make_preprocessor()),
                    ("clf", HistGradientBoostingClassifier(
                        max_iter=150, learning_rate=0.1, max_leaf_nodes=15, min_samples_leaf=200,
                        l2_regularization=1.0, early_stopping=False, random_state=C.RANDOM_SEED))])
    return {"B0_low_capital_rule": rule, "B1_logit": logit, "M1_hist_gbm": hgb}


def main():
    df = load_dev()
    rep = df["REPDTE"]
    cert = df["CERT"].to_numpy()
    D = dt.date.fromisoformat
    X = df.select(C.FEATURE_COLS).to_numpy().astype(np.float32)
    y = df["y_fail_4q"].to_numpy().astype(np.int8)
    print("dev rows:", len(y), "| positives:", int(y.sum()), "| last quarter:", rep.max())
    rows = []
    for k, (t_end, v_start, v_end) in enumerate(C.CV_FOLDS, 1):
        tr = (rep <= D(t_end)).to_numpy()
        va = ((rep >= D(v_start)) & (rep <= D(v_end))).to_numpy()
        assert not (tr & va).any()
        for name, m in make_models(X[tr]).items():
            t0 = time.time()
            if callable(m):
                s = m(X[va])
            else:
                m.fit(X[tr], y[tr])
                s = m.predict_proba(X[va])[:, 1]
            r = {"fold": k, "train_end": t_end, "val": f"{v_start}..{v_end}", "model": name,
                 "val_pos_banks": int(np.unique(cert[va & (y == 1)]).size)}
            r.update(metrics(y[va], s))
            r["sec"] = round(time.time() - t0, 1)
            rows.append(r)
            print({a: (round(b, 4) if isinstance(b, float) else b) for a, b in r.items()})
    res = pd.DataFrame(rows)
    res.to_csv(C.REPORT_DIR / "cv_baseline_results.csv", index=False)
    print("\n", res.pivot(index="model", columns="fold", values="pr_auc_lift").round(1).to_string())


if __name__ == "__main__":
    main()