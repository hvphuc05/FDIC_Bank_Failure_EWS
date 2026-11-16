"""Step 09b - SINGLE evaluation of the frozen design on the test period (REPDTE 2008Q1 - 2025Q2).
Frozen design: logistic regression fitted once on the development period; alarm when p >= 1/(1+r), r in {10,20,50,100};
B0 (low-capital rule) and M1 (gradient boosting) are evaluated once for comparison.
  python src/09b_test_evaluation.py --dry-run   # same code path on pseudo-test = validation fold 3; no test row is read
  python src/09b_test_evaluation.py             # the single test evaluation (refuses to run twice)"""
import argparse
import datetime as dt
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import polars as pl
import sklearn

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C
import evalcore as E

LOG = C.REPORT_DIR / "test_evaluation_log.json"
MODEL_PATH = C.PROCESSED_DATA_DIR / "final_models.joblib"
FREEZE_LOG = C.REPORT_DIR / "final_model_freeze.json"
EXPECTED = {"rows": 435_446, "pos_rows": 2_134, "pos_banks": 545}
PERIODS = [("2008-2014", np.datetime64("2008-01-01"), np.datetime64("2014-12-31")),
           ("2015-2025", np.datetime64("2015-01-01"), np.datetime64("2025-12-31"))]
DRY_PERIODS = [("1993-1999", np.datetime64("1993-01-01"), np.datetime64("1999-12-31")),
               ("2000-2006", np.datetime64("2000-01-01"), np.datetime64("2006-12-31"))]
COLS = ["CERT", "REPDTE", "y_fail_4q", "y_assist_or_fail_4q"] + C.FEATURE_COLS


def git(*args):
    try:
        r = subprocess.run(["git", *args], capture_output=True, text=True, cwd=C.PROJECT_ROOT, timeout=60)
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def failure_days(df):
    """Days from REPDTE to the earliest FAILURE date inside the label window, for positive rows (NaN elsewhere).
    Outcome information, used only to define the pre-declared secondary subset."""
    fe = (pl.read_parquet(C.PROCESSED_DATA_DIR / "failure_events.parquet")
            .filter(pl.col("RESTYPE") == "FAILURE")
            .select(pl.col("cert_num").cast(pl.Int64).alias("CERT"), pl.col("fail_dt").cast(pl.Date))
            .drop_nulls())
    pos = (df.filter(pl.col("y_fail_4q") == 1).select("CERT", "REPDTE").join(fe, on="CERT")
             .filter((pl.col("fail_dt") > pl.col("REPDTE")) & (pl.col("fail_dt") <= pl.col("REPDTE").dt.offset_by("12mo")))
             .group_by(["CERT", "REPDTE"]).agg(pl.col("fail_dt").min())
             .with_columns((pl.col("fail_dt") - pl.col("REPDTE")).dt.total_days().alias("days"))
             .select("CERT", "REPDTE", "days"))
    base = df.select("CERT", "REPDTE").with_row_index("i")
    return base.join(pos, on=["CERT", "REPDTE"], how="left").sort("i")["days"].to_numpy().astype(float)


def arrays(df):
    return (df["y_fail_4q"].to_numpy().astype(int), df["y_assist_or_fail_4q"].to_numpy().astype(int),
            df["CERT"].to_numpy(), df["REPDTE"].to_numpy().astype("datetime64[D]"), failure_days(df))


def b0_score(X, b0):
    return np.where(np.isnan(X[:, b0["index"]]), b0["median_of_negated"], -X[:, b0["index"]])


def thresholds_for(b0_thr):
    return {(m, r): (1 / (1 + r) if m != "B0_low_capital_rule" else b0_thr[str(r)])
            for m in E.MODELS for r in E.R_GRID}


def show(res):
    pd.set_option("display.width", 250)
    pd.set_option("display.max_rows", 200)
    print("\n=== ranking metrics ===\n", res["metrics"].round(4).to_string(index=False))
    print("\n=== cost saving against never alerting (%), alarm policy: logit/boosting p >= 1/(1+r); rule: dev threshold ===\n",
          res["cost"].drop(columns=["beats_always_alert"]).round(3).to_string(index=False))
    print("\n=== paired differences (same bank resamples) ===\n", res["pairs"].round(4).to_string(index=False))
    print("\n=== pre-declared secondary evaluations ===\n", res["secondary"].round(4).to_string(index=False))
    print("\n=== calibration (descriptive) ===\n", res["calibration"].round(4).to_string(index=False))


def save(res, prefix):
    for k, v in res.items():
        v.to_csv(C.REPORT_DIR / f"{prefix}{k}.csv", index=False)


def dry_run(n_boot):
    from modeling import make_models
    df = (pl.scan_parquet(C.FEATURE_PATH).filter(pl.col("split") == "dev").select(COLS).collect())
    assert df["REPDTE"].max() <= dt.date.fromisoformat(C.DEV_END), "non-dev rows loaded"
    t_end, v_start, v_end = C.CV_FOLDS[2]
    D = dt.date.fromisoformat
    rep = df["REPDTE"]
    tr = (rep <= D(t_end)).to_numpy()
    va = ((rep >= D(v_start)) & (rep <= D(v_end))).to_numpy()
    X = df.select(C.FEATURE_COLS).to_numpy().astype(np.float32)
    y_all = df["y_fail_4q"].to_numpy().astype(np.int8)
    models = make_models(X[tr])
    models["B1_logit"].fit(X[tr], y_all[tr])
    models["M1_hist_gbm"].fit(X[tr], y_all[tr])
    i = C.FEATURE_COLS.index("eq_asset")
    b0 = {"index": i, "median_of_negated": float(np.nanmedian(-X[tr][:, i]))}
    ev = df.filter(pl.Series(va))
    Xv = X[va]
    scores = {"B0_low_capital_rule": b0_score(Xv, b0), "B1_logit": models["B1_logit"].predict_proba(Xv)[:, 1],
              "M1_hist_gbm": models["M1_hist_gbm"].predict_proba(Xv)[:, 1]}
    oof = pl.read_parquet(C.PROCESSED_DATA_DIR / "oof_dev.parquet")
    yo, so = oof["y_fail_4q"].to_numpy(), oof["B0_low_capital_rule"].to_numpy()
    b0_thr = {str(r): float(E.best_threshold(yo, so, r)) for r in E.R_GRID}
    y, y_alt, cert, rep_np, days = arrays(ev)
    res = E.evaluate_all(y, y_alt, cert, rep_np, days, scores, thresholds_for(b0_thr), DRY_PERIODS, n_boot=n_boot)
    save(res, "dryrun_")
    show(res)
    # the pseudo-test is validation fold 3: the numbers must reproduce those of 08b and 08d exactly
    ci = pd.read_csv(C.REPORT_DIR / "cv_bootstrap_ci.csv")
    pc = pd.read_csv(C.REPORT_DIR / "cv_policy_comparison.csv")
    for _, row in res["metrics"].iterrows():
        ref = ci[(ci["fold"] == 3) & (ci["model"] == row["model"])].iloc[0]
        assert abs(row["pr_auc"] - ref["pr_auc"]) < 1e-9 and abs(row["roc_auc"] - ref["roc_auc"]) < 1e-9, row["model"]
    for _, row in res["cost"].iterrows():
        if row["model"] == "B0_low_capital_rule":
            continue
        ref = pc[(pc["model"] == row["model"]) & (pc["policy"] == "P2_p_star") & (pc["r"] == row["r"])].iloc[0]["sav_f3"]
        assert abs(row["saving_vs_never_pct"] - ref) < 1e-6, (row["model"], row["r"])
    print("\nDRY RUN REPRODUCES THE FOLD-3 NUMBERS OF 08b AND 08d (PR-AUC, ROC-AUC, P2 savings)")


def test_run(n_boot, allow_rerun):
    if LOG.exists() and not allow_rerun:
        sys.exit(f"The test period has already been evaluated (see {LOG.name}). Re-running needs --allow-rerun and a documented reason.")
    freeze = json.loads(FREEZE_LOG.read_text(encoding="utf-8"))
    if sha256(MODEL_PATH) != freeze["model_sha256"]:
        sys.exit("The model file does not match the freeze log: refit with 09a only if the design is intentionally changed.")
    runs = json.loads(LOG.read_text(encoding="utf-8")).get("runs", []) if LOG.exists() else []
    entry = {"started_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
             "git_head": git("rev-parse", "HEAD"), "git_status_porcelain": git("status", "--porcelain"),
             "model_sha256": freeze["model_sha256"], "status": "started", "n_boot": n_boot, "seed": 42}
    LOG.write_text(json.dumps({"runs": runs + [entry]}, indent=2), encoding="utf-8")

    frozen = joblib.load(MODEL_PATH)
    assert frozen["feature_cols"] == list(C.FEATURE_COLS)
    df = pl.scan_parquet(C.FEATURE_PATH).filter(pl.col("split") == "test").select(COLS).collect()
    assert df.height == EXPECTED["rows"] and int(df["y_fail_4q"].sum()) == EXPECTED["pos_rows"], "test sample differs from the split summary"
    assert df["y_fail_4q"].null_count() == 0 and df["REPDTE"].min() >= dt.date(2008, 3, 31) and df["REPDTE"].max() <= dt.date(2025, 6, 30)
    X = df.select(C.FEATURE_COLS).to_numpy().astype(np.float32)
    scores = {"B0_low_capital_rule": b0_score(X, frozen["B0"]), "B1_logit": frozen["B1_logit"].predict_proba(X)[:, 1],
              "M1_hist_gbm": frozen["M1_hist_gbm"].predict_proba(X)[:, 1]}
    y, y_alt, cert, rep_np, days = arrays(df)
    assert len(np.unique(cert[y == 1])) == EXPECTED["pos_banks"]
    res = E.evaluate_all(y, y_alt, cert, rep_np, days, scores, thresholds_for(frozen["B0"]["thresholds"]), PERIODS, n_boot=n_boot)
    save(res, "test_")
    pd.DataFrame({"CERT": cert, "REPDTE": df["REPDTE"].to_list(), "y_fail_4q": y, **scores}).to_parquet(
        C.PROCESSED_DATA_DIR / "test_predictions.parquet", index=False)
    show(res)
    entry.update({"status": "completed", "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                  "test_rows": int(len(y)), "test_positive_rows": int(y.sum()),
                  "test_positive_banks": EXPECTED["pos_banks"],
                  "versions": {"python": sys.version.split()[0], "sklearn": sklearn.__version__, "numpy": np.__version__,
                               "pandas": pd.__version__, "polars": pl.__version__}})
    LOG.write_text(json.dumps({"runs": runs + [entry]}, indent=2), encoding="utf-8")
    print("\nTEST EVALUATION COMPLETED AND LOGGED IN", LOG.name)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--allow-rerun", action="store_true")
    ap.add_argument("--n-boot", type=int, default=200)
    a = ap.parse_args()
    dry_run(a.n_boot) if a.dry_run else test_run(a.n_boot, a.allow_rerun)