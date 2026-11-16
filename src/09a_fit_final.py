"""Step 09a - fit the frozen final models on the WHOLE DEVELOPMENT PERIOD (rows up to 2006Q4).
DEV ONLY: the test rows are not read. Writes the model file, its SHA-256 and the freeze log."""
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
from modeling import load_dev, make_models

FREEZE_COMMIT = "508e235"
FROZEN_FILES = ["config.py", "src/07_features.py", "src/preprocess.py", "src/modeling.py", "src/08_model.py"]
MODEL_PATH = C.PROCESSED_DATA_DIR / "final_models.joblib"
FREEZE_LOG = C.REPORT_DIR / "final_model_freeze.json"


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


def main():
    df = load_dev()                                   # asserts split == dev and REPDTE <= 2006-12-31
    X = df.select(C.FEATURE_COLS).to_numpy().astype(np.float32)
    y = df["y_fail_4q"].to_numpy().astype(np.int8)
    assert len(y) == 1_193_818 and int(y.sum()) == 8_688, "development sample differs from the frozen split summary"
    models = make_models(X)
    logit = models["B1_logit"].fit(X, y)
    hgb = models["M1_hist_gbm"].fit(X, y)
    i = C.FEATURE_COLS.index("eq_asset")
    b0 = {"feature": "eq_asset", "index": i, "median_of_negated": float(np.nanmedian(-X[:, i]))}

    # B0 has no probabilities: its alarm threshold is the pooled out-of-fold optimum on DEV (policy P1)
    oof = pl.read_parquet(C.PROCESSED_DATA_DIR / "oof_dev.parquet")
    yo, so = oof["y_fail_4q"].to_numpy(), oof["B0_low_capital_rule"].to_numpy()
    b0["thresholds"] = {str(r): float(E.best_threshold(yo, so, r)) for r in E.R_GRID}

    joblib.dump({"B0": b0, "B1_logit": logit, "M1_hist_gbm": hgb, "feature_cols": list(C.FEATURE_COLS),
                 "fit_rows": int(len(y)), "fit_positives": int(y.sum()), "dev_end": C.DEV_END,
                 "sklearn": sklearn.__version__}, MODEL_PATH)

    pre, clf = logit.named_steps["prep"], logit.named_steps["clf"]
    w, imp, sc = pre.named_steps["winsor"], pre.named_steps["impute"], pre.named_steps["scale"]
    tab = pd.DataFrame({"feature": C.FEATURE_COLS, "coef_standardised": clf.coef_.ravel(), "winsor_lo": w.lo_,
                        "winsor_hi": w.hi_, "median_fill": imp.statistics_, "scale_mean": sc.mean_, "scale_sd": sc.scale_})
    tab = pd.concat([tab, pd.DataFrame([{"feature": "(intercept)", "coef_standardised": float(clf.intercept_[0])}])],
                    ignore_index=True)
    tab.to_csv(C.REPORT_DIR / "final_model_coefficients.csv", index=False)

    changed = git("diff", FREEZE_COMMIT, "--stat", "--", *FROZEN_FILES)
    log = {"timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
           "git_head": git("rev-parse", "HEAD"), "git_status_porcelain": git("status", "--porcelain"),
           "frozen_commit": FREEZE_COMMIT,
           "frozen_files_changed_since_freeze": None if changed is None else changed != "",
           "model_file": MODEL_PATH.name, "model_sha256": sha256(MODEL_PATH),
           "fit_rows": int(len(y)), "fit_positives": int(y.sum()), "test_rows_read": 0,
           "b0_thresholds": b0["thresholds"],
           "versions": {"python": sys.version.split()[0], "sklearn": sklearn.__version__, "numpy": np.__version__,
                        "pandas": pd.__version__, "polars": pl.__version__}}
    FREEZE_LOG.write_text(json.dumps(log, indent=2), encoding="utf-8")

    print("fitted on", len(y), "rows,", int(y.sum()), "positive rows | model sha256:", log["model_sha256"][:16], "...")
    print("frozen files changed since", FREEZE_COMMIT, ":", log["frozen_files_changed_since_freeze"])
    print("B0 thresholds (dev, pooled out-of-fold):", b0["thresholds"])
    print(tab.reindex(tab["coef_standardised"].abs().sort_values(ascending=False).index).head(10)
             [["feature", "coef_standardised"]].to_string(index=False))


if __name__ == "__main__":
    main()