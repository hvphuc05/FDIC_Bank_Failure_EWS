"""Shared modelling helpers (dev-only loader and the model zoo)."""
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C
from preprocess import make_preprocessor


def load_dev():
    df = (pl.scan_parquet(C.FEATURE_PATH).filter(pl.col("split") == "dev")
            .select(["CERT", "REPDTE", "y_fail_4q"] + C.FEATURE_COLS).collect())
    assert df["y_fail_4q"].null_count() == 0
    assert df["REPDTE"].max() <= dt.date.fromisoformat(C.DEV_END), "non-dev rows loaded"
    return df


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