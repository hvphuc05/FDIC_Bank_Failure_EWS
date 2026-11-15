"""Preprocessing pipeline. Every statistic is learned in fit(); call fit on DEV rows only."""
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class Winsorizer(BaseEstimator, TransformerMixin):
    """Clip each feature to [q_lo, q_hi] quantiles learned on the training rows."""
    def __init__(self, lower=0.005, upper=0.995):
        self.lower, self.upper = lower, upper

    def fit(self, X, y=None):
        X = np.asarray(X, dtype=np.float64)
        self.lo_ = np.nanquantile(X, self.lower, axis=0)
        self.hi_ = np.nanquantile(X, self.upper, axis=0)
        return self

    def transform(self, X):
        return np.clip(np.asarray(X, dtype=np.float32), self.lo_, self.hi_)  # NaN stays NaN


def make_preprocessor():
    return Pipeline([
        ("winsor", Winsorizer(0.005, 0.995)),
        ("impute", SimpleImputer(strategy="median", add_indicator=False)),
        ("scale", StandardScaler()),
    ])