import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as C
from preprocess import make_preprocessor

df = pl.read_parquet(C.FEATURE_PATH, columns=["split"] + C.FEATURE_COLS)
dev = df.filter(pl.col("split") == "dev").select(C.FEATURE_COLS).to_numpy()
test = df.filter(pl.col("split") == "test").select(C.FEATURE_COLS).to_numpy()
both = np.vstack([dev, test])

pre = make_preprocessor().fit(dev)                       # fit on dev only
w, imp = pre.named_steps["winsor"], pre.named_steps["impute"]

# 1) the learned statistics must equal the ones computed from dev alone
assert np.allclose(w.hi_, np.nanquantile(dev.astype(np.float64), 0.995, axis=0), equal_nan=True)
assert np.allclose(imp.statistics_, np.nanmedian(np.clip(dev, w.lo_, w.hi_), axis=0), rtol=1e-4)

# 2) and they must DIFFER from statistics that would use test rows (otherwise the check proves nothing)
hi_both = np.nanquantile(both.astype(np.float64), 0.995, axis=0)
n_diff = int((~np.isclose(w.hi_, hi_both, rtol=1e-6)).sum())
print(f"features whose 99.5% cut-off changes if test rows leak in: {n_diff} / {len(C.FEATURE_COLS)}")
assert n_diff > 0

# 3) transform of test needs no refit
xt = pre.transform(test)
print("test transformed:", xt.shape, "| NaN left:", int(np.isnan(xt).sum()))
print("LEAK CHECK PASSED")