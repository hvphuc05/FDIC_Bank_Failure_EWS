"""Numerical check that the Spark MLlib objective with regParam = 1/(C*N) (elasticNetParam = 0, standardization = False)
has the same minimiser as scikit-learn's LogisticRegression(C). Synthetic data; needs numpy, scipy and scikit-learn only.
Usage: python tools/check_mllib_equivalence.py"""
import numpy as np
from scipy.optimize import minimize
from sklearn.linear_model import LogisticRegression

rng = np.random.default_rng(3)
N, d, C = 20000, 6, 1.0
X = rng.normal(size=(N, d))
X = (X - X.mean(0)) / X.std(0)
w_true = np.array([1.5, -0.8, 0.3, 0.0, 0.7, -1.2])
y = (rng.random(N) < 1 / (1 + np.exp(-(X @ w_true - 4.0)))).astype(float)

sk = LogisticRegression(C=C, max_iter=1000, tol=1e-10).fit(X, y)
lam = 1.0 / (C * N)                                          # MLlib: (1/N) sum(loss) + (lam/2) ||w||^2, intercept free


def obj(theta):
    w, b = theta[:-1], theta[-1]
    z = X @ w + b
    return np.mean(np.logaddexp(0, z) - y * z) + 0.5 * lam * (w @ w)


def grad(theta):
    w, b = theta[:-1], theta[-1]
    r = 1 / (1 + np.exp(-(X @ w + b))) - y
    return np.r_[X.T @ r / N + lam * w, r.mean()]


res = minimize(obj, np.zeros(d + 1), jac=grad, method="L-BFGS-B", options={"maxiter": 2000, "ftol": 1e-15, "gtol": 1e-10})
dc, di = np.abs(res.x[:-1] - sk.coef_.ravel()).max(), abs(res.x[-1] - sk.intercept_[0])
print(f"max |coefficient difference| {dc:.2e} | |intercept difference| {di:.2e}")
assert dc < 1e-5 and di < 1e-5
print("EQUIVALENCE CONFIRMED: regParam = 1/(C*N)")