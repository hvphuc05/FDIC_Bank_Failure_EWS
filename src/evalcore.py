"""Evaluation core (numpy / pandas / scikit-learn only): ranking metrics, cost saving, bank-level recall,
bank-cluster bootstrap and the pre-declared secondary evaluations. The functions at_threshold and best_threshold are
identical to those of 08c_calibration_cost.py."""
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

R_GRID = [10, 20, 50, 100]
MODELS = ["B0_low_capital_rule", "B1_logit", "M1_hist_gbm"]
ALERT_RATE = 0.05


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
    idx = np.r_[np.flatnonzero(np.diff(ss) != 0), len(ss) - 1]
    cost = r * (int(ys.sum()) - tp[idx]) + fp[idx]
    j = int(np.argmin(cost))
    return ss[idx[j]] if cost[j] < r * int(ys.sum()) else np.inf


def rank_metrics(y, s):
    thr = np.quantile(s, 1 - ALERT_RATE)
    return {"pr_auc": average_precision_score(y, s), "roc_auc": roc_auc_score(y, s),
            "recall_top5pct": y[s >= thr].sum() / y.sum()}


def bank_recall(y, s, thr, bidx):
    pos = y == 1
    n_pos = len(np.unique(bidx[pos]))
    hit = len(np.unique(bidx[pos & (s >= thr)]))
    return hit, n_pos


def bootstrap(y, bidx, n_banks, scores, thresholds, n_boot, seed):
    """Bank-cluster bootstrap. AP and AUC via sample weights; savings via per-bank aggregates (thresholds fixed)."""
    rng = np.random.default_rng(seed)
    yf = (y == 1).astype(float)
    agg = {}
    for (m, r), thr in thresholds.items():
        a = scores[m] >= thr
        agg[(m, r)] = (np.bincount(bidx, weights=(a & (y == 1)).astype(float), minlength=n_banks),
                       np.bincount(bidx, weights=(a & (y == 0)).astype(float), minlength=n_banks))
    p_b = np.bincount(bidx, weights=yf, minlength=n_banks)
    out = {}
    for _ in range(n_boot):
        cnt = np.bincount(rng.integers(0, n_banks, n_banks), minlength=n_banks)
        w = cnt[bidx]
        keep = w > 0
        yk, wk = y[keep], w[keep]
        P = cnt @ p_b
        for m, s in scores.items():
            out.setdefault(("ap", m), []).append(average_precision_score(yk, s[keep], sample_weight=wk))
            out.setdefault(("auc", m), []).append(roc_auc_score(yk, s[keep], sample_weight=wk))
        for (m, r), (tp_b, fp_b) in agg.items():
            tp, fp = cnt @ tp_b, cnt @ fp_b
            out.setdefault(("sav", m, r), []).append(100 * (1 - (r * (P - tp) + fp) / (r * P)))
    return {k: np.array(v) for k, v in out.items()}


def ci(a):
    return float(np.percentile(a, 2.5)), float(np.percentile(a, 97.5))


def evaluate_all(y, y_alt, cert, rep, days, scores, thresholds, periods, n_boot=200, seed=42):
    """y, y_alt: 0/1 arrays; cert: bank ids; rep: datetime64[D] array; days: days from REPDTE to the failure for
    positive rows (NaN elsewhere); scores: model -> score array; thresholds: (model, r) -> threshold;
    periods: list of (name, start, end) with numpy datetime64 bounds (inclusive)."""
    banks, bidx = np.unique(cert, return_inverse=True)
    nb = len(banks)
    draws = bootstrap(y, bidx, nb, scores, thresholds, n_boot, seed) if n_boot else {}

    metrics, cost = [], []
    for m in MODELS:
        d = rank_metrics(y, scores[m])
        row = {"model": m, "rows": len(y), "pos_rows": int(y.sum()), "pos_banks": int(len(np.unique(bidx[y == 1]))),
               "base_rate_pct": 100 * y.mean(), **d}
        if n_boot:
            row["pr_auc_lo"], row["pr_auc_hi"] = ci(draws[("ap", m)])
            row["roc_auc_lo"], row["roc_auc_hi"] = ci(draws[("auc", m)])
        metrics.append(row)
        for r in R_GRID:
            thr = thresholds[(m, r)]
            c = {"model": m, "r": r, **at_threshold(y, scores[m], thr, r)}
            hit, n_pos = bank_recall(y, scores[m], thr, bidx)
            c["banks_alerted_before_failure"], c["failed_banks"] = hit, n_pos
            if n_boot:
                c["saving_lo"], c["saving_hi"] = ci(draws[("sav", m, r)])
            cost.append(c)

    pairs = []
    if n_boot:
        for a, b in (("B1_logit", "B0_low_capital_rule"), ("B1_logit", "M1_hist_gbm")):
            for key, label in (("ap", "PR-AUC"), ("auc", "ROC-AUC")):
                d = draws[(key, a)] - draws[(key, b)]
                pairs.append({"metric": label, "r": None, "difference": f"{a} - {b}", "mean_diff": d.mean(),
                              "lo": ci(d)[0], "hi": ci(d)[1], "share_positive": float((d > 0).mean())})
            for r in R_GRID:
                d = draws[("sav", a, r)] - draws[("sav", b, r)]
                pairs.append({"metric": "saving", "r": r, "difference": f"{a} - {b}", "mean_diff": d.mean(),
                              "lo": ci(d)[0], "hi": ci(d)[1], "share_positive": float((d > 0).mean())})

    subsets = {"excl_failure_within_45d": (y, ~((y == 1) & (days <= 45))), "alt_label": (y_alt, np.ones(len(y), bool))}
    for name, lo_, hi_ in periods:
        subsets[f"period_{name}"] = (y, (rep >= lo_) & (rep <= hi_))
    secondary = []
    for tag, (yy, mask) in subsets.items():
        ys = yy[mask]
        pb = bidx[mask]
        for m in MODELS:
            s = scores[m][mask]
            row = {"subset": tag, "model": m, "rows": int(mask.sum()), "pos_rows": int(ys.sum()),
                   "pos_banks": int(len(np.unique(pb[ys == 1])))}
            if 0 < ys.sum() < len(ys):
                row.update(pr_auc=average_precision_score(ys, s), roc_auc=roc_auc_score(ys, s))
                for r in R_GRID:
                    row[f"saving_r{r}"] = at_threshold(ys, s, thresholds[(m, r)], r)["saving_vs_never_pct"]
            secondary.append(row)

    calib = []
    for m in ("B1_logit", "M1_hist_gbm"):
        s = scores[m]
        pos = np.empty(len(s))
        pos[np.argsort(s, kind="mergesort")] = np.arange(len(s)) / len(s)
        top = pos >= 0.99
        calib.append({"model": m, "mean_pred_pct": 100 * s.mean(), "obs_pct": 100 * y.mean(),
                      "top1pct_pred_pct": 100 * s[top].mean(), "top1pct_obs_pct": 100 * y[top].mean()})
    return {"metrics": pd.DataFrame(metrics), "cost": pd.DataFrame(cost), "pairs": pd.DataFrame(pairs),
            "secondary": pd.DataFrame(secondary), "calibration": pd.DataFrame(calib)}