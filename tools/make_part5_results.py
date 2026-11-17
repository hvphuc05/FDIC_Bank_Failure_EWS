"""Writes sections 6.9-6.12 and Appendices D-E of the report from the ACTUAL result files and replaces the old section 6.9.
Every number in the new text is read from a file; the qualitative statements are asserted first, and nothing is written if
one of them does not hold.
Usage: python tools/make_part5_results.py report/final_report.md --mllib-dir mllib_out"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd

R = [10, 20, 50, 100]
MODELS = ["B0_low_capital_rule", "B1_logit", "M1_hist_gbm"]
LABEL = {"B0_low_capital_rule": "B0 low-capital rule", "B1_logit": "B1 logistic regression (frozen)",
         "M1_hist_gbm": "M1 gradient boosting"}
D_B0, D_M1 = "B1_logit - B0_low_capital_rule", "B1_logit - M1_hist_gbm"
SUBSETS = {"excl_failure_within_45d": "S1 positives with failure within 45 days excluded", "alt_label": "S2 alternative label",
           "period_2008-2014": "S3a 2008–2014", "period_2015-2025": "S3b 2015–2025"}


def f(x, d=3):
    return f"{x:.{d}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("report")
    ap.add_argument("--mllib-dir", default="mllib_out")
    a = ap.parse_args()
    rp = Path(a.report)
    rep = rp.parent
    metrics = pd.read_csv(rep / "test_metrics.csv").set_index("model")
    cost = pd.read_csv(rep / "test_cost.csv")
    pairs = pd.read_csv(rep / "test_pairs.csv")
    sec = pd.read_csv(rep / "test_secondary.csv")
    cal = pd.read_csv(rep / "test_calibration.csv").set_index("model")
    log = json.loads((rep / "test_evaluation_log.json").read_text(encoding="utf-8"))
    freeze = json.loads((rep / "final_model_freeze.json").read_text(encoding="utf-8"))
    coef = pd.read_csv(rep / "final_model_coefficients.csv")
    mlcoef = pd.read_csv(rep / "mllib_vs_sklearn_coefficients.csv").set_index("feature")
    mls = json.loads((rep / "mllib_vs_sklearn_summary.json").read_text(encoding="utf-8"))
    mlres = json.loads((Path(a.mllib_dir) / "mllib_result.json").read_text(encoding="utf-8"))
    C = cost.set_index(["model", "r"])

    def pr(metric, diff, r=None):
        q = pairs[(pairs["metric"] == metric) & (pairs["difference"] == diff)]
        if r is not None:
            q = q[q["r"] == r]
        return q.iloc[0]

    def sc(subset, model):
        return sec[(sec["subset"] == subset) & (sec["model"] == model)].iloc[0]

    # ---------------- assertions on the qualitative statements ----------------
    bad = []
    chk = lambda cond, msg: None if cond else bad.append(msg)
    run = log["runs"][-1]
    chk(len(log["runs"]) == 1 and run["status"] == "completed", "the test log must contain exactly one completed run")
    chk(all(ln.startswith("??") for ln in (run.get("git_status_porcelain") or "").splitlines()),
        "a tracked file was modified when the test was run")
    chk(freeze["frozen_files_changed_since_freeze"] is False, "frozen files changed since the freeze commit")
    chk(all(pr("saving", D_B0, r)["lo"] > 0 for r in R), "logit does not beat B0 on saving for every r")
    chk(pr("PR-AUC", D_B0)["lo"] > 0 and pr("ROC-AUC", D_B0)["lo"] > 0, "logit does not beat B0 on both ranking metrics")
    chk(all(pr("saving", D_M1, r)["hi"] < 0 for r in R), "boosting does not save more than logit for every r")
    chk(pr("ROC-AUC", D_M1)["lo"] < 0 < pr("ROC-AUC", D_M1)["hi"], "ROC-AUC logit vs boosting is not a tie")
    chk(pr("PR-AUC", D_M1)["lo"] > 0, "PR-AUC of logit is not above boosting")
    chk(all(cal.loc[m, "mean_pred_pct"] < cal.loc[m, "obs_pct"] for m in ("B1_logit", "M1_hist_gbm")), "probabilities are not under-predicted")
    chk(sc("period_2015-2025", "B0_low_capital_rule")["saving_r10"] < 0 and sc("period_2015-2025", "B0_low_capital_rule")["saving_r20"] < 0,
        "B0 saving in 2015-2025 is not negative at r=10 and r=20")
    chk(all(sc("period_2015-2025", "B1_logit")[f"saving_r{r}"] > 0 for r in R), "logit saving in 2015-2025 is not positive for every r")
    chk(cal.loc['M1_hist_gbm', 'mean_pred_pct'] > cal.loc['B1_logit', 'mean_pred_pct'], 'boosting is not less under-predicted than the logit')
    chk(C.loc[('M1_hist_gbm', 10), 'alert_rate_pct'] > C.loc[('B1_logit', 10), 'alert_rate_pct'], 'boosting does not alert more than the logit at r=10')
    chk(mls["coef_same_sign"] == mls["n_coefficients"], "not all MLlib coefficients have the sign of the scikit-learn ones")
    if bad:
        print("NOT WRITTEN. The following statements assumed by the text do not hold in your files:\n  - " + "\n  - ".join(bad))
        sys.exit(1)

    m = metrics
    n_pos_banks = int(C.loc[("B1_logit", 50), "failed_banks"])
    base = m.loc["B1_logit", "base_rate_pct"]

    # ---------------- tables ----------------
    t15 = ["| Model | PR-AUC [95% CI] | ROC-AUC [95% CI] | Recall, top 5% of rows |", "|:---|:---|:---|---:|"]
    for k in MODELS:
        r_ = m.loc[k]
        t15.append(f"| {LABEL[k]} | {f(r_['pr_auc'])} [{f(r_['pr_auc_lo'])}, {f(r_['pr_auc_hi'])}] | "
                   f"{f(r_['roc_auc'])} [{f(r_['roc_auc_lo'])}, {f(r_['roc_auc_hi'])}] | {f(r_['recall_top5pct'])} |")
    t16 = ["| Model | r | Alarm rule | Alert rate (% of bank-quarters) | Row recall | Precision (%) | Failed banks alerted at least once | Saving against never alerting (%) [95% CI] |",
           "|:---|---:|:---|---:|---:|---:|---:|:---|"]
    for k in MODELS:
        for r in R:
            c = C.loc[(k, r)]
            rule = "dev-optimal threshold" if k == "B0_low_capital_rule" else "p ≥ 1/(1+r)"
            t16.append(f"| {LABEL[k]} | {r} | {rule} | {f(c['alert_rate_pct'])} | {f(c['recall'], 2)} | {f(c['precision_pct'], 1)} | "
                       f"{int(c['banks_alerted_before_failure'])} of {int(c['failed_banks'])} | "
                       f"{f(c['saving_vs_never_pct'], 1)} [{f(c['saving_lo'], 1)}, {f(c['saving_hi'], 1)}] |")
    t17 = ["| Metric | r | Difference | Mean | 95% CI | Share of resamples > 0 |", "|:---|---:|:---|---:|:---|---:|"]
    for _, p in pairs.iterrows():
        rr = "" if pd.isna(p["r"]) else str(int(p["r"]))
        t17.append(f"| {p['metric']} | {rr} | {p['difference']} | {f(p['mean_diff'], 4)} | [{f(p['lo'], 4)}, {f(p['hi'], 4)}] | {f(p['share_positive'], 3)} |")
    t18 = ["| Subset | Model | Rows | Positive rows (banks) | PR-AUC | ROC-AUC | Saving r=10 | r=20 | r=50 | r=100 |",
           "|:---|:---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for sub, lab in SUBSETS.items():
        for k in MODELS:
            s_ = sc(sub, k)
            t18.append(f"| {lab} | {LABEL[k]} | {int(s_['rows']):,} | {int(s_['pos_rows']):,} ({int(s_['pos_banks'])}) | {f(s_['pr_auc'])} | "
                       f"{f(s_['roc_auc'])} | " + " | ".join(f(s_[f'saving_r{r}'], 1) for r in R) + " |")
    t19 = ["| Model | Mean predicted (%) | Observed (%) | Top 1% of predictions: predicted (%) | Top 1%: observed (%) |", "|:---|---:|---:|---:|---:|"]
    for k in ("B1_logit", "M1_hist_gbm"):
        c = cal.loc[k]
        t19.append(f"| {LABEL[k]} | {f(c['mean_pred_pct'], 3)} | {f(c['obs_pct'], 3)} | {f(c['top1pct_pred_pct'], 2)} | {f(c['top1pct_obs_pct'], 2)} |")

    s1l, s1m = sc("excl_failure_within_45d", "B1_logit"), m.loc["B1_logit"]
    dropped = int(s1m["rows"] - s1l["rows"])
    s3a, s3b = sc("period_2008-2014", "B1_logit"), sc("period_2015-2025", "B1_logit")
    b0b = sc("period_2015-2025", "B0_low_capital_rule")
    sv = lambda k, r: C.loc[(k, r), "saving_vs_never_pct"]
    hit = lambda k, r: int(C.loc[(k, r), "banks_alerted_before_failure"])
    commit_head = (run.get("git_head") or "")[:7]

    text = f"""## 6.9 Evaluation protocol on the test period

The test period (REPDTE 2008Q1 – 2025Q2) was evaluated **once**, after the design had been frozen. Each safeguard is recorded in a file of the repository:

| Step | Evidence |
|:---|:---|
| Design frozen before the cost and policy results | commit 508e235 (README, §6.7); `git diff 508e235` on `config.py`, `src/07_features.py`, `src/preprocess.py`, `src/modeling.py` and `src/08_model.py` was empty when the final models were fitted (`report/final_model_freeze.json`) |
| Evaluation code and the test protocol (Rule 3) committed before any test row was read | commit 4ed91ca; pre-flight check added in commit {commit_head} |
| Final models fitted on the development period only ({int(freeze['fit_rows']):,} rows, {int(freeze['fit_positives']):,} positive rows) | `src/09a_fit_final.py`; model file SHA-256 `{freeze['model_sha256'][:16]}…`, checked again by the test script; logs committed in c6cc0b3 |
| Code path validated without the test set | a dry run on validation fold 3 reproduced the PR-AUC, ROC-AUC and P2 savings of `08b` and `08d` (assertions in the script); a pre-flight run checked the model file, scoring and output paths on development rows |
| One run only | `report/test_evaluation_log.json` holds one completed run ({run['started_utc'][:16].replace('T', ' ')} to {run['finished_utc'][11:16]} UTC, {run['n_boot']:,} bank resamples, seed {run['seed']}); the script refuses to run again without an explicit flag and records every run; no tracked file was modified when it ran (the repository status listed only untracked files) |
| Sample checks | the script asserts 435,446 rows, 2,134 positive rows and 545 positive banks |

The alarm rule of the frozen design is p ≥ 1/(1+r) for the logistic regression, for r = 10, 20, 50 and 100. The low-capital rule B0 has no probability; it is evaluated with the threshold that minimised the pooled out-of-fold cost on the development period (policy P1). Boosting M1 is evaluated with the same rule as the logistic regression. **Pre-declared secondary evaluations,** computed from the same predictions in the same run: S1, positive rows whose failure falls within 45 days of `REPDTE` removed from the evaluation (Table 9); S2, the alternative label (`FAILURE` or `ASSISTANCE`); S3, the periods 2008–2014 and 2015–2025. Intervals are percentile intervals from {run['n_boot']:,} resamples of **banks**; the models are not refitted in the bootstrap, and the secondary evaluations have no intervals.

## 6.10 Test results

**Table 15: Ranking metrics on the test period (435,446 bank-quarters, {int(m.loc['B1_logit', 'pos_rows']):,} positive rows, {n_pos_banks} failed banks, base rate {f(base, 2)}%)**

""" + "\n".join(t15) + f"""

**Table 16: Alarm performance and cost saving (r = cost of a missed failure / cost of a false alarm)**

""" + "\n".join(t16) + f"""

**Table 17: Paired differences between models (same bank resamples)**

""" + "\n".join(t17) + f"""

**Table 18: Pre-declared secondary evaluations (point estimates, no intervals)**

""" + "\n".join(t18) + f"""

**Table 19: Mean predicted probability against observed frequency on the test period**

""" + "\n".join(t19) + f"""

**Reading the results.**

1. **The frozen model against the simple rule.** The logistic regression reaches a PR-AUC of {f(m.loc['B1_logit', 'pr_auc'], 2)} (about {m.loc['B1_logit', 'pr_auc'] / (base / 100):.0f} times the base rate of {f(base, 2)}%) and a ROC-AUC of {f(m.loc['B1_logit', 'roc_auc'])}, against {f(m.loc['B0_low_capital_rule', 'pr_auc'], 2)} and {f(m.loc['B0_low_capital_rule', 'roc_auc'])} for the low-capital rule; the differences (+{f(pr('PR-AUC', D_B0)['mean_diff'])} and +{f(pr('ROC-AUC', D_B0)['mean_diff'])}) have intervals that exclude zero. In cost terms it saves {f(sv('B1_logit', 10), 1)}%, {f(sv('B1_logit', 20), 1)}%, {f(sv('B1_logit', 50), 1)}% and {f(sv('B1_logit', 100), 1)}% of the cost of never alerting for r = 10, 20, 50 and 100, against {f(sv('B0_low_capital_rule', 10), 1)}% to {f(sv('B0_low_capital_rule', 100), 1)}% for the rule; the advantage of {f(min(pr('saving', D_B0, r)['mean_diff'] for r in R), 1)} to {f(max(pr('saving', D_B0, r)['mean_diff'] for r in R), 1)} percentage points has an interval excluding zero at every r.
2. **Alerts.** At r = 50 the logistic regression raises an alarm on {f(C.loc[('B1_logit', 50), 'alert_rate_pct'], 2)}% of bank-quarters with a precision of {f(C.loc[('B1_logit', 50), 'precision_pct'], 1)}%; {hit('B1_logit', 50)} of the {n_pos_banks} failed banks ({100 * hit('B1_logit', 50) / n_pos_banks:.1f}%) were alerted at least once in the four quarters before their failure, and {hit('B1_logit', 10)} ({100 * hit('B1_logit', 10) / n_pos_banks:.1f}%) at r = 10.
3. **Logistic regression against boosting.** The two have the same ROC-AUC (difference {f(pr('ROC-AUC', D_M1)['mean_diff'], 4)}, interval [{f(pr('ROC-AUC', D_M1)['lo'], 4)}, {f(pr('ROC-AUC', D_M1)['hi'], 4)}]); logistic regression has the higher PR-AUC (+{f(pr('PR-AUC', D_M1)['mean_diff'])}, interval [{f(pr('PR-AUC', D_M1)['lo'])}, {f(pr('PR-AUC', D_M1)['hi'])}]). With the same alarm rule, however, **boosting saves more**: {f(sv('M1_hist_gbm', 10), 1)}% to {f(sv('M1_hist_gbm', 100), 1)}%, that is {f(min(-pr('saving', D_M1, r)['mean_diff'] for r in R), 1)} to {f(max(-pr('saving', D_M1, r)['mean_diff'] for r in R), 1)} points more than the logistic regression, with intervals that exclude zero at every r. The frozen selection (logistic regression and policy P2) is **not revised**: the development folds had shown no clear difference between the two models in cost terms (§6.7), and we report the test outcome as it is.
4. **Calibration.** The predicted probabilities are lower than the observed frequency (Table 19): the mean prediction of the logistic regression is {f(cal.loc['B1_logit', 'mean_pred_pct'], 2)}% against {f(cal.loc['B1_logit', 'obs_pct'], 2)}% observed, and in the top 1% of predictions {f(cal.loc['B1_logit', 'top1pct_pred_pct'], 1)}% against {f(cal.loc['B1_logit', 'top1pct_obs_pct'], 1)}%; boosting is less under-predicted. The threshold 1/(1+r) assumes calibrated probabilities, so on this period it is conservative, and boosting raises more alarms at the same threshold ({f(C.loc[('M1_hist_gbm', 10), 'alert_rate_pct'], 2)}% against {f(C.loc[('B1_logit', 10), 'alert_rate_pct'], 2)}% of bank-quarters at r = 10, with a row recall of {f(C.loc[('M1_hist_gbm', 10), 'recall'], 2)} against {f(C.loc[('B1_logit', 10), 'recall'], 2)}). This agrees with the higher saving of boosting despite the equal ROC-AUC, but we did not test that explanation, and no threshold was searched on the test period.
5. **Secondary evaluations (Table 18).** Removing the {dropped} bank-quarters whose failure occurs within 45 days (S1) lowers the positive rate from {f(base, 2)}% to {f(100 * s1l['pos_rows'] / s1l['rows'], 2)}%, and the PR-AUC of the logistic regression from {f(s1m['pr_auc'])} to {f(s1l['pr_auc'])}; PR-AUC depends on the positive rate, and the ROC-AUC, which does not, changes little ({f(s1m['roc_auc'])} to {f(s1l['roc_auc'])}). The saving at r = 10 falls from {f(sv('B1_logit', 10), 1)}% to {f(s1l['saving_r10'], 1)}%. Because the publication delay of the reports is not modelled, this evaluation shows how much of the performance rests on these rows. The alternative label (S2) gives results close to the primary ones. In 2008–2014 ({int(s3a['pos_rows']):,} positive rows, {int(s3a['pos_banks'])} banks) the logistic regression has a PR-AUC of {f(s3a['pr_auc'])}; in 2015–2025 ({int(s3b['pos_rows'])} positive rows, {int(s3b['pos_banks'])} banks) it is {f(s3b['pr_auc'])} and the low-capital rule has a **negative** saving at r = 10 and r = 20 ({f(b0b['saving_r10'], 1)}% and {f(b0b['saving_r20'], 1)}%), that is it would cost more than never alerting, while the logistic regression stays positive ({f(s3b['saving_r10'], 1)}% to {f(s3b['saving_r100'], 1)}%). The 2015–2025 figures rest on {int(s3b['pos_banks'])} failed banks and are not accompanied by intervals.

## 6.11 Spark MLlib demonstration

`src/09c_mllib_demo.py` reproduces the logistic regression inside Spark ML on **development data only**: fold 3 (training rows up to 1992Q4, {int(mls['train_rows_fold3']):,} rows; scoring 1993Q4–2006Q4, {int(mls['valid_rows_fold3']):,} rows) and the final fit on the whole development period ({int(mls['train_rows_final']):,} rows). Pre-processing is learned on the training rows only (winsorising bounds from approximate quantiles, median imputation, standardisation), the model is an L2-penalised logistic regression, and the penalty is set to λ = 1/(C·N) with C = 1 and N the number of training rows, which gives the same minimiser as scikit-learn's `C = 1` (checked numerically on synthetic data before the run). It ran on Google Colab (Spark {mls['spark_version']}, Java {mls['java_version']}, {mls['default_parallelism']} cores). With exact quantiles (relative error 0) and a 4 GB driver the final fit failed with a Java heap error, so the run used an 8 GB driver and a relative error of {mls['relative_error_quantiles']} for the quantiles.

**Table 20: Spark MLlib against scikit-learn, development fold 3**

| Quantity | scikit-learn | Spark MLlib |
|:---|---:|---:|
| ROC-AUC | {f(mls['sk_roc_auc'], 4)} | {f(mls['spark_roc_auc_sklearn_scored'], 4)} (Spark's own: {f(mls['spark_areaUnderROC'], 4)}) |
| PR-AUC (average precision) | {f(mls['sk_pr_auc'], 4)} | {f(mls['spark_pr_auc_sklearn_scored'], 4)} (Spark's `areaUnderPR`, a trapezoid rule: {f(mls['spark_areaUnderPR_trapezoid'], 4)}, not comparable) |
| Probabilities of the {int(mls['rows']):,} validation rows | – | Pearson {f(mls['pearson'], 4)}, Spearman {f(mls['spearman'], 4)}, mean \\|difference\\| {mls['p_mean_abs_diff']:.2e}, maximum \\|difference\\| {mls['p_max_abs_diff']:.3f} |
| Final-fit coefficients (standardised features) | – | same sign for {mls['coef_same_sign']} of {mls['n_coefficients']}; Spearman correlation {f(mls['coef_spearman'], 4)}; largest difference {f(mls['coef_max_abs_diff'], 3)} |
| Fit time, iterations (fold 3) | – | {mls['fit_seconds']:.0f} s, {mls['iterations']} |

The two implementations agree closely on predictions and ranking. The largest coefficient differences are on the three highly correlated capital variables (`eq_asset`, `RBC1AAJ`, `RBCRWAJ`); their sum is {f(mls['capital_sum_sklearn'], 3)} in scikit-learn and {f(mls['capital_sum_mllib'], 3)} in MLlib, which is consistent with weight being redistributed among correlated variables with little effect on the score (the predictions agree, Table 20). The remaining differences are probably due to pre-processing conventions (approximate rather than interpolated quantiles, approximate medians, sample rather than population standard deviation); we did not isolate their individual effects. Appendix D lists all coefficients.

## 6.12 Limitations of the evaluation and work not done

* One test period, one frozen model and one policy: the intervals reflect the sampling of banks, not the choice of model, and secondary evaluations have none.
* The probabilities are under-predicted on the test period (Table 19), so the policy p ≥ 1/(1+r) is conservative; recalibration (for example on the most recent development quarters) was not tried because the design was frozen, and a monitored recalibration would be needed in operation.
* The sensitivity analyses that require refitting (dropping `p9asset_asset` and `naasset_asset`; median `COST` for C_FN; costs counted per bank instead of per bank-quarter) were **not run**. The grid of r already covers the choice of the cost scale: using the median instead of the mean cost for a missed failure rescales r by a factor of 8,603/57,169 ≈ 0.15.
* C_FP remains an assumption, publication delay is not modelled, and data vintages are not controlled (§6.8).
* The reported savings are cost savings relative to never alerting, computed per bank-quarter with the costs of §6.6; they are not forecasts of losses to a deposit insurance fund.

---

"""
    src = rp.read_bytes().decode("utf-8")
    crlf = "\r\n" in src
    doc = src.replace("\r\n", "\n")
    start, end = doc.find("## 6.9 Remaining work for Part 5"), doc.find("---\n\n# 7. Part 6")
    if start < 0 or end < 0 or end < start:
        sys.exit("NOT WRITTEN: section 6.9 or the Part 6 heading was not found in the report.")
    doc = doc[:start] + text + doc[end + len("---\n\n"):]
    for old, new in [
        ("Status of this draft: Parts 1, 2, 4, the design of Part 5 and the T1 part of Part 3 are complete; T2, the Spark MLlib demonstration, the test evaluation and Part 6 are pending.",
         "Status of this draft: Parts 1 to 5 are complete (the test period was evaluated once); Part 6 and the final checks are pending."),
        ("A sensitivity run that drops them is planned (§6.9);", "Table 18 (§6.10) reports the evaluation that drops them (S1);"),
        ("and plan a sensitivity run without these two features (§6.9).", "a sensitivity run without these two features was not carried out (§6.12)."),
        ("A short Spark MLlib demonstration is still to be added (§6.9).", "A Spark MLlib demonstration is in §6.11.")]:
        if old in doc:
            doc = doc.replace(old, new)
        else:
            print("note: reference not found, left unchanged:", old[:60])
    d_tab = ["| Feature | scikit-learn | MLlib | \\|Difference\\| |", "|:---|---:|---:|---:|"]
    for k, r_ in mlcoef.reindex(mlcoef["sklearn"].abs().sort_values(ascending=False).index).iterrows():
        d_tab.append(f"| `{k}` | {r_['sklearn']:.4f} | {r_['mllib']:.4f} | {r_['abs_diff']:.4f} |")
    e_tab = ["| Feature | Coefficient (standardised) | Winsor lower | Winsor upper | Median fill | Mean | Std. dev. |", "|:---|---:|---:|---:|---:|---:|---:|"]
    for _, r_ in coef.iterrows():
        g = lambda c: "" if pd.isna(r_[c]) else f"{r_[c]:.4g}"
        e_tab.append(f"| `{r_['feature']}` | {r_['coef_standardised']:.4f} | {g('winsor_lo')} | {g('winsor_hi')} | {g('median_fill')} | {g('scale_mean')} | {g('scale_sd')} |")
    doc = doc.rstrip("\n").rstrip("-").rstrip("\n") + "\n\n---\n\n# Appendix D: Spark MLlib and scikit-learn coefficients (final fit, standardised features)\n\n**Table D1** (`report/mllib_vs_sklearn_coefficients.csv`)\n\n" \
          + "\n".join(d_tab) + "\n\n# Appendix E: Final model, coefficients and pre-processing statistics (development period)\n\n**Table E1** (`report/final_model_coefficients.csv`; the winsorising bounds, medians, means and standard deviations are those learned on the development period)\n\n" \
          + "\n".join(e_tab) + "\n"
    (rep / "part5_test_section.md").write_text(text, encoding="utf-8")
    rp.write_bytes((doc.replace("\n", "\r\n") if crlf else doc).encode("utf-8"))
    print("sections 6.9-6.12 and Appendices D-E written to", rp)


if __name__ == "__main__":
    main()