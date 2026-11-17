## 6.9 Evaluation protocol on the test period

The test period (REPDTE 2008Q1 – 2025Q2) was evaluated **once**, after the design had been frozen. Each safeguard is recorded in a file of the repository:

| Step | Evidence |
|:---|:---|
| Design frozen before the cost and policy results | commit 508e235 (README, §6.7); `git diff 508e235` on `config.py`, `src/07_features.py`, `src/preprocess.py`, `src/modeling.py` and `src/08_model.py` was empty when the final models were fitted (`report/final_model_freeze.json`) |
| Evaluation code and the test protocol (Rule 3) committed before any test row was read | commit 4ed91ca; pre-flight check added in commit 9e33c74 |
| Final models fitted on the development period only (1,193,818 rows, 8,688 positive rows) | `src/09a_fit_final.py`; model file SHA-256 `b3522259c1fa8a9c…`, checked again by the test script; logs committed in c6cc0b3 |
| Code path validated without the test set | a dry run on validation fold 3 reproduced the PR-AUC, ROC-AUC and P2 savings of `08b` and `08d` (assertions in the script); a pre-flight run checked the model file, scoring and output paths on development rows |
| One run only | `report/test_evaluation_log.json` holds one completed run (2026-11-17 00:54 to 01:03 UTC, 1,000 bank resamples, seed 42); the script refuses to run again without an explicit flag and records every run; no tracked file was modified when it ran (the repository status listed only untracked files) |
| Sample checks | the script asserts 435,446 rows, 2,134 positive rows and 545 positive banks |

The alarm rule of the frozen design is p ≥ 1/(1+r) for the logistic regression, for r = 10, 20, 50 and 100. The low-capital rule B0 has no probability; it is evaluated with the threshold that minimised the pooled out-of-fold cost on the development period (policy P1). Boosting M1 is evaluated with the same rule as the logistic regression. **Pre-declared secondary evaluations,** computed from the same predictions in the same run: S1, positive rows whose failure falls within 45 days of `REPDTE` removed from the evaluation (Table 9); S2, the alternative label (`FAILURE` or `ASSISTANCE`); S3, the periods 2008–2014 and 2015–2025. Intervals are percentile intervals from 1,000 resamples of **banks**; the models are not refitted in the bootstrap, and the secondary evaluations have no intervals.

## 6.10 Test results

**Table 15: Ranking metrics on the test period (435,446 bank-quarters, 2,134 positive rows, 545 failed banks, base rate 0.49%)**

| Model | PR-AUC [95% CI] | ROC-AUC [95% CI] | Recall, top 5% of rows |
|:---|:---|:---|---:|
| B0 low-capital rule | 0.281 [0.248, 0.318] | 0.953 [0.944, 0.960] | 0.829 |
| B1 logistic regression (frozen) | 0.605 [0.568, 0.640] | 0.984 [0.978, 0.989] | 0.950 |
| M1 gradient boosting | 0.582 [0.541, 0.617] | 0.985 [0.980, 0.989] | 0.942 |

**Table 16: Alarm performance and cost saving (r = cost of a missed failure / cost of a false alarm)**

| Model | r | Alarm rule | Alert rate (% of bank-quarters) | Row recall | Precision (%) | Failed banks alerted at least once | Saving against never alerting (%) [95% CI] |
|:---|---:|:---|---:|---:|---:|---:|:---|
| B0 low-capital rule | 10 | dev-optimal threshold | 0.629 | 0.46 | 35.9 | 441 of 545 | 37.9 [34.9, 40.6] |
| B0 low-capital rule | 20 | dev-optimal threshold | 0.855 | 0.54 | 31.0 | 462 of 545 | 48.0 [45.0, 50.8] |
| B0 low-capital rule | 50 | dev-optimal threshold | 1.250 | 0.63 | 24.6 | 485 of 545 | 58.9 [56.0, 61.8] |
| B0 low-capital rule | 100 | dev-optimal threshold | 1.990 | 0.71 | 17.5 | 500 of 545 | 67.7 [64.9, 70.4] |
| B1 logistic regression (frozen) | 10 | p ≥ 1/(1+r) | 0.401 | 0.52 | 63.6 | 468 of 545 | 49.0 [46.3, 51.6] |
| B1 logistic regression (frozen) | 20 | p ≥ 1/(1+r) | 0.603 | 0.64 | 51.7 | 490 of 545 | 60.6 [57.8, 63.0] |
| B1 logistic regression (frozen) | 50 | p ≥ 1/(1+r) | 0.978 | 0.76 | 38.3 | 506 of 545 | 74.0 [71.2, 76.3] |
| B1 logistic regression (frozen) | 100 | p ≥ 1/(1+r) | 1.432 | 0.84 | 28.6 | 519 of 545 | 81.6 [79.1, 83.6] |
| M1 gradient boosting | 10 | p ≥ 1/(1+r) | 0.686 | 0.66 | 46.9 | 495 of 545 | 58.2 [55.2, 60.9] |
| M1 gradient boosting | 20 | p ≥ 1/(1+r) | 0.957 | 0.76 | 38.9 | 511 of 545 | 70.1 [67.4, 72.3] |
| M1 gradient boosting | 50 | p ≥ 1/(1+r) | 1.396 | 0.84 | 29.3 | 522 of 545 | 79.5 [77.1, 81.6] |
| M1 gradient boosting | 100 | p ≥ 1/(1+r) | 1.876 | 0.87 | 22.7 | 526 of 545 | 84.0 [81.8, 86.0] |

**Table 17: Paired differences between models (same bank resamples)**

| Metric | r | Difference | Mean | 95% CI | Share of resamples > 0 |
|:---|---:|:---|---:|:---|---:|
| PR-AUC |  | B1_logit - B0_low_capital_rule | 0.3237 | [0.2842, 0.3624] | 1.000 |
| ROC-AUC |  | B1_logit - B0_low_capital_rule | 0.0317 | [0.0264, 0.0374] | 1.000 |
| saving | 10 | B1_logit - B0_low_capital_rule | 11.2222 | [8.5680, 13.8878] | 1.000 |
| saving | 20 | B1_logit - B0_low_capital_rule | 12.6522 | [10.1451, 15.1278] | 1.000 |
| saving | 50 | B1_logit - B0_low_capital_rule | 15.0985 | [12.8515, 17.2197] | 1.000 |
| saving | 100 | B1_logit - B0_low_capital_rule | 13.8682 | [11.8188, 15.7766] | 1.000 |
| PR-AUC |  | B1_logit - M1_hist_gbm | 0.0240 | [0.0045, 0.0458] | 0.992 |
| ROC-AUC |  | B1_logit - M1_hist_gbm | -0.0007 | [-0.0054, 0.0032] | 0.377 |
| saving | 10 | B1_logit - M1_hist_gbm | -9.1459 | [-11.1235, -7.2078] | 0.000 |
| saving | 20 | B1_logit - M1_hist_gbm | -9.4524 | [-11.3211, -7.6752] | 0.000 |
| saving | 50 | B1_logit - M1_hist_gbm | -5.5041 | [-6.8098, -4.1718] | 0.000 |
| saving | 100 | B1_logit - M1_hist_gbm | -2.3929 | [-3.5081, -1.3438] | 0.000 |

**Table 18: Pre-declared secondary evaluations (point estimates, no intervals)**

| Subset | Model | Rows | Positive rows (banks) | PR-AUC | ROC-AUC | Saving r=10 | r=20 | r=50 | r=100 |
|:---|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| S1 positives with failure within 45 days excluded | B0 low-capital rule | 435,122 | 1,810 (544) | 0.198 | 0.946 | 29.7 | 40.8 | 52.9 | 63.0 |
| S1 positives with failure within 45 days excluded | B1 logistic regression (frozen) | 435,122 | 1,810 (544) | 0.517 | 0.982 | 42.7 | 55.4 | 70.5 | 78.9 |
| S1 positives with failure within 45 days excluded | M1 gradient boosting | 435,122 | 1,810 (544) | 0.490 | 0.983 | 52.2 | 65.8 | 76.4 | 81.5 |
| S2 alternative label | B0 low-capital rule | 435,446 | 2,181 (558) | 0.276 | 0.946 | 37.1 | 47.0 | 57.6 | 66.3 |
| S2 alternative label | B1 logistic regression (frozen) | 435,446 | 2,181 (558) | 0.594 | 0.977 | 48.0 | 59.3 | 72.4 | 79.8 |
| S2 alternative label | M1 gradient boosting | 435,446 | 2,181 (558) | 0.571 | 0.981 | 57.0 | 68.6 | 77.9 | 82.2 |
| S3a 2008–2014 | B0 low-capital rule | 212,645 | 2,005 (513) | 0.513 | 0.956 | 43.1 | 51.9 | 61.7 | 70.4 |
| S3a 2008–2014 | B1 logistic regression (frozen) | 212,645 | 2,005 (513) | 0.629 | 0.983 | 50.6 | 62.0 | 75.3 | 82.9 |
| S3a 2008–2014 | M1 gradient boosting | 212,645 | 2,005 (513) | 0.616 | 0.984 | 60.2 | 71.6 | 81.0 | 85.6 |
| S3b 2015–2025 | B0 low-capital rule | 222,801 | 129 (36) | 0.029 | 0.915 | -42.6 | -13.1 | 15.6 | 25.7 |
| S3b 2015–2025 | B1 logistic regression (frozen) | 222,801 | 129 (36) | 0.303 | 0.936 | 24.0 | 39.6 | 52.6 | 60.6 |
| S3b 2015–2025 | M1 gradient boosting | 222,801 | 129 (36) | 0.216 | 0.931 | 27.3 | 46.7 | 55.4 | 58.6 |

**Table 19: Mean predicted probability against observed frequency on the test period**

| Model | Mean predicted (%) | Observed (%) | Top 1% of predictions: predicted (%) | Top 1%: observed (%) |
|:---|---:|---:|---:|---:|
| B1 logistic regression (frozen) | 0.192 | 0.490 | 14.33 | 37.69 |
| M1 gradient boosting | 0.274 | 0.490 | 23.14 | 37.76 |

**Reading the results.**

1. **The frozen model against the simple rule.** The logistic regression reaches a PR-AUC of 0.61 (about 124 times the base rate of 0.49%) and a ROC-AUC of 0.984, against 0.28 and 0.953 for the low-capital rule; the differences (+0.324 and +0.032) have intervals that exclude zero. In cost terms it saves 49.0%, 60.6%, 74.0% and 81.6% of the cost of never alerting for r = 10, 20, 50 and 100, against 37.9% to 67.7% for the rule; the advantage of 11.2 to 15.1 percentage points has an interval excluding zero at every r.
2. **Alerts.** At r = 50 the logistic regression raises an alarm on 0.98% of bank-quarters with a precision of 38.3%; 506 of the 545 failed banks (92.8%) were alerted at least once in the four quarters before their failure, and 468 (85.9%) at r = 10.
3. **Logistic regression against boosting.** The two have the same ROC-AUC (difference -0.0007, interval [-0.0054, 0.0032]); logistic regression has the higher PR-AUC (+0.024, interval [0.005, 0.046]). With the same alarm rule, however, **boosting saves more**: 58.2% to 84.0%, that is 2.4 to 9.5 points more than the logistic regression, with intervals that exclude zero at every r. The frozen selection (logistic regression and policy P2) is **not revised**: the development folds had shown no clear difference between the two models in cost terms (§6.7), and we report the test outcome as it is.
4. **Calibration.** The predicted probabilities are lower than the observed frequency (Table 19): the mean prediction of the logistic regression is 0.19% against 0.49% observed, and in the top 1% of predictions 14.3% against 37.7%; boosting is less under-predicted. The threshold 1/(1+r) assumes calibrated probabilities, so on this period it is conservative, and boosting raises more alarms at the same threshold (0.69% against 0.40% of bank-quarters at r = 10, with a row recall of 0.66 against 0.52). This agrees with the higher saving of boosting despite the equal ROC-AUC, but we did not test that explanation, and no threshold was searched on the test period.
5. **Secondary evaluations (Table 18).** Removing the 324 bank-quarters whose failure occurs within 45 days (S1) lowers the positive rate from 0.49% to 0.42%, and the PR-AUC of the logistic regression from 0.605 to 0.517; PR-AUC depends on the positive rate, and the ROC-AUC, which does not, changes little (0.984 to 0.982). The saving at r = 10 falls from 49.0% to 42.7%. Because the publication delay of the reports is not modelled, this evaluation shows how much of the performance rests on these rows. The alternative label (S2) gives results close to the primary ones. In 2008–2014 (2,005 positive rows, 513 banks) the logistic regression has a PR-AUC of 0.629; in 2015–2025 (129 positive rows, 36 banks) it is 0.303 and the low-capital rule has a **negative** saving at r = 10 and r = 20 (-42.6% and -13.1%), that is it would cost more than never alerting, while the logistic regression stays positive (24.0% to 60.6%). The 2015–2025 figures rest on 36 failed banks and are not accompanied by intervals.

## 6.11 Spark MLlib demonstration

`src/09c_mllib_demo.py` reproduces the logistic regression inside Spark ML on **development data only**: fold 3 (training rows up to 1992Q4, 598,088 rows; scoring 1993Q4–2006Q4, 554,631 rows) and the final fit on the whole development period (1,193,818 rows). Pre-processing is learned on the training rows only (winsorising bounds from approximate quantiles, median imputation, standardisation), the model is an L2-penalised logistic regression, and the penalty is set to λ = 1/(C·N) with C = 1 and N the number of training rows, which gives the same minimiser as scikit-learn's `C = 1` (checked numerically on synthetic data before the run). It ran on Google Colab (Spark 4.0.5, Java 21.0.12.1, 2 cores). With exact quantiles (relative error 0) and a 4 GB driver the final fit failed with a Java heap error, so the run used an 8 GB driver and a relative error of 0.001 for the quantiles.

**Table 20: Spark MLlib against scikit-learn, development fold 3**

| Quantity | scikit-learn | Spark MLlib |
|:---|---:|---:|
| ROC-AUC | 0.8782 | 0.8789 (Spark's own: 0.8788) |
| PR-AUC (average precision) | 0.1465 | 0.1456 (Spark's `areaUnderPR`, a trapezoid rule: 0.0970, not comparable) |
| Probabilities of the 554,631 validation rows | – | Pearson 0.9994, Spearman 0.9996, mean \|difference\| 6.75e-05, maximum \|difference\| 0.050 |
| Final-fit coefficients (standardised features) | – | same sign for 23 of 23; Spearman correlation 0.9911; largest difference 0.564 |
| Fit time, iterations (fold 3) | – | 84 s, 74 |

The two implementations agree closely on predictions and ranking. The largest coefficient differences are on the three highly correlated capital variables (`eq_asset`, `RBC1AAJ`, `RBCRWAJ`); their sum is -4.113 in scikit-learn and -4.096 in MLlib, which is consistent with weight being redistributed among correlated variables with little effect on the score (the predictions agree, Table 20). The remaining differences are probably due to pre-processing conventions (approximate rather than interpolated quantiles, approximate medians, sample rather than population standard deviation); we did not isolate their individual effects. Appendix D lists all coefficients.

## 6.12 Limitations of the evaluation and work not done

* One test period, one frozen model and one policy: the intervals reflect the sampling of banks, not the choice of model, and secondary evaluations have none.
* The probabilities are under-predicted on the test period (Table 19), so the policy p ≥ 1/(1+r) is conservative; recalibration (for example on the most recent development quarters) was not tried because the design was frozen, and a monitored recalibration would be needed in operation.
* The sensitivity analyses that require refitting (dropping `p9asset_asset` and `naasset_asset`; median `COST` for C_FN; costs counted per bank instead of per bank-quarter) were **not run**. The grid of r already covers the choice of the cost scale: using the median instead of the mean cost for a missed failure rescales r by a factor of 8,603/57,169 ≈ 0.15.
* C_FP remains an assumption, publication delay is not modelled, and data vintages are not controlled (§6.8).
* The reported savings are cost savings relative to never alerting, computed per bank-quarter with the costs of §6.6; they are not forecasts of losses to a deposit insurance fund.

---

