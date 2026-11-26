"""Writes Part 6 (conclusions, limitations, implications for Vietnam) and the summary at the top of the report.
Test numbers are read from report/test_*.csv; the storage and benchmark figures are those of sections 2 and 4.
Usage: python tools/make_part6.py report/final_report.md"""
import sys
from pathlib import Path

import pandas as pd

rp = Path(sys.argv[1])
rep = rp.parent
m = pd.read_csv(rep / "test_metrics.csv").set_index("model")
cost = pd.read_csv(rep / "test_cost.csv").set_index(["model", "r"])
cal = pd.read_csv(rep / "test_calibration.csv").set_index("model")
sec = pd.read_csv(rep / "test_secondary.csv")
pairs = pd.read_csv(rep / "test_pairs.csv")
R = [10, 20, 50, 100]
f = lambda x, d=3: f"{x:.{d}f}"
b1, base = m.loc["B1_logit"], m.loc["B1_logit", "base_rate_pct"]
sv = lambda k, r: cost.loc[(k, r), "saving_vs_never_pct"]
hit = lambda r: int(cost.loc[("B1_logit", r), "banks_alerted_before_failure"])
nb = int(cost.loc[("B1_logit", 50), "failed_banks"])
d_b0 = pairs[(pairs["metric"] == "saving") & (pairs["difference"] == "B1_logit - B0_low_capital_rule")]["mean_diff"]
d_m1 = pairs[(pairs["metric"] == "saving") & (pairs["difference"] == "B1_logit - M1_hist_gbm")]["mean_diff"]
s3b = sec[(sec["subset"] == "period_2015-2025") & (sec["model"] == "B1_logit")].iloc[0]
s3a = sec[(sec["subset"] == "period_2008-2014") & (sec["model"] == "B1_logit")].iloc[0]
b0b = sec[(sec["subset"] == "period_2015-2025") & (sec["model"] == "B0_low_capital_rule")].iloc[0]
share_crisis = 100 * s3a["pos_rows"] / b1["pos_rows"]
ratio = cal.loc["B1_logit", "obs_pct"] / cal.loc["B1_logit", "mean_pred_pct"]

summary = f"""**Answers in brief.** **Q1 (storage and processing).** Typed Parquet with ZSTD halves the size of the raw CSV (174.1 MB against 350.1 MB) and, partitioned by year, reads one quarter about 70 times faster; Polars in lazy mode processes the whole 42-year panel on an 8 GB laptop in under two seconds for each of the two core transformations measured, and Spark in local mode was 3.8 to 12.6 times slower on the same machine, so a cluster is not justified at this size (§4.7). **Q2 (quality and look-ahead bias).** The audit found problems in the target, the exits and the income statement (year-to-date flows, assistance versus failure, 1,069 post-failure filings, 16,541 exits that are not failures); each is handled by a rule that uses only information available at the quarter being scored, and the label logic is checked by independent recomputations (§3, §4.6). **Q3 (validation).** The output supports an honest evaluation: with time-ordered purged validation, rules registered before the test period, and one logged test run, the frozen logistic regression reached a PR-AUC of {f(b1['pr_auc'], 2)} and a ROC-AUC of {f(b1['roc_auc'])} on 2008Q1–2025Q2, and saved {f(sv('B1_logit', 10), 0)}% to {f(sv('B1_logit', 100), 0)}% of the cost of never alerting under the assumed cost ratios; the limits are stated in §7.4."""

part6 = f"""# 7. Part 6: Conclusions & Policy Implications for Vietnam

## 7.1 Q1: How should four decades of bank reports be stored and processed on a laptop, and when is a Spark cluster needed?

* **Storage.** Typed Parquet with ZSTD compression takes 174.1 MB against 350.1 MB for the CSV (0.50 times); partitioned by year, reading one quarter takes 0.023 s against 1.655 s (about 70 times faster), and reading five columns takes 0.085 s against 0.617 s (7.3 times) (§2).
* **Processing.** On the 8 GB laptop, Polars in lazy mode computes the year-to-date de-cumulation (T1) over the 1.68 million bank-quarters in 0.46 s and at four times the size in 1.67 s with a peak of 1.76 GB, and the forward label with censoring (T2) in 0.12 s and 0.40 s. It was about 1.1 to 1.3 times faster than the pandas merge for T1 at the larger scales (the ratio varied by session) and 4 to 5 times faster for T2; its advantage comes from multi-threading (with one thread it was slower than pandas), and its default streaming engine used about half the memory of the in-memory engine (§4.3, §4.6). The pipeline therefore needs no cluster and runs inside one machine.
* **When a cluster would be needed.** Spark in local mode on the same 2-core Colab machine was 3.8 to 12.6 times slower than Polars (T1: 8.7 times at ×1 and 5.2 at ×4 with default settings, 3.8 with the best configuration measured; T2: 12.6 and 7.4 times) and needed about 7 to 14 s to start. No crossover appeared in the measured range. A cluster would be justified when the data no longer fit one machine even with the streaming engine (an extrapolation from the measured 0.2 GB per million rows of a 9-column slice puts this at the order of 70 million rows on a 16 GB machine), when many jobs or users must run at once or within a time limit that one machine cannot meet, or when the data already live in a distributed store. None applies to this panel (§4.7).

## 7.2 Q2: What quality problems do the raw data contain, and how does the pipeline handle them without look-ahead bias?

* **Income statement.** Seven income-statement variables are cumulative within the year (median |Q4|/|Q1| between 4.05 and 5.14); they are converted to single-quarter values with a calendar join that uses only quarters q and q−1, and the saw-tooth disappears (|Q4|/|Q1| of 1.03 to 1.10 for six variables; `NTLNLS` keeps a residual of 1.66).
* **Target.** Of the 3,273 events in scope, only the 2,861 `FAILURE` records define y = 1; assistance is not failure (380 of the 411 assisted charters never fail). Banks that disappear without a failure record keep all their rows with y = 0: 16,541 of the 19,751 exits (83.7%) are of this kind, and an earlier audit count of 3,210 failures, which also counted 373 assistance-only banks, was corrected to 2,837.
* **After-failure filings.** 1,069 bank-quarters dated on or after the last failure date of their charter are removed, and the label is checked by an independent recomputation (0 mismatches in 88,427 rows).
* **Cost and structure.** `COST` is missing for 23 failures and zero for 55 (20 of the zeros are recovered from an earlier assistance record, 35 remain unexplained) and is never a feature; `P3ASSET` is null before 2001 and is excluded; accounting-rule violations are flagged and not dropped, so that near-insolvent banks stay in the sample.
* **Look-ahead control.** Features use quarter q, the previous quarter of the same year and the same quarter one year earlier (calendar joins); all fitted pre-processing is learned on training rows only (checked: the learned cut-offs equal those computed from development rows alone, and would change for 23 of 23 features if test rows were included); outcome fields are excluded by an asserted list; the test period was read once, after the design was frozen. The label logic was reproduced in pandas and in Spark and matches the panel of step 04 on all 1,681,547 rows (§4.6).

## 7.3 Q3: Can the pipeline's output support an honestly validated early-warning model?

Yes, within the limits below. The validation used expanding windows with a purge of three quarters, bank-cluster bootstrap intervals, and decision rules registered before the test period; the test period (435,446 bank-quarters, {int(b1['pos_rows']):,} positive rows, {nb} failed banks) was evaluated once and the run is logged (§6.9).

* **Ranking.** The frozen logistic regression reached a PR-AUC of {f(b1['pr_auc'], 2)} ({b1['pr_auc'] / (base / 100):.0f} times the base rate of {f(base, 2)}%) and a ROC-AUC of {f(b1['roc_auc'])}, against {f(m.loc['B0_low_capital_rule', 'pr_auc'], 2)} and {f(m.loc['B0_low_capital_rule', 'roc_auc'])} for a one-variable low-capital rule.
* **Decision value.** With the alarm rule p ≥ 1/(1+r), it saved {f(sv('B1_logit', 10), 1)}%, {f(sv('B1_logit', 20), 1)}%, {f(sv('B1_logit', 50), 1)}% and {f(sv('B1_logit', 100), 1)}% of the cost of never alerting for r = 10, 20, 50 and 100 (intervals in Table 16), {f(d_b0.min(), 1)} to {f(d_b0.max(), 1)} points more than the simple rule; {hit(10)} to {hit(100)} of the {nb} failed banks ({100 * hit(10) / nb:.0f}% to {100 * hit(100) / nb:.0f}%) were alerted at least once in the four quarters before their failure.
* **Which model.** Gradient boosting, with the same rule, saved {f(-d_m1.max(), 1)} to {f(-d_m1.min(), 1)} points more than the logistic regression, with the same ROC-AUC. The frozen choice was not revised.
* **What the figures do not show.** {f(share_crisis, 0)}% of the positive rows fall in 2008–2014; in 2015–2025 ({int(s3b['pos_rows'])} positive rows, {int(s3b['pos_banks'])} banks) the PR-AUC is {f(s3b['pr_auc'], 2)} and the simple rule has a negative saving at r = 10 and 20 ({f(b0b['saving_r10'], 1)}% and {f(b0b['saving_r20'], 1)}%). The predicted probabilities are about {f(ratio, 1)} times lower than the observed frequency, so the alarm threshold is conservative.

## 7.4 Limitations

1. The test period is dominated by one crisis, and the recent quiet period rests on {int(s3b['pos_banks'])} failed banks, without intervals.
2. Probabilities are not calibrated on the test period; no recalibration was tried because the design was frozen.
3. The cost of a false alarm is an assumption (r from 10 to 100), costs are counted per bank-quarter, and the savings are not forecasts of losses to a deposit insurance fund.
4. The publication delay of the reports is not modelled, and the API gives the latest version of each report (no vintage control).
5. Twenty-three CAMELS-type accounting features only: no market, macroeconomic or forward-looking information; hyper-parameters were not tuned.
6. 21 of the 2,861 failures have no Call Report in the panel (reason not established) and can never be predicted.
7. One country and one definition of failure; the sensitivity analyses that require refitting were not run (§6.12).
8. The processing benchmark covers two transformations, replicated data, a noisy shared Colab machine and Spark in local mode only.

## 7.5 What would change for Vietnam

These points follow from our results and are not claims about Vietnamese data, which we did not use.

1. **Events and labels.** The target must be defined with the supervisor: which resolution events count as failure (for example special control or compulsory transfer, if these are the recorded tools) and which exits are mergers. In the US panel 83.7% of exits were not failures, and an audit count that included assistance-only banks overstated the failed banks by 373 (3,210 against 2,837); the same discipline (build y from the event registry, never from the exit itself, and keep mergers as y = 0) applies.
2. **Reporting forms.** We found that seven income-statement items are cumulative within the year; the same test (`dq_ytd_evidence`) must be run first on the supervisory returns. Reports should be stored as filed, with their versions, which the API did not allow here.
3. **Sample size.** The effective sample is the number of failed banks (2,290 in the development period here), not the number of rows (8,688 positive rows). If the number of resolution events in Vietnam is small, as the size of the system suggests (to be confirmed with the supervisor), a model like this cannot be trained on local failures alone; a transparent capital-based rule, which already reached a ROC-AUC of {f(m.loc['B0_low_capital_rule', 'roc_auc'], 2)} here, a score from a model trained elsewhere after checking calibration on local data, or both, would be the starting point, with bank-cluster intervals attached to any result.
4. **Calibration and thresholds.** Probabilities drifted by a factor of {f(ratio, 1)} between the development and the test periods; recalibration on recent quarters and a monitored threshold are needed in operation, and the cost ratio r must be set by the supervisor, since it cannot be estimated from the data.
5. **Computing.** The Polars pipeline ran on an 8 GB laptop, and the processing steps that were measured peaked below 2 GB (T1 at four times the size: 1.76 GB); Spark demonstrations ran on Colab. The pipeline can run inside the supervisor's own secure environment without a cluster or a cloud service. If the supervised population is of the order of tens or hundreds of institutions (to be confirmed with the supervisor), a Vietnamese bank-quarter panel would be a small fraction of the 1.68 million rows used here, far below the order of 70 million rows at which a 16 GB machine would be exceeded (§4.7).
6. **Governance.** The controls used here (a frozen design with a commit and a model hash, a log that allows one test run, rules registered before the evaluation, asserted leakage checks) are transferable and cost little.

---

"""

doc = rp.read_bytes().decode("utf-8")
crlf = "\r\n" in doc
doc = doc.replace("\r\n", "\n")
a = doc.find("*(To be finalized in Part 6")
b = doc.find(")*", a)
h0 = doc.find("# 7. Part 6: Conclusions & Policy Implications for Vietnam")
h1 = doc.find("\n---\n", h0)
if min(a, b, h0, h1) < 0:
    sys.exit("NOT WRITTEN: the summary placeholder or the Part 6 heading was not found.")
new = doc[:a] + summary + doc[b + 2:h0] + part6.rstrip("-\n") + "\n" + doc[h1:]
rp.write_bytes((new.replace("\n", "\r\n") if crlf else new).encode("utf-8"))
(rep / "part6_section.md").write_text(part6, encoding="utf-8")
print("summary and Part 6 written to", rp)