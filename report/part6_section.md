# 7. Part 6: Conclusions & Policy Implications for Vietnam

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

Yes, within the limits below. The validation used expanding windows with a purge of three quarters, bank-cluster bootstrap intervals, and decision rules registered before the test period; the test period (435,446 bank-quarters, 2,134 positive rows, 545 failed banks) was evaluated once and the run is logged (§6.9).

* **Ranking.** The frozen logistic regression reached a PR-AUC of 0.61 (124 times the base rate of 0.49%) and a ROC-AUC of 0.984, against 0.28 and 0.953 for a one-variable low-capital rule.
* **Decision value.** With the alarm rule p ≥ 1/(1+r), it saved 49.0%, 60.6%, 74.0% and 81.6% of the cost of never alerting for r = 10, 20, 50 and 100 (intervals in Table 16), 11.2 to 15.1 points more than the simple rule; 468 to 519 of the 545 failed banks (86% to 95%) were alerted at least once in the four quarters before their failure.
* **Which model.** Gradient boosting, with the same rule, saved 2.4 to 9.5 points more than the logistic regression, with the same ROC-AUC. The frozen choice was not revised.
* **What the figures do not show.** 94% of the positive rows fall in 2008–2014; in 2015–2025 (129 positive rows, 36 banks) the PR-AUC is 0.30 and the simple rule has a negative saving at r = 10 and 20 (-42.6% and -13.1%). The predicted probabilities are about 2.6 times lower than the observed frequency, so the alarm threshold is conservative.

## 7.4 Limitations

1. The test period is dominated by one crisis, and the recent quiet period rests on 36 failed banks, without intervals.
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
3. **Sample size.** The effective sample is the number of failed banks (2,290 in the development period here), not the number of rows (8,688 positive rows). If the number of resolution events in Vietnam is small, as the size of the system suggests (to be confirmed with the supervisor), a model like this cannot be trained on local failures alone; a transparent capital-based rule, which already reached a ROC-AUC of 0.95 here, a score from a model trained elsewhere after checking calibration on local data, or both, would be the starting point, with bank-cluster intervals attached to any result.
4. **Calibration and thresholds.** Probabilities drifted by a factor of 2.6 between the development and the test periods; recalibration on recent quarters and a monitored threshold are needed in operation, and the cost ratio r must be set by the supervisor, since it cannot be estimated from the data.
5. **Computing.** The whole pipeline ran on an 8 GB laptop, and the processing steps that were measured peaked below 2 GB (T1 at four times the size: 1.76 GB), so it can run inside the supervisor's own secure environment without a cluster or a cloud service.
6. **Governance.** The controls used here (a frozen design with a commit and a model hash, a log that allows one test run, rules registered before the evaluation, asserted leakage checks) are transferable and cost little.

---

