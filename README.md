# FDIC Bank Failure Early Warning System (BFT65B)

A reproducible big-data pipeline and early-warning model for bank failures, built on the public FDIC Call Reports (1984Q1 – 2026Q2). Final individual project of **BFT65B – Big Data Analytics in Finance** (National Economics University). Author: Hứa Văn Phúc, student ID 11231006.

## 1. Context

The project takes the viewpoint of a data engineer at the State Bank of Vietnam. Vietnamese supervisory data are not public, so the pipeline is built and tested on 42 years of US data: 170 quarterly Call Reports, the failure registry and the institutions directory. The emphasis is on the data pipeline (storage, quality, point-in-time correctness, scalable processing), not on model accuracy.

## 2. Status

| Part | Content | Status |
|:---|:---|:---|
| 1 | Acquisition, verification, storage layout benchmark | done |
| 2 | Data-quality audit, clean point-in-time panel, exit classification | done |
| 3 | Polars processing, pandas benchmark, PySpark reproduction, scaling argument | transformation T1 done (Polars, pandas and Spark benchmark, strict result checks, plan reading); T2 (labels) not benchmarked; write-up in the report §4 |
| 4 | Feature pipeline (23 features, fitted preprocessing) | done |
| 5 | Validation, models, cost-based threshold | design and development-period results done; **single test evaluation and Spark MLlib demo pending** |
| 6 | Conclusions | pending |

The test period (2008Q1 – 2025Q2) has **not** been evaluated yet.

## 3. Repository structure

```text
FDIC_Bank_Failure_EWS/
├── .gitignore                      # data_raw/ and data_processed/ are not committed
├── requirements.txt                # pinned dependencies (regenerate with pip freeze after the last change)
├── config.py                       # paths, seed (42), label horizon, split dates, CV folds, feature lists
├── README.md
├── docs/fdic/risview_properties.yaml   # official field dictionary (downloaded 2026-11-14)
├── data_raw/                       # (git-ignored) FDIC downloads + manifest.json with SHA-256
├── data_processed/                 # (git-ignored) outputs of steps 04-08
│   ├── failure_events.parquet      # 3,273 in-scope failure/assistance records with COST quality flags
│   ├── panel_decumulated.parquet   # 1,682,616 rows, YTD flows converted to single quarters
│   ├── panel/year=*/part-0.parquet # labelled, censored, split panel (1,681,547 rows)
│   ├── bank_exits.parquet          # exit class of each of the 24,064 charters
│   ├── features.parquet            # 23 features + labels + split
│   └── oof_dev.parquet             # out-of-fold predictions on the development period
├── src/
│   ├── 01_download.py              # resumable, verified API download
│   ├── 02_storage_benchmark.py     # six storage layouts compared
│   ├── 03_quality_report.py        # automated audit and problem register
│   ├── 04_build_panel.py           # stages 1-4: events, de-cumulation, labels/split, exits
│   ├── 05_polars_pandas_benchmark.py   # Part 3: T1 in Polars vs pandas, isolated subprocesses, thread sweep
│   ├── 05b_polars_engine_explain.py    # Part 3: Polars plans and stage timing
│   ├── 06_spark_pipeline.py        # Part 3: T1 in PySpark (laptop with Java, or Google Colab)
│   ├── 06b_compare_spark.py        # Part 3: strict row-level comparison Spark vs Polars
│   ├── 07_features.py              # stateless feature construction
│   ├── preprocess.py               # fitted pipeline: winsorize -> median impute -> scale
│   ├── modeling.py                 # dev-only loader and model definitions
│   ├── 08_model.py                 # time-ordered validation of three models
│   ├── 08b_oof_bootstrap.py        # out-of-fold predictions, bank-cluster bootstrap
│   ├── 08c_calibration_cost.py     # calibration and cost-based thresholds
│   ├── 08d_policy_comparison.py    # threshold policies compared across folds
│   ├── check_pipeline_leak.py      # proves the preprocessing statistics come from dev only
│   └── diagnostics/                # read-only evidence scripts (see section 5)
├── notebooks/                      # Colab notebook of the Spark run (with outputs)
└── report/
    ├── final_report.md             # master report
    ├── figures/
    ├── dq_*.csv, dq_report.md, dq_summary.json     # audit outputs
    ├── panel_split_summary.csv     # rows/positives per split
    ├── cv_baseline_results.csv, cv_bootstrap_ci.csv, cv_bootstrap_pairs.csv
    ├── cv_calibration.csv, cv_cost_thresholds.csv, cv_policy_comparison.csv
    ├── benchmark_T1_decumulate*.csv, benchmark_T1_thread_sweep.csv   # Part 3 laptop measurements
    ├── polars_plan_decumulate.txt, polars_plan_T1_*.txt, polars_stage_timing_T1.csv   # Polars plans
    └── spark_T1_result_*.json, spark_plan_T1_*.txt   # Part 3 Spark runs (Colab)
```

## 4. How to reproduce

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows PowerShell
pip install -r requirements.txt
```

Run from the repository root, in this order. Timings are from the author's laptop (Intel Core i5-10210U, 8 GB RAM, Windows 11); "n/r" = not recorded yet.

| Step | Command | Output / assertion | Time |
|:---|:---|:---|---:|
| 1.1 | `python src/01_download.py` (add `--verify-only` for an offline check) | 170 quarters, 1,682,616 rows, 24,064 banks; `RESULT: ALL CHECKS PASSED` | n/r (the assignment estimates about 1 h) |
| 1.2 | `python src/02_storage_benchmark.py` | `report/storage_benchmark_results.csv`, figure | n/r |
| 2.1 | `python src/03_quality_report.py` (`--quick` for 8 quarters) | `report/dq_*`; 0 duplicate keys | ≈ 11 s |
| 2.2 | `python src/04_build_panel.py` (`--from-stage k` resumes at stage k) | asserts: null accounting of de-cumulation; no row on/after last failure date; 0 label mismatches against a pandas recomputation | n/r |
| 4 | `python src/07_features.py` | 1,681,547 rows × 23 features; asserts forbidden-column list is disjoint; 0 mismatches in an independent growth check | n/r |
| 4 | `python src/check_pipeline_leak.py` | `LEAK CHECK PASSED` | n/r |
| 5 | `python src/08_model.py` | `report/cv_baseline_results.csv` | < 1 min |
| 5 | `python src/08b_oof_bootstrap.py` | CIs and paired differences; point estimates equal those of 08 | ≈ 1.5 min |
| 5 | `python src/08c_calibration_cost.py` | calibration, cost thresholds | n/r |
| 5 | `python src/08d_policy_comparison.py` | policy comparison | n/r |

All random operations use seed 42 (`config.RANDOM_SEED`; bootstrap: `numpy.random.default_rng(42)`).

### Part 3 (scalable processing)

Laptop (Windows PowerShell, from the repository root; `pip install psutil` is needed for memory measurement):

```powershell
python src/05_polars_pandas_benchmark.py --prep --scales 1 2 4     # builds data_processed/bench/ytd_input_x*.parquet
python src/05_polars_pandas_benchmark.py --verify                  # strict equality of Polars, pandas and step 04
python src/05_polars_pandas_benchmark.py --repeats 7               # report/benchmark_T1_decumulate.csv
python src/05_polars_pandas_benchmark.py --sweep 1 2 4 8 --sweep-scale 4 --repeats 7   # thread sweep, interleaved runs
python src/05b_polars_engine_explain.py 4                          # plans and stage timing
```

Spark on Google Colab (Java 21 and PySpark 4.0.5 were used): upload a zip with `config.py`, `src/` and `data_processed/bench/ytd_input_x*.parquet` (use `/` separators in the archive), unzip, `pip install pyspark polars psutil pandas pyarrow`, then:

```bash
python src/05_polars_pandas_benchmark.py --engines pandas polars_lazy --repeats 7
python src/06_spark_pipeline.py --input data_processed/bench/ytd_input_x1.parquet --label x1_default --write-out spark_T1_out_x1
python src/06_spark_pipeline.py --input data_processed/bench/ytd_input_x4.parquet --label x4_default
python src/06_spark_pipeline.py --input data_processed/bench/ytd_input_x4.parquet --label x4_shuf8 --shuffle-partitions 8 --repeats 3
```

Download `spark_T1_out_x1`, put it in `data_processed/spark_T1_out_x1/` and run `python src/06b_compare_spark.py data_processed/spark_T1_out_x1 1`; it must print `SPARK MATCHES POLARS`.

## 5. Diagnostic scripts (read-only evidence for the design decisions)

Run from the repository root; none of them writes data. They live in `src/diagnostics/`.

| Script | Question it answers |
|:---|:---|
| `check_failures_detail.py`, `…2.py`, `…3.py` | `RESTYPE` mix, `COST` quality, repeated `CERT`s, filings after the event date, `ESTYMD` test, labelable failures per period |
| `check_ytd_extra.py` | Which variables accumulate within the year (ratios Q2/Q1, Q3/Q1, Q4/Q1) |
| `check_gap_rows.py` | Why some quarters cannot be de-cumulated |
| `check_features_prep.py` | Days from report date to failure, null shares by era, scale of variables |
| `check_part5_prep.py` | `NCLNLS = P9ASSET + NAASSET` identity; validation fold sizes |

## 6. Key verified numbers

* Panel: 1,682,616 bank-quarters, 24,064 banks, 170 quarters, 0 duplicate keys. Failure registry: 4,117 records, 3,273 in scope (2,861 `FAILURE`, 412 `ASSISTANCE`), 3,251 linked to the panel, 22 not linked.
* **YTD flows.** Seven income-statement variables (`NETINC`, `INTINC`, `EINTEXP`, `NONII`, `NONIX`, `ELNATR`, `NTLNLS`) behave like cumulative year-to-date series (median |Q4|/|Q1| between 4.05 and 5.14; share of bank-years with Q1≤Q2≤Q3≤Q4 from 59.5% to 99.5%), and are converted to single-quarter values in step 04; `ROA`, `ROE`, `NIMY`, `ASSET` are not (ratios ≈ 1).
* **Exits.** 19,751 banks stop reporting before 2026Q2: 2,837 have a `FAILURE` record, 373 only an `ASSISTANCE` record, 16,541 neither. (An earlier audit counted 3,210 failures because it also included the 373 assistance-only banks; it was corrected.)
* **After-failure filings.** 1,069 bank-quarter rows with `REPDTE ≥` the last `FAILURE` date are removed (0.064% of the panel).
* **Split.** Development ≤ 2006Q4: 1,193,818 rows, 2,290 failed banks; gap 2007Q1–Q4; test 2008Q1–2025Q2: 435,446 rows, 545 failed banks; 17,529 rows after 2025Q2 have a null label.

## 7. Point-in-time and leakage rules

| Rule | Implementation |
|:---|:---|
| Label looks forward, features do not | y = 1 if a `FAILURE` date falls in (REPDTE, REPDTE + 12 months]; features use only quarter *q*, quarter *q−1* of the same year and quarter *q−4* (calendar join) |
| No report on or after the failure date | rows with `REPDTE ≥` the last `FAILURE` date of the same `CERT` are removed |
| Mergers are not failures and are not deleted | exits without a `FAILURE` record keep all rows with y = 0; no censoring by future events |
| Assistance is not failure | y = 1 only for `RESTYPE = FAILURE`; `ASSISTANCE` is used only for a sensitivity label |
| Outcome fields are never features | `COST`, `RESTYPE`, `ACTIVE`, `ENDEFYMD`, `CHANGEC1`, exit class and failure-event columns are listed in `FORBIDDEN_FEATURE_COLS` (assertion in step 07) |
| Fitted preprocessing sees training data only | winsorizer, imputer, scaler are fitted per fold on the training window, and for the final model on development rows only (`check_pipeline_leak.py`) |
| Test set untouched during design | modelling scripts load only `split = dev` and assert `REPDTE ≤ 2006-12-31` |
| `COST` | used only for failures of the development window, in the cost step |

## 8. Pre-registered decision rules

Fixed on **2026-11-14**, before the cost and policy results were computed (commit: **508e235**):

1. **Model rule.** Among the two learned models, the primary model has the higher average saving over r ∈ {10, 20, 50, 100}, measured on pooled out-of-fold predictions with the in-group optimal threshold; if the difference is below 1 percentage point, logistic regression is chosen.
2. **Threshold-policy rule.** Choose the (model, policy) pair with the highest average over r of the worst-fold saving (max-min).

Outcome: logistic regression with the threshold p\* = 1/(1+r) (see `report/final_report.md`, §6.7). The design is frozen; the test period is evaluated once.

## 9. Known limitations

Small number of failures in 1994–2007; wide intervals in the third validation fold; untuned hyper-parameters; the false-alarm cost is an assumption; costs counted per bank-quarter; publication delay of reports not modelled; the API gives the latest version of each report (no vintage control); 22 failure records have no panel data (reason not established); `NTLNLS` keeps a residual Q4 effect after de-cumulation; the Part 3 benchmark covers one transformation on a 9-column slice, with replicated data, a noisy shared Colab machine, Spark in local mode only, and different Polars versions on laptop (2.1.0) and Colab (1.35.2).

## 10. Data source and AI disclosure

* **Data:** Federal Deposit Insurance Corporation, BankFind Suite API (`https://api.fdic.gov/banks`), downloaded 2026-11-08/09. Field dictionary: `https://api.fdic.gov/banks/docs/risview_properties.yaml` (downloaded 2026-11-14, copy in `docs/fdic/`). No data file is committed.
* **AI tools:** Claude (Anthropic) and Google Antigravity / Gemini were used to draft and review code, to discuss design choices and to draft documentation. All code was executed on the author's machine, and every number in the report and in this file comes from the output of those runs. AI-proposed statements that the data did not support were removed or corrected during the project (examples are listed in the report, §8). The author takes responsibility for all content.