# FINAL INDIVIDUAL RESEARCH REPORT
## A Big-Data Pipeline for Bank Failure Early Warning

* **Student Full Name**: Hứa Văn Phúc
* **Student ID (MSSV)**: 11231006
* **Class / Cohort**: BFT65B – Big Data Analytics in Finance
* **GitHub Repository URL**: https://github.com/hvphuc05/FDIC_Bank_Failure_EWS
* **Data Ingestion Date**: 2026-11-08 / 2026-11-09
* **Ingestion Method**: FDIC Public BankFind Suite API (Automated, Resumable Python Pipeline with SHA-256 Checksum Validation)
* **Dataset Scope**: 1984Q1 to 2026Q2 (170 Quarters)
* **Total Verified Row Counts**:
  * Financial Call Reports (`financials`): 1,682,616 rows (24,064 unique bank charters)
  * Failure Registry (`failures`): 4,117 records
  * Institution Directory (`institutions`): 27,835 records
* **Modelling Scope**: 1984Q1 to 2025Q2 (Target window capped at 2025Q2 due to 4-quarter forward label requirement)
* **Development Period (Train / Val)**: 1984Q1 – 2007Q4 (Contains ~1,400+ bank failures from the S&L Crisis)
* **Purged Gap (Embargo)**: 2008Q1 – 2008Q4 (Strict 4-quarter gap to prevent look-ahead leakage)
* **Test Period (Out-of-Time Evaluation)**: 2009Q1 – 2014Q4 (Contains 400+ bank failures from the Global Financial Crisis)

---

## TABLE OF CONTENTS (20 PAGES BUDGET)

### 1. Introduction & Context (1.5 pages)
* Executive Summary: Addressing Q1, Q2, Q3 from the perspective of Vietnam's central bank.
* Regulatory Context: Call Reports, early-warning mechanisms, and supervisory constraints.

### 2. Part 1: Data Acquisition & Storage Design (3.0 pages)
* 2.1 Ingestion Architecture: Resumable chunking, atomic commits, SHA-256 manifest.
* 2.2 Storage Benchmark: Comparative analysis (CSV vs Parquet Snappy vs ZSTD vs Partitioning).
* 2.3 Measurements & Technical Justification: Column pruning and predicate pushdown.

### 3. Part 2: Data Quality Audit & Bank Panel Construction (3.5 pages)
* 3.1 Systematic Anomaly Detection: Missingness patterns, negative values, Basel reporting transitions.
* 3.2 Bank Exit Resolution: Distinguishing Failure vs M&A vs Voluntary Liquidation.
* 3.3 Leak-free Point-in-Time Panel Construction ($y_{i,q} = 1\{fail \in (q, q+4]\}$).

### 4. Part 3: Scalable Processing with Polars & PySpark (4.0 pages)
* 4.1 Polars Transformation Engine: Lazy execution, Arrow columnar memory, multi-threading.
* 4.2 Benchmark Analysis: Polars vs Pandas (CPU execution time & peak RAM RSS).
* 4.3 PySpark Verification: Catalyst optimizer logical/physical plan, DAG shuffle stages.
* 4.4 When is a Spark Cluster Justified? (Hardware threshold analysis answering Q1).

### 5. Part 4: Feature Pipeline & Leak-Free Architecture (2.5 pages)
* 5.1 Financial Feature Engineering: CAMELS framework implementation and macro trends.
* 5.2 Strict Point-in-Time Pipeline: Fit-on-train-only transformer pipeline design.

### 6. Part 5: Modelling, Validation & Cost-Sensitive Optimization (3.5 pages)
* 6.1 Validation Strategy: Time-ordered temporal split with 4-quarter purged embargo.
* 6.2 Class Imbalance Handling & Metrics: Precision-Recall AUC (PR-AUC) vs ROC-AUC.
* 6.3 Baseline vs Challenger Models (Logistic Regression, LightGBM, Spark MLlib).
* 6.4 Cost-Sensitive Alarm Threshold: Minimizing FDIC expected loss ($C_{FN} \gg C_{FP}$).
* 6.5 Final Evaluation on Out-of-Time Test Set (Evaluated strictly ONCE).

### 7. Part 6: Conclusions & Context for Vietnam (1.5 pages)
* 7.1 Direct Answers to Core Research Questions (Q1, Q2, Q3).
* 7.2 Pipeline Limitations.
* 7.3 Adaptation to the Vietnamese Banking Sector (SBV, Circulars 35 & 11, Special Supervision regime).

### 8. References & Academic Integrity (0.5 page)
### 9. Appendix (Max 10 pages)