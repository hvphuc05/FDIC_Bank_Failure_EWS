# FDIC Bank Failure Early Warning System (BFT65B)

A scalable, reproducible Big Data Pipeline and Early Warning System (EWS) for predicting bank failures using US FDIC Call Reports (1984Q1 – 2026Q2).

## 1. Project Overview & Context
This project acts from the perspective of a Data Engineer at the Central Bank of Vietnam (SBV). Due to regulatory privacy constraints on domestic supervisory data, the pipeline is constructed and validated on 40+ years of US Federal Deposit Insurance Corporation (FDIC) regulatory filings to benchmark data engineering architectures, point-in-time feature pipelines, and cost-sensitive failure risk classification.

## 2. Repository Structure
```text
FDIC_Bank_Failure_EWS/
├── .gitignore                      # Strict exclusion of data/ and binary artifacts
├── requirements.txt                # Pinned dependency manifest
├── config.py                       # Global paths, parameters, and random seeds (42)
├── README.md                       # End-to-end execution and reproducibility guide
├── data_raw/                       # Raw downloads (git-ignored)
│   └── fdic/
│       ├── financials/             # 170 quarterly Call Report files (Parquet)
│       ├── failures.parquet        # Complete failure registry (4,117 records)
│       ├── institutions.parquet    # Bank structure and event history (27,835 records)
│       └── manifest.json           # Cryptographic SHA-256 audit manifest
├── data_processed/                 # Clean panel and leak-free features (git-ignored)
├── src/
│   ├── 01_download.py              # Resumable, verified API acquisition
│   ├── 02_storage_benchmark.py     # Quantified storage layout benchmarks
│   ├── 03_quality_report.py        # Automated data quality audit & problem register
│   ├── 04_build_panel.py           # Point-in-time bank panel builder (clean panel)
│   ├── 05_polars_pandas_benchmark.py # Engine performance benchmarking
│   ├── 06_spark_pipeline.py        # PySpark reproduction & DAG plan analysis
│   ├── 07_features.py              # CAMELS risk ratio pipeline
│   └── 08_model.py                 # Time-ordered validation & cost matrix tuning
├── notebooks/                      # Exploratory data analysis notebooks
└── report/
    ├── figures/                    # High-resolution benchmark & audit plots
    │   ├── storage_benchmark.png   # Storage performance visualization (Part 1)
    │   ├── dq_missing_heatmap.png  # Missingness dynamics over 42 years (Part 2)
    │   ├── dq_banks_per_quarter.png # Active bank counts & M&A attrition (Part 2)
    │   └── dq_ytd_sawtooth.png     # Empirical proof of cumulative YTD flows (Part 2)
    ├── storage_benchmark_results.csv # Empirical benchmark measurements (Part 1)
    ├── schema_report.csv           # Multi-quarter schema mismatch audit (Part 1)
    ├── dq_problem_register.csv     # Problem register P01-P10 with handling (Part 2)
    ├── dq_report.md                # Automated audit findings markdown (Part 2)
    ├── dq_summary.json             # Provenance & summary metrics (Part 2)
    ├── dq_*.csv                    # Detailed audit tables (missing, YTD, exits, rules)
    └── final_report.md             # Complete master report (Course submission)
```

## 3. Execution Pipeline

### Part 1.1: Data Acquisition & Verification
The raw data is acquired directly from the public FDIC BankFind API using an atomic, resumable ingestion script:
```bash
python src/01_download.py
```
To perform an offline, zero-network integrity check on all downloaded files:
```bash
python src/01_download.py --verify-only
```
* **Verified Counts**: 170 quarters, 1,682,616 rows across 24,064 banks (`financials`), 27,835 records (`institutions`), 4,117 records (`failures`).
* **Integrity Status**: `RESULT: ALL CHECKS PASSED`.

---

### Part 1.2: Storage Layout Design & Benchmarks
To evaluate storage efficiency and query latency across four decades of Call Reports, run:
```bash
python src/02_storage_benchmark.py
```

#### Empirical Measurements (Full Panel: 1.68M Rows, 33 Columns)
Benchmarked on Intel Core i5-10210U @ 1.60GHz, 8.0 GB RAM, Windows 11:

| Storage Layout | Files | Size (MB) | vs CSV | Write (s) | Read All (s) | Read 5 Cols (s) | Read 1 Qtr (s) | Data Integrity | Exact Match |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Raw CSV (Uncompressed)** | 1 | 350.1 MB | 1.00x | 7.20s | 1.60s | 0.617s | 1.655s | Passed | Reference |
| **Parquet Naive (Snappy)** | 1 | 211.7 MB | 0.60x | 10.27s | 0.589s | 0.117s | 0.032s | Passed | N/A (Schema) |
| **Parquet Typed (Snappy)** | 1 | 205.9 MB | 0.59x | 10.33s | **0.548s** | **0.085s** | 0.033s | Passed | **Exact Match** |
| **Parquet Typed (ZSTD)** | 1 | **174.1 MB** | **0.50x** | 10.25s | 0.697s | 0.125s | 0.064s | Passed | **Exact Match** |
| **Parquet ZSTD (Partitioned: Year)** | 43 | 181.6 MB | 0.52x | **3.18s** | 0.610s | 0.103s | **0.023s** | Passed | **Exact Match** |
| **Parquet ZSTD (Partitioned: Quarter)** | 170 | 189.6 MB | 0.54x | 3.51s | 0.709s | 0.181s | **0.029s** | Passed | **Exact Match** |

* **Key Takeaway**: Typed Parquet with ZSTD compression cuts disk footprint by **50%**. For selective analytical workloads, Parquet Column Pruning delivers a **7.3x speedup** (0.085s vs 0.617s), while Partition Pruning enables an immediate **70x latency reduction** (0.023s vs 1.655s) when querying crisis quarters (e.g., 2008Q4).

---

### Part 2.1: Automated Data Quality Audit
Audits all 170 quarterly files, the Failure Registry, and the Institutions directory without mutating raw records:
```bash
# Smoke test on recent 8 quarters:
python src/03_quality_report.py --quick

# Full audit across all 170 quarters (~17 seconds):
python src/03_quality_report.py
```

#### Core Empirical Audit Findings (1.68M Rows, 170 Quarters)
* **Panel Integrity**: 1,682,616 rows, 24,064 distinct bank charters (`CERT`), 0 duplicate primary keys `(CERT, REPDTE)`. Only 32 banks exhibit internal reporting hiatuses (92 missing bank-quarters); rolling features must use calendar offsets rather than row offsets.
* **Problem Register (P01–P10)**: Cataloged in `report/dq_problem_register.csv` with quantitative evidence, downstream handling, and methodological rationale.
* **Cumulative YTD Income Flows**: Proved empirically via `report/dq_ytd_evidence.csv` and `report/figures/dq_ytd_sawtooth.png`. Net Interest Income (`INTINC`) and Interest Expense (`EINTEXP`) show 100% monotonic within-year progression with a median $|Q4|/|Q1| \approx 4.1$, whereas stock variable `ASSET` remains at 1.04. Within-year de-cumulation is mandatory in Step 04.
* **Failure Registry Linkage**: 3,273 failures within study scope (1984Q1–2026Q2), with **0% null `CERT`s** and 3,251 successfully linked to Call Reports (99.3%). 
* **Leakage Gate Safeguard**: 439 failed banks have filings recorded *after* their official closure date (e.g., bridge banks or receivership accounting). Filings post-failure must be censored in Step 04 to prevent target leakage.
* **Exit Classification**: 19,751 banks exit the panel before 2026Q2. Exactly 3,210 are confirmed failures ($y=1$), while **16,541 are M&A absorptions or voluntary exits ($y=0$)**. Mislabeling M&A exits as failures would corrupt the target distribution by 83%.

---

## 4. Environment Setup
```bash
python -m venv .venv
# Windows PowerShell activation:
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 5. Academic Integrity & AI Disclosure
In strict adherence to the academic guidelines of course **BFT65B – Big Data Analytics in Finance**:
* **Data Sources**: Public data provided by the Federal Deposit Insurance Corporation (FDIC) via the BankFind Suite API (`https://api.fdic.gov/banks`).
* **AI Assistance**: AI assistants (Google Antigravity / Gemini, Claude) were used for boilerplate pipeline architecture review, debugging, and technical documentation profiling. All pipeline logic, data engineering decisions, metric designs, and analytical conclusions were critically inspected, executed, and validated on local hardware by the author.
