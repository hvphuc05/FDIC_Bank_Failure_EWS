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
│   ├── 03_quality_report.py        # Automated data quality audit
│   ├── 04_build_panel.py           # Point-in-time bank panel builder
│   ├── 05_polars_pandas_benchmark.py # Engine performance benchmarking
│   ├── 06_spark_pipeline.py        # PySpark reproduction & DAG plan analysis
│   ├── 07_features.py              # CAMELS risk ratio pipeline
│   └── 08_model.py                 # Time-ordered validation & cost matrix tuning
├── notebooks/                      # Exploratory data analysis notebooks
└── report/
    ├── figures/                    # Exported high-resolution benchmark plots
    │   └── storage_benchmark.png   # Storage performance visualization
    ├── storage_benchmark_results.csv # Empirical benchmark measurements
    ├── schema_report.csv           # Multi-quarter schema mismatch audit
    ├── evidence_part1.md           # Verified audit evidence
    └── report_outline.md           # 20-page final report structure
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
* **AI Assistance**: AI assistants (Google Antigravity / Gemini, Claude) were used for boilerplate pipeline architecture review, debugging, and technical documentation profiling. All pipeline logic, data engineering decisions, metric designs, and analytical cSonclusions were critically inspected, executed, and validated on local hardware by the author.