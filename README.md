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
    └── final_report.pdf            # Individual Research Report (Max 20 pages)
```

## 3. Data Acquisition & Verification (Part 1.1)

### Execution Command
The raw data is acquired directly from the public FDIC BankFind API using an atomic, resumable ingestion script:
```bash
python src/01_download.py
```
To perform an offline, zero-network integrity check on all files:
```bash
python src/01_download.py --verify-only
```

### Verified Audit Results
* **Financials (`/financials`)**: 
  * Coverage: **170 / 170 quarters** (1984Q1 – 2026Q2).
  * Total Rows: **1,682,616** across **24,064 distinct bank charters (`CERT`)**.
  * Total Compressed Size: **236.8 MB** (Snappy Parquet).
  * Integrity: Zero row count discrepancies against API totals; zero duplicate `(CERT, REPDTE)` primary keys.
* **Institutions (`/institutions`)**: **27,835** institutions recorded.
* **Failures (`/failures`)**: **4,117** failure records.
  * *Data Quality Finding*: 488 historical failure records prior to 1984 exhibit null `CERT` identifiers due to legacy pre-computerization tracking. An automated audit confirms that **100% of failures within the study scope (1984Q1 onwards) possess valid, non-null `CERT`s**, ensuring zero label leakage or unlinked failure events.
  * 44 repeated `CERT` instances represent multi-stage resolutions or repeated failures of restructured charters and are preserved intact using `ID` as primary key.
* **Verification Status**: `RESULT: ALL CHECKS PASSED`.

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
* **AI Assistance**: An AI pair-programming assistant (Google Antigravity / Gemini) was utilized during the development process to assist in drafting boilerplate pipeline architecture, code review, optimization profiling, and technical documentation review. All pipeline logic, data engineering decisions, metric designs, and analytical conclusions were critically inspected, executed, and validated by the author.