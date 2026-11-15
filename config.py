from pathlib import Path

# ---------------------------------------------------------------------------
# Project directories
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent

RAW_DATA_DIR = PROJECT_ROOT / "data_raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data_processed"

SRC_DIR = PROJECT_ROOT / "src"
NOTEBOOK_DIR = PROJECT_ROOT / "notebooks"
REPORT_DIR = PROJECT_ROOT / "report"

# Reproducibility
RANDOM_SEED = 42

# ---------------------------------------------------------------------------
# FDIC BankFind API (https://api.fdic.gov/banks/docs/) - no API key needed
# ---------------------------------------------------------------------------
FDIC_API_BASE = "https://api.fdic.gov/banks"
FDIC_RAW_DIR = RAW_DATA_DIR / "fdic"

FDIC_PAGE_SIZE = 10_000          # max rows per call (value taken from the course starter script)
FDIC_REQUEST_TIMEOUT_SEC = 180
FDIC_MAX_RETRIES = 6
FDIC_PAUSE_SEC = 1.0             # polite pause after every successful request

# Scope required by the assignment: 1984Q1 -> 2026Q2
FDIC_START_DATE = "1984-03-31"
FDIC_END_DATE = "2026-06-30"

# Field lists copied unchanged from the instructor's starter script.
# Amounts in thousands of USD, ratios in %. Check each code in:
# https://api.fdic.gov/banks/docs/risview_properties.yaml
FDIC_FINANCIAL_FIELDS = [
    "CERT", "REPDTE", "STNAME",
    "ASSET", "DEP", "DEPDOM", "EQ", "BRO", "CHBAL", "SC",
    "LNLSGR", "LNLSNET", "LNRE", "LNCI", "LNCON", "LNATRES",
    "NCLNLS", "P3ASSET", "P9ASSET", "NAASSET", "NTLNLS", "ORE",
    "NETINC", "INTINC", "EINTEXP", "NONII", "NONIX", "ELNATR",
    "ROA", "ROE", "NIMY",
    "RBC1AAJ", "RBCRWAJ",
]

FDIC_RATIO_FIELDS = [
    "ROA", "ROE", "NIMY", "RBC1AAJ", "RBCRWAJ"
]

FDIC_FAILURE_FIELDS = [
    "ID", "CERT", "NAME", "CITYST", "FAILDATE", "FAILYR",
    "COST", "QBFASSET", "QBFDEP", "RESTYPE",
]
FDIC_INSTITUTION_FIELDS = [
    "CERT", "NAME", "STNAME", "BKCLASS", "ACTIVE",
    "ESTYMD", "ENDEFYMD", "CHANGEC1", "INSDATE",
]

# Soft sanity values taken from the assignment text (PDF section 2.1)
EXPECTED_FINANCIAL_ROWS = 1_680_000   # "about 1.68 million rows"
EXPECTED_FAILURE_ROWS = 4_100         # "about 4 100 rows"

# ---------------------------------------------------------------------------
# Label & split (decided in Part 2 design; single source of truth)
# ---------------------------------------------------------------------------
LABEL_HORIZON_QUARTERS = 4            # y = 1{FAILURE with FAILDATE in (q, q+4 quarters]}
FAILURE_SCOPE_START = "1984-01-01"    # same scope as the audit (3,273 events)
FAILURE_SCOPE_END = "2026-06-30"

# Out-of-time split on quarter-end dates (REPDTE)
DEV_END = "2006-12-31"                # dev: REPDTE <= 2006Q4 (labels look ahead to 2007Q4)
GAP_START = "2007-03-31"              # gap: 2007Q1..2007Q4 (4 quarters, in neither set)
GAP_END = "2007-12-31"
TEST_START = "2008-03-31"             # test: 2008Q1 .. LAST_LABELED_Q
LAST_LABELED_Q = "2025-06-30"         # later quarters have incomplete 4-quarter labels

# Flow variables confirmed as year-to-date by dq_ytd_evidence.csv (de-cumulate in step 04).
# NTLNLS and the ratio columns are decided after check_ytd_extra.py.
YTD_FLOW_COLUMNS = ["NETINC", "INTINC", "EINTEXP", "NONII", "NONIX", "ELNATR", "NTLNLS"]

PROCESSED_PANEL_DIR = PROCESSED_DATA_DIR / "panel"

# ---------------------------------------------------------------------------
# Feature design (Part 4). P3ASSET excluded: null until 1999 (era-confounded).
# ---------------------------------------------------------------------------
FEATURE_COLS = [
    # capital
    "eq_asset", "RBC1AAJ", "RBCRWAJ",
    # asset quality
    "nclnls_loans", "lnatres_loans", "ore_asset", "ntlnls_q_loans", "p9asset_asset", "naasset_asset",
    # efficiency / earnings
    "nonix_q_asset", "ROA", "ROE", "NIMY", "netinc_q_asset", "nim_q_asset",
    # liquidity / funding
    "chbal_asset", "sc_asset", "bro_dep", "lnlsnet_dep",
    # size / composition / growth
    "log_asset", "lnre_loans", "asset_growth_yoy", "loan_growth_yoy",
]
LABEL_COLS = ["y_fail_4q", "y_assist_or_fail_4q"]
FORBIDDEN_FEATURE_COLS = [
    "y_fail_4q", "y_assist_or_fail_4q", "label_observable", "split",
    "COST", "RESTYPE", "ACTIVE", "ENDEFYMD", "CHANGEC1",
    "cost_status", "cost_effective", "fail_dt", "censor_dt", "exit_class",
]
FEATURE_PATH = PROCESSED_DATA_DIR / "features.parquet"

# Expanding-window CV on DEV: (train_end, val_start = train_end + 12 months, val_end)
CV_FOLDS = [
    ("1986-12-31", "1987-12-31", "1989-12-31"),
    ("1989-12-31", "1990-12-31", "1992-12-31"),
    ("1992-12-31", "1993-12-31", "2006-12-31"),
]
