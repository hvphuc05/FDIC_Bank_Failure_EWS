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