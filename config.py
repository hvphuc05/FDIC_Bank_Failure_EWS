from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent

# Data directories
RAW_DATA_DIR = PROJECT_ROOT / "data_raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data_processed"

# Project directories
SRC_DIR = PROJECT_ROOT / "src"
NOTEBOOK_DIR = PROJECT_ROOT / "notebooks"
REPORT_DIR = PROJECT_ROOT / "report"

# Reproducibility
RANDOM_SEED = 42