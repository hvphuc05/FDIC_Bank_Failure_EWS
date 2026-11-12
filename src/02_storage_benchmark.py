"""
02_storage_benchmark.py - BFT65B Part 1: storage layout design and measurements.

What it does
------------
1. Reads the 170 raw quarterly Parquet files written by 01_download.py.
2. Documents the schema problem: the files do not share one schema
   (a column is int64 in one quarter, double in another, null in a third).
3. Builds ONE unified table in two versions from the same data:
     naive  : REPDTE as string, CERT int64, every number as float64
     typed  : REPDTE date32, CERT int32 (if it fits), money amounts int64
              (only if every value is a whole number), ratios float64
   Values are never changed: casts are checked and the script reports
   null counts and sums of every column before and after.
4. Writes the same data in several layouts (CSV, Parquet with different schema,
   compression and partitioning), checks they contain the same data, then measures
   size, write time, read time (all columns / 5 columns / one quarter).
5. Saves tables, a figure and the environment description under report/.

Usage (from the project root, virtual environment active):
    python src/02_storage_benchmark.py --quick     # smoke test on the last 8 quarters
    python src/02_storage_benchmark.py             # full benchmark (several minutes)
    python src/02_storage_benchmark.py --keep-files

Outputs (suffix _quick in quick mode, so test runs never overwrite real results):
    report/schema_report.csv
    report/storage_benchmark_results.csv
    report/storage_benchmark_meta.json
    report/figures/storage_benchmark.png
Scratch files go to data_processed/_storage_benchmark_tmp/ and are deleted at the end
unless --keep-files is given.

Exit code: 0 = all layouts verified, 1 = error or a layout failed verification.
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import math
import os
import platform
import shutil
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

# `python src/02_storage_benchmark.py` puts src/ on sys.path, add the project root for `config`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import matplotlib  # noqa: E402

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import psutil  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.compute as pc  # noqa: E402
import pyarrow.csv as pacsv  # noqa: E402
import pyarrow.dataset as ds  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

import config  # noqa: E402

LOG = logging.getLogger("storage_benchmark")

FIELDS = list(config.FDIC_FINANCIAL_FIELDS)
TEXT_AND_KEY_COLS = ["CERT", "REPDTE", "STNAME"]
RATIO_COLS = list(config.FDIC_RATIO_FIELDS)            # ratios in percent
NUMERIC_COLS = [c for c in FIELDS if c not in TEXT_AND_KEY_COLS]
AMOUNT_COLS = [c for c in NUMERIC_COLS if c not in RATIO_COLS]  # thousands of USD
SORT_KEYS = [("REPDTE", "ascending"), ("CERT", "ascending")]
FIVE_COLS = ["CERT", "REPDTE", "ASSET", "NETINC", "ROA"]

ROW_GROUP_SIZE = 50_000        # rows per Parquet row group (enables skipping by statistics)
PREFERRED_QUARTER = "20081231"  # quarter used for the "read one quarter" test
QUICK_N_QUARTERS = 8


class BenchmarkError(RuntimeError):
    """Raised when the benchmark cannot continue or data verification fails."""


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def setup_logging() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%H:%M:%S"))
    LOG.setLevel(logging.INFO)
    LOG.addHandler(handler)


def remove_path(path: Path) -> None:
    """Delete a file or folder; retry because Windows may hold a handle for a moment."""
    for attempt in range(5):
        try:
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
            return
        except OSError:
            gc.collect()
            time.sleep(0.5 * (attempt + 1))
    raise BenchmarkError(f"Cannot delete {path} (file in use?)")


def path_size(path: Path) -> tuple[int, int]:
    """(total bytes, number of files) of a file or a folder."""
    if path.is_file():
        return path.stat().st_size, 1
    files = [p for p in path.rglob("*") if p.is_file()]
    return sum(p.stat().st_size for p in files), len(files)


def mb(n_bytes: float) -> float:
    return n_bytes / 1024 / 1024


# ---------------------------------------------------------------------------
# Step 1: load raw files and document the schema problem
# ---------------------------------------------------------------------------
def list_quarters(raw_dir: Path, quick: bool) -> tuple[list[str], int]:
    """Complete quarters according to manifest.json, and their total row count."""
    manifest_path = raw_dir / "manifest.json"
    if not manifest_path.exists():
        raise BenchmarkError(f"{manifest_path} not found - run 01_download.py first")
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))["datasets"]
    quarters = sorted(k.split("/")[1] for k, v in entries.items()
                      if k.startswith("financials/") and v.get("status") == "complete")
    if not quarters:
        raise BenchmarkError("No complete financial quarter in the manifest")
    if quick:
        quarters = quarters[-QUICK_N_QUARTERS:]
    n_rows = sum(entries[f"financials/{q}"]["rows"] for q in quarters)
    return quarters, n_rows


def load_raw(raw_dir: Path, quarters: list[str]) -> dict[str, pa.Table]:
    tables: dict[str, pa.Table] = {}
    for q in quarters:
        path = raw_dir / "financials" / f"REPDTE={q}.parquet"
        if not path.exists():
            raise BenchmarkError(f"Missing file {path}")
        table = pq.ParquetFile(path).read()
        if table.schema.names != FIELDS:
            raise BenchmarkError(f"{path.name}: columns differ from config.FDIC_FINANCIAL_FIELDS")
        tables[q] = table
    return tables


def schema_evidence(raw: dict[str, pa.Table]) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Which types does each column have across quarters, and where is it entirely null?"""
    types: dict[str, set[str]] = {c: set() for c in FIELDS}
    all_null: dict[str, list[str]] = {c: [] for c in FIELDS}
    signatures: set[tuple[str, ...]] = set()
    for q, table in raw.items():
        signatures.add(tuple(str(f.type) for f in table.schema))
        for field in table.schema:
            types[field.name].add(str(field.type))
            if table[field.name].null_count == table.num_rows:
                all_null[field.name].append(q)

    try:
        pa.concat_tables(list(raw.values()))
        concat_msg = "pa.concat_tables without schema unification: succeeded"
    except pa.ArrowInvalid as exc:
        concat_msg = f"pa.concat_tables without schema unification FAILED: {str(exc)[:400]}"

    rows = [{"column": c, "types_across_quarters": " | ".join(sorted(types[c])),
             "n_quarters_all_null": len(all_null[c]),
             "first_all_null_quarter": all_null[c][0] if all_null[c] else "",
             "last_all_null_quarter": all_null[c][-1] if all_null[c] else ""} for c in FIELDS]
    info = {"n_distinct_raw_schemas": len(signatures), "concat_without_unification": concat_msg}
    return pd.DataFrame(rows), info


# ---------------------------------------------------------------------------
# Step 2: unified tables (naive and typed) - values are checked, never altered
# ---------------------------------------------------------------------------
def build_naive(raw: dict[str, pa.Table]) -> pa.Table:
    """All quarters in one table: REPDTE string, CERT int64, every number float64."""
    schema = pa.schema([
        (name, pa.int64() if name == "CERT" else pa.string() if name in ("REPDTE", "STNAME")
         else pa.float64()) for name in FIELDS])
    parts = [raw[q].cast(schema) for q in sorted(raw)]
    return pa.concat_tables(parts).sort_by(SORT_KEYS)


def choose_typed_schema(naive: pa.Table) -> dict[str, Any]:
    """Decide, from the data, which columns can be stored as integers without any loss."""
    limits = pc.min_max(naive["CERT"]).as_py()
    cert_int32 = limits["min"] >= -(2 ** 31) and limits["max"] <= 2 ** 31 - 1
    int_cols: list[str] = []
    float_cols: dict[str, str] = {}
    for name in AMOUNT_COLS:
        values = pc.drop_null(naive[name])
        whole = pc.all(pc.equal(pc.floor(values), values)).as_py()
        biggest = pc.max(pc.abs(values)).as_py()
        if whole is False:
            float_cols[name] = "has non-integer values"
        elif biggest is not None and biggest >= 2 ** 53:
            float_cols[name] = "values too large for exact float64"
        else:
            int_cols.append(name)
    return {"cert_int32": bool(cert_int32), "cert_min": limits["min"], "cert_max": limits["max"],
            "int64_amount_cols": int_cols, "float64_amount_cols": float_cols}


def build_typed(naive: pa.Table, decision: dict[str, Any]) -> pa.Table:
    """Same data with proper types. strptime raises on a bad date, safe casts raise on loss."""
    arrays = []
    for name in FIELDS:
        col = naive[name]
        if name == "REPDTE":
            col = pc.strptime(col, format="%Y%m%d", unit="s").cast(pa.date32())
        elif name == "CERT" and decision["cert_int32"]:
            col = col.cast(pa.int32())
        elif name in decision["int64_amount_cols"]:
            col = col.cast(pa.int64())
        arrays.append(col)
    return pa.table(arrays, names=FIELDS)


def fingerprint(table: pa.Table) -> dict[str, Any]:
    """Row count plus, per column, null count and sum (numbers) or distinct count (others)."""
    fp: dict[str, Any] = {"rows": table.num_rows}
    for name in FIELDS:
        col = table[name]
        entry: dict[str, Any] = {"nulls": col.null_count}
        if pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
            entry["sum"] = pc.sum(col.cast(pa.float64())).as_py() or 0.0
        else:
            entry["distinct"] = pc.count_distinct(col).as_py()
        fp[name] = entry
    return fp


def same_fingerprint(a: dict[str, Any], b: dict[str, Any]) -> bool:
    if a["rows"] != b["rows"]:
        return False
    for name in FIELDS:
        x, y = a[name], b[name]
        if x["nulls"] != y["nulls"]:
            return False
        if "sum" in x:
            if "sum" not in y or not math.isclose(x["sum"], y["sum"], rel_tol=1e-9, abs_tol=1e-6):
                return False
        elif x.get("distinct") != y.get("distinct"):
            return False
    return True


# ---------------------------------------------------------------------------
# Step 3: layouts, write, read, measure
# ---------------------------------------------------------------------------
@dataclass
class Layout:
    key: str
    description: str
    path: Path
    table_key: str                 # which in-memory table is written: naive/typed/typed_year/typed_qtr
    kind: str                      # csv | parquet_file | parquet_dataset
    compression: str | None
    partitioning: Any = None
    csv_types: pa.Schema | None = None


def make_layouts(tmp: Path, typed_schema: pa.Schema) -> list[Layout]:
    year_part = ds.partitioning(pa.schema([pa.field("YEAR", pa.int32())]), flavor="hive")
    qtr_part = ds.partitioning(pa.schema([pa.field("QTR", pa.string())]), flavor="hive")
    return [
        Layout("csv", "CSV, typed, uncompressed", tmp / "financials.csv", "typed", "csv", None,
               csv_types=typed_schema),
        Layout("pq_naive_snappy", "Parquet, naive schema, snappy", tmp / "naive_snappy.parquet",
               "naive", "parquet_file", "snappy"),
        Layout("pq_typed_snappy", "Parquet, typed schema, snappy", tmp / "typed_snappy.parquet",
               "typed", "parquet_file", "snappy"),
        Layout("pq_typed_zstd", "Parquet, typed schema, zstd", tmp / "typed_zstd.parquet",
               "typed", "parquet_file", "zstd"),
        Layout("pq_zstd_by_year", "Parquet, typed, zstd, partitioned by year", tmp / "typed_zstd_year",
               "typed_year", "parquet_dataset", "zstd", year_part),
        Layout("pq_zstd_by_quarter", "Parquet, typed, zstd, partitioned by quarter",
               tmp / "typed_zstd_qtr", "typed_qtr", "parquet_dataset", "zstd", qtr_part),
    ]


def write_layout(layout: Layout, tables: dict[str, pa.Table]) -> None:
    table = tables[layout.table_key]
    if layout.kind == "csv":
        pacsv.write_csv(table, str(layout.path))
    elif layout.kind == "parquet_file":
        pq.write_table(table, str(layout.path), compression=layout.compression,
                       row_group_size=ROW_GROUP_SIZE)
    else:
        options = ds.ParquetFileFormat().make_write_options(compression=layout.compression)
        ds.write_dataset(table, str(layout.path), format="parquet", partitioning=layout.partitioning,
                         file_options=options, existing_data_behavior="overwrite_or_ignore")


def read_layout(layout: Layout, columns: list[str] | None = None, flt: Any = None) -> pa.Table:
    """Read a layout. CSV has no index, so a filter can only be applied after parsing everything."""
    if layout.kind == "csv":
        kwargs: dict[str, Any] = {"column_types": layout.csv_types, "strings_can_be_null": False}
        if columns:
            kwargs["include_columns"] = columns
        table = pacsv.read_csv(str(layout.path), convert_options=pacsv.ConvertOptions(**kwargs))
        return ds.dataset(table).to_table(filter=flt) if flt is not None else table
    dataset = ds.dataset(str(layout.path), format="parquet", partitioning=layout.partitioning)
    return dataset.to_table(columns=columns, filter=flt)


def quarter_filter(layout: Layout, quarter: str) -> Any:
    """Filter expression for one quarter, using the partition column when the layout has one."""
    if layout.table_key == "naive":
        return ds.field("REPDTE") == quarter
    if layout.table_key == "typed_qtr":
        return ds.field("QTR") == quarter
    day = date(int(quarter[:4]), int(quarter[4:6]), int(quarter[6:]))
    expr = ds.field("REPDTE") == pa.scalar(day, type=pa.date32())
    if layout.table_key == "typed_year":
        expr = expr & (ds.field("YEAR") == int(quarter[:4]))
    return expr


def measure(op: Callable[[], Any], prepare: Callable[[], None] | None,
            repeats: int, warmup: int) -> list[float]:
    """Run op warmup+repeats times; return the timings of the last `repeats` runs (seconds)."""
    times: list[float] = []
    for i in range(warmup + repeats):
        if prepare is not None:
            prepare()
        gc.collect()
        start = time.perf_counter()
        result = op()
        elapsed = time.perf_counter() - start
        del result
        if i >= warmup:
            times.append(elapsed)
    return times


def stats(prefix: str, times: list[float]) -> dict[str, float]:
    return {f"{prefix}_s": statistics.median(times), f"{prefix}_s_min": min(times),
            f"{prefix}_s_max": max(times)}


def run_layout(layout: Layout, tables: dict[str, pa.Table], ref_table: pa.Table,
               ref_fp: dict[str, Any], quarter: str, repeats: int, warmup: int) -> dict[str, Any]:
    LOG.info("Layout %-20s write + verify", layout.key)
    prepare = lambda: remove_path(layout.path)  # noqa: E731
    prepare()
    write_layout(layout, tables)
    size_bytes, n_files = path_size(layout.path)

    back = read_layout(layout).select(FIELDS)
    fp_ok = same_fingerprint(ref_fp, fingerprint(back))
    exact = None
    if layout.table_key != "naive":
        exact = bool(back.sort_by(SORT_KEYS).equals(ref_table))
    del back

    flt = quarter_filter(layout, quarter)
    expected_q = ds.dataset(ref_table).count_rows(
        filter=ds.field("REPDTE") == pa.scalar(
            date(int(quarter[:4]), int(quarter[4:6]), int(quarter[6:])), type=pa.date32()))
    got_q = read_layout(layout, flt=flt).num_rows
    if got_q != expected_q:
        LOG.error("%s: quarter filter returned %d rows, expected %d", layout.key, got_q, expected_q)
        fp_ok = False

    LOG.info("Layout %-20s timing (%d warm-up + %d runs per operation)", layout.key, warmup, repeats)
    row: dict[str, Any] = {"layout": layout.key, "description": layout.description,
                           "files": n_files, "size_mb": mb(size_bytes),
                           "same_data_fingerprint": fp_ok, "exact_equal_to_reference": exact}
    row.update(stats("write", measure(lambda: write_layout(layout, tables), prepare, repeats, warmup)))
    row.update(stats("read_all", measure(lambda: read_layout(layout), None, repeats, warmup)))
    row.update(stats("read_5col", measure(lambda: read_layout(layout, columns=FIVE_COLS),
                                          None, repeats, warmup)))
    row.update(stats("read_quarter", measure(lambda: read_layout(layout, flt=flt),
                                             None, repeats, warmup)))
    LOG.info("Layout %-20s size %.1f MB | median s: write %.2f, read all %.2f, 5 cols %.2f, "
             "1 quarter %.2f", layout.key, row["size_mb"], row["write_s"], row["read_all_s"],
             row["read_5col_s"], row["read_quarter_s"])
    return row


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def plot_results(df: pd.DataFrame, out_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    x = np.arange(len(df))
    axes[0].bar(x, df["size_mb"], color="#4C72B0")
    axes[0].set_title("Size on disk (MB)")
    axes[1].set_title("Median time (s), log scale")
    ops = [("write_s", "write"), ("read_all_s", "read all"),
           ("read_5col_s", "read 5 columns"), ("read_quarter_s", "read 1 quarter")]
    width = 0.8 / len(ops)
    for i, (col, name) in enumerate(ops):
        axes[1].bar(x + (i - 1.5) * width, df[col], width, label=name)
    axes[1].set_yscale("log")
    axes[1].legend()
    for ax in axes:
        ax.set_xticks(x)
        ax.set_xticklabels(df["layout"], rotation=30, ha="right")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="BFT65B Part 1: storage layout benchmark.")
    ap.add_argument("--quick", action="store_true",
                    help=f"smoke test: last {QUICK_N_QUARTERS} quarters, at most 2 timed runs")
    ap.add_argument("--repeats", type=int, default=5, help="timed runs per operation (default 5)")
    ap.add_argument("--warmup", type=int, default=1, help="untimed warm-up runs (default 1)")
    ap.add_argument("--keep-files", action="store_true", help="keep the scratch layouts")
    return ap.parse_args()


def run(args: argparse.Namespace) -> bool:
    raw_dir = config.FDIC_RAW_DIR
    suffix = "_quick" if args.quick else ""
    repeats = min(args.repeats, 2) if args.quick else args.repeats
    config.REPORT_DIR.mkdir(exist_ok=True)
    (config.REPORT_DIR / "figures").mkdir(exist_ok=True)

    quarters, expected_rows = list_quarters(raw_dir, args.quick)
    LOG.info("Quarters: %d (%s -> %s), rows expected from manifest: %s",
             len(quarters), quarters[0], quarters[-1], f"{expected_rows:,}")

    raw = load_raw(raw_dir, quarters)
    raw_bytes = sum((raw_dir / "financials" / f"REPDTE={q}.parquet").stat().st_size for q in quarters)
    schema_df, schema_info = schema_evidence(raw)
    schema_df.to_csv(config.REPORT_DIR / f"schema_report{suffix}.csv", index=False)
    LOG.info("Distinct raw schemas: %d", schema_info["n_distinct_raw_schemas"])
    LOG.info("%s", schema_info["concat_without_unification"])

    naive = build_naive(raw)
    del raw
    gc.collect()
    if naive.num_rows != expected_rows:
        raise BenchmarkError(f"Rows {naive.num_rows:,} != manifest rows {expected_rows:,}")
    decision = choose_typed_schema(naive)
    typed = build_typed(naive, decision)
    LOG.info("Typed schema: CERT %s (max %s); %d amount columns int64; float64 amounts: %s",
             "int32" if decision["cert_int32"] else "int64", decision["cert_max"],
             len(decision["int64_amount_cols"]), decision["float64_amount_cols"] or "none")

    naive_fp, typed_fp = fingerprint(naive), fingerprint(typed)
    if not same_fingerprint(naive_fp, typed_fp):
        raise BenchmarkError("Typed table differs from naive table (null counts or sums)")
    LOG.info("Check passed: naive and typed tables have identical rows, null counts and sums")

    tables = {
        "naive": naive,
        "typed": typed,
        "typed_year": typed.append_column(
            "YEAR", pc.utf8_slice_codeunits(naive["REPDTE"], 0, 4).cast(pa.int32())),
        "typed_qtr": typed.append_column("QTR", naive["REPDTE"]),
    }
    quarter = PREFERRED_QUARTER if PREFERRED_QUARTER in quarters else quarters[len(quarters) // 2]

    tmp = config.PROCESSED_DATA_DIR / "_storage_benchmark_tmp"
    config.PROCESSED_DATA_DIR.mkdir(exist_ok=True)
    if tmp.exists():
        remove_path(tmp)
    tmp.mkdir()
    rows: list[dict[str, Any]] = []
    try:
        for layout in make_layouts(tmp, typed.schema):
            rows.append(run_layout(layout, tables, typed, typed_fp, quarter, repeats, args.warmup))
    finally:
        if not args.keep_files:
            remove_path(tmp)

    df = pd.DataFrame(rows)
    df["size_vs_csv"] = df["size_mb"] / df.loc[df["layout"] == "csv", "size_mb"].iloc[0]
    df.to_csv(config.REPORT_DIR / f"storage_benchmark_results{suffix}.csv", index=False)
    plot_results(df, config.REPORT_DIR / "figures" / f"storage_benchmark{suffix}.png")

    meta = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "quick_mode": args.quick, "repeats": repeats, "warmup": args.warmup,
        "python": platform.python_version(), "pyarrow": pa.__version__, "pandas": pd.__version__,
        "platform": platform.platform(), "cpu_logical": os.cpu_count(),
        "pyarrow_cpu_threads": pa.cpu_count(),
        "ram_total_mb": round(mb(psutil.virtual_memory().total)),
        "ram_available_mb_at_end": round(mb(psutil.virtual_memory().available)),
        "n_quarters": len(quarters), "first_quarter": quarters[0], "last_quarter": quarters[-1],
        "n_rows": naive.num_rows, "quarter_used_for_filter_test": quarter,
        "row_group_size": ROW_GROUP_SIZE,
        "raw_files_as_downloaded_mb": round(mb(raw_bytes), 1), "raw_files": len(quarters),
        "in_memory_mb_naive": round(mb(naive.nbytes), 1), "in_memory_mb_typed": round(mb(typed.nbytes), 1),
        "schema_decision": decision, **schema_info,
        "timing_note": "warm OS file cache; 1 warm-up run discarded; median of timed runs",
    }
    (config.REPORT_DIR / f"storage_benchmark_meta{suffix}.json").write_text(
        json.dumps(meta, indent=1, default=str), encoding="utf-8")

    show = df[["layout", "files", "size_mb", "size_vs_csv", "write_s", "read_all_s", "read_5col_s",
               "read_quarter_s", "same_data_fingerprint", "exact_equal_to_reference"]]
    LOG.info("\n%s", show.to_string(index=False, float_format=lambda v: f"{v:,.2f}"))
    LOG.info("Raw files as downloaded: %d files, %.1f MB", len(quarters), mb(raw_bytes))
    ok = bool(df["same_data_fingerprint"].all())
    LOG.info("RESULT: %s", "ALL LAYOUTS VERIFIED" if ok else "VERIFICATION FAILED FOR AT LEAST ONE LAYOUT")
    return ok


def main() -> int:
    args = parse_args()
    setup_logging()
    try:
        return 0 if run(args) else 1
    except BenchmarkError as exc:
        LOG.error("CRITICAL: %s", exc)
        return 1
    except KeyboardInterrupt:
        LOG.warning("Interrupted by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
