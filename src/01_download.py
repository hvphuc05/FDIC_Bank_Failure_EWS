"""
01_download.py - Download the FDIC datasets required by BFT65B Part 1.

Datasets (all justified by the assignment text, section 2.2):
    financials   quarterly Call Reports, 1984Q1 -> 2026Q2  (one Parquet file per quarter)
    failures     complete list of failed banks
    institutions complete list of banks (incl. why reporting ended)

Design (built on the instructor's starter script):
    * resumable: a quarter is skipped only if its file exists AND its sha256 matches
      the manifest AND the requested columns are unchanged
    * completeness: rows downloaded == meta.total reported by the API, no duplicate keys,
      every row has the requested REPDTE
    * files are written atomically (temp file + rename); a failed validation never
      leaves a file under the final name
    * values are NOT modified. Only the internal API field "ID" is dropped (as in the
      starter) and columns are ordered as requested.

Usage (from the project root):
    python src/01_download.py                              # full download
    python src/01_download.py --start 2023-03-31 --end 2023-12-31   # quick test
    python src/01_download.py --verify-only                # offline re-check, no network
    python src/01_download.py --force                      # ignore existing files

Exit code: 0 = everything complete and verified, 1 = at least one error.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

# `python src/01_download.py` puts src/ (not the project root) on sys.path,
# so add the root to be able to `import config`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import requests  # noqa: E402

import config  # noqa: E402

SCRIPT_VERSION = "1.0"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_CONSECUTIVE_FAILURES = 3

LOG = logging.getLogger("fdic_download")


class DownloadError(RuntimeError):
    """Raised when a download cannot be completed or fails validation."""


@dataclass(frozen=True)
class DatasetSpec:
    """Describes one FDIC table: endpoint, fields and its key."""
    name: str
    endpoint: str
    fields: list[str]
    key: list[str]
    key_dup_is_error: bool  # duplicate key -> error (True) or warning (False)


FINANCIALS = DatasetSpec("financials", "financials", config.FDIC_FINANCIAL_FIELDS,
                         ["CERT", "REPDTE"], True)
FAILURES = DatasetSpec("failures", "failures", config.FDIC_FAILURE_FIELDS, ["ID"], True)
INSTITUTIONS = DatasetSpec("institutions", "institutions", config.FDIC_INSTITUTION_FIELDS,
                           ["CERT"], False)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def setup_logging(out_dir: Path) -> None:
    """Log to console and to <out_dir>/download.log."""
    out_dir.mkdir(parents=True, exist_ok=True)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%H:%M:%S")
    LOG.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(sys.stdout),
                    logging.FileHandler(out_dir / "download.log", encoding="utf-8")):
        handler.setFormatter(fmt)
        LOG.addHandler(handler)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    """SHA-256 fingerprint of a file (read in 1 MB chunks)."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def quarter_ends(start: date, end: date) -> list[str]:
    """All quarter-end dates (YYYYMMDD) between start and end, inclusive."""
    ends = []
    for year in range(start.year, end.year + 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            q = date(year, month, day)
            if start <= q <= end:
                ends.append(q.strftime("%Y%m%d"))
    return ends


def human_size(n_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n_bytes < 1024 or unit == "GB":
            return f"{n_bytes:,.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:,.1f} GB"


def load_manifest(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"meta": {"created_utc": utc_now()}, "datasets": {}}


def save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    """Atomic write so an interruption cannot corrupt the manifest."""
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# HTTP layer: timeout, retry with back-off, rate-limit handling
# ---------------------------------------------------------------------------
def get_json(session: requests.Session, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    """GET one API page and return decoded JSON.

    Retries timeouts, connection errors, HTTP 429 and 5xx with exponential
    back-off (respecting Retry-After). Other HTTP errors are not retried
    because repeating a wrong request cannot fix it.
    """
    url = f"{config.FDIC_API_BASE}/{endpoint}"
    last_problem = "unknown"
    for attempt in range(1, config.FDIC_MAX_RETRIES + 1):
        wait = min(60, 2 ** attempt)
        try:
            resp = session.get(url, params=params, timeout=config.FDIC_REQUEST_TIMEOUT_SEC)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_problem = type(exc).__name__
        else:
            if resp.status_code == 200:
                try:
                    payload = resp.json()
                except ValueError:
                    last_problem = "response is not valid JSON"
                else:
                    time.sleep(config.FDIC_PAUSE_SEC)
                    return payload
            elif resp.status_code in RETRYABLE_STATUS:
                last_problem = f"HTTP {resp.status_code}"
                retry_after = resp.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    wait = max(wait, int(retry_after))
            else:
                raise DownloadError(f"HTTP {resp.status_code} for {url} "
                                    f"(not retryable). Body: {resp.text[:200]!r}")
        LOG.warning("%s on %s (attempt %d/%d) - waiting %ds",
                    last_problem, endpoint, attempt, config.FDIC_MAX_RETRIES, wait)
        time.sleep(wait)
    raise DownloadError(f"Giving up on {url} after {config.FDIC_MAX_RETRIES} attempts "
                        f"({last_problem})")


def fetch_all(session: requests.Session, spec: DatasetSpec,
              filters: str | None = None) -> tuple[list[dict[str, Any]], int, int]:
    """Page through an endpoint with limit/offset.

    Returns (records, total reported by the API, number of pages).
    """
    records: list[dict[str, Any]] = []
    total: int | None = None
    offset, pages = 0, 0
    while True:
        params: dict[str, Any] = {
            "fields": ",".join(spec.fields),
            "limit": config.FDIC_PAGE_SIZE,
            "offset": offset,
            "sort_by": "CERT",
            "sort_order": "ASC",
            "format": "json",
        }
        if filters:
            params["filters"] = filters
        payload = get_json(session, spec.endpoint, params)

        try:
            page_total = int(payload["meta"]["total"])
            page = [rec["data"] for rec in payload["data"]]
        except (KeyError, TypeError, ValueError) as exc:
            raise DownloadError(f"Unexpected JSON structure from {spec.endpoint}: {exc!r}") from exc

        if total is None:
            total = page_total
        elif page_total != total:
            raise DownloadError(f"API total changed during pagination ({total} -> {page_total}); "
                                "the data were updated while downloading - rerun.")
        if not page:
            break
        records.extend(page)
        pages += 1
        offset += len(page)
        if offset >= total:
            break
    return records, int(total or 0), pages


def latest_repdte(session: requests.Session) -> str:
    """Newest report date available in /financials (YYYYMMDD)."""
    payload = get_json(session, "financials", {"fields": "REPDTE", "sort_by": "REPDTE",
                                               "sort_order": "DESC", "limit": 1, "format": "json"})
    try:
        raw = payload["data"][0]["data"]["REPDTE"]
    except (KeyError, IndexError, TypeError) as exc:
        raise DownloadError(f"Cannot read latest REPDTE: {exc!r}") from exc
    return pd.to_datetime(str(raw), format="mixed").strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def to_frame(records: list[dict[str, Any]], spec: DatasetSpec) -> tuple[pd.DataFrame, list[str]]:
    """Build a DataFrame with exactly the requested columns (values untouched)."""
    warnings: list[str] = []
    df = pd.DataFrame(records)
    if "ID" not in spec.fields:  # keep ID only where it was requested
        df = df.drop(columns=["ID"], errors="ignore")
    missing = [c for c in spec.fields if c not in df.columns]
    extra = [c for c in df.columns if c not in spec.fields]
    if missing:
        warnings.append(f"requested columns absent from the response: {missing}")
    if extra:
        warnings.append(f"unrequested columns dropped: {extra}")
    return df.reindex(columns=spec.fields), warnings


def validate_frame(df: pd.DataFrame, spec: DatasetSpec, api_total: int,
                   expected_repdte: str | None) -> tuple[list[str], list[str]]:
    """Return (errors, warnings). Any error means the download is rejected."""
    errors: list[str] = []
    warnings: list[str] = []

    if len(df) != api_total:
        errors.append(f"row count {len(df):,} != API total {api_total:,}")
    for col in spec.key:
        n_null = int(df[col].isna().sum())
        if n_null:
            (errors if spec.key_dup_is_error else warnings).append(
                f"{n_null} null values in key column {col}")
    n_dup_key = int(df.duplicated(spec.key).sum())
    if n_dup_key:
        (errors if spec.key_dup_is_error else warnings).append(
            f"{n_dup_key} duplicate rows on key {spec.key}")

    # Failures: the key is ID, but CERT is what links a failure to the financials,
    # so its gaps and repeats are reported (not rejected, not removed).
    if spec.name == "failures" and "CERT" in df.columns:
        n_null_cert = int(df["CERT"].isna().sum())
        if n_null_cert:
            warnings.append(f"{n_null_cert} null CERT (cannot be joined to financials)")
        n_rep_cert = int(df["CERT"].dropna().duplicated().sum())
        if n_rep_cert:
            warnings.append(f"{n_rep_cert} repeated non-null CERT values")

    n_dup_row = int(df.duplicated().sum())
    if n_dup_row:
        errors.append(f"{n_dup_row} fully duplicated rows")

    if expected_repdte is not None and len(df):
        dates = pd.to_datetime(df["REPDTE"].astype(str), format="mixed").dt.strftime("%Y%m%d")
        n_bad = int((dates != expected_repdte).sum())
        if n_bad:
            errors.append(f"{n_bad} rows with REPDTE != {expected_repdte}")

    all_null = [c for c in df.columns if df[c].isna().all()]
    if all_null and len(df):
        warnings.append(f"columns entirely null: {all_null}")
    if spec.name == "institutions" and "ACTIVE" in df.columns and df["ACTIVE"].nunique() < 2:
        warnings.append("ACTIVE has a single value: inactive banks may be missing")
    return errors, warnings


# ---------------------------------------------------------------------------
# Download of one table / one quarter
# ---------------------------------------------------------------------------
def existing_file_is_valid(entry: dict[str, Any] | None, target: Path, spec: DatasetSpec) -> bool:
    """True if a previous download can be trusted and skipped."""
    return bool(
        entry
        and entry.get("status") == "complete"
        and entry.get("columns") == spec.fields
        and target.exists()
        and target.stat().st_size == entry.get("bytes")
        and sha256_file(target) == entry.get("sha256")
    )


def download_one(session: requests.Session, spec: DatasetSpec, key: str, target: Path,
                 manifest: dict[str, Any], *, filters: str | None = None,
                 expected_repdte: str | None = None, allow_empty: bool = False,
                 force: bool = False) -> dict[str, Any]:
    """Download, validate and store one table (or one quarter). Returns manifest entry."""
    entry = manifest["datasets"].get(key)
    if not force and existing_file_is_valid(entry, target, spec):
        LOG.info("%-24s valid file already on disk, skipped", key)
        return entry  # type: ignore[return-value]

    t0 = time.time()
    records, api_total, pages = fetch_all(session, spec, filters)
    base = {"endpoint": spec.endpoint, "filters": filters, "api_total": api_total,
            "pages": pages, "columns": spec.fields, "downloaded_at_utc": utc_now()}

    if not records:
        if allow_empty and api_total == 0:
            LOG.warning("%-24s no rows (quarter later than newest published report)", key)
            return {**base, "status": "not_published", "rows": 0, "errors": [],
                    "warnings": ["no rows returned for a quarter after the latest published one"]}
        raise DownloadError(f"{key}: API returned no rows (total={api_total})")

    df, warnings = to_frame(records, spec)
    errors, more_warnings = validate_frame(df, spec, api_total, expected_repdte)
    warnings += more_warnings
    if errors:
        return {**base, "status": "failed", "rows": len(df), "errors": errors,
                "warnings": warnings}

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False, engine="pyarrow", compression="snappy")
    os.replace(tmp, target)

    entry = {**base, "status": "complete", "rows": len(df), "n_columns": df.shape[1],
             "file": target.relative_to(config.FDIC_RAW_DIR).as_posix() if
             target.is_relative_to(config.FDIC_RAW_DIR) else target.name,
             "bytes": target.stat().st_size, "sha256": sha256_file(target),
             "elapsed_sec": round(time.time() - t0, 1), "errors": [], "warnings": warnings}
    LOG.info("%-24s %9s rows | API total %9s | %s | OK (%.1fs)", key, f"{len(df):,}",
             f"{api_total:,}", human_size(entry["bytes"]), entry["elapsed_sec"])
    for w in warnings:
        LOG.warning("%-24s %s", key, w)
    return entry


def download_financials(session: requests.Session, out_dir: Path, quarters: list[str],
                        latest: str, manifest: dict[str, Any], manifest_path: Path,
                        force: bool) -> None:
    """Download one Parquet file per quarter; stop after repeated failures."""
    consecutive_failures = 0
    for i, q in enumerate(quarters, 1):
        key = f"financials/{q}"
        target = out_dir / "financials" / f"REPDTE={q}.parquet"
        LOG.info("[%d/%d] quarter %s", i, len(quarters), q)
        try:
            entry = download_one(session, FINANCIALS, key, target, manifest,
                                 filters=f"REPDTE:{q}", expected_repdte=q,
                                 allow_empty=q > latest, force=force)
            consecutive_failures = 0 if entry["status"] != "failed" else consecutive_failures + 1
        except DownloadError as exc:
            LOG.error("%s failed: %s", key, exc)
            entry = {"status": "failed", "rows": 0, "errors": [str(exc)], "warnings": [],
                     "downloaded_at_utc": utc_now()}
            consecutive_failures += 1
        if entry["status"] == "failed":
            for msg in entry["errors"]:
                LOG.error("%s validation: %s", key, msg)
        manifest["datasets"][key] = entry
        save_manifest(manifest_path, manifest)  # save after every quarter (resumable)
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            raise DownloadError(f"{MAX_CONSECUTIVE_FAILURES} consecutive failures - "
                                "stopping. Check your connection and rerun (it will resume).")


def download_small_table(session: requests.Session, spec: DatasetSpec, out_dir: Path,
                         manifest: dict[str, Any], manifest_path: Path, force: bool) -> None:
    key = spec.name
    try:
        entry = download_one(session, spec, key, out_dir / f"{spec.name}.parquet",
                             manifest, force=force)
    except DownloadError as exc:
        LOG.error("%s failed: %s", key, exc)
        entry = {"status": "failed", "rows": 0, "errors": [str(exc)], "warnings": [],
                 "downloaded_at_utc": utc_now()}
    for msg in entry.get("errors", []):
        LOG.error("%s validation: %s", key, msg)
    manifest["datasets"][key] = entry
    save_manifest(manifest_path, manifest)


# ---------------------------------------------------------------------------
# Offline verification and summary (re-reads files from disk, no network)
# ---------------------------------------------------------------------------
def verify_local(out_dir: Path, manifest: dict[str, Any], quarters: list[str]) -> bool:
    """Re-check files on disk against the manifest and print a summary."""
    ds = manifest["datasets"]
    problems: list[str] = []
    n_rows = n_bytes = n_complete = 0
    certs: set[int] = set()
    unpublished: list[str] = []

    for q in quarters:
        entry = ds.get(f"financials/{q}")
        path = out_dir / "financials" / f"REPDTE={q}.parquet"
        if entry is None:
            problems.append(f"{q}: not in manifest (never downloaded)")
        elif entry["status"] == "not_published":
            unpublished.append(q)
        elif entry["status"] != "complete":
            problems.append(f"{q}: status={entry['status']} {entry.get('errors')}")
        elif not path.exists():
            problems.append(f"{q}: file missing")
        elif sha256_file(path) != entry["sha256"]:
            problems.append(f"{q}: sha256 mismatch (file changed or corrupted)")
        else:
            meta_rows = pq.ParquetFile(path).metadata.num_rows
            keys = pd.read_parquet(path, columns=["CERT", "REPDTE"])
            if not (meta_rows == entry["rows"] == entry["api_total"]):
                problems.append(f"{q}: rows file={meta_rows} manifest={entry['rows']} "
                                f"api_total={entry['api_total']}")
            if keys.duplicated().any():
                problems.append(f"{q}: duplicate (CERT, REPDTE)")
            n_complete += 1
            n_rows += meta_rows
            n_bytes += path.stat().st_size
            certs.update(keys["CERT"].dropna().astype("int64").tolist())

    LOG.info("")
    LOG.info("=" * 78)
    LOG.info("SUMMARY (verified from files on disk)")
    LOG.info("=" * 78)
    LOG.info("%-14s quarters=%d/%d  rows=%s  distinct CERT=%s  size=%s",
             "financials", n_complete, len(quarters), f"{n_rows:,}", f"{len(certs):,}",
             human_size(n_bytes))
    if unpublished:
        LOG.warning("financials: quarters not yet published by FDIC: %s", unpublished)
    if (n_complete == len(quarters)
            and abs(n_rows - config.EXPECTED_FINANCIAL_ROWS) > 0.05 * config.EXPECTED_FINANCIAL_ROWS):
        LOG.warning("financials rows differ >5%% from the ~%s quoted in the assignment",
                    f"{config.EXPECTED_FINANCIAL_ROWS:,}")

    for spec in (FAILURES, INSTITUTIONS):
        entry = ds.get(spec.name)
        path = out_dir / f"{spec.name}.parquet"
        if not entry or entry["status"] != "complete" or not path.exists():
            problems.append(f"{spec.name}: missing or not complete")
            LOG.error("%-14s NOT COMPLETE", spec.name)
            continue
        if sha256_file(path) != entry["sha256"]:
            problems.append(f"{spec.name}: sha256 mismatch")
        meta_rows = pq.ParquetFile(path).metadata.num_rows
        if not (meta_rows == entry["rows"] == entry["api_total"]):
            problems.append(f"{spec.name}: row count mismatch")
        LOG.info("%-14s rows=%s  columns=%d  size=%s  file=%s  status=%s",
                 spec.name, f"{meta_rows:,}", entry["n_columns"], human_size(path.stat().st_size),
                 path.relative_to(config.PROJECT_ROOT) if path.is_relative_to(config.PROJECT_ROOT)
                 else path, entry["status"])
        for w in entry.get("warnings", []):
            LOG.warning("%-14s %s", spec.name, w)
        if spec is FAILURES and abs(meta_rows - config.EXPECTED_FAILURE_ROWS) > 0.15 * config.EXPECTED_FAILURE_ROWS:
            LOG.warning("failures rows differ >15%% from the ~%s quoted in the assignment",
                        f"{config.EXPECTED_FAILURE_ROWS:,}")

    ok = not problems
    for p in problems:
        LOG.error("PROBLEM: %s", p)
    LOG.info("RESULT: %s", "ALL CHECKS PASSED" if ok else f"{len(problems)} PROBLEM(S) FOUND")
    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Download FDIC data for BFT65B Part 1.")
    ap.add_argument("--start", default=config.FDIC_START_DATE, help="first quarter-end YYYY-MM-DD")
    ap.add_argument("--end", default=config.FDIC_END_DATE,
                    help="last quarter-end YYYY-MM-DD, or 'latest'")
    ap.add_argument("--out", default=str(config.FDIC_RAW_DIR), help="output folder")
    ap.add_argument("--force", action="store_true", help="re-download even if files are valid")
    ap.add_argument("--verify-only", action="store_true", help="only re-check files on disk")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out)
    setup_logging(out_dir)
    manifest_path = out_dir / "manifest.json"
    manifest = load_manifest(manifest_path)

    session = requests.Session()
    session.headers.update({"User-Agent": "BFT65B-student-download/1.0"})

    try:
        if args.verify_only:
            end = date.fromisoformat(args.end if args.end != "latest" else "2100-12-31")
            quarters = quarter_ends(date.fromisoformat(args.start), end)
            if args.end == "latest":
                quarters = [q for q in quarters if f"financials/{q}" in manifest["datasets"]]
        else:
            latest = latest_repdte(session)
            end_str = args.end if args.end != "latest" else \
                datetime.strptime(latest, "%Y%m%d").strftime("%Y-%m-%d")
            quarters = quarter_ends(date.fromisoformat(args.start), date.fromisoformat(end_str))
            LOG.info("Latest report date published by FDIC: %s", latest)
            LOG.info("Downloading %d quarters %s -> %s into %s", len(quarters), quarters[0],
                     quarters[-1], out_dir.resolve())
            manifest["meta"].update({
                "script_version": SCRIPT_VERSION, "api_base": config.FDIC_API_BASE,
                "page_size": config.FDIC_PAGE_SIZE, "last_run_utc": utc_now(),
                "requested_range": [quarters[0], quarters[-1]],
                "latest_repdte_at_run": latest,
                "versions": {"python": sys.version.split()[0], "pandas": pd.__version__,
                             "requests": requests.__version__},
            })
            save_manifest(manifest_path, manifest)
            download_financials(session, out_dir, quarters, latest, manifest,
                                manifest_path, args.force)
            download_small_table(session, FAILURES, out_dir, manifest, manifest_path, args.force)
            download_small_table(session, INSTITUTIONS, out_dir, manifest, manifest_path,
                                 args.force)
        ok = verify_local(out_dir, manifest, quarters)
    except DownloadError as exc:
        LOG.error("CRITICAL: %s", exc)
        return 1
    except KeyboardInterrupt:
        LOG.warning("Interrupted by user. Progress is saved; rerun to resume.")
        return 130
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())