"""Step 05 - Polars vs pandas benchmark, transformation T1: de-cumulating YTD flows (same semantics in both).
Every measurement runs in a fresh subprocess, so peak memory of one run never contaminates another."""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as C

FLOWS = C.YTD_FLOW_COLUMNS
SRC = C.PROCESSED_DATA_DIR / "panel_decumulated.parquet"
BENCH_DIR = C.PROCESSED_DATA_DIR / "bench"
ENGINES = ["pandas", "pandas_groupby_diff", "polars_lazy", "polars_streaming", "polars_inmemory"]


def bench_path(k):
    return BENCH_DIR / f"ytd_input_x{k}.parquet"


def prep(scales):
    """Narrow input (CERT, REPDTE, 7 YTD flows). Scale k = k copies with shifted CERT (keys stay unique)."""
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    base = (pl.read_parquet(SRC, columns=["CERT", "REPDTE"] + FLOWS)
              .with_columns(pl.col("REPDTE").cast(pl.Datetime("ms"))))
    for k in scales:
        big = pl.concat([base.with_columns(pl.col("CERT") + i * 10_000_000) for i in range(k)])
        big.write_parquet(bench_path(k), compression="zstd")
        print(f"x{k}: {big.height:,} rows -> {bench_path(k).name} "
              f"({bench_path(k).stat().st_size / 1e6:.0f} MB)")


def decumulate_polars_lazy(path):
    base = pl.scan_parquet(path).with_columns(
        pl.col("REPDTE").dt.year().alias("yr"),
        ((pl.col("REPDTE").dt.month() - 1) // 3 + 1).alias("q"))
    prev = (base.select(["CERT", "yr", "q"] + FLOWS).with_columns(pl.col("q") + 1)
                .rename({c: f"{c}_prev" for c in FLOWS}))
    return (base.join(prev, on=["CERT", "yr", "q"], how="left")
                .with_columns([pl.when(pl.col("q") == 1).then(pl.col(c))
                                 .otherwise(pl.col(c) - pl.col(f"{c}_prev")).alias(f"{c}_q") for c in FLOWS])
                .select(["CERT", "REPDTE"] + [f"{c}_q" for c in FLOWS]))


def decumulate_polars(path, mode="default"):
    """mode: default (whatever the installed Polars picks), streaming, inmemory."""
    out = decumulate_polars_lazy(path)
    if mode == "streaming":
        try:
            return out.collect(engine="streaming")
        except TypeError:                       # older Polars versions
            return out.collect(streaming=True)
    if mode == "inmemory":
        return out.collect(engine="in-memory")
    return out.collect()


def decumulate_pandas(path):
    df = pd.read_parquet(path)
    df["REPDTE"] = pd.to_datetime(df["REPDTE"])
    df["yr"] = df["REPDTE"].dt.year
    df["q"] = (df["REPDTE"].dt.month - 1) // 3 + 1
    prev = df[["CERT", "yr", "q"] + FLOWS].rename(columns={c: f"{c}_prev" for c in FLOWS})
    prev["q"] = prev["q"] + 1
    m = df.merge(prev, on=["CERT", "yr", "q"], how="left")
    for c in FLOWS:
        m[f"{c}_q"] = np.where(m["q"] == 1, m[c], m[c] - m[f"{c}_prev"])
    return m[["CERT", "REPDTE"] + [f"{c}_q" for c in FLOWS]]


def decumulate_pandas_diff(path):
    """Idiomatic but row-based pandas: differs from the calendar join only where the previous quarter row is missing."""
    df = pd.read_parquet(path)
    df["REPDTE"] = pd.to_datetime(df["REPDTE"])
    df["yr"] = df["REPDTE"].dt.year
    df["q"] = (df["REPDTE"].dt.month - 1) // 3 + 1
    df = df.sort_values(["CERT", "REPDTE"], kind="stable")
    g = df.groupby(["CERT", "yr"], sort=False)
    for c in FLOWS:
        df[f"{c}_q"] = g[c].diff().where(df["q"] > 1, df[c])
    return df[["CERT", "REPDTE"] + [f"{c}_q" for c in FLOWS]]


def run_engine(engine, path):
    if engine == "pandas":
        return decumulate_pandas(path)
    if engine == "pandas_groupby_diff":
        return decumulate_pandas_diff(path)
    if engine == "polars_streaming":
        return decumulate_polars(path, "streaming")
    if engine == "polars_inmemory":
        return decumulate_polars(path, "inmemory")
    return decumulate_polars(path)


def peak_memory_mb():
    """Peak resident memory of this process: working-set peak on Windows, ru_maxrss on Linux/macOS."""
    try:
        import psutil
        mi = psutil.Process().memory_info()
        if hasattr(mi, "peak_wset"):
            return mi.peak_wset / 1e6
    except ImportError:
        pass
    try:
        import resource
        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return (kb * 1024 if sys.platform.startswith("linux") else kb) / 1e6
    except ImportError:
        return float("nan")


def worker(engine, k):
    t0 = time.perf_counter()
    res = run_engine(engine, bench_path(k))
    sec = time.perf_counter() - t0
    print(json.dumps({"engine": engine, "scale": k, "rows": int(len(res)),
                      "seconds": sec, "peak_mb": peak_memory_mb()}))


def orchestrate(engines, scales, repeats, threads=None):
    rows = []
    for k in scales:
        for e in engines:
            runs, err = [], ""
            for _ in range(repeats):
                env = dict(os.environ)
                if threads:
                    env["POLARS_MAX_THREADS"] = str(threads)   # must be set before Polars is imported
                p = subprocess.run([sys.executable, __file__, "--worker", e, str(k)],
                                   capture_output=True, text=True, env=env)
                if p.returncode != 0:
                    err = p.stderr.strip().splitlines()[-1][:120] if p.stderr.strip() else "nonzero exit"
                    break
                runs.append(json.loads(p.stdout.strip().splitlines()[-1]))
            if runs and not err:
                s = [r["seconds"] for r in runs]
                rows.append({"scale": k, "engine": e, "threads": threads or "default", "rows": runs[0]["rows"], "sec_median": statistics.median(s),
                             "sec_min": min(s), "sec_max": max(s), "peak_mb": max(r["peak_mb"] for r in runs),
                             "status": "ok"})
            else:
                rows.append({"scale": k, "engine": e, "status": f"FAILED: {err}"})
            print(rows[-1], flush=True)
    t = pd.DataFrame(rows)
    suffix = f"_threads{threads}" if threads else ""
    t.to_csv(C.REPORT_DIR / f"benchmark_T1_decumulate{suffix}.csv", index=False)
    pd.set_option("display.width", 250)
    print("\n", t.round(2).to_string(index=False))


def thread_sweep(scale, threads_list, repeats):
    """Settings are interleaved inside each repetition, so that session drift (heat, background load)
    hits every setting equally. A pandas run is included as same-session reference."""
    settings = [("pandas", "pandas", None), ("polars_default_threads", "polars_lazy", None),
                ("polars_inmemory_default", "polars_inmemory", None)]
    settings += [(f"polars_{t}_threads", "polars_lazy", str(t)) for t in threads_list]
    res = {name: [] for name, _, _ in settings}
    for _ in range(repeats):
        for name, engine, thr in settings:
            env = dict(os.environ)
            if thr:
                env["POLARS_MAX_THREADS"] = thr
            p = subprocess.run([sys.executable, __file__, "--worker", engine, str(scale)],
                               capture_output=True, text=True, env=env)
            if p.returncode != 0:
                print(f"{name}: FAILED {p.stderr.strip().splitlines()[-1][:150] if p.stderr.strip() else ''}")
                res[name] = None
                continue
            if res[name] is not None:
                res[name].append(json.loads(p.stdout.strip().splitlines()[-1]))
    rows = []
    for name, _, _ in settings:
        v = res[name]
        if not v:
            rows.append({"setting": name, "status": "FAILED"})
            continue
        s_ = [r["seconds"] for r in v]
        rows.append({"setting": name, "scale": scale, "sec_median": statistics.median(s_), "sec_min": min(s_),
                     "sec_max": max(s_), "peak_mb": max(r["peak_mb"] for r in v), "status": "ok"})
    t = pd.DataFrame(rows)
    t.to_csv(C.REPORT_DIR / "benchmark_T1_thread_sweep.csv", index=False)
    pd.set_option("display.width", 250)
    print(t.round(2).to_string(index=False))


def compare(a, b, tag, strict=False):
    assert a.height == b.height, f"{tag}: row counts differ {a.height} vs {b.height}"
    j = a.join(b, on=["CERT", "REPDTE"], how="inner", suffix="_b")
    assert j.height == a.height, f"{tag}: key sets differ"
    for c in [f"{x}_q" for x in FLOWS]:
        null_mismatch = j.filter(pl.col(c).is_null() != pl.col(f"{c}_b").is_null()).height
        both = j.filter(pl.col(c).is_not_null() & pl.col(f"{c}_b").is_not_null())
        mx = both.select((pl.col(c) - pl.col(f"{c}_b")).abs().max()).item() if both.height else 0.0
        print(f"  {tag} {c:10} null-pattern mismatches: {null_mismatch:6d} | max |diff|: {mx}")
        if strict:
            assert null_mismatch == 0 and mx == 0.0, f"{tag}: {c} differs"


def verify():
    path = bench_path(1)
    truth = (pl.read_parquet(SRC, columns=["CERT", "REPDTE"] + [f"{c}_q" for c in FLOWS])
               .with_columns(pl.col("REPDTE").cast(pl.Datetime("ms"))))
    pd_res = pl.from_pandas(decumulate_pandas(path)).with_columns(pl.col("REPDTE").cast(pl.Datetime("ms")))
    pl_res = decumulate_polars(path).with_columns(pl.col("REPDTE").cast(pl.Datetime("ms")))
    print("Polars lazy vs step-04 output:");  compare(pl_res, truth, "polars vs 04 ", strict=True)
    print("pandas vs step-04 output:");       compare(pd_res, truth, "pandas vs 04 ", strict=True)
    print("Polars vs pandas:");               compare(pl_res, pd_res, "polars vs pd", strict=True)
    print("pandas groupby-diff (row-based) vs step-04 output; differences are EXPECTED only where the previous quarter row is missing:")
    di = pl.from_pandas(decumulate_pandas_diff(path)).with_columns(pl.col("REPDTE").cast(pl.Datetime("ms")))
    compare(di, truth, "diff   vs 04 ")
    print("VERIFY PASSED")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prep", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--worker", nargs=2, metavar=("ENGINE", "SCALE"))
    ap.add_argument("--scales", type=int, nargs="+", default=[1, 2, 4])
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--sweep", type=int, nargs="+", default=None, help="thread counts for the interleaved sweep")
    ap.add_argument("--sweep-scale", type=int, default=4)
    ap.add_argument("--engines", nargs="+", default=ENGINES)
    a = ap.parse_args()
    if a.worker:
        worker(a.worker[0], int(a.worker[1]))
    elif a.sweep:
        thread_sweep(a.sweep_scale, a.sweep, a.repeats)
    elif a.prep:
        prep(a.scales)
    elif a.verify:
        verify()
    else:
        orchestrate(a.engines, a.scales, a.repeats, a.threads)