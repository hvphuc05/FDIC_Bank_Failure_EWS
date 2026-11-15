"""Step 05b - how Polars executes T1: full plans (unoptimised / optimised / streaming), the profile if the
installed version still offers it, and a stage-by-stage timing that works in every version."""
import importlib
import statistics
import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

b = importlib.import_module("05_polars_pandas_benchmark")


def timed(fn, repeats=3):
    fn()                                          # warm-up: file cache
    s = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        s.append(time.perf_counter() - t0)
    return statistics.median(s)


def stage_timing(path):
    scan = lambda: pl.scan_parquet(path)
    base = lambda: scan().with_columns(pl.col("REPDTE").dt.year().alias("yr"),
                                       ((pl.col("REPDTE").dt.month() - 1) // 3 + 1).alias("q"))

    def joined():
        bs = base()
        prev = (bs.select(["CERT", "yr", "q"] + b.FLOWS).with_columns(pl.col("q") + 1)
                  .rename({c: f"{c}_prev" for c in b.FLOWS}))
        return bs.join(prev, on=["CERT", "yr", "q"], how="left")

    stages = [("S0 metadata only (row count)", lambda: scan().select(pl.len()).collect()),
              ("S1 read and decode all 9 columns", lambda: scan().collect()),
              ("S2 + year and quarter columns", lambda: base().collect()),
              ("S3 + self-join on (CERT, year, quarter), all columns kept", lambda: joined().collect()),
              ("S4 full query (+ 7 de-cumulated columns, 9 output columns)",
               lambda: b.decumulate_polars_lazy(path).collect())]
    print("\n=== stage timing, seconds (median of 3; separate queries, so NOT additive: "
          "the optimiser prunes columns differently in each) ===")
    rows = []
    for name, fn in stages:
        rows.append((name, round(timed(fn), 3)))
        print(f"{rows[-1][1]:8.3f}  {name}")
    (C.REPORT_DIR / "polars_stage_timing_T1.csv").write_text(
        "stage,seconds\n" + "\n".join(f'"{n}",{t}' for n, t in rows), encoding="utf-8")


def main(scale=1):
    path = b.bench_path(scale)
    lf = b.decumulate_polars_lazy(path)
    print("polars version:", pl.__version__, "| thread pool size:", pl.thread_pool_size())
    plans = {"unoptimized": lf.explain(optimized=False), "optimized": lf.explain()}
    try:
        plans["streaming"] = lf.explain(engine="streaming")
    except Exception as e:
        print("streaming plan not available:", type(e).__name__, str(e)[:200])
    for name, txt in plans.items():
        (C.REPORT_DIR / f"polars_plan_T1_{name}.txt").write_text(txt, encoding="utf-8")
        print(f"\n=== {name} plan ({len(txt.splitlines())} lines) ===\n{txt}")
    try:
        res, timings = lf.profile()
        t = timings.with_columns((pl.col("end") - pl.col("start")).alias("duration_us")).sort("start")
        with pl.Config(tbl_rows=40, fmt_str_lengths=70):
            print("\n=== per-node timings (microseconds) ===")
            print(t)
        t.write_csv(C.REPORT_DIR / "polars_profile_T1.csv")
    except Exception as e:
        print("\nlf.profile() not available. Exact error:", type(e).__name__, "|", str(e)[:400])
    stage_timing(path)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 1)