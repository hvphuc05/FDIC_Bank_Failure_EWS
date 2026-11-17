"""Step 05c - Polars vs pandas benchmark, transformation T2: forward label with censoring (interval join).
T2 = for every bank-quarter: drop rows on/after the bank's last FAILURE date, flag y = 1 if a failure date falls in
(REPDTE, REPDTE + 12 months], leave the label null after the last labelable quarter. Same logic as step 04, stage 3.
Each measurement runs in a fresh subprocess."""
import argparse
import datetime as dt
import importlib
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

T1 = importlib.import_module("05_polars_pandas_benchmark")      # peak_memory_mb

KEYS_SRC = C.PROCESSED_DATA_DIR / "panel_decumulated.parquet"
EVENTS_SRC = C.PROCESSED_DATA_DIR / "failure_events.parquet"
BENCH_DIR = C.PROCESSED_DATA_DIR / "bench"
LAST_LABELED = dt.datetime.fromisoformat(C.LAST_LABELED_Q)
HORIZON_MONTHS = 3 * C.LABEL_HORIZON_QUARTERS                    # 12
LABEL_COLS = ["label_observable", "y_fail_4q", "y_assist_or_fail_4q"]
ENGINES = ["pandas", "polars_lazy", "polars_inmemory"]


def keys_path(k):
    return BENCH_DIR / f"t2_keys_x{k}.parquet"


def events_path(k):
    return BENCH_DIR / f"t2_events_x{k}.parquet"


def prep(scales):
    """Scale k = k copies with CERT shifted by 10 million (keys and events of each copy stay together)."""
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    keys = (pl.read_parquet(KEYS_SRC, columns=["CERT", "REPDTE"])
              .with_columns(pl.col("REPDTE").cast(pl.Datetime("ms"))))
    ev = (pl.read_parquet(EVENTS_SRC)
            .select(pl.col("cert_num").cast(pl.Int64).alias("CERT"), "RESTYPE",
                    pl.col("fail_dt").cast(pl.Datetime("ms")))
            .drop_nulls(["CERT", "fail_dt"]))
    for k in scales:
        shift = lambda df, i: df.with_columns(pl.col("CERT") + i * 10_000_000)
        pl.concat([shift(keys, i) for i in range(k)]).write_parquet(keys_path(k), compression="zstd")
        pl.concat([shift(ev, i) for i in range(k)]).write_parquet(events_path(k), compression="zstd")
        print(f"x{k}: keys {k * keys.height:,} rows, events {k * ev.height:,} rows")


# ----------------------------------------------------------------------------------------------- Polars
def label_polars_lazy(kp, ep):
    h = f"{HORIZON_MONTHS}mo"
    keys, ev = pl.scan_parquet(kp), pl.scan_parquet(ep)
    fails = ev.filter(pl.col("RESTYPE") == "FAILURE")
    censor = fails.group_by("CERT").agg(pl.col("fail_dt").max().alias("censor_dt"))

    def flag(events, name):
        return (keys.join(events.select("CERT", "fail_dt"), on="CERT", how="inner")
                    .filter((pl.col("fail_dt") > pl.col("REPDTE"))
                            & (pl.col("fail_dt") <= pl.col("REPDTE").dt.offset_by(h)))
                    .select("CERT", "REPDTE").unique()
                    .with_columns(pl.lit(1, dtype=pl.Int8).alias(name)))

    obs = pl.col("REPDTE") <= pl.lit(LAST_LABELED)
    return (keys.join(censor, on="CERT", how="left")
                .filter(pl.col("censor_dt").is_null() | (pl.col("REPDTE") < pl.col("censor_dt")))
                .join(flag(fails, "y_fail_4q"), on=["CERT", "REPDTE"], how="left")
                .join(flag(ev, "y_assist_or_fail_4q"), on=["CERT", "REPDTE"], how="left")
                .select("CERT", "REPDTE", obs.alias("label_observable"),
                        pl.when(obs).then(pl.col("y_fail_4q").fill_null(0)).otherwise(None)
                          .cast(pl.Int8).alias("y_fail_4q"),
                        pl.when(obs).then(pl.col("y_assist_or_fail_4q").fill_null(0)).otherwise(None)
                          .cast(pl.Int8).alias("y_assist_or_fail_4q")))


def label_polars(kp, ep, mode="default"):
    out = label_polars_lazy(kp, ep)
    if mode == "inmemory":
        return out.collect(engine="in-memory")
    return out.collect()


# ----------------------------------------------------------------------------------------------- pandas
def label_pandas(kp, ep):
    keys = pd.read_parquet(kp)
    keys["REPDTE"] = pd.to_datetime(keys["REPDTE"])
    ev = pd.read_parquet(ep)
    ev["fail_dt"] = pd.to_datetime(ev["fail_dt"])
    fails = ev[ev["RESTYPE"] == "FAILURE"]
    censor = fails.groupby("CERT", as_index=False)["fail_dt"].max().rename(columns={"fail_dt": "censor_dt"})
    kept = keys.merge(censor, on="CERT", how="left")
    kept = kept[kept["censor_dt"].isna() | (kept["REPDTE"] < kept["censor_dt"])].drop(columns="censor_dt")

    def flag(events, name):
        m = keys.merge(events[["CERT", "fail_dt"]], on="CERT", how="inner")
        m = m[(m["fail_dt"] > m["REPDTE"]) & (m["fail_dt"] <= m["REPDTE"] + pd.DateOffset(months=HORIZON_MONTHS))]
        return m[["CERT", "REPDTE"]].drop_duplicates().assign(**{name: np.int8(1)})

    out = (kept.merge(flag(fails, "y_fail_4q"), on=["CERT", "REPDTE"], how="left")
               .merge(flag(ev, "y_assist_or_fail_4q"), on=["CERT", "REPDTE"], how="left"))
    obs = out["REPDTE"] <= pd.Timestamp(LAST_LABELED)
    out["label_observable"] = obs
    for c in ("y_fail_4q", "y_assist_or_fail_4q"):
        out[c] = out[c].fillna(0).astype("int8").where(obs)          # NaN (null) when the label is not observable
    return out[["CERT", "REPDTE", "label_observable", "y_fail_4q", "y_assist_or_fail_4q"]]


def run_engine(engine, k):
    if engine == "pandas":
        return label_pandas(keys_path(k), events_path(k))
    return label_polars(keys_path(k), events_path(k), "inmemory" if engine == "polars_inmemory" else "default")


def worker(engine, k):
    t0 = time.perf_counter()
    res = run_engine(engine, k)
    sec = time.perf_counter() - t0
    print(json.dumps({"engine": engine, "scale": k, "rows": int(len(res)), "seconds": sec,
                      "peak_mb": T1.peak_memory_mb()}))


def orchestrate(engines, scales, repeats):
    rows = []
    for k in scales:
        for e in engines:
            runs, err = [], ""
            for _ in range(repeats):
                p = subprocess.run([sys.executable, __file__, "--worker", e, str(k)], capture_output=True, text=True)
                if p.returncode != 0:
                    err = p.stderr.strip().splitlines()[-1][:150] if p.stderr.strip() else "nonzero exit"
                    break
                runs.append(json.loads(p.stdout.strip().splitlines()[-1]))
            if runs and not err:
                s = [r["seconds"] for r in runs]
                rows.append({"scale": k, "engine": e, "rows": runs[0]["rows"], "sec_median": statistics.median(s),
                             "sec_min": min(s), "sec_max": max(s), "peak_mb": max(r["peak_mb"] for r in runs),
                             "status": "ok"})
            else:
                rows.append({"scale": k, "engine": e, "status": f"FAILED: {err}"})
            print(rows[-1], flush=True)
    t = pd.DataFrame(rows)
    t.to_csv(C.REPORT_DIR / "benchmark_T2_labels.csv", index=False)
    pd.set_option("display.width", 250)
    print("\n", t.round(2).to_string(index=False))


# ----------------------------------------------------------------------------------------------- checks
def compare(a, b, tag, strict=True):
    assert a.height == b.height, f"{tag}: row counts differ {a.height} vs {b.height}"
    j = a.join(b, on=["CERT", "REPDTE"], how="inner", suffix="_b")
    assert j.height == a.height, f"{tag}: key sets differ"
    for c in LABEL_COLS:
        bad = j.filter(~pl.col(c).cast(pl.Int8).eq_missing(pl.col(f"{c}_b").cast(pl.Int8))).height
        print(f"  {tag} {c:22} mismatches: {bad}")
        if strict:
            assert bad == 0, f"{tag}: {c} differs"


def verify():
    kp, ep = keys_path(1), events_path(1)
    ms = lambda df: df.with_columns(pl.col("REPDTE").cast(pl.Datetime("ms")))
    ref = ms(pl.read_parquet(str(C.PROCESSED_PANEL_DIR / "*" / "*.parquet"), columns=["CERT", "REPDTE"] + LABEL_COLS))
    res_pl = ms(label_polars(kp, ep))
    res_pd = ms(pl.from_pandas(label_pandas(kp, ep)))
    print("rows:", res_pl.height, "| positives y_fail_4q:", int(res_pl["y_fail_4q"].sum()),
          "| positives y_assist_or_fail_4q:", int(res_pl["y_assist_or_fail_4q"].sum()),
          "| non-null labels after the last labelable quarter:",
          res_pl.filter(~pl.col("label_observable") & pl.col("y_fail_4q").is_not_null()).height)
    print("Polars vs the panel written by step 04:");  compare(res_pl, ref, "polars vs 04")
    print("pandas vs the panel written by step 04:");  compare(res_pd, ref, "pandas vs 04")
    print("Polars vs pandas:");                        compare(res_pl, res_pd, "polars vs pd")
    print("VERIFY PASSED")


def explain():
    lf = label_polars_lazy(keys_path(1), events_path(1))
    for name, txt in (("unoptimized", lf.explain(optimized=False)), ("optimized", lf.explain())):
        (C.REPORT_DIR / f"polars_plan_T2_{name}.txt").write_text(txt, encoding="utf-8")
        print(f"\n=== {name} plan ({len(txt.splitlines())} lines) ===\n{txt}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prep", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--explain", action="store_true")
    ap.add_argument("--worker", nargs=2, metavar=("ENGINE", "SCALE"))
    ap.add_argument("--scales", type=int, nargs="+", default=[1, 2, 4])
    ap.add_argument("--repeats", type=int, default=7)
    ap.add_argument("--engines", nargs="+", default=ENGINES)
    a = ap.parse_args()
    if a.worker:
        worker(a.worker[0], int(a.worker[1]))
    elif a.prep:
        prep(a.scales)
    elif a.verify:
        verify()
    elif a.explain:
        explain()
    else:
        orchestrate(a.engines, a.scales, a.repeats)