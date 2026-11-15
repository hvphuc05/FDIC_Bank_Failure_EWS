"""Step 04 - point-in-time bank-quarter panel (built in stages).
Stage 1: failure_events table (labels, censoring dates, COST quality flags).
Stage 2: de-cumulate YTD income-statement flows via Polars lazy execution.
Stage 3: 4-quarter forward failure labels, post-failure censoring, dev/gap/test splits.
Stage 4: bank_exits table (exit classification: failure vs other exits)."""
import argparse
import datetime as dt
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as C

OUT = C.PROCESSED_DATA_DIR
OUT.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# STAGE 1: Failure Events
# ---------------------------------------------------------------------------
def build_failure_events() -> pd.DataFrame:
    f = pd.read_parquet(C.FDIC_RAW_DIR / "failures.parquet")
    f["ID"] = f["ID"].astype(str)
    f["fail_dt"] = pd.to_datetime(f["FAILDATE"], format="%m/%d/%Y", errors="coerce")
    f["cert_num"] = pd.to_numeric(f["CERT"], errors="coerce")
    f["cost_num"] = pd.to_numeric(f["COST"], errors="coerce")
    ev = f[(f["fail_dt"] >= C.FAILURE_SCOPE_START) & (f["fail_dt"] <= C.FAILURE_SCOPE_END)].copy()

    # COST quality status (COST = 0 and NaN are treated as missing, never as "no loss")
    ev["cost_status"] = np.select(
        [ev["cost_num"].isna(), ev["cost_num"] == 0], ["missing", "zero"], "reported")
    ev["cost_effective"] = ev["cost_num"].where(ev["cost_status"] == "reported")

    # Formalisation rows: FAILURE with COST = 0 after an earlier ASSISTANCE of the same CERT.
    # De-duplicate (CERT, COST) so the repeated ASSISTANCE of CERT 28448 is counted once.
    assist = (ev[(ev["RESTYPE"] == "ASSISTANCE") & (ev["cost_num"] > 0)]
              .drop_duplicates(["cert_num", "cost_num"])
              [["cert_num", "fail_dt", "cost_num"]]
              .rename(columns={"fail_dt": "assist_dt", "cost_num": "assist_cost"}))
    z = (ev.loc[(ev["RESTYPE"] == "FAILURE") & (ev["cost_status"] == "zero"),
                ["ID", "cert_num", "fail_dt"]].merge(assist, on="cert_num"))
    z = z[z["assist_dt"] < z["fail_dt"]].groupby("ID")["assist_cost"].max()
    mapped = ev["ID"].map(z)
    hit = mapped.notna()
    ev.loc[hit, "cost_effective"] = mapped[hit]
    ev.loc[hit, "cost_status"] = "from_prior_assistance"

    # Eligible for the Part 5 cost estimate: FAILURE, usable COST, inside the DEV label window only
    in_dev = (ev["fail_dt"] > "1984-03-31") & (ev["fail_dt"] <= C.GAP_END)
    ev["use_for_cost_dev"] = (ev["RESTYPE"] == "FAILURE") & ev["cost_effective"].notna() & in_dev

    keep = ["ID", "cert_num", "NAME", "RESTYPE", "FAILDATE", "fail_dt",
            "cost_num", "cost_status", "cost_effective", "use_for_cost_dev"]
    return ev[keep].sort_values(["cert_num", "fail_dt"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# STAGE 2: De-cumulate YTD Flows (Polars Lazy)
# ---------------------------------------------------------------------------
ID_COLS = ["CERT", "REPDTE", "STNAME"]
NUMERIC_COLS = [c for c in C.FDIC_FINANCIAL_FIELDS if c not in ID_COLS]


def _date_expr(dtype):
    c = pl.col("REPDTE")
    if dtype == pl.Date:
        return c
    if dtype == pl.Datetime:
        return c.dt.date()
    s = c.cast(pl.String)
    return pl.coalesce([s.str.strptime(pl.Date, f, strict=False)
                        for f in ("%m/%d/%Y", "%Y%m%d", "%Y-%m-%d")])


def _load_one(path):
    """Read one quarterly file lazily and force a common schema (some quarters differ)."""
    schema = pl.read_parquet_schema(path)
    ex = [pl.col("CERT").cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias("CERT"),
          _date_expr(schema["REPDTE"]).alias("REPDTE"),
          (pl.col("STNAME").cast(pl.String) if "STNAME" in schema
           else pl.lit(None, dtype=pl.String).alias("STNAME"))]
    for c in NUMERIC_COLS:
        ex.append(pl.col(c).cast(pl.Float64, strict=False) if c in schema
                  else pl.lit(None, dtype=pl.Float64).alias(c))
    return pl.scan_parquet(path).select(ex)


def build_decumulated_lazy():
    flows = C.YTD_FLOW_COLUMNS
    assert "NTLNLS" in flows, "YTD_FLOW_COLUMNS in config.py must include NTLNLS"
    files = sorted((C.FDIC_RAW_DIR / "financials").rglob("*.parquet"))
    base = pl.concat([_load_one(p) for p in files]).with_columns(
        pl.col("REPDTE").dt.year().alias("yr"),
        ((pl.col("REPDTE").dt.month() - 1) // 3 + 1).alias("q"))
    # previous quarter of the SAME CERT in the SAME calendar year (calendar join, not row shift)
    prev = (base.select(["CERT", "yr", "q"] + flows)
            .with_columns(pl.col("q") + 1)
            .rename({c: f"{c}_prev" for c in flows}))
    joined = base.join(prev, on=["CERT", "yr", "q"], how="left")
    return joined.with_columns([
        pl.when(pl.col("q") == 1).then(pl.col(c))
          .otherwise(pl.col(c) - pl.col(f"{c}_prev")).alias(f"{c}_q")
        for c in flows]), len(files)


def build_decumulated():
    flows = C.YTD_FLOW_COLUMNS
    lf, n_files = build_decumulated_lazy()
    (C.REPORT_DIR / "polars_plan_decumulate.txt").write_text(lf.explain(), encoding="utf-8")
    df = lf.collect()

    # structural checks
    print(f"files: {n_files} | rows: {df.height} | null REPDTE: {df['REPDTE'].null_count()} "
          f"| null CERT: {df['CERT'].null_count()}")
    print("duplicate (CERT, REPDTE):", df.height - df.unique(["CERT", "REPDTE"]).height)
    print("months:", sorted(df["REPDTE"].dt.month().unique().to_list()),
          "| quarter counts:", dict(sorted(df["q"].value_counts().iter_rows())))
    assert df["REPDTE"].null_count() == 0 and df["CERT"].null_count() == 0

    # null accounting and sign check
    print(f"\n{'col':8} {'null_ytd':>9} {'null_q':>8} {'gap_rows':>9} {'neg_q':>8}")
    for c in flows:
        n_y, n_q = df[c].null_count(), df[f"{c}_q"].null_count()
        gap = df.filter((pl.col("q") > 1) & pl.col(c).is_not_null()
                        & pl.col(f"{c}_prev").is_null()).height
        neg = df.filter(pl.col(f"{c}_q") < 0).height
        print(f"{c:8} {n_y:9d} {n_q:8d} {gap:9d} {neg:8d}")
        assert n_q == n_y + gap, f"null accounting broken for {c}"

    # sawtooth must disappear: |Q2|/|Q1|, |Q3|/|Q1|, |Q4|/|Q1| should now be about 1
    print(f"\n{'col':8} {'n_years':>8} {'|Q2|/|Q1|':>10} {'|Q3|/|Q1|':>10} {'|Q4|/|Q1|':>10}")
    for c in flows:
        w = (df.select(["CERT", "yr", "q", f"{c}_q"]).to_pandas()
               .pivot_table(index=["CERT", "yr"], columns="q", values=f"{c}_q", aggfunc="first")
               .dropna().abs())
        nz = w[1] > 0
        r = lambda k: (w.loc[nz, k] / w.loc[nz, 1]).median()
        print(f"{c:8} {len(w):8d} {r(2):10.2f} {r(3):10.2f} {r(4):10.2f}")

    out = df.drop([f"{c}_prev" for c in flows]).sort(["CERT", "REPDTE"])
    out.write_parquet(OUT / "panel_decumulated.parquet", compression="zstd")
    print("\nwritten:", OUT / "panel_decumulated.parquet", "| columns:", len(out.columns))


# ---------------------------------------------------------------------------
# STAGE 3: Labels, Censoring, Split
# ---------------------------------------------------------------------------
def _d(s):
    return pl.lit(dt.date.fromisoformat(s))


def build_labeled_panel():
    H = f"{3 * C.LABEL_HORIZON_QUARTERS}mo"
    ev = (pl.read_parquet(OUT / "failure_events.parquet")
          .select(pl.col("cert_num").cast(pl.Int64).alias("CERT"), "RESTYPE",
                  pl.col("fail_dt").cast(pl.Date))
          .drop_nulls(["CERT", "fail_dt"]))
    fails = ev.filter(pl.col("RESTYPE") == "FAILURE")
    censor = fails.group_by("CERT").agg(pl.col("fail_dt").max().alias("censor_dt"))

    panel = pl.scan_parquet(OUT / "panel_decumulated.parquet")
    n0 = panel.select(pl.len()).collect().item()
    keys = panel.select("CERT", "REPDTE")

    def label(events, name):
        return (keys.join(events.lazy(), on="CERT", how="inner")
                .filter((pl.col("fail_dt") > pl.col("REPDTE"))
                        & (pl.col("fail_dt") <= pl.col("REPDTE").dt.offset_by(H)))
                .select("CERT", "REPDTE").unique()
                .with_columns(pl.lit(1, dtype=pl.Int8).alias(name)))

    obs = pl.col("REPDTE") <= _d(C.LAST_LABELED_Q)
    lf = (panel.join(censor.lazy(), on="CERT", how="left")
          .filter(pl.col("censor_dt").is_null() | (pl.col("REPDTE") < pl.col("censor_dt")))
          .join(label(fails, "y_fail_4q"), on=["CERT", "REPDTE"], how="left")
          .join(label(ev, "y_assist_or_fail_4q"), on=["CERT", "REPDTE"], how="left")
          .with_columns(
              obs.alias("label_observable"),
              pl.when(obs).then(pl.col("y_fail_4q").fill_null(0)).otherwise(None)
                .cast(pl.Int8).alias("y_fail_4q"),
              pl.when(obs).then(pl.col("y_assist_or_fail_4q").fill_null(0)).otherwise(None)
                .cast(pl.Int8).alias("y_assist_or_fail_4q"),
              pl.when(pl.col("REPDTE") > _d(C.LAST_LABELED_Q)).then(pl.lit("unlabeled"))
                .when(pl.col("REPDTE") <= _d(C.DEV_END)).then(pl.lit("dev"))
                .when(pl.col("REPDTE") <= _d(C.GAP_END)).then(pl.lit("gap"))
                .otherwise(pl.lit("test")).alias("split"))
          .drop("censor_dt"))
    df = lf.collect().sort(["CERT", "REPDTE"])
    print(f"rows before censoring: {n0} | after: {df.height} | dropped: {n0 - df.height}")

    # ---- independent check in pandas (does not reuse the Polars label logic) ----
    fe = pd.read_parquet(OUT / "failure_events.parquet")
    fe = fe[fe["RESTYPE"] == "FAILURE"].copy()
    fe["CERT"] = fe["cert_num"].astype("int64")
    fe = fe[["CERT", "fail_dt"]]
    sub = (df.filter(pl.col("CERT").is_in(fe["CERT"].unique().tolist()))
             .select("CERT", "REPDTE", "y_fail_4q", "label_observable").to_pandas()
             .merge(fe, on="CERT"))
    sub["REPDTE"] = pd.to_datetime(sub["REPDTE"])
    viol = int((sub["REPDTE"] >= sub.groupby("CERT")["fail_dt"].transform("max")).sum())
    o = sub[sub["label_observable"]].copy()
    o["in_win"] = ((o["fail_dt"] > o["REPDTE"])
                   & (o["fail_dt"] <= o["REPDTE"] + pd.DateOffset(years=1)))
    chk = o.groupby(["CERT", "REPDTE"]).agg(y=("y_fail_4q", "first"),
                                            w=("in_win", "max")).reset_index()
    bad = int((chk["y"] != chk["w"].astype(int)).sum())
    print(f"CHECK rows at/after last FAILDATE still present: {viol} | "
          f"label mismatches vs pandas recomputation: {bad} (checked {len(chk)} rows)")
    assert viol == 0 and bad == 0

    gp = df.filter(pl.col("split") == "gap")["REPDTE"].min()
    print("first gap quarter:", gp, "| last dev quarter:",
          df.filter(pl.col("split") == "dev")["REPDTE"].max())

    # ---- split summary ----
    rows = []
    for sp in ["dev", "gap", "test", "unlabeled"]:
        s = df.filter(pl.col("split") == sp)
        labeled = sp != "unlabeled"
        pos = s.filter(pl.col("y_fail_4q") == 1)
        rows.append({"split": sp, "rows": s.height, "banks": s["CERT"].n_unique(),
                     "first_q": str(s["REPDTE"].min()), "last_q": str(s["REPDTE"].max()),
                     "pos_rows": pos.height if labeled else None,
                     "pos_rate_pct": round(100 * pos.height / s.height, 3) if labeled else None,
                     "pos_banks": pos["CERT"].n_unique() if labeled else None,
                     "pos_rows_assist_or_fail": (s.filter(pl.col("y_assist_or_fail_4q") == 1).height
                                                 if labeled else None)})
    summ = pd.DataFrame(rows)
    print("\n", summ.to_string(index=False))
    summ.to_csv(C.REPORT_DIR / "panel_split_summary.csv", index=False)

    # FAILURE records by label window (cross-check with check_failures_detail3.py: 2299 / 545)
    ev_f = pd.read_parquet(OUT / "failure_events.parquet")
    ev_f = ev_f[ev_f["RESTYPE"] == "FAILURE"]
    dev_n = int(((ev_f["fail_dt"] > "1984-03-31") & (ev_f["fail_dt"] <= C.GAP_END)).sum())
    test_n = int(((ev_f["fail_dt"] > C.TEST_START) & (ev_f["fail_dt"] <= C.FAILURE_SCOPE_END)).sum())
    print(f"\nFAILURE records: dev window {dev_n} | test window {test_n} | "
          f"in neither window {len(ev_f) - dev_n - test_n}")

    # ---- write partitioned by year ----
    if C.PROCESSED_PANEL_DIR.exists():
        shutil.rmtree(C.PROCESSED_PANEL_DIR)   # only data_processed/panel (see config.py)
    for yr in sorted(df["yr"].unique().to_list()):
        p = C.PROCESSED_PANEL_DIR / f"year={yr}"
        p.mkdir(parents=True)
        df.filter(pl.col("yr") == yr).write_parquet(p / "part-0.parquet", compression="zstd")
    mb = sum(f.stat().st_size for f in C.PROCESSED_PANEL_DIR.rglob("*.parquet")) / 1e6
    print(f"written: {C.PROCESSED_PANEL_DIR} | {mb:.1f} MB | columns: {len(df.columns)}")


# ---------------------------------------------------------------------------
# STAGE 4: Bank Exits
# ---------------------------------------------------------------------------
def build_bank_exits():
    p = pd.read_parquet(OUT / "panel_decumulated.parquet", columns=["CERT", "REPDTE"])
    p["REPDTE"] = pd.to_datetime(p["REPDTE"])
    life = (p.groupby("CERT")["REPDTE"].agg(first_q="min", last_q="max", n_q="size")
              .reset_index())

    ev = pd.read_parquet(OUT / "failure_events.parquet").dropna(subset=["cert_num"])
    ev["CERT"] = ev["cert_num"].astype("int64")
    ev["is_f"] = ev["RESTYPE"].eq("FAILURE")
    ev["is_a"] = ev["RESTYPE"].eq("ASSISTANCE")
    ev["f_dt"] = ev["fail_dt"].where(ev["is_f"])
    agg = ev.groupby("CERT").agg(n_failure=("is_f", "sum"), n_assist=("is_a", "sum"),
                                 last_failure_dt=("f_dt", "max")).reset_index()

    ex = life.merge(agg, on="CERT", how="left")
    ex[["n_failure", "n_assist"]] = ex[["n_failure", "n_assist"]].fillna(0).astype(int)
    ex["exited"] = ex["last_q"] < pd.Timestamp(C.FDIC_END_DATE)
    ex["exit_class"] = np.select(
        [~ex["exited"], ex["n_failure"] > 0, ex["n_assist"] > 0],
        ["still_reporting", "failure", "assistance_only"], "other_exit")
    # note: 'other_exit' = no FAILURE/ASSISTANCE record; merger vs voluntary is NOT distinguished here
    ex.to_parquet(OUT / "bank_exits.parquet", index=False, compression="zstd")

    print("banks in panel:", len(ex), "| exited before 2026Q2:", int(ex["exited"].sum()))
    print(pd.crosstab(ex["exited"], ex["exit_class"]).to_string())
    in_panel = set(ex["CERT"])
    miss = ev[~ev["CERT"].isin(in_panel)]
    print("failure_events rows whose CERT is not in the panel:", len(miss),
          "| by RESTYPE:", miss["RESTYPE"].value_counts().to_dict())
    print("FAILURE CERTs still reporting at 2026Q2:",
          int(((ex["exit_class"] == "still_reporting") & (ex["n_failure"] > 0)).sum()))

    # empirical cross-check against the institutions table (evidence only, never a feature)
    inst = pd.read_parquet(C.FDIC_RAW_DIR / "institutions.parquet")
    inst["CERT"] = pd.to_numeric(inst["CERT"], errors="coerce")
    inst = inst.dropna(subset=["CERT"]).drop_duplicates("CERT")
    inst["CERT"] = inst["CERT"].astype("int64")
    m = ex[ex["exited"]].merge(inst[["CERT", "CHANGEC1"]], on="CERT", how="left")
    print("\nCHANGEC1 of exited banks by exit_class (top 6 per class):")
    for k, s in m.groupby("exit_class"):
        print(f"  {k:16} n={len(s):6d}", s["CHANGEC1"].value_counts(dropna=False).head(6).to_dict())


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-stage", type=int, default=1)
    a = ap.parse_args()
    if a.from_stage <= 1:
        ev = build_failure_events()
        ev.to_parquet(OUT / "failure_events.parquet", index=False, compression="zstd")
        print("[stage 1] failure_events rows:", len(ev))
    if a.from_stage <= 2:
        print("\n[stage 2] de-cumulate YTD flows")
        build_decumulated()
    if a.from_stage <= 3:
        print("\n[stage 3] labels, censoring, split")
        build_labeled_panel()
    print("\n[stage 4] bank exits")
    build_bank_exits()


if __name__ == "__main__":
    main()