"""Step 06 - PySpark reproduction of T1 (year-to-date de-cumulation).
Runs locally (needs Java) or on Google Colab.
Example: python src/06_spark_pipeline.py --input data_processed/bench/ytd_input_x1.parquet --label x1 --write-out spark_T1_out_x1"""
import argparse
import contextlib
import io
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FLOWS = ["NETINC", "INTINC", "EINTEXP", "NONII", "NONIX", "ELNATR", "NTLNLS"]   # = config.YTD_FLOW_COLUMNS


def build(df):
    base = (df.withColumn("yr", F.year("REPDTE"))
              .withColumn("q", F.floor((F.month("REPDTE") - 1) / 3) + 1))
    # previous quarter of the SAME CERT in the SAME calendar year (calendar join, not a row shift)
    prev = base.select("CERT", "yr", (F.col("q") + 1).alias("q"),
                       *[F.col(c).alias(f"{c}_prev") for c in FLOWS])
    joined = base.join(prev, on=["CERT", "yr", "q"], how="left")
    return joined.select("CERT", "REPDTE",
                         *[F.when(F.col("q") == 1, F.col(c)).otherwise(F.col(c) - F.col(f"{c}_prev")).alias(f"{c}_q")
                           for c in FLOWS])


def fingerprint(out):
    aggs = [F.count(F.lit(1)).alias("rows")]
    for c in FLOWS:
        col = f"{c}_q"
        aggs += [F.count(col).alias(f"{col}__nonnull"), F.sum(col).alias(f"{col}__sum"),
                 F.min(col).alias(f"{col}__min"), F.max(col).alias(f"{col}__max")]
    return out.agg(*aggs).first().asDict()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--label", default="x1")
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--driver-memory", default="4g")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--shuffle-partitions", type=int, default=None)
    ap.add_argument("--aqe", choices=["default", "true", "false"], default="default")
    ap.add_argument("--write-out", default=None, help="directory for the Spark result (parquet, one file)")
    ap.add_argument("--report-dir", default=".")
    a = ap.parse_args()

    t_start = time.perf_counter()
    b = (SparkSession.builder.master(a.master).appName("T1_decumulate")
         .config("spark.sql.session.timeZone", "UTC").config("spark.driver.memory", a.driver_memory))
    if a.shuffle_partitions:
        b = b.config("spark.sql.shuffle.partitions", str(a.shuffle_partitions))
    if a.aqe != "default":
        b = b.config("spark.sql.adaptive.enabled", a.aqe)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    startup = time.perf_counter() - t_start

    out = build(spark.read.parquet(a.input))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        out.explain(mode="formatted")
    rep = Path(a.report_dir)
    (rep / f"spark_plan_T1_{a.label}.txt").write_text(buf.getvalue(), encoding="utf-8")

    def run():
        out.write.format("noop").mode("overwrite").save()      # forces the whole job without writing files

    t0 = time.perf_counter()
    run()
    first = time.perf_counter() - t0                            # includes JVM/code warm-up
    times = []
    for _ in range(a.repeats):
        t0 = time.perf_counter()
        run()
        times.append(time.perf_counter() - t0)

    res = {"label": a.label, "spark_version": spark.version, "master": a.master,
           "default_parallelism": spark.sparkContext.defaultParallelism,
           "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
           "aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
           "broadcast_threshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
           "shuffle_partitions_conf": spark.conf.get("spark.sql.shuffle.partitions"),
           "startup_sec": round(startup, 2), "first_run_sec": round(first, 2),
           "sec_median": round(statistics.median(times), 2), "sec_min": round(min(times), 2),
           "sec_max": round(max(times), 2), "repeats": a.repeats, "fingerprint": fingerprint(out)}
    if a.write_out:
        out.coalesce(1).write.mode("overwrite").parquet(a.write_out)
        res["written_to"] = a.write_out
    (rep / f"spark_T1_result_{a.label}.json").write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in res.items() if k != "fingerprint"}, indent=2, default=str))
    print("rows:", res["fingerprint"]["rows"])
    print("\n--- Spark physical plan ---\n" + buf.getvalue())
    spark.stop()


if __name__ == "__main__":
    main()