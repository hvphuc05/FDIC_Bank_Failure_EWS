"""Step 06c - PySpark reproduction of T2 (forward label with censoring). Same logic as step 04, stage 3.
Runs locally (needs Java) or on Google Colab.
Example: python src/06c_spark_labels.py --keys data_processed/bench/t2_keys_x1.parquet \
         --events data_processed/bench/t2_events_x1.parquet --label t2_x1 --write-out spark_T2_out_x1"""
import argparse
import contextlib
import io
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HORIZON_MONTHS = 12            # = 3 * config.LABEL_HORIZON_QUARTERS
LAST_LABELED = "2025-06-30"    # = config.LAST_LABELED_Q


def build(keys, events):
    kd = keys.withColumn("rep", F.to_date("REPDTE"))
    ev = events.withColumn("fdt", F.to_date("fail_dt"))
    fails = ev.filter(F.col("RESTYPE") == "FAILURE")
    censor = fails.groupBy("CERT").agg(F.max("fdt").alias("censor_dt"))

    def flag(e, name):
        j = (kd.select("CERT", "REPDTE", "rep").join(e.select("CERT", "fdt"), on="CERT", how="inner")
               .filter((F.col("fdt") > F.col("rep")) & (F.col("fdt") <= F.add_months(F.col("rep"), HORIZON_MONTHS))))
        return j.select("CERT", "REPDTE").distinct().withColumn(name, F.lit(1).cast("tinyint"))

    kept = (kd.join(censor, on="CERT", how="left")
              .filter(F.col("censor_dt").isNull() | (F.col("rep") < F.col("censor_dt"))))
    obs = F.col("rep") <= F.lit(LAST_LABELED).cast("date")
    out = (kept.join(flag(fails, "y_fail_4q"), on=["CERT", "REPDTE"], how="left")
               .join(flag(ev, "y_assist_or_fail_4q"), on=["CERT", "REPDTE"], how="left"))
    zero = F.lit(0).cast("tinyint")
    return out.select("CERT", "REPDTE", obs.alias("label_observable"),
                      F.when(obs, F.coalesce(F.col("y_fail_4q"), zero)).alias("y_fail_4q"),
                      F.when(obs, F.coalesce(F.col("y_assist_or_fail_4q"), zero)).alias("y_assist_or_fail_4q"))


def fingerprint(out):
    return out.agg(F.count(F.lit(1)).alias("rows"), F.sum("y_fail_4q").alias("y_fail_positives"),
                   F.sum("y_assist_or_fail_4q").alias("y_alt_positives"),
                   F.sum(F.col("label_observable").cast("int")).alias("observable_rows")).first().asDict()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", required=True)
    ap.add_argument("--events", required=True)
    ap.add_argument("--label", default="t2_x1")
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--driver-memory", default="4g")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--shuffle-partitions", type=int, default=None)
    ap.add_argument("--aqe", choices=["default", "true", "false"], default="default")
    ap.add_argument("--write-out", default=None, help="directory for the Spark result (parquet, one file)")
    ap.add_argument("--report-dir", default=".")
    a = ap.parse_args()

    t_start = time.perf_counter()
    b = (SparkSession.builder.master(a.master).appName("T2_labels")
         .config("spark.sql.session.timeZone", "UTC").config("spark.driver.memory", a.driver_memory))
    if a.shuffle_partitions:
        b = b.config("spark.sql.shuffle.partitions", str(a.shuffle_partitions))
    if a.aqe != "default":
        b = b.config("spark.sql.adaptive.enabled", a.aqe)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    startup = time.perf_counter() - t_start

    out = build(spark.read.parquet(a.keys), spark.read.parquet(a.events))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        out.explain(mode="formatted")
    rep = Path(a.report_dir)
    (rep / f"spark_plan_{a.label}.txt").write_text(buf.getvalue(), encoding="utf-8")

    def run():
        out.write.format("noop").mode("overwrite").save()

    t0 = time.perf_counter()
    run()
    first = time.perf_counter() - t0
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
    (rep / f"spark_{a.label}_result.json").write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    print(json.dumps(res, indent=2, default=str))
    print("\n--- Spark physical plan ---\n" + buf.getvalue())
    spark.stop()


if __name__ == "__main__":
    main()