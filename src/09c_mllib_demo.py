"""Step 09c - Spark MLlib demonstration (run on Google Colab or locally with Java). DEVELOPMENT DATA ONLY.
Reproduces the logistic regression of the frozen design inside Spark ML:
  1. fold 3 of the validation scheme: train on REPDTE <= 1992-12-31, score 1993-12-31 .. 2006-12-31
  2. final fit on the whole development period (coefficients only)
Pre-processing is learned on the training rows only (winsorising bounds, median imputation, standardisation), as in
src/preprocess.py. MLlib minimises (1/N) sum(loss) + (lambda/2)||w||^2, scikit-learn (1/2)||w||^2 + C sum(loss);
the two have the same minimiser when lambda = 1/(C N) (C = 1 in the frozen design).
Example: python src/09c_mllib_demo.py --features data_processed/bench/mllib_dev_features.parquet --out-dir mllib_out"""
import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as C

FEATURES = list(C.FEATURE_COLS)


def winsor_bounds(train, lo=0.005, hi=0.995, rel_err=0.001):
    q = train.approxQuantile(FEATURES, [lo, hi], rel_err)      # nulls are ignored; rel_err 0 (exact) ran out of heap memory with 4 GB
    return dict(zip(FEATURES, q))


def apply_winsor(df, bounds):
    cols = [F.when(F.col(c) < lo, F.lit(lo)).when(F.col(c) > hi, F.lit(hi)).otherwise(F.col(c)).alias(c)
            for c, (lo, hi) in bounds.items()]                  # a null stays null (comparisons give null)
    return df.select("CERT", "REPDTE", F.col("y_fail_4q").cast("double").alias("label"), *cols)


def make_pipeline(n_train, c_sklearn=1.0):
    imputed = [f"{c}__imp" for c in FEATURES]
    return Pipeline(stages=[
        Imputer(strategy="median", inputCols=FEATURES, outputCols=imputed),
        VectorAssembler(inputCols=imputed, outputCol="raw"),
        StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True),
        LogisticRegression(featuresCol="features", labelCol="label", family="binomial",
                           regParam=1.0 / (c_sklearn * n_train), elasticNetParam=0.0,
                           standardization=False, maxIter=300, tol=1e-8)])


def fit(train, rel_err):
    n = train.count()
    t0 = time.perf_counter()
    bounds = winsor_bounds(train, rel_err=rel_err)
    t_bounds = time.perf_counter() - t0
    model = make_pipeline(n).fit(apply_winsor(train, bounds))
    lr = model.stages[-1]
    info = {"train_rows": n, "seconds_bounds": round(t_bounds, 1), "seconds_total_fit": round(time.perf_counter() - t0, 1),
            "iterations": int(lr.summary.totalIterations), "intercept": float(lr.intercept),
            "coefficients": dict(zip(FEATURES, [float(v) for v in lr.coefficients.toArray()]))}
    return model, bounds, info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out-dir", default="mllib_out")
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--driver-memory", default="8g")
    ap.add_argument("--rel-err", type=float, default=0.001)
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    spark = (SparkSession.builder.master(a.master).appName("MLlib_demo").config("spark.driver.memory", a.driver_memory)
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(a.features).cache()
    d = lambda s: F.lit(s).cast("date")
    t_end, v_start, v_end = C.CV_FOLDS[2]
    assert df.agg(F.max("REPDTE")).first()[0].isoformat() <= C.DEV_END, "non-development rows present"

    result = {"spark_version": spark.version, "java_version": spark.sparkContext._jvm.System.getProperty("java.version"),
              "default_parallelism": spark.sparkContext.defaultParallelism, "relative_error_quantiles": a.rel_err}

    # ---- 1. fold 3: train <= 1992Q4, score 1993Q4 - 2006Q4 ----
    train = df.filter(F.col("REPDTE") <= d(t_end))
    valid = df.filter((F.col("REPDTE") >= d(v_start)) & (F.col("REPDTE") <= d(v_end)))
    model, bounds, info = fit(train, a.rel_err)
    pred = model.transform(apply_winsor(valid, bounds)).select(
        "CERT", "REPDTE", "label", vector_to_array("probability")[1].alias("p"), "rawPrediction").cache()
    t0 = time.perf_counter()
    ev = lambda m: BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName=m).evaluate(pred)
    info.update({"valid_rows": pred.count(), "valid_positive_rows": int(pred.agg(F.sum("label")).first()[0]),
                 "areaUnderROC": ev("areaUnderROC"), "areaUnderPR_spark_trapezoid": ev("areaUnderPR"),
                 "seconds_scoring_and_metrics": round(time.perf_counter() - t0, 1)})
    pred.select("CERT", "REPDTE", "label", "p").coalesce(1).write.mode("overwrite").parquet(str(out / "fold3_predictions"))
    result["fold3"] = info

    # ---- 2. final fit on the whole development period ----
    model2, _, info2 = fit(df, a.rel_err)
    result["final_fit"] = info2
    (out / "mllib_result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items()}, indent=2)[:3500])
    spark.stop()


if __name__ == "__main__":
    main()