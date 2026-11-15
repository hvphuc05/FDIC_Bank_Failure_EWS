"""Step 06b - strict row-level comparison of the Spark result with the Polars result (run locally)."""
import importlib
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

b = importlib.import_module("05_polars_pandas_benchmark")


def main(spark_dir, scale=1):
    sp = (pl.read_parquet(str(Path(spark_dir) / "*.parquet"))
            .with_columns(pl.col("REPDTE").cast(pl.Datetime("ms"))))
    ref = b.decumulate_polars(b.bench_path(scale)).with_columns(pl.col("REPDTE").cast(pl.Datetime("ms")))
    print("Spark rows:", sp.height, "| Polars rows:", ref.height)
    b.compare(sp, ref, "spark vs polars", strict=True)
    print("SPARK MATCHES POLARS")


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 1)