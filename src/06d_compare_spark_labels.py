"""Step 06d - strict comparison of the Spark T2 result with the Polars result and with the panel written by step 04."""
import importlib
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config as C

t2 = importlib.import_module("05c_label_benchmark")


def main(spark_dir):
    ms = lambda df: df.with_columns(pl.col("REPDTE").cast(pl.Datetime("ms")))
    sp = ms(pl.read_parquet(str(Path(spark_dir) / "*.parquet")))
    ref_pl = ms(t2.label_polars(t2.keys_path(1), t2.events_path(1)))
    ref_04 = ms(pl.read_parquet(str(C.PROCESSED_PANEL_DIR / "*" / "*.parquet"),
                                columns=["CERT", "REPDTE"] + t2.LABEL_COLS))
    print("Spark rows:", sp.height, "| Polars rows:", ref_pl.height, "| step-04 panel rows:", ref_04.height)
    print("Spark vs Polars:");                         t2.compare(sp, ref_pl, "spark vs polars")
    print("Spark vs the panel written by step 04:");   t2.compare(sp, ref_04, "spark vs 04  ")
    print("SPARK T2 MATCHES POLARS AND STEP 04")


if __name__ == "__main__":
    main(sys.argv[1])