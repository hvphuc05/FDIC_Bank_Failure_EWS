"""Adds transformation T2 to the Part 3 text of the report and to the README (in place, each file all-or-nothing).
Usage: python tools/apply_part3_T2.py report/final_report.md README.md"""
import sys
from pathlib import Path

SECTION = open(Path(__file__).with_name("t2_section.md"), encoding="utf-8").read()

REPORT = [
 ("T2 and T3 are not benchmarked.", "T2 is benchmarked in §4.6; T3 is not."),
 ("## 4.6 When is a cluster justified?", SECTION + "## 4.7 When is a cluster justified?"),
 ("and from 6.8 to 3.8 with the best configuration measured.",
  "and from 6.8 to 3.8 with the best configuration measured; for T2 it falls from 12.6 (×1) to 7.4 (×4) (§4.6)."),
 ("One transformation (T1) on a 9-column slice;", "Two transformations (T1 on a 9-column slice, T2 on a 2-column key table and a small event table);"),
]
README = [
 ("transformation T1 done (Polars, pandas and Spark benchmark, strict result checks, plan reading); T2 (labels) not benchmarked; write-up in the report §4",
  "transformations T1 and T2 done (Polars, pandas and Spark benchmarks, strict result checks, plan reading); write-up in the report §4"),
 ("│   ├── 06b_compare_spark.py        # Part 3: strict row-level comparison Spark vs Polars\n",
  "│   ├── 06b_compare_spark.py        # Part 3: strict row-level comparison Spark vs Polars\n"
  "│   ├── 05c_label_benchmark.py      # Part 3: T2 (forward label with censoring), Polars vs pandas\n"
  "│   ├── 06c_spark_labels.py         # Part 3: T2 in PySpark\n"
  "│   ├── 06d_compare_spark_labels.py # Part 3: Spark T2 vs Polars and vs the step-04 panel\n"),
 ("python src/05b_polars_engine_explain.py 4                          # plans and stage timing\n",
  "python src/05b_polars_engine_explain.py 4                          # plans and stage timing\n"
  "python src/05c_label_benchmark.py --prep --scales 1 2 4            # T2 inputs\n"
  "python src/05c_label_benchmark.py --verify                         # strict equality with the panel of step 04\n"
  "python src/05c_label_benchmark.py --explain                        # T2 plans\n"
  "python src/05c_label_benchmark.py                                  # report/benchmark_T2_labels.csv\n"),
 ("python src/06_spark_pipeline.py --input data_processed/bench/ytd_input_x4.parquet --label x4_shuf8 --shuffle-partitions 8 --repeats 3\n",
  "python src/06_spark_pipeline.py --input data_processed/bench/ytd_input_x4.parquet --label x4_shuf8 --shuffle-partitions 8 --repeats 3\n"
  "python src/05c_label_benchmark.py --engines pandas polars_lazy --repeats 7\n"
  "python src/06c_spark_labels.py --keys data_processed/bench/t2_keys_x1.parquet --events data_processed/bench/t2_events_x1.parquet --label t2_x1 --write-out spark_T2_out_x1\n"
  "python src/06c_spark_labels.py --keys data_processed/bench/t2_keys_x4.parquet --events data_processed/bench/t2_events_x4.parquet --label t2_x4\n"),
 ("it must print `SPARK MATCHES POLARS`.\n",
  "it must print `SPARK MATCHES POLARS`. For T2, put `spark_T2_out_x1` in `data_processed/spark_T2_out_x1/` and run `python src/06d_compare_spark_labels.py data_processed/spark_T2_out_x1`; it must print `SPARK T2 MATCHES POLARS AND STEP 04`.\n"),
 ("the Part 3 benchmark covers one transformation on a 9-column slice,",
  "the Part 3 benchmark covers two transformations (T1 on a 9-column slice, T2 on a 2-column key table),"),
]

def apply(path, repl):
    p = Path(path)
    raw = p.read_bytes().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    missing = [a[:70] for a, _ in repl if a not in text]
    if missing:
        print(f"NOT APPLIED to {path}. Not found:\n  " + "\n  ".join(missing))
        return False
    for a, b in repl:
        text = text.replace(a, b, 1)
    p.write_bytes((text.replace("\n", "\r\n") if crlf else text).encode("utf-8"))
    print(f"applied {len(repl)} replacements to {path}")
    return True

if __name__ == "__main__":
    ok = apply(sys.argv[1], REPORT)
    if len(sys.argv) > 2:
        ok = apply(sys.argv[2], README) and ok
    sys.exit(0 if ok else 1)