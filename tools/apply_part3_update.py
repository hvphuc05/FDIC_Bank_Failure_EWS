"""Applies the Spark-rerun corrections to the Part 3 text of report/final_report.md (in place, atomic).
Usage: python apply_part3_update.py report/final_report.md"""
import sys
from pathlib import Path

REPL = [
 ("| Default (200 shuffle partitions, AQE on) | 9.52 [8.02–11.45] (5 runs) | 39.21 [32.87–44.57] (5 runs) |",
  "| Default (200 shuffle partitions, AQE on) | 9.52 [8.02–11.45] (5 runs) | 32.71 [30.97–34.87] (5 runs) |"),
 ("Two runs of the default configuration at ×1 gave 9.59 and 9.52 s. Reducing the partitions lowered the ×4 time by 39% (39.21 → 23.92 s); at ×1 the differences between configurations (−13% and −22%) are within the spread of the runs and are not interpreted.",
  "Each default configuration was measured twice, in different sessions: ×1 gave 9.59 and 9.52 s, ×2 gave 17.47 and 18.21 s, ×4 gave 39.21 and 32.71 s; the later runs, made with the corrected script, are used in the tables, and the spread between the two runs (up to 17% at ×4) indicates the session-to-session noise. Reducing the partitions lowered the ×4 time by 27% relative to the later default run (32.71 → 23.92 s) and by 39% relative to the earlier one; at ×1 the differences between configurations (−13% and −22%) are within the spread of the runs and are not interpreted."),
 ("the first run 18.2–20.5 s at ×1 (41.1–43.1 s at ×4)", "the first run 18.2–20.5 s at ×1 (39.6 s at ×2, 41.1–43.5 s at ×4)"),
 ("| ×2 | 17.47 | – | 2.24 | 2.95 | 7.8 | – |", "| ×2 | 18.21 | – | 2.24 | 2.95 | 8.1 | – |"),
 ("| ×4 | 39.21 | 23.92 | 6.34 | 6.90 | 6.2 | 3.8 |", "| ×4 | 32.71 | 23.92 | 6.34 | 6.90 | 5.2 | 3.8 |"),
 ("The ×2 default time comes from the first run of the script (its JSON recorded the AQE flag wrongly because of an error in the script, which was corrected afterwards; the plan shows that AQE was on).",
  "The ×2 and ×4 default times are from a rerun with the corrected script (an earlier run, whose JSON recorded the AQE flag wrongly because of an error in the script, gave 17.47 and 39.21 s)."),
 ("Spark/Polars falls from 8.7 (×1) to 6.2 (×4) with default settings", "Spark/Polars falls from 8.7 (×1) to 5.2 (×4) with default settings"),
 ("would take about 6 workers (39.21 / 6.34) with the default configuration",
  "would take about 5 workers (32.71 / 6.34; 6 with the earlier ×4 measurement of 39.21 s) with the default configuration"),
]

def main(path):
    p = Path(path)
    raw = p.read_bytes().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    missing = [a[:70] for a, _ in REPL if a not in text]
    if missing:
        sys.exit("NOT APPLIED. These passages were not found (the file differs from the expected one):\n  " + "\n  ".join(missing))
    for a, b in REPL:
        text = text.replace(a, b)
    p.write_bytes((text.replace("\n", "\r\n") if crlf else text).encode("utf-8"))
    print(f"applied {len(REPL)} replacements to {path}")

if __name__ == "__main__":
    main(sys.argv[1])