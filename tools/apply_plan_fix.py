"""Corrects the 'Spark's plan' paragraph of section 4.6 (T2) with the confirmed plan details.
Usage: python tools/apply_plan_fix.py report/final_report.md"""
import sys
from pathlib import Path

OLD_START = "**Spark's plan.** Because the event table is small,"
OLD_END = "at this size it brings no advantage over a single node."
NEW = ("**Spark's plan** (`spark_plan_t2_x1.txt`). The three joins with the small event table (nodes 10, 22 and 37: the "
       "censoring aggregate, the `FAILURE` label and the all-events label) are `BroadcastHashJoin … BuildRight` with a "
       "`BroadcastExchange` of the small side (nodes 9, 21 and 36), so the large key table is not shuffled for them. The two left "
       "joins that attach the label flags back to the censored key table (nodes 28 and 43) are `SortMergeJoin LeftOuter` on "
       "`(CERT, REPDTE)`, each preceded by `Exchange hashpartitioning(…, 200)` and `Sort` nodes (13, 14, 25, 27, 40, 42), so the key "
       "table is shuffled there, as in T1 (§4.5). Broadcasting a small table against a large one scales out well, but we did "
       "not isolate its contribution to the Spark timings, and at this size it brings no advantage over a single node.")

p = Path(sys.argv[1])
raw = p.read_bytes().decode("utf-8")
crlf = "\r\n" in raw
text = raw.replace("\r\n", "\n")
a = text.find(OLD_START)
b = text.find(OLD_END, a)
if a < 0 or b < 0:
    sys.exit("NOT APPLIED: the paragraph was not found (the file differs from the expected one).")
text = text[:a] + NEW + text[b + len(OLD_END):]
p.write_bytes((text.replace("\n", "\r\n") if crlf else text).encode("utf-8"))
print("paragraph replaced in", p)