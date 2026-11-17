## 4.6 Transformation T2: forward label with censoring

T2 is stage 3 of step 04 (§3.2.4): drop every bank-quarter on or after the last `FAILURE` date of the same `CERT`, set y = 1 if a `FAILURE` date falls in (REPDTE, REPDTE + 12 months], build the alternative label from `FAILURE` and `ASSISTANCE` records, and leave the labels null after 2025Q2. The inputs are a key table (`CERT`, `REPDTE`; 1,682,616 rows) and the event table (3,273 rows). The range condition is implemented as an equality join on `CERT` followed by a filter; this is efficient here because a charter has at most two events in scope. Scale ×k is again k copies with `CERT` shifted by 10 million (keys and events of each copy stay together). Three implementations were written from the same specification: Polars lazy, pandas (a straightforward merge-and-filter formulation; a more specialised pandas implementation was not tried) and PySpark.

**Table P3-7: Correctness of T2**

| Check | Result |
|:---|:---|
| Polars, pandas and the panel written by step 04 (laptop, ×1, 1,681,547 rows; strict assertions) | 0 mismatches in `label_observable`, `y_fail_4q` and `y_assist_or_fail_4q` for Polars–step 04, pandas–step 04 and Polars–pandas |
| Positive rows | `y_fail_4q` 10,870 and `y_assist_or_fail_4q` 12,459, equal to the sums of Table 7 (dev + gap + test) |
| Non-null labels after the last labelable quarter | 0 |
| Spark (Colab, ×1): fingerprint | 1,681,547 rows; 10,870 and 12,459 positives; 1,664,018 observable rows (= 1,681,547 − the 17,529 unlabeled rows of Table 7) |
| Spark (×1) against Polars and against the step-04 panel, row by row (strict) | 0 mismatches in all three label columns |
| Spark (×4) | counts are exactly four times those of ×1 (6,726,188 rows; 43,480 and 49,836 positives; 6,656,072 observable rows), as expected for independent copies; this is a consistency check, not a row-level comparison |

**How Polars executes T2** (`report/polars_plan_T2_*.txt`, 66 lines unoptimised, 58 optimised):

| Observation | Interpretation |
|:---|:---|
| The filter `RESTYPE == "FAILURE"` sits above the scan in the unoptimised plan and appears as `SELECTION` inside the scan in the optimised plan | Predicate pushdown: rows are filtered while reading |
| The scan of the events table used for the all-events label reads 2 of 3 columns (`PROJECT 2/3 COLUMNS`) | Projection pushdown: `RESTYPE` is not needed there |
| The unoptimised plan scans the key file three times and the event file three times; the optimised plan has one scan of the key file (a `CACHE` node used by the censoring join and both label branches) and two scans of the event file | Scans are shared instead of repeated |
| The two filters above each inner join are replaced by one `FUSED PREDICATE` inside the `INNER JOIN` node, with `BUILD SIDE: PreferRight` | The range condition is evaluated while the join output is produced, and the hash table is built on the small event table; there is no dedicated interval-join operator |
| The condition `REPDTE <= 2025-06-30` is computed once (`__POLARS_CSER_…`) for the three output expressions | Common sub-expression elimination |
| `UNIQUE … BY None` after each join | Distinct (`CERT`, `REPDTE`) pairs, so that a bank-quarter with two events in the window is flagged once |

**Table P3-8: Laptop, median of 7 runs [min – max]**

| Scale | pandas time (s) | Polars time (s) | Polars in-memory time (s) | pandas peak (MB) | Polars peak (MB) | Polars in-memory peak (MB) |
|:---|:---|:---|:---|---:|---:|---:|
| ×1 | 0.51 [0.48–0.54] | 0.12 [0.11–0.13] | 0.17 [0.15–0.19] | 286 | 259 | 395 |
| ×2 | 1.10 [1.08–1.15] | 0.21 [0.21–0.22] | 0.31 [0.30–0.32] | 468 | 337 | 645 |
| ×4 | 2.08 [2.01–2.28] | 0.40 [0.39–0.42] | 0.61 [0.58–0.71] | 822 | 482 | 1,109 |

Polars is 4.3, 5.2 and 5.2 times faster than pandas at ×1, ×2 and ×4, with a peak memory 9%, 28% and 41% lower; against its own in-memory engine, the default engine is 1.4–1.5 times faster and uses 34–57% less memory. The advantage over pandas is much larger than for T1 (1.1–1.4 times, §4.3). T2 reads only two columns and joins against a table of a few thousand rows, and the Polars plan shows shared scans, pushdown and a fused predicate; we did not isolate which of these explains the gap, and the ratio applies to the straightforward pandas formulation used here. Doubling the rows roughly doubles the time for both.

**Table P3-9: Colab (2 vCPUs), median [min – max], seconds**

| Scale | pandas | Polars 1.35.2 | Spark (default settings) | Spark / Polars | Spark / pandas |
|:---|:---|:---|:---|---:|---:|
| ×1 | 0.43 [0.42–0.80] | 0.20 [0.17–0.60] | 2.52 [1.65–4.12] | 12.6 | 5.9 |
| ×2 | 0.79 [0.76–1.09] | 0.35 [0.35–0.49] | not run | – | – |
| ×4 | 1.81 [1.74–2.20] | 0.76 [0.74–1.06] | 5.65 [5.10–7.72] | 7.4 | 3.1 |

Spark ran with 200 shuffle partitions, AQE on and 5 repeats; the start-up of the session took 12.2 s (×1) and 7.1 s (×4), and the first run 8.2 s and 10.4 s. A cold ×1 run therefore costs about 20 s against 0.2 s for Polars. Spark's time grows by a factor of 2.2 when the rows grow four times, against 3.8 for Polars and 4.2 for pandas, so the ratio to Polars falls from 12.6 to 7.4 but Spark is still slower at ×4. The Colab peak memory of Polars (455, 689 and 1,228 MB) is 0–15% *higher* than pandas (413, 684, 1,066 MB), unlike on the laptop; it is closer to the laptop's in-memory engine than to its default engine, which is consistent with, but does not prove, that Polars 1.35.2 uses the in-memory engine by default. Operating system, memory measure and Polars version differ, so the two environments are not compared.

**Spark's plan.** Because the event table is small, Spark chose `BroadcastHashJoin … BuildRight` with a `BroadcastExchange` of the small side for the joins (nodes 10, 22 and 37 of the plan) [confirm in `spark_plan_t2_x1.txt` which joins are broadcast and whether any `Exchange` or `SortMergeJoin` remains]. The large key table is therefore not shuffled for these joins, unlike the sort-merge join of T1 (§4.5), which explains why T2 is cheaper for Spark in relative terms; it is the join pattern (large table against a small one) that scales out well, but at this size it brings no advantage over a single node.