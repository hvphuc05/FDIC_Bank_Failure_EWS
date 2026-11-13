# Data Quality Audit Report (Part 2 - Step 03)

**Audit Timestamp**: 2026-11-13T03:43:37+00:00 UTC  
**Source Ingestion Run**: 2026-11-08T11:40:41+00:00 UTC (Latest Available REPDTE: 20260630)  
**Execution Mode**: FULL 170-QUARTER AUDIT  

---

## 1. Panel Structure & Continuity

- **Total Rows Audited**: 1,682,616
- **Unique Bank Charters (`CERT`)**: 24,064
- **Temporal Span**: 170 quarters (1984Q1 to 2026Q2)
- **Quarterly Density Range**: 4,313 to 18,133 banks/quarter
- **Primary Key Violations (`CERT, REPDTE`)**: 0 duplicates (Null `CERT`: 0)
- **Internal Reporting Gaps**: 32 banks (92 missing bank-quarters)
- **Single-Quarter Reporters**: 204 banks
- **Banks Ceasing Filings Before Panel End**: 19,751 banks

---

## 2. Temporal Missingness & Structural Inception Years

| column | first_year_with_data | pct_null_first_panel_year | pct_null_last_panel_year |
|---|---|---|---|
| STNAME | 1984 | 0.00 | 0.00 |
| ASSET | 1984 | 0.00 | 0.00 |
| DEP | 1984 | 0.00 | 0.00 |
| DEPDOM | 1984 | 0.00 | 0.00 |
| EQ | 1984 | 0.16 | 0.39 |
| BRO | 1984 | 17.74 | 0.39 |
| CHBAL | 1984 | 0.00 | 0.00 |
| SC | 1984 | 0.00 | 0.00 |
| LNLSGR | 1984 | 0.00 | 0.00 |
| LNLSNET | 1984 | 0.00 | 0.00 |
| LNRE | 1984 | 0.00 | 0.00 |
| LNCI | 1984 | 0.00 | 0.00 |
| LNCON | 1984 | 0.00 | 0.39 |
| LNATRES | 1984 | 0.16 | 0.39 |
| NCLNLS | 1984 | 1.67 | 0.00 |
| P3ASSET | 2001 | 100.00 | 0.00 |
| P9ASSET | 1984 | 19.24 | 0.00 |
| NAASSET | 1984 | 19.24 | 0.00 |
| NTLNLS | 1984 | 18.49 | 0.39 |
| ORE | 1984 | 0.16 | 0.39 |
| NETINC | 1984 | 0.92 | 0.39 |
| INTINC | 1984 | 0.92 | 0.39 |
| EINTEXP | 1984 | 1.67 | 0.39 |
| NONII | 1984 | 0.92 | 0.39 |
| NONIX | 1984 | 0.92 | 0.39 |
| ELNATR | 1984 | 18.49 | 0.39 |
| ROA | 1984 | 0.00 | 0.00 |
| ROE | 1984 | 3.03 | 0.42 |
| NIMY | 1984 | 0.00 | 0.00 |
| RBC1AAJ | 1984 | 0.00 | 0.00 |
| RBCRWAJ | 1984 | 0.00 | 0.00 |

---

## 3. Plausibility Checks & Accounting Rule Violations

| rule | description | class | n_tested | n_flagged | pct_of_tested | n_banks | first_quarter | last_quarter |
|---|---|---|---|---|---|---|---|---|
| R01 | ASSET <= 0 (zero or negative total assets) | error-like | 1682616 | 0 | 0.00 | 0 |  |  |
| R02 | DEP < 0 (balance item assumed non-negative) | error-like | 1682615 | 0 | 0.00 | 0 |  |  |
| R03 | DEPDOM < 0 (balance item assumed non-negative) | error-like | 1682615 | 0 | 0.00 | 0 |  |  |
| R04 | SC < 0 (balance item assumed non-negative) | error-like | 1682615 | 96 | 0.01 | 40 | 1984Q3 | 2010Q2 |
| R05 | CHBAL < 0 (balance item assumed non-negative) | error-like | 1682615 | 2 | 0.00 | 1 | 2005Q1 | 2005Q2 |
| R06 | BRO < 0 (balance item assumed non-negative) | error-like | 1665855 | 0 | 0.00 | 0 |  |  |
| R07 | LNLSGR < 0 (balance item assumed non-negative) | error-like | 1682615 | 32 | 0.00 | 11 | 1984Q1 | 1993Q2 |
| R08 | LNRE < 0 (balance item assumed non-negative) | error-like | 1682615 | 6 | 0.00 | 1 | 1992Q3 | 1993Q4 |
| R09 | LNCI < 0 (balance item assumed non-negative) | error-like | 1682615 | 2 | 0.00 | 1 | 1991Q1 | 1991Q2 |
| R10 | LNCON < 0 (balance item assumed non-negative) | error-like | 1678676 | 0 | 0.00 | 0 |  |  |
| R11 | LNATRES < 0 (balance item assumed non-negative) | error-like | 1678467 | 9 | 0.00 | 7 | 1984Q4 | 2024Q3 |
| R12 | NCLNLS < 0 (balance item assumed non-negative) | error-like | 1680117 | 3 | 0.00 | 3 | 1990Q2 | 1993Q2 |
| R13 | P3ASSET < 0 (balance item assumed non-negative) | error-like | 710691 | 0 | 0.00 | 0 |  |  |
| R14 | P9ASSET < 0 (balance item assumed non-negative) | error-like | 1604885 | 0 | 0.00 | 0 |  |  |
| R15 | NAASSET < 0 (balance item assumed non-negative) | error-like | 1604886 | 2 | 0.00 | 2 | 1990Q2 | 1993Q2 |
| R16 | ORE < 0 (balance item assumed non-negative) | error-like | 1678467 | 646 | 0.04 | 225 | 1984Q1 | 2011Q4 |
| R17 | DEPDOM > DEP + 1 (domestic deposits exceed total deposits by > $1,000) | error-like | 1682615 | 0 | 0.00 | 0 |  |  |
| R18 | LNLSNET > ASSET + 1 (net loans exceed total assets by > $1,000) | error-like | 1682615 | 2 | 0.00 | 2 | 1989Q4 | 1990Q2 |
| R19 | REPDTE is not a standard quarter-end (0331, 0630, 0930, 1231) | error-like | 1682616 | 0 | 0.00 | 0 |  |  |
| R20 | |ROA| > 50% (implausible quarterly return on assets) | extreme value | 1682616 | 1056 | 0.06 | 250 | 1984Q1 | 2026Q2 |
| R21 | DEP > ASSET (deposits exceed assets; characteristic of balance-sheet insolvency) | informational | 1682615 | 6204 | 0.37 | 1561 | 1984Q1 | 2025Q2 |
| R22 | EQ <= 0 (book equity non-positive; critical insolvency indicator) | informational | 1678468 | 15683 | 0.93 | 2845 | 1984Q1 | 2026Q2 |
| R23 | Sign of NETINC differs from sign of ROA (divergent net income and ROA) | informational | 1677924 | 886 | 0.05 | 885 | 1984Q3 | 2003Q4 |
| R24 | |LNLSNET - (LNLSGR - LNATRES)| > 1% of LNLSGR (allowance identity discrepancy) | informational | 1678467 | 6 | 0.00 | 6 | 1984Q1 | 1992Q1 |
| R25 | |LNLSNET - (LNLSGR - LNATRES)| > 5% of LNLSGR (allowance identity discrepancy) | informational | 1678467 | 2 | 0.00 | 2 | 1984Q3 | 1991Q1 |

---

## 4. Empirical Distribution of Key Risk Ratios

| column | min | p0.1 | p1 | median | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|
| ASSET | 1.00 | 1,597.00 | 5,416.00 | 86,130.00 | 10,488,691.65 | 153,189,680.00 | 4,091,315,000.00 |
| ROA | -761.90 | -22.43 | -5.66 | 0.97 | 4.16 | 33.15 | 860.61 |
| ROE | -354,657.80 | -375.93 | -60.11 | 10.34 | 38.95 | 125.46 | 301,050.00 |
| NIMY | -4,515.15 | -4.59 | 0.24 | 4.05 | 7.72 | 15.74 | 1,125.00 |
| RBC1AAJ | -1,524.07 | -8.59 | 0.00 | 9.10 | 68.07 | 104.40 | 466,500.00 |
| RBCRWAJ | -16,629.63 | -9.64 | 0.00 | 13.28 | 109.15 | 496.12 | 274,636.36 |

---

## 5. Year-to-Date (YTD) Cumulative Reporting Evidence

| column | role | n_complete_bank_years | share_Q1<=Q2<=Q3<=Q4 | median_abs_Q4_over_abs_Q1 | evidence_finding |
|---|---|---|---|---|---|
| NETINC | flow candidate | 407328 | 0.77 | 4.08 | YTD cumulative-like (de-cumulation required in step 04) |
| INTINC | flow candidate | 407328 | 1.00 | 4.11 | YTD cumulative-like (de-cumulation required in step 04) |
| EINTEXP | flow candidate | 407068 | 1.00 | 4.05 | YTD cumulative-like (de-cumulation required in step 04) |
| NONII | flow candidate | 407328 | 0.97 | 4.26 | YTD cumulative-like (de-cumulation required in step 04) |
| NONIX | flow candidate | 407328 | 0.99 | 4.16 | YTD cumulative-like (de-cumulation required in step 04) |
| ELNATR | flow candidate | 397938 | 0.91 | 4.33 | YTD cumulative-like (de-cumulation required in step 04) |
| NTLNLS | flow candidate | 389153 | 0.59 | 5.14 | Mixed flow / volatile quarterly increments |
| ASSET | control (stock) | 408605 | 0.33 | 1.04 | Quarterly or stock-like (control pattern) |

---

## 6. Bank Failure Registry Audit & Panel Linkage

- **Total Registry Events**: 4,117 records (0 unparsed dates)
- **Failures in Study Scope (1984-01-01 to 2026-06-30)**: 3,273
  - Successfully Linked to Panel: **3,251**
  - Unmatched to Panel: 22 entities
  - In-Scope Null `CERT` Records: **0** (Zero label leakage)
- **Post-Exit Filings (`gap_days < 0`)**: 439 institutions
- **Median Latency (Last Report to Failure Date)**: 48.0 days

| last_report_vs_failure_date | n_failures |
|---|---|
| Post-exit reporting (after failure date) | 439 |
| 0-92 days (immediate preceding quarter) | 2472 |
| 93-184 days (lag 2 quarters) | 330 |
| 185-366 days (lag 3-4 quarters) | 10 |
| more than 366 days (severe reporting hiatus) | 0 |

---

## 7. Exit Group Breakdown (Failure vs M&A / Voluntary)

| exit_group | n_banks |
|---|---|
| active_reporting_at_panel_end | 4307 |
| exited_confirmed_failure | 3210 |
| exited_unlisted_other_exit | 16541 |
| reporting_at_end_listed_failed | 6 |

---

## 8. Problem Register & Downstream Handling Plan (Rubric Requirement)

| id | area | problem | evidence | proposed_handling | methodological_reason |
|---|---|---|---|---|---|
| P01 | Schema | Cross-quarter data type discrepancies | 14 of 33 columns possess heterogeneous data types (int64/double/null) | Enforce lossless unified typed schema established in Step 02 | Prevents memory bloating and Arrow serialization errors without altering values. |
| P02 | Missing Values | Structural temporal missingness due to evolving Basel regimes | P3ASSET: first available 2001 | Preserve structural nulls; evaluate native tree-based missingness handling or fit-on-train imputers | Backfilling older periods using modern values violates point-in-time constraints (look-ahead bias). |
| P03 | Income Statement | Cumulative Year-to-Date (YTD) flow structure | Median |Q4|/|Q1| ~ 4 with Q1 saw-tooth resets for: NETINC, INTINC, EINTEXP, NONII, NONIX, ELNATR | Implement within-year quarterly de-cumulation in Step 04 for consecutive reports; retain Q1 as reported | Avoids distorting financial profitability ratios by mixing multi-quarter flows with single-quarter assets. |
| P04 | Panel Continuity | Internal reporting hiatuses between bank inception and exit | 32 banks with 92 missing bank-quarters | Do not impute missing quarters; construct rolling/lagged features strictly on calendar dates, not row offsets | Prevents computing rolling growth metrics across non-consecutive temporal intervals. |
| P05 | Accounting Rules | Supervisory rule violations (e.g. non-positive assets or negative deposits) | R04: 96 rows; R05: 2 rows; R07: 32 rows; R08: 6 rows; R09: 2 rows; R11: 9 rows; R12: 3 rows; R15: 2 rows; R16: 646 rows; R18: 2 rows | Flag accounting violations; set invalid denominators to NaN; preserve legitimate distress signals (EQ<=0) | Arbitrary row dropping risks removing near-insolvent banks, introducing survivorship bias. |
| P06 | Target Linkage | Failure registry records unmatchable to Call Report panel by CERT | 0 null CERTs; 22 unmatched CERTs (in study scope) | Audit unmatched entities; document as regulatory perimeter difference (e.g., non-reporting branches) | Prevents misattributing failed institutions as healthy operational entities. |
| P07 | Target Definition | Multiple failure events associated with a single bank charter | 77 in-scope failure rows with repeated CERT numbers | Use unique failure ID as surrogate key; link failures to active reporting episodes chronologically | Captures multi-stage FDIC assistance transactions without duplicate row collisions. |
| P08 | Data Leakage Risk | Financial filings observed after recorded failure date | 439 linked failures exhibit filings dated after failure date | Censor observations post-failure date in Step 04; label only valid operating quarters prior to failure | Eliminates potential label leakage from receivership or resolution filings. |
| P09 | Exit Resolution | Banks ceasing filings without FDIC failure designation (M&A / Voluntary exits) | 16541 banks disappear from panel without matching FDIC failure registry | Classify as M&A/voluntary exit (label y=0); apply right-censoring in the 4 quarters preceding absorption | Assigning target y=1 to healthy merged banks would severely corrupt early-warning precision. |
| P10 | Look-Ahead Bias | Terminal regulatory snapshot fields in Institutions table | Institutions table provides current status (ACTIVE=27835 records) | Strictly isolate Institutions fields to exit categorization; exclude entirely from model feature matrices | Using terminal statuses as model inputs would directly trigger the exam's Leakage Gate penalty. |
