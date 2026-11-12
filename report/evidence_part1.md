# Evidence – Part 1 (Acquisition & Verification)

## Download session
- Download date (UTC): 2026-11-08T10:24:21+00:00 to 2026-11-08T11:40:41+00:00
- Script: src/01_download.py, version 1.0
- Data index / API base: https://api.fdic.gov/banks (latest_repdte_at_run: 20260630)

## Verified counts (--verify-only: ALL CHECKS PASSED)
- Financials: 170 quarters, 1,682,616 rows, 24,064 distinct CERT, 236.8 MB
- Failures: 4,117 rows, 10 columns, 231.2 KB
- Institutions: 27,835 rows, 9 columns, 836.4 KB

## Independent check against the live API & Manifest
| Quarter  | API meta.total | Rows in Parquet file | Status | Checksum SHA-256 (first 8 chars) |
|:---------|:--------------:|:--------------------:|:------:|:--------------------------------:|
| 19841231 | 17,949         | 17,949               | OK     | 158f69a7...                      |
| 19901231 | 15,426         | 15,426               | OK     | 6520875a...                      |
| 20221231 | 4,773          | 4,773                | OK     | 4747040e...                      |

## Failures: CERT issues (Audit Analysis)
- Null CERT total: 488 (Failed from 1984 on: **0** - 100% of failures within study scope have valid CERTs).
- Repeated non-null CERT: 44 unique CERTs (Total rows: 88; Rows with failure from 1984 on: 77).
- Resolution: `ID` is preserved as the unique primary key for the `failures` table to prevent data loss.

## Warnings in download.log
- Absent / entirely null columns: `['P3ASSET']` is entirely null in early quarters (1984–1990) as Past Due 30-89 days was not tracked or reported under this exact field by FDIC during that period.
- Retries or errors: Zero fatal errors; automatic back-off handled minor rate-limits seamlessly.

## Machine Hardware Specifications
- RAM: 8.00 GB (TotalPhysicalMemory: 8,419,581,952 bytes)
- CPU: Intel(R) Core(TM) i5-10210U CPU @ 1.60GHz (4 Cores, 8 Logical Processors)
- OS: Windows 11 64-bit