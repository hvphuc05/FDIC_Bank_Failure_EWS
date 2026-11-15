# CONTEXT.md — BFT65B FDIC Bank Failure EWS

Tài liệu bàn giao giữa các phiên làm việc. Ngày lập: 13/11/2026 (thứ Bảy). Hạn nộp: 23:00 ngày 26/11/2026 (còn 13 ngày).

Quy ước đọc tài liệu:
- **[ĐÃ XÁC MINH]**: có log, file kết quả hoặc nguồn chính thức được đọc trong phiên.
- **[BÁO CÁO CỦA SINH VIÊN]**: sinh viên cho biết, chưa có số liệu đi kèm trong phiên.
- **[SUY LUẬN]**: suy ra từ dữ liệu, chưa kiểm chứng trực tiếp.
- **[CHƯA CHỐT]**: quyết định thiết kế chưa được thống nhất.
- Không số liệu nào trong tài liệu này được suy đoán. Số nào không có trong phiên được ghi "chưa có".

---

# MỤC 1: BỐI CẢNH DỰ ÁN & QUY CHUẨN ĐỒ ÁN

## 1.1 Thông tin chung
- Tên đồ án: BFT65B – Big Data Analytics in Finance, Final Individual Research Report: *A Big-Data Pipeline for Bank Failure Early Warning*. Trường: National Economics University.
- Sinh viên: Hứa Văn Phúc, MSSV 11231006 [BÁO CÁO CỦA SINH VIÊN].
- Vai trò trong đề: Data Engineer tại Ngân hàng Nhà nước Việt Nam. Dữ liệu giám sát Việt Nam không công khai nên pipeline được xây trên dữ liệu FDIC của Mỹ.
- Dữ liệu: FDIC Call Reports 1984Q1–2026Q2 (170 quý), bảng Failures, bảng Institutions, liên kết bằng `CERT`.
- Đề đã nêu ba câu hỏi:
  - Q1: lưu trữ và xử lý bốn thập kỷ báo cáo trên laptop thế nào, khi nào cần cụm Spark.
  - Q2: dữ liệu thô có những vấn đề chất lượng nào, pipeline xử lý chúng ra sao mà không gây look-ahead bias.
  - Q3: kết quả pipeline có hỗ trợ một mô hình cảnh báo sớm được kiểm định trung thực hay không.

## 1.2 Rubric (đúng theo đề)
| Phần | Điểm |
|---|---|
| Part 1 – Acquisition & Storage | 15 |
| Part 2 – Data Quality & Panel | 20 |
| Part 3 – Scalable Processing (Polars, benchmark với pandas, PySpark khớp kết quả, kế hoạch thực thi, khi nào cần cluster) | 25 |
| Part 4 – Feature Pipeline | 15 |
| Part 5 – Modelling & Validation (kèm Spark MLlib demo) | 15 |
| Part 6 – Conclusions | 5 |
| Reproducibility | 5 |

Tổng 100. Các Part 1–4, 6 và Reproducibility cộng lại là 85 điểm, Part 5 là 15 điểm.

## 1.3 Quy định nộp bài và nhãn (trích từ đề)
- Báo cáo tiếng Anh, PDF hoặc DOCX, tối đa 20 trang cộng phụ lục tối đa 10 trang. Trang đầu ghi: họ tên, MSSV, URL GitHub, phương pháp và ngày tải dữ liệu, số dòng, giai đoạn dev và test cùng số failure của từng giai đoạn.
- Repo GitHub public từ hạn nộp đến khi có điểm. Có `README.md`, `requirements.txt` đã ghim phiên bản, code hoặc notebook (có output, seed cố định). Không commit file dữ liệu.
- Phải trích dẫn mọi nguồn, kể cả công cụ AI, trong cả báo cáo và README. Chép không trích dẫn bị xử lý như đạo văn.
- Nhãn: `y(i,q) = 1{bank i fails trong (q, q + 4 quý]}`. Sáp nhập không phải failure.
- Chỉ dùng quý đến 2025Q2 để mô hình hóa (nhãn của các quý sau chưa đầy đủ).
- Chia dữ liệu: test sau dev, **khoảng cách ít nhất 4 quý**, test có **ít nhất 50 failure**. Tuning, chọn đặc trưng và ngưỡng chỉ dùng dev. Test dùng đúng một lần.
- Công cụ cho phép: pandas/NumPy, scikit-learn, Polars, PyArrow, PySpark (chạy local, Colab hoặc Databricks Free Edition).
- **Leakage gate**: dùng bất kỳ thông tin tương lai nào thì Part 4 và Part 5 bị giới hạn ở 30% điểm của hai phần đó.

## 1.4 Môi trường
- Hệ điều hành: Windows (sinh viên nêu "Windows 11"; chưa đối chiếu với `report/storage_benchmark_meta.json`). VS Code. Python 3.12.3. Virtualenv `.venv` (dấu nhắc phải có `(.venv)`).
- Phần cứng [ĐÃ XÁC MINH bằng lệnh hệ thống]: Intel Core i5-10210U, 4 nhân, 8 luồng, 1.6 GHz cơ bản; RAM 8,030 MB.
- Thư viện đã có (từ `requirements.txt`): pandas 3.1.1, numpy 2.5.4, pyarrow 26.0.1, polars 2.1.0, requests 2.34.2, scikit-learn 1.9.1, matplotlib 3.11.2, psutil 7.2.2, seaborn 0.13.2, jupyter. **Chưa có `pyspark`**. **Java chưa được cài** (cần cho Spark local; hoặc dùng Colab/Databricks Free). Cần quyết định trước 17/11.
- Hệ thống thư viện cần khóa lại bằng `pip freeze > requirements.txt` sau mỗi lần thêm thư viện.

## 1.5 Giao thức làm việc đã thống nhất
1. Mỗi bước: giải thích mục tiêu, input, output, phụ thuộc → đưa code đầy đủ → lệnh PowerShell cụ thể → kết quả mong đợi → chờ sinh viên gửi log thật → mới chuyển bước.
2. Không đánh dấu COMPLETED khi chưa có bằng chứng (log, file kết quả hoặc tài liệu hoàn chỉnh).
3. Trạng thái dùng: NOT STARTED, IN PROGRESS, BLOCKED, COMPLETED, NEEDS REVIEW.
4. Mẫu phản hồi: A đánh giá, B checklist, C việc song song, D bước tiếp, E cảnh báo.
5. Không bịa số liệu, nguồn, trích dẫn. Phân biệt kết quả thực nghiệm với suy luận.
6. Khi sinh viên dán lệnh vào PowerShell: **chỉ dán lệnh, không dán kết quả**; kiểm tra có `(.venv)`.
7. Mọi thay đổi code đang chạy tốt phải có lý do. Không tạo file hoặc thư mục thừa.
8. Sinh viên có dùng thêm một công cụ AI khác để sửa code và soạn nội dung (đã từng tạo bảng từ điển sai, xem Mục 5). Phải ghi nhận mọi công cụ AI vào README và báo cáo.

---

# MỤC 2: CẤU TRÚC THƯ MỤC & HIỆN TRẠNG FILE

```
FDIC_Bank_Failure_EWS/
├── .venv/                              (git-ignored)
├── .gitignore                          (loại .venv/, data_raw/, data_processed/)
├── README.md                           (khung; cần thêm lệnh chạy 02, 03 và mục nguồn/AI)
├── config.py
├── requirements.txt
├── check_failures_detail.py            (script kiểm tra tạm, ở thư mục gốc, không thuộc pipeline)
├── data_raw/fdic/
│   ├── financials/REPDTE=YYYYMMDD.parquet   (170 file, 1984-03-31 → 2026-06-30)
│   ├── failures.parquet
│   ├── institutions.parquet
│   ├── manifest.json
│   └── download.log
├── data_processed/                     (thư mục tạm của 02 bị xóa sau khi chạy, trừ khi dùng --keep-files)
├── notebooks/exploratory_analysis.ipynb
├── report/
│   ├── schema_report.csv
│   ├── storage_benchmark_results.csv
│   ├── storage_benchmark_meta.json
│   ├── dq_*.csv                        (15 bảng; xem 2.3)
│   ├── dq_report.md
│   ├── dq_summary.json
│   └── figures/
│       ├── storage_benchmark.png
│       ├── dq_missing_heatmap.png
│       ├── dq_banks_per_quarter.png
│       └── dq_ytd_sawtooth.png
└── src/
    ├── 01_download.py
    ├── 02_storage_benchmark.py
    └── 03_quality_report.py
    (04 → 08 chưa tạo)
```

## 2.1 `config.py` (các giá trị chính)
- `PROJECT_ROOT`, `RAW_DATA_DIR`, `PROCESSED_DATA_DIR`, `SRC_DIR`, `NOTEBOOK_DIR`, `REPORT_DIR`, `RANDOM_SEED = 42`.
- `FDIC_API_BASE = "https://api.fdic.gov/banks"`, `FDIC_RAW_DIR = data_raw/fdic`.
- `FDIC_PAGE_SIZE = 10000`, `FDIC_REQUEST_TIMEOUT_SEC = 180`, `FDIC_MAX_RETRIES = 6`, `FDIC_PAUSE_SEC = 1.0`.
- `FDIC_START_DATE = "1984-03-31"`, `FDIC_END_DATE = "2026-06-30"`.
- `FDIC_FINANCIAL_FIELDS` (33 cột, thứ tự cố định): `CERT, REPDTE, STNAME, ASSET, DEP, DEPDOM, EQ, BRO, CHBAL, SC, LNLSGR, LNLSNET, LNRE, LNCI, LNCON, LNATRES, NCLNLS, P3ASSET, P9ASSET, NAASSET, NTLNLS, ORE, NETINC, INTINC, EINTEXP, NONII, NONIX, ELNATR, ROA, ROE, NIMY, RBC1AAJ, RBCRWAJ`.
- `FDIC_RATIO_FIELDS = ["ROA", "ROE", "NIMY", "RBC1AAJ", "RBCRWAJ"]` (do 02 và 03 cần).
- `FDIC_FAILURE_FIELDS = ID, CERT, NAME, CITYST, FAILDATE, FAILYR, COST, QBFASSET, QBFDEP, RESTYPE` (10 cột; `ID` được thêm sau).
- `FDIC_INSTITUTION_FIELDS = CERT, NAME, STNAME, BKCLASS, ACTIVE, ESTYMD, ENDEFYMD, CHANGEC1, INSDATE` (9 cột).
- `EXPECTED_FINANCIAL_ROWS = 1_680_000`, `EXPECTED_FAILURE_ROWS = 4_100` (chỉ để cảnh báo mềm, không phải điều kiện đạt/trượt).

## 2.2 Trạng thái từng script
| File | Trạng thái | Ghi chú |
|---|---|---|
| `src/01_download.py` | COMPLETED | Tải 170 quý, SHA-256 trong manifest, resume được. Lệnh: `python src/01_download.py`, `--verify-only`, `--force`, `--start`, `--end`, `--out`. Khóa kiểm tra: Financials `(CERT, REPDTE)` nghiêm ngặt; Failures khóa `["ID"]` nghiêm ngặt; Institutions khóa `["CERT"]` chỉ cảnh báo. `ID` chỉ được giữ ở Failures (hàm `to_frame` bỏ `ID` trừ khi nằm trong danh sách field yêu cầu). `validate_frame` thêm cảnh báo về `CERT` null và `CERT` lặp của Failures. Phiên bản này là bản gốc có sửa tay; **không phải** bản "1.1" (không có `parse_retry_after`, `check_stored_file`, `build_quarters`). |
| `src/02_storage_benchmark.py` | COMPLETED | Chạy đầy đủ, kết thúc `ALL LAYOUTS VERIFIED`. Tham số: `--quick`, `--repeats`, `--warmup`, `--keep-files`. Đầu ra có hậu tố `_quick` nếu chạy `--quick`. |
| `src/03_quality_report.py` | **NEEDS REVIEW** | Chạy xong, số liệu nhất quán nội bộ. Câu chữ bị phản đối ở các lượt trước đã được sửa (sinh viên báo `Select-String` không hiện dòng nào). Còn các bản vá chưa làm: xem 2.4. Gọi lại `list_quarters`, `load_raw`, `build_naive` của 02 bằng `importlib`. Tham số: `--quick`. |
| `check_failures_detail.py` | IN PROGRESS | Chạy được mục 1–4. Mục 5 báo `ValueError` vì `ID` trong `failures.parquet` là chuỗi còn `ID` trong CSV là số nguyên. Bản sửa đã được đưa ra (thay `d = detail.merge(f[[...]], ...)` bằng đoạn chuyển `ID` sang số rồi mới merge). **Chưa có xác nhận sinh viên đã áp dụng và chạy lại.** |
| `src/04_build_panel.py` | NOT STARTED, BLOCKED | Chờ kết quả `check_failures_detail.py` mục 5–7, định nghĩa field thật của Failures/Institutions, và quyết định ở Mục 7. |
| `src/05` đến `src/08` | NOT STARTED | Theo cấu trúc đã thống nhất: 05 Polars so với pandas, 06 Spark, 07 features, 08 model (kèm MLlib demo). |

## 2.3 Các bảng `report/dq_*.csv` do 03 tạo (15 bảng)
`dq_panel_by_quarter`, `dq_missing_by_column`, `dq_missing_by_year`, `dq_first_year_with_data`, `dq_rule_checks`, `dq_quantiles`, `dq_ytd_evidence`, `dq_failures_gap_to_last_report`, `dq_failures_unlinked_in_scope`, `dq_failures_repeated_cert_in_scope`, `dq_failures_linked_detail`, `dq_exit_groups`, `dq_exits_by_year`, `dq_exit_vs_institutions`, `dq_problem_register`.

## 2.4 Các bản vá còn nợ cho `03` (chưa thực hiện)
1. `exit_analysis` hiện dùng mọi bản ghi registry trong phạm vi. Cần tách theo `RESTYPE`; đổi tên nhóm `exited_confirmed_failure` (tên này sai nghĩa).
2. Phân tích "báo cáo sau ngày failure" cần tách theo `RESTYPE`.
3. Mô tả luật R20 ghi "quarterly return on assets"; từ điển nói `ROA` là tỷ lệ **đã quy năm**. Cần sửa.
4. Ngưỡng YTD (tỷ lệ 3.0–5.0) làm `NTLNLS` bị xếp "mixed" (tỷ lệ 5.14), trong khi từ điển xếp `NTLNLS` vào nhóm YTD. Cần chỉnh cách phân loại hoặc ghi nguồn từ điển.
5. `cert_repeated` tính trên toàn registry, nên "77 dòng, 42 CERT" lẫn các CERT có bản ghi thứ hai nằm ngoài phạm vi. Trong phạm vi chỉ có 35 CERT, 70 dòng (xem Mục 3). Cần tính lại trong phạm vi.
6. 15 cột "không âm" (`NONNEG_COLS`) chưa được từ điển xác nhận (nhiều cột không có mô tả). Chuyển phân loại từ "error-like" sang "đáng ngờ theo logic kế toán".

---

# MỤC 3: CÁC CON SỐ THỰC TẾ ĐÃ XÁC MINH

## 3.1 Tải dữ liệu (Part 1) [ĐÃ XÁC MINH bằng `--verify-only`: ALL CHECKS PASSED]
- Financials: 170/170 quý (1984-03-31 đến 2026-06-30), **1,682,616 dòng**, **24,064 `CERT`**, 236.8 MB (170 file Parquet snappy), 0 trùng `(CERT, REPDTE)`, 0 `CERT` null. Số dòng mỗi quý: nhỏ nhất 4,313, lớn nhất 18,133.
- Failures: 4,117 dòng, 10 cột, 231.2 KB. `ID` không null, không trùng; `CERT` có 488 null và 44 giá trị không null bị lặp (kết quả test API).
- Institutions: 27,835 dòng, 9 cột, 836.4 KB.
- Manifest: `last_run_utc = 2026-11-08T11:40:41+00:00`; `latest_repdte_at_run = 20260630`.
- Định dạng bộ lọc `REPDTE:YYYYMMDD` (dạng gộp) chạy đúng trong lần tải đầy đủ. Tài liệu FDIC ghi định dạng `yyyy-mm-dd`; dạng gộp không có trong tài liệu nhưng được xác nhận bằng kết quả thực tế.
- Đối chiếu độc lập với API: quý 19901231 có `meta.total = 15426`. Sinh viên báo ba lần đối chiếu (19841231, 19901231, 20221231) đều khớp [BÁO CÁO CỦA SINH VIÊN]; chưa thấy số của hai quý còn lại và số dòng file Parquet.
- Chỉ mục dữ liệu từ API: `risview_20260819185831`, tạo lúc `2026-08-19T18:58:33Z`. Mỗi bản ghi Financials có `ID` dạng `CERT_REPDTE` (ví dụ `10002_19901231`). `ID` của Failures là chuỗi văn bản. `FAILDATE` có dạng `M/D/YYYY`; `REPDTE` là chuỗi 8 ký tự `YYYYMMDD`.
- Chưa xác nhận được: sinh viên đã push GitHub, repo public, và không có `data_raw/` trong commit.

## 3.2 Schema (Part 1, từ 02) [ĐÃ XÁC MINH]
- 6 schema khác nhau trong 170 file. `pa.concat_tables` không thống nhất schema **báo lỗi** ("Schema at index 7 was different"). 14 trên 33 cột có nhiều kiểu qua các quý (`P3ASSET` lẫn `int64` và `null`).
- Sau thống nhất: `CERT` kiểu `int32` (lớn nhất 91,393), 25 cột số tiền kiểu `int64` (mọi giá trị đều là số nguyên, không cột nào phải giữ `float64`), `REPDTE` kiểu `date32`, 5 cột tỷ lệ kiểu `float64`.
- Bản "naive" và bản "typed" có cùng số dòng, số null và tổng từng cột.

## 3.3 Benchmark lưu trữ (từ 02, một lần chạy 10:14:16–10:20:32) [ĐÃ XÁC MINH]
Nguồn duy nhất cho báo cáo: `report/storage_benchmark_results.csv`. Đo bằng trung vị của 5 lần chạy sau 1 lần warm-up; cache hệ điều hành đang "nóng". Tất cả `same_data_fingerprint = True`.

| Layout | Số file | MB | So với CSV | Ghi (s) | Đọc toàn bộ (s) | Đọc 5 cột (s) | Đọc 1 quý 20081231 (s) | exact_equal |
|---|---|---|---|---|---|---|---|---|
| csv (typed) | 1 | 350.10 | 1.00 | 4.51 | 1.58 | 0.62 | 1.66 | True |
| pq_naive_snappy | 1 | 211.68 | 0.60 | 10.40 | 0.51 | 0.17 | 0.02 | None |
| pq_typed_snappy | 1 | 205.94 | 0.59 | 9.12 | 0.42 | 0.07 | 0.03 | True |
| pq_typed_zstd | 1 | 174.07 | 0.50 | 10.19 | 0.48 | 0.08 | 0.05 | True |
| pq_zstd_by_year | 43 | 181.60 | 0.52 | 2.69 | 0.52 | 0.10 | 0.03 | True |
| pq_zstd_by_quarter | 170 | 189.58 | 0.54 | 2.60 | 0.52 | 0.13 | 0.03 | True |

- 170 file như tải về: 236.8 MB.
- Parquet một file được sắp xếp theo `(REPDTE, CERT)`, row group 50,000 dòng.
- Một bản tóm tắt cũ mà sinh viên gửi có số khác (ví dụ 0.548 giây). Không dùng bản đó.

Diễn giải được phép:
- Parquet nhỏ hơn CSV 40%–50%; đọc toàn bộ nhanh hơn khoảng 3–3.8 lần; đọc 5 cột nhanh hơn khoảng 9 lần (typed snappy: 0.07 giây so với 0.62 giây).
- Schema typed chỉ nhỏ hơn naive 2.7% (205.94 so với 211.68 MB), nhưng đọc 5 cột nhanh hơn (0.07 so với 0.17 giây).
- zstd nhỏ hơn snappy 15% (174.07 so với 205.94 MB), đọc toàn bộ chậm hơn một chút (0.48 so với 0.42 giây).
- Ghi Parquet một file (9–10 giây) **chậm hơn** CSV (4.51 giây); ghi phân vùng nhanh hơn (2.6–2.7 giây). Nguyên nhân chưa kiểm chứng.
- Phân vùng không cho lợi thế đọc rõ rệt so với một file đã sắp xếp (đọc 1 quý đều 0.02–0.05 giây). Chưa xem cột `min`/`max` để biết chênh lệch có vượt dao động giữa các lần chạy hay không.
- Đề xuất layout chuẩn [CHƯA CHỐT]: Parquet typed, zstd, một file, sắp xếp theo `(REPDTE, CERT)`.

## 3.4 Chất lượng dữ liệu (Part 2, từ 03) [ĐÃ XÁC MINH từ `dq_summary.json` và các bảng]
Chạy toàn bộ trong khoảng 14 giây (15:25:43–15:25:57).

**Cấu trúc panel**
- 1,682,616 dòng; 24,064 ngân hàng; 170 quý; 0 trùng khóa; 0 `CERT` null.
- 32 ngân hàng có khoảng trống báo cáo, tổng 92 ngân hàng-quý thiếu; 204 ngân hàng chỉ báo cáo một quý.
- 19,751 ngân hàng ngừng báo cáo trước 2026Q2; 4,313 ngân hàng báo cáo ở quý cuối (4,307 + 6).
- Có 249 ngân hàng đổi `STNAME` theo thời gian; `STNAME` null: 0.

**Giá trị thiếu**
- Chỉ `P3ASSET` hoàn toàn rỗng đến hết 2000 (có dữ liệu từ 2001).
- Tỷ lệ null ở năm panel đầu tiên (1984) của các cột đáng chú ý: `P9ASSET` 19.24%, `NAASSET` 19.24%, `NTLNLS` 18.49%, `ELNATR` 18.49%, `BRO` 17.74%, `ROE` 3.03%, `NCLNLS` 1.67%, `EINTEXP` 1.67%. Ở năm cuối, hầu hết còn 0.39%–0.42% (`ROE` 0.42%).
- `RBC1AAJ` và `RBCRWAJ` có dữ liệu đủ từ 1984 (0% null).
- `ROE` null: từ điển ghi hiển thị NA khi lợi nhuận giữ lại âm.

**Luật kiểm tra đã gắn cờ (chỉ gắn cờ, không xóa)**
| Luật | Dòng | Ngân hàng | Quý đầu → cuối |
|---|---|---|---|
| R04 `SC < 0` | 96 | 40 | 1984Q3 → 2010Q2 |
| R05 `CHBAL < 0` | 2 | 1 | 2005Q1 → 2005Q2 |
| R07 `LNLSGR < 0` | 32 | 11 | 1984Q1 → 1993Q2 |
| R08 `LNRE < 0` | 6 | 1 | 1992Q3 → 1993Q4 |
| R09 `LNCI < 0` | 2 | 1 | 1991Q1 → 1991Q2 |
| R11 `LNATRES < 0` | 9 | 7 | 1984Q4 → 2024Q3 |
| R12 `NCLNLS < 0` | 3 | 3 | 1990Q2 → 1993Q2 |
| R15 `NAASSET < 0` | 2 | 2 | 1990Q2 → 1993Q2 |
| R16 `ORE < 0` | 646 | 225 | 1984Q1 → 2011Q4 |
| R18 `LNLSNET > ASSET + 1` | 2 | 2 | 1989Q4 → 1990Q2 |
| R20 `|ROA| > 50` | 1,056 | 250 | 1984Q1 → 2026Q2 |
| R21 `DEP > ASSET` | 6,204 | 1,561 | 1984Q1 → 2025Q2 |
| R22 `EQ <= 0` | 15,683 | 2,845 | 1984Q1 → 2026Q2 |
| R23 dấu `NETINC` khác dấu `ROA` | 886 | 885 | 1984Q3 → 2003Q4 |
| R24 chênh định đẳng thức `LNLSNET` > 1% | 6 | 6 | 1984Q1 → 1992Q1 |
| R25 chênh định đẳng thức > 5% | 2 | 2 | 1984Q3 → 1991Q1 |

Các luật R01, R02, R03, R06, R10, R13, R14, R17, R19 không gắn cờ dòng nào.

**Bằng chứng biến lũy kế (YTD)** (bank-year đủ 4 quý)
| Biến | Số bank-year | Tỷ lệ Q1≤Q2≤Q3≤Q4 | Trung vị \|Q4\|/\|Q1\| |
|---|---|---|---|
| NETINC | 407,328 | 0.7664 | 4.08 |
| INTINC | 407,328 | 0.9952 | 4.11 |
| EINTEXP | 407,068 | 0.9951 | 4.05 |
| NONII | 407,328 | 0.9660 | 4.26 |
| NONIX | 407,328 | 0.9945 | 4.16 |
| ELNATR | 397,938 | 0.9124 | 4.33 |
| NTLNLS | 389,153 | 0.5947 | 5.14 |
| ASSET (đối chứng) | 408,605 | 0.3312 | 1.04 |

Ngưỡng 3.0–5.0 do 03 tự đặt; `NTLNLS` bị xếp "mixed" chỉ vì vượt ngưỡng.

## 3.5 Failures (từ 03 và `check_failures_detail.py`) [ĐÃ XÁC MINH]
- Registry: 4,117 dòng; 3,524 `FAILURE` và 593 `ASSISTANCE`; 0 `FAILDATE` không phân tích được; 488 `CERT` null trong toàn lịch sử (tất cả trước 1984).
- Phạm vi nghiên cứu `1984-01-01` đến `2026-06-30`: **3,273 bản ghi** = **2,861 `FAILURE` + 412 `ASSISTANCE`**.
  - 0 bản ghi `CERT` null trong phạm vi.
  - 22 bản ghi có `CERT` không nằm trong panel (15 dòng đã xem đều thuộc tháng 1–4/1984).
  - 3,251 bản ghi có `CERT` nằm trong panel (99.33%). Con số này mới là liên kết bằng `CERT`, chưa phải xác nhận trùng thực thể.
- Số bản ghi theo năm và `RESTYPE` (trong phạm vi):

| Năm | ASSISTANCE | FAILURE | Năm | ASSISTANCE | FAILURE |
|---|---|---|---|---|---|
| 1984 | 23 | 83 | 1999 | 0 | 8 |
| 1985 | 41 | 139 | 2000 | 0 | 7 |
| 1986 | 42 | 162 | 2001 | 0 | 4 |
| 1987 | 45 | 217 | 2002 | 0 | 11 |
| 1988 | 238 | 232 | 2003 | 0 | 3 |
| 1989 | 4 | 530 | 2004 | 0 | 4 |
| 1990 | 1 | 381 | 2007 | 0 | 3 |
| 1991 | 3 | 268 | 2008 | 5 | 25 |
| 1992 | 2 | 179 | 2009 | 8 | 140 |
| 1993 | 0 | 50 | 2010 | 0 | 157 |
| 1994 | 0 | 15 | 2011 | 0 | 92 |
| 1995 | 0 | 8 | 2012 | 0 | 51 |
| 1996 | 0 | 6 | 2013 | 0 | 24 |
| 1997 | 0 | 1 | 2014 | 0 | 18 |
| 1998 | 0 | 3 | 2015–2026 | 0 | 8, 5, 8, 0, 4, 4, 0, 0, 5, 2, 2, 2 (lần lượt 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026) |

  Các năm không có trong bảng (2005, 2006) không có bản ghi.
- Số vụ `FAILURE` theo giai đoạn (tính từ bảng trên): 1984–2007 = 2,314; 2008–2014 = 507; 2009–2014 = 482; 2009–2013 = 464; 2010–2014 = 342. Đây là số bản ghi registry, chưa phải số dòng nhãn dương.
- Số liệu theo năm (không giới hạn trên) từng cho 2026 = 4 bản ghi; trong phạm vi chỉ có 2. Nghĩa là có 2 sự kiện sau 30/06/2026.
- `COST` trong phạm vi: 3,231 dòng có giá trị, 42 dòng thiếu; tổng 286,544,957.6; trung vị 12,264.6 (**đơn vị chưa xác minh**).

| RESTYPE | n | null | bằng 0 | trung vị | tổng |
|---|---|---|---|---|---|
| ASSISTANCE | 412 | 19 | 8 | 17,500.0 | 5.1456e7 |
| FAILURE | 2,861 | 23 | 55 | 11,910.6 | 2.3509e8 |

- Các giá trị `COST` có phần thập phân (ví dụ 79114.979658): chưa rõ có phải đã điều chỉnh.
- Chuỗi sự kiện của CERT lặp **trong phạm vi**: `ASSISTANCE > FAILURE` 31 CERT; `FAILURE > FAILURE` 3 CERT; `ASSISTANCE > ASSISTANCE` 1 CERT. Tổng 35 CERT, 70 dòng. Sổ vấn đề của 03 ghi 77 dòng / 42 CERT; chênh 7 dòng [SUY LUẬN]: do `cert_repeated` tính trên toàn registry.
- 24 dòng đầu của file lặp CERT (First City Texas...) đều là cặp ASSISTANCE (1988) → FAILURE (1990 hoặc 1992), và các dòng FAILURE có `COST = 0`. Mới xem 24 trong 77 dòng.
- Khoảng cách giữa báo cáo cuối và `FAILDATE` (3,251 failure liên kết được): sau ngày failure 439; 0–92 ngày 2,472; 93–184 ngày 330; 185–366 ngày 10; trên 366 ngày 0. Trung vị 48.0 ngày.
- 439 báo cáo sau ngày failure, chia theo độ lệch: dưới 92 ngày 84; 92–184 ngày 69; 184–366 ngày 62; 366–1,095 ngày 143; 1,095–3,650 ngày 54; trên 3,650 ngày 27. Mẫu lớn nhất: CERT 23473 (failure 16/10/1987, báo cáo cuối 2026-06-30, lệch −14,137 ngày); CERT 4178 (−13,075); CERT 11259 (−12,690); CERT 10957 (−12,208); CERT 9806 (−10,983); CERT 7213 Citibank N.A. (failure 23/11/2008, −6,428); CERT 3510 Bank of America N.A. (16/01/2009, −6,374); CERT 25178 (16/01/2009, −6,374).
- Sáu CERT báo cáo đến quý cuối nhưng nằm trong registry: khớp với nhóm `reporting_at_end_listed_failed = 6` [SUY LUẬN: gồm 23473, 4178, 11259, 7213, 3510, 25178].

## 3.6 Phân loại ngân hàng rời panel (từ 03)
| Nhóm (tên trong code) | Số ngân hàng | Ý nghĩa đo được |
|---|---|---|
| `active_reporting_at_panel_end` | 4,307 | Báo cáo ở quý cuối, không có trong registry trong phạm vi |
| `exited_confirmed_failure` | 3,210 | Ngừng báo cáo trước quý cuối **và** có `CERT` trong registry (cả `FAILURE` lẫn `ASSISTANCE`; tên nhóm sai nghĩa) |
| `exited_unlisted_other_exit` | 16,541 | Ngừng báo cáo trước quý cuối, không có trong registry. Nguyên nhân (sáp nhập, đóng tự nguyện...) **chưa được kiểm chứng** |
| `reporting_at_end_listed_failed` | 6 | Báo cáo ở quý cuối nhưng có trong registry |

Tổng: 24,064. Bảng thoát theo năm của báo cáo cuối (`dq_exits_by_year.csv`): xem file; bảng đã xem bị cắt ở các dòng 2000–2007 và sau 2014. Năm báo cáo cuối ≠ năm failure (ví dụ 2008: 44 ngân hàng nhóm registry nhưng chỉ 30 sự kiện năm 2008).

## 3.7 Institutions (từ 03)
- 27,835 dòng; 0 `CERT` trùng; 0 `CERT` null; 235 ngân hàng trong panel không có trong bảng institutions.
- Giá trị thô thực tế của `ACTIVE`, `CHANGEC1`, `ENDEFYMD`: **chưa có** (nằm ở mục 7 của `check_failures_detail.py`, chưa chạy).

## 3.8 Sổ vấn đề của 03 (10 mục, câu chữ cuối cùng cần xem lại)
P01 lệch kiểu cột giữa các quý (14/33 cột); P02 thiếu dữ liệu cấu trúc (chỉ `P3ASSET`, từ 2001); P03 biến thu nhập lũy kế YTD (NETINC, INTINC, EINTEXP, NONII, NONIX, ELNATR); P04 khoảng trống báo cáo (32 ngân hàng, 92 ngân hàng-quý); P05 vi phạm luật kế toán (R04, R05, R07, R08, R09, R11, R12, R15, R16, R18); P06 failure không liên kết được (0 CERT null, 22 CERT ngoài panel); P07 CERT lặp (77 dòng, cần tính lại trong phạm vi); P08 báo cáo sau ngày failure (439); P09 ngân hàng thoát không có trong registry (16,541); P10 trường của bảng Institutions là trạng thái cuối, không đưa vào đặc trưng.

---

# MỤC 4: NHỮNG PHÁT HIỆN NGHIỆP VỤ & BẪY KỸ THUẬT QUAN TRỌNG NHẤT

1. **`RESTYPE`**: Registry gồm `FAILURE` và `ASSISTANCE`. 03 hiện coi cả hai là failure. `ASSISTANCE` tập trung ở 1984–1988 (389/412), cộng 13 bản ghi năm 2008–2009. Chọn có tính `ASSISTANCE` hay không ảnh hưởng lớn đến tập dev và gần như không ảnh hưởng tập test (2009–2014: 482 `FAILURE` so với 490 tính cả `ASSISTANCE`). Ý nghĩa chính xác của `RESTYPE` **chưa được xác minh** từ tài liệu Failures.
2. **Báo cáo sau ngày failure (439)**: là nguy cơ rò rỉ nhãn nếu xử lý sai. Giả thuyết [CHƯA KIỂM CHỨNG]: (a) ngân hàng nhận `ASSISTANCE` vẫn hoạt động (Citibank, Bank of America, 2008–2009); (b) CERT bị dùng lại hoặc registry sai CERT (các CERT thất bại 1987–1991 vẫn báo cáo đến 2026). `check_failures_detail.py` mục 5 (tách theo `RESTYPE`) và mục 6 (so ngày thành lập `ESTYMD` với `FAILDATE`) sẽ cho bằng chứng.
3. **`COST = 0`**: 55 dòng `FAILURE` và 8 dòng `ASSISTANCE` có `COST = 0`; 23 và 19 dòng thiếu. 31 CERT có chuỗi `ASSISTANCE > FAILURE`. Nguyên nhân chưa giải thích đầy đủ. Ảnh hưởng trực tiếp đến ước tính chi phí bỏ lỡ một failure ở Part 5. `COST` có thể không phải đơn vị nghìn USD; tổng 286.5 tỷ nếu đúng đơn vị là lớn, cần kiểm tra từ điển.
4. **Biến thu nhập là lũy kế từ đầu năm (YTD)**: có bằng chứng thực nghiệm (Mục 3.4) và bằng chứng từ từ điển (Mục 5). Phải tách thành giá trị theo quý trước khi tính tỷ lệ.
5. **CERT null trong Failures chỉ xảy ra trước 1984** (488 dòng, 0 dòng trong phạm vi). Vấn đề CERT null không ảnh hưởng nhãn trong giai đoạn nghiên cứu.
6. **Ngân hàng thoát không phải do failure**: 16,541 ngân hàng (83.7% số ngân hàng rời panel) không có trong registry. Cách xử lý nhãn và quan sát của nhóm này **chưa chốt** (xem Mục 7). Đề định nghĩa sáp nhập không phải failure.
7. **Panel không cân bằng**: số ngân hàng giảm từ tối đa 18,133 xuống 4,313 ở quý cuối. Có 32 ngân hàng có khoảng trống nội bộ, nên phép trễ và thay đổi phải tính theo quý lịch, không theo vị trí hàng.
8. **Dữ liệu có thể bị điều chỉnh sau (restated)**: API trả bản mới nhất. Là hạn chế cho báo cáo; không phát hiện được từ dữ liệu này.
9. **Bảng Institutions và các cờ `FAILED`, `CLOSED`, `ACTEVT`, `CONSERVE`** là trạng thái cuối hoặc có ngữ nghĩa theo thời gian chưa biết. **Không** đưa vào đặc trưng (nguy cơ vi phạm Leakage gate).
10. **Các khẳng định không được dùng trong báo cáo** (đã sai hoặc chưa có bằng chứng):
    - "Warm-up loại trừ cache hệ điều hành" (sai: phép đo là cache nóng).
    - "Không tốn RAM" (02 không đo RAM).
    - "Partition pruning nhanh gấp 70 lần" (sai nguyên nhân: lợi ích chủ yếu đến từ việc dùng Parquet, không phải phân vùng).
    - "Mất dữ liệu bằng 0 tuyệt đối" cho mọi layout (chỉ đúng với các layout typed và CSV, `exact_equal = True`; layout naive là `None`).
    - "Spark chỉ cần khi hàng tỷ dòng" (phải lập luận bằng số đo Part 3).
    - "Khủng hoảng S&L và GFC" để gán cho đỉnh thoát: bảng thoát đếm mọi lần ngừng báo cáo.
    - Mọi câu về Citibank, Bank of America, TARP, CERT bị dùng lại, "ngân hàng tiếp quản" trước khi có bằng chứng.
    - "Dữ liệu cho thấy RBC1AAJ, RBCRWAJ thiếu trước thập niên 1990" (sai: đủ từ 1984).

---

# MỤC 5: TỪ ĐIỂN FIELD (FDIC SCHEMA CITATIONS)

## 5.1 Nguồn đã đọc
- Tài liệu chính: `https://api.fdic.gov/banks/docs/` (trang liệt kê các file định nghĩa: Institution, Summary, Failure, SOD, Financial, Location, History, Demographics).
- **`https://api.fdic.gov/banks/docs/risview_properties.yaml`**, ngày truy cập 13/11/2026. Đọc được phần đầu (bị cắt khoảng trường `NTRTMLGJR`). **Không** thấy trong phần đã đọc: `SC`, `ORE`, `NIMY`, `RBC1AAJ`, `RBCRWAJ`, `NONIIQ`.
- Lỗi cần lưu ý: URL `https://api.fdic.gov/banks/risview_properties.yaml` (thiếu `/docs/`) không mở được.

## 5.2 Những điều file `risview_properties.yaml` cho phép khẳng định (diễn giải lại, không trích nguyên văn)
- `ROA`: lợi nhuận sau thuế và khoản bất thường, **đã quy năm**, chia tài sản bình quân. Có `ROAQ` theo quý (không nằm trong danh sách field của dự án).
- `ROE`: lợi nhuận quy năm trên vốn chủ sở hữu bình quân hợp nhất; ghi chú hiển thị NA nếu lợi nhuận giữ lại âm. Có `ROEQ`.
- `ASSET`: tổng tài sản, không gồm tài khoản ngoại bảng. `DEP`: tổng tiền gửi, gồm cả văn phòng nước ngoài. `DEPDOM`: tiền gửi tại văn phòng trong nước. Luật `DEPDOM ≤ DEP` có cơ sở.
- `EQ`: tổng vốn chủ sở hữu. `NETINC`: có mô tả công thức (lãi ròng + thu nhập ngoài lãi + lãi/lỗ chứng khoán và khoản bất thường, trừ chi phí ngoài lãi, dự phòng và thuế).
- `CERT`: mô tả là số duy nhất do FDIC cấp.
- Nhiều trường chỉ có tiêu đề, không có mô tả (`BRO`, `CHBAL`, `LNLSGR`, `LNLSNET`, `LNRE`, `LNCI`, `LNCON`, `NCLNLS`, `NTLNLS`, `INTINC`, `EINTEXP`, `NONII`, `NONIX`, `ELNATR`, `LNATRES`, `ACTIVE`). Giả định "không âm" của 03 cho các cột này **không có trong từ điển**.
- **Bằng chứng YTD từ từ điển**: các tỷ lệ của 7 biến dòng tiền (`NETINCR`, `INTINCR`, `EINTEXPR`, `NONIIR`, `NONIXR`, `ELNATRR`, `NTLNLSCOR`) được định nghĩa với loại công thức `percentageYTD`; có các trường theo quý riêng `NETINCQ`, `INTINQ`, `EINTXQ`, `NONIXQ`, `ELNATQ`, `NTLNLSQ` và các tỷ lệ `percentageQuarterly` đi kèm. Trong phần đã đọc **không thấy** trường theo quý cho `NONII`. Chưa kiểm tra các trường `*Q` có đủ dữ liệu từ thập niên 1980 hay không.
- Có các cờ `FAILED` (mô tả: thất bại gồm sáp nhập có hỗ trợ và chi trả), `CLOSED`, `ACTEVT` (mã sự kiện sáp nhập hoặc đóng cửa), `CONSERVE`. Chưa biết các cờ này thay đổi theo thời gian hay là trạng thái hiện tại.

## 5.3 Chưa có định nghĩa chính thức (cần lấy từ file YAML của Failure và Institution)
`COST` (đơn vị, cơ sở giá), `RESTYPE`, `FAILDATE` (định dạng `M/D/YYYY` là quan sát từ dữ liệu), `ACTIVE`, `CHANGEC1`, `ENDEFYMD`, `ESTYMD`. Cách lấy: mở `https://api.fdic.gov/banks/docs/`, nhấp "Failure API Definitions" và "Institution API Definitions", sao chép nguyên văn, ghi URL và ngày truy cập.

## 5.4 Bảng từ điển KHÔNG hợp lệ
Bảng định nghĩa mà sinh viên dán lượt trước (có `example:` và `format: double`, đường dẫn `failures_properties.yaml`, `institutions_properties.yaml`) **không** trích từ file YAML thật: khác cấu trúc khóa và khác lời mô tả. Không dùng làm nguồn cho báo cáo hay README. Các tuyên bố riêng lẻ trong bảng đó (ví dụ `ENDEFYMD` ở dạng `YYYYMMDD`) đều chưa được xác minh.

---

# MỤC 6: MASTER CHECKLIST THEO DÕI TIẾN ĐỘ

| ID | Workstream | Task | File/Section | Dependency | Status | Evidence/Output |
|---|---|---|---|---|---|---|
| T1 | Tech | Tải và kiểm tra dữ liệu | `src/01_download.py` | n/a | COMPLETED | `--verify-only`: ALL CHECKS PASSED; 170/170; 1,682,616 dòng |
| T2 | Tech | Benchmark lưu trữ | `src/02_storage_benchmark.py` | T1 | COMPLETED | `ALL LAYOUTS VERIFIED`; `report/storage_benchmark_results.csv` |
| T3 | Tech | Báo cáo chất lượng dữ liệu | `src/03_quality_report.py` | T1, T2 | NEEDS REVIEW | Chạy xong 14 giây; còn 6 bản vá (Mục 2.4) |
| T4 | Tech | Panel sạch, nhãn, point-in-time | `src/04_build_panel.py` | T3, quyết định Mục 7 | BLOCKED | Chờ D8 mục 5–7, từ điển Failure/Institution, quyết định |
| T5 | Tech | Polars và benchmark với pandas | `src/05_polars_pandas_benchmark.py` | T4 | NOT STARTED | n/a |
| T6 | Tech | PySpark, plan, đối chiếu kết quả | `src/06_spark_pipeline.py` | T4, T5, Java hoặc Colab | NOT STARTED | Java chưa cài |
| T7 | Tech | Feature pipeline, kiểm tra leakage | `src/07_features.py` | T4 | NOT STARTED | n/a |
| T8 | Tech | Mô hình, validation, ngưỡng, MLlib demo | `src/08_model.py` | T7, T6 | NOT STARTED | n/a |
| D1 | Docs | README: lệnh chạy 01–03, thứ tự phụ thuộc, nguồn, công cụ AI | `README.md` | n/a | IN PROGRESS | Mới có khung |
| D2 | Docs | Khóa `requirements.txt` | `requirements.txt` | T6 | IN PROGRESS | Hiện chưa có `pyspark`; chạy `pip freeze` sau mỗi lần thêm thư viện |
| D3 | Docs | Outline báo cáo theo rubric | `report/` | n/a | NOT STARTED | Outline Part 2 đã đề xuất (Mục 8) |
| D4 | Docs | Bằng chứng Part 1 | `report/evidence_part1.md` | T1, T2 | NEEDS REVIEW | Sinh viên báo đã tạo; chưa thấy nội dung |
| D5 | Docs | Danh sách nguồn và công cụ AI | README và báo cáo | n/a | IN PROGRESS | Phải ghi cả công cụ AI thứ hai |
| D6 | Docs | Từ điển field (định nghĩa, đơn vị, YTD) | báo cáo, phụ lục | T1 | IN PROGRESS | `risview` đã xác nhận một phần; Failure/Institution chưa |
| D7 | Docs | Bằng chứng Part 2 | `report/evidence_part2.md` | T3 | NOT STARTED | Chỉ ghi số có trong file kết quả |
| D8 | Tech | Script kiểm tra Failures chi tiết | `check_failures_detail.py` | T3 | IN PROGRESS | Mục 1–4 có kết quả; mục 5–7 chờ chạy lại |
| R1 | Repro | Push GitHub, không có dữ liệu | Git | T1 | NEEDS REVIEW | Chưa xác nhận |
| R2 | Repro | Kiểm tra repo public (cửa sổ ẩn danh) | GitHub | R1 | NOT STARTED | n/a |
| R3 | Repro | Chạy lại toàn bộ từ môi trường sạch | README | T1–T8 | NOT STARTED | n/a |
| R4 | Repro | Seed, phiên bản thư viện, phần cứng | `config.py` | n/a | IN PROGRESS | `RANDOM_SEED = 42` đã có |

---

# MỤC 7: CÁC QUYẾT ĐỊNH THIẾT KẾ CHƯA CHỐT CHO `04_build_panel.py`

Mọi dòng "đề xuất" là đề xuất của trợ lý, **chưa được sinh viên chấp thuận**.

| # | Quyết định | Dữ kiện hiện có | Đề xuất hiện tại | Còn thiếu để chốt |
|---|---|---|---|---|
| 1 | Nhãn: có tính `ASSISTANCE` không? | 2,861 `FAILURE` và 412 `ASSISTANCE` trong phạm vi; `ASSISTANCE` chủ yếu 1984–1988 | `y = 1` chỉ cho `RESTYPE = FAILURE`, vì đề viết "bank fails"; chạy phương án thứ hai làm kiểm tra độ nhạy nếu còn thời gian | Định nghĩa `RESTYPE` thật; kết quả mục 5 của D8 |
| 2 | Xử lý dòng có `REPDTE` sau hoặc bằng `FAILDATE` | 439 dòng liên kết có báo cáo sau ngày failure | Loại dòng sau ngày failure **đối với `FAILURE`**; đối với `ASSISTANCE` theo quyết định 1 | Kết quả D8 mục 5 (tách `RESTYPE`) và mục 6 (`ESTYMD` so với `FAILDATE`); các CERT bị nghi dùng lại |
| 3 | Xử lý `COST` bằng 0 hoặc thiếu | 55 dòng `FAILURE` bằng 0, 23 thiếu; 31 chuỗi `ASSISTANCE > FAILURE` | Chưa có; hướng: gộp chi phí theo ngân hàng qua các sự kiện | Đơn vị và định nghĩa `COST`; kiểm tra cơ chế `COST = 0` |
| 4 | Chia dev/test | `FAILURE`: 1984–2007 = 2,314; 2009–2014 = 482; 2009–2013 = 464; 2010–2014 = 342 | Ứng viên: dev đến 2007Q4, khoảng cách 2008 (4 quý), test từ 2009Q1; dữ liệu đến 2025Q2 | Cách đếm "≥ 50 failure" (số vụ, số ngân hàng hay số dòng dương); cửa sổ nhãn 4 quý phải không chồng lấp sang tập test |
| 5 | Ngân hàng thoát không phải failure (16,541) | Chưa phân loại được nguyên nhân | Giữ nhãn 0 và giữ dòng; ghi hạn chế; kiểm tra độ nhạy nếu loại 4 quý cuối của riêng nhóm này (đối xử bất đối xứng với nhóm failure) | Giá trị thật của `ACTIVE`, `CHANGEC1`, `ENDEFYMD` (D8 mục 7) |
| 6 | Tách biến lũy kế | 7 biến YTD (bằng chứng thực nghiệm và từ điển) | Tự tách trong cùng năm khi quý liền trước tồn tại; Q1 giữ nguyên; thiếu thì NaN; kiểm tra lại bằng mẫu nhỏ các trường `*Q` qua API (chưa thử; chưa biết đủ từ 1980 chưa); riêng `NONII` không thấy trường theo quý | Quyết định có dùng `*Q` hay không |
| 7 | Layout lưu trữ cho các bước sau | Mục 3.3 | Parquet typed, zstd, một file, sắp xếp theo `(REPDTE, CERT)`, row group 50,000 dòng | Sinh viên xác nhận; tên file đầu ra (`panel_clean.parquet` hoặc `clean_panel.parquet`) |
| 8 | Độ trễ công bố của báo cáo | Trung vị 48 ngày từ báo cáo cuối đến failure | Ghi hạn chế; cân nhắc lùi đặc trưng 1 quý | Thời hạn nộp báo cáo thực tế (chưa kiểm chứng) |
| 9 | 22 failure không có trong panel | Đều trong đầu 1984 (15 dòng đã xem) | Ghi nhận, loại khỏi đếm test | Xem 7 dòng còn lại |

---

# MỤC 8: HÀNH ĐỘNG CẦN LÀM TIẾP THEO NGAY LẬP TỨC

## 8.1 Việc của sinh viên (theo thứ tự)
1. **D1 – chạy lại `check_failures_detail.py`** sau khi sửa lỗi `ID` (thay dòng `d = detail.merge(f[["ID", "RESTYPE", "cert_num", "fail_dt"]], on="ID", how="left")` bằng: tạo `fx = f[["ID","RESTYPE","cert_num","fail_dt"]].copy()`, `fx["ID"] = pd.to_numeric(fx["ID"], errors="coerce")`, rồi `d = detail.merge(fx, on="ID", how="left")`). Gửi **mục 5, 6, 7**:
   - mục 5: các báo cáo sau ngày failure, tách theo `RESTYPE`;
   - mục 6: so `ESTYMD` của bảng Institutions với `FAILDATE` cho các CERT thất bại vẫn báo cáo;
   - mục 7: giá trị thô của `ACTIVE`, `CHANGEC1`, `ENDEFYMD`.
2. **D2 – lấy định nghĩa thật**: từ `https://api.fdic.gov/banks/docs/` mở Failure và Institution definitions; sao chép nguyên văn `RESTYPE`, `COST`, `FAILDATE`, `ACTIVE`, `CHANGEC1`, `ENDEFYMD`, `ESTYMD`. Từ `risview_properties.yaml` (phần sau chưa đọc được) lấy `SC`, `ORE`, `NIMY`, `RBC1AAJ`, `RBCRWAJ`, `BRO`, `NONIIQ`.
3. Xác nhận lại các lệnh đã chạy: `Select-String` cho các cụm cấm trong `src/03_quality_report.py` trả về rỗng (sinh viên đã báo đạt).
4. Commit và push: `config.py`, `src/01_download.py`, `src/02_storage_benchmark.py`, `src/03_quality_report.py`, `report/*.csv`, `report/*.md`, `report/*.json`, `report/figures/*.png`. Chạy `git status --short` trước đó, **không có** `data_raw/` hoặc `data_processed/`. Kiểm tra repo public bằng cửa sổ ẩn danh.
5. README: thêm lệnh chạy `02`, `03`; thứ tự phụ thuộc (03 gọi hàm của 02); mục "Sources and AI tools" ghi cả hai công cụ AI đã dùng.
6. Khung báo cáo Part 2 (chỉ điền số có trong file kết quả): 2.1 phương pháp; 2.2 cấu trúc panel; 2.3 giá trị thiếu; 2.4 vi phạm kế toán và giá trị cực đoan; 2.5 YTD; 2.6 Failures, nhãn, thoát; 2.7 sổ vấn đề.
7. Quyết định Java trước 17/11: cài JDK cho Spark local, hoặc dùng Google Colab / Databricks Free Edition.

## 8.2 Việc của trợ lý (sau khi nhận kết quả 8.1)
1. Vá `03` theo Mục 2.4 và chạy lại.
2. Trình bày các lựa chọn Mục 7 để sinh viên chốt; không viết `04` trước đó.
3. Viết `src/04_build_panel.py` (nhãn theo ngày thật, tách YTD, xử lý thoát, point-in-time, xuất panel sạch dạng Parquet typed zstd), kèm lệnh chạy PowerShell, kết quả mong đợi, và bảng kiểm leakage.
4. Sau 04: `05` (Polars so với pandas, kế hoạch thực thi, đo bộ nhớ đỉnh bằng `psutil`), `06` (Spark, so khớp với Polars, `explain`), `07`, `08`.

## 8.3 Lịch (hôm nay 13/11/2026, hạn 26/11 lúc 23:00)
| Ngày | Công việc |
|---|---|
| 13–15/11 | D8 mục 5–7, từ điển thật, vá 03, chốt quyết định Mục 7 |
| 16–17/11 | `04_build_panel.py`; quyết định Java |
| 18–21/11 | `05`, `06` (Part 3, 25 điểm, nặng nhất) |
| 22/11 | `07` |
| 23/11 | `08` |
| 24/11 | Hoàn thiện báo cáo, chạy lại từ môi trường sạch |
| 25/11 | Dự phòng |
| 26/11 | Chỉ kiểm tra lần cuối; nộp **trước 18:00** (hạn cứng 23:00) |

## 8.4 Rủi ro đang mở
- **Leakage gate**: nhãn và loại dòng sau failure sai (Mục 7, quyết định 1 và 2) có thể kéo Part 4 và 5 xuống 30% điểm.
- **Số failure thực trong tập test**: sau khi loại 22 failure ngoài panel và xử lý `ASSISTANCE`, cần đếm lại theo đúng định nghĩa "≥ 50".
- **Java chưa cài** có thể chặn `06` nếu quyết định muộn.
- **RAM 8 GB**: khi chạy bản đầy đủ của 03 (chuyển 1.68 triệu dòng sang pandas) và các benchmark, đóng trình duyệt và ứng dụng nặng, cắm sạc.
- **Công cụ AI thứ hai** đã tạo nội dung sai (bảng từ điển); mọi nội dung từ công cụ đó cần đối chiếu nguồn trước khi dùng.