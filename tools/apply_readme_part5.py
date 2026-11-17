"""Updates README.md for Part 5 (final model, single test evaluation, MLlib). All-or-nothing.
Usage: python tools/apply_readme_part5.py README.md"""
import sys
from pathlib import Path

REPL = [
 ("| 5 | Validation, models, cost-based threshold | design and development-period results done; **single test evaluation and Spark MLlib demo pending** |",
  "| 5 | Validation, models, cost-based threshold | done: validation, cost-based policy, single test evaluation (2026-11-17, commit 9e33c74, `report/test_evaluation_log.json`) and Spark MLlib demonstration |"),
 ("The test period (2008Q1 – 2025Q2) has **not** been evaluated yet.",
  "The test period (2008Q1 – 2025Q2) was evaluated **once**, with the frozen design; results are in `report/final_report.md` §6.10."),
 ("│   ├── 08d_policy_comparison.py    # threshold policies compared across folds\n",
  "│   ├── 08d_policy_comparison.py    # threshold policies compared across folds\n"
  "│   ├── evalcore.py                 # ranking metrics, cost saving, bank-cluster bootstrap (numpy / scikit-learn only)\n"
  "│   ├── 09a_fit_final.py            # final models fitted on the development period only; freeze log\n"
  "│   ├── 09b_test_evaluation.py      # --preflight, --dry-run, and the single test evaluation\n"
  "│   ├── 09c_mllib_demo.py           # Spark MLlib logistic regression (Colab), development data only\n"
  "│   ├── 09d_compare_mllib.py        # --prep (MLlib input) and --compare (MLlib against scikit-learn)\n"),
 ("│   └── oof_dev.parquet             # out-of-fold predictions on the development period\n",
  "│   ├── oof_dev.parquet             # out-of-fold predictions on the development period\n"
  "│   ├── final_models.joblib         # frozen final models (SHA-256 in report/final_model_freeze.json)\n"
  "│   └── test_predictions.parquet    # test-period scores of the three models (written by 09b)\n"),
 ("    ├── cv_calibration.csv, cv_cost_thresholds.csv, cv_policy_comparison.csv\n",
  "    ├── cv_calibration.csv, cv_cost_thresholds.csv, cv_policy_comparison.csv\n"
  "    ├── test_*.csv, test_evaluation_log.json        # the single test evaluation (09b)\n"
  "    ├── final_model_freeze.json, final_model_coefficients.csv, mllib_vs_sklearn_*   # frozen model; MLlib comparison\n"),
 ("| 5 | `python src/08d_policy_comparison.py` | policy comparison | n/r |\n",
  "| 5 | `python src/08d_policy_comparison.py` | policy comparison | n/r |\n"
  "| 5 | `python src/09b_test_evaluation.py --dry-run` | reproduces the fold-3 numbers of 08b/08d by assertion; reads no test row | n/r |\n"
  "| 5 | `python src/09a_fit_final.py` | final models on the development period; `report/final_model_freeze.json` | n/r |\n"
  "| 5 | `python src/09b_test_evaluation.py --preflight`, then `python src/09b_test_evaluation.py --n-boot 1000` | the single test evaluation (the script refuses a second run without `--allow-rerun`) | ≈ 9 min |\n"),
 ("## 5. Diagnostic scripts",
  "### Part 5 (final model, single test evaluation, MLlib)\n\n"
  "```powershell\n"
  "python src/09b_test_evaluation.py --dry-run --n-boot 20     # code path check on development fold 3\n"
  "python src/09a_fit_final.py                                 # development period only\n"
  "python src/09b_test_evaluation.py --preflight               # no test row is read\n"
  "python src/09b_test_evaluation.py --n-boot 1000             # THE test evaluation; writes report/test_*.csv and the log\n"
  "python src/09d_compare_mllib.py --prep                      # MLlib input (development rows only)\n"
  "```\n\n"
  "On Colab (Spark 4.0.5, 2 vCPUs): `python src/09c_mllib_demo.py --features data_processed/bench/mllib_dev_features.parquet --out-dir mllib_out --driver-memory 8g --rel-err 0.001`; "
  "download `mllib_out/` and run `python src/09d_compare_mllib.py --compare mllib_out`.\n\n"
  "## 5. Diagnostic scripts"),
 ("`NTLNLS` keeps a residual Q4 effect after de-cumulation;",
  "on the test period the predicted probabilities are lower than the observed frequencies, so the alarm threshold 1/(1+r) is conservative (report §6.10); `NTLNLS` keeps a residual Q4 effect after de-cumulation;"),
 ("The design is frozen; the test period is evaluated once.",
  "The design was frozen and the test period was evaluated once on 2026-11-17 (commit 9e33c74; `report/test_evaluation_log.json`)."),
]

p = Path(sys.argv[1])
raw = p.read_bytes().decode("utf-8")
crlf = "\r\n" in raw
text = raw.replace("\r\n", "\n")
missing = [a[:70] for a, _ in REPL if a not in text]
if missing:
    sys.exit("NOT APPLIED. Not found:\n  " + "\n  ".join(missing))
for a, b in REPL:
    text = text.replace(a, b, 1)
p.write_bytes((text.replace("\n", "\r\n") if crlf else text).encode("utf-8"))
print(f"applied {len(REPL)} replacements to {p}")