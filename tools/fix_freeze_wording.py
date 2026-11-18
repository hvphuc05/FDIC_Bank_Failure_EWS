"""Corrects the wording about WHEN the selection rules were fixed, so that it claims only what the git history proves:
commit 508e235 precedes the test evaluation (it post-dates the development cost results of section 6.6).
Usage: python tools/fix_freeze_wording.py report/final_report.md README.md"""
import sys
from pathlib import Path

REPORT = [
 ("Two rules were written into the README on **2026-11-14, before the corresponding results were computed**; the commit that contains them is **508e235**.",
  "Two rules were written into the README on **2026-11-14** and committed in **508e235**, before the test period was evaluated. The commit was made after the development results of §6.6 had been computed, so it proves the order only with respect to the test period."),
 ("| Design frozen before the cost and policy results | commit 508e235 (README, §6.7);",
  "| Design frozen before any test row was read | commit 508e235 (README Rules 1 and 2, §6.7);"),
]
README = [
 ("Fixed on **2026-11-14**, before the cost and policy results were computed (commit: **508e235**):",
  "Written on **2026-11-14** and committed in **508e235**, before the test period was evaluated (the commit post-dates the development cost results, so it proves the order only with respect to the test period):"),
]


def apply(path, repl):
    p = Path(path)
    raw = p.read_bytes().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    missing = [a[:60] for a, _ in repl if a not in text]
    if missing:
        print(f"NOT APPLIED to {path}; not found (already corrected?):\n  " + "\n  ".join(missing))
        return
    for a, b in repl:
        text = text.replace(a, b, 1)
    p.write_bytes((text.replace("\n", "\r\n") if crlf else text).encode("utf-8"))
    print("applied to", path)


apply(sys.argv[1], REPORT)
if len(sys.argv) > 2:
    apply(sys.argv[2], README)