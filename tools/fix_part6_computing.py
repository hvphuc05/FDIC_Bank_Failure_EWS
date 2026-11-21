"""Patches point 5 of section 7.5 in final_report.md, part6_section.md and make_part6.py
to clarify that only Spark ran on Colab, while the Polars pipeline ran on an 8 GB laptop.
Usage: python tools/fix_part6_computing.py"""
import sys
from pathlib import Path

OLD = ("5. **Computing.** The whole pipeline ran on an 8 GB laptop, and the processing steps that were measured peaked "
       "below 2 GB (T1 at four times the size: 1.76 GB), so it can run inside the supervisor's own secure environment "
       "without a cluster or a cloud service.")

NEW = ("5. **Computing.** The Polars pipeline ran on an 8 GB laptop, and the processing steps that were measured peaked "
       "below 2 GB (T1 at four times the size: 1.76 GB); Spark demonstrations ran on Colab. The pipeline can run inside "
       "the supervisor's own secure environment without a cluster or a cloud service.")

FILES = [
    Path("report/final_report.md"),
    Path("report/part6_section.md"),
    Path("tools/make_part6.py"),
]

applied = 0
for f in FILES:
    if not f.exists():
        print(f"File not found: {f}")
        continue
    content = f.read_text(encoding="utf-8")
    if OLD in content:
        content = content.replace(OLD, NEW)
        f.write_text(content, encoding="utf-8")
        print(f"Patched: {f}")
        applied += 1
    elif NEW in content:
        print(f"Already patched: {f}")
    else:
        print(f"Target text not found in: {f}")

if applied == 0:
    print("No files were patched (already patched or target text not found).")