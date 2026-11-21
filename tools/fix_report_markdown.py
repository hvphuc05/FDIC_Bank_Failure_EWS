"""Fixes formatting, unspaced lists, heading 4.7, cover page indentation,
figure captions, and AI disclosure in report/final_report.md.
Idempotent: running multiple times yields the same result."""

from pathlib import Path

report_path = Path("report/final_report.md")
content = report_path.read_text(encoding="utf-8")

# 1. Heading 4.7 glued to previous sentence
content = content.replace(
    "and at this size it brings no advantage over a single node.## 4.7 When is a cluster justified?",
    "and at this size it brings no advantage over a single node.\n\n## 4.7 When is a cluster justified?"
)

# 2. Cover page sub-bullets: 2 spaces -> 4 spaces
cover_old = """* **Verified Row Counts**:
  * Financial Call Reports (`financials`): 1,682,616 rows across 24,064 distinct bank charters
  * Bank Structure & Event Directory (`institutions`): 27,835 records
  * Failure Registry (`failures`): 4,117 records"""

cover_new = """* **Verified Row Counts**:
    * Financial Call Reports (`financials`): 1,682,616 rows across 24,064 distinct bank charters
    * Bank Structure & Event Directory (`institutions`): 27,835 records
    * Failure Registry (`failures`): 4,117 records"""
content = content.replace(cover_old, cover_new)

# 3. 8 lists missing preceding blank line
list_fixes = [
    (
        "three core engineering safeguards:\n1. **Resumable Manifest Auditing**:",
        "three core engineering safeguards:\n\n1. **Resumable Manifest Auditing**:"
    ),
    (
        "was conducted across all on-disk artifacts:\n* **Call Reports (`financials`)**:",
        "was conducted across all on-disk artifacts:\n\n* **Call Reports (`financials`)**:"
    ),
    (
        "dynamically serializes numeric values:\n* Financial balance-sheet amounts",
        "dynamically serializes numeric values:\n\n* Financial balance-sheet amounts"
    ),
    (
        "**Unified Typed Schema** was designed:\n1. `REPDTE` is cast to `date32`",
        "**Unified Typed Schema** was designed:\n\n1. `REPDTE` is cast to `date32`"
    ),
    (
        "recommended production architecture:\n1. **Compression & Storage Footprint**:",
        "recommended production architecture:\n\n1. **Compression & Storage Footprint**:"
    ),
    (
        "**Data and documentation**\n1. Federal Deposit Insurance Corporation.",
        "**Data and documentation**\n\n1. Federal Deposit Insurance Corporation."
    ),
    (
        "**Methods**\n3. Elkan, C. (2001).",
        "**Methods**\n\n3. Elkan, C. (2001)."
    ),
    (
        "**Software**\n7. scikit-learn",
        "**Software**\n\n7. scikit-learn"
    )
]

for old, new in list_fixes:
    content = content.replace(old, new)

# 4. Figure captions
content = content.replace(
    "*Figure 1: Comparison of Disk Footprint and Query Latency across Storage Layouts (Log Scale).*",
    "*Figure 1: Comparison of disk footprint across storage layouts (MB).*"
)

content = content.replace(
    "*Figure 2: Active bank population and exits, 1984Q1 – 2026Q2.*",
    "*Figure 2: Active commercial banks per quarter, 1984Q1 – 2026Q2.*"
)

content = content.replace(
    "*Figure 4: Saw-tooth of cumulative flows versus the stock control.*",
    "*Figure 4: Saw-tooth pattern of cumulative year-to-date flows (quarterly average INTINC and NETINC).*"
)

# 6. AI disclosure
content = content.replace(
    "Every figure in this report comes from the output of scripts that the author executed on a local laptop.",
    "Every figure in this report comes from the output of scripts that the author executed on a local laptop (with Spark demonstrations and MLlib runs executed on Google Colab)."
)

report_path.write_text(content, encoding="utf-8")
print("Successfully patched report/final_report.md")
