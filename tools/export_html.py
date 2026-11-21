"""Converts report/final_report.md to report/final_report.html with academic print-ready CSS
(10pt Times New Roman, 1.8cm margins, compact 8pt tables, page-break before appendices).
Usage: python tools/export_html.py"""
import re
from pathlib import Path

# Cài đặt markdown nếu chưa có
try:
    import markdown
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "markdown"])
    import markdown

md_path = Path("report/final_report.md")
html_path = Path("report/final_report.html")

raw_md = md_path.read_text(encoding="utf-8")

# Thêm ngắt trang trước Phụ lục A
raw_md = raw_md.replace("# Appendix A:", '<div class="page-break"></div>\n\n# Appendix A:')

# Chuyển đổi Markdown sang HTML với extension bảng và code
html_body = markdown.markdown(raw_md, extensions=['tables', 'fenced_code', 'toc'])

css = """
@page {
    size: A4;
    margin: 1.8cm;
    @bottom-center {
        content: counter(page);
    }
}
body {
    font-family: "Times New Roman", Times, serif;
    font-size: 10pt;
    line-height: 1.35;
    color: #111;
    margin: 0;
    padding: 0;
}
h1 {
    font-size: 15pt;
    border-bottom: 1.5px solid #333;
    padding-bottom: 3px;
    margin-top: 18pt;
    margin-bottom: 8pt;
    break-after: avoid;
    page-break-after: avoid;
}
h2 {
    font-size: 12pt;
    margin-top: 14pt;
    margin-bottom: 6pt;
    break-after: avoid;
    page-break-after: avoid;
}
h3 {
    font-size: 10.5pt;
    margin-top: 10pt;
    margin-bottom: 4pt;
    break-after: avoid;
    page-break-after: avoid;
}
h4 {
    font-size: 9.5pt;
    margin-top: 8pt;
    margin-bottom: 3pt;
    break-after: avoid;
    page-break-after: avoid;
}
p, ul, ol {
    margin-top: 3pt;
    margin-bottom: 6pt;
    text-align: justify;
}
p:has(+ table), h3:has(+ table), h4:has(+ table) {
    break-after: avoid;
    page-break-after: avoid;
}
table {
    border-collapse: collapse;
    width: 100%;
    margin-top: 6pt;
    margin-bottom: 10pt;
    font-size: 7.8pt;
    break-inside: avoid;
    page-break-inside: avoid;
}
th, td {
    border: 1px solid #777;
    padding: 3.5px 5px;
    text-align: left;
}
th {
    background-color: #f2f2f2;
    font-weight: bold;
}
code {
    font-family: Consolas, monospace;
    font-size: 8.5pt;
    background-color: #f5f5f5;
    padding: 1px 3px;
}
pre {
    background-color: #f8f8f8;
    border: 1px solid #ddd;
    padding: 6px;
    font-size: 8pt;
    overflow-x: auto;
    page-break-inside: avoid;
}
.page-break {
    page-break-before: always;
}
"""

full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Final Examination Research Report - BFT65B</title>
<style>
{css}
</style>
</head>
<body>
{html_body}
</body>
</html>
"""

html_path.write_text(full_html, encoding="utf-8")
print(f"Exported successfully to: {html_path.resolve()}")