"""Converts report/final_report.md to report/final_report.html with academic print-ready CSS
Usage: python tools/export_html.py
"""
import re
from pathlib import Path

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
    margin: 15mm; 
    @bottom-center {
        content: counter(page);
    }
}
body { 
    font-family: "Times New Roman", Times, serif; 
    font-size: 10pt; 
    line-height: 1.22; 
    color: #111; 
    margin: 0; 
    padding: 0; 
}
h1 { 
    font-size: 13.5pt; 
    border-bottom: 1.2px solid #333; 
    padding-bottom: 2px; 
    margin-top: 10pt; 
    margin-bottom: 4pt; 
    break-after: avoid; 
    page-break-after: avoid; 
}
h2 { 
    font-size: 11pt; 
    margin-top: 8pt; 
    margin-bottom: 3pt; 
    break-after: avoid; 
    page-break-after: avoid; 
}
h3 { 
    font-size: 9.8pt; 
    margin-top: 6pt; 
    margin-bottom: 2pt; 
    break-after: avoid; 
    page-break-after: avoid; 
}
h4 { 
    font-size: 9.2pt; 
    margin-top: 5pt; 
    margin-bottom: 2pt; 
    break-after: avoid; 
    page-break-after: avoid; 
}
p, ul, ol { 
    margin-top: 1.5pt; 
    margin-bottom: 2.5pt; 
    text-align: justify; 
}
table { 
    border-collapse: collapse; 
    font-size: 8pt; 
    width: 100%; 
    margin-top: 3pt; 
    margin-bottom: 4pt; 
}
td, th { 
    border: 1px solid #999; 
    padding: 1.2px 3.2px; 
    text-align: left; 
}
th { 
    background-color: #f2f2f2; 
    font-weight: bold; 
}
tr { 
    break-inside: avoid; 
    page-break-inside: avoid; 
}
thead { 
    display: table-header-group; 
}
img { 
    max-width: 100%; 
    max-height: 5.5cm; 
    width: auto; 
    height: auto; 
    display: block; 
    margin: 3pt auto; 
}
img[src*="figure_a3"], img[alt*="Share of null values"], img[alt*="Missingness Heatmap"] { 
    max-width: 100%; 
    max-height: 12.5cm; 
    width: auto; 
    margin: 4pt auto; 
}
img[src*="figure_a1"], img[src*="figure_a2"], img[alt*="Storage Benchmark"], img[alt*="Bank Population"] { 
    max-width: 100%; 
    max-height: 8.5cm; 
    width: auto; 
    margin: 4pt auto; 
}
code { 
    font-family: Consolas, monospace; 
    font-size: 8pt; 
    background-color: #f5f5f5; 
    padding: 1px 2px; 
}
pre { 
    background-color: #f8f8f8; 
    border: 1px solid #ddd; 
    padding: 4px; 
    font-size: 7.5pt; 
    overflow-x: auto; 
    break-inside: avoid; 
}
.page-break { 
    page-break-before: always; 
    break-before: always; 
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