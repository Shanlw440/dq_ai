from pathlib import Path
import html, json

THEME_CSS = """
:root{ --bg1:#EAF2FF; --bg2:#DDE8FF; --text:#212529; --muted:#6B7280;
  --primary:#6EA8FE; --primary-strong:#3D8BFD; --card:rgba(255,255,255,0.96); --border:#E7ECF5;
  --badge-critical:#FEE2E2; --badge-critical-text:#991B1B;
  --badge-major:#FFE8CC; --badge-major-text:#9A3412;
  --badge-minor:#E6FFFB; --badge-minor-text:#0E7490;
}
html,body{height:100%}
body{margin:0;font-family:'Poppins',system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:var(--text);
     background:linear-gradient(145deg,var(--bg1),var(--bg2));background-attachment:fixed;}
.container{max-width:1040px;margin:0 auto;padding:18px;}
.hero{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px 20px;text-align:center;}
h1{margin:0 0 6px}
.subtitle{color:var(--muted);margin:4px 0 0}
.cards{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:14px;margin:14px 0}
.card{border:1px solid var(--border);border-radius:12px;background:var(--card);padding:14px}
.card-title{font-weight:600;margin-bottom:8px}
.big{font-size:36px;font-weight:600}
.badge{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;margin-left:6px}
.badge.critical{background:var(--badge-critical);color:var(--badge-critical-text)}
.badge.major{background:var(--badge-major);color:var(--badge-major-text)}
.badge.minor{background:var(--badge-minor);color:var(--badge-minor-text)}
.section{margin-top:10px}
.finding{border:1px solid var(--border);border-radius:12px;background:var(--card);padding:14px;margin-top:12px}
.table{width:100%;border-collapse:collapse;font-size:14px}
.table th{background:#f6f9ff;text-align:left;padding:8px;border-bottom:1px solid var(--border)}
.table td{padding:8px;border-bottom:1px solid var(--border)}
.footer{color:var(--muted);font-size:12px;margin:18px 0;text-align:center;line-height:1.5}
"""

def _badge(sev: str) -> str:
    sev = (sev or "").lower()
    return f'<span class="badge {sev}">{sev.title()}</span>'

def _table(headers, rows):
    head = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table class="table"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'

def render_report(ctx: dict, output_path: str) -> None:
    file = html.escape(str(ctx.get("file","")))
    rows = int(ctx.get("rows",0))
    cols = ctx.get("columns",[])
    score = ctx.get("score",0)
    credit = ctx.get("credit_html","")
    privacy = ctx.get("privacy_html",
        "Privacy: files you upload are processed in memory and not persisted. "
        "No data is sent to third-party APIs. Temporary artifacts are deleted automatically. "
        "Avoid uploading highly sensitive/regulated data.")

    from collections import Counter
    sev_counts = Counter([(r.get("severity") or "").lower() for r in ctx.get("results",[])])
    crit = sev_counts.get("critical",0); maj = sev_counts.get("major",0); minr = sev_counts.get("minor",0)

    summary_html = f"""
    <div class="cards">
      <div class="card"><div class="card-title">Overall score</div><div class="big">{score}</div></div>
      <div class="card"><div class="card-title">Issues</div>
        <div>Critical: {crit}</div><div>Major: {maj}</div><div>Minor: {minr}</div>
      </div>
      <div class="card"><div class="card-title">Dataset</div>
        <div>Rows: {rows}</div><div>Columns: {len(cols)}</div>
      </div>
    </div>
    """

    finding_blocks = []
    for r in ctx.get("results", []):
        name = html.escape(r.get("name",""))
        sev = (r.get("severity") or "").lower()
        issues = r.get("issues", [])

        # default: simple JSON list
        issues_html = "<ul>" + "".join(
            f"<li><code>{html.escape(json.dumps(i, ensure_ascii=False))}</code></li>" for i in issues
        ) + "</ul>"

        # Missing values per-column
        if name.startswith("Missing Values") and issues and all(("column" in i and "missing" in i) for i in issues):
            rows_html = [[i["column"], int(i["missing"])] for i in issues]
            issues_html = _table(["column", "missing"], rows_html)

        # Dtype drift
        if name == "Dtype Drift" and issues and all(set(["column","expected","actual"]).issubset(i) for i in issues):
            rows_html = [[i["column"], i["expected"], i["actual"]] for i in issues]
            issues_html = _table(["column","expected","actual"], rows_html)

        # Duplicate rows: show count + indices + sample table if provided
        if name == "Duplicate Rows" and issues:
            info = issues[0]
            dup_count = int(info.get("duplicates", 0))
            row_indices = info.get("row_indices", [])
            sample_rows = info.get("sample_rows", [])
            columns = info.get("columns", [])
            idx_text = ", ".join([html.escape(str(x)) for x in row_indices[:50]]) + (" …" if len(row_indices) > 50 else "")
            tbl = ""
            if sample_rows and columns:
                rows_html = [[row.get(c, "") for c in columns] for row in sample_rows]
                tbl = _table(columns, rows_html)
            issues_html = f"<p>Duplicate rows detected: <strong>{dup_count}</strong></p>"
            if row_indices:
                issues_html += f"<p>Row indices (sample): {idx_text}</p>"
            issues_html += tbl

        # IQR outliers: add explanation + sample of index/value pairs
        if name.startswith("IQR Outliers") and issues:
            info = issues[0]
            explain = info.get("explain")
            sample = info.get("sample_values", [])
            pairs = [[p.get("index",""), p.get("value","")] for p in sample]
            tbl = _table(["index","value"], pairs) if pairs else ""
            add = f"<p>{html.escape(explain)}</p>" if explain else ""
            if "row_indices" in info:
                add += f"<p>Outlier rows (count): <strong>{len(info['row_indices'])}</strong></p>"
            issues_html = add + tbl

        finding_blocks.append(f"""
        <div class="finding">
          <div class="card-title">{name} {_badge(sev)}</div>
          {issues_html}
        </div>
        """)

    html_doc = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>DQ-AI — Data Quality Report</title>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap" rel="stylesheet">
<style>{THEME_CSS}</style>
</head>
<body>
  <div class="container">
    <header class="hero">
      <h1>DQ-AI — Data Quality Auditor</h1>
      <p class="subtitle">File: {file} · Rows: {rows} · Columns: {len(cols)}</p>
    </header>

    {summary_html}

    <section class="section">
      <div class="card"><div class="card-title">Findings</div></div>
      {''.join(finding_blocks)}
    </section>

    <div class="footer">{credit}<br/>{privacy}</div>
  </div>
</body>
</html>"""
    Path(output_path).write_text(html_doc, encoding="utf-8")
