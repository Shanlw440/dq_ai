import base64
import io
import json
import tempfile
from collections import Counter
from pathlib import Path

import pandas as pd
from dash import Dash, html, dcc, dash_table, Input, Output, State

# Your audit engine + exporter
import run_audit as ra
from report.html_report import render_report


# -----------------------
# Helpers
# -----------------------
def parse_contents(contents, filename):
    """Decode uploaded file into a pandas DataFrame."""
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    if filename.lower().endswith(".csv"):
        return pd.read_csv(io.StringIO(decoded.decode("utf-8", errors="ignore")))
    elif filename.lower().endswith((".xls", ".xlsx")):
        return pd.read_excel(io.BytesIO(decoded))
    else:
        raise ValueError("Unsupported file format. Use CSV or Excel.")


FRIENDLY = {
    "Missing Values": ("Some columns contain nulls.", "Backfill from source, impute, or enforce NOT NULL."),
    "Duplicate Rows": ("Exact duplicate rows detected.", "De-duplicate by primary key or drop_duplicates in ETL."),
    "IQR Outliers": ("Unusual numeric values outside the typical range.", "Investigate; cap or winsorize if appropriate."),
    "Temporal Rule: ship_date ≥ order_date": ("Ship date precedes order date.", "Fix dates at source or timezone/format issues."),
    "Missing Columns": ("Dataset is missing expected fields (baseline).", "Fix mapping or update baseline if intentional."),
    "Unexpected Columns": ("New fields appeared vs baseline.", "Validate & update baseline if legitimate."),
    "Dtype Drift": ("Column data types changed vs baseline.", "Cast types in ingestion or adjust schema rules."),
    "Semantic Violations (email)": ("Values don’t look like valid emails.", "Validate at capture; standardize/clean."),
    "Semantic Violations (UK postcode)": ("Values don’t look like valid UK postcodes.", "Normalize formats; validate upstream."),
    "Primary Key Uniqueness": ("Primary key is not unique.", "Fix duplicates at source; enforce uniqueness."),
}


def badge(sev: str):
    return html.Span(sev.title(), className=f"badge {sev}")


def card(title, *children):
    return html.Div(className="card", children=[html.Div(className="card-title", children=title), *children])


# -----------------------
# App (theme + meta)
# -----------------------
app = Dash(__name__, assets_folder="assets", assets_url_path="/assets", serve_locally=True)
app.title = "DQ-AI Auditor"

# ✅ Allow embedding the app inside your site (iframe)
ALLOWED_EMBED = (
    "https://shannonwiseanalytics.com "
    "https://www.shannonwiseanalytics.com "
    "https://dq-ai.onrender.com"
)

@app.server.after_request
def _allow_iframe(resp):
    # remove default frame blocker and explicitly allow your domains
    resp.headers.pop("X-Frame-Options", None)
    resp.headers["Content-Security-Policy"] = f"frame-ancestors 'self' {ALLOWED_EMBED};"
    return resp

# cap upload size to 25 MB (adjust if you want)
app.server.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

# SEO/OpenGraph for LinkedIn/Twitter sharing
app.index_string = """
<!DOCTYPE html>
<html>
  <head>
    {%metas%}
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
    <meta name="description" content="Instant, executive-ready data quality report for your CSV/XLSX.">
    <meta property="og:title" content="DQ-AI — Data Quality Auditor">
    <meta property="og:description" content="Instant, executive-ready data quality report for your CSV/XLSX.">
    <meta property="og:image" content="/assets/og.png">
    <meta property="og:type" content="website">
    <meta name="twitter:card" content="summary_large_image">
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap" rel="stylesheet">
    <script defer src="https://cdnjs.cloudflare.com/ajax/libs/lottie-web/5.12.2/lottie.min.js"></script>
  </head>
  <body>
    {%app_entry%}
    <footer>
      {%config%}
      {%scripts%}
      {%renderer%}
    </footer>
  </body>
</html>
"""

# -----------------------
# Layout
# -----------------------
app.layout = html.Div(className="container", children=[
    # Hero with centered title and animation
    html.Div(className="hero", children=[
        html.Div(className="title-wrap", children=[
            html.H1("DQ-AI — Data Quality Auditor"),
            html.P("Executive-ready data quality reports. Upload a CSV/XLSX and (optionally) a baseline JSON.")
        ]),
        html.Div(id="lottie-container", className="lottie")
    ]),

    # Uploads row (2 columns) + actions row
    html.Div(className="controls", children=[
        html.Div(children=[
            html.Label("Dataset File"),
            dcc.Upload(id="upload-data",
                       children=html.Div(["Drag & Drop or ", html.A("Select File")]),
                       className="upload-box", multiple=False),
            html.Div(id="file-name", className="file-note muted")
        ]),
        html.Div(children=[
            html.Label("Baseline (optional) — baseline_schema.json"),
            dcc.Upload(id="upload-baseline",
                       children=html.Div(["Drag & Drop or ", html.A("Select Baseline JSON")]),
                       className="upload-box", multiple=False),
            html.Div(id="baseline-name", className="file-note muted")
        ]),
        html.Div(className="actions", children=[
            html.Button("Run Audit", id="run-btn", n_clicks=0, disabled=True, className="btn"),
            html.Span(id="status", className="muted")
        ], style={"gridColumn": "1 / -1"})
    ]),

    # Preview + filter
    html.Div(className="pre-summary", children=[
        html.Div(className="card", children=[
            html.Div(className="card-title", children="Data Preview"),
            html.Div(id="data-preview", className="preview")
        ]),
        html.Div(className="card", children=[
            html.Div(className="card-title", children="Filter Findings"),
            dcc.Checklist(
                id="sev-filter",
                options=[{"label": "Critical", "value": "critical"},
                         {"label": "Major", "value": "major"},
                         {"label": "Minor", "value": "minor"}],
                value=["critical", "major", "minor"],
                inline=True,
                className="pill-checklist"
            ),
            html.Div(className="muted", children="Toggle severities to show/hide those cards.")
        ]),
    ]),

    html.Hr(className="rule"),

    dcc.Loading(type="default", children=[
        html.Div(id="exec-summary", children=[]),
        html.Div(id="top-fixes", children=[]),
        html.H2("Findings", className="section-title"),
        html.Div(id="findings", children=[]),
        html.Div(id="download-link", children=[], className="download-area"),
    ]),

    # Store enriched results (with duplicate/outlier row details) for filtering
    dcc.Store(id="results-store"),

    # Footer with credit + privacy
    html.Footer(className="footer", children=[
        html.Div([
            "Created by Shannon Wise · ",
            html.A("shannonwiseanalytics.com",
                   href="https://shannonwiseanalytics.com/", target="_blank", rel="noopener")
        ]),
        html.Div(className="disclaimer",
            children=("Privacy: files you upload are processed in memory to generate this report and are not persisted. "
                      "No data is sent to third-party APIs. Temporary artifacts are deleted automatically. "
                      "Please avoid uploading highly sensitive or regulated data."))
    ])
])


# -----------------------
# Small helper callbacks
# -----------------------
@app.callback(
    Output("file-name", "children"),
    Output("run-btn", "disabled"),
    Input("upload-data", "filename"),
    prevent_initial_call=True
)
def show_file_name(filename):
    return (filename or ""), (filename is None)


@app.callback(
    Output("baseline-name", "children"),
    Input("upload-baseline", "filename"),
    prevent_initial_call=True
)
def show_baseline_name(filename):
    return filename or ""


@app.callback(
    Output("data-preview", "children"),
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
    prevent_initial_call=True
)
def preview(contents, filename):
    if not contents:
        return html.Div(className="muted", children="Upload a dataset to see a quick preview.")
    try:
        df = parse_contents(contents, filename)
        return dash_table.DataTable(
            columns=[{"name": c, "id": c} for c in df.columns],
            data=df.head(5).to_dict("records"),
            page_size=5, style_table={"overflowX": "auto"}
        )
    except Exception as e:
        return html.Div(className="muted", children=f"Could not preview file: {e}")


# -----------------------
# Main audit: run checks and build enriched results
# -----------------------
@app.callback(
    Output("status", "children"),
    Output("exec-summary", "children"),
    Output("download-link", "children"),
    Output("results-store", "data"),
    Input("run-btn", "n_clicks"),
    State("upload-data", "contents"), State("upload-data", "filename"),
    State("upload-baseline", "contents"), State("upload-baseline", "filename"),
    prevent_initial_call=True
)
def run(n, data_contents, data_name, base_contents, base_name):
    if not data_contents:
        return "No data file uploaded.", [], [], None

    try:
        df = parse_contents(data_contents, data_name)
    except Exception as e:
        return f"Failed to read data: {e}", [], [], None

    baseline = None
    if base_contents and base_name:
        try:
            _, content_string = base_contents.split(",")
            baseline = json.loads(base64.b64decode(content_string).decode("utf-8"))
        except Exception as e:
            return f"Failed to read baseline: {e}", [], [], None

    # run all checks
    results = []
    results += ra.check_schema(df, baseline)
    if baseline and "primary_key" in baseline:
        results += ra.check_primary_key(df, baseline.get("primary_key", []))
    results += ra.check_missing_duplicates_types(df)
    results += ra.check_outliers_iqr(df)
    results += ra.check_rare_categories(df)
    results += ra.check_semantic_regex(df)
    results += ra.check_dates(df)

    # enrich: duplicates (row indices + sample rows)
    try:
        dup_df = df[df.duplicated(keep=False)]
        if not dup_df.empty:
            for r in results:
                if r.name == "Duplicate Rows" and r.issues:
                    r.issues[0]["row_indices"] = [int(i) if isinstance(i, (int, float)) else str(i) for i in dup_df.index]
                    r.issues[0]["sample_rows"] = dup_df.head(10).to_dict("records")
                    r.issues[0]["columns"] = list(df.columns)
                    break
    except Exception:
        pass

    # enrich: IQR outliers (row indices + sample values) per affected column
    for r in results:
        if r.name.startswith("IQR Outliers"):
            # parse column name after colon
            try:
                col = r.name.split(":", 1)[1].strip()
            except Exception:
                col = None
            if col and col in df.columns and r.issues:
                info = r.issues[0]
                lo = info.get("lo")
                hi = info.get("hi")
                s = pd.to_numeric(df[col], errors="coerce")
                mask = (s < lo) | (s > hi)
                idxs = s[mask].index.tolist()
                sample = [{"index": idx, "value": None if pd.isna(s.loc[idx]) else float(s.loc[idx])}
                          for idx in s[mask].sort_values(key=lambda x: x.abs(), ascending=False).head(10).index]
                info["column"] = col
                info["row_indices"] = [int(i) if isinstance(i, (int, float)) else str(i) for i in idxs]
                info["sample_values"] = sample
                info["explain"] = (
                    "Outliers flagged using the IQR rule: values lower than Q1−1.5×IQR or higher than Q3+1.5×IQR. "
                    f"For “{col}”, the low/high cutoffs are {lo:.3f} / {hi:.3f}."
                )

    # numbers for summary
    score = ra.compute_score(results)
    sev_counts = Counter([r.severity for r in results])

    summary = html.Div(className="cards", children=[
        card("Overall score", html.Div(str(score), className="big")),
        card("Issues", html.Div([
            html.Div(["Critical: ", str(sev_counts.get("critical", 0))]),
            html.Div(["Major: ", str(sev_counts.get("major", 0))]),
            html.Div(["Minor: ", str(sev_counts.get("minor", 0))]),
        ])),
        card("Dataset", html.Div([html.Div(f"Rows: {len(df)}"), html.Div(f"Columns: {len(df.columns)}")])),
    ])

    # build themed HTML report with the enriched results
    with tempfile.TemporaryDirectory() as td:
        out_path = Path(td) / "report.html"
        render_report({
            "file": data_name,
            "rows": len(df),
            "columns": list(df.columns),
            "results": [r.__dict__ for r in results],  # enriched
            "score": score,
            "credit_html": ('Created by <strong>Shannon Wise</strong> · '
                            '<a href="https://shannonwiseanalytics.com/" target="_blank" rel="noopener">'
                            'shannonwiseanalytics.com</a>'),
            "privacy_html": ("Privacy: files you upload are processed in memory and not persisted. "
                             "No data is sent to third-party APIs. Temporary artifacts are deleted automatically. "
                             "Avoid uploading highly sensitive/regulated data.")
        }, str(out_path))
        html_bytes = out_path.read_bytes()

    b64_html = base64.b64encode(html_bytes).decode("utf-8")
    download = html.A("Download full HTML report",
                      href=f"data:text/html;base64,{b64_html}",
                      download="dq_ai_report.html",
                      className="btn")

    # store lean version for interactive rendering
    store = {"results": [{"name": r.name, "severity": r.severity, "impact": r.impact, "issues": r.issues} for r in results]}
    status = f"Audit complete. Score: {score}"
    return status, summary, download, store


# -----------------------
# Filter-driven rendering (no re-run)
# -----------------------
@app.callback(
    Output("top-fixes", "children"),
    Output("findings", "children"),
    Input("sev-filter", "value"),
    State("results-store", "data"),
    prevent_initial_call=True
)
def render_filtered(sev_filter, store):
    if not store or not store.get("results"):
        return [], []

    res = store["results"]
    allowed = set(sev_filter or [])
    filtered = [r for r in res if r["severity"] in allowed]

    # top fixes
    top = sorted([r for r in filtered if r["severity"] in ("critical", "major")],
                 key=lambda x: x["impact"], reverse=True)[:3]
    fixes = []
    for r in top:
        fk = next((k for k in FRIENDLY if r["name"].startswith(k)), r["name"])
        what, how = FRIENDLY.get(fk, ("Issue detected.", "Review and fix in source/ETL."))
        fixes.append(html.Li(html.Span([html.Strong(fk), badge(r["severity"]), html.Span(f" — {what}  Fix: {how}")])))
    topfixes = card("Top fixes (start here)", html.Ul(fixes or [html.Li("No high-impact issues.")]))

    # findings
    finding_cards = []
    for r in filtered:
        title = r["name"]; sev = r["severity"]; issues = r.get("issues", [])
        fk = next((k for k in FRIENDLY if title.startswith(k)), None)
        friendly = (html.P([html.Span(FRIENDLY[fk][0] + " "), html.Em("Fix: " + FRIENDLY[fk][1])])
                    if fk else html.P("See details below."))

        # default details
        details = html.Ul([html.Li(json.dumps(i)) for i in issues])

        # Duplicate Rows → show row indices + a sample table if present
        if title == "Duplicate Rows" and issues:
            info = issues[0]
            rows = info.get("row_indices", [])
            sample_rows = info.get("sample_rows", [])
            cols = info.get("columns", [])
            parts = [html.P(f"Duplicate rows detected: {len(rows)}")]
            if rows:
                preview_list = ", ".join([str(x) for x in rows[:20]]) + (" …" if len(rows) > 20 else "")
                parts.append(html.P(f"Row indices (sample): {preview_list}"))
            if sample_rows:
                parts.append(dash_table.DataTable(
                    columns=[{"name": c, "id": c} for c in cols],
                    data=sample_rows, page_size=5, style_table={"overflowX": "auto"}
                ))
            details = html.Div(parts)

        # IQR Outliers → explanation + sample index/value table
        if title.startswith("IQR Outliers") and issues:
            info = issues[0]
            explain = info.get("explain")
            sample_vals = info.get("sample_values", [])
            parts = []
            if explain:
                parts.append(html.P(explain))
            if "row_indices" in info:
                parts.append(html.P(f"Outlier rows (count): {len(info['row_indices'])}"))
            if sample_vals:
                parts.append(dash_table.DataTable(
                    columns=[{"name": "index", "id": "index"}, {"name": "value", "id": "value"}],
                    data=sample_vals, page_size=6, style_table={"overflowX": "auto"}
                ))
            details = html.Div(parts)

        finding_cards.append(card(title, badge(sev), friendly, details))

    return topfixes, finding_cards


# Expose Flask server for Gunicorn
server = app.server

if __name__ == "__main__":
    import os
    # local run (Render uses Gunicorn, not this branch)
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))
