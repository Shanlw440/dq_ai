# DQ-AI: Automated Data Quality Auditor (Starter)

## Quickstart
```bash
# 1) Create a virtual environment
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate

# 2) Install dependencies
pip install -r requirements.txt

# 3) Run an audit on the sample data
python run_audit.py --input data/sample_orders.csv --baseline rules/baseline_schema.json --out report.html

# 4) Run on your own file
python run_audit.py --input path/to/your.csv --out report.html
```
This starter runs core checks: schema drift vs baseline, primary key uniqueness, missing values, duplicates, data types, IQR outliers, rare categories, simple date validity, semantic regex (email/UK postcode), and PSI drift (if a reference sample is configured).

You can configure rules and expected schema in `rules/baseline_schema.json`.
