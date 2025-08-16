\
import argparse, json, pathlib, re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype, is_string_dtype
from scipy.stats import chisquare
from rapidfuzz import fuzz, process as rf_process
from report.html_report import render_report

SEVERITY_WEIGHTS = {"critical": 3, "major": 2, "minor": 1}

SEMANTIC_PATTERNS = {
    "email": re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
    "uk_postcode": re.compile(r"^(GIR ?0AA|(?:(?:[A-Z][0-9]{1,2}|[A-Z][A-HJ-Y][0-9]{1,2}|[A-Z][0-9][A-Z]|[A-Z][A-HJ-Y][0-9]?[A-Z]) ?[0-9][A-Z]{2}))$", re.I),
}

@dataclass
class CheckResult:
    name: str
    severity: str
    issues: List[Dict[str, Any]]
    impact: float  # 0..1 normalized

def norm(x: float, cap: float) -> float:
    return max(0.0, min(1.0, x / cap))

def load_df(path: str) -> pd.DataFrame:
    p = pathlib.Path(path)
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p)
    if p.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(p)
    raise ValueError("Unsupported file format. Use CSV or Excel.")

def check_schema(df: pd.DataFrame, baseline: Optional[Dict[str, Any]]) -> List[CheckResult]:
    issues = []
    if not baseline: 
        return issues
    expected_cols = set(baseline.get("columns", []))
    actual_cols = set(df.columns)
    missing = list(expected_cols - actual_cols)
    new = list(actual_cols - expected_cols)
    if missing:
        issues.append(CheckResult("Missing Columns", "critical", [{"missing": missing}], impact=norm(len(missing), 10)))
    if new:
        issues.append(CheckResult("Unexpected Columns", "major", [{"new": new}], impact=norm(len(new), 10)))
    exp_types = baseline.get("dtypes", {})
    drift = []
    for c, exp in exp_types.items():
        if c in df.columns:
            act = str(df[c].dtype)
            if act != exp:
                drift.append({"column": c, "expected": exp, "actual": act})
    if drift:
        issues.append(CheckResult("Dtype Drift", "major", drift, impact=norm(len(drift), 15)))
    return issues

def check_primary_key(df: pd.DataFrame, pk: List[str]) -> List[CheckResult]:
    if not pk: return []
    dupes = df.duplicated(subset=pk).sum()
    if dupes > 0:
        ex = df[df.duplicated(subset=pk, keep=False)][pk].head(5).to_dict(orient="records")
        return [CheckResult("Primary Key Uniqueness", "critical", [{"duplicates": int(dupes), "examples": ex}], impact=norm(dupes, max(10, len(df)*0.02)))]
    return []

def check_missing_duplicates_types(df: pd.DataFrame) -> List[CheckResult]:
    issues = []
    miss = df.isna().sum()
    miss = miss[miss > 0]
    if len(miss) > 0:
        issues.append(CheckResult("Missing Values", "major", [{"column": c, "missing": int(v)} for c, v in miss.to_dict().items()], impact=norm(int(miss.sum()), max(10, len(df)*0.05))))
    dups = int(df.duplicated().sum())
    if dups > 0:
        issues.append(CheckResult("Duplicate Rows", "major", [{"duplicates": dups}], impact=norm(dups, max(10, len(df)*0.03))))
    dtypes_issue = [{"column": c, "dtype": str(t)} for c, t in df.dtypes.items() if str(t) == "object"]
    # Not an issue by itself, but note prevalence of object types
    return issues

def check_outliers_iqr(df: pd.DataFrame) -> List[CheckResult]:
    issues = []
    for c in df.columns:
        if is_numeric_dtype(df[c]):
            s = pd.to_numeric(df[c], errors="coerce").dropna()
            if len(s) < 10: 
                continue
            q1, q3 = np.percentile(s, [25, 75])
            iqr = max(q3 - q1, 1e-9)
            lo, hi = q1 - 1.5*iqr, q3 + 1.5*iqr
            cnt = int(((s < lo) | (s > hi)).sum())
            if cnt > 0:
                issues.append(CheckResult(f"IQR Outliers: {c}", "major", [{"count": cnt, "lo": float(lo), "hi": float(hi)}], impact=norm(cnt, max(10, len(s)*0.05))))
    return issues

def check_rare_categories(df: pd.DataFrame, min_ratio: float = 0.01) -> List[CheckResult]:
    out = []
    for c in df.columns:
        if is_string_dtype(df[c]) or df[c].dtype == "category":
            vc = df[c].astype(str).value_counts(normalize=True)
            rare = vc[vc < min_ratio]
            if len(rare) > 0:
                out.append(CheckResult(f"Rare Categories: {c}", "minor", [{"n_rare": int(len(rare)), "examples": dict(rare.head(5))}], impact=norm(len(rare), 50)))
    return out

def check_semantic_regex(df: pd.DataFrame) -> List[CheckResult]:
    found = []
    for c in df.columns:
        if is_string_dtype(df[c]):
            sample = df[c].dropna().astype(str)
            if len(sample) == 0:
                continue
            # Heuristic: if >50% look like emails/postcodes and >30% fail pattern, flag
            like_email = sample.str.contains("@").mean() > 0.5
            like_pc = sample.str.contains(r"[A-Z]\d", regex=True).mean() > 0.5
            if like_email:
                bad = (~sample.str.match(SEMANTIC_PATTERNS["email"])).mean()
                if bad > 0.3:
                    found.append(CheckResult(f"Semantic Violations (email) in {c}", "major", [{"fail_rate": round(float(bad),3)}], impact=min(1.0, bad)))
            if like_pc:
                bad = (~sample.str.match(SEMANTIC_PATTERNS["uk_postcode"])).mean()
                if bad > 0.3:
                    found.append(CheckResult(f"Semantic Violations (UK postcode) in {c}", "major", [{"fail_rate": round(float(bad),3)}], impact=min(1.0, bad)))
    return found

def check_dates(df: pd.DataFrame) -> List[CheckResult]:
    issues = []
    date_cols = [c for c in df.columns if ("date" in c.lower() or is_datetime64_any_dtype(df[c]))]
    parsed = {}
    for c in date_cols:
        try:
            parsed[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
        except Exception:
            continue
    for c, s in parsed.items():
        invalid = int(s.isna().sum())
        if invalid > 0:
            issues.append(CheckResult(f"Invalid Dates: {c}", "minor", [{"invalid": invalid}], impact=norm(invalid, max(10, len(df)*0.05))))
    if "order_date" in parsed and "ship_date" in parsed:
        bad = int((parsed["ship_date"] < parsed["order_date"]).sum())
        if bad > 0:
            issues.append(CheckResult("Temporal Rule: ship_date ≥ order_date", "major", [{"violations": bad}], impact=norm(bad, max(10, len(df)*0.05))))
    return issues

def compute_score(results: List[CheckResult]) -> float:
    penalty = 0.0
    for r in results:
        penalty += SEVERITY_WEIGHTS.get(r.severity, 1) * r.impact * 10  # scale per check
    score = max(0.0, 100.0 - penalty)
    return round(score, 1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to CSV/XLSX to audit")
    ap.add_argument("--baseline", default=None, help="Path to baseline_schema.json (optional)")
    ap.add_argument("--out", default="report.html", help="Output HTML report path")
    args = ap.parse_args()

    df = load_df(args.input)
    baseline = None
    if args.baseline:
        with open(args.baseline, "r") as f:
            baseline = json.load(f)

    results: List[CheckResult] = []
    # Run checks
    results += check_schema(df, baseline)
    if baseline and "primary_key" in baseline:
        results += check_primary_key(df, baseline.get("primary_key", []))
    results += check_missing_duplicates_types(df)
    results += check_outliers_iqr(df)
    results += check_rare_categories(df)
    results += check_semantic_regex(df)
    results += check_dates(df)

    # Build summary
    summary = {
        "file": args.input,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "results": [r.__dict__ for r in results],
    }
    summary["score"] = compute_score(results)
    # Save HTML
    render_report(summary, args.out)
    print(f"Report written to {args.out}. Overall score: {summary['score']}")

if __name__ == "__main__":
    main()
