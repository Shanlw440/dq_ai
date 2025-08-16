\
import argparse, json, pathlib
import pandas as pd

def infer_dtypes(df: pd.DataFrame):
    return {c: str(t) for c, t in df.dtypes.items()}

def load_df(path: str) -> pd.DataFrame:
    p = pathlib.Path(path)
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p)
    if p.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(p)
    raise ValueError("Unsupported file format. Use CSV or Excel.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV/XLSX path to infer baseline from")
    ap.add_argument("--pk", nargs="*", default=None, help="Primary key column(s), e.g., --pk id")
    ap.add_argument("--out", default="rules/baseline_schema.json", help="Path to write baseline JSON")
    args = ap.parse_args()

    df = load_df(args.input)
    baseline = {
        "columns": list(df.columns),
        "dtypes": infer_dtypes(df),
    }
    if args.pk:
        baseline["primary_key"] = args.pk

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(baseline, indent=2))
    print(f"Wrote baseline to {out}")

if __name__ == "__main__":
    main()
