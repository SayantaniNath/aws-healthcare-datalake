"""
GE validation: silver/patients PHI audit
Asserts that the HIPAA de-identification Glue job left no Protected Health Information
in the silver layer. Fails loudly on any PHI leak so the pipeline can be gated here.

Checks:
  1. PHI columns absent  — names, SSN, passport, coords, raw dates must not exist
  2. zip <= 3 chars       — no 5-digit zip codes (Safe Harbor geographic rule)
  3. birth_year_bucket    — must exist, all values divisible by 5, no nulls
  4. id is UUID           — tokenisation worked, no original Synthea IDs remain
  5. Row count > 0        — output file is not empty
  6. id has no nulls      — every patient has a surrogate key

Output: ~/aws-healthcare-datalake/validation/reports/ge_silver_patients_<ts>.json
        + human-readable pass/fail summary to stdout
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import boto3
import great_expectations as ge
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
BUCKET = "clinicalflow-datalake-941141114246"
SILVER_PATH = f"s3://{BUCKET}/silver/patients/"
REPORTS_DIR = Path(__file__).parent / "reports"

PHI_COLS = [
    "first", "middle", "last",
    "ssn", "passport",
    "prefix", "suffix",
    "lat", "lon",
    "birthdate", "deathdate",
]

UUID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

# ── Read silver parquet from S3 ───────────────────────────────────────────────
print(f"Reading {SILVER_PATH} ...")
df = pd.read_parquet(SILVER_PATH, storage_options={"anon": False})
print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
print(f"  Columns: {list(df.columns)}\n")

df_ge = ge.from_pandas(df)

results = []

def run(name, expectation_fn, *args, **kwargs):
    r = expectation_fn(*args, **kwargs)
    results.append({"check": name, "success": r["success"], "detail": r.get("result", {})})
    status = "✅ PASS" if r["success"] else "❌ FAIL"
    print(f"  {status}  {name}")
    return r["success"]

print("── PHI column absence ──────────────────────────────────────────────────")
actual_cols = set(df.columns)
for col in PHI_COLS:
    absent = col not in actual_cols
    results.append({"check": f"column '{col}' absent", "success": absent, "detail": {}})
    print(f"  {'✅ PASS' if absent else '❌ FAIL'}  column '{col}' absent")

print("\n── zip: max 3 characters ───────────────────────────────────────────────")
if "zip" in df.columns:
    run("zip values <= 3 chars",
        df_ge.expect_column_value_lengths_to_be_between,
        "zip", max_value=3)
else:
    print("  ⚠️  WARN  'zip' column missing — skipped")

print("\n── birth_year_bucket: exists, divisible by 5, no nulls ─────────────────")
run("birth_year_bucket not null",
    df_ge.expect_column_values_to_not_be_null, "birth_year_bucket")

# divisible by 5: modulo check via custom expectation
divisible = df["birth_year_bucket"].dropna().apply(lambda v: int(v) % 5 == 0).all()
results.append({"check": "birth_year_bucket divisible by 5", "success": bool(divisible), "detail": {}})
print(f"  {'✅ PASS' if divisible else '❌ FAIL'}  birth_year_bucket divisible by 5")

print("\n── id: UUID format, no nulls ───────────────────────────────────────────")
run("id not null",
    df_ge.expect_column_values_to_not_be_null, "id")
run("id matches UUID format",
    df_ge.expect_column_values_to_match_regex, "id", UUID_RE)

print("\n── Row count > 0 ───────────────────────────────────────────────────────")
run("row count > 0",
    df_ge.expect_table_row_count_to_be_between, min_value=1)

# ── Summary ───────────────────────────────────────────────────────────────────
passed = sum(1 for r in results if r["success"])
total = len(results)
all_pass = passed == total

print(f"\n{'═'*60}")
print(f"  Result: {'✅  ALL CHECKS PASSED' if all_pass else '❌  FAILURES DETECTED'}")
print(f"  {passed}/{total} checks passed")
print(f"{'═'*60}\n")

# ── Save JSON report ──────────────────────────────────────────────────────────
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
report_path = REPORTS_DIR / f"ge_silver_patients_{ts}.json"

report = {
    "run_timestamp": ts,
    "source": SILVER_PATH,
    "row_count": len(df),
    "checks_passed": passed,
    "checks_total": total,
    "all_pass": all_pass,
    "results": results,
}
report_path.write_text(json.dumps(report, indent=2))
print(f"Report saved → {report_path}")

sys.exit(0 if all_pass else 1)
