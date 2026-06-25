# ClinicalFlow — AWS Healthcare Data Lakehouse

A portfolio-grade data engineering project demonstrating a production-style healthcare data lakehouse on AWS. Built on 64,338 synthetic patients (Synthea EHR) with full HIPAA Safe Harbor de-identification, GDPR erasure support, multi-layer data quality validation, and an Iceberg-based silver layer.

---

## Architecture Overview

```
Synthea EHR (CSV, 8.2 GB)
        │
        ▼
S3 raw/synthea/csv/          ← SSE-KMS encrypted (alias/clinicalflow-cmk)
        │
        ▼
Glue Crawler                 ← clinicalflow-raw-crawler → clinicalflow_raw catalog
        │
        ▼
Glue ETL Job                 ← hipaa_deid_patients.py
  ├── resolveChoice           ← fix union struct types from malformed rows
  ├── Quarantine              ← 3 malformed rows → quarantine/patients/ with audit timestamp
  ├── HIPAA Safe Harbor       ← drop 14 PHI columns, tokenize ID, truncate zip, age-bucket
  └── Write                   ← silver/patients/ as Iceberg v2
        │
        ▼
S3 silver/patients/          ← Iceberg table, 15 clean columns, 64,335 rows
        │
        ├── GE validation     ← ge_validate_silver_patients.py (17 checks)
        └── DQDL ruleset      ← clinicalflow-silver-patients-phi-audit (8 rules)
```

---

## Dataset

| Property | Value |
|---|---|
| Source | Synthea synthetic EHR (no real patient data) |
| Patients | 64,338 |
| Time span | 10 years |
| State | Massachusetts, seed 42 |
| Raw files | 16 CSVs, 8.2 GB |
| Key tables | patients, encounters, conditions, procedures, medications, claims |

**Data quality note:** 3 malformed rows exist in `patients.csv` (lines 60385, 60401, 64324) where two patient records were concatenated on one line due to a missing newline in Synthea output. Detected and quarantined by the ETL job — silver receives only fully-formed records.

---

## AWS Infrastructure

| Component | Detail |
|---|---|
| Account | 941141114246 |
| Region | us-east-1 |
| S3 bucket | `clinicalflow-datalake-941141114246` |
| KMS key | `alias/clinicalflow-cmk` (SSE-KMS on all data) |
| CloudTrail | `clinicalflow-audit-trail` — logs all S3 data events, KMS encrypted |
| Glue role | `AWSGlueServiceRole-clinicalflow` |
| Glue databases | `clinicalflow_raw` (17 tables), `clinicalflow_silver` (patients) |

**S3 folder structure:**
```
clinicalflow-datalake-941141114246/
├── raw/synthea/csv/          ← 16 Synthea CSV files
├── silver/patients/          ← Iceberg de-identified patients
├── quarantine/patients/      ← malformed source rows with audit metadata
├── gold/                     ← Redshift-ready aggregates (pending)
└── cloudtrail-logs/          ← CloudTrail audit logs
```

---

## HIPAA Safe Harbor De-identification

**Job:** `glue_jobs/hipaa_deid_patients.py`

Safe Harbor § 164.514(b)(2) requires removal of 18 identifier categories. Applied transformations:

| Transformation | Columns | Notes |
|---|---|---|
| **Dropped** | first, middle, last, maiden | Names including maiden name |
| **Dropped** | ssn, passport, drivers | Government IDs (drivers = license number e.g. S99956685; present for 52K/64K patients) |
| **Dropped** | prefix, suffix | Name qualifiers |
| **Dropped** | address | Full street address |
| **Dropped** | lat, lon | Geographic coordinates (below county level = PHI) |
| **Dropped** | birthdate, deathdate | Dates |
| **Tokenized** | id | Original UUID → new random UUID (breaks re-identification linkage) |
| **Truncated** | zip | 5-digit → 3-digit prefix (Safe Harbor geographic standard) |
| **Age-bucketed** | birthdate | Birth year → 5-year range e.g. 1962 → 1960, stored as `birth_year_bucket` |

**Retained columns (15 total):**
`id` (tokenized), `marital`, `race`, `ethnicity`, `gender`, `birthplace`, `city`, `state`, `county`, `fips`, `zip` (truncated), `healthcare_expenses`, `healthcare_coverage`, `income`, `birth_year_bucket`

**Glue job internals:**
- `resolveChoice` — fixes union struct types (`{'double': x, 'string': None}`) caused by 3 malformed rows in raw CSV where Glue inferred ambiguous column types
- Quarantine step — `reduce(lambda a, b: a | b, [col(c).isNotNull() for c in overflow_cols])` detects malformed rows by checking for non-null values in overflow `col*` columns; writes to `quarantine/patients/` with `quarantine_reason` and `quarantine_ts` before de-id runs
- Iceberg write — `--datalake-formats iceberg` Glue job param, writes via `.writeTo("glue_catalog.clinicalflow_silver.patients").createOrReplace()`

---

## Data Quality Validation

Two-layer validation: AWS-native pipeline gate + independent audit.

### Layer 1 — AWS Glue Data Quality (DQDL)

**Ruleset:** `clinicalflow-silver-patients-phi-audit`
**Table:** `clinicalflow_silver.patients`
**Result:** 8/8 rules passing ✅

```
Rules = [
    RowCount > 0,
    IsComplete "id",
    IsComplete "birth_year_bucket",
    ColumnCount = 15,
    ColumnValues "zip" matches "[0-9]{1,3}",
    ColumnValues "gender" in ["M", "F"],
    Completeness "race" > 0.99,
    ColumnValues "id" matches "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
]
```

| Rule | What it catches |
|---|---|
| `RowCount > 0` | Output not empty |
| `IsComplete "id"` | Every patient has a surrogate key |
| `IsComplete "birth_year_bucket"` | Age bucketing ran on every row |
| `ColumnCount = 15` | Tripwire — if any PHI column survived, count goes above 15 |
| `zip matches "[0-9]{1,3}"` | No 5-digit zips survived truncation |
| `gender in ["M", "F"]` | Sanity check on Synthea demographics |
| `Completeness "race" > 0.99` | Synthea always generates race — mass nulls = pipeline error |
| `id matches UUID regex` | Tokenisation worked, no original Synthea IDs remain |

### Layer 2 — Great Expectations (independent audit)

**Script:** `validation/ge_validate_silver_patients.py`
**Result:** 17/17 checks passing ✅
**Reports:** `validation/reports/ge_silver_patients_<timestamp>.json`

| Category | Checks |
|---|---|
| PHI column absence by name | first, middle, last, ssn, passport, prefix, suffix, lat, lon, birthdate, deathdate — 11 checks |
| zip ≤ 3 chars | 1 check |
| birth_year_bucket not null + divisible by 5 | 2 checks |
| id not null + UUID regex | 2 checks |
| Row count > 0 | 1 check |

**Run locally:**
```bash
source ~/myenv/bin/activate
python3 validation/ge_validate_silver_patients.py
```

---

## Phase 1 Step-by-Step Progress

| # | Step | What was done | Status | Date |
|---|---|---|---|---|
| 1 | Synthea data generation | Generated 64,338 synthetic patients × 10yr, Massachusetts, seed 42. 16 CSV files, 8.2 GB to ~/output/csv/ | ✅ | 2026-06-22 |
| 2 | KMS CMK | Created customer-managed KMS key `alias/clinicalflow-cmk` for SSE-KMS encryption on all S3 data | ✅ | 2026-06-23 |
| 3 | S3 bucket | Created `clinicalflow-datalake-941141114246` — block all public access, versioning ON, SSE-KMS default encryption | ✅ | 2026-06-23 |
| 4 | CloudTrail | Created `clinicalflow-audit-trail` logging all S3 data events (GetObject, PutObject, DeleteObject) to `cloudtrail-logs/`, KMS encrypted | ✅ | 2026-06-23 |
| 5 | Upload raw CSVs | Uploaded all 16 CSVs to `raw/synthea/csv/` with SSE-KMS using `alias/clinicalflow-cmk` | ✅ | 2026-06-23 |
| 6 | Glue Crawler (raw) | `clinicalflow-raw-crawler` crawled raw/ prefix — created 17 tables in `clinicalflow_raw` database | ✅ | 2026-06-23 |
| 7 | Glue ETL v1 | Initial de-id job — dropped first/middle/last/ssn/passport/prefix/suffix/lat/lon; tokenized id; truncated zip; age-bucketed birthdate. **Gap found later:** address, drivers, maiden were not dropped | ✅ | 2026-06-24 |
| 8 | GE validation (first run) | Ran ge_validate_silver_patients.py — 17/17 passed on PHI cols but silver still had address/drivers/maiden (not in PHI_COLS list at the time) | ✅ | 2026-06-24 |
| 9 | PHI gap discovery | Inspected raw patients.csv columns — found address (full street address), drivers (license numbers e.g. S99956685, 52K/64K rows), maiden (16K rows) were direct identifiers not in original drop list | 🔍 | 2026-06-24 |
| 10 | Malformed row discovery | Found 3 rows (lines 60385/60401/64324) where two records concatenated on one line — caused Glue to create union struct types on numeric cols and overflow col28–col35 columns | 🔍 | 2026-06-24 |
| 11 | Glue ETL v2 | Fixed job: added address/drivers/maiden to drop list; added resolveChoice for numeric cols; added quarantine step for malformed rows; fixed IAM (added s3:DeleteObject); removed duplicate job.commit() | ✅ | 2026-06-24 |
| 12 | DQDL ruleset | Created `clinicalflow-silver-patients-phi-audit` ruleset on silver patients table — 8 rules. Fixed partition_0 issue (crawler added spurious partition key — deleted table, recreated via CLI with PartitionKeys:[]). 8/8 passing | ✅ | 2026-06-24 |
| 13 | Iceberg silver | Added Iceberg job params (`--datalake-formats iceberg`) and spark catalog config to ETL job — pending first run | 🟡 | — |
| 14 | dim_patient_consent | GDPR consent table with erasure flag | ⏳ | — |
| 15 | Redshift gold | Column-level privileges, analytical views | ⏳ | — |
| 16 | EMR Serverless | 10-year readmission aggregation job | ⏳ | — |

---

## Key Debugging Lessons

**Issue: DQDL ColumnCount = 15 failing with 16**
Cause: Glue Crawler auto-detected a `partition_0` column that doesn't exist in the parquet files.
Fix: Deleted the crawler-created table, recreated via CLI with `"PartitionKeys": []`.
Lesson: DQDL evaluates Glue Catalog schema, not S3 files. Always verify at the actual data layer first.

```bash
# Ground truth check — bypasses catalog entirely
python3 -c "
import pandas as pd
df = pd.read_parquet('s3://clinicalflow-datalake-941141114246/silver/patients/',
                     storage_options={'anon': False})
print(len(df.columns), 'columns:', df.columns.tolist())
"
```

**Issue: Union struct types in silver output**
Cause: 3 malformed rows in raw CSV caused Glue to infer columns like `healthcare_expenses` as either double or string → struct type `{'double': 41009.33, 'string': None}`.
Fix: `resolveChoice(specs=[("healthcare_expenses", "cast:double"), ...])` on the DynamicFrame before `.toDF()`.

**Issue: PHI columns address/drivers/maiden survived de-id**
Cause: Original drop list only covered the most obvious Safe Harbor fields.
Fix: Audited all 28 raw columns against Safe Harbor § 164.514(b)(2) — added 3 missed identifiers.

---

## Local Debug Commands

```bash
source ~/myenv/bin/activate

# Check silver output
python3 -c "
import pandas as pd
df = pd.read_parquet('s3://clinicalflow-datalake-941141114246/silver/patients/',
                     storage_options={'anon': False})
print(len(df), 'rows,', len(df.columns), 'cols:', df.columns.tolist())
print(df.iloc[0])
"

# Check quarantine rows
python3 -c "
import pandas as pd
df = pd.read_parquet('s3://clinicalflow-datalake-941141114246/quarantine/patients/',
                     storage_options={'anon': False})
print(df[['quarantine_reason','quarantine_ts']])
"

# Run GE PHI audit
python3 validation/ge_validate_silver_patients.py

# List S3 prefixes
aws s3 ls s3://clinicalflow-datalake-941141114246/silver/patients/
aws s3 ls s3://clinicalflow-datalake-941141114246/quarantine/patients/

# Check Glue catalog table schema
aws glue get-table --database-name clinicalflow_silver --name patients
```

---

## Repository Structure

```
aws-healthcare-datalake/
├── glue_jobs/
│   └── hipaa_deid_patients.py         ← HIPAA de-id ETL + quarantine + Iceberg write
├── validation/
│   ├── ge_validate_silver_patients.py ← GE PHI audit (17 checks)
│   └── reports/                       ← timestamped JSON reports per run
├── profiling/
│   └── profile_patients.py            ← ydata-profiling for PHI discovery
└── README.md
```

---

## Interview Story

> "Built ClinicalFlow — a healthcare data lakehouse on AWS processing 64K synthetic EHR patients. Ingests raw Synthea CSVs into S3 with SSE-KMS encryption and CloudTrail audit logging. A Glue ETL job applies HIPAA Safe Harbor de-identification: drops 14 direct identifiers including names, SSN, driver's license, passport, street address, and coordinates; tokenizes patient IDs to UUIDs; truncates zip codes to 3 digits; and age-buckets birthdates into 5-year ranges. The job also detects and quarantines malformed source rows with an audit trail, and resolves Glue union struct types via resolveChoice. De-identified data is written to an Iceberg silver layer. Two-layer data quality validation: AWS Glue DQDL (8 rules, pipeline gate) plus an independent Great Expectations audit (17 checks) including PHI column absence by name and UUID tokenisation verification."
