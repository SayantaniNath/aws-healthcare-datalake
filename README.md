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
| 13 | Iceberg silver | Converted silver/patients from Parquet to Iceberg v2. Resolved 3 catalog config errors (see Debugging Log). GE 17/17 re-validated on Iceberg output | ✅ | 2026-06-25 |
| 14 | dim_patient_consent | GDPR consent table with erasure flag | ⏳ | — |
| 15 | Redshift gold | Column-level privileges, analytical views | ⏳ | — |
| 16 | EMR Serverless | 10-year readmission aggregation job | ⏳ | — |

---

## Debugging Log — All Errors and Fixes

### Glue ETL Job Errors

---

**Error: `s3:DeleteObject` not authorized on `write.mode("overwrite")`**

```
AccessDeniedException: User: arn:aws:iam::941141114246:role/AWSGlueServiceRole-clinicalflow
is not authorized to perform: s3:DeleteObject
```

Cause: `mode("overwrite")` deletes existing files in the S3 prefix before writing new ones. The Glue IAM role only had `s3:PutObject` and `s3:GetObject`.

Fix: Added `s3:DeleteObject` to the `AWSGlueServiceRole-clinicalflow` inline policy for the bucket.

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::clinicalflow-datalake-941141114246",
    "arn:aws:s3:::clinicalflow-datalake-941141114246/*"
  ]
}
```

---

**Error: Union struct types on numeric columns**

```
AnalysisException: Resolved attribute missing from child: healthcare_expenses#xx
(or struct type {'double': 41009.33, 'string': None} in silver output)
```

Cause: 3 malformed rows in `patients.csv` (lines 60385, 60401, 64324) had two records concatenated on one line. Glue saw some rows with numeric values and some with string spill — inferred column type as a union struct `{double, string}`.

Fix: Added `resolveChoice` on the DynamicFrame before `.toDF()` to force concrete types:

```python
.resolveChoice(specs=[
    ("zip",                 "cast:string"),
    ("healthcare_expenses", "cast:double"),
    ("healthcare_coverage", "cast:double"),
    ("income",              "cast:long"),
    ("fips",                "cast:long")
])
```

---

**Error: PHI columns `address`, `drivers`, `maiden` present in silver layer**

Cause: Initial drop list covered only the most obvious Safe Harbor fields. `maiden` (pre-marriage last name, 16K rows), `drivers` (license number e.g. S99956685, 52K/64K rows), and `address` (full street address) were missed.

Fix: Audited all 28 raw columns against Safe Harbor § 164.514(b)(2) — added all three to `cols_to_drop`.

---

**Error: Duplicate `job.commit()` — job failed or behaved unexpectedly**

Cause: Glue console auto-inserts a `job.commit()` at the top as boilerplate when creating a job. User also added one at the end, resulting in two calls. Calling `job.commit()` mid-script before the write completed caused issues.

Fix: Keep only the final `job.commit()` after the write step. Delete any auto-inserted one from the top.

---

**Error: Python syntax error — comments inside method chains**

```
SyntaxError: invalid syntax
```

Cause: Inline `# comments` were placed inside a chained method call using line continuation `\`:

```python
# WRONG — comment breaks the chain
quarantine_df \
    .withColumn('reason', F.lit('malformed')) \  # this breaks it
    .write.parquet(...)
```

Fix: Move comments above the chain block, never inline within `\`-continued expressions.

---

### DQDL / Data Quality Errors

---

**Error: DQDL `ColumnCount = 15` failing with count 16**

```
DQDL rule FAILED: ColumnCount = 15 (actual: 16)
```

Cause: Glue Crawler added a spurious `partition_0` partition key to the `clinicalflow_silver.patients` catalog table. This column existed in the catalog metadata but not in the actual parquet files.

First fix attempt: Added exclusion patterns in the crawler — did not remove the already-created partition key from the existing table.

Second fix attempt: `aws glue update-table` — failed:

```
InvalidInputException: PartitionColumns cannot be deleted when indexes are enabled
```

Final fix: Delete the table and recreate it via CLI with `"PartitionKeys": []`:

```bash
aws glue delete-table --database-name clinicalflow_silver --name patients
aws glue create-table --database-name clinicalflow_silver --table-input file://table_def.json
# table_def.json must have "PartitionKeys": []
```

Lesson: DQDL evaluates the Glue Catalog schema, not S3 files directly. Stale or incorrect catalog metadata = wrong DQDL results. When catalog and data disagree, ground truth is S3:

```bash
python3 -c "
import pandas as pd
df = pd.read_parquet('s3://clinicalflow-datalake-941141114246/silver/patients/',
                     storage_options={'anon': False})
print(len(df.columns), 'columns:', df.columns.tolist())
"
```

---

**Error: DQDL Spark exception on column statistics**

```
AnalysisException: Resolved attribute missing from child: partition_0
```

Cause: Follow-on from the `partition_0` issue above. DQDL's statistics engine tried to compute column-level stats, Spark attempted to read `partition_0` from S3 parquet files — it doesn't exist there, causing a Spark analysis failure.

Fix: Same as above — recreate table without partition keys.

---

### GE Validation Script Errors

---

**Error: `ArrowInvalid: GetFileInfo() yielded path outside base dir`**

Cause: `pyarrow.parquet.ParquetDataset` with an `s3fs` filesystem normalises paths internally, producing a path that doesn't match the base dir prefix.

```python
# Broken
import s3fs, pyarrow.parquet as pq
fs = s3fs.S3FileSystem()
df = pq.ParquetDataset(SILVER_PATH, filesystem=fs).read_pandas().to_pandas()
```

Fix: Replace with `pandas.read_parquet` using `storage_options`:

```python
df = pd.read_parquet(SILVER_PATH, storage_options={"anon": False})
# anon: False → use AWS credentials from ~/.aws/credentials instead of anonymous access
```

---

**Error: `AttributeError: 'PandasDataset' object has no attribute 'expect_column_to_not_exist'`**

Cause: GE 0.18 removed `expect_column_to_not_exist` from the built-in expectation set.

Fix: Manual Python check inline, stored in the same results list format:

```python
absent = col not in set(df.columns)
results.append({"check": f"column '{col}' absent", "success": absent, "detail": {}})
```

---

### Iceberg Catalog Errors (Glue Job)

---

**Error 1: `REQUIRES_SINGLE_PART_NAMESPACE`**

```
AnalysisException: [REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part
namespace, but got `glue_catalog`.`clinicalflow_silver`.
```

Cause: `glue_catalog` was not registered as a catalog — Spark fell back to the default `spark_catalog` and tried to interpret `glue_catalog.clinicalflow_silver` as a two-part namespace under it.

Root cause: Multiple `--conf` job parameters in the Glue console are stored as a JSON dict — duplicate keys mean only the last one survives. Setting `--conf` four times (once per catalog property) meant only the final value was applied, and none of the catalog settings were actually loaded.

Fix: Chain all `--conf` values into a single key's value, space-separated:

```
Key:   --conf
Value: spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
       --conf spark.sql.catalog.glue_catalog=...
       --conf spark.sql.catalog.glue_catalog.warehouse=...
       --conf spark.sql.catalog.glue_catalog.io-impl=...
```

Also: `spark.conf.set()` calls in the script for catalog config are ineffective — they run after `SparkContext` initialises, which is too late for catalog plugin registration. All catalog config must be in job parameters.

---

**Error 2: `Plugin class for catalog 'glue_catalog' does not implement CatalogPlugin: org.apache.iceberg.aws.glue.GlueCatalog`**

```
An error occurred while calling createOrReplace.
Plugin class for catalog 'glue_catalog' does not implement CatalogPlugin:
org.apache.iceberg.aws.glue.GlueCatalog
```

Cause: `org.apache.iceberg.aws.glue.GlueCatalog` is an Iceberg catalog implementation (storage backend), not a Spark `CatalogPlugin`. It does not implement the interface Spark requires for catalog registration. The correct Spark-facing plugin class is `org.apache.iceberg.spark.SparkCatalog`, which wraps `GlueCatalog`.

Fix: Use `SparkCatalog` as the catalog plugin and reference `GlueCatalog` as the `catalog-impl`:

```
spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.warehouse=s3://clinicalflow-datalake-941141114246/
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Full working `--conf` value (single key, all chained):

```
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://clinicalflow-datalake-941141114246/ --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Think of it as two layers: `SparkCatalog` is the Spark-facing plugin; `GlueCatalog` is the AWS-facing storage backend underneath it.

---

**Error 3: `Input Glue table is not an iceberg table: glue_catalog.clinicalflow_silver.patients (type=null)`**

```
An error occurred while calling createOrReplace.
Input Glue table is not an iceberg table:
glue_catalog.clinicalflow_silver.patients (type=null)
```

Cause: Catalog config was now working correctly (progress from Error 2), but the existing `clinicalflow_silver.patients` table in the Glue catalog was a plain Parquet table created by earlier job runs. `createOrReplace()` found it, saw it was not Iceberg format, and refused to overwrite it.

Fix: Delete the existing non-Iceberg table from the Glue catalog, then run the job. `createOrReplace()` creates a fresh Iceberg table when none exists.

```bash
aws glue delete-table --database-name clinicalflow_silver --name patients
```

After this, the job succeeded. Iceberg write confirmed by presence of `metadata/` folder in `s3://clinicalflow-datalake-941141114246/silver/patients/` — this is the snapshot metadata layer that Parquet does not have. GE validation re-run: 17/17 passing on the Iceberg output.

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
