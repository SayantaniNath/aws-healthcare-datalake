# AWS Healthcare Data Lake — ClinicalFlow

A production-grade, HIPAA-compliant healthcare data lakehouse built on AWS, demonstrating end-to-end data engineering across ingestion, de-identification, validation, transformation, and orchestration.

---

## Architecture Overview

```
Synthea EHR (64K patients × 10 years)
         │
         ▼
  S3 Raw Layer  ──────────────────────────────────────────────────────┐
  (KMS encrypted)                                                      │
  s3://clinicalflow-datalake/raw/synthea/csv/                         │ CloudTrail
         │                                                             │ Audit Log
         ▼                                                             │
  AWS Glue Crawler                                                     │
  (Auto-schema discovery → clinicalflow_raw catalog)                  │
         │                                                             │
         ▼                                                             │
  AWS Glue ETL — HIPAA Safe Harbor De-identification                  │
  (Drop PII · Tokenize IDs · Age-bucket · Zip-prefix)                │
         │                                                             │
         ▼                                                             │
  S3 Silver Layer (De-identified Parquet / Iceberg)                   │
  s3://clinicalflow-datalake/silver/                                  │
         │                                                             │
         ▼                                                             │
  Great Expectations Validation                                        │
  (Assert no PHI leak · Schema checks · Audit report)                 │
         │                                                             │
         ▼                                                             │
  S3 Gold Layer → Amazon Redshift                                     │
  (Analytical views · Column-level privileges)                        │
         │                                                             │
         ▼                                                             │
  Amazon EMR Serverless                                                │
  (10-year readmission aggregation at scale)                          │
         │                                                             │
         ▼                                                             │
  Apache Airflow (MWAA) Orchestration                  ───────────────┘
```

---

## Dataset

- **Source:** [Synthea](https://github.com/synthetichealth/synthea) — open-source synthetic EHR generator
- **Scale:** 64,338 patients × 10 years of clinical history (Massachusetts)
- **Size:** ~8.2 GB across 16 CSV tables
- **Key tables:** patients, encounters, claims, conditions, medications, procedures, imaging_studies, immunizations, allergies, careplans, devices, payer_transitions, payers, providers, organizations, supplies
- **Format:** FHIR R4 JSON + CSV (no real patient data — 100% synthetic, GitHub-safe)

---

## AWS Stack

| Service | Role |
|---|---|
| **S3** | Raw / Silver / Gold storage layers |
| **KMS (CMK)** | Customer-managed encryption key — HIPAA-grade encryption at rest |
| **CloudTrail** | Audit trail — every S3 read/write logged for HIPAA compliance |
| **AWS Glue Crawler** | Auto-schema discovery → Data Catalog |
| **AWS Glue ETL** | PySpark-based HIPAA Safe Harbor de-identification |
| **Amazon Redshift** | Gold layer analytical warehouse with column-level privileges |
| **EMR Serverless** | Large-scale batch aggregation (10-year readmission cohort) |
| **Amazon Kinesis** | Real-time streaming ingest path |
| **AWS Lambda** | Stream enrichment |
| **MWAA (Airflow)** | Pipeline orchestration and SLA monitoring |
| **IAM** | Role-based access control per service |

---

## HIPAA Safe Harbor De-identification

Implemented per 45 CFR §164.514(b) — Safe Harbor method. The following transformations are applied to the patients table before writing to the silver layer:

| Field | Transformation | Reason |
|---|---|---|
| `first`, `middle`, `last` | **Removed** | Direct name identifier |
| `ssn` | **Removed** | Direct identifier |
| `passport` | **Removed** | Direct identifier |
| `prefix`, `suffix` | **Removed** | Quasi-identifier |
| `lat`, `lon` | **Removed** | Precise geographic location |
| `id` (patient ID) | **Tokenized → UUID** | Breaks re-identification linkage |
| `birthdate` | **Age-bucketed → 5-year range** | Dates must be reduced under Safe Harbor |
| `deathdate` | **Removed** | Date field — Safe Harbor requirement |
| `zip` | **Truncated → 3-digit prefix** | Geographic quasi-identifier |
| `gender`, `race`, `ethnicity` | **Retained** | Permitted under Safe Harbor |
| `income`, `healthcare_expenses` | **Retained** | Not PHI |

---

## Data Profiling

**Tool:** [Sweetviz](https://github.com/fbdesignpro/sweetviz)

Before de-identification, a full HTML profile report is generated for `patients.csv` and `encounters.csv` to:
- Identify all PHI columns
- Document data types, missing values, and distributions
- Provide audit evidence that PHI was identified before removal

```bash
pip install sweetviz
python profiling/profile_patients.py
# Output: ~/Downloads/patients_profile.html
```

---

## Data Validation

**Tool:** [Great Expectations](https://greatexpectations.io/)

After de-identification, GE assertions run against the silver layer output to prove no PHI leaked:

- `ssn` column must not exist
- `first`, `last` columns must not exist
- `zip` length must be ≤ 3 characters
- `birthdate` column must not exist
- `id` must be UUID format (not original patient ID)
- `birth_year_bucket` must be divisible by 5

GE generates an HTML validation report that serves as the compliance audit artifact.

---

## Project Structure

```
aws-healthcare-datalake/
├── glue_jobs/
│   └── hipaa_deid_patients.py     # HIPAA Safe Harbor de-id ETL job
├── profiling/
│   └── profile_patients.py        # Sweetviz PHI discovery report
├── infrastructure/
│   └── setup.md                   # KMS, S3, IAM, CloudTrail setup notes
├── .gitignore
└── README.md
```

---

## Infrastructure Setup

### S3 Bucket
- **Name:** `clinicalflow-datalake-941141114246`
- **Encryption:** SSE-KMS with customer-managed key (`alias/clinicalflow-cmk`)
- **Versioning:** Enabled
- **Public access:** Blocked

### KMS
- **Alias:** `alias/clinicalflow-cmk`
- **Usage:** Encrypts all S3 objects; CloudTrail logs; Glue job outputs
- **Key policy:** Grants decrypt to Glue service principal and CloudTrail

### CloudTrail
- **Trail:** `clinicalflow-audit-trail`
- **Logs to:** `s3://clinicalflow-datalake-941141114246/cloudtrail-logs/`
- **Events:** Management + S3 data events (Read + Write)

### Glue
- **Crawler:** `clinicalflow-raw-crawler` → database `clinicalflow_raw` (17 tables)
- **ETL Role:** `AWSGlueServiceRole-clinicalflow` with S3 + KMS inline policies

---

## Running the Pipeline

```bash
# 1. Run Glue Crawler (catalog raw CSVs)
aws glue start-crawler --name clinicalflow-raw-crawler

# 2. Run HIPAA de-identification ETL
aws glue start-job-run --job-name clinicalflow-hipaa-deid-patients

# 3. Verify silver output
aws s3 ls s3://clinicalflow-datalake-941141114246/silver/patients/
```

---

## Portfolio Interview Story

> "Built a HIPAA-compliant healthcare data lakehouse on AWS processing 64K synthetic EHR patients across 16 clinical tables at 8+ GB scale. Implemented HIPAA Safe Harbor de-identification using AWS Glue PySpark — tokenizing patient IDs, age-bucketing dates, and truncating geographic identifiers — with full PHI audit trail via CloudTrail and Great Expectations validation reports. Architecture spans S3 raw/silver/gold layers with KMS customer-managed encryption, Glue Data Catalog, EMR Serverless for large-scale aggregation, Kinesis+Lambda for real-time streaming, and MWAA for orchestration."

---

## Status

| Phase | Status |
|---|---|
| Synthea data generation (64K patients) | ✅ Done |
| S3 bucket + KMS + CloudTrail | ✅ Done |
| Glue Crawler → Data Catalog (17 tables) | ✅ Done |
| HIPAA Safe Harbor de-id ETL (patients) | ✅ Done |
| Sweetviz PHI profiling | ✅ Done |
| Great Expectations validation | ⏳ Next |
| Iceberg silver tables | ⏳ |
| GDPR consent table | ⏳ |
| Redshift gold layer | ⏳ |
| EMR Serverless readmission aggregation | ⏳ |
| Kinesis + Lambda streaming path | ⏳ |
| MWAA orchestration | ⏳ |
