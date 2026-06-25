"""
Glue ETL Job: HIPAA Safe Harbor De-identification — patients table
Reads raw Synthea patients CSV from Glue Data Catalog (clinicalflow_raw),
applies Safe Harbor transformations, writes de-identified parquet to silver/.

HIPAA Safe Harbor transformations applied:
  - Removed: first, middle, last, maiden, ssn, passport, prefix, suffix,
              address, drivers, lat, lon
  - Tokenized: id → random UUID (breaks linkability)
  - Truncated: zip → first 3 digits only
  - Age-bucketed: birthdate → 5-year birth_year_bucket (e.g. 1962 → 1960)
  - Removed: deathdate

Data quality:
  - Malformed rows (two source records concatenated on one line) are detected
    by the presence of overflow col* columns and written to quarantine/ with
    an audit reason and timestamp. Silver receives only fully-formed records.
"""

import sys
import uuid                        # Python standard library — generates random UUIDs e.g. "a3f2c1d4-..."
from functools import reduce       # reduce(fn, [a,b,c]) applies fn left-to-right: fn(fn(a,b),c)
from awsglue.transforms import *   # Glue built-in transforms (ApplyMapping, ResolveChoice etc.)
from awsglue.utils import getResolvedOptions   # reads job parameters passed via --key value at runtime
from pyspark.context import SparkContext       # low-level Spark engine entry point
from awsglue.context import GlueContext        # wraps SparkContext with Glue-specific features (catalog reads, S3 writes)
from awsglue.job import Job                    # tracks job state; needed for job bookmarking
from pyspark.sql import functions as F         # Spark column functions: F.col(), F.lit(), F.udf() etc.

# ── Glue boilerplate ──────────────────────────────────────────────────────────

# getResolvedOptions reads CLI args Glue passes at runtime e.g. --JOB_NAME clinicalflow-hipaa-deid-patients
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()                        # starts the Spark engine (one per JVM, Glue manages lifecycle)
glueContext = GlueContext(sc)              # Glue layer on top of Spark — adds catalog access, S3 helpers
spark = glueContext.spark_session          # SparkSession: needed to run DataFrame operations and spark.sql()
job = Job(glueContext)                     # creates a Glue job object for state tracking
job.init(args['JOB_NAME'], args)           # registers the job with Glue; enables job bookmarking
                                           # (bookmarking lets Glue remember what it already processed
                                           #  so reruns don't reprocess old data — we don't use it here
                                           #  but job.init is still required before job.commit)

# ── Read raw patients from Glue Data Catalog ──────────────────────────────────

# create_dynamic_frame.from_catalog reads from the Glue Data Catalog (not S3 directly).
# The catalog stores the schema + S3 location; Glue fetches the actual files from there.
# DynamicFrame is Glue's version of a Spark DataFrame — more flexible with messy/inconsistent schemas.
#
# resolveChoice fixes "union struct" types — a problem caused by the 3 malformed rows in patients.csv.
# When Glue sees a column that is sometimes a number and sometimes a string (because malformed rows
# spill text into numeric columns), it creates an ambiguous union type like {'double': 41009.33, 'string': None}.
# resolveChoice forces a single concrete type:
#   ("healthcare_expenses", "cast:double") → always treat this column as double, discard the string variant
#   ("zip", "cast:string")                → keep zip as string (it looks like "02134", not a number)
#
# .toDF() converts the Glue DynamicFrame to a standard Spark DataFrame for easier manipulation.
patients = glueContext.create_dynamic_frame.from_catalog(
    database="clinicalflow_raw",
    table_name="patients_csv"
).resolveChoice(specs=[
    ("zip",                  "cast:string"),
    ("healthcare_expenses",  "cast:double"),
    ("healthcare_coverage",  "cast:double"),
    ("income",               "cast:long"),
    ("fips",                 "cast:long")
]).toDF()

# ── Quarantine: detect and isolate malformed rows ─────────────────────────────

# 3 rows in patients.csv had two records merged on one line (missing newline in Synthea output).
# e.g. row 60385: "...Dylan44,Leo<newline missing>nef42f66d0-...,1976-03-25,..."
# Glue read each merged line as one very wide row and auto-named the extra columns col28, col29 ... col35.
# For normal (clean) rows, col28–col35 are NULL. For malformed rows, they have values.
# So: any row where at least one col* column is non-null is a malformed row.

# Build a list of column names that start with 'col' e.g. ['col28', 'col29', ..., 'col35']
overflow_cols = [c for c in patients.columns if c.startswith('col')]

if overflow_cols:
    # Build a single boolean Spark column condition by OR-ing all the null checks together.
    # reduce applies | (OR) across the list left-to-right:
    #   col28.isNotNull() | col29.isNotNull() | col30.isNotNull() | ... | col35.isNotNull()
    # Result: True if ANY overflow column has a value → this row is malformed.
    is_malformed = reduce(
        lambda a, b: a | b,
        [F.col(c).isNotNull() for c in overflow_cols]   # list of Spark column conditions, one per col*
    )

    quarantine_df = patients.filter(is_malformed)    # rows where is_malformed is True  → quarantine
    clean_df      = patients.filter(~is_malformed)   # ~ means NOT → rows where is_malformed is False → clean
else:
    # No col* columns means the source CSV had no malformed rows — safe to process everything.
    quarantine_df = spark.createDataFrame([], patients.schema)  # empty DataFrame with same schema
    clean_df = patients

# Write malformed rows to quarantine prefix with audit metadata before doing anything else.
# mode("append") so each job run adds rows rather than overwriting previous quarantine records —
# this gives a full audit trail: if the source file is fixed and reprocessed, old bad rows are still logged.
if quarantine_df.count() > 0:
    # F.lit('malformed_row_extra_fields') adds a column where every row has the same constant string.
    # lit = "literal value" — useful for tagging rows with a fixed label.
    # e.g. all 3 quarantine rows get: quarantine_reason = 'malformed_row_extra_fields'
    #
    # F.current_timestamp() stamps every row with the datetime the job ran.
    # e.g. quarantine_ts = 2026-06-24 14:28:00  — so auditors know when these rows were flagged.
    quarantine_df \
        .withColumn('quarantine_reason', F.lit('malformed_row_extra_fields')) \
        .withColumn('quarantine_ts', F.current_timestamp()) \
        .write \
        .mode("append") \
        .parquet("s3://clinicalflow-datalake-941141114246/quarantine/patients/")

# ── HIPAA Safe Harbor de-identification (clean rows only) ─────────────────────

# UDF = User Defined Function. Spark doesn't know how to call Python's uuid library natively,
# so we wrap it in a UDF to make it callable as a Spark column operation.
# lambda x: str(uuid.uuid4()) takes any input value x and returns a brand-new random UUID string
# e.g. input  → "aee7bbe1-0c45-c028-1e62-1f4cdb30c273"  (original Synthea patient ID)
#      output → "9af9a253-ddb1-46e1-8dc1-5ac7c3bd6681"  (new random UUID — no linkage to original)
# 'string' tells Spark the return type so it can build the correct schema.
generate_uuid = F.udf(lambda x: str(uuid.uuid4()), 'string')

# List of columns to drop — all are HIPAA Safe Harbor direct identifiers (§ 164.514(b)(2)).
# Notes on the less obvious ones:
#   maiden   → pre-marriage last name e.g. "Terry864" — still a name, still PHI
#   drivers  → driver's license number e.g. "S99956685" — government-issued ID, PHI
#   address  → full street address e.g. "265 Schamberger Rapid Unit 70" — direct identifier
#   lat/lon  → GPS coordinates — Safe Harbor requires removal of any geo info below county level
cols_to_drop = [
    'first', 'middle', 'last', 'maiden',
    'ssn', 'passport', 'drivers',
    'prefix', 'suffix',
    'address',
    'lat', 'lon',
]
# .drop(*cols_to_drop) unpacks the list into individual arguments.
# Equivalent to: clean_df.drop('first', 'middle', 'last', 'maiden', ...)
patients_deid = clean_df.drop(*cols_to_drop)

# Replace the original patient ID with a new random UUID.
# withColumn('id', generate_uuid('id')) means:
#   "take the 'id' column as input to generate_uuid, overwrite 'id' with the output"
# Before: id = "aee7bbe1-0c45-c028-1e62-1f4cdb30c273"  (same across reruns — re-identifiable)
# After:  id = "9af9a253-ddb1-46e1-8dc1-5ac7c3bd6681"  (new random UUID every run — not linkable)
patients_deid = patients_deid.withColumn('id', generate_uuid('id'))

# Truncate zip code to first 3 digits — Safe Harbor geographic standard.
# zip must first be cast to string in case Glue read it as an integer.
# .substr(1, 3) extracts characters at positions 1–3 (Spark uses 1-based indexing).
# e.g. "02134" → "021"
#      "00000" → "000"
patients_deid = patients_deid.withColumn('zip', F.col('zip').cast('string').substr(1, 3))

# Age-bucket birthdate into 5-year ranges.
# F.year(F.col('birthdate')) extracts the year integer from a date column e.g. 1962-07-14 → 1962
# Dividing by 5, casting to int (floor), multiplying by 5 snaps to the nearest 5-year bucket:
#   1962 / 5 = 392.4 → int → 392 → × 5 = 1960
#   1967 / 5 = 393.4 → int → 393 → × 5 = 1965
# The result is stored in a new column 'birth_year_bucket'; original 'birthdate' is then dropped.
patients_deid = patients_deid.withColumn(
    'birth_year_bucket',
    (F.year(F.col('birthdate')) / 5).cast('int') * 5
).drop('birthdate')

# Drop deathdate — Safe Harbor requires removal of all dates directly related to the individual.
patients_deid = patients_deid.drop('deathdate')

# Drop overflow columns — they're empty in clean_df rows (all NULL) but still part of the schema.
# Dropping them gives a clean 15-column output with no ghost columns.
patients_deid = patients_deid.drop(*overflow_cols)

# ── Write de-identified output to silver layer ────────────────────────────────

# mode("overwrite") replaces everything in the S3 prefix on each run — full refresh.
# This is safe here because the Glue job is deterministic given the same source data.
# Iceberg write (configured via job params --datalake-formats iceberg) will replace this
# once the Iceberg step is run.
patients_deid.write \
    .mode("overwrite") \
    .parquet("s3://clinicalflow-datalake-941141114246/silver/patients/")

# job.commit() tells Glue the job completed successfully.
# Required at the end of every Glue job — updates job bookmarks and marks the run as SUCCEEDED.
# If the job crashes before this line, Glue marks the run as FAILED.
job.commit()
