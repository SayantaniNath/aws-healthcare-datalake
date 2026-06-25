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
import uuid
from functools import reduce
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# --- Glue boilerplate ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Read raw patients from Glue Data Catalog ---
# resolveChoice fixes union struct types caused by malformed rows in the source CSV
patients = glueContext.create_dynamic_frame.from_catalog(
    database="clinicalflow_raw",
    table_name="patients_csv"
).resolveChoice(specs=[
    ("zip",                  "cast:string"),
    ("healthcare_expenses",  "cast:double"),
    ("healthcare_coverage",  "cast:double"),
    ("income",               "cast:long"),
    ("fips",                 "cast:long"),
]).toDF()

# --- Quarantine: detect and isolate malformed rows ---
# Malformed rows are two source records concatenated onto one line (missing newline
# in Synthea output). Glue auto-named the overflow fields col28 through col35.
# Any row with a non-null value in a col* column is malformed.
overflow_cols = [c for c in patients.columns if c.startswith('col')]

if overflow_cols:
    is_malformed = reduce(
        lambda a, b: a | b,
        [F.col(c).isNotNull() for c in overflow_cols]
    )
    quarantine_df = patients.filter(is_malformed)
    clean_df = patients.filter(~is_malformed)
else:
    quarantine_df = spark.createDataFrame([], patients.schema)
    clean_df = patients

# Write malformed rows to quarantine with audit metadata
if quarantine_df.count() > 0:
    quarantine_df \
        .withColumn('quarantine_reason', F.lit('malformed_row_extra_fields')) \
        .withColumn('quarantine_ts', F.current_timestamp()) \
        .write \
        .mode("append") \
        .parquet("s3://clinicalflow-datalake-941141114246/quarantine/patients/")

# --- HIPAA Safe Harbor de-identification (clean rows only) ---

# UDF: replace patient ID with random UUID (breaks re-identification linkage)
generate_uuid = F.udf(lambda x: str(uuid.uuid4()), 'string')

# Drop direct identifiers (Safe Harbor § 164.514(b)(2))
cols_to_drop = [
    'first', 'middle', 'last', 'maiden',
    'ssn', 'passport', 'drivers',
    'prefix', 'suffix',
    'address',
    'lat', 'lon',
]
patients_deid = clean_df.drop(*cols_to_drop)

# Tokenize patient ID
patients_deid = patients_deid.withColumn('id', generate_uuid('id'))

# Truncate zip to 3-digit prefix (Safe Harbor geographic standard)
patients_deid = patients_deid.withColumn('zip', F.col('zip').cast('string').substr(1, 3))

# Age-bucket birthdate into 5-year ranges (e.g. 1962 → 1960)
patients_deid = patients_deid.withColumn(
    'birth_year_bucket',
    (F.year(F.col('birthdate')) / 5).cast('int') * 5
).drop('birthdate')

# Remove deathdate (Safe Harbor requires date removal)
patients_deid = patients_deid.drop('deathdate')

# Drop overflow columns (now empty in clean_df, but drop for clean schema)
patients_deid = patients_deid.drop(*overflow_cols)

# --- Write de-identified output to silver layer as Parquet ---
patients_deid.write \
    .mode("overwrite") \
    .parquet("s3://clinicalflow-datalake-941141114246/silver/patients/")

job.commit()
