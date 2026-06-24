"""
Glue ETL Job: HIPAA Safe Harbor De-identification — patients table
Reads raw Synthea patients CSV from Glue Data Catalog (clinicalflow_raw),
applies Safe Harbor transformations, writes de-identified parquet to silver/.

HIPAA Safe Harbor transformations applied:
  - Removed: first, middle, last, ssn, passport, prefix, suffix, lat, lon
  - Tokenized: id → random UUID (breaks linkability)
  - Truncated: zip → first 3 digits only
  - Age-bucketed: birthdate → 5-year birth_year_bucket (e.g. 1962 → 1960)
  - Removed: deathdate
"""

import sys
import uuid
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
patients = glueContext.create_dynamic_frame.from_catalog(
    database="clinicalflow_raw",
    table_name="patients_csv"
).toDF()

# --- HIPAA Safe Harbor de-identification ---

# UDF: replace patient ID with random UUID (breaks re-identification linkage)
generate_uuid = F.udf(lambda x: str(uuid.uuid4()), 'string')

# Drop direct identifiers
cols_to_drop = ['first', 'middle', 'last', 'ssn', 'passport', 'prefix', 'suffix', 'lat', 'lon']
patients_deid = patients.drop(*cols_to_drop)

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

# --- Write de-identified output to silver layer as Parquet ---
patients_deid.write \
    .mode("overwrite") \
    .parquet("s3://clinicalflow-datalake-941141114246/silver/patients/")

job.commit()
