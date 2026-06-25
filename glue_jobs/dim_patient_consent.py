import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_patients = spark.read.format("iceberg").load("glue_catalog.clinicalflow_silver.patients").select("id")

df_consent = df_patients \
    .withColumn("consent_given", F.lit(True)) \
    .withColumn("consent_date", F.lit("2023-01-01").cast(DateType())) \
    .withColumn("erasure_requested", F.when(F.rand() < 0.05, True).otherwise(False)) \
    .withColumn("erasure_date", F.when(F.col("erasure_requested") == True,
                                       F.lit("2024-06-01").cast(DateType()))
                                 .otherwise(F.lit(None).cast(DateType()))) \
    .withColumn("last_updated", F.current_timestamp())

df_consent.writeTo("glue_catalog.clinicalflow_silver.dim_patient_consent") \
    .tableProperty("format-version", "2") \
    .tableProperty("write.target-file-size-bytes", "134217728") \
    .createOrReplace()

job.commit()
