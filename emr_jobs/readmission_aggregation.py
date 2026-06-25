import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("readmission-aggregation") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://clinicalflow-datalake-941141114246/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

df_encounters = spark.read.option("header", "true").csv("s3://clinicalflow-datalake-941141114246/raw/synthea/csv/encounters.csv")
df_patients = spark.read.format("iceberg").load("glue_catalog.clinicalflow_silver.patients")

df_encounters = df_encounters.withColumn("START", F.expr("try_cast(START as timestamp)")) \
   .withColumn("STOP", F.expr("try_cast(STOP as timestamp)"))

w = Window.partitionBy("PATIENT").orderBy("START")
df_encounters = df_encounters.withColumn(
    "prev_stop", F.lag("STOP").over(w)
).withColumn(
    "days_since_last", F.datediff(F.col("START"), F.col("prev_stop"))
).withColumn(
    "is_readmission", F.when(F.col("days_since_last") <= 30, 1).otherwise(0)
)

df_encounters = df_encounters.withColumnRenamed("Id", "encounter_id")
df_joined = df_encounters.join(
    df_patients.select("id", "gender", "race", "ethnicity", "state", "birth_year_bucket"),
    df_encounters["PATIENT"] == df_patients["id"],
    "inner"
)
df_summary = df_joined.groupBy("gender", "race", "ethnicity", "state", "birth_year_bucket") \
    .agg(
        F.count("encounter_id").alias("total_encounters"),
        F.sum("is_readmission").alias("readmissions_30d")
    ) \
    .withColumn(
        "readmission_rate",
        F.round((F.col("readmissions_30d") / F.col("total_encounters")) * 100, 2)
    )
df_summary.write \
    .mode("overwrite") \
    .parquet("s3://clinicalflow-datalake-941141114246/gold/readmission_summary/")

spark.stop()