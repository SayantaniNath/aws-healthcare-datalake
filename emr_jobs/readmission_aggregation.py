import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("readmission-aggregation") \
    .getOrCreate()

BUCKET = "clinicalflow-datalake-941141114246"

# Read encounters (has original Synthea patient IDs)
df_encounters = spark.read.option("header", "true") \
    .csv(f"s3://{BUCKET}/raw/synthea/csv/encounters.csv")

# Read raw patients (original IDs match encounters.PATIENT)
# We use raw here because de-identified silver has tokenized IDs that won't join
df_patients = spark.read.option("header", "true") \
    .csv(f"s3://{BUCKET}/raw/synthea/csv/patients.csv") \
    .select("Id", "GENDER", "RACE", "ETHNICITY", "STATE", "BIRTHDATE") \
    .withColumn("birth_year_bucket",
        (F.year(F.col("BIRTHDATE").cast("date")) / 5).cast("int") * 5) \
    .drop("BIRTHDATE")

# Parse timestamps
df_encounters = df_encounters \
    .withColumn("START", F.expr("try_cast(START as timestamp)")) \
    .withColumn("STOP",  F.expr("try_cast(STOP  as timestamp)"))

# Flag 30-day readmissions using window function
w = Window.partitionBy("PATIENT").orderBy("START")
df_encounters = df_encounters \
    .withColumn("prev_stop", F.lag("STOP").over(w)) \
    .withColumn("days_since_last", F.datediff(F.col("START"), F.col("prev_stop"))) \
    .withColumn("is_readmission", F.when(F.col("days_since_last") <= 30, 1).otherwise(0)) \
    .withColumnRenamed("Id", "encounter_id")

# Join on original patient ID
df_joined = df_encounters.join(
    df_patients,
    df_encounters["PATIENT"] == df_patients["Id"],
    "inner"
)

# Aggregate by demographic group
df_summary = df_joined.groupBy("GENDER", "RACE", "ETHNICITY", "STATE", "birth_year_bucket") \
    .agg(
        F.count("encounter_id").alias("total_encounters"),
        F.sum("is_readmission").alias("readmissions_30d")
    ) \
    .withColumnRenamed("GENDER", "gender") \
    .withColumnRenamed("RACE", "race") \
    .withColumnRenamed("ETHNICITY", "ethnicity") \
    .withColumnRenamed("STATE", "state") \
    .withColumn(
        "readmission_rate",
        F.round((F.col("readmissions_30d") / F.col("total_encounters")) * 100, 2)
    )

df_summary.write \
    .mode("overwrite") \
    .parquet(f"s3://{BUCKET}/gold/readmission_summary/")

print(f"Done. Rows written: {df_summary.count()}")

spark.stop()
