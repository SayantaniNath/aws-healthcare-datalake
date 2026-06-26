from pyspark.sql import SparkSession

BUCKET = "clinicalflow-datalake-941141114246"

spark = SparkSession.builder \
    .appName("readmission-anomaly-detection") \
    .getOrCreate()

# Load your gold table
df = spark.read.parquet(f"s3://{BUCKET}/gold/readmission_summary/")
df.createOrReplaceTempView("readmission_summary")

# Flag rows where readmission_rate is unusually high or low
df_result = spark.sql("""
    SELECT *,
        ROUND(
            (readmission_rate - AVG(readmission_rate) OVER ()) /
            NULLIF(STDDEV(readmission_rate) OVER (), 0)
        , 3) AS z_score,
        ABS(
            (readmission_rate - AVG(readmission_rate) OVER ()) /
            NULLIF(STDDEV(readmission_rate) OVER (), 0)
        ) > 2.0 AS is_anomaly
    FROM readmission_summary
""")

# Write results
df_result.write \
    .mode("overwrite") \
    .parquet(f"s3://{BUCKET}/gold/readmission_anomalies/")

print("Done. Anomalies flagged:")
df_result.filter("is_anomaly = true").show()

spark.stop()
