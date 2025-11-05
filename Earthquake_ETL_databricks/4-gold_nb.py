# Databricks notebook source
from pyspark.sql.functions import avg, count, max, to_date, col
from datetime import date, datetime
from pyspark.sql import Row


# COMMAND ----------

silver_df = spark.table("earthquake_catalog.silver_schema.earthquake_silver")


# COMMAND ----------


gold_df = (
    silver_df
    .withColumn("ingestion_date", to_date((col("time") / 1000).cast("timestamp")))  # convert epoch to date
    .groupBy("ingestion_date")
    .agg(
        count("*").alias("total_quakes"),
        avg("magnitude").alias("avg_magnitude"),
        max("magnitude").alias("max_magnitude")
    )
)
gold_df.write.format("delta").mode("append").saveAsTable("earthquake_catalog.gold_schema.earthquake_gold")

# COMMAND ----------

top_regions_df = (
    silver_df.groupBy("place")
    .agg(
        avg("magnitude").alias("avg_magnitude"),
        count("*").alias("quake_count")
    )
    .orderBy(col("avg_magnitude").desc())
)

top_regions_df.write.format("delta").mode("overwrite").saveAsTable(
    "earthquake_catalog.gold_schema.earthquake_gold_top_regions"
)

# COMMAND ----------

# ✅ Initialize defaults
rows_from_api = 0
rows_added_in_bronze = 0
total_rows_in_bronze = 0
rows_added_in_silver = 0
total_rows_in_silver = 0
status = "FAILED"
message = ""
cur_time = datetime.now()

# COMMAND ----------

try:
    print("hello")
    # Collect values from earlier notebooks
    rows_from_api = dbutils.jobs.taskValues.get("1-Data_Ingestion", "rows_from_api", debugValue=0)
    print("hi")
    rows_added_in_bronze = dbutils.jobs.taskValues.get("2-Bronze_task", "rows_added_in_bronze", debugValue=0)
    total_rows_in_bronze = dbutils.jobs.taskValues.get("2-Bronze_task", "total_rows_in_bronze", debugValue=0)
    rows_added_in_silver = dbutils.jobs.taskValues.get("3-silver_task", "rows_added_in_silver", debugValue=0)
    total_rows_in_silver = dbutils.jobs.taskValues.get("3-silver_task", "total_rows_in_silver", debugValue=0)

    status = "SUCCESS"
    cur_time = datetime.now()
    message = "All tasks completed successfully"
except Exception as e:
    status = "FAILED"
    message = f"Error: {str(e)}"


# COMMAND ----------

print("rows_from_api:", rows_from_api)
print("rows_added_in_bronze:", rows_added_in_bronze)
print("total_rows_in_bronze:", total_rows_in_bronze)
print("rows_added_in_silver:", rows_added_in_silver)
print("total_rows_in_silver:", total_rows_in_silver)


# COMMAND ----------

log_entry = [
    Row(
        run_date = str(date.today()),
        rows_from_api = rows_from_api,
        rows_added_in_bronze = rows_added_in_bronze,
        total_rows_in_bronze = total_rows_in_bronze,
        rows_added_in_silver = rows_added_in_silver,
        total_rows_in_silver = total_rows_in_silver,
        status = status,
        message = message,
    )
]

df_log = spark.createDataFrame(log_entry)
df_log.write.option("mergeSchema", "true").mode("append").saveAsTable("earthquake_catalog.bronze_schema.pipeline_log")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from earthquake_catalog.bronze_schema.pipeline_log;
# MAGIC --show columns in earthquake_catalog.bronze_schema.pipeline_log;
# MAGIC --select count(*) from earthquake_catalog.bronze_schema.earthquake_bronze;
# MAGIC --select count(*) from earthquake_catalog.silver_schema.earthquake_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN earthquake_catalog.bronze_schema.pipeline_log;

# COMMAND ----------


bronze_df_x = spark.read.table("earthquake_catalog.bronze_schema.pipeline_log")
bronze_df_x.printSchema()