# Databricks notebook source

from datetime import date, timedelta
from pyspark.sql.functions import lit
from pyspark.sql.functions import col


# COMMAND ----------


today = date.today() - timedelta(1)
file_path = f"/Volumes/earthquake_catalog/bronze_schema/managed_etl_vol/{today}_earthquake_data.json"

# Read JSON into a DataFrame
df = spark.read.option("multiline", "true").json(file_path)
df = df.withColumn("ingestion_date", lit(today))
# Flatten nested fields
flat_df = df.select(
    col("id"),
    col("geometry").alias("geometry"),
    col("properties.*"),  # explode all subfields
    col("ingestion_date")
)

flat_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("earthquake_catalog.bronze_schema.earthquake_bronze")
#earthquake_catalog.bronze_schema

# COMMAND ----------

#for pipeline logs
rows_added_in_bronze = flat_df.count()
total_rows_in_bronze = spark.table("earthquake_catalog.bronze_schema.earthquake_bronze").count()

dbutils.jobs.taskValues.set(key="rows_added_in_bronze", value=rows_added_in_bronze)
dbutils.jobs.taskValues.set(key="total_rows_in_bronze", value=total_rows_in_bronze)
print(rows_added_in_bronze)
print(total_rows_in_bronze)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM earthquake_catalog.bronze_schema.earthquake_bronze LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN earthquake_catalog.bronze_schema.earthquake_bronze;