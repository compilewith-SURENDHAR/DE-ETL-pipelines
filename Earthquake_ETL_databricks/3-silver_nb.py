# Databricks notebook source
#imports
from pyspark.sql.functions import col

# COMMAND ----------

bronze_df = spark.read.table("earthquake_catalog.bronze_schema.earthquake_bronze")

# COMMAND ----------

bronze_df.printSchema()


# COMMAND ----------

silver_df = bronze_df.select(
    col("id").alias("id"),
    col("geometry.coordinates")[0].alias("longitude"),
    col("geometry.coordinates")[1].alias("latitude"),
    col("geometry.coordinates")[2].alias("depth"),
    col("geometry.type").alias("geometry_type"),
    col("properties.alert").alias("alert"),
    col("properties.cdi").alias("cdi"),
    col("properties.code").alias("code"),
    col("properties.detail").alias("detail"),
    col("properties.dmin").alias("dmin"),
    col("properties.felt").alias("felt"),
    col("properties.gap").alias("gap"),
    col("properties.ids").alias("ids"),
    col("properties.mag").alias("magnitude"),
    col("properties.magType").alias("magType"),
    col("properties.mmi").alias("mmi"),
    col("properties.net").alias("net"),
    col("properties.nst").alias("nst"),
    col("properties.place").alias("place"),
    col("properties.rms").alias("rms"),
    col("properties.sig").alias("sig"),
    col("properties.status").alias("status"),
    col("properties.sources").alias("sources"),
    col("properties.time").alias("time"),    
    col("properties.title").alias("title"),
    col("properties.tsunami").alias("tsunami"),
    col("properties.type").alias("type"),
    col("properties.updated").alias("updated"),
    col("properties.tz").alias("tz"),
    col("ingestion_date").alias("ingestion_date")
)

# COMMAND ----------

silver_df.count()

#simple data cleaning
silver_df = silver_df.dropna(subset=["id", "magnitude"])
silver_df = silver_df.dropDuplicates(["id"])

silver_df.count()

# COMMAND ----------

silver_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("earthquake_catalog.silver_schema.earthquake_silver")


# COMMAND ----------

rows_added_in_silver = silver_df.count()
total_rows_in_silver = spark.table("earthquake_catalog.silver_schema.earthquake_silver").count()

dbutils.jobs.taskValues.set(key="rows_added_in_silver", value=rows_added_in_silver)
dbutils.jobs.taskValues.set(key="total_rows_in_silver", value=total_rows_in_silver)
print("rows_added_in_silver", rows_added_in_silver)
print("total_rows_in_silver", total_rows_in_silver)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN earthquake_catalog.silver_schema.earthquake_silver;