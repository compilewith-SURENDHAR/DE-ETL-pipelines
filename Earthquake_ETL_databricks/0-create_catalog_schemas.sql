-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS earthquake_catalog;

-- COMMAND ----------

DESCRIBE CATALOG EXTENDED earthquake_catalog;

-- COMMAND ----------

--schemas creation
USE CATALOG earthquake_catalog;

--bronze schema
CREATE SCHEMA IF NOT EXISTS bronze_schema
COMMENT 'Stores raw ingested earthquake data';

--volume creation in the bronze schema
CREATE VOLUME IF NOT EXISTS earthquake_catalog.bronze_schema.managed_etl_vol
COMMENT 'volume that stores the raw json data: data ingestion from the api to this volume';

--silver schema
CREATE SCHEMA IF NOT EXISTS silver_schema
COMMENT 'Stores cleaned and structured earthquake data from the Bronze layer';

--gold schema
CREATE SCHEMA IF NOT EXISTS gold_schema
COMMENT 'Stores aggregated and summarized earthquake data from the Silver layer';


-- COMMAND ----------

--table for the pipeline logs
use catalog earthquake_catalog;

CREATE TABLE IF NOT EXISTS earthquake_catalog.bronze_schema.pipeline_log (
    log_date DATE,
    rows_from_api LONG,
    rows_added_in_bronze LONG,
    total_rows_in_bronze LONG,
    rows_added_in_silver LONG,
    total_rows_in_silver LONG,
    status STRING,
    message STRING,
    run_timestamp DATE 
);

-- COMMAND ----------

--didn’t specify a catalog-level storage path,
--so the default metastore storage root will be used
