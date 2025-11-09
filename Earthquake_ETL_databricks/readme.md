# ETL PIPELINE IN DATABRICKS

- Data pipeline in Databricks, using real-time earthquake data from the USGS Earthquake API.
- The pipeline is designed using the Medallion Architecture (Bronze–Silver–Gold), leverages Delta Lake, and is fully automated using Databricks Jobs & Workflows
- Concepts and tools used in databricks - ***Catalogs, Detla tables, Volumes, Jobs & workflows, Dbutils, pyspark***

  ## ARCHITECTURE
<img width="1269" height="512" alt="Screenshot 2025-11-09 232402" src="https://github.com/user-attachments/assets/017c3bdc-ac98-480c-b3c8-7f5680638fc8" />

  ## OVERVIEW
  1. Created a catalog and created schema for bronzr, silver and gold layer.
  2. Fetch live earthquake data daily from a public API and ingest raw JSON data into a Databricks Volume.
  3. This Json data is stored as delta table in the Bronze Schema.
  4. The data from the Bronze schema is transformed using Pyspark and it is stored in the Silver schema.
  5. used dbutils to pass parameters from one notebook to another.
  6. Then the aggregation is done on the transformed data and stored in gold layer which is ready for analytics and business purposes.
  7. this pipeline is automated and monitored with jobs in databricks.
  8. Also created a delta table that stores the logging of the pipeline execution to monitor and ensure data validation.

###   pipeline job run:
<img width="1663" height="681" alt="Screenshot 2025-11-09 232538" src="https://github.com/user-attachments/assets/a0bbd7c2-1133-45c4-a442-67c7be8a086c" />
