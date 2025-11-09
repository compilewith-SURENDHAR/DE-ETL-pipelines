# AWS-DE-PROJECT
-> A simple data ETL pipeline in the Azure cloud ecosystem, leveraging the Azure Data engineering tools. <br>
-> The tools includes - Azure Data Factory, Azure Data Lake Gen2, Azure Databricks, Azure Synapse Analytics, Apache Spark <br>

## ARCHITECTURE
<img width="1306" height="551" alt="Screenshot 2025-11-09 193519" src="https://github.com/user-attachments/assets/d81f2ba8-e308-4a53-8a77-6005a361b369" />

## OVERVIEW
1. Data Ingestion:
   - The source uses an HTTP connection to pull data directly from github <a href="https://github.com/compilewith-SURENDHAR/DE-ETL-pipelines/tree/main/azure-DE-Project-learning/Data" target="_blank">Link</a> <br>
   - We use the Azure Data Factory to create a pipeline, that fetches the data(multiple files) from the souruce dynamically. <br>
   - the data moves from the source to the sink Azure data lake gen2 ). We create the lnked service in ADF for the source and sink.
  
  2. Data Storage:
     - We use the Azure Data Lake Gen2 for teh storage.
     - created 3 containers( Bronze, Silver, Gold)
     - make sure we select the hierarchical namespace.

  3. Data Transformation:
     - Used Azure databricks to perform the transformation on the data.
     - Spark does the tranformation job.

  4. Serving layer ( Golder layer ):
      - Azure Synapse Analytics serves as the serving layer (Gold Layer) .
      -  Synapse takes the cleaned, transformed data residing in the Silver layer of the Data Lake and makes it ready for use by data analysis ( PowerBI )
      -  Creation of views 
