# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG earthquake_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS earthquake_catalog.bronze_schema.managed_etl_vol
# MAGIC COMMENT 'volume that stores the raw json data: data ingestion from the api to this volume';

# COMMAND ----------

import requests
import json
from datetime import date , timedelta

# COMMAND ----------

start_date = date.today() - timedelta(1)
end_date = date.today()
print(start_date)
print(end_date)
print(timedelta(1))

# COMMAND ----------

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

try:
    # Make the GET request to fetch data
    response = requests.get(url)

    # Check if the request was successful
    response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    data = response.json().get('features', [])

    if not data:
        print("No data returned for the specified date range.")
    else:
        # Specify the ADLS path
        file_path = f"/Volumes/earthquake_catalog/bronze_schema/managed_etl_vol/{start_date}_earthquake_data.json"

        # Save the JSON data
        json_data = json.dumps(data, indent=4)
        dbutils.fs.put(file_path, json_data, overwrite=True)
        print(f"Data successfully saved to {file_path}")
except requests.exceptions.RequestException as e:
    earthquake_catalog.bronze_schemarint(f"Error fetching data from API: {e}")


# COMMAND ----------

rows_from_api = len(data)

#dbutils.widgets.text("rows_from_api", "")
dbutils.jobs.taskValues.set(key="rows_from_api", value=rows_from_api)
print(rows_from_api)

# COMMAND ----------

print(len(data))
print(type(data))
print(data[0])
print(' \n')
x=list(data[0]['properties'].keys())
print(type(x))
print(x)
print(len(x))
for key in x:
  print(key,":", data[0]['properties'][key])

print(data[0]['geometry'])

