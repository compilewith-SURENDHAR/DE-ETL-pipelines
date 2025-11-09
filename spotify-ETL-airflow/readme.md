# 🎵 Spotify ELT Pipeline using Airflow and MySQL

This project automates the process of fetching and storing Spotify’s latest album releases using a modern ELT (Extract–Load–Transform) architecture. The pipeline is built with Python, Airflow, and MySQL, making it modular, scalable, and easy to schedule.

## 🔧 Workflow Overview

Extract – The pipeline retrieves the latest album data from the Spotify Web API using an access token generated via Spotify’s Client Credentials flow.

Load – The extracted data is inserted into a MySQL database (spotifyetl) in a structured format, creating or updating album records in the new_releases table.

(Transform – Future scope) Data transformations or aggregations can be added later for analytics or reporting.

## 🧩 Components

auth.py – Handles Spotify API authentication by generating an access token using client credentials.

extract.py – (Not shown here, but assumed) Responsible for fetching the latest album data from Spotify using the API token.

load.py – Connects to MySQL and loads the fetched album data into the new_releases table.

airflow_dag.py – Defines an Airflow DAG that runs the pipeline automatically every 5 minutes using a BashOperator.
