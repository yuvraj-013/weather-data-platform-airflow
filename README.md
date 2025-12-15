\#Project Overview



This project demonstrates a production-style data engineering pipeline that ingests real-time weather data from a public API, validates data quality, and loads clean data into a PostgreSQL data warehouse — all orchestrated using Apache Airflow and Docker.



The pipeline is fully automated, reproducible, and follows industry best practices such as data quality checks, orchestration, containerization, and clean version control.



\#Architecture Overview



Open-Meteo API

&nbsp;     ↓

Extract (Python)

&nbsp;     ↓

Raw Data Layer (JSON files)

&nbsp;     ↓

Data Quality Validation (Great Expectations)

&nbsp;     ↓

PostgreSQL Data Warehouse

&nbsp;     ↓

Orchestration \& Monitoring (Airflow UI)





\#Pipeline Workflow



The pipeline consists of three sequential tasks, orchestrated using an Airflow DAG:



1\. Extract Weather Data



Fetches real-time weather data from Open-Meteo API



Stores timestamped raw JSON files in a raw data layer



Ensures reproducibility and traceability



Script: scripts/extract\_weather.py



2\. Data Quality Validation



Validates raw data using Great Expectations



Enforces:



Temperature range checks



Non-null constraints



Schema consistency



Stops the pipeline immediately if validation fails (fail-fast design)



Script: scripts/validate\_weather.py



3\. Load to PostgreSQL Warehouse



Loads validated data into a structured fact table



Handles timestamp parsing and schema alignment



Enables downstream SQL analytics



Script: scripts/load\_to\_postgres.py



\# Data Model



Table: weather\_fact



Column	Description

id	Auto-increment primary key

city	City name

latitude	Geographic latitude

longitude	Geographic longitude

temperature\_celsius	Temperature in °C

windspeed	Wind speed

winddirection	Wind direction

weathercode	Weather condition code

timestamp\_utc	Observation timestamp

ingested\_at\_utc	Ingestion timestamp



\# Orchestration with Airflow



DAG Name: weather\_etl\_pipeline



Schedule: Daily



Execution Order:



extract\_weather → validate\_weather → load\_to\_postgres





\#Features:



Task dependencies



Retries on failure



Centralized logging



Visual monitoring via Airflow UI





\# Airflow UI: http://localhost:8080



\# Dockerized Setup



All services run locally using Docker Compose:



Airflow Webserver



Airflow Scheduler



PostgreSQL Database



Python dependencies are managed via \_PIP\_ADDITIONAL\_REQUIREMENTS to ensure consistent runtime environments across tasks.

