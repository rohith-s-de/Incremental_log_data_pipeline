# Incremental Log Data Pipeline (PySpark + Airflow)

## Overview

This project is an end-to-end data pipeline built to process log files incrementally. Instead of processing all files every time, the pipeline only picks up new files that haven’t been processed before.

It uses PySpark for data processing and Apache Airflow for scheduling. The pipeline also joins log data with user information from a MySQL database to generate useful insights.

---

## Tech Stack

- Python  
- PySpark  
- Apache Airflow  
- MySQL  
- Linux  

---

## How the Pipeline Works

- Detects new log files using a tracking file  
- Reads logs using PySpark  
- Parses each log line into structured columns  
- Joins with user data from MySQL  
- Filters only ERROR logs  
- Performs aggregations:
  - Error count per day  
  - Error count per city  
- Writes output as partitioned Parquet files  
- Updates processed files list  

---

## Project Structure

```
incremental_log_data_pipeline/
│── dags/
│── scripts/
│── data/
│── logs/
│── output/
│── processed_files.txt
│── requirements.txt
│── README.md
```

## Airflow Orchestration

- DAG runs every 5 minutes  
- Uses BashOperator to trigger the pipeline  
- Handles scheduling and automation  

---

## Key Features

- Incremental data processing  
- Spark-based transformations  
- MySQL integration (JDBC)  
- Partitioned output storage  
- Logging and error handling  
- Automated using Airflow  

---

## How to Run

Install dependencies:

pip install -r requirements.txt

Run manually:

python scripts/transform.py

Run with Airflow:

airflow webserver --port 8085
airflow scheduler

---

## Future Improvements

- Deploy pipeline on AWS (S3 + EMR)  
- Add monitoring and alerting  
- Extend to streaming pipeline  

---

## Author

Rohith S

