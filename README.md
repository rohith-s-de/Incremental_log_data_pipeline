# Incremental Log Data Pipeline (PySpark + Airflow)

## Overview

This project is an end-to-end data pipeline built to process log files incrementally. Instead of processing all files every time, the pipeline only picks up new files that haven’t been processed before.

It uses PySpark for data processing and Apache Airflow for scheduling. The pipeline also joins log data with user information from a MySQL database to generate useful insights.

The project is architecturally integrated with Amazon S3 for cloud-based parquet storage and supports querying datasets using Amazon Athena.

---

## Tech Stack

- Python
- PySpark
- Apache Airflow
- MySQL
- JDBC
- Linux
- Amazon S3
- AWS Glue (Familiarity)
- Amazon Athena

---

## Architecture Flow

Incremental Log Files  
↓  
PySpark ETL Pipeline  
↓  
MySQL JDBC Enrichment  
↓  
Transformations & Aggregations  
↓  
Partitioned Parquet Output  
↓  
Amazon S3 Storage  
↓  
Amazon Athena Querying  

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
- Stores processed datasets in Amazon S3
- Supports querying datasets using Amazon Athena
- Updates processed files list

---

## Project Structure

```text
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

---

## Airflow Orchestration

- Schedules the pipeline to run every 5 minutes
- Uses BashOperator to trigger the Spark transformation script

---

## Key Features

- Incremental data processing
- Spark-based transformations
- MySQL integration (JDBC)
- Partitioned parquet output storage
- Amazon S3 cloud storage integration
- AWS Glue ETL workflow familiarity
- Amazon Athena querying familiarity
- Logging and error handling
- Automated using Airflow

---

## How to Run

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run manually

```bash
python scripts/transform.py
```

### Run with Airflow

```bash
airflow webserver --port 8085
airflow scheduler
```

---

## Future Improvements

- Implement full AWS deployment using AWS Glue and EMR
- Add monitoring and alerting
- Extend to streaming pipeline

---

## Author

Rohith S
