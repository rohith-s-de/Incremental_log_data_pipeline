from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1),
    'email_on_failure':False,
    'email_on_retry':False}

dag = DAG(
    dag_id='incremental_log_data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026,4,14),
    catchup=False,
    description='Incremental log data pipeline using Spark')

run_transform= BashOperator(
    task_id='run_transform',
    bash_command='bash /home/rayaan/incremental_log_data_pipeline/scripts/wrapper_script.sh ',
    dag=dag)
