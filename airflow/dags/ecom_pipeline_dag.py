from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/home/lawre/ecom-lakehouse/airflow")

default_args = {
    "owner": "lawre",
    "depends_on_past": False,
    "retries": 1,
}

def bronze_task():
    exec(open(f"{AIRFLOW_HOME}/scripts/bronze_ingest.py").read())

def silver_task():
    exec(open(f"{AIRFLOW_HOME}/scripts/silver_transform.py").read())

def gold_task():
    exec(open(f"{AIRFLOW_HOME}/scripts/gold_aggregate.py").read())

with DAG(
    dag_id="ecom_orders_pipeline",
    start_date=datetime(2025, 10, 19),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_task
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=silver_task
    )

    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=gold_task
    )

    bronze >> silver >> gold

