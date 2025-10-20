from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'lawre',
    'start_date': datetime(2025, 10, 20),  # set explicit start_date
}

with DAG(
    dag_id='ecom_pipeline',
    default_args=default_args,
    description='E-commerce pipeline DAG',
    schedule='0 6 * * *',  # cron syntax
    catchup=False,
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='python3 ~/ecom-lakehouse/scripts/generate_data.py'
    )

    csv_to_bronze = BashOperator(
        task_id='csv_to_bronze',
        bash_command='python3 ~/ecom-lakehouse/scripts/csv_to_parquet.py'
    )

    partition_bronze = BashOperator(
        task_id='partition_bronze',
        bash_command='python3 ~/ecom-lakehouse/scripts/partition_bronze.py'
    )

    transform_silver = BashOperator(
        task_id='transform_silver',
        bash_command='python3 ~/ecom-lakehouse/scripts/transform_silver.py'
    )

    gold_layer = BashOperator(
        task_id='gold_layer',
        bash_command='python3 ~/ecom-lakehouse/scripts/gold_aggregations.py'
    )

    # Task dependencies
    generate_data >> csv_to_bronze >> partition_bronze >> transform_silver >> gold_layer
