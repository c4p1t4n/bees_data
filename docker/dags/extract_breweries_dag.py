from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extract_data_api import extract_breweries_to_minio

default_args = {
    "owner": "airflow",
    "retries": 2,
}

with DAG(
    dag_id="extract_breweries_dag",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "brewery"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_breweries",
        python_callable=extract_breweries_to_minio,
        op_kwargs={
            "bucket_name": "datalake",
            "object_key_prefix": "bronze/breweries",
        },
    )
