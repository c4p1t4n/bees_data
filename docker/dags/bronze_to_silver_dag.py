from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src.etl_bronze_to_silver import transform_breweries_bronze_to_silver


default_args = {
    "owner": "airflow",
    "retries": 2,
}

with DAG(
    dag_id="bronze_to_silver_dag",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["silver", "transform"]
) as dag:

    bronze_to_silver_task = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=transform_breweries_bronze_to_silver,
        op_kwargs={
            "bucket_name": "datalake",
            "bronze_prefix": "bronze/breweries",
            "silver_prefix": "silver/breweries"
        },
    )
