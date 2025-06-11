from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from src.extract_data_api import main
from src.etl_bronze_to_silver import transform_bronze_to_silver
from src.task_failure import send_email_via_ses
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.task_failure import task_failure_callback
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'on_failure_callback': task_failure_callback,
}

with DAG(
    dag_id="extract_breweries_dag",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "brewery"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_breweries",
        python_callable=main,
        op_kwargs={
            "bucket_name": "datalake",
            "object_key_prefix": "bronze/breweries",
        },
    )

    bronze_to_silver_task = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit "
            "--packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=airflowuser "
            "--conf spark.hadoop.fs.s3a.secret.key=airflowpass123 "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/airflow/src/etl_bronze_to_silver.py "
        ),
    )

    silver_to_gold_task = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "spark-submit "
            "--packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=airflowuser "
            "--conf spark.hadoop.fs.s3a.secret.key=airflowpass123 "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/airflow/src/etl_silver_to_gold.py "
        ),
    )

    extract_task >> bronze_to_silver_task >> silver_to_gold_task