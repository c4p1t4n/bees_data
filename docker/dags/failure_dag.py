from airflow import DAG
from airflow.operators.python import PythonOperator
from src.task_failure import task_failure_callback
from datetime import datetime



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'on_failure_callback': task_failure_callback,
}

with DAG(
    dag_id='failure_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def my_task():
        raise ValueError("Simulando falha")

    failing_task = PythonOperator(
        task_id='simulate_failure',
        python_callable=my_task,
    )
