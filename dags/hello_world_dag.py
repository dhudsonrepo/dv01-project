from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

def say_hello():
    print("Hello!")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='hello',
        default_args=default_args,
        description='Hello DAG',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=['hello']
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )

    start >> hello_task >> end
