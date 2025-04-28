from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts.loan_data_download import main

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'loan_data_pipeline_dag',
    default_args=default_args,
    description='Download loan data and run DBT transformations',
    schedule_interval='@daily',
    catchup=False
)

pipeline = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=main,
    dag=dag
)

dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd dv01_bigquery_dbt && dbt run',
    dag=dag
)

dbt_test = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd dv01_bigquery_dbt && dbt test',
    dag=dag
)

# ---------- DAG Dependency Chain ----------
pipeline >> dbt_run >> dbt_test
