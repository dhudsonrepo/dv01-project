from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'loan_dag',
    default_args=default_args,
    description='Download and upload loan data and run DBT transformations',
    schedule_interval='@daily',
    catchup=False
)

def upload_kiva_to_gcs():
    from scripts.helper import upload_to_gcs
    from scripts.download_kiva import download_kiva
    df, path = download_kiva()
    upload_to_gcs(df, path)

upload_kiva_task = PythonOperator(
    task_id='upload_kiva_to_gcs',
    python_callable=upload_kiva_to_gcs,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

dbt_run = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/dv01_bigquery_dbt && dbt run --profiles-dir /opt/airflow/profiles',
    dag=dag
)

dbt_test = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd /opt/airflow/dv01_bigquery_dbt && dbt test --profiles-dir /opt/airflow/profiles',
    dag=dag
)

# DAG Dependencies
upload_kiva_task >> dbt_run >> dbt_test