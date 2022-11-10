from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

univ_abierta_interamericana_DAG = DAG(
    dag_id='univ_abierta_interamericana',
    schedule_interval ='@hourly',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False,
    default_args={
        "retries": 5, # If a task fails, it will retry 5 times.
    },
    tags=['ETL']
)


with univ_abierta_interamericana_DAG as dag:
    # Use postgres hook to extract date from database.
    extract = EmptyOperator(task_id='extract_data')

    # Use pandas in PythonOperator to transform exracted data.
    transform = EmptyOperator(task_id='transform_data')

    # Use Amazon S3 Hook to load and store transformed data in the cloud.
    load = EmptyOperator(task_id='load_data')

    extract >> transform >> load
