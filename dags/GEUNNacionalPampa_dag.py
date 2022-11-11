from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging


LOGGER_NAME = 'Logger Univ. Nacional de la Pampa'
LOGGER_MSG = ''

def _log():
    logging.basicConfig(level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d')
    logger = logging.getLogger(LOGGER_NAME)
    logger.info('%s', LOGGER_MSG)


def extract():
    _log()


univ_nacional_pampa_DAG = DAG(
    dag_id='univ_nacional_pampa',
    schedule_interval ='@hourly',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False,
    default_args={
        "retries": 5, # If a task fails, it will retry 5 times.
    },
    tags=['ETL']
)

with univ_nacional_pampa_DAG as dag:
    # Use postgres hook to extract date from database.
    task_extract = PythonOperator(task_id='extract_data', python_callable=extract)

    # Use pandas in PythonOperator to transform exracted data.
    task_transform = EmptyOperator(task_id='transform_data')

    # Use Amazon S3 Hook to load and store transformed data in the cloud.
    task_load = EmptyOperator(task_id='load_data')

    task_extract >> task_transform >> task_load
