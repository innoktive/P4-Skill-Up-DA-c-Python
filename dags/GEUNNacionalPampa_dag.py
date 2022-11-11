from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import pandas as pd
import os


CONNECTION_ID = 'alkemy_db'
SCHEMA = 'training'

SQL_FILE = 'GEUNNacionalPampa.sql'
CSV_NAME = 'GEUNNacionalPampa_select.csv'

LOGGER_NAME = 'Logger Univ. Nacional de la Pampa'
LOGGER_MSG = ''

ABSOLUTE_PATH = os.path.dirname(__file__)


def _log():
    logging.basicConfig(level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d')
    logger = logging.getLogger(LOGGER_NAME)
    logger.info('%s', LOGGER_MSG)


def extract():
    _log()
    
    full_path = os.path.join(ABSOLUTE_PATH, '../include/')
    with open(full_path + SQL_FILE) as f:
        query = f.read()
    
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID, schema=SCHEMA)
    df = hook.get_pandas_df(sql=query)
    full_path = os.path.join(ABSOLUTE_PATH, '../files/')
    df.to_csv(full_path + CSV_NAME)


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
