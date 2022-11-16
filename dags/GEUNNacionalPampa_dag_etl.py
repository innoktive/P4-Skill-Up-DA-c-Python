from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
import os

import GE_transform

LOGGER_MSG = ''

ABSOLUTE_PATH = os.path.dirname(__file__)


def _log():
    logging.basicConfig(level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d')
    logger = logging.getLogger('Logger Univ. Nacional Pampa')
    logger.info('%s', LOGGER_MSG)


def extract():
    _log()
    
    full_path = os.path.join(ABSOLUTE_PATH, '../include/')
    with open(full_path + 'GEUNNacionalPampa.sql') as f:
        query = f.read()
    
    hook = PostgresHook(postgres_conn_id='alkemy_db', schema='training')
    df = hook.get_pandas_df(sql=query)
    full_path = os.path.join(ABSOLUTE_PATH, '../files/')
    df.to_csv(full_path + 'GEUNNacionalPampa' + '_select.csv')


def transform():
    full_path = os.path.join(ABSOLUTE_PATH, '../files/')
    df = pd.read_csv(full_path + 'GEUNNacionalPampa' + '_select.csv', index_col=0)


    full_path = os.path.join(ABSOLUTE_PATH, '../assets/')
    postal_codes = pd.read_csv(full_path + 'codigos_postales.csv')
    postal_codes.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace=True)
    postal_codes['location'] = postal_codes['location'].str.lower()

    transform_function = {'GEUNAbiertainteramericana': GE_transform.transform_GEUNAbiertaInteramericana,
                            'GEUNNacionalPampa': GE_transform.transform_GEUNNacionalPampa
    }
    transform_function['GEUNNacionalPampa'](df, postal_codes)

    full_path = os.path.join(ABSOLUTE_PATH, '../datasets/')
    df.to_csv(full_path + 'GEUNNacionalPampa' + '_process.txt')


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
    task_extract = PythonOperator(task_id='extract_data', python_callable=extract)

    # Use pandas in PythonOperator to transform exracted data.
    task_transform = PythonOperator(task_id='transform_data', python_callable=transform)

    # Use Amazon S3 Hook to load and store transformed data in the cloud.
    task_load = EmptyOperator(task_id='load_data')

    task_extract >> task_transform >> task_load