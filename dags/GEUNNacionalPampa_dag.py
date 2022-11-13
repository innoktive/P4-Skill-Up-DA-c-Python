from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
import os


CONNECTION_ID = 'alkemy_db'
SCHEMA = 'training'

SQL_FILE = 'GEUNNacionalPampa.sql'
CSV_NAME = 'GEUNNacionalPampa_select.csv'
TXT_NAME = 'GEUNNacionalPampa_process.txt'

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


def transform():
    full_path = os.path.join(ABSOLUTE_PATH, '../files/')
    df = pd.read_csv(full_path + CSV_NAME, index_col=0)

    full_path = os.path.join(ABSOLUTE_PATH, '../assets/')
    postal_codes = pd.read_csv(full_path + 'codigos_postales.csv')
    postal_codes.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace=True)
    postal_codes['location'] = postal_codes['location'].str.lower()


    df['university'] = df['university'].str.replace('-', ' ').str.lower()

    df['career'] = df['career'].str.replace('-', ' ').str.lower()

    df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%d/%m/%Y')

    reg_ex = '(mr. )|(mrs. )|(dr. )|(miss )|(ms. )|( jr.)|( phd)|( dvm)|( ii)|( iv)|( md)|( dds)|( mhd)'
    df['name'] = df['name'].str.lower().str.replace(reg_ex, '', regex=True)
    name = df['name'].str.split(' ', expand=True)
    name.columns = ['first_name', 'last_name']
    idx = name['last_name'].isnull()
    name.loc[idx, 'last_name'] = name.loc[idx, 'first_name']
    name.loc[idx, 'first_name'] = ''
    df.drop(columns='name', inplace=True)
    df['first_name'] = name['first_name']
    df['last_name'] = name['last_name']

    df['gender'] = df['gender'].str.lower().map(lambda x: 'male' if x == 'm' else 'female')

    df['age'] = pd.to_datetime(df['birth_date'], format='%d/%m/%Y')\
                .map(lambda x: relativedelta(date.today(), x).years if x <= pd.Timestamp('now') else 0)
    df.drop(columns='birth_date', inplace=True)

    postal_codes_dict = dict(zip(postal_codes['postal_code'], postal_codes['location']))
    df['location'] = df['postal_code'].map(postal_codes_dict)

    df['email'] = df['email'].str.lower()

    full_path = os.path.join(ABSOLUTE_PATH, '../datasets/')
    df.to_csv(full_path + TXT_NAME)


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
    task_transform = PythonOperator(task_id='transform_data', python_callable=transform)

    # Use Amazon S3 Hook to load and store transformed data in the cloud.
    task_load = EmptyOperator(task_id='load_data')

    task_extract >> task_transform >> task_load
