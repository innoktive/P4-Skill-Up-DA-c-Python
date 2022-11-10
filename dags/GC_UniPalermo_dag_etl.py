import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


filepath = Path(r'/usr/local/airflow/files/GC_UniPalermo.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']

def get_palermo_info(**kwargs):
    with open(r'/usr/local/airflow/include/Palermo.sql') as sqlfile:
        query = sqlfile.read()
    hook = PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_palermo_df(ti):
    palermo = ti.xcom_pull(task_ids=['get_palermo_info'], include_prior_dates = True)
    if not palermo:
        raise Exception('No info available')
    palermo_df = pd.DataFrame(data=palermo[0], columns = df_columns)
    print(palermo_df)
    palermo_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_palermo',
    schedule_interval ='@hourly',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get palermo University data from postgres table
    task_get_palermo_info = PythonOperator(
        task_id='get_palermo_info',
        python_callable=get_palermo_info,
        do_xcom_push=True,
        retries=5
    )
    #2. Save palermo data in dataframe
    task_create_palermo_df = PythonOperator(
        task_id='create_palermo_df',
        python_callable=create_palermo_df,
        retries=5
    )
    
    task_get_palermo_info >> task_create_palermo_df