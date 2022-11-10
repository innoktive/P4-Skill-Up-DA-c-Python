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


filepath = Path(r'/usr/local/airflow/files/GC_UniNalSalvador.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','last_name','gender','birth_date','location','email']

def get_salvador_info(**kwargs):
    with open(r'/usr/local/airflow/include/Salvador.sql') as sqlfile:
        query = sqlfile.read()
    hook = PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_salvador_df(ti):
    salvador = ti.xcom_pull(task_ids=['get_salvador_info'], include_prior_dates = True)
    if not salvador:
        raise Exception('No info available')
    salvador_df = pd.DataFrame(data=salvador[0], columns = df_columns)
    print(salvador_df)
    salvador_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_salvador',
    schedule_interval ='@hourly',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get salvador University data from postgres table
    task_get_salvador_info = PythonOperator(
        task_id='get_salvador_info',
        python_callable=get_salvador_info,
        do_xcom_push=True
    )
    #2. Save salvador data in dataframe
    task_create_salvador_df = PythonOperator(
        task_id='create_salvador_df',
        python_callable=create_salvador_df
    )
    
    task_get_salvador_info >> task_create_salvador_df