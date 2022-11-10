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


filepath = Path(r'/usr/local/airflow/files/GD_utn.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']

def get_utn_info(**kwargs):
    with open(r'/usr/local/airflow/include/utn.sql') as sqlfile:
        query = sqlfile.read()
    hook = PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_utn_df(ti):
    utn = ti.xcom_pull(task_ids=['get_utn_info'], include_prior_dates = True)
    if not utn:
        raise Exception('No info available')
    utn_df = pd.DataFrame(data=utn[0], columns = df_columns)
    print(utn_df)
    utn_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_utn',
    schedule_interval ='@daily',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get utn University data from postgres table
    task_get_utn_info = PythonOperator(
        task_id='get_utn_info',
        python_callable=get_utn_info,
        do_xcom_push=True
    )
    #2. Save utn data in dataframe
    task_create_utn_df = PythonOperator(
        task_id='create_utn_df',
        python_callable=create_utn_df
    )
    
    task_get_utn_info >> task_create_utn_df