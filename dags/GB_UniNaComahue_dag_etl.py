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


filepath = Path(r'/usr/local/airflow/files/GB_UniNalComahue.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','last_name','gender','birth_date','postal_code','location','email']

def get_comahue_info(**kwargs):
    with open(r'/usr/local/airflow/include/Comahue.sql') as sqlfile:
        query = sqlfile.read()
    hook = PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_comahue_df(ti):
    comahue = ti.xcom_pull(task_ids=['get_comahue_info'], include_prior_dates = True)
    if not comahue:
        raise Exception('No info available')
    comahue_df = pd.DataFrame(data=comahue[0], columns = df_columns)
    print(comahue_df)
    comahue_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_comahue',
    schedule_interval ='@hourly',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get comahue University data from postgres table
    task_get_comahue_info = PythonOperator(
        task_id='get_comahue_info',
        python_callable=get_comahue_info,
        do_xcom_push=True
    )
    #2. Save comahue data in dataframe
    task_create_comahue_df = PythonOperator(
        task_id='create_comahue_df',
        python_callable=create_comahue_df
    )
    
    task_get_comahue_info >> task_create_comahue_df