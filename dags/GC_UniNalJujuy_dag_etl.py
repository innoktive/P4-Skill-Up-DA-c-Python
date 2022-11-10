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


filepath=Path(r'/usr/local/airflow/files/GC_UniNalJujuy.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']

def get_jujuy_info(**kwargs):
    with open(r'/usr/local/airflow/include/Jujuy.sql') as sqlfile:
        query=sqlfile.read()
    hook=PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn=hook.get_conn()
    cursor=pg_conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_jujuy_df(ti):
    jujuy=ti.xcom_pull(task_ids=['get_jujuy_info'], include_prior_dates = True)
    if not jujuy:
        raise Exception('No info available')
    jujuy_df=pd.DataFrame(data=jujuy[0], columns = df_columns)
    print(jujuy_df)
    jujuy_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_jujuy',
    schedule_interval ='@hourly',
    start_date=datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get jujuy University data from postgres table
    task_get_jujuy_info=PythonOperator(
        task_id='get_jujuy_info',
        python_callable=get_jujuy_info,
        do_xcom_push=True,
        retries=5
    )
    #2. Save jujuy data in dataframe
    task_create_jujuy_df=PythonOperator(
        task_id='create_jujuy_df',
        python_callable=create_jujuy_df,
        retries=5
    )
    
    task_get_jujuy_info >> task_create_jujuy_df