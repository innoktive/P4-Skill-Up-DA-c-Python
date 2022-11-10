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


filepath = Path(r'/usr/local/airflow/files/GD_UniNalTresFebrero.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','full_name','gender','age','postal_code','location','email']

def get_tres_de_febrero_info(**kwargs):
    with open(r'/usr/local/airflow/include/tres_de_febrero.sql') as sqlfile:
        query = sqlfile.read()
    hook = PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_tres_de_febrero_df(ti):
    tres_de_febrero = ti.xcom_pull(task_ids=['get_tres_de_febrero_info'], include_prior_dates = True)
    if not tres_de_febrero:
        raise Exception('No info available')
    tres_de_febrero_df = pd.DataFrame(data=tres_de_febrero[0], columns = df_columns)
    print(tres_de_febrero_df)
    tres_de_febrero_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_tres_de_febrero',
    schedule_interval ='@hourly',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get tres_de_febrero University data from postgres table
    task_get_tres_de_febrero_info = PythonOperator(
        task_id='get_tres_de_febrero_info',
        python_callable=get_tres_de_febrero_info,
        do_xcom_push=True,
        retries=5
    )
    #2. Save tres_de_febrero data in dataframe
    task_create_tres_de_febrero_df = PythonOperator(
        task_id='create_tres_de_febrero_df',
        python_callable=create_tres_de_febrero_df,
        retries=5
    )
    
    task_get_tres_de_febrero_info >> task_create_tres_de_febrero_df