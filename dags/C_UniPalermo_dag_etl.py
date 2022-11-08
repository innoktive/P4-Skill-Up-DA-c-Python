import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

filepath = Path(r'C:\Users\User\Desktop\Alkemy_SkillUp\proyecto-env\Skill-Up-DA-c-Python\files\C_UniPalermo.csv')

def get_palermo_info():
    with open(r'C:\Users\User\Desktop\Alkemy_SkillUp\proyecto-env\Skill-Up-DA-c-Python\include\Palermo.sql') as sqlfile:
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
    palermo = ti.xcom_pull(task_ids=['get_palermo_info'])
    if not palermo:
        raise Exception('No info available')
    palermo_df= pd.DataFrame(data=palermo[0])
    palermo.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_palermo',
    schedule_interval ='@daily',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get Palermo University data from postgres table
    task_get_palermo_info = PythonOperator(
        task_id='get_palermo_info',
        python_callable=get_palermo_info,
        do_xcom_push=True
    )
    #2. Save Palermo data in dataframe
    task_create_palermo_df = PythonOperator(
        task_id='create_palermo_df',
        python_callable=create_palermo_df
    )
    
    task_get_palermo_info >> task_create_palermo_df