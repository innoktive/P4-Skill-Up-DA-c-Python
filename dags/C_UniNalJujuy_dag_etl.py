import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

filepath = Path(r'C:\Users\User\Desktop\Alkemy_SkillUp\proyecto-env\Skill-Up-DA-c-Python\files\C_UniNalJujuy.csv')

def get_jujuy_info():
    with open(r'C:\Users\User\Desktop\Alkemy_SkillUp\proyecto-env\Skill-Up-DA-c-Python\include\Jujuy.sql') as sqlfile:
        query = sqlfile.read()
    hook = PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_jujuy_df(ti):
    jujuy = ti.xcom_pull(task_ids=['get_jujuy_info'])
    if not jujuy:
        raise Exception('No info available')
    jujuy_df= pd.DataFrame(data=jujuy[0])
    jujuy.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_jujuy',
    schedule_interval ='@daily',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get Jujuy University data from postgres table
    task_get_jujuy_info = PythonOperator(
        task_id='get_jujuy_info',
        python_callable=get_jujuy_info,
        do_xcom_push=True
    )
    #2. Save Jujuy data in dataframe
    task_create_jujuy_df = PythonOperator(
        task_id='create_jujuy_df',
        python_callable=create_jujuy_df
    )
    
    task_get_jujuy_info >> task_create_jujuy_df