import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


filepath=Path(r'/usr/local/airflow/files/GC_UniNalJujuy.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
postal_codes_path=(r'/usr/local/airflow/assets/codigos_postales.csv')

def get_jujuy_info(**kwargs):
    with open(r'/usr/local/airflow/include/Jujuy.sql') as sqlfile:
        query=sqlfile.read()
    hook=PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
    pg_conn=hook.get_conn()
    cursor.execute(query)
    return cursor.fetchall()

def create_jujuy_df(ti):
    jujuy=ti.xcom_pull(task_ids=['get_jujuy_info'], include_prior_dates = True)
    if not jujuy:
        raise Exception('No info available')
    jujuy_df=pd.DataFrame(data=jujuy[0], columns = df_columns)
    print(jujuy_df)
    jujuy_df.to_csv(filepath, index=False, header=True)

# Calculate age from birth_date
today=pd.to_datetime('today')
def calculate_age(count,jujuy_df):
    birth=jujuy_df['birth_date'][count]
    birth_asdate=datetime.strptime(birth,"%Y/%m/%d").date()
    age = today.year-birth_asdate.year-((today.month,today.day)<(birth_asdate.month,birth_asdate.day))
    return age

def transform_jujuy_df(**kwargs):
    #Read data
    jujuy_df=pd.read_csv(filepath)
    postal_codes_df=pd.read_csv(postal_codes_path)
    printing_columns=['university','career','inscription_date','full_name','gender','age','postal_code','location','email']
    #Processing jujuy dataframe
    #A. String columns as lower, without spaces or hyphens (except for email)
    for i in jujuy_df.columns:
        if i=='email':
            jujuy_df['email']=jujuy_df['email'].str.lower().str.strip()
        else:
            if jujuy_df[i].dtypes == 'object' or jujuy_df[i].dtypes == 'str':
                jujuy_df[i]=jujuy_df[i].str.lower().str.strip().str.replace('-','')
    #B. Complete gender as male or female
    jujuy_df['gender']=jujuy_df['gender'].str.replace(r'\bf\b','female',regex=True).str.replace(r'\bm\b','male',regex=True)  
    #C. Calculate age from birth_date
    age_list=[]
    for count in range(len(jujuy_df['age'])):
        age_list.append(int(calculate_age(count,jujuy_df)))

    jujuy_df['age']=age_list
    #D. Getting postal code data from postal_codes_df
    jujuy_df=pd.merge(left=jujuy_df,right=postal_codes_df,how='left',left_on=jujuy_df['location'].str.upper(), right_on='localidad')
    jujuy_df['postal_code']=jujuy_df['codigo_postal'].astype('str')
    jujuy_df= jujuy_df.drop(columns='localidad').drop(columns='codigo_postal')
    #E. Renaming last_name as full_name
    jujuy_df.rename(columns={'last_name':'full_name'}, inplace=True)
    #F. Saving df as txt file
    jujuy_df.to_csv(r'/usr/local/airflow/datasets/GC_UniNalJujuy.txt',sep=',',mode='w',index=False,columns=printing_columns)

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
    #3. Process jujuy data and save it as txt file
    task_transform_jujuy_df=PythonOperator(
        task_id='transform_jujuy_df',
        python_callable=transform_jujuy_df,
        retries=5
    ) 
    
    task_get_jujuy_info >> task_create_jujuy_df >> task_transform_jujuy_df