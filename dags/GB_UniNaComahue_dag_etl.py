import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD

filepath=Path(r'/usr/local/airflow/files/GB_UniNalComahue.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
postal_codes_path=(r'/usr/local/airflow/assets/codigos_postales.csv')

def get_comahue_info(**kwargs):
    with open(r'/usr/local/airflow/include/Comahue.sql') as sqlfile:
        query=sqlfile.read()
    hook=PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn=hook.get_conn()
    cursor=pg_conn.cursor()
=======
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

<<<<<<< HEAD

filepath = Path(r'/usr/local/airflow/files/GB_UniNalComahue.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']

def get_comahue_info(**kwargs):
    with open(r'/usr/local/airflow/include/Comahue.sql') as sqlfile:
        query = sqlfile.read()
    hook = PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
=======
=======
>>>>>>> 0dd7351 (Update Group B DAGS to include transform functions)

filepath=Path(r'/usr/local/airflow/files/GB_UniNalComahue.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
postal_codes_path=(r'/usr/local/airflow/assets/codigos_postales.csv')

def get_comahue_info(**kwargs):
    with open(r'/usr/local/airflow/include/Comahue.sql') as sqlfile:
        query=sqlfile.read()
    hook=PostgresHook(
        postgres_conn_id='alkemy_db',
        schema='training'
    )
<<<<<<< HEAD
    pg_conn = hook.get_conn()
    cursor =pg_conn.cursor()
>>>>>>> 3d393a9 (Included succesful extraction DAG (csv in files folder))
=======
    pg_conn=hook.get_conn()
    cursor=pg_conn.cursor()
>>>>>>> 699e9be (modified DAGs to run succesfully in airflow)
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
    cursor.execute(query)
    return cursor.fetchall()

def create_comahue_df(ti):
<<<<<<< HEAD
    comahue = ti.xcom_pull(task_ids=['get_comahue_info'], include_prior_dates = True)
    if not comahue:
        raise Exception('No info available')
    comahue_df = pd.DataFrame(data=comahue[0], columns = df_columns)
    print(comahue_df)
    comahue_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_comahue',
    schedule_interval ='@daily',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get Comahue University data from postgres table
    task_get_comahueinfo = PythonOperator(
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
=======
<<<<<<< HEAD
<<<<<<< HEAD
    comahue=ti.xcom_pull(task_ids=['get_comahue_info'], include_prior_dates = True)
    if not comahue:
        raise Exception('No info available')
    comahue_df=pd.DataFrame(data=comahue[0], columns = df_columns)
    print(comahue_df)
    comahue_df.to_csv(filepath, index=False, header=True)

# Calculate age from birth_date
today=pd.to_datetime('today')
def calculate_age(count,comahue_df):
    birth=comahue_df['birth_date'][count]
    birth_asdate=datetime.strptime(birth,"%Y/%m/%d").date()
    age = today.year-birth_asdate.year-((today.month,today.day)<(birth_asdate.month,birth_asdate.day))
    return age

def transform_comahue_df(**kwargs):
    #Read data
    comahue_df=pd.read_csv(filepath)
    postal_codes_df=pd.read_csv(postal_codes_path)
    printing_columns=['university','career','inscription_date','full_name','gender','age','postal_code','location','email']
    #Processing comahue dataframe
    #A. String columns as lower, without spaces or hyphens (except for email)
    for i in comahue_df.columns:
        if i=='email':
            comahue_df['email']=comahue_df['email'].str.lower().str.strip()
        else:
            if comahue_df[i].dtypes == 'object' or comahue_df[i].dtypes == 'str':
                comahue_df[i]=comahue_df[i].str.lower().str.strip().str.replace('-','')
    #B. Complete gender as male or female
    comahue_df['gender']=comahue_df['gender'].str.replace(r'\bf\b','female',regex=True).str.replace(r'\bm\b','male',regex=True)  
    #C. Calculate age from birth_date
    age_list=[]
    for count in range(len(comahue_df['age'])):
        age_list.append(int(calculate_age(count,comahue_df)))

    comahue_df['age']=age_list
    #D. Getting postal code data from postal_codes_df
    comahue_df=pd.merge(left=comahue_df,right=postal_codes_df,how='left',left_on=comahue_df['location'].str.upper(), right_on='localidad')
    comahue_df['postal_code']=comahue_df['codigo_postal'].astype('str')
    comahue_df= comahue_df.drop(columns='localidad').drop(columns='codigo_postal')
    #E. Renaming last_name as full_name
    comahue_df.rename(columns={'last_name':'full_name'}, inplace=True)
    #F. Saving df as txt file
    comahue_df.to_csv(r'/usr/local/airflow/datasets/GC_UniNalComahue.txt',sep=',',mode='w',index=False,columns=printing_columns)

<<<<<<< HEAD
with DAG(
    dag_id='prueba_comahue',
    schedule_interval ='@hourly',
    start_date=datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get comahue University data from postgres table
    task_get_comahue_info=PythonOperator(
        task_id='get_comahue_info',
        python_callable=get_comahue_info,
        do_xcom_push=True,
        retries=5
    )
    #2. Save comahue data in dataframe
    task_create_comahue_df=PythonOperator(
        task_id='create_comahue_df',
        python_callable=create_comahue_df,
        retries=5
    )
    #. Process comahue data and save it as txt file
    task_transform_comahue_df=PythonOperator(
        task_id='transform_comahue_df',
        python_callable=transform_comahue_df,
        retries=5
    ) 
    
    task_get_comahue_info >> task_create_comahue_df >> task_transform_comahue_df
=======
    comahue = ti.xcom_pull(task_ids=['get_comahue_info'], include_prior_dates = True)
=======
    comahue=ti.xcom_pull(task_ids=['get_comahue_info'], include_prior_dates = True)
>>>>>>> 699e9be (modified DAGs to run succesfully in airflow)
    if not comahue:
        raise Exception('No info available')
    comahue_df=pd.DataFrame(data=comahue[0], columns = df_columns)
    print(comahue_df)
    comahue_df.to_csv(filepath, index=False, header=True)

=======
>>>>>>> 0dd7351 (Update Group C DAGS to include transform functions)
with DAG(
    dag_id='prueba_comahue',
    schedule_interval ='@hourly',
    start_date=datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get comahue University data from postgres table
    task_get_comahue_info=PythonOperator(
        task_id='get_comahue_info',
        python_callable=get_comahue_info,
        do_xcom_push=True,
        retries=5
    )
    #2. Save comahue data in dataframe
    task_create_comahue_df=PythonOperator(
        task_id='create_comahue_df',
        python_callable=create_comahue_df,
        retries=5
    )
    #. Process comahue data and save it as txt file
    task_transform_comahue_df=PythonOperator(
        task_id='transform_comahue_df',
        python_callable=transform_comahue_df,
        retries=5
    ) 
    
<<<<<<< HEAD
    task_get_comahue_info >> task_create_comahue_df
>>>>>>> 3d393a9 (Included succesful extraction DAG (csv in files folder))
=======
    task_get_comahue_info >> task_create_comahue_df >> task_transform_comahue_df
>>>>>>> 0dd7351 (Update Group C DAGS to include transform functions)
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
