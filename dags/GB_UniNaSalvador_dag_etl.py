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

filepath=Path(r'/usr/local/airflow/files/GB_UniNalSalvador.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
postal_codes_path=(r'/usr/local/airflow/assets/codigos_postales.csv')

def get_salvador_info(**kwargs):
    with open(r'/usr/local/airflow/include/Salvador.sql') as sqlfile:
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

filepath = Path(r'/usr/local/airflow/files/GB_UniNalSalvador.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']

def get_salvador_info(**kwargs):
    with open(r'/usr/local/airflow/include/Salvador.sql') as sqlfile:
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

filepath=Path(r'/usr/local/airflow/files/GB_UniNalSalvador.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
postal_codes_path=(r'/usr/local/airflow/assets/codigos_postales.csv')

def get_salvador_info(**kwargs):
    with open(r'/usr/local/airflow/include/Salvador.sql') as sqlfile:
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

def create_salvador_df(ti):
<<<<<<< HEAD
    salvador = ti.xcom_pull(task_ids=['get_salvador_info'], include_prior_dates = True)
    if not salvador:
        raise Exception('No info available')
    salvador_df = pd.DataFrame(data=salvador[0], columns = df_columns)
    print(salvador_df)
    salvador_df.to_csv(filepath, index=False, header=True)

with DAG(
    dag_id='prueba_salvador',
    schedule_interval ='@daily',
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get salvador University data from postgres table
    task_get_salvadorinfo = PythonOperator(
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
=======
<<<<<<< HEAD
<<<<<<< HEAD
    salvador=ti.xcom_pull(task_ids=['get_salvador_info'], include_prior_dates = True)
    if not salvador:
        raise Exception('No info available')
    salvador_df=pd.DataFrame(data=salvador[0], columns = df_columns)
    print(salvador_df)
    salvador_df.to_csv(filepath, index=False, header=True)

# Calculate age from birth_date
today=pd.to_datetime('today')
def calculate_age(count,salvador_df):
    birth=salvador_df['birth_date'][count]
    birth_asdate=datetime.strptime(birth,"%Y/%m/%d").date()
    age = today.year-birth_asdate.year-((today.month,today.day)<(birth_asdate.month,birth_asdate.day))
    return age

def transform_salvador_df(**kwargs):
    #Read data
    salvador_df=pd.read_csv(filepath)
    postal_codes_df=pd.read_csv(postal_codes_path)
    printing_columns=['university','career','inscription_date','full_name','gender','age','postal_code','location','email']
    #Processing salvador dataframe
    #A. String columns as lower, without spaces or hyphens (except for email)
    for i in salvador_df.columns:
        if i=='email':
            salvador_df['email']=salvador_df['email'].str.lower().str.strip()
        else:
            if salvador_df[i].dtypes == 'object' or salvador_df[i].dtypes == 'str':
                salvador_df[i]=salvador_df[i].str.lower().str.strip().str.replace('-','')
    #B. Complete gender as male or female
    salvador_df['gender']=salvador_df['gender'].str.replace(r'\bf\b','female',regex=True).str.replace(r'\bm\b','male',regex=True)  
    #C. Calculate age from birth_date
    age_list=[]
    for count in range(len(salvador_df['age'])):
        age_list.append(int(calculate_age(count,salvador_df)))

    salvador_df['age']=age_list
    #D. Getting postal code data from postal_codes_df
    salvador_df=pd.merge(left=salvador_df,right=postal_codes_df,how='left',left_on=salvador_df['location'].str.upper(), right_on='localidad')
    salvador_df['postal_code']=salvador_df['codigo_postal'].astype('str')
    salvador_df= salvador_df.drop(columns='localidad').drop(columns='codigo_postal')
    #E. Renaming last_name as full_name
    salvador_df.rename(columns={'last_name':'full_name'}, inplace=True)
    #F. Saving df as txt file
    salvador_df.to_csv(r'/usr/local/airflow/datasets/GC_UniNalsalvador.txt',sep=',',mode='w',index=False,columns=printing_columns)

<<<<<<< HEAD
with DAG(
    dag_id='prueba_salvador',
    schedule_interval ='@hourly',
    start_date=datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get salvador University data from postgres table
    task_get_salvador_info=PythonOperator(
        task_id='get_salvador_info',
        python_callable=get_salvador_info,
        do_xcom_push=True,
        retries=5
    )
    #2. Save salvador data in dataframe
    task_create_salvador_df=PythonOperator(
        task_id='create_salvador_df',
        python_callable=create_salvador_df,
        retries=5
    )
    #. Process salvador data and save it as txt file
    task_transform_salvador_df=PythonOperator(
        task_id='transform_salvador_df',
        python_callable=transform_salvador_df,
        retries=5
    ) 
    
    task_get_salvador_info >> task_create_salvador_df >> task_transform_salvador_df
=======
    salvador = ti.xcom_pull(task_ids=['get_salvador_info'], include_prior_dates = True)
=======
    salvador=ti.xcom_pull(task_ids=['get_salvador_info'], include_prior_dates = True)
>>>>>>> 699e9be (modified DAGs to run succesfully in airflow)
    if not salvador:
        raise Exception('No info available')
    salvador_df=pd.DataFrame(data=salvador[0], columns = df_columns)
    print(salvador_df)
    salvador_df.to_csv(filepath, index=False, header=True)

=======
>>>>>>> 0dd7351 (Update Group C DAGS to include transform functions)
with DAG(
    dag_id='prueba_salvador',
    schedule_interval ='@hourly',
    start_date=datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get salvador University data from postgres table
    task_get_salvador_info=PythonOperator(
        task_id='get_salvador_info',
        python_callable=get_salvador_info,
        do_xcom_push=True,
        retries=5
    )
    #2. Save salvador data in dataframe
    task_create_salvador_df=PythonOperator(
        task_id='create_salvador_df',
        python_callable=create_salvador_df,
        retries=5
    )
    #. Process salvador data and save it as txt file
    task_transform_salvador_df=PythonOperator(
        task_id='transform_salvador_df',
        python_callable=transform_salvador_df,
        retries=5
    ) 
    
<<<<<<< HEAD
    task_get_salvador_info >> task_create_salvador_df
>>>>>>> 3d393a9 (Included succesful extraction DAG (csv in files folder))
=======
    task_get_salvador_info >> task_create_salvador_df >> task_transform_salvador_df
>>>>>>> 0dd7351 (Update Group C DAGS to include transform functions)
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
