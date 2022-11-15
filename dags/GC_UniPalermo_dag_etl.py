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

filepath = Path(r'/usr/local/airflow/files/GC_UniPalermo.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
postal_codes_path=(r'/usr/local/airflow/assets/codigos_postales.csv')
=======
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


filepath = Path(r'/usr/local/airflow/files/GC_UniPalermo.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns = ['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
<<<<<<< HEAD
=======
>>>>>>> 3d393a9 (Included succesful extraction DAG (csv in files folder))
=======

filepath = Path(r'/usr/local/airflow/files/GC_UniPalermo.csv')
filepath.parent.mkdir(parents=True, exist_ok=True)
df_columns=['university','career','inscription_date','last_name','gender','birth_date','age','postal_code','location','email']
postal_codes_path=(r'/usr/local/airflow/assets/codigos_postales.csv')
>>>>>>> 0dd7351 (Update Group C DAGS to include transform functions)
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05

def get_palermo_info(**kwargs):
    with open(r'/usr/local/airflow/include/Palermo.sql') as sqlfile:
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
    palermo = ti.xcom_pull(task_ids=['get_palermo_info'], include_prior_dates = True)
    if not palermo:
        raise Exception('No info available')
    palermo_df = pd.DataFrame(data=palermo[0], columns = df_columns)
    print(palermo_df)
    palermo_df.to_csv(filepath, index=False, header=True)

<<<<<<< HEAD
with DAG(
    dag_id='prueba_palermo',
    schedule_interval ='@daily',
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0dd7351 (Update Group C DAGS to include transform functions)
# Calculate age from birth_date
today=pd.to_datetime('today')
def calculate_age(count,palermo_df):
    birth=palermo_df['birth_date'][count]
    birth_asdate=datetime.strptime(birth,'%d/%b/%y').date()
    if birth_asdate.year>2022:
        birth_asdate=birth_asdate.replace(year=birth_asdate.year-100)
    elif birth_asdate.year==2022:
        if (today.month,today.day)<(birth_asdate.month,birth_asdate.day):
            birth_asdate=birth_asdate.replace(year=birth_asdate.year-100)
    age=today.year-birth_asdate.year-((today.month,today.day)<(birth_asdate.month,birth_asdate.day))
    return age

def transform_palermo_df():
    #Read data
    palermo_df=pd.read_csv(filepath)
    postal_codes_df=pd.read_csv(postal_codes_path)
    printing_columns=['university','career','inscription_date','full_name','gender','age','postal_code','location','email']
    #Processing palermo dataframe
    #A. String columns as lower, without spaces or hyphens (except for email)
    for i in palermo_df.columns:
        if i=='email':
            palermo_df['email']=palermo_df['email'].str.lower().str.strip()
        else:
            if palermo_df[i].dtypes == 'object' or palermo_df[i].dtypes == 'str':
                palermo_df[i]=palermo_df[i].str.lower().str.replace('-','').str.replace('_',' ').str.strip()
    #B. Complete gender as male or female
    palermo_df['gender']=palermo_df['gender'].str.replace(r'\bf\b','female',regex=True).str.replace(r'\bm\b','male',regex=True)  
    #C. Calculate age from birth_date
    age_list=[]
    for count in range(len(palermo_df['age'])):
        age_list.append(int(calculate_age(count,palermo_df)))
    palermo_df['age']=age_list
    #D. Getting location data from postal_codes_df
    palermo_df=pd.merge(left=palermo_df,right=postal_codes_df,how='left',left_on=palermo_df['postal_code'], right_on='codigo_postal')
    palermo_df['location']=palermo_df['localidad'].astype('str').str.lower()
    palermo_df= palermo_df.drop(columns='localidad').drop(columns='codigo_postal')
    #E. Renaming last_name as full_name
    palermo_df.rename(columns={'last_name':'full_name'}, inplace=True)
    #F. Changing date formatting for inscription date
    palermo_df['inscription_date']=pd.to_datetime(palermo_df.inscription_date, format='%d/%b/%y').dt.strftime("%Y/%m/%d")
    #F. Saving df as txt file
    palermo_df.to_csv(r'/usr/local/airflow/datasets/GC_UniPalermo.txt',sep=',',mode='w', index=False, columns=printing_columns)

with DAG(
    dag_id='prueba_palermo',
    schedule_interval ='@hourly',
=======
with DAG(
    dag_id='prueba_palermo',
<<<<<<< HEAD
    schedule_interval ='@daily',
>>>>>>> 3d393a9 (Included succesful extraction DAG (csv in files folder))
=======
    schedule_interval ='@hourly',
>>>>>>> 699e9be (modified DAGs to run succesfully in airflow)
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
    start_date = datetime(year=2022, month=11, day=8),
    catchup=False
) as dag:
    #1. Get palermo University data from postgres table
    task_get_palermo_info = PythonOperator(
        task_id='get_palermo_info',
        python_callable=get_palermo_info,
<<<<<<< HEAD
        do_xcom_push=True
=======
<<<<<<< HEAD
<<<<<<< HEAD
        do_xcom_push=True,
        retries=5
=======
        do_xcom_push=True
>>>>>>> 3d393a9 (Included succesful extraction DAG (csv in files folder))
=======
        do_xcom_push=True,
        retries=5
>>>>>>> 699e9be (modified DAGs to run succesfully in airflow)
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
    )
    #2. Save palermo data in dataframe
    task_create_palermo_df = PythonOperator(
        task_id='create_palermo_df',
<<<<<<< HEAD
        python_callable=create_palermo_df
    )
    
    task_get_palermo_info >> task_create_palermo_df
=======
<<<<<<< HEAD
<<<<<<< HEAD
        python_callable=create_palermo_df,
        retries=5
    )
    #3. Transform palermo df and save it as txt file
    task_transform_palermo_df=PythonOperator(
        task_id='transform_palermo_df',
        python_callable=transform_palermo_df,
        retries=5
    )
    
<<<<<<< HEAD
    task_get_palermo_info >> task_create_palermo_df >> task_transform_palermo_df
=======
        python_callable=create_palermo_df
=======
        python_callable=create_palermo_df,
        retries=5
>>>>>>> 699e9be (modified DAGs to run succesfully in airflow)
    )
    
    task_get_palermo_info >> task_create_palermo_df
>>>>>>> 3d393a9 (Included succesful extraction DAG (csv in files folder))
=======
    task_get_palermo_info >> task_create_palermo_df >> task_transform_palermo_df
>>>>>>> 0dd7351 (Update Group C DAGS to include transform functions)
>>>>>>> 3637f8b510fb2064abc850d1636e6c63dbc28a05
