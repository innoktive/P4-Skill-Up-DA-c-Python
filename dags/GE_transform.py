
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

################################################################################
def transform_GEUNNacionalPampa(df, postal_codes):
    df['university'] = df['university'].str.replace('-', ' ').str.lower()

    df['career'] = df['career'].str.replace('-', ' ').str.lower()

    df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%d/%m/%Y')

    reg_ex = '(mr. )|(mrs. )|(dr. )|(miss )|(ms. )|( jr.)|( phd)|( dvm)|( ii)|( iv)|( md)|( dds)|( mhd)'
    df['name'] = df['name'].str.lower().str.replace(reg_ex, '', regex=True)
    name = df['name'].str.split(' ', expand=True)
    name.columns = ['first_name', 'last_name']
    idx = name['last_name'].isnull()
    name.loc[idx, 'last_name'] = name.loc[idx, 'first_name']
    name.loc[idx, 'first_name'] = ''
    df.drop(columns='name', inplace=True)
    df['first_name'] = name['first_name']
    df['last_name'] = name['last_name']

    df['gender'] = df['gender'].str.lower().map(lambda x: 'male' if x == 'm' else 'female')

    df['age'] = pd.to_datetime(df['birth_date'], format='%d/%m/%Y')\
                .map(lambda x: relativedelta(date.today(), x).years if x <= pd.Timestamp('now') else 0)
    df.drop(columns='birth_date', inplace=True)

    postal_codes_dict = dict(zip(postal_codes['postal_code'], postal_codes['location']))
    df['location'] = df['postal_code'].map(postal_codes_dict)

    df['email'] = df['email'].str.lower()


################################################################################
def transform_GEUNAbiertaInteramericana(df, postal_codes):
    df['university'] = df['university'].str.replace('-', ' ').str.lower()

    df['career'] = df['career'].str.replace('-', ' ').str.lower()

    df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%y/%b/%d')

    df['name'] = df['name'].str.lower().str.replace('(dr.-)|(-md)', ' ', regex=True)
    name = df['name'].str.split('-', expand=True)
    name.columns = ['first_name', 'last_name']
    idx = name['last_name'].isnull()
    name.loc[idx, 'last_name'] = name.loc[idx, 'first_name']
    name.loc[idx, 'first_name'] = ''
    df.drop(columns='name', inplace=True)
    df['first_name'] = name['first_name']
    df['last_name'] = name['last_name']

    df['gender'] = df['gender'].str.lower().map(lambda x: 'male' if x == 'm' else 'female')

    df['age'] = pd.to_datetime(df['birth_date'], format='%y/%b/%d')\
                .map(lambda x: relativedelta(date.today(), x).years if x <= pd.Timestamp('now') else 0)
    df.drop(columns='birth_date', inplace=True)

    df['location'] = df['location'].str.lower().str.replace('-', ' ')

    postal_codes_dict = dict(zip(postal_codes['location'], postal_codes['postal_code']))
    df['postal_code'] = df['location'].map(postal_codes_dict)

    df['email'] = df['email'].str.lower()
