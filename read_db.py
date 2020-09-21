import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sys

def create_db_engine():
# django database settings
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': '****',
        'USER': 'postgres',
        'PASSWORD': '****',
        'HOST': '127.0.0.1',
        'PORT': '5432'
    },
}

# choose the database
db = DATABASES['default']

# create engine connection string
engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
    user = db['USER'],
    password = db['PASSWORD'],
    host = db['HOST'],
    port = db['PORT'],
    database = db['NAME'],
)

# create sqlalchemy engine
engine = create_engine(engine_string)

  return engine

def read_from_db(engine,query):

    db_engine = create_db_engine()
    dbConnection    = db_engine.connect()
#    query_1 = "select cause from "+"\""+query+"\""
#    query = "select cause from \"earthquake\""
#    query2 = "select * from \"earthquake\""

    # To read full table
    df = pd.read_sql_table('tablename',engine)
    # To query
    df3 = pd.read_sql(query,engine)
    return df2

def write_to_db(engine,table_name,table_PK,df_data,if_table_exist='append'):

    df_data = df_data.set_index(table_PK)

    #Append to existing data
    if(if_table_exist='append'):
        frame = df_data.to_sql(table_name, engine, if_exists='append')

    #Replace existing data
    if(if_table_exist='replace'):
        frame = df_data.to_sql(table_name, engine, if_exists='replace')

    #Fail if table already exists
    if(if_table_exist='fail'):
        frame = df_data.to_sql(table_name, engine, if_exists='fail')

    # Create table
    if(if_table_exist='false'):
        frame = df_data.to_sql(table_name, engine)



    return

def main():

    # Read from a csv
    df = pd.read_csv('earthquake.csv')

    #Wring the above read dataframe into postgres DATABASES

    #create engine_string
    engine = create_db_engine()
    #Write the df into 'Earthquake' table name
    write_to_db(engine,'Earthquake','earthquake_id',df,if_table_exist='append')

    #Read data from postgres db, read only 'cause column' into dataframe:
    df2 = read_from_db(engine,"select cause from \"earthquake\"")
    return

if __name__ == '__main__':
    main()
