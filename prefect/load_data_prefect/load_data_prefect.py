import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    # Download the CSV file using wget
    os.system(f"wget {url} -O {csv_name}")

    # Read the CSV file in chunks
    data_iter = pd.read_csv(csv_name, iterator=True, compression='gzip')

    # Get the first chunk of data
    data = next(data_iter)

    # Convert datetime columns to datetime objects
    data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
    data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])

    return data

@task(log_prints=True)
def transform_data(data):
    print(f"pre: missing passenger count: {data['passenger_count'].isin([0]).sum()}")
    data = data[data['passenger_count'] != 0]
    print(f"post: missing passenger count: {data['passenger_count'].isin([0]).sum()}")
    return data

@task(log_prints=True, retries=3)
def load_data(user, password, host, port, db, table_name, data):

    # Create a PostgreSQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Create the table in the database
    data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert the first chunk of data into the table
    data.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest flow")
def main_flow(table_name: str):
    user= "root" 
    password="root" 
    host="localhost" 
    port="5432" 
    db="ny_taxi" 
    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    raw_data = extract_data(url)    
    log_subflow(table_name)
    data = transform_data(raw_data)
    load_data(user, password, host, port, db, table_name, data)

if  __name__ == '__main__':
    main_flow("yellow_taxi_trip")