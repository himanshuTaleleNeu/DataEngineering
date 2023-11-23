# #!/usr/bin/env python
# # coding: utf-8

# import os
# import argparse
# import pandas as pd
# from sqlalchemy import create_engine
# from time import time


# def main(params):
#     user = params.user
#     password = params.password
#     host = params.host
#     port = params.port
#     db = params.db
#     table_name = params.table_name
#     url = params.url

#     csv_name = 'output.csv'

#     os.system(f"wget {url} -O {csv_name}")

# # engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')



#     data_iter = pd.read_csv(csv_name, iterator= True , chunksize= 100000, compression='gzip')

#     data = next(data_iter)

#     data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
#     data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)


#     data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

#     data.to_sql(name=table_name, con=engine, if_exists='append')
        
#     try:
#         while True:
#             t_start = time()

#             data = next(data_iter)

#             data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
#             data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)

#             data.to_sql(name=table_name, con=engine, if_exists='append')

#             t_end = time()

#             print('Chunk inserted and took %.3f Second' % (t_end - t_start))

#     except StopIteration:
#         print('End of iteration')
   
# if __name__ == '__main__':

#     parser = argparse.ArgumentParser(description='Injest NYC data into Postgres DB.')
#     parser.add_argument('--user', help="user name for postgres")
#     parser.add_argument('--password', help="password for postgres")
#     parser.add_argument('--host', help="host for postgres")
#     parser.add_argument('--port', help="port for postgres")
#     parser.add_argument('--db', help="database name for postgres")
#     parser.add_argument('--table_name',help="name of the table where we will write the results to")
#     parser.add_argument('--url', help="url of the CSV")

#     args = parser.parse_args()

#     main(args)

#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    # Extract parameters from the argparse namespace
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    # Download the CSV file using wget
    os.system(f"wget {url} -O {csv_name}")

    # Create a PostgreSQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the CSV file in chunks
    data_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')

    # Get the first chunk of data
    data = next(data_iter)

    # Convert datetime columns to datetime objects
    data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
    data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])

    # Create the table in the database
    data.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert the first chunk of data into the table
    data.to_sql(name=table_name, con=engine, if_exists='append')

    try:
        while True:
            # For subsequent chunks, convert datetime columns and insert into the table
            t_start = time()
            data = next(data_iter)
            data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
            data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])
            data.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('Chunk inserted and took %.3f seconds' % (t_end - t_start))

    except StopIteration:
        print('End of iteration')

if __name__ == '__main__':
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Ingest NYC data into Postgres DB.')
    parser.add_argument('--user', help="user name for postgres", required=True)
    parser.add_argument('--password', help="password for postgres", required=True)
    parser.add_argument('--host', help="host for postgres", required=True)
    parser.add_argument('--port', help="port for postgres", required=True)
    parser.add_argument('--db', help="database name for postgres", required=True)
    parser.add_argument('--table_name', help="name of the table where we will write the results to", required=True)
    parser.add_argument('--url', help="url of the CSV", required=True)

    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args)
