#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from sqlalchemy import create_engine
import os
from time import time


def process_csv(table_name, csv_name, engine):
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        try:
            t_start = time()
                
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

def process_parquet(table_name, csv_name, engine):
    df = pd.read_parquet(csv_name)

    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    df.head(0).to_sql(con=engine, name=table_name, if_exists='replace', index=False)
    now = time()

    df.to_sql(con=engine, name=table_name, if_exists='append', index=False)

    print(f"Finished ingesting data, took {time() - now} seconds")


def main(params):
    user = params.user
    password = params.password
    host = params.host
    database = params.db
    port = params.port
    table_name = params.table_name
    url = params.url
    parquet = False

    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    elif url.endswith(".parquet"):
        csv_name = "output.parquet"
        parquet = True
    elif url.endswith(".csv"):
        csv_name = "output.csv"
    else:
        raise ValueError("Unknown file type")
    
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    if not parquet:
        process_csv(table_name, csv_name, engine)      
    else:
        process_parquet(table_name, csv_name, engine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest parquet data to postgres")

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--table_name', help='postgres table_name to write results to')
    parser.add_argument('--url', help='url of csv file')

    args = parser.parse_args()

    main(args)
