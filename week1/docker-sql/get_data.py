#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from sqlalchemy import create_engine
import os
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    database = params.db
    port = params.port
    table_name = params.table_name
    url = params.url

    csv_name = "output.parquet"
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    df = pd.read_parquet(csv_name)

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    df.head(0).to_sql(con=engine, name=table_name, if_exists='replace', index=False)
    now = time()

    df.to_sql(con=engine, name=table_name, if_exists='append', index=False)

    print(f"Finished ingesting data, took {time() - now} seconds")

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
