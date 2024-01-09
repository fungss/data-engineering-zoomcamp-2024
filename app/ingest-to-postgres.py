from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from time import time
import pandas as pd
import argparse
import os
import gzip
import shutil

WDIR = os.path.abspath(os.path.dirname('__name__'))


def convert_datetime_field(df: pd.DataFrame) -> None:
    """
    for a general purpose pipeline
    """
    for col in df.columns:
        if "datetime" in col:
            df[col] = pd.to_datetime(df[col])


def download_and_unzip(target_url: str) -> None:
    target_filepath = os.path.join(WDIR, "data", "output")
    if not any(target_url.endswith(valid_ext) for valid_ext in ['.csv', '.gz']):
        raise ValueError("Expecting .csv or .gz files")
    if target_url.endswith('.gz'):
        target_filepath = target_filepath + ".gz"
        os.system(f"wget -O {target_filepath} {target_url}")
        with gzip.open(target_filepath, 'r') as file_in:
            with open(target_filepath.replace('.gz', '.csv'), 'wb') as file_out:
                shutil.copyfileobj(file_in, file_out)
    else:
        target_filepath = target_filepath + ".csv"
        os.system(f"wget -O {target_filepath} {target_url}")


def main(params):

    csv_name = 'output.csv'
    csv_path = os.path.join(WDIR, 'data', csv_name)

    try:
        postgres_engine = create_engine(
                    URL.create(
                        drivername="postgresql+psycopg2",
                        username=params.user,
                        password=params.password,
                        host=params.host,
                        port=params.port,
                        database=params.database_name,
                    )
                )

        # check connections
        postgres_engine.connect()

        download_and_unzip(params.target_url)

        # begin reading file and load to postgres
        total_rows_ingested = 0
        total_time_spent = 0
        df_head = pd.read_csv(csv_path, dtype=object, nrows=0)
        convert_datetime_field(df_head)
        df_head.to_sql(name=params.table_name, con=postgres_engine, index=False, if_exists="replace")
        df_iter = pd.read_csv(csv_path, dtype=object, chunksize=100000)
        for df_chunk in df_iter:
            print(f'inserting {df_chunk.shape[0]} rows of data...')
            t_start = time()
            convert_datetime_field(df_chunk)
            df_chunk.to_sql(name=params.table_name, con=postgres_engine, index=False, if_exists="append")
            t_end = time()
            total_rows_ingested += df_chunk.shape[0]
            total_time_spent += t_end - t_start
            print(
                f'insertion complete..., took {(t_end - t_start): .3f} seconds'
            )

        print('ingestion finished successfully')
        print(
            f'inserted {total_rows_ingested} rows. total time spent: {total_time_spent: .3f}.'
        )
    except Exception as e:
        print(e)


if __name__ == '__main__':
    """
    Passing crendentials as args should be replaced with
    secure alternative in production.
    """
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument('--user', help='username for POSTGRES_USER')
    parser.add_argument('--password', help='password for POSTGRES_PASSWORD')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database_name', help='database name for POSTGRES_DB')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--target_url', help='url of target csv/zip/gz file')

    args = parser.parse_args()

    main(args)
