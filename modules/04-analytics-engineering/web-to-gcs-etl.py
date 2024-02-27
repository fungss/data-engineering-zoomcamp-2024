import os
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import re
import pyarrow as pa
import pyarrow.parquet as pq

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./.credentials/dtc-de-course-410021-60ea3845a080.json"
BUCKET = "module-4-bucket-3a24bd00550f1030"
WDIR = os.path.abspath(os.path.dirname(""))

base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

data_type_mapping = {
    'green': {
        'VendorID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'RatecodeID': pd.Int64Dtype(),
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'ehail_fee': pd.Float64Dtype(),
        'improvement_surcharge': float,
        'total_amount': float,
        'payment_type': pd.Int64Dtype(),
        'trip_type': pd.Int64Dtype(),
        'congestion_surcharge': float, 
        # 'lpep_pickup_datetime': pd.Int64Dtype(),
        # 'lpep_dropoff_datetime': pd.Int64Dtype()
    },
    'yellow': {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float,
        # 'tpep_pickup_datetime': pd.Int64Dtype(), 
        # 'tpep_dropoff_datetime': pd.Int64Dtype()
        # 'trip_type': pd.Int64Dtype(),
        # 'ehail_fee': float,
    },
    'fhv': {
        'dispatching_base_num': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'SR_Flag': pd.Int64Dtype(),
        # 'pickup_datetime': pd.Int64Dtype(),
        # 'dropOff_datetime': pd.Int64Dtype()
    }
}

parse_date_mapping = {
    'green': ['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
    'yellow': ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
    'fhv': ['pickup_datetime', 'dropOff_datetime']
}

# def pascal_to_snake(pascal_string):
#     # Insert underscores before uppercase letters (except before consecutive uppercase letters)
#     snake_case = re.sub(r'(?<=[a-z0-9])([A-Z])|([A-Z])(?=[a-z0-9])', r'_\1\2', pascal_string)
#     # Remove leading underscore and convert to lowercase
#     snake_case = snake_case.lstrip('_').lower()
#     return snake_case

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    target_months = [f"{i:02}" for i in range(1,13)]
    # target_months = [f"{i:02}" for i in range(1,2)]
    for month in target_months:

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # read it back into a parquet file
        # df = pd.read_csv(os.path.join(WDIR, "data", file_name), compression='gzip', dtype=data_type_mapping[service], parse_dates=parse_date_mapping[service])
        request_url = f"{base_url}/{service}/{file_name}"
        df = pd.read_csv(request_url, compression='gzip', dtype=data_type_mapping[service], parse_dates=parse_date_mapping[service], dtype_backend="pyarrow")
        # df = pd.read_csv(request_url, compression='gzip')
        # df.columns = [pascal_to_snake(col) for col in df.columns]

        file_name = file_name.replace('.csv.gz', '.parquet')
        # df.to_parquet(os.path.join(WDIR, "data", file_name), engine='pyarrow')
        # print(f"Parquet: {file_name}")
        
        root_path = f'{BUCKET}/bronze/ny_taxi/{service}/{file_name}'

        table = pa.Table.from_pandas(df)
        # print(table.schema)

        gcs = pa.fs.GcsFileSystem()

        pq.write_to_dataset(
            table,
            root_path=root_path,
            filesystem=gcs,
            coerce_timestamps='ms'
        )

        # upload it to gcs 
        # upload_to_gcs(BUCKET, f"bronze/{service}/{file_name}", os.path.join(WDIR, "data", file_name))
        print(f"GCS: {root_path}")

def create_external_table(vehicle_type, bucket_name) -> None:
    client = bigquery.Client()
    dataset_id = f"{client.project}.crude"
    table_id = f"{dataset_id}.ext_{vehicle_type}_tripdata"

    # job_config = bigquery.LoadJobConfig(
    #             schema=bq_schema[vehicle_type],
    #             source_format=bigquery.SourceFormat.PARQUET,
    #         )
    # job_config.autodetect=False
    # uri = f'gs://{bucket_name}/bronze/ny_taxi/{vehicle_type}/*.parquet'
    # load_job = client.load_table_from_uri(
    #     uri, table_id, job_config=job_config
    # )  # Make an API request.

    # load_job.result()  # Waits for the job to complete.

    # destination_table = client.get_table(table_id)
    # print("Loaded {} rows.".format(destination_table.num_rows))


    dataset_id = f"{client.project}.crude"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    dataset = client.create_dataset(dataset, timeout=30, exists_ok=True)  # Make an API request.

    # query to create external table
    table_id = f"{dataset_id}.ext_{vehicle_type}_tripdata"
    query = f"""
        -- Create external table from gcs path
        -- table_id = "your-project.your_dataset.your_table_name"
        CREATE OR REPLACE EXTERNAL TABLE `{table_id}`
        OPTIONS (
        format = 'Parquet',
        uris = ['gs://{bucket_name}/bronze/ny_taxi/{vehicle_type}/*.parquet']
        )
    """
    client.query_and_wait(query)
    
    try:
        client.get_table(table_id)
        print("Table {} is created.".format(table_id))
    except NotFound:
        print("Table {} is not found.".format(table_id))


if __name__ == '__main__':
    web_to_gcs('2019', 'green')
    web_to_gcs('2020', 'green')
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')
    web_to_gcs('2019', 'fhv')
    create_external_table('green', BUCKET)
    create_external_table('yellow', BUCKET)
    create_external_table('fhv', BUCKET)