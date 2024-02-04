import pyarrow as pa
import pyarrow.parquet as pq
import os

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ""


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """


    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'dev'

    bucket_name = 'module-2-bucket-636e6c4b0a8b7d88'
    project_id = 'dtc-de-course-410021'

    table_name = 'nyc_taxi_data_1'
    # object_key = 'nyc_taxi_data.parquet'

    root_path = f'{bucket_name}/{table_name}'

    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    # GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
    #     df,
    #     bucket_name,
    #     object_key,
    # )

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )

    print(f"Expected number of partitions: {df.lpep_pickup_date.nunique()}")
    print(f"Exported {len(pq.ParquetDataset(root_path, filesystem=gcs).files)} files to gcs")
    # print(f"Exported files: {pq.ParquetDataset(root_path, filesystem=gcs).files}")
