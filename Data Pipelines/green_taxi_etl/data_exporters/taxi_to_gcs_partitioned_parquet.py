from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

import os
import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/stone-passage-413413-da75581e05e2.json'
config_path = path.join(get_repo_path(), 'io_config.yaml')
config_profile = 'default'

bucket_name = 'mage-zoomcamp-sarath-ck'
project_id = 'stone-passage-413413'
table_name = 'nyc-taxi-green-data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(table, 
                        root_path=root_path, 
                        partition_cols=['lpep_pickup_date'],
                        filesystem=gcs)