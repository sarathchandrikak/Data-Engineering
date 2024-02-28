
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import io
import requests
import pandas as pd
from google.cloud import storage


BUCKET = os.environ.get("GCP_GCS_BUCKET", "data-warehouse-ip")


# def upload_to_gcs(bucket, object_name, local_file):
#     """
#     Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
#     """
#     # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
#     # # (Ref: https://github.com/googleapis/python-storage/issues/74)
#     # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
#     # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

#     client = storage.Client()
#     bucket = client.bucket(bucket)
#     blob = bucket.blob(object_name)
#     blob.upload_from_filename(local_file)


# target_string = ["green_tripdata_2019", "green_tripdata_2020"]

# # Directory to save downloaded files
# folder = "downloads"

from google.cloud import storage
import os

def upload_files_to_gcs(bucket_name, folder_path):
    # Initialize the GCS client
    client = storage.Client()

    # Get the bucket
    bucket = client.bucket(bucket_name)

    # List all files in the local folder
    for file_name in os.listdir(folder_path):
        local_file_path = os.path.join(folder_path, file_name)
        
        if os.path.isfile(local_file_path):
            # Define the destination path within GCS bucket
            destination_blob_name = f"{file_name}"

            # Upload the file to GCS
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            print(f"File {local_file_path} uploaded to GCS as {destination_blob_name}")

# Specify your GCS bucket name
bucket_name = 'data-warehouse-ip'

# Specify the local folder path containing files to upload
local_folder_path = '/Users/sarathchandrika/Documents/Data-Engineering/Analytics Engineering/downloads/'

# Upload files to GCS
upload_files_to_gcs(bucket_name, local_folder_path)
