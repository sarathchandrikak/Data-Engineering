import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import io
import requests
import pandas as pd
from google.cloud import storage


BUCKET = os.environ.get("GCP_GCS_BUCKET", "data-warehouse-ip")


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


# Function to download a file from a URL
def download_file(url, directory):
    # Get the filename from the URL
    filename = os.path.join(directory, os.path.basename(url))

    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Write the content to a local file
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"File downloaded: {filename}")
    else:
        print(f"Failed to download file from {url}. Status code: {response.status_code}")
    return filename

url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

target_string = ["yellow_tripdata_2019", "yellow_tripdata_2020"]


# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the HTML content of the webpage
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all <a> tags containing links
    links = soup.find_all('a', href=True)

    # Directory to save downloaded files
    download_directory = "downloads"

    # Create the download directory if it doesn't exist
    os.makedirs(download_directory, exist_ok=True)
    folder = "yellow"

    # Iterate over each link
    for link in links:
        # Get the absolute URL of the link
        absolute_url = urljoin(url, link['href']) 

        # Download the file if it has a file extension (e.g., .pdf, .xlsx)
        for t_str in target_string:
            if absolute_url.endswith(('.parquet')) and t_str in absolute_url:

                downloaded_filename = download_file(absolute_url, download_directory)
                if downloaded_filename is not None:
                    # upload it to gcs 
                    upload_to_gcs(BUCKET, f"{folder}/{downloaded_filename}", downloaded_filename)
                    print(f"GCS: {folder}/{downloaded_filename}")
else:
    print(f"Failed to retrieve webpage. Status code: {response.status_code}")

