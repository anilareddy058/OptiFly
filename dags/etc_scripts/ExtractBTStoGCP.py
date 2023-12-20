import pytz
import pandas as pd
from google.cloud import storage
from io import BytesIO
from datetime import datetime
import pyarrow.parquet as pq
import os

# Set the time zone to Mountain Time
mountain_time_zone = pytz.timezone('US/Mountain')

def read_parquet_and_upload_to_gcs(bucket_name, gcs_file_path):
    # Read CSV file into a Pandas DataFrame
    df = pd.read_csv('https://drive.google.com/uc?export=download&id=1y2ezfxWWvTzixsTg5jwNFfyQzvxDMoAU')

    # Set GCS credentials from environment variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")

    # Convert DataFrame to Parquet format
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Reset the position of the buffer to the beginning
    parquet_buffer.seek(0)


    # Upload the Parquet file to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Format the file path with the current date
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    gcs_file_path_formatted = gcs_file_path.format(current_date, current_date)

    # Upload the CSV file to GCS
    blob = bucket.blob(gcs_file_path_formatted)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')

    #print(f"CSV file uploaded to GCS: {gcs_file_path_formatted}")
    print(f"Parquet file uploaded to GCS: {gcs_file_path_formatted}")

def main():
    gcs_bucket_name = os.environ['GCS_BTS_BUCKET']
    gcs_file_path = 'data/bts_{}.parquet'

    read_parquet_and_upload_to_gcs(gcs_bucket_name, gcs_file_path)
