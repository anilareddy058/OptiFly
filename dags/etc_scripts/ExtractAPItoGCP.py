import pytz
import requests
import pandas as pd
from google.cloud import storage
from io import BytesIO
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
import os


# Set the time zone to Mountain Time
mountain_time_zone = pytz.timezone('US/Mountain')


def extract_data_from_api(limit=100, order='flight_date'):
    """
    Function to extract data from aviationstack API.
    """
    api_url = 'http://api.aviationstack.com/v1/flights?access_key=76a435e8abdd004c583e728377bdef41'
    
    api_key = '76a435e8abdd004c583e728377bdef41'
    
    headers = { 
        'accept': "application/json", 
        'apikey': api_key,
    }
    

    params = {
        '$limit': str(limit),
        '$order': order,
    }

    api_response = requests.get(api_url, headers=headers, params=params)
    print("response : ", api_response)
    latest_data = api_response.json()
 
    return latest_data


def create_aviationstack_dataframe(data):

    # Normalize the JSON data to create a flat table
    df = pd.json_normalize(data['data'])
    # Define the columns you want to extract and rename
    columns_mapping = {
        'flight_date': 'flight_date',
        'flight_status': 'flight_status',
        'departure.airport': 'departure_airport',
        'departure.icao': 'departure_icao',
        'departure.delay': 'departure_delay',
        'departure.scheduled': 'departure_scheduled',
        'arrival.airport': 'arrival_airport',
        'arrival.icao': 'arrival_icao',
        'arrival.delay': 'arrival_delay',
        'arrival.scheduled': 'arrival_scheduled',
        'airline.name': 'airline_name',
        'airline.icao': 'airline_icao',
        'flight.icao': 'flight_icao'
    }

    # Select and rename the columns
    df_selected = df[list(columns_mapping.keys())].rename(columns=columns_mapping)

    # Create a DataFrame using the list of dictionaries
    df = pd.DataFrame(df_selected)

    return df


def upload_to_gcs(dataframe, bucket_name, file_path):
    """
    Upload a DataFrame to a Google Cloud Storage bucket using service account credentials.
    """
    print("Writing data to GCS.....")
    
    # Set GCS credentials from environment variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    
    # Convert DataFrame to Parquet format
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)
    # Reset the position of the buffer to the beginning
    parquet_buffer.seek(0)

    # Upload the Parquet file to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    file_path_formatted = file_path.format(current_date, current_date)
    
    blob = bucket.blob(file_path_formatted)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')

    #blob.upload_from_string(csv_df, content_type='text/csv')
    print(f"Completed writing data to GCS with date: {current_date}.")


def main():
    data_extracted = extract_data_from_api(limit=100, order='flight_date')
    aviationstack_data = create_aviationstack_dataframe(data_extracted)

    gcs_bucket_name = os.environ['GCS_AVI_BUCKET']
    gcs_file_path = 'data/{}/aviationstack_{}.parquet'

    upload_to_gcs(aviationstack_data, gcs_bucket_name, gcs_file_path)
    
