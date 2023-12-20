import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from io import BytesIO
import pyarrow.parquet as pq
import os

from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')



def getdata(gcp_bucket):
    gcp_filepath = 'data/{}/aviationstack_{}.parquet'
    #gcp_filepath = 'data/bts_{}.csv'


    # Set GCS credentials from environment variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    client = storage.Client()
    
    bucket_id = client.bucket(gcp_bucket)
    
    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    
    # Format the file path with the current date
    formatted_file_path = gcp_filepath.format(current_date, current_date)
    
    # Read the parquet file from GCP into a DataFrame
    blob = bucket_id.blob(formatted_file_path)
    parquet_data = BytesIO(blob.download_as_bytes())
    #csv_file = blob.download_as_text()

    # Read the Parquet file into a DataFrame
    data = pd.read_parquet(parquet_data)
    #data = pd.read_csv(StringIO(csv_file))

    return data


def write_data_to_gcs(dataframe, bucket_name, file_path):
    print(f"Writing data to GCS.....")

    # Set GCS credentials from environment variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    client = storage.Client()

    #csv_data = dataframe.to_csv(index=False)
    # Convert DataFrame to Parquet format
    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)

    # Reset the position of the buffer to the beginning
    parquet_buffer.seek(0)
    
    bucket = client.get_bucket(bucket_name)
    
    #  current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    # formatted_file_path = file_path.format(current_date, current_date)
    
    blob = bucket.blob(file_path)
    #blob.upload_from_string(csv_data, content_type='text/csv')
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    print(f"Finished writing data to GCS.")


def prep_flight_tab_dim(data):
    print("Preparing Flight Dimensions Table Data")
    # extract columns only relevant to animal dim
    flight_dim_tab_data = data[['flight_date','flight_status','flight_icao']].drop_duplicates()
    flight_dim_tab_data['flight_key'] = range(1, len(flight_dim_tab_data) + 1)
    return flight_dim_tab_data


def prep_dep_tab_dim(data):
    dep_dim_tab_data = data[['departure_airport', 'departure_icao','departure_delay','departure_scheduled']].drop_duplicates()
    dep_dim_tab_data['dep_key'] = range(1, len(dep_dim_tab_data) + 1)
    return dep_dim_tab_data

def prep_av_arr_tab_dim(data):
    av_arr_dim_tab_data = data[['arrival_airport', 'arrival_icao','arrival_delay','arrival_scheduled']].drop_duplicates()
    av_arr_dim_tab_data['arr_key'] = range(1, len(av_arr_dim_tab_data) + 1)
    return av_arr_dim_tab_data

def prep_airline_tab_dim(data):
    airline_dim_tab_data = data[['airline_name', 'airline_icao']].drop_duplicates()
    airline_dim_tab_data['airline_key'] = range(1, len(airline_dim_tab_data) + 1)
    return airline_dim_tab_data




def prep_aviation_fct_tab(rawdata,flight_tab_dim,dep_tab_dim,av_arr_tab_dim,
                                              airline_tab_dim):
     # Create or append data to the Flight table, linking to dimension tables
    df_fact = rawdata.merge(flight_tab_dim, how='inner', left_on=['flight_date','flight_status','flight_icao'], right_on=['flight_date','flight_status','flight_icao'])
    df_fact = df_fact.merge(dep_tab_dim, how='inner', left_on=['departure_airport', 'departure_icao','departure_delay','departure_scheduled'], right_on=['departure_airport', 'departure_icao','departure_delay','departure_scheduled'])
    df_fact = df_fact.merge(av_arr_tab_dim, how='inner', left_on=['arrival_airport', 'arrival_icao','arrival_delay','arrival_scheduled'], right_on=['arrival_airport', 'arrival_icao','arrival_delay','arrival_scheduled'])
    df_fact = df_fact.merge(airline_tab_dim, how='inner', left_on=['airline_name', 'airline_icao'], right_on=['airline_name', 'airline_icao'])
  
    return df_fact

def transform(data):

    transformed_data = data.copy()

    # Convert nulls to zero
    transformed_data['departure_delay'].fillna(0, inplace=True)
    transformed_data['arrival_delay'].fillna(0, inplace=True)

    # Remove timestamp and zones from dates
    transformed_data['departure_scheduled'] = pd.to_datetime(transformed_data['departure_scheduled']).dt.date
    transformed_data['arrival_scheduled'] = pd.to_datetime(transformed_data['arrival_scheduled']).dt.date

    # Replace empty airline names with "Unknown"
    transformed_data['airline_name'].replace('', 'Unknown', inplace=True)
    transformed_data['airline_name'].replace('empty', 'Unknown', inplace=True)
    
    # Replace empty with "Unknown"
    transformed_data['flight_icao'].replace('', 'Unknown', inplace=True)
    transformed_data['arrival_icao'].replace('', 'Unknown', inplace=True)
    transformed_data['departure_icao'].replace('', 'Unknown', inplace=True)
    
    # Remove special characters from Airport names
    transformed_data['departure_airport'] = transformed_data['departure_airport'].str.encode('ascii', 'ignore').str.decode('ascii')
    transformed_data['arrival_airport'] = transformed_data['arrival_airport'].str.encode('ascii', 'ignore').str.decode('ascii')

    # Remove rows where Status,arrival_airport,departure_airport is 'unknown' & blanks
    
    
    # Remove rows with 'unknown' in 'flight_status' column
    transformed_data= transformed_data[transformed_data['flight_status'] != 'unknown']

    # Remove rows with 'unknown' or NaN in 'departure_airport' column
    transformed_data = transformed_data.dropna(subset=['departure_airport'])

    # Remove rows with 'unknown' or NaN in 'arrival_airport' column
    transformed_data = transformed_data.dropna(subset=['arrival_airport'])

    # Reset index after dropping rows
    transformed_data = transformed_data.reset_index(drop=True)

    mapping = {
    'flight_date': 'flight_date',
    'flight_status': 'flight_status',
    'departure_airport': 'departure_airport',
    'departure_icao': 'departure_icao',
    'departure_delay': 'departure_delay',
    'departure_scheduled': 'departure_scheduled',
    'arrival_airport': 'arrival_airport',
    'arrival_icao': 'arrival_icao',
    'arrival_delay':'arrival_delay',
    'arrival_scheduled':'arrival_scheduled',
    'airline_name': 'airline_name',
    'airline_icao': 'airline_icao',
    'flight_icao': 'flight_icao'
    }
    transformed_data.rename(columns=mapping, inplace=True)

    return transformed_data


def transform_data():

    bucket = os.environ['GCS_AVI_BUCKET']

    rawdata = getdata(bucket)
    
    rawdata = transform(rawdata)


    flight_tab_dim = prep_flight_tab_dim(rawdata)
    dep_tab_dim = prep_dep_tab_dim(rawdata)
    av_arr_tab_dim = prep_av_arr_tab_dim(rawdata)
    airline_tab_dim = prep_airline_tab_dim(rawdata)

    aviation_fact_tab = prep_aviation_fct_tab(rawdata,flight_tab_dim,dep_tab_dim,av_arr_tab_dim,
                                              airline_tab_dim)

    flight_tab_dim_path = "transformed_data/flight_tab_dim.parquet"
    dep_tab_dim_path = "transformed_data/dep_tab_dim.parquet"
    av_arr_tab_dim_path = "transformed_data/av_arr_tab_dim.parquet"
    airline_tab_dim_path = "transformed_data/airline_tab_dim.parquet"
    aviation_fact_tab_path = "transformed_data/aviation_fact_tab.parquet"
    rawdata_tab_path = "transformed_data/rawdata_tab.parquet"
 

    write_data_to_gcs(flight_tab_dim, bucket, flight_tab_dim_path)
    write_data_to_gcs(dep_tab_dim, bucket, dep_tab_dim_path)
    write_data_to_gcs(av_arr_tab_dim, bucket, av_arr_tab_dim_path)
    write_data_to_gcs(airline_tab_dim, bucket, airline_tab_dim_path)
    write_data_to_gcs(aviation_fact_tab, bucket, aviation_fact_tab_path)

    write_data_to_gcs(rawdata, bucket, rawdata_tab_path)