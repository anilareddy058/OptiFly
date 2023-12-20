from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.functions import from_utc_timestamp, lit, date_format
from pyspark.sql.functions import col, round
import os


def transform_data(data):
    transformed_data = data

    # Drop null values
    transformed_data = transformed_data.dropna()

    # Split 'airport_name' into 'city', 'state', and 'airport_name'
    transformed_data = transformed_data.withColumn('city', F.split('airport_name', ', ')[0])
    transformed_data = transformed_data.withColumn('state_airport', F.split('airport_name', ', ')[1])
    transformed_data = transformed_data.withColumn('state', F.split('state_airport', ': ')[0])
    transformed_data = transformed_data.withColumn('airport_name', F.split('state_airport', ': ')[1])
    transformed_data = transformed_data.drop('state_airport')

    # Calculate 'ontime_per' using PySpark functions
    transformed_data = transformed_data.withColumn('ontime_per', ((F.col('arr_flights') - F.col('arr_del15')) * 100) / F.col('arr_flights'))
    # Calculate 'aircarrierdelay_per'
    transformed_data = transformed_data.withColumn('aircarrierdelay_per', (F.col('carrier_ct') * 100) / F.col('arr_flights'))
    # Calculate 'weatherdelay_per'
    transformed_data = transformed_data.withColumn('weatherdelay_per', (F.col('weather_ct') * 100) / F.col('arr_flights'))
    # Calculate 'nasdelay_per'
    transformed_data = transformed_data.withColumn('nasdelay_per', (F.col('nas_ct') * 100) / F.col('arr_flights'))
    # Calculate 'securitydelay_per'
    transformed_data = transformed_data.withColumn('securitydelay_per', (F.col('security_ct') * 100) / F.col('arr_flights'))
    # Calculate 'airlate_per'
    transformed_data = transformed_data.withColumn('airlate_per', (F.col('late_aircraft_ct') * 100) / F.col('arr_flights'))
    # Calculate 'cancelled_per'
    transformed_data = transformed_data.withColumn('cancelled_per', (F.col('arr_cancelled') * 100) / F.col('arr_flights'))
    # Calculate 'diverted_per'
    transformed_data = transformed_data.withColumn('diverted_per', (F.col('arr_diverted') * 100) / F.col('arr_flights'))

    # Round values in the 'ontime_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("ontime_per", round("ontime_per", 2))
    # Round values in the 'aircarrierdelay_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("aircarrierdelay_per", round("aircarrierdelay_per", 2))
    # Round values in the 'weatherdelay_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("weatherdelay_per", round("weatherdelay_per", 2))
    # Round values in the 'nasdelay_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("nasdelay_per", round("nasdelay_per", 2))
    # Round values in the 'securitydelay_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("securitydelay_per", round("securitydelay_per", 2))
    # Round values in the 'airlate_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("airlate_per", round("airlate_per", 2))
    # Round values in the 'cancelled_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("cancelled_per", round("cancelled_per", 2))
    # Round values in the 'diverted_per' column to 2 decimal places
    transformed_data = transformed_data.withColumn("diverted_per", round("diverted_per", 2))


    # Rename columns
    column_mapping = {
        'year': 'year',
        'month': 'month',
        'carrier': 'carrier',
        'carrier_name': 'carrier_name',
        'carrier_ct': 'carrier_ct',
        'carrier_delay': 'carrier_delay',
        'airport': 'airport',
        'airport_name': 'airport_name',
        'city': 'city',
        'state': 'state',
        'arr_flights': 'arr_flights',
        'arr_del15': 'arr_del15',
        'arr_cancelled': 'arr_cancelled',
        'arr_diverted': 'arr_diverted',
        'arr_delay': 'arr_delay',
        'nas_delay': 'nas_delay',
        'nas_ct': 'nas_ct',
        'security_ct': 'security_ct',
        'security_delay': 'security_delay',
        'weather_ct': 'weather_ct',
        'weather_delay': 'weather_delay',
        'late_aircraft_ct': 'late_aircraft_ct',
        'late_aircraft_delay': 'late_aircraft_delay',
        'ontime_per': 'ontime_per',
        'aircarrierdelay_per': 'aircarrierdelay_per',
        'weatherdelay_per': 'weatherdelay_per',
        'nasdelay_per': 'nasdelay_per',
        'securitydelay_per': 'securitydelay_per',
        'airlate_per': 'airlate_per',
        'cancelled_per': 'cancelled_per',
        'diverted_per': 'diverted_per'
    }

    for old_col, new_col in column_mapping.items():
        transformed_data = transformed_data.withColumnRenamed(old_col, new_col)

    # Show the transformed DataFrame
    transformed_data.show()

    return transformed_data

def prep_timeperiod_dim(data):
    print("Preparing Time Period Dimensions Table Data")
    
    # Convert pandas DataFrame to PySpark DataFrame
    timeperiod_dim_tab_data = data[['year', 'month']].dropDuplicates()
    
    # Add a new column 'time_key'
    timeperiod_dim_tab_data = timeperiod_dim_tab_data.withColumn('time_key', F.monotonically_increasing_id() + 1)
    
    return timeperiod_dim_tab_data

def prep_carrier_tab_dim(data):
    print("Preparing Carrier Dimensions Table Data")
    
    # Convert pandas DataFrame to PySpark DataFrame
    carrier_dim_tab_data = data[['carrier', 'carrier_name']].dropDuplicates()
    
    # Add a new column 'carrier_key'
    carrier_dim_tab_data = carrier_dim_tab_data.withColumn('carrier_key', F.monotonically_increasing_id() + 1)
    
    return carrier_dim_tab_data

def prep_airport_tab_dim(data):
    print("Preparing Airport Dimensions Table Data")
    
    # Convert pandas DataFrame to PySpark DataFrame
    airport_dim_tab_data = data[['airport', 'airport_name']].dropDuplicates()
    
    # Add a new column 'airport_key'
    airport_dim_tab_data = airport_dim_tab_data.withColumn('airport_key', F.monotonically_increasing_id() + 1)
    
    return airport_dim_tab_data

def prep_state_tab_dim(data):
    print("Preparing State Dimensions Table Data")
    
    # Convert pandas DataFrame to PySpark DataFrame
    state_dim_tab_data = data[['state']].dropDuplicates()
    
    return state_dim_tab_data

def prep_city_tab_dim(data):
    print("Preparing City Dimensions Table Data")
    
    # Convert pandas DataFrame to PySpark DataFrame
    city_dim_tab_data = data[['city']].dropDuplicates()
    
    return city_dim_tab_data

def prep_flight_fct_tab(rawdata, dim_timeper_tab, carrier_tab_dim,
                         airport_tab_dim, state_tab_dim, city_tab_dim):
    
    print("Preparing Flight Fact Table Data")
    
    # Convert pandas DataFrames to PySpark DataFrames
    rawdata_spark = rawdata
    dim_timeper_tab_spark = dim_timeper_tab
    carrier_tab_dim_spark = carrier_tab_dim
    airport_tab_dim_spark = airport_tab_dim
    state_tab_dim_spark = state_tab_dim
    city_tab_dim_spark = city_tab_dim

    # Join tables
    df_fact = rawdata_spark.join(dim_timeper_tab_spark, ['month', 'year'], 'inner')
    df_fact = df_fact.join(carrier_tab_dim_spark, ['carrier', 'carrier_name'], 'inner')
    df_fact = df_fact.join(airport_tab_dim_spark, ['airport', 'airport_name'], 'inner')
    df_fact = df_fact.join(state_tab_dim_spark, ['state'], 'inner')
    df_fact = df_fact.join(city_tab_dim_spark, ['city'], 'inner')

    # Select columns
    df_fact_trimmed = df_fact.select('time_key', 'carrier_key', 'carrier_ct', 'carrier_delay', 
                                     'airport_key', 'state', 'city', 'nas_ct', 'nas_delay', 
                                     'arr_flights', 'arr_del15', 'arr_cancelled', 'arr_diverted', 
                                     'arr_delay', 'security_ct', 'security_delay', 'weather_ct', 
                                     'weather_delay', 'late_aircraft_ct', 'late_aircraft_delay',
                                     'ontime_per', 'aircarrierdelay_per', 'weatherdelay_per',
                                     'nasdelay_per', 'securitydelay_per', 'airlate_per',
                                     'cancelled_per', 'diverted_per')


    return df_fact_trimmed


def transform_data_from_gcs_and_load_to_gcs(gcs_bucket):

    # Initialize Spark session
    spark = SparkSession.builder.appName("GCSParquetReadWrite").getOrCreate()

    # Set the timezone to MST
    spark.conf.set("spark.sql.session.timeZone", "MST")

    # Get the current date in MST timezone
    current_date_mst = spark.sql("SELECT current_date() as current_date").collect()[0]["current_date"]

    # Format the date as needed
    formatted_date = current_date_mst.strftime("%Y-%m-%d")


    gcs_file_path = f'gs://{gcs_bucket}/data/bts_{formatted_date}.parquet'

    # Read Parquet file from GCS
    df = spark.read.parquet(gcs_file_path)

    rawdata = transform_data(df)

    dim_timeper_tab = prep_timeperiod_dim(rawdata)
    carrier_tab_dim = prep_carrier_tab_dim(rawdata)
    airport_tab_dim = prep_airport_tab_dim(rawdata)
    state_tab_dim = prep_state_tab_dim(rawdata)
    city_tab_dim = prep_city_tab_dim(rawdata)


    # Call the method
    result_flight_fct_tab = prep_flight_fct_tab(rawdata, dim_timeper_tab, carrier_tab_dim,
                                                airport_tab_dim, state_tab_dim, city_tab_dim)
    

    dim_timeper_tab_path = f'gs://{gcs_bucket}/transformed_data/dim_timeper_tab'
    carrier_tab_dim_path = f'gs://{gcs_bucket}/transformed_data/carrier_tab_dim'
    airport_tab_dim_path = f'gs://{gcs_bucket}/transformed_data/airport_tab_dim'
    state_tab_dim_path = f'gs://{gcs_bucket}/transformed_data/state_tab_dim'
    city_tab_dim_path = f'gs://{gcs_bucket}/transformed_data/city_tab_dim'
    flight_fact_tab_path = f'gs://{gcs_bucket}/transformed_data/flight_fact_tab'

    # Write the transformed DataFrame back to GCS as Parquet
    dim_timeper_tab.write.mode("overwrite").parquet(dim_timeper_tab_path)
    carrier_tab_dim.write.mode("overwrite").parquet(carrier_tab_dim_path)
    airport_tab_dim.write.mode("overwrite").parquet(airport_tab_dim_path)
    state_tab_dim.write.mode("overwrite").parquet(state_tab_dim_path)
    city_tab_dim.write.mode("overwrite").parquet(city_tab_dim_path)
    result_flight_fct_tab.write.mode("overwrite").parquet(flight_fact_tab_path)


if __name__ == '__main__':
    gcs_bucket = 'dc_bts_project'
    transform_data_from_gcs_and_load_to_gcs(gcs_bucket)
