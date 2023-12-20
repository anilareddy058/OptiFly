import os
import sys
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from etl_helpers.TransformAPIData import transform_data
from etl_helpers.ExtractAPItoGCP import main


default_args = {
    "owner": "OptiFly",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


with DAG(
    dag_id="aviationstack_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
        start = BashOperator(task_id = "START",
                             bash_command = "echo start")

        # copy_creds = BashOperator(task_id = "COPY_CREDS", bash_command = "echo start")

        extract_data_from_api_to_gcp =  PythonOperator(task_id = "EXTRACT_DATA_FROM_API_TO_GCP",
                                                  python_callable = main,)
        
        transform_data_from_api_to_gcp_step = PythonOperator(task_id="TRANSFORM_DATA_FROM_API_TO_GCP",
                                              python_callable=transform_data,)
        
        end = BashOperator(task_id = "END", bash_command = "echo end")

        start >> extract_data_from_api_to_gcp >> transform_data_from_api_to_gcp_step

        for table in ["flight_tab_dim", "dep_tab_dim", "av_arr_tab_dim", "airline_tab_dim"]:
        #for table in ["dim_timeper_tab", "carrier_tab_dim"]:    
            create_av_table = GCSToBigQueryOperator(
            task_id=f"create_av_{table}",
            destination_project_dataset_table=f"winter-joy-406123.bts.{table}",
            bucket='dc_aviationstack_project',
            source_objects=[f"transformed_data/{table}.parquet"],
            source_format='PARQUET',
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )
            

        #  start >> extract_data_from_api_to_gcp >> transform_data_step >> 
        # [load_dim_animals, load_dim_outcome_types, load_dim_dates, load_fct_outcomes] >> end
        #start >> extract_data_from_api_to_gcp >> transform_data_from_gcp_step >> [load_dim_animals_tab, load_dim_outcome_types_tab, load_dim_dates_tab] >> load_fct_outcomes_tab >> end
        #start >> extract_data_from_api_to_gcp >> transform_data_from_gcp_step >> drop_existing_table_data >> load_dim_animals_tab >> load_dim_outcome_types_tab >> load_dim_dates_tab >> load_fct_outcomes_tab >> end
            transform_data_from_api_to_gcp_step >> create_av_table


        for table in ["aviation_fact_tab"]:
        #for table in ["dim_timeper_tab", "carrier_tab_dim"]:    
            create_av_fact_table = GCSToBigQueryOperator(
            task_id=f"create_av_{table}",
            destination_project_dataset_table=f"winter-joy-406123.bts.{table}",
            bucket='dc_aviationstack_project',
            source_objects=[f"transformed_data/{table}.parquet"],
            source_format='PARQUET',
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )
            
        create_av_table >> create_av_fact_table >> end