from airflow import models
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from etl_helpers.ExtractBTStoGCP import main
import os

default_args = {
    "owner": "OptiFly",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}

CLUSTER_NAME=os.environ['CLUSTER_NAME']
REGION=os.environ['REGION']
PROJECT_ID=os.environ['GCP_PROJECT_ID']
PYSPARK_URI=os.environ['PYSPARK_URI'] # spark job location in cloud storage


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with models.DAG(
    "dataproc_airflow_gcp_to_gbq",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc_airflow"],
) as dag:
    
    start = BashOperator(task_id = "START",
                             bash_command = "echo start")
        
    extract_data_from_bts_to_gcp =  PythonOperator(task_id = "EXTRACT_DATA_FROM_BTS_TO_GCP",
                                                python_callable = main,)
    
    end = BashOperator(task_id = "END", bash_command = "echo end")

    start >> extract_data_from_bts_to_gcp

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    extract_data_from_bts_to_gcp >> create_cluster >> submit_job >> delete_cluster

    create_dim_timeper_tab_table = GCSToBigQueryOperator(
        task_id=f"create_bq_dim_timeper_tab",
        destination_project_dataset_table=f"{PROJECT_ID}.bts.dim_timeper_tab",
        bucket='dc_bts_project',
        source_objects=[f"transformed_data/dim_timeper_tab/*.snappy.parquet"],
        source_format='PARQUET',
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    create_carrier_tab_dim_table = GCSToBigQueryOperator(
        task_id=f"create_bq_carrier_tab_dim",
        destination_project_dataset_table=f"{PROJECT_ID}.bts.carrier_tab_dim",
        bucket='dc_bts_project',
        source_objects=[f"transformed_data/carrier_tab_dim/*.snappy.parquet"],
        source_format='PARQUET',
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    create_airport_tab_dim_table = GCSToBigQueryOperator(
        task_id=f"create_bq_airport_tab_dim",
        destination_project_dataset_table=f"{PROJECT_ID}.bts.airport_tab_dim",
        bucket='dc_bts_project',
        source_objects=[f"transformed_data/airport_tab_dim/*.snappy.parquet"],
        source_format='PARQUET',
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    create_state_tab_dim_table = GCSToBigQueryOperator(
        task_id=f"create_bq_state_tab_dim",
        destination_project_dataset_table=f"{PROJECT_ID}.bts.state_tab_dim",
        bucket='dc_bts_project',
        source_objects=[f"transformed_data/state_tab_dim/*.snappy.parquet"],
        source_format='PARQUET',
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    create_city_tab_dim_table = GCSToBigQueryOperator(
        task_id=f"create_bq_city_tab_dim",
        destination_project_dataset_table=f"{PROJECT_ID}.bts.city_tab_dim",
        bucket='dc_bts_project',
        source_objects=[f"transformed_data/city_tab_dim/*.snappy.parquet"],
        source_format='PARQUET',
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )
        
    for table in ["flight_fact_tab"]:
        create_fact_table = GCSToBigQueryOperator(
            task_id=f"create_bq_{table}",
            destination_project_dataset_table=f"{PROJECT_ID}.bts.{table}",
            bucket='dc_bts_project',
            source_objects=[f"transformed_data/{table}/*.snappy.parquet"],
            source_format='PARQUET',
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

    delete_cluster >> create_dim_timeper_tab_table >> create_carrier_tab_dim_table >> create_airport_tab_dim_table >> create_state_tab_dim_table >> create_city_tab_dim_table >> create_fact_table >> end

    