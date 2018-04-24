"""
DAG Function:   This DAG does the following:
                1) Waits for the files to be uploaded to a folder in a landing GCS bucket (Data Lake).
                2) Moves data from a folder in landing bucket to the respective folder in staging GCS bucket.
                3) Uploads the data from staging bucket in Data Lake to a table in BigQuery.
"""

# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------

from airflow import DAG
from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator

from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from datetime import timedelta
from datetime import datetime

import os

# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

# 'project_id': The Project ID of the GCP Project. E.g. 'gcp01-165521'
project_id = Variable.get('gcp_project_id')

bq_destination_dataset_table = Variable.get('bq_destination_dataset_table')

# The bucket names in Cloud Storage
gcs_bucket = Variable.get('gcs_landing_bucket')
gcs_dir = Variable.get('gcs_dir')
schema_gcs_path = Variable.get('schema_gcs_path')
success_file = '_SUCCESS'

# The credentials for service account. Create a connection in the WebUI and enter the name over here
google_cloud_conn_id = 'airflow-service-account'

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

dag = DAG('DAG_3_GCS_To_BigQuery',
          default_args=default_args,
          schedule_interval='@daily')

start = DummyOperator(
    task_id='start',
    trigger_rule='all_success',
    dag=dag
)

GCS_landing_sensor = GoogleCloudStorageObjectSensor(
    task_id='GCS_landing_sensor',
    bucket=gcs_bucket,
    object=os.path.join(gcs_dir, success_file),
    google_cloud_conn_id=google_cloud_conn_id,
    dag=dag
)

GCS_to_BigQuery = GoogleCloudStorageToBigQueryOperator(
    task_id='GCS_to_BigQuery',
    destination_project_dataset_table=bq_destination_dataset_table,
    bucket=gcs_bucket,
    source_format="AVRO",
    source_objects=[os.path.join(gcs_dir, '*.avro')],
    # schema_object=schema_gcs_path,
    create_disposition='CREATE_IF_NEEDED',
    # The following values are supported for `create_disposition`:
    # CREATE_IF_NEEDED: If the table does not exist, BigQuery creates the table.
    # CREATE_NEVER: The table must already exist. If it does not, a 'notFound' error is returned in the job result.
    # The default value is CREATE_IF_NEEDED.
    write_disposition='WRITE_TRUNCATE',
    # The following values are supported for `write_disposition`:
    # WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table data.
    # WRITE_APPEND: If the table already exists, BigQuery appends the data to the table.
    # WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result.
    # The default value is WRITE_EMPTY.
    bigquery_conn_id=google_cloud_conn_id,
    google_cloud_storage_conn_id=google_cloud_conn_id,
    dag=dag
)

start >> GCS_landing_sensor >> GCS_to_BigQuery
