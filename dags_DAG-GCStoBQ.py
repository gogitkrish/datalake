import csv
import datetime
import io
import logging

from airflow import models
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import gcs_to_bq
from airflow.operators import dummy_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# Import operator from plugins
# from gcs_plugin.operators import gcs_to_gcs


# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

# 'table_list_file_path': This variable will contain the location of the master
# file.
# table_list_file_path = models.Variable.get('table_list_file_path')

# Source Bucket
#source_bucket = models.Variable.get('gcs_source_bucket')

# Destination Bucket
#dest_bucket = models.Variable.get('gcs_dest_bucket')

# --------------------------------------------------------------------------------
# Set GCP logging
# --------------------------------------------------------------------------------

#logger = logging.getLogger('bq_copy_us_to_eu_01')

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'composer_sample_bq_copy_across_locations',
        default_args=default_args,
        schedule_interval=None) as dag:
        
	start = dummy_operator.DummyOperator(
	task_id='start',
	trigger_rule='all_success'
	)

	end = dummy_operator.DummyOperator(
	task_id='end',
	trigger_rule='all_success'
	)

	GCS_to_BQ = GoogleCloudStorageToBigQueryOperator(
	task_id='wikidata_names',
	destination_project_dataset_table='gdog-datalake-dev.MXM_FoodSvcDB.CampaignTypes',
	schema_fields=[
		{"name": "fruit", "type": "STRING", "mode": "NULLABLE"},
		{"name": "size", "type": "STRING", "mode": "NULLABLE"},
		{"name": "color", "type": "STRING", "mode": "NULLABLE"}],
	bucket='dl-data-dev',
	source_objects=['CampaignTypes.json'],
	source_format='NEWLINE_DELIMITED_JSON',
	create_disposition='CREATE_IF_NEEDED',
	write_disposition='WRITE_TRUNCATE',
	max_bad_records=0,
	dag=dag)

        start >> GCS_to_BQ >> end