import csv
import datetime
import io
import logging

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import dummy_operator

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
        'composer_sample_gcs_copy_across_locations',
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

	copyFiles = bash_operator.BashOperator(
        task_id='copyFilesacrossBuckets',
        bash_command="gsutil cp gs://dl-data-dev/CampaignTypes.json gs://data-migrations-import"
        )

        start >> copyFiles >> end