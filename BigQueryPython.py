from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='C:\\Users\\Gopal Ramaswarmy\\Desktop\\Personal\\Python\\gdog-datalake-dev-e534871aba63.json'

client = bigquery.Client()

query = 'select reference_name from `gdog-datalake-dev.rice3k.ERS467753` where reference_name = @refname LIMIT 10;'

query_params = [
    bigquery.ScalarQueryParameter('refname', 'STRING', 'Chr4')
]

job_config = bigquery.QueryJobConfig()
job_config.query_parameters = query_params


query_job = client.query(
    query,
    location='US',
    job_config=job_config)

for row in query_job:
    print(row)


#assert query_job.state == 'DONE'
