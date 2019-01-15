from google.cloud import bigquery
import os
import requests
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='C:\\Users\\Gopal Ramaswarmy\\Desktop\\Personal\\Python\\gdog-datalake-dev-e534871aba63.json'

client = bigquery.Client()

url = 'https://jsonplaceholder.typicode.com/todos/1'


response = requests.get(url)

jdata= json.loads(response.content)

for key in jdata:
    userid= response.json()[key]
    idx = response.json()[key]
    title = response.json()[key]
    completed = response.json()[key]

query = 'insert into gdog-datalake-dev.rice3k.testurl (userid, id, title, completed) values (@userid, @id,@title,@completed)'

query_params = [
    bigquery.ScalarQueryParameter('userid', 'INT64', userid),
    bigquery.ScalarQueryParameter('id', 'INT64', idx),
    bigquery.ScalarQueryParameter('title', 'STRING', title),
    bigquery.ScalarQueryParameter('completed', 'STRING', completed)
]

job_config = bigquery.QueryJobConfig()
job_config.query_parameters = query_params
query_job = client.query(
    query,
    location='US',
    job_config=job_config)
    
