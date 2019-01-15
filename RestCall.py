from google.cloud import bigquery
import os
import requests
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='C:\\Users\\Gopal Ramaswarmy\\Desktop\\Personal\\Python\\gdog-datalake-dev-e534871aba63.json'

client = bigquery.Client()

url = 'https://api.talkwalker.com/api/v3/stream/s/teststream?access_token=demo'
data = { "rules" : [{ "rule_id": "rule-1", "query": "cats" }] }
headers = 'Content-Type: application/json; charset=UTF-8'

response = requests.get(url,params = data)

jdata= json.loads(response.content)

print(jdata)

