import json
import requests
import random
import boto3
import logging

from kinesis.KinesisPub import KinesisPub

logger = logging.getLogger()
logger.setLevel(logging.INFO)

NEW_LINE = '\n'
file_path = "linkdinjobs.json"
url = "https://job-search4.p.rapidapi.com/linkedin/search"

headers = {
    "X-RapidAPI-Host": "job-search4.p.rapidapi.com",
    "X-RapidAPI-Key": "82af967b7amshd2264160ab0b289p1f7ffejsn6677f3b5bf4f"
}

session = boto3.session.Session(aws_access_key_id= 'AKIA46BTIN36GXJUOGFO', aws_secret_access_key= '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',region_name='us-east-1')
kinesisPub = KinesisPub('linkdin-raw-data', session)

def kinesis_send_data(stream_name, kinesis_client, jobs):
    for j in jobs:
        partition_key = random.randrange(999, 10000)
        kinesis_client.put_record(StreamName=stream_name, Data=json.dumps(j), PartitionKey=str(partition_key))


def request(i, client):
    querystring = {"page":"{}".format(i)}
    jobs = requests.request("GET", url, headers=headers, params=querystring)
    resp = json.loads(jobs.text)
    resp = resp["jobs"]
    logger.info("Publish MSG")

    for j in resp:
        kinesisPub.kinesis_send_data(j)


def lambda_handler(event, context):
    client = session.client('kinesis')

    for i in range (6):
        request(i, client)

    return {
        'statusCode': 200
    }
