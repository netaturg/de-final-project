import requests
from bs4 import BeautifulSoup as bs
import json
import boto3
from kinesis.KinesisPub import KinesisPub
from kinesis.KinesisSub import KinesisSub

session = boto3.session.Session(aws_access_key_id='',
                                aws_secret_access_key='',
                                region_name='us-east-1')
kinesisPub = KinesisPub('clean-data', session)
kinesisSub = KinesisSub('linkdin-raw-data', session)

def ecrichment_json(job):
    job = job['Data']
    job = json.loads(job)

    # Delete unneeded fields
    del job["source"]
    del job["location"]

    # Add fields using web scaping.
    url = job["detail_url"]
    resp = requests.get(url)
    soup = bs(resp.text, 'html.parser')
    description_job = soup.find_all('li', class_="description__job-criteria-item")

    for item in description_job:
        subheader = item.h3.string.strip()
        criteria = item.span.string.strip()
        job[subheader] = criteria

    return job


def lambda_handler(event, context):

    for jobs in kinesisSub.kinesis_get_data():
        for job in jobs:
            job = ecrichment_json(job)
            kinesisPub.kinesis_send_data(job)

    return {
        'statusCode': 200
    }
