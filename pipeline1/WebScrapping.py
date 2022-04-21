import requests
from bs4 import BeautifulSoup as bs
import json
import boto3
from kinesis.KinesisPub import KinesisPub
from kinesis.KinesisSub import KinesisSub
import configparser

def __init__(self):
    config = configparser.ConfigParser()
    config.read('app.properties')
    #aws
    aws_access_key_id=config.get("aws", "aws_access_key_id")
    aws_secret_access_key = config.get("aws", "aws_secret_access_key")
    region_name = config.get("aws", "region_name")
    #kinesis
    kinesis_sub_stream = config.get("kinesis", "linkdin_kinesis_pub")
    kinesis_pub_stream=config.get("kinesis", "webScraping_kinesis_pub")

    session = boto3.session.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    self.kinesisSub = KinesisSub(kinesis_sub_stream, session)
    self.kinesisPub = KinesisPub(kinesis_pub_stream, session)

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


def lambda_handler(self, event, context):

    for jobs in self.kinesisSub.kinesis_get_data():
        for job in jobs:
            job = ecrichment_json(job)
            self.kinesisPub.kinesis_send_data(job)

    return {
        'statusCode': 200
    }
