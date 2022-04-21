import json
import requests
import random
import boto3
import logging
import configparser
from kinesis.KinesisPub import KinesisPub

logger = logging.getLogger()
logger.setLevel(logging.INFO)

NEW_LINE = '\n'

class API :
    def __init__(self, website_name):
        config = configparser.ConfigParser()
        config.read('app.properties')
        # Linkdin
        self.url = config.get(website_name, "url")
        headers = config.get(website_name, "headers")
        self.headers = json.loads(str(headers))
        #aws
        aws_access_key_id=config.get("aws", "aws_access_key_id")
        aws_secret_access_key = config.get("aws", "aws_secret_access_key")
        region_name = config.get("aws", "region_name")
        #kinesis
        kinesis_pub=config.get("kinesis", "linkdin_kinesis_pub")

        self.session = boto3.session.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
        self.kinesisPub = KinesisPub(kinesis_pub, self.session)

    def kinesis_send_data(stream_name, kinesis_client, jobs):
        for j in jobs:
            partition_key = random.randrange(999, 10000)
            kinesis_client.put_record(StreamName=stream_name, Data=json.dumps(j), PartitionKey=str(partition_key))


    def request(self, i):
        querystring = {"page":"{}".format(i)}
        resp = requests.request("GET", self.url, headers=self.headers, params=querystring)
        jobs = json.loads(resp.text)
        jobs = jobs["jobs"]
        logger.info("Publish MSG")

        for job in jobs:
            self.kinesisPub.kinesis_send_data(job)


def lambda_handler(self, event, context):
    linkdin_api = API("linkdin")

    for i in range (6):
        linkdin_api.request(i)

    return {
        'statusCode': 200
    }
