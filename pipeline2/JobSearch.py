import json
import logging
from datetime import date
from kinesis.KinesisSub import KinesisSub
from kinesis.KinesisPub import KinesisPub
import boto3
from pipeline2.Athena import Athena
from pipeline2.Crawel import Crawler
import configparser

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('app.properties')
# aws
aws_access_key_id = config.get("aws", "aws_access_key_id")
aws_secret_access_key = config.get("aws", "aws_secret_access_key")
region_name = config.get("aws", "region_name")
session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)
# kinesis
kinesis_sub_stream = config.get("kinesis", "users_details_pub")
kinesis_pub_stream = config.get("kinesis", "users_db_stream")
kinesisPub = KinesisPub(kinesis_pub_stream, session)
kinesisSub = KinesisSub(kinesis_sub_stream, session)

def preppare_answer(job):
    schema_name = "mydatabase_new"
    today = date.today()
    s3_staging_dir = "s3://stack-overflow-neta/jobs/"
    s3_output_directory = "new"
    bucket_name = 'stack-overflow-neta'
    crawler_name = 'python-lab2'
    crawler = Crawler(aws_access_key_id, aws_secret_access_key, schema_name, s3_staging_dir, region_name,
                      s3_output_directory, crawler_name)
    crawler.create_database()
    crawler.create_glue_crawler()
    if crawler.check_exists_table():
        crawler.update_crawler()
    else:
        crawler.start_crawler()

    # Init Athena obj:
    athena = Athena(bucket_name, schema_name, crawler_name)
    # Creating the query:
    athena.do_query(job['location'], job['employment_type'], job['description'])
    # Sending the answer:
    send_answer(job['chat_id'], athena)

def send_answer(chat_id, athena):
    ans = None

    for job in athena.result_df.values:
        if job.any():
            ans = str(chat_id) + '@' + job[0] + ' ' + job[3]
            logger.info(ans)
            kinesisPub.kinesis_send_data(ans)


def lambda_handler(event, context):
    for jobs in kinesisSub.kinesis_get_data():
        for job in jobs:
            my_json = job['Data']
            resp = json.loads(my_json)
            logger.info("my_json", resp)
            preppare_answer(resp)

    return {
        'statusCode': 200
    }
