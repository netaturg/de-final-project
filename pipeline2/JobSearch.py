import json
import logging
from datetime import date
from kinesis.KinesisSub import KinesisSub
from kinesis.KinesisPub import KinesisPub
import boto3
from pipeline2.Athena import Athena
from pipeline2.Crawel import Crawler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.session.Session(aws_access_key_id='',
                                aws_secret_access_key='',
                                region_name='us-east-1')

kinesisPub = KinesisPub('boo', session)
kinesisSub = KinesisSub('moo')

def preppare_answer(job):
    aws_access_key = ""
    aws_secret_key = ""
    schema_name = "mydatabase"
    today = date.today()
    s3_staging_dir = "s3://stack-overflow-neta/jobs/{}".format(today)
    aws_region = "us-east-1"
    s3_output_directory = "new"
    bucket_name = 'stack-overflow-neta'
    crawler_name = 'python-lab1'
    crawler = Crawler(aws_access_key, aws_secret_key, schema_name, s3_staging_dir, aws_region,
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
    send_answer(athena)

def send_answer(athena):
    ans = None

    for job in athena.result_df.values:
        if job.any():
            ans = job[0] + ' ' + job[2]
            logger.info(ans)
            kinesisPub.kinesis_send_data(ans)


def lambda_handler(event, context):
    for jobs in kinesisSub.kinesis_get_data():
        for job in jobs:
            my_json = job['Data']
            resp = json.loads(my_json)
            logger.info("my_json", resp)
            logger.info(resp['name'])
            preppare_answer(resp)

    return {
        'statusCode': 200
    }
