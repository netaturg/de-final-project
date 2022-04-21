import boto3
import pandas as pd
from time import sleep
from datetime import date

class AthenaQueryFailed(Exception):
    pass

class Athena:

    def __init__(self, bucket_name, schema_name,crawler_name):
        self.client = boto3.client(service_name='athena', region_name='us-east-1')
        self.bucket_name = bucket_name
        self.schema_name = schema_name
        self.crawler_name = crawler_name
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.result_df = None

    def waiting_crawler_create(self):
        client = boto3.client('glue')
        response = client.list_crawlers()
        available_crawlers = response["CrawlerNames"]
        check_exists = False
        backoff_time = 10
        while not check_exists:
            for c_name in available_crawlers:
                response = client.get_crawler(Name=c_name)
                if response['Crawler']['Name'] == self.crawler_name:
                    check_exists = True
                sleep(backoff_time)

    def run_query(self, query):
        self.waiting_crawler_create()
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': str(self.schema_name)},
            ResultConfiguration={'OutputLocation': 's3://{}/fromglue/'.format(self.bucket_name)},
        )
        query_execution_id = response["QueryExecutionId"]
        self.wait_for_query_to_complete(query_execution_id)
        return response

    def validate_query(self, query_id):
        resp = ["FAILED", "SUCCEEDED", "CANCELLED"]
        response = self.client.get_query_execution(QueryExecutionId=query_id)
        # wait until query finishes
        while response["QueryExecution"]["Status"]["State"] not in resp:
            response = self.client.get_query_execution(QueryExecutionId=query_id)

        return response["QueryExecution"]["Status"]["State"]

    def wait_for_query_to_complete(self, query_execution_id):
        is_query_running = True
        backoff_time = 10
        while is_query_running:
            response = self.get_query_status_response(query_execution_id)
            status = response["QueryExecution"]["Status"]["State"]
            # possible responses: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
            if status == "SUCCEEDED":
                is_query_running = False
            elif status in ["CANCELED", "FAILED"]:
                raise AthenaQueryFailed(status)
            elif status in ["QUEUED", "RUNNING"]:
                sleep(backoff_time)
            else:
                raise AthenaQueryFailed(status)

    def get_query_status_response(self, query_execution_id):
        response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
        return response

    def read(self, query):
        print('start query: {}\n'.format(query))
        qe = self.run_query(query)
        qstate = self.validate_query(qe["QueryExecutionId"])
        print('query state: {}\n'.format(qstate))

        try:
            file_name = "fromglue/{}.csv".format(qe["QueryExecutionId"])
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_name)
            return pd.read_csv(obj['Body'])
        except Exception as err:
            print(str(err))

    def create_query(self, location, employment_type, description):
        string_where = f"where lower(city) like lower('%{location}%') and lower(description) like" \
                        f" lower('%{description}%')"
        return string_where

    def do_query(self, location, employment_type, description):
        current_date= str(date.today()).replace("-", "_")
        print(current_date)
        self.result_df = self.read(f'SELECT distinct * FROM python_2022_04_20 '
                                   f'{self.create_query(location, employment_type, description)}')
        print("Successfully do_query")