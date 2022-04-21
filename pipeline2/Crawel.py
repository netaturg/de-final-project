import boto3
import time
import timeit
from datetime import date

class Crawler():

    def __init__(self, aws_access_key,aws_secret_key,schema_name,s3_staging_dir,aws_region,s3_output_directory,crawler_name):
        self.client = boto3.client('glue')
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.schema_name = schema_name
        self.s3_staging_dir = s3_staging_dir
        self.aws_region = aws_region
        self.s3_output_directory = s3_output_directory
        self.crawler_name = crawler_name

    # Create database
    def create_database(self):
        response = self.client.get_databases()
        available_databases = response["DatabaseList"]
        check_exists = False
        for db_name in available_databases:
            if db_name['Name'] == self.schema_name:
                check_exists = True

        if not check_exists:
            try:
                response = self.client.create_database(
                    DatabaseInput={
                        'Name': self.schema_name,
                        'Description': 'This database is created using Python boto3',
                    }
                )
                print("Successfully created database")
            #except:
            except Exception as err:
                print("error in creating database")
                print(str(err))
        else:
            print('database already exists')

    # Create Glue Crawler
    def create_glue_crawler(self):
        # check if Crawler already exists
        response = self.client.list_crawlers()
        available_crawlers = response["CrawlerNames"]
        check_exists = False
        for crawler_name in available_crawlers:
            response = self.client.get_crawler(Name=crawler_name)
            if response['Crawler']['Name'] == self.crawler_name:
                check_exists = True

        if not check_exists:
            try:
                response = self.client.create_crawler(
                    Name=self.crawler_name,
                    Role='admin-role',
                    DatabaseName=self.schema_name,
                    Targets={
                        'S3Targets': [
                            {
                                'Path': self.s3_staging_dir
                            }
                        ]
                    },
                    Configuration="{\"Version\":1.0,\"Grouping\":{\"TableGroupingPolicy\":\"CombineCompatibleSchemas\"}}",
                    TablePrefix='python_'
                )
                print("Successfully created crawler")
            except Exception as err:
                print(str(err))
        else:
            print('crawler python-lab1 exists')

    def check_exists_table(self):
        print('check_exists_table')
        try:
            response = self.client.get_tables(DatabaseName=self.schema_name)
            table_list = response['TableList']
            check_exists = False
            for t in table_list:
                if t['Name'] == 'python_2022_04_20':
                    check_exists = True

            print("check_exists_table  " + str(check_exists))
            return check_exists
        except Exception as err:
            print(str(err))

    # This is the command to start the Crawler
    def start_crawler(self):
        try:
            response = self.client.start_crawler(
                Name=self.crawler_name
            )
            self.wait_until_ready()
            print("Successfully started crawler")
        #CrawlerRunningException
        except Exception as err:
            print(str(err))

    def wait_until_ready(self):
        state_previous = None
        timeout_seconds = 120 * 60
        start_time = timeit.default_timer()
        abort_time = start_time + timeout_seconds
        while True:
            response_get = self.client.get_crawler(Name=self.crawler_name)
            state = response_get["Crawler"]["State"]
            if state != state_previous and state != "READY":
                print(f'Crawler {self.crawler_name} is {state.lower()}.')
                # log.info(f"Crawler {crawler} is {state.lower()}.")
                state_previous = state
            if state == "READY":  # Other known states: RUNNING, STOPPING
                print(f'Crawler {self.crawler_name} is {state.lower()}.')
                return
            if timeit.default_timer() > abort_time:
                raise TimeoutError(
                    f"Failed to crawl {self.crawler_name}.")
            time.sleep(20)


    # This is the command to update the Crawler
    def update_crawler(self):
        print('update_crawler')
        try:
            self.stop_crawler()
            response = self.client.update_crawler(
                Name=self.crawler_name
            )
            # self.stop_crawler()
            print("Successfully updated crawler")
        except Exception as err:
            print(str(err))

    # Command to stop the Crawler
    def stop_crawler(self):
        print('stop_crawler')
        try:
            response = self.client.stop_crawler(
                Name=self.crawler_name
            )
            print("Successfully stoped crawler")
        except Exception as err:
            print(str(err))
