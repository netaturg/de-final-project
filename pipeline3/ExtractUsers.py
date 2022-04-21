import logging
import pymysql
import json
from kinesis.KinesisPub import KinesisPub
from datetime import date
import boto3
from Json import Json
import configparser

config = configparser.ConfigParser()
config.read('app.properties')

# RDS
rds_host = config.get("rds", "rds_host")
rds_user_name = config.get("rds", "rds_name")
rds_password = config.get("rds", "rds_password")
rds_db_name = config.get("rds", "rds_db_name")

# aws
aws_access_key_id = config.get("aws", "aws_access_key_id")
aws_secret_access_key = config.get("aws", "aws_secret_access_key")
region_name = config.get("aws", "region_name")

# kinesis
kinesis_pub = config.get("kinesis", "users_details_pub")

session = boto3.session.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)
kinesisPub = KinesisPub(kinesis_pub, session)

# logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def query(conn):
    logger.info("in insert_row")
    try:
        with conn.cursor() as cur:
            current_date = date.today()
            query = f"SELECT distinct * FROM jobs_project.Users where date like '%{current_date}%'"
            print(query)
            cur.execute(query)
            result = cur.fetchone()
            return result
        # conn.commit()
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not insert row to Users table..")
        logger.error(e)

    logger.info("Insert Success!")


def lambda_handler(event, context):
    logger.info("In here!")
    try:
        conn = pymysql.connect(host=rds_host, user=rds_user_name, passwd=rds_password, db=rds_db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
    print("SUCCESS: Connection to RDS MySQL instance succeeded")
    result = query(conn)
    # unpacking
    user_name, user_id, location, employment_type, description, date = result
    _json = Json(user_name, user_id, location, employment_type, description)
    print('create_load_json:')
    _json = _json.create_load_json()
    kinesisPub.kinesis_send_data(_json)

    return {
        'statusCode': 200
    }