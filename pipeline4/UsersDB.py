import logging
import pymysql
import json
from kinesis.KinesisSub import KinesisSub
from datetime import date
import configparser

config = configparser.ConfigParser()
config.read('app.properties')

# RDS
rds_host = config.get("rds", "rds_host")
rds_user_name = config.get("rds", "rds_name")
rds_password = config.get("rds", "rds_password")
rds_db_name = config.get("rds", "rds_db_name")

# kinesis
users_db_stream = config.get("kinesis", "users_db_stream")
kinesisSub = KinesisSub(users_db_stream)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def insert_row(conn, name, user_id, location, employment_type, description):
    logger.info("in insert_row")
    try:
        with conn.cursor() as cur:
            current_date = date.today()
            query = 'insert into Users (name, user_id, location, employment_type, description, date) /' \
                    'values("{}", "{}", "{}", "{}", "{}", "{}")'.format(
                name, user_id, location, employment_type, description, current_date)
            print(query)
            cur.execute(query)
        conn.commit()
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not insert row to Users table..")
        logger.error(e)

    logger.info("Insert Success!")


def lambda_handler(event, context):


    try:
        conn = pymysql.connect(host=rds_host, user=rds_user_name, passwd=rds_password, db=rds_db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    for jobs in kinesisSub.kinesis_get_data():
        for job in jobs:
            my_json = job["Data"]
            resp = json.loads(my_json)
            print(resp['name'])
            logger.info("my_json")
            insert_row(conn, resp['name'], resp['user_id'], resp['location'], resp['employment_type'],
                       resp['description'])

