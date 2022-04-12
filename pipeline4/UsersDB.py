import logging
import pymysql
import json
from kinesis.KinesisSub import KinesisSub
from datetime import date

# rds settings
rds_host = "database-1.cxxzuzxqj9zm.us-east-1.rds.amazonaws.com"
name = 'root'
password = 'root1234'
db_name = 'jobs_project'

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def insert_row(conn, name, user_id, location, employment_type, description):
    logger.info("in insert_row")
    try:
        with conn.cursor() as cur:
            current_date = date.today()
            query = 'insert into Users (name, user_id, location, employment_type, description, date) values("{}", "{}", "{}", "{}", "{}", "{}")'.format(
                name, user_id, location, employment_type, description, current_date)
            print(query)
            cur.execute(query)
        conn.commit()
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not insert row to Users table..")
        logger.error(e)

    logger.info("Insert Success!")


def lambda_handler(event, context):
    kinesisSub = KinesisSub('foo')

    try:
        conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
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

