import json
import logging
import pymysql
import pandas as pd

# rds settings
rds_host = "database-1.cxxzuzxqj9zm.us-east-1.rds.amazonaws.com"
name = 'root'
password = 'root1234'
db_name = 'jobs_project'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    empdata = pd.read_csv('data_csv.csv', index_col=False, delimiter=',')

    try:
        conn = pymysql.connect(host=rds_host, user=name, passwd=password, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
    try:
        with conn.cursor() as cur:
            for i, (name_csv, code_csv) in empdata.iterrows():
                sql = "INSERT INTO jobs_project.CountryCode VALUES (\"{}\",\"{}\")".format(name_csv, code_csv)
                cur.execute(sql)
            conn.commit()
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not insert row to CountryCode table..")
        logger.error(e)

    logger.info("Insert Success!")

    return {
        'statusCode': 200
    }
