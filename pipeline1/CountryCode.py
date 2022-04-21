import json
import logging
import pymysql
import pandas as pd
import configparser

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def __init__(self):
    config = configparser.ConfigParser()
    config.read('app.properties')
    #RDS
    self.rds_host = config.get("rds", "rds_host")
    self.rds_name = config.get("rds", "rds_name")
    self.rds_password = config.get("rds", "rds_password")
    self.rds_db_name = config.get("rds", "rds_db_name")
    self.rds_table_name = config.get("rds", "rds_table_name")

def lambda_handler(self, event, context):
    empdata = pd.read_csv('data_csv.csv', index_col=False, delimiter=',')

    try:
        conn = pymysql.connect(host=self.rds_host, user=self.name, passwd=self.password, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
    try:
        with conn.cursor() as cur:
            for i, (name_csv, code_csv) in empdata.iterrows():
                sql = "INSERT INTO {}.{} VALUES (\"{}\",\"{}\")".format(self.rds_db_name, self.rds_table_namename_csv, code_csv)
                cur.execute(sql)
            conn.commit()
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not insert row to CountryCode table..")
        logger.error(e)

    logger.info("Insert Success!")

    return {
        'statusCode': 200
    }
