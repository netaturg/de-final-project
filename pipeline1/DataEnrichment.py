import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=com.qubole.spark/spark-sql-kinesis_2.11/1.1.3-spark_2.4 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import json
from datetime import date
import configparser


config = configparser.ConfigParser()
config.read('app.properties')
# RDS
rds_host = config.get("rds", "rds_host")
rds_user_name = config.get("rds", "rds_name")
rds_password = config.get("rds", "rds_password")
rds_db_name = config.get("rds", "rds_db_name")
rds_table_name = config.get("rds", "rds_table_name")

#stream
stream_format = config.get("kinesis", "stream_format")
stream_name = config.get("kinesis", "webScraping_kinesis_pub")
endpoint_url = config.get("kinesis", "endpoint_url")

# aws
aws_access_key_id = config.get("aws", "aws_access_key_id")
aws_secret_access_key = config.get("aws", "aws_secret_access_key")
region_name = config.get("aws", "region_name")
startingposition = config.get("aws", "startingposition")

#s3
current_date = date.today()
mode = config.get("s3", "mode")
s3_format = config.get("s3", "s3_format")
processing_time = config.get("s3", "processing_time")
s3_path = config.get("s3", "s3_path")
s3_checkpoint_location = config.get("s3", "s3_checkpoint_location")

spark = SparkSession.builder \
         .master('local[*]') \
         .appName('PySparkKinesis') \
         .getOrCreate()

df_countries = spark.read.format("jdbc")\
    .option("url", rds_host) \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", rds_table_name) \
    .option("user", rds_user_name).option("password", rds_password).load()

df_countries.printSchema()

kinesis = spark \
        .readStream \
        .format(stream_format) \
        .option('streamName', stream_name) \
        .option('endpointUrl', endpoint_url)\
        .option('region', region_name) \
        .option('awsAccessKeyId', aws_access_key_id) \
        .option('awsSecretKey', aws_secret_access_key) \
        .option('startingposition', startingposition)\
        .load()

schema = StructType([
            StructField("title", StringType()),
            StructField("date_posted", StringType()),
            StructField("description", StringType()),
            StructField("detail_url", StringType()),
            StructField("country", StringType()),
            StructField("state", StringType()),
            StructField("city", StringType()),
            StructField("company_name", StringType()),
            StructField("Employment type", StringType())])

df = kinesis \
        .selectExpr('CAST(data AS STRING)') \
        .select(from_json('data', schema).alias('data')) \
        .select('data.*') \
        .toDF("title", "date_posted", "description", "detail_url", "country", "state", "city", "company_name",
              "Employment type")

df = df.join(df_countries ,df.country == df_countries.Code ,"left")\
           .drop('country') \
           .toDF("title","date_posted","description","detail_url","state","city","company_name","Employment type", "Name", "Code")

df.writeStream \
        .outputMode(mode) \
        .format(s3_format) \
        .trigger(processingTime=processing_time) \
        .option("path", s3_path.format(current_date)) \
        .option("header", True) \
        .option("checkpointLocation", s3_checkpoint_location) \
        .start() \
        .awaitTermination()



