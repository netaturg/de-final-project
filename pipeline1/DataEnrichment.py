import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=com.qubole.spark/spark-sql-kinesis_2.11/1.1.3-spark_2.4 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import json
from datetime import date

client = boto3.client('kinesis')

#country code.
country_code_url = "jdbc:mysql://database-1.cxxzuzxqj9zm.us-east-1.rds.amazonaws.com:3306/jobs_project"
dbtable = "CountryCode"
user = "root"
password = "root1234"

#stream
stream_format = 'kinesis'
stream_name='clean-data'
endpointUrl = 'https://kinesis.us-east-1.amazonaws.com'
region = 'us-east-1'
awsAccessKeyId = ''
awsSecretKey = ''
startingposition = 'TRIM_HORIZON'

#s3
current_date = date.today()
mode = "append"
s3_format = "json"
processingTime = '30 seconds'
s3_path = "s3a://stack-overflow-neta/jobs/{}"
s3_checkpointLocation = "s3a://stack-overflow-neta/checkpoint"

spark = SparkSession.builder \
         .master('local[*]') \
         .appName('PySparkKinesis') \
         .getOrCreate()

df_countries = spark.read.format("jdbc")\
    .option("url", country_code_url) \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", dbtable) \
    .option("user", user).option("password", password).load()

df_countries.printSchema()

kinesis = spark \
        .readStream \
        .format(stream_format) \
        .option('streamName', stream_name) \
        .option('endpointUrl', endpointUrl)\
        .option('region', region) \
        .option('awsAccessKeyId', awsAccessKeyId) \
        .option('awsSecretKey', awsSecretKey) \
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
        .trigger(processingTime=processingTime) \
        .repartition(1)\
        .option("path", s3_path.format(current_date)) \
        .option("header", True) \
        .option("checkpointLocation", s3_checkpointLocation) \
        .start() \
        .awaitTermination()



