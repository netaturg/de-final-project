import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=com.qubole.spark/spark-sql-kinesis_2.11/1.1.3-spark_2.4 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import boto3
import json
from datetime import date

client = boto3.client('kinesis')
stream_name='clean-data'

spark = SparkSession.builder \
         .master('local[*]') \
         .appName('PySparkKinesis') \
         .getOrCreate()

df_countries = spark.read.format("jdbc").option("url", "jdbc:mysql://database-1.cxxzuzxqj9zm.us-east-1.rds.amazonaws.com:3306/jobs_project") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "CountryCode") \
    .option("user", "root").option("password", "root1234").load()

df_countries.printSchema()

kinesis = spark \
        .readStream \
        .format('kinesis') \
        .option('streamName', stream_name) \
        .option('endpointUrl', 'https://kinesis.us-east-1.amazonaws.com')\
        .option('region', 'us-east-1') \
        .option('awsAccessKeyId', 'AKIA46BTIN36GXJUOGFO') \
        .option('awsSecretKey', '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') \
        .option('startingposition', 'TRIM_HORIZON')\
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

current_date = date.today()

df.writeStream \
        .outputMode("append") \
        .format("json") \
        .trigger(processingTime='30 seconds') \
        .repartition(1)\
        .option("path", "s3a://stack-overflow-neta/jobs/{}".format(current_date)) \
        .option("header", True) \
        .option("checkpointLocation", "s3a://stack-overflow-neta/checkpoint") \
        .start() \
        .awaitTermination()



