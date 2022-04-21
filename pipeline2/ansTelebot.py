import telebot
import configparser
import boto3
from kinesis.KinesisSub import KinesisSub
import json

def get_bot():
    token = ''
    return telebot.TeleBot(token)

class AnswerTelebot:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('app.properties')
        # aws
        aws_access_key_id = config.get("aws", "aws_access_key_id")
        aws_secret_access_key = config.get("aws", "aws_secret_access_key")
        region_name = config.get("aws", "region_name")
        session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        region_name=region_name)
        # kinesis
        kinesis_sub_stream = config.get("kinesis", "users_db_stream")
        self.kinesisSub = KinesisSub(kinesis_sub_stream, session)
        self.bot = get_bot()

    def send_msg(self, chat_id, msg):
        self.bot.send_message(chat_id=chat_id, text=msg)

def lambda_handler(event, context):
    tlb = AnswerTelebot()

    for jobs in tlb.kinesisSub.kinesis_get_data():
        for job in jobs:
            my_json = job['Data']
            resp = json.loads(my_json)
            split_resp = resp.split("@")
            tlb.send_msg(split_resp[0], split_resp[1])
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }





