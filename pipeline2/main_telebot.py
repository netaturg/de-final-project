import json
from pipeline2.firstTelebot import FirstTelebot
from kinesis.KinesisPub import KinesisPub
import configparser
import boto3

BOT_INTERVAL = 3
BOT_TIMEOUT = 30

def main():
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
    kinesis_pub_stream = config.get("kinesis", "users_details_pub")
    kinesisPub = KinesisPub(kinesis_pub_stream, session)
    kinesis_pub_stream2 = config.get("kinesis", "users_table")
    kinesisPub2 = KinesisPub(kinesis_pub_stream2, session)

    bot_p = FirstTelebot()
    bot_p.start_events()
    bot_p.bot.polling(none_stop=True, interval=BOT_INTERVAL, timeout=BOT_TIMEOUT) # looking for message
    indent = 5
    dictionary = {
        'name': 'ori',
        "user_id": bot_p.user_id,
        "location": bot_p.location,
        "employment_type": bot_p.employment_type,
        "description": bot_p.description,
        "chat_id": bot_p.chat_id
    }

    user = json.dumps(dictionary)
    user = json.loads(user)

    for i in range(10):
        kinesisPub.kinesis_send_data(user)
    kinesisPub2.kinesis_send_data(user)


if __name__ == '__main__':
    main()