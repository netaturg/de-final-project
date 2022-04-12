import random
import json

class KinesisPub():

    def __init__(self, stream_name, session):
        self.client = session.client('kinesis')
        self.stream_name = stream_name

    def kinesis_send_data(self, data):
        partition_key = random.randrange(999, 10000)
        self.client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data),
                PartitionKey=str(partition_key))