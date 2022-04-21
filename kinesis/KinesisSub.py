import time

class KinesisSub():

    def __init__(self, stream_name, session):
        self.client = session.client('kinesis')
        self.stream_name = stream_name
        self.ShardIteratorType = 'TRIM_HORIZON'
        self.describe_stream = self.client.describe_stream(StreamName=self.stream_name)
        self.my_shard_id = self.describe_stream['StreamDescription']['Shards'][0]['ShardId']

    def kinesis_get_data(self):
        response = self.client.get_shard_iterator(StreamName=self.stream_name, ShardId=self.my_shard_id, ShardIteratorType=self.ShardIteratorType)
        shard_iter = response['ShardIterator']
        record_response = self.client.get_records(ShardIterator=shard_iter, Limit=1)
        while 'NextShardIterator' in record_response:
             record_response = self.client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=1)
             yield record_response['Records']
             time.sleep(5)
