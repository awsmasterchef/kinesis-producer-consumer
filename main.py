import json
import time
from enum import Enum
from typing import Tuple, List

import boto3


class KinesisManager:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.kinesis_client = boto3.client('kinesis', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key,region_name="region_name comes here")

    class ShardIteratorType(Enum):
        LATEST = "LATEST"
        TRIM_HORIZON = "TRIM_HORIZON"
        AT_SEQUENCE_NUMBER = "AT_SEQUENCE_NUMBER"
        AFTER_SEQUENCE_NUMBER = "AFTER_SEQUENCE_NUMBER"
        AT_TIMESTAMP = "AT_TIMESTAMP"

    def push_data_to_kinesis(self, stream_name, data, partition_key):
        response = self.kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=partition_key
        )
        return response

    def get_shard_iterator(self, stream_name: str, shard_id: str,
                           shard_iterator_type: ShardIteratorType = ShardIteratorType.LATEST,
                           sequence_number: str = None, timestamp: int = None):
        if shard_iterator_type == self.ShardIteratorType.AT_SEQUENCE_NUMBER or \
                shard_iterator_type == self.ShardIteratorType.AFTER_SEQUENCE_NUMBER:
            if sequence_number is None:
                raise ValueError(
                    "sequence_number must be provided for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER types")
            response = self.kinesis_client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type.value,
                StartingSequenceNumber=sequence_number
            )
        elif shard_iterator_type == self.ShardIteratorType.AT_TIMESTAMP:
            if timestamp is None:
                raise ValueError("timestamp must be provided for AT_TIMESTAMP type")
            response = self.kinesis_client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type.value,
                Timestamp=timestamp
            )
        else:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType=shard_iterator_type.value
            )

        return response['ShardIterator']

    def consume_data(self, shard_iterator: object, limit: int) -> Tuple[object, List]:
        response = self.kinesis_client.get_records(
            ShardIterator=shard_iterator,
            Limit=limit
        )
        records = response['Records']
        shard_iterator = response['NextShardIterator']
        return shard_iterator, records


if __name__ == '__main__':
    stream_name = 'stream_name'
    partition_key = 'key1' #any string value
    manager = KinesisManager(aws_access_key_id="aws_access_key_id",
                             aws_secret_access_key="aws_secret_access_key")  # change it
    # for i in range(10):
    #     data = {'key': i}
    #     print(data)
    #     response = manager.push_data_to_kinesis(stream_name, data, partition_key)
    #     print(response)
    shard_id = 'shardId-000000000000'
    itr = manager.get_shard_iterator(stream_name=stream_name, shard_id=shard_id,
                                     shard_iterator_type=manager.ShardIteratorType.AFTER_SEQUENCE_NUMBER,sequence_number="49654562623121058677445974153872638346489209601398931458")
    while True:
        itr, data = manager.consume_data(itr, 100)
        for d in data:
            print(d['Data'])
