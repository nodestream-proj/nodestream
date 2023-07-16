from logging import getLogger
from typing import Any, Iterable

from .extractor import StreamConnector
from ..credential_utils import AwsClientFactory


class SqsStreamConnector(StreamConnector, alias="sqs"):
    """A SqsStreamConnector implements the StreamConnector interface for SQS.

    The SqsStreamConnector uses the boto3 library to connect to a SQS Queue.
    """

    def __init__(self, queue_url: str, **client_args):
        self.logger = getLogger(self.__class__.__name__)
        self.queue_url = queue_url
        self.boto_client = AwsClientFactory(**client_args).make_client("sqs")

    async def poll(self, timeout: int, max_records: int) -> Iterable[Any]:
        response = self.boto_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_records,
            WaitTimeSeconds=timeout,
        )

        for message in response["Messages"]:
            receipt_handle = message["ReceiptHandle"]
            yield message["Body"]
            self.boto_client.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
            )
