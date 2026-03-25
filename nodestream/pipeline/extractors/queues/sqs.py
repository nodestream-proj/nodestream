import asyncio
from logging import getLogger
from typing import Any, AsyncGenerator, Iterable, Optional

from nodestream.pipeline.extractors.queues.extractor import QueueConnector

from ..credential_utils import AwsClientFactory


class SQSQueueConnector(QueueConnector, alias="sqs"):
    @classmethod
    def from_file_data(
        cls,
        queue_url: str,
        message_system_attribute_names: Optional[list[str]] = ["All"],
        message_attribute_names: Optional[list[str]] = ["All"],
        max_batch_size: int = 10,
        delete_after_read: bool = True,
        max_batches: int = 10,
        **aws_client_args,
    ):
        return cls(
            queue_url=queue_url,
            message_system_attribute_names=message_system_attribute_names,
            message_attribute_names=message_attribute_names,
            max_batch_size=max_batch_size,
            delete_after_read=delete_after_read,
            max_batches=max_batches,
            sqs_client=AwsClientFactory(**aws_client_args).make_client("sqs"),
        )

    def __init__(
        self,
        sqs_client,
        queue_url: str,
        message_system_attribute_names: Optional[list[str]] = ["All"],
        message_attribute_names: Optional[list[str]] = ["All"],
        max_batch_size: int = 10,
        delete_after_read: bool = True,
        max_batches: int = 10,
    ) -> None:
        self.queue_url = queue_url
        self.message_system_attribute_names = message_system_attribute_names
        self.message_attribute_names = message_attribute_names
        self.max_batch_size = max_batch_size
        self.delete_after_read = delete_after_read
        self.max_batches = max_batches
        self.sqs_client = sqs_client
        self.logger = getLogger(__name__)

    async def poll(self) -> Iterable[Any]:
        results = []
        for _ in range(self.max_batches):
            try:
                messages = await self.get_next_messsage_batch()
                if messages is None:
                    self.logger.debug("Polling returned no messages")
                    continue
                for result in messages:
                    results.append(result)
            except Exception as e:
                self.logger.exception(f"error while polling SQS messages: {e}")
                break
        return results

    async def get_next_messsage_batch(self):
        loop = asyncio.get_running_loop()
        msgs = await loop.run_in_executor(None, self.get_message_batch)
        messages = msgs.get("Messages", [])
        if len(messages) == 0:
            return None
        return self.process_messages(messages)

    def process_messages(self, messages):
        for message in messages:
            yield message["Body"]
            if self.delete_after_read:
                self.delete_message(message)

    def get_message_batch(self) -> AsyncGenerator[Any, Any]:
        return self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MessageSystemAttributeNames=self.message_system_attribute_names,
            MessageAttributeNames=self.message_attribute_names,
            MaxNumberOfMessages=self.max_batch_size,
        )

    def delete_message(self, msg):
        receipt_handle = msg.get("ReceiptHandle")
        return self.sqs_client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
        )
