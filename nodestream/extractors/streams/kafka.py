from logging import getLogger
from typing import Any, Iterable, List, Optional

from aiokafka import AIOKafkaConsumer

from .extractor import StreamConnector

DEFAULT_GROUP_ID = "nodestream"


class KafkaStreamConnector(StreamConnector, alias="kafka"):
    """A KafkaStreamConnector implements the StreamConnector interface for Kafka.

    The KafkaStreamConnector uses the aiokafka library to connect to a
    Kafka cluster and poll data from it.
    """

    def __init__(
        self, bootstrap_servers: List[str], topic: str, group_id: Optional[str] = None
    ):
        self.bootstrap_servers = ",".join(bootstrap_servers)
        self.topic = topic
        self.group_id = group_id or DEFAULT_GROUP_ID
        self.consumer = None
        self.logger = getLogger(__name__)

    async def connect(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )
        await self.consumer.start()

    async def disconnect(self):
        await self.consumer.stop()

    async def poll(self, timeout: int, max_records: int) -> Iterable[Any]:
        result = await self.consumer.getmany(
            max_records=max_records, timeout_ms=timeout * 1000
        )
        for tp, messages in result.items():
            self.logger.debug(
                "Recived Kafka Messages",
                extra={"topic": tp.topic, "partition": tp.partition},
            )
            for message in messages:
                yield message.value
