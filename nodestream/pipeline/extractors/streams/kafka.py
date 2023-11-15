from logging import getLogger
from typing import Any, Iterable, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
from aiokafka.helpers import create_ssl_context

from .extractor import StreamConnector

DEFAULT_GROUP_ID = "nodestream"

class KafkaConsumerFactory:
    @classmethod
    def build_consumer_config(cls, offset_reset: str, security_protocol: str, group_id: str, bootstrap_servers: List[str]):
        return {
            "bootstrap.servers": ",".join(bootstrap_servers),
            "auto.offset.reset": offset_reset,
            "security.protocol": security_protocol,
            "group.id": group_id,
        }

    @classmethod
    def new(cls, topic_name: str, group_id: str, bootstrap_servers: List[str], offset_reset: str, security_protocol: str):
        consumer = Consumer(cls.build_consumer_config(offset_reset, security_protocol, group_id, bootstrap_servers))
        consumer.subscribe([topic_name])
        return consumer

class KafkaStreamConnector(StreamConnector, alias="kafka"):
    """A KafkaStreamConnector implements the StreamConnector interface for Kafka.

    The KafkaStreamConnector uses the aiokafka library to connect to a
    Kafka cluster and poll data from it.
    """

    def __init__(
        self,
        bootstrap_servers: List[str],
        topic: str,
        group_id: Optional[str] = None,
        offset_reset: str = "latest",
        security_protocol: str = "PLAINTEXT",
        max_records: int = 10,
        poll_timeout: int = 30,
    ):
        self.bootstrap_servers = ",".join(bootstrap_servers)
        self.topic = topic
        self.max_records = 10
        self.group_id = group_id or DEFAULT_GROUP_ID
        self.consumer = None
        self.offset_reset = offset_reset
        self.security_protocol = security_protocol
        self.max_records = max_records
        self.poll_timeout = poll_timeout
        self.logger = getLogger(__name__)

    async def connect(self):
        self.logger.debug("Starting Connection to Kafka Topic %s", self.topic)
        self.consumer = KafkaConsumerFactory.new(
            topic_name=self.topic_name,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            offset_reset=self.offset_reset,
            security_protocol=self.security_protocol
        )
        self.logger.info("Connected to Kafka Topic %s", self.topic)

    async def disconnect(self):
        await self.consumer.close()

    async def poll(self) -> Iterable[Any]:
        results = []
        for _ in range(self.max_records):
            msg = self.consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    self.logger.error(f'topic:{msg.topic()} partition:{msg.partition()} reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                results.append(msg)
        return results
