from logging import getLogger
from typing import Any, Iterable, List, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context

from .extractor import StreamConnector

DEFAULT_GROUP_ID = "nodestream"


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
        security_protocol: str = "PLAINTEXT",
        max_poll_interval_ms: int = 30000,
        max_records: int = 10,
        poll_timeout_ms: int = 30000,
    ):
        self.bootstrap_servers = ",".join(bootstrap_servers)
        self.topic = topic
        self.max_records = 10
        self.group_id = group_id or DEFAULT_GROUP_ID
        self.consumer = None
        self.security_protocol = security_protocol
        self.max_poll_interval_ms = (
            max_poll_interval_ms  # must be set higher than poll timeout
        )
        self.max_records = max_records
        self.poll_timeout_ms = poll_timeout_ms
        self.logger = getLogger(__name__)

    async def connect(self):
        self.logger.debug("Starting Connection to Kafka Topic %s", self.topic)

        # set default ssl context when using SSL or SASL_SSL security protocol
        ssl_context = None
        if self.security_protocol in ["SSL", "SASL_SSL"]:
            ssl_context = create_ssl_context()

        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            security_protocol=self.security_protocol,
            ssl_context=ssl_context,
            max_poll_interval_ms=self.max_poll_interval_ms,
            max_poll_records=self.max_records,
        )

        await self.consumer.start()
        self.logger.info("Connected to Kafka Topic %s", self.topic)

    async def disconnect(self):
        await self.consumer.stop()

    async def poll(self) -> Iterable[Any]:
        entries = []
        result = await self.consumer.getmany(timeout_ms=self.poll_timeout_ms)
        for tp, messages in result.items():
            self.logger.debug(
                "Recived Kafka Messages",
                extra={"topic": tp.topic, "partition": tp.partition},
            )
            for message in messages:
                entries.append(message.value)
        return entries
