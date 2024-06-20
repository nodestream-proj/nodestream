from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, Iterable

from nodestream.pipeline.extractors.streams.extractor import (
    STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY,
    StreamRecordFormat,
)

from ....pluggable import Pluggable
from ....subclass_registry import SubclassRegistry
from ...flush import Flush
from ..extractor import Extractor

QUEUE_CONNECTOR_SUBCLASS_REGISTRY = SubclassRegistry()


@QUEUE_CONNECTOR_SUBCLASS_REGISTRY.connect_baseclass
class QueueConnector(Pluggable, ABC):
    entrypoint_name = "queue_connectors"

    @abstractmethod
    async def poll(self) -> Iterable[Any]:
        raise NotImplementedError


class QueueExtractor(Extractor):
    """A QueueExtractor implements the standard behavior of polling data from a stream.

    The QueueExtractor requires both a QueueConnector and a QueueRecordFormat to delegate
    to for the actual polling implementation and parsing of the data, respectively.
    """

    @classmethod
    def from_file_data(cls, connector: str, record_format: str, **connector_args):
        # Import all plugins so that they can register themselves
        StreamRecordFormat.import_all()
        QueueConnector.import_all()

        object_format_cls = STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY.get(record_format)
        connector_cls = QUEUE_CONNECTOR_SUBCLASS_REGISTRY.get(connector)
        return cls(
            record_format=object_format_cls(),
            connector=connector_cls(**connector_args),
        )

    def __init__(
        self,
        connector: QueueConnector,
        record_format: StreamRecordFormat,
    ):
        self.connector = connector
        self.record_format = record_format
        self.log_flush = True
        self.logger = getLogger(__name__)

    async def poll(self):
        results = await self.connector.poll()
        if len(results) == 0:
            if self.log_flush:
                self.logger.info("flushing extractor because no records were returned")
                self.log_flush = False
            yield Flush
        else:
            self.logger.debug("Received Queue Messages")
            self.log_flush = True
            for record in results:
                yield self.record_format.parse(record)

    async def extract_records(self):
        try:
            while True:
                async for item in self.poll():
                    yield item
        except Exception:
            self.logger.exception("failed extracting records")
