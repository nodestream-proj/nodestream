import json
from abc import ABC, abstractmethod
from typing import Any, Iterable

from ....model import JsonLikeDocument
from ....pluggable import Pluggable
from ....subclass_registry import SubclassRegistry
from ...flush import Flush
from ..extractor import Extractor

STREAM_CONNECTOR_SUBCLASS_REGISTRY = SubclassRegistry()
STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY = SubclassRegistry()


@STREAM_CONNECTOR_SUBCLASS_REGISTRY.connect_baseclass
class StreamConnector(Pluggable, ABC):
    entrypoint_name = "stream_connectors"

    @abstractmethod
    async def connect(self):
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self):
        raise NotImplementedError

    @abstractmethod
    async def poll(self) -> Iterable[Any]:
        raise NotImplementedError


@STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY.connect_baseclass
class StreamRecordFormat(Pluggable, ABC):
    entrypoint_name = "record_formats"

    @abstractmethod
    def parse(self, record: Any) -> JsonLikeDocument:
        raise NotImplementedError


class JsonStreamRecordFormat(StreamRecordFormat, alias="json"):
    def parse(self, record: Any) -> JsonLikeDocument:
        return json.loads(record)


class StreamExtractor(Extractor):
    """A StreamExtractor implements the standard behavior of polling data from a stream.

    The StreamExtractor requires both a StreamConnector and a StreamRecordFormat to delegate
    to for the actual polling implementation and parsing of the data, respectively.
    """

    @classmethod
    def from_file_data(cls, connector: str, record_format: str, **connector_args):
        # Import all plugins so that they can register themselves
        StreamRecordFormat.import_all()
        StreamConnector.import_all()

        object_format_cls = STREAM_OBJECT_FORMAT_SUBCLASS_REGISTRY.get(record_format)
        connector_cls = STREAM_CONNECTOR_SUBCLASS_REGISTRY.get(connector)
        return cls(
            record_format=object_format_cls(),
            connector=connector_cls(**connector_args),
        )

    def __init__(
        self,
        connector: StreamConnector,
        record_format: StreamRecordFormat,
    ):
        self.connector = connector
        self.record_format = record_format

    def poll(self):
        return self.connector.poll()

    async def extract_records(self):
        await self.connector.connect()
        try:
            results = await self.poll()
            if len(results) == 0:
                yield Flush
            else:
                for record in results:
                    yield self.record_format.parse(record)
        finally:
            await self.connector.disconnect()
