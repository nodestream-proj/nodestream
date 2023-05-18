from abc import abstractmethod
from logging import INFO, getLevelName, getLogger
from typing import Any, AsyncGenerator

from .step import Step


class Writer(Step):
    """A `Writer` takes a given record and commits it to a downstream data store.

    `Writer` steps generally make up the end of an ETL pipeline and are responsible
    for ensuring that the newly transformed data is persisted. After writing the record,
    the record is passed downstream.
    """

    async def handle_async_record_stream(
        self, record_stream: AsyncGenerator[Any, Any]
    ) -> AsyncGenerator[Any, Any]:
        async for record in record_stream:
            await self.write_record(record)
            yield record

    @abstractmethod
    async def write_record(self, record: Any):
        raise NotImplementedError


class LoggerWriter(Writer):
    """A `Writer` that logs the record to a logger."""

    def __init__(self, logger_name=None, level=INFO) -> None:
        logger_name = logger_name or self.__class__.__name__
        self.logger = getLogger(logger_name)
        # At first glance this line would appear wrong. `getLevelName` does the
        # mapping bi-directionally. If you give it a number (level), you get the name
        # and vice-versa. Since we are standardizing on the int representation of
        # the level (thats what Logger#log requires), then we need to
        # call `getLevelName` when the value is the string name for the log level.
        #
        # see: https://docs.python.org/3/library/logging.html#logging.getLevelName
        self.level = getLevelName(level) if isinstance(level, str) else level

    async def write_record(self, record):
        self.logger.log(self.level, record)
