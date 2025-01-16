import logging
import os
from typing import Any

from pythonjsonlogger.jsonlogger import JsonFormatter

from ...metrics import Metrics
from ..commands.nodestream_command import NodestreamCommand
from .operation import Operation


def _get_logger_level():
    return logging.getLevelName(os.environ.get("NODESTREAM_LOG_LEVEL", "INFO").upper())


def configure_logging_with_json_defaults():
    logging.basicConfig(level=_get_logger_level(), force=True)
    old_record_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_record_factory(*args, **kwargs)
        metrics = Metrics.get()
        record.pipeline_name = metrics.pipeline_name
        record.pipeline_scope = metrics.scope_name
        return record

    logging.setLogRecordFactory(record_factory)

    formatter = JsonFormatter(
        "%(name)s %(levelname)s %(pipeline_name)s %(pipeline_scope)s %(message)s",
        timestamp=True,
    )
    logger = logging.getLogger()  # Configure the root logger.
    logger.handlers[0].setFormatter(formatter)


class InitializeLogger(Operation):
    async def perform(self, command: NodestreamCommand) -> Any:
        if command.has_json_logging_set:
            configure_logging_with_json_defaults()
