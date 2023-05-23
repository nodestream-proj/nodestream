import logging
import os

from pythonjsonlogger.jsonlogger import JsonFormatter


def _get_logger_level():
    return logging.getLevelName(os.environ.get("NODESTREAM_LOG_LEVEL", "INFO").upper())


def configure_logging_with_json_defaults():
    logging.basicConfig(level=_get_logger_level())
    formatter = JsonFormatter("%(name)s %(levelname)s %(message)s", timestamp=True)
    logger = logging.getLogger()  # Configure the root logger.
    logger.handlers[0].setFormatter(formatter)
