"""
Logging handler that increments metrics based on log levels.
"""

import logging
from typing import Optional

from .metrics import (
    LOG_DEBUG_COUNT,
    LOG_ERROR_COUNT,
    LOG_INFO_COUNT,
    LOG_WARNING_COUNT,
    Metrics,
)


class MetricsLoggingHandler(logging.Handler):
    """A logging handler that increments metrics based on log levels."""

    def __init__(self, metrics: Optional[Metrics] = None):
        super().__init__()
        self.metrics = metrics or Metrics.get()

    def emit(self, record: logging.LogRecord) -> None:
        """Increment the appropriate metric based on log level."""
        try:
            if record.levelno >= logging.ERROR:
                self.metrics.increment(LOG_ERROR_COUNT)
            elif record.levelno >= logging.WARNING:
                self.metrics.increment(LOG_WARNING_COUNT)
            elif record.levelno >= logging.INFO:
                self.metrics.increment(LOG_INFO_COUNT)
            elif record.levelno >= logging.DEBUG:
                self.metrics.increment(LOG_DEBUG_COUNT)
        except Exception:
            self.handleError(record)  # Handles any errors during metric increment