import logging

import pytest

from nodestream.logging_metrics import MetricsLoggingHandler
from nodestream.metrics import (
    LOG_DEBUG_COUNT,
    LOG_ERROR_COUNT,
    LOG_INFO_COUNT,
    LOG_WARNING_COUNT,
    ConsoleMetricHandler,
    Metrics,
)


class CapturingConsoleHandler(ConsoleMetricHandler):
    """Console handler that exposes its internal metrics for assertions."""

    def discharge(self):
        # Override to not reset metrics so we can assert after logging
        return {metric.name: value for metric, value in self.metrics.items()}


@pytest.fixture
def metrics():
    # Use a capturing console handler to observe metric values
    handler = CapturingConsoleHandler(command=None)  # command is not used
    with Metrics.capture(handler) as metrics:
        yield metrics


def test_metrics_logging_handler_increments_error(metrics):
    logger = logging.getLogger("test_logger_error")
    logger.setLevel(logging.DEBUG)

    handler = MetricsLoggingHandler(metrics)
    logger.addHandler(handler)

    logger.error("error message")

    # Verify metric incremented
    values = metrics.handler.discharge()
    assert values.get(LOG_ERROR_COUNT.name, 0) == 1


def test_metrics_logging_handler_increments_warning(metrics):
    logger = logging.getLogger("test_logger_warning")
    logger.setLevel(logging.DEBUG)

    handler = MetricsLoggingHandler(metrics)
    logger.addHandler(handler)

    logger.warning("warning message")

    values = metrics.handler.discharge()
    assert values.get(LOG_WARNING_COUNT.name, 0) == 1


def test_metrics_logging_handler_increments_info(metrics):
    logger = logging.getLogger("test_logger_info")
    logger.setLevel(logging.DEBUG)

    handler = MetricsLoggingHandler(metrics)
    logger.addHandler(handler)

    logger.info("info message")

    values = metrics.handler.discharge()
    assert values.get(LOG_INFO_COUNT.name, 0) == 1


def test_metrics_logging_handler_increments_debug(metrics):
    logger = logging.getLogger("test_logger_debug")
    logger.setLevel(logging.DEBUG)

    handler = MetricsLoggingHandler(metrics)
    logger.addHandler(handler)

    logger.debug("debug message")

    values = metrics.handler.discharge()
    assert values.get(LOG_DEBUG_COUNT.name, 0) == 1
