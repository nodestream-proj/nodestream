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


def test_root_logger_handler_increments_for_distinct_logger(metrics):
    """Attaching handler to root should capture records from a distinct logger."""
    root_logger = logging.getLogger()
    original_level = root_logger.level
    handler = MetricsLoggingHandler(metrics)
    try:
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

        distinct_logger = logging.getLogger("distinct.logger.name")
        distinct_logger.warning("root handler should capture this warning")

        values = metrics.handler.discharge()
        assert values.get(LOG_WARNING_COUNT.name, 0) == 1
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(original_level)


# --- Exception handling tests ---
class DummyMetrics:
    def __init__(self):
        self.calls = 0

    def increment(self, *_, **__):
        self.calls += 1
        raise RuntimeError("increment failed")


class SpyMetricsLoggingHandler(MetricsLoggingHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handled = []

    def handleError(self, record):  # noqa: N802 (match logging API)
        self.handled.append(record)


def test_handler_handles_increment_exception_without_raising():
    logger = logging.getLogger("test_logger_exception")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    dummy_metrics = DummyMetrics()
    handler = SpyMetricsLoggingHandler(dummy_metrics)
    logger.addHandler(handler)

    # Should not raise despite metrics increment failing
    logger.error("trigger exception in metrics")

    assert len(handler.handled) == 1
    assert dummy_metrics.calls == 1

    logger.removeHandler(handler)


def test_root_logger_exception_handling_with_distinct_logger():
    root_logger = logging.getLogger()
    original_level = root_logger.level

    dummy_metrics = DummyMetrics()
    handler = SpyMetricsLoggingHandler(dummy_metrics)

    try:
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(handler)

        distinct_logger = logging.getLogger("distinct.logger.exception")
        distinct_logger.info("trigger info path exception")

        assert len(handler.handled) == 1
        assert dummy_metrics.calls == 1
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(original_level)
